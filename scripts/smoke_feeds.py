#!/usr/bin/env python
"""Live smoke test of the external market-data feeds: PASS/FAIL per venue.

    python scripts/smoke_feeds.py                          # every enabled feed, 30 s
    python scripts/smoke_feeds.py --venues kraken,coinbase --seconds 60
    python scripts/smoke_feeds.py --venues deribit --save data/smoke   # keep the raw capture

Each feed runs through the production client (dh.feeds, same code as scripts/record.py) for
``--seconds``; the live normalized events are then checked:

  connected     transport connected and venue frames received
  snapshot      first book snapshot (BBO for BBO-only feeds, option quote for options) and
                its delay after connecting
  updates       >= --min-updates book updates / BBOs / quotes
  continuity    no sequence gaps (FeedStatus 'gap') and no normalizer errors
  checksum      venue checksum verified (Kraken CRC32), else n/a
  top_of_book   books valid, not crossed, spread <= --max-spread-bps, mid within --max-dev
                of the cross-venue median mid
  rate          venue frames per second (informational)
  latency       local receive time minus exchange timestamp, p50/p90/p99 ms; WARN when the
                median is negative beyond clock-skew tolerance or above 2 s
  aggressor     trade side vs the prevailing book: buyer-initiated trades should print at or
                above the mid. A low score means the side mapping is inverted (see each
                venue's TRADE_SIDE assumption); FAIL below 0.5 with >= 20 trades
  replay        re-normalizing the captured raw frames with a fresh state reproduces the live
                events exactly (determinism)

Exit status 0 only if no venue FAILs. Needs outbound network access to the venues.
"""

from __future__ import annotations

import argparse
import asyncio
import statistics
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import orjson

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from dh.core.events import (  # noqa: E402
    Event,
    ExtBBO,
    ExtBookDelta,
    ExtBookSnapshot,
    ExtTrade,
    FeedStatus,
    OptionQuote,
    PerpState,
)
from dh.feeds.base import FeedClient, is_marker, safe_normalize  # noqa: E402
from dh.feeds.books import BookTracker  # noqa: E402
from dh.feeds.registry import build_feeds, load_feeds_config  # noqa: E402

PASS, WARN, FAIL, NA = "PASS", "WARN", "FAIL", "n/a"


@dataclass
class Probe:
    key: str
    feed: FeedClient
    raw: list[tuple[str, int, bytes]] = field(default_factory=list)
    events: list[Event] = field(default_factory=list)
    frames: int = 0
    bytes: int = 0
    first_frame_ns: int = 0
    connected_ns: int = 0

    def emit_raw(self, stream: str, t: int, raw: bytes) -> None:
        self.raw.append((stream, t, raw))
        if is_marker(raw):
            m = orjson.loads(raw)
            if m.get("_dh") == "status" and m.get("status") == "connected" and not self.connected_ns:
                self.connected_ns = t
            return
        self.frames += 1
        self.bytes += len(raw)
        if not self.first_frame_ns:
            self.first_frame_ns = t


@dataclass
class Check:
    name: str
    result: str
    detail: str = ""


def _pct(xs: list[float], q: float) -> float:
    if not xs:
        return float("nan")
    xs = sorted(xs)
    i = min(len(xs) - 1, max(0, int(round(q * (len(xs) - 1)))))
    return xs[i]


def evaluate(p: Probe, seconds: float, args: argparse.Namespace, median_mid: float | None) -> list[Check]:
    ev = p.events
    checks: list[Check] = []
    statuses = [e for e in ev if isinstance(e, FeedStatus)]
    connected = any(s.status == "connected" and s.stream == p.feed.name for s in statuses)
    errors = [s for s in statuses if s.status == "error"]
    ok = connected and p.frames > 0
    checks.append(Check("connected", PASS if ok else FAIL, f"frames={p.frames}" + (f" first_error={errors[0].detail[:120]}" if errors and not ok else "")))

    options_feed = "options" in p.feed.channels
    snaps = [e for e in ev if isinstance(e, ExtBookSnapshot)]
    bbos = [e for e in ev if isinstance(e, ExtBBO)]
    quotes = [e for e in ev if isinstance(e, OptionQuote)]
    if options_feed:
        first = quotes[0] if quotes else None
    else:
        first = snaps[0] if snaps else (bbos[0] if bbos else None)
    if first is None:
        checks.append(Check("snapshot", FAIL, "no snapshot/BBO/quote"))
    else:
        delay = (first.ts - p.connected_ns) / 1e6 if p.connected_ns else float("nan")
        checks.append(Check("snapshot", PASS, f"{type(first).__name__} after {delay:.0f} ms"))

    n_upd = sum(isinstance(e, (ExtBookDelta, ExtBBO, OptionQuote)) for e in ev) + max(0, len(snaps) - 1)
    checks.append(Check("updates", PASS if n_upd >= args.min_updates else FAIL, f"n={n_upd} (min {args.min_updates})"))

    gaps = [s for s in statuses if s.status == "gap"]
    resynced = [s for s in statuses if s.status == "resynced"]
    norm_err = [s for s in errors if s.detail.startswith("normalize")]
    cont = FAIL if gaps or norm_err else PASS
    detail = f"gaps={len(gaps)} resynced={len(resynced)} normalize_errors={len(norm_err)}"
    if gaps:
        detail += f" first_gap='{gaps[0].detail[:100]}'"
    if norm_err:
        detail += f" first_error='{norm_err[0].detail[:160]}'"
    checks.append(Check("continuity", cont, detail))

    st = p.feed.state.stats
    if "checksum_ok" in st or "checksum_fail" in st:
        cs_ok, cs_bad = st.get("checksum_ok", 0), st.get("checksum_fail", 0)
        checks.append(Check("checksum", PASS if cs_ok and not cs_bad else FAIL, f"ok={cs_ok} fail={cs_bad}"))
    else:
        checks.append(Check("checksum", NA))

    tr = BookTracker(track_kalshi=False)
    trades_ok = trades_n = 0
    for e in ev:
        if isinstance(e, ExtTrade):
            b = tr.books.get((e.venue, e.symbol))
            top = b.top() if b is not None and b.valid else None
            bbo = tr.bbo.get((e.venue, e.symbol))
            mid = top.mid if top is not None else (0.5 * (bbo.bid + bbo.ask) if bbo is not None else None)
            if mid is not None and e.aggressor:
                trades_n += 1
                if (e.aggressor == "buy" and e.price >= mid) or (e.aggressor == "sell" and e.price <= mid):
                    trades_ok += 1
        tr.on_event(e)
    tob: list[str] = []
    res = PASS if not options_feed else NA
    for (_venue, sym), b in sorted(tr.books.items()):
        t = b.top()
        if t is None or not b.valid:
            res = FAIL
            tob.append(f"{sym}: invalid/empty")
            continue
        spread = (t.ask - t.bid) / t.mid * 1e4
        dev = abs(t.mid / median_mid - 1) if median_mid else 0.0
        bad = t.bid >= t.ask or spread > args.max_spread_bps or dev > args.max_dev
        if bad:
            res = FAIL
        tob.append(f"{sym}: {t.bid:.2f}/{t.ask:.2f} spread={spread:.2f}bp dev={dev * 1e4:.0f}bp levels={len(b.bids)}/{len(b.asks)}")
    for (venue, sym), q in sorted(tr.bbo.items()):
        if (venue, sym) not in tr.books:
            spread = (q.ask - q.bid) / ((q.ask + q.bid) / 2) * 1e4
            if q.bid >= q.ask or spread > args.max_spread_bps:
                res = FAIL
            tob.append(f"{sym} BBO: {q.bid:.2f}/{q.ask:.2f} spread={spread:.2f}bp")
    if not tob and not options_feed:
        res = FAIL
    checks.append(Check("top_of_book", res, "; ".join(tob)[:300]))

    rate = p.frames / seconds if seconds > 0 else 0.0
    checks.append(Check("rate", PASS if p.frames else FAIL, f"{rate:.1f} frames/s, {p.bytes / max(seconds, 1e-9) / 1e3:.1f} kB/s"))

    lat = [(e.ts - e.ts_exch) / 1e6 for e in ev if not isinstance(e, FeedStatus) and getattr(e, "ts_exch", 0) > 0]
    if lat:
        p50, p90, p99 = _pct(lat, 0.5), _pct(lat, 0.9), _pct(lat, 0.99)
        lres = PASS if -args.clock_tolerance_ms <= p50 <= 2000 else WARN
        checks.append(Check("latency", lres, f"recv-exch p50={p50:.1f} p90={p90:.1f} p99={p99:.1f} ms (n={len(lat)})"))
    else:
        checks.append(Check("latency", NA, "venue sends no timestamps on these messages"))

    if trades_n:
        score = trades_ok / trades_n
        ares = PASS if score >= 0.8 else (WARN if score >= 0.5 or trades_n < 20 else FAIL)
        checks.append(Check("aggressor", ares, f"{score:.2f} of {trades_n} trades consistent with the book"))
    else:
        checks.append(Check("aggressor", NA, "no trades with a valid book"))

    fresh = p.feed.new_state(p.feed.name)
    replayed: list[Event] = []
    for _s, t, raw in p.raw:
        replayed += safe_normalize(p.feed.normalize, raw, t, fresh)
    checks.append(Check("replay", PASS if replayed == ev else FAIL, f"{len(ev)} events"))
    perps = [e for e in ev if isinstance(e, PerpState)]
    notes = dict(st)
    if perps:
        last = perps[-1]
        notes["perp"] = f"mark={last.mark} index={last.index} funding={last.funding_rate} OI={last.open_interest:.1f}"
    if quotes:
        notes["option_instruments"] = len({q.instrument for q in quotes})
    checks.append(Check("notes", NA, orjson.dumps(notes, default=str).decode()[:300]))
    return checks


async def run_probes(probes: list[Probe], seconds: float) -> None:
    tasks = [asyncio.create_task(p.feed.run(p.emit_raw), name=p.key) for p in probes]
    await asyncio.sleep(seconds)
    for p in probes:
        p.feed.stop()
    await asyncio.sleep(0.5)
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--config", default=str(REPO / "config" / "feeds.yaml"))
    ap.add_argument("--venues", default="", help="comma list of feed keys / venues / streams (default: enabled feeds)")
    ap.add_argument("--all", action="store_true", help="include disabled feeds (e.g. geo-restricted binance)")
    ap.add_argument("--seconds", type=float, default=30.0)
    ap.add_argument("--min-updates", type=int, default=20)
    ap.add_argument("--max-spread-bps", type=float, default=50.0)
    ap.add_argument("--max-dev", type=float, default=0.01, help="max |mid / cross-venue median - 1|")
    ap.add_argument("--clock-tolerance-ms", type=float, default=100.0)
    ap.add_argument("--save", default="", help="also record the raw capture into this data root")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args(argv)

    cfg = load_feeds_config(args.config)
    only = [v for v in args.venues.split(",") if v] or None
    feeds = build_feeds(cfg, only=only, include_disabled=args.all)
    probes: list[Probe] = []
    for key, feed in feeds.items():
        if not feed.implemented:
            print(f"{key}: stub feed, skipped")
            continue
        probe = Probe(key, feed)
        feed.on_event = probe.events.append
        probes.append(probe)
    if not probes:
        print("no feeds selected")
        return 2
    print(f"running {len(probes)} feeds for {args.seconds:.0f} s: {', '.join(p.key for p in probes)}")
    t0 = time.monotonic()
    asyncio.run(run_probes(probes, args.seconds))
    elapsed = time.monotonic() - t0

    mids = []
    for p in probes:
        tr = BookTracker(track_kalshi=False)
        for e in p.events:
            tr.on_event(e)
        for b in tr.valid_books().values():
            t = b.top()
            if t is not None and "options" not in p.feed.channels:
                mids.append(t.mid)
    median_mid = statistics.median(mids) if mids else None

    if args.save:
        from dh.store.recorder import Recorder

        rec = Recorder(args.save, start=False)
        for p in probes:
            for s, t, raw in p.raw:
                rec.write(s, t, raw)
        rec.close()
        print(f"raw capture saved under {Path(args.save) / 'raw'}")

    results: dict[str, Any] = {}
    any_fail = False
    for p in probes:
        checks = evaluate(p, elapsed, args, median_mid)
        verdict = FAIL if any(c.result == FAIL for c in checks) else (WARN if any(c.result == WARN for c in checks) else PASS)
        any_fail |= verdict == FAIL
        results[p.key] = {"verdict": verdict, "stream": p.feed.name, "checks": [c.__dict__ for c in checks]}
        if not args.json:
            print(f"\n{p.key:<18} {verdict}   ({p.feed.url[:80]})")
            for c in checks:
                print(f"    {c.name:<12} {c.result:<4}  {c.detail}")
            if p.feed.geo_note and verdict == FAIL:
                print(f"    note: {p.feed.geo_note}")
    if args.json:
        print(orjson.dumps(results, option=orjson.OPT_INDENT_2).decode())
    else:
        print("\nsummary: " + ", ".join(f"{k}={v['verdict']}" for k, v in results.items()))
        if median_mid:
            print(f"cross-venue median mid: {median_mid:.2f}")
    return 1 if any_fail else 0


if __name__ == "__main__":
    sys.exit(main())
