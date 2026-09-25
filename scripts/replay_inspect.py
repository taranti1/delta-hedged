#!/usr/bin/env python
"""Inspect the raw store: coverage, event counts, gaps, top of book at a time, compaction.

    python scripts/replay_inspect.py list                              # streams + coverage
    python scripts/replay_inspect.py count --t0 2026-09-25T12 --t1 2026-09-25T13
    python scripts/replay_inspect.py gaps --streams 'coinbase.ws,kraken.ws' --silence 5
    python scripts/replay_inspect.py top --at 2026-09-25T12:34:56Z --warmup 3600
    python scripts/replay_inspect.py clock
    python scripts/replay_inspect.py compact --streams coinbase.ws --day 2026-09-25 --events

Times: ISO-8601 UTC ('2026-09-25T12:34:56.5Z', '2026-09-25 12:34', '2026-09-25'), or an
integer epoch in s / ms / us / ns (by magnitude). ``--streams`` takes a comma list with
fnmatch patterns ('kalshi.*'); default = every stream under <root>/raw.
"""

from __future__ import annotations

import argparse
import calendar
import re
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

import orjson

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from dh.core.events import FeedStatus  # noqa: E402
from dh.feeds.books import BookTracker  # noqa: E402
from dh.feeds.composite import nowcast  # noqa: E402
from dh.feeds.registry import SPOT_CONSTITUENTS  # noqa: E402
from dh.store.recorder import HOUR_NS  # noqa: E402
from dh.store.replay import (  # noqa: E402
    Normalizers,
    ReadStats,
    iter_raw,
    iter_records_events,
    list_streams,
    read_index,
    resolve_streams,
    segment_files,
)

END = 2**63 - 1
_ISO = re.compile(r"^(\d{4})-(\d{2})-(\d{2})(?:[T ](\d{2})(?::(\d{2})(?::(\d{2})(\.\d+)?)?)?)?Z?$")


def parse_time(s: str | None, default: int) -> int:
    """ISO-8601 UTC or epoch number -> ns."""
    if not s:
        return default
    s = s.strip()
    if re.fullmatch(r"\d+(\.\d+)?", s):
        x = float(s)
        if x > 1e17:
            return int(s) if "." not in s else int(x)
        if x > 1e14:
            return int(x * 1e3)
        if x > 1e11:
            return int(x * 1e6)
        return int(x * 1e9)
    m = _ISO.match(s)
    if not m:
        raise SystemExit(f"cannot parse time {s!r}")
    y, mo, d, h, mi, sec, frac = m.groups()
    secs = calendar.timegm((int(y), int(mo), int(d), int(h or 0), int(mi or 0), int(sec or 0), 0, 0, 0))
    ns = int((frac or ".0")[1:].ljust(9, "0")[:9]) if frac else 0
    return secs * 1_000_000_000 + ns


def fmt_ns(t: int) -> str:
    if not t:
        return "-"
    s, ns = divmod(t, 1_000_000_000)
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(s)) + f".{ns // 1_000_000:03d}Z"


def _streams(args: argparse.Namespace) -> list[str]:
    pats = [x for x in (args.streams or "").split(",") if x]
    return resolve_streams(args.root, pats or None)


# ============================================================================ commands
def cmd_list(args: argparse.Namespace) -> int:
    streams = list_streams(args.root)
    if not streams:
        print(f"no streams under {Path(args.root) / 'raw'}")
        return 1
    print(f"{'stream':<24} {'segs':>5} {'open':>5} {'records':>11} {'MB':>9}  first -> last")
    any_open = False
    for s in streams:
        segs = segment_files(args.root, s)
        n_open = 0
        count = 0
        size = 0
        first_t, last_t = 0, 0
        for hs, _part, path in segs:
            size += path.stat().st_size
            idx = read_index(path)
            if idx is None:
                n_open += 1
                if args.scan:
                    for rec in iter_raw(args.root, [s], hs, hs + HOUR_NS):
                        count += 1
                        first_t = rec.t if not first_t else min(first_t, rec.t)
                        last_t = max(last_t, rec.t)
                else:
                    first_t = first_t or hs
                    last_t = max(last_t, hs + HOUR_NS - 1)
                continue
            count += int(idx["count"])
            first_t = int(idx["min_t"]) if not first_t else min(first_t, int(idx["min_t"]))
            last_t = max(last_t, int(idx["max_t"]))
        any_open |= n_open > 0
        cnt = f"{count}" if (n_open == 0 or args.scan) else f">={count}"
        print(f"{s:<24} {len(segs):>5} {n_open:>5} {cnt:>11} {size / 1e6:>9.2f}  {fmt_ns(first_t)} -> {fmt_ns(last_t)}")
    if any_open and not args.scan:
        print("(open/unindexed segments: live or crashed; counted by hour only, use --scan for exact numbers)")
    return 0


def cmd_count(args: argparse.Namespace) -> int:
    t0, t1 = parse_time(args.t0, 0), parse_time(args.t1, END)
    streams = _streams(args)
    raw_n: Counter[str] = Counter()
    ev_n: dict[str, Counter[str]] = defaultdict(Counter)
    stats = ReadStats()
    for rec, evs in iter_records_events(args.root, streams, t0, t1, stats, Normalizers()):
        raw_n[rec.stream] += 1
        for e in evs:
            name = type(e).__name__
            if isinstance(e, FeedStatus):
                name += f"[{e.status}]"
            ev_n[rec.stream][name] += 1
    for s in streams:
        print(f"{s}: {raw_n[s]} records")
        for name, n in sorted(ev_n[s].items()):
            print(f"    {name:<32} {n}")
    _print_read_stats(stats)
    return 0


def cmd_gaps(args: argparse.Namespace) -> int:
    t0, t1 = parse_time(args.t0, 0), parse_time(args.t1, END)
    silence_ns = int(args.silence * 1e9)
    last_t: dict[str, int] = {}
    silences: dict[str, list[tuple[int, int]]] = defaultdict(list)
    order_viol: Counter[str] = Counter()
    status: dict[str, list[FeedStatus]] = defaultdict(list)
    stats = ReadStats()
    for rec, evs in iter_records_events(args.root, _streams(args), t0, t1, stats, Normalizers()):
        prev = last_t.get(rec.stream)
        if prev is not None:
            if rec.t < prev:
                order_viol[rec.stream] += 1
            elif rec.t - prev > silence_ns:
                silences[rec.stream].append((prev, rec.t))
        last_t[rec.stream] = rec.t if prev is None else max(prev, rec.t)
        for e in evs:
            if isinstance(e, FeedStatus) and e.status != "connected":
                status[rec.stream].append(e)
    for s in sorted(set(last_t) | set(status)):
        sil = silences.get(s, [])
        sts = status.get(s, [])
        kinds = Counter(f"{e.status}" for e in sts)
        print(f"{s}: silences>{args.silence:g}s={len(sil)} time_order_violations={order_viol[s]} statuses={dict(kinds)}")
        for a, b in sil[: args.limit]:
            print(f"    silence {fmt_ns(a)} -> {fmt_ns(b)} ({(b - a) / 1e9:.1f} s)")
        for e in sts[: args.limit]:
            print(f"    {fmt_ns(e.ts)} {e.stream} {e.status} {e.detail[:120]}")
    _print_read_stats(stats)
    return 0


def cmd_top(args: argparse.Namespace) -> int:
    at = parse_time(args.at, 0)
    if not at:
        raise SystemExit("--at is required")
    t0 = at - int(args.warmup * 1e9)
    tr = BookTracker()
    n = 0
    for _rec, evs in iter_records_events(args.root, _streams(args), t0, at + 1, None, Normalizers()):
        for e in evs:
            tr.on_event(e)
            n += 1
    print(f"state at {fmt_ns(at)} after {n} events since {fmt_ns(t0)}")
    for (venue, sym), b in sorted(tr.books.items()):
        t = b.top()
        if t is None:
            print(f"  {venue:<16} {sym:<14} empty valid={b.valid}")
            continue
        spread = (t.ask - t.bid) / t.mid * 1e4
        print(f"  {venue:<16} {sym:<14} {t.bid_size:10.4f} @ {t.bid:<12.2f} | {t.ask:<12.2f} @ {t.ask_size:<10.4f} "
              f"mid={t.mid:.2f} spr={spread:.2f}bp levels={len(b.bids)}/{len(b.asks)} age={(at - b.ts) / 1e9:.1f}s valid={b.valid}")
    for (venue, sym), q in sorted(tr.bbo.items()):
        print(f"  {venue:<16} {sym:<14} BBO {q.bid_size:.4f} @ {q.bid:.2f} | {q.ask:.2f} @ {q.ask_size:.4f} age={(at - q.ts) / 1e9:.1f}s")
    for (venue, sym), p in sorted(tr.perp.items()):
        print(f"  {venue:<16} {sym:<14} PERP mark={p.mark} index={p.index} funding={p.funding_rate} OI={p.open_interest:.1f}")
    for iid, i in sorted(tr.index.items()):
        print(f"  index {iid:<24} {i.value} ({i.feed}) age={(at - i.ts) / 1e9:.1f}s")
    for ticker, kb in sorted(tr.kalshi.items())[: args.limit]:
        print(f"  kalshi {ticker:<32} bid={kb.best_bid()} ask={kb.best_ask()} valid={kb.valid}")
    spot = tr.books_by_venue(SPOT_CONSTITUENTS)
    if spot:
        nc = nowcast({v: b for v, b in spot.items() if b.valid}, at)
        print(f"  nowcast median_mid={nc.median_mid} depth_weighted_mid={nc.depth_weighted_mid} microprice_median={nc.microprice_median}")
        if nc.brti is not None:
            print(f"  BRTI replica={nc.brti.value:.2f} v_T={nc.brti.utilized_depth:.3f} BTC venues={','.join(nc.brti.venues)}")
        for q in nc.quotes:
            if not q.included:
                print(f"    excluded {q.venue}: {q.reason}")
    return 0


def cmd_clock(args: argparse.Namespace) -> int:
    t0, t1 = parse_time(args.t0, 0), parse_time(args.t1, END)
    rows: list[dict[str, Any]] = []
    for rec in iter_raw(args.root, ["clock"], t0, t1):
        o = orjson.loads(rec.data)
        o["t"] = rec.t
        rows.append(o)
    if not rows:
        print("no clock samples")
        return 1
    offs = [abs(r["offset_s"]) for r in rows if isinstance(r.get("offset_s"), (int, float))]
    unsynced = sum(1 for r in rows if r.get("synced") is False)
    print(f"{len(rows)} samples {fmt_ns(rows[0]['t'])} -> {fmt_ns(rows[-1]['t'])}; sources={Counter(r.get('src') for r in rows)}")
    if offs:
        print(f"|offset| max={max(offs) * 1e3:.3f} ms median={sorted(offs)[len(offs) // 2] * 1e3:.3f} ms; unsynced samples={unsynced}")
    for r in rows[-args.limit :]:
        print(f"  {fmt_ns(r['t'])} src={r.get('src')} offset_s={r.get('offset_s')} est_error_s={r.get('est_error_s')} synced={r.get('synced')}")
    return 0


def cmd_compact(args: argparse.Namespace) -> int:
    from dh.store.parquet import compact_events, compact_raw

    if not args.day:
        raise SystemExit("--day YYYY-MM-DD is required")
    for s in _streams(args):
        if not args.events or args.raw:
            p = compact_raw(args.root, s, args.day)
            print(f"{s}: raw -> {p}")
        if args.events:
            out = compact_events(args.root, s, args.day, warmup_ns=int(args.warmup * 1e9))
            for name, p in out.items():
                print(f"{s}: {name} -> {p}")
    return 0


def _print_read_stats(st: ReadStats) -> None:
    if st.truncated_files or st.corrupt_files or st.bad_lines:
        print(f"read issues: truncated={st.truncated_files} corrupt={st.corrupt_files} bad_lines={st.bad_lines}")
    if st.time_order_violations:
        print(f"time-order violations (t decreased within a stream): {st.time_order_violations}")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--root", default=str(REPO / "data"), help="data root (contains raw/)")
    sub = ap.add_subparsers(dest="cmd", required=True)
    sp = sub.add_parser("list", help="streams and time coverage")
    sp.add_argument("--scan", action="store_true", help="read unindexed (open/crashed) segments for exact counts")
    for name, helptext in (("count", "events by type per stream"), ("gaps", "silences, time-order violations, gap/stale/disconnect statuses"),
                           ("clock", "clock-health samples")):
        sp = sub.add_parser(name, help=helptext)
        sp.add_argument("--streams", default="")
        sp.add_argument("--t0", default=None)
        sp.add_argument("--t1", default=None)
        sp.add_argument("--silence", type=float, default=10.0, help="report record gaps longer than this (s)")
        sp.add_argument("--limit", type=int, default=20)
    sp = sub.add_parser("top", help="reconstructed top of book (and nowcast) at a timestamp")
    sp.add_argument("--at", required=True)
    sp.add_argument("--warmup", type=float, default=3600.0, help="seconds replayed before --at")
    sp.add_argument("--streams", default="")
    sp.add_argument("--limit", type=int, default=20)
    sp = sub.add_parser("compact", help="Parquet compaction of one day")
    sp.add_argument("--streams", default="")
    sp.add_argument("--day", required=True)
    sp.add_argument("--events", action="store_true", help="normalized events per type")
    sp.add_argument("--raw", action="store_true", help="raw records (default when --events is not given)")
    sp.add_argument("--warmup", type=float, default=0.0)
    args = ap.parse_args(argv)
    return {"list": cmd_list, "count": cmd_count, "gaps": cmd_gaps, "top": cmd_top, "clock": cmd_clock, "compact": cmd_compact}[args.cmd](args)


if __name__ == "__main__":
    sys.exit(main())
