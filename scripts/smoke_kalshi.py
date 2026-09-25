#!/usr/bin/env python
"""Manual LIVE check of the Kalshi adapter (REST + WS + BRTI). Not part of pytest.

    export KALSHI_KEY_ID=<key id>  KALSHI_PRIVATE_KEY_PATH=~/.kalshi/key.pem
    python scripts/smoke_kalshi.py --seconds 30            # production
    python scripts/smoke_kalshi.py --demo --seconds 30     # demo environment
    python scripts/smoke_kalshi.py --record /tmp/kalshi_smoke.jsonl   # keep raw frames for replay

Checks (PASS/FAIL each; exit 1 if any fails):
  1. GET /exchange/status
  2. GET /account/limits + /account/endpoint_costs -> rate limiter configured
  3. current KXBTCD event: markets, MarketSpecs, rules sanity flags, fee schedule
  4. REST orderbooks (GET /markets/orderbooks) for the N markets nearest 50c
  5. WS for --seconds: orderbook_delta + trade on those markets, cfbenchmarks_value and
     cfbenchmarks_value_5hz on BRTI (signed handshake, subscriptions, sequencing)
  6. WS-maintained books == a fresh REST snapshot (up to 5 attempts to avoid races) — also
     validates the NO-side price convention (use_yes_price=false)
  7. BRTI tick rate per feed, latency recv - source_ts and Kalshi hop received_at - source_ts
  8. no WS gaps / errors during the run
Places NO orders.
"""

from __future__ import annotations

import argparse
import asyncio
import itertools
import statistics
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Any

import orjson

from dh.core.book import KalshiBook
from dh.core.events import (
    FeedStatus,
    IndexTick,
    KalshiBookDelta,
    KalshiBookSnapshot,
    KalshiTrade,
)
from dh.core.units import NS_PER_MS
from dh.kalshi.config import load_config
from dh.kalshi.metadata import MarketRegistry, fetch_series_bundle
from dh.kalshi.normalize import rest_orderbook_to_snapshot
from dh.kalshi.rest import KalshiRest
from dh.kalshi.wire import opt_iso_to_ns
from dh.kalshi.ws import KalshiWS, Subscription, websockets_connect_factory


class JsonlRecorder:
    """Raw records as JSON lines {stream, recv_ns, raw} (raw decoded as UTF-8) for replay."""

    def __init__(self, path: Path) -> None:
        self.f = path.open("ab")

    def write(self, stream: str, recv_ns: int, raw: bytes) -> None:
        self.f.write(orjson.dumps({"stream": stream, "recv_ns": recv_ns, "raw": raw.decode("utf-8", "replace")}) + b"\n")

    def close(self) -> None:
        self.f.close()


def _pct(xs: list[float], q: float) -> float:
    if not xs:
        return float("nan")
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(q * (len(xs) - 1) + 0.5))]


def _mid_distance(m: dict[str, Any]) -> float:
    try:
        return abs((float(m.get("yes_bid_dollars") or 0) + float(m.get("yes_ask_dollars") or 1)) / 2 - 0.5)
    except (TypeError, ValueError):
        return 1.0


def _book_levels(b: KalshiBook) -> tuple[tuple, tuple]:
    return tuple(b.yes_bids.items()), tuple(b.no_bids.items())


async def compare_books(rest: KalshiRest, books: dict[str, KalshiBook], attempts: int = 5) -> dict[str, str]:
    """ticker -> '' if WS book == REST book on some attempt, else a short diff description."""
    pending = dict.fromkeys(books, "not compared")
    for _ in range(attempts):
        body = await rest.get_orderbooks(sorted(pending))
        for ob in body.get("orderbooks") or []:
            t = ob["ticker"]
            if t not in pending:
                continue
            snap = rest_orderbook_to_snapshot(t, ob, 0)
            ws_b = books[t]
            if not ws_b.valid:
                pending[t] = f"ws book invalid: {ws_b.invalid_reason}"
                continue
            rest_levels = (snap.yes_bids, snap.no_bids)
            if _book_levels(ws_b) == rest_levels:
                pending.pop(t)
            else:
                yb, nb = _book_levels(ws_b)
                pending[t] = (f"yes ws={len(yb)} rest={len(rest_levels[0])} lvls, no ws={len(nb)} rest={len(rest_levels[1])} lvls; "
                              f"best ws={ws_b.best_bid()}/{ws_b.best_ask()}")
        if not pending:
            break
        await asyncio.sleep(1.0)
    return pending


async def amain(args: argparse.Namespace) -> int:
    cfg = load_config(args.config, env="demo" if args.demo else None)
    signer = cfg.signer()
    if signer is None:
        print("credentials required: set KALSHI_KEY_ID and KALSHI_PRIVATE_KEY_PATH", file=sys.stderr)
        return 2
    results: list[tuple[str, bool]] = []

    def check(name: str, ok: bool, detail: str = "") -> None:
        results.append((name, ok))
        print(f"[{'PASS' if ok else 'FAIL'}] {name}: {detail}")

    rec = JsonlRecorder(args.record) if args.record else None
    on_raw = rec.write if rec else None
    try:
        async with KalshiRest(cfg.rest_url, signer, cfg.limiter(), on_raw=on_raw, **cfg.rest_kwargs()) as rest:
            st = await rest.get_exchange_status()
            check("exchange status", bool(st.get("exchange_active")), orjson.dumps(st).decode()[:200])
            lim = await rest.configure_rate_limits()
            lm = lim["limits"]
            check("account limits", "read" in lm and "write" in lm,
                  f"tier={lm.get('usage_tier')} read={lm.get('read')} write={lm.get('write')} "
                  f"default_cost={lim['endpoint_costs'].get('default_cost')}")

            bundle = await fetch_series_bundle(rest, args.series, status="open")
            reg = MarketRegistry.from_bundles([bundle], cfg.fee_engine())
            now = time.time_ns()
            live = [m for m in bundle.markets if opt_iso_to_ns(m.get("close_time")) > now]
            if not live:
                check("market discovery", False, f"no open {args.series} markets")
                return 1
            nearest_close = min(opt_iso_to_ns(m["close_time"]) for m in live)
            event_markets = [m for m in live if opt_iso_to_ns(m["close_time"]) == nearest_close]
            event_markets.sort(key=_mid_distance)
            tickers = [m["ticker"] for m in event_markets[: args.markets]]
            n_specs = sum(1 for m in event_markets if m["ticker"] in reg.specs)
            flags = Counter(f for m in event_markets for f in reg.flags.get(m["ticker"], []))
            check("market discovery", bool(tickers) and n_specs == len(event_markets),
                  f"event {event_markets[0]['event_ticker']}: {len(event_markets)} markets, {n_specs} specs, "
                  f"rejected={ {t: r for t, r in reg.rejected.items() if t in {m['ticker'] for m in event_markets}} }, flags={dict(flags)}")
            check("rules sanity", not any(reg.blocking_flags(t) for t in tickers), f"{dict(flags)}")
            sched = reg.fee_schedule(tickers[0])
            check("fee schedule", sched.supported,
                  f"{sched.fee_type} x{sched.multiplier} ({sched.source}); maker@50c={sched.expected_fee_per_contract(5000, False):.6f}$ "
                  f"taker@50c={sched.expected_fee_per_contract(5000, True):.6f}$")
            obs = await rest.get_orderbooks(tickers)
            check("REST orderbooks", len(obs.get("orderbooks") or []) == len(tickers), f"{len(tickers)} books")

            books = {t: KalshiBook(t) for t in tickers}
            ticks: dict[str, list[IndexTick]] = {"1hz": [], "5hz": []}
            statuses: Counter[str] = Counter()
            trades = [0]

            def on_event(ev: Any) -> None:
                if isinstance(ev, KalshiBookSnapshot) and ev.ticker in books:
                    books[ev.ticker].apply_snapshot(ev)
                elif isinstance(ev, KalshiBookDelta) and ev.ticker in books:
                    books[ev.ticker].apply_delta(ev)
                elif isinstance(ev, IndexTick) and ev.feed in ticks:
                    ticks[ev.feed].append(ev)
                elif isinstance(ev, KalshiTrade):
                    trades[0] += 1
                elif isinstance(ev, FeedStatus):
                    statuses[ev.status] += 1
                    if ev.stream.startswith("kalshi.book:") and ev.status in ("gap", "disconnected"):
                        t = ev.stream.split(":", 1)[1]
                        if t in books:
                            books[t].invalidate(ev.status)
                    if ev.status in ("gap", "error", "stale", "disconnected"):
                        print(f"  feed status: {ev.stream} {ev.status} {ev.detail}")

            wscfg = cfg.ws or {}
            ws = KalshiWS(
                cfg.ws_url,
                signer,
                [Subscription(["orderbook_delta", "trade"], market_tickers=list(tickers)),
                 Subscription(["cfbenchmarks_value"], index_ids=list(cfg.index_ids)),
                 Subscription(["cfbenchmarks_value_5hz"], index_ids=list(cfg.index_ids))],
                on_raw=on_raw,
                on_event=on_event,
                connect=websockets_connect_factory(
                    ping_interval=float(wscfg.get("ping_interval_s", 10)), ping_timeout=float(wscfg.get("ping_timeout_s", 10)),
                    proxy=True if bool((cfg.rest or {}).get("use_env_proxy", True)) else None,
                ),
                stale_after_s=float(wscfg.get("stale_after_s", 15)),
                use_yes_price=bool(wscfg.get("use_yes_price", False)),
            )
            t0 = time.monotonic()
            task = asyncio.create_task(ws.run())
            await asyncio.sleep(args.seconds)
            diffs = await compare_books(rest, books)
            elapsed = time.monotonic() - t0
            await ws.stop()
            await asyncio.wait_for(task, 10)

            check("WS connected + subscribed", ws.connects >= 1 and statuses["connected"] >= 1,
                  f"connects={ws.connects} frames={ws.stats['frames']} events={ws.stats['events']} trades={trades[0]}")
            check("WS book == REST snapshot", not diffs, "all books match" if not diffs else orjson.dumps(diffs).decode()[:500])
            for feed, min_rate in (("1hz", 0.5), ("5hz", 1.0)):
                xs = ticks[feed]
                rate = len(xs) / elapsed if elapsed else 0.0
                lat = [(t.ts - t.ts_exch) / NS_PER_MS for t in xs if t.ts_exch]
                hop = [(t.kalshi_recv_ns - t.ts_exch) / NS_PER_MS for t in xs if t.ts_exch and t.kalshi_recv_ns]
                check(f"BRTI {feed} ticks", rate >= min_rate,
                      f"{len(xs)} ticks, {rate:.2f}/s; recv-source ms p50={_pct(lat, .5):.1f} p90={_pct(lat, .9):.1f} "
                      f"p99={_pct(lat, .99):.1f}; kalshi hop ms p50={_pct(hop, .5):.1f}"
                      + (f"; last={xs[-1].value} avg60={xs[-1].avg60}" if xs else ""))
            if ticks["1hz"]:
                gaps = [(b.ts_exch - a.ts_exch) / 1e9 for a, b in itertools.pairwise(ticks["1hz"])]
                print(f"  1hz source spacing s: median={statistics.median(gaps) if gaps else float('nan'):.3f} max={max(gaps, default=0):.3f}")
            check("no WS gaps/errors", statuses["gap"] == 0 and statuses["error"] == 0,
                  f"statuses={dict(statuses)} counters={ws.state.counters}")
    finally:
        if rec:
            rec.close()
    failed = [n for n, ok in results if not ok]
    print(f"\n{len(results) - len(failed)}/{len(results)} checks passed" + (f"; FAILED: {failed}" if failed else ""))
    return 1 if failed else 0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0], formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--demo", action="store_true", help="use the demo environment")
    ap.add_argument("--config", default=None)
    ap.add_argument("--series", default="KXBTCD")
    ap.add_argument("--markets", type=int, default=6, help="markets nearest 50c to stream")
    ap.add_argument("--seconds", type=float, default=30.0)
    ap.add_argument("--record", type=Path, default=None, help="append raw REST/WS records to this JSONL file")
    return ap.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    return asyncio.run(amain(parse_args(argv)))


if __name__ == "__main__":
    sys.exit(main())
