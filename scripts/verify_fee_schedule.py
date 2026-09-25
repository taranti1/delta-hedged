#!/usr/bin/env python
"""Compare our fee model with the account's real fills and with the series fee settings.

    export KALSHI_KEY_ID=...  KALSHI_PRIVATE_KEY_PATH=...
    python scripts/verify_fee_schedule.py --days 14
    python scripts/verify_fee_schedule.py --series KXBTCD KXBTC --ticker-prefix KXBTC --show 50

1. For each series: fee_type / fee_multiplier (GET /series/{s}) and scheduled changes
   (GET /series/fee_changes); FAIL if our config/fees.yaml cannot price the type.
2. For the account's fills in the window (GET /portfolio/fills, then /historical/fills):
   resolve each market's schedule (event override > series), replay each ORDER's fills in
   time order through OrderFeeAccumulator at BOTH balance precisions ($0.01 and $0.0001) and
   compare every fill's reported fee_cost: exact net / exact trade-only / within rounding /
   mismatch. The summary tells which rounding convention and precision the account follows
   (set fees.balance_precision_dollars accordingly) and lists every mismatch.
Exit 1 if a series is unsupported or fills mismatch beyond tolerance at the best precision.
Read-only: places no orders.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
import time
from collections import Counter, defaultdict
from typing import Any

from dh.core.units import NS_PER_S, px_from_dollars, qty_from_fp
from dh.kalshi.config import load_config
from dh.kalshi.fees import FeeEngine, reconcile_fill_fee
from dh.kalshi.normalize import book_side_of
from dh.kalshi.rest import KalshiHTTPError, KalshiRest
from dh.kalshi.wire import opt_iso_to_ns

PRECISIONS = {"0.01": 10_000, "0.0001": 100}


class MetaCache:
    """market / event / series lookups with archive fallbacks."""

    def __init__(self, rest: KalshiRest) -> None:
        self.rest = rest
        self.markets: dict[str, dict[str, Any]] = {}
        self.events: dict[str, dict[str, Any]] = {}
        self.series: dict[str, dict[str, Any]] = {}

    async def market(self, ticker: str) -> dict[str, Any]:
        if ticker not in self.markets:
            try:
                self.markets[ticker] = (await self.rest.get_market(ticker))["market"]
            except KalshiHTTPError:
                self.markets[ticker] = (await self.rest.get_historical_market(ticker))["market"]
        return self.markets[ticker]

    async def event(self, event_ticker: str) -> dict[str, Any]:
        if event_ticker not in self.events:
            self.events[event_ticker] = (await self.rest.get_event(event_ticker))["event"]
        return self.events[event_ticker]

    async def series_of(self, series_ticker: str) -> dict[str, Any]:
        if series_ticker not in self.series:
            self.series[series_ticker] = (await self.rest.get_series(series_ticker))["series"]
        return self.series[series_ticker]


async def check_series(rest: KalshiRest, engine: FeeEngine, series: list[str]) -> int:
    problems = 0
    print("== series fee settings ==")
    for s in series:
        body = (await rest.get_series(s))["series"]
        changes = (await rest.get_series_fee_changes(s, show_historical=True)).get("series_fee_change_arr") or []
        sched = engine.schedule_for(body)
        ok = sched.supported
        problems += not ok
        print(f"  {s:10s} fee_type={body.get('fee_type')!s:34s} multiplier={body.get('fee_multiplier')!s:6s} "
              f"{'OK' if ok else 'UNSUPPORTED'}; scheduled changes: {len(changes)}")
        for ch in changes[-5:]:
            print(f"      {ch.get('scheduled_ts')} -> {ch.get('fee_type')} x{ch.get('fee_multiplier')}")
    return problems


async def check_fills(rest: KalshiRest, engine: FeeEngine, days: float, prefix: str, show: int) -> int:
    min_ts = int(time.time() - days * 86400)
    fills = [f async for f in rest.iter_fills(min_ts=min_ts)]
    try:
        fills += [f async for f in rest.iter_historical_fills(min_ts=min_ts)]
    except KalshiHTTPError:
        pass
    seen: set[str] = set()
    uniq = []
    for f in fills:
        fid = str(f.get("fill_id") or f.get("trade_id"))
        if fid not in seen and str(f.get("ticker") or f.get("market_ticker", "")).startswith(prefix):
            seen.add(fid)
            uniq.append(f)
    print(f"\n== fills: {len(uniq)} in the last {days:g} days (prefix {prefix!r}) ==")
    if not uniq:
        return 0
    by_order: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for f in uniq:
        by_order[str(f["order_id"])].append(f)
    cache = MetaCache(rest)
    tallies = {bp: Counter() for bp in PRECISIONS}
    mism: dict[str, list[str]] = {bp: [] for bp in PRECISIONS}
    unsupported = 0
    for order_id, fs in by_order.items():
        fs.sort(key=lambda f: (opt_iso_to_ns(f.get("created_time")) or int(f.get("ts") or 0) * NS_PER_S, str(f.get("fill_id") or f.get("trade_id"))))
        ticker = str(fs[0].get("ticker") or fs[0].get("market_ticker"))
        market = await cache.market(ticker)
        event = await cache.event(str(market["event_ticker"]))
        series = await cache.series_of(str(event["series_ticker"]))
        sched = engine.schedule_for(series, event, market)
        if not sched.supported:
            unsupported += len(fs)
            print(f"  {ticker}: unsupported fee schedule {sched.fee_type!r} ({sched.source})")
            continue
        for label, bp in PRECISIONS.items():
            acc = sched.order_accumulator(book_side_of(fs[0]), balance_precision_micros=bp)
            for f in fs:
                px = px_from_dollars(str(f["yes_price_dollars"]))
                qty = qty_from_fp(str(f["count_fp"]))
                b = acc.apply_fill(px, qty, bool(f["is_taker"]), now_ns=opt_iso_to_ns(f.get("created_time")) or None)
                chk = reconcile_fill_fee(b.net_micros, str(f.get("fee_cost")), breakdown=b)
                t = tallies[label]
                t["fills"] += 1
                if chk.matched == "net":
                    t["exact_net"] += 1
                elif chk.matched == "trade":
                    t["exact_trade_only"] += 1
                elif chk.ok:
                    t["within_rounding"] += 1
                else:
                    t["mismatch"] += 1
                    mism[label].append(
                        f"{ticker} order={order_id} fill={f.get('fill_id') or f.get('trade_id')} px={px} qty={qty} "
                        f"taker={f['is_taker']} {sched.fee_type}x{sched.multiplier}: model trade={b.trade_micros} "
                        f"round={b.rounding_micros} rebate={b.rebate_micros} net={b.net_micros} reported={chk.reported_micros}"
                    )
    for label, t in tallies.items():
        print(f"  precision ${label}: {dict(t)}")
    best = min(PRECISIONS, key=lambda k: (tallies[k]["mismatch"], -tallies[k]["exact_net"]))
    print(f"  best-matching balance precision: ${best}")
    for line in mism[best][:show]:
        print("   MISMATCH", line)
    return tallies[best]["mismatch"] + unsupported


async def amain(args: argparse.Namespace) -> int:
    cfg = load_config(args.config, env="demo" if args.demo else None)
    signer = cfg.signer()
    if signer is None:
        print("credentials required: set KALSHI_KEY_ID and KALSHI_PRIVATE_KEY_PATH", file=sys.stderr)
        return 2
    engine = cfg.fee_engine()
    print(f"fee config: {engine.rates.source} (effective_from={engine.rates.effective_from}, "
          f"verified_against_live_fills={engine.rates.verified_against_live_fills})")
    async with KalshiRest(cfg.rest_url, signer, cfg.limiter(), **cfg.rest_kwargs()) as rest:
        await rest.configure_rate_limits()
        bad = await check_series(rest, engine, list(args.series))
        bad += await check_fills(rest, engine, args.days, args.ticker_prefix, args.show)
    print("\nRESULT:", "OK" if not bad else f"{bad} problem(s)")
    return 1 if bad else 0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0], formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--series", nargs="+", default=["KXBTCD", "KXBTC", "KXBTC15M"])
    ap.add_argument("--days", type=float, default=14.0)
    ap.add_argument("--ticker-prefix", default="", help="only fills whose ticker starts with this")
    ap.add_argument("--show", type=int, default=25, help="max mismatches to print")
    ap.add_argument("--demo", action="store_true")
    ap.add_argument("--config", default=None)
    return ap.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    return asyncio.run(amain(parse_args(argv)))


if __name__ == "__main__":
    sys.exit(main())
