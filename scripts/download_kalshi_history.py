#!/usr/bin/env python
"""Download settled Kalshi BTC markets + public trades (+ candles, fees, incentives, BRTI).

Powers Experiment 0 (maker P&L to settlement from public trades). Resumable: every event is
checkpointed per dataset; re-running skips finished work.

    python scripts/download_kalshi_history.py --start 2025-06-01 --end 2025-07-01
    python scripts/download_kalshi_history.py --series KXBTCD --start 2025-06-01 --end 2025-06-02 \
        --datasets markets,trades,candles,fees,incentives,brti

Sources (openapi 3.30.0, see GET /historical/cutoff):
  events       GET /events?series_ticker=S&status=settled&min_close_ts=start   (all events)
  markets      GET /historical/markets?event_ticker=E (settled before market_settled_ts)
               else GET /markets?event_ticker=E                 (tries the other if empty)
  trades       GET /historical/trades?ticker=T (trades before trades_created_ts) and/or
               GET /markets/trades?ticker=T; de-duplicated by trade_id
  candles      GET /historical/markets/{T}/candlesticks or /series/{S}/markets/{T}/candlesticks
  fees         GET /series/{S}, /series/fee_changes?series_ticker=S&show_historical=true,
               GET /events/fee_changes (all pages, filtered to the series)
  incentives   GET /incentive_programs?status=all&type=all (filtered to the series)
  brti         CF Benchmarks passthrough around each expiration (+/- window), needs auth

Output (Parquet, explicit schemas) under --out (default data/external/kalshi):
  meta/historical_cutoff.json            series/<S>.json      fees/series_fee_changes/<S>.json
  events/series=<S>/events.parquet       markets/series=<S>/<EVENT>.parquet
  trades/series=<S>/<EVENT>.parquet      candles/series=<S>/<EVENT>.parquet
  brti/series=<S>/<EVENT>.parquet (+ <EVENT>.raw.json with the request and raw body)
  fees/event_fee_changes.parquet         incentives/incentive_programs.parquet
  _checkpoints/<dataset>-<S>.json

Trade schema (exact integer units, dh.core.units):
  trade_id str | ticker str | yes_px int64 (1e-4 $) | qty int64 (0.01 contract) |
  taker_outcome_side str ('yes' = taker bought YES) | taker_book_side str ('bid' == 'yes') |
  created_time str (as sent) | ts_ms int64 | is_block_trade bool | event_ticker | series_ticker |
  source ('historical' | 'live')
Maker gross P&L to settlement per trade = qty/100 * ((settle - yes_px) if taker sold YES
else (yes_px - settle)) / 1e4 dollars, with settle = settlement_px from the markets file.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
import time
from collections.abc import Iterable
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import orjson
import pyarrow as pa
import pyarrow.parquet as pq

from dh.core.units import NS_PER_MS, NS_PER_S, PX_SCALE, px_from_dollars, qty_from_fp
from dh.kalshi.normalize import cf_history_to_ticks, taker_outcome_side
from dh.kalshi.rest import KalshiHTTPError
from dh.kalshi.wire import opt_iso_to_ns, to_float

DATASETS = ("markets", "trades", "candles", "fees", "incentives", "brti")
DEFAULT_DATASETS = ("markets", "trades", "fees", "incentives")

TRADE_SCHEMA = pa.schema(
    [
        ("trade_id", pa.string()),
        ("ticker", pa.string()),
        ("yes_px", pa.int64()),
        ("qty", pa.int64()),
        ("taker_outcome_side", pa.string()),
        ("taker_book_side", pa.string()),
        ("created_time", pa.string()),
        ("ts_ms", pa.int64()),
        ("is_block_trade", pa.bool_()),
        ("event_ticker", pa.string()),
        ("series_ticker", pa.string()),
        ("source", pa.string()),
    ]
)
MARKET_SCHEMA = pa.schema(
    [
        ("ticker", pa.string()),
        ("event_ticker", pa.string()),
        ("series_ticker", pa.string()),
        ("market_type", pa.string()),
        ("status", pa.string()),
        ("strike_type", pa.string()),
        ("floor_strike", pa.float64()),
        ("cap_strike", pa.float64()),
        ("open_time", pa.string()),
        ("close_time", pa.string()),
        ("expected_expiration_time", pa.string()),
        ("settlement_ts", pa.string()),
        ("open_ts_ms", pa.int64()),
        ("close_ts_ms", pa.int64()),
        ("expected_expiration_ts_ms", pa.int64()),
        ("settlement_ts_ms", pa.int64()),
        ("result", pa.string()),
        ("expiration_value", pa.string()),
        ("expiration_value_f", pa.float64()),
        ("settlement_value_dollars", pa.string()),
        ("settlement_px", pa.int64()),
        ("volume", pa.int64()),
        ("open_interest", pa.int64()),
        ("price_level_structure", pa.string()),
        ("price_ranges_json", pa.string()),
        ("rules_primary", pa.string()),
        ("fee_waiver_expiration_time", pa.string()),
        ("source", pa.string()),
        ("raw_json", pa.string()),
    ]
)
EVENT_SCHEMA = pa.schema(
    [
        ("event_ticker", pa.string()),
        ("series_ticker", pa.string()),
        ("title", pa.string()),
        ("sub_title", pa.string()),
        ("strike_date", pa.string()),
        ("strike_ts_ms", pa.int64()),
        ("mutually_exclusive", pa.bool_()),
        ("fee_type_override", pa.string()),
        ("fee_multiplier_override", pa.string()),
        ("raw_json", pa.string()),
    ]
)
CANDLE_SCHEMA = pa.schema(
    [
        ("ticker", pa.string()),
        ("end_period_ts", pa.int64()),
        ("yes_bid_open", pa.string()),
        ("yes_bid_low", pa.string()),
        ("yes_bid_high", pa.string()),
        ("yes_bid_close", pa.string()),
        ("yes_ask_open", pa.string()),
        ("yes_ask_low", pa.string()),
        ("yes_ask_high", pa.string()),
        ("yes_ask_close", pa.string()),
        ("price_open", pa.string()),
        ("price_low", pa.string()),
        ("price_high", pa.string()),
        ("price_close", pa.string()),
        ("price_mean", pa.string()),
        ("price_previous", pa.string()),
        ("volume", pa.int64()),
        ("open_interest", pa.int64()),
        ("source", pa.string()),
    ]
)
BRTI_SCHEMA = pa.schema(
    [
        ("event_ticker", pa.string()),
        ("index_id", pa.string()),
        ("expiration_ts_ms", pa.int64()),
        ("ts_ns", pa.int64()),
        ("value", pa.float64()),
    ]
)
EVENT_FEE_SCHEMA = pa.schema(
    [
        ("id", pa.string()),
        ("event_ticker", pa.string()),
        ("series_ticker", pa.string()),
        ("fee_type_override", pa.string()),
        ("fee_multiplier_override", pa.string()),
        ("scheduled_ts", pa.string()),
    ]
)
INCENTIVE_SCHEMA = pa.schema(
    [
        ("id", pa.string()),
        ("market_ticker", pa.string()),
        ("incentive_type", pa.string()),
        ("incentive_description", pa.string()),
        ("start_date", pa.string()),
        ("end_date", pa.string()),
        ("period_reward_centicents", pa.int64()),
        ("paid_out", pa.bool_()),
        ("raw_json", pa.string()),
    ]
)


# ============================================================================ io helpers
def write_parquet(path: Path, rows: list[dict[str, Any]], schema: pa.Schema) -> None:
    """Atomic write (tmp + rename) with an explicit schema (empty tables keep their types)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(rows, schema=schema)
    tmp = path.with_suffix(path.suffix + ".tmp")
    pq.write_table(table, tmp)
    os.replace(tmp, path)


def write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_bytes(orjson.dumps(obj, option=orjson.OPT_INDENT_2 | orjson.OPT_SORT_KEYS))
    os.replace(tmp, path)


class Checkpoint:
    """Set of finished keys persisted as JSON (atomic rewrite after every mark)."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.done: set[str] = set()
        if path.is_file():
            self.done = set(orjson.loads(path.read_bytes()).get("done", []))

    def __contains__(self, key: str) -> bool:
        return key in self.done

    def mark(self, key: str) -> None:
        self.done.add(key)
        write_json(self.path, {"done": sorted(self.done), "updated_ns": time.time_ns()})


def _dumps(obj: Any) -> str:
    return orjson.dumps(obj).decode()


def _num_str(v: Any) -> str | None:
    if v is None:
        return None
    return str(v)


def _ms(iso: Any) -> int | None:
    ns = opt_iso_to_ns(iso)
    return ns // NS_PER_MS if ns else None


# ============================================================================ row builders
def trade_row(t: dict[str, Any], event_ticker: str, series: str, source: str) -> dict[str, Any]:
    """openapi Trade -> TRADE_SCHEMA row (exact ints)."""
    outcome = taker_outcome_side(t)
    if t.get("yes_price_dollars") not in (None, ""):
        yes_px = px_from_dollars(str(t["yes_price_dollars"]))
    else:
        yes_px = PX_SCALE - px_from_dollars(str(t["no_price_dollars"]))
    created = str(t.get("created_time") or "")
    return {
        "trade_id": str(t["trade_id"]),
        "ticker": str(t.get("ticker") or t.get("market_ticker")),
        "yes_px": yes_px,
        "qty": qty_from_fp(str(t["count_fp"])),
        "taker_outcome_side": outcome,
        "taker_book_side": str(t.get("taker_book_side") or ("bid" if outcome == "yes" else "ask")),
        "created_time": created,
        "ts_ms": opt_iso_to_ns(created) // NS_PER_MS,
        "is_block_trade": bool(t.get("is_block_trade", False)),
        "event_ticker": event_ticker,
        "series_ticker": series,
        "source": source,
    }


def market_row(m: dict[str, Any], series: str, source: str) -> dict[str, Any]:
    """openapi Market -> MARKET_SCHEMA row. settlement_px from settlement_value_dollars
    (else 10_000 for result 'yes', 0 for 'no'); expiration_value kept verbatim."""
    result = str(m.get("result") or "")
    sval = m.get("settlement_value_dollars")
    if sval not in (None, ""):
        spx: int | None = px_from_dollars(str(sval))
    else:
        spx = PX_SCALE if result == "yes" else 0 if result == "no" else None
    return {
        "ticker": str(m["ticker"]),
        "event_ticker": str(m.get("event_ticker") or ""),
        "series_ticker": series,
        "market_type": m.get("market_type"),
        "status": m.get("status"),
        "strike_type": m.get("strike_type"),
        "floor_strike": to_float(m.get("floor_strike")),
        "cap_strike": to_float(m.get("cap_strike")),
        "open_time": m.get("open_time"),
        "close_time": m.get("close_time"),
        "expected_expiration_time": m.get("expected_expiration_time"),
        "settlement_ts": m.get("settlement_ts"),
        "open_ts_ms": _ms(m.get("open_time")),
        "close_ts_ms": _ms(m.get("close_time")),
        "expected_expiration_ts_ms": _ms(m.get("expected_expiration_time")),
        "settlement_ts_ms": _ms(m.get("settlement_ts")),
        "result": result,
        "expiration_value": _num_str(m.get("expiration_value")),
        "expiration_value_f": to_float(m.get("expiration_value")),
        "settlement_value_dollars": _num_str(sval),
        "settlement_px": spx,
        "volume": qty_from_fp(str(m["volume_fp"])) if m.get("volume_fp") not in (None, "") else None,
        "open_interest": qty_from_fp(str(m["open_interest_fp"])) if m.get("open_interest_fp") not in (None, "") else None,
        "price_level_structure": m.get("price_level_structure"),
        "price_ranges_json": _dumps(m.get("price_ranges") or []),
        "rules_primary": m.get("rules_primary"),
        "fee_waiver_expiration_time": m.get("fee_waiver_expiration_time"),
        "source": source,
        "raw_json": _dumps(m),
    }


def event_row(e: dict[str, Any]) -> dict[str, Any]:
    return {
        "event_ticker": str(e["event_ticker"]),
        "series_ticker": e.get("series_ticker"),
        "title": e.get("title"),
        "sub_title": e.get("sub_title"),
        "strike_date": e.get("strike_date"),
        "strike_ts_ms": _ms(e.get("strike_date")),
        "mutually_exclusive": e.get("mutually_exclusive"),
        "fee_type_override": e.get("fee_type_override"),
        "fee_multiplier_override": _num_str(e.get("fee_multiplier_override")),
        "raw_json": _dumps({k: v for k, v in e.items() if k != "markets"}),
    }


def _ohlc(d: Any, keys: Iterable[str], suffix: str) -> list[str | None]:
    d = d if isinstance(d, dict) else {}
    return [_num_str(d.get(k + suffix)) for k in keys]


def candle_rows(ticker: str, body: dict[str, Any], source: str) -> list[dict[str, Any]]:
    """Live MarketCandlestick (*_dollars, *_fp) or MarketCandlestickHistorical rows; prices
    are kept as exact dollar strings (means need not lie on the tick grid)."""
    out = []
    sfx = "_dollars" if source == "live" else ""
    for c in body.get("candlesticks") or []:
        yb = _ohlc(c.get("yes_bid"), ("open", "low", "high", "close"), sfx)
        ya = _ohlc(c.get("yes_ask"), ("open", "low", "high", "close"), sfx)
        pr = _ohlc(c.get("price"), ("open", "low", "high", "close", "mean", "previous"), sfx)
        vol = c.get("volume_fp", c.get("volume"))
        oi = c.get("open_interest_fp", c.get("open_interest"))
        out.append(
            {
                "ticker": ticker,
                "end_period_ts": int(c["end_period_ts"]),
                "yes_bid_open": yb[0], "yes_bid_low": yb[1], "yes_bid_high": yb[2], "yes_bid_close": yb[3],
                "yes_ask_open": ya[0], "yes_ask_low": ya[1], "yes_ask_high": ya[2], "yes_ask_close": ya[3],
                "price_open": pr[0], "price_low": pr[1], "price_high": pr[2], "price_close": pr[3],
                "price_mean": pr[4], "price_previous": pr[5],
                "volume": qty_from_fp(str(vol)) if vol not in (None, "") else None,
                "open_interest": qty_from_fp(str(oi)) if oi not in (None, "") else None,
                "source": source,
            }
        )
    return out


# ============================================================================ fetchers
async def list_events(
    rest: Any, series: str, start_ns: int, end_ns: int, *, use_close_filter: bool = True
) -> list[dict[str, Any]]:
    """Settled events of a series whose strike_date lies in [start_ns, end_ns) (events without
    strike_date are kept and filtered later by their markets' close times). use_close_filter
    passes min_close_ts=start to the server (unverified for archived events: disable with
    --no-event-close-filter if old ranges come back empty)."""
    out = []
    kw: dict[str, Any] = {"series_ticker": series, "status": "settled"}
    if use_close_filter:
        kw["min_close_ts"] = start_ns // NS_PER_S
    async for ev in rest.iter_events(**kw):
        ts = opt_iso_to_ns(ev.get("strike_date"))
        if ts and not start_ns <= ts < end_ns:
            continue
        out.append({k: v for k, v in ev.items() if k != "markets"})
    out.sort(key=lambda e: (opt_iso_to_ns(e.get("strike_date")), e["event_ticker"]))
    return out


async def fetch_event_markets(rest: Any, event: dict[str, Any], market_cut_ns: int) -> tuple[list[dict[str, Any]], str]:
    """Markets of one event from the historical or live endpoint (falls back to the other)."""
    et = event["event_ticker"]
    ts = opt_iso_to_ns(event.get("strike_date"))
    order = ["historical", "live"] if (ts and ts < market_cut_ns) else ["live", "historical"]
    for src in order:
        it = rest.iter_historical_markets(event_ticker=et) if src == "historical" else rest.iter_markets(event_ticker=et)
        markets = [m async for m in it]
        if markets:
            return markets, src
    return [], order[0]


async def fetch_market_trades(rest: Any, m: dict[str, Any], trades_cut_ns: int) -> list[tuple[dict[str, Any], str]]:
    """All public trades of one market, from /historical/trades and/or /markets/trades."""
    open_ns = opt_iso_to_ns(m.get("open_time"))
    close_ns = opt_iso_to_ns(m.get("close_time"))
    sources = []
    if not open_ns or open_ns < trades_cut_ns:  # some trades may predate the cutoff
        sources.append("historical")
    if not close_ns or close_ns >= trades_cut_ns:  # some trades may be after it
        sources.append("live")
    seen: set[str] = set()
    out: list[tuple[dict[str, Any], str]] = []
    for src in sources:
        async for t in rest.iter_trades(ticker=m["ticker"], historical=(src == "historical")):
            tid = str(t["trade_id"])
            if tid not in seen:
                seen.add(tid)
                out.append((t, src))
    return out


async def fetch_candles(rest: Any, series: str, m: dict[str, Any], market_source: str, period: int) -> list[dict[str, Any]]:
    """1/60/1440-minute candles over [open, close] from the endpoint family the market was
    found in (historical markets -> /historical/markets/{t}/candlesticks)."""
    start_s = opt_iso_to_ns(m.get("open_time")) // NS_PER_S
    end_s = opt_iso_to_ns(m.get("close_time")) // NS_PER_S
    if market_source == "historical":
        body = await rest.get_historical_candlesticks(m["ticker"], start_s, end_s, period)
        return candle_rows(m["ticker"], body, "historical")
    body = await rest.get_market_candlesticks(series, m["ticker"], start_s, end_s, period)
    return candle_rows(m["ticker"], body, "live")


def _fill(template: str, **kw: Any) -> str:
    return template.format(**kw)


async def fetch_brti(
    rest: Any, event_ticker: str, expiration_ns: int, window_s: int, timespan_tpl: str, timestamp_tpl: str
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """BRTI via the CF passthrough for [T - window - 60 s, T + window]. The passthrough's
    parameter formats are undocumented: they are templates (see --brti-timespan)."""
    start_ns = expiration_ns - (window_s + 60) * NS_PER_S
    end_ns = expiration_ns + window_s * NS_PER_S
    kw = dict(
        start_ms=start_ns // NS_PER_MS,
        end_ms=end_ns // NS_PER_MS,
        start_s=start_ns // NS_PER_S,
        end_s=end_ns // NS_PER_S,
        span_s=(end_ns - start_ns) // NS_PER_S,
        span_ms=(end_ns - start_ns) // NS_PER_MS,
    )
    params = {"timespan": _fill(timespan_tpl, **kw), "timestamp": _fill(timestamp_tpl, **kw)}
    body = await rest.get_cfbenchmarks_history("BRTI", timespan=params["timespan"], timestamp=params["timestamp"])
    ticks = cf_history_to_ticks(body, 0, "BRTI")
    rows = [
        {"event_ticker": event_ticker, "index_id": t.index_id, "expiration_ts_ms": expiration_ns // NS_PER_MS,
         "ts_ns": t.ts_exch, "value": t.value}
        for t in ticks
    ]
    return rows, {"event_ticker": event_ticker, "params": params, "body": body}


# ============================================================================ driver
async def download(
    rest: Any,
    series_list: list[str],
    start_ns: int,
    end_ns: int,
    out: Path,
    *,
    datasets: Iterable[str] = DEFAULT_DATASETS,
    skip_zero_volume: bool = True,
    candle_period: int = 1,
    brti_window_s: int = 300,
    brti_timespan: str = "{span_s}s",
    brti_timestamp: str = "{end_ms}",
    max_events: int | None = None,
    concurrency: int = 4,
    have_auth: bool = False,
    event_close_filter: bool = True,
    log: Any = print,
) -> dict[str, int]:
    """Download everything requested; returns counters. Safe to re-run (checkpoints)."""
    ds = set(datasets)
    unknown = ds - set(DATASETS)
    if unknown:
        raise ValueError(f"unknown datasets {sorted(unknown)}")
    stats = {"events": 0, "events_skipped": 0, "markets": 0, "trades": 0, "candles": 0, "brti_ticks": 0, "errors": 0}
    cutoff = await rest.get_historical_cutoff()
    write_json(out / "meta" / "historical_cutoff.json", {"fetched_ns": time.time_ns(), **cutoff})
    market_cut = opt_iso_to_ns(cutoff.get("market_settled_ts"))
    trades_cut = opt_iso_to_ns(cutoff.get("trades_created_ts"))
    sem = asyncio.Semaphore(max(1, concurrency))

    if "fees" in ds:
        fee_rows = []
        async for ch in rest.iter_event_fee_changes():
            if ch.get("series_ticker") in series_list or str(ch.get("event_ticker", "")).split("-", 1)[0] in series_list:
                fee_rows.append({
                    "id": str(ch.get("id")), "event_ticker": ch.get("event_ticker"), "series_ticker": ch.get("series_ticker"),
                    "fee_type_override": ch.get("fee_type_override"),
                    "fee_multiplier_override": _num_str(ch.get("fee_multiplier_override")),
                    "scheduled_ts": ch.get("scheduled_ts"),
                })
        write_parquet(out / "fees" / "event_fee_changes.parquet", fee_rows, EVENT_FEE_SCHEMA)
    if "incentives" in ds:
        inc = []
        async for p in rest.iter_incentive_programs():
            mt = str(p.get("market_ticker") or "")
            if any(mt.startswith(s + "-") for s in series_list):
                inc.append({
                    "id": str(p.get("id")), "market_ticker": mt, "incentive_type": p.get("incentive_type"),
                    "incentive_description": p.get("incentive_description"), "start_date": p.get("start_date"),
                    "end_date": p.get("end_date"), "period_reward_centicents": p.get("period_reward"),
                    "paid_out": p.get("paid_out"), "raw_json": _dumps(p),
                })
        write_parquet(out / "incentives" / "incentive_programs.parquet", inc, INCENTIVE_SCHEMA)

    for series in series_list:
        if "fees" in ds:
            write_json(out / "series" / f"{series}.json", await rest.get_series(series))
            write_json(out / "fees" / "series_fee_changes" / f"{series}.json",
                       await rest.get_series_fee_changes(series, show_historical=True))
        events = await list_events(rest, series, start_ns, end_ns, use_close_filter=event_close_filter)
        if max_events is not None:
            events = events[:max_events]
        ev_path = out / "events" / f"series={series}" / "events.parquet"
        existing = {r["event_ticker"]: r for r in (pq.read_table(ev_path).to_pylist() if ev_path.is_file() else [])}
        existing.update({e["event_ticker"]: event_row(e) for e in events})
        write_parquet(ev_path, [existing[k] for k in sorted(existing)], EVENT_SCHEMA)
        log(f"{series}: {len(events)} events in range")

        ck_mt = Checkpoint(out / "_checkpoints" / f"events-{series}.json")
        ck_c = Checkpoint(out / "_checkpoints" / f"candles-{series}.json")
        ck_b = Checkpoint(out / "_checkpoints" / f"brti-{series}.json")
        for ev in events:
            et = ev["event_ticker"]
            need_mt = bool({"markets", "trades"} & ds) and et not in ck_mt
            need_c = "candles" in ds and et not in ck_c
            need_b = "brti" in ds and have_auth and et not in ck_b
            if not (need_mt or need_c or need_b):
                stats["events_skipped"] += 1
                continue
            try:
                markets, msrc = await fetch_event_markets(rest, ev, market_cut)
                markets = [m for m in markets if _in_range(m, start_ns, end_ns, ev)]
                if need_mt:
                    await _markets_and_trades(rest, series, et, markets, msrc, trades_cut, out, ds, skip_zero_volume, sem, stats)
                    ck_mt.mark(et)
                if need_c:
                    await _candles(rest, series, et, markets, msrc, candle_period, skip_zero_volume, out, sem, stats)
                    ck_c.mark(et)
                if need_b and markets:
                    exp_ns = max(opt_iso_to_ns(m.get("expected_expiration_time")) or opt_iso_to_ns(m.get("close_time")) for m in markets)
                    rows, raw = await fetch_brti(rest, et, exp_ns, brti_window_s, brti_timespan, brti_timestamp)
                    write_parquet(out / "brti" / f"series={series}" / f"{et}.parquet", rows, BRTI_SCHEMA)
                    write_json(out / "brti" / f"series={series}" / f"{et}.raw.json", raw)
                    stats["brti_ticks"] += len(rows)
                    ck_b.mark(et)
                stats["events"] += 1
            except KalshiHTTPError as exc:
                stats["errors"] += 1
                log(f"{et}: {exc}")
    return stats


def _in_range(m: dict[str, Any], start_ns: int, end_ns: int, ev: dict[str, Any]) -> bool:
    if ev.get("strike_date"):
        return True  # event already filtered by strike_date
    c = opt_iso_to_ns(m.get("close_time"))
    return not c or start_ns <= c < end_ns


async def _markets_and_trades(
    rest: Any, series: str, et: str, markets: list[dict[str, Any]], msrc: str, trades_cut: int, out: Path,
    ds: set[str], skip_zero_volume: bool, sem: asyncio.Semaphore, stats: dict[str, int],
) -> None:
    write_parquet(out / "markets" / f"series={series}" / f"{et}.parquet", [market_row(m, series, msrc) for m in markets], MARKET_SCHEMA)
    stats["markets"] += len(markets)
    if "trades" not in ds:
        return

    async def one(m: dict[str, Any]) -> list[dict[str, Any]]:
        if skip_zero_volume and m.get("volume_fp") is not None and qty_from_fp(str(m["volume_fp"])) == 0:
            return []
        async with sem:
            return [trade_row(t, et, series, src) for t, src in await fetch_market_trades(rest, m, trades_cut)]

    rows = [r for chunk in await asyncio.gather(*(one(m) for m in markets)) for r in chunk]
    rows.sort(key=lambda r: (r["ts_ms"], r["ticker"], r["trade_id"]))
    write_parquet(out / "trades" / f"series={series}" / f"{et}.parquet", rows, TRADE_SCHEMA)
    stats["trades"] += len(rows)


async def _candles(
    rest: Any, series: str, et: str, markets: list[dict[str, Any]], msrc: str, period: int,
    skip_zero_volume: bool, out: Path, sem: asyncio.Semaphore, stats: dict[str, int],
) -> None:
    async def one(m: dict[str, Any]) -> list[dict[str, Any]]:
        if skip_zero_volume and m.get("volume_fp") is not None and qty_from_fp(str(m["volume_fp"])) == 0:
            return []
        async with sem:
            return await fetch_candles(rest, series, m, msrc, period)

    rows = [r for chunk in await asyncio.gather(*(one(m) for m in markets)) for r in chunk]
    write_parquet(out / "candles" / f"series={series}" / f"{et}.parquet", rows, CANDLE_SCHEMA)
    stats["candles"] += len(rows)


# ============================================================================ CLI
def _date_ns(s: str) -> int:
    d = date.fromisoformat(s)
    return int(datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp()) * NS_PER_S


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0], formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--series", nargs="+", default=["KXBTCD", "KXBTC", "KXBTC15M"])
    ap.add_argument("--start", required=True, help="UTC date YYYY-MM-DD (inclusive)")
    ap.add_argument("--end", required=True, help="UTC date YYYY-MM-DD (exclusive)")
    ap.add_argument("--out", type=Path, default=None, help="output root (default: config history.out_dir)")
    ap.add_argument("--datasets", default=",".join(DEFAULT_DATASETS), help=f"comma list of {','.join(DATASETS)}")
    ap.add_argument("--config", default=None, help="kalshi config YAML (default config/kalshi.yaml or example)")
    ap.add_argument("--demo", action="store_true", help="use the demo environment")
    ap.add_argument("--base-url", default=None, help="override REST base URL")
    ap.add_argument("--include-zero-volume", action="store_true", help="also query trades/candles of untraded markets")
    ap.add_argument("--candle-period", type=int, default=1, choices=[1, 60, 1440])
    ap.add_argument("--brti-window-s", type=int, default=300)
    ap.add_argument("--brti-timespan", default="{span_s}s", help="template: {start_ms},{end_ms},{start_s},{end_s},{span_s},{span_ms}")
    ap.add_argument("--brti-timestamp", default="{end_ms}", help="template, see --brti-timespan")
    ap.add_argument("--max-events", type=int, default=None, help="per series (smoke runs)")
    ap.add_argument("--concurrency", type=int, default=4)
    ap.add_argument("--no-event-close-filter", action="store_true",
                    help="do not pass min_close_ts to GET /events (page all settled events, filter locally)")
    return ap.parse_args(argv)


async def amain(args: argparse.Namespace) -> int:
    from dh.kalshi.config import load_config
    from dh.kalshi.rest import KalshiRest

    cfg = load_config(args.config, env="demo" if args.demo else None)
    signer = cfg.signer()
    datasets = [d.strip() for d in args.datasets.split(",") if d.strip()]
    if "brti" in datasets and signer is None:
        print("brti requested but no credentials (KALSHI_KEY_ID / KALSHI_PRIVATE_KEY_PATH): skipping BRTI", file=sys.stderr)
    out = args.out or Path(cfg.history.get("out_dir", "data/external/kalshi"))
    async with KalshiRest(args.base_url or cfg.rest_url, signer, cfg.limiter(), **cfg.rest_kwargs()) as rest:
        if signer is not None:
            await rest.configure_rate_limits()
        stats = await download(
            rest, list(args.series), _date_ns(args.start), _date_ns(args.end), out,
            datasets=datasets, skip_zero_volume=not args.include_zero_volume, candle_period=args.candle_period,
            brti_window_s=args.brti_window_s, brti_timespan=args.brti_timespan, brti_timestamp=args.brti_timestamp,
            max_events=args.max_events, concurrency=args.concurrency, have_auth=signer is not None,
            event_close_filter=not args.no_event_close_filter,
        )
    print(orjson.dumps(stats).decode())
    return 1 if stats["errors"] else 0


def main(argv: list[str] | None = None) -> int:
    return asyncio.run(amain(parse_args(argv)))


if __name__ == "__main__":
    sys.exit(main())
