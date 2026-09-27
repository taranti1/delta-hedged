#!/usr/bin/env python
"""Download settled Kalshi BTC markets + public trades (+ BRTI history, fees, incentives, candles).

Powers Experiment 0 (maker P&L to settlement from public trades) and the M1.2 settlement check.
Resumable and idempotent: every event (markets + trades) and every BRTI hour is checkpointed;
re-running skips finished work. Events are processed NEWEST FIRST, so a partial run always
covers the most recent, contiguous period.

    python scripts/download_kalshi_history.py --series KXBTCD --days 30
    python scripts/download_kalshi_history.py --series KXBTCD KXBTC --start 2026-06-25 --end 2026-09-25
    python scripts/download_kalshi_history.py --datasets brti --days 90          # CF passthrough only

Two request lanes (docs/research/KALSHI_DOCS_RECONCILIATION.md s.b, finding 15):
  * PUBLIC endpoints (events, markets, historical markets/trades, trades, series, fee changes,
    incentives, candles) are sent UNSIGNED: unauthenticated requests do not draw the account's
    rate-limit tokens, which are shared with other live systems. They are paced at --rate
    requests/s (default 5; unauthenticated requests do see 429s when unthrottled) with a pause
    and jittered retries on 429 / 5xx.
  * The CF Benchmarks passthrough (BRTI history) needs a signature and costs 50 read tokens. It
    runs on a signed read-only client whose limiter holds --cf-share (default 0.1) of the
    account's read budget (basic tier 200 tokens/s -> 20 tokens/s -> one call per 2.5 s).

Sources (openapi 3.31.0, GET /historical/cutoff):
  events       GET /events?series_ticker=S&status=settled&min_close_ts=start   (all events)
  markets      GET /historical/markets?event_ticker=E (settled before market_settled_ts)
               else GET /markets?event_ticker=E                 (tries the other if empty)
  trades       GET /historical/trades?ticker=T (trades before trades_created_ts) and/or
               GET /markets/trades?ticker=T; de-duplicated by trade_id
  candles      GET /historical/markets/{T}/candlesticks or /series/{S}/markets/{T}/candlesticks
  fees         GET /series/{S}, /series/fee_changes?series_ticker=S&show_historical=true,
               GET /events/fee_changes (all pages, filtered to the series)
  incentives   GET /incentive_programs?status=all&type=all (filtered to the series)
  brti         GET /cfbenchmarks/history/values?id=BRTI&timespan=HOUR&timestamp=<hour START,
               ISO ms> (VERIFIED 2026-09-25: the body is {"data": {"serverTime", "payload":
               [{"time": ms, "value": "83737.50"}, ...]}} with the 18,000 5 Hz ticks of
               [timestamp, timestamp + 1 h); MINUTE / non-aligned timestamps return 400)

Output (Parquet, explicit schemas) under --out (default data/external/kalshi):
  meta/historical_cutoff.json            series/<S>.json      fees/series_fee_changes/<S>.json
  events/series=<S>/events.parquet       markets/series=<S>/<EVENT>.parquet
  trades/series=<S>/<EVENT>.parquet      candles/series=<S>/<EVENT>.parquet
  brti/hourly/<YYYY-MM-DD>/<HH>.parquet  (t_ms int64 CF source time, cents int64; every 5 Hz tick)
  fees/event_fee_changes.parquet         incentives/incentive_programs.parquet
  _checkpoints/<dataset>-<S>.log         _progress.json

Settlement time: T = close_time (dh.settlement.convention); ``expected_expiration_time`` (close +
5 min) is kept as metadata only.

Trade schema (exact integer units, dh.core.units):
  trade_id str | ticker str | yes_px int64 (1e-4 $) | qty int64 (0.01 contract) |
  taker_outcome_side str ('yes' = taker bought YES) | taker_book_side str ('bid' == 'yes') |
  ts_ms int64 | is_block_trade bool | event_ticker | series_ticker | source ('historical' | 'live')
Maker gross P&L to settlement per trade = qty/100 * ((settle - yes_px) if taker sold YES
else (yes_px - settle)) / 1e4 dollars, with settle = settlement_px from the markets file.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import random
import sys
import time
from collections.abc import Iterable
from datetime import date, datetime, timedelta, timezone
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
BRTI_SCHEMA = pa.schema([("t_ms", pa.int64()), ("cents", pa.int64())])

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
def write_parquet(path: Path, rows: list[dict[str, Any]] | pa.Table, schema: pa.Schema, *, delta_cols: tuple[str, ...] = ()) -> None:
    """Atomic write (tmp + rename), zstd, explicit schema (empty tables keep their types).
    ``delta_cols``: integer columns written with DELTA_BINARY_PACKED (sorted time series)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    table = rows if isinstance(rows, pa.Table) else pa.Table.from_pylist(rows, schema=schema)
    tmp = path.with_suffix(path.suffix + ".tmp")
    kw: dict[str, Any] = {"compression": "zstd"}
    if delta_cols:
        kw.update(use_dictionary=[c for c in table.column_names if c not in delta_cols],
                  column_encoding={c: "DELTA_BINARY_PACKED" for c in delta_cols}, compression_level=9)
    pq.write_table(table, tmp, **kw)
    os.replace(tmp, path)


def valid_parquet(path: Path, schema: pa.Schema, *, nonempty: bool = False) -> bool:
    """A checkpoint is only a hint: missing, truncated or wrong-schema outputs need repair."""
    try:
        f = pq.ParquetFile(path)
        return f.schema_arrow.equals(schema) and (not nonempty or f.metadata.num_rows > 0)
    except (OSError, pa.ArrowException):
        return False


def merge_metadata(path: Path, rows: list[dict[str, Any]], schema: pa.Schema) -> None:
    """Upsert fetched identities, retaining all earlier series and historical records."""
    old = pq.ParquetFile(path).read().to_pylist() if path.is_file() else []
    def key(r):
        identity = r.get("id")
        return ("id", identity) if identity and identity != "None" else ("row", _dumps(r))
    merged = {key(r): r for r in old}
    merged.update({key(r): r for r in rows})
    write_parquet(path, list(merged.values()), schema)


def write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_bytes(orjson.dumps(obj, option=orjson.OPT_INDENT_2 | orjson.OPT_SORT_KEYS))
    os.replace(tmp, path)


class Checkpoint:
    """Set of finished keys, persisted as an append-only log (one key per line; O(1) per mark).
    A legacy JSON checkpoint ({"done": [...]}) next to it is read too."""

    def __init__(self, path: Path) -> None:
        self.path = path.with_suffix(".log") if path.suffix == ".json" else path
        self.done: set[str] = set()
        legacy = self.path.with_suffix(".json")
        if legacy.is_file():
            self.done |= set(orjson.loads(legacy.read_bytes()).get("done", []))
        if self.path.is_file():
            self.done |= {ln for ln in self.path.read_text().splitlines() if ln}

    def __contains__(self, key: str) -> bool:
        return key in self.done

    def mark(self, key: str) -> None:
        if key in self.done:
            return
        self.done.add(key)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.path, "a") as f:
            f.write(key + "\n")


class Pacer:
    """Rate limiter for UNSIGNED public requests (KalshiRest ``limiter`` protocol): at most
    ``rate`` request starts per second; a 429 pauses every request for ``pause_s`` (doubling
    per consecutive 429, capped at 120 s, reset by the next success window)."""

    def __init__(self, rate: float = 5.0, pause_s: float = 5.0, clock: Any = time.monotonic, sleep: Any = asyncio.sleep) -> None:
        self.dt = 1.0 / max(rate, 1e-6)
        self.pause_s = pause_s
        self._next = 0.0
        self._streak = 0
        self._clock = clock
        self._sleep = sleep
        self.n = 0
        self.n_429 = 0

    async def acquire(self, method: str, path: str, n_items: int = 1) -> float:
        now = self._clock()
        slot = max(now, self._next)
        self._next = slot + self.dt
        wait = slot - now
        if wait > 0:
            await self._sleep(wait)
        self.n += 1
        if self._streak and self.n % 50 == 0:
            self._streak = 0
        return wait

    def on_429(self, method: str) -> None:
        self.n_429 += 1
        self._streak += 1
        pause = min(120.0, self.pause_s * 2 ** (self._streak - 1)) * (0.75 + 0.5 * random.random())
        self._next = max(self._next, self._clock() + pause)


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


def iso_ms(t_ns: int) -> str:
    """UTC ISO-8601 with milliseconds and 'Z' (the CF Benchmarks timestamp format)."""
    d = datetime.fromtimestamp(t_ns // NS_PER_S, tz=timezone.utc)
    return d.strftime("%Y-%m-%dT%H:%M:%S.") + f"{(t_ns // NS_PER_MS) % 1000:03d}Z"


def brti_hour_path(out: Path, hour_ns: int) -> Path:
    d = datetime.fromtimestamp(hour_ns // NS_PER_S, tz=timezone.utc)
    return out / "brti" / "hourly" / d.strftime("%Y-%m-%d") / f"{d.hour:02d}.parquet"


async def fetch_brti_hour(rest: Any, hour_ns: int) -> tuple[pa.Table, dict[str, int]]:
    """Every BRTI tick of [hour, hour + 1 h) from the CF passthrough (timespan=HOUR, timestamp =
    the hour START, verified live). Returns (table t_ms/cents ascending, stats)."""
    body = await rest.get_cfbenchmarks_history("BRTI", timespan="HOUR", timestamp=iso_ms(hour_ns))
    ticks = cf_history_to_ticks(body, 0, "BRTI")
    lo, hi = hour_ns // NS_PER_MS, hour_ns // NS_PER_MS + 3_600_000
    t_ms: list[int] = []
    cents: list[int] = []
    off = 0
    for t in ticks:
        ms = t.ts_exch // NS_PER_MS
        if not lo <= ms < hi:
            continue
        c = round(t.value * 100)
        off += abs(t.value * 100 - c) > 1e-4
        t_ms.append(ms)
        cents.append(int(c))
    return pa.table({"t_ms": pa.array(t_ms, pa.int64()), "cents": pa.array(cents, pa.int64())}), {"ticks": len(t_ms), "off_cent": off}


async def download_brti(
    rest: Any, start_ns: int, end_ns: int, out: Path, *, now_ns: int | None = None, recent_delay_s: float = 1800.0,
    log: Any = print, stats: dict[str, int] | None = None,
) -> dict[str, int]:
    """Hourly BRTI files for [start, end), newest hour first; an existing file = done. An empty
    hour ending within ``recent_delay_s`` of now is skipped (CF publication delay), an older
    empty hour is written empty (an outage is data) and logged."""
    st = stats if stats is not None else {}
    for k in ("brti_hours", "brti_ticks", "brti_empty", "brti_errors", "brti_skipped"):
        st.setdefault(k, 0)
    now = now_ns if now_ns is not None else time.time_ns()
    H = 3600 * NS_PER_S
    h = min(end_ns, now) // H * H - H  # newest complete hour
    first = start_ns // H * H
    t_start = time.time()
    n_new = 0
    total = max(0, (h - first) // H + 1)
    while h >= first:
        path = brti_hour_path(out, h)
        # An empty file is only ever written for an hour older than recent_delay_s (a real
        # outage is data): it is done. Unreadable / wrong-schema files are fetched again.
        if valid_parquet(path, BRTI_SCHEMA):
            st["brti_skipped"] += 1
            h -= H
            continue
        try:
            table, s1 = await fetch_brti_hour(rest, h)
        except KalshiHTTPError as exc:
            st["brti_errors"] += 1
            log(f"brti {iso_ms(h)}: {exc}")
            h -= H
            continue
        if table.num_rows == 0 and h + H > now - int(recent_delay_s * NS_PER_S):
            h -= H
            continue
        if table.num_rows == 0:
            st["brti_empty"] += 1
            log(f"brti {iso_ms(h)}: no ticks (outage?) -> empty file")
        if s1["off_cent"]:
            log(f"brti {iso_ms(h)}: {s1['off_cent']} values not on the cent grid (rounded)")
        write_parquet(path, table, BRTI_SCHEMA, delta_cols=("t_ms", "cents"))
        st["brti_hours"] += 1
        st["brti_ticks"] += table.num_rows
        n_new += 1
        if n_new % 24 == 0:
            el = time.time() - t_start
            done = (min(end_ns, now) // H * H - H - h) // H + 1
            log(f"brti: {iso_ms(h)} done; {n_new} new hours in {el:.0f}s; "
                f"ETA {(total - done) * el / max(n_new, 1) / 60:.0f} min for {total - done} hours")
        h -= H
    return st


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
    max_events: int | None = None,
    concurrency: int = 4,
    have_auth: bool = False,
    cf_rest: Any = None,
    event_close_filter: bool = True,
    now_ns: int | None = None,
    log: Any = print,
) -> dict[str, int]:
    """Download everything requested; returns counters. Safe to re-run (checkpoints).

    ``rest`` serves the public endpoints (unsigned in the CLI); ``cf_rest`` (signed; defaults to
    ``rest``) serves the CF passthrough, used only when ``have_auth``. Events are processed
    newest first."""
    ds = set(datasets)
    unknown = ds - set(DATASETS)
    if unknown:
        raise ValueError(f"unknown datasets {sorted(unknown)}")
    stats = {"events": 0, "events_skipped": 0, "markets": 0, "trades": 0, "candles": 0, "brti_ticks": 0, "errors": 0}
    cf = cf_rest if cf_rest is not None else rest
    if "brti" in ds and have_auth:
        # the signed lane is independent of the event loop below: run it concurrently
        brti_task = asyncio.ensure_future(download_brti(cf, start_ns, end_ns, out, now_ns=now_ns, log=log, stats=stats))
    else:
        brti_task = None
    if not ({"markets", "trades", "candles", "fees", "incentives"} & ds):
        if brti_task is not None:
            await brti_task
        return stats
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
        merge_metadata(out / "fees" / "event_fee_changes.parquet", fee_rows, EVENT_FEE_SCHEMA)
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
        merge_metadata(out / "incentives" / "incentive_programs.parquet", inc, INCENTIVE_SCHEMA)

    for series in series_list:
        if "fees" in ds:
            snapshot = {**await rest.get_series(series), "fetched_ns": time.time_ns()}
            write_json(out / "fees" / "snapshots" / "series" / series / f"{snapshot['fetched_ns']}.json", snapshot)
            write_json(out / "series" / f"{series}.json", snapshot)
            write_json(out / "fees" / "series_fee_changes" / f"{series}.json",
                       await rest.get_series_fee_changes(series, show_historical=True))
        events = await list_events(rest, series, start_ns, end_ns, use_close_filter=event_close_filter)
        events.reverse()  # newest first: a partial run covers the most recent period
        if max_events is not None:
            events = events[:max_events]
        ev_path = out / "events" / f"series={series}" / "events.parquet"
        existing = {r["event_ticker"]: r for r in (pq.read_table(ev_path).to_pylist() if ev_path.is_file() else [])}
        existing.update({e["event_ticker"]: event_row(e) for e in events})
        write_parquet(ev_path, [existing[k] for k in sorted(existing)], EVENT_SCHEMA)
        legacy_events = Checkpoint(out / "_checkpoints" / f"events-{series}.log")
        checkpoints = {d: Checkpoint(out / "_checkpoints" / f"{d}-{series}.log")
                       for d in ("markets", "trades", "candles")}
        schemas = {"markets": MARKET_SCHEMA, "trades": TRADE_SCHEMA, "candles": CANDLE_SCHEMA}
        def needs(et: str, dataset: str) -> bool:
            path = out / dataset / f"series={series}" / f"{et}.parquet"
            return dataset in ds and ((et not in checkpoints[dataset] and et not in legacy_events)
                                      or not valid_parquet(path, schemas[dataset]))
        todo = [e for e in events if any(needs(e["event_ticker"], d) for d in checkpoints)]
        stats["events_skipped"] += len(events) - len(todo)
        log(f"{series}: {len(events)} events in range, {len(todo)} to do")
        t0 = time.time()
        n0 = stats["trades"]
        for i, ev in enumerate(todo):
            et = ev["event_ticker"]
            needed = {d for d in checkpoints if needs(et, d)}
            need_mt = bool({"markets", "trades"} & needed)
            need_c = "candles" in needed
            try:
                markets, msrc = await fetch_event_markets(rest, ev, market_cut)
                markets = [m for m in markets if _in_range(m, start_ns, end_ns, ev)]
                if need_mt:
                    await _markets_and_trades(rest, series, et, markets, msrc, trades_cut, out, needed, skip_zero_volume, sem, stats)
                    checkpoints["markets"].mark(et)
                    if "trades" in needed:
                        checkpoints["trades"].mark(et)
                if need_c:
                    await _candles(rest, series, et, markets, msrc, candle_period, skip_zero_volume, out, sem, stats)
                    checkpoints["candles"].mark(et)
                stats["events"] += 1
            except KalshiHTTPError as exc:
                stats["errors"] += 1
                log(f"{et}: {exc}")
            if (i + 1) % 10 == 0 or i + 1 == len(todo):
                el = time.time() - t0
                eta = el / (i + 1) * (len(todo) - i - 1)
                log(f"{series}: {i + 1}/{len(todo)} events (last {et}), {stats['trades'] - n0} trades, "
                    f"{el / 60:.1f} min, ETA {eta / 60:.1f} min")
                write_json(out / "_progress.json", {"series": series, "done": i + 1, "todo": len(todo), "last_event": et,
                                                    "eta_min": round(eta / 60, 1), "stats": stats, "updated_ns": time.time_ns()})
    if brti_task is not None:
        await brti_task
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
    mrows = [market_row(m, series, msrc) for m in markets]
    stats["markets"] += len(markets)
    if "trades" in ds:
        async def one(m: dict[str, Any]) -> list[dict[str, Any]]:
            if skip_zero_volume and m.get("volume_fp") is not None and qty_from_fp(str(m["volume_fp"])) == 0:
                return []
            async with sem:
                return [trade_row(t, et, series, src) for t, src in await fetch_market_trades(rest, m, trades_cut)]

        rows = [r for chunk in await asyncio.gather(*(one(m) for m in markets)) for r in chunk]
        rows.sort(key=lambda r: (r["ts_ms"], r["ticker"], r["trade_id"]))
        write_parquet(out / "trades" / f"series={series}" / f"{et}.parquet", rows, TRADE_SCHEMA)
        stats["trades"] += len(rows)
    # markets last: a markets file without its trades file never exists for a finished event
    write_parquet(out / "markets" / f"series={series}" / f"{et}.parquet", mrows, MARKET_SCHEMA)


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


def estimate(days: float, series: Iterable[str], rate: float, cf_share: float = 0.1, read_budget: float = 200.0) -> dict[str, Any]:
    """Rough wall-clock estimate from request counts measured 2026-09-25 (one day of data):
    KXBTCD ~1,100 public requests (646 traded markets, ~310k trades), KXBTC ~500 (344 markets,
    ~17k trades), KXBTC15M ~3,600 (96 markets, ~3.4M trades), plus 24 CF calls (50 tokens each)."""
    per_day = {"KXBTCD": 1100, "KXBTC": 500, "KXBTC15M": 3600}
    req = sum(per_day.get(s, 1000) for s in series) * days
    cf_s = 24 * days * 50 / (cf_share * read_budget)
    return {"public_requests": int(req), "public_hours": round(req / rate / 3600, 1), "brti_hours": round(cf_s / 3600, 1)}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0], formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--series", nargs="+", default=["KXBTCD", "KXBTC", "KXBTC15M"])
    ap.add_argument("--start", default=None, help="UTC date YYYY-MM-DD (inclusive)")
    ap.add_argument("--end", default=None, help="UTC date YYYY-MM-DD (exclusive; default: tomorrow)")
    ap.add_argument("--days", type=float, default=None, help="instead of --start: the last N days up to --end")
    ap.add_argument("--out", type=Path, default=None, help="output root (default: config history.out_dir)")
    ap.add_argument("--datasets", default=",".join(DEFAULT_DATASETS), help=f"comma list of {','.join(DATASETS)}")
    ap.add_argument("--config", default=None, help="kalshi config YAML (default config/kalshi.yaml or example)")
    ap.add_argument("--demo", action="store_true", help="use the demo environment")
    ap.add_argument("--base-url", default=None, help="override REST base URL")
    ap.add_argument("--include-zero-volume", action="store_true", help="also query trades/candles of untraded markets")
    ap.add_argument("--candle-period", type=int, default=1, choices=[1, 60, 1440])
    ap.add_argument("--max-events", type=int, default=None, help="per series (smoke runs)")
    ap.add_argument("--concurrency", type=int, default=4)
    ap.add_argument("--rate", type=float, default=5.0, help="public (unsigned) requests per second")
    ap.add_argument("--cf-share", type=float, default=0.1, help="fraction of the account read budget for the CF passthrough")
    ap.add_argument("--estimate", action="store_true", help="print the time estimate and exit (no requests)")
    ap.add_argument("--plan", default=None,
                    help="sequential steps SERIES[+SERIES]:DAYS,... (e.g. 'KXBTCD:30,KXBTC15M:14,KXBTCD:90'), each the last "
                         "DAYS up to --end; later steps skip what earlier ones finished. Overrides --series/--days/--start")
    ap.add_argument("--pid-file", type=Path, default=None, help="write this process id here (removed on exit)")
    ap.add_argument("--no-event-close-filter", action="store_true",
                    help="do not pass min_close_ts to GET /events (page all settled events, filter locally)")
    return ap.parse_args(argv)


def _range(args: argparse.Namespace) -> tuple[int, int]:
    end = _date_ns(args.end) if args.end else _date_ns((datetime.now(timezone.utc) + timedelta(days=1)).date().isoformat())
    if args.days is not None:
        return end - int(args.days * 86400) * NS_PER_S, end
    if not args.start:
        raise SystemExit("--start or --days is required")
    return _date_ns(args.start), end


def parse_plan(plan: str) -> list[tuple[list[str], float]]:
    """'KXBTCD:30,KXBTC15M+KXBTC:14' -> [(['KXBTCD'], 30.0), (['KXBTC15M', 'KXBTC'], 14.0)]."""
    steps = []
    for item in plan.split(","):
        item = item.strip()
        if not item:
            continue
        ser, _, days = item.partition(":")
        steps.append(([x for x in ser.split("+") if x], float(days)))
    return steps


async def amain(args: argparse.Namespace) -> int:
    if args.pid_file:
        args.pid_file.parent.mkdir(parents=True, exist_ok=True)
        args.pid_file.write_text(str(os.getpid()))
    try:
        if not args.plan:
            return await _run_once(args)
        rc = 0
        for series, days in parse_plan(args.plan):
            a = argparse.Namespace(**{**vars(args), "series": series, "days": days, "start": None, "plan": None})
            print(time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), f"=== plan step {'+'.join(series)} last {days:g} days",
                  flush=True)
            rc |= await _run_once(a)
        return rc
    finally:
        if args.pid_file and args.pid_file.is_file() and args.pid_file.read_text().strip() == str(os.getpid()):
            args.pid_file.unlink()


async def _run_once(args: argparse.Namespace) -> int:
    from dh.kalshi.config import load_config
    from dh.kalshi.rate_limit import KalshiRateLimiter
    from dh.kalshi.rest import KalshiRest

    start_ns, end_ns = _range(args)
    datasets = [d.strip() for d in args.datasets.split(",") if d.strip()]
    if args.estimate:
        print(orjson.dumps(estimate((end_ns - start_ns) / 86400e9, args.series if set(datasets) - {"brti"} else [],
                                    args.rate, args.cf_share)).decode())
        return 0
    cfg = load_config(args.config, env="demo" if args.demo else None)
    signer = cfg.signer() if "brti" in datasets else None
    if "brti" in datasets and signer is None:
        print(f"brti requested but no credentials ({cfg.credentials_hint()}): skipping BRTI", file=sys.stderr)
    out = args.out or Path(cfg.history.get("out_dir", "data/external/kalshi"))
    base = args.base_url or cfg.rest_url
    kw = {**cfg.rest_kwargs(), "max_get_retries": 10, "backoff_max_s": 60.0}
    log = lambda *a: print(time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), *a, flush=True)  # noqa: E731
    log(f"range {iso_ms(start_ns)} .. {iso_ms(end_ns)} series={args.series} datasets={datasets} rate={args.rate}/s "
        f"estimate={estimate((end_ns - start_ns) / 86400e9, args.series if set(datasets) - {'brti'} else [], args.rate, args.cf_share)}")
    # public lane: NO signer (unauthenticated requests do not draw the shared account budget)
    async with KalshiRest(base, None, Pacer(args.rate), read_only=True, **kw) as rest:
        cf_rest = None
        if signer is not None:
            cf_rest = KalshiRest(base, signer, KalshiRateLimiter(account_share=args.cf_share), read_only=True, **kw)
            await cf_rest.configure_rate_limits()
            log("cf lane:", cf_rest.limiter.describe())
        try:
            stats = await download(
                rest, list(args.series), start_ns, end_ns, out,
                datasets=datasets, skip_zero_volume=not args.include_zero_volume, candle_period=args.candle_period,
                max_events=args.max_events, concurrency=args.concurrency, have_auth=signer is not None,
                cf_rest=cf_rest, event_close_filter=not args.no_event_close_filter, log=log,
            )
        finally:
            if cf_rest is not None:
                await cf_rest.close()
        log("public lane:", rest.stats, "cf lane:", cf_rest.stats if cf_rest is not None else None)
    print(orjson.dumps(stats).decode(), flush=True)
    return 1 if stats["errors"] else 0


def main(argv: list[str] | None = None) -> int:
    return asyncio.run(amain(parse_args(argv)))


if __name__ == "__main__":
    sys.exit(main())
