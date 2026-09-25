"""Replay environment: rebuild the Kalshi market universe from a recording and replay the
production strategy (MarketMaker + KalshiExchangeSim + Ledger) over it.

    uni = build_universe(root, t0, t1)                        # specs, fee schedules, settlements
    res = run_replay(root, t0, t1, cfg, policy="B")          # ReplayResult(df, summary, ...)

Everything is read from the append-only store written by scripts/record.py (nothing is fetched):

  kalshi.rest.markets / .market / .events   Market / EventData objects: strikes, open/close/
                                            expected-expiration times, tick grid (price_ranges)
  kalshi.rest.series / .fees                Series fee_type/fee_multiplier; GET /series/fee_changes
                                            and /events/fee_changes (scheduled changes)
  kalshi.ws  market_lifecycle_v2            'created' (additional_metadata strikes), 'metadata_updated'
                                            (KXBTC15M strikes set after open), close_date_updated,
                                            price_level_structure_updated, determined/settled
  kalshi.ws  event_fee_update               event-level fee overrides
  kalshi.ws  orderbook/trade/cfbenchmarks   market data, through the live sequencer (dh.store.replay)
  <venue>.*                                 external venues (dh.feeds normalizers)
  events.md.*                               normalized market-data caches (codec events; used by the
                                            synthetic recording for its external venues)
  kalshi.rest.cfbenchmarks                  CF Benchmarks history: fair-value warm-up ONLY (never fed
                                            to the strategy as live ticks)

Market availability (no look-ahead): a market enters the replay at the receive time of the first
record from which a complete MarketSpec can be built (strike known, tick grid known), never
before t0. Markets available at t0 are passed to MarketMaker(...); later ones are added with
``MarketMaker.add_markets`` at that time (at the earliest ``expiration - add_horizon_s``), with a
book snapshot synthesized from the replayed book, exactly like the live runner's roll-over. Fee
type/multiplier are resolved as of max(availability, t0): event override (EventData, scheduled
/events/fee_changes, WS event_fee_update) > series (+ scheduled /series/fee_changes) > market.

Own-order footprint (recordings made while we were trading) is removed by OwnFootprintFilter:
  * orderbook_delta messages carrying our client_order_id (Kalshi sets it only on deltas we
    caused: placements, cancels, amends, our taker consumption) are dropped; our resting qty per
    level is tracked from them and subtracted from every later snapshot, so downstream books hold
    OTHER participants' orders only;
  * public trades that were our fills (trade_id == trade_id of a private `fill` message, pre-
    scanned for the replay window) are reduced by our fill qty (dropped when fully ours);
  * the taker-caused level decrease that consumed our resting qty does NOT carry our
    client_order_id: it is absorbed against our tracked qty using the trade_id-matched fills
    (pending within a 2 s window; a decrease seen before its print is corrected retroactively);
  * private channels (fill, user_orders, market_positions, order_group_updates) and their
    sequence-gap statuses are dropped: the simulator generates the counterfactual ones. Liquidity we took as a taker is kept in the
    counterfactual book until its level empties or the next snapshot.
The filter's counters are reported in ``summary['own_filter']``.

Windows: events in [t0, t1) drive the strategy; records in [t0 - state_warm_s, t0) prime books,
connection state and the settlement window (synthesized snapshots at t0); only settlement
messages are passed in [t1, t1 + tail_s). The fair-value model is warmed from recorded BRTI
ticks in [t0 - fv_warm_s, t0) (WS 1 Hz/5 Hz and CF-history REST records), or from a CSV/Parquet
price file, or (flagged, synthetic) a seeded GBM path: ``warm='recorded'|'recorded+gbm'|'gbm'|
'csv:<path>'|'none'``. Missing settlements are filled from recorded REST market results.

Fitted inputs and look-ahead: the fair-value parameters (default dh/models/data/fv_recommended.json,
fitted on history through its ``data_end_utc``) and optional taker-flow segments
(dh.research.calibrate_flow / flow_recording JSON, ``meta.fit_end_ms``) are checked against the
replay's t0. A replay that starts before the end of their fitting data is IN-SAMPLE: the summary
says so (``fv_params``, ``fv_params_in_sample``, ``flow_segments``, ``flow_in_sample``) and a warning
is raised; bind walk-forward inputs with ``bind_replay_inputs(universe, fv_config=..,
flow_segments=..)`` (CLI --fv-config / --flow-segments) or pass them to run_replay. Recordings made
after the FV fit (every recording from 2026-09-25 on, for the committed config) are
out-of-sample. Settlement prints are mapped only by dh.settlement (SettlementTracker: 1 Hz tick
with source time u = print for second ceil(u), later of two kept); nothing here re-derives it.

Determinism: identical inputs and seeds -> identical results (no clock, seeded latency).
"""

from __future__ import annotations

import dataclasses
import math
import time
from collections import Counter, defaultdict, deque
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import numpy as np
import orjson
import pandas as pd

from dh.backtest.ledger import Ledger
from dh.backtest.runner import END_OF_TIME_NS, with_timers
from dh.core.actions import Halt, Log, PlaceOrder
from dh.core.book import KalshiBook
from dh.core.events import (
    CancelAck,
    Event,
    FeedStatus,
    IndexTick,
    KalshiBookDelta,
    KalshiBookSnapshot,
    KalshiFeeUpdate,
    KalshiFill,
    KalshiMarketLifecycle,
    KalshiOrderGroupUpdate,
    KalshiOrderUpdate,
    KalshiPositionSnapshot,
    KalshiTrade,
    OrderAck,
    OrderReject,
    Settlement,
    Timer,
)
from dh.core.market import MarketSpec
from dh.core.units import NS_PER_S, PX_SCALE, QTY_SCALE
from dh.execution.exchange_sim import KalshiExchangeSim
from dh.execution.latency import LatencyModel
from dh.feeds.books import BookTracker
from dh.feeds.registry import has_normalizer
from dh.kalshi.fees import FeeEngine, FeeSchedule, OrderFeeAccumulator, apply_scheduled_changes, resolve_fee_fields
from dh.kalshi.normalize import (
    UnsupportedMarket,
    cf_history_to_ticks,
    rest_market_to_spec,
    ws_message_to_events,
)
from dh.kalshi.wire import normalize_route, opt_iso_to_ns
from dh.models.fvmodel import FairValueModel, load_recommended_config
from dh.research.exp_common import policy_letter
from dh.store.codec import decode_event
from dh.store.replay import Normalizers, ReadStats, iter_raw, list_streams, resolve_streams
from dh.strategy.config import StrategyConfig
from dh.strategy.mm import MarketMaker

HOUR_S = 3600.0
DAY_S = 86400.0
META_STREAMS = ("kalshi.rest.markets", "kalshi.rest.market", "kalshi.rest.events", "kalshi.rest.series",
                "kalshi.rest.fees")
CF_REST_STREAM = "kalshi.rest.cfbenchmarks"
MD_CACHE_PREFIX = "events.md."  # normalized market-data caches (codec events)
PRIVATE_TYPES = (KalshiFill, KalshiOrderUpdate, KalshiOrderGroupUpdate, KalshiPositionSnapshot, OrderAck, OrderReject,
                 CancelAck)
# per-channel sequence-gap statuses of our private channels (dh.kalshi.sequencer): the recording
# account's own activity, irrelevant to (and misleading in) a counterfactual replay
PRIVATE_CHANNEL_STREAMS = frozenset(f"kalshi.ws:{c}" for c in ("fill", "user_orders", "market_positions",
                                                                 "order_group_updates"))
_STRIKE_KEYS = ("strike_type", "floor_strike", "cap_strike", "custom_strike")
_MARKET_SUBRESOURCES = {"trades", "orderbooks", "candlesticks"}


def _iso(sec: int | float) -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(int(sec)))


def _ns(x: float) -> int:
    return int(round(x * NS_PER_S))


# ============================================================================ stream selection
def kalshi_ws_streams(root: str | Path) -> list[str]:
    """The raw Kalshi WebSocket stream ('kalshi.ws': collector and live runner both record there)."""
    return [s for s in list_streams(root) if s == "kalshi.ws"]


def default_streams(root: str | Path) -> list[str]:
    """Market-data streams replayed into the strategy, in merge-rank order: Kalshi WS, external
    venues (registered normalizers), normalized market-data caches, collector status."""
    avail = list_streams(root)
    out = kalshi_ws_streams(root)
    out += [s for s in avail if has_normalizer(s) and s not in out]
    out += [s for s in avail if s.startswith(MD_CACHE_PREFIX)]
    if "status" in avail:
        out.append("status")
    return out


# ============================================================================ universe
@dataclass
class MarketRecord:
    """Everything the recording says about one market (raw dicts merged in receive order)."""

    ticker: str
    first_seen_ns: int = 0
    avail_ns: int = 0  # first receive time at which a complete MarketSpec could be built (0 = never)
    spec: MarketSpec | None = None
    reject: str = "no market metadata"
    raw: dict[str, Any] = field(default_factory=dict)
    obs: list[tuple[int, str, dict[str, Any]]] = field(default_factory=list)  # (recv, kind, payload)
    result: str = ""
    settle_px: int | None = None  # YES payout, 1e-4 $
    expiration_value: float | None = None
    settled_ns: int = 0


@dataclass
class Universe:
    """Market universe of a recording window (see module docstring)."""

    root: str
    t0: int
    t1: int
    markets: dict[str, MarketRecord] = field(default_factory=dict)
    series: dict[str, list[tuple[int, dict[str, Any]]]] = field(default_factory=dict)
    events: dict[str, list[tuple[int, dict[str, Any]]]] = field(default_factory=dict)
    series_fee_changes: list[dict[str, Any]] = field(default_factory=list)
    event_fee_changes: list[dict[str, Any]] = field(default_factory=list)
    ws_fee_updates: list[tuple[int, str, str | None, str | None]] = field(default_factory=list)
    own_fills: dict[str, KalshiFill] = field(default_factory=dict)
    synthetic: bool = False
    meta: list[dict[str, Any]] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)
    cache: dict[Any, Any] = field(default_factory=dict, repr=False)  # e.g. BRTI tick scans (see brti_ticks)
    # fitted replay inputs bound to this window (bind_replay_inputs; CLI --fv-config / --flow-segments)
    fv_config: dict[str, Any] | None = None  # FairValueModel config; None = dh/models/data/fv_recommended.json
    fv_config_source: str = ""
    flow_segments: dict[tuple[str, str, str], Any] | None = None  # taker-flow segments; None = cfg.fill defaults
    flow_meta: dict[str, Any] = field(default_factory=dict)
    flow_source: str = ""

    # ------------------------------------------------------------------ fee resolution
    def series_at(self, series_ticker: str, t: int) -> dict[str, Any] | None:
        """Series object in force at t: latest snapshot received at or before t (the earliest
        snapshot if none precedes t) with scheduled fee changes up to t applied."""
        snaps = self.series.get(series_ticker)
        if not snaps:
            return None
        before = [s for s in snaps if s[0] <= t]
        recv, obj = before[-1] if before else snaps[0]
        ch = [c for c in self.series_fee_changes if str(c.get("series_ticker") or series_ticker) == series_ticker]
        return apply_scheduled_changes(obj, ch, min(recv, t), t) if ch else dict(obj)

    def event_at(self, event_ticker: str, t: int) -> dict[str, Any] | None:
        snaps = self.events.get(event_ticker)
        obj: dict[str, Any] = {}
        recv = 0
        if snaps:
            before = [s for s in snaps if s[0] <= t]
            recv, obj = before[-1] if before else snaps[0]
            obj = dict(obj)
        ch = [c for c in self.event_fee_changes if str(c.get("event_ticker") or "") == event_ticker]
        if ch:
            obj = apply_scheduled_changes(obj, ch, min(recv, t) if recv else -1, t, type_key="fee_type_override",
                                          mult_key="fee_multiplier_override")
        for r, et, ftype, mult in self.ws_fee_updates:
            if et == event_ticker and r <= t:
                obj["fee_type_override"] = ftype
                obj["fee_multiplier_override"] = mult
        if not obj:
            return None
        obj.setdefault("event_ticker", event_ticker)
        return obj

    def fee_fields(self, ticker: str, t: int) -> tuple[str, float, str]:
        rec = self.markets[ticker]
        spec = rec.spec
        series_t = spec.series_ticker if spec is not None else str(rec.raw.get("event_ticker", "")).split("-", 1)[0]
        event_t = spec.event_ticker if spec is not None else str(rec.raw.get("event_ticker", ""))
        ftype, mult, src = resolve_fee_fields(self.series_at(series_t, t), self.event_at(event_t, t), rec.raw)
        return ftype, float(mult) if mult is not None else 1.0, src

    def fee_changes_in_window(self) -> list[str]:
        out = []
        for c in self.series_fee_changes:
            ts = opt_iso_to_ns(c.get("scheduled_ts"))
            if self.t0 <= ts < self.t1:
                out.append(f"series {c.get('series_ticker')} -> {c.get('fee_type')} x{c.get('fee_multiplier')} at {_iso(ts // NS_PER_S)}")
        for c in self.event_fee_changes:
            ts = opt_iso_to_ns(c.get("scheduled_ts"))
            if self.t0 <= ts < self.t1:
                out.append(f"event {c.get('event_ticker')} -> {c.get('fee_type_override')} at {_iso(ts // NS_PER_S)}")
        for r, et, ftype, mult in self.ws_fee_updates:
            if self.t0 <= r < self.t1:
                out.append(f"event {et} -> {ftype} x{mult} (ws) at {_iso(r // NS_PER_S)}")
        return out

    # ------------------------------------------------------------------ queries
    def specs(self, series: Iterable[str] | None = None, *, active_in_window: bool = True) -> list[MarketSpec]:
        """Resolved specs (fee as of max(availability, t0)), optionally restricted to series and
        to markets available before t1 and expiring after t0."""
        ss = None if series is None else set(series)
        out = []
        for t in sorted(self.markets):
            rec = self.markets[t]
            s = rec.spec
            if s is None or (ss is not None and s.series_ticker not in ss):
                continue
            if active_in_window and (s.expiration_ts <= self.t0 or rec.avail_ns >= self.t1):
                continue
            out.append(s)
        return out

    def availability(self) -> dict[str, int]:
        return {t: r.avail_ns for t, r in self.markets.items() if r.spec is not None}

    def settlement_values(self) -> dict[str, float]:
        """{ticker: YES payout in dollars} for markets with a recorded result."""
        return {t: r.settle_px / PX_SCALE for t, r in self.markets.items() if r.settle_px is not None}

    def series_fee_timeline(self, series_ticker: str) -> list[dict[str, Any]]:
        """Fee schedule of a series over time: one row per recorded series snapshot and per
        scheduled change ({'effective_ns', 'fee_type', 'fee_multiplier', 'source'}), ascending."""
        rows = [{"effective_ns": r, "fee_type": o.get("fee_type"), "fee_multiplier": o.get("fee_multiplier"),
                 "source": "series_snapshot"} for r, o in self.series.get(series_ticker, [])]
        for c in self.series_fee_changes:
            if str(c.get("series_ticker") or series_ticker) == series_ticker:
                rows.append({"effective_ns": opt_iso_to_ns(c.get("scheduled_ts")), "fee_type": c.get("fee_type"),
                             "fee_multiplier": c.get("fee_multiplier"), "source": "scheduled_change"})
        return sorted(rows, key=lambda r: r["effective_ns"])

    def fee_schedules(self, fee_engine: FeeEngine | None = None, at_ns: int | None = None) -> dict[str, FeeSchedule]:
        """{series: FeeSchedule} in force at ``at_ns`` (default t0), series-level (no event override)."""
        fe = fee_engine or FeeEngine.from_config()
        t = self.t0 if at_ns is None else at_ns
        return {s: fe.schedule_for(self.series_at(s, t)) for s in sorted(self.series)}

    def fee_table(self) -> pd.DataFrame:
        rows = []
        for t, rec in sorted(self.markets.items()):
            if rec.spec is None:
                continue
            rows.append({"series": rec.spec.series_ticker, "fee_type": rec.spec.fee_type,
                         "fee_multiplier": rec.spec.fee_multiplier})
        if not rows:
            return pd.DataFrame(columns=["series", "fee_type", "fee_multiplier", "markets"])
        df = pd.DataFrame(rows)
        return df.groupby(["series", "fee_type", "fee_multiplier"], as_index=False).size().rename(columns={"size": "markets"})

    def rejected(self) -> dict[str, str]:
        return {t: r.reject for t, r in self.markets.items() if r.spec is None}

    def expirations(self) -> list[int]:
        return sorted({r.spec.expiration_ts for r in self.markets.values() if r.spec is not None})


def restrict_universe(uni: Universe, spec_filter: Callable[[list[MarketSpec], Universe], list[MarketSpec]],
                      series: Iterable[str] | None = None, reason: str = "filtered") -> Universe:
    """Copy of ``uni`` in which only the markets kept by ``spec_filter`` have specs (research speed
    knob, e.g. NearestStrikes(15) for every experiment; the strategy then never sees the others)."""
    keep = {s.ticker for s in spec_filter(uni.specs(series), uni)}
    out = dataclasses.replace(uni, markets={}, notes=list(uni.notes) + [f"universe restricted: {reason}"])
    for t, r in uni.markets.items():
        if r.spec is not None and t not in keep:
            out.markets[t] = dataclasses.replace(r, spec=None, reject=reason)
        else:
            out.markets[t] = r
    return out


def _iter_json_records(root: str | Path, streams: Sequence[str], t0: int, t1: int) -> Iterator[tuple[int, str, Any]]:
    names = resolve_streams(root, streams)
    names = [s for s in names if s in set(list_streams(root))]
    if not names:
        return
    for rec in iter_raw(root, names, t0, t1):
        try:
            yield rec.t, rec.stream, orjson.loads(rec.data)
        except orjson.JSONDecodeError:
            continue


def _lifecycle_market(msg: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    """market_lifecycle_v2 message body -> (kind, market-shaped patch)."""
    et = str(msg.get("event_type") or "")
    patch: dict[str, Any] = {}
    if et == "created":
        am = msg.get("additional_metadata") if isinstance(msg.get("additional_metadata"), dict) else {}
        for k in ("event_ticker", "title", "yes_sub_title", "no_sub_title", "rules_primary", "rules_secondary",
                  "can_close_early", *_STRIKE_KEYS):
            if am.get(k) is not None:
                patch[k] = am[k]
        if msg.get("open_ts"):
            patch["open_time"] = _iso(msg["open_ts"])
        if msg.get("close_ts"):
            patch["close_time"] = _iso(msg["close_ts"])
        if am.get("expected_expiration_ts"):
            patch["expected_expiration_time"] = _iso(am["expected_expiration_ts"])
        if msg.get("price_ranges"):
            patch["price_ranges"] = msg["price_ranges"]
        if msg.get("price_level_structure"):
            patch["price_level_structure"] = msg["price_level_structure"]
        patch["status"] = "initialized"
        return "created", patch
    if et == "metadata_updated":
        src = msg.get("additional_metadata") if isinstance(msg.get("additional_metadata"), dict) else msg
        for k in (*_STRIKE_KEYS, "yes_sub_title", "rules_primary", "rules_secondary"):
            if k in src and src[k] is not None:
                patch[k] = src[k]
        return "patch", patch
    if et == "close_date_updated" and msg.get("close_ts"):
        return "patch", {"close_time": _iso(msg["close_ts"])}
    if et == "price_level_structure_updated" and msg.get("price_ranges"):
        return "patch", {"price_ranges": msg["price_ranges"], "price_level_structure": msg.get("price_level_structure", "")}
    if et in ("determined", "settled"):
        return "result", {k: msg.get(k) for k in ("result", "settlement_value", "determination_ts", "settled_ts")}
    return "", {}


def _merge(raw: dict[str, Any], patch: Mapping[str, Any]) -> None:
    """Update raw with patch; never erase a known strike with a null."""
    for k, v in patch.items():
        if v is None and k in _STRIKE_KEYS and raw.get(k) is not None:
            continue
        raw[k] = v


def _try_spec(rec: MarketRecord) -> MarketSpec | None:
    rec.raw.setdefault("ticker", rec.ticker)
    try:
        spec = rest_market_to_spec(rec.raw, None, None)
    except (UnsupportedMarket, KeyError, ValueError, TypeError) as exc:
        rec.reject = str(exc)[:200]
        return None
    return spec


def build_universe(root: str | Path, t0: int, t1: int, *, lookback_s: float = 6 * HOUR_S,
                   settle_grace_s: float = 2 * HOUR_S, own_fill_scan: bool = True,
                   state_warm_s: float = 900.0) -> Universe:
    """Rebuild MarketSpecs, fee schedules and settlements for [t0, t1) from recorded REST
    records and WS lifecycle messages in [t0 - lookback_s, t1 + settle_grace_s) (module doc)."""
    root = str(root)
    u = Universe(root=root, t0=t0, t1=t1)
    lo, hi = t0 - _ns(lookback_s), t1 + _ns(settle_grace_s)
    mk: dict[str, MarketRecord] = {}

    def market_obs(recv: int, m: Mapping[str, Any], kind: str = "rest") -> None:
        t = m.get("ticker") or m.get("market_ticker")
        if not t:
            return
        rec = mk.setdefault(str(t), MarketRecord(str(t)))
        rec.obs.append((recv, kind, dict(m)))

    # ---------------------------------------------------------------- REST metadata records
    seen_sfc: set[tuple[str, str]] = set()
    seen_efc: set[tuple[str, str]] = set()
    for recv, _stream, rec in _iter_json_records(root, META_STREAMS, lo, hi):
        if not isinstance(rec, dict) or str(rec.get("method", "GET")).upper() != "GET":
            continue
        if not 200 <= int(rec.get("status") or 0) < 300:
            continue
        body = rec.get("body")
        if not isinstance(body, dict):
            continue
        path = normalize_route(str(rec.get("path", "")))
        parts = [p for p in path.split("/") if p]
        if path == "/series/fee_changes":
            for c in body.get("series_fee_change_arr") or []:
                key = (str(c.get("series_ticker")), str(c.get("scheduled_ts")))
                if key not in seen_sfc:
                    seen_sfc.add(key)
                    u.series_fee_changes.append(dict(c))
        elif path == "/events/fee_changes":
            for c in body.get("event_fee_changes") or []:
                key = (str(c.get("event_ticker")), str(c.get("scheduled_ts")))
                if key not in seen_efc:
                    seen_efc.add(key)
                    u.event_fee_changes.append(dict(c))
        elif path == "/series" and isinstance(body.get("series"), list):
            for s in body["series"]:
                if isinstance(s, dict) and s.get("ticker"):
                    u.series.setdefault(str(s["ticker"]), []).append((recv, dict(s)))
        elif len(parts) == 2 and parts[0] == "series" and isinstance(body.get("series"), dict):
            s = body["series"]
            u.series.setdefault(str(s.get("ticker") or parts[1]), []).append((recv, dict(s)))
        elif path == "/events":
            for ev in body.get("events") or []:
                if not isinstance(ev, dict):
                    continue
                u.events.setdefault(str(ev.get("event_ticker")), []).append((recv, {k: v for k, v in ev.items() if k != "markets"}))
                for m in ev.get("markets") or []:
                    market_obs(recv, {**m, "event_ticker": m.get("event_ticker") or ev.get("event_ticker")})
        elif len(parts) == 2 and parts[0] == "events":
            ev = body.get("event")
            if isinstance(ev, dict):
                u.events.setdefault(str(ev.get("event_ticker") or parts[1]), []).append(
                    (recv, {k: v for k, v in ev.items() if k != "markets"}))
                for m in (body.get("markets") or []) + (ev.get("markets") or []):
                    market_obs(recv, {**m, "event_ticker": m.get("event_ticker") or ev.get("event_ticker")})
        elif path in ("/markets", "/historical/markets"):
            for m in body.get("markets") or []:
                if isinstance(m, dict):
                    market_obs(recv, m)
        elif (len(parts) == 2 and parts[0] == "markets" and parts[1] not in _MARKET_SUBRESOURCES) or (
                len(parts) == 3 and parts[:2] == ["historical", "markets"]):
            if isinstance(body.get("market"), dict):
                market_obs(recv, body["market"])

    # ---------------------------------------------------------------- WS lifecycle / fee updates
    ws = kalshi_ws_streams(root)
    if ws:
        for rec in iter_raw(root, ws, lo, hi):
            d = rec.data
            if b"market_lifecycle_v2" not in d and b"event_fee_update" not in d:
                continue
            try:
                msg = orjson.loads(d)
            except orjson.JSONDecodeError:
                continue
            typ = msg.get("type")
            body = msg.get("msg") if isinstance(msg.get("msg"), dict) else {}
            if typ == "market_lifecycle_v2" and body.get("market_ticker"):
                kind, patch = _lifecycle_market(body)
                if kind:
                    rec_m = mk.setdefault(str(body["market_ticker"]), MarketRecord(str(body["market_ticker"])))
                    rec_m.obs.append((rec.t, kind, patch))
            elif typ == "event_fee_update" and body.get("event_ticker"):
                for ev in ws_message_to_events(msg, rec.t):
                    if isinstance(ev, KalshiFeeUpdate):
                        u.ws_fee_updates.append((rec.t, ev.event_ticker, ev.fee_type_override, ev.fee_multiplier_override))

    # ---------------------------------------------------------------- merge observations
    for t, rec in mk.items():
        rec.obs.sort(key=lambda o: o[0])
        rec.first_seen_ns = rec.obs[0][0] if rec.obs else 0
        for recv, kind, payload in rec.obs:
            if kind == "result":
                res = str(payload.get("result") or "")
                if res in ("yes", "no") and not rec.result:
                    rec.result = res
                    sv = payload.get("settlement_value")
                    rec.settle_px = _px_or_none(sv) if sv not in (None, "") else (PX_SCALE if res == "yes" else 0)
                    rec.settled_ns = recv
                continue
            _merge(rec.raw, payload)
            if kind == "rest" and str(payload.get("result") or "") in ("yes", "no"):
                if not rec.result:
                    rec.result = str(payload["result"])
                    sv = payload.get("settlement_value_dollars")
                    rec.settle_px = _px_or_none(sv) if sv not in (None, "") else (PX_SCALE if rec.result == "yes" else 0)
                    rec.settled_ns = recv
                if payload.get("expiration_value") not in (None, ""):
                    try:
                        rec.expiration_value = float(payload["expiration_value"])
                    except (TypeError, ValueError):
                        pass
            if rec.spec is None:
                spec = _try_spec(rec)
                if spec is not None:
                    rec.spec = spec
                    rec.avail_ns = recv
                    rec.reject = ""
        rec.obs = []  # free memory; raw holds the merged state
    u.markets = mk

    # ---------------------------------------------------------------- fees (as of availability)
    for t, rec in mk.items():
        if rec.spec is None:
            continue
        t_ref = max(rec.avail_ns, t0)
        ftype, mult, src = u.fee_fields(t, t_ref)
        rec.spec = dataclasses.replace(rec.spec, fee_type=ftype, fee_multiplier=mult)
        if not ftype:
            rec.reject = "fee_unresolved (no series/event fee record): MarketMaker will not quote it"

    # ---------------------------------------------------------------- session metadata
    if "meta" in list_streams(root):
        for recv, _s, m in _iter_json_records(root, ["meta"], lo, t1):
            if isinstance(m, dict):
                u.meta.append(m)
                if m.get("synthetic"):
                    u.synthetic = True
    if own_fill_scan and ws:
        u.own_fills = prescan_own_fills(root, t0 - _ns(state_warm_s), t1)
    for c in u.fee_changes_in_window():
        u.notes.append(f"fee change inside the window (replay uses the fee in force at availability): {c}")
    return u


def _px_or_none(v: Any) -> int | None:
    try:
        return int(round(float(v) * PX_SCALE))
    except (TypeError, ValueError):
        return None


def prescan_own_fills(root: str | Path, t0: int, t1: int) -> dict[str, KalshiFill]:
    """Our private fills (WS `fill` channel) recorded in [t0, t1): trade_id -> KalshiFill."""
    out: dict[str, KalshiFill] = {}
    ws = kalshi_ws_streams(root)
    if not ws:
        return out
    for rec in iter_raw(root, ws, t0, t1):
        if b'"fill"' not in rec.data:
            continue
        try:
            msg = orjson.loads(rec.data)
        except orjson.JSONDecodeError:
            continue
        if msg.get("type") != "fill":
            continue
        try:
            for ev in ws_message_to_events(msg, rec.t):
                if isinstance(ev, KalshiFill) and ev.trade_id not in out:
                    out[ev.trade_id] = ev
        except (ValueError, KeyError, TypeError):
            continue
    return out


# ============================================================================ own footprint
Level = tuple[str, str, int]  # (ticker, 'yes'|'no', px on that book's scale)


class OwnFootprintFilter:
    """Removes our own live orders' footprint from recorded public Kalshi data (module doc).

    Call it on every normalized event in stream order; returns the events to replay (0..n).
    Downstream books contain other participants' orders only.
    """

    def __init__(self, own_fills: Mapping[str, KalshiFill] | None = None, match_window_ns: int = 2 * NS_PER_S) -> None:
        self.own_fills = dict(own_fills or {})
        self.window = int(match_window_ns)
        self.books: dict[str, KalshiBook] = {}
        self.own: dict[str, dict[tuple[str, int], int]] = {}  # ticker -> {(side, px): our qty in the recorded book}
        self.pend: dict[Level, deque[list[int]]] = {}  # our fill qty whose level decrease has not arrived
        self.recent_dec: dict[Level, deque[list[int]]] = {}  # unexplained decreases at levels holding our qty
        self.applied: set[str] = set()
        self.stats: Counter[str] = Counter()

    # ------------------------------------------------------------------ entry point
    def __call__(self, ev: Event) -> list[Event]:
        t = type(ev)
        if t is KalshiBookDelta:
            return self._delta(ev)  # type: ignore[arg-type]
        if t is KalshiBookSnapshot:
            return self._snapshot(ev)  # type: ignore[arg-type]
        if t is KalshiTrade:
            return self._trade(ev)  # type: ignore[arg-type]
        if isinstance(ev, PRIVATE_TYPES):
            self.stats[f"private_{t.__name__}"] += 1
            if t is KalshiFill:
                return self._private_fill(ev)  # type: ignore[arg-type]
            return []
        if t is FeedStatus and (ev.stream.startswith("kalshi.order_group:")  # type: ignore[union-attr]
                                or ev.stream in PRIVATE_CHANNEL_STREAMS):  # type: ignore[union-attr]
            self.stats["private_status"] += 1
            return []
        return [ev]

    # ------------------------------------------------------------------ helpers
    @staticmethod
    def _lvl_qty(b: KalshiBook, side: str, px: int) -> int:
        return (b.yes_bids if side == "yes" else b.no_bids).get(px, 0)

    def _own_level(self, fill: KalshiFill) -> tuple[str, int]:
        return ("yes", fill.yes_px) if fill.book_side == "bid" else ("no", PX_SCALE - fill.yes_px)

    def _correction(self, ts: int, ticker: str, side: str, px: int, d: int) -> KalshiBookDelta:
        return KalshiBookDelta(ts=ts, ts_exch=0, ticker=ticker, sid=0, seq=0, side=side, px=px, delta=d)  # type: ignore[arg-type]

    def _expire(self, dq: deque[list[int]], now: int) -> None:
        while dq and now - dq[0][0] > self.window:
            dq.popleft()

    # ------------------------------------------------------------------ book events
    def _snapshot(self, ev: KalshiBookSnapshot) -> list[Event]:
        b = self.books.get(ev.ticker)
        if b is None:
            b = self.books[ev.ticker] = KalshiBook(ev.ticker)
        b.apply_snapshot(ev)
        for lvl in [k for k in self.pend if k[0] == ev.ticker]:
            del self.pend[lvl]
        for lvl in [k for k in self.recent_dec if k[0] == ev.ticker]:
            del self.recent_dec[lvl]
        own = self.own.get(ev.ticker)
        if not own:
            return [ev]
        for key in list(own):
            q = self._lvl_qty(b, key[0], key[1])
            own[key] = min(max(own[key], 0), q)
            if own[key] == 0:
                del own[key]
        if not own:
            return [ev]
        self.stats["snapshots_adjusted"] += 1
        yes = tuple((p, q - own.get(("yes", p), 0)) for p, q in ev.yes_bids if q - own.get(("yes", p), 0) > 0)
        no = tuple((p, q - own.get(("no", p), 0)) for p, q in ev.no_bids if q - own.get(("no", p), 0) > 0)
        return [dataclasses.replace(ev, yes_bids=yes, no_bids=no)]

    def _delta(self, ev: KalshiBookDelta) -> list[Event]:
        b = self.books.get(ev.ticker)
        is_own = bool(ev.own_client_order_id)
        own = self.own.get(ev.ticker)
        if b is None or not b.valid:
            if b is not None:
                b.apply_delta(ev)
            if is_own:
                self.stats["own_deltas_dropped_no_book"] += 1
                return []
            return [ev]
        key = (ev.side, ev.px)
        o_old = own.get(key, 0) if own else 0
        r_old = self._lvl_qty(b, ev.side, ev.px)
        oth_old = max(0, r_old - o_old)
        if not b.apply_delta(ev):
            self.own.pop(ev.ticker, None)
            return [ev]
        r_new = self._lvl_qty(b, ev.side, ev.px)
        lvl: Level = (ev.ticker, ev.side, ev.px)
        if is_own:
            self.stats["own_deltas"] += 1
            o_new = o_old + ev.delta
            if o_new < 0:
                self.stats["taker_ghost_qty"] += -min(0, o_new) - (-min(0, o_old))
        else:
            o_new = o_old
            if ev.delta < 0 and o_old > 0:
                dq = self.pend.get(lvl)
                if dq:
                    self._expire(dq, ev.ts)
                    need = min(-ev.delta, o_old)
                    while dq and need > 0:
                        use = min(dq[0][1], need)
                        dq[0][1] -= use
                        need -= use
                        o_new -= use
                        self.stats["absorbed_qty"] += use
                        if dq[0][1] <= 0:
                            dq.popleft()
                    if not dq:
                        self.pend.pop(lvl, None)
                unexplained = -ev.delta - (o_old - o_new)
                if unexplained > 0 and o_new > 0:
                    self.recent_dec.setdefault(lvl, deque()).append([ev.ts, unexplained])
        if o_new > r_new:
            self.stats["clamped_qty"] += o_new - r_new
            o_new = r_new
        if r_new == 0 and o_new < 0:
            o_new = 0
        if own is None:
            own = self.own.setdefault(ev.ticker, {})
        if o_new:
            own[key] = o_new
        else:
            own.pop(key, None)
        oth_new = max(0, r_new - o_new)
        d = oth_new - oth_old
        if d == 0:
            return []
        return [dataclasses.replace(ev, delta=d, own_client_order_id="")]

    # ------------------------------------------------------------------ trades / fills
    def _own_match(self, ts: int, fill: KalshiFill, qty: int) -> list[Event]:
        """Our maker fill of qty at its resting level: retroactively absorb an earlier unexplained
        decrease, else register the qty as pending for the coming decrease."""
        side, px = self._own_level(fill)
        lvl: Level = (fill.ticker, side, px)
        out: list[Event] = []
        b = self.books.get(fill.ticker)
        own = self.own.get(fill.ticker, {})
        dq = self.recent_dec.get(lvl)
        left = qty
        if dq and b is not None and b.valid:
            self._expire(dq, ts)
            took = 0
            while dq and left > 0:
                use = min(dq[0][1], left)
                dq[0][1] -= use
                left -= use
                took += use
                if dq[0][1] <= 0:
                    dq.popleft()
            if took:
                r = self._lvl_qty(b, side, px)
                o_old = own.get((side, px), 0)
                o_new = max(0, o_old - took)
                oth_old, oth_new = max(0, r - o_old), max(0, r - o_new)
                if o_new:
                    own[(side, px)] = o_new
                else:
                    own.pop((side, px), None)
                self.stats["retro_absorbed_qty"] += took
                if oth_new != oth_old:
                    out.append(self._correction(ts, fill.ticker, side, px, oth_new - oth_old))
        if left > 0:
            self.pend.setdefault(lvl, deque()).append([ts, left])
        return out

    def _trade(self, ev: KalshiTrade) -> list[Event]:
        f = self.own_fills.get(ev.trade_id)
        if f is None:
            return [ev]
        ours = min(f.qty, ev.qty)
        out: list[Event] = []
        if ev.trade_id not in self.applied:
            self.applied.add(ev.trade_id)
            if not f.is_taker:
                out += self._own_match(ev.ts, f, ours)
        self.stats["own_print_qty_removed"] += ours
        left = ev.qty - ours
        if left > 0:
            out.append(dataclasses.replace(ev, qty=left))
        return out

    def _private_fill(self, ev: KalshiFill) -> list[Event]:
        if ev.trade_id in self.applied or ev.is_taker:
            return []
        self.applied.add(ev.trade_id)
        return self._own_match(ev.ts, ev, ev.qty)


# ============================================================================ event stream
class ReplayStream:
    """Normalized, own-footprint-filtered events for [t0, t1) with primed state at t0 (module doc).

    Re-iterable (each iteration re-reads the store). ``pre_brti`` holds the BRTI ticks received
    in [t0 - brti_prime_s, t0) after an iteration (for the settlement tracker)."""

    def __init__(self, root: str | Path, t0: int, t1: int, *, streams: Sequence[str] | None = None,
                 state_warm_s: float = 900.0, tail_s: float = 300.0, own_fills: Mapping[str, KalshiFill] | None = None,
                 own_filter: bool = True, kalshi_use_yes_price: bool = False, brti_prime_s: float = 120.0) -> None:
        self.root = str(root)
        self.t0, self.t1 = int(t0), int(t1)
        self.streams = list(streams) if streams is not None else default_streams(root)
        self.state_warm_ns = _ns(state_warm_s)
        self.tail_ns = _ns(tail_s)
        self.own_fills = dict(own_fills or {})
        self.own_filter = own_filter
        self.kalshi_use_yes_price = kalshi_use_yes_price
        self.brti_prime_ns = _ns(brti_prime_s)
        self.read_stats = ReadStats()
        self.filter: OwnFootprintFilter | None = None
        self.pre_brti: list[IndexTick] = []
        self.counts: Counter[str] = Counter()

    def __iter__(self) -> Iterator[Event]:
        norm = Normalizers(kalshi_use_yes_price=self.kalshi_use_yes_price)
        filt = OwnFootprintFilter(self.own_fills) if self.own_filter else None
        self.filter = filt
        self.read_stats = ReadStats()
        self.pre_brti = []
        self.counts = Counter()
        tracker = BookTracker()
        kalshi_up: bool | None = None
        bad_books: set[str] = set()
        primed = False
        t0, t1 = self.t0, self.t1
        for rec in iter_raw(self.root, self.streams, t0 - self.state_warm_ns, t1 + self.tail_ns, self.read_stats):
            evs = norm(rec)
            if filt is not None and evs:
                evs = [x for e in evs for x in filt(e)]
            if rec.t < t0:
                for e in evs:
                    tracker.on_event(e)
                    if isinstance(e, FeedStatus):
                        if e.stream == "kalshi.ws":
                            if e.status in ("connected", "resynced", "resumed"):
                                kalshi_up = True
                            elif e.status in ("disconnected", "stale", "gap"):
                                kalshi_up = False
                        elif e.stream.startswith("kalshi.book:"):
                            tk = e.stream.split(":", 1)[1]
                            if e.status in ("gap", "disconnected", "stale", "error"):
                                bad_books.add(tk)
                            elif e.status in ("resynced", "connected", "resumed"):
                                bad_books.discard(tk)
                    elif isinstance(e, KalshiBookSnapshot):
                        bad_books.discard(e.ticker)
                    elif isinstance(e, IndexTick) and e.index_id == "BRTI" and rec.t >= t0 - self.brti_prime_ns:
                        self.pre_brti.append(e)
                continue
            if not primed:
                primed = True
                yield from self._prime(tracker, kalshi_up, bad_books)
            if rec.t >= t1:
                for e in evs:
                    if isinstance(e, Settlement) or (isinstance(e, KalshiMarketLifecycle)
                                                     and e.event_type in ("determined", "settled")):
                        self.counts["tail_settlements"] += 1
                        yield e
                continue
            self.counts["events"] += len(evs)
            yield from evs
        if not primed:
            yield from self._prime(tracker, kalshi_up, bad_books)

    def _prime(self, tracker: BookTracker, kalshi_up: bool | None, bad_books: set[str]) -> Iterator[Event]:
        t0 = self.t0
        if kalshi_up or (kalshi_up is None and tracker.kalshi):
            yield FeedStatus(ts=t0, ts_exch=0, stream="kalshi.ws", status="connected", detail="replay priming")
        for ev in tracker.state_events(t0):
            if isinstance(ev, KalshiBookSnapshot) and ev.ticker in bad_books:
                continue
            self.counts["primed"] += 1
            yield ev
        for tk in sorted(bad_books):
            yield FeedStatus(ts=t0, ts_exch=0, stream=f"kalshi.book:{tk}", status="gap", detail="replay priming")


@dataclass
class Recording:
    """A recording window: rebuilt universe, specs, per-series fee schedules and a LAZY event
    iterator (re-reads the store on every call; nothing is materialized)."""

    universe: Universe
    specs: list[MarketSpec]
    fee_schedules: dict[str, FeeSchedule]
    stream_kw: dict[str, Any] = field(default_factory=dict)

    def events(self) -> Iterator[Event]:
        """Own-footprint-filtered, primed replay events for [t0, t1) (ReplayStream)."""
        u = self.universe
        return iter(ReplayStream(u.root, u.t0, u.t1, own_fills=u.own_fills, **self.stream_kw))

    def raw_events(self, streams: Sequence[str] | None = None, warmup_s: float = 900.0) -> Iterator[Event]:
        """Plain dh.store.replay.iter_events over the market-data streams (no own-order filtering,
        no connection priming) for tools that want the recording exactly as captured."""
        from dh.store.replay import iter_events

        u = self.universe
        return iter_events(u.root, list(streams) if streams is not None else default_streams(u.root), u.t0, u.t1,
                           warmup_ns=_ns(warmup_s))


def open_recording(root: str | Path, t0: int, t1: int, *, series: Iterable[str] | None = None,
                   fee_engine: FeeEngine | None = None, **stream_kw: Any) -> Recording:
    """build_universe + specs (optionally restricted to series) + per-series fee schedules at t0."""
    u = build_universe(root, t0, t1)
    return Recording(u, u.specs(series), u.fee_schedules(fee_engine), dict(stream_kw))


# ============================================================================ fitted inputs (look-ahead checks)
def resolve_fv_config(fv_config: str | Path | Mapping[str, Any] | None = None) -> tuple[dict[str, Any], str]:
    """(config dict, source label) of the fair-value parameters (None = committed recommended config)."""
    if fv_config is None:
        return load_recommended_config(), "fv_recommended.json"
    if isinstance(fv_config, Mapping):
        return dict(fv_config), str(fv_config.get("source") or "given config")
    return load_recommended_config(fv_config), Path(fv_config).name


def _iso_ns(t: int | None) -> str:
    from dh.research.exp_common import fmt_ns

    return fmt_ns(t) if t is not None else "?"


def fv_params_status(cfg: Mapping[str, Any], source: str, t0: int, synthetic: bool) -> tuple[dict[str, Any], str | None]:
    """Is the replay window out of the FV parameters' fitting sample? -> (summary fields, warning).
    In sample = the fit used data at or after t0 (``data_end_utc`` > t0): look-ahead parameters."""
    end = cfg.get("data_end_utc")
    end_ns = int(end) * NS_PER_S if end is not None else None
    if synthetic:
        status, ins = "n/a (synthetic recording)", None
    elif end_ns is None:
        status, ins = "UNKNOWN fit window (no data_end_utc): treat as in-sample", True
    elif t0 < end_ns:
        status, ins = f"IN-SAMPLE (fitted on data through {_iso_ns(end_ns)}, after t0)", True
    else:
        status, ins = f"out-of-sample (fitted on data through {_iso_ns(end_ns)}, before t0)", False
    info = {"fv_config": source, "fv_params_data_end": _iso_ns(end_ns) if end_ns else None,
            "fv_params_in_sample": ins, "fv_params": f"{source}: {status}"}
    warn = None
    if ins:
        warn = (f"in-sample FV: the fair-value parameters ({source}) were fitted on data through {_iso_ns(end_ns)}, "
                f"not before this replay's t0 {_iso_ns(t0)}; fair values, markouts and P&L use look-ahead parameters. "
                "Refit walk-forward on data strictly before t0 (dh/research/fv_study) and pass --fv-config, or "
                "report the result as in-sample FV")
    return info, warn


def flow_status(seg: Mapping[Any, Any] | None, meta: Mapping[str, Any], source: str, t0: int,
                synthetic: bool = False) -> tuple[dict[str, Any], str | None]:
    """Same check for taker-flow segments (``meta.fit_end_ms``: every training datum precedes it)."""
    if not seg:
        return {"flow_segments": "config defaults (cfg.fill)", "flow_in_sample": None}, None
    end_ms = meta.get("fit_end_ms")
    end_ns = int(end_ms) * 1_000_000 if end_ms is not None else None
    ins = True if end_ns is None else t0 < end_ns
    info = {"flow_segments": f"{source or 'given'} ({len(seg)} segments, fit through {_iso_ns(end_ns)})",
            "flow_in_sample": ins}
    warn = None
    if ins:
        warn = (f"in-sample flow: the taker-flow segments ({source or 'given'}) were fitted on data through "
                f"{_iso_ns(end_ns)}, not before this replay's t0 {_iso_ns(t0)}; fit them strictly before t0 "
                "(run_experiment.py flow --t1 <replay t0>)")
    return info, warn


def fv_label_for(uni: Universe, t0: int) -> tuple[dict[str, Any], str | None]:
    """fv_params_status of the FV parameters bound to ``uni`` (default: the committed config)."""
    cfg, src = (uni.fv_config, uni.fv_config_source) if uni.fv_config else resolve_fv_config(None)
    return fv_params_status(cfg, src, t0, uni.synthetic)


def inputs_meta(uni: Universe, t0: int) -> tuple[dict[str, Any], list[str]]:
    """Report metadata + warnings for the fitted inputs of a window's replays (FV parameters and
    taker-flow segments; in-sample = fitted on data not strictly before t0)."""
    fv_info, fv_warn = fv_label_for(uni, t0)
    fl_info, fl_warn = flow_status(uni.flow_segments, uni.flow_meta, uni.flow_source, t0, uni.synthetic)
    return ({"FV parameters": fv_info["fv_params"], "taker flow": fl_info["flow_segments"]},
            [w for w in (fv_warn, fl_warn) if w])


def bind_replay_inputs(uni: Universe, *, fv_config: str | Path | Mapping[str, Any] | None = None,
                       flow_segments: str | Path | Mapping[Any, Any] | None = None) -> Universe:
    """Attach fitted replay inputs to a universe (every replay/probe of that universe uses them)."""
    if fv_config is not None:
        uni.fv_config, uni.fv_config_source = resolve_fv_config(fv_config)
    if flow_segments is not None:
        if isinstance(flow_segments, Mapping):
            uni.flow_segments, uni.flow_meta, uni.flow_source = dict(flow_segments), {}, "given"
        else:
            from dh.research.calibrate_flow import load_segments

            seg, meta = load_segments(flow_segments)
            uni.flow_segments, uni.flow_meta, uni.flow_source = seg, meta, Path(flow_segments).name
    return uni


# ============================================================================ fair-value warm-up
def brti_ticks(root: str | Path, t0: int, t1: int, *, include_rest: bool = True,
               cache: dict[Any, Any] | None = None) -> list[IndexTick]:
    """BRTI IndexTicks RECEIVED in [t0, t1) from kalshi.ws cfbenchmarks frames (prefiltered,
    stateless normalization), CF-history REST records and events.md.* caches, sorted by
    (source time, receive time); duplicates (same feed class and source time) removed.
    ``cache`` (e.g. Universe.cache) memoizes the scan: a long warm-up window is read once per
    universe instead of once per replay."""
    key = ("brti", str(root), int(t0), int(t1), include_rest)
    if cache is not None and key in cache:
        return cache[key]
    out = _brti_scan(root, t0, t1, include_rest)
    if cache is not None:
        cache[key] = out
    return out


def _brti_scan(root: str | Path, t0: int, t1: int, include_rest: bool) -> list[IndexTick]:
    avail = set(list_streams(root))
    names = [s for s in kalshi_ws_streams(root)]
    if include_rest and CF_REST_STREAM in avail:
        names.append(CF_REST_STREAM)
    names += sorted(s for s in avail if s.startswith(MD_CACHE_PREFIX))
    out: list[IndexTick] = []
    for rec in iter_raw(root, names, t0, t1) if names else ():
        d = rec.data
        if rec.stream.startswith("kalshi.ws"):
            if b"cfbenchmarks_value" not in d:
                continue
            try:
                evs = ws_message_to_events(orjson.loads(d), rec.t)
            except (orjson.JSONDecodeError, ValueError, KeyError, TypeError):
                continue
        elif rec.stream == CF_REST_STREAM:
            try:
                r = orjson.loads(d)
            except orjson.JSONDecodeError:
                continue
            if not (isinstance(r, dict) and 200 <= int(r.get("status") or 0) < 300):
                continue
            params = r.get("params") or {}
            evs = cf_history_to_ticks(r.get("body"), rec.t, index_id=str(params.get("id") or "BRTI"))
        else:
            if b"IndexTick" not in d:
                continue
            try:
                evs = [decode_event(d)]
            except Exception:  # noqa: BLE001
                continue
        for e in evs:
            if isinstance(e, IndexTick) and e.index_id == "BRTI" and math.isfinite(e.value):
                out.append(e)
    seen: set[tuple[str, int]] = set()
    uniq = []
    for e in sorted(out, key=lambda e: (e.ts_exch or e.ts, e.ts)):
        key = ("1" if e.feed in ("1hz", "rest") else "5", e.ts_exch or e.ts)
        if key in seen:
            continue
        seen.add(key)
        uniq.append(e)
    return uniq


def load_price_file(path: str | Path) -> pd.DataFrame:
    """CSV/Parquet of prices -> DataFrame(ts_ns, price). Columns: ts_ms|ts_ns|timestamp(s) and
    price|close|value."""
    p = Path(path)
    df = pd.read_parquet(p) if p.suffix in (".parquet", ".pq") else pd.read_csv(p)
    if "ts_ns" in df:
        ts = df["ts_ns"].astype("int64")
    elif "ts_ms" in df:
        ts = df["ts_ms"].astype("int64") * 1_000_000
    elif "timestamp" in df:
        ts = (df["timestamp"].astype("float64") * NS_PER_S).astype("int64")
    else:
        raise KeyError(f"{p}: need ts_ns, ts_ms or timestamp column")
    col = next((c for c in ("price", "close", "value") if c in df), None)
    if col is None:
        raise KeyError(f"{p}: need price, close or value column")
    return pd.DataFrame({"ts_ns": ts.to_numpy(), "price": df[col].astype(float).to_numpy()}).sort_values("ts_ns")


@dataclass
class WarmInfo:
    source: str
    n_ticks: int
    ready: bool
    last_value: float | None
    synthetic_fallback: bool = False


def parse_warm(warm: str | None) -> list[str]:
    """'recorded' | 'recorded+gbm' | 'csv:/p/x.csv,recorded' | 'gbm' | 'none' -> tokens."""
    s = (warm or "none").replace("+gbm", ",gbm").replace("+recorded", ",recorded").replace("+csv:", ",csv:")
    toks = [t.strip() for t in s.split(",") if t.strip()]
    for t in toks:
        if t not in ("recorded", "gbm", "none") and not t.startswith("csv:"):
            raise ValueError(f"unknown warm-up source {t!r} (recorded | gbm | csv:<path> | none)")
    return toks


def warm_fair_value(fv: FairValueModel, root: str | Path, t0: int, warm: str = "recorded", *,
                    fv_warm_s: float = 1.5 * DAY_S, gbm_vol_ann: float = 0.35, seed: int = 0,
                    cache: dict[Any, Any] | None = None, fv_config: Mapping[str, Any] | None = None) -> WarmInfo:
    """Warm the vol EWMAs with prices received strictly before t0.

    Tokens (comma/plus separated): 'recorded' = BRTI ticks in [t0 - fv_warm_s, t0) (WS 1 Hz/5 Hz
    and CF-history REST records); 'csv:<path>' = price file (dh.research.replay_env.load_price_file);
    'gbm' = seeded GBM history (vol gbm_vol_ann) ending at the first known price, used only if the
    other sources left the model not ready -- SYNTHETIC, flagged in WarmInfo/summary.
    """
    toks = parse_warm(warm)
    src: list[str] = []
    prices: list[tuple[int, float]] = []
    t_from = t0 - _ns(fv_warm_s)
    for tok in toks:
        if tok.startswith("csv:"):
            df = load_price_file(tok[4:])
            df = df[(df.ts_ns >= t_from) & (df.ts_ns < t0)]
            prices += [(int(a), float(b)) for a, b in zip(df.ts_ns.to_numpy(), df.price.to_numpy())]
            src.append(f"csv:{Path(tok[4:]).name}")
        elif tok == "recorded":
            ticks = [e for e in brti_ticks(root, t_from, t0, cache=cache) if e.ts < t0 and e.feed in ("1hz", "5hz", "rest")]
            prices += [(e.ts_exch or e.ts, e.value) for e in ticks]
            src.append("recorded_brti")
    prices.sort()
    for ts, v in prices:
        fv.update(ts, v)
    last = prices[-1][1] if prices else None
    fallback = False
    if "gbm" in toks and not fv.ready:
        from dh.backtest.kat import warm_fv_model

        end_ns, S_end = (prices[0][0] - 60 * NS_PER_S, prices[0][1]) if prices else (t0, None)
        if S_end is None:
            nxt = brti_ticks(root, t0, t0 + _ns(900))
            S_end = nxt[0].value if nxt else None
        if S_end is not None:
            fv.vol = FairValueModel.from_config(dict(fv_config) if fv_config else load_recommended_config()).vol  # fresh
            warm_fv_model(fv, end_ns, S_end, gbm_vol_ann, seed=seed)
            for ts, v in prices:
                fv.update(ts, v)
            fallback = True
            src.append(f"gbm_vol{gbm_vol_ann:g}_SYNTHETIC")
            last = last if last is not None else S_end
    return WarmInfo("+".join(src) or "none", len(prices), fv.ready, last, fallback)


# ============================================================================ simulator
class TickerFeeExchangeSim(KalshiExchangeSim):
    """KalshiExchangeSim with per-ticker fee schedules.

    Fee model: when the simulator supports order-aware fees (``order_fee_fn``), every order gets a
    ``dh.kalshi.fees.OrderFeeAccumulator`` of its market's schedule, i.e. the exact Kalshi
    per-order balance rounding with carried rebates ('per_order_rounding'); otherwise the per-fill
    trade fee ceiled to micro-dollars ('per_fill_trade_fee'). The base class does not pass the
    ticker to ``fee_fn`` (proposed upstream change), so the fallback path records it in ``_fill``."""

    def __init__(self, latency: LatencyModel, fill_policy: str, schedules: Mapping[str, FeeSchedule], seed: int = 0,
                 *, order_rounding: bool = True, **kw: Any) -> None:
        import inspect

        self.schedules = dict(schedules)
        self._fee_ticker = ""
        self._accs: dict[str, OrderFeeAccumulator] = {}
        self.fee_model = "per_fill_trade_fee"
        if order_rounding and "order_fee_fn" in inspect.signature(KalshiExchangeSim.__init__).parameters:
            kw["order_fee_fn"] = self._order_fee
            self.fee_model = "per_order_rounding"
        super().__init__(latency, fill_policy, self._fee, seed, **kw)

    def add_schedule(self, ticker: str, sched: FeeSchedule) -> None:
        self.schedules[ticker] = sched

    def _schedule(self, ticker: str) -> FeeSchedule:
        s = self.schedules.get(ticker)
        if s is None or not s.supported:
            raise RuntimeError(f"no supported fee schedule for {ticker!r}")
        return s

    def _fee(self, px: int, qty: int, is_taker: bool) -> int:
        return self._schedule(self._fee_ticker).trade_fee_micros(px, qty, is_taker)

    def _order_fee(self, order_key: str, book_side: str, px: int, qty: int, is_taker: bool) -> int:
        acc = self._accs.get(order_key)
        if acc is None:
            acc = self._accs[order_key] = OrderFeeAccumulator(self._schedule(self.orders[order_key].ticker), book_side)
        return acc.apply_fill(px, qty, is_taker).net_micros

    def _fill(self, o, qty, px, is_taker, t, mech):  # type: ignore[no-untyped-def]
        self._fee_ticker = o.ticker
        return super()._fill(o, qty, px, is_taker, t, mech)


# ============================================================================ availability
class ReplayFeed:
    """ReplayStream + market availability: specs available at t0 go to the strategy's
    constructor; later ones are announced through ``on_add`` at their availability time (at the
    earliest ``expiration - add_horizon_s``) followed by a snapshot of their replayed book."""

    def __init__(self, stream: ReplayStream, universe: Universe, specs: Sequence[MarketSpec], *,
                 add_horizon_s: float | None = None, prune_every_s: float = HOUR_S) -> None:
        self.stream = stream
        self.universe = universe
        avail = universe.availability()
        t0 = stream.t0
        hz = None if add_horizon_s is None else _ns(add_horizon_s)
        self.initial: list[MarketSpec] = []
        pend: list[tuple[int, str, MarketSpec]] = []
        for s in specs:
            a = max(avail.get(s.ticker, t0), t0)
            if hz is not None:
                a = max(a, s.expiration_ts - hz)
            if a >= stream.t1 or s.expiration_ts <= t0:
                continue
            if a <= t0:
                self.initial.append(s)
            else:
                pend.append((a, s.ticker, s))
        pend.sort(key=lambda x: (x[0], x[1]))
        self.pending = [(a, s) for a, _, s in pend]
        self.prune_ns = _ns(prune_every_s)
        self.added_late = 0

    def events(self, on_add: Callable[[list[MarketSpec]], None] | None = None,
               on_prune: Callable[[int], None] | None = None) -> Iterator[Event]:
        pending = self.pending
        i = 0
        books: dict[str, KalshiBook] = {s.ticker: KalshiBook(s.ticker) for _, s in pending}
        next_prune = self.stream.t0 + self.prune_ns
        self.added_late = 0
        for ev in self.stream:
            ts = ev.ts
            if i < len(pending) and pending[i][0] <= ts:
                batch = []
                while i < len(pending) and pending[i][0] <= ts:
                    batch.append(pending[i][1])
                    i += 1
                if on_add is not None:
                    on_add(batch)
                self.added_late += len(batch)
                for s in batch:
                    b = books.pop(s.ticker, None)
                    if b is not None and b.valid:
                        yield KalshiBookSnapshot(ts=ts, ts_exch=0, ticker=s.ticker, sid=b.sid, seq=b.seq,
                                                 yes_bids=tuple(b.yes_bids.items()), no_bids=tuple(b.no_bids.items()))
            if on_prune is not None and ts >= next_prune:
                on_prune(ts)
                next_prune = ts + self.prune_ns
            if books:
                if type(ev) is KalshiBookDelta:
                    b = books.get(ev.ticker)  # type: ignore[union-attr]
                    if b is not None:
                        b.apply_delta(ev)  # type: ignore[arg-type]
                elif type(ev) is KalshiBookSnapshot:
                    b = books.get(ev.ticker)  # type: ignore[union-attr]
                    if b is not None:
                        b.apply_snapshot(ev)  # type: ignore[arg-type]
            yield ev


# ============================================================================ driver
def drive(events: Iterable[Event], strategy: Any, sim: Any | None, *, timer_period_ns: int, end_ns: int | None,
          on_event: Callable[[Event], None] | None = None,
          on_action: Callable[[int, Any], None] | None = None) -> Counter[str]:
    """dh.backtest.runner.run's exact protocol without retaining actions/logs in memory (long
    replays). tests/research/test_replay_env.py checks equality with runner.run."""
    sims = [sim] if sim is not None else []
    counts: Counter[str] = Counter()

    def handle(ev: Event) -> None:
        counts["events_seen"] += 1
        if on_event is not None:
            on_event(ev)
        for a in strategy.on_event(ev) or ():
            if on_action is not None:
                on_action(ev.ts, a)
            if isinstance(a, Log):
                counts["logs"] += 1
                continue
            counts["actions"] += 1
            if isinstance(a, Halt):
                counts["halts"] += 1
            for s in sims:
                s.submit(a, ev.ts)

    def drain(until: int) -> None:
        while True:
            best_t, best = None, None
            for s in sims:
                t = s.next_due_ns()
                if t is not None and t <= until and (best_t is None or t < best_t):
                    best_t, best = t, s
            if best is None:
                return
            for out in best.pop_due(best_t):
                counts["sim_events"] += 1
                handle(out)

    for ev in with_timers(events, timer_period_ns, end_ns):
        if end_ns is not None and ev.ts > end_ns:
            break
        if type(ev) is Timer:
            counts["timers"] += 1
        drain(ev.ts - 1)
        due: list[Event] = []
        for s in sims:
            due.extend(s.on_market_event(ev))
        handle(ev)
        due.sort(key=lambda e: e.ts)
        for out in due:
            counts["sim_events"] += 1
            handle(out)
        drain(ev.ts)
    drain(END_OF_TIME_NS if end_ns is None else end_ns)
    return counts


# ============================================================================ collectors
class FillCapture:
    """Per-fill strategy state captured just BEFORE the strategy processes each simulated fill:
    client order id, queue estimate, order age, fair value band and z of the last quote cycle."""

    def __init__(self, mm: MarketMaker) -> None:
        self.mm = mm
        self.rows: list[dict[str, Any]] = []
        self.public_qty: Counter[str] = Counter()  # public traded qty (0.01 ct) per ticker
        self.quotes: dict[str, tuple[str, float, float]] = {}  # coid -> (position, q_eff, modeled value)
        self._last_place = ""

    def on_action(self, ts: int, a: Any) -> None:
        # a Log('quote') follows the PlaceOrder it describes (MarketMaker._admit)
        t = type(a)
        if t is PlaceOrder:
            self._last_place = a.client_order_id
        elif t is Log and a.kind == "quote" and self._last_place:
            p = a.payload
            self.quotes[self._last_place] = (str(p.get("position", "")), float(p.get("q_eff", math.nan)),
                                             float(p.get("value", math.nan)))
            self._last_place = ""

    def on_event(self, ev: Event) -> None:
        t = type(ev)
        if t is KalshiTrade:
            self.public_qty[ev.ticker] += ev.qty  # type: ignore[union-attr]
        elif t is KalshiFill:
            mm = self.mm
            w = mm.om.order(ev.client_order_id)  # type: ignore[union-attr]
            fv = mm.fvc.get(ev.ticker)  # type: ignore[union-attr]
            qa = mm.queue.queue_ahead(ev.client_order_id)  # type: ignore[union-attr]
            self.rows.append({
                "coid": ev.client_order_id, "order_id": ev.order_id, "trade_id": ev.trade_id,  # type: ignore[union-attr]
                "queue_ahead_ct": (qa / QTY_SCALE) if qa is not None else math.nan,
                "order_age_s": (ev.ts - w.created_ns) / NS_PER_S if w is not None else math.nan,
                "pos_before_ct": mm.om.position(ev.ticker) / QTY_SCALE,  # type: ignore[union-attr]
                "F_cycle": fv.F if fv is not None else math.nan,
                "F_lo": fv.F_lo if fv is not None else math.nan,
                "F_hi": fv.F_hi if fv is not None else math.nan,
                "delta_btc": fv.delta if fv is not None else math.nan,
                "z": fv.z if fv is not None else math.nan,
                "fv_age_s": (ev.ts - fv.ts) / NS_PER_S if fv is not None else math.nan,
            })


class PortfolioSampler:
    """Every `period_s` of event time: signed portfolio delta D (BTC), gross delta sum|q d|,
    total |position|, and collateral (cost of open positions: long YES pays px, short YES pays 1-px)."""

    def __init__(self, mm: MarketMaker, period_s: float = 1.0) -> None:
        self.mm = mm
        self.period = _ns(period_s)
        self.next_t = 0
        self.rows: list[tuple[int, float, float, float, float, float]] = []

    def on_event(self, ev: Event) -> None:
        if type(ev) is not Timer or ev.ts < self.next_t:
            return
        self.next_t = ev.ts + self.period
        mm = self.mm
        D = G = pos_abs = coll = net_pos = 0.0
        for t in list(mm.specs):
            if t in mm.settled:
                continue
            q = mm.om.position(t) / QTY_SCALE
            if not q:
                continue
            f = mm.fvc.get(t)
            d = f.delta if f is not None else 0.0
            D += q * d
            G += abs(q * d)
            pos_abs += abs(q)
            net_pos += q
            cost = -mm.om.cash_micros(t) / 1e6  # money paid (>0 long YES); short YES: proceeds (<0)
            coll += cost if q > 0 else (abs(q) - (-cost))  # long: paid; short: (1 - px) * |q|
        self.rows.append((ev.ts, D, G, pos_abs, coll, net_pos))

    def result(self) -> pd.DataFrame:
        return pd.DataFrame(self.rows, columns=["ts", "delta_btc", "gross_delta_btc", "abs_position_ct", "collateral_usd",
                                                "net_position_ct"])

    frame = result


# ============================================================================ replay
@dataclass
class ReplayResult:
    """df: ledger attribution (one row per simulated fill, ledger columns + FillCapture columns +
    simulator mechanism); summary: dict (ledger summary + run metadata)."""

    df: pd.DataFrame
    summary: dict[str, Any]
    universe: Universe | None = None
    extras: dict[str, Any] = field(default_factory=dict)
    mm: Any = None
    sim: Any = None
    ledger: Any = None


def default_latency() -> LatencyModel:
    """Placeholder latency (dh.execution.latency.LatencyModel defaults); replace with measured."""
    return LatencyModel(0)


def run_replay(root: str | Path, t0: int, t1: int, cfg: StrategyConfig | None = None, policy: str = "realistic",
               latency: LatencyModel | None = None, warm: str = "recorded", *, seed: int = 1,
               universe: Universe | None = None, spec_filter: Callable[[list[MarketSpec], Universe], list[MarketSpec]] | None = None,
               strategy_factory: Callable[..., MarketMaker] | None = None, factory_kwargs: dict[str, Any] | None = None,
               collectors: Sequence[Callable[[MarketMaker], Any]] = (),
               action_hooks: Sequence[Callable[[int, Any], None]] = (), streams: Sequence[str] | None = None,
               state_warm_s: float = 900.0, tail_s: float = 300.0, own_filter: bool = True,
               fv_warm_s: float = 1.5 * DAY_S, gbm_vol_ann: float = 0.35, add_horizon_s: float | None = None,
               keep_objects: bool = False, fee_engine: FeeEngine | None = None, n_boot: int = 500,
               postprocess: Callable[[ReplayResult], None] | None = None,
               fv_config: str | Path | Mapping[str, Any] | None = None,
               flow_segments: str | Path | Mapping[Any, Any] | None = None) -> ReplayResult:
    """Replay MarketMaker + KalshiExchangeSim (+ Ledger) over the recording in [t0, t1).

    policy: fill policy A/B/C (optimistic/realistic/conservative); latency: LatencyModel
    (default placeholders; policy C scales it by 1.5); warm: fair-value warm-up source.
    spec_filter(specs, universe) -> specs restricts the quoted markets (e.g. Experiment 9);
    strategy_factory(cfg, specs, **kw) builds a MarketMaker subclass (research hooks);
    collectors: callables mm -> object with on_event(ev) (and optionally on_action(ts, action));
    their ``result()`` values are returned in ``extras['collectors']``.
    postprocess(result) runs while result.mm/sim/ledger are still attached (use it to derive
    columns from the ledger inside a worker process); they are detached unless keep_objects.
    fv_config / flow_segments override the inputs bound to the universe (bind_replay_inputs);
    both are checked for look-ahead against t0 (summary fv_params / flow_segments + warnings).
    Returns ReplayResult(df, summary, ...) (see class doc).
    """
    cfg = cfg or StrategyConfig()
    letter = policy_letter(policy)
    uni = universe if universe is not None else build_universe(root, t0, t1, state_warm_s=state_warm_s)
    if universe is not None and (uni.t0 > t0 or uni.t1 < t1):
        uni.notes.append("universe window narrower than the replay window")
    specs = uni.specs(cfg.quoting.enabled_series)
    specs = [s for s in specs if s.expiration_ts > t0]
    if spec_filter is not None:
        specs = list(spec_filter(specs, uni))
    fee_engine = fee_engine or FeeEngine.from_config()
    if fv_config is not None or flow_segments is not None:
        uni = bind_replay_inputs(dataclasses.replace(uni), fv_config=fv_config, flow_segments=flow_segments)
    fv_cfg = uni.fv_config or resolve_fv_config(None)[0]
    fv_info, fv_warn = fv_label_for(uni, t0)
    flow_info, flow_warn = flow_status(uni.flow_segments, uni.flow_meta, uni.flow_source, t0, uni.synthetic)
    fv = FairValueModel.from_config(fv_cfg)
    winfo = warm_fair_value(fv, root, t0, warm, fv_warm_s=fv_warm_s, gbm_vol_ann=gbm_vol_ann, seed=seed,
                            cache=uni.cache, fv_config=fv_cfg)
    if uni.flow_segments:
        factory_kwargs = {"flow_segments": dict(uni.flow_segments), **(factory_kwargs or {})}
    stream = ReplayStream(root, t0, t1, streams=streams, state_warm_s=state_warm_s, tail_s=tail_s,
                          own_fills=uni.own_fills, own_filter=own_filter)
    horizon = add_horizon_s if add_horizon_s is not None else cfg.quoting.max_tau_s + 300.0
    feed = ReplayFeed(stream, uni, specs, add_horizon_s=horizon)
    factory = strategy_factory or MarketMaker
    mm = factory(cfg, feed.initial, fv_model=fv, fee_engine=fee_engine, **(factory_kwargs or {}))
    can_add = hasattr(mm, "add_markets")
    if not can_add:  # older MarketMaker: every spec up front (books arrive via synthesized snapshots)
        mm = factory(cfg, feed.initial + [s for _, s in feed.pending], fv_model=fv, fee_engine=fee_engine,
                     **(factory_kwargs or {}))
    scheds = {s.ticker: fee_engine.schedule_for_spec(s.fee_type, s.fee_multiplier) for s in specs}
    sim = TickerFeeExchangeSim(latency or default_latency(), POLICY_FULL[letter], scheds, seed=seed)
    for s in feed.initial:
        sim.register_market(s)
    all_specs = {r.ticker: r.spec for r in uni.markets.values() if r.spec is not None}
    ledger = Ledger({t: s.event_ticker for t, s in all_specs.items()}, {t: s.expiration_ts for t, s in all_specs.items()})
    cap = FillCapture(mm)
    extra = [c(mm) for c in collectors]

    def on_add(batch: list[MarketSpec]) -> None:
        if can_add:
            mm.add_markets(batch)
        for s in batch:
            sim.register_market(s)

    def on_prune(ts: int) -> None:
        if hasattr(mm, "prune_settled"):
            mm.prune_settled(ts - _ns(2 * HOUR_S))

    def on_event(ev: Event) -> None:
        ledger.on_event(ev)
        cap.on_event(ev)
        for c in extra:
            c.on_event(ev)

    act_collectors = [c for c in extra if hasattr(c, "on_action")]

    def on_action(ts: int, a: Any) -> None:
        if isinstance(a, Log):
            ledger.on_log(ts, a)
        cap.on_action(ts, a)
        for c in act_collectors:
            c.on_action(ts, a)
        for h in action_hooks:
            h(ts, a)

    it = feed.events(on_add=on_add, on_prune=on_prune)
    # prime the settlement tracker with the BRTI prints received just before t0 (window state);
    # the stream fills stream.pre_brti on its first records, so defer priming to the first event
    def primed(events: Iterator[Event]) -> Iterator[Event]:
        first = True
        for ev in events:
            if first:
                first = False
                for tick in stream.pre_brti:
                    mm.tracker.on_index(tick)
            yield ev

    wall0 = time.perf_counter()
    counts = drive(primed(it), mm, sim, timer_period_ns=cfg.timers.quote_period_ms * 1_000_000,
                   end_ns=t1 + _ns(tail_s), on_event=on_event, on_action=on_action)
    wall = time.perf_counter() - wall0
    # settlements missing from the stream: recorded REST/lifecycle results (after-the-fact only)
    settle = uni.settlement_values()
    filled_from_rest = 0
    for f in ledger.fills:
        if f.ticker not in ledger.settle and f.ticker in settle:
            ledger.settle[f.ticker] = settle[f.ticker]
            filled_from_rest += 1
    df = ledger.attribute()
    if len(df):
        meta = pd.DataFrame(cap.rows)
        if len(meta) == len(df):
            df = pd.concat([df.reset_index(drop=True), meta.reset_index(drop=True)], axis=1)
        mech = {r.trade_id: r.mechanism for r in sim.fill_log}
        if "trade_id" in df:
            df["mechanism"] = df["trade_id"].map(mech)
        if "coid" in df:
            q = cap.quotes
            df["quote_position"] = df["coid"].map(lambda c: q.get(c, ("", math.nan, math.nan))[0])
            df["q_eff_at_place_ct"] = df["coid"].map(lambda c: q.get(c, ("", math.nan, math.nan))[1])
            df["modeled_value_c"] = 100.0 * df["coid"].map(lambda c: q.get(c, ("", math.nan, math.nan))[2])
        df["policy"] = letter
        df["yes_px_c"] = 100.0 * df["px"]
    days = max((t1 - t0) / (DAY_S * NS_PER_S), 1e-9)
    if len(df):
        summary = ledger.summary(df, n_boot=n_boot)
        done = df[df["settle"].notna()]
    else:
        summary = {"fills": 0, "settled_fills": 0, "contracts": 0.0, "net_usd": 0.0, **{
            f"hedge_{k}" if not k.startswith("hedge") else k: v for k, v in ledger.hedge_totals.items()}}
        done = df
    summary.update({
        "policy": letter, "t0": t0, "t1": t1, "days": days,
        "fills_per_day": len(done) / days if len(df) else 0.0,
        "contracts_per_day": float(done["contracts"].sum()) / days if len(df) else 0.0,
        "net_usd_per_day": float(done["net"].sum()) / days if len(df) else 0.0,
        "unsettled_fills": int(len(df) - len(done)) if len(df) else 0,
        "settlements_from_rest": filled_from_rest,
        "fv_warm": winfo.source, "fv_warm_ticks": winfo.n_ticks, "fv_ready_at_t0": winfo.ready,
        "fv_warm_synthetic_fallback": winfo.synthetic_fallback,
        "n_specs": len(specs), "n_initial": len(feed.initial), "n_added_late": feed.added_late,
        "n_rejected_markets": len(uni.rejected()), "synthetic": uni.synthetic,
        "own_filter": dict(stream.filter.stats) if stream.filter is not None else {}, "fee_model": sim.fee_model,
        "stream_events": stream.counts.get("events", 0), "truncated_files": len(stream.read_stats.truncated_files),
        "mm_cycles": mm.stats.cycles, "mm_quotes_placed": mm.stats.quotes_placed, "mm_cancels": mm.stats.cancels,
        "mm_top_reasons": dict(sorted(mm.stats.reasons.items(), key=lambda kv: -kv[1])[:8]),
        "sim_stats": dict(sim.stats), "public_contracts": sum(cap.public_qty.values()) / QTY_SCALE,
        "public_contracts_quoted": sum(q for t, q in cap.public_qty.items() if t in {s.ticker for s in specs}) / QTY_SCALE,
        "wall_s": wall, "drive_counts": dict(counts), "universe_notes": list(uni.notes),
        "quote_hours": quote_hours(sim, t1 + _ns(tail_s)), "orders": len(sim.orders),
        **fv_info, **flow_info,
    })
    summary["net_usd_per_quote_hour"] = (float(done["net"].sum()) / summary["quote_hours"]
                                         if len(df) and summary["quote_hours"] > 0 else 0.0)
    warns = []
    if not winfo.ready:
        warns.append("fair-value model NOT warm at t0 (the strategy does not quote until its EWMAs are warm): "
                     "start t0 later, record CF history before t0, or use warm='recorded+gbm' / 'csv:<prices>'")
    if winfo.synthetic_fallback:
        warns.append("fair-value warm-up used a SYNTHETIC GBM history (vol %g)" % gbm_vol_ann)
    if not specs:
        warns.append("no tradable market specs in the window (check series/enabled_series/fees)")
    warns += [w for w in (fv_warn, flow_warn) if w]
    summary["warnings"] = warns
    extras: dict[str, Any] = {"collectors": [c.result() if hasattr(c, "result") else c for c in extra]}
    res = ReplayResult(df, summary, uni, extras, mm, sim, ledger)
    if postprocess is not None:
        postprocess(res)
    if keep_objects:
        extras["collector_objects"] = extra
        extras["capture"] = cap
    else:
        res.mm = res.sim = res.ledger = None
    return res


def quote_hours(sim: KalshiExchangeSim, end_ns: int) -> float:
    """Sum over our simulated orders of time resting on the book (hours)."""
    tot = 0
    for o in sim.orders.values():
        end = o.done_ns if o.done_ns else end_ns
        tot += max(0, end - o.created_ns)
    return tot / (HOUR_S * NS_PER_S)


POLICY_FULL = {"A": "optimistic", "B": "realistic", "C": "conservative"}


# ============================================================================ spec filters
@dataclass(frozen=True)
class NearestStrikes:
    """Keep the n strikes of each event nearest to the benchmark at the event's first quotable
    time (max(availability, expiration - horizon_s)); n <= 0 keeps all. Causal: uses the last
    BRTI tick received before that time."""

    n: int
    horizon_s: float = 3900.0

    def __call__(self, specs: list[MarketSpec], uni: Universe) -> list[MarketSpec]:
        if self.n <= 0:
            return specs
        ticks = brti_ticks(uni.root, uni.t0 - _ns(2 * HOUR_S), uni.t1, cache=uni.cache)
        ts = np.array([e.ts for e in ticks], dtype=np.int64)
        vals = np.array([e.value for e in ticks], dtype=float)
        order = np.argsort(ts, kind="stable")
        ts, vals = ts[order], vals[order]
        avail = uni.availability()
        by_event: dict[str, list[MarketSpec]] = defaultdict(list)
        for s in specs:
            by_event[s.event_ticker].append(s)
        keep: list[MarketSpec] = []
        for ev in sorted(by_event):
            group = by_event[ev]
            t_ref = max(max(avail.get(s.ticker, uni.t0) for s in group), min(s.expiration_ts for s in group) - _ns(self.horizon_s),
                        uni.t0)
            i = int(np.searchsorted(ts, t_ref, side="right")) - 1
            S = float(vals[i]) if i >= 0 else (float(vals[0]) if len(vals) else math.nan)

            def dist(s: MarketSpec) -> float:
                k = [x for x in (s.floor_strike, s.cap_strike) if x is not None]
                return abs(float(np.mean(k)) - S) if k and math.isfinite(S) else math.inf

            keep += sorted(group, key=lambda s: (dist(s), s.ticker))[: self.n]
        return sorted(keep, key=lambda s: s.ticker)


# ============================================================================ fair-value probe
class FvProbe:
    """The MarketMaker's own fair-value band without quoting (research: E3 live fills, E8).

    Feed it the replayed market events (Timers are ignored, actions discarded; no quote cycle
    ever runs, so no orders exist). ``fair(ticker, now)`` returns the MarketFV the strategy would
    compute at ``now`` (same nowcast, vol, tails, settlement window and band)."""

    def __init__(self, cfg: StrategyConfig, specs: Sequence[MarketSpec], fv_model: FairValueModel,
                 fee_engine: FeeEngine | None = None) -> None:
        self.mm = MarketMaker(cfg, specs, fv_model=fv_model, fee_engine=fee_engine or FeeEngine.from_config(),
                              use_order_group=False)

    def add(self, specs: Sequence[MarketSpec]) -> None:
        if hasattr(self.mm, "add_markets"):
            self.mm.add_markets(specs)

    def on_event(self, ev: Event) -> None:
        if type(ev) is not Timer:
            self.mm.on_event(ev)

    def fair(self, ticker: str, now: int):  # -> MarketFV | None
        mm = self.mm
        spec = mm.specs.get(ticker)
        if spec is None or not mm.fv.ready:
            return None
        S, ns_sd = mm._nowcast(now)
        if S is None:
            return None
        ws = mm.tracker.window_state(spec.settlement, spec.expiration_ts, now)
        return mm._band(spec, ws, S, now, ns_sd)

    def spot(self) -> float | None:
        return self.mm._spot()


def probe_for_window(root: str | Path, t0: int, t1: int, cfg: StrategyConfig, universe: Universe, *,
                     warm: str = "recorded", specs: Sequence[MarketSpec] | None = None, seed: int = 0,
                     state_warm_s: float = 900.0, add_horizon_s: float | None = None,
                     own_filter: bool = True) -> tuple[FvProbe, ReplayFeed, WarmInfo]:
    """(probe, feed, warm info) wired like run_replay: iterate ``feed.events(on_add=probe.add)``
    and call ``probe.on_event`` on every event."""
    specs = list(specs if specs is not None else universe.specs(cfg.quoting.enabled_series))
    fv_cfg = universe.fv_config or load_recommended_config()
    fv = FairValueModel.from_config(fv_cfg)
    winfo = warm_fair_value(fv, root, t0, warm, seed=seed, cache=universe.cache, fv_config=fv_cfg)
    stream = ReplayStream(root, t0, t1, state_warm_s=state_warm_s, own_fills=universe.own_fills, own_filter=own_filter)
    feed = ReplayFeed(stream, universe, specs, add_horizon_s=add_horizon_s if add_horizon_s is not None
                      else cfg.quoting.max_tau_s + 300.0)
    probe = FvProbe(cfg, feed.initial, fv)
    return probe, feed, winfo


def prime_probe(probe: FvProbe, stream: ReplayStream) -> None:
    for tick in stream.pre_brti:
        probe.mm.tracker.on_index(tick)


__all__ = [
    "Universe", "MarketRecord", "build_universe", "prescan_own_fills", "OwnFootprintFilter", "ReplayStream",
    "ReplayFeed", "brti_ticks", "warm_fair_value", "TickerFeeExchangeSim", "drive", "run_replay", "ReplayResult",
    "NearestStrikes", "FvProbe", "probe_for_window", "prime_probe", "FillCapture", "PortfolioSampler", "restrict_universe",
    "default_streams", "load_price_file", "Recording", "open_recording", "bind_replay_inputs", "resolve_fv_config",
    "fv_params_status", "fv_label_for", "flow_status", "inputs_meta",
]
