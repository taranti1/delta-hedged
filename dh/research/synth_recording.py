"""Synthetic recording in the collector's on-disk formats: SYNTHETIC DATA, pipeline validation only.

    info = write_synthetic_recording("data/synth", SynthRecordingConfig(n_events=4))
    run_replay(info.root, info.t0, info.t1, cfg)            # the real replay path, end to end

What is written (dh.store.recorder layout; the same readers as for live captures):

  kalshi.ws            RAW Kalshi WebSocket frames exactly as the live client records them:
                       orderbook_snapshot / orderbook_delta (sid 1), trade (sid 2),
                       cfbenchmarks_value (1 Hz, sid 3), cfbenchmarks_value_5hz (sid 4),
                       market_lifecycle_v2 created / metadata_updated / determined (sid 5), plus
                       the client's synthetic 'connected' status frame. Per-sid seq numbers are
                       contiguous, so dh.kalshi.sequencer validates them like live data.
  kalshi.rest.*        RAW KalshiRest records {method, path, params, status, body}:
                       GET /markets (listing, strike null when strike_delay_s > 0),
                       GET /events/{e}, GET /series/{s}, GET /series/fee_changes,
                       GET /events/fee_changes, GET /markets/{t} after settlement (result,
                       expiration_value), and GET /cfbenchmarks/history/values with a seeded
                       1-minute BRTI history (fair-value warm-up).
  events.md.ext        external venues as a NORMALIZED-EVENT CACHE (dh.store.codec ExtBBO /
                       ExtTrade), replayed by dh.store.replay's codec path. Raw venue frames are
                       venue-specific (Coinbase/Kraken/Bitstamp protocols); fidelity there adds
                       nothing to the experiments, which consume normalized venue books.
  meta                 session record with "synthetic": true and the injected parameters; every
                       report built on this root carries the SYNTHETIC banner.

Market data come from dh.sim.synthetic.SyntheticMarket, one instance per event (expiries every
``event_spacing_s``), chained so the BTC path is continuous. Injected effects (known answers):
background makers price off BTC lagged by ``mm_lag_s`` (Kalshi staleness, E1/E8), latency takers
pick off stale quotes (informed flow, E3), the benchmark is published ``brti_delay_ms`` after
the venues move (nowcastable, E2), and an optional favorite-longshot bias.
"""

from __future__ import annotations

import dataclasses
import math
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import numpy as np
import orjson

from dh.core.events import (
    ExtBBO,
    ExtTrade,
    FeedStatus,
    IndexTick,
    KalshiBookDelta,
    KalshiBookSnapshot,
    KalshiMarketLifecycle,
    KalshiTrade,
    Settlement,
)
from dh.core.units import NS_PER_MS, NS_PER_S, PX_SCALE, px_to_dollars, qty_to_fp
from dh.kalshi.sequencer import synthetic_status_frame
from dh.sim.synthetic import SynthConfig, SyntheticMarket
from dh.store.codec import encode_event
from dh.store.recorder import Recorder

SEC_YR = 365.0 * 24 * 3600
_MONTHS = ("JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC")
RULES = ("If the simple average of the sixty seconds of CF Benchmarks' Bitcoin Real-Time Index (BRTI) before "
         "{when} is above {strike}, then the market resolves to Yes. SYNTHETIC MARKET.")
EXT_STREAM = "events.md.ext"


@dataclass(frozen=True)
class SynthRecordingConfig:
    start_ns: int = 1_790_251_200 * NS_PER_S  # 2026-09-24T12:00:00Z (window start)
    n_events: int = 4
    event_spacing_s: int = 900  # one event per spacing; each simulated over its whole spacing
    series: str = "KXBTCD"
    fee_type: str = "quadratic_with_maker_fees"
    fee_multiplier: int = 1
    strike_delay_s: float = 0.0  # >0: markets listed without strike; metadata_updated this long after open
    warm_history_days: float = 2.0  # CF-history REST record (1-minute BRTI) before start
    synth: SynthConfig = field(default_factory=lambda: SynthConfig(n_strikes_each_side=4, mm_lag_s=1.5, informed=True,
                                                                     informed_edge_ticks=1.0, vol_ann=0.6))
    seed: int = 7


@dataclass
class SynthRecordingInfo:
    root: str
    t0: int
    t1: int
    series: str
    expirations: list[int]
    tickers: dict[str, list[str]]  # event ticker -> market tickers
    settlement_values: dict[str, float]  # event ticker -> settlement average
    n_records: dict[str, int]
    config: dict[str, Any]


def event_ticker(series: str, T_ns: int) -> str:
    tm = time.gmtime(T_ns // NS_PER_S)
    return f"{series}-{tm.tm_year % 100:02d}{_MONTHS[tm.tm_mon - 1]}{tm.tm_mday:02d}{tm.tm_hour:02d}{tm.tm_min:02d}"


def _iso(ns: int) -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ns // NS_PER_S))


def _levels(levels: tuple[tuple[int, int], ...]) -> list[list[str]]:
    return [[px_to_dollars(p), qty_to_fp(q)] for p, q in levels]


def _market_dict(ticker: str, event: str, K: float, open_ns: int, T_ns: int, with_strike: bool,
                 status: str = "active") -> dict[str, Any]:
    return {
        "ticker": ticker, "event_ticker": event, "market_type": "binary",
        "title": f"SYNTHETIC Bitcoin price at {_iso(T_ns)}?", "yes_sub_title": f"${K:,.2f} or above",
        "open_time": _iso(open_ns), "close_time": _iso(T_ns), "expected_expiration_time": _iso(T_ns),
        "latest_expiration_time": _iso(T_ns + 7 * 86400 * NS_PER_S), "status": status,
        "price_level_structure": "linear_cent", "price_ranges": [{"start": "0.0000", "end": "1.0000", "step": "0.0100"}],
        "rules_primary": RULES.format(when=_iso(T_ns), strike=f"{K:.2f}"), "rules_secondary": "",
        "strike_type": "greater", "floor_strike": K if with_strike else None, "result": "",
    }


def _rest(path: str, params: dict[str, Any], body: Any) -> bytes:
    return orjson.dumps({"method": "GET", "path": path, "params": params, "status": 200, "body": body})


def _warm_history(start_ns: int, S_end: float, vol_ann: float, days: float, seed: int) -> list[dict[str, Any]]:
    rng = np.random.default_rng(seed)
    n = int(days * 1440)
    sig = vol_ann * math.sqrt(60.0 / SEC_YR)
    steps = rng.standard_normal(n) * sig
    logp = math.log(S_end) - np.concatenate([[0.0], np.cumsum(steps[::-1])])[::-1][:-1]
    t_end = (start_ns // (60 * NS_PER_S)) * 60 * NS_PER_S - 60 * NS_PER_S
    return [{"time": int((t_end - (n - 1 - i) * 60 * NS_PER_S) // NS_PER_MS), "value": f"{math.exp(lp):.2f}"}
            for i, lp in enumerate(logp)]


def write_synthetic_recording(root: str | Path, cfg: SynthRecordingConfig = SynthRecordingConfig(), *,
                              overwrite: bool = False) -> SynthRecordingInfo:
    """Generate and write the recording (see module docstring). Deterministic for a config.

    Refuses to write into a root that already holds raw data (the recorder never overwrites
    segments; a second run would add duplicate '.pN' parts) unless ``overwrite=True``, which
    deletes ``<root>/raw`` first."""
    root = Path(root)
    raw = root / "raw"
    if raw.exists() and any(raw.iterdir()):
        if not overwrite:
            raise FileExistsError(f"{raw} is not empty (pass overwrite=True to replace it)")
        import shutil

        shutil.rmtree(raw)
    spacing = int(cfg.event_spacing_s) * NS_PER_S
    recs: list[tuple[int, int, int, str, bytes]] = []  # (t, stream rank, gen order, stream, raw)
    order = 0
    rank = {"meta": 0, "kalshi.rest.series": 1, "kalshi.rest.fees": 2, "kalshi.rest.cfbenchmarks": 3,
            "kalshi.rest.markets": 4, "kalshi.rest.events": 5, "kalshi.ws": 6, EXT_STREAM: 7, "kalshi.rest.market": 8}

    def put(t: int, stream: str, raw: bytes) -> None:
        nonlocal order
        recs.append((int(t), rank[stream], order, stream, raw))
        order += 1

    ws: list[tuple[int, int, int, dict[str, Any]]] = []  # (t, gen order, sid, frame without seq)

    def ws_put(t: int, sid: int, frame: dict[str, Any]) -> None:
        nonlocal order
        ws.append((int(t), order, sid, frame))
        order += 1

    base = cfg.synth
    series = cfg.series
    start = cfg.start_ns
    injected = {k: getattr(base, k) for k in ("mm_lag_s", "informed", "informed_edge_ticks", "vol_ann", "longshot_bias",
                                              "brti_delay_ms", "venue_delay_ms", "mm_update_prob", "noise_taker_rate_per_s",
                                              "n_strikes_each_side", "strike_step", "S0")}
    meta = {"kind": "session_start", "synthetic": True, "generator": "dh.research.synth_recording",
            "note": "SYNTHETIC DATA - pipeline validation only", "injected": injected,
            "recording": {"n_events": cfg.n_events, "event_spacing_s": cfg.event_spacing_s, "series": series,
                          "fee_type": cfg.fee_type, "strike_delay_s": cfg.strike_delay_s, "seed": cfg.seed}}
    pre = start - 90 * NS_PER_S  # collector start-up records precede the first listing (start - 60 s)
    put(pre, "meta", orjson.dumps(meta))
    put(pre + NS_PER_S, "kalshi.rest.series", _rest(f"/series/{series}", {}, {"series": {
        "ticker": series, "frequency": "custom", "title": "SYNTHETIC Bitcoin price above/below", "category": "Crypto",
        "fee_type": cfg.fee_type, "fee_multiplier": cfg.fee_multiplier, "settlement_sources": [
            {"name": "CF Benchmarks", "url": "https://www.cfbenchmarks.com/data/indices/BRTI"}]}}))
    put(pre + NS_PER_S, "kalshi.rest.fees", _rest("/series/fee_changes", {"series_ticker": series,
                                                                                "show_historical": "false"},
                                                         {"series_fee_change_arr": []}))
    put(pre + NS_PER_S, "kalshi.rest.fees", _rest("/events/fee_changes", {}, {"event_fee_changes": [], "cursor": ""}))
    ws_put(pre + 3 * NS_PER_S, 0, {"_status": "connected"})

    hist = _warm_history(start, float(base.S0), base.vol_ann, cfg.warm_history_days, cfg.seed)
    put(pre + 2 * NS_PER_S, "kalshi.rest.cfbenchmarks", _rest("/cfbenchmarks/history/values",
                                                                {"id": "BRTI", "timespan": "2d"}, {"payload": hist}))
    counts: dict[str, int] = {}
    seqs: dict[int, int] = {}
    fixed_clock = start
    rec = Recorder(root, start=False, clock_ns=lambda: fixed_clock, flush_interval_s=3600.0)

    def flush_until(limit: int | None) -> None:
        """Write every pending record received before `limit` (None = all): per-sid contiguous
        WS sequence numbers in receive order (stable within a timestamp), then all streams in
        (t, stream rank, generation order). Later events never produce records before `limit`."""
        nonlocal ws, recs
        ws.sort(key=lambda x: (x[0], x[1]))
        cut = len(ws) if limit is None else next((i for i, x in enumerate(ws) if x[0] >= limit), len(ws))
        for t, g, sid, frame in ws[:cut]:
            if sid == 0:
                recs.append((t, rank["kalshi.ws"], g, "kalshi.ws", synthetic_status_frame("connected", "synthetic recording")))
                continue
            seqs[sid] = seqs.get(sid, 0) + 1
            recs.append((t, rank["kalshi.ws"], g, "kalshi.ws",
                         orjson.dumps({"type": frame["type"], "sid": sid, "seq": seqs[sid], "msg": frame["msg"]})))
        ws = ws[cut:]
        recs.sort(key=lambda r: (r[0], r[1], r[2]))
        cut = len(recs) if limit is None else next((i for i, r in enumerate(recs) if r[0] >= limit), len(recs))
        for t, _r, _g, stream, raw_b in recs[:cut]:
            rec.write(stream, t, raw_b)
            counts[stream] = counts.get(stream, 0) + 1
        recs = recs[cut:]
        rec.flush()

    info_tickers: dict[str, list[str]] = {}
    settle_vals: dict[str, float] = {}
    exps: list[int] = []
    try:
        _events(cfg, base, series, start, spacing, put, ws_put, flush_until, exps, info_tickers, settle_vals)
        flush_until(None)
    finally:
        rec.close()
    t1 = exps[-1]
    return SynthRecordingInfo(str(root), start, t1, series, exps, info_tickers, settle_vals, counts,
                              {"injected": injected, **meta["recording"]})


def _events(cfg: SynthRecordingConfig, base: SynthConfig, series: str, start: int, spacing: int, put, ws_put,
            flush_until, exps: list[int], info_tickers: dict[str, list[str]], settle_vals: dict[str, float]) -> None:
    """Generate the events one SyntheticMarket at a time (chained BTC path) and hand their
    records to put/ws_put, flushing everything that precedes the next event's listing."""
    S_prev = base.S0
    prev_T = None
    for k in range(cfg.n_events):
        T = start + (k + 1) * spacing
        exps.append(T)
        sc = dataclasses.replace(base, seed=cfg.seed * 1000 + k, expiration_ns=T, duration_s=float(cfg.event_spacing_s),
                                 S0=S_prev)
        sm = SyntheticMarket(sc)
        evs = sm.generate()
        S_prev = sm.true_price[-1][1]
        ev_t = event_ticker(series, T)
        name = {s.ticker: f"{ev_t}-T{s.floor_strike:.2f}".rstrip("0").rstrip(".") for s in sm.specs()}
        info_tickers[ev_t] = sorted(name.values())
        settle_vals[ev_t] = float(sm.settlement_value)
        open_ns = T - spacing
        list_ns = open_ns - 60 * NS_PER_S
        with_strike = cfg.strike_delay_s <= 0
        mkts = [_market_dict(name[s.ticker], ev_t, float(s.floor_strike), open_ns, T, with_strike, "initialized")
                for s in sm.specs()]
        put(list_ns, "kalshi.rest.markets", _rest("/markets", {"series_ticker": series, "status": "open", "limit": "1000"},
                                                  {"markets": mkts, "cursor": ""}))
        put(list_ns, "kalshi.rest.events", _rest(f"/events/{ev_t}", {}, {
            "event": {"event_ticker": ev_t, "series_ticker": series, "title": f"SYNTHETIC BTC at {_iso(T)}",
                      "strike_date": _iso(T), "mutually_exclusive": False}, "markets": mkts}))
        for s in sm.specs():
            am = {"event_ticker": ev_t, "title": f"SYNTHETIC Bitcoin price at {_iso(T)}?",
                  "rules_primary": RULES.format(when=_iso(T), strike=f"{s.floor_strike:.2f}"),
                  "expected_expiration_ts": T // NS_PER_S, "strike_type": "greater"}
            if with_strike:
                am["floor_strike"] = float(s.floor_strike)
            ws_put(list_ns + NS_PER_S, 5, {"type": "market_lifecycle_v2", "msg": {
                "market_ticker": name[s.ticker], "event_type": "created", "open_ts": open_ns // NS_PER_S,
                "close_ts": T // NS_PER_S, "additional_metadata": am, "price_level_structure": "linear_cent",
                "price_ranges": [{"start": "0.0000", "end": "1.0000", "step": "0.0100"}]}})
            if not with_strike:
                ws_put(open_ns + int(cfg.strike_delay_s * NS_PER_S), 5, {"type": "market_lifecycle_v2", "msg": {
                    "market_ticker": name[s.ticker], "event_type": "metadata_updated", "strike_type": "greater",
                    "floor_strike": float(s.floor_strike)}})
        for e in evs:
            if isinstance(e, FeedStatus):
                continue
            if prev_T is not None and isinstance(e, (IndexTick, ExtBBO, ExtTrade)) and e.ts_exch <= prev_T:
                continue  # boundary print already emitted by the previous event's generator
            if isinstance(e, KalshiBookSnapshot):
                ws_put(e.ts, 1, {"type": "orderbook_snapshot", "msg": {
                    "market_ticker": name[e.ticker], "market_id": f"syn-{name[e.ticker]}",
                    "yes_dollars_fp": _levels(e.yes_bids), "no_dollars_fp": _levels(e.no_bids)}})
            elif isinstance(e, KalshiBookDelta):
                ws_put(e.ts, 1, {"type": "orderbook_delta", "msg": {
                    "market_ticker": name[e.ticker], "market_id": f"syn-{name[e.ticker]}",
                    "price_dollars": px_to_dollars(e.px), "delta_fp": qty_to_fp(e.delta), "side": e.side,
                    "ts_ms": e.ts_exch // NS_PER_MS}})
            elif isinstance(e, KalshiTrade):
                ws_put(e.ts, 2, {"type": "trade", "msg": {
                    "trade_id": f"{ev_t}-{e.trade_id}", "market_ticker": name[e.ticker],
                    "yes_price_dollars": px_to_dollars(e.yes_px), "no_price_dollars": px_to_dollars(PX_SCALE - e.yes_px),
                    "count_fp": qty_to_fp(e.qty), "taker_side": e.taker_side, "taker_outcome_side": e.taker_side,
                    "taker_book_side": "bid" if e.taker_side == "yes" else "ask", "is_block_trade": False,
                    "ts": e.ts_exch // NS_PER_S, "ts_ms": e.ts_exch // NS_PER_MS}})
            elif isinstance(e, IndexTick):
                src_ms = e.ts_exch // NS_PER_MS
                if e.feed == "1hz":
                    data = orjson.dumps({"type": "value", "id": "BRTI", "time": src_ms, "value": f"{e.value:.8f}"}).decode()
                    ws_put(e.ts, 3, {"type": "cfbenchmarks_value", "msg": {
                        "index_id": "BRTI", "received_at": e.ts // NS_PER_MS - 5, "data": data}})
                else:
                    ws_put(e.ts, 4, {"type": "cfbenchmarks_value_5hz", "msg": {
                        "index_id": "BRTI", "value_usd": f"{e.value:.8f}", "source_ts_ms": src_ms,
                        "received_at": e.ts // NS_PER_MS - 5}})
            elif isinstance(e, KalshiMarketLifecycle):
                ws_put(e.ts, 5, {"type": "market_lifecycle_v2", "msg": {
                    "market_ticker": name[e.ticker], "event_type": e.event_type, "result": e.result,
                    "settlement_value": e.settlement_value, "determination_ts": e.ts_exch // NS_PER_S}})
            elif isinstance(e, Settlement):
                m = _market_dict(name[e.ticker], ev_t, float(e.ticker.split("-")[1]), open_ns, T, True, "finalized")
                m.update(result=e.result, expiration_value=f"{e.expiration_value:.2f}",
                         settlement_value_dollars=px_to_dollars(e.settlement_px), settlement_ts=_iso(e.ts + 60 * NS_PER_S))
                put(e.ts + 60 * NS_PER_S, "kalshi.rest.market", _rest(f"/markets/{name[e.ticker]}", {}, {"market": m}))
            elif isinstance(e, ExtTrade):
                put(e.ts, EXT_STREAM, encode_event(dataclasses.replace(e, trade_id=f"{ev_t}-{e.trade_id}")))
            elif isinstance(e, ExtBBO):
                put(e.ts, EXT_STREAM, encode_event(e))
        prev_T = T
        flush_until(T - 60 * NS_PER_S)  # the next event's first record is its listing at T - 60 s


def synth_strategy_config(series: str = "KXBTCD", **quoting_over: Any):
    """StrategyConfig tuned for fast synthetic replays (500 ms quote cycle, wide price band,
    KAT-like limits). Research-only; not a trading configuration."""
    from dh.backtest.kat import default_kat_config

    cfg = default_kat_config()
    q = dataclasses.replace(cfg.quoting, enabled_series=(series,), **quoting_over)
    return dataclasses.replace(cfg, quoting=q, run_prefix="synth")


__all__ = ["SynthRecordingConfig", "SynthRecordingInfo", "write_synthetic_recording", "synth_strategy_config",
           "event_ticker", "EXT_STREAM"]
