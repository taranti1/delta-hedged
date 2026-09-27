"""Taker-flow calibration from a recording (dh.research.calibrate_flow on recorded data).

    trades, markets, btc = flow_inputs(root, t0, t1)            # recorded public tape, universe, BRTI
    res = fit_flow(root, t0, t1, out)                            # IS/OOS report + flow_segments.json

CLI:  python scripts/run_experiment.py flow --root data --t0 A --t1 B [--flow-split 0.7 | --walk-forward-days 1]
      then replays with t0 >= B:  ... e4 --t0 B --t1 C --flow-segments <out>/flow_segments.json

Inputs (all causal):
  trades    public prints from kalshi.ws `trade` frames received in [t0, t1), stamped with the
            exchange time for sweep identity and receive time for causal features/exposure; prints where WE were the taker (trade_id of one of our private `fill`
            messages with is_taker) are removed -- they are not external flow. Prints that filled
            our resting orders stay: that flow arrived from other participants.
  markets   the window's markets with a spec that EXPIRED in (t0, t1] (fully observed); exposure
            starts at max(open, strike availability, t0).
  btc       BRTI ticks (WS 1 Hz / 5 Hz) as the point-in-time reference, AVAILABLE at their receive
            time (close_ts_ms = receive ms, btc_bar_ms = 0).

``flow_segments.json`` is fitted on every market of the window, so ``meta.fit_end_ms`` is the last
expiration (<= t1): use it only for replays that start after it (run_replay warns otherwise).
``meta.provenance`` records the fitting window (UTC), the dataset (recording root, window and a
content hash of the fitting sample) and the fit method (dh.research.calibrate_flow).
``flow_segments_train.json`` is the training-period fit that the in-sample / out-of-sample table
grades (chronological split by expiration, or walk-forward by day). Exposure and trades
are restricted to observed valid-book intervals, fresh public frames (5 s), and fresh BRTI
(3 s). These fits still use proxy moneyness and cannot be bound to production models.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import orjson
import pandas as pd

from dh.core.events import IndexTick, KalshiTrade
from dh.kalshi.normalize import ws_message_to_events
from dh.research.calibrate_flow import FlowSplit, calibrate_split, walk_forward_by_day, write_split_report
from dh.research.exp_common import fmt_ns
from dh.research.replay_env import Universe, brti_ticks, build_universe, kalshi_ws_streams
from dh.store.replay import Normalizers, iter_raw, list_streams
from dh.research.flow_coverage import healthy_book_intervals, intersect, merge_intervals

MS = 1_000_000


def recorded_trades(root: str | Path, t0: int, t1: int, own_taker_ids: set[str] | None = None) -> pd.DataFrame:
    """Public prints received in [t0, t1) (calibrate_flow trade schema), minus our taker prints."""
    rows: list[dict[str, Any]] = []
    own = own_taker_ids or set()
    streams = kalshi_ws_streams(root)
    for rec in iter_raw(root, streams, t0, t1) if streams else ():
        if b'"trade"' not in rec.data:
            continue
        try:
            msg = orjson.loads(rec.data)
        except orjson.JSONDecodeError:
            continue
        if not isinstance(msg, dict) or msg.get("type") != "trade":
            continue
        try:
            evs = ws_message_to_events(msg, rec.t)
        except (ValueError, KeyError, TypeError):
            continue
        for e in evs:
            if isinstance(e, KalshiTrade) and not e.is_block and e.trade_id not in own:
                rows.append({"ticker": e.ticker, "ts_ms": (e.ts_exch or e.ts) // MS, "ts_recv_ms": rec.t // MS, "yes_px": e.yes_px, "qty": e.qty,
                             "taker_side": e.taker_side, "trade_id": e.trade_id})
    cols = ["ticker", "ts_ms", "ts_recv_ms", "yes_px", "qty", "taker_side", "trade_id"]
    return pd.DataFrame(rows, columns=cols).drop_duplicates("trade_id").reset_index(drop=True)


def recorded_markets(uni: Universe, t0: int, t1: int) -> pd.DataFrame:
    """Markets with a spec that expired in (t0, t1]; exposure from max(open, availability, t0)."""
    rows = []
    for t, rec in sorted(uni.markets.items()):
        s = rec.spec
        if s is None or not (t0 < s.expiration_ts <= t1):
            continue
        start = max(s.open_ts, rec.avail_ns or s.open_ts, t0)
        if start >= s.expiration_ts:
            continue
        rows.append({"ticker": t, "event_ticker": s.event_ticker, "strike_type": s.strike_type,
                     "floor_strike": s.floor_strike, "cap_strike": s.cap_strike, "open_ts_ms": start // MS,
                     "expiration_ts_ms": s.expiration_ts // MS})
    cols = ["ticker", "event_ticker", "strike_type", "floor_strike", "cap_strike", "open_ts_ms", "expiration_ts_ms"]
    return pd.DataFrame(rows, columns=cols)


def recorded_btc(root: str | Path, t0: int, t1: int, cache: dict[Any, Any] | None = None,
                 lookback_s: float = 3600.0) -> pd.DataFrame:
    """BRTI WS ticks received in [t0 - lookback_s, t1): ts_ms = source ms, close_ts_ms = receive ms."""
    ticks = [e for e in brti_ticks(root, t0 - int(lookback_s * 1e9), t1, include_rest=False, cache=cache)
             if isinstance(e, IndexTick) and e.feed in ("1hz", "5hz")]
    return pd.DataFrame({"ts_ms": [(e.ts_exch or e.ts) // MS for e in ticks], "close_ts_ms": [e.ts // MS for e in ticks],
                         "price": [float(e.value) for e in ticks]})


def flow_inputs(root: str | Path, t0: int, t1: int, *, universe: Universe | None = None
                ) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    uni = universe if universe is not None else build_universe(root, t0, t1)
    own_taker = {tid for tid, f in uni.own_fills.items() if f.is_taker}
    return (recorded_trades(root, t0, t1, own_taker), recorded_markets(uni, t0, t1),
            recorded_btc(root, t0, t1, cache=uni.cache))


def recorded_coverage(root: str | Path, t0: int, t1: int, btc: pd.DataFrame) -> dict:
    streams = [s for s in list_streams(root) if s == "kalshi.ws" or s == "status" or s.startswith("events.")]
    coverage = healthy_book_intervals(iter_raw(root, streams, t0, t1), Normalizers(), t0 // MS, t1 // MS)
    # Reference source and arrival must both be fresh; a late historical tick is not current.
    fresh = merge_intervals((int(r.close_ts_ms), min(int(r.close_ts_ms) + 3000, int(r.ts_ms) + 3000))
                            for r in btc.itertuples())
    return {k: intersect(v, fresh) for k, v in coverage.items()}


def fit_flow(root: str | Path, t0: int, t1: int, out: str | Path, *, universe: Universe | None = None,
             train_frac: float = 0.7, walk_forward_days: int = 0, vol_ann: float = 0.40, min_orders: int = 30,
             prior_s: float = 1800.0) -> FlowSplit:
    """Calibrate taker flow on [t0, t1) of a recording with a time split; writes the report and
    the segment JSON files to ``out`` (see module docstring)."""
    uni = universe if universe is not None else build_universe(root, t0, t1)
    trades, markets, btc = flow_inputs(root, t0, t1, universe=uni)
    if not len(markets) or not len(btc):
        raise SystemExit(f"flow: no fully observed markets ({len(markets)}) or no BRTI ticks ({len(btc)}) in "
                         f"{fmt_ns(t0)} .. {fmt_ns(t1)}")
    coverage = recorded_coverage(root, t0, t1, btc)
    if not any(coverage.values()):
        raise SystemExit("flow: no verified healthy recording exposure (requires snapshots and fresh BRTI)")
    kw = dict(vol_ann=vol_ann, min_orders=min_orders, prior_s=prior_s, btc_bar_ms=0,
              coverage=coverage, max_price_age_ms=3000, grid_s=1)
    if walk_forward_days > 0:
        res = walk_forward_by_day(trades, markets, btc, min_train_days=walk_forward_days, **kw)
    else:
        res = calibrate_split(trades, markets, btc, train_frac=train_frac, **kw)
    coverage_hash = hashlib.sha256(json.dumps(coverage, sort_keys=True).encode()).hexdigest()
    res.meta.update({"coverage_sha256": coverage_hash, "coverage": "valid snapshots + sequence/status health + WS age <=5s + BRTI age <=3s",
                     "healthy_market_seconds": sum(b-a for v in coverage.values() for a,b in v) / 1000,
                     "source": str(root), "window": f"{fmt_ns(t0)} .. {fmt_ns(t1)}", "trades": len(trades),
                     "markets_in_window": len(markets), "own_taker_prints_removed":
                     sum(1 for f in uni.own_fills.values() if f.is_taker)})
    src = f"recording {root} [{fmt_ns(t0)} .. {fmt_ns(t1)}]: kalshi.ws public prints + BRTI ticks"
    for prov in (res.provenance, res.provenance_all):
        if prov:
            prov["dataset_id"] = f"{prov.get('dataset_id', '')} coverage-sha256:{coverage_hash} ({src})"
    write_split_report(res, out, title="Taker-flow calibration from a recording (time split)",
                       note=f"Window {fmt_ns(t0)} .. {fmt_ns(t1)}; {len(trades)} public prints, {len(markets)} fully "
                            "observed markets; BRTI ticks as the point-in-time reference.", synthetic=uni.synthetic)
    return res


__all__ = ["flow_inputs", "fit_flow", "recorded_trades", "recorded_markets", "recorded_btc"]
