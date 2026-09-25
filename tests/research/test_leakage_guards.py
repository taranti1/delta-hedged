"""Leakage guards (audit): causal BTC joins, time-split flow calibration, look-ahead labels for
fitted replay inputs (fair-value parameters, taker-flow segments)."""

from __future__ import annotations

import json
import math

import numpy as np
import pandas as pd
import pytest

from dh.core.units import NS_PER_S
from dh.research.calibrate_flow import (
    _price_fn,
    calibrate,
    calibrate_split,
    load_segments,
    order_sizes,
    save_segments,
    taker_orders,
    walk_forward_by_day,
)
from dh.research.exp0_maker_pnl import prepare
from dh.research.kalshi_data import btc_price_asof, normalize_markets, normalize_trades
from dh.research.replay_env import flow_status, fv_params_status, resolve_fv_config

H_MS = 3_600_000
DAY_MS = 86_400_000


# ------------------------------------------------------------------ causal BTC join
def test_btc_bar_is_used_only_after_it_closes():
    # 1-minute bars stamped at their OPEN: [0, 60 s) closes at 100, [60 s, 120 s) closes at 200
    btc = pd.DataFrame({"ts_ms": [0, 60_000], "price": [100.0, 200.0]})
    px = btc_price_asof(btc, [59_999, 60_000, 61_000, 119_999, 120_000])
    assert math.isnan(px[0])  # no bar has closed yet
    assert px[1] == 100.0 and px[3] == 100.0
    assert px[2] == 100.0  # trade 1 s after the second bar OPENS: that bar is NOT used
    assert px[4] == 200.0  # ... only from its close
    # point-in-time prices (bar_ms = 0) are usable at their stamp; an explicit close time wins
    assert btc_price_asof(btc, [61_000], bar_ms=0)[0] == 200.0
    closes = btc.assign(close_ts_ms=[30_000, 90_000])
    assert list(btc_price_asof(closes, [61_000, 90_000])) == [100.0, 200.0]


def _mkt(ticker="M0", K=100_000.0, open_ms=0, exp_ms=H_MS, result="no"):
    return dict(ticker=ticker, event_ticker="E" + ticker, strike_type="greater", floor_strike=K, cap_strike=np.nan,
                result=result, open_ts_ms=open_ms, expected_expiration_ts_ms=exp_ms)


def test_exp0_z_uses_the_previous_bar_not_the_open_one():
    # bar opening at 10:00 jumps far from the strike; a trade 1 s after it opens must still see
    # the previous (closed) bar at the strike -> z = 0
    btc = pd.DataFrame({"ts_ms": [540_000, 600_000], "price": [100_000.0, 130_000.0]})
    trades = pd.DataFrame([dict(ticker="M0", ts_ms=601_000, yes_px=5000, qty=100, taker_outcome_side="yes")])
    df = prepare(trades, pd.DataFrame([_mkt()]), btc)
    assert df.z.iloc[0] == pytest.approx(0.0)
    leaky = prepare(trades, pd.DataFrame([_mkt()]), btc, btc_bar_ms=0)  # would-be as-of join on the open stamp
    assert leaky.z.iloc[0] > 3


def test_calibrate_flow_ignores_a_bar_that_has_not_closed():
    trades = pd.DataFrame([dict(ticker="M0", ts_ms=601_000, yes_px=5000, qty=700, taker_outcome_side="no")])
    markets = pd.DataFrame([_mkt()])
    only_open_bar = pd.DataFrame({"ts_ms": [600_000], "price": [100_000.0]})
    seg = calibrate(trades, markets, only_open_bar, prior_s=1e-9)
    assert ("*", "*", "bid") not in seg  # the trade had no causal reference price: dropped
    assert ("*", "*", "bid") in calibrate(trades, markets, only_open_bar, prior_s=1e-9, btc_bar_ms=0)
    # with an earlier closed bar the trade is segmented by THAT bar (at the strike -> atm), not
    # by the open bar 30 000 away (-> far)
    btc = pd.DataFrame({"ts_ms": [540_000, 600_000], "price": [100_000.0, 130_000.0]})
    mk = normalize_markets(markets)
    orders = taker_orders(normalize_trades(trades)).merge(
        mk[["ticker", "expiration_ts_ms", "floor_strike", "cap_strike"]], on="ticker")
    keys = list(order_sizes(orders, _price_fn(btc, 60_000), 0.40))
    assert len(keys) == 1 and keys[0][1] == "atm" and keys[0][2] == "bid"


# ------------------------------------------------------------------ time-split flow calibration
def _flow_sample(n_markets: int, fast_from: int) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Hourly ATM markets back to back; 1 taker order per minute, 2 per minute from market
    ``fast_from`` on (a regime change the in-sample fit cannot see)."""
    mk, rows = [], []
    for i in range(n_markets):
        o = i * H_MS
        mk.append(_mkt(f"M{i:03d}", open_ms=o, exp_ms=o + H_MS))
        offs = (20_000, 50_000) if i >= fast_from else (20_000,)
        for k in range(60):
            for off in offs:
                rows.append(dict(ticker=f"M{i:03d}", ts_ms=o + 60_000 * k + off, yes_px=5000, qty=100,
                                 taker_outcome_side="no"))
    btc = pd.DataFrame({"ts_ms": [0], "price": [100_000.0]})
    return pd.DataFrame(rows), pd.DataFrame(mk), btc


def test_calibrate_split_grades_out_of_sample_and_sees_the_regime_change():
    trades, markets, btc = _flow_sample(40, fast_from=28)
    res = calibrate_split(trades, markets, btc, train_frac=0.7, btc_bar_ms=0, prior_s=1e-9)
    m = res.metrics.set_index("sample")
    assert res.split_ms == 28 * H_MS and res.train_end_ms == res.split_ms  # 70 % of expirations
    assert m.loc["in_sample", "markets"] == 28 and m.loc["out_of_sample", "markets"] == 12
    assert m.loc["in_sample", "ratio_ct"] == pytest.approx(1.0, abs=0.05)  # flatters itself in sample
    assert m.loc["out_of_sample", "ratio_ct"] == pytest.approx(0.5, abs=0.05)  # later data: flow doubled
    assert m.loc["out_of_sample", "wape_ct"] > m.loc["in_sample", "wape_ct"]
    # the graded fit used training markets only; the whole-sample fit is for later replays
    assert res.segments[("*", "*", "bid")].order_rate == pytest.approx(1 / 60, rel=0.05)
    assert res.segments_all[("*", "*", "bid")].order_rate > res.segments[("*", "*", "bid")].order_rate
    assert res.all_end_ms == 40 * H_MS
    assert set(res.per_segment["sample"]) == {"in_sample", "out_of_sample"}


def test_walk_forward_by_day_refits_on_earlier_days_only():
    trades, markets, btc = _flow_sample(24 * 5, fast_from=24 * 3)
    res = walk_forward_by_day(trades, markets, btc, min_train_days=1, btc_bar_ms=0, prior_s=1e-9)
    m = res.metrics.set_index("sample")
    days = [s for s in m.index if s.startswith("oos_day")]
    assert len(days) == 4  # days 2..5, each fitted on the days before it
    assert m.loc[days[0], "ratio_ct"] == pytest.approx(1.0, abs=0.05)
    assert m.loc[days[2], "ratio_ct"] == pytest.approx(0.5, abs=0.05)  # first fast day, fit on slow days
    assert m.loc["out_of_sample", "ratio_ct"] < 0.85 < m.loc["in_sample", "ratio_ct"]


def test_segments_json_roundtrip(tmp_path):
    trades, markets, btc = _flow_sample(6, fast_from=99)
    seg = calibrate(trades, markets, btc, btc_bar_ms=0)
    p = save_segments(seg, tmp_path / "s.json", meta={"fit_end_ms": 6 * H_MS})
    back, meta = load_segments(p)
    assert meta["fit_end_ms"] == 6 * H_MS and set(back) == set(seg)
    k = ("*", "*", "bid")
    assert back[k].rate_contracts_per_s == pytest.approx(seg[k].rate_contracts_per_s)


# ------------------------------------------------------------------ look-ahead labels
def test_fv_parameters_fitted_after_t0_are_flagged_in_sample():
    cfg, src = resolve_fv_config(None)
    end_ns = int(cfg["data_end_utc"]) * NS_PER_S
    info, warn = fv_params_status(cfg, src, end_ns - 3600 * NS_PER_S, synthetic=False)
    assert info["fv_params_in_sample"] is True and "IN-SAMPLE" in info["fv_params"] and "in-sample FV" in warn
    info, warn = fv_params_status(cfg, src, end_ns + 3600 * NS_PER_S, synthetic=False)
    assert info["fv_params_in_sample"] is False and warn is None
    info, warn = fv_params_status(cfg, src, end_ns - 3600 * NS_PER_S, synthetic=True)
    assert info["fv_params_in_sample"] is None and warn is None and "synthetic" in info["fv_params"]
    info, warn = fv_params_status({k: v for k, v in cfg.items() if k != "data_end_utc"}, "custom", end_ns, False)
    assert info["fv_params_in_sample"] is True and warn  # unknown fitting window: treated as in-sample


def test_flow_segments_fitted_after_t0_are_flagged_in_sample():
    seg = {("*", "*", "bid"): object()}
    info, warn = flow_status(seg, {"fit_end_ms": 1_000}, "s.json", 999 * 1_000_000)
    assert info["flow_in_sample"] is True and "in-sample flow" in warn
    info, warn = flow_status(seg, {"fit_end_ms": 1_000}, "s.json", 1_000 * 1_000_000)
    assert info["flow_in_sample"] is False and warn is None
    assert flow_status(None, {}, "", 0)[0]["flow_in_sample"] is None


def test_flow_command_and_replay_wiring_on_the_synthetic_recording(tiny_rec, synth_cfg, tmp_path):
    """SYNTHETIC: fit flow on the recording, then replay the SAME window with it -> the replay
    uses the segments and is labelled in-sample (warning); FV parameters n/a (synthetic)."""
    from dh.research.flow_recording import fit_flow
    from dh.research.replay_env import run_replay

    res = fit_flow(tiny_rec.root, tiny_rec.t0, tiny_rec.t1, tmp_path / "flow", train_frac=0.5)
    assert {"in_sample", "out_of_sample"} <= set(res.metrics["sample"])
    meta = json.loads((tmp_path / "flow" / "flow_segments.json").read_text())["meta"]
    assert meta["fit_end_ms"] * 1_000_000 <= tiny_rec.t1 and meta["synthetic"] is True
    assert "SYNTHETIC" in (tmp_path / "flow" / "flow_calibration.md").read_text()
    t0 = tiny_rec.t0 + 30 * NS_PER_S
    rr = run_replay(tiny_rec.root, t0, t0 + 30 * NS_PER_S, synth_cfg, "B", warm="recorded",
                    flow_segments=tmp_path / "flow" / "flow_segments.json", keep_objects=True)
    s = rr.summary
    assert s["flow_in_sample"] is True and any(w.startswith("in-sample flow") for w in s["warnings"])
    assert s["fv_params_in_sample"] is None and "synthetic" in s["fv_params"]
    loaded, _ = load_segments(tmp_path / "flow" / "flow_segments.json")
    assert rr.mm.flow.segments == loaded
