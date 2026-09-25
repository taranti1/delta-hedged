"""Inference fixes: calibrated intervals (audit m7), multiplicity control (C5), settlement-event
clusters across series (M5), markout staleness cap (m6)."""

from __future__ import annotations

import math

import numpy as np
import pandas as pd
import pytest

from dh.research.exp_common import (
    Report,
    cluster_mean_ci,
    holm,
    holm_adjusted,
    n_events,
    paired_diff_ci,
    settlement_key,
    slope_ci,
    sum_diff_ci,
)


def _settlement_driven(rng, k):
    """r7 design: per-event P&L driven by the binary outcome x net inventory, zero true edge."""
    vals, wts, cl = [], [], []
    for e in range(k):
        n = 1 + rng.poisson(15)
        p = rng.uniform(0.1, 0.9)
        settle = float(rng.random() < p)
        side = rng.choice([-1.0, 1.0], n, p=[0.3, 0.7]) if rng.random() < 0.5 else rng.choice([-1.0, 1.0], n, p=[0.7, 0.3])
        vals += list(100 * side * (settle - p))
        wts += list(rng.integers(1, 10, n).astype(float))
        cl += [e] * n
    return vals, wts, cl


def test_cluster_ci_one_sided_error_is_near_nominal_with_few_events():
    """The percentile bootstrap gave P(lo > 0 | mean 0) = 3.2 % at 20 events (nominal 2.5 %); the
    studentized/jackknife hull stays at or below nominal on the same design (fixed seeds)."""
    rng = np.random.default_rng(3)
    R, fp = 600, 0
    for r in range(R):
        v, w, c = _settlement_driven(rng, 20)
        fp += cluster_mean_ci(v, w, c, n_boot=400, seed=r).lo > 0
    assert fp / R <= 0.025


def test_holm_controls_the_family_and_adjusts_monotonically():
    p = [0.004, 0.012, 0.02, 0.5, np.nan]
    assert list(holm(p, 0.025)) == [True, False, False, False, False]  # 0.004 <= 0.025/4; 0.012 > 0.025/3
    adj = holm_adjusted(p)
    assert adj[0] == pytest.approx(0.016) and adj[1] == pytest.approx(0.036) and np.isnan(adj[4])
    assert all(np.diff(adj[np.argsort(p)][:4]) >= 0)


def test_series_expiring_together_are_one_settlement_event():
    assert settlement_key("KXBTCD-26OCT0114") == settlement_key("KXBTC-26OCT0114") == settlement_key("KXBTC15M-26OCT011400")
    assert settlement_key("KXBTC15M-26OCT011415") != settlement_key("KXBTCD-26OCT0114")
    assert settlement_key("exp 2026-10-01T14:00:00Z") == "exp 2026-10-01T14:00:00Z"
    d = pd.DataFrame({"event": [f"{s}-26OCT01{h:02d}" for h in range(8) for s in ("KXBTCD", "KXBTC", "KXBTC15M")]})
    assert n_events(d) == 8
    r = Report("x", "x", ".", verdict="ACCEPT", decision_events=n_events(d), policies=["B", "C"])
    assert r.final_verdict().startswith("INCONCLUSIVE")


def test_cross_series_ci_widens_when_clustered_by_expiration():
    rng = np.random.default_rng(5)
    rows = []
    for T in range(12):
        shock = rng.normal(0, 20)
        for s in ("KXBTCD", "KXBTC", "KXBTC15M"):
            rows += [{"event": f"{s}-26OCT01{T:02d}", "net_c_per_ct": 1.0 + shock + rng.normal(0, 1), "contracts": 5.0}
                     for _ in range(10)]
    d = pd.DataFrame(rows)
    by_ticker = cluster_mean_ci(d.net_c_per_ct, d.contracts, d.event, n_boot=500)
    other = d.assign(net_c_per_ct=d.net_c_per_ct + 0.0)
    paired = paired_diff_ci(d, other.assign(net_c_per_ct=other.net_c_per_ct + 1.0), "net_c_per_ct", n_boot=200)
    assert paired.clusters == 12  # paired CIs cluster event tickers by expiration
    assert by_ticker.clusters == 36  # a raw cluster label stays as given (callers pass settlement keys)


def test_slope_and_sum_ci_cover_their_true_values():
    rng = np.random.default_rng(7)
    x = rng.normal(size=4000)
    y = 0.4 * x + rng.normal(size=4000)
    ci = slope_ci(x, y, np.arange(4000) // 40, n_boot=200)
    assert ci.lo < 0.4 < ci.hi and ci.clusters == 100
    a = pd.DataFrame({"event": np.arange(50), "net": rng.normal(0, 1, 50)})
    b = a.assign(net=a.net + 0.5)
    s = sum_diff_ci(a, b, "net", scale=1.0, n_boot=200)
    assert s.mean == pytest.approx(25.0) and s.lo == pytest.approx(25.0) and s.hi == pytest.approx(25.0)


def test_markout_fair_value_older_than_max_age_is_missing():
    from dh.core.events import KalshiFill
    from dh.execution.markout import AsOf, compute_markouts

    fv = {"M": AsOf(np.array([0, 10 * 10**9]), np.array([0.5, 0.6]))}
    f = KalshiFill(ts=1 * 10**9, ts_exch=0, ticker="M", trade_id="t", order_id="o", client_order_id="c",
                   book_side="bid", yes_px=5000, qty=100, is_taker=False, fee_micros=0, post_position=100)
    old = compute_markouts([f], fv, (5.0,), time_basis="recv")[0]
    assert old.markouts_c[0] == pytest.approx(0.0)  # default: unlimited as-of (value from t=0, 6 s old)
    new = compute_markouts([f], fv, (5.0,), time_basis="recv", max_age_ns=5 * 10**9)[0]
    assert math.isnan(new.markouts_c[0]) and new.fv_at_fill == pytest.approx(0.5)


def test_day_block_second_check_applies_from_five_days():
    from dh.research.exp_common import CI, day_block_ok

    assert day_block_ok(CI(1.0, 0.5, 1.5, 3)) is None  # < 5 day blocks: not applicable
    assert day_block_ok(CI(1.0, 0.5, 1.5, 6)) is True
    assert day_block_ok(CI(1.0, -0.2, 2.0, 6)) is False  # the day-block CI disagrees: no decision


def test_regime_columns_split_by_tau_vol_tercile_and_weekday():
    from dh.research.exp_common import add_regimes, regime_table

    ts = np.array([pd.Timestamp("2026-10-03T12:00Z").value, pd.Timestamp("2026-10-05T12:00Z").value] * 30)  # Sat, Mon
    d = pd.DataFrame({"ts": ts, "tau_s": np.tile([20.0, 700.0], 30), "rv_1h": np.linspace(0.2, 0.8, 60),
                      "event": np.repeat([f"exp {i}" for i in range(20)], 3), "net_c_per_ct": 1.0, "contracts": 1.0,
                      "policy": "B"})
    r = add_regimes(d)
    assert set(r.tau_bucket) == {"<30s", "10-30m"} and set(r.vol_tercile) == {"low", "mid", "high"}
    assert set(r.weekday) == {"weekend", "weekday"}
    t = regime_table(r)
    assert set(t.family) == {"tau_bucket", "vol_tercile", "weekday"} and (t["mean"] == 1.0).all()


def test_reports_say_the_lookahead_check_cannot_cover_fill_and_adverse_parameters():
    from dh.research.replay_env import Universe, inputs_meta

    meta, _ = inputs_meta(Universe(root="/nonexistent", t0=0, t1=1), 0)
    assert "NOT covered by the look-ahead check" in meta["strategy fill/adverse parameters"]
