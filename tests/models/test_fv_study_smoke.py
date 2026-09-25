"""Smoke test of the fair-value study machinery on synthetic Brownian minute bars.

Checks the timing invariants that guard against look-ahead (spot candle ends at the decision
time, settlement candle is [T-60, T)), that the walk-forward only trains on earlier hours, and
that on Gaussian data with constant volatility the fitted models are near the oracle.
"""

from __future__ import annotations

import math

import numpy as np
import pytest

from dh.research.fv_study.config import StudyConfig, utc
from dh.research.fv_study.data import Minutes, _runs_mask
from dh.research.fv_study.evaluate import loss_terms, make_cells, predict
from dh.research.fv_study.panel import build_panel, estimate_kappa, raw_ewmas
from dh.research.fv_study.walkforward import ModelDef, SeasonalSpec, norm_sigma_series, run_period

SIGMA = 6e-5  # per sqrt(second)


@pytest.fixture(scope="module")
def synthetic():
    rng = np.random.default_rng(42)
    start = utc("2024-01-01")
    n_min = 75 * 1440
    sub = 6  # 10-second sub-steps
    steps = rng.standard_normal((n_min, sub)) * SIGMA * math.sqrt(60.0 / sub)
    lp = np.log(50_000.0) + np.cumsum(steps.ravel()).reshape(n_min, sub)
    px = np.exp(lp)
    o = np.r_[50_000.0, px[:-1, -1]]  # open = previous close (continuous path)
    h = np.maximum(px.max(axis=1), o)
    l = np.minimum(px.min(axis=1), o)
    c = px[:, -1]
    ts = start + 60 * np.arange(n_min, dtype=np.int64)
    v = np.ones(n_min)
    zero_flat = np.zeros(n_min, dtype=bool)
    outage = _runs_mask(zero_flat, 5)
    r = np.r_[np.nan, np.diff(np.log(c))]
    M = Minutes(ts=ts, o=o, h=h, l=l, c=c, v=v, zero_flat=zero_flat, outage=outage, r=r,
                r_valid=np.r_[False, np.ones(n_min - 1, dtype=bool)])
    cfg = StudyConfig(taus_min=(60, 10, 2), train_months=1)
    P = build_panel(M, cfg)
    raw = raw_ewmas(M, cfg.half_lives_min)
    ns = norm_sigma_series(M)
    return M, P, raw, ns, cfg


def test_timing_invariants_no_look_ahead(synthetic):
    M, P, raw, ns, cfg = synthetic
    t = P.T[:, None] - P.taus_s[None, :]
    # spot candle ends exactly at the decision time; settlement candle is [T-60, T)
    assert np.all(M.ts[P.j0] + 60 == t)
    assert np.all(M.ts[P.j1] == P.T - 60)
    assert np.all(P.j0 < P.j1[:, None])
    assert np.all(P.spot == M.c[P.j0])


def test_walk_forward_near_oracle_on_gaussian_data(synthetic):
    M, P, raw, ns, cfg = synthetic
    models = [ModelDef("G-raw-2h", "raw:120", "gauss"), ModelDef("G-blend", "blend", "gauss"),
              ModelDef("T-blend", "blend", "t")]
    R = run_period(M, P, raw, ns, "t", "2024-03-01", "2024-03-15", models, SeasonalSpec(layout="flat"), cfg)
    # training never includes the evaluation month
    months = sorted({p["month"] for p in R.params})
    assert months == [utc("2024-03-01")]
    assert all(p["n_train"] > 600 for p in R.params)
    nu = [p["value"] for p in R.params if p["model"] == "T-blend" and p["param"] == "nu"]
    assert min(nu) > 6.0  # near-Gaussian data -> light tails
    C = make_cells(P, R, "z")
    ll = {m: loss_terms(predict(P, R, C, m), C.y)[1].mean() for m in R.sd}
    # oracle: true sigma, true proxy variance time (kappa from the same data)
    kappa = estimate_kappa(M, utc("2024-02-01"), utc("2024-03-01"))
    from dh.models.fairvalue import digital_vec

    h = R.eval_h
    ve = (P.taus_s[C.k] - 60.0) + kappa * 60.0
    sd = P.spot[h][C.e, C.k] * SIGMA * np.sqrt(ve)
    p_or = digital_vec("greater", P.spot[h][C.e, C.k], sd, "gauss", floor=C.K).p_yes
    ll_or = loss_terms(p_or, C.y)[1].mean()
    for m, v in ll.items():
        assert v < ll_or + 0.01, (m, v, ll_or)
