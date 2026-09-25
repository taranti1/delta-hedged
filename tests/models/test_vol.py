from __future__ import annotations

import math

import numpy as np
import pytest

from dh.core.units import NS_PER_S
from dh.models.vol import (
    FLAT_SEASONAL,
    EwmaVol,
    SeasonalVol,
    VolForecaster,
    VolForecasterConfig,
    annualized,
    bipower_variation,
    blended_sigma,
    ewma_regular,
    garman_klass_var,
    jump_share,
    parkinson_var,
    per_sqrt_second,
    realized_variance,
)

T0 = 1_735_689_600  # 2025-01-01 00:00:00 UTC (a Wednesday)


def gbm_path(rng, sigma, times_s, p0=100_000.0):
    dt = np.diff(times_s)
    r = sigma * np.sqrt(dt) * rng.standard_normal(dt.size)
    return p0 * np.exp(np.r_[0.0, np.cumsum(r)])


def test_unit_conversions():
    s = per_sqrt_second(0.35)
    assert annualized(s) == pytest.approx(0.35)
    assert s == pytest.approx(0.35 / math.sqrt(365.25 * 86400))


@pytest.mark.parametrize("min_dt", [0.0, 1.0, 5.0])
def test_ewma_recovers_sigma_with_irregular_steps(min_dt):
    rng = np.random.default_rng(11)
    sigma = 6e-5
    dts = rng.exponential(0.4, size=400_000)  # ~2.5 ticks per second, irregular
    t = np.cumsum(dts)
    p = gbm_path(rng, sigma, np.r_[0.0, t])
    ev = EwmaVol(half_life_s=20_000.0, min_dt_s=min_dt)
    for ti, pi in zip(np.r_[0.0, t], p):
        ev.update(int(ti * NS_PER_S), float(pi))
    assert ev.ready
    # effective sample ~ T/half-life * ticks; allow 5%
    assert ev.sigma(int(t[-1] * NS_PER_S)) == pytest.approx(sigma, rel=0.05)


def test_ewma_half_life_semantics_and_warmup_bias_correction():
    # constant per-second variance samples v1 then v2 on a 1 s grid: after one half-life of v2,
    # the (fully warmed) estimate is halfway in variance.
    H = 600.0
    ev = EwmaVol(half_life_s=H, min_dt_s=0.0)
    p = 100.0
    t = 0
    ev.update(t, p)
    v1, v2 = 1e-8, 4e-8
    for i in range(20 * int(H)):
        t += NS_PER_S
        p *= math.exp(math.sqrt(v1) * (1 if i % 2 else -1))
        ev.update(t, p)
    assert ev.variance() == pytest.approx(v1, rel=1e-9)
    for i in range(int(H)):
        t += NS_PER_S
        p *= math.exp(math.sqrt(v2) * (1 if i % 2 else -1))
        ev.update(t, p)
    assert ev.variance() == pytest.approx(0.5 * (v1 + v2), rel=1e-3)
    # warm-up: a single return already gives an unbiased estimate (bias-corrected weight)
    ev2 = EwmaVol(half_life_s=3600.0)
    ev2.update(0, 100.0)
    ev2.update(10 * NS_PER_S, 100.0 * math.exp(0.001))
    assert ev2.variance() == pytest.approx(0.001**2 / 10)
    assert not ev2.ready and math.isnan(EwmaVol(60.0).sigma())


def test_ewma_min_dt_aggregates_and_max_dt_drops_outages():
    ev = EwmaVol(half_life_s=100.0, min_dt_s=1.0)
    ev.update(0, 100.0)
    ev.update(int(0.4 * NS_PER_S), 101.0)  # < min_dt: not folded yet
    assert ev.n_updates == 0
    ev.update(int(1.2 * NS_PER_S), 102.0)  # folds the whole 1.2 s return 100 -> 102
    assert ev.n_updates == 1
    assert ev.variance() == pytest.approx(math.log(1.02) ** 2 / 1.2)
    ev_gap = EwmaVol(half_life_s=100.0, min_dt_s=1.0, max_dt_s=60.0)
    ev_gap.update(0, 100.0)
    ev_gap.update(NS_PER_S, 100.1)
    w_before, v_before = ev_gap.weight, ev_gap.variance()
    ev_gap.update(1000 * NS_PER_S, 150.0)  # outage return is dropped, history decays
    assert ev_gap.variance() == pytest.approx(v_before) and ev_gap.weight < w_before
    ev_gap.update(1001 * NS_PER_S, 150.0 * math.exp(0.0005))
    assert ev_gap.variance() < v_before  # new small return now dominates the decayed history
    # non-positive prices and out-of-order timestamps are ignored
    ev_gap.update(999 * NS_PER_S, 1.0)
    ev_gap.update(1002 * NS_PER_S, -5.0)
    assert ev_gap.n_updates == 2


def test_ewma_regular_matches_streaming():
    rng = np.random.default_rng(5)
    n = 5000
    lp = np.cumsum(rng.standard_normal(n) * 1e-3)
    r = np.diff(lp)
    valid = np.ones(r.size, bool)
    valid[1000:1010] = False
    x = r * r / 60.0
    y = ewma_regular(x, valid, half_life_steps=30.0)
    ev = EwmaVol(half_life_s=30 * 60.0, min_dt_s=1.0, max_dt_s=90.0)
    ev.update(0, math.exp(lp[0]))
    out = []
    for i in range(1, n):
        if valid[i - 1]:
            ev.update(i * 60 * NS_PER_S, math.exp(lp[i]))
        else:
            # emulate an outage minute: advance the anchor without a return (decay only)
            ev._S *= 2.0 ** (-60.0 / ev.half_life_s)
            ev._W *= 2.0 ** (-60.0 / ev.half_life_s)
            ev._anchor_ts, ev._anchor_lp = i * 60 * NS_PER_S, lp[i]
        out.append(ev.variance())
    assert np.allclose(y, out, rtol=1e-10)


def _synthetic_seasonal_returns(rng, profile_fn, days=120):
    ts = T0 + 60 * np.arange(days * 1440)
    f = profile_fn(ts)
    r = 1e-4 * f * rng.standard_normal(ts.size) * math.sqrt(60.0)
    return ts, r


def test_seasonal_fit_recovers_profile_and_normalizes():
    rng = np.random.default_rng(2)

    def prof(ts):
        hod = (ts // 3600) % 24
        dow = ((ts // 86400) + 3) % 7
        return np.where(dow >= 5, 0.6, 1.0) * (1.0 + 0.8 * (hod == 14))

    ts, r = _synthetic_seasonal_returns(rng, prof)
    sv = SeasonalVol.fit(ts, r, 60.0, layout="day_type", bucket_s=3600)
    f = np.asarray(sv.factors)
    # ratios are what matter (normalization rescales)
    assert f[14] / f[3] == pytest.approx(1.8, rel=0.05)
    assert f[24 + 3] / f[3] == pytest.approx(0.6, rel=0.05)
    assert (5 * np.sum(f[:24] ** 2) + 2 * np.sum(f[24:] ** 2)) / (7 * 24) == pytest.approx(1.0)
    how = SeasonalVol.fit(ts, r, 60.0, layout="hour_of_week", bucket_s=3600)
    assert np.mean(np.asarray(how.factors) ** 2) == pytest.approx(1.0)
    # the fitted profile evaluated at timestamps matches the generating ratios
    vals = how.factors_at(np.array([T0 + 14 * 3600 + 5, T0 + 3 * 3600 + 5]))
    assert vals[0] / vals[1] == pytest.approx(1.8, rel=0.06)
    abs_fit = SeasonalVol.fit(ts, r, 60.0, layout="day_type", method="abs")
    assert abs_fit.factors[14] / abs_fit.factors[3] == pytest.approx(1.8, rel=0.05)


def test_seasonal_factor_scalar_vector_and_mean_var_factor():
    rng = np.random.default_rng(4)
    f = tuple(float(x) for x in 0.5 + rng.random(7 * 48))
    sv = SeasonalVol(factors=f, layout="hour_of_week", bucket_s=1800, tz="America/New_York")
    ts = T0 + rng.integers(0, 365 * 86400, size=200)
    vec = sv.factors_at(ts)
    sc = [sv.factor(int(t) * NS_PER_S) for t in ts]
    assert np.allclose(vec, sc)
    # exact piecewise integration vs brute force on a 1 s grid
    t0, t1 = T0 + 1234, T0 + 1234 + 5000
    brute = np.mean(sv.factors_at(np.arange(t0, t1) + 0.5) ** 2)
    assert sv.mean_var_factor(t0 * NS_PER_S, t1 * NS_PER_S) == pytest.approx(brute, rel=1e-9)
    t0v = np.array([T0 + 60 * 7, T0 + 3600 * 30])
    t1v = t0v + np.array([3600, 1200])
    got = sv.mean_var_factor_vec(t0v, t1v)
    for a, b, g in zip(t0v, t1v, got):
        assert g == pytest.approx(np.mean(sv.factors_at(np.arange(a, b) + 0.5) ** 2), rel=1e-9)
    # DST: 2025-03-09 is the spring-forward day in New York; UTC 13:30 is 09:30 local after it
    utc = SeasonalVol(factors=f, layout="hour_of_week", bucket_s=1800, tz="UTC")
    t_after = 1741527000  # 2025-03-09 13:30 UTC (Sunday) -> 09:30 EDT
    assert sv.factor(t_after * NS_PER_S) == pytest.approx(f[6 * 48 + 19])
    assert utc.factor(t_after * NS_PER_S) == pytest.approx(f[6 * 48 + 27])
    # flat
    assert FLAT_SEASONAL.factor(123) == 1.0 and FLAT_SEASONAL.mean_var_factor(0, 10**12) == 1.0
    rt = SeasonalVol.from_dict(sv.to_dict())
    assert rt == sv
    with pytest.raises(ValueError):
        SeasonalVol(factors=(1.0, 2.0), layout="time_of_day", bucket_s=3600)


def test_realized_measures():
    rng = np.random.default_rng(8)
    r = 1e-3 * rng.standard_normal(50_000)
    assert bipower_variation(r) == pytest.approx(realized_variance(r), rel=0.03)
    rj = r.copy()
    rj[100] += 0.4  # a jump (RV share 0.16 vs 0.05 diffusive)
    assert realized_variance(rj) > 3 * bipower_variation(rj)
    assert jump_share(rj) > 0.6 and jump_share(r) < 0.05
    # range estimators are unbiased for Brownian bars (checked coarsely by simulation)
    n, m = 4000, 400
    paths = np.cumsum(rng.standard_normal((n, m)) * math.sqrt(1.0 / m), axis=1) * 0.01
    paths = np.hstack([np.zeros((n, 1)), paths])
    P = np.exp(paths)
    o, c, h, l = P[:, 0], P[:, -1], P.max(axis=1), P.min(axis=1)
    assert np.mean(parkinson_var(h, l)) == pytest.approx(1e-4, rel=0.1)
    assert np.mean(garman_klass_var(o, h, l, c)) == pytest.approx(1e-4, rel=0.1)


def test_blended_sigma_and_forecaster():
    assert blended_sigma([2.0, 4.0], [0.25, 0.75], intercept_var=1.0, seasonal_ratio=2.0) == pytest.approx(math.sqrt(2 * (1 + 1 + 12)))
    with pytest.raises(ValueError):
        blended_sigma([1.0], [0.5, 0.5])
    rng = np.random.default_rng(9)
    times = np.arange(0, 86400 * 2, 1.0)
    p = gbm_path(rng, 5e-5, times)
    cfg = VolForecasterConfig(half_lives_s=(3600.0,), weights=(1.0,), min_dt_s=1.0, max_dt_s=None)
    fc = VolForecaster(cfg)
    ev = EwmaVol(3600.0, min_dt_s=1.0)
    for t, px in zip(times, p):
        fc.update(int(t * NS_PER_S), float(px))
        ev.update(int(t * NS_PER_S), float(px))
    now = int(times[-1] * NS_PER_S)
    assert fc.sigma(now) == pytest.approx(ev.sigma(now), rel=1e-12)
    assert fc.sigma_abs(now, now + 600 * NS_PER_S, 2.0) == pytest.approx(2.0 * ev.sigma(now), rel=1e-12)
    capped = VolForecaster(VolForecasterConfig(half_lives_s=(3600.0,), weights=(1.0,), min_dt_s=1.0, sigma_cap=1e-6))
    capped.update(0, 100.0)
    capped.update(NS_PER_S, 101.0)
    assert capped.sigma(NS_PER_S) == 1e-6
    # seasonal ratio scales the forecast
    prof = tuple([2.0 if h == 10 else 1.0 for h in range(24)])
    sv = SeasonalVol(factors=prof, layout="time_of_day", bucket_s=3600)
    fs = VolForecaster(cfg, seasonal=sv)
    fs.update(0, 100.0)
    fs.update(3600 * NS_PER_S, 100.5)  # a return in hour 0 (factor ~1)
    s_h0 = fs.sigma(3600 * NS_PER_S, 3601 * NS_PER_S)
    s_h10 = fs.sigma(10 * 3600 * NS_PER_S, 10 * 3600 * NS_PER_S + 60 * NS_PER_S)
    assert s_h10 / s_h0 == pytest.approx(2.0, rel=1e-9)
    with pytest.raises(ValueError):
        VolForecasterConfig(half_lives_s=(1.0, 2.0), weights=(1.0,))


def test_horizon_dependent_weights():
    cfg = VolForecasterConfig(
        half_lives_s=(600.0, 86400.0),
        weights=(0.5, 0.5),
        weights_by_horizon=((120.0, (1.0, 0.0)), (3600.0, (0.2, 0.8))),
        min_dt_s=1.0,
    )
    assert cfg.weights_for(None) == (0.5, 0.5)
    assert cfg.weights_for(60.0) == (1.0, 0.0)
    assert cfg.weights_for(7200.0) == (0.2, 0.8)
    mid = cfg.weights_for(1860.0)
    assert mid[0] == pytest.approx(0.6) and mid[1] == pytest.approx(0.4)
    fc = VolForecaster(cfg)
    fc.update(0, 100.0)
    fc.update(NS_PER_S, 100.0 * math.exp(1e-3))
    s_short = fc.sigma(NS_PER_S, NS_PER_S + 60 * NS_PER_S)
    s_long = fc.sigma(NS_PER_S, NS_PER_S + 3600 * NS_PER_S)
    # both EWMAs hold the same single sample, so any convex weights give the same sigma
    assert s_short == pytest.approx(s_long) == pytest.approx(1e-3)
    with pytest.raises(ValueError):
        VolForecasterConfig(half_lives_s=(1.0, 2.0), weights=(1.0, 0.0), weights_by_horizon=((10.0, (1.0,)),))
    with pytest.raises(ValueError):
        VolForecasterConfig(half_lives_s=(1.0,), weights=(1.0,), weights_by_horizon=((10.0, (1.0,)), (5.0, (1.0,))))
