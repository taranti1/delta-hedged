from __future__ import annotations

import math

import numpy as np
import pytest
from scipy import integrate, stats

from dh.core.market import MarketSpec
from dh.models import (
    GAUSS,
    digital_band,
    EmpiricalTail,
    Gauss,
    StudentT,
    VolMixture,
    avg_variance_time,
    avg_variance_time_general,
    digital,
    digital_vec,
    make_tail,
    remaining_avg_variance_time,
)
from dh.settlement import WindowState

TAILS = [Gauss(), StudentT(3.5), StudentT(8.0), VolMixture(0.4), VolMixture(0.9)]


def spec(strike_type="greater", floor=100_000.0, cap=None):
    return MarketSpec(
        ticker="KXBTCD-X",
        event_ticker="KXBTCD-E",
        series_ticker="KXBTCD",
        strike_type=strike_type,
        floor_strike=floor,
        cap_strike=cap,
        open_ts=0,
        close_ts=0,
        expiration_ts=0,
    )


def ws_(k=0, m=60, tau=600.0, step=1.0, sum_fixed=None, fixed_level=100_000.0):
    sf = k * fixed_level if sum_fixed is None else sum_fixed
    return WindowState(n_obs=k + m, k_fixed=k, sum_fixed=sf, m_remaining=m, tau_first_s=tau, step_s=step)


# ----------------------------------------------------------------------------- variance time
@pytest.mark.parametrize("m", [1, 2, 3, 10, 59, 60, 200])
@pytest.mark.parametrize("tau", [0.0, 0.37, 30.0, 3541.0])
@pytest.mark.parametrize("step", [1.0, 0.2, 5.0])
def test_variance_time_formula_equals_double_sum(m, tau, step):
    taus = tau + step * np.arange(m)
    brute = np.minimum.outer(taus, taus).sum() / m**2
    ws = ws_(k=60 - m if m <= 60 else 0, m=m, tau=tau, step=step)
    assert remaining_avg_variance_time(ws) == pytest.approx(brute, rel=1e-12, abs=1e-12)
    assert avg_variance_time(tau, step, m) == pytest.approx(brute, rel=1e-12, abs=1e-12)
    assert avg_variance_time_general(taus[::-1]) == pytest.approx(brute, rel=1e-12, abs=1e-12)


def test_variance_time_known_values_and_vectorized():
    # full 60-print window: tau_first + 19.50 s
    assert avg_variance_time(0.0, 1.0, 60) == pytest.approx(7381 / 360 - 1)
    assert avg_variance_time(100.0, 1.0, 60) - 100.0 == pytest.approx(19.5028, abs=1e-4)
    assert avg_variance_time(5.0, 1.0, 1) == 5.0
    assert avg_variance_time(5.0, 1.0, 0) == 0.0
    v = avg_variance_time(np.array([0.0, 10.0]), 1.0, np.array([60, 0]))
    assert v[0] == pytest.approx(19.5028, abs=1e-4) and v[1] == 0.0


def test_variance_time_monte_carlo():
    rng = np.random.default_rng(7)
    m, tau, step = 60, 12.5, 1.0
    taus = tau + step * np.arange(m)
    dts = np.diff(np.r_[0.0, taus])
    W = np.cumsum(rng.standard_normal((100_000, m)) * np.sqrt(dts), axis=1)
    mc = W.mean(axis=1).var()
    theory = avg_variance_time(tau, step, m)
    # sampling sd of a variance estimate ~ sqrt(2/N)
    assert mc == pytest.approx(theory, rel=4 * math.sqrt(2 / 100_000))


# ----------------------------------------------------------------------------- Monte Carlo agreement
def _mc_average(rng, n_paths, spot, sigma_abs, ws, vol_scale=None):
    taus = ws.tau_first_s + ws.step_s * np.arange(ws.m_remaining)
    dts = np.diff(np.r_[0.0, taus])
    z = rng.standard_normal((n_paths, ws.m_remaining)) * np.sqrt(dts)
    if vol_scale is not None:
        z *= vol_scale[:, None]
    S = spot + sigma_abs * np.cumsum(z, axis=1)
    return (ws.sum_fixed + S.sum(axis=1)) / ws.n_obs


@pytest.mark.parametrize(
    "k,m,tau",
    [(0, 60, 240.0), (20, 40, 0.3), (45, 15, 0.9), (59, 1, 0.5)],
)
def test_gauss_digital_matches_monte_carlo_with_fixed_prints(k, m, tau):
    rng = np.random.default_rng(1234 + k)
    spot, sigma_abs = 100_000.0, 6.0
    # the fixed prints averaged 30 $ below spot
    ws = ws_(k=k, m=m, tau=tau, fixed_level=spot - 30.0)
    A = _mc_average(rng, 200_000, spot, sigma_abs, ws)
    for K in (spot - 80.0, spot - 25.0, spot - 5.0, spot + 10.0, spot + 60.0):
        for st, flo, cap in (("greater", K, None), ("less", None, K), ("between", K - 20.0, K + 15.0)):
            d = digital(spec(st, flo, cap), ws, spot, sigma_abs, "gauss")
            mc = np.mean([spec(st, flo, cap).yes_wins(a) for a in A[:40_000]]) if st == "between" else (
                np.mean(A > K) if st == "greater" else np.mean(A < K)
            )
            n = 40_000 if st == "between" else A.size
            se = math.sqrt(max(d.p_yes * (1 - d.p_yes), 1e-4) / n)
            assert abs(d.p_yes - mc) < 4.5 * se + 1e-4, (st, K, d.p_yes, mc)


def test_vol_mixture_matches_monte_carlo_scale_mixture():
    rng = np.random.default_rng(99)
    cv = 0.7
    om2 = math.log1p(cv * cv)
    n = 400_000
    s = np.exp(-om2 + math.sqrt(om2) * rng.standard_normal(n))  # E[s^2] = 1, sd(s)/E[s] = cv
    spot, sigma_abs = 100_000.0, 5.0
    ws = ws_(k=10, m=50, tau=120.0, fixed_level=spot)
    A = _mc_average(rng, n, spot, sigma_abs, ws, vol_scale=s)
    tail = VolMixture(cv)
    for K in (spot + 10.0, spot + 60.0, spot + 120.0, spot + 180.0):
        d = digital(spec("greater", K), ws, spot, sigma_abs, tail)
        mc = float(np.mean(A > K))
        se = math.sqrt(max(d.p_yes * (1 - d.p_yes), 1e-5) / n)
        assert abs(d.p_yes - mc) < 4.5 * se, (K, d.p_yes, mc)


# ----------------------------------------------------------------------------- tail models
@pytest.mark.parametrize("tail", TAILS, ids=lambda t: t.describe())
def test_tail_models_are_unit_variance_symmetric_densities(tail):
    var = integrate.quad(lambda z: z * z * tail.pdf(z), -np.inf, np.inf, limit=400)[0]
    mass = integrate.quad(lambda z: tail.pdf(z), -np.inf, np.inf, limit=400)[0]
    assert mass == pytest.approx(1.0, abs=1e-7)
    assert var == pytest.approx(1.0, rel=2e-5)
    z = np.linspace(-8, 8, 161)
    assert np.allclose(tail.cdf(z) + tail.sf(z), 1.0, atol=1e-12)
    assert np.allclose(tail.cdf(-z), tail.sf(z), atol=1e-14)
    # pdf is the derivative of the cdf; dpdf the derivative of the pdf
    h = 1e-5
    assert np.allclose((tail.cdf(z + h) - tail.cdf(z - h)) / (2 * h), tail.pdf(z), atol=1e-7)
    assert np.allclose((tail.pdf(z + h) - tail.pdf(z - h)) / (2 * h), tail.dpdf(z), atol=1e-6)
    q = np.array([1e-4, 0.01, 0.3, 0.5, 0.9, 0.999])
    assert np.allclose(tail.cdf(tail.ppf(q)), q, atol=1e-6)


def test_student_t_matches_scipy_and_limits():
    nu = 4.5
    t = StudentT(nu)
    sc = math.sqrt((nu - 2) / nu)
    z = np.array([-6.0, -2.0, 0.0, 1.0, 3.0, 10.0])
    assert np.allclose(t.cdf(z), stats.t.cdf(z / sc, nu), atol=1e-13)
    assert np.allclose(t.pdf(z), stats.t.pdf(z / sc, nu) / sc, atol=1e-13)
    # deep upper tail computed without cancellation
    assert t.sf(40.0) == pytest.approx(stats.t.sf(40.0 / sc, nu), rel=1e-9)
    # large nu -> Gauss
    assert StudentT(1e6).sf(2.0) == pytest.approx(GAUSS.sf(2.0), rel=1e-4)
    with pytest.raises(ValueError):
        StudentT(2.0)


def test_vol_mixture_limits_and_kurtosis():
    assert VolMixture(0.0).sf(2.3) == pytest.approx(GAUSS.sf(2.3), rel=1e-12)
    cv = 0.5
    assert VolMixture(cv).kurtosis() == pytest.approx(3 * (1 + cv * cv) ** 4, rel=1e-6)
    # fatter tails than Gauss at 3 sd, thinner shoulders at 1 sd
    assert VolMixture(cv).sf(3.0) > GAUSS.sf(3.0)
    assert VolMixture(cv).sf(1.0) < GAUSS.sf(1.0)


def test_empirical_tail_approximates_gauss():
    rng = np.random.default_rng(3)
    u = rng.standard_normal(20_000)
    e = EmpiricalTail(u)
    z = np.array([-2.0, -1.0, 0.0, 0.5, 2.0])
    assert np.allclose(e.cdf(z), GAUSS.cdf(z), atol=0.01)
    assert np.all(np.diff(e.cdf(np.linspace(-6, 6, 200))) >= -1e-12)
    assert e.variance() == pytest.approx(1.0, abs=0.05)
    es = EmpiricalTail(3.0 * u + 1.0, standardize=True)
    assert es.cdf(0.0) == pytest.approx(0.5, abs=0.01)


def test_make_tail_parsing():
    assert make_tail("gauss") is GAUSS and make_tail(None) is GAUSS
    assert make_tail("student_t(5)") == StudentT(5.0)
    assert make_tail("t(4.5)") == StudentT(4.5)
    assert make_tail("vol_mixture(0.6)") == VolMixture(0.6)
    assert make_tail({"kind": "mixture", "cv": 0.3}) == VolMixture(0.3)
    t = StudentT(7.0)
    assert make_tail(t) is t
    with pytest.raises(ValueError):
        make_tail("cauchy(1)")
    with pytest.raises(ValueError):
        make_tail("student_t")


# ----------------------------------------------------------------------------- parity + greeks
@pytest.mark.parametrize("tail", TAILS, ids=lambda t: t.describe())
@pytest.mark.parametrize("k", [0, 30, 58])
def test_parity_relations(tail, k):
    spot, sig = 100_000.0, 4.0
    ws = ws_(k=k, m=60 - k, tau=0.5 if k else 300.0, fixed_level=spot + 12.0)
    for K in (spot - 150.0, spot - 3.0, spot + 20.0, spot + 400.0):
        g = digital(spec("greater", K), ws, spot, sig, tail)
        ge = digital(spec("greater_or_equal", K), ws, spot, sig, tail)
        le = digital(spec("less_or_equal", None, K), ws, spot, sig, tail)
        lt = digital(spec("less", None, K), ws, spot, sig, tail)
        assert g.p_yes + le.p_yes == pytest.approx(1.0, abs=1e-12)
        assert ge.p_yes + lt.p_yes == pytest.approx(1.0, abs=1e-12)
        assert g.delta + le.delta == pytest.approx(0.0, abs=1e-15)
        assert g.gamma + le.gamma == pytest.approx(0.0, abs=1e-15)
        lo, hi = K - 250.0, K
        b = digital(spec("between", lo, hi), ws, spot, sig, tail)
        ga = digital(spec("greater_or_equal", lo), ws, spot, sig, tail)
        assert b.p_yes == pytest.approx(ga.p_yes - g.p_yes, abs=1e-12)
        assert b.delta == pytest.approx(ga.delta - g.delta, abs=1e-15)
        # symmetric tails: P(A > K | spot) == P(A < K' | spot) reflected around the forecast mean
        mean_final = (ws.sum_fixed + ws.m_remaining * spot) / ws.n_obs
        refl = digital(spec("less", None, 2 * mean_final - K), ws, spot, sig, tail)
        assert refl.p_yes == pytest.approx(g.p_yes, abs=1e-9)


@pytest.mark.parametrize("tail", TAILS, ids=lambda t: t.describe())
@pytest.mark.parametrize("st", ["greater", "less_or_equal", "between"])
@pytest.mark.parametrize("k,tau", [(0, 900.0), (40, 0.4)])
def test_delta_gamma_match_finite_differences(tail, st, k, tau):
    spot, sig = 100_000.0, 5.0
    ws = ws_(k=k, m=60 - k, tau=tau, fixed_level=spot - 20.0)
    sd = sig * math.sqrt(remaining_avg_variance_time(ws))
    h = sd * 1e-3
    for zk in (-1.7, -0.4, 0.0, 0.9, 2.6):
        K = spot + zk * sd * ws.m_remaining / ws.n_obs + (ws.sum_fixed + ws.m_remaining * spot) / ws.n_obs - spot
        sp = spec(st, K if st != "less_or_equal" else None, K if st == "less_or_equal" else None) if st != "between" else spec(st, K - 0.6 * sd, K + 0.5 * sd)
        d0 = digital(sp, ws, spot, sig, tail)
        up = digital(sp, ws, spot + h, sig, tail)
        dn = digital(sp, ws, spot - h, sig, tail)
        fd_delta = (up.p_yes - dn.p_yes) / (2 * h)
        fd_gamma = (up.p_yes - 2 * d0.p_yes + dn.p_yes) / (h * h)
        scale_d = 1.0 / sd
        # central differences: O(h^2) truncation (sharp-peaked mixtures) -> relative tolerance
        assert d0.delta == pytest.approx(fd_delta, rel=1e-4, abs=1e-7 * scale_d)
        assert d0.gamma == pytest.approx(fd_gamma, rel=2e-3, abs=2e-4 * scale_d / sd)
        # delta by differentiating w.r.t. the fixed prints is the same object: sign conventions
        if st == "greater":
            assert d0.delta > 0
        if st == "less_or_equal":
            assert d0.delta < 0


def test_greeks_scale_and_hedge_notional_example():
    # the premise doc example: BTC $84,541, 35% annual vol, 60 min, ATM -> hedge notional ~ $107
    spot = 84_541.0
    sigma_abs = spot * 0.35 / math.sqrt(365.25 * 86400)
    ws = ws_(k=0, m=60, tau=3600.0 - 59.0)
    d = digital(spec("greater", spot), ws, spot, sigma_abs, "gauss")
    assert d.p_yes == pytest.approx(0.5)
    assert d.delta * spot == pytest.approx(107.3, abs=0.2)
    assert d.gamma == pytest.approx(0.0, abs=1e-15)
    otm = digital(spec("greater", spot + d.sd_remaining), ws, spot, sigma_abs, "gauss")
    assert otm.gamma > 0 and otm.z == pytest.approx(1.0)


# ----------------------------------------------------------------------------- limiting cases
def test_all_fixed_is_deterministic_and_respects_strictness():
    ws = WindowState(n_obs=60, k_fixed=60, sum_fixed=60 * 100_000.0, m_remaining=0, tau_first_s=0.0, step_s=1.0)
    for tail in ("gauss", StudentT(4.0)):
        assert digital(spec("greater", 100_000.0), ws, 1.0, 5.0, tail).p_yes == 0.0
        assert digital(spec("greater_or_equal", 100_000.0), ws, 1.0, 5.0, tail).p_yes == 1.0
        assert digital(spec("less_or_equal", None, 100_000.0), ws, 1.0, 5.0, tail).p_yes == 1.0
        assert digital(spec("less", None, 100_000.0), ws, 1.0, 5.0, tail).p_yes == 0.0
        d = digital(spec("greater", 99_999.99), ws, 1e9, 5.0, tail)  # spot is irrelevant now
        assert d.p_yes == 1.0 and d.delta == 0.0 and d.gamma == 0.0 and d.z == -math.inf
        assert digital(spec("between", 99_990.0, 100_000.0), ws, 0.0, 5.0, tail).p_yes == 1.0
        assert digital(spec("between", 99_990.0, 99_999.0), ws, 0.0, 5.0, tail).p_yes == 0.0


def test_zero_vol_and_tau_to_zero():
    ws = ws_(k=59, m=1, tau=0.0, fixed_level=100_000.0)
    # one print left, due now: no randomness -> step function of spot
    assert digital(spec("greater", 100_000.0), ws, 100_000.6, 5.0, "gauss").p_yes == 1.0
    assert digital(spec("greater", 100_000.0), ws, 99_999.4, 5.0, "gauss").p_yes == 0.0
    # sigma 0 with many prints remaining: deterministic too
    ws2 = ws_(k=0, m=60, tau=100.0)
    assert digital(spec("greater", 100.0), ws2, 101.0, 0.0).p_yes == 1.0
    # tau -> 0: continuity of p as tau_first shrinks
    # strike such that the last print must beat spot by $1: K_req - spot = 1
    K = (59 * 100_000.0 + 100_001.0) / 60
    ps = [digital(spec("greater", K), ws_(k=59, m=1, tau=t, fixed_level=100_000.0), 100_000.0, 5.0).p_yes for t in (1.0, 0.1, 0.01, 1e-6)]
    assert all(np.diff(ps) < 0) and ps[0] == pytest.approx(GAUSS.sf(0.2)) and ps[-1] < 1e-12


def test_far_strikes_are_finite_and_saturate():
    ws = ws_(k=0, m=60, tau=3000.0)
    for tail in TAILS:
        far_up = digital(spec("greater", 1e7), ws, 100_000.0, 5.0, tail)
        far_dn = digital(spec("greater", 1.0), ws, 100_000.0, 5.0, tail)
        assert 0.0 <= far_up.p_yes < 1e-9 and far_dn.p_yes > 1 - 1e-9
        for d in (far_up, far_dn):
            assert all(math.isfinite(v) for v in (d.p_yes, d.delta, d.gamma, d.z))
    # monotone in strike
    Ks = np.linspace(99_000, 101_000, 41)
    ps = [digital(spec("greater", K), ws, 100_000.0, 5.0, VolMixture(0.8)).p_yes for K in Ks]
    assert np.all(np.diff(ps) < 0)


def test_digital_vec_matches_scalar():
    spot, sig = 100_000.0, 4.0
    ws = ws_(k=25, m=35, tau=0.7, fixed_level=spot + 5.0)
    sd = sig * math.sqrt(remaining_avg_variance_time(ws))
    Ks = np.linspace(spot - 300, spot + 300, 13)
    for tail in (GAUSS, StudentT(5.0), VolMixture(0.5)):
        for st in ("greater", "greater_or_equal", "less", "less_or_equal", "between"):
            if st == "between":
                v = digital_vec(st, spot, sd, tail, floor=Ks, cap=Ks + 100.0, n_obs=60, k_fixed=25, sum_fixed=ws.sum_fixed)
                sc = [digital(spec(st, K, K + 100.0), ws, spot, sig, tail) for K in Ks]
            elif st.startswith("greater"):
                v = digital_vec(st, spot, sd, tail, floor=Ks, n_obs=60, k_fixed=25, sum_fixed=ws.sum_fixed)
                sc = [digital(spec(st, K), ws, spot, sig, tail) for K in Ks]
            else:
                v = digital_vec(st, spot, sd, tail, cap=Ks, n_obs=60, k_fixed=25, sum_fixed=ws.sum_fixed)
                sc = [digital(spec(st, None, K), ws, spot, sig, tail) for K in Ks]
            assert np.allclose(v.p_yes, [d.p_yes for d in sc], atol=1e-13)
            assert np.allclose(v.delta, [d.delta for d in sc], atol=1e-15)
            assert np.allclose(v.gamma, [d.gamma for d in sc], atol=1e-15)
            assert np.allclose(v.z, [d.z for d in sc])
    # determined elements inside an array
    v = digital_vec("greater", spot, np.array([sd, 0.0, sd]), GAUSS, floor=np.array([spot, spot - 1, spot + 1]),
                    n_obs=60, k_fixed=np.array([0, 0, 60]), sum_fixed=np.array([0.0, 0.0, 60 * spot]))
    assert v.p_yes[0] == pytest.approx(0.5) and v.p_yes[1] == 1.0 and v.p_yes[2] == 0.0
    assert v.delta[1] == 0.0 and v.delta[2] == 0.0 and v.sd_remaining[2] == 0.0
    with pytest.raises(ValueError):
        digital_vec("greater", spot, sd, GAUSS)


def test_nowcast_sd_adds_in_quadrature_and_smooths_the_last_print():
    spot = 100_000.0
    ws = ws_(k=59, m=1, tau=0.0, fixed_level=spot)
    # without nowcast noise the last print is known -> step function; with it -> smooth
    # average = spot + 0.5/60 = spot + 0.0083 > K = spot + 0.005 (required last print: spot + 0.3)
    assert digital(spec("greater", spot + 0.005), ws, spot + 0.5, 5.0).p_yes == 1.0
    d = digital(spec("greater", spot + 0.005), ws, spot + 0.5, 5.0, nowcast_sd=2.0)
    assert d.z == pytest.approx((0.3 - 0.5) / 2.0)
    assert 0.5 < d.p_yes < 1.0 and d.delta > 0 and d.sd_remaining == pytest.approx(2.0)
    # quadrature with the diffusion sd
    ws2 = ws_(k=0, m=60, tau=100.0)
    base = digital(spec("greater", spot + 30.0), ws2, spot, 3.0)
    noisy = digital(spec("greater", spot + 30.0), ws2, spot, 3.0, nowcast_sd=4.0)
    assert noisy.sd_remaining == pytest.approx(math.hypot(base.sd_remaining, 4.0))
    v = digital_vec("greater", spot, base.sd_remaining, GAUSS, floor=spot + 30.0, nowcast_sd=4.0)
    assert v.p_yes[()] == pytest.approx(noisy.p_yes)
    # all prints fixed: nowcast noise is irrelevant
    final = WindowState(n_obs=60, k_fixed=60, sum_fixed=60 * spot, m_remaining=0, tau_first_s=0.0, step_s=1.0)
    assert digital(spec("greater", spot - 1), final, 0.0, 5.0, nowcast_sd=50.0).p_yes == 1.0
    with pytest.raises(ValueError):
        digital(spec("greater", spot), ws2, spot, 3.0, nowcast_sd=-1.0)


def test_digital_band_brackets_scenarios():
    spot, sig = 100_000.0, 4.0
    ws = ws_(k=0, m=60, tau=600.0)
    sd = sig * math.sqrt(remaining_avg_variance_time(ws))
    sp = spec("greater", spot + 2.2 * sd)
    b = digital_band(sp, ws, spot, [sig, 0.8 * sig, 1.25 * sig], [StudentT(4.5), GAUSS], nowcast_sd_values=[0.0, 5.0])
    ps = [digital(sp, ws, spot, s_, t, nowcast_sd=n).p_yes for s_ in (sig, 0.8 * sig, 1.25 * sig)
          for t in (StudentT(4.5), GAUSS) for n in (0.0, 5.0)]
    assert b.p_lo == pytest.approx(min(ps)) and b.p_hi == pytest.approx(max(ps))
    assert b.p_lo <= b.center.p_yes <= b.p_hi
    assert b.center.p_yes == pytest.approx(digital(sp, ws, spot, sig, StudentT(4.5)).p_yes)
    # at the money the band collapses for symmetric models (vol does not move P = 0.5)
    atm = digital_band(spec("greater", spot), ws, spot, [sig, 2 * sig], [GAUSS, StudentT(3.0)])
    assert atm.p_lo == pytest.approx(0.5) and atm.p_hi == pytest.approx(0.5)
    with pytest.raises(ValueError):
        digital_band(sp, ws, spot, [], [GAUSS])
