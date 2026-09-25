from __future__ import annotations

import numpy as np
import pytest
from scipy.stats import norm

from dh.core.market import MarketSpec
from dh.strategy.scenario import (
    EventGrid,
    book_pnl,
    cvar_loss,
    marginal_risk_charge,
    payoff_vector,
    risk_stats,
    tail_pdf,
    tail_sf,
    worst_case_loss,
)


def spec(t, st="greater", floor=None, cap=None):
    return MarketSpec(ticker=t, event_ticker="E", series_ticker="KXBTCD", strike_type=st,
                      floor_strike=floor, cap_strike=cap, open_ts=0, close_ts=1, expiration_ts=1)


@pytest.mark.parametrize("tail,kw", [("gauss", {}), ("student_t", {"nu": 5.0}), ("vol_mixture", {"cv": 0.4})])
def test_tail_unit_variance_and_sf(tail, kw):
    x = np.linspace(-40, 40, 400001)
    p = tail_pdf(x, tail, **kw)
    dx = x[1] - x[0]
    assert abs(p.sum() * dx - 1) < 1e-4
    assert abs((p * x * x).sum() * dx - 1) < 2e-3
    # survival function consistent with density
    for z in (0.5, 1.5, 3.0):
        num = p[x > z].sum() * dx
        assert abs(num - float(tail_sf(np.array([z]), tail, **kw)[0])) < 2e-4


def test_grid_probability_matches_gaussian_closed_form():
    S, sd = 84000.0, 300.0
    g = EventGrid.build(n_obs=60, sum_fixed=0.0, m_remaining=60, mu_R=S, sd_R=sd, spot=S,
                        breakpoints_A=(83500.0, 84000.0, 84600.0))
    for K in (83500.0, 84000.0, 84600.0):
        p = g.prob(spec("a", floor=K))
        assert abs(p - (1 - norm.cdf((K - S) / sd))) < 1e-9


def test_partially_fixed_window_required_average():
    # 30 of 60 prints fixed averaging 84100; strike 84000 => need remaining avg > 83900.
    S, sd = 83950.0, 20.0
    g = EventGrid.build(n_obs=60, sum_fixed=30 * 84100.0, m_remaining=30, mu_R=S, sd_R=sd, spot=S,
                        breakpoints_A=(84000.0,))
    p = g.prob(spec("a", floor=84000.0))
    assert abs(p - (1 - norm.cdf((83900.0 - S) / sd))) < 1e-9


def test_stein_dollar_delta_and_hedged_variance():
    S, sd = 84000.0, 300.0
    g = EventGrid.build(n_obs=60, sum_fixed=0.0, m_remaining=60, mu_R=S, sd_R=sd, spot=S, n_points=4001,
                        breakpoints_A=(S,))
    m = spec("a", floor=S)
    pay = {"a": payoff_vector(m, g.A)}
    pnl = book_pnl(g, pay, {"a": 1.0}, {"a": 0.5})
    r = risk_stats(g, pnl, 0.0)
    assert abs(r.dollar_delta - norm.pdf(0) / sd) < 2e-5  # delta in BTC per contract
    hedged = book_pnl(g, pay, {"a": 1.0}, {"a": 0.5}, hedge_btc=-r.dollar_delta)
    rh = risk_stats(g, hedged, 0.0)
    assert abs(r.var - 0.25) < 2e-3 and abs(rh.var - (0.25 - norm.pdf(0) ** 2)) < 3e-3


def test_cross_strike_netting_reduces_marginal_charge():
    S, sd = 84000.0, 300.0
    g = EventGrid.build(n_obs=60, sum_fixed=0.0, m_remaining=60, mu_R=S, sd_R=sd, spot=S)
    a, b = spec("a", floor=83900.0), spec("b", floor=84100.0)
    pay = {"a": payoff_vector(a, g.A), "b": payoff_vector(b, g.A)}
    base = book_pnl(g, pay, {"a": 20.0}, {"a": 0.6})
    lam = 1e-2
    buy_more_a = marginal_risk_charge(g, base, pay["b"], 0.4, +1.0, lam, 0.0, 0.0)
    sell_b = marginal_risk_charge(g, base, pay["b"], 0.4, -1.0, lam, 0.0, 0.0)
    assert sell_b < buy_more_a  # selling YES at the nearby strike offsets long YES exposure


def test_worst_case_and_cvar():
    a = spec("a", floor=84000.0)
    wc = worst_case_loss({"a": a}, {"a": 10.0}, {"a": 0.7}, spot=84000.0)
    assert wc == pytest.approx(7.0)  # lose 10 * 0.70 if A <= K
    wc_short = worst_case_loss({"a": a}, {"a": -10.0}, {"a": 0.7}, spot=84000.0)
    assert wc_short == pytest.approx(3.0)  # sold YES at 0.70: lose 10 * 0.30 if A > K
    pnl = np.array([-10.0, 0.0, 1.0, 2.0])
    w = np.array([0.02, 0.03, 0.45, 0.5])
    assert cvar_loss(pnl, w, 0.05) == pytest.approx((0.02 * 10 + 0.03 * 0) / 0.05)


def test_fully_fixed_window_is_deterministic():
    g = EventGrid.build(n_obs=60, sum_fixed=60 * 84010.0, m_remaining=0, mu_R=84000.0, sd_R=0.0)
    assert g.prob(spec("a", floor=84000.0)) == 1.0
