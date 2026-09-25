from __future__ import annotations

import math

import numpy as np
import pytest

from dh.core.actions import CancelAll, Halt
from dh.core.book import KalshiBook
from dh.core.events import FeedStatus, KalshiBookSnapshot
from dh.core.market import MarketSpec
from dh.core.units import NS_PER_S
from dh.strategy.config import AdverseSelCfg, FillModelCfg, HedgeCfg, RiskCfg
from dh.strategy.fill_model import AdverseSelectionModel, FillIntensityModel, lognormal_call, lognormal_params
from dh.strategy.hedging import decide_hedge, no_trade_band
from dh.strategy.quoting import ExistingOrder, MarketQuoteContext, decide_side
from dh.strategy.risk import RiskEngine
from dh.strategy.scenario import EventGrid, book_pnl, payoff_vector


def test_lognormal_call_matches_mc():
    mu, sg = lognormal_params(20.0, 2.0)
    x = np.random.default_rng(0).lognormal(mu, sg, 2_000_000)
    for a in (0.0, 5.0, 20.0, 80.0):
        assert lognormal_call(mu, sg, a) == pytest.approx(np.maximum(x - a, 0).mean(), rel=0.02)


def test_intensity_monotone_in_queue():
    m = FillIntensityModel(FillModelCfg())
    k = (">30m", "atm", "bid")
    vals = [m.intensity(key=k, q_eff=q, size=5, position="touch") for q in (0, 10, 50, 200)]
    assert all(a > b for a, b in zip(vals, vals[1:]))
    assert m.intensity(key=k, q_eff=10, size=5, position="behind") < vals[1]


def _ctx(F=0.50, band=0.0, bb=4800, ba=5200, existing=None, capacity=10.0, tau=1800.0):
    spec = MarketSpec(ticker="M", event_ticker="E", series_ticker="KXBTCD", strike_type="greater",
                      floor_strike=84000.0, cap_strike=None, open_ts=0, close_ts=1, expiration_ts=1)
    book = KalshiBook("M")
    book.apply_snapshot(KalshiBookSnapshot(ts=0, ts_exch=0, ticker="M", sid=1, seq=1,
                                           yes_bids=((bb, 5000),), no_bids=((10000 - ba, 5000),)))
    grid = EventGrid.build(n_obs=60, sum_fixed=0.0, m_remaining=60, mu_R=84000.0, sd_R=300.0,
                           spot=84000.0, breakpoints_A=(84000.0,))
    pay = payoff_vector(spec, grid.A)
    return MarketQuoteContext(
        spec=spec, book=book, F=F, F_lo=F - band, F_hi=F + band, delta_btc=0.0013, tau_s=tau, z=0.0,
        maker_fee=lambda px: 0.0175 * (px / 1e4) * (1 - px / 1e4), dF_recent=0.0, grid=grid,
        base_pnl=np.zeros_like(grid.A), payoff_k=pay, lam=1e-3, lambda_tail=0.0, tail_budget=50.0,
        D_btc=0.0, hedge_cost_frac=6e-4, spot=84000.0, rho_hedged=0.0, clip_contracts=5.0,
        capacity_contracts={"bid": capacity, "ask": capacity}, existing=existing or {},
    )


def test_decide_side_places_inside_wide_spread_and_never_crosses():
    ctx = _ctx()
    fm, am = FillIntensityModel(FillModelCfg()), AdverseSelectionModel(AdverseSelCfg())
    d = decide_side(ctx, "bid", fm, am, v_min=0.001, kappa_replace=0.0)
    assert d.place is not None and d.place.px < 5200 and d.place.value >= 0.001
    a = decide_side(ctx, "ask", fm, am, v_min=0.001, kappa_replace=0.0)
    assert a.place is not None and a.place.px > 4800


def test_no_quote_when_edge_insufficient():
    ctx = _ctx(bb=4990, ba=5010)  # 2c-wide market at fair 50c, 5c adverse selection: nothing pays
    fm, am = FillIntensityModel(FillModelCfg()), AdverseSelectionModel(AdverseSelCfg(a0=0.05))
    d = decide_side(ctx, "bid", fm, am, v_min=0.001, kappa_replace=0.0)
    assert d.place is None


def test_existing_order_kept_or_canceled():
    fm, am = FillIntensityModel(FillModelCfg()), AdverseSelectionModel(AdverseSelCfg())
    good = _ctx(existing={"bid": [ExistingOrder("c1", 4800, 500, 3.0)]})
    d = decide_side(good, "bid", fm, am, v_min=0.001, kappa_replace=1.0)
    assert d.keep == ["c1"] and d.place is None
    bad = _ctx(F=0.40, existing={"bid": [ExistingOrder("c1", 4800, 500, 3.0)]})
    d2 = decide_side(bad, "bid", fm, am, v_min=0.001, kappa_replace=0.0)
    assert "c1" in d2.cancel


def test_zero_capacity_means_no_new_quote():
    ctx = _ctx(capacity=0.0)
    d = decide_side(ctx, "bid", FillIntensityModel(FillModelCfg()), AdverseSelectionModel(AdverseSelCfg()), 0.001, 0.0)
    assert d.place is None


def test_hedge_band_and_boundary_trade():
    cfg = HedgeCfg(enabled=True, band_min_btc=0.01)
    b = no_trade_band(cfg, spot=84000.0, sigma_abs_per_sqrt_s=5.27, h_eff_s=1800, lam=1e-4)
    assert b > 1.0  # M1-scale risk aversion: band far above reachable delta
    d = decide_hedge(cfg, D_btc=0.5, spot=84000.0, sigma_abs_per_sqrt_s=5.27, h_eff_s=1800, lam=1e-4,
                     force_band_btc=0.2)
    assert d.target_btc == pytest.approx(-0.3) and not d.urgent
    d2 = decide_hedge(cfg, D_btc=0.5, pending_btc=-0.3, spot=84000.0, sigma_abs_per_sqrt_s=5.27, h_eff_s=1800,
                      lam=1e-4, force_band_btc=0.2)
    assert d2.target_btc == 0.0
    off = decide_hedge(HedgeCfg(enabled=False), D_btc=5.0, spot=84000.0, sigma_abs_per_sqrt_s=5.27,
                       h_eff_s=1800, lam=1e-4)
    assert off.target_btc == 0.0


def test_risk_engine_capacity_and_halts():
    r = RiskEngine(RiskCfg(max_pos_per_market=25))
    assert r.market_capacity(side="bid", position=10, working_bid=5, working_ask=0, tau_s=3000) == 10
    assert r.market_capacity(side="ask", position=10, working_bid=0, working_ask=5, tau_s=3000) == 30
    assert r.market_capacity(side="bid", position=0, working_bid=0, working_ask=0, tau_s=100) == 12.5
    t0 = 10 * NS_PER_S
    assert not r.health(t0).quoting_allowed  # kalshi not connected yet
    r.on_feed_status(FeedStatus(ts=t0, ts_exch=0, stream="kalshi.ws", status="connected"))
    r.note_brti(t0)
    assert not r.health(t0 + NS_PER_S).quoting_allowed  # 5 s settle time after (re)connect
    assert r.health(t0 + 6 * NS_PER_S).quoting_allowed
    acts = r.on_feed_status(FeedStatus(ts=t0 + 7 * NS_PER_S, ts_exch=0, stream="kalshi.ws", status="gap"))
    assert any(isinstance(a, CancelAll) for a in acts)
    r.on_equity(t0, 0.0)
    acts = r.on_equity(t0 + 1, -80.0)
    assert any(isinstance(a, Halt) for a in acts) and r.halted_all


def test_behind_quotes_carry_more_adverse_selection():
    am = AdverseSelectionModel(AdverseSelCfg(a0=0.01, behind_mult=2.0))
    k = (">30m", "atm", "bid")
    assert am.expected(key=k, adverse_recent_move=0, tau_s=1000, position="behind") == pytest.approx(
        2 * am.expected(key=k, adverse_recent_move=0, tau_s=1000, position="touch"))
