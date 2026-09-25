from __future__ import annotations

import pytest

from dh.core.book import ExtBook, KalshiBook
from dh.core.events import KalshiBookDelta, KalshiBookSnapshot
from dh.core.market import MarketSpec, PriceRange, SettlementSpec
from dh.core.strategy import IdGen
from dh.core.units import (
    UnitError,
    contracts,
    micros_from_dollars,
    notional_micros,
    px_from_dollars,
    px_to_dollars,
    qty_from_fp,
    qty_to_fp,
)


def test_units_roundtrip():
    assert px_from_dollars("0.5600") == 5600
    assert px_from_dollars("0.56") == 5600
    assert px_from_dollars("0.0001") == 1
    assert px_to_dollars(5600) == "0.5600"
    assert px_to_dollars(10_000) == "1.0000"
    assert qty_from_fp("10.00") == 1000
    assert qty_to_fp(1234) == "12.34"
    assert qty_to_fp(-5) == "-0.05"
    assert contracts(3) == 300
    assert micros_from_dollars("0.004375") == 4375
    # 10 contracts at $0.56 = $5.60 = 5_600_000 micros
    assert notional_micros(5600, 1000) == 5_600_000
    with pytest.raises(UnitError):
        px_from_dollars("0.00005")
    with pytest.raises(UnitError):
        qty_from_fp("1.001")
    with pytest.raises(TypeError):
        px_from_dollars(0.56)  # type: ignore[arg-type]


def _spec(**kw):
    base = dict(
        ticker="KXBTCD-T",
        event_ticker="KXBTCD-E",
        series_ticker="KXBTCD",
        strike_type="greater",
        floor_strike=85000.0,
        cap_strike=None,
        open_ts=0,
        close_ts=3_600_000_000_000,
        expiration_ts=3_600_000_000_000,
    )
    base.update(kw)
    return MarketSpec(**base)


def test_market_payoff_and_ticks():
    m = _spec()
    assert m.yes_wins(85000.01) and not m.yes_wins(85000.0)
    ge = _spec(strike_type="greater_or_equal")
    assert ge.yes_wins(85000.0)
    btw = _spec(strike_type="between", floor_strike=85000.0, cap_strike=85249.99)
    assert btw.yes_wins(85000.0) and btw.yes_wins(85249.99) and not btw.yes_wins(85250.0)
    with pytest.raises(ValueError):
        _spec(strike_type="functional")
    tapered = _spec(
        price_ranges=(PriceRange(10, 1000, 10), PriceRange(1000, 9000, 100), PriceRange(9000, 9990, 10))
    )
    assert tapered.is_valid_px(10) and tapered.is_valid_px(1500) and not tapered.is_valid_px(1510)
    assert tapered.next_tick_up(990) == 1000 and tapered.next_tick_up(1000) == 1100
    assert tapered.next_tick_down(1000) == 990


def test_settlement_obs_times():
    s = SettlementSpec()
    T = 3_600 * 10**9
    obs = s.obs_times(T)
    assert len(obs) == 60 and obs[-1] == T and obs[0] == T - 59 * 10**9


def test_kalshi_book_yes_view_and_invalidation():
    b = KalshiBook("X")
    snap = KalshiBookSnapshot(ts=1, ts_exch=0, ticker="X", sid=1, seq=1,
                              yes_bids=((4500, 1000), (4400, 500)), no_bids=((5300, 700), (5000, 100)))
    b.apply_snapshot(snap)
    assert b.best_bid() == 4500 and b.best_ask() == 4700 and b.spread() == 200
    assert b.ask_qty(4700) == 700 and b.bids(1) == [(4500, 1000)] and b.asks(2) == [(4700, 700), (5000, 100)]
    assert b.apply_delta(KalshiBookDelta(ts=2, ts_exch=0, ticker="X", sid=1, seq=2, side="no", px=5300, delta=-700))
    assert b.best_ask() == 5000
    assert not b.apply_delta(KalshiBookDelta(ts=3, ts_exch=0, ticker="X", sid=1, seq=3, side="yes", px=4500, delta=-2000))
    assert not b.valid


def test_ext_book_impact():
    e = ExtBook("coinbase", "BTC-USD")
    e.snapshot([(100.0, 1.0), (99.0, 2.0)], [(101.0, 1.0), (102.0, 3.0)], ts=1)
    assert e.top().mid == 100.5
    assert e.impact_price("buy", 2.0) == pytest.approx(101.5)
    e.update("a", 101.0, 0)
    assert e.top().ask == 102.0


def test_idgen():
    g = IdGen("dhA")
    assert g.next() == "dhA-1" and g.next() == "dhA-2"
