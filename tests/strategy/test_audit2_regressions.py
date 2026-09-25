"""MarketMaker regressions for the second audit (M6, M7, m2, m3); each scenario is the
auditor's reproducer, with a control run showing the market is quoted when the hazard is absent."""
from __future__ import annotations

from dataclasses import replace

from dh.backtest.kat import default_kat_config
from dh.core.actions import CancelOrder, PlaceOrder
from dh.core.events import (
    CancelAck,
    FeedStatus,
    KalshiBookSnapshot,
    KalshiFeeUpdate,
    KalshiFill,
    KalshiMarketLifecycle,
    OrderAck,
    OrderReject,
)
from dh.core.units import NS_PER_S

from .mm_driver import T0, TICK, Driver, spec


def _ready(d: Driver, tickers=(TICK,), bid=4500, no_bid=4500) -> None:
    d.feed(FeedStatus(T0, 0, "kalshi.ws", "connected"))
    for t in tickers:
        d.feed(KalshiBookSnapshot(T0, 0, t, 1, 1, ((bid, 10000),), ((no_bid, 10000),)))
    d.inputs(T0)


def _places(acts, ticker=None):
    return [a for a in acts if isinstance(a, PlaceOrder) and (ticker is None or a.ticker == ticker)]


def test_control_market_is_quoted():
    d = Driver([spec()])
    _ready(d)
    assert _places(d.advance(T0 + 6 * NS_PER_S))


def test_unsupported_fee_override_makes_market_untradable():
    """M6: an event_fee_update to a fee type the model cannot price stops quoting that market."""
    d = Driver([spec()])
    _ready(d)
    d.feed(KalshiFeeUpdate(T0, 0, "KXBTCD-TEST", "flat", "1"))
    assert TICK not in d.mm.fee_sched
    assert not _places(d.advance(T0 + 6 * NS_PER_S))


def test_fill_fee_mismatch_halts_quoting():
    """M6: a fill whose exchange-reported fee disagrees with the fee model trips the fee check."""
    d = Driver([spec()])
    _ready(d)
    d.feed(KalshiFill(T0, 0, TICK, "t-1", "oid-x", "", "bid", 5000, 100, False, 1_000_000, 100, True))
    assert d.mm.risk.fee_mismatch
    assert not _places(d.advance(T0 + 6 * NS_PER_S))


def test_between_market_with_cap_at_the_money_is_not_quoted_in_the_final_window():
    """M7: the final-window near-strike guard must use the nearest strike (cap for 'between')."""
    base = default_kat_config()
    cfg = replace(base, quoting=replace(base.quoting, enabled_series=("KXBTC",), clip_contracts=1.0))
    exp = T0 + 70 * NS_PER_S
    b = spec("KXBTC-TEST-B83500", 83000.0, "between", exp, cap=84000.0, event="KXBTC-TEST", series="KXBTC")
    far = spec("KXBTC-TEST-B81500", 81000.0, "between", exp, cap=82000.0, event="KXBTC-TEST", series="KXBTC")
    d = Driver([b, far], cfg=cfg)
    _ready(d, (b.ticker, far.ticker), bid=4000, no_bid=4000)
    d.advance(T0 + 10 * NS_PER_S)
    assert abs(d.mm.fvc[b.ticker].z_near) < 1.0 < abs(d.mm.fvc[far.ticker].z_near)
    assert d.mm.stats.reasons.get("final_window_near_strike", 0) > 0
    assert not _places(d.advance(T0 + 12 * NS_PER_S), b.ticker)


def test_stale_brti_source_time_blocks_near_expiry_quoting():
    """m2: BRTI freshness uses the source timestamp too, not only the local receive time."""
    exp = T0 + 400 * NS_PER_S  # tau < 600 s: the near-expiry freshness rule applies
    fresh, stale = Driver([spec(exp=exp)]), Driver([spec(exp=exp)])
    for d in (fresh, stale):
        _ready(d, bid=4000, no_bid=4000)
    assert _places(fresh.advance(T0 + 10 * NS_PER_S))
    acts = stale.advance(T0 + 10 * NS_PER_S, brti_src_lag_ns=20 * NS_PER_S)
    assert not stale.mm.risk.health(stale.now).near_expiry_allowed
    assert not _places(acts)


def test_paused_market_is_not_requoted_until_reactivated():
    """m3: after lifecycle 'deactivated' the market is cancelled once and not re-quoted."""
    d = Driver([spec()])
    _ready(d)
    qty = {}
    for a in _places(d.advance(T0 + 6 * NS_PER_S)):
        qty[a.client_order_id] = a.qty
        d.feed(OrderAck(d.now, 0, a.client_order_id, "X" + a.client_order_id, a.ticker, 0, a.qty, "create"))
    t = d.now + 1
    out = d.feed(KalshiMarketLifecycle(t, 0, TICK, "deactivated", is_deactivated=True))
    cancels = [a for a in out if isinstance(a, CancelOrder)]
    assert sorted(a.client_order_id for a in cancels) == sorted(qty)
    for a in cancels:
        d.feed(CancelAck(t + 1, 0, a.client_order_id, a.order_id, a.ticker, qty[a.client_order_id]))
    places = []
    while d.now < t + 5 * NS_PER_S:
        for a in _places(d.advance(d.now + 200 * 1_000_000)):
            places.append(a)
            d.feed(OrderReject(d.now + 1, 0, a.client_order_id, a.ticker, "market_paused", 400, "create"))
    assert places == []
    d.feed(KalshiMarketLifecycle(d.now + 1, 0, TICK, "activated", is_deactivated=False))
    assert _places(d.advance(d.now + 3 * NS_PER_S))


def test_fee_override_reprices_the_cached_order_fee():
    """The exact per-order fee (trade fee + balance rounding) is cached per (ticker, px, size,
    side); a fee override must invalidate it."""
    import pytest

    d = Driver([spec()])
    _ready(d)
    assert d.mm._order_fee(TICK, 5000, 5.0, "bid") == pytest.approx(0.006)  # ceil(2.19c) / 5
    assert d.mm._order_fee(TICK, 5000, 4.0, "bid") == pytest.approx(0.005)  # ceil(1.75c) / 4
    d.feed(KalshiFeeUpdate(T0, 0, "KXBTCD-TEST", "quadratic", "1"))  # makers no longer pay
    assert d.mm._order_fee(TICK, 5000, 5.0, "bid") == 0.0
