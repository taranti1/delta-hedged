"""Audit C1 regressions: a trade print arriving after its book delta must not fill (or move the
queue of) orders that joined after the match."""
from __future__ import annotations

import pytest

from dh.core.actions import PlaceOrder
from dh.core.events import KalshiBookDelta, KalshiBookSnapshot, KalshiTrade
from dh.execution import KalshiExchangeSim, LatencyModel

T = "KXBTCD-26SEP2517-T85000"
MS = 1_000_000


def _sim(pol):
    return KalshiExchangeSim(LatencyModel.zero(), pol, lambda px, q, t: 0, latency_multiplier=1.0)


@pytest.mark.parametrize("pol", ["A", "B", "C"])
def test_late_print_does_not_fill_order_that_joined_after_match(pol):
    sim = _sim(pol)
    sim.on_market_event(KalshiBookSnapshot(0, 0, T, 1, 1, ((5000, 1000),), ((4500, 1000),)))
    sim.on_market_event(KalshiBookDelta(100 * MS, 0, T, 1, 2, "yes", 5000, -1000))
    sim.submit(PlaceOrder("c1", T, "bid", 5000, 500, post_only=True), 110 * MS)
    sim.pop_due(110 * MS)
    sim.on_market_event(KalshiTrade(150 * MS, 0, T, "t1", 5000, 1000, "no"))
    assert sim.order_status("c1")["filled"] == 0


@pytest.mark.parametrize("pol", ["A", "B", "C"])
def test_join_after_partial_trade_keeps_correct_queue(pol):
    sim = _sim(pol)
    sim.on_market_event(KalshiBookSnapshot(0, 0, T, 1, 1, ((5000, 10000),), ((4500, 1000),)))
    sim.on_market_event(KalshiBookDelta(100 * MS, 0, T, 1, 2, "yes", 5000, -3000))  # 30 traded (print late)
    sim.submit(PlaceOrder("c1", T, "bid", 5000, 500, post_only=True), 110 * MS)
    sim.pop_due(110 * MS)
    sim.on_market_event(KalshiTrade(150 * MS, 0, T, "t1", 5000, 3000, "no"))  # the late print of those 30
    # a later 45-lot sale cannot reach us: 70 contracts are ahead
    sim.on_market_event(KalshiTrade(300 * MS, 0, T, "t2", 5000, 4500, "no"))
    sim.on_market_event(KalshiBookDelta(301 * MS, 0, T, 1, 3, "yes", 5000, -4500))
    assert sim.order_status("c1")["filled"] == 0


@pytest.mark.parametrize("pol", ["A", "B", "C"])
def test_late_sweep_uses_exchange_time(pol):
    sim = _sim(pol)
    sim.on_market_event(KalshiBookSnapshot(0, 0, T, 1, 1, ((4900, 1000), (5000, 1000)), ((4500, 1000),)))
    sim.on_market_event(KalshiBookDelta(100 * MS, 100 * MS, T, 1, 2, "yes", 5000, -1000))
    sim.submit(PlaceOrder("c1", T, "bid", 5000, 500, post_only=True), 110 * MS)
    sim.pop_due(110 * MS)
    # the same sweep's second level is reported 20 ms later but executed at 100 ms (ts_exch)
    sim.on_market_event(KalshiBookDelta(120 * MS, 100 * MS, T, 1, 3, "yes", 4900, -1000))
    sim.on_market_event(KalshiTrade(150 * MS, 100 * MS, T, "t1", 5000, 1000, "no"))
    sim.on_market_event(KalshiTrade(151 * MS, 100 * MS, T, "t2", 4900, 1000, "no"))
    assert sim.order_status("c1")["filled"] == 0
