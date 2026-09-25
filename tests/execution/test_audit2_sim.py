"""Simulator regressions for the second audit: M8 (per-order fee rounding) and m1 (a book
snapshot must not restore liquidity our own simulated takes already consumed)."""
from __future__ import annotations

import pytest

from dh.core.actions import PlaceOrder
from dh.core.events import KalshiBookSnapshot
from dh.execution import KalshiExchangeSim, LatencyModel
from dh.kalshi.fees import FeeEngine, OrderFeeAccumulator

from .helpers import MS, T, no_fee


def test_snapshot_does_not_let_the_same_displayed_ask_be_taken_twice():
    sim = KalshiExchangeSim(LatencyModel.zero(), "C", no_fee, latency_multiplier=1.0)
    book = KalshiBookSnapshot(0, 0, T, 1, 1, ((4500, 1000),), ((4800, 1000),))  # YES ask 0.52 x 10
    sim.on_market_event(book)
    sim.submit(PlaceOrder("buy1", T, "bid", 5200, 1000, post_only=False), 1 * MS)
    sim.pop_due(1 * MS)
    # a resync snapshot from the recording still shows the ask: the recording never saw our take
    sim.on_market_event(KalshiBookSnapshot(2 * MS, 0, T, 1, 5, ((4500, 1000),), ((4800, 1000),)))
    sim.submit(PlaceOrder("buy2", T, "bid", 5200, 1000, post_only=False), 3 * MS)
    sim.pop_due(3 * MS)
    assert sum(f.qty for f in sim.fill_log) == 1000
    # a snapshot showing the level shrank below what we consumed still leaves nothing to take
    sim.on_market_event(KalshiBookSnapshot(4 * MS, 0, T, 1, 9, ((4500, 1000),), ((4800, 400),)))
    sim.submit(PlaceOrder("buy3", T, "bid", 5200, 1000, post_only=False), 5 * MS)
    sim.pop_due(5 * MS)
    assert sum(f.qty for f in sim.fill_log) == 1000


def test_order_fee_fn_books_kalshi_per_order_rounding():
    """M8: with an order-aware fee function the simulator books trade fee + balance rounding
    (net of carried rebates) per order, not the bare per-fill trade fee."""
    sched = FeeEngine.from_config().schedule("quadratic_with_maker_fees", 1)
    accs: dict[str, OrderFeeAccumulator] = {}

    def order_fee_fn(key, side, px, qty, is_taker):
        acc = accs.setdefault(key, OrderFeeAccumulator(sched, side))
        return acc.apply_fill(px, qty, is_taker).net_micros

    sim = KalshiExchangeSim(LatencyModel.zero(), "C", lambda px, q, t: sched.trade_fee_micros(px, q, t),
                            latency_multiplier=1.0, order_fee_fn=order_fee_fn)
    sim.on_market_event(KalshiBookSnapshot(0, 0, T, 1, 1, ((4500, 1000),), ((4800, 500),)))  # ask 0.52 x 5
    sim.submit(PlaceOrder("buy", T, "bid", 5200, 500, post_only=False), 1 * MS)
    sim.pop_due(1 * MS)
    (f,) = sim.fill_log
    ref = OrderFeeAccumulator(sched, "bid").apply_fill(5200, 500, True)
    assert f.fee_micros == ref.net_micros != sched.trade_fee_micros(5200, 500, True)


def test_quote_value_charges_expected_rounding_per_order():
    """M8: the EV fee term is maker fee + expected rounding per order / order size."""
    from dh.strategy.config import AdverseSelCfg, FillModelCfg
    from dh.strategy.fill_model import AdverseSelectionModel, FillIntensityModel
    from dh.strategy.quoting import evaluate

    from tests.strategy.test_strategy_parts import _ctx

    ctx = _ctx()
    ctx.rounding_per_order = 0.005
    fm, am = FillIntensityModel(FillModelCfg()), AdverseSelectionModel(AdverseSelCfg())
    small = evaluate(ctx, "bid", fm, am, 4800, "touch", 10.0, 2.0)
    large = evaluate(ctx, "bid", fm, am, 4800, "touch", 10.0, 10.0)
    base = ctx.maker_fee(4800)
    assert small.fee == pytest.approx(base + 0.005 / 2) and large.fee == pytest.approx(base + 0.005 / 10)
