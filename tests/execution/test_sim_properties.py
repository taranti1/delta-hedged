"""Conservation and live/backtest consistency of the simulated exchange + OrderManager."""

from __future__ import annotations

from collections import defaultdict

from hypothesis import given, settings
from hypothesis import strategies as st

from dh.core.actions import PlaceOrder
from dh.execution import KalshiExchangeSim, LatencyModel, OrderState, run_interleaved
from tests.execution.helpers import Scripted, gen_schedule, gen_stream, simple_fee

STATUS = {"executed": OrderState.FILLED, "canceled": OrderState.CANCELED, "resting": OrderState.RESTING}


@settings(max_examples=120, deadline=None)
@given(seed=st.integers(0, 10**6), n=st.integers(5, 250), n_orders=st.integers(1, 10),
       policy=st.sampled_from(["A", "B", "C"]), lat_seed=st.integers(0, 999), md_ms=st.sampled_from([0.0, 20.0]))
def test_conservation_and_consistency(seed, n, n_orders, policy, lat_seed, md_ms):
    events = gen_stream(seed, n)
    horizon = events[-1].ts - events[0].ts + 1
    sched = gen_schedule(seed, n_orders, horizon)
    sim = KalshiExchangeSim(LatencyModel(lat_seed, md=md_ms), policy, simple_fee, seed=5)
    strat = Scripted(sched)
    out = run_interleaved(events, strat, [sim])

    # delivery is time ordered
    assert all(a.ts <= b.ts for a, b in zip(out[:-1], out[1:], strict=True))
    # per order: fills never exceed the order, never precede arrival, never follow the order
    # leaving the book (cancel / close processed at the matching engine)
    filled = defaultdict(int)
    for r in sim.fill_log:
        o = sim.orders[r.order_id]
        filled[r.order_id] += r.qty
        assert r.ts_exch >= o.created_ns
        if o.done_ns:
            assert r.sim_ts <= o.done_ns
    signed = 0
    for oid, o in sim.orders.items():
        assert filled[oid] == o.filled <= o.cap
        signed += o.filled if o.book_side == "bid" else -o.filled
    om = strat.om
    tickers = {a.ticker for _, a in sched if isinstance(a, PlaceOrder)}
    for t in tickers:
        assert om.position(t) == sim.position(t)
    assert sum(sim.positions.values()) == signed
    # the OrderManager (fed only the delivered messages) agrees with the matching engine
    for _, a in sched:
        if not isinstance(a, PlaceOrder):
            continue
        so = sim.order_status(a.client_order_id)
        wo = om.order(a.client_order_id)
        if so is None:
            assert wo.state is OrderState.REJECTED
            continue
        assert wo.filled_qty == so["filled"] and wo.inflight_fill_qty == 0
        assert wo.state is STATUS[so["status"]]
    for t in tickers:
        live_bid = sum(o.remaining for o in sim.orders.values() if o.ticker == t and o.book_side == "bid")
        assert om.worst_case_exposure(t, "bid") == om.position(t) + live_bid
    # fees are the fee function applied fill by fill
    assert om.fees_micros() == sum(r.fee_micros for r in sim.fill_log)
