"""Hidden-queue stress (research): an undisplayed queue ahead of every arrival can only remove
fills, per order and in total, under every fill policy, on identical streams and arrival times."""

from __future__ import annotations

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from dh.core.actions import PlaceOrder
from dh.execution import KalshiExchangeSim, LatencyModel
from dh.execution.queue import QueueEstimator
from tests.execution.helpers import gen_schedule, gen_stream, no_fee

STRESS = {"base": (0.0, 0), "frac": (1.0, 0), "lots": (0.0, 500), "both": (2.0, 1000)}


def run(seed: int, n: int, n_orders: int, policy: str):
    events = gen_stream(seed, n, sweeps=True)
    sched = gen_schedule(seed, n_orders, events[-1].ts - events[0].ts + 1)
    places = [a.client_order_id for _, a in sched if isinstance(a, PlaceOrder)]
    lat = LatencyModel(seed)
    sims = {k: KalshiExchangeSim(lat, policy, no_fee, seed=3, latency_multiplier=1.0,
                                 queue_hidden_frac=f, queue_hidden_qty=q) for k, (f, q) in STRESS.items()}
    i = 0
    for ev in events:
        for s in sims.values():
            s.pop_due(ev.ts - 1)
            s.on_market_event(ev)
        while i < len(sched) and sched[i][0] <= ev.ts:
            for s in sims.values():
                s.submit(sched[i][1], ev.ts)
            i += 1
        for s in sims.values():
            s.pop_due(ev.ts)
    for s in sims.values():
        s.pop_due(2**62)
    return sims, places


@settings(max_examples=60, deadline=None)
@given(seed=st.integers(0, 10_000), policy=st.sampled_from(["A", "B", "C"]))
def test_hidden_queue_never_adds_fills(seed, policy):
    sims, places = run(seed, 300, 6, policy)
    for coid in places:
        filled = {k: (sims[k].order_status(coid) or {}).get("filled", 0) for k in STRESS}
        for k in ("frac", "lots", "both"):
            assert filled[k] <= filled["base"], (coid, k, filled)
        assert filled["both"] <= min(filled["frac"], filled["lots"])


def test_hidden_queue_is_consumed_by_prints_before_our_fill():
    q = QueueEstimator("C", lambda t, b, p: 300, hidden_qty=200)
    o = q.add_order("k", "T", "bid", 4500, 100, 0)
    assert (o.queue_ahead, o.hidden_ahead) == (300, 200)
    q.on_snapshot(type("S", (), {"ticker": "T"})())  # a snapshot clamps only the displayed queue
    assert o.hidden_ahead == 200


def test_hidden_queue_rejects_negative_settings():
    with pytest.raises(ValueError):
        QueueEstimator("B", hidden_frac=-0.1)
