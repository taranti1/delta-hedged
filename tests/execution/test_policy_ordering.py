"""Fill-policy ordering: on identical recorded streams and identical order arrival times,
cumulative fills satisfy optimistic (A) >= realistic (B) >= conservative (C), per order and at
every point in time. (Policy C's default 1.5x latency stress is disabled here so the comparison
isolates the fill model; with it, C also acts later, which is not monotone in general.)"""

from __future__ import annotations

from hypothesis import given, settings
from hypothesis import strategies as st

from dh.core.actions import PlaceOrder
from dh.execution import KalshiExchangeSim, LatencyModel
from tests.execution.helpers import gen_schedule, gen_stream, no_fee

POLS = ("A", "B", "C")


def lockstep(seed: int, n: int, n_orders: int, lat_seed: int, check_every_event: bool = True, sweeps: bool = True):
    events = gen_stream(seed, n, sweeps=sweeps)
    horizon = events[-1].ts - events[0].ts + 1
    sched = gen_schedule(seed, n_orders, horizon)
    places = [a.client_order_id for _, a in sched if isinstance(a, PlaceOrder)]
    lat = LatencyModel(lat_seed)
    sims = {p: KalshiExchangeSim(lat, p, no_fee, seed=3, latency_multiplier=1.0) for p in POLS}

    def check():
        for coid in places:
            st_ = [sims[p].order_status(coid) for p in POLS]
            assert len({s is None for s in st_}) == 1, coid  # accepted/rejected identically
            if st_[0] is None:
                continue
            fa, fb, fc = (s["filled"] for s in st_)
            assert fa >= fb >= fc, (coid, fa, fb, fc)
        pa, pb, pc = (sum(abs(r.qty) for r in sims[p].fill_log) for p in POLS)
        assert pa >= pb >= pc

    i = 0
    area = dict.fromkeys(POLS, 0)  # time-integrated cumulative fills: rewards filling earlier
    for ev in events:
        for s in sims.values():
            s.pop_due(ev.ts - 1)
        for s in sims.values():
            s.on_market_event(ev)
        while i < len(sched) and sched[i][0] <= ev.ts:
            for s in sims.values():
                s.submit(sched[i][1], ev.ts)
            i += 1
        for s in sims.values():
            s.pop_due(ev.ts)
        if check_every_event:
            check()
        for p in POLS:
            area[p] += sum(r.qty for r in sims[p].fill_log)
    for s in sims.values():
        s.pop_due(2**62)
    check()
    return {p: sum(r.qty for r in sims[p].fill_log) for p in POLS}, area


@settings(max_examples=150, deadline=None)
@given(seed=st.integers(0, 10**6), n=st.integers(5, 250), n_orders=st.integers(1, 10), lat_seed=st.integers(0, 999),
       sweeps=st.booleans())
def test_policy_ordering_pathwise(seed, n, n_orders, lat_seed, sweeps):
    lockstep(seed, n, n_orders, lat_seed, sweeps=sweeps)


def test_policies_actually_differ():
    """Guard against a vacuous ordering test. With multi-level sweeps every order eventually
    fills under every policy (sweeps fill through price priority), so policies differ in *when*
    they fill; on sweep-free streams they also differ in how much."""
    runs = [lockstep(s, 300, 8, s, check_every_event=False, sweeps=False) for s in range(8)]
    tot = {p: sum(r[0][p] for r in runs) for p in POLS}
    assert tot["A"] > tot["B"] > tot["C"] > 0
    sweep_runs = [lockstep(s, 300, 8, s, check_every_event=False) for s in range(6)]
    area = {p: sum(r[1][p] for r in sweep_runs) for p in POLS}
    assert area["A"] >= area["B"] > area["C"]
