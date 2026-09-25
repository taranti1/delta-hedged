from __future__ import annotations

import math

import pytest

from dh.core.events import IndexTick
from dh.core.market import SettlementSpec
from dh.core.units import NS_PER_MS, NS_PER_S
from dh.settlement import (
    SettlementTracker,
    WindowState,
    pre_window_state,
    required_remaining_avg,
    window_state_from_prints,
)

S = NS_PER_S
T = 1_790_301_600 * S  # 2026-09-25 02:00:00 UTC, an hourly expiry (and a quarter-hour close)
SPEC = SettlementSpec()


def tick(src_ns: int, value: float, feed: str = "1hz", **kw) -> IndexTick:
    return IndexTick(ts=src_ns + 150 * NS_PER_MS, ts_exch=src_ns, index_id="BRTI", value=value, feed=feed, **kw)


def val(sec_ns: int) -> float:
    """Deterministic synthetic index value for a second."""
    return 84_000.0 + (sec_ns // S) % 1000 * 0.37


def feed_1hz(tr: SettlementTracker, t0: int, t1: int, skip: set[int] = frozenset()) -> None:
    for t in range(t0, t1 + 1, S):
        if t not in skip:
            tr.on_index(tick(t, val(t)))


def test_pre_window_state_and_obs_times():
    ws = pre_window_state(SPEC, T, T - 3600 * S)
    assert ws.k_fixed == 0 and ws.m_remaining == 60 and ws.n_obs == 60
    assert ws.tau_first_s == pytest.approx(3600 - 59)
    assert ws.step_s == 1.0
    # inside the window the time to the first unfixed obs is clamped at 0
    assert pre_window_state(SPEC, T, T - 10 * S).tau_first_s == 0.0


def test_windowstate_validation():
    with pytest.raises(ValueError):
        WindowState(n_obs=60, k_fixed=10, sum_fixed=0.0, m_remaining=10, tau_first_s=0.0, step_s=1.0)
    with pytest.raises(ValueError):
        WindowState(n_obs=60, k_fixed=0, sum_fixed=0.0, m_remaining=60, tau_first_s=-1.0, step_s=1.0)


def test_tracker_before_and_inside_window_1hz():
    tr = SettlementTracker()
    feed_1hz(tr, T - 120 * S, T - 30 * S)
    now = T - 30 * S + 400 * NS_PER_MS
    ws = tr.window_state(SPEC, T, now)
    fixed = [val(t) for t in range(T - 59 * S, T - 30 * S + 1, S)]
    assert ws.k_fixed == 30 and ws.m_remaining == 30 and ws.n_obs == 60
    assert ws.sum_fixed == pytest.approx(sum(fixed))
    assert ws.tau_first_s == pytest.approx(0.6)  # next obs at T-29s
    assert ws.n_filled == 0 and ws.n_pending == 0
    # before the window: nothing fixed
    ws0 = tr.window_state(SPEC, T, T - 61 * S)
    assert ws0.k_fixed == 0 and ws0.tau_first_s == pytest.approx(2.0)
    # the start-boundary tick T-60s is excluded, the close tick T is included
    feed_1hz(tr, T - 29 * S, T + 5 * S)
    ws_end = tr.window_state(SPEC, T, T + 3 * S)
    assert ws_end.is_final and ws_end.k_fixed == 60
    expect = sum(val(t) for t in range(T - 59 * S, T + 1, S)) / 60
    assert ws_end.settlement_value == pytest.approx(expect)
    assert tr.settlement_value(SPEC, T) == pytest.approx(expect)


def test_1hz_ticks_with_millisecond_offsets_map_to_their_second():
    tr = SettlementTracker()
    for t in range(T - 70 * S, T + 2 * S, S):
        tr.on_index(tick(t + 123 * NS_PER_MS, val(t)))
    # obs at second s is the tick stamped in [s, s+1)
    status, v = tr.print_for(T - 59 * S)
    assert status == "exact" and v == val(T - 59 * S)
    assert tr.settlement_value(SPEC, T) == pytest.approx(sum(val(t) for t in range(T - 59 * S, T + 1, S)) / 60)


def test_duplicates_and_conflicts():
    tr = SettlementTracker()
    feed_1hz(tr, T - 60 * S, T - 50 * S)
    before = tr.window_state(SPEC, T, T - 50 * S)
    tr.on_index(tick(T - 55 * S, val(T - 55 * S)))  # exact duplicate
    tr.on_index(tick(T - 55 * S, 1.0))  # conflicting value for the same source time: ignored
    after = tr.window_state(SPEC, T, T - 50 * S)
    assert after == before
    assert tr.stats["duplicates"] == 1 and tr.stats["conflicts"] == 1
    # other index ids are ignored
    tr.on_index(IndexTick(ts=1, ts_exch=T - 54 * S, index_id="ETHUSD_RTI", value=3000.0, feed="1hz"))
    assert tr.stats["ignored"] == 1


@pytest.mark.parametrize("policy", ["carry_forward", "skip"])
def test_missing_second_policy(policy):
    tr = SettlementTracker(gap_policy=policy)
    missing = T - 40 * S
    feed_1hz(tr, T - 65 * S, T + 1 * S, skip={missing})
    ws = tr.window_state(SPEC, T, T + S)
    assert ws.m_remaining == 0
    present = [val(t) for t in range(T - 59 * S, T + 1, S) if t != missing]
    if policy == "carry_forward":
        assert ws.n_obs == 60 and ws.n_filled == 1
        assert ws.sum_fixed == pytest.approx(sum(present) + val(missing - S))
    else:
        assert ws.n_obs == 59 and ws.n_filled == 0
        assert ws.sum_fixed == pytest.approx(sum(present))
        assert ws.settlement_value == pytest.approx(sum(present) / 59)


def test_pending_observation_is_unfixed_with_zero_tau():
    tr = SettlementTracker()
    feed_1hz(tr, T - 70 * S, T - 20 * S)
    # 1.7 s later: prints for T-19s and T-18s are due but not yet received
    now = T - 18 * S + 700 * NS_PER_MS
    ws = tr.window_state(SPEC, T, now)
    assert ws.k_fixed == 40 and ws.m_remaining == 20
    assert ws.n_pending == 2 and ws.tau_first_s == 0.0
    tr.on_index(tick(T - 19 * S, val(T - 19 * S)))
    tr.on_index(tick(T - 18 * S, val(T - 18 * S)))
    ws2 = tr.window_state(SPEC, T, now)
    assert ws2.k_fixed == 42 and ws2.n_pending == 0 and ws2.tau_first_s == pytest.approx(0.3)


def test_5hz_only_uses_last_tick_at_or_before_second():
    tr = SettlementTracker()
    # 5 Hz ticks offset by 100 ms: at s-0.9, s-0.7, ..., s-0.1, s+0.1 -> print for s is the s-0.1 tick
    t = T - 70 * S + 100 * NS_PER_MS
    while t <= T + 2 * S:
        tr.on_index(tick(t, t / S, feed="5hz"))
        t += 200 * NS_PER_MS
    status, v = tr.print_for(T - 30 * S)
    assert status == "exact5"
    assert v == pytest.approx((T - 30 * S - 100 * NS_PER_MS) / S)
    ws = tr.window_state(SPEC, T, T + S)
    assert ws.is_final and ws.n_filled == 0
    expect = sum((s - 100 * NS_PER_MS) / S for s in range(T - 59 * S, T + 1, S)) / 60
    assert ws.settlement_value == pytest.approx(expect)
    # the last second is final only once a later 5 Hz tick exists
    tr2 = SettlementTracker()
    tr2.on_index(tick(T - 1 * S, 1.0, feed="5hz"))
    tr2.on_index(tick(T, 2.0, feed="5hz"))
    assert tr2.print_for(T)[0] == "pending"
    tr2.on_index(tick(T + 200 * NS_PER_MS, 3.0, feed="5hz"))
    assert tr2.print_for(T) == ("exact5", 2.0)


def test_5hz_out_of_order_and_gap():
    tr = SettlementTracker()
    tr.on_index(tick(T - 10 * S, 10.0, feed="5hz"))
    tr.on_index(tick(T - 8 * S, 8.0, feed="5hz"))
    tr.on_index(tick(T - 9 * S, 9.0, feed="5hz"))  # late but in the past: inserted in order
    assert tr.stats["out_of_order"] == 1
    assert tr.print_for(T - 9 * S) == ("exact5", 9.0)
    # a 5 Hz gap of > 1 s: second T-7 has no tick within the preceding second -> missing
    tr.on_index(tick(T - 5 * S, 5.0, feed="5hz"))
    assert tr.print_for(T - 7 * S) == ("filled", 8.0)
    tr_skip = SettlementTracker(gap_policy="skip")
    for s_, v in ((T - 8 * S, 8.0), (T - 5 * S, 5.0)):
        tr_skip.on_index(tick(s_, v, feed="5hz"))
    assert tr_skip.print_for(T - 7 * S) == ("skipped", None)


def test_1hz_preferred_over_5hz_and_5hz_fills_latency():
    tr = SettlementTracker()
    tr.on_index(tick(T - 30 * S, 100.0, feed="5hz"))
    tr.on_index(tick(T - 30 * S + 200 * NS_PER_MS, 101.0, feed="5hz"))
    # no 1 Hz print yet: the 5 Hz value is used (lower latency)
    assert tr.print_for(T - 30 * S) == ("exact5", 100.0)
    tr.on_index(tick(T - 30 * S, 100.5, feed="1hz"))
    assert tr.print_for(T - 30 * S) == ("exact", 100.5)
    tr5 = SettlementTracker(prefer_1hz=False)
    tr5.on_index(tick(T - 30 * S, 100.0, feed="5hz"))
    tr5.on_index(tick(T - 30 * S + 200 * NS_PER_MS, 101.0, feed="5hz"))
    tr5.on_index(tick(T - 30 * S, 100.5, feed="1hz"))
    assert tr5.print_for(T - 30 * S) == ("exact5", 100.0)


def test_required_remaining_avg_and_edge_cases():
    ws = WindowState(n_obs=60, k_fixed=45, sum_fixed=45 * 100.0, m_remaining=15, tau_first_s=0.2, step_s=1.0)
    # average ends above 101 iff remaining average > (101*60 - 4500)/15 = 104
    assert required_remaining_avg(101.0, ws) == pytest.approx(104.0)
    assert SettlementTracker().required_remaining_avg(101.0, ws) == pytest.approx(104.0)
    final = WindowState(n_obs=60, k_fixed=60, sum_fixed=6000.0, m_remaining=0, tau_first_s=0.0, step_s=1.0)
    assert required_remaining_avg(99.0, final) == -math.inf
    assert required_remaining_avg(101.0, final) == math.inf
    assert math.isnan(required_remaining_avg(100.0, final))


def test_window_state_from_prints_matches_tracker():
    tr = SettlementTracker()
    missing = {T - 33 * S}
    feed_1hz(tr, T - 61 * S, T - 10 * S, skip=missing)
    prints = {t: val(t) for t in range(T - 59 * S, T - 9 * S, S) if t not in missing}
    now = T - 10 * S + 500 * NS_PER_MS
    a = tr.window_state(SPEC, T, now)
    b = window_state_from_prints(SPEC, T, now, prints, last_before=val(T - 60 * S))
    assert (a.k_fixed, a.m_remaining, a.n_filled) == (b.k_fixed, b.m_remaining, b.n_filled)
    assert a.sum_fixed == pytest.approx(b.sum_fixed) and a.tau_first_s == pytest.approx(b.tau_first_s)


def test_kalshi_quarter_hour_average_is_recorded():
    tr = SettlementTracker()
    tr.on_index(tick(T - 20 * S, 1.0, qh_avg=84_100.5, qh_n=40))
    tr.on_index(tick(T - 10 * S, 1.0, qh_avg=84_101.5, qh_n=50))
    assert tr.kalshi_window_avg(T) == (84_101.5, 50)
    assert tr.kalshi_window_avg(T - 900 * S) is None


def test_memory_is_bounded():
    tr = SettlementTracker(retain_s=600)
    t0 = T - 5 * 3600 * S
    for i in range(5 * 3600):
        t = t0 + i * S
        tr.on_index(tick(t, 1.0 + i))
        for j in range(5):
            tr.on_index(tick(t + j * 200 * NS_PER_MS, 1.0 + i, feed="5hz"))
    assert len(tr._p1) < 2 * 600 + 1100
    assert len(tr._t5) < 5 * (600 + 60) + 4200
    ws = tr.window_state(SPEC, T, T - S)
    assert ws.k_fixed == 59
