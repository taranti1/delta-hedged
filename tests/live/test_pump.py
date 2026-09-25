"""EventPump: the live consumer core must feed the strategy exactly what the backtest runner
feeds it (timers on the grid, simulator protocol), whatever the wake-up pattern."""

from __future__ import annotations

import random

import pytest

from dh.backtest.kat import default_kat_config, warm_fv_model
from dh.backtest.runner import run, with_timers
from dh.core.actions import Log
from dh.core.events import FeedStatus, IndexTick, Timer
from dh.core.units import NS_PER_MS, NS_PER_S
from dh.execution.exchange_sim import KalshiExchangeSim
from dh.execution.latency import LatencyModel
from dh.kalshi.fees import FeeEngine
from dh.live.pump import EventPump, OrderingError, ReentrancyError
from dh.models.fvmodel import FairValueModel, load_recommended_config
from dh.sim.synthetic import SynthConfig, SyntheticMarket
from dh.strategy.mm import MarketMaker

from .fakes import T0, RecordingStrategy

P = 200 * NS_PER_MS


def _tick(ts: int, v: float = 84_000.0) -> IndexTick:
    return IndexTick(ts=ts, ts_exch=ts, index_id="BRTI", value=v, feed="5hz")


def test_timers_on_grid_after_equal_ts_events_and_monotonic():
    s = RecordingStrategy()
    pump = EventPump(s, P)
    evs = [_tick(T0 + 50 * NS_PER_MS), _tick(T0 + 200 * NS_PER_MS), _tick(T0 + 200 * NS_PER_MS), _tick(T0 + 1_030 * NS_PER_MS)]
    for ev in evs:
        pump.feed(ev)
    pump.advance(T0 + 1_401 * NS_PER_MS)
    got = s.events
    ts = [e.ts for e in got]
    assert ts == sorted(ts), "delivered ts must be non-decreasing"
    timers = [e for e in got if isinstance(e, Timer)]
    assert [(t.ts - T0) // NS_PER_MS for t in timers] == [200, 400, 600, 800, 1000, 1200, 1400] and all(t.ts % P == 0 for t in timers)
    assert all(t.period_ns == P for t in timers)
    # a timer at the same ts as events comes after them (backtest convention)
    i = next(i for i, e in enumerate(got) if isinstance(e, Timer) and e.ts == T0 + 200 * NS_PER_MS)
    assert all(isinstance(e, IndexTick) for e in got[i - 2:i])
    # identical to the backtest merge
    assert list(with_timers(evs, P, end_ns=T0 + 1_400 * NS_PER_MS)) == got


def test_no_timers_before_first_event_and_ordering_guard():
    s = RecordingStrategy()
    pump = EventPump(s, P)
    pump.advance(T0 + 10 * NS_PER_S)
    assert s.events == []
    pump.feed(_tick(T0 + 11 * NS_PER_S))
    with pytest.raises(OrderingError):
        pump.feed(_tick(T0 + 10 * NS_PER_S))


def test_reentrancy_is_refused():
    holder: dict = {}

    def respond(ev):
        holder["pump"].feed(_tick(ev.ts + 1))  # a bug: feeding from inside on_event
        return []

    s = RecordingStrategy(respond)
    pump = EventPump(s, P)
    holder["pump"] = pump
    with pytest.raises(ReentrancyError):
        pump.feed(_tick(T0))


def _mm_and_sim(events, specs, cfg, synth, seed: int = 3):
    fv = FairValueModel.from_config(load_recommended_config())
    warm_fv_model(fv, events[0].ts, synth.S0, synth.vol_ann, seed=0)
    fee_engine = FeeEngine.from_config()
    mm = MarketMaker(cfg, specs, fv_model=fv, fee_engine=fee_engine)
    sched = fee_engine.schedule_for_spec(specs[0].fee_type, specs[0].fee_multiplier)
    sim = KalshiExchangeSim(LatencyModel.fixed(submit_ms=40, response_ms=40, ws_ms=25), "conservative",
                            lambda px, q, t: sched.trade_fee_micros(px, q, t), seed=seed)
    for s in specs:
        sim.register_market(s)
    return mm, sim


@pytest.fixture(scope="module")
def synthetic():
    synth = SynthConfig(duration_s=180, seed=12, informed=False, mm_lag_s=1.5)
    sm = SyntheticMarket(synth)
    return synth, sm.generate(), sm.specs()


def test_paper_pump_equals_backtest_runner(synthetic):
    """Paper mode = replay protocol: identical actions, whatever the wake-up pattern."""
    synth, events, specs = synthetic
    cfg = default_kat_config()
    period = cfg.timers.quote_period_ms * NS_PER_MS
    end = events[-1].ts

    mm1, sim1 = _mm_and_sim(events, specs, cfg, synth)
    ref = run(events, mm1, sim1, timer_period_ns=period, end_ns=end)

    mm2, sim2 = _mm_and_sim(events, specs, cfg, synth)
    got: list = []
    logs: list = []

    def on_actions(ev, actions, handled, origin):
        for a in actions:
            (logs if isinstance(a, Log) else got).append((ev.ts, a))

    pump = EventPump(mm2, period, sim=sim2, on_actions=on_actions)
    rng = random.Random(5)
    prev = None
    for ev in events:
        if prev is not None and ev.ts > prev and rng.random() < 0.3:
            pump.advance(rng.randint(prev + 1, ev.ts))  # a wake-up between two events
        pump.feed(ev)
        prev = ev.ts
    pump.advance(end + 1)
    assert ref.actions == got
    assert ref.logs == logs
    assert mm2.stats.fills == mm1.stats.fills and mm1.stats.fills > 0
    assert pump.stats.sim_events == ref.sim_events
    for t in mm1.specs:
        assert mm2.om.position(t) == mm1.om.position(t) == sim2.position(t)


def test_live_pump_without_sim_matches_timer_merge(synthetic):
    synth, events, specs = synthetic
    evs = [e for e in events[:3000]] + [FeedStatus(events[2999].ts, 0, "kalshi.ws", "connected")]
    s = RecordingStrategy()
    pump = EventPump(s, P)
    rng = random.Random(9)
    prev = None
    for ev in evs:
        if prev is not None and ev.ts > prev and rng.random() < 0.5:
            pump.advance(rng.randint(prev + 1, ev.ts))
        pump.feed(ev)
        prev = ev.ts
    pump.advance(evs[-1].ts + 1)
    assert s.events == list(with_timers(evs, P, end_ns=evs[-1].ts))
