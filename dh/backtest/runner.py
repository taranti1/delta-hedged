"""Deterministic replay runner: the SAME Strategy object used live, driven by recorded events.

Protocol (identical to dh.execution.driver.run_interleaved, which the simulators are built
for), applied to the input stream merged with deterministic timers:

    for ev in merged stream (non-decreasing ts; a timer at the same ts as a market event
                              comes after it):
        deliver every simulator message due strictly before ev.ts   (drain(ev.ts - 1))
        every simulator applies ev at the exchange                 (on_market_event)
        the strategy sees ev, then the simulator messages due exactly at ev.ts
        actions decided at time t are submitted with decision_ns = t (latency applied by sims)
    after the stream: timers continue up to end_ns, then messages due <= end_ns are delivered

Causality guarantee (tests/test_determinism.py): the actions emitted up to time t depend only
on events with ts <= t. Running on the stream truncated at t with end_ns = t yields exactly the
same actions up to t.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Iterable, Iterator, Protocol

from dh.core.actions import Action, Halt, Log
from dh.core.events import Event, Timer

END_OF_TIME_NS = 2**62


class Simulator(Protocol):
    def on_market_event(self, ev: Event) -> list[Event]: ...
    def submit(self, action: Action, decision_ns: int) -> bool: ...
    def pop_due(self, until_ns: int) -> list[Event]: ...
    def next_due_ns(self) -> int | None: ...


@dataclass
class RunResult:
    actions: list[tuple[int, Action]] = field(default_factory=list)
    logs: list[tuple[int, Log]] = field(default_factory=list)
    halts: list[tuple[int, Halt]] = field(default_factory=list)
    events_seen: int = 0
    sim_events: int = 0
    timers: int = 0


def with_timers(events: Iterable[Event], period_ns: int, end_ns: int | None = None) -> Iterator[Event]:
    """Merge Timer events (every period_ns of event time, on the grid k * period_ns) into a
    receive-time-ordered stream. Timers at the same ts as market events come after them.
    After the stream ends, timers continue until end_ns (inclusive), default = last event ts."""
    it = iter(events)
    ev = next(it, None)
    if ev is None:
        return
    next_t = (ev.ts // period_ns + 1) * period_ns if ev.ts % period_ns else ev.ts
    last = ev.ts
    while ev is not None:
        while next_t < ev.ts:
            yield Timer(ts=next_t, period_ns=period_ns)
            next_t += period_ns
        yield ev
        last = ev.ts
        ev = next(it, None)
    end = last if end_ns is None else end_ns
    while next_t <= end:
        yield Timer(ts=next_t, period_ns=period_ns)
        next_t += period_ns


def run(
    events: Iterable[Event],
    strategy: Any,
    kalshi_sim: Simulator | None = None,
    hedge_sim: Simulator | None = None,
    *,
    timer_period_ns: int = 200_000_000,
    end_ns: int | None = None,
    on_event: Callable[[Event], None] | None = None,
    on_action: Callable[[int, Action], None] | None = None,
    keep_actions: bool = True,
) -> RunResult:
    """Replay `events` (receive-time ordered) through `strategy` with simulated venues."""
    sims = [s for s in (kalshi_sim, hedge_sim) if s is not None]
    res = RunResult()

    def handle(ev: Event) -> None:
        res.events_seen += 1
        if isinstance(ev, Timer):
            res.timers += 1
        if on_event is not None:
            on_event(ev)
        for a in strategy.on_event(ev) or ():
            if on_action is not None:
                on_action(ev.ts, a)
            if isinstance(a, Log):
                res.logs.append((ev.ts, a))
                continue
            if keep_actions:
                res.actions.append((ev.ts, a))
            if isinstance(a, Halt):
                res.halts.append((ev.ts, a))
            for s in sims:
                s.submit(a, ev.ts)  # each simulator ignores actions that are not its own

    def drain(until: int) -> None:
        while True:
            best_t, best = None, None
            for s in sims:
                t = s.next_due_ns()
                if t is not None and t <= until and (best_t is None or t < best_t):
                    best_t, best = t, s
            if best is None:
                return
            for out in best.pop_due(best_t):
                res.sim_events += 1
                handle(out)

    for ev in with_timers(events, timer_period_ns, end_ns):
        if end_ns is not None and ev.ts > end_ns:
            break
        drain(ev.ts - 1)
        due: list[Event] = []
        for s in sims:
            due.extend(s.on_market_event(ev))
        handle(ev)
        due.sort(key=lambda e: e.ts)
        for out in due:
            res.sim_events += 1
            handle(out)
        drain(ev.ts)
    drain(END_OF_TIME_NS if end_ns is None else end_ns)
    return res
