"""Deterministic replay runner: the SAME Strategy object used live, driven by recorded events.

Event interleaving (all by receive time `ts`; ties broken by this priority, then arrival order):
  0. simulator-generated events due at t (acks, fills, cancel acks, hedge fills) — the exchange
     already processed them, we learn about them at t
  1. recorded market events at t (fed first to the exchange simulator, then to the strategy)
  2. timers at t (deterministic, every `timer_period_ns` of event time)

Actions returned by the strategy are submitted to the simulators with decision time = the ts
of the event that caused them; the simulator applies submit latency.

Causality guarantee (tested): the actions emitted up to time t depend only on events with
ts <= t. Truncating the input stream at t yields an identical action prefix.
"""

from __future__ import annotations

import heapq
import math
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable, Iterator, Protocol

from dh.core.actions import (
    Action,
    AmendOrder,
    CancelAll,
    CancelHedge,
    CancelOrder,
    DecreaseOrder,
    Halt,
    Log,
    PlaceHedge,
    PlaceOrder,
    Resume,
)
from dh.core.events import Event, Timer


class ExchangeSim(Protocol):
    def on_market_event(self, ev: Event) -> list[Event]: ...
    def submit(self, action: Action, decision_ns: int) -> None: ...
    def pop_due(self, until_ns: int) -> list[Event]: ...
    def next_due_ns(self) -> int | None: ...


@dataclass
class RunResult:
    actions: list[tuple[int, Action]] = field(default_factory=list)
    logs: list[tuple[int, Log]] = field(default_factory=list)
    events_seen: int = 0
    sim_events: int = 0
    halts: list[tuple[int, Halt]] = field(default_factory=list)


def run(
    events: Iterable[Event],
    strategy: Any,
    kalshi_sim: ExchangeSim | None = None,
    hedge_sim: ExchangeSim | None = None,
    *,
    timer_period_ns: int = 200_000_000,
    on_event: Callable[[Event], None] | None = None,
    on_action: Callable[[int, Action], None] | None = None,
    keep_actions: bool = True,
    until_ns: int | None = None,
) -> RunResult:
    """Replay `events` (receive-time ordered) through `strategy` with simulated venues."""
    res = RunResult()
    it: Iterator[Event] = iter(events)
    pending: list[tuple[int, int, int, Event]] = []  # (ts, prio, seq, event) for sim output
    seq = 0
    nxt = next(it, None)
    next_timer = None if nxt is None else (nxt.ts // timer_period_ns + 1) * timer_period_ns

    def sims() -> list[ExchangeSim]:
        return [s for s in (kalshi_sim, hedge_sim) if s is not None]

    def collect_due(t: int) -> None:
        nonlocal seq
        for s in sims():
            for e in s.pop_due(t):
                heapq.heappush(pending, (e.ts, 0, seq, e))
                seq += 1

    def dispatch(actions: list[Action], t: int) -> None:
        for a in actions:
            if on_action is not None:
                on_action(t, a)
            if isinstance(a, Log):
                res.logs.append((t, a))
                continue
            if keep_actions:
                res.actions.append((t, a))
            if isinstance(a, Halt):
                res.halts.append((t, a))
                continue
            if isinstance(a, (PlaceHedge, CancelHedge)):
                if hedge_sim is not None:
                    hedge_sim.submit(a, t)
            elif isinstance(a, (PlaceOrder, CancelOrder, AmendOrder, DecreaseOrder, CancelAll)):
                if kalshi_sim is not None:
                    kalshi_sim.submit(a, t)
            elif isinstance(a, Resume):
                pass

    def deliver(e: Event) -> None:
        res.events_seen += 1
        if on_event is not None:
            on_event(e)
        dispatch(strategy.on_event(e), e.ts)

    while True:
        t_mkt = nxt.ts if nxt is not None else math.inf
        due = [d for d in (s.next_due_ns() for s in sims()) if d is not None]
        t_sim = min(due) if due else math.inf
        t_pend = pending[0][0] if pending else math.inf
        t_tim = next_timer if (next_timer is not None and nxt is not None) else math.inf
        t = min(t_mkt, t_sim, t_pend, t_tim)
        if t == math.inf or (until_ns is not None and t > until_ns):
            break
        if t_sim <= t:
            collect_due(int(t_sim))
            t_pend = pending[0][0] if pending else math.inf
        if pending and pending[0][0] <= t:
            _, _, _, e = heapq.heappop(pending)
            res.sim_events += 1
            deliver(e)
            continue
        if t_mkt <= t_tim and nxt is not None:
            ev = nxt
            nxt = next(it, None)
            for s in sims():
                for e in s.on_market_event(ev):  # immediate sim outputs (rare) queued by their ts
                    heapq.heappush(pending, (e.ts, 0, seq, e))
                    seq += 1
            deliver(ev)
            continue
        if next_timer is not None:
            deliver(Timer(ts=int(next_timer), period_ns=timer_period_ns))
            next_timer += timer_period_ns
    return res
