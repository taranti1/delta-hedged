"""Reference event loop interleaving recorded market events with simulator messages.

This is the exact protocol the simulators are designed for (the Phase-2 backtest runner can
use it directly or copy it):

    for ev in events (non-decreasing ts):
        deliver every simulator message due strictly before ev.ts   (pop_due(ev.ts - 1))
        every simulator applies ev at the exchange                 (on_market_event)
        the strategy sees ev, then the messages due exactly at ev.ts
        actions decided at time t are submitted with decision_ns = t

Messages are delivered in time order across simulators; each simulator ignores actions and
events that are not its own.
"""

from __future__ import annotations

from typing import Any, Callable, Iterable, Sequence

END_OF_TIME_NS = 2**62


def run_interleaved(events: Iterable[Any], on_event: Callable[[Any], Iterable[Any] | None], sims: Sequence[Any], *,
                    until_ns: int | None = None) -> list:
    """Drive ``on_event`` (e.g. ``strategy.on_event``) with market events and simulator messages.

    Args:
        events: market/timer events in replay order.
        on_event: callable returning the actions decided at ``ev.ts``.
        sims: KalshiExchangeSim / HedgeVenueSim instances.
        until_ns: after the stream, deliver messages due up to this time (None = drain all).
    Returns every simulator message delivered, in delivery order.
    """
    delivered: list = []

    def dispatch(ev: Any) -> None:
        for a in on_event(ev) or ():
            for s in sims:
                s.submit(a, ev.ts)

    def drain(until: int) -> None:
        while True:
            best_t, best_i = None, -1
            for i, s in enumerate(sims):
                t = s.next_due_ns()
                if t is not None and t <= until and (best_t is None or t < best_t):
                    best_t, best_i = t, i
            if best_t is None:
                return
            for out in sims[best_i].pop_due(best_t):
                delivered.append(out)
                dispatch(out)

    for ev in events:
        drain(ev.ts - 1)
        due: list = []
        for s in sims:
            due.extend(s.on_market_event(ev))
        dispatch(ev)
        due.sort(key=lambda e: e.ts)
        for out in due:
            delivered.append(out)
            dispatch(out)
        drain(ev.ts)
    drain(END_OF_TIME_NS if until_ns is None else until_ns)
    return delivered
