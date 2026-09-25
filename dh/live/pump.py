"""Deterministic event pump: the incremental (push) form of ``dh.backtest.runner.run``.

The live runner owns ONE pump and calls it from ONE consumer task:

    pump.feed(ev)        an inbound event (receive-time ordered, non-decreasing ts)
    pump.advance(now)    a quiet moment: deliver what is due strictly before ``now``

The pump merges deterministic ``Timer`` events into the stream exactly like
``dh.backtest.runner.with_timers`` (grid ``k * period_ns``; a timer at the same ts as an
event comes after it; the first timer is the first grid point at or after the first event)
and, when a simulator is attached (paper mode), applies the replay protocol of
``dh.backtest.runner.run`` / ``dh.execution.driver.run_interleaved`` item by item:

    deliver simulator messages due strictly before the item    (drain(ts - 1))
    the simulator applies the item at the exchange              (on_market_event)
    the strategy sees the item, then the messages due at its ts
    actions decided at t are submitted to the simulator with decision_ns = t

Consequently a paper session replays bit-for-bit through ``dh.backtest.runner.run`` on the
recorded events (tests/live/test_pump.py checks the equivalence), and a live session is the
same code path with the simulator replaced by asynchronous venue adapters whose results come
back as ordinary events.

The strategy is called synchronously and never re-entered: an action handler that tried to
feed an event back from inside ``on_event`` raises ``ReentrancyError``.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from dh.core.actions import Action, Log
from dh.core.events import Event, Timer

ActionsHook = Callable[[Event, list[Action], list[bool], str], None]
DeliveredHook = Callable[[Event, str], None]


class ReentrancyError(RuntimeError):
    """The strategy was entered while it was already running (a bug in the caller)."""


class OrderingError(ValueError):
    """An event older than one already delivered was fed (the caller must keep ts order)."""


@dataclass
class PumpStats:
    events: int = 0  # input events delivered
    timers: int = 0
    sim_events: int = 0  # simulator messages delivered (paper mode)
    actions: int = 0  # non-Log actions returned by the strategy
    logs: int = 0


class EventPump:
    """Feeds one strategy with input events, synthesized timers and simulator messages.

    Args:
        strategy: object with ``on_event(ev) -> list[Action]``.
        period_ns: timer period (``StrategyConfig.timers.quote_period_ms`` * 1e6).
        sim: optional simulator (``KalshiExchangeSim``-like: on_market_event, submit,
            pop_due, next_due_ns). None in live mode.
        on_actions: called after every strategy call with (event, actions, sim_handled,
            origin); ``sim_handled[i]`` is True when the simulator accepted ``actions[i]``.
            origin is 'input' | 'timer' | 'sim'.
        on_delivered: called before the strategy sees each event, with its origin.
    """

    def __init__(
        self,
        strategy: Any,
        period_ns: int,
        *,
        sim: Any = None,
        on_actions: ActionsHook | None = None,
        on_delivered: DeliveredHook | None = None,
    ) -> None:
        if period_ns <= 0:
            raise ValueError("period_ns must be > 0")
        self.strategy = strategy
        self.period_ns = int(period_ns)
        self.sim = sim
        self.on_actions = on_actions
        self.on_delivered = on_delivered
        self.next_timer_ns: int | None = None  # None until the first input event
        self.last_ts = 0  # ts of the last item handed to the strategy (input or timer)
        self.stats = PumpStats()
        self._busy = False

    # ------------------------------------------------------------------ public API
    @property
    def busy(self) -> bool:
        """True while the strategy is executing ``on_event``."""
        return self._busy

    def feed(self, ev: Event) -> None:
        """Deliver an input event (and every timer / simulator message due before it)."""
        if self._busy:
            raise ReentrancyError("feed() called from inside strategy.on_event")
        ts = ev.ts
        if ts < self.last_ts:
            raise OrderingError(f"event ts {ts} < last delivered ts {self.last_ts} ({type(ev).__name__})")
        p = self.period_ns
        if self.next_timer_ns is None:
            self.next_timer_ns = ts if ts % p == 0 else (ts // p + 1) * p
        while self.next_timer_ns < ts:
            t = self.next_timer_ns
            self.next_timer_ns = t + p
            self._item(Timer(ts=t, period_ns=p), "timer")
        self._item(ev, "input")

    def advance(self, now_ns: int) -> None:
        """Quiet period: deliver timers with ts < now_ns and simulator messages due < now_ns.

        Every later input event must have ts >= now_ns (the runner's clock guarantees it).
        """
        if self._busy:
            raise ReentrancyError("advance() called from inside strategy.on_event")
        if self.next_timer_ns is not None:
            p = self.period_ns
            while self.next_timer_ns < now_ns:
                t = self.next_timer_ns
                self.next_timer_ns = t + p
                self._item(Timer(ts=t, period_ns=p), "timer")
        if self.sim is not None:
            self._drain(now_ns - 1)

    def next_deadline_ns(self) -> int | None:
        """Earliest time something becomes deliverable without new input (next timer or
        simulator message); the runner schedules a wake-up just after it."""
        cand = [t for t in (self.next_timer_ns, self.sim.next_due_ns() if self.sim is not None else None) if t is not None]
        return min(cand) if cand else None

    # ------------------------------------------------------------------ protocol
    def _item(self, x: Event, origin: str) -> None:
        """One item of the merged (input + timer) stream, replay protocol."""
        sim = self.sim
        due: list[Event] = []
        if sim is not None:
            self._drain(x.ts - 1)
            due = list(sim.on_market_event(x))
        self.last_ts = max(self.last_ts, x.ts)
        self._handle(x, origin)
        if due:
            due.sort(key=lambda e: e.ts)
            for m in due:
                self._handle(m, "sim")
        if sim is not None:
            self._drain(x.ts)

    def _drain(self, until: int) -> None:
        sim = self.sim
        while True:
            t = sim.next_due_ns()
            if t is None or t > until:
                return
            for m in sim.pop_due(t):
                self._handle(m, "sim")

    def _handle(self, ev: Event, origin: str) -> None:
        if self._busy:
            raise ReentrancyError("strategy re-entered")
        st = self.stats
        if origin == "timer":
            st.timers += 1
        elif origin == "sim":
            st.sim_events += 1
        else:
            st.events += 1
        if self.on_delivered is not None:
            self.on_delivered(ev, origin)
        self._busy = True
        try:
            actions = list(self.strategy.on_event(ev) or ())
        finally:
            self._busy = False
        handled = [False] * len(actions)
        sim = self.sim
        for i, a in enumerate(actions):
            if isinstance(a, Log):
                st.logs += 1
                continue
            st.actions += 1
            if sim is not None:
                handled[i] = bool(sim.submit(a, ev.ts))
        if self.on_actions is not None and actions:
            self.on_actions(ev, actions, handled, origin)
