"""The deterministic Strategy contract.

    actions = strategy.on_event(event)

Rules every Strategy implementation must obey (enforced by tests/test_determinism.py):
  1. Pure function of (initial config, ordered event stream). No wall clock, no network, no
     filesystem, no unseeded randomness, no threads. "Now" is ``event.ts``.
  2. Identical code path in live and replay. Live/replay differences live only in adapters.
  3. Own orders/fills are known only through events (acks, rejects, fills, order updates).
     An action is a *request*; state changes when the corresponding event arrives.
  4. Every decision that could have been a trade is logged with a ``Log`` action carrying the
     inputs needed to evaluate it counterfactually (fair value, P(fill), EV terms).
"""

from __future__ import annotations

from typing import Protocol

from dh.core.actions import Action
from dh.core.events import Event


class Strategy(Protocol):
    def on_event(self, ev: Event) -> list[Action]:  # pragma: no cover - protocol
        ...


class IdGen:
    """Deterministic client_order_id generator: '<prefix>-<n>' (Kalshi allows any string id).

    The prefix should encode the run/session so ids never collide across restarts, e.g.
    'dh20260925a'. Keep ids short: they are echoed on every order/fill message.
    """

    __slots__ = ("prefix", "n")

    def __init__(self, prefix: str, start: int = 0) -> None:
        if not prefix or any(c.isspace() for c in prefix):
            raise ValueError("prefix must be non-empty without whitespace")
        self.prefix = prefix
        self.n = start

    def next(self) -> str:
        self.n += 1
        return f"{self.prefix}-{self.n}"
