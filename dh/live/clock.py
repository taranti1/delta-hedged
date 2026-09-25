"""Receive-time clock of a live session and the per-session client-order-id token.

``AnchoredClock``: every receive time of a session (KalshiWS frames, REST results, the
runner's wake-ups and side items, venue results) comes from ONE clock that is

  * anchored to the wall clock once, at construction (the time.time_ns() epoch the rest of
    the system uses), then advanced by time.monotonic_ns(): a wall-clock step (NTP
    makestep, a manual date change) never moves it, so receive times never go backwards;
  * strictly increasing: two calls never return the same value (1 ns apart at worst), so
    every queued item of the runner has a unique timestamp and the recorded streams merge
    back into exactly the processing order on replay (dh.store.replay orders equal
    timestamps by stream rank, which could otherwise differ from the live queue order).

Its offset from true time is monitored by the runner (``drift_ns`` = wall - anchored, plus
the chrony offset of the wall clock): a persistent offset blocks new orders; a restart
re-anchors.

``session_token``: a short, unique token for the session's client_order_id prefix
(``<run_prefix>-<token>-<n>``), so ids never repeat across restarts (Kalshi keeps recent
client_order_ids unique only among live and recent orders).
"""

from __future__ import annotations

import secrets
import time
from collections.abc import Callable

_B36 = "0123456789abcdefghijklmnopqrstuvwxyz"


class AnchoredClock:
    """Monotonic, strictly increasing ns clock anchored to the wall clock (see module doc)."""

    def __init__(self, wall_ns: Callable[[], int] = time.time_ns, mono_ns: Callable[[], int] = time.monotonic_ns) -> None:
        self._wall = wall_ns
        self._mono = mono_ns
        self.anchor_wall_ns = int(wall_ns())
        self.anchor_mono_ns = int(mono_ns())
        self._last = self.anchor_wall_ns - 1

    def __call__(self) -> int:
        t = self.anchor_wall_ns + (int(self._mono()) - self.anchor_mono_ns)
        if t <= self._last:
            t = self._last + 1
        self._last = t
        return t

    def peek(self) -> int:
        """Current reading without consuming a value (never below the last one returned)."""
        return max(self._last, self.anchor_wall_ns + (int(self._mono()) - self.anchor_mono_ns))

    def drift_ns(self) -> int:
        """Wall clock minus this clock: grows when the wall clock is stepped after the anchor."""
        return int(self._wall()) - self.peek()


def to_base36(n: int, width: int = 0) -> str:
    if n < 0:
        raise ValueError("negative")
    out = ""
    while True:
        n, r = divmod(n, 36)
        out = _B36[r] + out
        if n == 0:
            break
    return out.rjust(width, "0")


def session_token(now_ns: int | None = None, rand: int | None = None) -> str:
    """8 base36 characters: 6 for the start second (unique per restart, sortable) + 2 random
    (two sessions started in the same second still differ)."""
    s = (time.time_ns() if now_ns is None else int(now_ns)) // 1_000_000_000
    r = secrets.randbelow(36 * 36) if rand is None else int(rand) % (36 * 36)
    return to_base36(s % 36**6, 6) + to_base36(r, 2)


__all__ = ["AnchoredClock", "session_token", "to_base36"]
