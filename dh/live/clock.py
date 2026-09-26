"""Receive-time clock of a live session and the per-session client-order-id token.

``AnchoredClock``: every receive time of a session (KalshiWS frames, REST results, the
runner's wake-ups and side items, venue results) comes from ONE clock that is

  * anchored to the wall clock once, at construction (the time.time_ns() epoch the rest of
    the system uses), then advanced by time.monotonic_ns(), SLEWED toward the wall clock at
    a bounded rate (``max_slew_ppm``, 50 ppm = 50 us per second): the monotonic clock's own
    rate error (this Mac: mach_absolute_time runs ~3.2 ppm off the disciplined wall clock,
    ~11.5 ms/h, which crossed the 250 ms offset block ~16 h into a session) is absorbed and
    the clock tracks the wall clock, while a wall-clock STEP (NTP makestep, a manual date
    change) moves it by at most 50 us per second: a 1 s step still shows as a large offset
    for ~5.5 h (fail safe: new orders stay blocked; a restart re-anchors);
  * never stepped and strictly increasing: the slew never exceeds the monotonic advance
    (50 ppm < 1), and two calls never return the same value (1 ns apart at worst), so every
    queued item of the runner has a unique timestamp and the recorded streams merge back
    into exactly the processing order on replay (dh.store.replay orders equal timestamps by
    stream rank, which could otherwise differ from the live queue order). Receive times are
    recorded, so replay never depends on this clock.

Its offset from true time is monitored by the runner (``drift_ns`` = wall - this clock, plus
the chrony / sntp offset of the wall clock): a persistent offset blocks new orders.

``session_token``: a short, unique token for the session's client_order_id prefix
(``<run_prefix>-<token>-<n>``), so ids never repeat across restarts (Kalshi keeps recent
client_order_ids unique only among live and recent orders).
"""

from __future__ import annotations

import secrets
import time
from collections.abc import Callable

_B36 = "0123456789abcdefghijklmnopqrstuvwxyz"


MAX_SLEW_PPM = 50.0  # the session clock moves toward the wall clock by at most 50 us per second
SLEW_STEP_NS = 1_000_000  # the slew is applied once per ms of monotonic time (sub-ns rates accumulate)


class AnchoredClock:
    """Monotonic, strictly increasing ns clock anchored to the wall clock and slewed toward it
    at <= ``max_slew_ppm`` (see module doc)."""

    def __init__(self, wall_ns: Callable[[], int] = time.time_ns, mono_ns: Callable[[], int] = time.monotonic_ns,
                 *, max_slew_ppm: float = MAX_SLEW_PPM) -> None:
        if not 0.0 <= max_slew_ppm < 1_000_000.0:
            raise ValueError("max_slew_ppm must be in [0, 1e6)")
        self._wall = wall_ns
        self._mono = mono_ns
        self.max_slew_ppm = float(max_slew_ppm)
        self.anchor_wall_ns = int(wall_ns())
        self.anchor_mono_ns = int(mono_ns())
        self._offset_ns = 0  # slew applied so far: this clock = anchor_wall + (mono - anchor_mono) + offset
        self._slew_mono_ns = self.anchor_mono_ns  # monotonic time the slew was last applied at
        self._last = self.anchor_wall_ns - 1

    def _raw(self, mono: int) -> int:
        return self.anchor_wall_ns + (mono - self.anchor_mono_ns) + self._offset_ns

    def _slew(self, mono: int) -> None:
        """Move the offset toward (wall - clock) by at most max_slew_ppm of the monotonic time
        elapsed since the last adjustment (never a step, never faster than the clock advances)."""
        dt = mono - self._slew_mono_ns
        if dt < SLEW_STEP_NS:
            return
        self._slew_mono_ns = mono
        err = int(self._wall()) - self._raw(mono)  # > 0: this clock is behind the wall clock
        cap = int(dt * self.max_slew_ppm / 1_000_000.0)
        self._offset_ns += max(-cap, min(cap, err))

    def __call__(self) -> int:
        mono = int(self._mono())
        self._slew(mono)
        t = self._raw(mono)
        if t <= self._last:
            t = self._last + 1
        self._last = t
        return t

    def peek(self) -> int:
        """Current reading without consuming a value (never below the last one returned)."""
        mono = int(self._mono())
        self._slew(mono)
        return max(self._last, self._raw(mono))

    def drift_ns(self) -> int:
        """Wall clock minus this clock: stays near zero while the wall clock is merely disciplined
        (the slew absorbs the monotonic clock's rate error); a wall-clock STEP after the anchor
        shows here and decays at most max_slew_ppm."""
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


__all__ = ["MAX_SLEW_PPM", "AnchoredClock", "session_token", "to_base36"]
