"""Dead-man watchdog: cancel every resting order when the live runner's heartbeat stops.

Runs as a SEPARATE process (scripts/watchdog.py) with its own Kalshi API session (optionally
its own API key), so it keeps working when the runner crashes, hangs or loses its event loop.
It only ever calls ``DELETE /portfolio/events/orders`` (cancel all): it never places orders.

State machine (poll every ``poll_s``):
  DISARMED   waiting for a fresh heartbeat of a LIVE runner (state running/stopping)
  ARMED      heartbeat fresh; a heartbeat with state 'stopped' (clean shutdown after a
             confirmed cancel-all) disarms
  TRIGGERED  heartbeat older than ``stale_s`` (or the file vanished): cancel all now, retry
             every ``retry_s`` until one succeeds, then repeat every ``repeat_s`` (orders in
             flight when the runner died can still land) up to ``max_repeats`` while stale;
             a fresh heartbeat re-arms (runner recovered / restarted)
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from dh.core.units import NS_PER_S
from dh.live.config import WatchdogCfg
from dh.live.monitor import read_heartbeat

log = logging.getLogger("dh.live.watchdog")

CancelAllFn = Callable[[], Awaitable[bool]]


@dataclass
class WatchdogState:
    state: str = "DISARMED"
    last_hb_ns: int = 0
    last_mode: str = ""
    last_state: str = ""
    triggered_at_ns: int = 0
    last_attempt_ns: int = 0
    last_success_ns: int = 0
    successes: int = 0
    attempts: int = 0
    failures: int = 0
    events: list[tuple[int, str]] = field(default_factory=list)  # (ns, message), bounded


class Watchdog:
    """Pure-ish state machine + async loop. ``cancel_all`` returns True on success."""

    def __init__(
        self,
        heartbeat_path: str | Path,
        cancel_all: CancelAllFn,
        cfg: WatchdogCfg | None = None,
        *,
        clock_ns: Callable[[], int] = time.time_ns,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        arm_on_start: bool = False,
        on_event: Callable[[str, dict[str, Any]], None] | None = None,
    ) -> None:
        self.path = Path(heartbeat_path)
        self.cancel_all = cancel_all
        self.cfg = cfg or WatchdogCfg()
        self._clock = clock_ns
        self._sleep = sleep
        self.st = WatchdogState(state="ARMED" if arm_on_start else "DISARMED")
        self._on_event = on_event

    def _note(self, msg: str, **kw: Any) -> None:
        now = self._clock()
        self.st.events.append((now, msg))
        del self.st.events[:-200]
        log.warning("watchdog: %s %s", msg, kw if kw else "")
        if self._on_event is not None:
            self._on_event(msg, kw)

    def _read(self) -> tuple[int | None, str, str]:
        """(heartbeat ns or None if missing, mode, state). A missing file keeps the last
        known mode/state: a runner that vanished is exactly what the watchdog is for."""
        hb = read_heartbeat(self.path)
        if hb is None:
            return None, self.st.last_mode, self.st.last_state
        mode, state = str(hb.get("mode", "live")), str(hb.get("state", "running"))
        self.st.last_mode, self.st.last_state = mode, state
        return int(hb.get("t", 0)), mode, state

    async def step(self) -> str:
        """One poll; returns the resulting state."""
        now = self._clock()
        t, mode, hstate = self._read()
        c = self.cfg
        stale_ns = int(c.stale_s * NS_PER_S)
        st = self.st
        relevant = (mode == "live") or not c.only_live
        fresh = t is not None and now - t <= stale_ns
        if t is not None:
            st.last_hb_ns = t
        if st.state == "DISARMED":
            if fresh and relevant and hstate in ("running", "stopping"):
                st.state = "ARMED"
                self._note("armed", mode=mode)
            return st.state
        if st.state == "ARMED":
            if not relevant:
                st.state = "DISARMED"
                self._note("disarmed: heartbeat is not from a live runner", mode=mode)
            elif hstate == "stopped" and fresh:
                st.state = "DISARMED"
                self._note("disarmed: runner stopped cleanly")
            elif not fresh:
                st.state = "TRIGGERED"
                st.triggered_at_ns = now
                st.successes = 0
                age = (now - t) / NS_PER_S if t is not None else None
                self._note("TRIGGERED: heartbeat stale", age_s=age, missing=t is None)
                await self._attempt(now)
            return st.state
        # TRIGGERED
        if fresh and relevant and hstate in ("running", "stopping"):
            st.state = "ARMED"
            self._note("re-armed: heartbeat fresh again")
            return st.state
        if fresh and hstate == "stopped":
            st.state = "DISARMED"
            self._note("disarmed: runner stopped cleanly (cancel confirmed) after trigger")
            return st.state
        if st.successes == 0:
            if now - st.last_attempt_ns >= int(c.retry_s * NS_PER_S):
                await self._attempt(now)
        elif st.successes <= c.max_repeats and now - st.last_success_ns >= int(c.repeat_s * NS_PER_S):
            await self._attempt(now)
        return st.state

    async def _attempt(self, now: int) -> None:
        st = self.st
        st.attempts += 1
        st.last_attempt_ns = now
        try:
            ok = bool(await self.cancel_all())
        except Exception as exc:  # noqa: BLE001 - keep trying
            ok = False
            self._note("cancel-all raised", error=f"{type(exc).__name__}: {exc}"[:200])
        if ok:
            st.successes += 1
            st.last_success_ns = self._clock()
            self._note("cancel-all OK", n=st.successes)
        else:
            st.failures += 1
            self._note("cancel-all FAILED (will retry)", failures=st.failures)

    async def run(self, stop: asyncio.Event | None = None) -> None:
        while stop is None or not stop.is_set():
            try:
                await self.step()
            except Exception:  # noqa: BLE001 - the watchdog must not die
                log.exception("watchdog step failed")
            await self._sleep(self.cfg.poll_s)


def rest_cancel_all(rest: Any, subaccount: int | None = None) -> CancelAllFn:
    """cancel_all for Watchdog on top of KalshiRest: True only on a definite 2xx."""
    from dh.kalshi.rest import UnknownOutcome

    async def _cancel() -> bool:
        res = await rest.cancel_all_orders(subaccount=subaccount)
        return not isinstance(res, UnknownOutcome)

    return _cancel
