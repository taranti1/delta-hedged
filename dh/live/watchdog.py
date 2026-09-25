"""Dead-man watchdog: cancel every resting order when the live runner's heartbeat stops.

Runs as a SEPARATE process (scripts/watchdog.py) with its own Kalshi API session (optionally
its own API key), so it keeps working when the runner crashes, hangs or loses its event loop.
It only ever calls ``DELETE /portfolio/events/orders?subaccount=<n>`` (cancel all, with the
subaccount explicit: omitted means ALL subaccounts): it never places orders.

State machine (poll every ``poll_s``):
  DISARMED   waiting for a fresh heartbeat of a LIVE runner (state running/stopping); it then
             LOCKS ONTO that runner (pid + session)
  ARMED      only the locked runner's heartbeats count: a heartbeat written by any other
             process (a paper runner sharing the file, a second instance) is ignored, so it
             can neither disarm the watchdog nor keep it quiet. The locked runner's 'stopped'
             (clean shutdown after a confirmed cancel-all) disarms
  TRIGGERED  the locked runner's last heartbeat is older than ``stale_s``, the file vanished,
             or the runner has been 'stopping' for longer than its own shutdown_timeout_s +
             ``stopping_grace_s`` (a hung shutdown): cancel all now, retry every ``retry_s``
             until one succeeds, then repeat every ``repeat_s`` (orders in flight when the
             runner died can still land) up to ``max_repeats`` while stale; a fresh heartbeat
             of the locked runner, or of a NEW live runner (a restart), re-arms on it

``arm_on_start`` (a watchdog restarted while a runner may have died meanwhile) acts on the
FIRST poll only, and only on an existing LIVE heartbeat (live mode, state running/stopping):
it locks onto that runner, and triggers at once when the heartbeat is already stale. A
missing file, an unreadable one, or any other heartbeat (paper, 'starting', 'stopped') leaves
it DISARMED, waiting for a fresh live heartbeat as usual.

After every cancel-all attempt the watchdog writes ``<heartbeat>.cancel_all`` ({"t", "ok",
"watched": [pid, session]}). A live runner that finds a marker written after its own start
about ITSELF halts (Halt(all): it was alive but unresponsive); one about another runner holds
new orders for the cancel-all tail and reconciles. A runner renames a marker older than its
start at start-up.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from dh.core.units import NS_PER_S
from dh.live.config import WatchdogCfg
from dh.live.monitor import cancel_all_marker_path, read_heartbeat, write_json_atomic

log = logging.getLogger("dh.live.watchdog")

CancelAllFn = Callable[[], Awaitable[bool]]


@dataclass
class WatchdogState:
    state: str = "DISARMED"
    armed: tuple[Any, Any] | None = None  # (pid, session) of the runner we watch
    last_hb_ns: int = 0  # the watched runner's last heartbeat time
    last_mode: str = ""
    last_state: str = ""
    stopping_seen_ns: int = 0  # when the watched runner was first seen 'stopping'
    shutdown_timeout_s: float = 10.0  # the watched runner's own (from its heartbeat)
    triggered_at_ns: int = 0
    last_attempt_ns: int = 0
    last_success_ns: int = 0
    successes: int = 0
    attempts: int = 0
    failures: int = 0
    foreign: int = 0  # heartbeats from other writers ignored
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
        marker_path: str | Path | None = None,
    ) -> None:
        self.path = Path(heartbeat_path)
        self.cancel_all = cancel_all
        self.cfg = cfg or WatchdogCfg()
        self._clock = clock_ns
        self._sleep = sleep
        self.st = WatchdogState()
        self._arm_on_start = arm_on_start  # consumed by the first poll
        self._on_event = on_event
        self.marker = Path(marker_path) if marker_path else cancel_all_marker_path(self.path)

    def _note(self, msg: str, **kw: Any) -> None:
        now = self._clock()
        self.st.events.append((now, msg))
        del self.st.events[:-200]
        log.warning("watchdog: %s %s", msg, kw if kw else "")
        if self._on_event is not None:
            self._on_event(msg, kw)

    def _relevant(self, mode: str) -> bool:
        return mode == "live" or not self.cfg.only_live

    def _arm(self, hb: dict[str, Any], note: str) -> None:
        st = self.st
        st.state = "ARMED"
        st.armed = (hb.get("pid"), hb.get("session"))
        st.last_hb_ns = int(hb.get("t", 0))
        st.last_state = str(hb.get("state", "running"))
        st.stopping_seen_ns = 0
        st.shutdown_timeout_s = float(hb.get("shutdown_timeout_s") or 10.0)
        self._note(note, pid=hb.get("pid"), session=hb.get("session"))

    async def step(self) -> str:
        """One poll; returns the resulting state."""
        now = self._clock()
        c = self.cfg
        st = self.st
        stale_ns = int(c.stale_s * NS_PER_S)
        hb = read_heartbeat(self.path)
        t = int(hb.get("t", 0)) if hb is not None else None
        mode = str(hb.get("mode", "live")) if hb is not None else st.last_mode
        hstate = str(hb.get("state", "running")) if hb is not None else st.last_state
        fresh = t is not None and now - t <= stale_ns
        ident = (hb.get("pid"), hb.get("session")) if hb is not None else None
        if hb is not None:
            st.last_mode = mode
        if self._arm_on_start:
            self._arm_on_start = False
            live = (hb is not None and not hb.get("unparsed") and self._relevant(mode)
                    and hstate in ("running", "stopping"))
            if live:  # lock onto the live runner the file names; the ARMED branch triggers if stale
                self._arm(hb, "armed on start: existing live heartbeat" + ("" if fresh else " (STALE)"))  # type: ignore[arg-type]
            else:
                why = ("no heartbeat file" if hb is None else "unreadable heartbeat" if hb.get("unparsed")
                       else f"heartbeat mode={mode} state={hstate}")
                self._note(f"arm-on-start: {why}: DISARMED until a fresh live heartbeat")
        ours = hb is not None and st.armed is not None and ident == st.armed
        if ours:
            st.last_hb_ns = max(st.last_hb_ns, t or 0)
            st.last_state = hstate
            st.shutdown_timeout_s = float(hb.get("shutdown_timeout_s") or st.shutdown_timeout_s)
            if hstate == "stopping":
                st.stopping_seen_ns = st.stopping_seen_ns or now
            else:
                st.stopping_seen_ns = 0
        elif hb is not None and st.armed is not None and st.state != "DISARMED":
            st.foreign += 1
            if st.foreign == 1 or st.foreign % 1000 == 0:
                self._note("ignoring a heartbeat from another writer", pid=hb.get("pid"), session=hb.get("session"),
                           mode=mode, state=hstate)
        parsed = hb is not None and not hb.get("unparsed")  # an unreadable file names no runner to lock onto
        if st.state == "DISARMED":
            if fresh and parsed and self._relevant(mode) and hstate in ("running", "stopping"):
                self._arm(hb, "armed")  # type: ignore[arg-type]
            return st.state
        if st.state == "ARMED":
            if ours and hstate == "stopped" and fresh:
                st.state, st.armed = "DISARMED", None
                self._note("disarmed: runner stopped cleanly")
                return st.state
            stuck = bool(st.stopping_seen_ns) and now - st.stopping_seen_ns > int(
                (st.shutdown_timeout_s + c.stopping_grace_s) * NS_PER_S)
            if hb is None or now - st.last_hb_ns > stale_ns or stuck:
                st.state = "TRIGGERED"
                st.triggered_at_ns = now
                st.successes = 0
                why = "heartbeat file missing" if hb is None else ("shutdown hung" if stuck else "heartbeat stale")
                self._note(f"TRIGGERED: {why}", age_s=(now - st.last_hb_ns) / NS_PER_S if st.last_hb_ns else None)
                await self._attempt(now)
            return st.state
        # TRIGGERED
        if parsed and fresh and self._relevant(mode) and hstate == "running":
            # the watched runner recovered, or a new live runner started (a restart)
            self._arm(hb, "re-armed: runner alive again" if ours else "re-armed on a new live runner")
            return st.state
        if ours and fresh and hstate == "stopped":
            st.state, st.armed = "DISARMED", None
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
        try:
            write_json_atomic(self.marker, {"t": now, "ok": ok, "by": "watchdog", "pid": os.getpid(),
                                            "watched": list(st.armed) if st.armed else None})
        except OSError as exc:
            self._note("cancel-all marker write failed", error=str(exc)[:200])

    async def run(self, stop: asyncio.Event | None = None) -> None:
        while stop is None or not stop.is_set():
            try:
                await self.step()
            except Exception:  # noqa: BLE001 - the watchdog must not die
                log.exception("watchdog step failed")
            await self._sleep(self.cfg.poll_s)


def rest_cancel_all(rest: Any, subaccount: int | None = None) -> CancelAllFn:
    """cancel_all for Watchdog on top of KalshiRest: True only on a definite 2xx. The
    subaccount is always explicit (None -> 0, the primary): Kalshi reads an omitted
    subaccount as ALL subaccounts."""
    from dh.kalshi.rest import UnknownOutcome

    sub = int(subaccount) if subaccount is not None else 0

    async def _cancel() -> bool:
        res = await rest.cancel_all_orders(subaccount=sub)
        return not isinstance(res, UnknownOutcome)

    return _cancel
