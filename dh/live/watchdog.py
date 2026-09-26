"""Dead-man watchdog: cancel every resting order when the live runner's heartbeat stops.

Runs as a SEPARATE process (scripts/watchdog.py) with its own Kalshi API session (its own API
key, restricted to the runner's subaccount), so it keeps working when the runner crashes, hangs
or loses its event loop. It never places orders. On a trigger it first TRIGGERS the runner's
order groups named in its last heartbeat (``PUT /portfolio/order_groups/{id}/trigger?
subaccount=<n>&exchange_index=<s>``: the fastest scoped kill, no documented trailing tail; only
groups of the configured subaccount), then cancels every resting order of the subaccount. On a
SHARED account (``venue.shared_account``) that is ``rest_scoped_cancel_all``: GET
/portfolio/orders?status=resting&subaccount=<n> and a batch cancel BY ID (each item with the
subaccount and the order's shard), repeated until the list is empty; the bulk
``DELETE /portfolio/events/orders?subaccount=<n>`` (``rest_cancel_all``) only on an account
declared not shared (its one-minute tail's subaccount scope is unverified). During an
EXCHANGE pause both are rejected (Kalshi blocks cancels too): it keeps retrying;
``cancel_order_on_pause`` protects.

It only ever locks onto a heartbeat that names ITS configured subaccount (review H2), and it
writes its own liveness file ``<heartbeat>.watchdog`` (pid, subaccount, state, armed runner,
last poll) every ``beat_interval_s`` while it runs (``EXITED`` when it stops): the live runner
refuses to start without a fresh one for its subaccount and blocks new orders while it is
stale, names another subaccount, or is not armed on it (review M3). Liveness is not capability
(review NEW-2): at start and every ``api_probe_interval_s`` it runs a read-only authenticated
probe with its OWN key (``rest_api_probe``: GET /portfolio/orders?subaccount=<n>&status=resting&
limit=1); the beat carries ``api_ok``, ``api_ok_ns`` (last success; a successful cancel-all
counts too), ``api_error`` and ``step_ok`` (false after 3 failed polls in a row), and the runner
treats a watchdog whose key cannot be shown to work as not protecting.

Timestamps (review NEW-3): a runner heartbeat stamped more than ``max_future_s`` in the FUTURE
is untrusted: it neither arms the watchdog nor refreshes the watched runner's liveness, so an
armed watchdog fires exactly as for a stale heartbeat (fail-safe: a clock step between the two
processes costs a cancel-all, never a silent dead-man switch). The same absolute-age rule
applies to the watchdog's own beat as read by the runner.

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
from dh.live.monitor import cancel_all_marker_path, read_heartbeat, watchdog_beat_path, write_heartbeat, write_json_atomic

log = logging.getLogger("dh.live.watchdog")

CancelAllFn = Callable[[], Awaitable[bool]]
TriggerFn = Callable[[list[dict[str, Any]]], Awaitable[int]]
ProbeFn = Callable[[], Awaitable[Any]]  # raises when the key cannot reach the API
STEP_FAILURES_REPORTED = 3  # consecutive failed polls before the beat says step_ok: false


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
    groups: list[dict[str, Any]] = field(default_factory=list)  # the watched runner's order groups (heartbeat)
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
        trigger_groups: TriggerFn | None = None,
        subaccount: int | None = None,
        bulk: bool = False,
        beat_path: str | Path | None = None,
        api_probe: ProbeFn | None = None,
    ) -> None:
        self.path = Path(heartbeat_path)
        self.cancel_all = cancel_all
        self.trigger_groups = trigger_groups
        self.cfg = cfg or WatchdogCfg()
        self._clock = clock_ns
        self._sleep = sleep
        self.st = WatchdogState()
        self._arm_on_start = arm_on_start  # consumed by the first poll
        self._on_event = on_event
        self.marker = Path(marker_path) if marker_path else cancel_all_marker_path(self.path)
        # the subaccount this watchdog acts for: a runner heartbeat naming another one is never armed on
        self.subaccount = None if subaccount is None else int(subaccount)
        self.bulk = bool(bulk)  # cancel_all is the bulk endpoint (a non-shared account); recorded in the marker
        self.beat_path = Path(beat_path) if beat_path else watchdog_beat_path(self.path)
        self._last_beat_ns = 0
        self._last_poll_ns = 0
        self._mismatch_noted = 0
        # capability (review NEW-2): the authenticated read-only probe of its own key
        self.api_probe = api_probe
        self.api_ok = False
        self.api_ok_ns = 0
        self.api_error = "not probed yet" if api_probe is not None else "no API probe configured"
        self.api_probes = 0
        self.step_failures = 0  # consecutive failed polls
        self.step_error = ""
        self.future_hb = 0  # runner heartbeats stamped in the future (not trusted)

    def _note(self, msg: str, **kw: Any) -> None:
        now = self._clock()
        self.st.events.append((now, msg))
        del self.st.events[:-200]
        log.warning("watchdog: %s %s", msg, kw if kw else "")
        if self._on_event is not None:
            self._on_event(msg, kw)

    def _relevant(self, mode: str) -> bool:
        return mode == "live" or not self.cfg.only_live

    def _sub_ok(self, hb: dict[str, Any]) -> bool:
        """The heartbeat names this watchdog's subaccount (review H2: a runner of another
        subaccount, or a heartbeat without one, is never armed on)."""
        if self.subaccount is None:
            return True
        s = hb.get("subaccount")
        ok = isinstance(s, int) and not isinstance(s, bool) and s == self.subaccount
        if not ok:
            self._mismatch_noted += 1
            if self._mismatch_noted == 1 or self._mismatch_noted % 1000 == 0:
                self._note("ignoring a live heartbeat of another subaccount", heartbeat_subaccount=s,
                           configured=self.subaccount, pid=hb.get("pid"), session=hb.get("session"))
        return ok

    def write_beat(self, *, state: str | None = None, force: bool = False) -> None:
        """This watchdog's own liveness file (``<heartbeat>.watchdog``), at most every
        ``beat_interval_s`` unless forced (state changes, exit)."""
        now = self._clock()
        if not force and now - self._last_beat_ns < int(self.cfg.beat_interval_s * NS_PER_S):
            return
        self._last_beat_ns = now
        try:
            write_heartbeat(self.beat_path, {
                "pid": os.getpid(), "subaccount": self.subaccount, "state": state or self.st.state,
                "armed": list(self.st.armed) if self.st.armed else None, "last_poll_ns": self._last_poll_ns,
                "heartbeat": str(self.path), "bulk_cancel": self.bulk, "stale_s": self.cfg.stale_s,
                "api_ok": self.api_ok, "api_ok_ns": self.api_ok_ns or None, "api_error": self.api_error,
                "step_ok": self.step_failures < STEP_FAILURES_REPORTED, "step_error": self.step_error}, now_ns=now)
        except OSError as exc:
            log.error("watchdog: cannot write its beat %s: %s", self.beat_path, exc)

    async def probe_api(self) -> bool:
        """One read-only authenticated probe with the watchdog's own key (review NEW-2); the result
        goes into the beat (``api_ok``, ``api_ok_ns``, ``api_error``). Bounded by
        ``api_probe_timeout_s``. A successful cancel-all also counts as proof."""
        if self.api_probe is None:
            return False
        self.api_probes += 1
        try:
            await asyncio.wait_for(self.api_probe(), timeout=max(0.1, self.cfg.api_probe_timeout_s))
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - any failure: the key cannot be shown to work
            was = self.api_ok
            self.api_ok = False
            self.api_error = f"{type(exc).__name__}: {exc}"[:200] or type(exc).__name__
            if was or self.api_probes == 1:
                self._note("API probe FAILED: the watchdog's key cannot be shown to reach the API", error=self.api_error)
            self.write_beat(force=True)
            return False
        was = self.api_ok
        self.api_ok, self.api_ok_ns, self.api_error = True, self._clock(), ""
        if not was:
            self._note("API probe OK (the watchdog's key reaches the API)")
            self.write_beat(force=True)
        return True

    async def _probe_loop(self, stop: asyncio.Event | None) -> None:
        while stop is None or not stop.is_set():
            await self.probe_api()
            await self._sleep(max(0.05, self.cfg.api_probe_interval_s))

    def _hb_time_ok(self, t: int | None, now: int) -> bool:
        """A runner heartbeat time that is not in the FUTURE beyond ``max_future_s`` (review NEW-3):
        a future-stamped heartbeat (a clock step, a writer with a wrong clock) is not trusted, so
        it never refreshes the watched runner's liveness (the watchdog then fires as for a stale
        one) and never arms."""
        if t is None:
            return False
        if now - t < -int(self.cfg.max_future_s * NS_PER_S):
            self.future_hb += 1
            if self.future_hb == 1 or self.future_hb % 1000 == 0:
                self._note("runner heartbeat stamped in the FUTURE: not trusted (treated as stale)",
                           ahead_s=round((t - now) / NS_PER_S, 3))
            return False
        return True

    def _arm(self, hb: dict[str, Any], note: str) -> None:
        st = self.st
        st.state = "ARMED"
        st.armed = (hb.get("pid"), hb.get("session"))
        st.last_hb_ns = int(hb.get("t", 0))
        st.last_state = str(hb.get("state", "running"))
        st.stopping_seen_ns = 0
        st.shutdown_timeout_s = float(hb.get("shutdown_timeout_s") or 10.0)
        st.groups = _groups_of(hb)
        self._note(note, pid=hb.get("pid"), session=hb.get("session"), groups=len(st.groups))

    async def step(self) -> str:
        """One poll; returns the resulting state."""
        now = self._clock()
        self._last_poll_ns = now
        c = self.cfg
        st = self.st
        stale_ns = int(c.stale_s * NS_PER_S)
        hb = read_heartbeat(self.path)
        t = int(hb.get("t", 0)) if hb is not None else None
        mode = str(hb.get("mode", "live")) if hb is not None else st.last_mode
        hstate = str(hb.get("state", "running")) if hb is not None else st.last_state
        t_ok = self._hb_time_ok(t, now)
        fresh = t_ok and now - t <= stale_ns  # type: ignore[operator]
        ident = (hb.get("pid"), hb.get("session")) if hb is not None else None
        if hb is not None:
            st.last_mode = mode
        if self._arm_on_start:
            self._arm_on_start = False
            live = (hb is not None and not hb.get("unparsed") and self._relevant(mode)
                    and hstate in ("running", "stopping") and self._sub_ok(hb))
            if live:  # lock onto the live runner the file names; the ARMED branch triggers if stale
                self._arm(hb, "armed on start: existing live heartbeat" + ("" if fresh else " (STALE)"))  # type: ignore[arg-type]
            else:
                why = ("no heartbeat file" if hb is None else "unreadable heartbeat" if hb.get("unparsed")
                       else f"heartbeat mode={mode} state={hstate}")
                self._note(f"arm-on-start: {why}: DISARMED until a fresh live heartbeat")
        same = hb is not None and st.armed is not None and ident == st.armed
        # a future-stamped heartbeat of the watched runner vouches for nothing: its liveness (and
        # state) is not refreshed, so the ARMED branch below fires once the last trusted one is stale
        ours = same and t_ok
        if same and not t_ok and "order_groups" in hb:  # type: ignore[operator]
            st.groups = _groups_of(hb)  # type: ignore[arg-type]  # still the freshest list of groups to trigger
        if ours:
            st.last_hb_ns = max(st.last_hb_ns, t or 0)
            st.last_state = hstate
            st.shutdown_timeout_s = float(hb.get("shutdown_timeout_s") or st.shutdown_timeout_s)
            if "order_groups" in hb:  # the runner deletes its groups at shutdown: follow the heartbeat
                st.groups = _groups_of(hb)
            if hstate == "stopping":
                st.stopping_seen_ns = st.stopping_seen_ns or now
            else:
                st.stopping_seen_ns = 0
        elif not same and hb is not None and st.armed is not None and st.state != "DISARMED":
            st.foreign += 1
            if st.foreign == 1 or st.foreign % 1000 == 0:
                self._note("ignoring a heartbeat from another writer", pid=hb.get("pid"), session=hb.get("session"),
                           mode=mode, state=hstate)
        parsed = hb is not None and not hb.get("unparsed")  # an unreadable file names no runner to lock onto
        if st.state == "DISARMED":
            if fresh and parsed and self._relevant(mode) and hstate in ("running", "stopping") and self._sub_ok(hb):  # type: ignore[arg-type]
                self._arm(hb, "armed")  # type: ignore[arg-type]
            return st.state
        if st.state == "ARMED":
            if ours and hstate == "stopped" and fresh:
                st.state, st.armed = "DISARMED", None
                self._note("disarmed: runner stopped cleanly")
                return st.state
            stuck = bool(st.stopping_seen_ns) and now - st.stopping_seen_ns > int(
                (st.shutdown_timeout_s + c.stopping_grace_s) * NS_PER_S)
            age = now - st.last_hb_ns
            # absolute age (review NEW-3): a recorded heartbeat now in the future (the watchdog's clock
            # stepped back) is as untrusted as a stale one
            if hb is None or age > stale_ns or age < -int(c.max_future_s * NS_PER_S) or stuck:
                st.state = "TRIGGERED"
                st.triggered_at_ns = now
                st.successes = 0
                why = "heartbeat file missing" if hb is None else ("shutdown hung" if stuck else "heartbeat stale")
                self._note(f"TRIGGERED: {why}", age_s=(now - st.last_hb_ns) / NS_PER_S if st.last_hb_ns else None)
                await self._attempt(now)
            return st.state
        # TRIGGERED
        if parsed and fresh and self._relevant(mode) and hstate == "running" and self._sub_ok(hb):  # type: ignore[arg-type]
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
        triggered = 0
        if self.trigger_groups is not None and st.groups:
            try:  # first, fastest scoped kill: the runner's own order groups
                triggered = int(await self.trigger_groups(list(st.groups)))
                self._note("order groups triggered", n=triggered, of=len(st.groups))
            except Exception as exc:  # noqa: BLE001 - the cancel-all below still runs
                self._note("order-group trigger raised", error=f"{type(exc).__name__}: {exc}"[:200])
        try:
            ok = bool(await self.cancel_all())
        except Exception as exc:  # noqa: BLE001 - keep trying
            ok = False
            self._note("cancel-all raised", error=f"{type(exc).__name__}: {exc}"[:200])
        if ok:
            st.successes += 1
            st.last_success_ns = self._clock()
            self.api_ok, self.api_ok_ns, self.api_error = True, st.last_success_ns, ""  # it did reach the API
            self._note("cancel-all OK", n=st.successes)
        else:
            st.failures += 1
            self._note("cancel-all FAILED (will retry)", failures=st.failures)
        try:
            write_json_atomic(self.marker, {"t": now, "ok": ok, "by": "watchdog", "pid": os.getpid(),
                                            "watched": list(st.armed) if st.armed else None,
                                            "groups_triggered": triggered, "bulk": self.bulk})
        except OSError as exc:
            self._note("cancel-all marker write failed", error=str(exc)[:200])

    async def run(self, stop: asyncio.Event | None = None) -> None:
        """Poll until ``stop``; the own beat is written every ``beat_interval_s`` (at once on a
        state change) and set to EXITED when the loop ends."""
        probe: asyncio.Task[Any] | None = None
        try:
            if self.api_probe is not None:
                await self.probe_api()  # at start, before the first beat vouches for anything
                probe = asyncio.ensure_future(self._probe_loop_after_first(stop))
            while stop is None or not stop.is_set():
                before = (self.st.state, self.st.armed, self.step_failures >= STEP_FAILURES_REPORTED)
                try:
                    await self.step()
                    self.step_failures, self.step_error = 0, ""
                except Exception as exc:  # noqa: BLE001 - the watchdog must not die
                    self.step_failures += 1
                    self.step_error = f"{type(exc).__name__}: {exc}"[:200]
                    log.exception("watchdog step failed (%d in a row)", self.step_failures)
                after = (self.st.state, self.st.armed, self.step_failures >= STEP_FAILURES_REPORTED)
                self.write_beat(force=after != before)
                await self._sleep(self.cfg.poll_s)
        finally:
            if probe is not None:
                probe.cancel()
                await asyncio.gather(probe, return_exceptions=True)
            self.write_beat(state="EXITED", force=True)

    async def _probe_loop_after_first(self, stop: asyncio.Event | None) -> None:
        await self._sleep(max(0.05, self.cfg.api_probe_interval_s))
        await self._probe_loop(stop)


def _groups_of(hb: dict[str, Any]) -> list[dict[str, Any]]:
    out = []
    for g in hb.get("order_groups") or []:
        if isinstance(g, dict) and g.get("id"):
            out.append({"id": str(g["id"]), "exchange_index": g.get("exchange_index"), "subaccount": g.get("subaccount"),
                        "logical": g.get("logical")})
    return out


def rest_cancel_all(rest: Any, subaccount: int) -> CancelAllFn:
    """BULK cancel_all for Watchdog on top of KalshiRest: True only on a definite 2xx. The
    subaccount is REQUIRED and always sent (0 = the primary): Kalshi reads an omitted
    subaccount as ALL subaccounts. ONLY for an account declared NOT shared (the one-minute
    tail's subaccount scope is unverified); a shared account uses ``rest_scoped_cancel_all``."""
    from dh.kalshi.rest import UnknownOutcome

    if subaccount is None or isinstance(subaccount, bool):
        raise ValueError("rest_cancel_all needs an explicit subaccount (omitted = ALL subaccounts)")
    sub = int(subaccount)

    async def _cancel() -> bool:
        res = await rest.cancel_all_orders(subaccount=sub)
        return not isinstance(res, UnknownOutcome)

    return _cancel


def rest_scoped_cancel_all(rest: Any, subaccount: int, *, rounds: int = 3, batch: int = 20,
                           sleep: Callable[[float], Awaitable[None]] = asyncio.sleep) -> CancelAllFn:
    """cancel_all for Watchdog on a SHARED account (review M5): never the bulk endpoint.
    GET /portfolio/orders?status=resting&subaccount=<n> (every page, every shard), then
    DELETE /portfolio/events/orders/batched BY ID, every item naming the subaccount and the
    order's own shard (-1 = auto-route by ticker when the row has none), repeated until the list
    is empty or after ``rounds``. True only when the final list is empty."""
    from dh.kalshi.normalize import shard_value

    if subaccount is None or isinstance(subaccount, bool):
        raise ValueError("rest_scoped_cancel_all needs an explicit subaccount")
    sub = int(subaccount)

    async def _cancel() -> bool:
        for rnd in range(max(1, rounds) + 1):
            rows = [o async for o in rest.iter_orders(status="resting", subaccount=sub)]
            rows = [o for o in rows if o.get("order_id")]
            if not rows:
                return True
            if rnd == max(1, rounds):
                log.error("watchdog: %d orders of subaccount %d still resting after %d cancel rounds", len(rows), sub, rounds)
                return False
            items = []
            for o in rows:
                sh = shard_value(o.get("exchange_index"))
                items.append({"order_id": str(o["order_id"]), "market_ticker": str(o.get("ticker") or ""),
                              "subaccount": sub, "exchange_index": -1 if sh is None else sh})
            for i in range(0, len(items), max(1, batch)):
                try:
                    await rest.batch_cancel_orders(items[i:i + max(1, batch)])
                except Exception as exc:  # noqa: BLE001 - the next round lists what still rests
                    log.warning("watchdog: batch cancel of %d orders failed: %s", len(items[i:i + batch]), exc)
            await sleep(0.5 * (rnd + 1))
        return False

    return _cancel


def rest_api_probe(rest: Any, subaccount: int) -> ProbeFn:
    """The watchdog's capability probe (review NEW-2): GET /portfolio/orders?subaccount=<n>&
    status=resting&limit=1 with its own key: read-only, explicitly scoped to its subaccount, and
    the same list the scoped cancel-all starts from. Raises on any failure (401/403, network)."""
    if subaccount is None or isinstance(subaccount, bool):
        raise ValueError("rest_api_probe needs an explicit subaccount")
    sub = int(subaccount)

    async def _probe() -> Any:
        body = await rest.get_orders(subaccount=sub, status="resting", limit=1)
        if not isinstance(body, dict) or not isinstance(body.get("orders", []), list):
            raise ValueError(f"unexpected GET /portfolio/orders body: {str(body)[:120]}")
        return body

    return _probe


def rest_trigger_groups(rest: Any, subaccount: int) -> TriggerFn:
    """trigger_groups for Watchdog: ``PUT /portfolio/order_groups/{id}/trigger`` with the
    explicit subaccount and the group's exchange shard, for every group the runner's heartbeat
    names ON THIS SUBACCOUNT (a group naming another subaccount, or no shard, is skipped: never
    act on another system's subaccount). Returns the number triggered (2xx)."""
    from dh.kalshi.normalize import shard_value
    from dh.kalshi.rest import UnknownOutcome

    if subaccount is None or isinstance(subaccount, bool):
        raise ValueError("rest_trigger_groups needs an explicit subaccount")
    sub = int(subaccount)

    async def _trigger(groups: list[dict[str, Any]]) -> int:
        n = 0
        for g in groups:
            sh = shard_value(g.get("exchange_index"))
            gs = g.get("subaccount")
            if sh is None or gs is None or int(gs) != sub:
                log.warning("watchdog: order group %s skipped (subaccount %s, shard %s; configured subaccount %d)",
                            g.get("id"), gs, g.get("exchange_index"), sub)
                continue
            try:
                res = await rest.trigger_order_group(str(g["id"]), subaccount=sub, exchange_index=sh)
            except Exception as exc:  # noqa: BLE001 - 404 (deleted), 429, network: the cancel-all follows
                log.warning("watchdog: trigger of order group %s failed: %s", g.get("id"), exc)
                continue
            if not isinstance(res, UnknownOutcome):
                n += 1
        return n

    return _trigger
