"""Kalshi order venue for the live runner: strategy actions -> REST V2 -> result events.

    venue = KalshiVenue(rest, sink=runner.push_result, cfg=live_cfg.venue)
    venue.submit(actions, decision_ns)       # from the consumer; never blocks (asyncio tasks)

Every REST write runs in its own asyncio task; its outcome comes back through ``sink`` as
ordinary events stamped with the local receive time (``OrderAck`` / ``OrderReject`` /
``CancelAck`` / ``KalshiOrderUpdate``), which the runner records and queues for the strategy.

Mapping (bodies via dh.kalshi.orders, results via dh.kalshi.orders.*_result_to_events):
  PlaceOrder               POST /portfolio/events/orders; several places decided together go
                           to POST .../orders/batched (chunks of <= max_batch, also capped by
                           the write bucket). A quote that would wait longer than
                           ``max_place_wait_s`` for write tokens is rejected locally
                           (reason 'rate_budget') instead of arriving stale.
  CancelOrder              DELETE /portfolio/events/orders/{id} (several -> DELETE .../batched)
  CancelAll()              DELETE /portfolio/events/orders (every resting order; retried: idempotent)
  CancelAll(tickers)       the strategy already sends one CancelOrder per known order; the venue
                           additionally sweeps GET /portfolio/orders?status=resting&ticker=...
                           and cancels whatever is still resting there
  AmendOrder/DecreaseOrder amend_order / decrease_order (not used by the M1 strategy)
  Create/Reset/UpdateLimit/DeleteOrderGroup
                           order-group endpoints. The exchange assigns group ids: the venue maps
                           the strategy's logical id (MarketMaker.ORDER_GROUP_ID) to it.
                           Reset/limit updates are retried (idempotent).

Outcomes (never resubmit a create blindly):
  2xx                      -> events from dh.kalshi.orders
  KalshiHTTPError (4xx)    -> OrderReject (definite). Cancel rejects are normalized for the
                              OrderManager: HTTP 404 -> 'not_found', 429 -> 'rate_limited';
                              every other cancel failure is also reconciled (GET order)
  HTTP 409 on a create     -> OrderReject('duplicate_client_order_id'): definite (Kalshi refused
                              the id; creates are never retried, so it cannot be our own order)
  NotSentError             -> OrderReject(reason='not_sent') (the request never left)
  UnknownOutcome / other   -> no event now; create: reconcile by client_order_id (GET
                              /portfolio/orders with the explicit subaccount; a match counts only
                              if it was created at or after the request time minus
                              ``create_match_skew_s``) -> KalshiOrderUpdate, or
                              OrderReject('reconciled_missing') once it has stayed absent for
                              ``reconcile_missing_after_s``; a missing create is looked for again
                              after ``missing_recheck_s`` and cancelled if it turns up resting;
                              cancel: GET /portfolio/orders/{id} -> KalshiOrderUpdate, re-cancel
                              (capped backoff) for as long as it is still resting: never dropped;
                              after ``max_cancel_retries`` the order is flagged STUCK (metric +
                              log). A 404 on that GET is final only when the resting-order list
                              confirms the order is gone.

Subaccount: every request carries ``subaccount`` explicitly (0 = primary): Kalshi reads an
omitted subaccount as ALL subaccounts on GET orders / GET fills / cancel-all.

Write budget: a place is dispatched only if its write tokens would be available within
``max_place_wait_s``. Tokens of requests dispatched but not yet through the rate limiter are
reserved; the reservation is released the moment the limiter hands the request its tokens
(the limiter's ``acquire`` is wrapped once, per limiter instance), so in-flight requests are
never counted twice.

Periodic reads (tasks started by the runner, each skipped when the read bucket is low):
  queue positions   GET /portfolio/orders/queue_positions every ``queue_positions_interval_s``
  positions         GET /portfolio/positions every ``positions_interval_s`` (+ resting orders
                    for the ghost-order sweep); the runner compares them with the strategy.
"""

from __future__ import annotations

import asyncio
import contextvars
import logging
import time
from collections.abc import Awaitable, Callable, Iterable, Sequence
from dataclasses import dataclass, field, replace
from typing import Any

from dh.core.actions import (
    Action,
    AmendOrder,
    CancelAll,
    CancelOrder,
    CreateOrderGroup,
    DecreaseOrder,
    DeleteOrderGroup,
    PlaceOrder,
    ResetOrderGroup,
    UpdateOrderGroupLimit,
)
from dh.core.events import CancelAck, Event, KalshiOrderUpdate, OrderAck, OrderReject
from dh.core.units import NS_PER_S, qty_from_fp
from dh.kalshi.normalize import order_to_update
from dh.kalshi.orders import (
    amend_order_body,
    canonical_reason,
    amend_result_to_events,
    batch_create_results_to_events,
    cancel_result_to_events,
    create_result_to_events,
    decrease_order_kwargs,
    decrease_result_to_events,
    place_order_body,
)
from dh.kalshi.rest import KalshiError, KalshiHTTPError, NotSentError, UnknownOutcome
from dh.kalshi.wire import ms_to_ns, opt_iso_to_ns, opt_qty
from dh.live.config import VenueCfg

log = logging.getLogger("dh.live.venue")

Sink = Callable[[Event], None]
LogFn = Callable[[str, dict[str, Any]], None]
TIF = {"gtc": "good_till_canceled", "ioc": "immediate_or_cancel", "fok": "fill_or_kill"}
CREATE_PATH = "/portfolio/events/orders"
BATCH_CREATE_PATH = "/portfolio/events/orders/batched"
KALSHI_ORDER_ACTIONS = (PlaceOrder, CancelOrder, AmendOrder, DecreaseOrder, CancelAll, CreateOrderGroup,
                        ResetOrderGroup, UpdateOrderGroupLimit, DeleteOrderGroup)
DUPLICATE_ID_REASON = "duplicate_client_order_id"
MAX_OID_MAP = 100_000


class _Reservation:
    """Write tokens reserved for one dispatched request until the limiter grants them."""

    __slots__ = ("venue", "cost", "done")

    def __init__(self, venue: KalshiVenue, cost: float) -> None:
        self.venue = venue
        self.cost = cost
        self.done = cost <= 0

    def release(self) -> None:
        if not self.done:
            self.done = True
            self.venue._reserved_write = max(0.0, self.venue._reserved_write - self.cost)  # noqa: SLF001


_RESERVATION: contextvars.ContextVar[_Reservation | None] = contextvars.ContextVar("dh_venue_reservation", default=None)


def _hook_limiter(lim: Any) -> None:
    """Wrap ``lim.acquire`` once: when the limiter grants tokens to a request running inside a
    venue task, that task's reservation is released (see the module docstring)."""
    if lim is None or getattr(lim, "_dh_reservation_hook", False):
        return
    orig = lim.acquire

    async def acquire(method: str, path: str, n_items: int = 1) -> float:
        try:
            return await orig(method, path, n_items)
        finally:
            r = _RESERVATION.get()
            if r is not None:
                r.release()

    try:
        lim.acquire = acquire
        lim._dh_reservation_hook = True  # noqa: SLF001
    except AttributeError:  # pragma: no cover - a limiter without instance attributes
        pass


def _created_ns(o: dict[str, Any]) -> int:
    """Creation time of a REST Order (created_ts_ms, else created_time), 0 if unknown."""
    return ms_to_ns(o.get("created_ts_ms")) or opt_iso_to_ns(o.get("created_time")) or 0


def cancel_reject_reason(status: int, code: str, message: str) -> str:
    """A failed cancel in the OrderManager's reject vocabulary (dh.kalshi.orders.canonical_reason:
    not_found / already_filled / already_canceled / rate_limited / ...). Every failure except
    'rate_limited' (not processed: the order still rests) is also reconciled with
    GET /portfolio/orders/{id}, so the exact final state never depends on the wording."""
    return canonical_reason(KalshiHTTPError("DELETE", "/portfolio/events/orders", status,
                                            {"error": {"code": code, "message": message}}))


def batch_cancel_results_to_events(
    res: dict[str, Any] | UnknownOutcome | KalshiHTTPError, acts: Sequence[CancelOrder], recv_ns: int
) -> tuple[list[Event], list[CancelOrder]]:
    """BatchCancelOrdersV2Response -> (CancelAck / OrderReject events, cancels to reconcile)."""
    if isinstance(res, KalshiHTTPError):
        reason = canonical_reason(res)
        evs: list[Event] = [OrderReject(recv_ns, 0, a.client_order_id, a.ticker, reason, res.status, "cancel") for a in acts]
        return evs, ([] if reason == "rate_limited" else list(acts))
    if isinstance(res, UnknownOutcome):
        return [], list(acts)
    by_oid = {a.order_id: a for a in acts}
    seen: set[str] = set()
    out: list[Event] = []
    recon: list[CancelOrder] = []
    for r in res.get("orders") or []:
        oid = str(r.get("order_id") or "")
        a = by_oid.get(oid)
        if a is None or oid in seen:
            continue
        seen.add(oid)
        err = r.get("error")
        if err:
            err = err if isinstance(err, dict) else {"message": str(err)}
            reason = cancel_reject_reason(0, str(err.get("code", "")), str(err.get("message", "")))
            out.append(OrderReject(recv_ns, 0, a.client_order_id, a.ticker, reason, 0, "cancel"))
            recon.append(a)
        else:
            out.append(CancelAck(recv_ns, ms_to_ns(r.get("ts_ms")), str(r.get("client_order_id") or a.client_order_id),
                                 oid, a.ticker, opt_qty(r.get("reduced_by"))))
    recon += [a for a in acts if a.order_id not in seen]
    return out, recon


@dataclass
class _PendingCreate:
    action: PlaceOrder
    first_ns: int
    attempts: int = 0
    next_ns: int = 0


@dataclass
class _PendingOrder:
    coid: str
    oid: str
    ticker: str
    recancel: bool
    reason: str
    attempts: int = 0
    next_ns: int = 0
    cancels: int = 0
    not_found: int = 0
    stuck: bool = False


@dataclass
class _Tombstone:
    action: PlaceOrder
    first_ns: int
    due_ns: list[int]


@dataclass
class VenueStats:
    requests: dict[str, int] = field(default_factory=dict)
    local_rejects: int = 0
    unknown: int = 0
    reconciled: int = 0
    reconciled_missing: int = 0
    revived: int = 0
    stuck_cancels: int = 0
    lookup_rejected_matches: int = 0
    errors: int = 0

    def bump(self, key: str, n: int = 1) -> None:
        self.requests[key] = self.requests.get(key, 0) + n


class KalshiVenue:
    """Executes Kalshi actions asynchronously; results go to ``sink`` as events.

    Args:
        rest: dh.kalshi.rest.KalshiRest (or a test double with the same coroutines).
        sink: receives result events (the runner stamps nothing: events carry recv time).
        cfg: VenueCfg (batching, reconciliation, polls).
        clock_ns: wall clock for receive stamps (int ns).
        log_fn: structured log hook (kind, payload), e.g. JsonLog.
        observe_rtt: hook(op, seconds, outcome) for latency metrics.
    """

    def __init__(
        self,
        rest: Any,
        *,
        sink: Sink,
        cfg: VenueCfg | None = None,
        clock_ns: Callable[[], int] = time.time_ns,
        monotonic: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        log_fn: LogFn | None = None,
        observe_rtt: Callable[[str, float, str], None] | None = None,
    ) -> None:
        self.rest = rest
        self.sink = sink
        self.cfg = cfg or VenueCfg()
        self._clock = clock_ns
        self._mono = monotonic
        self._sleep = sleep
        self.log_fn = log_fn
        self.observe_rtt = observe_rtt
        self.groups: dict[str, str] = {}  # logical order group id -> exchange id
        self.on_group_map: Callable[[str, str], None] | None = None  # (logical, exchange id) recorder hook
        self.group_limits: dict[str, int] = {}
        self.stats = VenueStats()
        self.sub = self.cfg.sub  # explicit subaccount on every request (0 = primary)
        self.last_cancel_all_ns = 0  # last global cancel-all REQUEST (Kalshi may cancel orders placed in the next minute)
        self._oid_by_coid: dict[str, str] = {}
        self._tasks: set[asyncio.Task[Any]] = set()
        self._creates: dict[str, _PendingCreate] = {}
        self._orders: dict[str, _PendingOrder] = {}
        self._tombstones: dict[str, _Tombstone] = {}
        self._recon_wake = asyncio.Event()
        self._reserved_write = 0.0
        self._closed = False
        _hook_limiter(getattr(rest, "limiter", None))

    # ================================================================== helpers
    def _now(self) -> int:
        return self._clock()

    def _log(self, kind: str, /, **payload: Any) -> None:
        if self.log_fn is not None:
            try:
                self.log_fn(kind, payload)
            except Exception:  # noqa: BLE001 - logging must never break order handling
                log.exception("venue log hook failed")

    def _emit(self, events: Iterable[Event]) -> None:
        for ev in events:
            if isinstance(ev, OrderAck) and ev.order_id:
                self._learn(ev.client_order_id, ev.order_id)
            self.sink(ev)

    def _learn(self, coid: str, oid: str) -> None:
        m = self._oid_by_coid
        if coid not in m:
            m[coid] = oid
            while len(m) > MAX_OID_MAP:
                m.pop(next(iter(m)))

    def _spawn(self, coro: Awaitable[Any], name: str) -> asyncio.Task[Any]:
        t = asyncio.ensure_future(coro)
        t.set_name(f"venue:{name}")
        self._tasks.add(t)
        t.add_done_callback(self._task_done)
        return t

    def _task_done(self, t: asyncio.Task[Any]) -> None:
        self._tasks.discard(t)
        if not t.cancelled() and t.exception() is not None:
            self.stats.errors += 1
            log.error("venue task %s failed: %r", t.get_name(), t.exception())

    def _rtt(self, op: str, t0: float, outcome: str) -> None:
        if self.observe_rtt is not None:
            self.observe_rtt(op, self._mono() - t0, outcome)

    def observe(self, ev: Event) -> None:
        """Learn order ids from inbound events (the runner calls this for every event)."""
        if isinstance(ev, (KalshiOrderUpdate, OrderAck)) and ev.order_id and ev.client_order_id:
            self._learn(ev.client_order_id, ev.order_id)

    def oid_of(self, coid: str) -> str:
        return self._oid_by_coid.get(coid, "")

    @property
    def inflight(self) -> int:
        return len(self._tasks)

    async def wait_idle(self, timeout_s: float) -> bool:
        """Wait until every in-flight request task finished (True) or the timeout (False)."""
        deadline = self._mono() + timeout_s
        while self._tasks:
            left = deadline - self._mono()
            if left <= 0:
                return False
            await asyncio.wait(set(self._tasks), timeout=left)
        return True

    async def close(self) -> None:
        """Stop background work (in-flight requests are awaited by the runner first)."""
        self._closed = True
        self._recon_wake.set()
        for t in list(self._tasks):
            t.cancel()
        await asyncio.gather(*list(self._tasks), return_exceptions=True)

    # ================================================================== dispatch
    def submit(self, actions: Sequence[Action], decision_ns: int) -> list[Action]:
        """Dispatch Kalshi actions decided at ``decision_ns``; returns the actions it did
        not handle (hedge orders, Halt/Resume, Log). Never blocks."""
        rest: list[Action] = []
        cancel_alls: list[CancelAll] = []
        cancels: list[CancelOrder] = []
        places: list[PlaceOrder] = []
        others: list[Action] = []
        for a in actions:
            if isinstance(a, CancelAll):
                cancel_alls.append(a)
            elif isinstance(a, CancelOrder):
                cancels.append(a)
            elif isinstance(a, PlaceOrder):
                places.append(a)
            elif isinstance(a, KALSHI_ORDER_ACTIONS):
                others.append(a)
            else:
                rest.append(a)
        # cancels first: they reduce risk and must never queue behind new quotes
        for a in cancel_alls:
            self._spawn(self._cancel_all(a), "cancel_all")
        if cancels:
            self.cancel_orders(cancels)
        for a in others:
            if isinstance(a, (CreateOrderGroup, ResetOrderGroup, UpdateOrderGroupLimit, DeleteOrderGroup)):
                self._spawn(self._group_op(a), type(a).__name__)
            elif isinstance(a, AmendOrder):
                self._spawn(self._amend(a), "amend")
            elif isinstance(a, DecreaseOrder):
                self._spawn(self._decrease(a), "decrease")
        if places:
            for chunk in self._chunks(places, "POST", BATCH_CREATE_PATH):
                path = CREATE_PATH if len(chunk) == 1 else BATCH_CREATE_PATH
                reason = self._budget_check("POST", path, len(chunk))
                if reason:
                    now = self._now()
                    self.stats.local_rejects += len(chunk)
                    self._emit([OrderReject(now, 0, a.client_order_id, a.ticker, reason, 0, "create") for a in chunk])
                    continue
                # reserve now, so the next chunk's budget check sees this one
                self._spawn(self._places(chunk, decision_ns, self._reserve("POST", path, len(chunk))), "place")
        return rest

    def reserved_write_tokens(self) -> float:
        return self._reserved_write

    def _chunks(self, acts: Sequence[Any], method: str, batch_path: str) -> list[list[Any]]:
        n = max(1, int(self.cfg.max_batch))
        lim = getattr(self.rest, "limiter", None)
        if lim is not None:
            try:
                n = min(n, lim.max_batch_items(method, batch_path))
            except Exception:  # noqa: BLE001
                pass
        return [list(acts[i:i + n]) for i in range(0, len(acts), n)]

    def _budget_check(self, method: str, path: str, n: int) -> str:
        """'' if a place request would get its write tokens within max_place_wait_s."""
        lim = getattr(self.rest, "limiter", None)
        if lim is None:
            return ""
        try:
            cost = float(lim.cost_for(method, path, n))
            bucket = lim.bucket_for(method)
            deficit = self._reserved_write + cost - bucket.tokens
            wait = max(0.0, deficit) / bucket.limit.refill_rate
        except Exception:  # noqa: BLE001 - the limiter will enforce it anyway
            return ""
        if wait > self.cfg.max_place_wait_s:
            return "rate_budget"
        return ""

    def _reserve(self, method: str, path: str, n: int) -> _Reservation:
        lim = getattr(self.rest, "limiter", None)
        cost = 0.0
        if lim is not None:
            try:
                cost = float(lim.cost_for(method, path, n))
            except Exception:  # noqa: BLE001
                cost = 0.0
        self._reserved_write += cost
        return _Reservation(self, cost)

    def _reserve_cancels(self, n: int) -> _Reservation:
        if n == 1:
            return self._reserve("DELETE", "/portfolio/events/orders/{order_id}", 1)
        return self._reserve("DELETE", "/portfolio/events/orders/batched", n)

    # ================================================================== places
    def place_body(self, a: PlaceOrder) -> dict[str, Any]:
        """CreateOrderV2Request for ``a`` with the exchange order-group id (ValueError if the
        logical group has no exchange id: never send a quote outside its group)."""
        body = place_order_body(
            a,
            self_trade_prevention=self.cfg.self_trade_prevention,
            time_in_force=TIF.get(a.time_in_force, a.time_in_force),
            subaccount=self.sub,
        )
        if a.order_group_id:
            gid = self.groups.get(a.order_group_id)
            if not gid:
                raise ValueError(f"order group {a.order_group_id!r} has no exchange id (not created)")
            body["order_group_id"] = gid
        return body

    async def _places(self, acts: list[PlaceOrder], decision_ns: int, reserved: _Reservation | None = None) -> None:
        tok = _RESERVATION.set(reserved)
        try:
            await self._places_inner(acts, decision_ns)
        finally:
            if reserved is not None:
                reserved.release()
            _RESERVATION.reset(tok)

    async def _places_inner(self, acts: list[PlaceOrder], decision_ns: int) -> None:
        now = self._now()
        ok: list[PlaceOrder] = []
        bodies: list[dict[str, Any]] = []
        bad: list[Event] = []
        for a in acts:
            try:
                bodies.append(self.place_body(a))
                ok.append(a)
            except ValueError as exc:
                bad.append(OrderReject(now, 0, a.client_order_id, a.ticker, f"invalid_order: {exc}"[:200], 0, "create"))
        if bad:
            self.stats.local_rejects += len(bad)
            self._emit(bad)
        if not ok:
            return
        single = len(ok) == 1
        path = CREATE_PATH if single else BATCH_CREATE_PATH
        self.stats.bump("create" if single else "batch_create")
        t0 = self._mono()
        res: Any
        try:
            if single:
                res = await self.rest.create_order(bodies[0])
            else:
                res = await self.rest.batch_create_orders(bodies)
        except KalshiHTTPError as exc:
            res = exc
        except NotSentError as exc:
            self._rtt("create", t0, "not_sent")
            recv = self._now()
            self._emit([OrderReject(recv, 0, a.client_order_id, a.ticker, "not_sent", 0, "create") for a in ok])
            self._log("order_not_sent", coids=[a.client_order_id for a in ok], error=str(exc)[:200])
            return
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - unknown whether it reached Kalshi
            res = UnknownOutcome("POST", path, bodies, f"{type(exc).__name__}: {exc}"[:200])
        recv = self._now()
        outcome = "unknown" if isinstance(res, UnknownOutcome) else ("error" if isinstance(res, KalshiHTTPError) else "ok")
        self._rtt("create", t0, outcome)
        if single and isinstance(res, UnknownOutcome) and res.status == 409:
            # Conflict on a create = the client_order_id is taken (creates are never retried,
            # so it is not ours): a DEFINITE reject, never 30 s of phantom exposure
            self.stats.bump("create_409")
            self._log("create_conflict", coid=ok[0].client_order_id, body=str(res.body)[:200])
            self._emit([OrderReject(recv, 0, ok[0].client_order_id, ok[0].ticker, DUPLICATE_ID_REASON, 409, "create")])
            return
        evs = create_result_to_events(res, ok[0], recv) if single else batch_create_results_to_events(res, ok, recv)
        answered = {e.client_order_id for e in evs}
        self._emit(evs)
        unknown = [a for a in ok if a.client_order_id not in answered]
        if unknown:
            self.stats.unknown += len(unknown)
            reason = res.reason if isinstance(res, UnknownOutcome) else "missing from batch response"
            self._log("unknown_outcome", request="create", coids=[a.client_order_id for a in unknown], reason=reason)
            for a in unknown:
                self._creates[a.client_order_id] = _PendingCreate(a, now, 0, recv + self._backoff_ns(0))
            self._recon_wake.set()

    # ================================================================== cancels
    async def _cancels(self, acts: list[CancelOrder], reserved: _Reservation | None = None) -> None:
        tok = _RESERVATION.set(reserved)
        try:
            await self._cancels_inner(acts)
        finally:
            if reserved is not None:
                reserved.release()
            _RESERVATION.reset(tok)

    async def _cancels_inner(self, acts: list[CancelOrder]) -> None:
        now = self._now()
        ready: list[CancelOrder] = []
        early: list[Event] = []
        for a in acts:
            oid = a.order_id or self._oid_by_coid.get(a.client_order_id, "")
            if not oid:  # the cancel overtook the create ack: the OrderManager re-sends it later
                early.append(OrderReject(now, 0, a.client_order_id, a.ticker, "not_found", 0, "cancel"))
            else:
                ready.append(a if a.order_id == oid else replace(a, order_id=oid))
        if early:
            self._emit(early)
        if not ready:
            return
        if len(ready) == 1:
            await self._cancel_one(ready[0])
            return
        self.stats.bump("batch_cancel")
        t0 = self._mono()
        body = [self._cancel_item(a) for a in ready]
        try:
            res: Any = await self.rest.batch_cancel_orders(body)
        except KalshiHTTPError as exc:
            res = exc
        except NotSentError:
            recv = self._now()
            self._emit([OrderReject(recv, 0, a.client_order_id, a.ticker, "not_sent", 0, "cancel") for a in ready])
            return
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            res = UnknownOutcome("DELETE", "/portfolio/events/orders/batched", body, f"{type(exc).__name__}: {exc}"[:200])
        recv = self._now()
        self._rtt("cancel", t0, "unknown" if isinstance(res, UnknownOutcome) else "ok")
        evs, recon = batch_cancel_results_to_events(res, ready, recv)
        self._emit(evs)
        for a in recon:
            self.check_order(a.client_order_id, a.order_id, a.ticker, recancel=True, reason="batch_cancel")

    def _cancel_item(self, a: CancelOrder) -> dict[str, Any]:
        return {"order_id": a.order_id, "market_ticker": a.ticker, "subaccount": self.sub}

    async def _cancel_one(self, a: CancelOrder) -> None:
        self.stats.bump("cancel")
        t0 = self._mono()
        try:
            res: Any = await self.rest.cancel_order(a.order_id, market_ticker=a.ticker, subaccount=self.sub)
        except KalshiHTTPError as exc:
            res = exc
        except NotSentError:
            self._emit([OrderReject(self._now(), 0, a.client_order_id, a.ticker, "not_sent", 0, "cancel")])
            return
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            res = UnknownOutcome("DELETE", f"/portfolio/events/orders/{a.order_id}", None, f"{type(exc).__name__}: {exc}"[:200])
        recv = self._now()
        if isinstance(res, KalshiHTTPError):
            self._rtt("cancel", t0, "error")
            reason = canonical_reason(res)
            self._emit([OrderReject(recv, 0, a.client_order_id, a.ticker, reason, res.status, "cancel")])
            if reason != "rate_limited":
                self.check_order(a.client_order_id, a.order_id, a.ticker, recancel=False, reason=reason)
            return
        if isinstance(res, UnknownOutcome):
            self._rtt("cancel", t0, "unknown")
            self.stats.unknown += 1
            self._log("unknown_outcome", request="cancel", coid=a.client_order_id, oid=a.order_id, reason=res.reason)
            self.check_order(a.client_order_id, a.order_id, a.ticker, recancel=True, reason="cancel_unknown")
            return
        self._rtt("cancel", t0, "ok")
        self._emit(cancel_result_to_events(res, a, recv))

    # ================================================================== cancel all
    async def cancel_all_now(self, reason: str, *, attempts: int = 5) -> bool:
        """DELETE /portfolio/events/orders?subaccount=<ours>, retried on unknown outcomes /
        throttling (cancel-all is idempotent). True on a 2xx. Kalshi may also cancel orders
        placed during the minute after the request (``last_cancel_all_ns``: the runner holds
        new orders for ``cancel_all_hold_s``)."""
        for i in range(max(1, attempts)):
            self.stats.bump("cancel_all")
            t0 = self._mono()
            self.last_cancel_all_ns = self._now()
            try:
                res = await self.rest.cancel_all_orders(subaccount=self.sub)
            except KalshiHTTPError as exc:
                self._rtt("cancel_all", t0, "error")
                self._log("cancel_all_error", reason=reason, status=exc.status, error=str(exc)[:200])
                if exc.status != 429:
                    return False
            except (NotSentError, KalshiError) as exc:
                self._rtt("cancel_all", t0, "not_sent")
                self._log("cancel_all_error", reason=reason, error=str(exc)[:200])
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                self._log("cancel_all_error", reason=reason, error=f"{type(exc).__name__}: {exc}"[:200])
            else:
                if isinstance(res, UnknownOutcome):
                    self._rtt("cancel_all", t0, "unknown")
                    self._log("cancel_all_unknown", reason=reason, detail=res.reason)
                else:
                    self._rtt("cancel_all", t0, "ok")
                    self._log("cancel_all", reason=reason, attempt=i + 1)
                    return True
            await self._sleep(self._backoff_ns(i) / NS_PER_S)
        return False

    async def _cancel_all(self, a: CancelAll) -> None:
        if not a.tickers:
            await self.cancel_all_now(a.reason)
            return
        await self.sweep_resting(a.tickers, a.reason)

    async def cancel_all_verified(self, reason: str, *, rounds: int = 3, wait_s: float = 2.0) -> list[dict[str, Any]]:
        """Cancel-all, then confirm with the resting-order list; whatever still rests is
        cancelled individually (the read API can lag the cancel a moment). Returns the orders
        still resting after ``rounds`` checks ([] = verified clean). Raises if the cancel-all
        itself failed."""
        if not await self.cancel_all_now(reason):
            raise RuntimeError(f"cancel-all failed ({reason})")
        left: list[dict[str, Any]] = []
        for attempt in range(max(1, rounds)):
            left = await self.resting_orders()
            if not left:
                return []
            self._log("cancel_all_leftovers", reason=reason, n=len(left), attempt=attempt + 1)
            self.cancel_orders([CancelOrder(str(o.get("client_order_id") or ""), str(o.get("ticker") or ""),
                                            str(o["order_id"]), reason=f"verify:{reason}") for o in left if o.get("order_id")])
            await self.wait_idle(wait_s)
            await self._sleep(0.5 * (attempt + 1))
        return await self.resting_orders()

    async def resting_orders(self, *, ticker: str | None = None) -> list[dict[str, Any]]:
        """GET /portfolio/orders?status=resting&subaccount=<ours> (every page)."""
        kw: dict[str, Any] = {"status": "resting"}
        if ticker:
            kw["ticker"] = ticker
        kw["subaccount"] = self.sub
        return [o async for o in self.rest.iter_orders(**kw)]

    async def sweep_resting(self, tickers: Iterable[str] | None, reason: str, skip_oids: Iterable[str] = ()) -> int:
        """Cancel every order still resting in ``tickers`` (None = all markets; REST view),
        except ``skip_oids`` (cancels already on their way); returns the count."""
        found: list[CancelOrder] = []
        skip = set(skip_oids)
        groups: list[str | None] = [None] if tickers is None else sorted(set(tickers))
        for t in groups:
            try:
                rows = await self.resting_orders(ticker=t)
            except Exception as exc:  # noqa: BLE001
                self._log("sweep_error", ticker=t, error=f"{type(exc).__name__}: {exc}"[:200])
                continue
            for o in rows:
                oid = str(o.get("order_id") or "")
                if oid and oid not in skip:
                    skip.add(oid)
                    found.append(CancelOrder(str(o.get("client_order_id") or ""), str(o.get("ticker") or t or ""),
                                             oid, reason=f"sweep:{reason}"))
        if found:
            self._log("sweep", reason=reason, n=len(found), oids=[a.order_id for a in found])
            self.cancel_orders(found)
        return len(found)

    def cancel_orders(self, acts: Sequence[CancelOrder]) -> None:
        """Dispatch cancels (batched, each chunk reserving its write tokens)."""
        for chunk in self._chunks(list(acts), "DELETE", BATCH_CREATE_PATH):
            self._spawn(self._cancels(chunk, self._reserve_cancels(len(chunk))), "cancel")

    # ================================================================== amend / decrease
    async def _amend(self, a: AmendOrder) -> None:
        self.stats.bump("amend")
        try:
            res: Any = await self.rest.amend_order(a.order_id, amend_order_body(a), subaccount=self.sub)
        except KalshiHTTPError as exc:
            res = exc
        except NotSentError:
            self._emit([OrderReject(self._now(), 0, a.client_order_id, a.ticker, "not_sent", 0, "amend")])
            return
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            res = UnknownOutcome("POST", "amend", None, f"{type(exc).__name__}: {exc}"[:200])
        self._emit(amend_result_to_events(res, a, self._now()))
        if isinstance(res, UnknownOutcome):
            self.check_order(a.client_order_id, a.order_id, a.ticker, recancel=False, reason="amend_unknown")

    async def _decrease(self, a: DecreaseOrder) -> None:
        self.stats.bump("decrease")
        try:
            res: Any = await self.rest.decrease_order(a.order_id, subaccount=self.sub, **decrease_order_kwargs(a))
        except KalshiHTTPError as exc:
            res = exc
        except NotSentError:
            self._emit([OrderReject(self._now(), 0, a.client_order_id, a.ticker, "not_sent", 0, "decrease")])
            return
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            res = UnknownOutcome("POST", "decrease", None, f"{type(exc).__name__}: {exc}"[:200])
        self._emit(decrease_result_to_events(res, a, self._now()))
        if isinstance(res, UnknownOutcome):
            self.check_order(a.client_order_id, a.order_id, a.ticker, recancel=False, reason="decrease_unknown")

    # ================================================================== order groups
    async def ensure_order_group(self, logical_id: str, contracts_limit: int) -> str | None:
        """Create the exchange order group for ``logical_id`` (idempotent per session).
        Returns the exchange id, or None when it could not be established."""
        if logical_id in self.groups:
            if self.group_limits.get(logical_id) != contracts_limit:
                await self._retry_group("limit", logical_id, contracts_limit)
            return self.groups[logical_id]
        before: set[str] | None = None
        try:
            before = {str(g.get("id")) for g in (await self.rest.get_order_groups(subaccount=self.sub)).get("order_groups") or []}
        except Exception as exc:  # noqa: BLE001 - only needed to resolve an unknown outcome
            self._log("order_group_list_error", error=f"{type(exc).__name__}: {exc}"[:200])
        for attempt in range(3):
            self.stats.bump("create_order_group")
            try:
                res = await self.rest.create_order_group(contracts_limit, subaccount=self.sub)
            except KalshiHTTPError as exc:
                self._log("order_group_error", op="create", status=exc.status, error=str(exc)[:200])
                if exc.status != 429:
                    return None
                res = None
            except NotSentError as exc:
                self._log("order_group_error", op="create", error=str(exc)[:200])
                res = None
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                res = UnknownOutcome("POST", "/portfolio/order_groups/create", None, f"{type(exc).__name__}: {exc}")
            if isinstance(res, dict) and res.get("order_group_id"):
                gid = str(res["order_group_id"])
                self._map_group(logical_id, gid, contracts_limit, "create")
                return gid
            if isinstance(res, UnknownOutcome) or (isinstance(res, dict) and not res.get("order_group_id")):
                # did it get created? adopt a group that was not there before with our limit
                try:
                    groups = (await self.rest.get_order_groups(subaccount=self.sub)).get("order_groups") or []
                except Exception:  # noqa: BLE001
                    groups = []
                if before is not None:
                    fresh = [g for g in groups if str(g.get("id")) not in before
                             and _fp_qty(g.get("contracts_limit_fp")) in (contracts_limit, None)]
                    if len(fresh) == 1:
                        gid = str(fresh[0]["id"])
                        self._map_group(logical_id, gid, contracts_limit, "adopt")
                        return gid
            await self._sleep(self._backoff_ns(attempt) / NS_PER_S)
        return None

    def _map_group(self, logical_id: str, gid: str, limit: int, op: str) -> None:
        self.groups[logical_id] = gid
        self.group_limits[logical_id] = limit
        self._log("order_group", op=op, logical=logical_id, id=gid, limit=limit)
        if self.on_group_map is not None:
            self.on_group_map(logical_id, gid)

    async def _retry_group(self, op: str, logical_id: str, limit: int = 0, attempts: int = 5) -> bool:
        gid = self.groups.get(logical_id)
        if not gid:
            self._log("order_group_error", op=op, logical=logical_id, error="no exchange id")
            return False
        for i in range(attempts):
            self.stats.bump(f"order_group_{op}")
            try:
                if op == "reset":
                    res: Any = await self.rest.reset_order_group(gid, subaccount=self.sub)
                elif op == "limit":
                    res = await self.rest.update_order_group_limit(gid, limit, subaccount=self.sub)
                else:
                    res = await self.rest.delete_order_group(gid, subaccount=self.sub)
            except KalshiHTTPError as exc:
                self._log("order_group_error", op=op, id=gid, status=exc.status, error=str(exc)[:200])
                if exc.status != 429:
                    return False
                res = None
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - NotSent / transport: retry (idempotent)
                self._log("order_group_error", op=op, id=gid, error=f"{type(exc).__name__}: {exc}"[:200])
                res = None
            if isinstance(res, dict):
                if op == "limit":
                    self.group_limits[logical_id] = limit
                if op == "delete":
                    self.groups.pop(logical_id, None)
                self._log("order_group", op=op, logical=logical_id, id=gid, limit=limit or None)
                return True
            await self._sleep(self._backoff_ns(i) / NS_PER_S)
        return False

    async def _group_op(self, a: Action) -> None:
        if isinstance(a, CreateOrderGroup):
            await self.ensure_order_group(a.order_group_id, a.contracts_limit)
        elif isinstance(a, ResetOrderGroup):
            await self._retry_group("reset", a.order_group_id)
        elif isinstance(a, UpdateOrderGroupLimit):
            await self._retry_group("limit", a.order_group_id, a.contracts_limit)
        elif isinstance(a, DeleteOrderGroup):
            await self._retry_group("delete", a.order_group_id)

    def logical_group_of(self, exchange_id: str) -> str:
        """Logical id for an exchange order-group id ('' if it is not ours)."""
        for k, v in self.groups.items():
            if v == exchange_id:
                return k
        return ""

    # ================================================================== reconciliation
    def _backoff_ns(self, attempt: int) -> int:
        b = self.cfg.reconcile_backoff_s or (1.0,)
        return int(b[min(attempt, len(b) - 1)] * NS_PER_S)

    def _recancel_backoff_ns(self, attempt: int) -> int:
        return min(self._backoff_ns(attempt), int(self.cfg.recancel_backoff_max_s * NS_PER_S))

    def check_order(self, coid: str, oid: str, ticker: str, *, recancel: bool, reason: str) -> None:
        """Look the order up (GET /portfolio/orders/{id}) and feed its state back; with
        ``recancel`` keep cancelling it for as long as it is still resting."""
        p = self._orders.get(oid)
        if p is None:
            self._orders[oid] = _PendingOrder(coid, oid, ticker, recancel, reason, 0, self._now() + self._backoff_ns(0))
        else:
            p.recancel = p.recancel or recancel
        self._recon_wake.set()

    def has_pending_creates(self, tickers: Iterable[str] | None = None) -> bool:
        """True if a create with unknown outcome is being reconciled (in ``tickers``, or any)."""
        if tickers is None:
            return bool(self._creates) or bool(self._tombstones)
        ts = set(tickers)
        return any(p.action.ticker in ts for p in self._creates.values()) or any(
            t.action.ticker in ts for t in self._tombstones.values())

    @property
    def pending_reconciliations(self) -> int:
        return len(self._creates) + len(self._orders)

    @property
    def stuck_orders(self) -> list[str]:
        """Order ids still resting although every cancel failed (alarm)."""
        return sorted(q.oid for q in self._orders.values() if q.stuck)

    async def run_reconciler(self, tick_s: float = 0.25) -> None:
        """Background task: resolve unknown outcomes (see module docstring)."""
        while not self._closed:
            await self.reconcile_due()
            nxt = ([p.next_ns for p in self._creates.values()] + [p.next_ns for p in self._orders.values()]
                   + [t.due_ns[0] for t in self._tombstones.values() if t.due_ns])
            wait = tick_s if not nxt else max(0.0, min(tick_s * 8, (min(nxt) - self._now()) / NS_PER_S))
            self._recon_wake.clear()
            try:
                await asyncio.wait_for(self._recon_wake.wait(), timeout=max(wait, 0.01))
            except TimeoutError:
                pass

    async def reconcile_due(self) -> None:
        """One reconciliation pass over the entries that are due now."""
        now = self._now()
        for p in list(self._creates.values()):
            if p.next_ns <= now:
                await self._reconcile_create(p)
        for q in list(self._orders.values()):
            if q.next_ns <= now:
                await self._reconcile_order(q)
        for t in list(self._tombstones.values()):
            if t.due_ns and t.due_ns[0] <= now:
                await self._recheck_missing(t)

    async def find_created(self, a: PlaceOrder, request_ns: int) -> dict[str, Any] | None:
        """Our order for ``a``: GET /portfolio/orders?ticker&min_ts&subaccount, matched on
        client_order_id AND created at or after ``request_ns - create_match_skew_s`` (an older
        order with the same id, e.g. of an earlier session, is never adopted)."""
        not_before = request_ns - int(self.cfg.create_match_skew_s * NS_PER_S)
        async for o in self.rest.iter_orders(ticker=a.ticker, min_ts=request_ns // NS_PER_S - 60, subaccount=self.sub):
            if o.get("client_order_id") != a.client_order_id:
                continue
            created = _created_ns(o)
            if created and created >= not_before:
                return o
            self.stats.lookup_rejected_matches += 1
            self._log("lookup_match_rejected", coid=a.client_order_id, oid=str(o.get("order_id") or ""),
                      created_ns=created, not_before_ns=not_before)
        return None

    async def _reconcile_create(self, p: _PendingCreate) -> None:
        a = p.action
        p.attempts += 1
        self.stats.bump("reconcile_create")
        try:
            o = await self.find_created(a, p.first_ns)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - try again later
            self._log("reconcile_error", coid=a.client_order_id, error=f"{type(exc).__name__}: {exc}"[:200])
            p.next_ns = self._now() + self._backoff_ns(p.attempts)
            return
        recv = self._now()
        if o is not None:
            self._creates.pop(a.client_order_id, None)
            self.stats.reconciled += 1
            try:
                ev = order_to_update(o, recv)
            except (KeyError, ValueError, TypeError) as exc:
                self._log("reconcile_error", coid=a.client_order_id, error=f"unparseable order: {exc}")
                return
            self._log("reconciled", request="create", coid=a.client_order_id, oid=ev.order_id, status=ev.status,
                      attempts=p.attempts)
            self._emit([self._ours(ev)])
            return
        if recv - p.first_ns >= int(self.cfg.reconcile_missing_after_s * NS_PER_S) and p.attempts >= self.cfg.reconcile_min_attempts:
            self._creates.pop(a.client_order_id, None)
            self.stats.reconciled_missing += 1
            self._log("reconciled", request="create", coid=a.client_order_id, status="missing", attempts=p.attempts)
            self._emit([OrderReject(recv, 0, a.client_order_id, a.ticker, "reconciled_missing", 0, "create")])
            due = [recv + int(s * NS_PER_S) for s in sorted(self.cfg.missing_recheck_s)]
            if due:  # it may still turn up (a slow shard): look again, cancel it if it rests
                self._tombstones[a.client_order_id] = _Tombstone(a, p.first_ns, due)
            return
        p.next_ns = recv + self._backoff_ns(p.attempts)

    async def _recheck_missing(self, t: _Tombstone) -> None:
        a = t.action
        t.due_ns.pop(0)
        try:
            o = await self.find_created(a, t.first_ns)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            self._log("reconcile_error", coid=a.client_order_id, error=f"recheck: {type(exc).__name__}: {exc}"[:200])
            o = None
        if o is None:
            if not t.due_ns:
                self._tombstones.pop(a.client_order_id, None)
            return
        self._tombstones.pop(a.client_order_id, None)
        try:
            ev = order_to_update(o, self._now())
        except (KeyError, ValueError, TypeError):
            return
        self.stats.revived += 1
        log.error("order %s declared missing turned up (%s): feeding it back%s", a.client_order_id, ev.status,
                  " and cancelling it" if ev.status == "resting" else "")
        self._log("revived", coid=a.client_order_id, oid=ev.order_id, status=ev.status)
        self._emit([self._ours(ev)])
        if ev.status == "resting" and ev.order_id:
            self.check_order(ev.client_order_id or a.client_order_id, ev.order_id, a.ticker, recancel=True, reason="revived")
            self.cancel_orders([CancelOrder(ev.client_order_id or a.client_order_id, a.ticker, ev.order_id, reason="revived")])

    def _ours(self, ev: KalshiOrderUpdate) -> KalshiOrderUpdate:
        """Venue lookups only ever return our own subaccount's orders: stamp it (the REST
        Order omits subaccount_number for the primary account)."""
        return ev if getattr(ev, "subaccount", self.sub) == self.sub else replace(ev, subaccount=self.sub)

    async def _gone(self, q: _PendingOrder) -> bool | None:
        """GET order said 404: True only if the resting-order list confirms it is not resting,
        False if it is listed as resting, None if the list could not be read."""
        try:
            rows = await self.resting_orders(ticker=q.ticker or None)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            self._log("reconcile_error", oid=q.oid, coid=q.coid, error=f"404 check: {type(exc).__name__}: {exc}"[:200])
            return None
        return not any(str(o.get("order_id") or "") == q.oid for o in rows)

    def _recancel(self, q: _PendingOrder, recv: int, coid: str) -> None:
        q.cancels += 1
        q.next_ns = recv + self._recancel_backoff_ns(q.attempts)
        if q.cancels > self.cfg.max_cancel_retries and not q.stuck:
            q.stuck = True
            self.stats.stuck_cancels += 1
            log.error("CANCEL STUCK: order %s (%s) still resting after %d cancels: still retrying", q.oid, q.ticker, q.cancels - 1)
            self._log("cancel_stuck", oid=q.oid, coid=coid, ticker=q.ticker, cancels=q.cancels - 1)
        self._spawn(self._cancel_one(CancelOrder(coid, q.ticker, q.oid, reason="recancel")), "recancel")

    async def _reconcile_order(self, q: _PendingOrder) -> None:
        q.attempts += 1
        self.stats.bump("reconcile_order")
        try:
            body = await self.rest.get_order(q.oid)
        except KalshiHTTPError as exc:
            if exc.status == 404:
                q.not_found += 1
                gone = await self._gone(q)
                recv = self._now()
                if gone:
                    self._orders.pop(q.oid, None)
                    self._log("reconciled", request=q.reason, oid=q.oid, coid=q.coid, status="gone",
                              note="GET 404 and not in the resting list")
                    return
                if gone is False and q.recancel:  # listed as resting: keep cancelling it
                    self._recancel(q, recv, q.coid)
                    return
                q.next_ns = recv + self._backoff_ns(q.attempts)
                return
            q.next_ns = self._now() + self._backoff_ns(q.attempts)
            return
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            self._log("reconcile_error", oid=q.oid, coid=q.coid, error=f"{type(exc).__name__}: {exc}"[:200])
            q.next_ns = self._now() + self._backoff_ns(q.attempts)
            return
        recv = self._now()
        o = body.get("order") if isinstance(body, dict) else None
        if not isinstance(o, dict):
            q.next_ns = recv + self._backoff_ns(q.attempts)
            return
        try:
            ev = order_to_update(o, recv)
        except (KeyError, ValueError, TypeError) as exc:
            q.next_ns = recv + self._backoff_ns(q.attempts)
            self._log("reconcile_error", oid=q.oid, error=f"unparseable order: {exc}")
            return
        self.stats.reconciled += 1
        self._emit([self._ours(ev)])
        self._log("reconciled", request=q.reason, oid=q.oid, coid=ev.client_order_id, status=ev.status, attempts=q.attempts)
        if ev.status == "resting" and q.recancel:
            self._recancel(q, recv, ev.client_order_id or q.coid)  # never dropped while it rests
            return
        self._orders.pop(q.oid, None)

    # ================================================================== periodic reads
    def read_budget_ok(self, cost_paths: Sequence[str] = ("/portfolio/orders",)) -> bool:
        lim = getattr(self.rest, "limiter", None)
        if lim is None:
            return True
        try:
            need = sum(lim.cost_for("GET", p) for p in cost_paths)
            return lim.read.tokens >= need + self.cfg.read_reserve_tokens
        except Exception:  # noqa: BLE001
            return True

    async def fetch_queue_positions(self, tickers: Sequence[str]) -> list[tuple[str, str, int]]:
        """[(order_id, market_ticker, queue_qty in 0.01 contracts)] for our resting orders."""
        out: list[tuple[str, str, int]] = []
        n = max(1, int(self.cfg.queue_positions_max_tickers))
        tl = sorted(set(tickers))
        for i in range(0, len(tl), n):
            body = await self.rest.get_queue_positions(market_tickers=tl[i:i + n], subaccount=self.sub)
            for r in body.get("queue_positions") or []:
                try:
                    out.append((str(r["order_id"]), str(r.get("market_ticker") or ""), qty_from_fp(str(r["queue_position_fp"]))))
                except (KeyError, ValueError):
                    continue
        return out

    async def fetch_fills(self, min_ts_s: int) -> list[dict[str, Any]]:
        """GET /portfolio/fills since ``min_ts_s`` (Unix seconds), every page."""
        return [f async for f in self.rest.iter_fills(min_ts=int(min_ts_s), subaccount=self.sub)]

    async def fetch_positions(self, *, strict: bool = False) -> dict[str, int]:
        """{ticker: signed YES qty} of every non-zero market position (GET /portfolio/positions).
        A malformed row raises ValueError when ``strict`` (start-up: the inventory must be
        known), else it is skipped and counted (the positions check then flags the market)."""
        from dh.kalshi.normalize import market_position

        body = await self.rest.get_all_positions(count_filter="position", subaccount=self.sub)
        out: dict[str, int] = {}
        for m in body.get("market_positions") or []:
            try:
                mp = market_position(m)
            except (KeyError, ValueError, TypeError) as exc:
                if strict:
                    raise ValueError(f"position row {str(m)[:120]}: {type(exc).__name__}: {exc}") from exc
                self._log("position_row_malformed", row=str(m)[:200], error=f"{type(exc).__name__}: {exc}"[:200])
                continue
            if mp.ticker:
                out[mp.ticker] = mp.position
            elif strict:
                raise ValueError(f"position row without a ticker: {str(m)[:120]}")
        return out


def _fp_qty(v: Any) -> int | None:
    if v in (None, ""):
        return None
    try:
        return qty_from_fp(str(v))
    except ValueError:
        return None
