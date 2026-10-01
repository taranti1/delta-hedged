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
  CancelAll()              DELETE /portfolio/events/orders?subaccount=<n> (every resting order of
                           the subaccount on every shard; retried: idempotent)
  CancelAll(tickers)       the strategy already sends one CancelOrder per known order; the venue
                           additionally sweeps GET /portfolio/orders?status=resting&ticker=...
                           and cancels whatever is still resting there
  AmendOrder/DecreaseOrder amend_order / decrease_order (not used by the M1 strategy)
  Create/Reset/UpdateLimit/DeleteOrderGroup
                           order-group endpoints. The exchange assigns group ids: the venue maps
                           the strategy's logical id (MarketMaker.ORDER_GROUP_ID) to one group
                           PER EXCHANGE SHARD in use (groups do not work across shards).
                           Reset/limit updates are retried (idempotent).
  latch_kill(reason)       the kill switch's first, fastest scoped step: PUT
                           /portfolio/order_groups/{id}/trigger on every group (cancels the
                           group's orders, rejects new ones until a reset; no documented trailing
                           tail); from then on no group is created or reset in this session.

Exchange shards (Kalshi sharding, 2026-08; every KXBTC* market is on shard 2): every order
write names its shard explicitly (``exchange_index``): creates, amends and decreases in the
body, cancels as a query parameter or per batch item, order-group writes as a query parameter
(the group is created on its shard). The shard of a market comes from its MarketSpec
(``register_markets``); a market whose shard is unknown, or not in ``venue.exchange_indexes``,
is never placed (local reject). A cancel of an order whose shard is unknown uses -1 (documented:
"require auto-routing by market ticker") with its ticker: a cancel is never withheld.

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
omitted subaccount as ALL subaccounts on GET orders / GET fills / cancel-all. On a shared
account (``venue.shared_account``) the venue refuses to exist for subaccount 0.

Write budget: a place is dispatched only if its write tokens would be available within
``max_place_wait_s``. Tokens of requests dispatched but not yet through the rate limiter are
reserved; the reservation is released the moment the limiter hands the request its tokens
(the limiter's ``acquire`` is wrapped once, per limiter instance), so in-flight requests are
never counted twice.

Periodic reads (tasks started by the runner, each skipped when the read bucket is low):
  queue positions   GET /portfolio/orders/queue_positions every ``queue_positions_interval_s``
  positions         GET /portfolio/positions every ``positions_interval_s`` (+ resting orders
                    for the ghost-order sweep); the runner compares them with the strategy.
                    A confirming read is preceded by GET /exchange/user_data_timestamp (the
                    time up to which Kalshi's portfolio reads are validated).
  balance           GET /portfolio/balance?subaccount=<n>&exchange_index=<shard> per shard.
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
from dh.live.config import VenueCfg, subaccount_problems

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
        # logical order group id -> {exchange shard: exchange group id} (one group per shard in use)
        self.groups: dict[str, dict[int, str]] = {}
        self.on_group_map: Callable[[str, str], None] | None = None  # (logical, exchange id) recorder hook
        self.group_limits: dict[str, int] = {}
        self.stats = VenueStats()
        problems = subaccount_problems(self.cfg)  # explicit subaccount AND shared flag; 0 only when allowed
        if problems:
            raise ValueError("; ".join(problems))
        self.sub = self.cfg.sub  # explicit subaccount on every request
        self.shard_of: dict[str, int] = {}  # ticker -> exchange shard (MarketSpec.exchange_index)
        self.kill_latched = ""  # kill switch reason: groups triggered, no group created or reset again
        self._oid_shard: dict[str, int] = {}  # order id -> shard, from REST Order rows (bounded)
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

    # ================================================================== exchange shards
    def register_markets(self, specs: Iterable[Any]) -> list[int]:
        """Learn each market's exchange shard (MarketSpec.exchange_index; None = unknown: the
        market is never placed). Returns the shards in use afterwards."""
        for s in specs:
            sh = getattr(s, "exchange_index", None)
            if sh is None or isinstance(sh, bool):
                continue
            self.shard_of.setdefault(str(s.ticker), int(sh))
        return self.shards_in_use()

    def shards_in_use(self) -> list[int]:
        """Shards of the registered markets this runner may trade (venue.exchange_indexes)."""
        allowed = set(self.cfg.exchange_indexes)
        return sorted({sh for sh in self.shard_of.values() if not allowed or sh in allowed})

    def trade_shard(self, ticker: str) -> int:
        """The shard a NEW order (place / amend) in ``ticker`` is sent to; ValueError when it is
        unknown or not configured (never trade a market whose shard is unknown)."""
        sh = self.shard_of.get(ticker)
        if sh is None:
            raise ValueError(f"exchange shard of {ticker} unknown: not trading it")
        if self.cfg.exchange_indexes and sh not in self.cfg.exchange_indexes:
            raise ValueError(f"{ticker} is on exchange shard {sh}, not in venue.exchange_indexes "
                             f"{list(self.cfg.exchange_indexes)}")
        return sh

    def cancel_shard(self, ticker: str, oid: str = "") -> int:
        """Shard of a cancel / decrease: the order's own (REST row), else its market's, else -1
        ("require auto-routing by market ticker"): a cancel is never withheld for want of a shard."""
        sh = self._oid_shard.get(oid) if oid else None
        if sh is None:
            sh = self.shard_of.get(ticker)
        if sh is None:
            self.stats.bump("cancel_autoroute")
            return -1
        return sh

    def _learn_rows(self, rows: Iterable[dict[str, Any]]) -> None:
        from dh.kalshi.normalize import shard_value

        m = self._oid_shard
        for o in rows:
            oid = str(o.get("order_id") or "")
            sh = shard_value(o.get("exchange_index"))
            if oid and sh is not None:
                m[oid] = sh
        while len(m) > MAX_OID_MAP:
            m.pop(next(iter(m)))

    def group_refs(self) -> list[dict[str, Any]]:
        """Every exchange order group of this session: [{logical, id, exchange_index, subaccount}]
        (the heartbeat carries them so the watchdog can trigger them)."""
        return [{"logical": lg, "id": gid, "exchange_index": sh, "subaccount": self.sub}
                for lg, per in sorted(self.groups.items()) for sh, gid in sorted(per.items())]

    @property
    def inflight(self) -> int:
        return len(self._tasks)

    async def wait_idle(self, timeout_s: float) -> bool:
        """Wait until every in-flight request task finished (True) or the timeout (False). The
        calling task itself is never waited for (review NEW-5: a kill running INSIDE a venue task
        would otherwise wait the full timeout on itself)."""
        me = asyncio.current_task()
        deadline = self._mono() + timeout_s
        while True:
            pending = {t for t in self._tasks if t is not me}
            if not pending:
                return True
            left = deadline - self._mono()
            if left <= 0:
                return False
            await asyncio.wait(pending, timeout=left)

    def _cancel_and_track(self, acts: Sequence[CancelOrder]) -> set[asyncio.Task[Any]]:
        """``cancel_orders`` returning the request tasks it started (a scoped cancel-all round
        waits for its own cancels only, never for itself or unrelated requests)."""
        before = set(self._tasks)
        self.cancel_orders(acts)
        return set(self._tasks) - before

    async def _wait_tasks(self, tasks: set[asyncio.Task[Any]], timeout_s: float) -> None:
        tasks = {t for t in tasks if t is not asyncio.current_task() and not t.done()}
        if tasks and timeout_s > 0:
            await asyncio.wait(tasks, timeout=timeout_s)

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
        """CreateOrderV2Request for ``a``: explicit subaccount AND exchange shard, and the
        exchange order-group id of that shard (ValueError if the shard is unknown / not
        configured or the logical group has no exchange id there: never send a quote outside
        its group, never auto-route a create)."""
        sh = self.trade_shard(a.ticker)
        body = place_order_body(
            a,
            self_trade_prevention=self.cfg.self_trade_prevention,
            time_in_force=TIF.get(a.time_in_force, a.time_in_force),
            subaccount=self.sub,
            exchange_index=sh,
        )
        if a.order_group_id:
            gid = self.groups.get(a.order_group_id, {}).get(sh)
            if not gid:
                raise ValueError(f"order group {a.order_group_id!r} has no exchange id on shard {sh} (not created)")
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
        return {"order_id": a.order_id, "market_ticker": a.ticker, "subaccount": self.sub,
                "exchange_index": self.cancel_shard(a.ticker, a.order_id)}

    async def _cancel_one(self, a: CancelOrder) -> None:
        self.stats.bump("cancel")
        t0 = self._mono()
        try:
            res: Any = await self.rest.cancel_order(a.order_id, market_ticker=a.ticker, subaccount=self.sub,
                                                    exchange_index=self.cancel_shard(a.ticker, a.order_id))
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
    @property
    def bulk_cancel_allowed(self) -> bool:
        """The BULK cancel-all endpoint may be used only on an account declared NOT shared
        (``VenueCfg.bulk_cancel_allowed``); a shared account cancels by id (``scoped_cancel_all``)."""
        return self.cfg.bulk_cancel_allowed

    async def cancel_all_now(self, reason: str, *, attempts: int = 5) -> bool:
        """Cancel every resting order of our subaccount. Non-shared account: the bulk
        DELETE /portfolio/events/orders (``_bulk_cancel_all``). Shared account: NEVER the bulk
        endpoint (its one-minute tail's subaccount scope is unverified): the resting-order list
        of our subaccount, cancelled by id, repeated until empty (``scoped_cancel_all``; order
        groups are triggered first by ``latch_kill`` on every terminal path). True on success."""
        if self.bulk_cancel_allowed:
            return await self._bulk_cancel_all(reason, attempts=attempts)
        for i in range(max(1, attempts)):
            try:
                left = await self.scoped_cancel_all(reason)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - the resting-order list failed: retry
                self._log("cancel_all_error", reason=reason, scoped=True, error=f"{type(exc).__name__}: {exc}"[:200])
                await self._sleep(self._backoff_ns(i) / NS_PER_S)
                continue
            return not left
        return False

    async def scoped_cancel_all(self, reason: str, *, rounds: int | None = None, wait_s: float = 2.0) -> list[dict[str, Any]]:
        """Shared-account cancel-all: GET /portfolio/orders?status=resting&subaccount=<ours> and a
        batch cancel BY ID (every item names our subaccount and the order's own shard), repeated
        until the list is empty or after ``rounds`` (venue.cancel_rounds). Returns the orders
        still resting ([] = confirmed clean). Raises when the list cannot be read.

        Each round waits (at most ``wait_s``) for ITS OWN cancel requests only (review NEW-5: run
        inside a venue task, as on the kill / halt paths, waiting for every venue task included
        itself and always took the full ``wait_s``). A create still in flight lands on the next
        round's list; the order-group trigger (``latch_kill``, first on every terminal path)
        rejects it on arrival; shutdown waits for in-flight requests before its own pass."""
        n_rounds = max(1, int(self.cfg.cancel_rounds if rounds is None else rounds))
        self.stats.bump("cancel_all_scoped")
        for rnd in range(n_rounds):
            left = await self.resting_orders()
            if not left:
                self._log("cancel_all", reason=reason, scoped=True, rounds=rnd)
                return []
            self._log("cancel_all_round", reason=reason, scoped=True, n=len(left), round=rnd + 1)
            mine = self._cancel_and_track([CancelOrder(str(o.get("client_order_id") or ""), str(o.get("ticker") or ""),
                                                       str(o["order_id"]), reason=f"cancel_all:{reason}")
                                           for o in left if o.get("order_id")])
            await self._wait_tasks(mine, wait_s)
            await self._sleep(0.5 * (rnd + 1))
        left = await self.resting_orders()
        if left:
            log.error("CANCEL-ALL (by id) UNCONFIRMED: %d orders of subaccount %d still resting after %d rounds (%s)",
                      len(left), self.sub, n_rounds, reason)
            self._log("cancel_all_leftovers", reason=reason, scoped=True, n=len(left),
                      oids=[o.get("order_id") for o in left][:20])
        return left

    async def _bulk_cancel_all(self, reason: str, *, attempts: int = 5) -> bool:
        """DELETE /portfolio/events/orders?subaccount=<ours>, retried on unknown outcomes /
        throttling (cancel-all is idempotent). True on a 2xx. Kalshi may also cancel orders
        placed during the minute after the request (``last_cancel_all_ns``: the runner holds
        new orders for ``cancel_all_hold_s``). NEVER on a shared account (raises)."""
        if not self.bulk_cancel_allowed:
            raise RuntimeError("the bulk cancel-all is forbidden on a shared account (venue.shared_account)")
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
        itself failed. Shared account: ``scoped_cancel_all`` (list + cancel by id, no bulk)."""
        if not self.bulk_cancel_allowed:
            return await self.scoped_cancel_all(reason, rounds=max(1, rounds), wait_s=wait_s)
        if not await self._bulk_cancel_all(reason):
            raise RuntimeError(f"cancel-all failed ({reason})")
        left: list[dict[str, Any]] = []
        for attempt in range(max(1, rounds)):
            left = await self.resting_orders()
            if not left:
                return []
            self._log("cancel_all_leftovers", reason=reason, n=len(left), attempt=attempt + 1)
            mine = self._cancel_and_track([CancelOrder(str(o.get("client_order_id") or ""), str(o.get("ticker") or ""),
                                                       str(o["order_id"]), reason=f"verify:{reason}")
                                           for o in left if o.get("order_id")])
            await self._wait_tasks(mine, wait_s)
            await self._sleep(0.5 * (attempt + 1))
        return await self.resting_orders()

    async def resting_orders(self, *, ticker: str | None = None) -> list[dict[str, Any]]:
        """GET /portfolio/orders?status=resting&subaccount=<ours> (every page, every shard: a
        leftover on any shard of our subaccount must be seen). Learns each order's shard."""
        kw: dict[str, Any] = {"status": "resting"}
        if ticker:
            kw["ticker"] = ticker
        kw["subaccount"] = self.sub
        rows = [o async for o in self.rest.iter_orders(**kw)]
        self._learn_rows(rows)
        return rows

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
        try:  # an amend may raise the order's size or move its price: same shard rule as a create
            body = amend_order_body(a, exchange_index=self.trade_shard(a.ticker))
        except ValueError as exc:
            self.stats.local_rejects += 1
            self._emit([OrderReject(self._now(), 0, a.client_order_id, a.ticker, f"invalid_order: {exc}"[:200], 0, "amend")])
            return
        try:
            res: Any = await self.rest.amend_order(a.order_id, body, subaccount=self.sub)
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
            res: Any = await self.rest.decrease_order(a.order_id, subaccount=self.sub,
                                                      exchange_index=self.cancel_shard(a.ticker, a.order_id),
                                                      **decrease_order_kwargs(a))
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
    async def ensure_order_group(self, logical_id: str, contracts_limit: int, *,
                                 exchange_index: int | None = None) -> str | None:
        """Create the exchange order group(s) for ``logical_id``: one per shard in use (or only
        ``exchange_index``), idempotent per session (an existing one only gets its limit
        updated). Returns an exchange id (the only shard's; with several shards the first one),
        or None when any shard's group could not be established (the caller refuses to trade)."""
        shards = [int(exchange_index)] if exchange_index is not None else self.shards_in_use()
        if not shards:
            self._log("order_group_error", op="create", logical=logical_id,
                      error="no market with a known, configured exchange shard")
            return None
        gids: list[str] = []
        for sh in shards:
            gid = await self._ensure_group_on(logical_id, contracts_limit, sh)
            if gid is None:
                return None
            gids.append(gid)
        return gids[0]

    async def _ensure_group_on(self, logical_id: str, contracts_limit: int, sh: int) -> str | None:
        per = self.groups.get(logical_id, {})
        if sh in per:
            if self.group_limits.get(logical_id) != contracts_limit:
                await self._retry_group("limit", logical_id, contracts_limit)
            return per[sh]
        if self.kill_latched:
            self._log("order_group_error", op="create", logical=logical_id, exchange_index=sh,
                      error=f"kill switch latched ({self.kill_latched}): no new group")
            return None
        before: set[str] | None = None
        try:
            before = {str(g.get("id")) for g in (await self.rest.get_order_groups(subaccount=self.sub)).get("order_groups") or []}
        except Exception as exc:  # noqa: BLE001 - only needed to resolve an unknown outcome
            self._log("order_group_list_error", error=f"{type(exc).__name__}: {exc}"[:200])
        for attempt in range(3):
            self.stats.bump("create_order_group")
            try:
                res = await self.rest.create_order_group(contracts_limit, subaccount=self.sub, exchange_index=sh)
            except KalshiHTTPError as exc:
                self._log("order_group_error", op="create", exchange_index=sh, status=exc.status, error=str(exc)[:200])
                if exc.status != 429:
                    return None
                res = None
            except NotSentError as exc:
                self._log("order_group_error", op="create", exchange_index=sh, error=str(exc)[:200])
                res = None
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                res = UnknownOutcome("POST", "/portfolio/order_groups/create", None, f"{type(exc).__name__}: {exc}")
            if isinstance(res, dict) and res.get("order_group_id"):
                gid = str(res["order_group_id"])
                self._map_group(logical_id, sh, gid, contracts_limit, "create")
                return gid
            if isinstance(res, UnknownOutcome) or (isinstance(res, dict) and not res.get("order_group_id")):
                # did it get created? adopt a group that was not there before, on this shard,
                # with our limit
                try:
                    groups = (await self.rest.get_order_groups(subaccount=self.sub)).get("order_groups") or []
                except Exception:  # noqa: BLE001
                    groups = []
                if before is not None:
                    fresh = [g for g in groups if str(g.get("id")) not in before
                             and _fp_qty(g.get("contracts_limit_fp")) in (contracts_limit, None)
                             and _shard_or_none(g.get("exchange_index")) in (sh, None)]
                    if len(fresh) == 1:
                        gid = str(fresh[0]["id"])
                        self._map_group(logical_id, sh, gid, contracts_limit, "adopt")
                        return gid
            await self._sleep(self._backoff_ns(attempt) / NS_PER_S)
        return None

    def _map_group(self, logical_id: str, sh: int, gid: str, limit: int, op: str) -> None:
        self.groups.setdefault(logical_id, {})[sh] = gid
        self.group_limits[logical_id] = limit
        self._log("order_group", op=op, logical=logical_id, id=gid, limit=limit, exchange_index=sh,
                  subaccount=self.sub)
        if self.on_group_map is not None:
            self.on_group_map(logical_id, gid)

    async def _retry_group(self, op: str, logical_id: str, limit: int = 0, attempts: int = 5) -> bool:
        """``op`` (reset / limit / delete / trigger) on every shard's group of ``logical_id``,
        each with its explicit subaccount and shard; True only if every one succeeded."""
        per = dict(self.groups.get(logical_id) or {})
        if not per:
            self._log("order_group_error", op=op, logical=logical_id, error="no exchange id")
            return False
        if op == "reset" and self.kill_latched:
            self._log("order_group_error", op=op, logical=logical_id,
                      error=f"kill switch latched ({self.kill_latched}): the group stays triggered")
            return False
        ok = True
        for sh, gid in sorted(per.items()):
            ok = await self._group_write(op, logical_id, sh, gid, limit, attempts) and ok
        return ok

    async def _group_write(self, op: str, logical_id: str, sh: int, gid: str, limit: int, attempts: int) -> bool:
        for i in range(max(1, attempts)):
            self.stats.bump(f"order_group_{op}")
            try:  # every group write names its subaccount AND shard (groups live on one shard)
                if op == "reset":
                    res: Any = await self.rest.reset_order_group(gid, subaccount=self.sub, exchange_index=sh)
                elif op == "limit":
                    res = await self.rest.update_order_group_limit(gid, limit, subaccount=self.sub, exchange_index=sh)
                elif op == "trigger":
                    res = await self.rest.trigger_order_group(gid, subaccount=self.sub, exchange_index=sh)
                else:
                    res = await self.rest.delete_order_group(gid, subaccount=self.sub, exchange_index=sh)
            except KalshiHTTPError as exc:
                self._log("order_group_error", op=op, id=gid, exchange_index=sh, status=exc.status, error=str(exc)[:200])
                if exc.status != 429:
                    return False
                res = None
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - NotSent / transport: retry (idempotent)
                self._log("order_group_error", op=op, id=gid, exchange_index=sh, error=f"{type(exc).__name__}: {exc}"[:200])
                res = None
            if isinstance(res, dict):
                if op == "limit":
                    self.group_limits[logical_id] = limit
                if op == "delete":
                    per = self.groups.get(logical_id, {})
                    per.pop(sh, None)
                    if not per:
                        self.groups.pop(logical_id, None)
                self._log("order_group", op=op, logical=logical_id, id=gid, exchange_index=sh, limit=limit or None)
                return True
            await self._sleep(self._backoff_ns(i) / NS_PER_S)
        return False

    async def trigger_groups(self, reason: str, *, attempts: int = 3) -> int:
        """PUT /portfolio/order_groups/{id}/trigger (explicit subaccount and shard) on every group
        of this session: Kalshi cancels each group's resting orders and rejects new ones in it
        until a reset. Returns the number of groups triggered."""
        n = 0
        for logical in sorted(self.groups):
            for sh, gid in sorted((self.groups.get(logical) or {}).items()):
                if await self._group_write("trigger", logical, sh, gid, 0, attempts):
                    n += 1
        self._log("groups_triggered", reason=reason, n=n, groups=self.group_refs())
        return n

    def latch_kill(self, reason: str) -> None:
        """The kill switch's first and fastest scoped step (kill file, a manual halt, a fee
        mismatch, the watchdog's cancel-all about this runner, shutdown): trigger every order
        group now (Kalshi documents no trailing tail for a trigger, unlike cancel-all's minute),
        and never create or reset a group again in this session. The caller still sends the
        subaccount's cancel-all (orders outside a group, other shards)."""
        if self.kill_latched:
            return
        self.kill_latched = reason or "kill"
        if self.groups:
            log.warning("kill switch: triggering %d order group(s) (%s)", len(self.group_refs()), self.kill_latched)
            self._spawn(self.trigger_groups(self.kill_latched), "trigger_groups")

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
        for k, per in self.groups.items():
            if exchange_id in per.values():
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

    async def lookup_order(self, oid: str) -> dict[str, Any] | None:
        """Read-only GET /portfolio/orders/{oid} (review NEW-1: who owns an order a fill names?).
        The endpoint takes no subaccount parameter: the ROW's ``subaccount_number`` is checked by
        the caller (a key restricted to our subaccount cannot see another subaccount's orders).
        Returns the Order row, None on 404; raises on anything else."""
        try:
            body = await self.rest.get_order(oid)
        except KalshiHTTPError as exc:
            if exc.status == 404:
                return None
            raise
        o = body.get("order") if isinstance(body, dict) else None
        if not isinstance(o, dict):
            raise ValueError(f"GET /portfolio/orders/{oid}: no order object")
        self._learn_rows([o])
        return o

    async def find_order(self, oid: str, *, ticker: str = "",
                         min_ts_s: int | None = None) -> tuple[dict[str, Any] | None, dict[str, Any]]:
        """Who owns order ``oid`` (read-only, review F1 (b)): GET /portfolio/orders/{oid} (no
        subaccount / exchange_index parameter: it may not see an order on shard 2) and, when that
        404s and ``ticker`` is known, the LIST GET /portfolio/orders?subaccount=<ours>&ticker=<t>
        (every shard) searched for the id: first ``status=resting`` (a parked fill's order is
        usually still resting: one short page), then every status from ``min_ts_s`` on (review L2:
        not the market's whole order history on every retry; the runner passes its session start
        minus a margin: every order of the subaccount that can fill during the session was placed
        after it, the start-up cancel-all having cleared the older ones). The API filters on one
        status only, and the order of a parked fill may equally be executed or canceled (after a
        partial fill), so the second read takes every status. Returns (row or None, via) with
        via = {"by_id": bool, "by_list": bool | None (not tried), "shard": the row's
        exchange_index}; raises on anything but a 404 of the by-id read."""
        from dh.kalshi.normalize import shard_value

        via: dict[str, Any] = {"by_id": False, "by_list": None, "shard": None}
        row = await self.lookup_order(oid)
        if row is not None:
            via["by_id"] = True
        elif ticker:
            found = None
            for extra in ({"status": "resting"}, {} if min_ts_s is None else {"min_ts": int(min_ts_s)}):
                it = self.rest.iter_orders(subaccount=self.sub, ticker=ticker, **extra)
                try:
                    async for o in it:
                        if str(o.get("order_id") or "") == oid:
                            found = o
                            break
                finally:
                    aclose = getattr(it, "aclose", None)
                    if aclose is not None:
                        await aclose()
                if found is not None:
                    break
            via["by_list"] = found is not None
            if found is not None:
                self._learn_rows([found])
                row = found
        if row is not None:
            via["shard"] = shard_value(row.get("exchange_index"))
        return row, via

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
        """[(order_id, market_ticker, queue_qty in 0.01 contracts)] for our resting orders.

        openapi 3.31.0 gives GET /portfolio/orders/queue_positions only ``market_tickers``,
        ``event_ticker`` and ``subaccount`` (default 0): no ``exchange_index``. Whether it
        covers shard-2 orders is undocumented (finding 13): the call carries the explicit
        subaccount and the runner measures the coverage (``dh_queue_positions_coverage``,
        one ``verify_live`` log line)."""
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

    async def fetch_balances(self) -> dict[int, dict[str, Any]]:
        """GET /portfolio/balance?subaccount=<ours>&exchange_index=<shard> for every configured
        shard (collateral is local to a shard): {shard: GetBalanceResponse}. Raises on failure."""
        out: dict[int, dict[str, Any]] = {}
        for sh in sorted(set(self.cfg.exchange_indexes)):
            out[int(sh)] = await self.rest.get_balance(subaccount=self.sub, exchange_index=int(sh))
        return out

    async def fetch_shard_funds(self) -> dict[int, dict[str, Any]]:
        """Funds of our subaccount on every configured shard, at cost:
            available  GET /portfolio/balance?subaccount&exchange_index (``balance``: cash not
                       tied up in positions; VERIFIED LIVE 2026-09-30: resting orders are NOT
                       deducted from it, i.e. $75.00 with $19.10 of orders resting)
            positions  the cost of the open positions (``market_exposure_dollars`` of
                       GET /portfolio/positions?subaccount&exchange_index)
            resting    the collateral of the resting orders (GET /portfolio/orders?status=
                       resting&subaccount&exchange_index: a bid px x remaining, an ask
                       (1 - px) x remaining), EXCEPT the part of an order that closes a held
                       position (an ask against a long YES position, a bid against a short
                       one): Kalshi reserves nothing for it, so counting it would overstate
                       the funds (review L2)
            funds      available + positions: what the shard holds for this system at cost, so
                       our own fills never make it look defunded (a real loss lowers it).
                       ``resting`` is reported but NOT added: the balance already contains it
                       (adding it overstated the funds by the resting collateral, live
                       2026-09-30)
        {shard: {..., "body": GetBalanceResponse}}; ``funds`` None when the balance is
        unreadable. Raises on a failed read."""
        from dh.core.units import PX_SCALE, px_from_dollars
        from dh.kalshi.normalize import market_position

        out: dict[int, dict[str, Any]] = {}
        for sh in sorted(set(self.cfg.exchange_indexes)):
            sh = int(sh)
            body = await self.rest.get_balance(subaccount=self.sub, exchange_index=sh)
            avail = balance_dollars(body)
            pos = await self.rest.get_all_positions(count_filter="position", subaccount=self.sub, exchange_index=sh)
            cost = 0.0
            held: dict[str, int] = {}  # ticker -> signed YES position (qty units)
            for m in pos.get("market_positions") or []:
                try:
                    cost += abs(float(str(m.get("market_exposure_dollars") or "0")))
                except ValueError:
                    pass
                try:
                    mp = market_position(m)
                except (KeyError, ValueError, TypeError):
                    continue
                if mp.ticker:
                    held[mp.ticker] = held.get(mp.ticker, 0) + int(mp.position)
            rows = [o async for o in self.rest.iter_orders(status="resting", subaccount=self.sub, exchange_index=sh)]
            self._learn_rows(rows)
            reserved = 0.0
            closable_long = {t: q for t, q in held.items() if q > 0}  # asks closing a long YES position
            closable_short = {t: -q for t, q in held.items() if q < 0}  # bids closing a short (NO) position
            for o in sorted(rows, key=lambda r: str(r.get("order_id") or "")):
                try:
                    px = px_from_dollars(str(o.get("yes_price_dollars")))
                    rem = qty_from_fp(str(o.get("remaining_count_fp") or "0"))
                except (TypeError, ValueError, ArithmeticError):
                    continue
                t = str(o.get("ticker") or "")
                bid = str(o.get("book_side") or "") == "bid"
                room = closable_short if bid else closable_long
                closing = min(rem, max(0, room.get(t, 0)))
                if closing:
                    room[t] -= closing
                per = px if bid else PX_SCALE - px
                reserved += per * (rem - closing) / 1e6  # px (1e-4 $) x qty (1e-2) = 1e-6 $
            out[sh] = {"available": avail, "positions": round(cost, 6), "resting": round(reserved, 6),
                       "funds": None if avail is None else round(avail + cost, 6), "body": body}
        return out

    async def fetch_user_data_ns(self) -> int | None:
        """GET /exchange/user_data_timestamp -> ``as_of_time`` (ns): the time up to which Kalshi's
        GetBalance / GetOrders / GetFills / GetPositions data was last validated; None if it
        cannot be read (the caller then falls back to its other guards)."""
        try:
            body = await self.rest.get_user_data_timestamp()
            ns = opt_iso_to_ns((body or {}).get("as_of_time"))
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            self._log("user_data_timestamp_error", error=f"{type(exc).__name__}: {exc}"[:200])
            return None
        return int(ns) if ns else None

    async def fetch_positions_checked(self) -> tuple[dict[str, int], int | None]:
        """(positions, as_of ns): the user-data timestamp is read FIRST, so the positions that
        follow reflect at least everything up to it (finding 12)."""
        as_of = await self.fetch_user_data_ns()
        return await self.fetch_positions(), as_of

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


def _shard_or_none(v: Any) -> int | None:
    from dh.kalshi.normalize import shard_value

    return shard_value(v)


def balance_dollars(body: Any) -> float | None:
    """Available balance of a GetBalanceResponse in dollars (``balance_dollars``, else the
    ``balance`` cents), None when neither parses."""
    if not isinstance(body, dict):
        return None
    v = body.get("balance_dollars")
    if v not in (None, ""):
        try:
            return float(str(v))
        except ValueError:
            pass
    c = body.get("balance")
    if isinstance(c, (int, float)) and not isinstance(c, bool):
        return float(c) / 100.0
    return None
