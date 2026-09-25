"""Order state machine used INSIDE the deterministic Strategy (identical in live and backtest).

Actions are requests. ``request_*`` records intent (PENDING_* states); confirmed states change
only when events arrive through ``on_event``. Quantities are ints (0.01 contracts), prices ints
(1e-4 $, YES scale), money ints (micro-dollars).

Per-order quantity model (robust to every message ordering Kalshi can produce):
    cap        max fillable total at the exchange (initial count, amended count, reduced by
               decrease / IOC remainder / terminal status)
    fill_sum   sum of fill messages attributed to the order (deduplicated by trade_id)
    filled_rep max filled qty reported by acks / order updates / cancel acks (monotone)
    filled     = max(fill_sum, filled_rep)
    remaining  = cap - filled while live, else 0
    inflight   = filled_rep - fill_sum: fills that happened but whose message has not arrived
Position comes only from fill messages (never from reported counts), so it is exact once all
fills are delivered; ``worst_case_exposure`` adds everything that could still fill or is still
in flight. docs/EXECUTION_MODEL.md lists every race and how it resolves.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum

from dh.core.actions import AmendOrder, CancelAll, CancelOrder, DecreaseOrder, PlaceOrder
from dh.core.events import (
    CancelAck,
    KalshiFill,
    KalshiOrderGroupUpdate,
    KalshiOrderUpdate,
    KalshiPositionSnapshot,
    OrderAck,
    OrderReject,
    Timer,
)
from dh.core.units import NS_PER_S, PX_SCALE, notional_micros


class OrderState(str, Enum):
    PENDING_NEW = "PENDING_NEW"
    RESTING = "RESTING"
    PENDING_CANCEL = "PENDING_CANCEL"
    PENDING_AMEND = "PENDING_AMEND"  # amend or decrease in flight
    FILLED = "FILLED"
    CANCELED = "CANCELED"
    REJECTED = "REJECTED"


TERMINAL = frozenset({OrderState.FILLED, OrderState.CANCELED, OrderState.REJECTED})
LIVE = frozenset({OrderState.PENDING_NEW, OrderState.RESTING, OrderState.PENDING_CANCEL, OrderState.PENDING_AMEND})

# Reject reasons (normalized by the adapter / produced by the simulator).
UNKNOWN_OUTCOME_REASONS = frozenset({"timeout", "unknown", "network_error", "connection_error", "http_5xx",
                                     "internal_error", "service_unavailable"})
FILLED_REASONS = frozenset({"already_filled", "executed", "order_executed"})
GONE_REASONS = frozenset({"already_canceled", "canceled", "order_canceled", "expired"})
NOT_FOUND_REASONS = frozenset({"not_found", "order_not_found", "404"})

EVENT_KINDS = (
    "accepted", "rejected", "fill", "filled", "canceled", "amended", "decreased", "cancel_rejected",
    "amend_rejected", "decrease_rejected", "cancel_ready", "reconcile_needed", "position_mismatch",
    "orphan_fill", "unknown_order", "group_triggered", "group_reset",
)


@dataclass(frozen=True, slots=True)
class OrderEvent:
    """Typed notification returned by ``OrderManager.on_event`` (kind in EVENT_KINDS).

    qty: fill qty ('fill', 'orphan_fill'), canceled qty ('canceled'), requested qty ('rejected').
    """

    ts: int
    kind: str
    client_order_id: str
    ticker: str
    order_id: str = ""
    state: OrderState | None = None
    book_side: str = ""
    px: int = 0
    qty: int = 0
    filled_qty: int = 0
    remaining_qty: int = 0
    is_taker: bool = False
    fee_micros: int = 0
    detail: str = ""


@dataclass(frozen=True, slots=True)
class WorkingOrder:
    """Immutable snapshot of one order (quantities in 0.01 contracts, px in 1e-4 $ YES)."""

    client_order_id: str
    order_id: str
    ticker: str
    book_side: str
    px: int
    total_qty: int
    filled_qty: int
    remaining_qty: int
    state: OrderState
    created_ns: int
    updated_ns: int
    post_only: bool
    expiration_ts: int
    order_group_id: str
    unknown_outcome: bool
    cancel_requested: bool
    pending_px: int
    pending_total_qty: int
    inflight_fill_qty: int
    unresolved: bool


@dataclass(slots=True)
class _Order:
    coid: str
    ticker: str
    book_side: str
    px: int
    cap: int
    post_only: bool
    expiration_ts: int
    group: str
    created_ns: int
    seq: int
    updated_ns: int = 0
    state: OrderState = OrderState.PENDING_NEW
    prev_state: OrderState = OrderState.PENDING_NEW
    order_id: str = ""
    fill_sum: int = 0
    filled_rep: int = 0
    fees: int = 0
    unknown: bool = False  # create outcome unknown (timeout) -> reconcile
    unresolved: bool = False  # terminal but final filled qty unknown -> reconcile
    flagged: bool = False  # reconcile_needed already emitted for the current pending request
    cancel_requested: bool = False
    cancel_deferred: bool = False  # cancel wanted but no order_id yet: re-send after the ack
    cancel_sent_ns: int = 0
    amend: tuple[str, int, int] | None = None  # (new coid, px, total qty)
    decrease_to: int | None = None
    change_sent_ns: int = 0
    decrease_anchor_ts: int = 0
    last_exch_ts: int = 0
    aliases: list[str] = field(default_factory=list)

    @property
    def filled(self) -> int:
        return max(self.fill_sum, self.filled_rep)

    @property
    def remaining(self) -> int:
        return max(0, self.cap - self.filled) if self.state in LIVE else 0

    @property
    def inflight(self) -> int:
        return max(0, self.filled_rep - self.fill_sum)

    def could_fill(self) -> int:
        """Qty that may still turn into fill messages (remaining + in flight + pending amend-up)."""
        if self.state is OrderState.REJECTED:
            return 0
        extra = self.inflight
        if self.state in LIVE:
            rem = max(0, self.cap - self.filled)
            if self.amend is not None:
                rem = max(rem, max(0, self.amend[2] - self.filled))
            extra += rem
        elif self.unresolved:
            extra += max(0, self.cap - self.filled)
        return extra


class OrderManager:
    """Per-client_order_id state machine plus fill-derived position, cash and fees.

    Args:
        ack_timeout_ns: PENDING_NEW longer than this -> outcome unknown, 'reconcile_needed'.
        change_timeout_ns: PENDING_CANCEL / PENDING_AMEND longer than this -> 'reconcile_needed'.
        initial_positions: {ticker: signed YES qty} held before this session (0.01 contracts).
    Timeouts are checked on ``Timer`` events (or ``check_timeouts(now_ns)``); no wall clock.
    """

    def __init__(self, *, ack_timeout_ns: int = 10 * NS_PER_S, change_timeout_ns: int = 10 * NS_PER_S,
                 initial_positions: dict[str, int] | None = None) -> None:
        self.ack_timeout_ns = int(ack_timeout_ns)
        self.change_timeout_ns = int(change_timeout_ns)
        self._orders: list[_Order] = []
        self._by_coid: dict[str, _Order] = {}
        self._by_oid: dict[str, _Order] = {}
        self._seen_trades: set[str] = set()
        self._orphans: dict[str, list[KalshiFill]] = {}  # keyed 'oid:<id>' or 'coid:<id>'
        self._pos: dict[str, int] = dict(initial_positions or {})
        self._exch_pos: dict[str, int] = {}
        self._cash: dict[str, int] = {}
        self._fees: dict[str, int] = {}
        self._volume: dict[str, int] = {}
        self._seq = 0
        self._groups_blocked: set[str] = set()
        self.stats = {"duplicate_fills": 0, "orphan_fills": 0, "position_mismatches": 0, "unknown_events": 0,
                      "stale_updates": 0}

    # ================================================================== intents
    def request_place(self, a: PlaceOrder, now_ns: int) -> None:
        """Record a new order request (state PENDING_NEW). Raises on a reused client_order_id."""
        if a.client_order_id in self._by_coid:
            raise ValueError(f"client_order_id {a.client_order_id!r} already used")
        if a.book_side not in ("bid", "ask"):
            raise ValueError(f"bad book_side {a.book_side!r}")
        if a.qty <= 0 or not 0 < a.px < PX_SCALE:
            raise ValueError(f"bad qty/px {a.qty}/{a.px}")
        self._seq += 1
        o = _Order(a.client_order_id, a.ticker, a.book_side, a.px, a.qty, a.post_only, a.expiration_ts,
                   a.order_group_id, now_ns, self._seq, updated_ns=now_ns)
        self._orders.append(o)
        self._by_coid[o.coid] = o

    def request_cancel(self, a: CancelOrder, now_ns: int) -> bool:
        """Record cancel intent. Returns True if the CancelOrder can be sent now; False if there
        is nothing to cancel, a cancel is already pending, or the order_id is not known yet (the
        cancel is then deferred and a 'cancel_ready' event is emitted when the ack arrives)."""
        o = self._lookup(a.client_order_id, a.order_id)
        if o is None or o.state in TERMINAL or o.state is OrderState.PENDING_CANCEL:
            return False
        o.prev_state = o.state
        o.state = OrderState.PENDING_CANCEL
        o.cancel_requested = True
        o.cancel_sent_ns = now_ns
        o.updated_ns = now_ns
        o.flagged = False
        if not o.order_id:
            o.cancel_deferred = True
            return False
        return True

    def request_cancel_all(self, a: CancelAll, now_ns: int) -> list[str]:
        """Mark every live order (in a.tickers, or all) PENDING_CANCEL; returns their ids."""
        out = []
        for o in self._orders:
            if o.state in LIVE and o.state is not OrderState.PENDING_CANCEL and (not a.tickers or o.ticker in a.tickers):
                self.request_cancel(CancelOrder(o.coid, o.ticker, o.order_id, a.reason), now_ns)
                out.append(o.coid)
        return out

    def request_amend(self, a: AmendOrder, now_ns: int) -> bool:
        """Record an amend (price and/or total count = filled + desired remaining). Only a
        RESTING order with a known order_id can be amended; returns False otherwise."""
        o = self._lookup(a.client_order_id, a.order_id)
        if o is None or o.state is not OrderState.RESTING or not o.order_id:
            return False
        if a.new_client_order_id != o.coid and a.new_client_order_id in self._by_coid:
            raise ValueError(f"client_order_id {a.new_client_order_id!r} already used")
        o.amend = (a.new_client_order_id, a.px, a.total_qty)
        if a.new_client_order_id != o.coid:
            self._by_coid[a.new_client_order_id] = o
            o.aliases.append(a.new_client_order_id)
        o.prev_state, o.state = o.state, OrderState.PENDING_AMEND
        o.change_sent_ns = o.updated_ns = now_ns
        o.flagged = False
        return True

    def request_decrease(self, a: DecreaseOrder, now_ns: int) -> bool:
        """Record a size decrease (keeps priority on Kalshi). RESTING orders only."""
        o = self._lookup(a.client_order_id, a.order_id)
        if o is None or o.state is not OrderState.RESTING or not o.order_id:
            return False
        o.decrease_to = max(0, a.reduce_to)
        o.prev_state, o.state = o.state, OrderState.PENDING_AMEND
        o.change_sent_ns = o.updated_ns = now_ns
        o.flagged = False
        return True

    # ================================================================== events
    def on_event(self, ev) -> list[OrderEvent]:
        """Consume an exchange event; returns the resulting OrderEvents (possibly empty)."""
        if isinstance(ev, KalshiFill):
            return self._on_fill(ev)
        if isinstance(ev, OrderAck):
            return self._on_ack(ev)
        if isinstance(ev, CancelAck):
            return self._on_cancel_ack(ev)
        if isinstance(ev, KalshiOrderUpdate):
            return self._on_update(ev)
        if isinstance(ev, OrderReject):
            return self._on_reject(ev)
        if isinstance(ev, Timer):
            return self.check_timeouts(ev.ts)
        if isinstance(ev, KalshiOrderGroupUpdate):
            return self._on_group(ev)
        if isinstance(ev, KalshiPositionSnapshot):
            return self.reconcile_position(ev.ticker, ev.position, ev.ts)
        return []

    def _on_group(self, g: KalshiOrderGroupUpdate) -> list[OrderEvent]:
        """A triggered group cancels all its orders at the exchange (their cancel updates follow)
        and rejects new ones until reset; track it so the strategy can stop using the group."""
        if g.event_type == "triggered":
            self._groups_blocked.add(g.order_group_id)
            return [OrderEvent(g.ts, "group_triggered", "", "", detail=g.order_group_id)]
        if g.event_type in ("reset", "created", "deleted"):
            was = g.order_group_id in self._groups_blocked
            self._groups_blocked.discard(g.order_group_id)
            if was:
                return [OrderEvent(g.ts, "group_reset", "", "", detail=f"{g.order_group_id}:{g.event_type}")]
        return []

    def group_blocked(self, order_group_id: str) -> bool:
        """True after the group triggered (limit hit) until it is reset / recreated."""
        return order_group_id in self._groups_blocked

    def check_timeouts(self, now_ns: int) -> list[OrderEvent]:
        """Flag requests without a response for too long (unknown outcome -> reconcile)."""
        out: list[OrderEvent] = []
        for o in self._orders:
            if o.state is OrderState.PENDING_NEW and not o.unknown and now_ns - o.created_ns > self.ack_timeout_ns:
                o.unknown = True
                out.append(self._ev("reconcile_needed", o, now_ns, detail="ack_timeout"))
            elif o.state is OrderState.PENDING_CANCEL and not o.flagged and now_ns - o.cancel_sent_ns > self.change_timeout_ns:
                o.flagged = True
                out.append(self._ev("reconcile_needed", o, now_ns, detail="cancel_timeout"))
            elif o.state is OrderState.PENDING_AMEND and not o.flagged and now_ns - o.change_sent_ns > self.change_timeout_ns:
                o.flagged = True
                out.append(self._ev("reconcile_needed", o, now_ns, detail="amend_timeout"))
        return out

    # ------------------------------------------------------------------ fills
    def _on_fill(self, f: KalshiFill) -> list[OrderEvent]:
        if f.trade_id:
            if f.trade_id in self._seen_trades:
                self.stats["duplicate_fills"] += 1
                return []
            self._seen_trades.add(f.trade_id)
        t = f.ticker
        signed = f.qty if f.book_side == "bid" else -f.qty
        self._pos[t] = self._pos.get(t, 0) + signed
        self._cash[t] = self._cash.get(t, 0) - signed * f.yes_px  # px*qty == micros
        self._fees[t] = self._fees.get(t, 0) + f.fee_micros
        self._volume[t] = self._volume.get(t, 0) + f.qty
        out: list[OrderEvent] = []
        o = self._lookup(f.client_order_id, f.order_id)
        if o is None:
            self.stats["orphan_fills"] += 1
            key = f"oid:{f.order_id}" if f.order_id else f"coid:{f.client_order_id}"
            self._orphans.setdefault(key, []).append(f)
            out.append(OrderEvent(f.ts, "orphan_fill", f.client_order_id, t, f.order_id, None, f.book_side,
                                  f.yes_px, f.qty, is_taker=f.is_taker, fee_micros=f.fee_micros))
        else:
            out.extend(self._apply_fill(o, f))
        if f.has_post_position:
            self._exch_pos[t] = f.post_position
            if f.post_position != self._pos[t]:
                self.stats["position_mismatches"] += 1
                out.append(OrderEvent(f.ts, "position_mismatch", f.client_order_id, t, f.order_id,
                                      detail=f"ours={self._pos[t]} exchange={f.post_position}"))
        return out

    def _apply_fill(self, o: _Order, f: KalshiFill) -> list[OrderEvent]:
        out: list[OrderEvent] = []
        self._bind_oid(o, f.order_id, f.ts, out, attach=False)
        o.fill_sum += f.qty
        o.fees += f.fee_micros
        o.updated_ns = f.ts
        o.unknown = False
        if o.decrease_anchor_ts and f.ts_exch and f.ts_exch <= o.decrease_anchor_ts:
            o.cap += f.qty  # happened before the decrease: not part of the reported remaining
        if o.state is OrderState.PENDING_NEW:
            o.state = OrderState.RESTING  # a fill proves the order reached the book
        elif o.state is OrderState.REJECTED:  # the exchange is authoritative: it did reach the book
            o.state = self._working_state(o)
            out.append(self._ev("reconcile_needed", o, f.ts, detail="fill_on_rejected"))
        out.append(self._ev("fill", o, f.ts, qty=f.qty, px=f.yes_px, is_taker=f.is_taker, fee=f.fee_micros))
        if o.fill_sum > o.cap:  # more fills than the order could have: our view of cap is wrong
            o.cap = o.fill_sum
            out.append(self._ev("reconcile_needed", o, f.ts, detail="overfill"))
        if o.state in LIVE and o.filled >= o.cap:
            self._terminate(o, OrderState.FILLED, f.ts)
            out.append(self._ev("filled", o, f.ts))
        return out

    # ------------------------------------------------------------------ acks
    def _on_ack(self, a: OrderAck) -> list[OrderEvent]:
        o = self._lookup(a.client_order_id, a.order_id)
        if o is None:
            return self._unknown(a.ts, a.client_order_id, a.ticker, a.order_id, f"ack:{a.request}")
        out: list[OrderEvent] = []
        o.unknown = False
        o.updated_ns = a.ts
        if a.request == "create":
            o.filled_rep = max(o.filled_rep, a.fill_qty)
            if a.remaining_qty >= 0:
                o.cap = min(o.cap, a.fill_qty + a.remaining_qty)
            self._bind_oid(o, a.order_id, a.ts, out)
            if o.state is OrderState.PENDING_NEW:
                o.state = OrderState.RESTING
            out.insert(0, self._ev("accepted", o, a.ts))
            if o.state in LIVE and (a.remaining_qty == 0 or o.filled >= o.cap):
                st = OrderState.FILLED if o.filled >= o.cap else OrderState.CANCELED
                canceled = max(0, o.cap - o.filled)
                self._terminate(o, st, a.ts)
                out.append(self._ev("filled" if st is OrderState.FILLED else "canceled", o, a.ts, qty=canceled,
                                    detail="" if st is OrderState.FILLED else "ioc_or_stp_remainder"))
            if o.cancel_deferred and o.state is OrderState.PENDING_CANCEL:
                o.cancel_deferred = False
                o.cancel_sent_ns = a.ts
                out.append(self._ev("cancel_ready", o, a.ts))
            elif o.cancel_deferred and o.state in TERMINAL:
                o.cancel_deferred = False
            return out
        if a.request == "amend":
            self._bind_oid(o, a.order_id, a.ts, out)
            if o.amend is None:
                out.append(self._ev("reconcile_needed", o, a.ts, detail="unexpected_amend_ack"))
                return out
            new_coid, px, total = o.amend
            o.amend = None
            o.px, o.cap = px, total
            if new_coid != o.coid:
                o.aliases.append(o.coid)
                o.coid = new_coid
            if a.remaining_qty >= 0:
                o.filled_rep = max(o.filled_rep, total - a.remaining_qty)
            if o.state is OrderState.PENDING_AMEND:
                o.state = OrderState.RESTING
            out.append(self._ev("amended", o, a.ts))
            if o.state in LIVE and (a.remaining_qty == 0 or o.filled >= o.cap):
                st = OrderState.FILLED if o.filled >= o.cap else OrderState.CANCELED
                self._terminate(o, st, a.ts)
                out.append(self._ev("filled" if st is OrderState.FILLED else "canceled", o, a.ts))
            return out
        if a.request == "decrease":
            self._bind_oid(o, a.order_id, a.ts, out)
            o.decrease_to = None
            if a.remaining_qty >= 0:
                o.cap = o.filled + a.remaining_qty
                o.decrease_anchor_ts = a.ts_exch
            if o.state is OrderState.PENDING_AMEND:
                o.state = OrderState.RESTING
            out.append(self._ev("decreased", o, a.ts))
            if o.state in LIVE and o.filled >= o.cap:
                self._terminate(o, OrderState.CANCELED, a.ts)  # decreased to zero remaining
                out.append(self._ev("canceled", o, a.ts, detail="decreased_to_zero"))
            return out
        return self._unknown(a.ts, a.client_order_id, a.ticker, a.order_id, f"ack:{a.request}")

    def _on_cancel_ack(self, c: CancelAck) -> list[OrderEvent]:
        o = self._lookup(c.client_order_id, c.order_id)
        if o is None:
            return self._unknown(c.ts, c.client_order_id, c.ticker, c.order_id, "cancel_ack")
        out: list[OrderEvent] = []
        self._bind_oid(o, c.order_id, c.ts, out)
        o.updated_ns = c.ts
        if o.state in TERMINAL and not o.unresolved:
            return out  # already final (e.g. canceled/executed update arrived first)
        # At the matching engine: final filled = cap - canceled qty (exact, even with fills in flight).
        o.filled_rep = max(o.filled_rep, o.cap - c.canceled_qty)
        cap_before = o.cap
        o.cap = o.filled
        st = OrderState.CANCELED if c.canceled_qty > 0 or o.filled < cap_before else OrderState.FILLED
        self._terminate(o, st, c.ts)
        out.append(self._ev("canceled" if st is OrderState.CANCELED else "filled", o, c.ts, qty=c.canceled_qty))
        return out

    def _on_update(self, u: KalshiOrderUpdate) -> list[OrderEvent]:
        o = self._lookup(u.client_order_id, u.order_id)
        if o is None:
            return self._unknown(u.ts, u.client_order_id, u.ticker, u.order_id, f"update:{u.status}")
        out: list[OrderEvent] = []
        self._bind_oid(o, u.order_id, u.ts, out)
        o.unknown = False
        o.filled_rep = max(o.filled_rep, u.fill_qty)
        stale = bool(u.ts_exch and o.last_exch_ts and u.ts_exch < o.last_exch_ts)
        if u.ts_exch:
            o.last_exch_ts = max(o.last_exch_ts, u.ts_exch)
        o.updated_ns = u.ts
        if u.status in ("canceled", "executed"):
            if o.state in TERMINAL and not o.unresolved:
                return out
            cap_before = o.cap
            o.cap = max(u.fill_qty, o.fill_sum)
            st = OrderState.FILLED if u.status == "executed" else OrderState.CANCELED
            self._terminate(o, st, u.ts)
            out.append(self._ev("filled" if st is OrderState.FILLED else "canceled", o, u.ts,
                                qty=max(0, cap_before - u.fill_qty) if st is OrderState.CANCELED else 0,
                                detail=f"update:{u.status}"))
            return out
        if u.status == "resting":
            if stale:
                self.stats["stale_updates"] += 1
                return out
            if u.remaining_qty >= 0 and o.amend is None:
                o.cap = min(o.cap, u.fill_qty + u.remaining_qty)
            if o.state is OrderState.REJECTED:  # we were told "rejected" but it is resting
                o.state = self._working_state(o)
                out.append(self._ev("reconcile_needed", o, u.ts, detail="resting_after_reject"))
            if o.state is OrderState.PENDING_NEW:
                o.state = OrderState.RESTING
                out.insert(0, self._ev("accepted", o, u.ts, detail="via_update"))
            if o.cancel_deferred and o.state is OrderState.PENDING_CANCEL:
                o.cancel_deferred = False
                o.cancel_sent_ns = u.ts
                out.append(self._ev("cancel_ready", o, u.ts))
            if o.state in LIVE and o.filled >= o.cap:
                self._terminate(o, OrderState.FILLED, u.ts)
                out.append(self._ev("filled", o, u.ts))
            return out
        out.append(self._ev("reconcile_needed", o, u.ts, detail=f"status:{u.status}"))
        return out

    def _on_reject(self, r: OrderReject) -> list[OrderEvent]:
        o = self._lookup(r.client_order_id, "")
        if o is None:
            return self._unknown(r.ts, r.client_order_id, r.ticker, "", f"reject:{r.request}:{r.reason}")
        out: list[OrderEvent] = []
        o.updated_ns = r.ts
        reason = r.reason
        if r.request == "create":
            if reason in UNKNOWN_OUTCOME_REASONS:
                o.unknown = True
                out.append(self._ev("reconcile_needed", o, r.ts, detail=f"create:{reason}"))
            elif o.state in TERMINAL or o.fill_sum > 0 or o.order_id:
                out.append(self._ev("reconcile_needed", o, r.ts, detail=f"reject_contradicts_state:{reason}"))
            else:
                self._terminate(o, OrderState.REJECTED, r.ts)
                out.append(self._ev("rejected", o, r.ts, qty=o.cap, detail=reason))
            return out
        if r.request == "cancel":
            if o.state in TERMINAL:
                out.append(self._ev("cancel_rejected", o, r.ts, detail=reason))
                return out
            if reason in FILLED_REASONS:
                o.filled_rep = max(o.filled_rep, o.cap)  # fully filled; fill messages may be in flight
                self._terminate(o, OrderState.FILLED, r.ts)
                out.append(self._ev("cancel_rejected", o, r.ts, detail=reason))
                out.append(self._ev("filled", o, r.ts, detail="per_cancel_reject"))
            elif reason in NOT_FOUND_REASONS and not o.order_id:
                o.state = OrderState.PENDING_CANCEL  # the cancel overtook the create: retry after ack
                o.cancel_deferred = True
                out.append(self._ev("cancel_rejected", o, r.ts, detail="not_found_before_ack"))
            elif reason in GONE_REASONS or reason in NOT_FOUND_REASONS:
                self._terminate(o, OrderState.CANCELED, r.ts)
                o.unresolved = True  # final filled qty unknown until an update / reconciliation
                out.append(self._ev("cancel_rejected", o, r.ts, detail=reason))
                out.append(self._ev("reconcile_needed", o, r.ts, detail=f"cancel:{reason}"))
            else:  # transient (rate limit, 5xx...): the order is still working
                o.cancel_requested = False
                o.cancel_deferred = False
                o.state = self._working_state(o)
                out.append(self._ev("cancel_rejected", o, r.ts, detail=reason))
            return out
        if r.request in ("amend", "decrease"):
            kind = "amend_rejected" if r.request == "amend" else "decrease_rejected"
            if o.amend is not None and r.request == "amend":
                new_coid = o.amend[0]
                if new_coid != o.coid and self._by_coid.get(new_coid) is o:
                    del self._by_coid[new_coid]
                    if new_coid in o.aliases:
                        o.aliases.remove(new_coid)
                o.amend = None
            if r.request == "decrease":
                o.decrease_to = None
            if o.state in TERMINAL:
                out.append(self._ev(kind, o, r.ts, detail=reason))
            elif reason in FILLED_REASONS:
                o.filled_rep = max(o.filled_rep, o.cap)
                self._terminate(o, OrderState.FILLED, r.ts)
                out.append(self._ev(kind, o, r.ts, detail=reason))
                out.append(self._ev("filled", o, r.ts, detail="per_amend_reject"))
            elif reason in GONE_REASONS or reason in NOT_FOUND_REASONS:
                self._terminate(o, OrderState.CANCELED, r.ts)
                o.unresolved = True
                out.append(self._ev(kind, o, r.ts, detail=reason))
                out.append(self._ev("reconcile_needed", o, r.ts, detail=f"{r.request}:{reason}"))
            else:
                if o.state is OrderState.PENDING_AMEND:
                    o.state = self._working_state(o)
                out.append(self._ev(kind, o, r.ts, detail=reason))
            return out
        return self._unknown(r.ts, r.client_order_id, r.ticker, "", f"reject:{r.request}:{reason}")

    # ================================================================== reconciliation
    def reconcile_missing(self, client_order_id: str, now_ns: int) -> list[OrderEvent]:
        """The exchange has no record of this order (REST lookup by client_order_id found
        nothing): a create with unknown outcome never reached the book -> REJECTED."""
        o = self._by_coid.get(client_order_id)
        if o is None:
            return []
        if o.state is OrderState.PENDING_NEW and o.fill_sum == 0 and not o.order_id:
            self._terminate(o, OrderState.REJECTED, now_ns)
            return [self._ev("rejected", o, now_ns, qty=o.cap, detail="reconciled_missing")]
        if o.state in LIVE or o.unresolved:
            o.unresolved = False
            o.cap = o.filled
            self._terminate(o, OrderState.CANCELED, now_ns)
            return [self._ev("canceled", o, now_ns, detail="reconciled_missing")]
        return []

    def reconcile_position(self, ticker: str, exchange_position: int, now_ns: int, *,
                           adopt: bool = False) -> list[OrderEvent]:
        """Compare with GET /portfolio/positions; adopt=True overwrites ours (logged event)."""
        ours = self._pos.get(ticker, 0)
        self._exch_pos[ticker] = exchange_position
        if ours == exchange_position:
            return []
        self.stats["position_mismatches"] += 1
        if adopt:
            self._pos[ticker] = exchange_position
        return [OrderEvent(now_ns, "position_mismatch", "", ticker,
                           detail=f"ours={ours} exchange={exchange_position} adopted={adopt}")]

    # ================================================================== views
    def working(self, ticker: str | None = None) -> list[WorkingOrder]:
        """Live orders (PENDING_NEW / RESTING / PENDING_CANCEL / PENDING_AMEND), oldest first."""
        return [self._snap(o) for o in self._orders if o.state in LIVE and (ticker is None or o.ticker == ticker)]

    def order(self, client_order_id: str) -> WorkingOrder | None:
        o = self._by_coid.get(client_order_id)
        return None if o is None else self._snap(o)

    def all_orders(self) -> list[WorkingOrder]:
        return [self._snap(o) for o in self._orders]

    def position(self, ticker: str) -> int:
        """Signed YES position from fill messages (0.01 contracts; + long YES, - long NO)."""
        return self._pos.get(ticker, 0)

    def exchange_position(self, ticker: str) -> int | None:
        """Last exchange-reported post_position (or reconciled position), if any."""
        return self._exch_pos.get(ticker)

    def open_qty(self, ticker: str, side: str) -> int:
        """Remaining working qty on 'bid' or 'ask' (live orders only)."""
        return sum(o.remaining for o in self._orders if o.ticker == ticker and o.book_side == side and o.state in LIVE)

    def worst_case_exposure(self, ticker: str, side: str) -> int:
        """Signed YES position if everything on ``side`` that could still fill does fill
        (resting + pending-new + pending-cancel + amend-up + fills in flight / unresolved).
        'bid' -> max long position, 'ask' -> min (most short) position. 0.01 contracts."""
        if side not in ("bid", "ask"):
            raise ValueError("side must be 'bid' or 'ask'")
        extra = sum(o.could_fill() for o in self._orders if o.ticker == ticker and o.book_side == side)
        pos = self.position(ticker)
        return pos + extra if side == "bid" else pos - extra

    def cash_micros(self, ticker: str | None = None) -> int:
        """Trade cash flow excluding fees (buying YES pays px, selling receives px), micros."""
        return self._cash.get(ticker, 0) if ticker is not None else sum(self._cash.values())

    def fees_micros(self, ticker: str | None = None) -> int:
        """Exchange fees from fill messages, micros (positive = paid)."""
        return self._fees.get(ticker, 0) if ticker is not None else sum(self._fees.values())

    def volume(self, ticker: str | None = None) -> int:
        return self._volume.get(ticker, 0) if ticker is not None else sum(self._volume.values())

    def settled_pnl_micros(self, ticker: str, settle_px: int) -> int:
        """Net P&L if the position settles at settle_px (10_000 = YES wins): cash - fees + pos*px."""
        return self.cash_micros(ticker) - self.fees_micros(ticker) + notional_micros(settle_px, self.position(ticker))

    def prune(self, before_ns: int) -> int:
        """Forget terminal, fully resolved orders last updated before ``before_ns``."""
        keep, dropped = [], 0
        for o in self._orders:
            if o.state in TERMINAL and not o.unresolved and o.inflight == 0 and o.updated_ns < before_ns:
                for c in [o.coid, *o.aliases]:
                    if self._by_coid.get(c) is o:
                        del self._by_coid[c]
                if o.order_id and self._by_oid.get(o.order_id) is o:
                    del self._by_oid[o.order_id]
                dropped += 1
            else:
                keep.append(o)
        self._orders = keep
        return dropped

    # ================================================================== internals
    def _lookup(self, coid: str, oid: str) -> _Order | None:
        o = self._by_coid.get(coid) if coid else None
        if o is None and oid:
            o = self._by_oid.get(oid)
        return o

    def _bind_oid(self, o: _Order, oid: str, ts: int, out: list[OrderEvent], attach: bool = True) -> None:
        if oid and not o.order_id:
            o.order_id = oid
            self._by_oid[oid] = o
        if attach:
            self._attach_orphans(o, out)

    def _attach_orphans(self, o: _Order, out: list[OrderEvent]) -> None:
        if not self._orphans:
            return
        keys = ([f"oid:{o.order_id}"] if o.order_id else []) + [f"coid:{c}" for c in (o.coid, *o.aliases)]
        for k in keys:
            fills = self._orphans.pop(k, None)
            for f in fills or ():
                out.extend(self._apply_fill(o, f))

    @staticmethod
    def _working_state(o: _Order) -> OrderState:
        """Non-terminal state implied by the facts (used when a pending request is refused)."""
        if o.cancel_requested:
            return OrderState.PENDING_CANCEL
        if o.amend is not None or o.decrease_to is not None:
            return OrderState.PENDING_AMEND
        if o.order_id or o.fill_sum:
            return OrderState.RESTING
        return OrderState.PENDING_NEW

    def _terminate(self, o: _Order, st: OrderState, ts: int) -> None:
        o.state = st
        o.updated_ns = ts
        o.unresolved = False
        o.amend = None
        o.decrease_to = None

    def _unknown(self, ts: int, coid: str, ticker: str, oid: str, detail: str) -> list[OrderEvent]:
        self.stats["unknown_events"] += 1
        return [OrderEvent(ts, "unknown_order", coid, ticker, oid, detail=detail)]

    def _ev(self, kind: str, o: _Order, ts: int, *, qty: int = 0, px: int | None = None, is_taker: bool = False,
            fee: int = 0, detail: str = "") -> OrderEvent:
        return OrderEvent(ts, kind, o.coid, o.ticker, o.order_id, o.state, o.book_side, o.px if px is None else px,
                          qty, o.filled, o.remaining, is_taker, fee, detail)

    def _snap(self, o: _Order) -> WorkingOrder:
        am = o.amend
        return WorkingOrder(o.coid, o.order_id, o.ticker, o.book_side, o.px, o.cap, o.filled, o.remaining, o.state,
                            o.created_ns, o.updated_ns, o.post_only, o.expiration_ts, o.group, o.unknown,
                            o.cancel_requested, am[1] if am else 0, am[2] if am else 0, o.inflight, o.unresolved)
