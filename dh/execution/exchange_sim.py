"""Queue-aware, latency-aware simulated Kalshi matching engine for backtests.

Clock: the simulator runs on the *recorded* timeline (every market event's local receive
``ts``). A recorded event at ``ts`` happened at the exchange at ``ts - md_offset``
(``LatencyModel.md_offset_ns``, 0 by default). Our action decided at ``d`` reaches the
matching engine at exchange time ``d + submit`` == sim time ``d + submit + md_offset``; at a
tie with a recorded event, the recorded event is processed first. Responses are delivered at
``exchange time + response`` (REST: acks/rejects/cancel acks) or ``+ ws`` (fills and order
updates, FIFO like one WebSocket), never earlier than the sim time that produced them.

The recorded book is never modified by our simulated orders (no market impact: other
participants do not see or react to us). Our marketable orders consume displayed liquidity
through a per-level "phantom consumption" ledger so the same displayed size cannot be taken
twice; resting orders get maker fills only through ``dh.execution.queue.QueueEstimator``:
  (i)   prints at our price beyond the queue ahead of us,
  (ii)  prints through our price (taker swept past our level; policy C only beyond its queue),
  (iii) A/B only: a new opposite level crossing our resting price (inferred aggressor).
See docs/EXECUTION_MODEL.md for the full model, the race conditions it reproduces and its
limitations.

Runner protocol (``dh.execution.driver.run_interleaved`` implements it):
    for ev in events (ts order):
        deliver sim.pop_due(ev.ts - 1) to the strategy   # our messages strictly before ev
        due = sim.on_market_event(ev)                    # exchange state advances to ev.ts
        strategy sees ev, then ``due`` (messages at exactly ev.ts)
        every action decided at time t -> sim.submit(action, t)
"""

from __future__ import annotations

import heapq
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Callable

from dh.core.actions import AmendOrder, CancelAll, CancelOrder, DecreaseOrder, PlaceOrder
from dh.core.book import KalshiBook
from dh.core.events import (
    CancelAck,
    KalshiBookDelta,
    KalshiBookSnapshot,
    KalshiFill,
    KalshiMarketLifecycle,
    KalshiOrderUpdate,
    KalshiTrade,
    OrderAck,
    OrderReject,
    Settlement,
)
from dh.core.market import MarketSpec
from dh.core.units import NS_PER_S, PX_SCALE
from dh.execution.latency import LatencyModel
from dh.execution.queue import QueueEstimator, QueueFill, normalize_policy

FeeFn = Callable[[int, int, bool], int]  # (yes_px, qty, is_taker) -> fee in micro-dollars
GROUP_WINDOW_NS = 15 * NS_PER_S
KALSHI_ACTIONS = (PlaceOrder, CancelOrder, AmendOrder, DecreaseOrder, CancelAll)


@dataclass(slots=True)
class _SimOrder:
    order_id: str
    coid: str
    ticker: str
    book_side: str  # 'bid' | 'ask'
    px: int  # YES price
    cap: int  # max fillable total (initial / amended)
    post_only: bool
    expiration_ns: int  # exchange time, whole seconds; 0 = GTC
    group: str
    cancel_on_pause: bool
    created_ns: int  # exchange time of arrival
    filled: int = 0
    status: str = "resting"  # resting | executed | canceled
    maker_fees: int = 0
    taker_fees: int = 0
    cancel_reason: str = ""
    done_ns: int = 0  # sim time the order left the book (executed or canceled)

    @property
    def remaining(self) -> int:
        return self.cap - self.filled if self.status == "resting" else 0


@dataclass(slots=True)
class _Market:
    ticker: str
    book: KalshiBook
    spec: MarketSpec | None = None
    close_ns: int = 0  # exchange time trading stops (0 = unknown)
    close_gen: int = 0
    closed: bool = False
    paused: bool = False
    consumed: dict[tuple[str, int], int] = field(default_factory=dict)  # our taker fills per level


@dataclass(slots=True)
class _Group:
    limit: int  # max matched qty per rolling 15 s (0.01 contracts)
    window: deque = field(default_factory=deque)  # (sim time, qty)
    matched: int = 0
    triggered: bool = False


@dataclass(frozen=True, slots=True)
class SimFillRecord:
    """One simulated fill, for diagnostics and markout studies."""

    sim_ts: int  # sim clock (recorded timeline) when matched
    ts_exch: int
    ticker: str
    client_order_id: str
    order_id: str
    book_side: str
    yes_px: int
    qty: int
    is_taker: bool
    fee_micros: int
    mechanism: str  # queue | sweep | cross | taker
    trade_id: str


class KalshiExchangeSim:
    """Simulated Kalshi exchange for our orders against recorded market data.

    Args:
        latency: LatencyModel (forked with ``seed``; the caller's model is not consumed).
        fill_policy: 'optimistic'|'realistic'|'conservative' (or 'A'|'B'|'C').
        fee_fn: (yes_px, qty, is_taker) -> fee micros for one fill.
        seed: forks the latency streams (same seed -> identical run).
        latency_multiplier: extra latency scale; default 1.5 for policy C, else 1.0.
        match_window_ns: trade-print <-> book-delta matching window (queue model).
        emit_order_updates: also emit KalshiOrderUpdate (user_orders) messages.
    """

    def __init__(self, latency: LatencyModel, fill_policy: str, fee_fn: FeeFn, seed: int = 0, *,
                 latency_multiplier: float | None = None, match_window_ns: int = 250_000_000,
                 emit_order_updates: bool = True, id_prefix: str = "sim") -> None:
        self.policy = normalize_policy(fill_policy)
        mult = latency_multiplier if latency_multiplier is not None else (1.5 if self.policy == "conservative" else 1.0)
        self.latency = latency.fork(seed, latency.multiplier * mult)
        self.fee_fn = fee_fn
        self.md_offset = self.latency.md_offset_ns
        self.emit_order_updates = emit_order_updates
        self.id_prefix = id_prefix
        self.queue = QueueEstimator(self.policy, self._level_qty, match_window_ns=match_window_ns)
        self.markets: dict[str, _Market] = {}
        self.orders: dict[str, _SimOrder] = {}  # order_id -> order (all, including done)
        self._resting: dict[str, _SimOrder] = {}  # order_id -> resting order (insertion ordered)
        self._coid: dict[str, _SimOrder] = {}  # every client_order_id ever accepted (incl. amend ids)
        self._chain: dict[str, str] = {}  # amend id -> original id (per-order FIFO of requests)
        self._last_arrival: dict[str, int] = {}
        self.groups: dict[str, _Group] = {}
        self.positions: dict[str, int] = {}
        self._xq: list[tuple[int, int, str, Any]] = []  # exchange-side items
        self._dq: list[tuple[int, int, Any]] = []  # deliveries to the strategy
        self._seq = 0
        self._now = 0
        self._ws_last = 0
        self._n_orders = 0
        self._n_fills = 0
        self.fill_log: list[SimFillRecord] = []
        self.stats: dict[str, int] = {"queue": 0, "sweep": 0, "cross": 0, "taker": 0, "orders": 0, "rejects": 0,
                                      "cancels": 0, "group_triggers": 0}

    # ================================================================== configuration
    def register_market(self, spec: MarketSpec) -> None:
        """Tick grid validation and scheduled close (also learned from lifecycle events)."""
        m = self._market(spec.ticker)
        m.spec = spec
        if spec.close_ts:
            self._set_close(m, spec.close_ts)

    def create_order_group(self, group_id: str, contracts_limit: int) -> None:
        """Kalshi order group: at most ``contracts_limit`` (0.01-contract units) matched per
        rolling 15 s; reaching it cancels every order of the group and blocks new ones until
        ``reset_order_group``. Applied immediately (no latency)."""
        if contracts_limit <= 0:
            raise ValueError("contracts_limit must be > 0")
        self.groups[group_id] = _Group(int(contracts_limit))

    def reset_order_group(self, group_id: str) -> None:
        g = self.groups[group_id]
        g.window.clear()
        g.matched = 0
        g.triggered = False

    def update_order_group_limit(self, group_id: str, contracts_limit: int) -> None:
        g = self.groups[group_id]
        g.limit = int(contracts_limit)
        self._roll(g, self._now)
        if g.matched >= g.limit:
            self._trigger_group(group_id, self._now)

    def delete_order_group(self, group_id: str) -> None:
        for o in list(self._resting.values()):
            if o.group == group_id:
                self._cancel(o, self._now, "order_group_deleted")
        self.groups.pop(group_id, None)

    # ================================================================== runner API
    def submit(self, action: Any, decision_ns: int) -> bool:
        """Send a strategy action; it reaches the matching engine after latency.

        Returns False for actions this venue does not handle (hedge orders, logs, halts)."""
        if not isinstance(action, KALSHI_ACTIONS):
            return False
        lat = self.latency.submit_ns() if isinstance(action, PlaceOrder) else self.latency.cancel_ns()
        resp = self.latency.response_ns()
        t = decision_ns + lat + self.md_offset
        key = getattr(action, "client_order_id", "")
        if key:
            root = self._chain.get(key, key)
            if isinstance(action, AmendOrder) and action.new_client_order_id != key:
                self._chain[action.new_client_order_id] = root
            t = max(t, self._last_arrival.get(root, -1) + 1)  # one order's requests stay in order
            self._last_arrival[root] = t
        self._push_x(t, "arrive", (action, decision_ns, resp))
        return True

    def next_due_ns(self) -> int | None:
        """Earliest pending simulator item (exchange-side processing or delivery), sim ns."""
        tx = self._xq[0][0] if self._xq else None
        td = self._dq[0][0] if self._dq else None
        if tx is None:
            return td
        return tx if td is None else min(tx, td)

    def pop_due(self, until_ns: int) -> list:
        """Process exchange-side items and return our messages delivered at ts <= until_ns,
        in delivery order (their ``ts`` is the time WE receive them)."""
        out = []
        xq, dq = self._xq, self._dq
        while True:
            tx = xq[0][0] if xq else None
            td = dq[0][0] if dq else None
            if tx is not None and tx <= until_ns and (td is None or tx <= td):
                t, _, kind, payload = heapq.heappop(xq)
                self._now = max(self._now, t)
                self._process_x(t, kind, payload)
            elif td is not None and td <= until_ns:
                out.append(heapq.heappop(dq)[2])
            else:
                return out

    def on_market_event(self, ev: Any) -> list:
        """Advance the exchange to ``ev.ts`` and apply a recorded market event.

        Returns our messages due at or before ``ev.ts`` (deliver them after ``ev``)."""
        ts = ev.ts
        while self._xq and self._xq[0][0] < ts:
            t, _, kind, payload = heapq.heappop(self._xq)
            self._now = max(self._now, t)
            self._process_x(t, kind, payload)
        self._now = max(self._now, ts)
        if isinstance(ev, KalshiBookDelta):
            m = self._market(ev.ticker)
            m.book.apply_delta(ev)
            key = (ev.side, ev.px)
            if key in m.consumed:
                lvl = self._level_qty(ev.ticker, ev.side, ev.px)
                if lvl < m.consumed[key]:
                    m.consumed[key] = lvl
            self._apply_queue_fills(m, self.queue.on_book_delta(ev), ts)
        elif isinstance(ev, KalshiTrade):
            m = self._market(ev.ticker)
            self._apply_queue_fills(m, self.queue.on_trade(ev), ts)
        elif isinstance(ev, KalshiBookSnapshot):
            m = self._market(ev.ticker)
            m.book.apply_snapshot(ev)
            m.consumed.clear()
            self.queue.on_snapshot(ev)
        elif isinstance(ev, KalshiMarketLifecycle):
            self._on_lifecycle(ev, ts)
        elif isinstance(ev, Settlement):
            self._close_market(self._market(ev.ticker), ts)
        return self.pop_due(ts)

    # ================================================================== introspection
    def position(self, ticker: str) -> int:
        """Simulated signed YES position (0.01 contracts)."""
        return self.positions.get(ticker, 0)

    def order_status(self, client_order_id: str) -> dict | None:
        """Matching-engine view of an order (status, filled, remaining, px, queue_ahead)."""
        o = self._coid.get(client_order_id)
        if o is None:
            return None
        return {"order_id": o.order_id, "client_order_id": o.coid, "status": o.status, "filled": o.filled,
                "remaining": o.remaining, "px": o.px, "book_side": o.book_side,
                "queue_ahead": self.queue.queue_ahead(o.order_id), "cancel_reason": o.cancel_reason,
                "done_ns": o.done_ns}

    def resting(self, ticker: str | None = None) -> list[dict]:
        return [self.order_status(o.coid) for o in self._resting.values() if ticker is None or o.ticker == ticker]

    def book(self, ticker: str) -> KalshiBook:
        return self._market(ticker).book

    # ================================================================== exchange internals
    def _exch(self, t: int) -> int:
        """Exchange time of sim time t."""
        return t - self.md_offset

    def _push_x(self, t: int, kind: str, payload: Any) -> None:
        self._seq += 1
        heapq.heappush(self._xq, (t, self._seq, kind, payload))

    def _deliver(self, d: int, ev: Any) -> None:
        self._seq += 1
        heapq.heappush(self._dq, (d, self._seq, ev))

    def _rest_time(self, t: int, resp: int) -> int:
        return max(self._exch(t) + resp, t)

    def _ws_time(self, t: int) -> int:
        d = max(self._exch(t) + self.latency.ws_ns(), t, self._ws_last)
        self._ws_last = d
        return d

    def _market(self, ticker: str) -> _Market:
        m = self.markets.get(ticker)
        if m is None:
            m = self.markets[ticker] = _Market(ticker, KalshiBook(ticker))
        return m

    def _level_qty(self, ticker: str, book: str, px: int) -> int:
        m = self.markets.get(ticker)
        if m is None:
            return 0
        levels = m.book.yes_bids if book == "yes" else m.book.no_bids
        return levels.get(px, 0)

    def _open(self, m: _Market, t: int) -> bool:
        return not m.closed and (m.close_ns == 0 or self._exch(t) < m.close_ns)

    def _find(self, coid: str, oid: str) -> _SimOrder | None:
        o = self._coid.get(coid) if coid else None
        if o is None and oid:
            o = self.orders.get(oid)
        return o

    def _process_x(self, t: int, kind: str, payload: Any) -> None:
        if kind == "arrive":
            a, _decision, resp = payload
            if isinstance(a, PlaceOrder):
                self._arrive_place(a, t, resp)
            elif isinstance(a, CancelOrder):
                self._arrive_cancel(a, t, resp)
            elif isinstance(a, AmendOrder):
                self._arrive_amend(a, t, resp)
            elif isinstance(a, DecreaseOrder):
                self._arrive_decrease(a, t, resp)
            elif isinstance(a, CancelAll):
                for o in list(self._resting.values()):
                    if not a.tickers or o.ticker in a.tickers:
                        self._cancel(o, t, "cancel_all")
        elif kind == "expire":
            oid, exp = payload
            o = self.orders.get(oid)
            if o is not None and o.status == "resting" and o.expiration_ns == exp:
                self._cancel(o, t, "expired")
        elif kind == "close":
            ticker, gen = payload
            m = self.markets[ticker]
            if gen == m.close_gen:
                self._close_market(m, t)

    def _reject(self, t: int, resp: int, coid: str, ticker: str, reason: str, request: str, status: int = 400) -> None:
        self.stats["rejects"] += 1
        self._deliver(self._rest_time(t, resp), OrderReject(self._rest_time(t, resp), self._exch(t), coid, ticker,
                                                            reason, status, request))

    def _update_msg(self, o: _SimOrder, t: int) -> None:
        if self.emit_order_updates:
            d = self._ws_time(t)
            self._deliver(d, KalshiOrderUpdate(d, self._exch(t), o.ticker, o.order_id, o.coid, o.status, o.book_side,
                                               o.px, o.cap, o.filled, o.remaining, o.maker_fees, o.taker_fees))

    # ------------------------------------------------------------------ arrivals
    def _validate_place(self, a: PlaceOrder, m: _Market, t: int) -> str:
        if a.client_order_id in self._coid:
            return "duplicate_client_order_id"
        if a.book_side not in ("bid", "ask"):
            return "invalid_side"
        if a.qty <= 0:
            return "invalid_count"
        if not 0 < a.px < PX_SCALE or (m.spec is not None and not m.spec.is_valid_px(a.px)):
            return "invalid_price"
        if not self._open(m, t):
            return "market_closed"
        if m.paused:
            return "market_paused"
        if a.order_group_id:
            g = self.groups.get(a.order_group_id)
            if g is None:
                return "order_group_not_found"
            if g.triggered:
                return "order_group_triggered"
        if a.expiration_ts and (a.expiration_ts // NS_PER_S) * NS_PER_S <= self._exch(t):
            return "invalid_expiration"
        if a.post_only and self._would_cross(m, a.book_side, a.px):
            return "post_only_cross"
        return ""

    def _arrive_place(self, a: PlaceOrder, t: int, resp: int) -> None:
        m = self._market(a.ticker)
        reason = self._validate_place(a, m, t)
        if reason:
            self._reject(t, resp, a.client_order_id, a.ticker, reason, "create",
                         409 if reason == "duplicate_client_order_id" else 400)
            return
        self._n_orders += 1
        self.stats["orders"] += 1
        exp = (a.expiration_ts // NS_PER_S) * NS_PER_S if a.expiration_ts else 0
        o = _SimOrder(f"{self.id_prefix}-o{self._n_orders}", a.client_order_id, a.ticker, a.book_side, a.px, a.qty,
                      a.post_only, exp, a.order_group_id, a.cancel_on_pause, self._exch(t))
        self.orders[o.order_id] = o
        self._coid[o.coid] = o
        taker = 0
        if not a.post_only:
            taker = self._take(o, m, t)
        if o.status == "resting" and o.remaining > 0:
            g = self.groups.get(o.group) if o.group else None
            if g is not None and g.triggered:
                self._cancel(o, t, "order_group_triggered", notify=False)
            else:
                self._resting[o.order_id] = o
                self.queue.add_order(o.order_id, o.ticker, o.book_side, o.px, o.remaining, t)
                if exp:
                    self._push_x(max(t, exp + self.md_offset), "expire", (o.order_id, exp))
        d = self._rest_time(t, resp)
        self._deliver(d, OrderAck(d, self._exch(t), o.coid, o.order_id, o.ticker, taker, o.remaining, "create"))
        self._update_msg(o, t)

    def _arrive_cancel(self, a: CancelOrder, t: int, resp: int) -> None:
        o = self._find(a.client_order_id, a.order_id)
        if o is None:
            self._reject(t, resp, a.client_order_id, a.ticker, "not_found", "cancel", 404)
        elif o.status == "executed":
            self._reject(t, resp, a.client_order_id, a.ticker, "already_filled", "cancel", 404)
        elif o.status == "canceled":
            self._reject(t, resp, a.client_order_id, a.ticker, "already_canceled", "cancel", 404)
        else:
            qty = self._cancel(o, t, "user", notify=False)
            d = self._rest_time(t, resp)
            self._deliver(d, CancelAck(d, self._exch(t), o.coid, o.order_id, o.ticker, qty))
            self._update_msg(o, t)

    def _arrive_amend(self, a: AmendOrder, t: int, resp: int) -> None:
        o = self._find(a.client_order_id, a.order_id)
        m = self._market(a.ticker)
        reason = ""
        if o is None:
            reason = "not_found"
        elif o.status == "executed":
            reason = "already_filled"
        elif o.status == "canceled":
            reason = "already_canceled"
        elif a.book_side != o.book_side:
            reason = "invalid_side"
        elif a.total_qty <= o.filled:
            reason = "invalid_count"
        elif not 0 < a.px < PX_SCALE or (m.spec is not None and not m.spec.is_valid_px(a.px)):
            reason = "invalid_price"
        elif a.new_client_order_id != o.coid and a.new_client_order_id in self._coid:
            reason = "duplicate_client_order_id"
        elif a.px != o.px and self._would_cross(m, o.book_side, a.px):
            reason = "post_only_cross" if o.post_only else "amend_would_cross"
        if reason or o is None:
            self._reject(t, resp, a.client_order_id, a.ticker, reason, "amend", 404 if "not_found" in reason else 400)
            return
        new_rem = a.total_qty - o.filled
        lose_priority = a.px != o.px or new_rem > o.remaining  # Kalshi: only size decreases keep priority
        o.px, o.cap = a.px, a.total_qty
        if a.new_client_order_id != o.coid:
            o.coid = a.new_client_order_id
            self._coid[o.coid] = o
        if lose_priority:
            self.queue.requeue(o.order_id, o.ticker, o.book_side, o.px, new_rem, t)
        else:
            self.queue.reduce(o.order_id, new_rem)
        d = self._rest_time(t, resp)
        self._deliver(d, OrderAck(d, self._exch(t), o.coid, o.order_id, o.ticker, 0, o.remaining, "amend"))
        self._update_msg(o, t)

    def _arrive_decrease(self, a: DecreaseOrder, t: int, resp: int) -> None:
        o = self._find(a.client_order_id, a.order_id)
        if o is None or o.status != "resting":
            reason = "not_found" if o is None else ("already_filled" if o.status == "executed" else "already_canceled")
            self._reject(t, resp, a.client_order_id, a.ticker, reason, "decrease", 404)
            return
        new_rem = max(0, min(o.remaining, a.reduce_to))
        if new_rem == 0:
            self._cancel(o, t, "decrease", notify=False)
        else:
            o.cap = o.filled + new_rem
            self.queue.reduce(o.order_id, new_rem)
        d = self._rest_time(t, resp)
        self._deliver(d, OrderAck(d, self._exch(t), o.coid, o.order_id, o.ticker, 0, o.remaining, "decrease"))
        self._update_msg(o, t)

    # ------------------------------------------------------------------ matching
    def _own_best(self, ticker: str, book_side: str) -> int | None:
        """Best price of our own resting orders on book_side (YES scale)."""
        pxs = [o.px for o in self._resting.values() if o.ticker == ticker and o.book_side == book_side]
        if not pxs:
            return None
        return max(pxs) if book_side == "bid" else min(pxs)

    def _available_best(self, m: _Market, book: str) -> int | None:
        """Best px on bid book 'yes'|'no' with displayed qty not consumed by our taker fills."""
        levels = m.book.yes_bids if book == "yes" else m.book.no_bids
        for px in reversed(levels.keys()):
            if levels[px] - m.consumed.get((book, px), 0) > 0:
                return px
        return None

    def _would_cross(self, m: _Market, book_side: str, px: int) -> bool:
        """Would an order at YES px on book_side take liquidity (displayed or our own)?"""
        if book_side == "bid":
            no_best = self._available_best(m, "no")
            own = self._own_best(m.ticker, "ask")
            return (no_best is not None and PX_SCALE - no_best <= px) or (own is not None and own <= px)
        yes_best = self._available_best(m, "yes")
        own = self._own_best(m.ticker, "bid")
        return (yes_best is not None and yes_best >= px) or (own is not None and own >= px)

    def _take(self, o: _SimOrder, m: _Market, t: int) -> int:
        """Marketable part of a non-post-only order: walk displayed opposite levels up to the
        limit (taker fees, maker's price). Self-trade prevention (taker_at_cross): stop before
        reaching our own resting opposite order and cancel the remainder. Returns qty taken."""
        if not self._open(m, t) or m.paused:
            return 0
        opp = "no" if o.book_side == "bid" else "yes"
        levels = m.book.no_bids if opp == "no" else m.book.yes_bids
        thr = PX_SCALE - o.px if o.book_side == "bid" else o.px  # opposite-book px must be >= thr
        own = self._own_best(o.ticker, "ask" if o.book_side == "bid" else "bid")
        own_opp = None if own is None else (PX_SCALE - own if opp == "no" else own)
        taken = 0
        stp = False
        for px in list(reversed(levels.keys())):
            if px < thr or o.remaining <= 0:
                break
            if own_opp is not None and px <= own_opp:
                stp = True
                break
            avail = levels[px] - m.consumed.get((opp, px), 0)
            if avail <= 0:
                continue
            want = min(avail, o.remaining)
            got = self._fill(o, want, PX_SCALE - px if opp == "no" else px, True, t, "taker")
            m.consumed[(opp, px)] = m.consumed.get((opp, px), 0) + got
            taken += got
            if got < want:
                break
        if stp and o.status == "resting" and o.remaining > 0:
            self._cancel(o, t, "self_trade_prevention", notify=False)
        return taken

    def _apply_queue_fills(self, m: _Market, fills: list[QueueFill], t: int) -> None:
        if not fills or not self._open(m, t) or m.paused:
            return
        for oid, qty, mech in fills:
            o = self._resting.get(oid)
            if o is not None:
                self._fill(o, qty, o.px, False, t, mech)

    def _fill(self, o: _SimOrder, qty: int, px: int, is_taker: bool, t: int, mech: str) -> int:
        g = self.groups.get(o.group) if o.group else None
        if g is not None:
            self._roll(g, t)
            if g.triggered:
                return 0
            qty = min(qty, g.limit - g.matched)
        qty = min(qty, o.remaining)
        if qty <= 0:
            return 0
        o.filled += qty
        fee = int(self.fee_fn(px, qty, is_taker))
        if is_taker:
            o.taker_fees += fee
        else:
            o.maker_fees += fee
            self.queue.on_own_fill(o.order_id, qty)
        pos = self.positions.get(o.ticker, 0) + (qty if o.book_side == "bid" else -qty)
        self.positions[o.ticker] = pos
        if o.filled >= o.cap:
            o.status = "executed"
            o.done_ns = t
            self._resting.pop(o.order_id, None)
            self.queue.remove_order(o.order_id)
        self._n_fills += 1
        trade_id = f"{self.id_prefix}-f{self._n_fills}"
        self.stats[mech] += qty
        self.fill_log.append(SimFillRecord(t, self._exch(t), o.ticker, o.coid, o.order_id, o.book_side, px, qty,
                                           is_taker, fee, mech, trade_id))
        d = self._ws_time(t)
        self._deliver(d, KalshiFill(d, self._exch(t), o.ticker, trade_id, o.order_id, o.coid, o.book_side, px, qty,
                                    is_taker, fee, pos, True))
        if not is_taker:
            self._update_msg(o, t)  # taker fills: the create path sends one update after matching
        if g is not None:
            g.window.append((t, qty))
            g.matched += qty
            if g.matched >= g.limit:
                self._trigger_group(o.group, t)
        return qty

    def _cancel(self, o: _SimOrder, t: int, reason: str, *, notify: bool = True) -> int:
        """Remove an order from the book; returns the canceled (remaining) qty."""
        qty = o.remaining
        o.status = "canceled"
        o.cancel_reason = reason
        o.done_ns = t
        self._resting.pop(o.order_id, None)
        self.queue.remove_order(o.order_id)
        self.stats["cancels"] += 1
        if notify:
            self._update_msg(o, t)
        return qty

    # ------------------------------------------------------------------ groups / lifecycle
    def _roll(self, g: _Group, t: int) -> None:
        w = g.window
        while w and w[0][0] <= t - GROUP_WINDOW_NS:
            g.matched -= w.popleft()[1]

    def _trigger_group(self, group_id: str, t: int) -> None:
        g = self.groups[group_id]
        if g.triggered:
            return
        g.triggered = True
        self.stats["group_triggers"] += 1
        for o in list(self._resting.values()):
            if o.group == group_id:
                self._cancel(o, t, "order_group_triggered")

    def _set_close(self, m: _Market, close_ns: int) -> None:
        m.close_ns = close_ns
        m.close_gen += 1
        self._push_x(max(close_ns + self.md_offset, self._now), "close", (m.ticker, m.close_gen))

    def _close_market(self, m: _Market, t: int) -> None:
        m.closed = True
        for o in list(self._resting.values()):
            if o.ticker == m.ticker:
                self._cancel(o, t, "market_closed")

    def _on_lifecycle(self, ev: KalshiMarketLifecycle, t: int) -> None:
        m = self._market(ev.ticker)
        et = ev.event_type
        if ev.close_ts and et in ("created", "close_date_updated"):
            self._set_close(m, ev.close_ts)
        if et == "deactivated" or ev.is_deactivated is True:
            m.paused = True
            for o in list(self._resting.values()):
                if o.ticker == m.ticker and o.cancel_on_pause:
                    self._cancel(o, t, "market_paused")
        elif et == "activated" or ev.is_deactivated is False:
            m.paused = False
        if et in ("determined", "settled"):
            self._close_market(m, t)
