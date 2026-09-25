"""Simulated hedge venue (BTC perp or spot) driven by recorded external L2 + trades.

Units: prices in USD (float), sizes in BTC (float), fees in USD. Fees are ``bps`` of notional.

* Market orders (``order_type='market'``) walk the displayed book at their arrival time
  (decision + submit latency): partial fills when depth is insufficient, remainder canceled
  (IOC). Marketable non-post-only limit orders take up to their limit, the rest rests.
* Post-only limit orders that would cross are rejected; resting orders join behind the
  displayed size at their price and fill on trade prints at their price (beyond the queue
  ahead) or through it (price priority; policy C only once its queue is exhausted), with the
  same blocking rules as the Kalshi queue model (dh.execution.queue).
* Size decreases at our level not explained by prints move the queue by policy
  (A all ahead, B pro-rata, C behind us). Unlike the Kalshi estimator, prints are matched only
  to *later* decreases (trade-first), which is how most venues sequence their feeds.
* Our taker fills consume displayed liquidity through a phantom-consumption ledger (never
  modify the recorded book). Every execution records slippage vs the mid at decision time
  and at arrival (``slippage_log``).
* Funding: ``PerpState`` events carry ``funding_rate`` (per interval) and ``next_funding_ts``;
  at each funding time the position pays ``position_btc * mark * rate`` (longs pay a positive
  rate) into ``funding_usd`` and the optional ``on_funding(ts, usd)`` hook.
"""

from __future__ import annotations

import heapq
from collections import deque
from dataclasses import dataclass
from typing import Any, Callable

from dh.core.actions import CancelHedge, PlaceHedge
from dh.core.book import ExtBook
from dh.core.events import ExtBookDelta, ExtBookSnapshot, ExtTrade, HedgeFill, HedgeOrderUpdate, PerpState
from dh.execution.latency import LatencyModel
from dh.execution.queue import normalize_policy

_EPS = 1e-12


def cancel_update_float(q: float, level_before: float, vol: float, policy: str) -> float:
    """Float version of dh.execution.queue.cancel_update (sizes in BTC)."""
    if vol <= 0:
        return q
    after = max(0.0, level_before - vol)
    if policy == "optimistic":
        q2 = q - vol
    elif policy == "realistic":
        q2 = q - vol * (q / max(level_before, q, _EPS))
    else:
        q2 = q
    return max(0.0, min(q2, after))


@dataclass(slots=True)
class _HOrder:
    coid: str
    side: str  # buy | sell
    qty: float
    order_type: str
    limit_px: float
    post_only: bool
    decision_mid: float | None
    filled: float = 0.0
    status: str = "pending"  # pending | resting | filled | canceled | rejected
    queue_ahead: float = 0.0
    seq: int = 0

    @property
    def remaining(self) -> float:
        return max(0.0, self.qty - self.filled) if self.status in ("pending", "resting") else 0.0


@dataclass(frozen=True, slots=True)
class SlippageRecord:
    ts: int  # sim time of execution
    client_order_id: str
    side: str
    qty_requested: float
    qty_filled: float
    vwap: float
    decision_mid: float | None
    arrival_mid: float | None
    arrival_touch: float | None  # best ask for buys, best bid for sells
    slippage_bps_vs_decision: float | None  # signed cost: + = paid more than decision mid
    levels: int


class HedgeVenueSim:
    """Latency-aware simulated hedge venue for one (venue, symbol).

    Args:
        venue: venue id matching ``Ext*``/``PerpState`` events and ``PlaceHedge.venue``.
        maker_bps / taker_bps: fees in basis points of notional.
        latency: LatencyModel (forked with ``seed``).
        symbol: restrict to one symbol (None = any symbol of the venue).
        fill_policy: queue policy for resting orders ('A'|'B'|'C' or names).
    Runner protocol identical to KalshiExchangeSim (submit / on_market_event / pop_due).
    """

    def __init__(self, venue: str, maker_bps: float, taker_bps: float, latency: LatencyModel, seed: int = 0, *,
                 symbol: str | None = None, fill_policy: str = "realistic", latency_multiplier: float | None = None,
                 on_funding: Callable[[int, float], None] | None = None) -> None:
        self.venue, self.symbol = venue, symbol
        self.maker_bps, self.taker_bps = float(maker_bps), float(taker_bps)
        self.policy = normalize_policy(fill_policy)
        mult = latency_multiplier if latency_multiplier is not None else (1.5 if self.policy == "conservative" else 1.0)
        self.latency = latency.fork(seed, latency.multiplier * mult)
        self.md_offset = self.latency.md_offset_ns
        self.book = ExtBook(venue, symbol or "")
        self.orders: dict[str, _HOrder] = {}
        self._resting: dict[str, _HOrder] = {}
        self._consumed: dict[tuple[str, float], float] = {}
        self._pend_trade: dict[tuple[str, float], deque[list[float]]] = {}
        self.match_window_ns = 250_000_000
        self.position_btc = 0.0
        self.cash_usd = 0.0
        self.fees_usd = 0.0
        self.funding_usd = 0.0
        self.mark = 0.0
        self._funding_rate = 0.0
        self._funding_next = 0
        self.on_funding = on_funding
        self.slippage_log: list[SlippageRecord] = []
        self.funding_log: list[tuple[int, float, float, float]] = []  # (ts, rate, mark, usd)
        self._xq: list[tuple[int, int, str, Any]] = []
        self._dq: list[tuple[int, int, Any]] = []
        self._seq = 0
        self._now = 0
        self._ws_last = 0
        self._oseq = 0

    # ================================================================== runner API
    def _mine(self, ev: Any) -> bool:
        return getattr(ev, "venue", None) == self.venue and (self.symbol is None or getattr(ev, "symbol", self.symbol) == self.symbol)

    def submit(self, action: Any, decision_ns: int) -> bool:
        """PlaceHedge / CancelHedge for this venue; False for anything else."""
        if not isinstance(action, (PlaceHedge, CancelHedge)) or action.venue != self.venue:
            return False
        if isinstance(action, PlaceHedge) and self.symbol is not None and action.symbol != self.symbol:
            return False
        lat = self.latency.submit_ns() if isinstance(action, PlaceHedge) else self.latency.cancel_ns()
        resp = self.latency.response_ns()
        top = self.book.top()
        self._push_x(decision_ns + lat + self.md_offset, "arrive", (action, None if top is None else top.mid, resp))
        return True

    def next_due_ns(self) -> int | None:
        tx = self._xq[0][0] if self._xq else None
        td = self._dq[0][0] if self._dq else None
        if tx is None:
            return td
        return tx if td is None else min(tx, td)

    def pop_due(self, until_ns: int) -> list:
        out = []
        while True:
            tx = self._xq[0][0] if self._xq else None
            td = self._dq[0][0] if self._dq else None
            if tx is not None and tx <= until_ns and (td is None or tx <= td):
                t, _, kind, payload = heapq.heappop(self._xq)
                self._now = max(self._now, t)
                self._process_x(t, kind, payload)
            elif td is not None and td <= until_ns:
                out.append(heapq.heappop(self._dq)[2])
            else:
                return out

    def on_market_event(self, ev: Any) -> list:
        ts = ev.ts
        while self._xq and self._xq[0][0] < ts:
            t, _, kind, payload = heapq.heappop(self._xq)
            self._now = max(self._now, t)
            self._process_x(t, kind, payload)
        self._now = max(self._now, ts)
        if self._mine(ev):
            if isinstance(ev, ExtBookSnapshot):
                self.book.snapshot(ev.bids, ev.asks, ev.ts, ev.seq)
                self._consumed.clear()
                self._pend_trade.clear()
                for o in self._resting.values():
                    o.queue_ahead = min(o.queue_ahead, self._level(o.side, o.limit_px))
            elif isinstance(ev, ExtBookDelta):
                for side, price, size in ev.changes:
                    self._on_level(side, price, size, ts)
            elif isinstance(ev, ExtTrade):
                self._on_trade(ev, ts)
            elif isinstance(ev, PerpState):
                self._on_perp(ev, ts)
        return self.pop_due(ts)

    # ================================================================== state views
    def unrealized_usd(self, mark: float | None = None) -> float:
        """Cash + position marked at ``mark`` (default: last mark or book mid), before fees/funding."""
        px = mark if mark is not None else (self.mark or (self.book.top().mid if self.book.top() else 0.0))
        return self.cash_usd + self.position_btc * px

    def pnl_usd(self, mark: float | None = None) -> float:
        """Total hedge P&L: marked position + cash - fees + funding."""
        return self.unrealized_usd(mark) - self.fees_usd + self.funding_usd

    # ================================================================== internals
    def _push_x(self, t: int, kind: str, payload: Any) -> None:
        self._seq += 1
        heapq.heappush(self._xq, (t, self._seq, kind, payload))

    def _deliver(self, d: int, ev: Any) -> None:
        self._seq += 1
        heapq.heappush(self._dq, (d, self._seq, ev))

    def _exch(self, t: int) -> int:
        return t - self.md_offset

    def _rest_time(self, t: int, resp: int) -> int:
        return max(self._exch(t) + resp, t)

    def _ws_time(self, t: int) -> int:
        d = max(self._exch(t) + self.latency.ws_ns(), t, self._ws_last)
        self._ws_last = d
        return d

    def _book_side(self, order_side: str) -> str:
        return "b" if order_side == "buy" else "a"

    def _level(self, order_side: str, price: float) -> float:
        levels = self.book.bids if order_side == "buy" else self.book.asks
        return levels.get(price, 0.0)

    def _update(self, o: _HOrder, t: int, status: str, reason: str = "", *, rest: int | None = None) -> None:
        d = self._rest_time(t, rest) if rest is not None else self._ws_time(t)
        self._deliver(d, HedgeOrderUpdate(d, self._exch(t), self.venue, o.coid, status, o.filled, o.remaining, reason))

    def _process_x(self, t: int, kind: str, payload: Any) -> None:
        if kind == "arrive":
            a, decision_mid, resp = payload
            if isinstance(a, PlaceHedge):
                self._arrive_place(a, decision_mid, t, resp)
            else:
                self._arrive_cancel(a, t, resp)
        elif kind == "funding":
            self._apply_funding(t, payload)

    def _arrive_place(self, a: PlaceHedge, decision_mid: float | None, t: int, resp: int) -> None:
        self._oseq += 1
        o = _HOrder(a.client_order_id, a.side, float(a.qty_btc), a.order_type, float(a.limit_px), a.post_only,
                    decision_mid, seq=self._oseq)
        reason = ""
        if a.client_order_id in self.orders:
            reason = "duplicate_client_order_id"
        elif a.side not in ("buy", "sell") or o.qty <= 0:
            reason = "invalid_order"
        elif a.order_type == "limit" and o.limit_px <= 0:
            reason = "invalid_price"
        elif a.reduce_only:
            reducible = -self.position_btc if a.side == "buy" else self.position_btc
            if reducible <= _EPS:
                reason = "reduce_only"
            else:
                o.qty = min(o.qty, reducible)
        if not reason and a.order_type == "limit" and a.post_only and self._crosses(o):
            reason = "post_only_cross"
        if reason:
            o.status = "rejected"
            if a.client_order_id not in self.orders:
                self.orders[o.coid] = o
            self._update(o, t, "rejected", reason, rest=resp)
            return
        self.orders[o.coid] = o
        o.status = "pending"
        taken = self._take(o, t)
        if o.remaining <= _EPS:
            o.status = "filled"
            self._update(o, t, "filled", rest=resp)
        elif a.order_type == "market":
            o.status = "canceled"  # IOC remainder
            self._update(o, t, "canceled", "insufficient_depth", rest=resp)
        else:
            o.status = "resting"
            o.queue_ahead = self._level(o.side, o.limit_px)
            self._resting[o.coid] = o
            self._update(o, t, "partially_filled" if taken > _EPS else "accepted", rest=resp)

    def _arrive_cancel(self, a: CancelHedge, t: int, resp: int) -> None:
        o = self.orders.get(a.client_order_id)
        if o is None or o.status != "resting":
            reason = "not_found" if o is None else ("already_filled" if o.status == "filled" else o.status)
            d = self._rest_time(t, resp)
            rem = 0.0 if o is None else o.remaining
            self._deliver(d, HedgeOrderUpdate(d, self._exch(t), self.venue, a.client_order_id, "rejected",
                                              0.0 if o is None else o.filled, rem, reason))
            return
        o.status = "canceled"
        self._resting.pop(o.coid, None)
        self._update(o, t, "canceled", rest=resp)

    def _crosses(self, o: _HOrder) -> bool:
        if o.side == "buy":
            best = self._best_available("a")
            return best is not None and best <= o.limit_px
        best = self._best_available("b")
        return best is not None and best >= o.limit_px

    def _best_available(self, side: str) -> float | None:
        items = self.book.asks.items() if side == "a" else reversed(self.book.bids.items())
        for p, s in items:
            if s - self._consumed.get((side, p), 0.0) > _EPS:
                return p
        return None

    def _take(self, o: _HOrder, t: int) -> float:
        """Walk displayed liquidity (asks for buys, bids for sells) up to the limit."""
        side = "a" if o.side == "buy" else "b"
        levels = list(self.book.asks.items()) if side == "a" else list(reversed(self.book.bids.items()))
        top = self.book.top()
        arrival_mid = None if top is None else top.mid
        touch = None if not levels else levels[0][0]
        taken, cost, n = 0.0, 0.0, 0
        for p, s in levels:
            if o.remaining <= _EPS:
                break
            if o.order_type == "limit" and ((side == "a" and p > o.limit_px) or (side == "b" and p < o.limit_px)):
                break
            avail = s - self._consumed.get((side, p), 0.0)
            if avail <= _EPS:
                continue
            q = min(avail, o.remaining)
            self._consumed[(side, p)] = self._consumed.get((side, p), 0.0) + q
            self._execute(o, q, p, False, t)
            taken += q
            cost += q * p
            n += 1
        if taken > _EPS:
            vwap = cost / taken
            slip = None
            if o.decision_mid:
                sign = 1.0 if o.side == "buy" else -1.0
                slip = sign * (vwap - o.decision_mid) / o.decision_mid * 1e4
            self.slippage_log.append(SlippageRecord(t, o.coid, o.side, o.qty, taken, vwap, o.decision_mid, arrival_mid,
                                                    touch, slip, n))
        return taken

    def _execute(self, o: _HOrder, q: float, p: float, is_maker: bool, t: int) -> None:
        o.filled += q
        fee = p * q * (self.maker_bps if is_maker else self.taker_bps) / 1e4
        sign = 1.0 if o.side == "buy" else -1.0
        self.position_btc += sign * q
        self.cash_usd -= sign * q * p
        self.fees_usd += fee
        d = self._ws_time(t)
        self._deliver(d, HedgeFill(d, self._exch(t), self.venue, self.symbol or "", o.coid, o.side, q, p, fee, is_maker))

    def _on_level(self, side: str, price: float, size: float, ts: int) -> None:
        old = (self.book.bids if side == "b" else self.book.asks).get(price, 0.0)
        self.book.update(side, price, size)
        key = (side, price)
        if key in self._consumed and size < self._consumed[key]:
            self._consumed[key] = size
        dec = old - size
        if dec <= _EPS:
            return
        explained = self._consume_trade(key, dec, ts)
        rest = dec - explained
        if rest <= _EPS:
            return
        for o in self._resting.values():
            if self._book_side(o.side) == side and o.limit_px == price:
                o.queue_ahead = cancel_update_float(o.queue_ahead, old, rest, self.policy)

    def _consume_trade(self, key: tuple[str, float], vol: float, ts: int) -> float:
        dq = self._pend_trade.get(key)
        used = 0.0
        while dq and used < vol - _EPS:
            it = dq[0]
            if it[0] < ts - self.match_window_ns:
                dq.popleft()
                continue
            take = min(it[1], vol - used)
            it[1] -= take
            used += take
            if it[1] <= _EPS:
                dq.popleft()
        return used

    def _on_trade(self, tr: ExtTrade, ts: int) -> None:
        agg = tr.aggressor
        if agg not in ("buy", "sell"):
            top = self.book.top()
            if top is None:
                return
            agg = "sell" if tr.price <= top.bid else ("buy" if tr.price >= top.ask else "")
            if not agg:
                return
        hit_side = "b" if agg == "sell" else "a"  # resting side that traded
        self._pend_trade.setdefault((hit_side, tr.price), deque()).append([ts, tr.size])
        our_side = "buy" if hit_side == "b" else "sell"
        cands = [o for o in self._resting.values() if o.side == our_side and
                 (o.limit_px >= tr.price if our_side == "buy" else o.limit_px <= tr.price)]
        if not cands:
            return
        # price-time priority: best price first, then arrival
        cands.sort(key=lambda o: (-o.limit_px if our_side == "buy" else o.limit_px, o.seq))
        conservative = self.policy == "conservative"
        fills: list[tuple[_HOrder, float]] = []
        blocked, i = 0.0, 0
        while i < len(cands):
            px = cands[i].limit_px
            j = i
            same, level_q = 0.0, 0.0
            at_price = px == tr.price
            while j < len(cands) and cands[j].limit_px == px:
                o = cands[j]
                q = o.queue_ahead if (at_price or conservative) else 0.0
                f = min(o.remaining, max(0.0, tr.size - (blocked + q + same)))
                if f > _EPS:
                    fills.append((o, f))
                same += o.remaining
                level_q = max(level_q, q)
                j += 1
            blocked += level_q + same
            i = j
        for o in cands:
            if o.limit_px == tr.price:
                o.queue_ahead = max(0.0, o.queue_ahead - tr.size)
            elif not conservative:
                o.queue_ahead = 0.0
        for o, f in fills:
            self._execute(o, f, o.limit_px, True, ts)
            if o.remaining <= _EPS:
                o.status = "filled"
                self._resting.pop(o.coid, None)
                self._update(o, ts, "filled")
            else:
                self._update(o, ts, "partially_filled")

    def _on_perp(self, ev: PerpState, ts: int) -> None:
        if ev.mark:
            self.mark = ev.mark
        self._funding_rate = ev.funding_rate
        if ev.next_funding_ts and ev.next_funding_ts != self._funding_next:
            self._funding_next = ev.next_funding_ts
            self._push_x(max(ev.next_funding_ts + self.md_offset, ts), "funding", ev.next_funding_ts)

    def _apply_funding(self, t: int, funding_ts: int) -> None:
        mark = self.mark or (self.book.top().mid if self.book.top() else 0.0)
        usd = -self.position_btc * mark * self._funding_rate
        self.funding_usd += usd
        self.funding_log.append((t, self._funding_rate, mark, usd))
        if self.on_funding is not None:
            self.on_funding(t, usd)
