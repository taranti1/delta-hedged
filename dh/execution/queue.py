"""Queue-position estimation for OUR resting Kalshi orders (exact integer qty units).

Model (docs/EXECUTION_MODEL.md has the full statement and the proofs of policy ordering):

* Kalshi has two *bid* books: YES bids and NO bids. Our YES bid at p rests in the YES book at
  p; our YES ask at p rests in the NO book at NO price 1 - p. On both books a higher price is
  better, so "px >= p" means "at or better than p" everywhere in this module.
* A public trade with taker_outcome_side 'no' (taker_book_side 'ask') sells YES into the YES
  book at yes_px; 'yes' (taker_book_side 'bid') buys YES from the NO book at NO px 1 - yes_px.
* On arrival we join behind everything displayed at our price: ``queue_ahead = displayed level
  qty`` (minus trade prints whose book delta has not arrived yet).
* Trades at our price deplete the queue ahead of us FIFO: ``q -= traded qty``.
* Kalshi publishes BOTH a trade message and an orderbook_delta for one match. Trades and
  negative deltas at the same (ticker, book, px) are matched within ``match_window_ns``,
  whichever arrives first, so a match is never counted twice. A negative delta not explained
  by a trade print within the window is a cancellation and moves ``q`` by policy:
      optimistic   (A): every cancel was ahead of us      q' = q - v
      realistic    (B): pro rata                          q' = q - floor(v * q / L)
      conservative (C): cancels are behind us if possible q' = min(q, L - v)
  (L = level qty excluding our own orders before the cancel; every policy clamps q to the
  level size after the cancel, so C only moves when the level is smaller than its queue).
* Positive deltas join behind us.

Fill capacity (used by the exchange simulator; in live mode it is a prediction to compare
with real fills). For a print of qty v at price p on book b, our orders on b at px >= p are
visited in price-time priority; order k can receive ``min(R_k, max(0, v - blocking_k))``:
    at-price orders:        blocking = own better-priced R + q_k + own earlier same-level R
    better-priced (sweep):  A/B: blocking = own better-priced R + own earlier same-level R
                            (price priority: the taker went through our level to reach p)
                            C:   also counts q_k and the queue at better own levels
A positive delta on the *opposite* book at a price that crosses our resting price (policy A/B
only) is inferred to be an aggressor that would have matched us: our crossed orders can
receive up to the delta size, blocked by q and our own better orders.

Every operation is a monotone map of (q, R) and the pending trade/delta bookkeeping depends
only on market events, so on identical event streams with identical order arrival times the
cumulative fills satisfy A >= B >= C per order (tests/execution/test_policy_ordering.py).

Live calibration: pass ``book_includes_own=True`` (the live book contains our orders); the
own-order positive delta (orderbook_delta carrying our client_order_id) fixes the arrival
queue, and ``ingest_exchange_queue_position`` compares the estimate with Kalshi's
GET /portfolio/orders/queue_positions. ``QueueCalibrator`` runs all three policies at once.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from typing import Callable

from dh.core.events import KalshiBookDelta, KalshiBookSnapshot, KalshiTrade
from dh.core.units import NS_PER_MS, PX_SCALE

POLICIES: tuple[str, ...] = ("optimistic", "realistic", "conservative")
_POLICY_ALIASES = {
    "a": "optimistic",
    "optimistic": "optimistic",
    "b": "realistic",
    "realistic": "realistic",
    "c": "conservative",
    "conservative": "conservative",
}

Level = tuple[str, str, int]  # (ticker, book 'yes'|'no', px on that book's scale)
QueueFill = tuple[str, int, str]  # (order key, qty, mechanism 'queue'|'sweep'|'cross')


def normalize_policy(policy: str) -> str:
    """'A'|'optimistic' -> 'optimistic', 'B'|'realistic' -> 'realistic', 'C'|... -> 'conservative'."""
    try:
        return _POLICY_ALIASES[policy.strip().lower()]
    except KeyError:
        raise ValueError(f"unknown fill policy {policy!r}; use one of {POLICIES} or A/B/C") from None


def resting_book(book_side: str) -> str:
    """Kalshi bid book our V2 order rests in: 'bid' (buy YES) -> 'yes', 'ask' (sell YES) -> 'no'."""
    if book_side == "bid":
        return "yes"
    if book_side == "ask":
        return "no"
    raise ValueError(f"book_side must be 'bid' or 'ask', got {book_side!r}")


def book_px(book_side: str, yes_px: int) -> int:
    """Price of our order on its resting book's own scale (NO scale for asks)."""
    return yes_px if book_side == "bid" else PX_SCALE - yes_px


def yes_px_of(book: str, px: int) -> int:
    """YES price of a level on book 'yes' | 'no'."""
    return px if book == "yes" else PX_SCALE - px


def trade_maker_book(tr: KalshiTrade) -> tuple[str, int]:
    """(book, px on that book's scale) of the resting orders a public trade executed against."""
    if tr.taker_side == "no":  # taker sold YES (taker_book_side 'ask') -> hit YES bids
        return "yes", tr.yes_px
    return "no", PX_SCALE - tr.yes_px  # taker bought YES -> lifted YES asks == NO bids


def cancel_update(q: int, level_before: int, vol: int, policy: str) -> int:
    """Queue ahead after ``vol`` qty of unexplained cancellations at a level of ``level_before``.

    All quantities in 0.01-contract units; ``level_before`` excludes our own orders. Every
    policy clamps the result to ``[0, level_before - vol]``.
    """
    if vol <= 0:
        return q
    after = max(0, level_before - vol)
    if policy == "optimistic":
        q2 = q - vol
    elif policy == "realistic":
        denom = max(level_before, q, 1)
        q2 = q - (vol * q) // denom
    elif policy == "conservative":
        q2 = q
    else:  # pragma: no cover - normalized upstream
        raise ValueError(policy)
    return max(0, min(q2, after))


@dataclass(slots=True)
class QueueOrder:
    """One of our resting orders as seen by the estimator."""

    key: str
    ticker: str
    book: str  # 'yes' | 'no'
    px: int  # on the book's own scale
    remaining: int  # our resting qty (0.01 contracts)
    queue_ahead: int  # others' qty ahead of us at our level (0.01 contracts)
    seq: int  # arrival order (time priority among our own orders)
    arrival_ts: int
    pending: bool = False  # live mode: waiting for our own positive book delta

    @property
    def level(self) -> Level:
        return (self.ticker, self.book, self.px)


@dataclass(slots=True)
class _Pend:
    ts: int
    vol: int
    lvl: Level
    kind: str  # 't' = trade print awaiting its delta, 'd' = depletion awaiting a print
    level_before: int = 0
    max_seq: int = 0  # orders with seq <= max_seq were resting when the delta happened


@dataclass(frozen=True, slots=True)
class QueueCalibrationSample:
    ts: int
    key: str
    estimated: int  # our estimate of the exchange queue position (qty units)
    reported: int  # exchange-reported queue_position_fp (qty units)

    @property
    def error(self) -> int:
        return self.estimated - self.reported


class QueueEstimator:
    """Tracks queue_ahead for our resting orders under one fill policy.

    ``level_qty(ticker, book, px)`` must return the displayed qty at that level of the
    *current* book (after the event being processed was applied). Call order per event:
    apply the event to the book, then call ``on_book_delta`` / ``on_trade`` / ``on_snapshot``.
    """

    def __init__(
        self,
        policy: str = "realistic",
        level_qty: Callable[[str, str, int], int] | None = None,
        *,
        match_window_ns: int = 250 * NS_PER_MS,
        book_includes_own: bool = False,
    ) -> None:
        self.policy = normalize_policy(policy)
        self._level_qty = level_qty or (lambda ticker, book, px: 0)
        self.match_window_ns = int(match_window_ns)
        self.book_includes_own = book_includes_own
        self.orders: dict[str, QueueOrder] = {}
        self._side: dict[tuple[str, str], dict[int, list[str]]] = {}
        self._pend_trade: dict[Level, deque[_Pend]] = {}
        self._pend_depl: dict[Level, deque[_Pend]] = {}
        self._fifo: deque[_Pend] = deque()
        self._seq = 0
        self.calibration: list[QueueCalibrationSample] = []
        self.stats = {"trade_matched_delta": 0, "delta_matched_trade": 0, "cancel_volume": 0,
                      "stale_trade_volume": 0}

    # ------------------------------------------------------------------ our orders
    def add_order(self, key: str, ticker: str, book_side: str, yes_px: int, qty: int, ts: int, *,
                  queue_ahead: int | None = None, pending: bool = False) -> QueueOrder:
        """Register our order at its arrival at the matching engine.

        queue_ahead=None joins behind the displayed level (minus prints whose delta is still
        pending). pending=True (live mode) defers that until our own positive book delta.
        """
        if key in self.orders:
            raise ValueError(f"duplicate queue key {key!r}")
        self._seq += 1
        book, px = resting_book(book_side), book_px(book_side, yes_px)
        o = QueueOrder(key, ticker, book, px, int(qty), 0, self._seq, ts, pending)
        self.orders[key] = o
        self._side.setdefault((ticker, book), {}).setdefault(px, []).append(key)
        if not pending:
            o.queue_ahead = self._arrival_queue(o) if queue_ahead is None else max(0, int(queue_ahead))
        return o

    def activate(self, key: str, queue_ahead: int | None = None) -> None:
        """Live fallback when no own delta was seen: join behind the displayed level now."""
        o = self.orders.get(key)
        if o is not None and o.pending:
            o.pending = False
            o.queue_ahead = self._arrival_queue(o) if queue_ahead is None else max(0, int(queue_ahead))

    def remove_order(self, key: str) -> None:
        o = self.orders.pop(key, None)
        if o is None:
            return
        levels = self._side.get((o.ticker, o.book))
        if levels is not None:
            keys = levels.get(o.px)
            if keys is not None:
                keys.remove(key)
                if not keys:
                    del levels[o.px]
            if not levels:
                del self._side[(o.ticker, o.book)]

    def on_own_fill(self, key: str, qty: int) -> None:
        """Our order filled ``qty``: reduce its resting qty (removed when it reaches 0)."""
        o = self.orders.get(key)
        if o is None:
            return
        o.remaining -= int(qty)
        if o.remaining <= 0:
            self.remove_order(key)

    def reduce(self, key: str, new_remaining: int) -> None:
        """Size decrease that keeps time priority (Kalshi: decrease / amend down)."""
        o = self.orders.get(key)
        if o is None:
            return
        if new_remaining <= 0:
            self.remove_order(key)
        else:
            o.remaining = min(o.remaining, int(new_remaining))

    def requeue(self, key: str, ticker: str, book_side: str, yes_px: int, qty: int, ts: int) -> QueueOrder:
        """Price change or size increase: loses priority, re-joins at the back of its level."""
        self.remove_order(key)
        return self.add_order(key, ticker, book_side, yes_px, qty, ts)

    def queue_ahead(self, key: str) -> int | None:
        o = self.orders.get(key)
        return None if o is None or o.pending else o.queue_ahead

    def estimated_position(self, key: str) -> int | None:
        """Estimated exchange queue position: others ahead + our own earlier orders at the level."""
        o = self.orders.get(key)
        if o is None or o.pending:
            return None
        own = 0
        for k in self._side.get((o.ticker, o.book), {}).get(o.px, []):
            if k == key:
                break
            ok = self.orders[k]
            if not ok.pending:
                own += ok.remaining
        return o.queue_ahead + own

    # ------------------------------------------------------------------ market data
    def on_snapshot(self, ev: KalshiBookSnapshot) -> None:
        """Fresh book: drop pending matches for the ticker and clamp every queue to its level."""
        for it in self._fifo:
            if it.lvl[0] == ev.ticker:
                it.vol = 0
        for o in self.orders.values():
            if o.ticker == ev.ticker and not o.pending:
                o.queue_ahead = min(o.queue_ahead, self._level_excl(o.level))

    def on_book_delta(self, ev: KalshiBookDelta) -> list[QueueFill]:
        """Process a delta already applied to the book. Returns inferred crossing fills (A/B)."""
        self._expire(ev.ts)
        lvl: Level = (ev.ticker, ev.side, ev.px)
        own = ev.own_client_order_id
        if own and self.book_includes_own:
            o = self.orders.get(own)
            if o is not None and o.pending and ev.delta > 0 and o.level == lvl:
                o.pending = False
                o.queue_ahead = self._arrival_queue(o)
            return []  # our own orders are not "others" in the live book
        if ev.delta < 0:
            vol = -ev.delta
            explained = self._consume(self._pend_trade, lvl, vol)
            if explained:
                self.stats["delta_matched_trade"] += explained
            rest = vol - explained
            if rest > 0:
                it = _Pend(ev.ts, rest, lvl, "d", level_before=self._level_excl(lvl) + rest, max_seq=self._seq)
                self._pend_depl.setdefault(lvl, deque()).append(it)
                self._fifo.append(it)
            return []
        if ev.delta > 0 and self.policy != "conservative":
            return self._crossing_fills(ev)
        return []

    def on_trade(self, tr: KalshiTrade) -> list[QueueFill]:
        """Process a public trade print; returns our fill capacity (caller applies fills and
        then calls ``on_own_fill``). Updates the queue ahead of our orders at/through the price."""
        self._expire(tr.ts)
        if tr.is_block or tr.qty <= 0:
            return []
        book, p = trade_maker_book(tr)
        lvl: Level = (tr.ticker, book, p)
        v = tr.qty
        matched = self._consume(self._pend_depl, lvl, v)
        if matched:
            self.stats["trade_matched_delta"] += matched
        if v - matched > 0:
            it = _Pend(tr.ts, v - matched, lvl, "t")
            self._pend_trade.setdefault(lvl, deque()).append(it)
            self._fifo.append(it)
        fills = self._fills_through(tr.ticker, book, p, v, cross=False)
        levels = self._side.get((tr.ticker, book))
        if levels:
            for px, keys in levels.items():
                if px < p:
                    continue
                for k in keys:
                    o = self.orders[k]
                    if o.pending:
                        continue
                    if px == p:
                        o.queue_ahead = max(0, o.queue_ahead - v)
                    elif self.policy != "conservative":
                        o.queue_ahead = 0  # price priority: the whole level ahead of us traded
        return fills

    def advance(self, now_ns: int) -> None:
        """Classify expired unmatched deltas as cancels (call on timers / before decisions)."""
        self._expire(now_ns)

    # ------------------------------------------------------------------ calibration (live)
    def ingest_exchange_queue_position(self, key: str, reported_qty: int, ts: int, *,
                                       resync: bool = False) -> QueueCalibrationSample | None:
        """Record estimator error vs Kalshi's queue_position_fp (qty units, includes our own
        earlier orders at the level). resync=True replaces the estimate with the truth."""
        est = self.estimated_position(key)
        if est is None:
            return None
        s = QueueCalibrationSample(ts, key, est, int(reported_qty))
        self.calibration.append(s)
        if resync:
            o = self.orders[key]
            o.queue_ahead = max(0, o.queue_ahead + (int(reported_qty) - est))
        return s

    def calibration_stats(self) -> dict[str, float]:
        """n, mean error (bias), MAE and RMSE of the estimate, in 0.01-contract units."""
        errs = [s.error for s in self.calibration]
        n = len(errs)
        if n == 0:
            return {"n": 0, "mean_error": 0.0, "mae": 0.0, "rmse": 0.0}
        return {"n": n, "mean_error": sum(errs) / n, "mae": sum(abs(e) for e in errs) / n,
                "rmse": (sum(e * e for e in errs) / n) ** 0.5}

    # ------------------------------------------------------------------ internals
    def _own_at(self, lvl: Level) -> int:
        keys = self._side.get((lvl[0], lvl[1]), {}).get(lvl[2], [])
        return sum(self.orders[k].remaining for k in keys if not self.orders[k].pending)

    def _level_excl(self, lvl: Level) -> int:
        q = int(self._level_qty(*lvl))
        if self.book_includes_own:
            q -= self._own_at(lvl)
        return max(0, q)

    def _pending_total(self, table: dict[Level, deque[_Pend]], lvl: Level) -> int:
        dq = table.get(lvl)
        return sum(it.vol for it in dq) if dq else 0

    def _arrival_queue(self, o: QueueOrder) -> int:
        return max(0, self._level_excl(o.level) - self._pending_total(self._pend_trade, o.level))

    def _consume(self, table: dict[Level, deque[_Pend]], lvl: Level, vol: int) -> int:
        dq = table.get(lvl)
        if not dq:
            return 0
        used = 0
        while dq and used < vol:
            it = dq[0]
            take = min(it.vol, vol - used)
            it.vol -= take
            used += take
            if it.vol <= 0:
                dq.popleft()
        if not dq:
            del table[lvl]
        return used

    def _expire(self, now: int) -> None:
        limit = now - self.match_window_ns
        fifo = self._fifo
        while fifo and fifo[0].ts < limit:
            it = fifo.popleft()
            if it.vol <= 0:
                continue
            table = self._pend_trade if it.kind == "t" else self._pend_depl
            dq = table.get(it.lvl)
            if dq:
                while dq and dq[0].vol <= 0:
                    dq.popleft()
                if dq and dq[0] is it:
                    dq.popleft()
                if not dq:
                    del table[it.lvl]
            if it.kind == "t":
                self.stats["stale_trade_volume"] += it.vol
            else:
                self._classify_cancel(it)
            it.vol = 0

    def _classify_cancel(self, it: _Pend) -> None:
        vol, it.vol = it.vol, 0
        self.stats["cancel_volume"] += vol
        ticker, book, px = it.lvl
        keys = self._side.get((ticker, book), {}).get(px, [])
        if not keys:
            return
        # Invariant: others ahead of us <= displayed others + depletion not yet classified.
        # (FIFO print<->delta pairing can make the stored level_before stale; the policy-
        # independent clamp keeps every policy consistent and preserves A <= B <= C.)
        bound = self._level_excl(it.lvl) + self._pending_total(self._pend_depl, it.lvl)
        for k in keys:
            o = self.orders[k]
            if o.pending or o.seq > it.max_seq:
                continue
            o.queue_ahead = min(cancel_update(o.queue_ahead, it.level_before, vol, self.policy), bound)

    def _fills_through(self, ticker: str, book: str, p: int, v: int, *, cross: bool) -> list[QueueFill]:
        """Fill capacity of volume v reaching book ``book`` at price p (see module doc)."""
        levels = self._side.get((ticker, book))
        if not levels or v <= 0:
            return []
        out: list[QueueFill] = []
        blocked = 0  # volume that must pass before reaching the current level
        conservative = self.policy == "conservative"
        for px in sorted(levels, reverse=True):
            if px < p:
                break
            at_price = px == p
            use_q = cross or at_price or conservative
            mech = "cross" if cross else ("queue" if at_price else "sweep")
            same = 0
            level_q = 0
            for k in levels[px]:
                o = self.orders[k]
                if o.pending:
                    continue
                q = o.queue_ahead if use_q else 0
                f = min(o.remaining, max(0, v - (blocked + q + same)))
                if f > 0:
                    out.append((k, f, mech))
                same += o.remaining
                level_q = max(level_q, q)
            blocked += level_q + same
            if blocked >= v:
                break
        return out

    def _crossing_fills(self, ev: KalshiBookDelta) -> list[QueueFill]:
        # A new level on book X at px_X is a YES-equivalent offer crossing our orders on the
        # opposite book at px >= 1 - px_X (NO bid q == YES ask 1-q; YES bid p == NO ask 1-p).
        opp = "no" if ev.side == "yes" else "yes"
        return self._fills_through(ev.ticker, opp, PX_SCALE - ev.px, ev.delta, cross=True)


class QueueCalibrator:
    """Runs one QueueEstimator per policy on the same live stream and compares each with the
    exchange-reported queue positions (GET /portfolio/orders/queue_positions)."""

    def __init__(self, level_qty: Callable[[str, str, int], int], *, match_window_ns: int = 250 * NS_PER_MS,
                 book_includes_own: bool = True) -> None:
        self.estimators = {p: QueueEstimator(p, level_qty, match_window_ns=match_window_ns,
                                             book_includes_own=book_includes_own) for p in POLICIES}

    def add_order(self, key: str, ticker: str, book_side: str, yes_px: int, qty: int, ts: int, *,
                  pending: bool = True) -> None:
        for e in self.estimators.values():
            e.add_order(key, ticker, book_side, yes_px, qty, ts, pending=pending)

    def activate(self, key: str) -> None:
        for e in self.estimators.values():
            e.activate(key)

    def remove_order(self, key: str) -> None:
        for e in self.estimators.values():
            e.remove_order(key)

    def on_own_fill(self, key: str, qty: int) -> None:
        for e in self.estimators.values():
            e.on_own_fill(key, qty)

    def on_snapshot(self, ev: KalshiBookSnapshot) -> None:
        for e in self.estimators.values():
            e.on_snapshot(ev)

    def on_book_delta(self, ev: KalshiBookDelta) -> dict[str, list[QueueFill]]:
        return {p: e.on_book_delta(ev) for p, e in self.estimators.items()}

    def on_trade(self, tr: KalshiTrade) -> dict[str, list[QueueFill]]:
        """Returns each policy's *predicted* fills (compare with the real fills we receive)."""
        return {p: e.on_trade(tr) for p, e in self.estimators.items()}

    def ingest_exchange_queue_position(self, key: str, reported_qty: int, ts: int) -> None:
        for e in self.estimators.values():
            e.ingest_exchange_queue_position(key, reported_qty, ts)

    def summary(self) -> dict[str, dict[str, float]]:
        return {p: e.calibration_stats() for p, e in self.estimators.items()}
