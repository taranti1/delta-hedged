"""Kalshi L2 order book (exact integer units) and external-venue L2 book (floats).

Kalshi publishes two bid books per market: YES bids and NO bids (asyncapi `orderbook_snapshot`
yes_dollars_fp / no_dollars_fp; `orderbook_delta` with side yes|no). A NO bid at price q is a
YES ask at 1 - q. `KalshiBook` stores both verbatim and exposes the YES-book view.

Sequence numbers on Kalshi are per *subscription* (sid), shared by every market in it, so
gap detection belongs to the WebSocket client (dh.kalshi.ws), which emits FeedStatus('gap')
and requests fresh snapshots. The book validates what it can locally: deltas for an
un-snapshotted book, and sizes that would go negative (proof of a missed message), both
invalidate it until the next snapshot.
"""

from __future__ import annotations

from dataclasses import dataclass

from sortedcontainers import SortedDict

from dh.core.events import KalshiBookDelta, KalshiBookSnapshot
from dh.core.units import PX_SCALE, QTY_SCALE


class KalshiBook:
    __slots__ = ("ticker", "yes_bids", "no_bids", "valid", "sid", "seq", "ts", "invalid_reason")

    def __init__(self, ticker: str) -> None:
        self.ticker = ticker
        self.yes_bids: SortedDict = SortedDict()  # px -> qty (YES price scale)
        self.no_bids: SortedDict = SortedDict()  # px -> qty (NO price scale)
        self.valid = False
        self.sid = 0
        self.seq = 0
        self.ts = 0
        self.invalid_reason = "no snapshot"

    # ------------------------------------------------------------------ updates
    def apply_snapshot(self, ev: KalshiBookSnapshot) -> None:
        self.yes_bids = SortedDict({px: q for px, q in ev.yes_bids if q > 0})
        self.no_bids = SortedDict({px: q for px, q in ev.no_bids if q > 0})
        self.valid = True
        self.invalid_reason = ""
        self.sid, self.seq, self.ts = ev.sid, ev.seq, ev.ts

    def apply_delta(self, ev: KalshiBookDelta) -> bool:
        """Apply a delta; returns False (and invalidates) if the result is impossible."""
        self.ts = ev.ts
        self.seq = ev.seq
        if not self.valid:
            return False
        book = self.yes_bids if ev.side == "yes" else self.no_bids
        new = book.get(ev.px, 0) + ev.delta
        if new < 0:
            self.invalidate(f"negative size at {ev.side} {ev.px}: {new}")
            return False
        if new == 0:
            book.pop(ev.px, None)
        else:
            book[ev.px] = new
        return True

    def invalidate(self, reason: str) -> None:
        self.valid = False
        self.invalid_reason = reason

    # ------------------------------------------------------------------ YES-book view
    def best_bid(self) -> int | None:
        """Best YES bid px or None."""
        return self.yes_bids.peekitem(-1)[0] if self.yes_bids else None

    def best_ask(self) -> int | None:
        """Best YES ask px (= 1 - best NO bid) or None."""
        return PX_SCALE - self.no_bids.peekitem(-1)[0] if self.no_bids else None

    def bid_qty(self, px: int) -> int:
        return self.yes_bids.get(px, 0)

    def ask_qty(self, px: int) -> int:
        """Resting YES-ask qty at YES price px (NO bids at 1 - px)."""
        return self.no_bids.get(PX_SCALE - px, 0)

    def best_bid_qty(self) -> int:
        b = self.best_bid()
        return 0 if b is None else self.yes_bids[b]

    def best_ask_qty(self) -> int:
        a = self.best_ask()
        return 0 if a is None else self.ask_qty(a)

    def bids(self, n: int = 10) -> list[tuple[int, int]]:
        """Top-n YES bids, best first."""
        items = self.yes_bids.items()
        return [(px, q) for px, q in reversed(items[-n:])] if n else []

    def asks(self, n: int = 10) -> list[tuple[int, int]]:
        """Top-n YES asks on the YES scale, best (lowest) first."""
        items = self.no_bids.items()
        return [(PX_SCALE - px, q) for px, q in reversed(items[-n:])] if n else []

    def mid(self) -> float | None:
        b, a = self.best_bid(), self.best_ask()
        if b is None or a is None:
            return None
        return (b + a) / 2

    def spread(self) -> int | None:
        b, a = self.best_bid(), self.best_ask()
        return None if b is None or a is None else a - b

    def microprice(self) -> float | None:
        b, a = self.best_bid(), self.best_ask()
        if b is None or a is None:
            return None
        qb, qa = self.yes_bids[b], self.ask_qty(a)
        if qb + qa == 0:
            return (a + b) / 2
        return (b * qa + a * qb) / (qa + qb)

    def depth_within(self, side: str, ticks_px: int) -> int:
        """Total qty on 'bid'|'ask' within ticks_px of the touch (YES scale)."""
        if side == "bid":
            b = self.best_bid()
            if b is None:
                return 0
            return sum(q for px, q in self.yes_bids.items() if px >= b - ticks_px)
        a = self.best_ask()
        if a is None:
            return 0
        return sum(q for px, q in self.no_bids.items() if PX_SCALE - px <= a + ticks_px)

    def crossed(self) -> bool:
        b, a = self.best_bid(), self.best_ask()
        return b is not None and a is not None and b >= a

    def __repr__(self) -> str:  # pragma: no cover - debug aid
        b, a = self.best_bid(), self.best_ask()
        return (
            f"KalshiBook({self.ticker} {b}x{self.best_bid_qty() / QTY_SCALE:g} / "
            f"{a}x{self.best_ask_qty() / QTY_SCALE:g} valid={self.valid})"
        )


@dataclass(slots=True)
class ExtTop:
    bid: float
    bid_size: float
    ask: float
    ask_size: float

    @property
    def mid(self) -> float:
        return 0.5 * (self.bid + self.ask)

    @property
    def microprice(self) -> float:
        s = self.bid_size + self.ask_size
        if s <= 0:
            return self.mid
        return (self.bid * self.ask_size + self.ask * self.bid_size) / s


class ExtBook:
    """Price-level book for an external BTC venue (absolute-size updates)."""

    __slots__ = ("venue", "symbol", "bids", "asks", "ts", "seq", "valid")

    def __init__(self, venue: str, symbol: str) -> None:
        self.venue, self.symbol = venue, symbol
        self.bids: SortedDict = SortedDict()  # price -> size
        self.asks: SortedDict = SortedDict()
        self.ts = 0
        self.seq = 0
        self.valid = False

    def snapshot(self, bids, asks, ts: int, seq: int = 0) -> None:
        self.bids = SortedDict({p: s for p, s in bids if s > 0})
        self.asks = SortedDict({p: s for p, s in asks if s > 0})
        self.ts, self.seq, self.valid = ts, seq, True

    def update(self, side: str, price: float, size: float) -> None:
        book = self.bids if side == "b" else self.asks
        if size <= 0:
            book.pop(price, None)
        else:
            book[price] = size

    def top(self) -> ExtTop | None:
        if not self.bids or not self.asks:
            return None
        bp, bs = self.bids.peekitem(-1)
        ap, as_ = self.asks.peekitem(0)
        return ExtTop(bp, bs, ap, as_)

    def depth_usd(self, side: str, bps: float) -> float:
        """USD notional resting within `bps` of the touch on one side."""
        t = self.top()
        if t is None:
            return 0.0
        if side == "b":
            lo = t.bid * (1 - bps / 1e4)
            return sum(p * s for p, s in self.bids.items() if p >= lo)
        hi = t.ask * (1 + bps / 1e4)
        return sum(p * s for p, s in self.asks.items() if p <= hi)

    def impact_price(self, side: str, qty: float) -> float | None:
        """Average execution price to BUY (side='buy', walks asks) or SELL qty BTC."""
        levels = self.asks.items() if side == "buy" else reversed(self.bids.items())
        left, cost = qty, 0.0
        for p, s in levels:
            take = min(left, s)
            cost += take * p
            left -= take
            if left <= 1e-12:
                return cost / qty
        return None

    def crossed(self) -> bool:
        t = self.top()
        return t is not None and t.bid >= t.ask
