"""Consumer-side state built from normalized events: external books, BBOs, perp states,
index ticks and Kalshi books. Pure (no clock, no I/O): used by the composite nowcast, replay
warm-up priming, scripts/replay_inspect.py and scripts/smoke_feeds.py.

Contract with the normalizers (dh.feeds.base): after ``FeedStatus(status='gap')`` for stream
``<venue>.book:<symbol>`` no deltas arrive for that book until a new snapshot, so the tracker
simply marks the book invalid on 'gap' and valid again on the next snapshot.
"""

from __future__ import annotations

from collections.abc import Iterator

from dh.core.book import ExtBook, KalshiBook
from dh.core.events import (
    Event,
    ExtBBO,
    ExtBookDelta,
    ExtBookSnapshot,
    FeedStatus,
    IndexTick,
    KalshiBookDelta,
    KalshiBookSnapshot,
    PerpState,
)

BookId = tuple[str, str]  # (venue, symbol)


class BookTracker:
    """Applies ExtBookSnapshot/ExtBookDelta/ExtBBO/PerpState/IndexTick/Kalshi book events."""

    def __init__(self, track_kalshi: bool = True) -> None:
        self.books: dict[BookId, ExtBook] = {}
        self.depth_limited: dict[BookId, bool] = {}
        self.last_ts_exch: dict[BookId, int] = {}
        self.bbo: dict[BookId, ExtBBO] = {}
        self.perp: dict[BookId, PerpState] = {}
        self.index: dict[str, IndexTick] = {}
        self.kalshi: dict[str, KalshiBook] = {}
        self.track_kalshi = track_kalshi
        self.status: dict[str, FeedStatus] = {}
        self.n_events = 0
        self.crossed_events = 0

    # ------------------------------------------------------------------ updates
    def on_event(self, ev: Event) -> None:
        self.n_events += 1
        if isinstance(ev, ExtBookDelta):
            b = self.books.get((ev.venue, ev.symbol))
            if b is None or not b.valid:
                return
            for side, p, s in ev.changes:
                b.update(side, p, s)
            b.ts, b.seq = ev.ts, ev.seq
            self.last_ts_exch[(ev.venue, ev.symbol)] = ev.ts_exch
            if b.crossed():
                self.crossed_events += 1
        elif isinstance(ev, ExtBookSnapshot):
            key = (ev.venue, ev.symbol)
            b = self.books.get(key)
            if b is None:
                b = self.books[key] = ExtBook(ev.venue, ev.symbol)
            b.snapshot(ev.bids, ev.asks, ev.ts, ev.seq)
            self.depth_limited[key] = ev.depth_limited
            self.last_ts_exch[key] = ev.ts_exch
        elif isinstance(ev, ExtBBO):
            self.bbo[(ev.venue, ev.symbol)] = ev
        elif isinstance(ev, PerpState):
            self.perp[(ev.venue, ev.symbol)] = ev
        elif isinstance(ev, IndexTick):
            self.index[ev.index_id] = ev
        elif isinstance(ev, FeedStatus):
            self.status[ev.stream] = ev
            if ev.status == "gap" and ".book:" in ev.stream:
                venue, sym = ev.stream.split(".book:", 1)
                b = self.books.get((venue, sym))
                if b is not None:
                    b.valid = False
        elif self.track_kalshi and isinstance(ev, KalshiBookSnapshot):
            kb = self.kalshi.get(ev.ticker)
            if kb is None:
                kb = self.kalshi[ev.ticker] = KalshiBook(ev.ticker)
            kb.apply_snapshot(ev)
        elif self.track_kalshi and isinstance(ev, KalshiBookDelta):
            kb = self.kalshi.get(ev.ticker)
            if kb is not None:
                kb.apply_delta(ev)

    # ------------------------------------------------------------------ views
    def valid_books(self) -> dict[BookId, ExtBook]:
        return {k: b for k, b in self.books.items() if b.valid and b.top() is not None}

    def books_by_venue(self, venues: tuple[str, ...] | None = None) -> dict[str, ExtBook]:
        """One book per venue (first symbol seen), optionally restricted to ``venues``."""
        out: dict[str, ExtBook] = {}
        for (v, _s), b in sorted(self.books.items()):
            if venues is not None and v not in venues:
                continue
            out.setdefault(v, b)
        return out

    def state_events(self, ts: int) -> Iterator[Event]:
        """Synthesized events that recreate the tracked state at time ``ts`` (replay warm-up):
        one ExtBookSnapshot per valid book, the last ExtBBO / PerpState / IndexTick per key and
        a KalshiBookSnapshot per valid Kalshi book. Deterministic order (sorted keys)."""
        for key in sorted(self.books):
            b = self.books[key]
            if not b.valid:
                continue
            bids = tuple((p, s) for p, s in reversed(b.bids.items()))
            asks = tuple(b.asks.items())
            yield ExtBookSnapshot(ts, self.last_ts_exch.get(key, 0), key[0], key[1], bids, asks, b.seq, self.depth_limited.get(key, True))
        for key in sorted(self.bbo):
            e = self.bbo[key]
            yield ExtBBO(ts, e.ts_exch, e.venue, e.symbol, e.bid, e.bid_size, e.ask, e.ask_size, e.seq)
        for key in sorted(self.perp):
            p = self.perp[key]
            yield PerpState(ts, p.ts_exch, p.venue, p.symbol, p.mark, p.index, p.funding_rate, p.funding_interval_s, p.next_funding_ts, p.open_interest)
        for iid in sorted(self.index):
            i = self.index[iid]
            yield IndexTick(ts, i.ts_exch, i.index_id, i.value, i.feed, i.kalshi_recv_ns, i.avg60, i.avg60_n, i.qh_avg, i.qh_n)
        for ticker in sorted(self.kalshi):
            kb = self.kalshi[ticker]
            if not kb.valid:
                continue
            yield KalshiBookSnapshot(ts, 0, ticker, kb.sid, kb.seq, tuple(kb.yes_bids.items()), tuple(kb.no_bids.items()))
