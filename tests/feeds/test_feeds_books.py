from __future__ import annotations

from dh.core.events import ExtBBO, ExtBookDelta, ExtBookSnapshot, FeedStatus, IndexTick, KalshiBookDelta, KalshiBookSnapshot, PerpState
from dh.feeds.books import BookTracker
from tests.feeds.helpers import VENUE_FIXTURES, run_fixture


def test_tracker_applies_and_invalidates():
    tr = BookTracker()
    tr.on_event(ExtBookSnapshot(1, 0, "v", "S", ((100.0, 1.0),), ((101.0, 1.0),), seq=1))
    tr.on_event(ExtBookDelta(2, 0, "v", "S", (("b", 100.5, 2.0), ("a", 101.0, 0.0), ("a", 102.0, 1.0)), seq=2))
    b = tr.books[("v", "S")]
    assert b.top().bid == 100.5 and b.top().ask == 102.0 and b.ts == 2 and b.seq == 2
    tr.on_event(FeedStatus(3, 0, "v.book:S", "gap", "x"))
    assert not b.valid
    tr.on_event(ExtBookDelta(4, 0, "v", "S", (("b", 105.0, 1.0),)))  # ignored while invalid
    assert b.top().bid == 100.5
    tr.on_event(ExtBookSnapshot(5, 0, "v", "S", ((99.0, 1.0),), ((100.0, 1.0),)))
    assert b.valid and b.top().bid == 99.0


def test_state_events_recreate_state():
    tr = BookTracker()
    for name in VENUE_FIXTURES:
        ev, _ = run_fixture(name)
        for e in ev:
            tr.on_event(e)
    tr.on_event(KalshiBookSnapshot(1, 0, "KXBTCD-X", 7, 1, ((4500, 100),), ((5300, 200),)))
    tr.on_event(KalshiBookDelta(2, 0, "KXBTCD-X", 7, 2, "yes", 4600, 50))
    synth = list(tr.state_events(10**19))
    assert all(e.ts == 10**19 for e in synth)
    assert any(isinstance(e, PerpState) for e in synth) and any(isinstance(e, ExtBBO) for e in synth)
    assert any(isinstance(e, IndexTick) for e in synth)
    fresh = BookTracker()
    for e in synth:
        fresh.on_event(e)
    for key, b in tr.valid_books().items():
        fb = fresh.books[key]
        assert list(fb.bids.items()) == list(b.bids.items()) and list(fb.asks.items()) == list(b.asks.items())
    assert fresh.kalshi["KXBTCD-X"].best_bid() == 4600
    assert list(tr.state_events(10**19)) == synth  # deterministic
