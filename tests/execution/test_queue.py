from __future__ import annotations

from hypothesis import given, settings
from hypothesis import strategies as st

from dh.core.book import KalshiBook
from dh.execution.queue import (
    POLICIES,
    QueueCalibrator,
    QueueEstimator,
    cancel_update,
    normalize_policy,
    trade_maker_book,
)
from tests.execution.helpers import MS, T, delta, snap, trade

W = 250 * MS  # default match window


class BookFeed:
    """A KalshiBook plus estimator(s) fed in the simulator's order (book first, then queue)."""

    def __init__(self, policies=POLICIES, **kw):
        self.book = KalshiBook(T)
        self.q = {p: QueueEstimator(p, self.level, **kw) for p in policies}

    def level(self, ticker, book, px):
        return (self.book.yes_bids if book == "yes" else self.book.no_bids).get(px, 0)

    def snap(self, ev):
        self.book.apply_snapshot(ev)
        for e in self.q.values():
            e.on_snapshot(ev)

    def delta(self, ev):
        self.book.apply_delta(ev)
        return {p: e.on_book_delta(ev) for p, e in self.q.items()}

    def trade(self, ev):
        return {p: e.on_trade(ev) for p, e in self.q.items()}

    def add(self, key, side, px, qty, ts=0):
        for e in self.q.values():
            e.add_order(key, T, side, px, qty, ts)

    def qa(self, key):
        return {p: e.queue_ahead(key) for p, e in self.q.items()}


def test_policy_aliases_and_trade_side_mapping():
    assert normalize_policy("A") == "optimistic" and normalize_policy("c") == "conservative"
    # taker sold YES -> hit YES bids at yes_px; taker bought YES -> lifted NO bids at 1 - yes_px
    assert trade_maker_book(trade(0, 4500, 100, "no")) == ("yes", 4500)
    assert trade_maker_book(trade(0, 4700, 100, "yes")) == ("no", 5300)


@given(q=st.integers(0, 5000), extra=st.integers(0, 5000), vol=st.integers(0, 6000))
def test_cancel_update_ordering_and_clamp(q, extra, vol):
    level = q + extra  # others at the level; q of them ahead of us
    a, b, c = (cancel_update(q, level, vol, p) for p in POLICIES)
    after = max(0, level - vol)
    assert 0 <= a <= b <= c <= q
    assert c <= after and b <= after
    assert a == max(0, min(q - vol, after))
    assert c == min(q, after)


@given(q1=st.integers(0, 3000), dq=st.integers(0, 3000), extra=st.integers(0, 3000), vol=st.integers(0, 6000))
def test_cancel_update_monotone_in_queue(q1, dq, extra, vol):
    q2 = q1 + dq
    level = q2 + extra
    for p in POLICIES:
        assert cancel_update(q1, level, vol, p) <= cancel_update(q2, level, vol, p)


def test_arrival_and_trade_first_then_delta_no_double_count():
    f = BookFeed()
    f.snap(snap(0))  # YES 45c x 10.00
    f.add("o1", "bid", 4500, 300)
    assert f.qa("o1") == {p: 1000 for p in POLICIES}
    fills = f.trade(trade(10 * MS, 4500, 400, "no"))
    assert all(v == [] for v in fills.values())
    assert f.qa("o1") == {p: 600 for p in POLICIES}
    f.delta(delta(11 * MS, "yes", 4500, -400))  # the book side of the same match
    for e in f.q.values():
        e.advance(10 * W)
    assert f.qa("o1") == {p: 600 for p in POLICIES}  # not reduced twice


def test_delta_first_then_trade_no_double_count():
    f = BookFeed()
    f.snap(snap(0))
    f.add("o1", "bid", 4500, 300)
    f.delta(delta(10 * MS, "yes", 4500, -400))  # delta arrives before its trade print
    assert f.qa("o1") == {p: 1000 for p in POLICIES}  # not classified yet (inside window)
    f.trade(trade(12 * MS, 4500, 400, "no"))
    for e in f.q.values():
        e.advance(10 * W)
    assert f.qa("o1") == {p: 600 for p in POLICIES}
    assert all(e.stats["cancel_volume"] == 0 for e in f.q.values())


def test_unexplained_delta_is_cancel_by_policy():
    f = BookFeed()
    f.snap(snap(0, yes=((4500, 1000),)))
    f.add("o1", "bid", 4500, 100)  # q = 1000
    f.delta(delta(1 * MS, "yes", 4500, 500))  # joins behind us: level 1500
    f.delta(delta(2 * MS, "yes", 4500, -600))  # cancel, level_before 1500 -> after 900
    for e in f.q.values():
        e.advance(2 * MS + W + 1)
    got = f.qa("o1")
    assert got["optimistic"] == 400  # all ahead
    assert got["realistic"] == 1000 - (600 * 1000) // 1500  # 600
    assert got["conservative"] == 900  # behind us as far as possible, clamped to the level


def test_trade_fill_capacity_and_partial():
    f = BookFeed()
    f.snap(snap(0))
    f.add("o1", "bid", 4500, 300)  # q = 1000
    out = f.trade(trade(1 * MS, 4500, 1100, "no"))  # 100 beyond the queue
    assert out == {p: [("o1", 100, "queue")] for p in POLICIES}
    for e in f.q.values():
        e.on_own_fill("o1", 100)
    out = f.trade(trade(2 * MS, 4500, 500, "no"))
    assert out == {p: [("o1", 200, "queue")] for p in POLICIES}


def test_trade_on_other_book_side_does_not_touch_us():
    f = BookFeed()
    f.snap(snap(0))
    f.add("o1", "bid", 4500, 300)
    out = f.trade(trade(1 * MS, 4700, 700, "yes"))  # buys YES from asks: NO book
    assert all(v == [] for v in out.values()) and f.qa("o1") == {p: 1000 for p in POLICIES}


def test_sweep_through_fill_rules():
    f = BookFeed()
    f.snap(snap(0, yes=((4500, 1000), (4400, 500))))
    f.add("o1", "bid", 4500, 300)  # q = 1000 at 45
    # taker swept through 45 down to 44 (its print at 45 is in a separate message we skip here)
    out = f.trade(trade(1 * MS, 4400, 200, "no"))
    assert out["optimistic"] == [("o1", 200, "sweep")] and out["realistic"] == [("o1", 200, "sweep")]
    assert out["conservative"] == []  # its queue (1000) was never shown to be exhausted
    assert f.qa("o1")["optimistic"] == 0 and f.qa("o1")["conservative"] == 1000
    # conservative fills through once the queue ahead at our price is exhausted by prints
    f2 = BookFeed(("conservative",))
    f2.snap(snap(0, yes=((4500, 1000), (4400, 500))))
    f2.add("o1", "bid", 4500, 300)
    assert f2.trade(trade(1 * MS, 4500, 1000, "no")) == {"conservative": []}
    assert f2.trade(trade(1 * MS, 4400, 200, "no")) == {"conservative": [("o1", 200, "sweep")]}


def test_crossing_level_fill_A_B_only():
    f = BookFeed()
    f.snap(snap(0, yes=((4400, 1000),), no=((5300, 700),)))  # 44 / 47
    f.add("o1", "bid", 4600, 300)  # bid at 46 inside the spread, q = 0
    # new NO bid at 55 == YES ask at 45 <= our bid 46: an aggressor would have hit us
    out = f.delta(delta(1 * MS, "no", 5500, 200))
    assert out["optimistic"] == [("o1", 200, "cross")] and out["realistic"] == [("o1", 200, "cross")]
    assert out["conservative"] == []
    assert f.delta(delta(2 * MS, "no", 5300, 100)) == {p: [] for p in POLICIES}  # 47 ask: no cross


def test_time_priority_among_own_orders():
    f = BookFeed(("realistic",))
    f.snap(snap(0))
    f.add("o1", "bid", 4500, 300)  # q = 1000
    f.delta(delta(1 * MS, "yes", 4500, 200))  # others join (level 1200)
    f.add("o2", "bid", 4500, 300)  # q = 1200, and behind o1
    e = f.q["realistic"]
    assert e.estimated_position("o1") == 1000 and e.estimated_position("o2") == 1200 + 300
    out = f.trade(trade(2 * MS, 4500, 1600, "no"))
    # o1 gets 1600-1000 = 600 -> capped 300; o2 needs 1200 + o1's 300 -> gets 100
    assert out == {"realistic": [("o1", 300, "queue"), ("o2", 100, "queue")]}


def test_snapshot_clamps_queue_and_positive_delta_joins_behind():
    f = BookFeed()
    f.snap(snap(0))
    f.add("o1", "bid", 4500, 300)
    f.delta(delta(1 * MS, "yes", 4500, 700))
    assert f.qa("o1") == {p: 1000 for p in POLICIES}
    f.snap(snap(5_000_000_000, yes=((4500, 250),)))
    assert f.qa("o1") == {p: 250 for p in POLICIES}


def test_orders_arriving_after_a_delta_are_not_hit_by_its_classification():
    f = BookFeed()
    f.snap(snap(0))
    f.delta(delta(1 * MS, "yes", 4500, -400))  # unexplained, level now 600
    f.add("late", "bid", 4500, 100, ts=2 * MS)  # joins behind the 600 displayed
    for e in f.q.values():
        e.advance(10 * W)
    assert f.qa("late") == {p: 600 for p in POLICIES}


def test_live_mode_own_delta_sets_arrival_queue_and_calibration():
    book = KalshiBook(T)
    lvl = lambda t, b, p: (book.yes_bids if b == "yes" else book.no_bids).get(p, 0)  # noqa: E731
    cal = QueueCalibrator(lvl)
    book.apply_snapshot(snap(0))
    cal.on_snapshot(snap(0))
    cal.add_order("c1", T, "bid", 4500, 300, 0, pending=True)
    d_other = delta(1 * MS, "yes", 4500, 200)
    book.apply_delta(d_other)
    cal.on_book_delta(d_other)
    d_own = delta(2 * MS, "yes", 4500, 300, own="c1")  # our order joins the live book
    book.apply_delta(d_own)
    cal.on_book_delta(d_own)
    for e in cal.estimators.values():
        assert e.queue_ahead("c1") == 1200
    d_c = delta(3 * MS, "yes", 4500, -600)  # someone cancels 600 (level excl us: 1200 -> 600)
    book.apply_delta(d_c)
    cal.on_book_delta(d_c)
    for e in cal.estimators.values():
        e.advance(3 * MS + W + 1)
    cal.ingest_exchange_queue_position("c1", 600, 4 * MS)
    s = cal.summary()
    assert s["optimistic"]["mean_error"] == 0 and s["realistic"]["mean_error"] == 0
    assert s["conservative"]["mean_error"] == 0  # clamp: only 600 others remain
    assert s["optimistic"]["n"] == 1


@settings(max_examples=60, deadline=None)
@given(st.lists(st.tuples(st.sampled_from(["add", "cxl", "trade"]), st.integers(1, 400), st.booleans(),
                          st.integers(0, 400)), min_size=1, max_size=40))
def test_estimator_invariants_random(ops):
    """q stays within [0, level + pending], and ordering A <= B <= C holds at every step."""
    f = BookFeed()
    f.snap(snap(0, yes=((4500, 1000),)))
    f.add("o1", "bid", 4500, 500)
    ts = 0
    for kind, qty, trade_first, gap in ops:
        ts += gap * MS
        lvl = f.book.bid_qty(4500)
        if kind == "add":
            f.delta(delta(ts, "yes", 4500, qty))
        elif kind == "cxl" and lvl > 0:
            f.delta(delta(ts, "yes", 4500, -min(qty, lvl)))
        elif kind == "trade" and lvl > 0:
            v = min(qty, lvl)
            if trade_first:
                f.trade(trade(ts, 4500, v, "no"))
                f.delta(delta(ts, "yes", 4500, -v))
            else:
                f.delta(delta(ts, "yes", 4500, -v))
                f.trade(trade(ts, 4500, v, "no"))
        qa = f.qa("o1")
        assert 0 <= qa["optimistic"] <= qa["realistic"] <= qa["conservative"]
    for e in f.q.values():
        e.advance(ts + 10 * W)
    qa = f.qa("o1")
    assert 0 <= qa["optimistic"] <= qa["realistic"] <= qa["conservative"] <= f.book.bid_qty(4500)
