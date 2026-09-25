from __future__ import annotations

import math

import pytest

from dh.core.book import ExtBook
from dh.feeds.composite import (
    BrtiParams,
    brti_from_levels,
    brti_replica,
    consolidated_levels,
    depth_weighted_mid,
    median_mid,
    nowcast,
    price_volume_segments,
    screen_quotes,
    size_cap,
    venue_quote,
)

NOW = 1_790_337_600_000_000_000


def book(venue, bids, asks, ts=NOW):
    b = ExtBook(venue, "BTC-USD")
    b.snapshot(bids, asks, ts)
    return b


def test_venue_quote_mid_micro_spread_depth():
    b = book("x", [(100.0, 1.0), (99.0, 2.0)], [(101.0, 3.0), (102.0, 1.0)], ts=NOW - 5_000_000)
    q = venue_quote("x", b, NOW, depth_bps=150)
    assert q.mid == 100.5 and q.microprice == pytest.approx((100 * 3 + 101 * 1) / 4)
    assert q.spread_bps == pytest.approx(1 / 100.5 * 1e4) and q.age_ms == pytest.approx(5.0)
    assert q.depth_bid_usd == pytest.approx(100 + 198) and q.depth_ask_usd == pytest.approx(303 + 102)
    assert venue_quote("y", book("y", [], [(1.0, 1.0)]), NOW) is None


def test_screens_and_simple_nowcasts():
    qs = [
        venue_quote("a", book("a", [(100.0, 1)], [(100.2, 1)]), NOW),
        venue_quote("b", book("b", [(100.1, 3)], [(100.3, 3)]), NOW),
        venue_quote("c", book("c", [(100.0, 1)], [(100.4, 1)], ts=NOW - 60 * 10**9), NOW),  # stale
        venue_quote("d", book("d", [(110.0, 1)], [(110.2, 1)]), NOW),  # 10% away
        venue_quote("e", book("e", [(100.3, 1)], [(100.1, 1)]), NOW),  # crossed
    ]
    s = screen_quotes(qs, stale_after_ms=30_000, deviation_limit=0.05)
    status = {q.venue: (q.included, q.reason.split(" ")[0]) for q in s}
    assert status == {"a": (True, ""), "b": (True, ""), "c": (False, "stale"), "d": (False, "deviation"), "e": (False, "crossed")}
    assert median_mid(s) == pytest.approx(100.15)
    # depth-weighted: b has 3x the depth of a
    wa, wb = 100.0 * 1, 100.1 * 3
    assert depth_weighted_mid(s) == pytest.approx((wa * 100.1 + wb * 100.2) / (wa + wb))


def test_price_volume_segments():
    bids = [(100.0, 1.0), (99.0, 2.0)]
    asks = [(101.0, 2.0), (103.0, 1.0)]
    assert price_volume_segments(bids, asks) == [(0.0, 1.0, 100.0, 101.0), (1.0, 2.0, 99.0, 101.0), (2.0, 3.0, 99.0, 103.0)]
    assert price_volume_segments([], asks) == []


def test_brti_integral_exact():
    bids = [(100.0, 1.0), (99.0, 2.0)]
    asks = [(101.0, 2.0), (103.0, 1.0)]
    p = BrtiParams(spread_threshold=0.05, lambda_factor=0.3)
    value, vT, lam, n = brti_from_levels(bids, asks, p)
    assert vT == 3.0 and lam == pytest.approx(1 / 0.9) and n == 3
    e = lambda v: math.exp(-lam * v)  # noqa: E731
    expected = (100.5 * (e(0) - e(1)) + 100.0 * (e(1) - e(2)) + 101.0 * (e(2) - e(3))) / (1 - e(3))
    assert value == pytest.approx(expected, rel=1e-15)
    # tighter spread threshold: the third segment (midSV = 103/101 - 1 ~ 1.98%) is excluded
    value2, vT2, lam2, _ = brti_from_levels(bids, asks, BrtiParams(spread_threshold=0.015))
    assert vT2 == 2.0
    e2 = lambda v: math.exp(-lam2 * v)  # noqa: E731
    assert value2 == pytest.approx((100.5 * (1 - e2(1)) + 100.0 * (e2(1) - e2(2))) / (1 - e2(2)))
    # touch spread above threshold -> falls back to the touch mid
    v3, vT3, _, _ = brti_from_levels(bids, asks, BrtiParams(spread_threshold=0.001))
    assert vT3 == 0.0 and v3 == 100.5


def test_size_cap_limits_outsized_orders():
    bids = [(100.0 - 0.01 * i, 1.0) for i in range(100)]
    asks = [(100.01 + 0.01 * i, 1.0) for i in range(100)]
    asks[0] = (100.01, 1000.0)
    cap = size_cap(bids, asks, BrtiParams(size_cap_k=5.0, size_cap_band=0.01))
    assert 1.0 < cap < 1000.0
    assert size_cap(bids, asks, BrtiParams(size_cap_k=0)) == math.inf
    books = {"a": book("a", bids, asks)}
    _b, a, c = consolidated_levels(books, BrtiParams(size_cap_k=5.0, size_cap_band=0.01))
    assert c == pytest.approx(cap) and a[0] == (100.01, pytest.approx(cap))


def test_replica_invariances_and_exclusion():
    bids = [(84500.0 - i, 0.5 + 0.1 * i) for i in range(30)]
    asks = [(84501.0 + i, 0.4 + 0.1 * i) for i in range(30)]
    p = BrtiParams(size_cap_k=0.0)
    one = brti_replica({"a": book("a", bids, asks)}, NOW, p)
    two = brti_replica({"a": book("a", bids, asks), "b": book("b", bids, asks)}, NOW, p)
    assert one.value == pytest.approx(two.value, rel=1e-12)  # duplicating a venue changes nothing
    assert two.utilized_depth == pytest.approx(2 * one.utilized_depth)
    assert 84500.0 < one.value < 84501.0 and one.venues == ("a",)
    far = book("z", [(92000.0, 5.0)], [(92001.0, 5.0)])
    stale = book("s", [(84000.0, 50.0)], [(84001.0, 50.0)], ts=NOW - 120 * 10**9)
    many = {"a": book("a", bids, asks), "b": book("b", bids, asks), "c": book("c", bids, asks), "z": far, "s": stale}
    res = brti_replica(many, NOW, p)
    assert res.venues == ("a", "b", "c") and res.value == pytest.approx(one.value, rel=1e-12)
    # with only two live venues a median screen cannot tell which one is wrong: both stay
    assert brti_replica({"a": book("a", bids, asks), "z": far}, NOW, p).venues == ("a", "z")


def test_nowcast_bundle():
    books = {
        "coinbase": book("coinbase", [(84500.0, 1.0), (84499.0, 2.0)], [(84501.0, 1.0), (84502.0, 2.0)]),
        "kraken": book("kraken", [(84499.5, 0.5)], [(84500.5, 0.5)]),
    }
    nc = nowcast(books, NOW)
    assert nc.median_mid == pytest.approx((84500.5 + 84500.0) / 2)
    assert nc.brti is not None and 84499.5 <= nc.brti.value <= 84501.0
    assert {q.venue for q in nc.quotes} == {"coinbase", "kraken"}
    assert nowcast({}, NOW).brti is None
