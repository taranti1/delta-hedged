from __future__ import annotations

import random
from decimal import Decimal

import orjson
import pytest

from dh.core.events import ExtBookSnapshot, FeedStatus
from dh.feeds.base import (
    Backoff,
    DepthBook,
    FeedConfig,
    NormalizerState,
    book_gap,
    book_snapshot_ok,
    handle_marker,
    is_marker,
    make_marker,
    ms_to_ns,
    parse_rfc3339_ns,
    safe_normalize,
    snapshot_event,
    to_dec,
    trade_seen,
)
from dh.feeds.registry import VENUES, build_feed, feed_class, normalizer_for, venue_of


def test_parse_rfc3339_ns_variants():
    base = 1790337600 * 10**9  # 2026-09-25T12:00:00Z
    assert parse_rfc3339_ns("2026-09-25T12:00:00Z") == base
    assert parse_rfc3339_ns("2026-09-25T12:00:00.714964855Z") == base + 714964855
    assert parse_rfc3339_ns("2026-09-25T12:00:00.5Z") == base + 500_000_000
    assert parse_rfc3339_ns("2026-09-25T12:00:00.123456Z") == base + 123456000
    assert parse_rfc3339_ns("2026-09-25T14:00:00.000001+02:00") == base + 1000
    assert parse_rfc3339_ns("2026-09-25 12:00:00.25") == base + 250_000_000
    assert parse_rfc3339_ns("1970-01-01T00:00:00Z") == 0
    # more than 9 fractional digits are truncated, never rounded
    assert parse_rfc3339_ns("2026-09-25T12:00:00.1234567899Z") == base + 123456789


def test_ms_to_ns():
    assert ms_to_ns(1_700_000_000_123) == 1_700_000_000_123_000_000
    assert ms_to_ns("1700000000123") == 1_700_000_000_123_000_000
    assert ms_to_ns("1700000000123.5") == 1_700_000_000_123_500_000
    assert ms_to_ns(1.5) == 1_500_000


def test_to_dec_exact():
    assert to_dec("0.10000000") == Decimal("0.1") and str(to_dec("0.10000000")) == "0.10000000"
    assert to_dec(45283.5) == Decimal("45283.5")
    assert to_dec(3) == Decimal(3)


def test_depth_book_truncate_and_roundtrip():
    b = DepthBook(depth=2)
    for p, q in (("100.0", "1"), ("99.5", "2"), ("99.0", "3")):
        b.set("b", Decimal(p), Decimal(q))
    for p, q in (("101.0", "1"), ("102.0", "2"), ("103.00", "0.50")):
        b.set("a", Decimal(p), Decimal(q))
    removed = b.truncate()
    assert sorted(removed) == [("a", 103.0, 0.0), ("b", 99.0, 0.0)]
    assert b.top_bids(5) == [(Decimal("100.0"), Decimal("1")), (Decimal("99.5"), Decimal("2"))]
    assert b.top_asks(1) == [(Decimal("101.0"), Decimal("1"))]
    b.set("b", Decimal("100.0"), Decimal("0"))
    assert b.top_bids(1)[0][0] == Decimal("99.5")
    d = orjson.loads(orjson.dumps(b.to_dict()))
    assert DepthBook.from_dict(d) == b
    bids, asks = b.snapshot_levels()
    assert bids == ((99.5, 2.0),) and asks == ((101.0, 1.0), (102.0, 2.0))


def test_normalizer_state_roundtrip_is_json():
    st = NormalizerState(stream="kraken.ws", venue="kraken")
    st.books["BTC/USD"] = DepthBook(10)
    st.books["BTC/USD"].set("b", Decimal("1.50"), Decimal("2.000"))
    st.seq["x"] = 5
    st.buffers["btcusd"] = [[1, [["1", "2"]], []]]
    st.meta["depth"] = {"BTC/USD": 10}
    st.bump("gaps")
    blob = st.to_json()
    again = NormalizerState.from_dict(orjson.loads(blob))
    assert again.to_json() == blob
    assert again.books["BTC/USD"] == st.books["BTC/USD"]


def test_snapshot_event_sorting_and_zero_drop():
    ev = snapshot_event(1, 0, "v", "S", [(99.0, 1.0), (100.0, 2.0), (98.0, 0.0)], [(102.0, 1.0), (101.0, 3.0)])
    assert ev.bids == ((100.0, 2.0), (99.0, 1.0)) and ev.asks == ((101.0, 3.0), (102.0, 1.0))


def test_markers_reset_state_and_emit_status():
    st = NormalizerState(stream="x.ws", venue="x")
    st.seq["conn"] = 9
    st.valid["x.book:S"] = True
    raw = make_marker("status", 1, status="connected", url="wss://x")
    assert is_marker(raw) and not is_marker(b'{"channel":"l2"}')
    evs = handle_marker(raw, 123, st)
    assert evs == [FeedStatus(123, 0, "x.ws", "connected", "wss://x")]
    assert st.seq == {} and st.valid["x.book:S"] is False and st.conn == 1 and st.conn_ns == 123
    assert handle_marker(make_marker("status", 1, status="stale", detail="no frame"), 5, st)[0].status == "stale"
    resumed = handle_marker(make_marker("status", 1, status="resumed"), 6, st)[0]
    assert resumed.status == "connected" and resumed.detail.startswith("resumed")
    assert handle_marker(make_marker("status", 1, status="disconnected", detail="bye"), 7, st)[0].status == "disconnected"
    assert handle_marker(make_marker("sent", 1, msg="{}"), 8, st) == []


def test_gap_reported_once_then_resynced():
    st = NormalizerState(stream="x.ws", venue="x")
    st.valid["x.book:S"] = True
    first = book_gap(st, "S", 10, "seq 1 -> 3")
    assert first == [FeedStatus(10, 0, "x.book:S", "gap", "seq 1 -> 3")]
    assert book_gap(st, "S", 11, "again") == []  # not re-reported while waiting
    assert st.stats["gaps"] == 2
    ok = book_snapshot_ok(st, "S", 12)
    assert ok == [FeedStatus(12, 0, "x.book:S", "resynced", "snapshot")]
    assert book_snapshot_ok(st, "S", 13) == []  # plain snapshot: no status


def test_trade_seen_bounded():
    st = NormalizerState()
    assert not trade_seen(st, "S", 0, "1")
    assert trade_seen(st, "S", 0, "1")
    for i in range(2, 100):
        trade_seen(st, "S", 0, str(i), keep=10)
    assert len(st.cursors["S"]) == 10
    assert not trade_seen(st, "S", 0, "")  # empty ids are never de-duplicated


def test_safe_normalize_turns_exceptions_into_status():
    def bad(raw, t, st):
        raise ValueError("boom")

    st = NormalizerState(stream="v.ws", venue="v")
    evs = safe_normalize(bad, b"{garbage", 5, st)
    assert len(evs) == 1 and evs[0].status == "error" and "boom" in evs[0].detail
    assert st.stats["normalize_errors"] == 1
    # every real normalizer survives garbage deterministically
    for venue in VENUES:
        cls = feed_class(venue)
        s1, s2 = cls.new_state(f"{venue}.ws"), cls.new_state(f"{venue}.ws")
        assert safe_normalize(cls.normalize, b"\x00not json", 1, s1) == safe_normalize(cls.normalize, b"\x00not json", 1, s2)


def test_backoff_bounds_and_reset():
    b = Backoff(1.0, 8.0, random.Random(7))
    delays = [b.next() for _ in range(6)]
    caps = [1, 2, 4, 8, 8, 8]
    assert all(c / 2 <= d <= c for d, c in zip(delays, caps))
    b.reset()
    assert b.next() <= 1.0


def test_feed_config_from_mapping_and_registry():
    cfg = FeedConfig.from_mapping({"stream": "deribit.options", "symbols": ["BTC-PERPETUAL"], "channels": ["options"], "option_expiries": 3})
    assert cfg.symbols == ("BTC-PERPETUAL",) and cfg.options == {"option_expiries": 3}
    feed = build_feed("deribit_options", {"venue": "deribit", "stream": "deribit.options", "channels": ["options"], "option_expiries": 3})
    assert feed.name == "deribit.options" and feed.options["option_expiries"] == 3
    with pytest.raises(ValueError):
        build_feed("coinbase", {"stream": "kraken.ws"})
    assert venue_of("deribit.options") == "deribit"
    fn, st = normalizer_for("deribit.options")
    assert st.stream == "deribit.options" and st.venue == "deribit"
    with pytest.raises(KeyError):
        feed_class("nope")


def test_every_feed_builds_subscriptions():
    for venue in VENUES:
        cls = feed_class(venue)
        f = cls()
        msgs = f.subscribe_messages()
        for m in msgs:
            orjson.loads(m)  # valid JSON commands
        if cls.implemented and venue not in ("binance_futures", "paxos"):
            assert msgs, venue


def test_stub_feeds_raise():
    from dh.feeds.kalshi_perp import KalshiPerpFeed
    from dh.feeds.stubs import BullishFeed, LmaxFeed

    for cls in (KalshiPerpFeed, BullishFeed, LmaxFeed):
        with pytest.raises(NotImplementedError):
            cls.normalize(b"{}", 0, cls.new_state())
        assert cls.implemented is False


def test_snapshot_event_type():
    assert isinstance(snapshot_event(1, 0, "v", "s", [], []), ExtBookSnapshot)
