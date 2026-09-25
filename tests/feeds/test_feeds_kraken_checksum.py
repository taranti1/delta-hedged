"""Kraken v2 CRC32 book checksum (documented algorithm and example)."""

from __future__ import annotations

import json
import zlib
from decimal import Decimal

import orjson

from dh.core.events import ExtBookDelta, ExtBookSnapshot, FeedStatus
from dh.feeds.base import DepthBook, make_marker
from dh.feeds.kraken import KrakenFeed, book_checksum
from tests.feeds.fixtures._generate import KR_ASKS, KR_BIDS, kraken_ref_checksum

DOC_CHECKSUM = 3310070434  # docs.kraken.com v2 book snapshot example


def _book(bids, asks, depth=10) -> DepthBook:
    b = DepthBook(depth)
    for p, q in bids:
        b.set("b", Decimal(p), Decimal(q))
    for p, q in asks:
        b.set("a", Decimal(p), Decimal(q))
    return b


def test_documented_example():
    assert book_checksum(_book(KR_BIDS, KR_ASKS)) == DOC_CHECKSUM
    assert kraken_ref_checksum(KR_BIDS, KR_ASKS) == DOC_CHECKSUM


def test_checksum_string_construction():
    # asks ascending then bids descending; '.' removed and leading zeros stripped
    b = _book([("0.5657", "1098.39475580")], [("0.5660", "10.00000000")])
    expected = zlib.crc32(b"56601000000000" + b"5657109839475580") & 0xFFFFFFFF
    assert book_checksum(b) == expected


def test_precision_padding_when_wire_lost_trailing_zeros():
    # a JSON encoder that drops trailing zeros ("0.1" instead of "0.10000000") still verifies
    stripped_b = [(p, str(Decimal(q).normalize())) for p, q in KR_BIDS]
    stripped_a = [(p, str(Decimal(q).normalize())) for p, q in KR_ASKS]
    b = _book(stripped_b, stripped_a)
    assert book_checksum(b) != DOC_CHECKSUM
    assert book_checksum(b, (1, 8)) == DOC_CHECKSUM


def _frame(typ, bids, asks, checksum):
    lv = lambda levels: [{"price": p, "qty": q} for p, q in levels]  # noqa: E731
    return json.dumps({"channel": "book", "type": typ, "data": [{"symbol": "BTC/USD", "bids": lv(bids), "asks": lv(asks), "checksum": checksum}]}).encode()


def test_update_checksums_and_mismatch_invalidate_book():
    st = KrakenFeed.new_state()
    KrakenFeed.normalize(make_marker("status", 1, status="connected"), 1, st)
    ev = KrakenFeed.normalize(_frame("snapshot", KR_BIDS, KR_ASKS, DOC_CHECKSUM), 2, st)
    assert isinstance(ev[0], ExtBookSnapshot)
    # a correct update: change the best ask size
    bids, asks = dict(KR_BIDS), dict(KR_ASKS)
    asks["45285.2"] = "0.50000000"
    good = kraken_ref_checksum(list(bids.items()), list(asks.items()))
    ev = KrakenFeed.normalize(_frame("update", [], [("45285.2", "0.50000000")], good), 3, st)
    assert isinstance(ev[0], ExtBookDelta) and st.stats["checksum_ok"] == 2
    # a wrong checksum: gap reported, nothing applied downstream, later deltas suppressed
    ev = KrakenFeed.normalize(_frame("update", [("45283.4", "2.00000000")], [], good), 4, st)
    assert [(e.stream, e.status) for e in ev] == [("kraken.book:BTC/USD", "gap")]
    ev = KrakenFeed.normalize(_frame("update", [("45283.4", "3.00000000")], [], 1), 5, st)
    assert ev == [] and st.stats["deltas_suppressed"] == 1
    # snapshot restores
    ev = KrakenFeed.normalize(_frame("snapshot", KR_BIDS, KR_ASKS, DOC_CHECKSUM), 6, st)
    assert isinstance(ev[0], ExtBookSnapshot) and ev[1] == FeedStatus(6, 0, "kraken.book:BTC/USD", "resynced", "snapshot")


def test_bad_snapshot_checksum_is_a_gap():
    st = KrakenFeed.new_state()
    ev = KrakenFeed.normalize(_frame("snapshot", KR_BIDS, KR_ASKS, DOC_CHECKSUM + 1), 2, st)
    assert [e.status for e in ev] == ["gap"] and st.stats["checksum_fail"] == 1


def test_depth_inferred_from_snapshot_or_subscribe_ack():
    st = KrakenFeed.new_state()
    KrakenFeed.normalize(_frame("snapshot", KR_BIDS, KR_ASKS, DOC_CHECKSUM), 2, st)
    assert st.books["BTC/USD"].depth == 10
    ack = orjson.dumps({"method": "subscribe", "result": {"channel": "book", "depth": 25, "snapshot": True, "symbol": "BTC/USD"}, "success": True})
    KrakenFeed.normalize(ack, 3, st)
    KrakenFeed.normalize(_frame("snapshot", KR_BIDS, KR_ASKS, DOC_CHECKSUM), 4, st)
    assert st.books["BTC/USD"].depth == 25
    err = orjson.dumps({"error": "Currency pair not supported", "method": "subscribe", "success": False})
    assert KrakenFeed.normalize(err, 5, st)[0].status == "error"
