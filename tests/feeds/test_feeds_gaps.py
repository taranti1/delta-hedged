"""Sequence-gap detection: gap -> FeedStatus('gap') on '<venue>.book:<symbol>', deltas
suppressed until the next snapshot, then FeedStatus('resynced')."""

from __future__ import annotations

import orjson

from dh.core.events import ExtBookDelta, ExtBookSnapshot, FeedStatus
from dh.feeds.base import make_marker
from dh.feeds.coinbase import CoinbaseFeed
from dh.feeds.cryptocom import CryptoComFeed
from dh.feeds.deribit import DeribitFeed

CONNECT = make_marker("status", 1, status="connected", url="wss://x")


def _types(evs):
    return [type(e).__name__ if not isinstance(e, FeedStatus) else f"FeedStatus:{e.status}" for e in evs]


def _cb(seq, typ="update", updates=None):
    updates = updates or [{"side": "bid", "event_time": "2026-09-25T12:00:00Z", "price_level": "100.00", "new_quantity": "1"}]
    return orjson.dumps({"channel": "l2_data", "client_id": "", "timestamp": "2026-09-25T12:00:00.000000001Z", "sequence_num": seq,
                         "events": [{"type": typ, "product_id": "BTC-USD", "updates": updates}]})


def test_coinbase_sequence_gap_duplicate_and_resync():
    st = CoinbaseFeed.new_state()
    run = lambda raw, t: CoinbaseFeed.normalize(raw, t, st)  # noqa: E731
    run(CONNECT, 1)
    snap_updates = [{"side": "bid", "event_time": "x", "price_level": "100.00", "new_quantity": "1"},
                    {"side": "offer", "event_time": "x", "price_level": "101.00", "new_quantity": "1"}]
    assert _types(run(_cb(1, "snapshot", snap_updates), 2)) == ["ExtBookSnapshot"]
    assert _types(run(_cb(2), 3)) == ["ExtBookDelta"]
    assert run(_cb(2), 4) == [] and st.stats["stale_seq_dropped"] == 1  # duplicate ignored
    assert _types(run(_cb(5), 5)) == ["FeedStatus:gap"]  # 2 -> 5
    assert run(_cb(6), 6) == []  # suppressed
    assert _types(run(_cb(7, "snapshot", snap_updates), 7)) == ["ExtBookSnapshot", "FeedStatus:resynced"]
    assert _types(run(_cb(8), 8)) == ["ExtBookDelta"]
    # a heartbeat also advances the connection-level counter
    hb = orjson.dumps({"channel": "heartbeats", "client_id": "", "timestamp": "2026-09-25T12:00:01Z", "sequence_num": 9,
                       "events": [{"current_time": "x", "heartbeat_counter": 5}]})
    assert run(hb, 9) == []
    assert _types(run(_cb(10), 10)) == ["ExtBookDelta"]
    # reconnect: counters restart without a false gap
    run(make_marker("status", 2, status="connected"), 11)
    assert _types(run(_cb(0, "snapshot", snap_updates), 12)) == ["ExtBookSnapshot"]


def test_coinbase_client_resubscribes_level2():
    msgs = [orjson.loads(m) for m in CoinbaseFeed().resubscribe_messages()]
    assert [m["type"] for m in msgs] == ["unsubscribe", "subscribe"] and all(m["channel"] == "level2" for m in msgs)


def _cc(ch, data):
    return orjson.dumps({"id": -1, "method": "subscribe", "code": 0,
                         "result": {"instrument_name": "BTC_USD", "subscription": "book.BTC_USD.50", "channel": ch, "depth": 50, "data": data}})


def test_cryptocom_pu_chain():
    st = CryptoComFeed.new_state()
    run = lambda raw, t: CryptoComFeed.normalize(raw, t, st)  # noqa: E731
    run(CONNECT, 1)
    run(_cc("book", [{"bids": [["100", "1", "1"]], "asks": [["101", "1", "1"]], "t": 1, "u": 10}]), 2)
    assert _types(run(_cc("book.update", [{"update": {"bids": [["100", "2", "1"]], "asks": []}, "t": 2, "u": 11, "pu": 10}]), 3)) == ["ExtBookDelta"]
    # empty keep-alive delta keeps the chain
    assert run(_cc("book.update", [{"update": {"bids": [], "asks": []}, "t": 3, "u": 12, "pu": 11}]), 4) == []
    assert _types(run(_cc("book.update", [{"update": {"bids": [["99", "1", "1"]], "asks": []}, "t": 4, "u": 14, "pu": 13}]), 5)) == ["FeedStatus:gap"]
    assert run(_cc("book.update", [{"update": {"bids": [["98", "1", "1"]], "asks": []}, "t": 5, "u": 15, "pu": 14}]), 6) == []
    out = run(_cc("book", [{"bids": [["100", "1", "1"]], "asks": [["101", "1", "1"]], "t": 6, "u": 20}]), 7)
    assert _types(out) == ["ExtBookSnapshot", "FeedStatus:resynced"]


def test_cryptocom_truncates_to_depth():
    st = CryptoComFeed.new_state()
    CryptoComFeed.normalize(CONNECT, 1, st)
    bids = [[str(100 - i), "1", "1"] for i in range(50)]
    CryptoComFeed.normalize(_cc("book", [{"bids": bids, "asks": [["101", "1", "1"]], "t": 1, "u": 1}]), 2, st)
    out = CryptoComFeed.normalize(_cc("book.update", [{"update": {"bids": [["100.5", "1", "1"]], "asks": []}, "t": 2, "u": 2, "pu": 1}]), 3, st)
    assert isinstance(out[0], ExtBookDelta) and out[0].changes == (("b", 100.5, 1.0), ("b", 51.0, 0.0))


def _db(typ, cid, prev=None, bids=(), asks=()):
    d = {"type": typ, "timestamp": 1, "instrument_name": "BTC-PERPETUAL", "change_id": cid, "bids": list(bids), "asks": list(asks)}
    if prev is not None:
        d["prev_change_id"] = prev
    return orjson.dumps({"jsonrpc": "2.0", "method": "subscription", "params": {"channel": "book.BTC-PERPETUAL.100ms", "data": d}})


def test_deribit_change_id_chain():
    st = DeribitFeed.new_state()
    run = lambda raw, t: DeribitFeed.normalize(raw, t, st)  # noqa: E731
    run(CONNECT, 1)
    assert isinstance(run(_db("snapshot", 100, bids=[["new", 100.0, 1000]], asks=[["new", 101.0, 1000]]), 2)[0], ExtBookSnapshot)
    assert _types(run(_db("change", 105, 100, bids=[["change", 100.0, 2000]]), 3)) == ["ExtBookDelta"]
    assert run(_db("change", 105, 104), 4) == []  # stale duplicate
    assert _types(run(_db("change", 110, 107), 5)) == ["FeedStatus:gap"]
    assert run(_db("change", 111, 110), 6) == []
    assert _types(run(_db("snapshot", 200, bids=[["new", 100.0, 1000]], asks=[["new", 101.0, 1000]]), 7)) == ["ExtBookSnapshot", "FeedStatus:resynced"]
    rs = [orjson.loads(m) for m in DeribitFeed().resubscribe_messages()]
    assert [r["method"] for r in rs] == ["public/unsubscribe", "public/subscribe"]
