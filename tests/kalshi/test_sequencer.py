from __future__ import annotations

import orjson

from dh.core.events import (
    FeedStatus,
    KalshiBookDelta,
    KalshiBookSnapshot,
    KalshiFill,
    KalshiTrade,
)
from dh.kalshi.sequencer import (
    KalshiWsState,
    normalize_ws_frame,
    synthetic_status_frame,
)


def f(obj) -> bytes:
    return orjson.dumps(obj)


def subscribed(cid: int, channel: str, sid: int) -> bytes:
    return f({"id": cid, "type": "subscribed", "msg": {"channel": channel, "sid": sid}})


def snap(sid: int, seq: int, ticker: str, yes=(("0.4500", "10.00"),), no=(("0.5300", "5.00"),)) -> bytes:
    return f({"type": "orderbook_snapshot", "sid": sid, "seq": seq,
              "msg": {"market_ticker": ticker, "market_id": "m", "yes_dollars_fp": [list(x) for x in yes], "no_dollars_fp": [list(x) for x in no]}})


def delta(sid: int, seq: int, ticker: str, px="0.4500", d="1.00", side="yes") -> bytes:
    return f({"type": "orderbook_delta", "sid": sid, "seq": seq,
              "msg": {"market_ticker": ticker, "market_id": "m", "price_dollars": px, "delta_fp": d, "side": side, "ts_ms": 1}})


def trade(sid: int, seq: int, tid: str) -> bytes:
    return f({"type": "trade", "sid": sid, "seq": seq, "msg": {
        "trade_id": tid, "market_ticker": "A", "yes_price_dollars": "0.4600", "no_price_dollars": "0.5400",
        "count_fp": "1.00", "taker_side": "yes", "taker_outcome_side": "yes", "taker_book_side": "bid",
        "is_block_trade": False, "ts": 1, "ts_ms": 1000}})


def run(frames: list[bytes], state: KalshiWsState | None = None) -> tuple[list, KalshiWsState]:
    st = state or KalshiWsState()
    out = []
    for i, fr in enumerate(frames):
        out.extend(normalize_ws_frame(fr, 1_000 + i, st))
    return out, st


def statuses(evs, stream=None):
    return [(e.stream, e.status) for e in evs if isinstance(e, FeedStatus) and (stream is None or e.stream == stream)]


def test_in_order_stream_passes_through():
    evs, st = run([subscribed(1, "orderbook_delta", 1), snap(1, 1, "A"), delta(1, 2, "A"), delta(1, 3, "A", d="-1.00")])
    assert [type(e) for e in evs] == [KalshiBookSnapshot, KalshiBookDelta, KalshiBookDelta]
    assert st.sids[1].last_seq == 3 and st.counters["gaps"] == 0


def test_duplicates_and_out_of_order_suppressed():
    evs, st = run([snap(1, 1, "A"), delta(1, 2, "A"), delta(1, 2, "A"), delta(1, 1, "A"), delta(1, 3, "A")])
    assert [e.seq for e in evs] == [1, 2, 3]
    assert st.counters["dups"] == 2


def test_gap_invalidates_books_requests_resync_and_recovers():
    frames = [subscribed(1, "orderbook_delta", 1), snap(1, 1, "A"), snap(1, 2, "B"), delta(1, 3, "A"),
              delta(1, 6, "A"),            # gap: 4,5 missed
              delta(1, 7, "B"),            # B invalid -> suppressed
              snap(1, 8, "A"),             # A resynced
              delta(1, 9, "A"),            # A flows again
              delta(1, 10, "B"),           # B still invalid
              snap(1, 11, "B"), delta(1, 12, "B")]
    st = KalshiWsState()
    evs, _ = run(frames[:5], st)
    assert statuses(evs) == [("kalshi.ws", "gap"), ("kalshi.book:A", "gap"), ("kalshi.book:B", "gap")]
    gap = [e for e in evs if isinstance(e, FeedStatus) and e.stream == "kalshi.ws"][0]
    assert "expected=4 got=6 missed=2" in gap.detail
    assert st.take_resync_requests() == [(1, ("A", "B"))]
    assert st.take_resync_requests() == []
    assert not any(isinstance(e, KalshiBookDelta) and e.seq == 6 for e in evs)
    evs2, _ = run(frames[5:], st)
    kinds = [(type(e).__name__, getattr(e, "ticker", getattr(e, "stream", ""))) for e in evs2]
    assert kinds == [
        ("KalshiBookSnapshot", "A"), ("FeedStatus", "kalshi.book:A"),
        ("KalshiBookDelta", "A"),
        ("KalshiBookSnapshot", "B"), ("FeedStatus", "kalshi.book:B"), ("FeedStatus", "kalshi.ws"),
        ("KalshiBookDelta", "B"),
    ]
    assert statuses(evs2, "kalshi.ws") == [("kalshi.ws", "resynced")]
    assert st.counters["suppressed_deltas"] == 3 and st.invalid_books() == {}


def test_delta_before_snapshot_triggers_resync():
    evs, st = run([subscribed(1, "orderbook_delta", 1), delta(1, 1, "A")])
    assert statuses(evs) == [("kalshi.book:A", "gap")]
    assert st.take_resync_requests() == [(1, ("A",))]


def test_gap_on_trade_channel_keeps_the_message():
    evs, st = run([subscribed(2, "trade", 5), trade(5, 1, "t1"), trade(5, 4, "t4")])
    assert statuses(evs) == [("kalshi.ws", "gap")]
    assert [e.trade_id for e in evs if isinstance(e, KalshiTrade)] == ["t1", "t4"]
    assert st.take_resync_requests() == []


def test_ok_and_error_responses_consume_sequence_numbers(asyncapi_examples):
    ok = dict(asyncapi_examples["okResponse"][0], sid=1, seq=3)
    evs, st = run([snap(1, 1, "A"), delta(1, 2, "A"), f(ok), delta(1, 4, "A")])
    assert st.counters["gaps"] == 0 and len([e for e in evs if isinstance(e, KalshiBookDelta)]) == 2


def test_mid_stream_start_with_ok_first_still_resyncs_books(asyncapi_examples):
    """Replay from the middle of a session: no 'subscribed' frame, an 'ok' is the sid's first message."""
    ok = dict(asyncapi_examples["okResponse"][0], sid=4, seq=10)
    evs, st = run([f(ok), delta(4, 11, "A"), snap(4, 12, "A"), delta(4, 13, "A"), delta(4, 20, "A")])
    assert st.sids[4].channel == "orderbook_delta"
    assert ("kalshi.book:A", "gap") in statuses(evs)  # delta before snapshot, then the seq gap
    assert st.take_resync_requests() == [(4, ("A",)), (4, ("A",))]
    assert st.sid_for_ticker("A") == 4


def test_fill_dedupe(asyncapi_examples):
    fill = f(asyncapi_examples["fill"][0])
    evs, st = run([fill, fill])
    assert len([e for e in evs if isinstance(e, KalshiFill)]) == 1 and st.counters["dup_fills"] == 1


def test_synthetic_connection_records_reset_state():
    frames = [synthetic_status_frame("connected", "wss://x"), subscribed(1, "orderbook_delta", 1), snap(1, 1, "A"),
              delta(1, 2, "A"), synthetic_status_frame("disconnected", "boom"),
              synthetic_status_frame("connected", "wss://x"), subscribed(1, "orderbook_delta", 1),
              snap(1, 1, "A")]  # sid 1 reused, seq restarts at 1: not a duplicate
    evs, st = run(frames)
    assert statuses(evs) == [("kalshi.ws", "connected"), ("kalshi.book:A", "disconnected"),
                             ("kalshi.ws", "disconnected"), ("kalshi.ws", "connected")]
    assert [type(e) for e in evs if not isinstance(e, FeedStatus)] == [KalshiBookSnapshot, KalshiBookDelta, KalshiBookSnapshot]
    assert st.counters["dups"] == 0


def test_subscribed_resets_reused_sid_without_synthetic_records():
    evs, st = run([snap(1, 1, "A"), delta(1, 2, "A"), subscribed(9, "orderbook_delta", 1), snap(1, 1, "A")])
    assert st.counters["dups"] == 0 and isinstance(evs[-1], KalshiBookSnapshot)


def test_malformed_frames():
    evs, st = run([b"{not json", f([1, 2]), snap(1, 1, "A"), delta(1, 2, "A", px="0.00001")])
    assert [e.status for e in evs if isinstance(e, FeedStatus) and e.stream == "kalshi.ws"] == ["error", "error", "error"]
    assert ("kalshi.book:A", "gap") in statuses(evs)
    assert st.take_resync_requests() == [(1, ("A",))]


def test_replay_is_deterministic():
    frames = [synthetic_status_frame("connected"), subscribed(1, "orderbook_delta", 1), snap(1, 1, "A"), delta(1, 2, "A"),
              delta(1, 5, "A"), snap(1, 6, "A"), subscribed(2, "trade", 2), trade(2, 1, "x")]
    a, _ = run(frames)
    b, _ = run(frames)
    assert a == b and len(a) > 5
