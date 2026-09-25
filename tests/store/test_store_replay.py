"""Replay: k-way merge ordering, determinism, normalizer dispatch (incl. lazy Kalshi import),
warm-up priming."""

from __future__ import annotations

import sys
import types

import orjson
import pytest

from dh.core.events import ExtBookSnapshot, ExtTrade, FeedStatus, IndexTick
from dh.feeds.base import safe_normalize
from dh.feeds.registry import normalizer_for
from dh.store.recorder import HOUR_NS, Recorder
from dh.store.replay import (
    Normalizers,
    ReadStats,
    ReplayError,
    iter_events,
    iter_raw,
    iter_records_events,
    list_streams,
    resolve_streams,
)
from tests.feeds.helpers import T0_NS, VENUE_FIXTURES, load_fixture

END = 2**63 - 1


def build_store(root, names=VENUE_FIXTURES, extra=()):
    rec = Recorder(root, start=False)
    for name in names:
        for t, s, _q, raw in load_fixture(name):
            rec.write(s, t, raw)
    for s, t, raw in extra:
        rec.write(s, t, raw)
    rec.close()
    return root


def test_kway_merge_order_and_stream_rank(tmp_path):
    rec = Recorder(tmp_path, start=False)
    rec.write("b.ws", 100, b"b1")
    rec.write("a.ws", 100, b"a1")  # same t: rank decides
    rec.write("a.ws", 50, b"a2")  # clock stepped back inside stream a
    rec.write("b.ws", 160, b"b2")
    rec.write("a.ws", 200, b"a3")
    rec.write("c.ws", 150, b"c1")
    rec.close()
    order = [r.data for r in iter_raw(tmp_path, ["b.ws", "a.ws", "c.ws"], 0, END)]
    # b1(100,r0) vs a1(100,r1): b1 first; a2 (t=50) cannot jump ahead of a1 in its own stream
    assert order == [b"b1", b"a1", b"a2", b"c1", b"b2", b"a3"]
    order2 = [r.data for r in iter_raw(tmp_path, ["a.ws", "b.ws", "c.ws"], 0, END)]
    assert order2 == [b"a1", b"a2", b"b1", b"c1", b"b2", b"a3"]
    stats = ReadStats()
    list(iter_raw(tmp_path, None, 0, END, stats))
    assert stats.time_order_violations == 1 and stats.records == 6
    # streams=None -> sorted names
    assert [r.data for r in iter_raw(tmp_path, None, 0, END)] == order2


def test_time_window_and_patterns(tmp_path):
    rec = Recorder(tmp_path, start=False)
    for h in range(3):
        rec.write("kalshi.rest.markets", T0_NS + h * HOUR_NS + 1, b"m%d" % h)
        rec.write("kalshi.rest.orderbook", T0_NS + h * HOUR_NS + 2, b"o%d" % h)
    rec.write("coinbase.ws", T0_NS, b"x")
    rec.close()
    assert list_streams(tmp_path) == ["coinbase.ws", "kalshi.rest.markets", "kalshi.rest.orderbook"]
    assert resolve_streams(tmp_path, ["kalshi.rest.*", "coinbase.ws", "kalshi.rest.markets"]) == [
        "kalshi.rest.markets", "kalshi.rest.orderbook", "coinbase.ws"]
    got = [r.data for r in iter_raw(tmp_path, ["kalshi.rest.*"], T0_NS + HOUR_NS, T0_NS + 2 * HOUR_NS + 2)]
    assert got == [b"m1", b"o1", b"m2"]  # t1 is exclusive
    assert list(iter_raw(tmp_path, ["missing.ws"], 0, END)) == []


def test_iter_events_deterministic_and_equal_to_per_stream_normalization(tmp_path):
    build_store(tmp_path)
    a = list(iter_events(tmp_path, None, 0, END))
    b = list(iter_events(tmp_path, None, 0, END))
    assert a == b and len(a) > 50
    # merged output == each stream normalized alone (same events per stream, same order)
    for name in VENUE_FIXTURES:
        recs = load_fixture(name)
        norm, st = normalizer_for(recs[0][1])
        alone = []
        for t, _s, _q, raw in recs:
            alone += safe_normalize(norm, raw, t, st)
        venue = recs[0][1].split(".")[0]
        merged = [e for e in a if getattr(e, "venue", None) == venue or (isinstance(e, FeedStatus) and e.stream.startswith(venue + "."))
                  or (isinstance(e, IndexTick) and e.index_id.startswith(venue + ":"))]
        assert merged == alone, name
    # global order is by receive time
    ts = [e.ts for e in a]
    assert ts == sorted(ts)


def test_kalshi_dispatch_requires_module(tmp_path, monkeypatch):
    build_store(tmp_path, names=(), extra=[("kalshi.ws", T0_NS, b'{"type":"ticker","sid":1,"msg":{}}'),
                                          ("kalshi.rest.markets", T0_NS, b'{"status":200}')])
    monkeypatch.setitem(sys.modules, "dh.kalshi.sequencer", None)  # both imports fail
    monkeypatch.setitem(sys.modules, "dh.kalshi.normalize", None)
    with pytest.raises(ReplayError, match=r"dh\.kalshi\.normalize"):
        list(iter_events(tmp_path, ["kalshi.ws"], 0, END))
    with pytest.raises(ReplayError, match="normalize_rest_record"):
        list(iter_events(tmp_path, ["kalshi.rest.*"], 0, END))


def test_kalshi_ws_uses_live_sequencer(tmp_path):
    """kalshi.ws is replayed with dh.kalshi.sequencer (same code as the live KalshiWS)."""
    seq = pytest.importorskip("dh.kalshi.sequencer")
    frames = [
        seq.synthetic_status_frame("connected", "wss://kalshi"),
        b'{"type":"subscribed","id":1,"msg":{"channel":"trade","sid":3}}',
        seq.synthetic_status_frame("disconnected", "1006"),
    ]
    build_store(tmp_path, names=(), extra=[("kalshi.ws", T0_NS + i, f) for i, f in enumerate(frames)])
    ev = list(iter_events(tmp_path, ["kalshi.ws"], 0, END))
    assert [(e.stream, e.status) for e in ev] == [("kalshi.ws", "connected"), ("kalshi.ws", "disconnected")]
    live_state = seq.KalshiWsState()
    live = [e for i, f in enumerate(frames) for e in seq.normalize_ws_frame(f, T0_NS + i, live_state)]
    assert ev == live


def test_kalshi_dispatch_with_module(tmp_path, monkeypatch):
    monkeypatch.setitem(sys.modules, "dh.kalshi.sequencer", None)  # stateless fallback path
    calls = []
    fake = types.ModuleType("dh.kalshi.normalize")

    def ws_message_to_events(msg, recv_ns):
        calls.append(("ws", msg, recv_ns))
        if msg.get("type") == "boom":
            raise KeyError("bad")
        return [FeedStatus(recv_ns, 0, "kalshi.ws", "connected", msg["type"])]

    def normalize_rest_record(msg, recv_ns):
        calls.append(("rest", msg, recv_ns))
        return []

    fake.ws_message_to_events = ws_message_to_events
    fake.normalize_rest_record = normalize_rest_record
    monkeypatch.setitem(sys.modules, "dh.kalshi.normalize", fake)
    build_store(tmp_path, names=("coinbase",), extra=[
        ("kalshi.ws", T0_NS + 1, b'{"type":"subscribed","id":1}'),
        ("kalshi.ws", T0_NS + 2, b'{"type":"boom"}'),
        ("kalshi.rest.markets", T0_NS + 3, b'{"markets":[]}'),
    ])
    ev = list(iter_events(tmp_path, ["kalshi.*", "coinbase.ws"], 0, END))
    assert calls == [("ws", {"type": "subscribed", "id": 1}, T0_NS + 1), ("ws", {"type": "boom"}, T0_NS + 2),
                     ("rest", {"markets": []}, T0_NS + 3)]
    kalshi_status = [e for e in ev if isinstance(e, FeedStatus) and e.stream == "kalshi.ws"]
    assert kalshi_status[0].detail == "subscribed" and kalshi_status[1].status == "error"
    assert any(isinstance(e, ExtBookSnapshot) for e in ev)


def test_status_events_and_unknown_streams(tmp_path):
    from dh.store.codec import encode_event

    st = FeedStatus(T0_NS + 5, 0, "kalshi.ws", "disconnected", "1006")
    build_store(tmp_path, names=(), extra=[("status", st.ts, encode_event(st)), ("clock", T0_NS, b'{"src":"unknown"}'),
                                          ("mystery.feed", T0_NS, b"{}")])
    assert list(iter_events(tmp_path, None, 0, END)) == [st]
    with pytest.raises(ReplayError):
        list(iter_events(tmp_path, ["mystery.feed"], 0, END, normalizers=Normalizers(strict=True)))


def test_warmup_primes_books_at_t0(tmp_path):
    build_store(tmp_path)
    t0 = T0_NS + 105_000_000  # after the coinbase snapshot and first delta, before later updates
    streams = ["coinbase.ws", "kraken.ws"]
    primed = list(iter_events(tmp_path, streams, t0, END, warmup_ns=HOUR_NS))
    full = list(iter_events(tmp_path, streams, 0, END))
    cold = list(iter_events(tmp_path, streams, t0, END))
    snaps = [e for e in primed if isinstance(e, ExtBookSnapshot) and e.ts == t0]
    assert {(s.venue, s.symbol) for s in snaps} == {("coinbase", "BTC-USD"), ("kraken", "BTC/USD")}
    cb = next(s for s in snaps if s.venue == "coinbase")
    assert cb.bids[0] == (84500.01, 0.61) and cb.asks[0] == (84501.0, 0.75)  # delta applied before t0
    # after the synthesized state: exactly what a replay from the beginning emits from t0 on
    assert primed[len(snaps):] == [e for e in full if e.ts >= t0]
    assert not any(isinstance(e, ExtTrade) and e.ts < t0 for e in primed)
    # a cold start at t0 has no books: kraken deltas are suppressed until the next snapshot
    assert not any(isinstance(e, ExtBookSnapshot) and e.venue == "kraken" and e.ts < T0_NS + 200_000_000 for e in cold)


def test_records_events_pairs(tmp_path):
    build_store(tmp_path, names=("okx",))
    pairs = list(iter_records_events(tmp_path, ["okx.ws"], 0, END))
    assert len(pairs) == len(load_fixture("okx")) and all(r.stream == "okx.ws" for r, _ in pairs)
    assert orjson.loads(pairs[0][0].data)["_dh"] == "status"
