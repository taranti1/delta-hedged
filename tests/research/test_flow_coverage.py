from dh.core.events import FeedStatus, KalshiBookSnapshot
from dh.research.flow_coverage import healthy_book_intervals, intersect
from dh.store.replay import RawRecord


def test_snapshot_gap_and_freshness_coverage():
    events = {
        0: [KalshiBookSnapshot(0, 0, "M", 1, 1, ((5000, 100),), ((5000, 100),))],
        2000: [FeedStatus(2_000_000_000, 0, "kalshi.book:M", "gap", "missing seq")],
        4000: [KalshiBookSnapshot(4_000_000_000, 0, "M", 1, 1, ((5000, 100),), ((5000, 100),))],
    }
    records = [RawRecord(t*1_000_000, "kalshi.ws", i, b"") for i,t in enumerate((0, 1000, 2000, 4000))]
    result = healthy_book_intervals(records, lambda r: events.get(r.t//1_000_000, []), 0, 20_000)
    assert result == {"M": [(0, 2000), (4000, 9000)]}
    assert intersect(result["M"], [(1000, 5000)]) == [(1000, 2000), (4000, 5000)]


def test_silent_outage_requires_new_snapshot_not_just_resumed_frames():
    snap = KalshiBookSnapshot(0, 0, "M", 1, 1, (), ())
    records = [RawRecord(t*1_000_000, "kalshi.ws", i, b"") for i,t in enumerate((0, 1000, 10_000, 11_000))]
    result = healthy_book_intervals(records, lambda r: [snap] if r.q == 0 else [], 0, 20_000)
    assert result == {"M": [(0, 6000)]}


def test_recorded_trade_retains_both_clocks(monkeypatch):
    from dh.core.events import KalshiTrade
    import dh.research.flow_recording as recording
    rec = RawRecord(4_000_000_000, "kalshi.ws", 1, b'{"type":"trade"}')
    event = KalshiTrade(rec.t, 1_000_000_000, "M", "trade-1", 5000, 100, "no")
    monkeypatch.setattr(recording, "kalshi_ws_streams", lambda _: ["kalshi.ws"])
    monkeypatch.setattr(recording, "iter_raw", lambda *args: [rec])
    monkeypatch.setattr(recording, "ws_message_to_events", lambda *args: [event])
    table = recording.recorded_trades("unused", 0, 10_000_000_000)
    assert table.iloc[0].ts_ms == 1000 and table.iloc[0].ts_recv_ms == 4000
