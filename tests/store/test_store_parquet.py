from __future__ import annotations

import polars as pl
import pyarrow.parquet as pq

from dh.core.events import EVENT_TYPES, ExtBookDelta, ExtBookSnapshot, FeedStatus, IndexTick, KalshiBookSnapshot, OptionQuote
from dh.store.codec import decode_event, encode_event, event_from_dict, event_to_dict
from dh.store.parquet import arrow_type, compact_events, compact_raw, event_schema
from tests.feeds.helpers import load_fixture
from tests.store.test_store_replay import build_store


def test_codec_roundtrip_all_shapes():
    evs = [
        ExtBookSnapshot(1, 2, "v", "S", ((100.0, 1.0),), ((101.0, 2.0),), 3, False),
        ExtBookDelta(1, 2, "v", "S", (("b", 100.0, 0.0), ("a", 101.5, 2.0)), 4, 3),
        KalshiBookSnapshot(1, 0, "T", 1, 2, ((4500, 100),), ((5300, 200),)),
        IndexTick(1, 2, "BRTI", 84500.5, "1hz", 0, 84499.9, 59, None, 0),
        FeedStatus(1, 0, "x.ws", "gap", "d"),
        OptionQuote(1, 2, "deribit", "BTC-26SEP26-85000-C", 3, 85000.0, "C", 0.01, 0.02, 0.4, 0.39, 0.41, 84500.0),
    ]
    for e in evs:
        assert decode_event(encode_event(e)) == e
        assert event_from_dict(event_to_dict(e)) == e


def test_every_event_type_has_a_schema():
    for name, cls in EVENT_TYPES.items():
        schema, _ = event_schema(cls)
        assert len(schema) == len(cls.__dataclass_fields__), name
    assert str(arrow_type("tuple[tuple[str, float, float], ...]")[0]) == "list<item: struct<side: string, price: double, size: double>>"
    assert str(arrow_type("float | None")[0]) == "double"


def test_compact_raw_and_events(tmp_path):
    build_store(tmp_path, names=("coinbase", "deribit"))
    p = compact_raw(tmp_path, "coinbase.ws", "2026-09-25")
    t = pq.read_table(p)
    recs = load_fixture("coinbase")
    assert t.num_rows == len(recs) and t.column("d").to_pylist()[3] == recs[3][3].decode()
    assert compact_raw(tmp_path, "coinbase.ws", "2026-09-24") is None
    out = compact_events(tmp_path, "coinbase.ws", "2026-09-25")
    assert set(out) == {"ExtBookSnapshot", "ExtBookDelta", "ExtTrade", "FeedStatus"}
    snaps = pl.read_parquet(out["ExtBookSnapshot"])
    assert snaps.height == 2 and snaps["bids"][0][0] == {"price": 84500.01, "size": 0.51}
    deltas = pl.read_parquet(out["ExtBookDelta"])
    assert deltas["changes"][0][1] == {"side": "a", "price": 84500.02, "size": 0.0}
    trades = pl.read_parquet(out["ExtTrade"])
    assert trades["trade_id"].to_list() == ["812345672", "812345673"]
    d_out = compact_events(tmp_path, "deribit.ws", "2026-09-25")
    opt = pl.read_parquet(d_out["OptionQuote"])
    assert opt["mark_iv"][0] == 0.385 and opt["cp"][0] == "C"
    idx = pl.read_parquet(d_out["IndexTick"])
    assert idx["avg60"].null_count() == 2
