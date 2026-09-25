"""Venue wire formats verified against REAL frames captured on 2026-09-25 (first live contact,
scripts/smoke_feeds.py). Public market data only. Fixtures: tests/feeds/fixtures/live/."""

from __future__ import annotations

from pathlib import Path

import orjson

from dh.core.events import ExtBookSnapshot, ExtTrade
from dh.feeds.base import is_marker, make_marker, safe_normalize
from dh.feeds.bitstamp import BitstampFeed
from dh.feeds.coinbase import CoinbaseFeed
from dh.feeds.hyperliquid import HyperliquidFeed
from dh.feeds.registry import build_feed

LIVE = Path(__file__).parent / "fixtures" / "live"


def load(name: str) -> list[tuple[int, bytes]]:
    out = []
    for line in (LIVE / f"{name}_2026-09-25.jsonl").read_bytes().splitlines():
        if line.strip():
            o = orjson.loads(line)
            out.append((int(o["t"]), o["d"].encode()))
    return out


def test_coinbase_market_trades_side_is_maker_side():
    """Real frames: three 'BUY' prints at 83981.6 while the book was 83981.60 / 83981.61 (they
    hit the BID): the aggressor was the seller. 'side' is the maker side."""
    st = CoinbaseFeed.new_state("coinbase.ws")
    evs = [e for t, raw in load("coinbase_trades") for e in safe_normalize(CoinbaseFeed.normalize, raw, t, st)]
    trades = [e for e in evs if isinstance(e, ExtTrade)]
    assert trades and all(t.price == 83981.6 for t in trades[:3])
    raw_sides = [tr["side"] for _, raw in load("coinbase_trades")[:3]
                 for ev in orjson.loads(raw)["events"] for tr in ev["trades"]]
    assert set(raw_sides) == {"BUY"} and {t.aggressor for t in trades} == {"sell"}
    # real envelopes carry no client_id; timestamps are RFC3339 with ns digits
    assert all(t.ts_exch > 0 for t in trades)


def test_make_marker_accepts_a_kind_field():
    """Found live: record_rest() passed kind=... into make_marker(kind, conn, **fields), so every
    Bitstamp REST snapshot raised TypeError and the book never became valid."""
    raw = make_marker("rest", 3, kind="order_book", url="u", status=200, req_ns=1, body="{}", symbol="btcusd")
    assert is_marker(raw)
    m = orjson.loads(raw)
    assert m["_dh"] == "rest" and m["conn"] == 3 and m["kind"] == "order_book" and m["symbol"] == "btcusd"


def test_bitstamp_real_diffs_then_rest_snapshot_align():
    """Real diff frames buffer until the REST order_book snapshot marker arrives; the snapshot
    (microtimestamp before the diffs) then yields a valid book with the diffs folded in."""
    feed = build_feed("bitstamp", {"enabled": True})
    st = BitstampFeed.new_state(feed.name)
    frames = load("bitstamp")
    out = []
    for t, raw in frames:
        out += safe_normalize(BitstampFeed.normalize, raw, t, st)
    assert not [e for e in out if isinstance(e, ExtBookSnapshot)]  # buffered, no book yet
    snap_body = orjson.dumps({"timestamp": "1790364945", "microtimestamp": "1790364945000000",
                              "bids": [["83960.00", "1.0"], ["83958.48", "0.5"]],
                              "asks": [["83995.21", "0.3"], ["83999.41", "0.2"], ["84010.00", "1.0"]]}).decode()
    marker = make_marker("rest", 1, kind="order_book", url="https://www.bitstamp.net/api/v2/order_book/btcusd/",
                         status=200, req_ns=frames[0][0], body=snap_body, symbol="btcusd")
    evs = safe_normalize(BitstampFeed.normalize, marker, frames[-1][0] + 1, st)
    snaps = [e for e in evs if isinstance(e, ExtBookSnapshot)]
    assert len(snaps) == 1
    bids, asks = dict(snaps[0].bids), dict(snaps[0].asks)
    assert bids[83958.48] == 0.08491064 and 83995.21 not in asks and 83999.41 in asks  # real diffs folded in
    trades = [e for t, raw in frames for e in safe_normalize(BitstampFeed.normalize, raw, t, BitstampFeed.new_state(feed.name))
              if isinstance(e, ExtTrade)]
    assert trades and trades[0].aggressor == "sell"  # real frame: "type": 1 = sell-initiated


def test_hyperliquid_l2book_subscribes_fast():
    """Real ack without 'fast' echoes "fast": false (snapshots every ~3-5 s); the adapter now
    subscribes with fast=true (~0.5 s, measured live)."""
    acks = [orjson.loads(raw) for _, raw in load("hyperliquid_acks")]
    assert acks[0]["data"]["subscription"]["fast"] is False
    feed = build_feed("hyperliquid", {"enabled": True, "symbols": ["BTC"], "channels": ["l2Book", "trades"]})
    subs = [orjson.loads(m) for m in feed.subscribe_messages()]
    assert subs[0]["subscription"] == {"type": "l2Book", "coin": "BTC", "fast": True}
    assert subs[1]["subscription"] == {"type": "trades", "coin": "BTC"}
    slow = build_feed("hyperliquid", {"enabled": True, "fast": False, "channels": ["l2Book"]})
    assert "fast" not in orjson.loads(slow.subscribe_messages()[0])["subscription"]
