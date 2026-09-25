"""Paxos (formerly itBit) public market-data WebSocket, BTCUSD book. EXPERIMENTAL.

itBit's legacy API was folded into Paxos. This adapter implements the Paxos public market-data
stream from memory of its documentation; it is disabled by default and MUST be validated with
``python scripts/smoke_feeds.py --venues paxos`` before being enabled.

ASSUMED WIRE FORMAT (docs.paxos.com "Market Data WebSocket"; UNVERIFIED):
  endpoint  wss://ws.paxos.com/marketdata/BTCUSD   (no subscribe message; the path selects the
            market)
  snapshot  {"type":"SNAPSHOT","market":"BTCUSD","bids":[{"price":"19000.25","amount":"0.5"},..],
             "asks":[...],"final_snapshot":true}   (may be split over several frames; the book
             is complete when final_snapshot is true)
  update    {"type":"UPDATE","market":"BTCUSD","side":"BUY"|"SELL","price":"...","amount":"..."}
            amount = ABSOLUTE size at the level ("0" deletes). No sequence numbers.
Trades (execution data stream wss://ws.paxos.com/executiondata/BTCUSD) are not captured.
"""

from __future__ import annotations

from typing import ClassVar

import orjson

from dh.core.events import Event, ExtBookDelta
from dh.feeds.base import FeedClient, NormalizerState, book_snapshot_ok, book_valid, handle_marker, is_marker, snapshot_event

VENUE = "paxos"
URL = "wss://ws.paxos.com/marketdata/BTCUSD"


class PaxosFeed(FeedClient):
    venue: ClassVar[str] = VENUE
    default_stream: ClassVar[str] = "paxos.ws"
    default_url: ClassVar[str] = URL
    default_symbols: ClassVar[tuple[str, ...]] = ("BTCUSD",)
    stale_after_s: ClassVar[float] = 30.0
    dead_after_s: ClassVar[float] = 120.0
    resnapshot_interval_s: ClassVar[float] = 3600.0  # by reconnect
    experimental: ClassVar[bool] = True

    @staticmethod
    def normalize(raw: bytes, recv_ns: int, state: NormalizerState) -> list[Event]:
        if is_marker(raw):
            return handle_marker(raw, recv_ns, state)
        msg = orjson.loads(raw)
        typ = msg.get("type")
        sym = str(msg.get("market", ""))
        if typ == "SNAPSHOT":
            part = state.buffers.setdefault("snap:" + sym, [])
            part.append([msg.get("bids", []), msg.get("asks", [])])
            if not msg.get("final_snapshot", True):
                return []
            bids = [(float(x["price"]), float(x["amount"])) for b, _ in part for x in b]
            asks = [(float(x["price"]), float(x["amount"])) for _, a in part for x in a]
            state.buffers.pop("snap:" + sym, None)
            return [snapshot_event(recv_ns, 0, VENUE, sym, bids, asks, depth_limited=False)] + book_snapshot_ok(state, sym, recv_ns)
        if typ == "UPDATE":
            if not book_valid(state, sym):
                state.bump("deltas_suppressed")
                return []
            side = "b" if str(msg.get("side", "")).upper() == "BUY" else "a"
            return [ExtBookDelta(recv_ns, 0, VENUE, sym, ((side, float(msg["price"]), float(msg["amount"])),))]
        return []
