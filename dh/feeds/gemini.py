"""Gemini market data v2 WebSocket, BTCUSD L2 (+ trades). BRTI constituent.

ASSUMED WIRE FORMAT (docs.gemini.com "Market Data Version 2"; verify live with
``python scripts/smoke_feeds.py --venues gemini``):

  endpoint   wss://api.gemini.com/v2/marketdata
  subscribe  {"type":"subscribe","subscriptions":[{"name":"l2","symbols":["BTCUSD"]}]}
  l2         {"type":"l2_updates","symbol":"BTCUSD","changes":[["buy","9122.04","0.00121425"],
              ["sell","9123.00","0.5"],...], "trades":[...], "auction_events":[...]}
             The FIRST l2_updates per symbol after subscribing is the full book (snapshot,
             also carrying recent trades); later l2_updates carry only changed levels with the
             ABSOLUTE new quantity ("0" deletes). No sequence numbers -> no gap detection.
  trade      {"type":"trade","symbol":"BTCUSD","event_id":3575573053,"timestamp":1560976400428,
              "price":"9004.21","quantity":"0.0911","side":"buy"|"sell"}  (ms timestamp;
              ``side`` assumed = TAKER side; the smoke test checks it)
  other      {"type":"heartbeat",...} (if sent), auction messages: ignored.

Snapshot detection relies on the connection marker: the first l2_updates for a symbol on a
connection is the snapshot. Hence the periodic resnapshot for Gemini is a reconnect (an
in-place unsubscribe/subscribe could confuse in-flight updates with the new snapshot).
"""

from __future__ import annotations

from typing import ClassVar

import orjson

from dh.core.events import Event, ExtBookDelta, ExtTrade, FeedStatus
from dh.feeds.base import (
    FeedClient,
    NormalizerState,
    book_snapshot_ok,
    book_valid,
    dumps,
    handle_marker,
    is_history,
    is_marker,
    ms_to_ns,
    snapshot_event,
    trade_seen,
)

VENUE = "gemini"
URL = "wss://api.gemini.com/v2/marketdata"
TRADE_SIDE_IS_TAKER = True  # MUST VERIFY LIVE (smoke test aggressor check)


def _aggr(side: str) -> str:
    s = side.lower()
    if s not in ("buy", "sell"):
        return ""
    return s if TRADE_SIDE_IS_TAKER else ("sell" if s == "buy" else "buy")


class GeminiFeed(FeedClient):
    venue: ClassVar[str] = VENUE
    default_stream: ClassVar[str] = "gemini.ws"
    default_url: ClassVar[str] = URL
    default_symbols: ClassVar[tuple[str, ...]] = ("BTCUSD",)
    default_channels: ClassVar[tuple[str, ...]] = ("l2",)
    stale_after_s: ClassVar[float] = 15.0
    dead_after_s: ClassVar[float] = 60.0
    max_frame_bytes: ClassVar[int] = 64 * 2**20

    def subscribe_messages(self) -> list[str]:
        return [dumps({"type": "subscribe", "subscriptions": [{"name": ch, "symbols": list(self.symbols)} for ch in self.channels]})]

    def resubscribe_messages(self) -> list[str] | None:
        return None  # reconnect (see module docstring)

    @staticmethod
    def normalize(raw: bytes, recv_ns: int, state: NormalizerState) -> list[Event]:
        if is_marker(raw):
            return handle_marker(raw, recv_ns, state)
        msg = orjson.loads(raw)
        typ = msg.get("type")
        out: list[Event] = []
        if typ == "l2_updates":
            sym = msg.get("symbol", "")
            snap_conn = state.meta.get("snap_conn", {}).get(sym)
            is_snapshot = state.conn > 0 and snap_conn != state.conn
            ts_exch = 0
            if is_snapshot:
                bids, asks = [], []
                for side, p, q in msg.get("changes", ()):
                    (bids if side == "buy" else asks).append((float(p), float(q)))
                for t in msg.get("trades", ()) or ():  # history: remember ids only
                    trade_seen(state, sym, 0, str(t.get("event_id", t.get("tid", ""))))
                out.append(snapshot_event(recv_ns, ts_exch, VENUE, sym, bids, asks, depth_limited=False))
                out += book_snapshot_ok(state, sym, recv_ns)
            elif book_valid(state, sym):
                changes = tuple(
                    ("b" if side == "buy" else "a", float(p), float(q)) for side, p, q in msg.get("changes", ())
                )
                if changes:
                    out.append(ExtBookDelta(recv_ns, ts_exch, VENUE, sym, changes))
                for t in msg.get("trades", ()) or ():
                    out += _trade(t, recv_ns, state)
            else:
                state.bump("deltas_suppressed")
            return out
        if typ == "trade":
            return _trade(msg, recv_ns, state)
        if typ == "error" or msg.get("result") == "error":
            return [FeedStatus(recv_ns, 0, state.stream, "error", str(msg.get("reason", msg.get("message", "")))[:300])]
        return out


def _trade(t: dict, recv_ns: int, state: NormalizerState) -> list[Event]:
    sym = t.get("symbol", "")
    tid = str(t.get("event_id", t.get("tid", "")))
    tx = ms_to_ns(t["timestamp"]) if t.get("timestamp") else 0
    if trade_seen(state, sym, tx, tid) or is_history(state, tx):
        return []
    return [ExtTrade(recv_ns, tx, VENUE, sym, float(t["price"]), float(t["quantity"]), _aggr(str(t.get("side", ""))), tid)]
