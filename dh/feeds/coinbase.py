"""Coinbase Advanced Trade WebSocket (public market data), BTC-USD. BRTI constituent.

ASSUMED WIRE FORMAT (Advanced Trade WS docs, "WebSocket Channels"; verify live with
``python scripts/smoke_feeds.py --venues coinbase``):

  endpoint   wss://advanced-trade-ws.coinbase.com  (market data; no auth for level2/
             market_trades/heartbeats). One channel per subscribe message; subscribe within
             5 s of connecting or the server disconnects:
               {"type":"subscribe","product_ids":["BTC-USD"],"channel":"level2"}
               {"type":"subscribe","product_ids":["BTC-USD"],"channel":"market_trades"}
               {"type":"subscribe","channel":"heartbeats"}
  envelope   {"channel": "...", "client_id": "", "timestamp": "2023-02-09T20:32:50.714964855Z",
              "sequence_num": 12, "events": [...]}
  level2     channel "l2_data"; events [{"type": "snapshot"|"update", "product_id": "BTC-USD",
              "updates": [{"side": "bid"|"offer", "event_time": "...", "price_level": "21921.73",
              "new_quantity": "0.06317902"}]}]. new_quantity is the ABSOLUTE size ("0" deletes).
              The snapshot is the full book in one (large, several MB) frame.
  trades     channel "market_trades"; events [{"type": "snapshot"|"update", "trades":
              [{"trade_id": "...", "product_id": "BTC-USD", "price": "...", "size": "...",
              "side": "BUY"|"SELL", "time": "RFC3339"}]}]
  heartbeats channel "heartbeats" once per second (keeps idle subscriptions open).
  errors     {"type": "error", "message": "..."}

  SEQUENCE_SCOPE   sequence_num is assumed to be ONE counter per connection, incremented by
                   exactly 1 for every message on that connection across all channels. A jump
                   is a gap -> the book is invalidated and level2 is resubscribed (fresh
                   snapshot). If live data shows per-channel counters, set SEQUENCE_SCOPE to
                   "channel".
  TRADE_SIDE       market_trades ``side`` is assumed to be the TAKER (aggressor) side. The
                   smoke test measures this (buy-aggressor trades should print at/above mid);
                   flip TRADE_SIDE_IS_TAKER if it reports the inverse.
"""

from __future__ import annotations

from typing import ClassVar

import orjson

from dh.core.events import Event, ExtBookDelta, ExtTrade, FeedStatus
from dh.feeds.base import (
    FeedClient,
    NormalizerState,
    book_gap,
    book_snapshot_ok,
    book_valid,
    dumps,
    handle_marker,
    is_history,
    is_marker,
    parse_rfc3339_ns,
    snapshot_event,
    trade_seen,
)

VENUE = "coinbase"
URL = "wss://advanced-trade-ws.coinbase.com"
SEQUENCE_SCOPE = "connection"  # "connection" | "channel"   (MUST VERIFY LIVE)
TRADE_SIDE_IS_TAKER = True  # (MUST VERIFY LIVE: smoke test aggressor check)


def _aggressor(side: str) -> str:
    s = side.lower()
    if s not in ("buy", "sell"):
        return ""
    if TRADE_SIDE_IS_TAKER:
        return s
    return "sell" if s == "buy" else "buy"


class CoinbaseFeed(FeedClient):
    venue: ClassVar[str] = VENUE
    default_stream: ClassVar[str] = "coinbase.ws"
    default_url: ClassVar[str] = URL
    default_symbols: ClassVar[tuple[str, ...]] = ("BTC-USD",)
    default_channels: ClassVar[tuple[str, ...]] = ("level2", "market_trades", "heartbeats")
    stale_after_s: ClassVar[float] = 5.0  # heartbeats every second
    dead_after_s: ClassVar[float] = 30.0
    max_frame_bytes: ClassVar[int] = 128 * 2**20  # full-book snapshot is one large frame

    def subscribe_messages(self) -> list[str]:
        out = []
        for ch in self.channels:
            if ch == "heartbeats":
                out.append(dumps({"type": "subscribe", "channel": "heartbeats"}))
            else:
                out.append(dumps({"type": "subscribe", "product_ids": list(self.symbols), "channel": ch}))
        return out

    def resubscribe_messages(self) -> list[str] | None:
        if "level2" not in self.channels:
            return []
        p = list(self.symbols)
        return [
            dumps({"type": "unsubscribe", "product_ids": p, "channel": "level2"}),
            dumps({"type": "subscribe", "product_ids": p, "channel": "level2"}),
        ]

    @staticmethod
    def normalize(raw: bytes, recv_ns: int, state: NormalizerState) -> list[Event]:
        if is_marker(raw):
            return handle_marker(raw, recv_ns, state)
        msg = orjson.loads(raw)
        ch = msg.get("channel")
        if ch is None:
            if msg.get("type") == "error":
                return [FeedStatus(recv_ns, 0, state.stream, "error", str(msg.get("message", ""))[:300])]
            return []
        out: list[Event] = []
        ts_exch = parse_rfc3339_ns(msg["timestamp"]) if msg.get("timestamp") else 0
        seq = msg.get("sequence_num")
        if seq is not None:
            key = "conn" if SEQUENCE_SCOPE == "connection" else f"ch:{ch}"
            last = state.seq.get(key)
            if last is not None:
                if seq <= last:
                    state.bump("stale_seq_dropped")
                    return out  # duplicate / out-of-order: ignore (Coinbase docs)
                if seq != last + 1:
                    detail = f"sequence_num {last} -> {seq} ({ch})"
                    for sym in _book_symbols(state):
                        out += book_gap(state, sym, recv_ns, detail)
            state.seq[key] = seq
        if ch == "l2_data":
            for ev in msg.get("events", ()):
                pid = ev.get("product_id", "")
                typ = ev.get("type")
                if typ == "snapshot":
                    bids, asks = [], []
                    for u in ev.get("updates", ()):
                        lvl = (float(u["price_level"]), float(u["new_quantity"]))
                        (bids if u["side"] == "bid" else asks).append(lvl)
                    out.append(
                        snapshot_event(recv_ns, ts_exch, VENUE, pid, bids, asks, seq=seq or 0, depth_limited=False)
                    )
                    _remember_symbol(state, pid)
                    out += book_snapshot_ok(state, pid, recv_ns)
                elif typ == "update":
                    if not book_valid(state, pid):
                        state.bump("deltas_suppressed")
                        continue
                    changes = tuple(
                        ("b" if u["side"] == "bid" else "a", float(u["price_level"]), float(u["new_quantity"]))
                        for u in ev.get("updates", ())
                    )
                    if changes:
                        out.append(ExtBookDelta(recv_ns, ts_exch, VENUE, pid, changes, seq=seq or 0))
        elif ch == "market_trades":
            for ev in msg.get("events", ()):
                if ev.get("type") == "snapshot":
                    for t in ev.get("trades", ()):  # history: only remember ids for de-dup
                        trade_seen(state, t.get("product_id", ""), 0, str(t.get("trade_id", "")))
                    continue
                for t in ev.get("trades", ()):
                    pid = t.get("product_id", "")
                    tid = str(t.get("trade_id", ""))
                    tx = parse_rfc3339_ns(t["time"]) if t.get("time") else ts_exch
                    if trade_seen(state, pid, tx, tid) or is_history(state, tx):
                        continue
                    out.append(
                        ExtTrade(recv_ns, tx, VENUE, pid, float(t["price"]), float(t["size"]), _aggressor(t.get("side", "")), tid)
                    )
        # heartbeats / subscriptions: sequence bookkeeping only
        return out


def _remember_symbol(state: NormalizerState, sym: str) -> None:
    syms = state.meta.setdefault("book_symbols", [])
    if sym not in syms:
        syms.append(sym)


def _book_symbols(state: NormalizerState) -> list[str]:
    return list(state.meta.get("book_symbols", []))
