"""Hyperliquid public WebSocket for the BTC perp: l2Book, trades, activeAssetCtx.

ASSUMED WIRE FORMAT (hyperliquid.gitbook.io "WebSocket / Subscriptions"; verify live with
``python scripts/smoke_feeds.py --venues hyperliquid``):

  endpoint   wss://api.hyperliquid.xyz/ws
  subscribe  {"method":"subscribe","subscription":{"type":"l2Book","coin":"BTC"}}
             {"method":"subscribe","subscription":{"type":"trades","coin":"BTC"}}
             {"method":"subscribe","subscription":{"type":"activeAssetCtx","coin":"BTC"}}
             ack {"channel":"subscriptionResponse","data":{"method":"subscribe","subscription":{...}}}
  keepalive  {"method":"ping"} -> {"channel":"pong"}; the server drops connections that sent
             nothing for 60 s.
  l2Book     {"channel":"l2Book","data":{"coin":"BTC","time":<ms>,"levels":[[{"px":"...","sz":"...",
              "n":3},...bids best first], [...asks best first]]}}  full top-20 SNAPSHOT each msg
  trades     {"channel":"trades","data":[{"coin":"BTC","side":"B"|"A","px":"...","sz":"...",
              "hash":"0x..","time":<ms>,"tid":<int>,"users":[buyer, seller]}]}
              side "B" = the taker bought (bid side aggressor), "A" = taker sold (ASSUMED; the
              smoke test checks it). Recent trades may be replayed on subscribe (history filter).
  activeAssetCtx {"channel":"activeAssetCtx","data":{"coin":"BTC","ctx":{"markPx","oraclePx",
              "midPx","funding" (per HOUR),"openInterest" (BTC),"premium","dayNtlVlm",...}}}
Sizes are in BTC. Funding is hourly on Hyperliquid.
"""

from __future__ import annotations

from typing import ClassVar

import orjson

from dh.core.events import Event, ExtTrade, FeedStatus, PerpState
from dh.feeds.base import FeedClient, NormalizerState, dumps, handle_marker, is_history, is_marker, ms_to_ns, snapshot_event, trade_seen

VENUE = "hyperliquid"
URL = "wss://api.hyperliquid.xyz/ws"
FUNDING_INTERVAL_S = 3600


class HyperliquidFeed(FeedClient):
    venue: ClassVar[str] = VENUE
    default_stream: ClassVar[str] = "hyperliquid.ws"
    default_url: ClassVar[str] = URL
    default_symbols: ClassVar[tuple[str, ...]] = ("BTC",)
    default_channels: ClassVar[tuple[str, ...]] = ("l2Book", "trades", "activeAssetCtx")
    stale_after_s: ClassVar[float] = 10.0
    dead_after_s: ClassVar[float] = 60.0
    keepalive_interval_s: ClassVar[float | None] = 30.0
    resnapshot_interval_s: ClassVar[float] = 0.0  # l2Book messages are full snapshots

    def subscribe_messages(self) -> list[str]:
        return [
            dumps({"method": "subscribe", "subscription": {"type": ch, "coin": coin}})
            for ch in self.channels
            for coin in self.symbols
        ]

    def keepalive_message(self) -> str | None:
        return dumps({"method": "ping"})

    @staticmethod
    def normalize(raw: bytes, recv_ns: int, state: NormalizerState) -> list[Event]:
        if is_marker(raw):
            return handle_marker(raw, recv_ns, state)
        msg = orjson.loads(raw)
        ch = msg.get("channel")
        data = msg.get("data")
        if ch == "l2Book":
            coin = str(data.get("coin", ""))
            levels = data.get("levels") or [[], []]
            bids = [(float(x["px"]), float(x["sz"])) for x in levels[0]]
            asks = [(float(x["px"]), float(x["sz"])) for x in (levels[1] if len(levels) > 1 else [])]
            return [snapshot_event(recv_ns, ms_to_ns(data.get("time", 0)), VENUE, coin, bids, asks, depth_limited=True)]
        if ch == "trades":
            out: list[Event] = []
            for t in data or ():
                coin = str(t.get("coin", ""))
                tid = str(t.get("tid", t.get("hash", "")))
                tx = ms_to_ns(t.get("time", 0))
                if trade_seen(state, coin, tx, tid, keep=256) or is_history(state, tx):
                    continue
                side = t.get("side")
                aggr = "buy" if side == "B" else "sell" if side == "A" else ""
                out.append(ExtTrade(recv_ns, tx, VENUE, coin, float(t["px"]), float(t["sz"]), aggr, tid))  # type: ignore[arg-type]
            return out
        if ch == "activeAssetCtx":
            coin = str(data.get("coin", ""))
            c = data.get("ctx") or {}
            return [
                PerpState(
                    ts=recv_ns,
                    ts_exch=0,
                    venue=VENUE,
                    symbol=coin,
                    mark=float(c.get("markPx") or 0),
                    index=float(c.get("oraclePx") or 0),
                    funding_rate=float(c.get("funding") or 0),
                    funding_interval_s=FUNDING_INTERVAL_S,
                    next_funding_ts=0,
                    open_interest=float(c.get("openInterest") or 0),
                )
            ]
        if ch == "error":
            return [FeedStatus(recv_ns, 0, state.stream, "error", str(data)[:300])]
        return []
