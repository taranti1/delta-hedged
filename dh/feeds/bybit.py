"""Bybit v5 public linear WebSocket for BTCUSDT: orderbook.50, publicTrade, tickers
(mark/index/funding/OI), allLiquidation.

ASSUMED WIRE FORMAT (bybit-exchange.github.io/docs/v5/websocket/public/*; verify live with
``python scripts/smoke_feeds.py --venues bybit``):

  endpoint   wss://stream.bybit.com/v5/public/linear
  subscribe  {"op":"subscribe","args":["orderbook.50.BTCUSDT","publicTrade.BTCUSDT",
              "tickers.BTCUSDT","allLiquidation.BTCUSDT"]}; ack {"success":true,"op":"subscribe",...}
  ping       client sends {"op":"ping"} every 20 s -> {"success":true,"ret_msg":"pong","op":"ping"}
  orderbook  {"topic":"orderbook.50.BTCUSDT","type":"snapshot"|"delta","ts":<ms>,"cts":<ms>,
              "data":{"s":"BTCUSDT","b":[["16493.50","0.006"],...],"a":[...],"u":18521288,
              "seq":7961638724}}; sizes ABSOLUTE ("0" deletes); a new snapshot (or u == 1)
              resets the book. ``u`` increases per message; contiguity (+1) is NOT documented,
              so only regressions are treated as errors unless U_CONTIGUOUS is set. The book is
              truncated to the topic depth.
  publicTrade {"topic":"publicTrade.BTCUSDT","type":"snapshot","ts":<ms>,"data":[{"T":<ms>,
              "s":"BTCUSDT","S":"Buy"|"Sell" (TAKER side),"v":"qty","p":"price","i":"<trade id>",
              "BT":false}]}   (type is "snapshot" on every message)
  tickers    {"topic":"tickers.BTCUSDT","type":"snapshot"|"delta","ts":<ms>,"data":{"symbol":
              "BTCUSDT","markPrice","indexPrice","fundingRate","nextFundingTime"(ms str),
              "openInterest" (BTC),"fundingIntervalHour"(optional),"bid1Price",...}}
              Deltas contain only changed fields: merged into normalizer state.
  allLiquidation {"topic":"allLiquidation.BTCUSDT","type":"snapshot","ts":<ms>,"data":[{"T":<ms>,
              "s":"BTCUSDT","S":"Buy"|"Sell","v":"qty","p":"price"}]}; ``S`` = POSITION side
              (Buy = a long was liquidated -> the liquidation order is a sell). The legacy
              ``liquidation.{symbol}`` topic (data {"updatedTime","symbol","side","size","price"})
              is also parsed.
Linear sizes are in BTC.
"""

from __future__ import annotations

from typing import Any, ClassVar

import orjson

from dh.core.events import Event, ExtBookDelta, ExtTrade, FeedStatus, Liquidation, PerpState
from dh.feeds.base import (
    DepthBook,
    FeedClient,
    NormalizerState,
    book_gap,
    book_snapshot_ok,
    book_valid,
    dumps,
    handle_marker,
    is_history,
    is_marker,
    ms_to_ns,
    snapshot_event,
    to_dec,
    trade_seen,
)

VENUE = "bybit"
URL = "wss://stream.bybit.com/v5/public/linear"
U_CONTIGUOUS = False  # MUST VERIFY LIVE (smoke test reports u increments)
DEFAULT_FUNDING_H = 8
TOPIC = {"orderbook": "orderbook.{depth}.{sym}", "publicTrade": "publicTrade.{sym}", "tickers": "tickers.{sym}",
         "allLiquidation": "allLiquidation.{sym}", "liquidation": "liquidation.{sym}"}
PERP_FIELDS = ("markPrice", "indexPrice", "fundingRate", "nextFundingTime", "openInterest", "fundingIntervalHour")


def _liq_side(position_side: str) -> str:
    s = position_side.lower()
    return "sell" if s == "buy" else "buy" if s == "sell" else ""


class BybitFeed(FeedClient):
    venue: ClassVar[str] = VENUE
    default_stream: ClassVar[str] = "bybit.ws"
    default_url: ClassVar[str] = URL
    default_symbols: ClassVar[tuple[str, ...]] = ("BTCUSDT",)
    default_channels: ClassVar[tuple[str, ...]] = ("orderbook", "publicTrade", "tickers", "allLiquidation")
    stale_after_s: ClassVar[float] = 5.0
    dead_after_s: ClassVar[float] = 30.0
    keepalive_interval_s: ClassVar[float | None] = 20.0

    @property
    def depth(self) -> int:
        return int(self.options.get("depth", 50))

    def _topics(self, only_book: bool = False) -> list[str]:
        chans = ("orderbook",) if only_book else self.channels
        return [TOPIC.get(c, c + ".{sym}").format(depth=self.depth, sym=s) for c in chans for s in self.symbols]

    def subscribe_messages(self) -> list[str]:
        return [dumps({"op": "subscribe", "args": self._topics()})]

    def resubscribe_messages(self) -> list[str] | None:
        if "orderbook" not in self.channels:
            return []
        t = self._topics(only_book=True)
        return [dumps({"op": "unsubscribe", "args": t}), dumps({"op": "subscribe", "args": t})]

    def keepalive_message(self) -> str | None:
        return dumps({"op": "ping"})

    @staticmethod
    def normalize(raw: bytes, recv_ns: int, state: NormalizerState) -> list[Event]:
        if is_marker(raw):
            return handle_marker(raw, recv_ns, state)
        msg = orjson.loads(raw)
        if "op" in msg and "topic" not in msg:
            if msg.get("success") is False:
                return [FeedStatus(recv_ns, 0, state.stream, "error", f"{msg.get('op')}: {msg.get('ret_msg')}"[:300])]
            return []
        topic = str(msg.get("topic", ""))
        kind = topic.split(".", 1)[0]
        if kind == "orderbook":
            return _book(topic, msg, recv_ns, state)
        if kind == "publicTrade":
            out: list[Event] = []
            for t in msg.get("data", ()):
                sym = t.get("s", "")
                tid = str(t.get("i", ""))
                tx = ms_to_ns(t.get("T", 0))
                if trade_seen(state, sym, tx, tid) or is_history(state, tx):
                    continue
                side = str(t.get("S", "")).lower()
                out.append(ExtTrade(recv_ns, tx, VENUE, sym, float(t["p"]), float(t["v"]), side if side in ("buy", "sell") else "", tid))  # type: ignore[arg-type]
            return out
        if kind == "tickers":
            d = msg.get("data") or {}
            sym = str(d.get("symbol", topic.split(".")[-1]))
            f = state.fields.setdefault(sym, {})
            if msg.get("type") == "snapshot":
                f.clear()
            changed = False
            for k in PERP_FIELDS:
                if k in d and d[k] not in (None, ""):
                    f[k] = d[k]
                    changed = True
            if not changed:
                return []
            return [_perp_state(sym, f, recv_ns, ms_to_ns(msg.get("ts", 0)))]
        if kind == "allLiquidation":
            out = []
            for x in msg.get("data", ()):
                out.append(Liquidation(recv_ns, ms_to_ns(x.get("T", 0)), VENUE, x.get("s", ""), _liq_side(str(x.get("S", ""))), float(x["p"]), float(x["v"])))  # type: ignore[arg-type]
            return out
        if kind == "liquidation":
            x = msg.get("data") or {}
            return [Liquidation(recv_ns, ms_to_ns(x.get("updatedTime", 0)), VENUE, x.get("symbol", ""), _liq_side(str(x.get("side", ""))), float(x["price"]), float(x["size"]))]  # type: ignore[arg-type]
        return []


def _perp_state(sym: str, f: dict[str, Any], ts: int, tx: int) -> PerpState:
    hours = int(float(f.get("fundingIntervalHour") or DEFAULT_FUNDING_H))
    return PerpState(
        ts=ts,
        ts_exch=tx,
        venue=VENUE,
        symbol=sym,
        mark=float(f.get("markPrice") or 0),
        index=float(f.get("indexPrice") or 0),
        funding_rate=float(f.get("fundingRate") or 0),
        funding_interval_s=hours * 3600,
        next_funding_ts=ms_to_ns(f.get("nextFundingTime") or 0),
        open_interest=float(f.get("openInterest") or 0),
    )


def _book(topic: str, msg: dict[str, Any], recv_ns: int, state: NormalizerState) -> list[Event]:
    parts = topic.split(".")
    depth = int(parts[1]) if len(parts) == 3 and parts[1].isdigit() else 50
    d = msg.get("data") or {}
    sym = str(d.get("s", parts[-1]))
    u = int(d.get("u", 0))
    tx = ms_to_ns(msg.get("cts") or msg.get("ts") or 0)
    key = f"u:{sym}"
    out: list[Event] = []
    if msg.get("type") == "snapshot" or u == 1:
        book = DepthBook(depth)
        for p, q in d.get("b", ()):
            book.set("b", to_dec(p), to_dec(q))
        for p, q in d.get("a", ()):
            book.set("a", to_dec(p), to_dec(q))
        book.truncate()
        state.books[sym] = book
        state.seq[key] = u
        b, a = book.snapshot_levels()
        out.append(snapshot_event(recv_ns, tx, VENUE, sym, b, a, seq=u, depth_limited=True))
        out += book_snapshot_ok(state, sym, recv_ns)
        return out
    book = state.books.get(sym)
    if book is None or not book_valid(state, sym):
        state.bump("deltas_suppressed")
        return out
    last = state.seq.get(key, 0)
    if u <= last:
        state.bump("stale_update_dropped")
        return out
    if u != last + 1:
        state.bump("u_noncontiguous")
        if U_CONTIGUOUS:
            return book_gap(state, sym, recv_ns, f"u {last} -> {u}")
    state.seq[key] = u
    changes: list[tuple[str, float, float]] = []
    for side, k in (("b", "b"), ("a", "a")):
        for p, q in d.get(k, ()):
            dp, dq = to_dec(p), to_dec(q)
            book.set(side, dp, dq)
            changes.append((side, float(dp), float(dq)))
    changes += book.truncate()
    if changes:
        out.append(ExtBookDelta(recv_ns, tx, VENUE, sym, tuple(changes), seq=u, prev_seq=last))
    return out
