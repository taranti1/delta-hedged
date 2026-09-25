"""Kraken Spot WebSocket v2, BTC/USD book (depth 100, CRC32-checked) + trades. BRTI constituent.

ASSUMED WIRE FORMAT (docs.kraken.com "Spot WebSocket v2": book, trade, "Book checksum" guide;
verify live with ``python scripts/smoke_feeds.py --venues kraken``):

  endpoint  wss://ws.kraken.com/v2
  subscribe {"method":"subscribe","params":{"channel":"book","symbol":["BTC/USD"],"depth":100,
             "snapshot":true},"req_id":1}
            {"method":"subscribe","params":{"channel":"trade","symbol":["BTC/USD"],
             "snapshot":false},"req_id":2}
  ack       {"method":"subscribe","result":{"channel":"book","depth":100,"symbol":"BTC/USD",...},
             "success":true,"req_id":1,"time_in":"...","time_out":"..."}
  book      {"channel":"book","type":"snapshot"|"update","data":[{"symbol":"BTC/USD",
             "bids":[{"price":45283.5,"qty":0.10000000},...],"asks":[...],
             "checksum":3310070434,"timestamp":"2023-10-06T17:35:55.440295Z"}]}
            Prices/quantities are JSON NUMBERS written at the pair's full precision (trailing
            zeros kept). qty 0 deletes a level. After applying an update the book must be
            truncated to the subscribed depth (no explicit deletes for levels that fall out).
  checksum  CRC32 (unsigned) of: for the top 10 asks (ascending) then the top 10 bids
            (descending): price text + qty text, each with the '.' removed and leading zeros
            stripped, concatenated. Text = value at the pair's precision (we parse numbers as
            Decimal to keep the wire text and pad to PRECISION when known).
  trade     {"channel":"trade","type":"update","data":[{"symbol":"BTC/USD","side":"buy"|"sell",
             "price":...,"qty":...,"ord_type":"market"|"limit","trade_id":123,
             "timestamp":"RFC3339"}]}; ``side`` = TAKER side.
  other     {"channel":"heartbeat"} ~1/s while subscribed; {"channel":"status",...} on connect;
            {"method":"pong",...} replies to our pings.

On a checksum mismatch the book is invalidated (FeedStatus gap) and the client resubscribes
the book channel (unsubscribe + subscribe -> fresh snapshot).
"""

from __future__ import annotations

import json
import zlib
from decimal import Decimal
from typing import ClassVar

from dh.core.events import Event, ExtBookDelta, ExtTrade, FeedStatus
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
    parse_rfc3339_ns,
    snapshot_event,
    trade_seen,
)

VENUE = "kraken"
URL = "wss://ws.kraken.com/v2"
VALID_DEPTHS = (10, 25, 100, 500, 1000)
DEFAULT_DEPTH = 100
CHECKSUM_LEVELS = 10
VERIFY_CHECKSUM = True
# (price_precision, qty_precision) per pair, from the `instrument` channel (MUST VERIFY LIVE).
# Used only to pad wire text that lost trailing zeros; learned values from an `instrument`
# snapshot in the stream take precedence.
PRECISION: dict[str, tuple[int, int]] = {"BTC/USD": (1, 8), "XBT/USD": (1, 8)}


def _loads(raw: bytes) -> dict:
    return json.loads(raw, parse_float=Decimal)


def _fmt(d: Decimal, prec: int | None) -> str:
    """Decimal -> checksum text: fixed-point at max(wire decimals, prec), '.' removed,
    leading zeros stripped."""
    exp = d.as_tuple().exponent
    wire_dec = -exp if isinstance(exp, int) and exp < 0 else 0
    if prec is not None and prec > wire_dec:
        s = f"{d:.{prec}f}"
    else:
        s = format(d, "f")
    return s.replace(".", "").lstrip("0")


def book_checksum(book: DepthBook, precision: tuple[int, int] | None = None) -> int:
    """Kraken v2 CRC32 checksum over the top 10 asks (ascending) and top 10 bids (descending)."""
    pp, qp = precision if precision else (None, None)
    parts: list[str] = []
    for p, q in book.top_asks(CHECKSUM_LEVELS):
        parts.append(_fmt(p, pp) + _fmt(q, qp))
    for p, q in book.top_bids(CHECKSUM_LEVELS):
        parts.append(_fmt(p, pp) + _fmt(q, qp))
    return zlib.crc32("".join(parts).encode()) & 0xFFFFFFFF


def _dec(x: object) -> Decimal:
    return x if isinstance(x, Decimal) else Decimal(str(x))


def _precision(state: NormalizerState, sym: str) -> tuple[int, int] | None:
    learned = state.meta.get("precision", {}).get(sym)
    if learned:
        return (int(learned[0]), int(learned[1]))
    return PRECISION.get(sym)


class KrakenFeed(FeedClient):
    venue: ClassVar[str] = VENUE
    default_stream: ClassVar[str] = "kraken.ws"
    default_url: ClassVar[str] = URL
    default_symbols: ClassVar[tuple[str, ...]] = ("BTC/USD",)
    default_channels: ClassVar[tuple[str, ...]] = ("book", "trade")
    stale_after_s: ClassVar[float] = 5.0  # heartbeat channel ~1/s
    dead_after_s: ClassVar[float] = 30.0
    keepalive_interval_s: ClassVar[float | None] = 30.0

    @property
    def depth(self) -> int:
        return int(self.options.get("depth", DEFAULT_DEPTH))

    def _book_sub(self, method: str, req_id: int) -> str:
        params = {"channel": "book", "symbol": list(self.symbols), "depth": self.depth}
        if method == "subscribe":
            params["snapshot"] = True
        return dumps({"method": method, "params": params, "req_id": req_id})

    def subscribe_messages(self) -> list[str]:
        out = []
        if "instrument" in self.channels:
            out.append(dumps({"method": "subscribe", "params": {"channel": "instrument", "snapshot": True}, "req_id": 3}))
        if "book" in self.channels:
            out.append(self._book_sub("subscribe", 1))
        if "trade" in self.channels:
            out.append(
                dumps({"method": "subscribe", "params": {"channel": "trade", "symbol": list(self.symbols), "snapshot": False}, "req_id": 2})
            )
        return out

    def resubscribe_messages(self) -> list[str] | None:
        if "book" not in self.channels:
            return []
        return [self._book_sub("unsubscribe", 11), self._book_sub("subscribe", 12)]

    def keepalive_message(self) -> str | None:
        return dumps({"method": "ping"})

    @staticmethod
    def normalize(raw: bytes, recv_ns: int, state: NormalizerState) -> list[Event]:
        if is_marker(raw):
            return handle_marker(raw, recv_ns, state)
        msg = _loads(raw)
        if "method" in msg:
            if msg.get("success") is False:
                return [FeedStatus(recv_ns, 0, state.stream, "error", f"{msg.get('method')}: {msg.get('error')}")]
            res = msg.get("result") or {}
            if msg.get("method") == "subscribe" and res.get("channel") == "book" and res.get("depth"):
                state.meta.setdefault("depth", {})[res.get("symbol", "")] = int(res["depth"])
            return []
        ch = msg.get("channel")
        if ch == "book":
            return _book(msg, recv_ns, state)
        if ch == "trade":
            out: list[Event] = []
            snap = msg.get("type") == "snapshot"
            for t in msg.get("data", ()):
                sym = t.get("symbol", "")
                tid = str(t.get("trade_id", ""))
                tx = parse_rfc3339_ns(t["timestamp"]) if t.get("timestamp") else 0
                if trade_seen(state, sym, tx, tid) or snap or is_history(state, tx):
                    continue
                side = str(t.get("side", "")).lower()
                out.append(ExtTrade(recv_ns, tx, VENUE, sym, float(t["price"]), float(t["qty"]), side if side in ("buy", "sell") else "", tid))
            return out
        if ch == "instrument":
            prec = state.meta.setdefault("precision", {})
            for p in (msg.get("data") or {}).get("pairs", ()) if isinstance(msg.get("data"), dict) else ():
                if "symbol" in p and "price_precision" in p and "qty_precision" in p:
                    prec[p["symbol"]] = [int(p["price_precision"]), int(p["qty_precision"])]
            return []
        if ch == "status":
            for d in msg.get("data", ()):
                if d.get("system") not in (None, "online"):
                    return [FeedStatus(recv_ns, 0, state.stream, "error", f"system {d.get('system')}")]
            return []
        return []  # heartbeat and anything else


def _depth_for(state: NormalizerState, sym: str, n_bids: int, n_asks: int) -> int:
    d = state.meta.get("depth", {}).get(sym)
    if d:
        return int(d)
    n = max(n_bids, n_asks)
    for v in VALID_DEPTHS:
        if n <= v:
            return v
    return VALID_DEPTHS[-1]


def _book(msg: dict, recv_ns: int, state: NormalizerState) -> list[Event]:
    out: list[Event] = []
    typ = msg.get("type")
    for d in msg.get("data", ()):
        sym = d.get("symbol", "")
        tx = parse_rfc3339_ns(d["timestamp"]) if d.get("timestamp") else 0
        if typ == "snapshot":
            bids, asks = d.get("bids", ()), d.get("asks", ())
            book = DepthBook(_depth_for(state, sym, len(bids), len(asks)))
            for lvl in bids:
                book.set("b", _dec(lvl["price"]), _dec(lvl["qty"]))
            for lvl in asks:
                book.set("a", _dec(lvl["price"]), _dec(lvl["qty"]))
            book.truncate()
            state.books[sym] = book
            if VERIFY_CHECKSUM and "checksum" in d:
                ok = book_checksum(book, _precision(state, sym)) == int(d["checksum"])
                state.bump("checksum_ok" if ok else "checksum_fail")
                if not ok:
                    out += book_gap(state, sym, recv_ns, "checksum mismatch on snapshot")
                    continue
            b, a = book.snapshot_levels()
            out.append(snapshot_event(recv_ns, tx, VENUE, sym, b, a, depth_limited=True))
            out += book_snapshot_ok(state, sym, recv_ns)
        elif typ == "update":
            book = state.books.get(sym)
            if book is None or not book_valid(state, sym):
                state.bump("deltas_suppressed")
                continue
            changes: list[tuple[str, float, float]] = []
            for side, key in (("b", "bids"), ("a", "asks")):
                for lvl in d.get(key, ()):
                    p, q = _dec(lvl["price"]), _dec(lvl["qty"])
                    book.set(side, p, q)
                    changes.append((side, float(p), float(q)))
            changes += book.truncate()
            if VERIFY_CHECKSUM and "checksum" in d:
                ok = book_checksum(book, _precision(state, sym)) == int(d["checksum"])
                state.bump("checksum_ok" if ok else "checksum_fail")
                if not ok:
                    out += book_gap(state, sym, recv_ns, f"checksum mismatch (expected {d['checksum']})")
                    continue
            if changes:
                out.append(ExtBookDelta(recv_ns, tx, VENUE, sym, tuple(changes)))
    return out
