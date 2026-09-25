"""Crypto.com Exchange API v1 market WebSocket, BTC_USD book (depth 50) + trades.
BRTI constituent.

ASSUMED WIRE FORMAT (exchange-docs.crypto.com "Exchange API v1", book.{instrument}.{depth},
trade.{instrument}; verify live with ``python scripts/smoke_feeds.py --venues cryptocom``):

  endpoint   wss://stream.crypto.com/exchange/v1/market  (wait ~1 s after connecting before
             sending requests: rate limits are pro-rated from connection time)
  subscribe  {"id":1,"method":"subscribe","params":{"channels":["book.BTC_USD.50"],
              "book_subscription_type":"SNAPSHOT_AND_UPDATE","book_update_frequency":10},
              "nonce":<ms>}
             {"id":2,"method":"subscribe","params":{"channels":["trade.BTC_USD"]},"nonce":<ms>}
  snapshot   {"id":-1,"method":"subscribe","code":0,"result":{"instrument_name":"BTC_USD",
              "subscription":"book.BTC_USD.50","channel":"book","depth":50,"data":[{"asks":
              [["30082.5","0.1689","1"],...],"bids":[...],"t":1654780033786,"tt":...,"u":542048017824}]}}
  delta      same envelope with "channel":"book.update" and data [{"update":{"asks":[...],
              "bids":[...]},"t":...,"tt":...,"u":542048017825,"pu":542048017824}]
             Level = [price, ABSOLUTE quantity, order count]; quantity "0" deletes. ``pu`` must
             equal the previous ``u`` or the book is broken (resubscribe). Levels that leave
             the top-50 window are not guaranteed to be deleted explicitly, so the normalizer
             truncates to the subscribed depth. Empty deltas may be sent as keep-alives.
  trade      "channel":"trade", data [{"d":"<trade id>","t":<ms>,"tn":"<ns>","p":"51327.5",
              "q":"0.0001","s":"BUY"|"SELL","i":"BTC_USD"}]; ``s`` = TAKER side.
  heartbeat  {"id":<n>,"method":"public/heartbeat","code":0} every ~30 s; the client MUST reply
             {"id":<n>,"method":"public/respond-heartbeat"} within 5 s or it is disconnected.
"""

from __future__ import annotations

import time
from typing import ClassVar

import orjson

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
    ms_to_ns,
    snapshot_event,
    to_dec,
    trade_seen,
)

VENUE = "cryptocom"
URL = "wss://stream.crypto.com/exchange/v1/market"
DEFAULT_DEPTH = 50


class CryptoComFeed(FeedClient):
    venue: ClassVar[str] = VENUE
    default_stream: ClassVar[str] = "cryptocom.ws"
    default_url: ClassVar[str] = URL
    default_symbols: ClassVar[tuple[str, ...]] = ("BTC_USD",)
    default_channels: ClassVar[tuple[str, ...]] = ("book", "trade")
    stale_after_s: ClassVar[float] = 10.0
    dead_after_s: ClassVar[float] = 60.0
    subscribe_delay_s: ClassVar[float] = 1.0

    def __init__(self, *a, **kw) -> None:  # type: ignore[no-untyped-def]
        super().__init__(*a, **kw)
        self._req_id = 0

    @property
    def depth(self) -> int:
        return int(self.options.get("depth", DEFAULT_DEPTH))

    def _next_id(self) -> int:
        self._req_id += 1
        return self._req_id

    def _book_channels(self) -> list[str]:
        return [f"book.{s}.{self.depth}" for s in self.symbols]

    def _book_sub(self, method: str) -> str:
        params: dict = {"channels": self._book_channels()}
        if method == "subscribe":
            params["book_subscription_type"] = "SNAPSHOT_AND_UPDATE"
            params["book_update_frequency"] = int(self.options.get("book_update_frequency_ms", 10))
        return dumps({"id": self._next_id(), "method": method, "params": params, "nonce": time.time_ns() // 1_000_000})

    def subscribe_messages(self) -> list[str]:
        out = []
        if "book" in self.channels:
            out.append(self._book_sub("subscribe"))
        if "trade" in self.channels:
            out.append(
                dumps({"id": self._next_id(), "method": "subscribe", "params": {"channels": [f"trade.{s}" for s in self.symbols]}, "nonce": time.time_ns() // 1_000_000})
            )
        return out

    def resubscribe_messages(self) -> list[str] | None:
        if "book" not in self.channels:
            return []
        return [self._book_sub("unsubscribe"), self._book_sub("subscribe")]

    def control_replies(self, raw: bytes) -> list[str]:
        if b"public/heartbeat" in raw:
            try:
                hid = orjson.loads(raw).get("id")
            except orjson.JSONDecodeError:
                return []
            return [dumps({"id": hid, "method": "public/respond-heartbeat"})]
        return []

    @staticmethod
    def normalize(raw: bytes, recv_ns: int, state: NormalizerState) -> list[Event]:
        if is_marker(raw):
            return handle_marker(raw, recv_ns, state)
        msg = orjson.loads(raw)
        if msg.get("method") == "public/heartbeat":
            return []
        code = msg.get("code", 0)
        if code not in (0, None):
            return [FeedStatus(recv_ns, 0, state.stream, "error", f"code {code}: {msg.get('message', '')}"[:300])]
        res = msg.get("result")
        if not isinstance(res, dict):
            return []
        ch = res.get("channel")
        sym = res.get("instrument_name", "")
        out: list[Event] = []
        if ch == "book":
            depth = int(res.get("depth") or DEFAULT_DEPTH)
            for d in res.get("data", ()):
                book = DepthBook(depth)
                for p, q, *_ in d.get("bids", ()):
                    book.set("b", to_dec(p), to_dec(q))
                for p, q, *_ in d.get("asks", ()):
                    book.set("a", to_dec(p), to_dec(q))
                book.truncate()
                state.books[sym] = book
                u = int(d.get("u", 0))
                state.seq[f"u:{sym}"] = u
                b, a = book.snapshot_levels()
                out.append(snapshot_event(recv_ns, ms_to_ns(d.get("t", 0)), VENUE, sym, b, a, seq=u, depth_limited=True))
                out += book_snapshot_ok(state, sym, recv_ns)
        elif ch == "book.update":
            for d in res.get("data", ()):
                u, pu = int(d.get("u", 0)), int(d.get("pu", 0))
                book = state.books.get(sym)
                if book is None or not book_valid(state, sym):
                    state.bump("deltas_suppressed")
                    continue
                last = state.seq.get(f"u:{sym}")
                if last is not None and pu != last:
                    if u <= last:
                        state.bump("stale_update_dropped")
                        continue
                    out += book_gap(state, sym, recv_ns, f"pu {pu} != last u {last}")
                    continue
                state.seq[f"u:{sym}"] = u
                upd = d.get("update", {})
                changes: list[tuple[str, float, float]] = []
                for side, key in (("b", "bids"), ("a", "asks")):
                    for p, q, *_ in upd.get(key, ()):
                        dp, dq = to_dec(p), to_dec(q)
                        book.set(side, dp, dq)
                        changes.append((side, float(dp), float(dq)))
                changes += book.truncate()
                if changes:
                    out.append(ExtBookDelta(recv_ns, ms_to_ns(d.get("t", 0)), VENUE, sym, tuple(changes), seq=u, prev_seq=pu))
        elif ch == "trade":
            for t in res.get("data", ()):
                isym = t.get("i", sym)
                tid = str(t.get("d", ""))
                tx = int(t["tn"]) if t.get("tn") else ms_to_ns(t.get("t", 0))
                if trade_seen(state, isym, tx, tid) or is_history(state, tx):
                    continue
                side = str(t.get("s", "")).lower()
                out.append(ExtTrade(recv_ns, tx, VENUE, isym, float(t["p"]), float(t["q"]), side if side in ("buy", "sell") else "", tid))
        return out
