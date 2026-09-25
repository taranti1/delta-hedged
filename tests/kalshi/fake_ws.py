"""In-process fake of the Kalshi WS server for KalshiWS tests (no sockets).

Each connection plays a script. Commands sent by the client get protocol-correct replies
('subscribed' with fresh per-connection sids, orderbook snapshots, get_snapshot replies)
that are delivered before the rest of the script unless held. Script items:
  str/bytes          a frame
  callable(conn)     returns a frame (or None) lazily (so it can use assigned sids/seqs)
  Exception          raised from recv() (simulated disconnect)
  STALL              recv() never returns (staleness)
  RELEASE            deliver replies held while ``hold_replies`` was on
"""

from __future__ import annotations

import asyncio
from collections import deque
from typing import Any

import orjson
from websockets.exceptions import ConnectionClosedError

STALL = object()
RELEASE = object()


def dumps(obj: Any) -> str:
    return orjson.dumps(obj).decode()


class FakeConn:
    def __init__(self, script: list[Any], *, hold_replies: bool = False, drop_snapshots: bool = False) -> None:
        self.script: deque[Any] = deque(script)
        self.replies: deque[Any] = deque()  # str frames or thunks built at delivery time
        self.held: deque[Any] = deque()
        self.hold_replies = hold_replies
        self.drop_snapshots = drop_snapshots
        self.sent: list[dict[str, Any]] = []
        self.closed = False
        self.next_sid = 1
        self.seq: dict[int, int] = {}
        self.sid_channel: dict[int, str] = {}
        self.sid_tickers: dict[int, list[str]] = {}
        self._wake = asyncio.Event()

    # ------------------------------------------------------------------ frame builders
    def sid_of(self, channel: str) -> int:
        return next(s for s, c in self.sid_channel.items() if c == channel)

    def next_seq(self, sid: int, skip: int = 0) -> int:
        self.seq[sid] = self.seq.get(sid, 0) + 1 + skip
        return self.seq[sid]

    def snapshot(self, sid: int, ticker: str, cid: int | None = None) -> str:
        msg: dict[str, Any] = {"type": "orderbook_snapshot", "sid": sid, "seq": self.next_seq(sid),
                               "msg": {"market_ticker": ticker, "market_id": "m-" + ticker,
                                       "yes_dollars_fp": [["0.4500", "10.00"]], "no_dollars_fp": [["0.5300", "5.00"]]}}
        if cid is not None:
            msg["id"] = cid
        return dumps(msg)

    def delta(self, ticker: str, *, skip: int = 0, px: str = "0.4500", d: str = "1.00") -> str:
        sid = self.sid_of("orderbook_delta")
        return dumps({"type": "orderbook_delta", "sid": sid, "seq": self.next_seq(sid, skip),
                      "msg": {"market_ticker": ticker, "market_id": "m-" + ticker, "price_dollars": px,
                              "delta_fp": d, "side": "yes", "ts_ms": 1}})

    def trade(self, tid: str, ticker: str = "A") -> str:
        sid = self.sid_of("trade")
        return dumps({"type": "trade", "sid": sid, "seq": self.next_seq(sid), "msg": {
            "trade_id": tid, "market_ticker": ticker, "yes_price_dollars": "0.4600", "no_price_dollars": "0.5400",
            "count_fp": "1.00", "taker_side": "yes", "taker_outcome_side": "yes", "taker_book_side": "bid",
            "is_block_trade": False, "ts": 1, "ts_ms": 1000}})

    def index(self, value: str = "68000.10", ts_ms: int = 1710000000000) -> str:
        sid = self.sid_of("cfbenchmarks_value_5hz")
        return dumps({"type": "cfbenchmarks_value_5hz", "sid": sid, "seq": self.next_seq(sid), "msg": {
            "index_id": "BRTI", "value_usd": value, "source_ts_ms": ts_ms, "received_at": ts_ms + 10, "data": ""}})

    def error(self, code: int, sid: int | None = None) -> str:
        m: dict[str, Any] = {"type": "error", "msg": {"code": code, "msg": "boom"}}
        if sid is not None:
            m["sid"] = sid
        return dumps(m)

    # ------------------------------------------------------------------ protocol
    def _reply(self, frame: Any) -> None:
        (self.held if self.hold_replies else self.replies).append(frame)

    async def send(self, message: str) -> None:
        cmd = orjson.loads(message)
        self.sent.append(cmd)
        p = cmd.get("params") or {}
        if cmd["cmd"] == "subscribe":
            for ch in p["channels"]:
                sid = self.next_sid
                self.next_sid += 1
                self.sid_channel[sid] = ch
                self.replies.append(dumps({"id": cmd["id"], "type": "subscribed", "msg": {"channel": ch, "sid": sid}}))
                if ch == "orderbook_delta":
                    self.sid_tickers[sid] = list(p.get("market_tickers") or [])
                    for t in self.sid_tickers[sid]:
                        self.replies.append(lambda sid=sid, t=t: self.snapshot(sid, t))
        elif cmd["cmd"] == "update_subscription":
            sid = p.get("sid")
            if p["action"] == "get_snapshot":
                if not self.drop_snapshots:
                    for t in p["market_tickers"]:
                        self._reply(lambda sid=sid, t=t, cid=cmd["id"]: self.snapshot(sid, t, cid=cid))
            elif p["action"] == "add_markets" and self.sid_channel.get(sid) == "orderbook_delta":
                self.sid_tickers[sid] += p["market_tickers"]
                tickers = list(self.sid_tickers[sid])
                self.replies.append(lambda sid=sid, cid=cmd["id"], tickers=tickers: dumps(
                    {"id": cid, "sid": sid, "seq": self.next_seq(sid), "type": "ok", "msg": {"market_tickers": tickers}}))
                for t in p["market_tickers"]:
                    self.replies.append(lambda sid=sid, t=t: self.snapshot(sid, t))
        self._wake.set()

    async def recv(self) -> str | bytes:
        while True:
            if self.closed:
                raise ConnectionClosedError(None, None)
            if self.replies:
                r = self.replies.popleft()
                return r() if callable(r) else r
            if self.script:
                item = self.script.popleft()
                if callable(item):
                    item = item(self)
                if item is None:
                    continue
                if item is RELEASE:
                    self.replies.extend(self.held)
                    self.held.clear()
                    self.hold_replies = False
                    continue
                if item is STALL:
                    await asyncio.Event().wait()
                if isinstance(item, BaseException):
                    raise item
                return item
            self._wake.clear()
            await self._wake.wait()

    async def close(self) -> None:
        self.closed = True
        self._wake.set()


class FakeKalshi:
    """Connect factory: one script (or exception) per connection attempt."""

    def __init__(self, scripts: list[Any], **conn_kw: Any) -> None:
        self.scripts = list(scripts)
        self.conn_kw = conn_kw
        self.conns: list[FakeConn] = []
        self.headers: list[dict[str, str]] = []
        self.urls: list[str] = []

    async def connect(self, url: str, headers: dict[str, str]) -> FakeConn:
        self.urls.append(url)
        self.headers.append(dict(headers))
        if not self.scripts:
            raise ConnectionRefusedError("no more scripted connections")
        script = self.scripts.pop(0)
        if isinstance(script, BaseException):
            raise script
        conn = FakeConn(script, **self.conn_kw)
        self.conns.append(conn)
        return conn
