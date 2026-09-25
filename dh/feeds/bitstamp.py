"""Bitstamp WebSocket v2 diff order book + live trades, with REST snapshot alignment.
BRTI constituent.

ASSUMED WIRE FORMAT (bitstamp.net/websocket/v2 and /api "Order book" docs; verify live with
``python scripts/smoke_feeds.py --venues bitstamp``):

  endpoint   wss://ws.bitstamp.net
  subscribe  {"event":"bts:subscribe","data":{"channel":"diff_order_book_btcusd"}}
             {"event":"bts:subscribe","data":{"channel":"live_trades_btcusd"}}
  ack        {"event":"bts:subscription_succeeded","channel":"diff_order_book_btcusd","data":{}}
  diff book  {"event":"data","channel":"diff_order_book_btcusd","data":{"timestamp":"1712345678",
              "microtimestamp":"1712345678123456","bids":[["65000.00","0.12345678"],...],
              "asks":[...]}}  -- amounts are the ABSOLUTE new size at the level, "0..." deletes.
              No sequence numbers: continuity cannot be checked; only microtimestamps.
  trade      {"event":"trade","channel":"live_trades_btcusd","data":{"id":212337562,
              "timestamp":"1643643584","amount":0.0028,"amount_str":"0.00280000","price":38416,
              "price_str":"38416","type":0|1,"microtimestamp":"1643643584587000",
              "buy_order_id":...,"sell_order_id":...}}; type 0 = buy, 1 = sell = TAKER side.
  control    {"event":"bts:request_reconnect",...} -> client reconnects;
             {"event":"bts:heartbeat"} -> {"event":"bts:heartbeat","data":{"status":"success"}}
  REST       GET https://www.bitstamp.net/api/v2/order_book/btcusd/  ->
             {"timestamp":"...","microtimestamp":"1712345678000000","bids":[["p","a"],...],
              "asks":[...]}   (full depth, grouped by price)

Snapshot alignment (Bitstamp's documented procedure): subscribe to the diff channel first and
buffer diffs; fetch the REST book; discard buffered diffs with microtimestamp <= the book's
microtimestamp; apply the rest; afterwards drop any diff not newer than the book. The
normalizer keeps a rolling 60 s buffer of diffs in its state and folds the newer ones into the
snapshot, emitting ONE ExtBookSnapshot at the REST receive time (ts_exch = microtimestamp of
the last folded diff). The REST response is recorded in the stream as a ``_dh: rest`` marker,
so alignment is identical in replay. Snapshots are refreshed every ``resnapshot_interval_s``
(default 1 h) because the diff channel has no gap detection.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, ClassVar

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
    snapshot_event,
    trade_seen,
)

log = logging.getLogger(__name__)

VENUE = "bitstamp"
URL = "wss://ws.bitstamp.net"
REST_BOOK_URL = "https://www.bitstamp.net/api/v2/order_book/{pair}/"
BUFFER_US = 60_000_000  # rolling diff buffer kept for snapshot alignment (60 s)
BUFFER_MAX = 20_000


def _levels(rows: list) -> list[tuple[float, float]]:
    return [(float(r[0]), float(r[1])) for r in rows]


def _on_rest(m: dict[str, Any], recv_ns: int, state: NormalizerState) -> list[Event]:
    if m.get("kind") != "order_book":
        return []
    sym = str(m.get("symbol", ""))
    if int(m.get("status", 0)) != 200:
        return [FeedStatus(recv_ns, 0, state.stream, "error", f"REST order_book HTTP {m.get('status')}")]
    body = orjson.loads(m["body"])
    smts = int(body["microtimestamp"])
    bids = {float(p): float(a) for p, a, *_ in body.get("bids", ())}
    asks = {float(p): float(a) for p, a, *_ in body.get("asks", ())}
    buf = state.buffers.get(sym, [])
    last = smts
    folded = 0
    if buf and buf[0][0] > smts and not book_valid(state, sym):
        state.bump("alignment_uncertain")  # buffering started after the snapshot time
    for mts, db, da in buf:
        if mts <= smts:
            continue
        for p, a, *_ in db:
            pf, af = float(p), float(a)
            if af > 0:
                bids[pf] = af
            else:
                bids.pop(pf, None)
        for p, a, *_ in da:
            pf, af = float(p), float(a)
            if af > 0:
                asks[pf] = af
            else:
                asks.pop(pf, None)
        last = max(last, mts)
        folded += 1
    state.seq[f"mts:{sym}"] = last
    state.meta.setdefault("folded", {})[sym] = folded
    out: list[Event] = [
        snapshot_event(recv_ns, last * 1000, VENUE, sym, bids.items(), asks.items(), depth_limited=False)
    ]
    out += book_snapshot_ok(state, sym, recv_ns, detail=f"rest snapshot, {folded} diffs folded")
    return out


class BitstampFeed(FeedClient):
    venue: ClassVar[str] = VENUE
    default_stream: ClassVar[str] = "bitstamp.ws"
    default_url: ClassVar[str] = URL
    default_symbols: ClassVar[tuple[str, ...]] = ("btcusd",)
    default_channels: ClassVar[tuple[str, ...]] = ("diff_order_book", "live_trades")
    stale_after_s: ClassVar[float] = 10.0
    dead_after_s: ClassVar[float] = 60.0
    keepalive_interval_s: ClassVar[float | None] = 20.0

    def __init__(self, *a: Any, **kw: Any) -> None:
        super().__init__(*a, **kw)
        self._snap_evt: asyncio.Event | None = None

    def subscribe_messages(self) -> list[str]:
        return [
            dumps({"event": "bts:subscribe", "data": {"channel": f"{ch}_{sym}"}})
            for ch in self.channels
            for sym in self.symbols
        ]

    def keepalive_message(self) -> str | None:
        return dumps({"event": "bts:heartbeat"})

    def control_replies(self, raw: bytes) -> list[str]:
        if b'"bts:request_reconnect"' in raw:
            self.request_reconnect("server requested reconnect")
        return []

    async def resync(self, reason: str) -> None:
        if self._snap_evt is not None:
            self._snap_evt.set()

    def extra_tasks(self) -> list:
        if "diff_order_book" not in self.channels:
            return []
        self._snap_evt = asyncio.Event()
        return [self._rest_loop()]

    async def _rest_loop(self) -> None:
        import aiohttp

        assert self._snap_evt is not None
        await asyncio.sleep(float(self.options.get("snapshot_delay_s", 1.5)))  # let diffs buffer
        timeout = aiohttp.ClientTimeout(total=float(self.options.get("rest_timeout_s", 15)))
        async with aiohttp.ClientSession(timeout=timeout, trust_env=True) as session:
            while True:
                for sym in self.symbols:
                    await self._fetch(session, sym)
                await self._snap_evt.wait()
                self._snap_evt.clear()

    async def _fetch(self, session: Any, sym: str) -> None:
        url = str(self.options.get("rest_url", REST_BOOK_URL)).format(pair=sym)
        req_ns = time.time_ns()
        try:
            async with session.get(url) as resp:
                body = await resp.read()
                self.record_rest("order_book", url, resp.status, body, req_ns, symbol=sym)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            log.warning("%s: REST snapshot failed: %s", self.name, exc)
            self._marker("status", status="error", detail=f"REST snapshot failed: {type(exc).__name__}: {exc}"[:300])

    @staticmethod
    def normalize(raw: bytes, recv_ns: int, state: NormalizerState) -> list[Event]:
        if is_marker(raw):
            return handle_marker(raw, recv_ns, state, on_rest=_on_rest)
        msg = orjson.loads(raw)
        ev = msg.get("event")
        ch = str(msg.get("channel", ""))
        if ev == "data" and ch.startswith("diff_order_book_"):
            sym = ch[len("diff_order_book_") :]
            d = msg["data"]
            mts = int(d["microtimestamp"])
            bids, asks = d.get("bids", []), d.get("asks", [])
            buf = state.buffers.setdefault(sym, [])
            buf.append([mts, bids, asks])
            cutoff = mts - BUFFER_US
            drop = 0
            while drop < len(buf) and buf[drop][0] < cutoff:
                drop += 1
            if len(buf) - drop > BUFFER_MAX:
                drop = len(buf) - BUFFER_MAX
            if drop:
                del buf[:drop]
            if not book_valid(state, sym):
                state.bump("deltas_buffered")
                return []
            key = f"mts:{sym}"
            if mts <= state.seq.get(key, 0):
                state.bump("diff_not_newer_than_book")
                return []
            state.seq[key] = mts
            changes = tuple([("b", p, s) for p, s in _levels(bids)] + [("a", p, s) for p, s in _levels(asks)])
            if not changes:
                return []
            return [ExtBookDelta(recv_ns, mts * 1000, VENUE, sym, changes)]
        if ev == "trade" and ch.startswith("live_trades_"):
            sym = ch[len("live_trades_") :]
            d = msg["data"]
            tid = str(d.get("id", ""))
            tx = int(d["microtimestamp"]) * 1000 if d.get("microtimestamp") else int(d.get("timestamp", 0)) * 10**9
            if trade_seen(state, sym, tx, tid) or is_history(state, tx):
                return []
            price = float(d["price_str"]) if "price_str" in d else float(d["price"])
            size = float(d["amount_str"]) if "amount_str" in d else float(d["amount"])
            typ = d.get("type")
            aggr = "buy" if typ == 0 else "sell" if typ == 1 else ""
            return [ExtTrade(recv_ns, tx, VENUE, sym, price, size, aggr, tid)]
        if ev == "bts:error":
            return [FeedStatus(recv_ns, 0, state.stream, "error", str(msg.get("data", ""))[:300])]
        return []
