"""Binance USD-M futures public streams for BTCUSDT: bookTicker, aggTrade, markPrice@1s
(mark, index, funding), forceOrder (liquidations).

GEO-RESTRICTION: fstream.binance.com refuses connections from the US and some other
jurisdictions (HTTP 451 / 403). Run this feed only where Binance permits it; it is disabled by
default in config/feeds.yaml.

ASSUMED WIRE FORMAT (developers.binance.com "USDS-M Futures / WebSocket Market Streams";
verify live with ``python scripts/smoke_feeds.py --venues binance_futures``):

  endpoint  wss://fstream.binance.com/stream?streams=btcusdt@bookTicker/btcusdt@aggTrade/
            btcusdt@markPrice@1s/btcusdt@forceOrder   (combined: {"stream":"...","data":{...}})
            NOTE: Binance announced a split of futures WS URLs into /public, /market and
            /private paths; if the legacy URL stops working, set ``url`` in config/feeds.yaml
            (e.g. wss://fstream.binance.com/public/stream?streams=... for bookTicker). MUST VERIFY.
  bookTicker {"e":"bookTicker","u":<update id>,"E":<ms>,"T":<ms>,"s":"BTCUSDT","b":"bid","B":"qty",
              "a":"ask","A":"qty"}   (qty in BTC)
  aggTrade   {"e":"aggTrade","E":<ms>,"a":<agg id>,"s":"BTCUSDT","p":"price","q":"qty","f":..,"l":..,
              "T":<ms>,"m":<buyer is maker>}  -> aggressor = "sell" if m else "buy"
  markPrice  {"e":"markPriceUpdate","E":<ms>,"s":"BTCUSDT","p":"mark","i":"index","P":"est. settle",
              "r":"funding rate","T":<next funding ms>}
  forceOrder {"e":"forceOrder","E":<ms>,"o":{"s":"BTCUSDT","S":"SELL"|"BUY","o":"LIMIT","f":"IOC",
              "q":"orig qty","p":"price","ap":"avg price","X":"FILLED","l":"last qty","z":"cum qty",
              "T":<ms>}}   ``S`` = side of the liquidation ORDER (SELL = a long was liquidated).
              Binance pushes at most one liquidation per symbol per second (a sample, not all).
  Server pings every ~3 min (auto-ponged by the websocket library); connections are cut after
  24 h (the client reconnects).

Funding interval assumed 8 h for BTCUSDT (FUNDING_INTERVAL_S); Binance can change intervals per
symbol (GET /fapi/v1/fundingInfo).
"""

from __future__ import annotations

from typing import ClassVar

import orjson

from dh.core.events import Event, ExtBBO, ExtTrade, FeedStatus, Liquidation, PerpState
from dh.feeds.base import FeedClient, NormalizerState, handle_marker, is_history, is_marker, ms_to_ns, trade_seen

VENUE = "binance_futures"
BASE_URL = "wss://fstream.binance.com/stream?streams="
FUNDING_INTERVAL_S = 8 * 3600
STREAM_SUFFIX = {"bookTicker": "bookTicker", "aggTrade": "aggTrade", "markPrice": "markPrice@1s", "forceOrder": "forceOrder"}


class BinanceFuturesFeed(FeedClient):
    venue: ClassVar[str] = VENUE
    default_stream: ClassVar[str] = "binance_futures.ws"
    default_url: ClassVar[str] = ""  # built from symbols/channels
    default_symbols: ClassVar[tuple[str, ...]] = ("BTCUSDT",)
    default_channels: ClassVar[tuple[str, ...]] = ("bookTicker", "aggTrade", "markPrice", "forceOrder")
    stale_after_s: ClassVar[float] = 5.0
    dead_after_s: ClassVar[float] = 30.0
    resnapshot_interval_s: ClassVar[float] = 0.0  # BBO only: nothing to resnapshot
    geo_note: ClassVar[str] = "fstream.binance.com is geo-restricted (e.g. US IPs get HTTP 451)"

    def __init__(self, *a, **kw) -> None:  # type: ignore[no-untyped-def]
        super().__init__(*a, **kw)
        if not self.url:
            streams = [f"{s.lower()}@{STREAM_SUFFIX.get(c, c)}" for s in self.symbols for c in self.channels]
            self.url = BASE_URL + "/".join(streams)

    @staticmethod
    def normalize(raw: bytes, recv_ns: int, state: NormalizerState) -> list[Event]:
        if is_marker(raw):
            return handle_marker(raw, recv_ns, state)
        msg = orjson.loads(raw)
        d = msg.get("data", msg) if isinstance(msg, dict) else {}
        if not isinstance(d, dict):
            return []
        e = d.get("e")
        if e == "bookTicker":
            sym = d["s"]
            u = int(d.get("u", 0))
            key = f"u:{sym}"
            if u and u < state.seq.get(key, 0):
                state.bump("bbo_out_of_order")
                return []
            state.seq[key] = u
            return [
                ExtBBO(recv_ns, ms_to_ns(d.get("T") or d.get("E") or 0), VENUE, sym, float(d["b"]), float(d["B"]), float(d["a"]), float(d["A"]), seq=u)
            ]
        if e == "aggTrade":
            sym = d["s"]
            aid = int(d["a"])
            tx = ms_to_ns(d.get("T") or d.get("E") or 0)
            if trade_seen(state, sym, tx, str(aid)) or is_history(state, tx):
                return []
            key = f"a:{sym}"
            last = state.seq.get(key)
            if last is not None and aid > last + 1:
                state.bump("aggtrade_id_gaps")  # informational (some trades may be excluded)
            if last is None or aid > last:
                state.seq[key] = aid
            return [ExtTrade(recv_ns, tx, VENUE, sym, float(d["p"]), float(d["q"]), "sell" if d.get("m") else "buy", str(aid))]
        if e == "markPriceUpdate":
            sym = d["s"]
            return [
                PerpState(
                    ts=recv_ns,
                    ts_exch=ms_to_ns(d.get("E", 0)),
                    venue=VENUE,
                    symbol=sym,
                    mark=float(d.get("p", 0) or 0),
                    index=float(d.get("i", 0) or 0),
                    funding_rate=float(d.get("r", 0) or 0),
                    funding_interval_s=FUNDING_INTERVAL_S,
                    next_funding_ts=ms_to_ns(d.get("T", 0) or 0),
                    open_interest=0.0,
                )
            ]
        if e == "forceOrder":
            o = d.get("o", {})
            side = str(o.get("S", "")).lower()
            px = float(o.get("ap") or 0) or float(o.get("p") or 0)
            qty = float(o.get("z") or 0) or float(o.get("q") or 0)
            return [Liquidation(recv_ns, ms_to_ns(o.get("T") or d.get("E") or 0), VENUE, o.get("s", ""), side if side in ("buy", "sell") else "", px, qty)]  # type: ignore[arg-type]
        if "error" in msg or ("code" in msg and "msg" in msg):
            return [FeedStatus(recv_ns, 0, state.stream, "error", str(msg)[:300])]
        return []
