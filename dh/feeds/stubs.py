"""Other BRTI constituent exchanges whose free public API could not be pinned down offline.

CF Benchmarks' BRTI constituent list changes over time (check the current "CF Bitcoin Real Time
Index" constituent page before relying on any list). Venues covered with implemented adapters:
Coinbase, Kraken, Bitstamp, Gemini, Crypto.com (and Paxos/itBit, experimental). The venues below
are stubs: the class exists so config/registry/composite code can name them, but ``run`` and
``normalize`` raise NotImplementedError.

TODO(bullish): Bullish Exchange public market data. Believed to be a JSON-RPC style WebSocket
  (e.g. wss://api.exchange.bullish.com/trading-api/v1/market-data/orderbook with a
  ``{"jsonrpc":"2.0","type":"command","method":"subscribe","params":{"topic":"l2Orderbook",
  "symbol":"BTCUSDC"},"id":"1"}`` subscription). Unverified: endpoint, topic names, whether the
  BRTI pair is BTC/USD or BTC/USDC, snapshot/delta semantics and sequence fields.
TODO(lmax): LMAX Digital market data. Historically requires an account (FIX / authenticated
  WebSocket); no documented free public order-book stream. Likely unobtainable for free:
  approximate its contribution in the composite by re-weighting the other constituents.
"""

from __future__ import annotations

from typing import ClassVar

from dh.core.events import Event
from dh.feeds.base import FeedClient, NormalizerState


class _StubFeed(FeedClient):
    implemented: ClassVar[bool] = False

    @staticmethod
    def normalize(raw: bytes, recv_ns: int, state: NormalizerState) -> list[Event]:
        raise NotImplementedError(f"{state.venue}: stub feed (see dh/feeds/stubs.py TODOs)")


class BullishFeed(_StubFeed):
    venue: ClassVar[str] = "bullish"
    default_stream: ClassVar[str] = "bullish.ws"
    default_url: ClassVar[str] = ""  # TODO(bullish)
    default_symbols: ClassVar[tuple[str, ...]] = ("BTCUSDC",)


class LmaxFeed(_StubFeed):
    venue: ClassVar[str] = "lmax"
    default_stream: ClassVar[str] = "lmax.ws"
    default_url: ClassVar[str] = ""  # TODO(lmax)
    default_symbols: ClassVar[tuple[str, ...]] = ("BTC/USD",)
