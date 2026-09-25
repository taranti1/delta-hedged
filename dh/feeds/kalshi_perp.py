"""Kalshi Perps (KXBTCPERP) market-data feed: INTERFACE STUB ONLY.

TODO(kalshi-perps): the Kalshi Perps specifications (``perps.openapi.yaml``,
``perps.asyncapi.yaml``) and fee page could not be obtained in the build environment (egress
blocked; no official SDK on npm/PyPI; see docs/ENVIRONMENT.md). When they are available:

  1. Vendor them into docs/kalshi_specs/ with provenance (version, sha256).
  2. Fill in ``default_url``, ``subscribe_messages`` (book/trades/funding/mark channels for
     KXBTCPERP), heartbeat handling and authentication (the prediction-market WS authenticates
     at the handshake with KALSHI-ACCESS-* headers; perps likely similar).
  3. Implement ``normalize`` -> ExtBookSnapshot / ExtBookDelta (absolute sizes, BTC units),
     ExtTrade (aggressor side), PerpState (mark, index, funding per interval, next funding
     time, open interest), Liquidation; with sequence-gap detection and resync.
  4. Add fixture frames from the spec examples to tests/feeds and a smoke check.

Until then the class exists so configuration, registry and the hedge engine can reference the
venue name, but ``run`` and ``normalize`` raise NotImplementedError.
"""

from __future__ import annotations

from typing import ClassVar

from dh.core.events import Event
from dh.feeds.base import FeedClient, NormalizerState

VENUE = "kalshi_perp"


class KalshiPerpFeed(FeedClient):
    venue: ClassVar[str] = VENUE
    default_stream: ClassVar[str] = "kalshi_perp.ws"
    default_url: ClassVar[str] = ""  # TODO(kalshi-perps): from perps.asyncapi.yaml servers
    default_symbols: ClassVar[tuple[str, ...]] = ("KXBTCPERP",)
    implemented: ClassVar[bool] = False

    @staticmethod
    def normalize(raw: bytes, recv_ns: int, state: NormalizerState) -> list[Event]:
        raise NotImplementedError("Kalshi Perps feed: specs not available yet (see module TODO)")
