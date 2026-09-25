"""Venue registry: stream name -> normalizer, config -> FeedClient instances.

Stream names are ``<venue>.<suffix>`` (``coinbase.ws``, ``deribit.options``); the part before
the first '.' selects the venue class, so several connections of one venue (with different
suffixes) share the same pure normalizer but keep separate NormalizerState in replay.
"""

from __future__ import annotations

import importlib
from collections.abc import Callable
from typing import Any

from dh.core.events import Event
from dh.feeds.base import FeedClient, FeedConfig, NormalizerState

# venue -> "module:Class" (imported lazily so a broken optional adapter never breaks the rest)
VENUES: dict[str, str] = {
    "coinbase": "dh.feeds.coinbase:CoinbaseFeed",
    "kraken": "dh.feeds.kraken:KrakenFeed",
    "bitstamp": "dh.feeds.bitstamp:BitstampFeed",
    "gemini": "dh.feeds.gemini:GeminiFeed",
    "cryptocom": "dh.feeds.cryptocom:CryptoComFeed",
    "paxos": "dh.feeds.paxos:PaxosFeed",
    "bullish": "dh.feeds.stubs:BullishFeed",
    "lmax": "dh.feeds.stubs:LmaxFeed",
    "deribit": "dh.feeds.deribit:DeribitFeed",
    "binance_futures": "dh.feeds.binance_futures:BinanceFuturesFeed",
    "bybit": "dh.feeds.bybit:BybitFeed",
    "okx": "dh.feeds.okx:OkxFeed",
    "hyperliquid": "dh.feeds.hyperliquid:HyperliquidFeed",
    "kalshi_perp": "dh.feeds.kalshi_perp:KalshiPerpFeed",
}

# CF Benchmarks BRTI constituents we can observe (spot USD books). Research may re-weight.
SPOT_CONSTITUENTS = ("coinbase", "kraken", "bitstamp", "gemini", "cryptocom", "paxos", "bullish", "lmax")

Normalizer = Callable[[bytes, int, NormalizerState], list[Event]]


def venue_of(stream: str) -> str:
    return stream.split(".", 1)[0]


def feed_class(venue: str) -> type[FeedClient]:
    try:
        target = VENUES[venue]
    except KeyError:
        raise KeyError(f"unknown venue {venue!r}; known: {sorted(VENUES)}") from None
    mod, cls = target.split(":")
    return getattr(importlib.import_module(mod), cls)


def has_normalizer(stream: str) -> bool:
    return venue_of(stream) in VENUES


def normalizer_for(stream: str) -> tuple[Normalizer, NormalizerState]:
    """(pure normalize function, fresh state) for a recorded external-venue stream."""
    cls = feed_class(venue_of(stream))
    return cls.normalize, cls.new_state(stream)


def build_feed(name: str, spec: dict[str, Any], **kw: Any) -> FeedClient:
    """Instantiate a FeedClient from a config/feeds.yaml entry.

    ``name`` is the config key; ``spec['venue']`` defaults to the key's prefix before '_' is
    NOT assumed: set ``venue`` explicitly when the key differs from the venue name.
    """
    spec = dict(spec)
    venue = spec.pop("venue", name)
    cls = feed_class(venue)
    cfg = FeedConfig.from_mapping(spec)
    if cfg.stream is None:
        cfg.stream = cls.default_stream
    if venue_of(cfg.stream) != venue:
        raise ValueError(f"feed {name}: stream {cfg.stream!r} must start with '{venue}.'")
    return cls(cfg, **kw)
