"""Shared helpers for feed/store tests: load venue fixtures (recorder record format)."""

from __future__ import annotations

from pathlib import Path

import orjson

from dh.core.events import Event
from dh.feeds.base import NormalizerState, safe_normalize
from dh.feeds.registry import normalizer_for

FIXTURES = Path(__file__).parent / "fixtures"
T0_NS = 1790337600 * 10**9  # 2026-09-25T12:00:00Z (fixture epoch)
VENUE_FIXTURES = (
    "coinbase",
    "kraken",
    "bitstamp",
    "gemini",
    "cryptocom",
    "deribit",
    "binance_futures",
    "bybit",
    "okx",
    "hyperliquid",
)


def load_fixture(name: str) -> list[tuple[int, str, int, bytes]]:
    """[(t, stream, q, raw bytes)] from tests/feeds/fixtures/<name>.jsonl."""
    out = []
    with open(FIXTURES / f"{name}.jsonl", "rb") as f:
        for line in f:
            if line.strip():
                o = orjson.loads(line)
                out.append((int(o["t"]), o["s"], int(o["q"]), o["d"].encode()))
    return out


def run_fixture(name: str, state: NormalizerState | None = None) -> tuple[list[Event], NormalizerState]:
    recs = load_fixture(name)
    norm, st = normalizer_for(recs[0][1])
    if state is not None:
        st = state
    events: list[Event] = []
    for t, _s, _q, raw in recs:
        events += safe_normalize(norm, raw, t, st)
    return events, st


def of_type(events: list[Event], cls: type) -> list:
    return [e for e in events if isinstance(e, cls)]
