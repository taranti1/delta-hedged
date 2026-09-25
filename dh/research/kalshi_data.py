"""Shared loading/normalization of Kalshi trade and market tables for research scripts.

Accepts the schema written by scripts/download_kalshi_history.py (trade_row / market_row) and
the minimal research schema used in tests:

  trades:  ticker, ts_ms, yes_px (int 1e-4 $), qty (int 0.01), taker_side | taker_outcome_side,
           [is_block_trade], [event_ticker, series_ticker, ...]
  markets: ticker, event_ticker, strike_type, floor_strike, cap_strike, result,
           expiration_ts_ms | expected_expiration_ts_ms | close_ts_ms, [open_ts_ms]

Rules (audit M6): block trades (matched off book via RFQ / negotiated blocks) are dropped; they
are neither maker fills nor taker flow on the order book. Trade-side event/series columns are
dropped before joining so the market table is the single source of event membership.

BTC reference prices (``btc``: ts_ms, price, optional close_ts_ms) are joined CAUSALLY with
``btc_price_asof``: a price is usable only from the time it was known. Convention: ``ts_ms`` is
the OPEN time of a ``bar_ms``-long bar whose ``price`` is the bar CLOSE (Bitstamp/Kraken OHLC
exports), so the price becomes available at ``ts_ms + bar_ms`` (or ``close_ts_ms`` when the column
exists). Point-in-time prices (index ticks stamped when observed) use ``bar_ms = 0``. A trade 1 s
after a bar opens therefore sees the PREVIOUS bar's close, never the bar that has not closed yet
(audit: the bar-open stamp joined as-of would leak up to 59 s of future BTC).
"""

from __future__ import annotations

import numpy as np
import pandas as pd

DEFAULT_BTC_BAR_MS = 60_000  # 1-minute OHLC bars stamped at their open (Bitstamp export)


def btc_price_asof(btc: pd.DataFrame, ts_ms, bar_ms: int = DEFAULT_BTC_BAR_MS) -> np.ndarray:
    """Causal reference price at each ``ts_ms`` (ms): the last price AVAILABLE at or before it,
    i.e. from the last bar whose close time (``close_ts_ms``, else ``ts_ms + bar_ms``) is <= ts_ms.
    NaN when no price is available yet. See the module docstring for the convention."""
    t = np.asarray(ts_ms, dtype=np.int64)
    if btc is None or not len(btc):
        return np.full(t.shape, np.nan)
    avail = (btc["close_ts_ms"] if "close_ts_ms" in btc else btc["ts_ms"] + int(bar_ms)).to_numpy(dtype=np.int64)
    px = btc["price"].to_numpy(dtype=float)
    order = np.argsort(avail, kind="stable")
    a, p = avail[order], px[order]
    idx = np.searchsorted(a, t, side="right") - 1
    return np.where(idx >= 0, p[np.clip(idx, 0, None)], np.nan)


def normalize_trades(trades: pd.DataFrame) -> pd.DataFrame:
    t = trades.copy()
    if "taker_side" not in t.columns:
        if "taker_outcome_side" not in t.columns:
            raise KeyError("trades need taker_side or taker_outcome_side")
        t["taker_side"] = t["taker_outcome_side"]
    if "is_block_trade" in t.columns:
        t = t[~t["is_block_trade"].fillna(False).astype(bool)]
    t = t.drop(columns=[c for c in ("event_ticker", "series_ticker") if c in t.columns])
    bad = ~t["taker_side"].isin(["yes", "no"])
    if bad.any():
        t = t[~bad]
    return t.reset_index(drop=True)


def normalize_markets(markets: pd.DataFrame) -> pd.DataFrame:
    m = markets.copy()
    if "expiration_ts_ms" not in m.columns:
        for alt in ("expected_expiration_ts_ms", "close_ts_ms"):
            if alt in m.columns:
                m["expiration_ts_ms"] = m[alt]
                break
        else:
            raise KeyError("markets need expiration_ts_ms / expected_expiration_ts_ms / close_ts_ms")
    for c in ("floor_strike", "cap_strike"):
        if c not in m.columns:
            m[c] = float("nan")
    if "open_ts_ms" not in m.columns:
        m["open_ts_ms"] = m["expiration_ts_ms"] - 3600 * 1000
    return m.reset_index(drop=True)
