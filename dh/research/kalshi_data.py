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
"""

from __future__ import annotations

import pandas as pd


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
