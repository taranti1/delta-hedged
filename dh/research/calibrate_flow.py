"""Calibrate the M1 fill-intensity model (taker flow per segment) from public Kalshi trades.

Taker orders are reconstructed from the trade tape: prints with the same
(ticker, ts_ms, taker_side) are one aggressive order sweeping one or more levels.
Segments = (tau bucket, |z| bucket, maker side), matching dh.strategy.fill_model.segment_key.

Exposure (market-seconds per segment) is EXACT in time to expiry: each 60 s cell of a market's
open window is split at the tau-bucket boundaries (30/60/300/600/1800 s), so every bucket gets
its true duration (audit M3). |z| is evaluated causally at each cell start from the BTC
reference (price at or before t; cells before the first print are skipped: audit minor 7).

Rates use gamma-Poisson shrinkage toward the pooled rate of the same (tau bucket, side)
(audit M4), so thin segments (far strikes) are estimated from their own exposure instead of
silently inheriting a global default:
    order_rate(seg) = (N_seg + r_pool * prior_s) / (E_seg + prior_s)
    contract_rate   = order_rate * size_mean(seg or pooled if N_seg < min_orders)
Pooled per-side defaults are returned under keys ('*', '*', side) for unseen segments.
"""

from __future__ import annotations

import math
from collections import defaultdict

import numpy as np
import pandas as pd

from dh.research.kalshi_data import normalize_markets, normalize_trades
from dh.strategy.fill_model import SegmentFlow, segment_key

SEC_YR = 365.0 * 24 * 3600
TAU_BOUNDS = (30.0, 60.0, 300.0, 600.0, 1800.0)


def taker_orders(trades: pd.DataFrame) -> pd.DataFrame:
    """Group prints into taker orders: ticker, ts_ms, taker_side, contracts, n_prints, yes_px."""
    t = trades.copy()
    t["contracts"] = t.qty / 100.0
    g = t.groupby(["ticker", "ts_ms", "taker_side"], sort=True).agg(
        contracts=("contracts", "sum"), n_prints=("contracts", "size"), yes_px=("yes_px", "first"))
    return g.reset_index()


def _z(S: float, K: float, tau_s: float, vol_ann: float) -> float:
    sd = S * vol_ann * math.sqrt(max(tau_s - 40.0, 20.0) / SEC_YR)
    return abs(K - S) / sd


def split_tau(tau_hi: float, tau_lo: float) -> list[tuple[float, float]]:
    """Split the tau interval (tau_lo, tau_hi] at bucket boundaries -> [(duration, tau_mid)]."""
    cuts = [tau_lo] + [b for b in TAU_BOUNDS if tau_lo < b < tau_hi] + [tau_hi]
    return [(b - a, 0.5 * (a + b)) for a, b in zip(cuts, cuts[1:]) if b > a]


def exposure_seconds(markets: pd.DataFrame, price_at, vol_ann: float, grid_s: int = 60) -> dict:
    exposure: dict[tuple[str, str, str], float] = defaultdict(float)
    for m in markets.itertuples():
        K = m.floor_strike if not pd.isna(m.floor_strike) else m.cap_strike
        if pd.isna(K):
            continue
        start, end = int(m.open_ts_ms), int(m.expiration_ts_ms)
        for t in range(start, end, grid_s * 1000):
            S = price_at(t)
            if S is None:
                continue
            t_end = min(t + grid_s * 1000, end)
            tau_hi, tau_lo = (end - t) / 1000.0, (end - t_end) / 1000.0
            z = _z(S, float(K), tau_hi, vol_ann)
            for dur, tau_mid in split_tau(tau_hi, tau_lo):
                for side in ("bid", "ask"):
                    exposure[segment_key(tau_mid, z, side)] += dur
    return exposure


def calibrate(
    trades: pd.DataFrame,
    markets: pd.DataFrame,
    btc: pd.DataFrame,
    vol_ann: float = 0.40,
    grid_s: int = 60,
    min_orders: int = 30,
    prior_s: float = 1800.0,
) -> dict[tuple[str, str, str], SegmentFlow]:
    """trades/markets: downloader schema or minimal schema (dh.research.kalshi_data);
    btc: ts_ms, price. Returns {segment_key: SegmentFlow} incl. ('*','*',side) pooled defaults."""
    trades = normalize_trades(trades)
    markets = normalize_markets(markets)
    b = btc.sort_values("ts_ms")
    b_ts = b.ts_ms.to_numpy()
    b_px = b.price.to_numpy()

    def price_at(ts_ms: int) -> float | None:
        i = int(np.searchsorted(b_ts, ts_ms, side="right")) - 1
        return float(b_px[i]) if i >= 0 else None

    exposure = exposure_seconds(markets, price_at, vol_ann, grid_s)
    orders = taker_orders(trades)
    orders = orders.merge(markets[["ticker", "expiration_ts_ms", "floor_strike", "cap_strike"]], on="ticker")
    sizes: dict[tuple[str, str, str], list[float]] = defaultdict(list)
    for r in orders.itertuples():
        K = r.floor_strike if not pd.isna(r.floor_strike) else r.cap_strike
        S = price_at(int(r.ts_ms))
        if S is None or pd.isna(K):
            continue
        tau = (r.expiration_ts_ms - r.ts_ms) / 1000.0
        if tau <= 0:
            continue
        z = _z(S, float(K), tau, vol_ann)
        # a taker selling YES ('no') hits resting YES bids -> fills makers' BIDS
        maker_side = "bid" if r.taker_side == "no" else "ask"
        sizes[segment_key(tau, z, maker_side)].append(float(r.contracts))

    def pooled(pred) -> tuple[float, list[float]]:
        E = sum(v for k, v in exposure.items() if pred(k))
        s = [x for k, v in sizes.items() if pred(k) for x in v]
        return (len(s) / E if E > 0 else 0.0), s

    def fit_sizes(arr: list[float]) -> tuple[float, float]:
        a = np.asarray(arr, dtype=float)
        mean = float(a.mean())
        cv = float(a.std(ddof=1) / mean) if len(a) > 1 and mean > 0 else 1.0
        return mean, max(cv, 0.1)

    out: dict[tuple[str, str, str], SegmentFlow] = {}
    for side in ("bid", "ask"):
        r_all, s_all = pooled(lambda k, sd=side: k[2] == sd)
        if s_all:
            mean, cv = fit_sizes(s_all)
            out[("*", "*", side)] = SegmentFlow(r_all * mean, mean, cv)
    for key, E in exposure.items():
        if E <= 0:
            continue
        tb, _, side = key
        r_pool, s_pool = pooled(lambda k, tb=tb, sd=side: k[0] == tb and k[2] == sd)
        n = len(sizes.get(key, []))
        order_rate = (n + r_pool * prior_s) / (E + prior_s)
        src = sizes.get(key, []) if n >= min_orders else (s_pool or [x for v in sizes.values() for x in v])
        if not src:
            continue
        mean, cv = fit_sizes(src)
        out[key] = SegmentFlow(rate_contracts_per_s=order_rate * mean, size_mean=mean, size_cv=cv)
    return out
