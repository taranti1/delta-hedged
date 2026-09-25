"""Calibrate the M1 fill-intensity model (taker flow per segment) from public Kalshi trades.

Taker orders are reconstructed from the trade tape: consecutive prints with the same
(ticker, ts_ms, taker_side) are one aggressive order sweeping one or more levels.
Segments = (tau bucket, |z| bucket, maker side), matching dh.strategy.fill_model.segment_key.

    rate(seg)  = taker contracts on that side / market-seconds spent in that segment
    size(seg)  = lognormal fit (mean, cv) of taker ORDER sizes (contracts)

Market-seconds per segment are accumulated on a 60-second grid per market over its open
window, using a BTC reference series for |z| (causal: price at or before each grid time).
Output: YAML/JSON-able dict {segment -> {rate, size_mean, size_cv, n_orders}} for
`FillIntensityModel(segments=...)`.
"""

from __future__ import annotations

import math
from collections import defaultdict

import numpy as np
import pandas as pd

from dh.strategy.fill_model import SegmentFlow, segment_key

SEC_YR = 365.0 * 24 * 3600


def taker_orders(trades: pd.DataFrame) -> pd.DataFrame:
    """Group prints into taker orders: columns ticker, ts_ms, taker_side, contracts, n_prints."""
    t = trades.copy()
    t["contracts"] = t.qty / 100.0
    g = t.groupby(["ticker", "ts_ms", "taker_side"], sort=True).agg(
        contracts=("contracts", "sum"), n_prints=("contracts", "size"), yes_px=("yes_px", "first"))
    return g.reset_index()


def _z(S: float, K: float, tau_s: float, vol_ann: float) -> float:
    sd = S * vol_ann * math.sqrt(max(tau_s - 40.0, 20.0) / SEC_YR)
    return abs(K - S) / sd


def calibrate(
    trades: pd.DataFrame,
    markets: pd.DataFrame,
    btc: pd.DataFrame,
    vol_ann: float = 0.40,
    grid_s: int = 60,
    min_orders: int = 30,
) -> dict[tuple[str, str, str], SegmentFlow]:
    """trades: ticker, ts_ms, yes_px, qty, taker_side; markets: ticker, open_ts_ms,
    expiration_ts_ms, floor_strike|cap_strike; btc: ts_ms, price (sorted)."""
    b_ts = btc.ts_ms.to_numpy()
    b_px = btc.price.to_numpy()

    def price_at(ts_ms: int) -> float:
        i = int(np.searchsorted(b_ts, ts_ms, side="right")) - 1
        return float(b_px[max(i, 0)])

    exposure: dict[tuple[str, str, str], float] = defaultdict(float)
    mk = markets.set_index("ticker")
    for tk, m in mk.iterrows():
        K = m.floor_strike if not pd.isna(m.floor_strike) else m.cap_strike
        start, end = int(m.open_ts_ms), int(m.expiration_ts_ms)
        for t in range(start, end, grid_s * 1000):
            tau = (end - t) / 1000.0
            z = _z(price_at(t), float(K), tau, vol_ann)
            for side in ("bid", "ask"):
                exposure[segment_key(tau, z, side)] += grid_s
    orders = taker_orders(trades)
    orders = orders.merge(markets[["ticker", "expiration_ts_ms", "floor_strike", "cap_strike"]], on="ticker")
    vol: dict[tuple[str, str, str], list[float]] = defaultdict(list)
    for r in orders.itertuples():
        K = r.floor_strike if not pd.isna(r.floor_strike) else r.cap_strike
        tau = (r.expiration_ts_ms - r.ts_ms) / 1000.0
        z = _z(price_at(int(r.ts_ms)), float(K), tau, vol_ann)
        # a taker selling YES ('no') hits resting YES bids -> fills makers' BIDS
        maker_side = "bid" if r.taker_side == "no" else "ask"
        vol[segment_key(tau, z, maker_side)].append(float(r.contracts))
    out: dict[tuple[str, str, str], SegmentFlow] = {}
    for key, sizes in vol.items():
        if len(sizes) < min_orders or exposure.get(key, 0) <= 0:
            continue
        arr = np.asarray(sizes)
        mean = float(arr.mean())
        cv = float(arr.std(ddof=1) / mean) if len(arr) > 1 and mean > 0 else 1.0
        out[key] = SegmentFlow(rate_contracts_per_s=float(arr.sum()) / exposure[key], size_mean=mean,
                               size_cv=max(cv, 0.1))
    return out
