"""Experiment 0: where do makers earn on Kalshi BTC markets? (public trades vs settlement)

For every public trade, the maker's gross P&L to settlement per contract is

    taker bought YES at p  (maker sold YES):   p - settle
    taker sold YES at p    (maker bought YES): settle - p          settle in {0, 1}

Net of the maker fee (only for series whose fee type charges makers):
    maker_net = maker_gross - maker_rate * p * (1 - p)

Segments: YES price bucket, maker side, time to expiry, normalized strike distance (needs a
BTC reference series), hour of day, weekday/weekend, trade size. All trades of one event share
one settlement, so inference clusters by event: 95% CIs come from an event-level bootstrap
(resampling events with replacement; seeded).

This is an UNCONDITIONAL average over all makers (front-of-queue professionals and slow
back-of-queue makers alike). It bounds where edge can exist; it is not our fill-conditioned
P&L (Experiments 3/4).

Inputs (Parquet or DataFrames):
  trades:  ticker, ts_ms, yes_px (int, 1e-4 $), qty (int, 0.01 contracts), taker_side ('yes'|'no')
  markets: ticker, event_ticker, expiration_ts_ms, result ('yes'|'no'), strike_type,
           floor_strike, cap_strike
  btc (optional): ts_ms, price  (any BRTI proxy; used for z = distance / expected move). ts_ms is
           the bar OPEN time of --btc-bar-ms bars (default 60 000: 1-minute OHLC closes); the
           join is causal (the bar is used only after it closed; kalshi_data.btc_price_asof)
"""

from __future__ import annotations

import argparse
import math
from pathlib import Path

import numpy as np
import pandas as pd

from dh.research.kalshi_data import DEFAULT_BTC_BAR_MS, btc_price_asof, normalize_markets, normalize_trades

PRICE_BUCKETS = [0, 500, 1000, 2000, 3500, 5000, 6500, 8000, 9000, 9500, 10001]  # px units
TAU_BUCKETS = [0, 30, 60, 300, 600, 1800, 3600, 1e9]  # seconds
TAU_LABELS = ["<30s", "30-60s", "1-5m", "5-10m", "10-30m", "30-60m", ">60m"]
Z_BUCKETS = [0, 0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 1e9]


def prepare(trades: pd.DataFrame, markets: pd.DataFrame, btc: pd.DataFrame | None = None,
            vol_ann: float = 0.40, maker_rate: float = 0.0175, btc_bar_ms: int = DEFAULT_BTC_BAR_MS) -> pd.DataFrame:
    """Join trades to settlements and compute per-trade maker P&L columns (dollars/contract).

    Accepts the downloader's schema (see dh.research.kalshi_data); block trades are dropped.
    The BTC reference for z is joined causally (kalshi_data.btc_price_asof): ``btc.ts_ms`` is a
    bar OPEN time and the bar's price is usable only from its close (``btc_bar_ms`` later; 0 for
    point-in-time prices).
    """
    trades = normalize_trades(trades)
    markets = normalize_markets(markets)
    df = trades.merge(markets[["ticker", "event_ticker", "expiration_ts_ms", "result", "strike_type",
                               "floor_strike", "cap_strike"]], on="ticker", how="inner")
    df = df[df.result.isin(["yes", "no"])].copy()
    df["p"] = df.yes_px / 1e4
    df["contracts"] = df.qty / 100.0
    df["settle"] = (df.result == "yes").astype(float)
    df["maker_side"] = np.where(df.taker_side == "yes", "sold_yes", "bought_yes")
    df["maker_gross"] = np.where(df.taker_side == "yes", df.p - df.settle, df.settle - df.p)
    df["maker_fee"] = maker_rate * df.p * (1 - df.p)
    df["maker_net"] = df.maker_gross - df.maker_fee
    df["tau_s"] = (df.expiration_ts_ms - df.ts_ms) / 1000.0
    df["tau_b"] = pd.cut(df.tau_s, TAU_BUCKETS, labels=TAU_LABELS, right=False)
    df["px_b"] = pd.cut(df.yes_px, PRICE_BUCKETS, right=False)
    ts = pd.to_datetime(df.ts_ms, unit="ms", utc=True)
    df["hour"] = ts.dt.hour
    df["weekend"] = ts.dt.dayofweek >= 5
    df["size_b"] = pd.cut(df.contracts, [0, 5, 25, 100, 500, 1e9], right=False)
    if btc is not None and len(btc):
        S = btc_price_asof(btc, df.ts_ms.to_numpy(), btc_bar_ms)
        K = df.floor_strike.fillna(df.cap_strike).to_numpy(dtype=float)
        sd = S * vol_ann * np.sqrt(np.maximum(df.tau_s.to_numpy() - 40.0, 20.0) / (365 * 24 * 3600.0))
        df["z"] = np.abs(K - S) / sd
        df["z_b"] = pd.cut(df.z, Z_BUCKETS, right=False)
    return df


def event_bootstrap(df: pd.DataFrame, col: str, n_boot: int = 500, seed: int = 11) -> tuple[float, float, float]:
    """Contract-weighted mean of `col` with a 95% CI from resampling events."""
    g = df.groupby("event_ticker").apply(
        lambda x: pd.Series({"s": float((x[col] * x.contracts).sum()), "w": float(x.contracts.sum())}),
        include_groups=False,
    )
    s, w = g.s.to_numpy(), g.w.to_numpy()
    if w.sum() <= 0:
        return math.nan, math.nan, math.nan
    mean = s.sum() / w.sum()
    rng = np.random.default_rng(seed)
    k = len(s)
    bs = []
    for _ in range(n_boot):
        i = rng.integers(0, k, k)
        ww = w[i].sum()
        if ww > 0:
            bs.append(s[i].sum() / ww)
    lo, hi = np.percentile(bs, [2.5, 97.5]) if bs else (math.nan, math.nan)
    return mean, float(lo), float(hi)


def segment_table(df: pd.DataFrame, by: list[str], n_boot: int = 300, min_events: int = 20) -> pd.DataFrame:
    rows = []
    for key, g in df.groupby(by, observed=True):
        n_ev = g.event_ticker.nunique()
        if n_ev < min_events:
            continue
        gross = event_bootstrap(g, "maker_gross", n_boot)
        net = event_bootstrap(g, "maker_net", n_boot)
        key = key if isinstance(key, tuple) else (key,)
        rows.append({**dict(zip(by, key)), "contracts": g.contracts.sum(), "trades": len(g), "events": n_ev,
                     "maker_gross_c": 100 * gross[0], "gross_lo_c": 100 * gross[1], "gross_hi_c": 100 * gross[2],
                     "maker_net_c": 100 * net[0], "net_lo_c": 100 * net[1], "net_hi_c": 100 * net[2]})
    return pd.DataFrame(rows)


def run(trades: pd.DataFrame, markets: pd.DataFrame, btc: pd.DataFrame | None, out: Path,
        maker_rate: float = 0.0175, n_boot: int = 300, btc_bar_ms: int = DEFAULT_BTC_BAR_MS) -> dict[str, pd.DataFrame]:
    df = prepare(trades, markets, btc, maker_rate=maker_rate, btc_bar_ms=btc_bar_ms)
    out.mkdir(parents=True, exist_ok=True)
    tables = {
        "overall": segment_table(df.assign(all="all"), ["all"], n_boot),
        "price": segment_table(df, ["px_b"], n_boot),
        "price_side": segment_table(df, ["px_b", "maker_side"], n_boot),
        "tau": segment_table(df, ["tau_b"], n_boot),
        "tau_price": segment_table(df, ["tau_b", "px_b"], n_boot),
        "hour": segment_table(df, ["hour"], n_boot),
        "weekend": segment_table(df, ["weekend"], n_boot),
        "size": segment_table(df, ["size_b"], n_boot),
    }
    if "z_b" in df:
        tables["z"] = segment_table(df, ["z_b"], n_boot)
        tables["tau_z"] = segment_table(df, ["tau_b", "z_b"], n_boot)
    for k, t in tables.items():
        t.to_csv(out / f"exp0_{k}.csv", index=False, float_format="%.4f")
    return tables


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--trades", required=True, help="Parquet of trades (schema in module doc)")
    ap.add_argument("--markets", required=True)
    ap.add_argument("--btc", default=None)
    ap.add_argument("--out", default="docs/research/tables")
    ap.add_argument("--maker-rate", type=float, default=0.0175,
                    help="maker fee rate for the series (0 for fee_type 'quadratic')")
    ap.add_argument("--btc-bar-ms", type=int, default=DEFAULT_BTC_BAR_MS,
                    help="length of the BTC bars whose OPEN time is ts_ms (0 = point-in-time prices)")
    a = ap.parse_args()
    tables = run(pd.read_parquet(a.trades), pd.read_parquet(a.markets),
                 pd.read_parquet(a.btc) if a.btc else None, Path(a.out), a.maker_rate, btc_bar_ms=a.btc_bar_ms)
    for k, t in tables.items():
        print(f"\n== {k}\n{t.to_string(index=False)}")


if __name__ == "__main__":
    main()
