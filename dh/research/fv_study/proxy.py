"""Settlement-proxy error: OHLC4 of the final 1-minute candle vs the true 60-print average.

The true KXBTCD settlement is the mean of 60 one-second BRTI prints at T-59s..T.  The study
only has Bitstamp 1-minute candles, so it uses A_T = OHLC4 of the candle [T-60s, T).
This module quantifies the proxy by simulation: Brownian log price on a 0.1 s grid, BRTI
prints = the path at whole seconds, Bitstamp trades = Poisson arrivals at rate lam (per s)
executing at the path price +/- a half-spread (bid-ask bounce).  Reported in units of the
one-minute standard deviation sigma*sqrt(60):
  err_sd      sd(OHLC4 - true average)
  kappa       Var(OHLC4 - C_prev) / Var(C - C_prev)     (proxy variance time / 60 s)
  true_kappa  Var(true average - C_prev) / Var(C - C_prev)
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from dh.research.fv_study.config import SEED


def simulate_proxy(lams=(0.05, 0.2, 0.5, 1.0, 3.0), half_spread=(0.0, 0.05), n_paths: int = 20_000, dt: float = 0.1,
                   seed: int = SEED) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    steps_per_min = int(round(60 / dt))
    n_steps = 2 * steps_per_min  # previous minute + settlement minute
    rows = []
    for lam in lams:
        for hs in half_spread:
            W = np.cumsum(rng.standard_normal((n_paths, n_steps)) * np.sqrt(dt / 60.0), axis=1)  # sd 1 per minute
            W = np.hstack([np.zeros((n_paths, 1)), W])
            # BRTI prints at T-59..T: settlement minute starts at step steps_per_min
            sec = steps_per_min + np.arange(1, 61) * int(round(1 / dt))
            avg = W[:, sec].mean(axis=1)
            # trades: Poisson counts per step
            trades = rng.random((n_paths, n_steps + 1)) < lam * dt
            noise = hs * rng.choice([-1.0, 1.0], size=(n_paths, n_steps + 1))
            px = W + noise
            ar = np.arange(n_paths)
            tp = trades[:, :steps_per_min]
            hasp = tp.any(axis=1)
            lastp = steps_per_min - 1 - np.argmax(tp[:, ::-1], axis=1)
            cprev = np.where(hasp, px[ar, lastp], W[:, 0])
            seg = px[:, steps_per_min:n_steps]
            tr = trades[:, steps_per_min:n_steps]
            has = tr.any(axis=1)
            fi = np.argmax(tr, axis=1)
            la = seg.shape[1] - 1 - np.argmax(tr[:, ::-1], axis=1)
            o = np.where(has, seg[ar, fi], cprev)
            c = np.where(has, seg[ar, la], cprev)
            h = np.where(has, np.max(np.where(tr, seg, -np.inf), axis=1), cprev)
            l = np.where(has, np.min(np.where(tr, seg, np.inf), axis=1), cprev)
            ccur_close = c
            ohlc4 = (o + h + l + c) / 4
            err = ohlc4 - avg
            var_cc = np.var(ccur_close - cprev)
            rows.append({
                "trades_per_s": lam, "half_spread_min_sd": hs, "n_paths": n_paths,
                "err_mean": float(err.mean()), "err_sd": float(err.std()),
                "corr_proxy_true": float(np.corrcoef(ohlc4 - cprev, avg - cprev)[0, 1]),
                "kappa": float(np.var(ohlc4 - cprev) / var_cc),
                "true_kappa": float(np.var(avg - cprev) / var_cc),
                "p_no_trade_minute": float(np.mean(~trades[:, steps_per_min:n_steps].any(axis=1))),
            })
    return pd.DataFrame(rows)
