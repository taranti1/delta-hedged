"""Probability-forecast scoring and calibration diagnostics (deterministic).

All functions take predicted YES probabilities ``p`` in [0, 1] and binary outcomes ``y``
(1 = YES) as array-likes of equal length; optional ``w`` are non-negative sample weights.

    brier(p, y)                      mean squared error (lower is better; 0.25 = coin flip at 0.5)
    log_loss(p, y, eps)              mean negative log-likelihood, p clipped to [eps, 1-eps] (nats)
    reliability(p, y, bins)          table: bin, n, mean forecast, observed frequency, Wilson CI
    ece(p, y, bins)                  expected calibration error (weighted |freq - forecast|)
    murphy(p, y, bins)               Brier = reliability - resolution + uncertainty (binned)
    by_bucket(p, y, keys)            Brier / log loss / calibration-in-the-large per group
    wilson_ci(k, n, z)               Wilson score interval for a proportion
    block_bootstrap_mean(...)        CI of a mean with resampling of whole blocks (e.g. days)
"""

from __future__ import annotations

import math
from typing import Sequence

import numpy as np
import pandas as pd
from numpy.typing import ArrayLike, NDArray

FloatArr = NDArray[np.float64]

DEFAULT_BINS: tuple[float, ...] = tuple(np.round(np.linspace(0.0, 1.0, 11), 10))
TAIL_BINS: tuple[float, ...] = (0.0, 0.005, 0.01, 0.02, 0.05, 0.10, 0.25, 0.5, 0.75, 0.9, 0.95, 0.98, 0.99, 0.995, 1.0)


def _py(p: ArrayLike, y: ArrayLike, w: ArrayLike | None = None) -> tuple[FloatArr, FloatArr, FloatArr]:
    pa = np.asarray(p, dtype=np.float64).ravel()
    ya = np.asarray(y, dtype=np.float64).ravel()
    if pa.shape != ya.shape:
        raise ValueError("p and y must have the same length")
    wa = np.ones_like(pa) if w is None else np.asarray(w, dtype=np.float64).ravel()
    if wa.shape != pa.shape:
        raise ValueError("w must match p")
    return pa, ya, wa


def brier(p: ArrayLike, y: ArrayLike, w: ArrayLike | None = None) -> float:
    """Mean (weighted) squared error of probability forecasts."""
    pa, ya, wa = _py(p, y, w)
    return float(np.sum(wa * (pa - ya) ** 2) / np.sum(wa))


def log_loss(p: ArrayLike, y: ArrayLike, eps: float = 1e-6, w: ArrayLike | None = None) -> float:
    """Mean (weighted) negative log-likelihood in nats with p clipped to [eps, 1 - eps]."""
    pa, ya, wa = _py(p, y, w)
    pc = np.clip(pa, eps, 1.0 - eps)
    ll = -(ya * np.log(pc) + (1.0 - ya) * np.log1p(-pc))
    return float(np.sum(wa * ll) / np.sum(wa))


def brier_terms(p: ArrayLike, y: ArrayLike) -> FloatArr:
    """Per-sample squared errors (for custom aggregation / bootstrap)."""
    pa, ya, _ = _py(p, y)
    return (pa - ya) ** 2


def log_loss_terms(p: ArrayLike, y: ArrayLike, eps: float = 1e-6) -> FloatArr:
    """Per-sample negative log-likelihoods (nats), p clipped to [eps, 1 - eps]."""
    pa, ya, _ = _py(p, y)
    pc = np.clip(pa, eps, 1.0 - eps)
    return -(ya * np.log(pc) + (1.0 - ya) * np.log1p(-pc))


def wilson_ci(k: ArrayLike, n: ArrayLike, z: float = 1.959963984540054) -> tuple[FloatArr, FloatArr]:
    """Wilson score interval for k successes in n trials (vectorized); nan where n == 0."""
    ka = np.asarray(k, dtype=np.float64)
    na = np.asarray(n, dtype=np.float64)
    with np.errstate(invalid="ignore", divide="ignore"):
        ph = ka / na
        den = 1.0 + z * z / na
        c = (ph + z * z / (2 * na)) / den
        h = z * np.sqrt(ph * (1 - ph) / na + z * z / (4 * na * na)) / den
        lo = np.where(na > 0, np.clip(c - h, 0.0, 1.0), np.nan)
        hi = np.where(na > 0, np.clip(c + h, 0.0, 1.0), np.nan)
    return lo, hi


def _bin_index(p: FloatArr, bins: Sequence[float] | int) -> tuple[NDArray[np.int64], FloatArr]:
    edges = np.linspace(0.0, 1.0, bins + 1) if isinstance(bins, (int, np.integer)) else np.asarray(bins, dtype=np.float64)
    if edges[0] > 0 or edges[-1] < 1 or np.any(np.diff(edges) <= 0):
        raise ValueError("bins must be increasing edges covering [0, 1]")
    idx = np.clip(np.searchsorted(edges, p, side="right") - 1, 0, len(edges) - 2)
    return idx, edges


def reliability(p: ArrayLike, y: ArrayLike, bins: Sequence[float] | int = DEFAULT_BINS, w: ArrayLike | None = None) -> pd.DataFrame:
    """Reliability table.

    Columns: bin_lo, bin_hi, n (weight sum), p_mean (mean forecast), y_mean (observed
    frequency), y_lo / y_hi (95% Wilson interval, iid assumption), gap = y_mean - p_mean.
    Empty bins are dropped.  Bins are [lo, hi) except the last, which includes 1.
    """
    pa, ya, wa = _py(p, y, w)
    idx, edges = _bin_index(pa, bins)
    nb = len(edges) - 1
    n = np.bincount(idx, weights=wa, minlength=nb)
    sp = np.bincount(idx, weights=wa * pa, minlength=nb)
    sy = np.bincount(idx, weights=wa * ya, minlength=nb)
    keep = n > 0
    with np.errstate(invalid="ignore", divide="ignore"):
        pm = sp / n
        ym = sy / n
    lo, hi = wilson_ci(sy, n)
    df = pd.DataFrame(
        {
            "bin_lo": edges[:-1],
            "bin_hi": edges[1:],
            "n": n,
            "p_mean": pm,
            "y_mean": ym,
            "y_lo": lo,
            "y_hi": hi,
        }
    )[keep].reset_index(drop=True)
    df["gap"] = df["y_mean"] - df["p_mean"]
    return df


def ece(p: ArrayLike, y: ArrayLike, bins: Sequence[float] | int = DEFAULT_BINS, w: ArrayLike | None = None) -> float:
    """Expected calibration error: sum_b (n_b / N) |y_mean_b - p_mean_b|."""
    t = reliability(p, y, bins, w)
    return float(np.sum(t["n"] * np.abs(t["gap"])) / np.sum(t["n"]))


def murphy(p: ArrayLike, y: ArrayLike, bins: Sequence[float] | int = 20, w: ArrayLike | None = None) -> dict[str, float]:
    """Murphy decomposition of the Brier score on binned forecasts.

    brier ~= reliability - resolution + uncertainty, where (weights n_b, N = sum n_b)
      reliability = sum n_b (p_mean_b - y_mean_b)^2 / N     (lower is better)
      resolution  = sum n_b (y_mean_b - ybar)^2 / N          (higher is better)
      uncertainty = ybar (1 - ybar)
    'within_bin' = brier - (rel - res + unc) is the discretization residual (0 for forecasts
    constant within bins).
    """
    pa, ya, wa = _py(p, y, w)
    t = reliability(pa, ya, bins, wa)
    N = float(np.sum(t["n"]))
    ybar = float(np.sum(wa * ya) / np.sum(wa))
    rel = float(np.sum(t["n"] * (t["p_mean"] - t["y_mean"]) ** 2) / N)
    res = float(np.sum(t["n"] * (t["y_mean"] - ybar) ** 2) / N)
    unc = ybar * (1.0 - ybar)
    b = brier(pa, ya, wa)
    return {"brier": b, "reliability": rel, "resolution": res, "uncertainty": unc, "within_bin": b - (rel - res + unc)}


def by_bucket(
    p: ArrayLike,
    y: ArrayLike,
    keys: dict[str, ArrayLike] | pd.DataFrame,
    eps: float = 1e-6,
    w: ArrayLike | None = None,
) -> pd.DataFrame:
    """Per-group scores: n, brier, log_loss, p_mean, y_mean, citl (= y_mean - p_mean).

    ``keys`` maps column names to arrays of group labels (all the same length as p).
    """
    pa, ya, wa = _py(p, y, w)
    kdf = keys.copy() if isinstance(keys, pd.DataFrame) else pd.DataFrame({k: np.asarray(v) for k, v in keys.items()})
    kdf = kdf.reset_index(drop=True)
    kdf["_w"] = wa
    kdf["_b"] = wa * (pa - ya) ** 2
    pc = np.clip(pa, eps, 1 - eps)
    kdf["_l"] = -wa * (ya * np.log(pc) + (1 - ya) * np.log1p(-pc))
    kdf["_p"] = wa * pa
    kdf["_y"] = wa * ya
    cols = [c for c in kdf.columns if not c.startswith("_")]
    g = kdf.groupby(cols, sort=True, observed=True)[["_w", "_b", "_l", "_p", "_y"]].sum()
    out = pd.DataFrame(
        {
            "n": g["_w"],
            "brier": g["_b"] / g["_w"],
            "log_loss": g["_l"] / g["_w"],
            "p_mean": g["_p"] / g["_w"],
            "y_mean": g["_y"] / g["_w"],
        }
    )
    out["citl"] = out["y_mean"] - out["p_mean"]
    return out.reset_index()


def block_bootstrap_mean(
    values: ArrayLike,
    blocks: ArrayLike,
    n_boot: int = 1000,
    seed: int = 0,
    alpha: float = 0.05,
    weights: ArrayLike | None = None,
) -> tuple[float, float, float]:
    """(estimate, lo, hi) for a (weighted) mean, resampling whole blocks with replacement.

    Use blocks = calendar day to respect overlap/serial correlation between samples of the
    same day (multiple strikes and decision times per hour, adjacent hours).
    """
    v = np.asarray(values, dtype=np.float64).ravel()
    wv = np.ones_like(v) if weights is None else np.asarray(weights, dtype=np.float64).ravel()
    b = np.asarray(blocks).ravel()
    _, inv = np.unique(b, return_inverse=True)
    nb = int(inv.max()) + 1 if inv.size else 0
    if nb == 0:
        return math.nan, math.nan, math.nan
    num = np.bincount(inv, weights=wv * v, minlength=nb)
    den = np.bincount(inv, weights=wv, minlength=nb)
    return block_bootstrap_counts(num, den, n_boot=n_boot, seed=seed, alpha=alpha)


def block_bootstrap_counts(
    num: ArrayLike,
    den: ArrayLike,
    n_boot: int = 1000,
    seed: int = 0,
    alpha: float = 0.05,
) -> tuple[float, float, float]:
    """Ratio-of-sums CI from per-block sums (num_b, den_b): (sum num / sum den, lo, hi)."""
    nu = np.asarray(num, dtype=np.float64).ravel()
    de = np.asarray(den, dtype=np.float64).ravel()
    nb = nu.size
    if nb == 0 or de.sum() <= 0:
        return math.nan, math.nan, math.nan
    est = float(nu.sum() / de.sum())
    rng = np.random.default_rng(seed)
    idx = rng.integers(0, nb, size=(n_boot, nb))
    bn = nu[idx].sum(axis=1)
    bd = de[idx].sum(axis=1)
    bs = bn / np.maximum(bd, 1e-300)
    lo, hi = np.quantile(bs, [alpha / 2, 1 - alpha / 2])
    return est, float(lo), float(hi)


__all__ = [
    "brier",
    "log_loss",
    "brier_terms",
    "log_loss_terms",
    "reliability",
    "ece",
    "murphy",
    "by_bucket",
    "wilson_ci",
    "block_bootstrap_mean",
    "block_bootstrap_counts",
    "DEFAULT_BINS",
    "TAIL_BINS",
]
