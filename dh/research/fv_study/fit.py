"""Walk-forward fitting routines (training data only; all deterministic).

fit_blend_qlike   non-negative variance weights for a set of EWMA forecasts (HAR-style),
                  minimizing QLIKE = mean(y / f + log f) against realized per-second variance
fit_gauss_scale   c = rms of standardized outcomes
fit_student_t     (c, nu) maximum likelihood, eps = u / c ~ unit-variance t_nu
fit_vol_mixture   (c, cv) maximum likelihood, eps = u / c ~ lognormal Gaussian scale mixture
"""

from __future__ import annotations

import math

import numpy as np
from scipy import optimize, special



def fit_blend_qlike(X: np.ndarray, y: np.ndarray, max_iter: int = 500) -> np.ndarray:
    """Non-negative weights w minimizing mean(y / (X w) + log(X w)).

    X: (n, K) positive variance forecasts; y: (n,) realized variances (same units).
    QLIKE is minimized by the conditional mean of y, so the fitted combination is an unbiased
    variance forecast; it is robust to the heavy right tail of squared returns.
    """
    X = np.asarray(X, dtype=np.float64)
    y = np.asarray(y, dtype=np.float64)
    ok = np.all(np.isfinite(X), axis=1) & np.all(X > 0, axis=1) & np.isfinite(y)
    X, y = X[ok], y[ok]
    n, K = X.shape
    sx = X.mean(axis=0)
    Xs = X / sx  # scale columns for conditioning
    w0 = np.full(K, y.mean() / K)

    f_floor = 1e-8 * float(np.mean(y)) if np.mean(y) > 0 else 1e-300

    def fun(w):
        f = np.maximum(Xs @ w, f_floor)
        val = np.mean(y / f + np.log(f))
        g = Xs.T @ (1.0 / f - y / (f * f)) / n
        return val, g

    res = optimize.minimize(fun, w0, jac=True, method="L-BFGS-B", bounds=[(0.0, None)] * K,
                            options={"maxiter": max_iter, "ftol": 1e-13, "gtol": 1e-11})
    return res.x / sx


def qlike(f: np.ndarray, y: np.ndarray) -> float:
    """Mean QLIKE loss of variance forecasts f for realized y (lower is better)."""
    f = np.asarray(f, dtype=np.float64)
    return float(np.mean(y / f + np.log(f)))


def fit_gauss_scale(u: np.ndarray) -> float:
    """MLE scale of a zero-mean Gaussian: sqrt(mean(u^2))."""
    u = np.asarray(u, dtype=np.float64)
    u = u[np.isfinite(u)]
    return float(np.sqrt(np.mean(u * u)))


def _compress_abs(u: np.ndarray, width: float = 0.002) -> tuple[np.ndarray, np.ndarray]:
    """Symmetric likelihoods depend on |u| only: round |u| to a fine grid, return (values, counts).

    Rounding |u| by at most width/2 changes each log-density by ~ |u| * width / 2 (negligible
    at width = 0.002 for u of order 1) and shrinks thousands of samples to ~1000 unique values.
    """
    a = np.round(np.abs(u) / width) * width
    vals, cnt = np.unique(a, return_counts=True)
    return vals, cnt.astype(np.float64)


def _t_nll(params, a, w):
    logc, lognu2 = params
    c = math.exp(logc)
    nu = 2.0 + math.exp(lognu2)
    s = math.sqrt((nu - 2.0) / nu)
    x = a / (c * s)
    logk = special.gammaln((nu + 1) / 2) - special.gammaln(nu / 2) - 0.5 * math.log(nu * math.pi)
    ll = logk - (nu + 1) / 2 * np.log1p(x * x / nu) - math.log(c * s)
    return -float(np.dot(w, ll))


def fit_student_t(u: np.ndarray, nu_bounds: tuple[float, float] = (2.05, 200.0)) -> tuple[float, float]:
    """MLE (c, nu) for u / c ~ unit-variance Student-t(nu)."""
    u = np.asarray(u, dtype=np.float64)
    u = u[np.isfinite(u)]
    a, w = _compress_abs(u)
    c0 = fit_gauss_scale(u)
    best = None
    for nu0 in (3.0, 5.0, 10.0):
        x0 = np.array([math.log(c0), math.log(nu0 - 2.0)])
        res = optimize.minimize(_t_nll, x0, args=(a, w), method="Nelder-Mead",
                                options={"xatol": 1e-5, "fatol": 1e-7, "maxiter": 2000})
        if best is None or res.fun < best.fun:
            best = res
    c = math.exp(best.x[0])
    nu = float(np.clip(2.0 + math.exp(best.x[1]), *nu_bounds))
    return c, nu


_MX, _MW = np.polynomial.hermite.hermgauss(32)
_MW = _MW / math.sqrt(math.pi)


def _mix_nll(params, a, w):
    logc, cv = params
    if cv < 0 or cv > 5:
        return 1e300
    c = math.exp(logc)
    om2 = math.log1p(cv * cv)
    s = np.exp(-om2 + math.sqrt(2.0 * om2) * _MX)
    s = s / math.sqrt(float(np.sum(_MW * s * s)))
    x = a[:, None] / (c * s[None, :])
    dens = (np.exp(-0.5 * x * x) / (math.sqrt(2 * math.pi) * c * s[None, :])) @ _MW
    return -float(np.dot(w, np.log(np.maximum(dens, 1e-300))))


def fit_vol_mixture(u: np.ndarray) -> tuple[float, float]:
    """MLE (c, cv) for u / c ~ VolMixture(cv) (lognormal Gaussian scale mixture)."""
    u = np.asarray(u, dtype=np.float64)
    u = u[np.isfinite(u)]
    a, w = _compress_abs(u)
    c0 = fit_gauss_scale(u)
    best = None
    for cv0 in (0.3, 0.7):
        res = optimize.minimize(_mix_nll, np.array([math.log(c0), cv0]), args=(a, w), method="Nelder-Mead",
                                options={"xatol": 1e-5, "fatol": 1e-7, "maxiter": 2000})
        if best is None or res.fun < best.fun:
            best = res
    return math.exp(best.x[0]), float(max(best.x[1], 0.0))
