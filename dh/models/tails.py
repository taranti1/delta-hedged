"""Standardized distributions ("tail models") for the remaining settlement average.

A TailModel describes eps in  R = spot + drift + sd * eps,  where sd is the model standard
deviation of the remaining average R.  Every model here is symmetric with E[eps] = 0 and
(except EmpiricalTail(standardize=False)) Var[eps] = 1, so sd keeps its meaning across models
and only the SHAPE (tail weight) changes.

Each model provides, vectorized over numpy arrays (all deterministic, no sampling):
    cdf(z)   P(eps <= z)
    sf(z)    P(eps >  z)      (computed directly, accurate deep in the upper tail)
    pdf(z)   density
    dpdf(z)  derivative of the density (for gamma)
    ppf(q)   quantile

Models
    Gauss()                 standard normal
    StudentT(nu)            Student-t with nu > 2 degrees of freedom scaled to unit variance
    VolMixture(cv)          Gaussian scale mixture eps = s * N(0,1), s lognormal with
                            E[s^2] = 1 and coefficient of variation cv = sd(s)/E[s]; integrated
                            with Gauss-Hermite quadrature (exact up to quadrature error).
                            Represents uncertainty about the volatility over the horizon.
                            Kurtosis = 3 (1 + cv^2)^4.
    EmpiricalTail(u)        Gaussian-kernel-smoothed empirical distribution of standardized
                            residuals u (research reference; tabulated on a grid).
"""

from __future__ import annotations

import math
import re
from dataclasses import dataclass, field

import numpy as np
from numpy.typing import ArrayLike, NDArray
from scipy import special

_INV_SQRT_2PI = 1.0 / math.sqrt(2.0 * math.pi)

FloatArr = NDArray[np.float64]


def _arr(z: ArrayLike) -> FloatArr:
    return np.asarray(z, dtype=np.float64)


def _ret(x: FloatArr, like: ArrayLike):
    """Return a python float for scalar input, else the array."""
    return float(x) if np.ndim(like) == 0 else x


def norm_pdf(z: ArrayLike) -> FloatArr:
    """Standard normal density."""
    z = _arr(z)
    return _INV_SQRT_2PI * np.exp(-0.5 * z * z)


class TailModel:
    """Base class: symmetric, zero-mean standardized distribution (see module docstring)."""

    name: str = "base"

    def cdf(self, z: ArrayLike):  # pragma: no cover - abstract
        raise NotImplementedError

    def sf(self, z: ArrayLike):
        """P(eps > z); symmetric models use cdf(-z) (accurate in the upper tail)."""
        return self.cdf(-_arr(z)) if np.ndim(z) else float(self.cdf(-float(z)))

    def pdf(self, z: ArrayLike):  # pragma: no cover - abstract
        raise NotImplementedError

    def dpdf(self, z: ArrayLike):  # pragma: no cover - abstract
        raise NotImplementedError

    def ppf(self, q: ArrayLike):
        """Quantile by monotone inversion on a fine grid (override where closed form exists)."""
        q = _arr(q)
        grid = np.linspace(-40.0, 40.0, 160_001)
        c = self.cdf(grid)
        c = np.maximum.accumulate(c)
        keep = np.r_[True, np.diff(c) > 0]
        out = np.interp(q, c[keep], grid[keep])
        return _ret(out, q)

    def variance(self) -> float:
        """Var[eps] (1 for unit-variance models)."""
        return 1.0

    def kurtosis(self) -> float:  # pragma: no cover - overridden
        return float("nan")

    def describe(self) -> str:
        return self.name


@dataclass(frozen=True)
class Gauss(TailModel):
    """Standard normal tail."""

    name: str = "gauss"

    def cdf(self, z: ArrayLike):
        return _ret(special.ndtr(_arr(z)), z)

    def sf(self, z: ArrayLike):
        return _ret(special.ndtr(-_arr(z)), z)

    def pdf(self, z: ArrayLike):
        return _ret(norm_pdf(z), z)

    def dpdf(self, z: ArrayLike):
        zz = _arr(z)
        return _ret(-zz * norm_pdf(zz), z)

    def ppf(self, q: ArrayLike):
        return _ret(special.ndtri(_arr(q)), q)

    def kurtosis(self) -> float:
        return 3.0

    def describe(self) -> str:
        return "gauss"


@dataclass(frozen=True)
class StudentT(TailModel):
    """Student-t(nu) scaled to unit variance: eps = T_nu * sqrt((nu - 2) / nu), nu > 2."""

    nu: float = 5.0
    name: str = "student_t"
    _scale: float = field(init=False, repr=False)
    _logc: float = field(init=False, repr=False)

    def __post_init__(self) -> None:
        if not (self.nu > 2.0) or not math.isfinite(self.nu):
            raise ValueError("StudentT needs finite nu > 2 (unit variance)")
        object.__setattr__(self, "_scale", math.sqrt((self.nu - 2.0) / self.nu))
        nu = self.nu
        logc = special.gammaln((nu + 1) / 2) - special.gammaln(nu / 2) - 0.5 * math.log(nu * math.pi)
        object.__setattr__(self, "_logc", float(logc))

    def _x(self, z: ArrayLike) -> FloatArr:
        return _arr(z) / self._scale

    def cdf(self, z: ArrayLike):
        return _ret(special.stdtr(self.nu, self._x(z)), z)

    def sf(self, z: ArrayLike):
        return _ret(special.stdtr(self.nu, -self._x(z)), z)

    def _tpdf(self, x: FloatArr) -> FloatArr:
        nu = self.nu
        return np.exp(self._logc - (nu + 1) / 2 * np.log1p(x * x / nu))

    def pdf(self, z: ArrayLike):
        x = self._x(z)
        return _ret(self._tpdf(x) / self._scale, z)

    def dpdf(self, z: ArrayLike):
        x = self._x(z)
        nu = self.nu
        d = -self._tpdf(x) * (nu + 1) * x / (nu + x * x)
        return _ret(d / (self._scale * self._scale), z)

    def ppf(self, q: ArrayLike):
        return _ret(special.stdtrit(self.nu, _arr(q)) * self._scale, q)

    def kurtosis(self) -> float:
        return 3.0 + 6.0 / (self.nu - 4.0) if self.nu > 4 else math.inf

    def describe(self) -> str:
        return f"student_t(nu={self.nu:.3g})"


@dataclass(frozen=True)
class VolMixture(TailModel):
    """Gaussian scale mixture with lognormal volatility multiplier s (E[s^2] = 1).

    log s ~ N(-omega^2, omega^2), omega^2 = log(1 + cv^2), so cv = sd(s)/E[s].  Integrals are
    evaluated with ``n_nodes``-point Gauss-Hermite quadrature; node scales are renormalized so
    the quadrature variance is exactly 1.  cv = 0 reduces to Gauss.
    """

    cv: float = 0.5
    n_nodes: int = 48
    name: str = "vol_mixture"
    _s: FloatArr = field(init=False, repr=False, compare=False)
    _w: FloatArr = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        if not (self.cv >= 0.0) or not math.isfinite(self.cv):
            raise ValueError("cv must be finite and >= 0")
        if self.n_nodes < 2:
            raise ValueError("n_nodes >= 2")
        x, w = np.polynomial.hermite.hermgauss(self.n_nodes)
        w = w / math.sqrt(math.pi)
        om2 = math.log1p(self.cv * self.cv)
        om = math.sqrt(om2)
        s = np.exp(-om2 + math.sqrt(2.0) * om * x)
        s = s / math.sqrt(float(np.sum(w * s * s)))  # exact unit variance under the quadrature
        keep = w > 1e-300
        object.__setattr__(self, "_s", s[keep])
        object.__setattr__(self, "_w", w[keep])

    @property
    def omega(self) -> float:
        """Standard deviation of log s."""
        return math.sqrt(math.log1p(self.cv * self.cv))

    def _mix(self, z: ArrayLike, fn) -> FloatArr:
        zz = _arr(z)
        out = np.zeros(zz.shape, dtype=np.float64)
        for s, w in zip(self._s, self._w):
            out += w * fn(zz, s)
        return out

    def cdf(self, z: ArrayLike):
        return _ret(self._mix(z, lambda zz, s: special.ndtr(zz / s)), z)

    def sf(self, z: ArrayLike):
        return _ret(self._mix(z, lambda zz, s: special.ndtr(-zz / s)), z)

    def pdf(self, z: ArrayLike):
        return _ret(self._mix(z, lambda zz, s: norm_pdf(zz / s) / s), z)

    def dpdf(self, z: ArrayLike):
        return _ret(self._mix(z, lambda zz, s: -(zz / (s * s)) * norm_pdf(zz / s) / s), z)

    def kurtosis(self) -> float:
        return float(3.0 * np.sum(self._w * self._s**4))

    def describe(self) -> str:
        return f"vol_mixture(cv={self.cv:.3g})"


class EmpiricalTail(TailModel):
    """Kernel-smoothed empirical distribution of standardized residuals (research reference).

    samples      standardized residuals u_i (e.g. (outcome - spot) / model sd) from TRAINING data
    bandwidth    Gaussian kernel bandwidth; default Silverman's rule
    standardize  rescale samples to unit variance (and zero mean) first; False keeps the
                 empirical location/scale so the model also corrects a biased sd forecast
    The CDF/PDF are tabulated on a grid (linear binning + exact kernel sums) and interpolated.
    """

    name = "empirical"

    def __init__(
        self,
        samples: ArrayLike,
        bandwidth: float | None = None,
        standardize: bool = False,
        symmetric: bool = False,
        grid_n: int = 2401,
    ) -> None:
        u = _arr(samples).ravel()
        u = u[np.isfinite(u)]
        if u.size < 20:
            raise ValueError("EmpiricalTail needs at least 20 finite samples")
        if standardize:
            u = (u - u.mean()) / u.std()
        if symmetric:
            u = np.concatenate([u, -u])
        n = u.size
        sd = float(u.std())
        iqr = float(np.subtract(*np.percentile(u, [75, 25])))
        if bandwidth is None:
            spread = min(sd, iqr / 1.349) if iqr > 0 else sd
            bandwidth = 0.9 * spread * n ** (-0.2)
        h = float(bandwidth)
        if not h > 0:
            raise ValueError("bandwidth must be positive")
        self.h = h
        self.n = n
        self._mean = float(u.mean())
        self._var = float(u.var() + h * h)  # variance of the smoothed distribution
        self._m4 = float(np.mean((u - self._mean) ** 4))
        lim = max(12.0, 1.25 * float(np.max(np.abs(u))) + 8 * h)
        grid = np.linspace(-lim, lim, grid_n)
        # linear binning of samples onto a fine bin grid, then exact Gaussian kernel sums
        nb = max(2001, int(2 * lim / (h / 8.0)) + 1)
        edges = np.linspace(-lim, lim, nb)
        pos = (u + lim) / (edges[1] - edges[0])
        i0 = np.clip(np.floor(pos).astype(np.int64), 0, nb - 2)
        f = pos - i0
        cnt = np.bincount(i0, weights=1.0 - f, minlength=nb) + np.bincount(i0 + 1, weights=f, minlength=nb)
        nz = cnt > 0
        c, wts = edges[nz], cnt[nz] / n
        cdf = np.empty(grid_n)
        pdf = np.empty(grid_n)
        dpdf = np.empty(grid_n)
        chunk = max(1, int(4_000_000 // max(1, c.size)))
        for a in range(0, grid_n, chunk):
            g = grid[a : a + chunk, None]
            x = (g - c[None, :]) / h
            phi = norm_pdf(x)
            cdf[a : a + chunk] = special.ndtr(x) @ wts
            pdf[a : a + chunk] = (phi / h) @ wts
            dpdf[a : a + chunk] = (-(x / (h * h)) * phi) @ wts
        self._grid, self._cdf, self._pdf, self._dpdf = grid, cdf, pdf, dpdf
        self._sfv = 1.0 - cdf

    def cdf(self, z: ArrayLike):
        return _ret(np.interp(_arr(z), self._grid, self._cdf, left=0.0, right=1.0), z)

    def sf(self, z: ArrayLike):
        return _ret(np.interp(_arr(z), self._grid, self._sfv, left=1.0, right=0.0), z)

    def pdf(self, z: ArrayLike):
        return _ret(np.interp(_arr(z), self._grid, self._pdf, left=0.0, right=0.0), z)

    def dpdf(self, z: ArrayLike):
        return _ret(np.interp(_arr(z), self._grid, self._dpdf, left=0.0, right=0.0), z)

    def variance(self) -> float:
        return self._var

    def kurtosis(self) -> float:
        return self._m4 / (self._var - self.h * self.h) ** 2

    def describe(self) -> str:
        return f"empirical(n={self.n}, h={self.h:.3g})"


GAUSS = Gauss()

_TAIL_RE = re.compile(r"^\s*(\w+)\s*(?:\(\s*([0-9.eE+-]+)\s*\))?\s*$")


def make_tail(spec: str | TailModel | dict | None) -> TailModel:
    """Build a tail model from a config value.

    Accepts a TailModel, None/'gauss'/'normal', 'student_t(4.5)' / 't(4.5)',
    'vol_mixture(0.6)' / 'mixture(0.6)', or a dict {'kind': ..., 'nu'|'cv': ...}.
    """
    if spec is None:
        return GAUSS
    if isinstance(spec, TailModel):
        return spec
    if isinstance(spec, dict):
        kind = str(spec.get("kind", "gauss")).lower()
        if kind in ("gauss", "normal"):
            return GAUSS
        if kind in ("student_t", "t", "studentt"):
            return StudentT(float(spec["nu"]))
        if kind in ("vol_mixture", "mixture", "volmixture"):
            return VolMixture(float(spec["cv"]), int(spec.get("n_nodes", 48)))
        raise ValueError(f"unknown tail kind {kind!r}")
    m = _TAIL_RE.match(str(spec))
    if not m:
        raise ValueError(f"cannot parse tail model {spec!r}")
    kind, arg = m.group(1).lower(), m.group(2)
    if kind in ("gauss", "normal"):
        return GAUSS
    if arg is None:
        raise ValueError(f"tail model {kind!r} needs a parameter, e.g. {kind}(5)")
    if kind in ("student_t", "t", "studentt"):
        return StudentT(float(arg))
    if kind in ("vol_mixture", "mixture", "volmixture"):
        return VolMixture(float(arg))
    raise ValueError(f"unknown tail kind {kind!r}")


__all__ = [
    "TailModel",
    "Gauss",
    "StudentT",
    "VolMixture",
    "EmpiricalTail",
    "GAUSS",
    "make_tail",
    "norm_pdf",
]
