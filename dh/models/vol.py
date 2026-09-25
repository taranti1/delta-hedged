"""Volatility estimation for the fair-value model (deterministic, streaming + vectorized).

Units: every sigma here is in LOG-RETURN units per sqrt(second); every variance is per second.
Convert to the $ volatility the pricing model wants with ``sigma_abs = spot * sigma``.

Streaming (live + backtest, fed from the benchmark / nowcast price):
    EwmaVol(half_life_s, min_dt_s, ...)   EWMA of per-second variance, irregular time steps
    SeasonalVol                           multiplicative intraday/weekly sigma profile
    VolForecaster                         deseasonalized multi-half-life EWMA blend x forward
                                          seasonal factor over the pricing horizon
Vectorized research helpers:
    ewma_regular(x, valid, half_life_steps)       bias-corrected EWMA on a regular grid
    realized_variance, bipower_variation, jump_share, parkinson_var, garman_klass_var
    blended_sigma(...)                            the combination rule used by VolForecaster

Irregular-step EWMA
-------------------
Returns are folded in once at least ``min_dt_s`` has elapsed since the previous anchor price:
x = r^2 / dt is a per-second variance sample over dt seconds, and with a = 2^(-dt / H)

    S <- a S + (1 - a) x,     W <- a W + (1 - a),     var = S / W

W is the total weight of the observed history, so var is a bias-corrected (time-weighted)
average during warm-up and does not depend on how finely the price stream is sampled
(for Brownian prices, E[x] = sigma^2 for any dt).  A return spanning more than ``max_dt_s``
(feed outage) is not folded in; the history is decayed by the elapsed time instead.
"""

from __future__ import annotations

import datetime as _dt
import math
import zoneinfo
from dataclasses import dataclass, field
from typing import Sequence

import numpy as np
from numpy.typing import ArrayLike, NDArray
from scipy.signal import lfilter

from dh.core.units import NS_PER_S

LN2 = math.log(2.0)
SECONDS_PER_YEAR = 365.25 * 86400.0
FloatArr = NDArray[np.float64]


def annualized(sigma_per_sqrt_s: float) -> float:
    """Convert sigma per sqrt(second) to an annualized volatility (fraction)."""
    return sigma_per_sqrt_s * math.sqrt(SECONDS_PER_YEAR)


def per_sqrt_second(sigma_annual: float) -> float:
    """Convert an annualized volatility (fraction) to sigma per sqrt(second)."""
    return sigma_annual / math.sqrt(SECONDS_PER_YEAR)


# ============================================================================ seasonal profile
_LAYOUTS = ("flat", "time_of_day", "day_type", "hour_of_week")


@dataclass(frozen=True)
class SeasonalVol:
    """Multiplicative sigma profile f(t); the time-average of f^2 over a week is 1.

    layout      'flat' (f = 1), 'time_of_day' (bucket of the day), 'day_type' (time of day x
                {weekday, weekend}), 'hour_of_week' (bucket of the 7-day week, Monday first)
    bucket_s    bucket width in seconds (divides 86400), e.g. 3600 or 1800
    tz          time zone the buckets are defined in ('UTC' or e.g. 'America/New_York')
    factors     sigma multipliers, one per bucket (length depends on layout)
    Weekend = Saturday and Sunday in ``tz``.
    """

    factors: tuple[float, ...] = (1.0,)
    layout: str = "flat"
    bucket_s: int = 3600
    tz: str = "UTC"
    meta: dict = field(default_factory=dict, compare=False)

    def __post_init__(self) -> None:
        if self.layout not in _LAYOUTS:
            raise ValueError(f"layout must be one of {_LAYOUTS}")
        if 86400 % self.bucket_s:
            raise ValueError("bucket_s must divide 86400")
        n = self.n_buckets(self.layout, self.bucket_s)
        if len(self.factors) != n:
            raise ValueError(f"{self.layout} with bucket_s={self.bucket_s} needs {n} factors, got {len(self.factors)}")
        if any(not (f > 0 and math.isfinite(f)) for f in self.factors):
            raise ValueError("factors must be positive and finite")
        object.__setattr__(self, "_f", np.asarray(self.factors, dtype=np.float64))
        object.__setattr__(self, "_zone", None if self.tz == "UTC" else zoneinfo.ZoneInfo(self.tz))

    # ------------------------------------------------------------------ layout helpers
    @staticmethod
    def n_buckets(layout: str, bucket_s: int) -> int:
        per_day = 86400 // bucket_s
        return {"flat": 1, "time_of_day": per_day, "day_type": 2 * per_day, "hour_of_week": 7 * per_day}[layout]

    def _offset_s(self, ts_s: int) -> int:
        if self._zone is None:  # type: ignore[attr-defined]
            return 0
        d = _dt.datetime.fromtimestamp(ts_s, tz=_dt.timezone.utc).astimezone(self._zone)  # type: ignore[attr-defined]
        return int(d.utcoffset().total_seconds())  # type: ignore[union-attr]

    def _offsets_vec(self, ts_s: FloatArr) -> FloatArr:
        """UTC offsets (s) for many timestamps (DST-aware, exact at hour granularity)."""
        if self._zone is None:  # type: ignore[attr-defined]
            return np.zeros_like(ts_s)
        lo = int(np.floor(np.min(ts_s) / 3600.0)) - 1
        hi = int(np.floor(np.max(ts_s) / 3600.0)) + 1
        hours = np.arange(lo, hi + 1)
        offs = np.array([self._offset_s(int(h) * 3600) for h in hours], dtype=np.float64)
        idx = np.floor(ts_s / 3600.0).astype(np.int64) - lo
        return offs[idx]

    def bucket_of_local(self, local_s: ArrayLike) -> NDArray[np.int64]:
        """Bucket index for local time(s) in seconds since the local epoch."""
        t = np.asarray(local_s, dtype=np.float64)
        day = np.floor(t / 86400.0).astype(np.int64)
        tod = np.floor((t - day * 86400.0) / self.bucket_s).astype(np.int64)
        per_day = 86400 // self.bucket_s
        dow = (day + 3) % 7  # 1970-01-01 was a Thursday -> Monday = 0
        if self.layout == "flat":
            return np.zeros_like(tod)
        if self.layout == "time_of_day":
            return tod
        if self.layout == "day_type":
            return tod + per_day * (dow >= 5)
        return tod + per_day * dow

    def buckets(self, ts_s: ArrayLike) -> NDArray[np.int64]:
        """Bucket index for UTC epoch seconds (vectorized)."""
        t = np.asarray(ts_s, dtype=np.float64)
        return self.bucket_of_local(t + self._offsets_vec(t))

    # ------------------------------------------------------------------ evaluation
    def factor(self, ts_ns: int) -> float:
        """Sigma multiplier at ``ts_ns`` (UTC ns)."""
        ts_s = ts_ns // NS_PER_S
        b = int(self.bucket_of_local(ts_s + self._offset_s(ts_s)))
        return float(self._f[b])  # type: ignore[attr-defined]

    def factors_at(self, ts_s: ArrayLike) -> FloatArr:
        """Sigma multipliers at UTC epoch seconds (vectorized)."""
        return self._f[self.buckets(ts_s)]  # type: ignore[attr-defined]

    def mean_var_factor(self, t0_ns: int, t1_ns: int) -> float:
        """Time-average of f^2 over [t0, t1) (exact piecewise integration); f^2(t0) if t1 <= t0."""
        if t1_ns <= t0_ns:
            f = self.factor(t0_ns)
            return f * f
        if self.layout == "flat":
            return float(self._f[0] ** 2)  # type: ignore[attr-defined]
        t = t0_ns / NS_PER_S
        end = t1_ns / NS_PER_S
        acc = 0.0
        guard = 0
        while t < end and guard < 100_000:
            guard += 1
            off = self._offset_s(int(math.floor(t)))
            local = t + off
            b = int(self.bucket_of_local(local))
            nxt_local = (math.floor(local / self.bucket_s) + 1) * self.bucket_s
            nxt = min(end, nxt_local - off)
            if nxt <= t:  # DST edge: step at least a second
                nxt = min(end, t + 1.0)
            f = float(self._f[b])  # type: ignore[attr-defined]
            acc += f * f * (nxt - t)
            t = nxt
        return acc / (end - t0_ns / NS_PER_S)

    def mean_var_factor_vec(self, t0_s: ArrayLike, t1_s: ArrayLike, resolution_s: int = 60) -> FloatArr:
        """Vectorized time-average of f^2 over [t0, t1) for many intervals (research).

        Integrates on a ``resolution_s`` grid (exact when bucket edges and the interval ends are
        multiples of resolution_s, e.g. minute data with hourly buckets).
        """
        t0 = np.asarray(t0_s, dtype=np.float64)
        t1 = np.asarray(t1_s, dtype=np.float64)
        lo = math.floor(float(np.min(t0)) / resolution_s) * resolution_s
        hi = math.ceil(float(np.max(np.maximum(t1, t0))) / resolution_s) * resolution_s + resolution_s
        grid = np.arange(lo, hi + resolution_s, resolution_s, dtype=np.float64)
        f2 = self.factors_at(grid[:-1] + 0.5 * resolution_s) ** 2
        cum = np.r_[0.0, np.cumsum(f2 * resolution_s)]
        c0 = np.interp(t0, grid, cum)
        c1 = np.interp(t1, grid, cum)
        dur = t1 - t0
        at0 = self.factors_at(t0) ** 2
        with np.errstate(invalid="ignore", divide="ignore"):
            return np.where(dur > 0, (c1 - c0) / np.where(dur > 0, dur, 1.0), at0)

    # ------------------------------------------------------------------ fitting
    @classmethod
    def fit(
        cls,
        ts_s: ArrayLike,
        returns: ArrayLike,
        dt_s: ArrayLike | float = 60.0,
        valid: ArrayLike | None = None,
        layout: str = "hour_of_week",
        bucket_s: int = 3600,
        tz: str = "UTC",
        normalizer: ArrayLike | None = None,
        method: str = "sq",
        winsor: float | None = 6.0,
        shrink: float = 0.0,
        min_count: int = 30,
    ) -> "SeasonalVol":
        """Fit the profile from TRAINING returns only.

        ts_s        start time of each return (UTC s)
        returns     log returns over dt_s seconds
        valid       mask of usable returns (outages excluded)
        normalizer  optional per-return sigma per sqrt(s) (e.g. a slow causal EWMA) to remove
                    the vol level before averaging, so a crash week does not imprint itself
                    on whichever buckets it hit
        method      'sq': bucket factor^2 = mean(u^2) (variance-unbiased, winsorized at
                    ``winsor`` x the pooled rms); 'abs': factor = mean|u| (robust)
        shrink      shrink factors^2 toward 1 by this weight (0..1)
        Buckets with fewer than ``min_count`` returns get factor 1.  Normalized so that the
        weekly time-average of f^2 equals 1.
        """
        ts = np.asarray(ts_s, dtype=np.float64)
        r = np.asarray(returns, dtype=np.float64)
        dt = np.broadcast_to(np.asarray(dt_s, dtype=np.float64), r.shape)
        ok = np.isfinite(r) & (dt > 0)
        if valid is not None:
            ok &= np.asarray(valid, dtype=bool)
        u = r / np.sqrt(np.where(dt > 0, dt, 1.0))
        if normalizer is not None:
            nz = np.asarray(normalizer, dtype=np.float64)
            ok &= np.isfinite(nz) & (nz > 0)
            u = u / np.where(ok, nz, 1.0)
        proto = cls(factors=(1.0,) * cls.n_buckets(layout, bucket_s), layout=layout, bucket_s=bucket_s, tz=tz)
        b = proto.buckets(ts + 0.5 * dt)  # bucket of the return's midpoint
        nb = cls.n_buckets(layout, bucket_s)
        uu = u[ok]
        bb = b[ok]
        if method == "sq":
            x = uu * uu
            if winsor is not None:
                cap = (winsor**2) * float(np.mean(x))
                x = np.minimum(x, cap)
            num = np.bincount(bb, weights=x, minlength=nb)
            cnt = np.bincount(bb, minlength=nb).astype(np.float64)
            f2 = np.where(cnt >= min_count, num / np.maximum(cnt, 1), np.nan)
        elif method == "abs":
            num = np.bincount(bb, weights=np.abs(uu), minlength=nb)
            cnt = np.bincount(bb, minlength=nb).astype(np.float64)
            f = np.where(cnt >= min_count, num / np.maximum(cnt, 1), np.nan)
            f2 = f * f
        else:
            raise ValueError("method must be 'sq' or 'abs'")
        pooled = float(np.nanmean(f2)) if np.any(np.isfinite(f2)) else 1.0
        f2 = np.where(np.isfinite(f2), f2 / pooled, 1.0)
        f2 = (1.0 - shrink) * f2 + shrink * 1.0
        f2 = f2 / cls._week_mean(f2, layout, bucket_s)
        meta = {"n": int(ok.sum()), "method": method, "winsor": winsor, "shrink": shrink, "min_count": min_count}
        return cls(factors=tuple(float(v) for v in np.sqrt(f2)), layout=layout, bucket_s=bucket_s, tz=tz, meta=meta)

    @staticmethod
    def _week_mean(f2: FloatArr, layout: str, bucket_s: int) -> float:
        per_day = 86400 // bucket_s
        if layout == "day_type":
            return float((5 * f2[:per_day].sum() + 2 * f2[per_day:].sum()) / (7 * per_day))
        return float(np.mean(f2))

    # ------------------------------------------------------------------ io
    def to_dict(self) -> dict:
        return {"layout": self.layout, "bucket_s": self.bucket_s, "tz": self.tz, "factors": list(self.factors), "meta": self.meta}

    @classmethod
    def from_dict(cls, d: dict) -> "SeasonalVol":
        return cls(factors=tuple(float(x) for x in d["factors"]), layout=d["layout"], bucket_s=int(d["bucket_s"]), tz=d.get("tz", "UTC"), meta=d.get("meta", {}))

    @classmethod
    def flat(cls) -> "SeasonalVol":
        return cls()


FLAT_SEASONAL = SeasonalVol()


# ============================================================================ streaming EWMA
class EwmaVol:
    """Streaming EWMA of per-second log-return variance with irregular time steps.

    half_life_s  EWMA half-life in seconds of elapsed time
    min_dt_s     fold a return in only once this much time has elapsed since the last anchor
                 (sub-sampling guard against microstructure noise; ticks in between are
                 covered by the next return)
    max_dt_s     returns spanning more than this (outage) are dropped; None = never
    seasonal     if given, each return is deseasonalized by the mean f^2 over its interval,
                 so ``sigma`` is the deseasonalized level
    var0         optional prior per-second variance (counts as a fully weighted history)
    Output: sigma(now_ns) in log-return units per sqrt(second); nan before the first return.
    """

    __slots__ = ("half_life_s", "min_dt_s", "max_dt_s", "seasonal", "_S", "_W", "_anchor_ts", "_anchor_lp", "_last_ts", "n_updates")

    def __init__(
        self,
        half_life_s: float,
        min_dt_s: float = 1.0,
        max_dt_s: float | None = None,
        seasonal: SeasonalVol | None = None,
        var0: float | None = None,
    ) -> None:
        if not half_life_s > 0:
            raise ValueError("half_life_s must be > 0")
        if min_dt_s < 0:
            raise ValueError("min_dt_s must be >= 0")
        self.half_life_s = float(half_life_s)
        self.min_dt_s = float(min_dt_s)
        self.max_dt_s = None if max_dt_s is None else float(max_dt_s)
        self.seasonal = seasonal
        self._S = 0.0 if var0 is None else float(var0)
        self._W = 0.0 if var0 is None else 1.0
        self._anchor_ts: int | None = None
        self._anchor_lp = 0.0
        self._last_ts: int | None = None
        self.n_updates = 0

    def update(self, ts_ns: int, price: float) -> None:
        """Feed a price observation (ts_ns: event/source time in ns; price > 0)."""
        if not (price > 0) or not math.isfinite(price):
            return
        lp = math.log(price)
        if self._anchor_ts is None:
            self._anchor_ts, self._anchor_lp, self._last_ts = ts_ns, lp, ts_ns
            return
        if ts_ns <= self._anchor_ts:
            return  # stale / out-of-order observation
        self._last_ts = max(self._last_ts or ts_ns, ts_ns)
        dt = (ts_ns - self._anchor_ts) / NS_PER_S
        if dt < self.min_dt_s:
            return
        a = 2.0 ** (-dt / self.half_life_s)
        if self.max_dt_s is not None and dt > self.max_dt_s:
            self._S *= a
            self._W *= a
        else:
            r = lp - self._anchor_lp
            x = r * r / dt
            if self.seasonal is not None:
                x /= max(self.seasonal.mean_var_factor(self._anchor_ts, ts_ns), 1e-12)
            self._S = a * self._S + (1.0 - a) * x
            self._W = a * self._W + (1.0 - a)
            self.n_updates += 1
        self._anchor_ts, self._anchor_lp = ts_ns, lp

    def variance(self, now_ns: int | None = None) -> float:
        """Per-second log variance estimate (deseasonalized if a seasonal profile is set)."""
        return self._S / self._W if self._W > 0 else math.nan

    def sigma(self, now_ns: int | None = None) -> float:
        """Sigma in log-return units per sqrt(second); nan before the first folded return."""
        v = self.variance(now_ns)
        return math.sqrt(v) if v == v else math.nan

    @property
    def weight(self) -> float:
        """Total weight of observed history in [0, 1] (1 - 2^(-observed_time / half_life))."""
        return self._W

    @property
    def ready(self) -> bool:
        """At least one half-life of observed history (weight >= 0.5)."""
        return self._W >= 0.5

    @property
    def last_ts_ns(self) -> int | None:
        return self._last_ts

    def state(self) -> tuple[float, float, int | None, float]:
        """(S, W, anchor_ts, anchor_logprice) for deterministic checkpoints."""
        return self._S, self._W, self._anchor_ts, self._anchor_lp


# ============================================================================ blended forecaster
def blended_sigma(
    sigmas: Sequence[float],
    weights: Sequence[float],
    intercept_var: float = 0.0,
    seasonal_ratio: float = 1.0,
) -> float:
    """sqrt(seasonal_ratio * (intercept_var + sum_i weights_i * sigmas_i^2)).

    sigmas are (deseasonalized) EWMA sigmas per sqrt(s); weights are non-negative variance
    weights (HAR-style), intercept_var a per-second variance anchor; seasonal_ratio is the mean
    f^2 over the pricing horizon.  Returns sigma per sqrt(second) (nan if any input is nan).
    """
    if len(sigmas) != len(weights):
        raise ValueError("sigmas and weights must have equal length")
    v = intercept_var + sum(w * s * s for w, s in zip(weights, sigmas))
    v *= seasonal_ratio
    return math.sqrt(v) if v >= 0 and v == v else math.nan


@dataclass(frozen=True)
class VolForecasterConfig:
    """Configuration of the production vol forecaster (values chosen in docs/research/01)."""

    half_lives_s: tuple[float, ...] = (1800.0, 21600.0)
    weights: tuple[float, ...] = (0.5, 0.5)
    intercept_var: float = 0.0
    scale: float = 1.0  # multiplicative calibration of sigma (variance scaled by scale^2)
    min_dt_s: float = 1.0
    max_dt_s: float | None = 600.0
    sigma_floor: float = 0.0  # per sqrt(s)
    sigma_cap: float = math.inf

    def __post_init__(self) -> None:
        if len(self.half_lives_s) != len(self.weights) or not self.half_lives_s:
            raise ValueError("half_lives_s and weights must be non-empty and of equal length")
        if any(w < 0 for w in self.weights) or self.intercept_var < 0 or self.scale <= 0:
            raise ValueError("weights/intercept must be >= 0 and scale > 0")


class VolForecaster:
    """Deseasonalized multi-half-life EWMA blend times the forward seasonal factor.

    sigma(now, until)^2 = scale^2 * mean_f2(now, until) * (intercept + sum_i w_i ewma_i(now))
    where ewma_i run on deseasonalized returns.  Output in log units per sqrt(second); pass
    ``sigma_abs = spot * sigma`` to ``dh.models.fairvalue.digital``.
    """

    def __init__(self, cfg: VolForecasterConfig | None = None, seasonal: SeasonalVol | None = None) -> None:
        self.cfg = cfg or VolForecasterConfig()
        self.seasonal = seasonal if seasonal is not None else FLAT_SEASONAL
        self._ewmas = [
            EwmaVol(h, min_dt_s=self.cfg.min_dt_s, max_dt_s=self.cfg.max_dt_s, seasonal=self.seasonal)
            for h in self.cfg.half_lives_s
        ]

    def update(self, ts_ns: int, price: float) -> None:
        """Feed a benchmark/nowcast price (ts_ns in ns)."""
        for e in self._ewmas:
            e.update(ts_ns, price)

    @property
    def ready(self) -> bool:
        return all(e.ready for e in self._ewmas)

    def base_sigma(self) -> float:
        """Deseasonalized blended sigma (per sqrt(s)) before the forward seasonal factor."""
        return blended_sigma([e.sigma() for e in self._ewmas], self.cfg.weights, self.cfg.intercept_var) * self.cfg.scale

    def sigma(self, now_ns: int, until_ns: int | None = None) -> float:
        """Sigma per sqrt(s) for pricing the interval [now_ns, until_ns] (default: spot factor)."""
        ratio = self.seasonal.mean_var_factor(now_ns, until_ns if until_ns is not None else now_ns)
        s = self.base_sigma() * math.sqrt(ratio)
        if s != s:
            return s
        return min(max(s, self.cfg.sigma_floor), self.cfg.sigma_cap)

    def sigma_abs(self, now_ns: int, until_ns: int | None, spot: float) -> float:
        """$ volatility per sqrt(second) at price ``spot``."""
        return spot * self.sigma(now_ns, until_ns)


# ============================================================================ vectorized helpers
def ewma_regular(x: ArrayLike, valid: ArrayLike | None, half_life_steps: float) -> FloatArr:
    """Bias-corrected EWMA of x on a regular grid; invalid samples are skipped (decay continues).

    y[n] = S[n] / W[n],  S[n] = a S[n-1] + (1-a) x[n] v[n],  W[n] = a W[n-1] + (1-a) v[n],
    a = 2^(-1/half_life_steps).  y[n] includes sample n (use y[n-1] for a forecast at the end
    of step n-1).  nan until the first valid sample.
    """
    xa = np.asarray(x, dtype=np.float64)
    v = np.ones_like(xa) if valid is None else np.asarray(valid, dtype=np.float64)
    xa = np.where(v > 0, xa, 0.0)
    a = 2.0 ** (-1.0 / half_life_steps)
    S = lfilter([1.0 - a], [1.0, -a], xa * v)
    W = lfilter([1.0 - a], [1.0, -a], v)
    with np.errstate(invalid="ignore", divide="ignore"):
        return np.where(W > 0, S / np.where(W > 0, W, 1.0), np.nan)


def realized_variance(r: ArrayLike) -> float:
    """Sum of squared returns (log units^2)."""
    ra = np.asarray(r, dtype=np.float64)
    return float(np.sum(ra * ra))


def bipower_variation(r: ArrayLike) -> float:
    """Barndorff-Nielsen/Shephard bipower variation (pi/2) * sum |r_i||r_{i-1}| * n/(n-1).

    Consistent for the continuous part of quadratic variation (robust to jumps).
    """
    ra = np.abs(np.asarray(r, dtype=np.float64))
    n = ra.size
    if n < 2:
        return math.nan
    return float((math.pi / 2.0) * np.sum(ra[1:] * ra[:-1]) * n / (n - 1))


def jump_share(r: ArrayLike) -> float:
    """max(0, 1 - BV/RV): share of realized variance attributable to jumps."""
    rv = realized_variance(r)
    if rv <= 0:
        return 0.0
    return max(0.0, 1.0 - bipower_variation(r) / rv)


def parkinson_var(high: ArrayLike, low: ArrayLike) -> FloatArr:
    """Parkinson range variance per bar: (ln H/L)^2 / (4 ln 2) (log units^2)."""
    h = np.asarray(high, dtype=np.float64)
    lo = np.asarray(low, dtype=np.float64)
    return np.log(h / lo) ** 2 / (4.0 * LN2)


def garman_klass_var(o: ArrayLike, h: ArrayLike, l: ArrayLike, c: ArrayLike) -> FloatArr:
    """Garman-Klass variance per bar: 0.5 ln(H/L)^2 - (2 ln2 - 1) ln(C/O)^2 (log units^2)."""
    oa, ha, la, ca = (np.asarray(v, dtype=np.float64) for v in (o, h, l, c))
    return 0.5 * np.log(ha / la) ** 2 - (2.0 * LN2 - 1.0) * np.log(ca / oa) ** 2


__all__ = [
    "EwmaVol",
    "SeasonalVol",
    "FLAT_SEASONAL",
    "VolForecaster",
    "VolForecasterConfig",
    "blended_sigma",
    "ewma_regular",
    "realized_variance",
    "bipower_variation",
    "jump_share",
    "parkinson_var",
    "garman_klass_var",
    "annualized",
    "per_sqrt_second",
    "SECONDS_PER_YEAR",
]
