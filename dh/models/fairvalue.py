"""Fair value, delta and gamma of Kalshi crypto threshold/range contracts.

Model (full derivation in docs/MODELS_fairvalue.md)
---------------------------------------------------
The settlement value is A = (sum_fixed + m * R) / n where R is the average of the m
not-yet-printed observations at times tau_j = tau_first + (j - 1) * step (seconds from now).
The benchmark is modelled as arithmetic Brownian motion around the current nowcast ``spot``:

    S(now + u) = spot + mu(u) + sigma_abs * W(u),     sigma_abs in $ per sqrt(second)

so R = spot + drift_abs + sd * eps with

    sd^2 = sigma_abs^2 * V,   V = (1/m^2) sum_i sum_j min(tau_i, tau_j)
                               = tau_first + step * ((m + 1)(2m + 1) / (6m) - 1)   [seconds]

(for m = 60, step = 1 s: V = tau_first + 19.50 s).  eps has a standardized "tail model"
distribution (``dh.models.tails``): Gauss (exact under the Brownian model), Student-t or a
lognormal Gaussian-scale mixture (fat tails / vol uncertainty).  YES iff A beats the strike,
i.e. iff R beats the *required remaining average*  K_req = (K * n - sum_fixed) / m.

    greater(_or_equal):  P = sf(z)                 z = (K_req - spot - drift_abs) / sd
    less(_or_equal):     P = cdf(z)
    between:             P = cdf(z_cap) - cdf(z_floor)
    delta = dP/dspot   (per $;  == BTC hedge quantity per YES contract on a $1 payout)
    gamma = d2P/dspot2 (per $^2)

Greeks are analytic in the tail density: for greater, delta = pdf(z)/sd, gamma = -pdf'(z)/sd^2.
Strict vs non-strict inequalities only differ on a null set in the continuous model; they are
honoured exactly when the outcome is already determined (m = 0 or sd = 0) via
``MarketSpec.yes_wins``.

Scalar API:     digital(spec, ws, spot, sigma_abs, tail, drift_abs) -> Digital
Vectorized API: digital_vec(...) (arrays of strikes / spots / sds, used by research)
"""

from __future__ import annotations

import math
from dataclasses import dataclass

import numpy as np
from numpy.typing import ArrayLike, NDArray

from dh.core.market import MarketSpec, SUPPORTED_STRIKE_TYPES
from dh.models.tails import (
    GAUSS,
    EmpiricalTail,
    Gauss,
    StudentT,
    TailModel,
    VolMixture,
    make_tail,
)
from dh.settlement.window import WindowState, required_remaining_avg

FloatArr = NDArray[np.float64]

_UPPER = ("greater", "greater_or_equal")
_LOWER = ("less", "less_or_equal")


# --------------------------------------------------------------------------- variance time
def avg_variance_time(tau_first_s: ArrayLike, step_s: ArrayLike, m: ArrayLike):
    """Var[(1/m) sum_j W(tau_j)] in seconds for tau_j = tau_first + (j-1)*step, j = 1..m.

    = tau_first + step * ((m+1)(2m+1)/(6m) - 1).  Vectorized; m == 0 gives 0.
    Units: seconds (multiply by sigma_abs^2 [$^2/s] to get the variance of R in $^2).
    """
    tf = np.asarray(tau_first_s, dtype=np.float64)
    st = np.asarray(step_s, dtype=np.float64)
    mm = np.asarray(m, dtype=np.float64)
    with np.errstate(divide="ignore", invalid="ignore"):
        v = np.where(mm > 0, tf + st * ((mm + 1.0) * (2.0 * mm + 1.0) / (6.0 * np.maximum(mm, 1.0)) - 1.0), 0.0)
    return float(v) if v.ndim == 0 else v


def remaining_avg_variance_time(ws: WindowState) -> float:
    """Variance time (seconds) of the remaining average R for a WindowState.

    Var[(1/m) sum_j W(tau_j)] with tau_j = tau_first + (j-1)*step
    = tau_first + step * ((m+1)(2m+1)/(6m) - 1);  0 when m == 0.
    """
    if ws.m_remaining == 0:
        return 0.0
    m = ws.m_remaining
    return ws.tau_first_s + ws.step_s * ((m + 1) * (2 * m + 1) / (6.0 * m) - 1.0)


def avg_variance_time_general(taus: ArrayLike) -> float:
    """Exact variance time (s) of the mean of W at arbitrary times taus >= 0 (any order).

    (1/m^2) sum_i sum_j min(tau_i, tau_j) = (1/m^2) sum_k tau_(k) * (2(m - k) + 1), k = 1..m
    with tau_(k) sorted ascending.  Used for irregular/pending observation times and tests.
    """
    t = np.sort(np.maximum(np.asarray(taus, dtype=np.float64).ravel(), 0.0))
    m = t.size
    if m == 0:
        return 0.0
    k = np.arange(1, m + 1, dtype=np.float64)
    return float(np.sum(t * (2.0 * (m - k) + 1.0)) / (m * m))


def remaining_sd(ws: WindowState, sigma_abs: float) -> float:
    """Standard deviation ($) of the remaining average R given sigma_abs ($/sqrt(s))."""
    return float(sigma_abs) * math.sqrt(remaining_avg_variance_time(ws))


def sigma_abs_from_log(spot: float, sigma_log: float) -> float:
    """Convert a log-return vol (per sqrt(second)) to $ per sqrt(second) at price ``spot``."""
    return float(spot) * float(sigma_log)


# --------------------------------------------------------------------------- results
@dataclass(frozen=True, slots=True)
class Digital:
    """Fair value and greeks of one YES contract paying $1.

    p_yes         probability of YES (fair YES price in dollars)
    delta         dP/dspot per $ of benchmark (== BTC to SELL per long YES to hedge; sign:
                  >0 for greater, <0 for less)
    gamma         d2P/dspot2 per $^2
    sd_remaining  standard deviation of the remaining average R ($)
    z             (K_req - E[R]) / sd for the floor (or only) threshold; +-inf when determined
    z_cap         same for the cap threshold of 'between' (nan otherwise)
    """

    p_yes: float
    delta: float
    gamma: float
    sd_remaining: float
    z: float
    z_cap: float = math.nan

    @property
    def p_no(self) -> float:
        return 1.0 - self.p_yes


@dataclass(frozen=True, slots=True)
class DigitalArrays:
    """Vectorized Digital: every field is an ndarray of the broadcast shape."""

    p_yes: FloatArr
    delta: FloatArr
    gamma: FloatArr
    sd_remaining: FloatArr
    z: FloatArr
    z_cap: FloatArr


# --------------------------------------------------------------------------- core math
def _tail_terms(tail: TailModel, z: FloatArr):
    return tail.cdf(z), tail.sf(z), tail.pdf(z), tail.dpdf(z)


def _upper_prob(tail: TailModel, z: FloatArr, sd: FloatArr):
    _, sf, pdf, dpdf = _tail_terms(tail, z)
    return sf, pdf / sd, -dpdf / (sd * sd)


def _lower_prob(tail: TailModel, z: FloatArr, sd: FloatArr):
    cdf, _, pdf, dpdf = _tail_terms(tail, z)
    return cdf, -pdf / sd, dpdf / (sd * sd)


def _between_prob(tail: TailModel, zf: FloatArr, zc: FloatArr, sd: FloatArr):
    cf, sff, pf, dpf = _tail_terms(tail, zf)
    cc, sfc, pc, dpc = _tail_terms(tail, zc)
    # choose the cancellation-free form: both thresholds in the upper tail -> sf(zf) - sf(zc)
    p = np.where(zf > 0, sff - sfc, np.where(zc < 0, cc - cf, 1.0 - sfc - cf))
    p = np.clip(p, 0.0, 1.0)
    delta = (pf - pc) / sd
    gamma = (dpc - dpf) / (sd * sd)
    return p, delta, gamma


def _deterministic_yes(strike_type: str, v: FloatArr, floor: FloatArr, cap: FloatArr) -> FloatArr:
    """Exact payoff (strictness honoured) for determined outcomes, vectorized."""
    if strike_type == "greater":
        return (v > floor).astype(np.float64)
    if strike_type == "greater_or_equal":
        return (v >= floor).astype(np.float64)
    if strike_type == "less":
        return (v < cap).astype(np.float64)
    if strike_type == "less_or_equal":
        return (v <= cap).astype(np.float64)
    return ((v >= floor) & (v <= cap)).astype(np.float64)


def _det_z(v: FloatArr, K: FloatArr) -> FloatArr:
    """z of a determined outcome: -inf if v > K, +inf if v < K, 0 if equal, nan if K is nan."""
    with np.errstate(invalid="ignore"):
        return np.where(v > K, -np.inf, np.where(v < K, np.inf, np.where(np.isnan(K), np.nan, 0.0)))


def digital_vec(
    strike_type: str,
    spot: ArrayLike,
    sd: ArrayLike,
    tail: TailModel | str | None = None,
    floor: ArrayLike | None = None,
    cap: ArrayLike | None = None,
    *,
    n_obs: ArrayLike = 60,
    k_fixed: ArrayLike = 0,
    sum_fixed: ArrayLike = 0.0,
    drift_abs: ArrayLike = 0.0,
    nowcast_sd: ArrayLike = 0.0,
) -> DigitalArrays:
    """Vectorized fair value and greeks (numpy broadcasting over all array arguments).

    strike_type  one of dh.core.market.SUPPORTED_STRIKE_TYPES (scalar string)
    spot         benchmark nowcast ($)
    sd           standard deviation of the remaining average R ($), e.g.
                 sigma_abs * sqrt(avg_variance_time(tau_first, step, m))
    floor, cap   strike(s) ($) as required by the strike type
    n_obs, k_fixed, sum_fixed   window accounting (m = n_obs - k_fixed)
    drift_abs    expected change of R vs spot ($)
    nowcast_sd   sd ($) of spot vs the true current benchmark (basis / latency noise); added
                 in quadrature: sd_total = sqrt(sd^2 + nowcast_sd^2)
    Returns DigitalArrays (p_yes, delta per $, gamma per $^2, sd_remaining $, z, z_cap).
    """
    if strike_type not in SUPPORTED_STRIKE_TYPES:
        raise ValueError(f"unsupported strike_type {strike_type!r}")
    tail = make_tail(tail)
    spot_a = np.asarray(spot, dtype=np.float64)
    sd_a = np.sqrt(np.asarray(sd, dtype=np.float64) ** 2 + np.asarray(nowcast_sd, dtype=np.float64) ** 2)
    n = np.asarray(n_obs, dtype=np.float64)
    k = np.asarray(k_fixed, dtype=np.float64)
    sfix = np.asarray(sum_fixed, dtype=np.float64)
    drift = np.asarray(drift_abs, dtype=np.float64)
    m = n - k
    need_floor = strike_type in _UPPER or strike_type == "between"
    need_cap = strike_type in _LOWER or strike_type == "between"
    fl = np.asarray(floor if floor is not None else np.nan, dtype=np.float64)
    cp = np.asarray(cap if cap is not None else np.nan, dtype=np.float64)
    if need_floor and floor is None:
        raise ValueError(f"{strike_type} needs floor")
    if need_cap and cap is None:
        raise ValueError(f"{strike_type} needs cap")
    shape = np.broadcast_shapes(spot_a.shape, sd_a.shape, n.shape, k.shape, sfix.shape, drift.shape, fl.shape, cp.shape)
    spot_a, sd_a, n, k, sfix, drift, fl, cp, m = (np.broadcast_to(x, shape) for x in (spot_a, sd_a, n, k, sfix, drift, fl, cp, m))
    mu = spot_a + drift
    random = (m > 0) & (sd_a > 0) & np.isfinite(sd_a)
    safe_m = np.where(m > 0, m, 1.0)
    safe_sd = np.where(random, sd_a, 1.0)
    kreq_f = (fl * n - sfix) / safe_m
    kreq_c = (cp * n - sfix) / safe_m
    zf = (kreq_f - mu) / safe_sd
    zc = (kreq_c - mu) / safe_sd
    if strike_type in _UPPER:
        p, d, g = _upper_prob(tail, zf, safe_sd)
        z, z_cap = zf, np.full(shape, np.nan)
    elif strike_type in _LOWER:
        p, d, g = _lower_prob(tail, zc, safe_sd)
        z, z_cap = zc, np.full(shape, np.nan)
    else:
        p, d, g = _between_prob(tail, zf, zc, safe_sd)
        z, z_cap = zf, zc
    if not np.all(random):
        # determined: all prints fixed (m == 0) or no randomness left (sd == 0)
        v_final = np.where(m > 0, (sfix + m * mu) / n, sfix / n)
        det = _deterministic_yes(strike_type, v_final, fl, cp)
        p = np.where(random, p, det)
        d = np.where(random, d, 0.0)
        g = np.where(random, g, 0.0)
        zf_det, zc_det = _det_z(v_final, fl), _det_z(v_final, cp)
        if strike_type in _LOWER:
            z = np.where(random, z, zc_det)
        else:
            z = np.where(random, z, zf_det)
        if strike_type == "between":
            z_cap = np.where(random, z_cap, zc_det)
    sd_out = np.where(m > 0, np.where(np.isfinite(sd_a), sd_a, np.nan), 0.0)
    return DigitalArrays(
        p_yes=np.asarray(p, dtype=np.float64),
        delta=np.asarray(d, dtype=np.float64),
        gamma=np.asarray(g, dtype=np.float64),
        sd_remaining=np.asarray(sd_out, dtype=np.float64),
        z=np.asarray(z, dtype=np.float64),
        z_cap=np.asarray(z_cap, dtype=np.float64),
    )


def digital(
    spec: MarketSpec,
    ws: WindowState,
    spot: float,
    sigma_abs: float,
    tail: TailModel | str | None = "gauss",
    drift_abs: float = 0.0,
    nowcast_sd: float = 0.0,
) -> Digital:
    """Fair value and greeks of one YES contract of ``spec`` given the window state.

    spot       current nowcast of the settlement benchmark ($)
    sigma_abs  benchmark volatility in $ per sqrt(second) (= spot * log-vol per sqrt(s))
    tail       TailModel or config string ('gauss', 'student_t(5)', 'vol_mixture(0.5)')
    drift_abs  expected change of the remaining average vs spot ($), usually 0
    nowcast_sd sd ($) of spot vs the true current benchmark value (venue basis, feed latency);
               added in quadrature to the sd of R.  Keeps the price and greeks finite in the
               last seconds, when the diffusion sd of R goes to zero.
    Returns Digital(p_yes, delta [per $], gamma [per $^2], sd_remaining [$], z, z_cap).
    Edge cases: m_remaining == 0 -> exact payoff of the fixed average, delta = gamma = 0;
    sigma_abs == 0 -> deterministic with R = spot + drift_abs.
    """
    if not (sigma_abs >= 0.0):
        raise ValueError("sigma_abs must be >= 0")
    tail_m = make_tail(tail)
    if not (nowcast_sd >= 0.0):
        raise ValueError("nowcast_sd must be >= 0")
    sd = remaining_sd(ws, sigma_abs)
    if ws.m_remaining > 0 and nowcast_sd > 0.0:
        sd = math.sqrt(sd * sd + nowcast_sd * nowcast_sd)
    if ws.m_remaining == 0 or sd <= 0.0:
        v = ws.sum_fixed / ws.n_obs if ws.m_remaining == 0 else (ws.sum_fixed + ws.m_remaining * (spot + drift_abs)) / ws.n_obs
        yes = spec.yes_wins(v)
        zf = float(_det_z(np.float64(v), np.float64(spec.floor_strike if spec.floor_strike is not None else np.nan)))
        zc = float(_det_z(np.float64(v), np.float64(spec.cap_strike if spec.cap_strike is not None else np.nan)))
        z = zc if spec.strike_type in _LOWER else zf
        z_cap = zc if spec.strike_type == "between" else math.nan
        return Digital(p_yes=1.0 if yes else 0.0, delta=0.0, gamma=0.0, sd_remaining=sd, z=z, z_cap=z_cap)
    mu = spot + drift_abs
    st = spec.strike_type
    if st in _UPPER:
        kf = required_remaining_avg(float(spec.floor_strike), ws)  # type: ignore[arg-type]
        z = (kf - mu) / sd
        p, d, g = _upper_prob(tail_m, np.float64(z), np.float64(sd))
        return Digital(float(p), float(d), float(g), sd, float(z))
    if st in _LOWER:
        kc = required_remaining_avg(float(spec.cap_strike), ws)  # type: ignore[arg-type]
        z = (kc - mu) / sd
        p, d, g = _lower_prob(tail_m, np.float64(z), np.float64(sd))
        return Digital(float(p), float(d), float(g), sd, float(z))
    kf = required_remaining_avg(float(spec.floor_strike), ws)  # type: ignore[arg-type]
    kc = required_remaining_avg(float(spec.cap_strike), ws)  # type: ignore[arg-type]
    zf, zc = (kf - mu) / sd, (kc - mu) / sd
    p, d, g = _between_prob(tail_m, np.float64(zf), np.float64(zc), np.float64(sd))
    return Digital(float(p), float(d), float(g), sd, float(zf), float(zc))


@dataclass(frozen=True, slots=True)
class DigitalBand:
    """Range of P(YES) over model-uncertainty scenarios, plus the central Digital."""

    p_lo: float
    p_hi: float
    center: Digital


def digital_band(
    spec: MarketSpec,
    ws: WindowState,
    spot: float,
    sigma_abs_values: tuple[float, ...] | list[float],
    tails: tuple | list = ("gauss",),
    drift_abs: float = 0.0,
    nowcast_sd_values: tuple[float, ...] | list[float] = (0.0,),
) -> DigitalBand:
    """Fair-value band [p_lo, p_hi] over every combination of the given scenarios.

    sigma_abs_values  e.g. (central, low, high) $ vols per sqrt(second)
    tails             e.g. (fitted Student-t, Gauss)
    nowcast_sd_values e.g. (central, stressed) nowcast error sds ($)
    The first element of each list defines ``center``.  Used for the quoting band of
    docs/MODELS.md section 1 (bids against p_lo, asks against p_hi).
    """
    if not sigma_abs_values or not tails or not nowcast_sd_values:
        raise ValueError("each scenario list needs at least one element")
    center = digital(spec, ws, spot, sigma_abs_values[0], tails[0], drift_abs, nowcast_sd=nowcast_sd_values[0])
    lo = hi = center.p_yes
    for sa in sigma_abs_values:
        for t in tails:
            for ns in nowcast_sd_values:
                p = digital(spec, ws, spot, sa, t, drift_abs, nowcast_sd=ns).p_yes
                lo, hi = min(lo, p), max(hi, p)
    return DigitalBand(p_lo=lo, p_hi=hi, center=center)


def hedge_notional_usd(delta: float, spot: float, contracts: float = 1.0) -> float:
    """BTC hedge notional ($) for ``contracts`` YES contracts: |delta| * spot * contracts."""
    return abs(delta) * spot * contracts


__all__ = [
    "Digital",
    "DigitalArrays",
    "DigitalBand",
    "digital_band",
    "TailModel",
    "Gauss",
    "StudentT",
    "VolMixture",
    "EmpiricalTail",
    "GAUSS",
    "make_tail",
    "avg_variance_time",
    "avg_variance_time_general",
    "remaining_avg_variance_time",
    "remaining_sd",
    "sigma_abs_from_log",
    "digital",
    "digital_vec",
    "hedge_notional_usd",
]
