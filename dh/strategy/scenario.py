"""Scenario-grid risk engine for binary books on a common settlement average A.

For one event (one expiration T), every market pays a function of the same settlement average

    A = (sum_fixed + m * R) / n,   R = mu_R + sd_R * eps,   eps ~ unit-variance tail model

We discretize eps on a dense grid, carry probability weights, and evaluate the whole book's
P&L at every grid point. Means, variances, CVaR and marginal risk charges of candidate orders
follow from vector operations. The approach is exact for any mix of strike types
(greater/less/between) and any tail model with a density, and it captures cross-strike netting
without approximation.

Hedge leg: a perp position H held for the event is modeled as paying H * (R - spot), where
R = (n*A - sum_fixed)/m is the average of the REMAINING prints. This is exact when the hedge is
unwound as a TWAP across the remaining settlement prints (the hedge engine's unwind rule) and
it keeps the hedge mean-zero. Inside the window (m < n) using A instead of R would overstate the
BTC delta by n/m and give the perp a spurious drift (audit finding M2).

Cross-event (different expirations) risk uses a common-factor approximation: each event's P&L
is regressed on its own R; with Brownian increments Cov(R_e, R_f) ~= sigma_S^2 * min(tau_e,
tau_f), so Cov(PnL_e, PnL_f) ~= D_e D_f sigma_S^2 min(tau_e, tau_f), D_e = Cov(PnL_e, R_e)/Var(R_e)
(the BTC delta of the event book).
"""

from __future__ import annotations

import math
from dataclasses import dataclass

import numpy as np
from scipy import special, stats

from dh.core.market import MarketSpec

# ----------------------------------------------------------------------------- tail densities
_GH_NODES, _GH_WEIGHTS = np.polynomial.hermite_e.hermegauss(21)  # probabilists' Hermite
_GH_WEIGHTS = _GH_WEIGHTS / _GH_WEIGHTS.sum()


def tail_pdf(x: np.ndarray, tail: str = "gauss", nu: float = 5.0, cv: float = 0.25) -> np.ndarray:
    """Unit-variance density of eps.

    gauss        standard normal
    student_t    Student-t with nu > 2 dof, scaled to unit variance
    vol_mixture  Gaussian scale mixture: eps = s * Z, log s ~ N(mu, sd^2) with E[s^2] = 1 and
                 sd = sqrt(log(1 + cv^2)) (cv = coefficient of variation of the scale s);
                 integrated with 21-node Gauss-Hermite quadrature (deterministic). Legacy path:
                 production code passes dh.models.tails objects instead.
    """
    x = np.asarray(x, dtype=float)
    if tail == "gauss":
        return stats.norm.pdf(x)
    if tail == "student_t":
        if nu <= 2:
            raise ValueError("student_t requires nu > 2")
        s = math.sqrt((nu - 2.0) / nu)
        return stats.t.pdf(x / s, df=nu) / s
    if tail == "vol_mixture":
        sd = math.sqrt(math.log(1.0 + cv * cv))
        mu = -sd * sd  # E[s^2] = exp(2mu + 2sd^2) = 1
        out = np.zeros_like(x)
        for node, wgt in zip(_GH_NODES, _GH_WEIGHTS):
            s = math.exp(mu + sd * node)
            out += wgt * stats.norm.pdf(x / s) / s
        return out
    raise ValueError(f"unknown tail model {tail!r}")


def tail_sf(x: np.ndarray, tail: str = "gauss", nu: float = 5.0, cv: float = 0.25) -> np.ndarray:
    """P(eps > x) for the unit-variance tail model."""
    x = np.asarray(x, dtype=float)
    if tail == "gauss":
        return special.ndtr(-x)
    if tail == "student_t":
        s = math.sqrt((nu - 2.0) / nu)
        return stats.t.sf(x / s, df=nu)
    if tail == "vol_mixture":
        sd = math.sqrt(math.log(1.0 + cv * cv))
        mu = -sd * sd
        out = np.zeros_like(x)
        for node, wgt in zip(_GH_NODES, _GH_WEIGHTS):
            out += wgt * special.ndtr(-x / math.exp(mu + sd * node))
        return out
    raise ValueError(f"unknown tail model {tail!r}")


def _sf(tail: object, x: np.ndarray, nu: float, cv: float) -> np.ndarray:
    """Survival function of the unit-variance tail. Accepts a dh.models.tails.TailModel (the
    SAME object the pricer uses, so risk and fair value share one distribution) or a legacy
    name ('gauss' | 'student_t' | 'vol_mixture') with nu/cv."""
    if hasattr(tail, "sf"):
        return np.asarray(tail.sf(x), dtype=float)  # type: ignore[attr-defined]
    if isinstance(tail, str) and tail not in ("gauss", "student_t", "vol_mixture"):
        from dh.models.tails import make_tail

        return np.asarray(make_tail(tail).sf(x), dtype=float)
    return tail_sf(x, str(tail), nu, cv)


# ----------------------------------------------------------------------------- payoffs
def payoff_vector(spec: MarketSpec, A: np.ndarray) -> np.ndarray:
    """YES payoff (0/1 as float) of `spec` at each settlement value in A."""
    st = spec.strike_type
    if st == "greater":
        return (A > spec.floor_strike).astype(float)
    if st == "greater_or_equal":
        return (A >= spec.floor_strike).astype(float)
    if st == "less":
        return (A < spec.cap_strike).astype(float)
    if st == "less_or_equal":
        return (A <= spec.cap_strike).astype(float)
    return ((A >= spec.floor_strike) & (A <= spec.cap_strike)).astype(float)


def breakpoints(spec: MarketSpec) -> list[float]:
    return [v for v in (spec.floor_strike, spec.cap_strike) if v is not None]


# ----------------------------------------------------------------------------- event grid
@dataclass
class EventGrid:
    """Probability grid over the settlement average A of one event."""

    A: np.ndarray  # settlement values
    w: np.ndarray  # probability weights, sum to 1
    mean_A: float
    var_A: float
    spot: float
    n_obs: int = 60
    sum_fixed: float = 0.0
    m_remaining: int = 60

    @property
    def R(self) -> np.ndarray:
        """Average of the remaining (unfixed) prints at each grid point."""
        if self.m_remaining <= 0:
            return np.full(self.A.shape, self.spot)
        return (self.n_obs * self.A - self.sum_fixed) / self.m_remaining

    @property
    def mean_R(self) -> float:
        return float(np.dot(self.w, self.R))

    @property
    def var_R(self) -> float:
        r = self.R
        mr = float(np.dot(self.w, r))
        return float(np.dot(self.w, (r - mr) ** 2))

    @classmethod
    def build(
        cls,
        *,
        n_obs: int,
        sum_fixed: float,
        m_remaining: int,
        mu_R: float,
        sd_R: float,
        tail: object = "gauss",
        nu: float = 5.0,
        cv: float = 0.25,
        n_points: int = 1601,
        eps_max: float = 10.0,
        spot: float | None = None,
        breakpoints_A: tuple[float, ...] | list[float] = (),
    ) -> EventGrid:
        """Grid for A = (sum_fixed + m * (mu_R + sd_R * eps)) / n_obs.

        Cells have edges on a uniform eps grid plus every strike breakpoint (mapped to eps),
        so no cell straddles a strike: payoffs evaluated at cell midpoints are exact and
        probabilities of every market are exact up to the CDF's numerical precision.
        Cell masses are exact CDF differences; the tails beyond +/-eps_max are folded into the
        end cells.
        """
        sp = spot if spot is not None else mu_R
        if m_remaining <= 0 or sd_R <= 0:
            a = (sum_fixed + m_remaining * mu_R) / n_obs
            return cls(np.array([a]), np.array([1.0]), a, 0.0, sp, n_obs, sum_fixed, m_remaining)
        edges = np.linspace(-eps_max, eps_max, n_points + 1)
        if breakpoints_A:
            be = (np.asarray(breakpoints_A, dtype=float) * n_obs - sum_fixed) / m_remaining
            be = (be - mu_R) / sd_R
            be = be[(be > -eps_max) & (be < eps_max)]
            edges = np.unique(np.concatenate([edges, be]))
        cdf = 1.0 - _sf(tail, edges, nu, cv)
        cdf[0], cdf[-1] = 0.0, 1.0  # fold tails into the end cells
        w = np.diff(cdf)
        mid = 0.5 * (edges[:-1] + edges[1:])
        keep = w > 0
        w, mid = w[keep], mid[keep]
        w = w / w.sum()
        A = (sum_fixed + m_remaining * (mu_R + sd_R * mid)) / n_obs
        mean_A = float(np.dot(w, A))
        var_A = float(np.dot(w, (A - mean_A) ** 2))
        return cls(A, w, mean_A, var_A, sp, n_obs, sum_fixed, m_remaining)

    def prob(self, spec: MarketSpec) -> float:
        return float(np.dot(self.w, payoff_vector(spec, self.A)))


@dataclass
class BookRisk:
    mean: float
    var: float
    cvar95_loss: float  # expected loss in the worst 5% (positive = loss)
    worst_loss: float  # worst-case loss over the stress range (positive = loss)
    dollar_delta: float  # Cov(PnL, A)/Var(A): BTC-equivalent exposure of the event book


def book_pnl(
    grid: EventGrid,
    payoffs: dict[str, np.ndarray],
    positions: dict[str, float],
    cost_basis: dict[str, float],
    hedge_btc: float = 0.0,
) -> np.ndarray:
    """P&L ($) at each grid point: sum_i q_i (payoff_i - c_i) + hedge_btc * (R - spot).

    positions in contracts (signed YES), cost_basis in $ per contract (YES price paid, average).
    The hedge leg uses R (remaining-print average), see module docstring.
    """
    pnl = np.full(grid.A.shape, 0.0)
    for t, q in positions.items():
        if q:
            pnl += q * (payoffs[t] - cost_basis.get(t, 0.0))
    if hedge_btc:
        pnl += hedge_btc * (grid.R - grid.spot)
    return pnl


def cvar_loss(pnl: np.ndarray, w: np.ndarray, alpha: float = 0.05) -> float:
    """Expected loss (positive) in the worst `alpha` probability mass."""
    order = np.argsort(pnl, kind="stable")
    p, ww = pnl[order], w[order]
    cum = np.cumsum(ww)
    k = int(np.searchsorted(cum, alpha))
    take = ww[: k + 1].copy()
    over = cum[k] - alpha
    take[-1] -= max(over, 0.0)
    tot = take.sum()
    if tot <= 0:
        return float(-p[0])
    return float(-(np.dot(take, p[: k + 1]) / tot))


def worst_case_loss(
    specs: dict[str, MarketSpec],
    positions: dict[str, float],
    cost_basis: dict[str, float],
    spot: float,
    hedge_btc: float = 0.0,
    stress_frac: float = 0.15,
    n_obs: int = 60,
    sum_fixed: float = 0.0,
    m_remaining: int = 60,
) -> float:
    """Exact worst-case loss over A in [spot(1-x), spot(1+x)] (piecewise-constant + linear).

    The hedge leg pays hedge_btc * (R - spot) with R = (n*A - sum_fixed)/m (see module doc).
    """
    lo, hi = spot * (1 - stress_frac), spot * (1 + stress_frac)
    pts = {lo, hi}
    for t, q in positions.items():
        if q:
            for b in breakpoints(specs[t]):
                for d in (-1e-6, 0.0, 1e-6):
                    v = b + d
                    if lo <= v <= hi:
                        pts.add(v)
    A = np.array(sorted(pts))
    pnl = np.zeros_like(A)
    for t, q in positions.items():
        if q:
            pnl += q * (payoff_vector(specs[t], A) - cost_basis.get(t, 0.0))
    if hedge_btc:
        R = (n_obs * A - sum_fixed) / m_remaining if m_remaining > 0 else np.full(A.shape, spot)
        pnl += hedge_btc * (R - spot)
    return float(max(0.0, -pnl.min()))


def risk_stats(grid: EventGrid, pnl: np.ndarray, worst: float) -> BookRisk:
    """dollar_delta = Cov(PnL, R)/Var(R): the event book's BTC delta (hedge -dollar_delta)."""
    mean = float(np.dot(grid.w, pnl))
    var = float(np.dot(grid.w, (pnl - mean) ** 2))
    R = grid.R
    mr = float(np.dot(grid.w, R))
    var_r = float(np.dot(grid.w, (R - mr) ** 2))
    cov = float(np.dot(grid.w, (pnl - mean) * (R - mr)))
    dd = cov / var_r if var_r > 0 else 0.0
    return BookRisk(mean, var, cvar_loss(pnl, grid.w), worst, dd)


def risk_objective(r: BookRisk, lam: float, lambda_tail: float, tail_budget: float) -> float:
    """Risk charge ($): (lam/2) Var + lambda_tail * max(0, CVaR95 - budget)."""
    return 0.5 * lam * r.var + lambda_tail * max(0.0, r.cvar95_loss - tail_budget)


def marginal_risk_charge(
    grid: EventGrid,
    base_pnl: np.ndarray,
    payoff_k: np.ndarray,
    price: float,
    dq: float,
    lam: float,
    lambda_tail: float,
    tail_budget: float,
) -> float:
    """Risk charge per contract of adding dq contracts (signed) of market k at `price` ($)."""
    if dq == 0:
        return 0.0

    def charge(pnl: np.ndarray) -> float:
        mean = float(np.dot(grid.w, pnl))
        var = float(np.dot(grid.w, (pnl - mean) ** 2))
        tail = 0.0
        # CVaR <= worst grid loss, so the O(N log N) CVaR is needed only if the worst loss
        # exceeds the budget (exact shortcut; keeps requote cycles fast)
        if lambda_tail > 0 and -float(pnl.min()) > tail_budget:
            tail = cvar_loss(pnl, grid.w)
        return 0.5 * lam * var + lambda_tail * max(0.0, tail - tail_budget)

    new = base_pnl + dq * (payoff_k - price)
    return (charge(new) - charge(base_pnl)) / abs(dq)


def cross_event_variance(
    dollar_deltas: list[float], taus_s: list[float], sigma_abs_per_sqrt_s: float
) -> float:
    """Sum over event pairs e != f of D_e D_f sigma^2 min(tau_e, tau_f) (common BTC factor)."""
    tot = 0.0
    n = len(dollar_deltas)
    s2 = sigma_abs_per_sqrt_s**2
    for i in range(n):
        for j in range(n):
            if i != j:
                tot += dollar_deltas[i] * dollar_deltas[j] * s2 * min(taus_s[i], taus_s[j])
    return tot


def worst_case_loss_with_orders(
    specs: dict[str, MarketSpec],
    positions: dict[str, float],
    cost_basis: dict[str, float],
    working: list[tuple[str, str, float, float]],
    spot: float,
    hedge_btc: float = 0.0,
    stress_frac: float = 0.15,
    n_obs: int = 60,
    sum_fixed: float = 0.0,
    m_remaining: int = 60,
) -> float:
    """Worst-case loss over A in the stress range AND over which working orders fill.

    working: (ticker, 'bid'|'ask', price $, contracts). At each settlement value the adversary
    fills exactly the orders that lose money there (a bid at p loses p for YES->0, an ask at p
    loses 1-p for YES->1), which is the exact worst case over all fill subsets.
    """
    lo, hi = spot * (1 - stress_frac), spot * (1 + stress_frac)
    pts = {lo, hi}
    tickers = set(positions) | {w[0] for w in working}
    for t in tickers:
        for b in breakpoints(specs[t]):
            for d in (-1e-6, 0.0, 1e-6):
                v = b + d
                if lo <= v <= hi:
                    pts.add(v)
    A = np.array(sorted(pts))
    pnl = np.zeros_like(A)
    for t, q in positions.items():
        if q:
            pnl += q * (payoff_vector(specs[t], A) - cost_basis.get(t, 0.0))
    for t, side, px, n in working:
        pay = payoff_vector(specs[t], A)
        contrib = (pay - px) * n if side == "bid" else (px - pay) * n
        pnl += np.minimum(contrib, 0.0)
    if hedge_btc:
        R = (n_obs * A - sum_fixed) / m_remaining if m_remaining > 0 else np.full(A.shape, spot)
        pnl += hedge_btc * (R - spot)
    return float(max(0.0, -pnl.min()))
