"""Fill-intensity and adverse-selection models used by the quote engine.

M1 = interpretable parametric forms with per-segment parameters measured from recorded public
trades (dh.research.calibrate_fill). M2 replaces them with fitted models only if that
improves out-of-sample realized net P&L.

Fill intensity (contracts/s filled for OUR order):
    taker orders arrive at rate r = Lambda / E[X] (orders/s) on our side, sizes X ~ LogNormal.
    An order of size X first consumes Q_eff (displayed qty at better prices on our side plus the
    queue ahead of us at our price), then fills us up to our size z:
        filled = min((X - Q_eff)^+, z)
    E[filled] = C(Q_eff) - C(Q_eff + z),   C(a) = E[(X - a)^+] (lognormal call formula)
    intensity = r * E[filled] * mult     (mult discounts sweeps for prices behind the touch)
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field

from scipy.special import ndtr

from dh.strategy.config import AdverseSelCfg, FillModelCfg


def lognormal_params(mean: float, cv: float) -> tuple[float, float]:
    """(mu, sigma) of a lognormal with the given mean and coefficient of variation."""
    s2 = math.log(1.0 + cv * cv)
    return math.log(mean) - 0.5 * s2, math.sqrt(s2)


def lognormal_call(mu: float, sigma: float, a: float) -> float:
    """E[(X - a)^+] for X ~ LogNormal(mu, sigma)."""
    mean = math.exp(mu + 0.5 * sigma * sigma)
    if a <= 0:
        return mean - a
    d1 = (mu + sigma * sigma - math.log(a)) / sigma
    d2 = d1 - sigma
    return mean * float(ndtr(d1)) - a * float(ndtr(d2))


@dataclass(frozen=True)
class SegmentFlow:
    """Taker flow reaching one side of the touch in one segment."""

    rate_contracts_per_s: float
    size_mean: float
    size_cv: float

    @property
    def order_rate(self) -> float:
        return self.rate_contracts_per_s / self.size_mean


def segment_key(tau_s: float, abs_z: float, side: str) -> tuple[str, str, str]:
    """Segment buckets (tau x |z| x side) used for flow and adverse-selection parameters."""
    if tau_s > 1800:
        tb = ">30m"
    elif tau_s > 600:
        tb = "10-30m"
    elif tau_s > 300:
        tb = "5-10m"
    elif tau_s > 60:
        tb = "1-5m"
    elif tau_s > 30:
        tb = "30-60s"
    else:
        tb = "<30s"
    zb = "atm" if abs_z < 0.5 else "near" if abs_z < 1.5 else "mid" if abs_z < 2.5 else "far"
    return tb, zb, side


@dataclass
class FillIntensityModel:
    cfg: FillModelCfg
    segments: dict[tuple[str, str, str], SegmentFlow] = field(default_factory=dict)

    def flow(self, key: tuple[str, str, str]) -> SegmentFlow:
        f = self.segments.get(key)
        if f is None:
            return SegmentFlow(self.cfg.taker_rate_per_s, self.cfg.taker_size_mean, self.cfg.taker_size_cv)
        return f

    def intensity(self, *, key: tuple[str, str, str], q_eff: float, size: float, position: str) -> float:
        """Expected contracts/s filled for our order.

        q_eff     contracts that must trade before us (better-priced displayed qty + queue ahead)
        size      our order size (contracts)
        position  'touch' | 'improve' | 'behind'
        """
        f = self.flow(key)
        mu, sg = lognormal_params(f.size_mean, f.size_cv)
        filled = lognormal_call(mu, sg, q_eff) - lognormal_call(mu, sg, q_eff + size)
        mult = 1.0
        if position == "improve":
            mult = self.cfg.improve_rate_mult
        elif position == "behind":
            mult = self.cfg.behind_rate_mult
        return max(0.0, f.order_rate * filled * mult)


@dataclass
class AdverseSelectionModel:
    """E[fair-value move against us | our fill] in $ per contract (M1 parametric)."""

    cfg: AdverseSelCfg
    coefs: dict[tuple[str, str, str], tuple[float, float, float]] = field(default_factory=dict)

    def expected(
        self, *, key: tuple[str, str, str], adverse_recent_move: float, tau_s: float, position: str = "touch"
    ) -> float:
        """adverse_recent_move: recent fair-value change ($/contract) in the direction that
        hurts this side (>= 0 means the market is moving against the quote). Quotes resting
        behind the touch only fill on sweeps, which carry more information: scaled by
        cfg.behind_mult."""
        a0, b, c = self.coefs.get(key, (self.cfg.a0, self.cfg.b_momentum, self.cfg.c_final))
        extra = c if tau_s < 120.0 else 0.0
        base = max(0.0, a0 + b * max(0.0, adverse_recent_move) + extra)
        return base * (self.cfg.behind_mult if position == "behind" else 1.0)
