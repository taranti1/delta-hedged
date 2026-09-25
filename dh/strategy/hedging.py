"""Portfolio delta hedge decisions: mean-variance no-trade band, trade to the boundary.

    D     portfolio delta in BTC = sum_i q_i * Delta_i + H (perp position)
    B     = max(B_min, 2 * c_h * S / (lam * sigma_S^2 * h_eff))    (docs/MODELS.md section 4)
    if |D| > B: trade -(D - sign(D) * B)  (back to the band edge, not to zero)

Hedging reduces variance; it does nothing for adverse selection. At M1 sizes B is far above any
reachable |D|, so the engine is effectively inactive unless `force_band_btc` is configured
(used to exercise hedge plumbing at tiny size).
"""

from __future__ import annotations

import math
from dataclasses import dataclass

from dh.strategy.config import HedgeCfg


@dataclass(frozen=True)
class HedgeDecision:
    band_btc: float
    target_btc: float  # desired change in hedge position (signed BTC); 0 = no action
    urgent: bool
    reason: str


def hedge_cost_frac(cfg: HedgeCfg, urgent: bool) -> float:
    """All-in one-way cost as a fraction of notional (fee + half spread)."""
    fee = cfg.fee_bps_taker if urgent else cfg.fee_bps_maker
    return (fee + cfg.half_spread_bps) / 1e4


def no_trade_band(
    cfg: HedgeCfg, *, spot: float, sigma_abs_per_sqrt_s: float, h_eff_s: float, lam: float
) -> float:
    """Half-width of the no-trade band in BTC."""
    c = hedge_cost_frac(cfg, urgent=False)
    h = min(max(h_eff_s, 1.0), cfg.h_eff_cap_s)
    denom = lam * sigma_abs_per_sqrt_s**2 * h
    b = math.inf if denom <= 0 else 2.0 * c * spot / denom
    return max(cfg.band_min_btc, b)


def decide_hedge(
    cfg: HedgeCfg,
    *,
    D_btc: float,
    spot: float,
    sigma_abs_per_sqrt_s: float,
    h_eff_s: float,
    lam: float,
    pending_btc: float = 0.0,
    force_band_btc: float | None = None,
) -> HedgeDecision:
    """Decide the hedge trade. pending_btc = hedge orders already working (signed), counted as
    if filled so we never double-hedge while an order is in flight."""
    if not cfg.enabled:
        return HedgeDecision(math.inf, 0.0, False, "disabled")
    band = force_band_btc if force_band_btc is not None else no_trade_band(
        cfg, spot=spot, sigma_abs_per_sqrt_s=sigma_abs_per_sqrt_s, h_eff_s=h_eff_s, lam=lam
    )
    eff = D_btc + pending_btc
    if abs(eff) <= band:
        return HedgeDecision(band, 0.0, False, "inside_band")
    trade = -(eff - math.copysign(band, eff))
    if abs(trade) < cfg.min_order_btc:
        return HedgeDecision(band, 0.0, False, "below_min_order")
    urgent = abs(eff) > cfg.urgent_mult * band
    return HedgeDecision(band, trade, urgent, "outside_band")
