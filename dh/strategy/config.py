"""Strategy configuration (deterministic; loaded from YAML, hashed into every run record)."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field, fields, is_dataclass
from pathlib import Path
from typing import Any

import yaml


@dataclass(frozen=True)
class FairValueCfg:
    tail: str = "gauss"  # gauss | student_t | vol_mixture (research picks; see docs/research/01)
    student_nu: float = 5.0
    mixture_cv: float = 0.25
    vol_half_life_s: float = 1800.0  # EWMA half-life on benchmark returns
    vol_floor_ann: float = 0.15  # annualized floor
    vol_cap_ann: float = 3.0
    use_seasonality: bool = True
    band_sigma_lo_mult: float = 0.85  # sigma multipliers spanning vol uncertainty for F_lo/F_hi
    band_sigma_hi_mult: float = 1.20
    band_tail_alt: str = "gauss"  # alternative tail model spanned by the band (production tail is the fitted Student-t)
    nowcast: str = "brti_last"  # brti_last | brti_plus_composite (Experiment 2 decides)
    nowcast_beta: float = 0.0
    max_benchmark_age_s: float = 3.0


@dataclass(frozen=True)
class QuotingCfg:
    enabled_series: tuple[str, ...] = ("KXBTCD",)
    max_ticks_from_touch: int = 3
    clip_contracts: float = 5.0  # size per quote (contracts)
    v_min_dollars: float = 0.001  # min net value per filled contract to quote (0.1c)
    kappa_replace_per_s: float = 0.0002  # EVrate improvement ($/s) required to cancel/replace
    replace_rel: float = 0.5  # ...and a relative improvement of this fraction (hysteresis)
    min_order_age_ms: int = 3000  # positive-value orders are never replaced younger than this
    min_tau_s: float = 90.0  # no new near-strike quotes after T - min_tau_s
    z_min_final: float = 2.5  # |z| required to quote inside min_tau_s
    max_tau_s: float = 3900.0  # only quote events expiring within this horizon
    price_floor_px: int = 100  # never quote below 1c / above 99c YES (M1)
    price_cap_px: int = 9900
    requote_min_interval_ms: int = 250  # per market-side, rate-limit budget protection
    post_only: bool = True
    order_expiry_s: float = 0.0  # 0 = GTC; >0 sets expiration_time (dead-man for stale orders)


@dataclass(frozen=True)
class FillModelCfg:
    # M1 parametric defaults (overwritten by calibrate_fill_model from recorded trades).
    taker_rate_per_s: float = 0.5  # contracts/s arriving at the touch per side (segment default)
    taker_size_mean: float = 20.0  # contracts, mean of lognormal size distribution
    taker_size_cv: float = 2.0
    improve_rate_mult: float = 1.0  # intensity multiplier when first in queue at an improved price
    behind_rate_mult: float = 0.15  # sweep intensity for prices behind the touch (per tick)


@dataclass(frozen=True)
class AdverseSelCfg:
    a0: float = 0.002  # $ per contract baseline adverse selection (0.2c)
    b_momentum: float = 0.5  # fraction of the recent adverse fair-value move expected to continue
    c_final: float = 0.01  # extra $ per contract within the final 120 s
    lookback_s: float = 2.0  # window for the recent fair-value move
    behind_mult: float = 2.0  # sweep fills (quotes behind the touch) are more toxic (Experiment 4)
    horizon_s: float = 60.0


@dataclass(frozen=True)
class RiskCfg:
    risk_capital: float = 2_000.0
    kelly_fraction: float = 0.25
    lambda_tail: float = 1.0
    tail_budget: float = 50.0  # CVaR95 loss budget per event ($)
    max_pos_per_market: float = 25.0  # contracts
    max_event_worst_loss: float = 50.0  # $
    max_total_worst_loss: float = 150.0  # $
    max_abs_delta_btc: float = 0.05
    max_hedge_notional: float = 10_000.0
    daily_loss_halt: float = 75.0
    settlement_loss_halt: float = 40.0
    near_expiry_s: float = 300.0
    near_expiry_limit_mult: float = 0.5
    stress_move_frac: float = 0.15  # +/- range of A for worst-case with linear hedge
    abnormal_move_sigma: float = 6.0
    abnormal_pause_s: float = 120.0
    stale_ext_s: float = 2.0
    stale_brti_cancel_near_s: float = 3.0
    stale_brti_cancel_all_s: float = 10.0
    order_group_limit_contracts: float = 50.0
    order_group_cooldown_s: float = 60.0  # pause after the exchange auto-cancels a fill burst
    brti_resume_after_s: float = 30.0  # after a BRTI outage, require this long of fresh ticks
    book_resume_after_s: float = 5.0  # after a book gap/resync, wait before quoting that market
    own_gap_pause_s: float = 30.0  # pause after a sequence gap on our own fill/order channels
    kalshi_stream: str = "kalshi.ws"
    hedge_stream: str = "kalshi_perp.ws"  # FeedStatus stream of the hedge venue


@dataclass(frozen=True)
class HedgeCfg:
    enabled: bool = False  # M1: plumbing only; economics say no hedge at M1 size
    venue: str = "kalshi_perp"
    symbol: str = "KXBTCPERP"
    fee_bps_maker: float = 5.0
    fee_bps_taker: float = 12.0
    half_spread_bps: float = 1.0
    rho_hedged_fraction: float = 0.0  # expected fraction of delta that ends up hedged (quote cost term)
    band_min_btc: float = 0.01
    urgent_mult: float = 3.0  # |D| > urgent_mult * B -> IOC instead of post-only
    min_order_btc: float = 0.001
    h_eff_cap_s: float = 3600.0


@dataclass(frozen=True)
class TimerCfg:
    quote_period_ms: int = 200
    risk_period_ms: int = 1000


@dataclass(frozen=True)
class StrategyConfig:
    run_prefix: str = "dh"
    fair_value: FairValueCfg = field(default_factory=FairValueCfg)
    quoting: QuotingCfg = field(default_factory=QuotingCfg)
    fill: FillModelCfg = field(default_factory=FillModelCfg)
    adverse: AdverseSelCfg = field(default_factory=AdverseSelCfg)
    risk: RiskCfg = field(default_factory=RiskCfg)
    hedge: HedgeCfg = field(default_factory=HedgeCfg)
    timers: TimerCfg = field(default_factory=TimerCfg)

    @property
    def lam(self) -> float:
        """Mean-variance risk aversion (1/$): 1 / (kelly_fraction * risk_capital)."""
        return 1.0 / (self.risk.kelly_fraction * self.risk.risk_capital)

    def digest(self) -> str:
        blob = json.dumps(asdict(self), sort_keys=True, default=str).encode()
        return hashlib.sha256(blob).hexdigest()[:16]


def _build(cls: type, data: dict[str, Any]) -> Any:
    kwargs = {}
    names = {f.name: f for f in fields(cls)}
    for k, v in (data or {}).items():
        if k not in names:
            raise KeyError(f"unknown config key {cls.__name__}.{k}")
        f = names[k]
        default = f.default_factory() if callable(f.default_factory) else f.default  # type: ignore[misc]
        if is_dataclass(default):
            kwargs[k] = _build(type(default), v)
        elif isinstance(default, tuple):
            kwargs[k] = tuple(v)
        else:
            kwargs[k] = v
    return cls(**kwargs)


def load_config(path: str | Path | None) -> StrategyConfig:
    if path is None:
        return StrategyConfig()
    data = yaml.safe_load(Path(path).read_text()) or {}
    return _build(StrategyConfig, data)
