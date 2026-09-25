"""Strategy configuration (deterministic; loaded from YAML, hashed into every run record)."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field, fields, is_dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import yaml

PROVENANCE_STATUSES = ("prior", "fitted")


def _utc_iso(v: Any) -> str:
    """'' | ISO-8601 string | datetime/date (YAML parses unquoted timestamps) | epoch s/ms/ns ->
    normalized ISO-8601 UTC string ('YYYY-MM-DDTHH:MM:SSZ', sub-second kept), '' if empty."""
    if v is None or v == "":
        return ""
    if isinstance(v, datetime):
        dt = v if v.tzinfo is not None else v.replace(tzinfo=timezone.utc)
    elif isinstance(v, date):
        dt = datetime(v.year, v.month, v.day, tzinfo=timezone.utc)
    elif isinstance(v, (int, float)) and not isinstance(v, bool):
        x = float(v)
        sec = x / 1e9 if abs(x) > 1e17 else x / 1e3 if abs(x) > 1e11 else x
        dt = datetime.fromtimestamp(sec, tz=timezone.utc)
    else:
        s = str(v).strip()
        dt = datetime.fromisoformat(s[:-1] + "+00:00" if s.endswith("Z") else s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
    dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S") + (f".{dt.microsecond:06d}" if dt.microsecond else "") + "Z"


def utc_from_ms(ms: int | float | None) -> str:
    """Epoch MILLISECONDS -> ISO-8601 UTC ('' for None); explicit unit (no magnitude guessing)."""
    return "" if ms is None else _utc_iso(datetime.fromtimestamp(float(ms) / 1e3, tz=timezone.utc))


def utc_from_ns(ns: int | None) -> str:
    """Epoch NANOSECONDS -> ISO-8601 UTC ('' for None); explicit unit (no magnitude guessing)."""
    if ns is None:
        return ""
    sec, rem = divmod(int(ns), 1_000_000_000)
    return _utc_iso(datetime.fromtimestamp(sec, tz=timezone.utc).replace(microsecond=rem // 1_000))


@dataclass(frozen=True)
class ParamProvenance:
    """Where a fitted parameter set comes from (look-ahead guard, docs/research/EXPERIMENTS_RUNBOOK.md
    section 2a). Replays label the set against their evaluation window exactly like the fair-value
    parameters (dh.research.replay_env.provenance_status):

      status           'prior'  = placeholder / [ESTIMATE] value never fitted on data: out-of-sample
                                  for every window, labelled "prior";
                       'fitted' = estimated on data in [fitted_from_utc, fitted_to_utc): IN-SAMPLE for a
                                  window starting before fitted_to_utc; a fitted set without
                                  fitted_to_utc has an unknown fitting window (treated as in-sample)
      fitted_from_utc  first datum used (ISO-8601 UTC; '' = unknown)
      fitted_to_utc    every datum used precedes this time (ISO-8601 UTC)
      dataset_id       dataset id and/or content hash of the fitting data
      method           fit method
    """

    status: str = "prior"
    fitted_from_utc: str = ""
    fitted_to_utc: str = ""
    dataset_id: str = ""
    method: str = ""
    note: str = ""

    def __post_init__(self) -> None:
        if self.status not in PROVENANCE_STATUSES:
            raise ValueError(f"provenance status must be one of {PROVENANCE_STATUSES}, not {self.status!r}")
        for k in ("fitted_from_utc", "fitted_to_utc"):
            object.__setattr__(self, k, _utc_iso(getattr(self, k)))

    @property
    def is_prior(self) -> bool:
        return self.status == "prior"

    @staticmethod
    def _ns(s: str) -> int | None:
        if not s:
            return None
        dt = datetime.fromisoformat(s[:-1] + "+00:00" if s.endswith("Z") else s)
        return int(dt.timestamp()) * 1_000_000_000 + dt.microsecond * 1_000

    @property
    def fitted_from_ns(self) -> int | None:
        return self._ns(self.fitted_from_utc)

    @property
    def fitted_to_ns(self) -> int | None:
        return self._ns(self.fitted_to_utc)

    def as_dict(self) -> dict[str, str]:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict[str, Any] | None) -> "ParamProvenance":
        d = dict(d or {})
        return cls(**{f.name: d[f.name] for f in fields(cls) if f.name in d and d[f.name] is not None})

    @classmethod
    def fitted(cls, fitted_from: Any, fitted_to: Any, *, dataset_id: str = "", method: str = "",
               note: str = "") -> "ParamProvenance":
        return cls("fitted", _utc_iso(fitted_from), _utc_iso(fitted_to), dataset_id, method, note)


PRIOR_METHOD = "M1 parametric placeholder (docs/BUILD_PLAN.md D: [ESTIMATE])"


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
    # $ per order, used only when no exact order-fee function is available (the MarketMaker
    # prices the exact single-fill fee incl. rounding): ~half the 1c balance precision
    expected_rounding_per_order: float = 0.005
    min_tau_s: float = 90.0  # no new near-strike quotes after T - min_tau_s
    z_min_final: float = 2.5  # |z| required to quote inside min_tau_s
    # a BRTI print of the settlement window is missing (WindowState.n_missing > 0): incomplete
    # data resolves No, so only markets whose fair-value band stays at or below this YES
    # probability are still quoted (dh.settlement.window module doc)
    window_gap_max_yes_p: float = 0.02
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
    # fitting window of the values above (look-ahead guard); default: a prior never fitted on data
    provenance: ParamProvenance = field(default_factory=lambda: ParamProvenance(method=PRIOR_METHOD))


@dataclass(frozen=True)
class AdverseSelCfg:
    a0: float = 0.002  # $ per contract baseline adverse selection (0.2c)
    b_momentum: float = 0.5  # fraction of the recent adverse fair-value move expected to continue
    c_final: float = 0.01  # extra $ per contract within the final 120 s
    lookback_s: float = 2.0  # window for the recent fair-value move
    behind_mult: float = 2.0  # sweep fills (quotes behind the touch) are more toxic (Experiment 4)
    horizon_s: float = 60.0
    # fitting window of the values above (look-ahead guard); default: a prior never fitted on data
    provenance: ParamProvenance = field(default_factory=lambda: ParamProvenance(method=PRIOR_METHOD))


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
    # runner-emitted FeedStatus streams (audit live M1/M3): consumer/data lag ('stale' ->
    # cancel all + no quoting until 'resumed') and own-activity reconciliation after a
    # reconnect ('stale' = reconciling ... 'resynced')
    lag_stream: str = "runner.lag"
    reconcile_stream: str = "kalshi.reconcile"
    clock_stream: str = "runner.clock"  # receive clock offset beyond the runner's limit
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
