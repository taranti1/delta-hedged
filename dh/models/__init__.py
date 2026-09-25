"""dh.models: fair value (digital pricing with exact settlement-window math), tail models,
volatility estimation and forecast calibration metrics.  See docs/MODELS_fairvalue.md."""

from dh.models.fairvalue import (
    Digital,
    DigitalArrays,
    DigitalBand,
    avg_variance_time,
    avg_variance_time_general,
    digital,
    digital_band,
    digital_vec,
    hedge_notional_usd,
    remaining_avg_variance_time,
    remaining_sd,
    sigma_abs_from_log,
)
from dh.models.tails import GAUSS, EmpiricalTail, Gauss, StudentT, TailModel, VolMixture, make_tail

__all__ = [
    "Digital",
    "DigitalArrays",
    "DigitalBand",
    "avg_variance_time",
    "avg_variance_time_general",
    "digital",
    "digital_band",
    "digital_vec",
    "hedge_notional_usd",
    "remaining_avg_variance_time",
    "remaining_sd",
    "sigma_abs_from_log",
    "GAUSS",
    "EmpiricalTail",
    "Gauss",
    "StudentT",
    "TailModel",
    "VolMixture",
    "make_tail",
]
