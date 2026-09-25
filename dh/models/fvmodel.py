"""Production fair-value model: vol forecaster + tail schedule + exact settlement-window pricing.

    fv = FairValueModel.from_config(load_recommended_config())
    fv.update(ts_ns, brti_value)                       # every benchmark / nowcast print
    d = fv.price(spec, ws, spot, now_ns)               # Digital(p_yes, delta, gamma, ...)

The configuration (dh/models/data/fv_recommended.json) is written by the research pipeline
(python -m dh.research.fv_study.run) from the walk-forward study documented in
docs/research/01_fair_value_calibration.md:
  vol   deseasonalized EWMAs (half-lives 10m..1d) blended with horizon-dependent weights,
        times the forward seasonal factor over [now, expiration]
  tail  Student-t with horizon-dependent nu and scale c (sd_used = c * sd_model)
Horizon = seconds from now to the expiration time T (clamped to the fitted knots, whose
shortest is 120 s: inside the final minute the 2-minute parameters are used).
Everything is deterministic and driven only by the timestamps passed in.
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from pathlib import Path

from dh.core.market import MarketSpec
from dh.core.units import NS_PER_S
from dh.models.fairvalue import Digital, digital
from dh.models.tails import GAUSS, StudentT, TailModel
from dh.models.vol import SeasonalVol, VolForecaster, VolForecasterConfig
from dh.settlement.window import WindowState

RECOMMENDED_CONFIG_PATH = Path(__file__).resolve().parent / "data" / "fv_recommended.json"


@dataclass(frozen=True)
class TailSchedule:
    """Tail parameters by pricing horizon: knots (horizon_s, nu, scale), ascending.

    Linear interpolation of nu and scale in the horizon, clamped outside the knots.
    kind 'gauss' ignores nu (scale still applies).
    """

    knots: tuple[tuple[float, float, float], ...] = ((0.0, math.inf, 1.0),)
    kind: str = "student_t"

    def __post_init__(self) -> None:
        hs = [k[0] for k in self.knots]
        if not hs or any(b <= a for a, b in zip(hs, hs[1:])):
            raise ValueError("knots must be non-empty and strictly ascending in horizon")
        if self.kind not in ("student_t", "gauss"):
            raise ValueError("kind must be 'student_t' or 'gauss'")

    def params(self, horizon_s: float) -> tuple[float, float]:
        """(nu, scale) at a horizon (s)."""
        k = self.knots
        if horizon_s <= k[0][0]:
            return k[0][1], k[0][2]
        if horizon_s >= k[-1][0]:
            return k[-1][1], k[-1][2]
        for (h0, n0, c0), (h1, n1, c1) in zip(k, k[1:]):
            if h0 <= horizon_s <= h1:
                a = (horizon_s - h0) / (h1 - h0)
                return (1 - a) * n0 + a * n1, (1 - a) * c0 + a * c1
        return k[-1][1], k[-1][2]  # pragma: no cover

    def at(self, horizon_s: float) -> tuple[TailModel, float]:
        """(tail model, sd scale) at a horizon (s)."""
        nu, c = self.params(horizon_s)
        if self.kind == "gauss" or not math.isfinite(nu):
            return GAUSS, c
        return StudentT(max(nu, 2.05)), c


class FairValueModel:
    """Stateful fair-value engine for one benchmark (feed it prices, ask it for Digitals)."""

    def __init__(self, vol: VolForecaster, tails: TailSchedule | None = None) -> None:
        self.vol = vol
        self.tails = tails or TailSchedule(kind="gauss")

    # ------------------------------------------------------------------ config
    @classmethod
    def from_config(cls, cfg: dict) -> "FairValueModel":
        """Build from the research-generated config dict (see RECOMMENDED_CONFIG_PATH)."""
        v = cfg["vol"]
        wbh = tuple(sorted((float(h), tuple(float(x) for x in w)) for h, w in v["weights_by_horizon_s"].items()))
        hl = tuple(float(h) for h in v["half_lives_s"])
        vcfg = VolForecasterConfig(
            half_lives_s=hl,
            weights=wbh[-1][1],
            weights_by_horizon=wbh,
            min_dt_s=float(v.get("min_dt_s", 60.0)),
            max_dt_s=None if v.get("max_dt_s") is None else float(v["max_dt_s"]),
            sigma_floor=float(v.get("sigma_floor", 0.0)),
            sigma_cap=float(v.get("sigma_cap", math.inf)),
        )
        seasonal = SeasonalVol.from_dict(cfg["seasonal"]) if cfg.get("seasonal") else None
        t = cfg.get("tail", {"kind": "gauss"})
        if t.get("kind", "gauss") == "gauss":
            tails = TailSchedule(kind="gauss")
        else:
            knots = tuple(sorted((float(h), float(p["nu"]), float(p.get("scale", 1.0))) for h, p in t["by_horizon_s"].items()))
            tails = TailSchedule(knots=knots, kind="student_t")
        return cls(VolForecaster(vcfg, seasonal), tails)

    # ------------------------------------------------------------------ streaming
    def update(self, ts_ns: int, price: float) -> None:
        """Feed a benchmark / nowcast print (ts_ns = source time in ns, price in $)."""
        self.vol.update(ts_ns, price)

    @property
    def ready(self) -> bool:
        return self.vol.ready

    # ------------------------------------------------------------------ pricing
    def sigma_abs(self, now_ns: int, expiration_ns: int, spot: float) -> float:
        """$ vol per sqrt(second) for pricing a window expiring at ``expiration_ns``."""
        return self.vol.sigma_abs(now_ns, max(expiration_ns, now_ns), spot)

    def price(
        self,
        spec: MarketSpec,
        ws: WindowState,
        spot: float,
        now_ns: int,
        drift_abs: float = 0.0,
        nowcast_sd: float = 0.0,
    ) -> Digital:
        """Digital for ``spec`` given the settlement window state at ``now_ns``.

        spot: benchmark nowcast ($); nowcast_sd: its error sd vs the true index ($).
        """
        horizon = max(0.0, (spec.expiration_ts - now_ns) / NS_PER_S)
        tail, c = self.tails.at(horizon)
        sig = self.sigma_abs(now_ns, spec.expiration_ts, spot) * c
        return digital(spec, ws, spot, sig, tail, drift_abs, nowcast_sd=nowcast_sd)


def load_recommended_config(path: str | Path | None = None) -> dict:
    """Load the research-generated fair-value config (JSON)."""
    p = Path(path) if path is not None else RECOMMENDED_CONFIG_PATH
    return json.loads(p.read_text())


__all__ = ["FairValueModel", "TailSchedule", "load_recommended_config", "RECOMMENDED_CONFIG_PATH"]
