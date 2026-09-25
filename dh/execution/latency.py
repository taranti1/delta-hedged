"""Seeded latency model for the simulated Kalshi exchange and hedge venue.

All distributions are specified in **milliseconds** and sampled into integer **nanoseconds**.

Kinds of delay (each has its own independent, seeded random stream, so that e.g. a different
number of fill messages under two fill policies never shifts the submit/cancel latencies of
later orders -- this is what makes cross-policy comparisons pathwise):

  submit    decision -> order request reaches the matching engine
  cancel    decision -> cancel/amend/decrease request reaches the matching engine
  response  matching engine -> REST response (OrderAck / OrderReject / CancelAck) received
  ws        matching engine -> private WebSocket message (KalshiFill / KalshiOrderUpdate) received
  md        exchange event -> our receipt of the public market-data message

Market-data latency enters the simulator only as a **constant offset** (``md_offset_ns``, the
median of the ``md`` distribution times the multiplier): recorded market events carry our local
receive time, so an event recorded at ``ts`` happened at the exchange at ``ts - md_offset``.
A constant offset keeps the recorded event order intact; md jitter should be folded into the
submit/cancel distributions (see docs/EXECUTION_MODEL.md).

Policy C (conservative) scales every delay by a multiplier (default 1.5 in the simulator).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

import numpy as np

from dh.core.units import NS_PER_MS

KINDS: tuple[str, ...] = ("submit", "cancel", "response", "ws", "md")


class Dist:
    """A non-negative delay distribution in milliseconds."""

    def sample(self, rng: np.random.Generator) -> float:  # pragma: no cover - interface
        raise NotImplementedError

    def median(self) -> float:  # pragma: no cover - interface
        raise NotImplementedError


@dataclass(frozen=True, slots=True)
class Fixed(Dist):
    """Deterministic delay of ``ms`` milliseconds."""

    ms: float

    def __post_init__(self) -> None:
        if self.ms < 0:
            raise ValueError("latency must be >= 0 ms")

    def sample(self, rng: np.random.Generator) -> float:
        return float(self.ms)

    def median(self) -> float:
        return float(self.ms)


@dataclass(frozen=True, slots=True)
class LogNormal(Dist):
    """Lognormal delay: ``median_ms * exp(sigma * Z)``, clipped to [floor_ms, cap_ms]."""

    median_ms: float
    sigma: float = 0.5
    floor_ms: float = 0.0
    cap_ms: float | None = None

    def __post_init__(self) -> None:
        if self.median_ms <= 0 or self.sigma < 0 or self.floor_ms < 0:
            raise ValueError("invalid lognormal latency parameters")
        if self.cap_ms is not None and self.cap_ms < self.floor_ms:
            raise ValueError("cap_ms < floor_ms")

    def _clip(self, x: float) -> float:
        x = max(self.floor_ms, x)
        return x if self.cap_ms is None else min(self.cap_ms, x)

    def sample(self, rng: np.random.Generator) -> float:
        return self._clip(self.median_ms * float(np.exp(self.sigma * rng.standard_normal())))

    def median(self) -> float:
        return self._clip(self.median_ms)


@dataclass(frozen=True, slots=True)
class Empirical(Dist):
    """Resample (with replacement) from measured delays, e.g. live REST round-trips in ms."""

    samples_ms: tuple[float, ...]

    def __init__(self, samples_ms: Sequence[float]) -> None:
        vals = tuple(float(x) for x in samples_ms)
        if not vals:
            raise ValueError("empirical latency needs at least one sample")
        if min(vals) < 0:
            raise ValueError("latency samples must be >= 0 ms")
        object.__setattr__(self, "samples_ms", vals)

    def sample(self, rng: np.random.Generator) -> float:
        return self.samples_ms[int(rng.integers(len(self.samples_ms)))]

    def median(self) -> float:
        return float(np.median(np.asarray(self.samples_ms)))


def _as_dist(x: Dist | float | int) -> Dist:
    if isinstance(x, Dist):
        return x
    return Fixed(float(x))


class LatencyModel:
    """Seeded sampler of submit / cancel / response / ws / md delays (ns).

    Parameters are ``Dist`` objects or plain numbers (fixed ms). ``cancel`` defaults to the
    ``submit`` distribution. ``multiplier`` scales every sampled delay (policy C stress).
    Deterministic: two models with equal parameters and seed produce identical sequences
    per kind, independent of how calls to the other kinds interleave.
    """

    __slots__ = ("dists", "multiplier", "entropy", "_rng")

    DEFAULTS: dict[str, Dist] = {
        # PLACEHOLDERS until measured live (docs/EXECUTION_MODEL.md, "Calibration").
        "submit": LogNormal(30.0, 0.4),
        "response": LogNormal(30.0, 0.4),
        "ws": LogNormal(10.0, 0.5),
        "md": Fixed(0.0),
    }

    def __init__(
        self,
        seed: int | Sequence[int] = 0,
        *,
        submit: Dist | float | None = None,
        cancel: Dist | float | None = None,
        response: Dist | float | None = None,
        ws: Dist | float | None = None,
        md: Dist | float | None = None,
        multiplier: float = 1.0,
    ) -> None:
        if multiplier <= 0:
            raise ValueError("multiplier must be > 0")
        sub = _as_dist(submit) if submit is not None else self.DEFAULTS["submit"]
        self.dists: dict[str, Dist] = {
            "submit": sub,
            "cancel": _as_dist(cancel) if cancel is not None else sub,
            "response": _as_dist(response) if response is not None else self.DEFAULTS["response"],
            "ws": _as_dist(ws) if ws is not None else self.DEFAULTS["ws"],
            "md": _as_dist(md) if md is not None else self.DEFAULTS["md"],
        }
        self.multiplier = float(multiplier)
        self.entropy: tuple[int, ...] = (int(seed),) if isinstance(seed, (int, np.integer)) else tuple(
            int(s) for s in seed
        )
        children = np.random.SeedSequence(list(self.entropy)).spawn(len(KINDS))
        self._rng = {k: np.random.default_rng(c) for k, c in zip(KINDS, children)}

    # ------------------------------------------------------------------ constructors
    @classmethod
    def zero(cls, seed: int = 0) -> "LatencyModel":
        """All delays exactly 0 ns (unit tests, idealized studies)."""
        return cls(seed, submit=0.0, cancel=0.0, response=0.0, ws=0.0, md=0.0)

    @classmethod
    def fixed(cls, submit_ms: float, response_ms: float, ws_ms: float, *, cancel_ms: float | None = None,
              md_ms: float = 0.0, seed: int = 0) -> "LatencyModel":
        """Deterministic delays (ms)."""
        return cls(seed, submit=submit_ms, cancel=submit_ms if cancel_ms is None else cancel_ms,
                   response=response_ms, ws=ws_ms, md=md_ms)

    def fork(self, seed: int | None = None, multiplier: float | None = None) -> "LatencyModel":
        """A fresh model with the same distributions and fresh streams.

        The new entropy is ``self.entropy + (seed,)`` so forks are reproducible; the original
        model's streams are untouched (three simulators forked from one model with the same
        seed sample identical latencies).
        """
        ent = self.entropy + ((int(seed),) if seed is not None else ())
        d = self.dists
        return LatencyModel(ent, submit=d["submit"], cancel=d["cancel"], response=d["response"], ws=d["ws"],
                            md=d["md"], multiplier=self.multiplier if multiplier is None else multiplier)

    # ------------------------------------------------------------------ sampling (ns)
    def sample_ns(self, kind: str) -> int:
        """One delay of ``kind`` in integer ns (>= 0), multiplier applied."""
        ms = self.dists[kind].sample(self._rng[kind]) * self.multiplier
        return max(0, int(round(ms * NS_PER_MS)))

    def submit_ns(self) -> int:
        return self.sample_ns("submit")

    def cancel_ns(self) -> int:
        return self.sample_ns("cancel")

    def response_ns(self) -> int:
        return self.sample_ns("response")

    def ws_ns(self) -> int:
        return self.sample_ns("ws")

    def md_ns(self) -> int:
        return self.sample_ns("md")

    @property
    def md_offset_ns(self) -> int:
        """Constant exchange-time -> receive-time offset used to align recorded events (ns)."""
        return max(0, int(round(self.dists["md"].median() * self.multiplier * NS_PER_MS)))

    def __repr__(self) -> str:  # pragma: no cover - debug aid
        return f"LatencyModel(entropy={self.entropy}, x{self.multiplier:g}, {self.dists})"
