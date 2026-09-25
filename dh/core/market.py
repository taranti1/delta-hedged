"""Static market specification: strike semantics, settlement window, tick grid.

Settlement (Kalshi crypto contract terms; VERIFIED convention, see dh.settlement.convention and
docs/research/M1_2_SETTLEMENT_CHECK.md):
  T = the market's ``close_time`` (NOT ``expected_expiration_time``, which is close + 5 min on
  every KXBTC* market and is kept only as metadata in ``MarketSpec.expected_expiration_ts``).
  The expiration value is the simple average of the CF Benchmarks Real-Time Index (BRTI for BTC)
  over the 60 seconds before T, modelled as the mean of ``n_obs`` once-per-second prints stamped
  at T-59s, ..., T-1s, T (window (T-60s, T]: start-boundary tick excluded, close tick included),
  ROUNDED to cents (``SettlementSpec.round_decimals``) before it is compared with the strike.
  Missing or incomplete benchmark data resolves the market No (contract terms).

Strike semantics (openapi Market.strike_type; floor/cap = min/max expiration value for YES),
applied to the ROUNDED expiration value v:
  greater:            YES iff v >  floor_strike
  greater_or_equal:   YES iff v >= floor_strike   (KXBTC15M: floor = previous quarter's value)
  less:               YES iff v <  cap_strike
  less_or_equal:      YES iff v <= cap_strike
  between:            YES iff floor_strike <= v <= cap_strike
Other strike types are rejected (the strategy does not trade them). For the continuous pricing
model the rounded comparison is equivalent to comparing the UNROUNDED average with the shifted
thresholds of ``MarketSpec.settle_thresholds`` (e.g. greater K -> average >= K + 0.005).
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field

import numpy as np

from dh.core.units import NS_PER_S, PX_SCALE

SUPPORTED_STRIKE_TYPES = ("greater", "greater_or_equal", "less", "less_or_equal", "between")
_GRID_TOL = 1e-6  # a strike within this many grid units of the rounding grid is ON the grid


def round_half_up(v: float, decimals: int = 2) -> float:
    """Round like Kalshi's published expiration value: to ``decimals`` places, halves up.

    A 1e-6-unit tolerance absorbs binary noise so that an exact half (a sum of 2-decimal prints
    divided by 60, computed in floating point) still rounds up. Returns the correctly rounded
    double (e.g. 92799.99 == float("92799.99")).
    """
    scale = 10.0**decimals
    return math.floor(v * scale + 0.5 + 1e-6) / scale


def _grid_units(K: float, scale: float) -> tuple[int, bool]:
    """(nearest grid index, is K on the grid) for strike K on a 1/scale grid."""
    x = K * scale
    r = round(x)
    return int(r), abs(x - r) <= _GRID_TOL


@dataclass(frozen=True, slots=True)
class SettlementSpec:
    index_id: str = "BRTI"
    n_obs: int = 60
    step_ns: int = NS_PER_S
    # Observation k (1..n_obs) is stamped at T - (n_obs - k) * step. True => window (T-60, T].
    include_close_tick: bool = True
    # The published expiration value is the average rounded half-up to this many decimals: 2 on
    # KXBTCD, KXBTC, KXBTC15M (dh.kalshi.normalize.default_settlement sets it for every real
    # market); None = compare the unrounded average (synthetic / analytic tests).
    round_decimals: int | None = None

    def round_value(self, v: float) -> float:
        """The published expiration value for an average v (rounded per ``round_decimals``)."""
        return v if self.round_decimals is None or not math.isfinite(v) else round_half_up(v, self.round_decimals)

    def obs_times(self, expiration_ns: int) -> list[int]:
        """Source timestamps (ns) of the settlement observations, ascending."""
        last = expiration_ns if self.include_close_tick else expiration_ns - self.step_ns
        return [last - (self.n_obs - k) * self.step_ns for k in range(1, self.n_obs + 1)]

    def window_start_ns(self, expiration_ns: int) -> int:
        """Earliest observation timestamp in the window."""
        return self.obs_times(expiration_ns)[0]


@dataclass(frozen=True, slots=True)
class PriceRange:
    start_px: int  # inclusive
    end_px: int  # inclusive
    step_px: int


@dataclass(frozen=True, slots=True)
class MarketSpec:
    ticker: str
    event_ticker: str
    series_ticker: str
    strike_type: str
    floor_strike: float | None
    cap_strike: float | None
    open_ts: int  # ns
    close_ts: int  # ns: trading stops
    expiration_ts: int  # ns: settlement reference time T (= close_time for KXBTC*; window (T-60 s, T])
    settlement: SettlementSpec = field(default_factory=SettlementSpec)
    price_ranges: tuple[PriceRange, ...] = (PriceRange(100, 9900, 100),)
    fee_type: str = ""  # resolved from series/event at runtime; '' = unresolved
    fee_multiplier: float = 1.0
    title: str = ""
    # the fee WITHOUT any event override (series, else market): what applies again when an
    # override is cleared. '' = same as fee_type (no override known at construction)
    base_fee_type: str = ""
    base_fee_multiplier: float | None = None
    # metadata only: Kalshi's expected_expiration_time (close + 5 min on KXBTC*), 0 = unknown.
    # NEVER the settlement reference time (that is expiration_ts).
    expected_expiration_ts: int = 0

    @property
    def base_fee(self) -> tuple[str, float]:
        """(fee_type, multiplier) without event overrides (falls back to the effective fee)."""
        if self.base_fee_type:
            mult = self.base_fee_multiplier
            return self.base_fee_type, (1.0 if mult is None else float(mult))
        return self.fee_type, self.fee_multiplier

    def __post_init__(self) -> None:
        if self.strike_type not in SUPPORTED_STRIKE_TYPES:
            raise ValueError(f"unsupported strike_type {self.strike_type!r} for {self.ticker}")
        if self.strike_type in ("greater", "greater_or_equal") and self.floor_strike is None:
            raise ValueError(f"{self.ticker}: floor_strike required")
        if self.strike_type in ("less", "less_or_equal") and self.cap_strike is None:
            raise ValueError(f"{self.ticker}: cap_strike required")
        if self.strike_type == "between" and (self.floor_strike is None or self.cap_strike is None):
            raise ValueError(f"{self.ticker}: floor and cap required for between")

    # -------------------------------------------------------------- payoff
    def yes_wins(self, v: float) -> bool:
        """Outcome for a settlement AVERAGE v: v is first rounded like the published expiration
        value (``SettlementSpec.round_decimals``), then compared per the strike type."""
        v = self.settlement.round_value(v)
        st = self.strike_type
        if st == "greater":
            return v > self.floor_strike  # type: ignore[operator]
        if st == "greater_or_equal":
            return v >= self.floor_strike  # type: ignore[operator]
        if st == "less":
            return v < self.cap_strike  # type: ignore[operator]
        if st == "less_or_equal":
            return v <= self.cap_strike  # type: ignore[operator]
        return self.floor_strike <= v <= self.cap_strike  # type: ignore[operator]

    def yes_wins_vec(self, A: np.ndarray) -> np.ndarray:
        """Vectorized ``yes_wins`` (bool array) for settlement averages A (same rounding)."""
        A = np.asarray(A, dtype=np.float64)
        lo, hi = self.settle_thresholds
        if self.settlement.round_decimals is None:
            st = self.strike_type
            if st == "greater":
                return A > lo
            if st == "greater_or_equal":
                return A >= lo
            if st == "less":
                return A < hi
            if st == "less_or_equal":
                return A <= hi
            return (A >= lo) & (A <= hi)
        # rounded: YES iff lo <= A < hi on the shifted thresholds (exact up to the tie tolerance)
        yes = np.ones(A.shape, dtype=bool)
        if lo is not None:
            yes &= A >= lo - 1e-9
        if hi is not None:
            yes &= A < hi - 1e-9
        return yes

    @property
    def settle_thresholds(self) -> tuple[float | None, float | None]:
        """(lo, hi) thresholds on the UNROUNDED average: YES iff lo <= average < hi (None = open).

        With rounding to a grid h = 10**-d (half up): greater K -> lo = g - h/2 for the smallest
        grid value g > K; greater_or_equal -> smallest g >= K; less K -> hi = g + h/2 for the
        largest g < K; less_or_equal -> largest g <= K; between = both (inclusive ends). E.g.
        KXBTCD "above 92799.99" -> average >= 92799.995; KXBTC15M "at least 83950.62" -> average
        >= 83950.615. Without rounding the raw strikes are returned (strictness then lives in
        ``yes_wins``). The continuous pricing model uses these (``dh.models.fairvalue``).
        """
        d = self.settlement.round_decimals
        st = self.strike_type
        F, C = self.floor_strike, self.cap_strike
        if d is None:
            return (F if st in ("greater", "greater_or_equal", "between") else None,
                    C if st in ("less", "less_or_equal", "between") else None)
        scale = 10.0**d
        # every threshold is (j - 1/2) / scale for the first grid index j at which YES starts
        # (lo) or stops (hi), computed the same way so that complementary strikes (greater K /
        # less_or_equal K, greater_or_equal K / less K) share bit-identical thresholds
        lo = hi = None
        if st in ("greater", "greater_or_equal", "between"):
            n, on = _grid_units(float(F), scale)  # type: ignore[arg-type]
            if on:
                j = n + 1 if st == "greater" else n
            else:
                j = math.floor(float(F) * scale) + 1  # type: ignore[arg-type]  # smallest grid value > F
            lo = (j - 0.5) / scale
        if st in ("less", "less_or_equal", "between"):
            n, on = _grid_units(float(C), scale)  # type: ignore[arg-type]
            if on:
                g = n - 1 if st == "less" else n  # largest grid value that still pays YES
            else:
                g = math.floor(float(C) * scale)  # type: ignore[arg-type]  # largest grid value < C
            hi = (g + 1 - 0.5) / scale
        return lo, hi

    def breakpoints(self) -> list[float]:
        """Settlement averages where the YES payoff jumps (the ``settle_thresholds``)."""
        return [x for x in self.settle_thresholds if x is not None]

    @property
    def is_upper_tail(self) -> bool:
        """YES pays when the average is ABOVE a threshold (delta > 0 in BTC)."""
        return self.strike_type in ("greater", "greater_or_equal")

    # -------------------------------------------------------------- ticks
    def is_valid_px(self, px: int) -> bool:
        for r in self.price_ranges:
            if r.start_px <= px <= r.end_px and (px - r.start_px) % r.step_px == 0:
                return True
        return False

    def tick_grid(self) -> list[int]:
        out: set[int] = set()
        for r in self.price_ranges:
            out.update(range(r.start_px, r.end_px + 1, r.step_px))
        return sorted(p for p in out if 0 < p < PX_SCALE)

    def next_tick_up(self, px: int) -> int | None:
        grid = self.tick_grid()
        for p in grid:
            if p > px:
                return p
        return None

    def next_tick_down(self, px: int) -> int | None:
        grid = self.tick_grid()
        for p in reversed(grid):
            if p < px:
                return p
        return None

    def seconds_to_expiry(self, now_ns: int) -> float:
        return (self.expiration_ts - now_ns) / NS_PER_S
