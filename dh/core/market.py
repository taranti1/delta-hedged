"""Static market specification: strike semantics, settlement window, tick grid.

Settlement (Kalshi crypto contract terms; asyncapi `cfbenchmarks_value` averaging notes):
  The expiration value is the simple average of the CF Benchmarks Real-Time Index (BRTI for
  BTC) over the 60 seconds before the expiration time T.  We model it as the mean of
  ``n_obs`` once-per-second prints stamped at T-59s, ..., T-1s, T  (window (T-60s, T]:
  start-boundary tick excluded, close tick included — the convention Kalshi documents for
  `last_60s_windowed_average_15min`).  This convention MUST be verified against settled
  markets' `expiration_value` (dh.research.settlement_check) before trading size.

Strike semantics (openapi Market.strike_type; floor/cap = min/max expiration value for YES):
  greater:            YES iff v >  floor_strike
  greater_or_equal:   YES iff v >= floor_strike
  less:               YES iff v <  cap_strike
  less_or_equal:      YES iff v <= cap_strike
  between:            YES iff floor_strike <= v <= cap_strike
Other strike types are rejected (the strategy does not trade them).
"""

from __future__ import annotations

from dataclasses import dataclass, field

from dh.core.units import NS_PER_S, PX_SCALE

SUPPORTED_STRIKE_TYPES = ("greater", "greater_or_equal", "less", "less_or_equal", "between")


@dataclass(frozen=True, slots=True)
class SettlementSpec:
    index_id: str = "BRTI"
    n_obs: int = 60
    step_ns: int = NS_PER_S
    # Observation k (1..n_obs) is stamped at T - (n_obs - k) * step. True => window (T-60, T].
    include_close_tick: bool = True

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
    expiration_ts: int  # ns: settlement reference time T (expected_expiration_time)
    settlement: SettlementSpec = field(default_factory=SettlementSpec)
    price_ranges: tuple[PriceRange, ...] = (PriceRange(100, 9900, 100),)
    fee_type: str = ""  # resolved from series/event at runtime; '' = unresolved
    fee_multiplier: float = 1.0
    title: str = ""
    # the fee WITHOUT any event override (series, else market): what applies again when an
    # override is cleared. '' = same as fee_type (no override known at construction)
    base_fee_type: str = ""
    base_fee_multiplier: float | None = None

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
