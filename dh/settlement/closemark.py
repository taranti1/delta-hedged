"""Marks for a position held between a market's close and its result (day P&L, loss limits).

After ``close_time`` T a KXBTC* market no longer trades, but its outcome is already FIXED: the
settlement window [T-60 s, T) is complete one second before the close
(docs/research/M1_2_SETTLEMENT_CHECK.md s.5). Kalshi publishes the result later: KXBTC15M
``determined`` about 1 s after the close, KXBTCD / KXBTC about 90 s (up to ~5 min) after it;
settlement can take much longer (p99 32 min, max 24 h). Meanwhile Kalshi's quotes say nothing:
the book is emptied (the ticker shows 0 / 1.00) and REST ``yes_bid`` / ``yes_ask`` stay stale
pre-close quotes for up to ~9 s. The mark of such a position is, in order:

  1. a result exists (WS ``determined`` / lifecycle result, REST ``result``) -> the payout
     (``result_ws`` / ``result_rest``; the WS message comes first);
  2. no result yet -> the EXACT payout from our own BRTI prints of the window
     (``own_benchmark``): the 60 prints stamped T-60 s .. T-1 s (``SettlementSpec.obs_times``),
     averaged and rounded half up to cents like the published expiration value, compared with
     the strike per the market's strike type (``MarketSpec.yes_wins``);
     * a print of the window is missing (a later print exists but not this one: the contract
       resolves "No" on incomplete data, and our feed may simply have missed it), or the rounded
       value is within $0.01 of a strike (the rounding rule changed once; a one-cent
       disagreement decides the outcome there) -> the WORST case for our side (``worst_case``:
       long YES $0, short YES $1);
  3. the window cannot be evaluated at all (prints not received yet, or the window lies before
     the first print we hold, e.g. a failed start-up back-fill) -> the last trade
     (``last_trade``), else the worst case.
REST bid / ask are never used after ``close_time`` (they are the pre-close book).

Everything here is a pure function of the spec, the prints (a ``SettlementTracker``) and the
position: deterministic, so a replay of the recorded prints and lifecycle messages reproduces
every mark.
"""

from __future__ import annotations

import math
from dataclasses import dataclass

from dh.core.market import MarketSpec
from dh.core.units import PX_SCALE
from dh.settlement.window import SettlementTracker

# mark sources (metrics label ``source``, log field ``source``)
RESULT_WS = "result_ws"  # the WS lifecycle `determined` / `settled` message (or a Settlement event)
RESULT_REST = "result_rest"  # REST Market.result
OWN_BENCHMARK = "own_benchmark"  # the exact payout from our own BRTI prints of the window
LAST_TRADE = "last_trade"  # the market's last trade (the window could not be evaluated)
WORST_CASE = "worst_case"  # long YES $0, short YES $1
EXCHANGE_QUOTE = "exchange_quote"  # before the close only: REST YES bid (long) / ask (short)
MODEL = "model"  # before the close only: the strategy's fair value
CLOSE_SOURCES = (RESULT_WS, RESULT_REST, OWN_BENCHMARK, LAST_TRADE, WORST_CASE)

NEAR_STRIKE_USD = 0.01  # a rounded value this close to a strike is not trusted (worst case)
_EPS = 1e-9


@dataclass(frozen=True, slots=True)
class WindowOutcome:
    """What our prints say about a closed market's settlement window.

    status      'yes' | 'no'   decided (every print present, clear of the strikes)
                'near_strike'  every print present, rounded value within $0.01 of a strike
                'missing'      some print is missing (a later print exists): incomplete data
                'unavailable'  some print is not known yet / not held (cannot be evaluated)
    value       the rounded expiration value (every print present), else None
    n_prints    prints found (of ``n_obs``)
    n_missing   prints declared missing
    n_pending   prints not received yet (no later print either): may still arrive
    n_before    prints older than the first print we hold (never arriving here)
    """

    status: str
    value: float | None = None
    n_obs: int = 0
    n_prints: int = 0
    n_missing: int = 0
    n_pending: int = 0
    n_before: int = 0
    detail: str = ""

    @property
    def decided(self) -> bool:
        return self.status in ("yes", "no")

    @property
    def final(self) -> bool:
        """More prints cannot change it (only a pending print can still arrive)."""
        return self.status != "unavailable" or self.n_pending == 0


@dataclass(frozen=True, slots=True)
class CloseMark:
    """(YES price, source) a closed market's position is valued at; ``px`` in 1e-4 $."""

    px: int
    source: str
    detail: str = ""
    value: float | None = None

    def as_log(self) -> dict:
        d = {"px": self.px, "source": self.source, "detail": self.detail}
        if self.value is not None:
            d["value"] = round(self.value, 6)
        return d


def worst_case_px(q: float) -> int:
    """The worst YES price for a position of signed size ``q``: long -> $0, short -> $1."""
    return 0 if q > 0 else PX_SCALE


def _strikes(spec: MarketSpec) -> list[float]:
    st = spec.strike_type
    out: list[float] = []
    if st in ("greater", "greater_or_equal", "between") and spec.floor_strike is not None:
        out.append(float(spec.floor_strike))
    if st in ("less", "less_or_equal", "between") and spec.cap_strike is not None:
        out.append(float(spec.cap_strike))
    return out


def expiration_value(spec: MarketSpec, prints: list[float]) -> float:
    """The published expiration value for a complete window of ``prints`` ($): the average,
    rounded half up per ``spec.settlement.round_decimals``. Two-decimal prints (every CF print)
    are summed in integer cents, so a half-cent tie rounds up exactly."""
    n = len(prints)
    d = spec.settlement.round_decimals
    if d == 2:
        cents = [round(v * 100) for v in prints]
        if all(abs(v * 100 - c) < 1e-6 for v, c in zip(prints, cents, strict=True)):
            s = sum(cents)
            return ((2 * s + n) // (2 * n)) / 100  # half up on integer cents
    return spec.settlement.round_value(math.fsum(prints) / n)


def near_strike(spec: MarketSpec, value: float) -> bool:
    """``value`` (a rounded expiration value) is within $0.01 of one of the market's strikes
    (equality included: the `greater_or_equal` KXBTC15M tie)."""
    return any(abs(value - k) <= NEAR_STRIKE_USD + _EPS for k in _strikes(spec))


def evaluate_window(spec: MarketSpec, tracker: SettlementTracker | None, *, first_src_ns: int | None = None) -> WindowOutcome:
    """Evaluate ``spec``'s settlement window from the prints in ``tracker`` (module docstring).

    A print counts only when the tracker resolves it exactly ('exact' 1 Hz or 'exact5' 5 Hz);
    a carried-forward / skipped print is missing, unless its second lies before the first print
    the tracker holds (``first_src_ns``, default ``tracker.first_src_ns``): we never saw that
    part of the stream, which says nothing about the benchmark."""
    obs = spec.settlement.obs_times(spec.expiration_ts)
    if tracker is None:
        return WindowOutcome("unavailable", n_obs=len(obs), n_before=len(obs), detail="no benchmark prints")
    first = tracker.first_src_ns if first_src_ns is None else int(first_src_ns)
    prints: list[float] = []
    missing: list[int] = []
    pending = before = 0
    for t in obs:
        status, v = tracker.print_for(t)
        if status in ("exact", "exact5") and v is not None and math.isfinite(v):
            prints.append(float(v))
        elif first < 0 or t < first:
            before += 1
        elif status == "pending":
            pending += 1
        else:
            missing.append(t)
    n = len(obs)
    if missing:
        return WindowOutcome("missing", None, n, len(prints), len(missing), pending, before,
                             f"{len(missing)} of {n} prints missing (first at {missing[0] // 10**9} s)")
    if pending or before:
        why = []
        if pending:
            why.append(f"{pending} not received yet")
        if before:
            why.append(f"{before} before the first print held")
        return WindowOutcome("unavailable", None, n, len(prints), 0, pending, before,
                             f"{len(prints)} of {n} prints ({', '.join(why)})")
    value = expiration_value(spec, prints)
    if near_strike(spec, value):
        return WindowOutcome("near_strike", value, n, n, detail=f"expiration value {value:.2f} within "
                             f"${NEAR_STRIKE_USD:.2f} of strike {', '.join(f'{k:.2f}' for k in _strikes(spec))}")
    yes = spec.yes_wins(value)
    return WindowOutcome("yes" if yes else "no", value, n, n,
                         detail=f"expiration value {value:.2f} ({spec.strike_type} "
                                f"{', '.join(f'{k:.2f}' for k in _strikes(spec))}) -> {'yes' if yes else 'no'}")


def close_mark(q: float, outcome: WindowOutcome | None, *, last_trade_px: int | None = None) -> CloseMark:
    """The mark of a position of signed size ``q`` in a CLOSED market without a result (rules
    2 and 3 of the module docstring). A result, when there is one, is the payout instead."""
    if outcome is not None and outcome.decided:
        return CloseMark(PX_SCALE if outcome.status == "yes" else 0, OWN_BENCHMARK, outcome.detail, outcome.value)
    if outcome is not None and outcome.status in ("missing", "near_strike"):
        return CloseMark(worst_case_px(q), WORST_CASE, f"{outcome.status}: {outcome.detail}", outcome.value)
    why = outcome.detail if outcome is not None else "no benchmark prints"
    if last_trade_px is not None and 0 < int(last_trade_px) < PX_SCALE:
        return CloseMark(int(last_trade_px), LAST_TRADE, f"window not evaluable ({why})")
    return CloseMark(worst_case_px(q), WORST_CASE, f"window not evaluable ({why}) and no last trade")


__all__ = ["CLOSE_SOURCES", "EXCHANGE_QUOTE", "LAST_TRADE", "MODEL", "NEAR_STRIKE_USD", "OWN_BENCHMARK", "RESULT_REST",
           "RESULT_WS", "WORST_CASE", "CloseMark", "WindowOutcome", "close_mark", "evaluate_window", "expiration_value",
           "near_strike", "worst_case_px"]
