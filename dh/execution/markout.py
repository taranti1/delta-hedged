"""Fill markouts: how fair value moved after each of our fills.

Signed markout at horizon h, in **cents per contract** (positive = good for us):
    bid (bought YES) : 100 * (fv(t + h) - px)
    ask (sold YES)   : 100 * (px - fv(t + h))
with fv the YES fair value as a probability / dollar price in [0, 1] and px = yes_px / 1e4.
``to settlement`` uses the settlement payout (1.0 YES, 0.0 NO) instead of fv(t + h).
Fees are reported separately (``fee_c``, cents per contract) so gross and net can be studied.

Fair value inputs (``fv``):
  * callable ``fv(ts_ns) -> float``                      (single market)
  * tuple ``(ts_ns_array, values_array)``                 as-of lookup (last value <= t);
                                                          NaN before the first / after the last sample
  * dict ``{ticker: callable | tuple}``                   per market
  * callable with ``per_ticker=True``: ``fv(ticker, ts_ns) -> float``
Time basis: ``'exch'`` uses ``fill.ts_exch`` when non-zero (else ``ts``); ``'recv'`` uses ``ts``.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Mapping, Sequence

import numpy as np

from dh.core.events import KalshiFill
from dh.core.units import MICROS, NS_PER_S, PX_SCALE, QTY_SCALE

DEFAULT_HORIZONS_S: tuple[float, ...] = (0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0)


@dataclass(frozen=True, slots=True)
class FillMarkout:
    ticker: str
    trade_id: str
    client_order_id: str
    ts: int  # fill time used (ns)
    book_side: str
    yes_px: int
    qty: int
    is_taker: bool
    fee_c: float  # fee, cents per contract
    fv_at_fill: float
    horizons_s: tuple[float, ...]
    markouts_c: tuple[float, ...]  # gross signed markout per horizon, cents per contract (NaN = no data)
    settle_c: float  # gross signed markout to settlement, cents per contract (NaN = unknown)

    def net(self, i: int) -> float:
        """Markout at horizon index i net of fees (cents per contract)."""
        return self.markouts_c[i] - self.fee_c


class AsOf:
    """As-of (last value at or before t) lookup over a sorted series; NaN outside its range."""

    __slots__ = ("ts", "vals")

    def __init__(self, ts: Sequence[int] | np.ndarray, vals: Sequence[float] | np.ndarray) -> None:
        self.ts = np.asarray(ts, dtype=np.int64)
        self.vals = np.asarray(vals, dtype=float)
        if self.ts.shape != self.vals.shape or self.ts.ndim != 1:
            raise ValueError("ts and values must be 1-d arrays of equal length")
        if len(self.ts) > 1 and np.any(np.diff(self.ts) < 0):
            raise ValueError("fair-value timestamps must be sorted")

    def __call__(self, t: int | np.ndarray) -> Any:
        t_arr = np.asarray(t, dtype=np.int64)
        if len(self.ts) == 0:
            out = np.full(t_arr.shape, np.nan)
        else:
            idx = np.searchsorted(self.ts, t_arr, side="right") - 1
            ok = (idx >= 0) & (t_arr <= self.ts[-1])
            out = np.where(ok, self.vals[np.clip(idx, 0, len(self.vals) - 1)], np.nan)
        return float(out) if out.ndim == 0 else out


def _lookup(fv: Any) -> Callable[[int], float]:
    if isinstance(fv, tuple) and len(fv) == 2:
        return AsOf(*fv)
    if callable(fv):
        return fv
    raise TypeError("fv must be a callable fv(ts) or a (ts_array, values_array) tuple")


def _resolver(fv: Any, per_ticker: bool) -> Callable[[str, int], float]:
    if isinstance(fv, Mapping):
        table = {k: _lookup(v) for k, v in fv.items()}

        def f(ticker: str, t: int) -> float:
            g = table.get(ticker)
            return math.nan if g is None else float(g(t))

        return f
    if per_ticker:
        return lambda ticker, t: float(fv(ticker, t))
    g = _lookup(fv)
    return lambda ticker, t: float(g(t))


def fee_cents_per_contract(fee_micros: int, qty: int) -> float:
    """Fee in cents per contract for a fill of qty (0.01-contract units)."""
    if qty <= 0:
        return 0.0
    dollars = fee_micros / MICROS
    n_contracts = qty / QTY_SCALE
    return 100.0 * dollars / n_contracts


def compute_markouts(fills: Iterable[KalshiFill], fv: Any, horizons_s: Sequence[float] = DEFAULT_HORIZONS_S, *,
                     settlement: Mapping[str, float] | None = None, time_basis: str = "exch",
                     per_ticker: bool = False) -> list[FillMarkout]:
    """Per-fill signed markouts (cents per contract) at ``horizons_s`` and to settlement.

    settlement: {ticker: YES payout in dollars (1.0 / 0.0)}; missing tickers -> NaN.
    """
    if time_basis not in ("exch", "recv"):
        raise ValueError("time_basis must be 'exch' or 'recv'")
    get = _resolver(fv, per_ticker)
    hs = tuple(float(h) for h in horizons_s)
    out: list[FillMarkout] = []
    for f in fills:
        t = f.ts_exch if (time_basis == "exch" and f.ts_exch) else f.ts
        px = f.yes_px / PX_SCALE
        sign = 1.0 if f.book_side == "bid" else -1.0
        mk = tuple(100.0 * sign * (get(f.ticker, t + int(round(h * NS_PER_S))) - px) for h in hs)
        settle = math.nan
        if settlement is not None and f.ticker in settlement:
            settle = 100.0 * sign * (float(settlement[f.ticker]) - px)
        out.append(FillMarkout(f.ticker, f.trade_id, f.client_order_id, t, f.book_side, f.yes_px, f.qty, f.is_taker,
                               fee_cents_per_contract(f.fee_micros, f.qty), get(f.ticker, t), hs, mk, settle))
    return out


def markout_matrix(markouts: Sequence[FillMarkout]) -> tuple[np.ndarray, np.ndarray]:
    """(M, w): M[i, j] = markout of fill i at horizon j (last column = settlement), w = qty in contracts."""
    if not markouts:
        return np.zeros((0, 0)), np.zeros(0)
    m = np.array([list(x.markouts_c) + [x.settle_c] for x in markouts], dtype=float)
    w = np.array([x.qty / QTY_SCALE for x in markouts], dtype=float)
    return m, w


def summarize(markouts: Sequence[FillMarkout], *, net_of_fees: bool = False) -> dict[str, dict[str, float]]:
    """Contract-weighted mean markout (cents/contract), its standard error and count per
    horizon (keys '0.1s', ..., 'settle'); NaN entries are excluded horizon by horizon."""
    res: dict[str, dict[str, float]] = {}
    if not markouts:
        return res
    m, w = markout_matrix(markouts)
    if net_of_fees:
        fees = np.array([x.fee_c for x in markouts])
        m = m - fees[:, None]
    labels = [f"{h:g}s" for h in markouts[0].horizons_s] + ["settle"]
    for j, lab in enumerate(labels):
        col = m[:, j]
        ok = ~np.isnan(col)
        n = int(ok.sum())
        if n == 0:
            res[lab] = {"mean_c": math.nan, "se_c": math.nan, "n": 0, "contracts": 0.0}
            continue
        ww, xx = w[ok], col[ok]
        mean = float(np.average(xx, weights=ww))
        var = float(np.average((xx - mean) ** 2, weights=ww))
        n_eff = float(ww.sum() ** 2 / (ww ** 2).sum())
        se = math.sqrt(var / n_eff) if n_eff > 1 else math.nan
        res[lab] = {"mean_c": mean, "se_c": se, "n": n, "contracts": float(ww.sum())}
    return res
