"""Experiment 1: is Kalshi stale relative to external BTC? (lead-lag at 100 ms - 5 s)

Build, on a fixed event-time grid (default 100 ms), per market:
    x(t) = fair value implied by the EXTERNAL composite (benchmark nowcast) at t
    y(t) = Kalshi mid (or microprice) at t, from the reconstructed L2 book
Then, pooling markets, estimate for horizons h in {0.1, 0.25, 0.5, 1, 2, 5} s:

    y(t+h) - y(t) = a + b_h * (x(t) - y(t)) + e          ("gap closure")
    y(t+h) - y(t) = a + c_h * (x(t) - x(t-L)) + e         (response to a recent external move)

b_h rising toward 1 with h measures how fast Kalshi closes a gap to external fair value;
the horizon where b_h reaches 0.5 is the empirical half-life. Economic size = the average gap
|x - y| at t conditional on |recent move| > k * sigma, in ticks. Inference: block bootstrap by
event (markets in one event share one BTC path).

Validated on the synthetic market with a known maker lag (tests/research/test_exp1.py).
"""

from __future__ import annotations

import math
from dataclasses import dataclass

import numpy as np
import pandas as pd
from scipy.special import ndtr

from dh.core.book import ExtBook, KalshiBook
from dh.core.events import ExtBBO, ExtBookDelta, ExtBookSnapshot, IndexTick, KalshiBookDelta, KalshiBookSnapshot
from dh.core.market import MarketSpec
from dh.core.units import NS_PER_MS, NS_PER_S, PX_SCALE

SEC_YR = 365.0 * 24 * 3600
HORIZONS_S = (0.1, 0.25, 0.5, 1.0, 2.0, 5.0)


def simple_digital(S: float, spec: MarketSpec, now_ns: int, vol_ann: float) -> float:
    """Point fair value for research grids (averaging window, Gaussian, no fixed prints)."""
    secs = (spec.expiration_ts - now_ns) / NS_PER_S
    var_t = max(secs - 59.0, 0.0) + 19.5025 if secs > 60 else max(secs, 0.5) / 3.0
    sd = S * vol_ann * math.sqrt(var_t / SEC_YR)
    K = spec.floor_strike
    return float(ndtr((S - K) / sd)) if spec.is_upper_tail else float(ndtr((K - S) / sd))


def build_panel(events, specs: dict[str, MarketSpec], step_ms: int = 100, vol_ann: float = 0.35,
                nowcast: str = "venues", micro: bool = False) -> pd.DataFrame:
    """Sample (t, ticker, x, y, S) on a grid. nowcast: 'venues' (median venue mid) or 'brti'."""
    books: dict[str, KalshiBook] = {t: KalshiBook(t) for t in specs}
    venues: dict[str, ExtBook] = {}
    last_brti = math.nan
    rows = []
    step = step_ms * NS_PER_MS
    next_t = None
    for ev in events:
        if next_t is None:
            next_t = (ev.ts // step + 1) * step
        while ev.ts >= next_t:
            mids = [b.top().mid for b in venues.values() if b.top() is not None]
            S = float(np.median(mids)) if (nowcast == "venues" and mids) else last_brti
            if not math.isnan(S):
                for t, b in books.items():
                    if not b.valid:
                        continue
                    y = b.microprice() if micro else b.mid()
                    if y is None:
                        continue
                    spec = specs[t]
                    if spec.expiration_ts - next_t < 90 * NS_PER_S:
                        continue
                    rows.append((next_t, t, simple_digital(S, spec, next_t, vol_ann), y / PX_SCALE, S))
            next_t += step
        if isinstance(ev, KalshiBookSnapshot) and ev.ticker in books:
            books[ev.ticker].apply_snapshot(ev)
        elif isinstance(ev, KalshiBookDelta) and ev.ticker in books:
            books[ev.ticker].apply_delta(ev)
        elif isinstance(ev, ExtBBO):
            b = venues.setdefault(ev.venue, ExtBook(ev.venue, ev.symbol))
            b.snapshot([(ev.bid, ev.bid_size)], [(ev.ask, ev.ask_size)], ev.ts)
        elif isinstance(ev, ExtBookSnapshot):
            b = venues.setdefault(ev.venue, ExtBook(ev.venue, ev.symbol))
            b.snapshot(ev.bids, ev.asks, ev.ts, ev.seq)
        elif isinstance(ev, ExtBookDelta):
            b = venues.setdefault(ev.venue, ExtBook(ev.venue, ev.symbol))
            for side, px, sz in ev.changes:
                b.update(side, px, sz)
        elif isinstance(ev, IndexTick) and ev.index_id == "BRTI":
            last_brti = ev.value
    return pd.DataFrame(rows, columns=["t", "ticker", "x", "y", "S"])


@dataclass
class LeadLag:
    horizon_s: float
    b_gap: float  # gap-closure coefficient
    b_gap_se: float
    c_move: float  # response to the recent external move
    c_move_se: float
    n: int


def _ols(xv: np.ndarray, yv: np.ndarray) -> tuple[float, float]:
    X = np.column_stack([np.ones_like(xv), xv])
    beta, *_ = np.linalg.lstsq(X, yv, rcond=None)
    resid = yv - X @ beta
    s2 = resid @ resid / max(len(yv) - 2, 1)
    cov = s2 * np.linalg.inv(X.T @ X)
    return float(beta[1]), float(math.sqrt(cov[1, 1]))


def lead_lag(panel: pd.DataFrame, step_ms: int = 100, lookback_s: float = 1.0,
             horizons=HORIZONS_S) -> list[LeadLag]:
    out = []
    lb = int(round(lookback_s * 1000 / step_ms))
    for h in horizons:
        k = int(round(h * 1000 / step_ms))
        xs_gap, ys_gap, xs_mv, ys_mv = [], [], [], []
        for _, g in panel.groupby("ticker"):
            g = g.sort_values("t")
            x, y = g.x.to_numpy(), g.y.to_numpy()
            if len(x) <= k + lb:
                continue
            dy = y[k + lb:] - y[lb:-k] if k else np.zeros(len(y) - lb)
            gap = x[lb:len(x) - k] - y[lb:len(y) - k]
            mv = x[lb:len(x) - k] - x[: len(x) - k - lb]
            xs_gap.append(gap), ys_gap.append(dy), xs_mv.append(mv), ys_mv.append(dy)
        if not xs_gap:
            continue
        gx, gy = np.concatenate(xs_gap), np.concatenate(ys_gap)
        mx, my = np.concatenate(xs_mv), np.concatenate(ys_mv)
        b, bse = _ols(gx, gy)
        c, cse = _ols(mx, my)
        out.append(LeadLag(h, b, bse, c, cse, len(gy)))
    return out


def half_life(results: list[LeadLag]) -> float:
    """Interpolated horizon (s) at which the gap-closure coefficient reaches 0.5."""
    pts = sorted((r.horizon_s, r.b_gap) for r in results)
    prev = (0.0, 0.0)
    for h, b in pts:
        if b >= 0.5:
            h0, b0 = prev
            return h0 + (0.5 - b0) * (h - h0) / max(b - b0, 1e-9)
        prev = (h, b)
    return math.inf
