"""Experiment 1: is Kalshi stale relative to external BTC? (lead-lag at 100 ms - 5 s)

Build, on a fixed event-time grid (default 100 ms), per market:
    x(t) = fair value implied by the EXTERNAL composite (benchmark nowcast) at t, computed with the
           production pricer (dh.models.fairvalue.digital, all strike types)
    y(t) = Kalshi mid (or microprice) at t, from the reconstructed L2 book (NaN if invalid)
Each ticker's series is reindexed to the full grid, so every pair (t, t+h) spans exactly h
(audit M5). For horizons h in {0.1, 0.25, 0.5, 1, 2, 5} s:

    y(t+h) - y(t) = a + b_h * (x(t) - y(t)) + e          ("gap closure")
    y(t+h) - y(t) = a + c_h * (x(t) - x(t-L)) + e         (response to a recent external move)

b_h rising toward 1 measures how fast Kalshi closes a gap to external fair value; the horizon
where b_h reaches ~1 estimates the lag (half-life ~ lag/2 for a uniformly stale maker).
Inference: block bootstrap over (event, time block) units, because overlapping horizons and a
shared BTC path make observations dependent; OLS standard errors are reported only for reference.
Economic size: mean |gap| in ticks conditional on a recent external move larger than k sigma.

Known-answer test: tests/research/test_known_answers.py (synthetic market with injected lag).
"""

from __future__ import annotations

import math
from dataclasses import dataclass

import numpy as np
import pandas as pd

from dh.core.book import ExtBook, KalshiBook
from dh.core.events import ExtBBO, ExtBookDelta, ExtBookSnapshot, IndexTick, KalshiBookDelta, KalshiBookSnapshot
from dh.core.market import MarketSpec
from dh.core.units import NS_PER_MS, NS_PER_S, PX_SCALE
from dh.models.fairvalue import avg_variance_time, digital, digital_vec
from dh.settlement.window import pre_window_state

SEC_YR = 365.0 * 24 * 3600
HORIZONS_S = (0.1, 0.25, 0.5, 1.0, 2.0, 5.0)


def research_fair_value(S: float, spec: MarketSpec, now_ns: int, vol_ann: float, tail="gauss") -> float:
    """Pre-window fair value with the production pricer (no fixed prints)."""
    ws = pre_window_state(spec.settlement, spec.expiration_ts, now_ns)
    sigma_abs = S * vol_ann / math.sqrt(SEC_YR)
    return digital(spec, ws, S, sigma_abs, tail).p_yes


def build_panel(events, specs: dict[str, MarketSpec], step_ms: int = 100, vol_ann: float = 0.35,
                nowcast: str = "venues", micro: bool = False, skip_final_s: float = 90.0) -> pd.DataFrame:
    """Full-grid panel: one row per (grid time, ticker) from the first grid time to the last;
    y is NaN whenever the Kalshi book is invalid or one-sided, x is NaN without a nowcast.
    nowcast: 'venues' (median venue mid) or 'brti' (last benchmark tick). x is computed with the
    vectorized production pricer after the event walk."""
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
            for t, b in books.items():
                spec = specs[t]
                if spec.expiration_ts - next_t < skip_final_s * NS_PER_S:
                    continue
                y = (b.microprice() if micro else b.mid()) if b.valid else None
                rows.append((next_t, t, spec.event_ticker, S, math.nan if y is None else y / PX_SCALE))
            next_t += step
        if isinstance(ev, KalshiBookSnapshot) and ev.ticker in books:
            books[ev.ticker].apply_snapshot(ev)
        elif isinstance(ev, KalshiBookDelta) and ev.ticker in books:
            books[ev.ticker].apply_delta(ev)
        elif isinstance(ev, ExtBBO):
            venues.setdefault(ev.venue, ExtBook(ev.venue, ev.symbol)).apply_bbo(ev)
        elif isinstance(ev, ExtBookSnapshot):
            venues.setdefault(ev.venue, ExtBook(ev.venue, ev.symbol)).apply_snapshot(ev)
        elif isinstance(ev, ExtBookDelta):
            venues.setdefault(ev.venue, ExtBook(ev.venue, ev.symbol)).apply(ev)
        elif isinstance(ev, IndexTick) and ev.index_id == "BRTI":
            last_brti = ev.value
    df = pd.DataFrame(rows, columns=["t", "ticker", "event", "S", "y"])
    df["x"] = np.nan
    for tk, idx in df.groupby("ticker").groups.items():
        spec = specs[tk]
        sub = df.loc[idx]
        secs = (spec.expiration_ts - sub.t.to_numpy()) / NS_PER_S
        tau_first = np.maximum(secs - (spec.settlement.n_obs - 1) * spec.settlement.step_ns / NS_PER_S, 0.0)
        vt = avg_variance_time(tau_first, spec.settlement.step_ns / NS_PER_S, spec.settlement.n_obs)
        S = sub.S.to_numpy()
        sd = S * vol_ann / math.sqrt(SEC_YR) * np.sqrt(vt)
        ok = np.isfinite(S)
        if not ok.any():
            continue
        res = digital_vec(spec.strike_type, S[ok], sd[ok], "gauss", floor=spec.floor_strike,
                          cap=spec.cap_strike, n_obs=spec.settlement.n_obs)
        vals = np.full(len(sub), np.nan)
        vals[ok] = res.p_yes
        df.loc[idx, "x"] = vals
    return df[["t", "ticker", "event", "x", "y", "S"]]


@dataclass
class LeadLag:
    horizon_s: float
    b_gap: float
    b_gap_se_ols: float
    b_gap_ci: tuple[float, float]
    c_move: float
    c_move_ci: tuple[float, float]
    n: int
    units: int


def _pairs(panel: pd.DataFrame, k: int, lb: int, block_ns: int):
    """Per ticker on its full grid: (gap, move, dy, unit_id) with all six values present."""
    out = []
    for tk, g in panel.groupby("ticker", sort=True):
        g = g.sort_values("t")
        x, y, t = g.x.to_numpy(), g.y.to_numpy(), g.t.to_numpy()
        ev = g.event.iloc[0]
        n = len(g)
        if n <= k + lb:
            continue
        i = np.arange(lb, n - k)
        gap = x[i] - y[i]
        mv = x[i] - x[i - lb]
        dy = y[i + k] - y[i]
        ok = np.isfinite(gap) & np.isfinite(mv) & np.isfinite(dy)
        units = [f"{ev}|{int(tt // block_ns)}" for tt in t[i][ok]]
        out.append((gap[ok], mv[ok], dy[ok], units))
    if not out:
        return None
    gap = np.concatenate([o[0] for o in out])
    mv = np.concatenate([o[1] for o in out])
    dy = np.concatenate([o[2] for o in out])
    units = np.concatenate([np.asarray(o[3]) for o in out])
    return gap, mv, dy, units


def _slope(xv: np.ndarray, yv: np.ndarray) -> float:
    xm = xv - xv.mean()
    d = float(xm @ xm)
    return float(xm @ (yv - yv.mean()) / d) if d > 0 else math.nan


def _ols_se(xv: np.ndarray, yv: np.ndarray) -> float:
    b = _slope(xv, yv)
    a = yv.mean() - b * xv.mean()
    r = yv - a - b * xv
    xm = xv - xv.mean()
    return float(math.sqrt((r @ r) / max(len(yv) - 2, 1) / max(float(xm @ xm), 1e-300)))


def lead_lag(panel: pd.DataFrame, step_ms: int = 100, lookback_s: float = 1.0, horizons=HORIZONS_S,
             block_s: float = 60.0, n_boot: int = 200, seed: int = 3) -> list[LeadLag]:
    out = []
    lb = max(1, int(round(lookback_s * 1000 / step_ms)))
    rng = np.random.default_rng(seed)
    for h in horizons:
        k = int(round(h * 1000 / step_ms))
        pr = _pairs(panel, k, lb, int(block_s * NS_PER_S))
        if pr is None:
            continue
        gap, mv, dy, units = pr
        b = _slope(gap, dy)
        c = _slope(mv, dy)
        uniq, inv = np.unique(units, return_inverse=True)
        idx_by_unit = [np.flatnonzero(inv == u) for u in range(len(uniq))]
        bs_b, bs_c = [], []
        for _ in range(n_boot):
            pick = rng.integers(0, len(uniq), len(uniq))
            sel = np.concatenate([idx_by_unit[p] for p in pick])
            bs_b.append(_slope(gap[sel], dy[sel]))
            bs_c.append(_slope(mv[sel], dy[sel]))
        ci_b = tuple(np.nanpercentile(bs_b, [2.5, 97.5]).tolist())
        ci_c = tuple(np.nanpercentile(bs_c, [2.5, 97.5]).tolist())
        out.append(LeadLag(h, b, _ols_se(gap, dy), ci_b, c, ci_c, len(dy), len(uniq)))  # type: ignore[arg-type]
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


def gap_after_moves(panel: pd.DataFrame, step_ms: int = 100, lookback_s: float = 1.0, k_sigma: float = 2.0,
                    tick: float = 0.01) -> dict[str, float]:
    """Economic size: mean |x - y| in ticks at times when the external fair value moved more than
    k_sigma standard deviations over the lookback, vs unconditionally."""
    lb = max(1, int(round(lookback_s * 1000 / step_ms)))
    rows = []
    for _, g in panel.groupby("ticker"):
        g = g.sort_values("t")
        x, y = g.x.to_numpy(), g.y.to_numpy()
        if len(x) <= lb:
            continue
        mv = x[lb:] - x[:-lb]
        gap = np.abs(x[lb:] - y[lb:])
        ok = np.isfinite(mv) & np.isfinite(gap)
        rows.append((mv[ok], gap[ok]))
    if not rows:
        return {}
    mv = np.concatenate([r[0] for r in rows])
    gap = np.concatenate([r[1] for r in rows])
    sd = float(np.std(mv)) or 1e-12
    big = np.abs(mv) > k_sigma * sd
    return {"gap_ticks_all": float(gap.mean() / tick), "gap_ticks_after_move": float(gap[big].mean() / tick)
            if big.any() else math.nan, "share_big_moves": float(big.mean())}
