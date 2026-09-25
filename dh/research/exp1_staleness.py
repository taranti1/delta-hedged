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
                rows.append((next_t, t, spec.event_ticker, spec.expiration_ts, S, math.nan if y is None else y / PX_SCALE))
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
    df = pd.DataFrame(rows, columns=["t", "ticker", "event", "T", "S", "y"])
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
    return df[["t", "ticker", "event", "T", "x", "y", "S"]]


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
    anchor_s: float = 0.0  # responses measured from t + anchor (the Kalshi receive latency)
    big_move: float = math.nan  # mean |x(t) - x(t-L)| over external moves > 2 sd (probability units)
    response_ticks: float = math.nan  # c_move * big_move / tick: Kalshi's response to a > 2 sd external move
    response_lo_ticks: float = math.nan
    response_hi_ticks: float = math.nan


def _pairs(panel: pd.DataFrame, k: int, lb: int, block_ns: int, anchor: int = 0):
    """Per ticker on its full grid: (gap, move, dy, unit_id) with all values present.
    dy = y(t + anchor + k) - y(t + anchor); units = time blocks SHARED by every market (the BTC
    path is common to all strikes and events, audit C1)."""
    out = []
    for tk, g in panel.groupby("ticker", sort=True):
        g = g.sort_values("t")
        x, y, t = g.x.to_numpy(), g.y.to_numpy(), g.t.to_numpy()
        n = len(g)
        if n <= k + lb + anchor:
            continue
        i = np.arange(lb, n - k - anchor)
        gap = x[i] - y[i]
        mv = x[i] - x[i - lb]
        dy = y[i + anchor + k] - y[i + anchor]
        ok = np.isfinite(gap) & np.isfinite(mv) & np.isfinite(dy)
        out.append((gap[ok], mv[ok], dy[ok], (t[i][ok] // block_ns).astype(np.int64)))
    if not out:
        return None
    gap = np.concatenate([o[0] for o in out])
    mv = np.concatenate([o[1] for o in out])
    dy = np.concatenate([o[2] for o in out])
    units = np.concatenate([o[3] for o in out])
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
             block_s: float = 60.0, n_boot: int = 200, seed: int = 3, anchor_s: float = 0.0,
             k_sigma: float = 2.0, tick: float = 0.01) -> list[LeadLag]:
    """Per horizon: gap-closure slope b (reference only: quote noise and model error make b > 0
    even with zero lag) and the lag coefficient c of y(t+a+h) - y(t+a) on the PAST external move
    x(t) - x(t-L) (TEST_MATRIX E1; a = anchor_s rounded up to the grid, the receive latency), with
    95 % CIs over time blocks shared by every market (exp_common.slope_ci), and the implied
    response in ticks to an external move larger than k_sigma sd."""
    from dh.research.exp_common import slope_ci

    out = []
    lb = max(1, int(round(lookback_s * 1000 / step_ms)))
    anchor = int(math.ceil(max(anchor_s, 0.0) * 1000 / step_ms - 1e-9))
    for h in horizons:
        k = int(round(h * 1000 / step_ms))
        pr = _pairs(panel, k, lb, int(block_s * NS_PER_S), anchor)
        if pr is None or len(pr[2]) < 3:
            continue
        gap, mv, dy, units = pr
        cb = slope_ci(gap, dy, units, n_boot, seed)
        cc = slope_ci(mv, dy, units, n_boot, seed + 1)
        sd = float(np.std(mv))
        big = np.abs(mv) > k_sigma * sd if sd > 0 else np.zeros(len(mv), dtype=bool)
        bm = float(np.abs(mv[big]).mean()) if big.any() else math.nan
        out.append(LeadLag(h, cb.mean, _ols_se(gap, dy), (cb.lo, cb.hi), cc.mean, (cc.lo, cc.hi), len(dy), cc.clusters,
                           anchor * step_ms / 1000.0, bm, cc.mean * bm / tick, cc.lo * bm / tick, cc.hi * bm / tick))
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


# ============================================================================ recorded-data runner
RULE_E1 = ("accept if the lag coefficient on the PAST external fair-value change has a 95% CI lower bound > 0 at some "
           "horizon <= 1 s measured beyond the Kalshi receive latency, the implied response to a > 2 sd external move is "
           ">= 0.5 tick, and both hold in every calendar month of the window; reject if the response is < 0.2 tick "
           "(CI upper bound) at every horizon <= 1 s beyond the receive latency")
DECISION_MAX_H_S = 1.0
MIN_MONTH_PAIRS = 1000


def _passes(res: list[LeadLag]) -> list[LeadLag]:
    """Horizons <= 1 s where the lag coefficient CI is above 0 and the response >= 0.5 tick."""
    return [r for r in res if r.horizon_s <= DECISION_MAX_H_S + 1e-9 and math.isfinite(r.c_move_ci[0])
            and r.c_move_ci[0] > 0 and r.response_ticks >= 0.5]


def e1_verdict(pooled: list[LeadLag], by_month: dict[str, list[LeadLag]]) -> str:
    """TEST_MATRIX E1 on the lag coefficient (not the gap-closure slope, audit C1)."""
    dec = [r for r in pooled if r.horizon_s <= DECISION_MAX_H_S + 1e-9]
    if not dec:
        return "INCONCLUSIVE (no usable pairs at horizons <= 1 s)"
    ok = _passes(pooled)
    bad_months = [m for m, res in sorted(by_month.items()) if not _passes(res)]
    if ok and by_month and not bad_months:
        best = max(ok, key=lambda r: r.response_ticks)
        return (f"ACCEPT (lag coefficient {best.c_move:.3f} [{best.c_move_ci[0]:.3f}, {best.c_move_ci[1]:.3f}] at "
                f"{best.horizon_s:g} s beyond {best.anchor_s:g} s receive latency; response to a > 2 sd external move "
                f"{best.response_ticks:.2f} tick; holds in every month: {', '.join(sorted(by_month))})")
    hi = max((r.response_hi_ticks for r in dec if math.isfinite(r.response_hi_ticks)), default=math.nan)
    if math.isfinite(hi) and hi < 0.2:
        return (f"REJECT (response to a > 2 sd external move < 0.2 tick at every horizon <= 1 s beyond the receive "
                f"latency: largest CI upper bound {hi:.3f} tick)")
    why = []
    if not ok:
        why.append("no horizon <= 1 s with lag coefficient CI > 0 and response >= 0.5 tick")
    if bad_months:
        why.append(f"not stable: fails in month(s) {', '.join(bad_months)}")
    return "INCONCLUSIVE (" + "; ".join(why) + ")"


def realized_vol_ann(root, t0: int, t1: int) -> float:
    """Annualized realized vol of the recorded benchmark (60 s returns received in [t0, t1))."""
    from dh.research.replay_env import brti_ticks

    ticks = [e for e in brti_ticks(root, t0, t1) if e.feed in ("1hz", "rest")]
    if len(ticks) < 120:
        return 0.35
    s = pd.Series([e.value for e in ticks], index=pd.to_datetime([e.ts_exch or e.ts for e in ticks], unit="ns"))
    r = np.log(s.resample("60s").last().dropna()).diff().dropna()
    return float(r.std() * math.sqrt(365 * 24 * 60)) if len(r) > 10 else 0.35


def run(root, t0: int, t1: int, out, *, cfg=None, step_ms: int = 100, nowcast: str = "venues", micro: bool = False,
        vol_ann: float | None = None, universe=None, n_boot: int = 200, md_latency_ms: float | None = None) -> dict:
    """E1 on a recording: the ReplayStream (own-footprint filtered) -> build_panel -> lead_lag.

    Uses the market universe's specs (enabled series of ``cfg``) and the realized vol of the
    recorded benchmark over the 6 h BEFORE t0 (causal: no test-window data) unless ``vol_ann`` is
    given. E1's research fair value is Gaussian with that vol; it does not use the fitted FV
    parameters (dh/models/data/fv_recommended.json). Responses are measured from t + the Kalshi
    receive latency (``md_latency_ms``, default: the recording's measured median receive -
    exchange time; replay_env.measure_md_latency_ms). Writes CSV + markdown to ``out``."""
    from pathlib import Path

    from dh.research.exp_common import Report, fmt_ns
    from dh.research.replay_env import ReplayStream, build_universe, measure_md_latency_ms
    from dh.strategy.config import StrategyConfig

    cfg = cfg or StrategyConfig()
    Path(out).mkdir(parents=True, exist_ok=True)
    uni = universe or build_universe(root, t0, t1)
    specs = {s.ticker: s for s in uni.specs(cfg.quoting.enabled_series)}
    vol = vol_ann if vol_ann is not None else realized_vol_ann(root, t0 - 6 * 3600 * NS_PER_S, t0)
    vol_src = "given" if vol_ann is not None else "realized, 6 h before t0 (0.35 if < 2 h of 1 Hz data)"
    if md_latency_ms is None:
        md_latency_ms, md_note = measure_md_latency_ms(root, t0, t1, cache=uni.cache)
    else:
        md_note = "given"
    stream = ReplayStream(root, t0, t1, own_fills=uni.own_fills, prime_tickers=list(specs))
    panel = build_panel((e for e in stream if e.ts < t1), specs, step_ms=step_ms, vol_ann=vol, nowcast=nowcast,
                        micro=micro)
    anchor_s = md_latency_ms / 1000.0
    res = lead_lag(panel, step_ms=step_ms, n_boot=n_boot, anchor_s=anchor_s)
    months: dict[str, list[LeadLag]] = {}
    mrows = []
    if len(panel):
        mon = pd.to_datetime(panel["t"], unit="ns", utc=True).dt.strftime("%Y-%m")
        for m in sorted(mon.unique()):
            sub = panel[mon == m]
            rr = lead_lag(sub, step_ms=step_ms, n_boot=n_boot, anchor_s=anchor_s,
                          horizons=[h for h in HORIZONS_S if h <= DECISION_MAX_H_S + 1e-9])
            if not rr or max(r.n for r in rr) < MIN_MONTH_PAIRS:
                rr = []
            months[m] = rr
            for r in rr:
                mrows.append({"month": m, "horizon_s": r.horizon_s, "c_move": r.c_move, "c_move_lo": r.c_move_ci[0],
                              "c_move_hi": r.c_move_ci[1], "response_ticks": r.response_ticks, "n": r.n,
                              "time_blocks": r.units})
            if not rr:
                mrows.append({"month": m, "horizon_s": math.nan, "n": 0})
    tab = pd.DataFrame([{"horizon_s": r.horizon_s, "c_move": r.c_move, "c_move_lo": r.c_move_ci[0],
                         "c_move_hi": r.c_move_ci[1], "response_ticks": r.response_ticks,
                         "response_lo_ticks": r.response_lo_ticks, "response_hi_ticks": r.response_hi_ticks,
                         "big_move_prob": r.big_move, "b_gap": r.b_gap, "b_gap_lo": r.b_gap_ci[0],
                         "b_gap_hi": r.b_gap_ci[1], "n": r.n, "time_blocks": r.units, "anchor_s": r.anchor_s}
                        for r in res])
    econ = gap_after_moves(panel, step_ms=step_ms)
    hl = half_life(res) if res else math.inf
    n_exp = int(panel["T"].nunique()) if "T" in panel else int(panel["event"].nunique()) if len(panel) else 0
    rep = Report("e1_staleness", "E1 — Is Kalshi stale relative to external BTC?", Path(out), synthetic=uni.synthetic,
                 rule=RULE_E1, meta={"root": str(root), "window": f"{fmt_ns(t0)} .. {fmt_ns(t1)}", "markets": len(specs),
                                     "panel_rows": len(panel), "nowcast": nowcast, "vol_ann": vol, "vol_source": vol_src,
                                     "Kalshi receive latency (anchor)": f"{md_latency_ms:.1f} ms ({md_note})",
                                     "FV parameters": "not used (research Gaussian fair value)",
                                     "inference": "95% CI over 60 s time blocks shared by every market (hull of "
                                                  "studentized bootstrap-t and jackknife-t)",
                                     "gap-closure half-life (s, reference only)": hl,
                                     **{f"{k} (reference only)": v for k, v in econ.items()}})
    rep.decision_events = n_exp
    rep.verdict = e1_verdict(res, months)
    rep.line("b_gap (gap closure) and the gap-after-move economics are reported for reference only: quote noise and "
             "fair-value model error make them positive in a zero-lag market (audit C1).")
    rep.table("lead_lag", tab, "Lag coefficient c: y(t+a+h) - y(t+a) on x(t) - x(t-1s), a = receive latency rounded up "
                               "to the grid; response_ticks = c x mean |external move| over moves > 2 sd, in ticks.")
    rep.table("by_month", pd.DataFrame(mrows), "Stability: the same regression per calendar month (horizons <= 1 s).")
    rep.write()
    return {"lead_lag": tab, "by_month": pd.DataFrame(mrows), "economics": econ, "half_life_s": hl, "panel": panel,
            "verdict": rep.final_verdict(), "rule_outcome": rep.verdict}
