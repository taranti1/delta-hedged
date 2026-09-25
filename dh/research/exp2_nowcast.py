"""Experiment 2: do constituent venues / perps predict the next BRTI print and the 60-s window average?

Panel (one row per ``step_ms`` of RECEIVE time, features use only events received <= t):
    brti_last        last BRTI value received (Kalshi cfbenchmarks_value / _5hz)
    g_med, g_dw      median venue mid / depth-weighted mid  minus brti_last        ($)
    g_micro, g_rep   median venue microprice / dh.feeds.composite.brti_replica minus brti_last
    basis            median perp mid minus median spot mid ($; NaN without perp venues)
    ofi_1s, ofi_5s   aggressor-signed spot trade volume over the last 1 s / 5 s (BTC)
    r_brti_1s/5s     brti_last change over the last 1 s / 5 s; r_ven_0.2s/1s: median-mid change
    brti_age_s       receive age of the last BRTI tick
Targets: y_h = brti_last(t + h) - brti_last(t) for h in horizons (default 0.2, 0.5, 1 s) and the
next print received after t (y_next). Benchmark: the last print (y_hat = 0); reference nowcasts:
median mid and the BRTI replica (y_hat = gap). Models: walk-forward ridge and LightGBM (time-
ordered folds: UTC days when the window spans >= 3 days, else equal time blocks; an embargo of
max(h) + 5 s before every test block; never shuffled). Metrics: OOS RMSE/MAE by horizon, RMSE
gain vs the last print with a moving-block bootstrap CI (60 s blocks).

Window average: for every expiration T of the universe and decision times in the last 120 s
before T, the settlement estimate (sum_fixed + m * S_hat) / n with S_hat = brti_last (baseline)
or brti_last + y_hat(1 s) (walk-forward ridge), against the realized 60-print average.

P&L hook (feeds E1/E3): ``NowcastMarketMaker`` replaces the strategy's nowcast by
brti_last + beta * (median venue mid - brti_last); ``pnl_hook`` fits beta on the first part of
the window and replays baseline vs nowcast strategy on the rest under fill policies B and C
(paired event-bootstrap CI of the realized net c/contract difference). The same strategy factory
plugs into E3 (``replay_fill_table(..., strategy_factory=NowcastMarketMaker)``) to measure fill
toxicity with the better nowcast, and its fair value is E1's x series.

Decision rule (docs/TEST_MATRIX.md E2): accept if RMSE improves >= 10% at 0.2-1 s AND the E1/E3
P&L improves in replay; reject on < 5% RMSE gain or no P&L gain.
"""

from __future__ import annotations

import math
import statistics
from collections import deque
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import orjson
import pandas as pd

from dh.core.book import ExtBook
from dh.core.events import ExtBBO, ExtBookDelta, ExtBookSnapshot, ExtTrade, IndexTick, PerpState
from dh.core.units import NS_PER_MS, NS_PER_S
from dh.feeds.composite import BrtiParams, brti_replica, nowcast
from dh.feeds.registry import SPOT_CONSTITUENTS, has_normalizer
from dh.kalshi.normalize import ws_message_to_events
from dh.research.exp_common import Report, fmt_ns, n_events, paired_diff_ci, policy_letter
from dh.research.replay_env import (
    MD_CACHE_PREFIX,
    Universe,
    build_universe,
    inputs_meta,
    kalshi_ws_streams,
)
from dh.core.market import SettlementSpec
from dh.settlement.window import SettlementTracker
from dh.store.replay import Normalizers, iter_raw, list_streams
from dh.strategy.config import StrategyConfig
from dh.strategy.mm import MarketMaker

HORIZONS_S = (0.2, 0.5, 1.0)
PERP_VENUES = ("binance_futures", "bybit", "okx", "hyperliquid", "kalshi_perp", "deribit")
FEATURES = ("g_med", "g_dw", "g_micro", "g_rep", "basis", "ofi_1s", "ofi_5s", "r_brti_1s", "r_brti_5s",
            "r_ven_0.2s", "r_ven_1s", "brti_age_s")
RULE_E2 = ("accept if OOS RMSE improves >= 10% at 0.2-1 s AND the E1/E3 P&L improves in replay; reject if the "
           "RMSE gain is < 5% or there is no P&L gain")


# ============================================================================ data
def iter_index_and_venues(root: str | Path, t0: int, t1: int, *, warm_s: float = 900.0) -> Iterator[Any]:
    """BRTI ticks (kalshi.ws cfbenchmarks frames, prefiltered, stateless) + external venue and
    cached market-data events, receive-time ordered, from t0 - warm_s (book warm-up) to t1."""
    avail = list_streams(root)
    ws = kalshi_ws_streams(root)
    venues = [s for s in avail if has_normalizer(s)]
    caches = [s for s in avail if s.startswith(MD_CACHE_PREFIX)]
    norm = Normalizers()
    for rec in iter_raw(root, ws + venues + caches, t0 - int(warm_s * NS_PER_S), t1):
        if rec.stream in ws:
            if b"cfbenchmarks_value" not in rec.data:
                continue
            try:
                yield from ws_message_to_events(orjson.loads(rec.data), rec.t)
            except (orjson.JSONDecodeError, ValueError, KeyError, TypeError):
                continue
        else:
            yield from norm(rec)


@dataclass
class PanelResult:
    panel: pd.DataFrame
    window_rows: pd.DataFrame  # (T, t, k_fixed, sum_fixed, m, n, brti_last)
    brti: pd.DataFrame  # received BRTI series (ts, value)


def build_nowcast_panel(events, t0: int, t1: int, *, step_ms: int = 200, horizons_s: Sequence[float] = HORIZONS_S,
                        expirations: Sequence[int] = (), replica: bool = True, params: BrtiParams = BrtiParams(),
                        spot_venues: Sequence[str] = SPOT_CONSTITUENTS) -> PanelResult:
    """Walk the events once and build the causal feature panel + targets (module doc)."""
    spot: dict[str, ExtBook] = {}
    perp: dict[str, ExtBook] = {}
    perp_mark: dict[str, float] = {}
    trades: deque[tuple[int, float]] = deque()  # (ts, signed BTC) spot trades, last 5 s
    b_ts: list[int] = []
    b_val: list[float] = []
    last_brti = math.nan
    last_brti_ts = 0
    hist_brti: deque[tuple[int, float]] = deque()
    hist_ven: deque[tuple[int, float]] = deque()
    tracker = SettlementTracker()
    exps = sorted(e for e in expirations if t0 <= e <= t1 + 120 * NS_PER_S)
    step = step_ms * NS_PER_MS
    next_t = (t0 // step + 1) * step
    rows: list[tuple] = []
    wrows: list[tuple] = []

    def asof(hist: deque[tuple[int, float]], t: int) -> float:
        for ts, v in reversed(hist):
            if ts <= t:
                return v
        return math.nan

    def emit(g: int) -> None:
        live = {v: b for v, b in spot.items() if b.valid and b.top() is not None}
        nc = nowcast(live, g, params) if live else None
        med = nc.median_mid if nc is not None and nc.median_mid is not None else math.nan
        dw = nc.depth_weighted_mid if nc is not None and nc.depth_weighted_mid is not None else math.nan
        mic = nc.microprice_median if nc is not None and nc.microprice_median is not None else math.nan
        rep = math.nan
        if replica and live:
            r = brti_replica(live, g, params)
            rep = r.value if r is not None else math.nan
        pm = [b.top().mid for b in perp.values() if b.top() is not None] + list(perp_mark.values())
        basis = (statistics.median(pm) - med) if pm and math.isfinite(med) else math.nan
        while trades and g - trades[0][0] > 5 * NS_PER_S:
            trades.popleft()
        ofi5 = sum(x for _, x in trades)
        ofi1 = sum(x for ts, x in trades if g - ts <= NS_PER_S)
        hist_ven.append((g, med))
        while hist_ven and g - hist_ven[0][0] > 2 * NS_PER_S:
            hist_ven.popleft()
        while hist_brti and g - hist_brti[0][0] > 6 * NS_PER_S:
            hist_brti.popleft()
        B = last_brti
        rows.append((g, B, med - B, dw - B, mic - B, rep - B, basis, ofi1, ofi5,
                     B - asof(hist_brti, g - NS_PER_S), B - asof(hist_brti, g - 5 * NS_PER_S),
                     med - asof(hist_ven, g - 200 * NS_PER_MS), med - asof(hist_ven, g - NS_PER_S),
                     (g - last_brti_ts) / NS_PER_S if last_brti_ts else math.nan, med, rep))
        if exps and g % NS_PER_S == 0:
            for T in exps:
                if 0 < T - g <= 120 * NS_PER_S:
                    ws = tracker.window_state(SettlementSpec(), T, g)
                    wrows.append((T, g, ws.k_fixed, ws.sum_fixed, ws.m_remaining, ws.n_obs, B))

    for ev in events:
        ts = ev.ts
        if ts >= t1:
            break
        while ts >= next_t and next_t < t1:
            if next_t >= t0 and math.isfinite(last_brti):
                emit(next_t)
            next_t += step
        t = type(ev)
        if t is IndexTick:
            if ev.index_id == "BRTI":
                last_brti, last_brti_ts = ev.value, ts
                b_ts.append(ts)
                b_val.append(ev.value)
                hist_brti.append((ts, ev.value))
                tracker.on_index(ev)
        elif t in (ExtBBO, ExtBookSnapshot, ExtBookDelta):
            target = perp if ev.venue in PERP_VENUES else (spot if ev.venue in spot_venues else None)
            if target is None:
                continue
            b = target.get(ev.venue)
            if b is None:
                b = target[ev.venue] = ExtBook(ev.venue, ev.symbol)
            if t is ExtBBO:
                b.apply_bbo(ev)
            elif t is ExtBookSnapshot:
                b.apply_snapshot(ev)
            elif b.valid:
                b.apply(ev)
        elif t is ExtTrade:
            if ev.venue in spot_venues and ev.aggressor in ("buy", "sell"):
                trades.append((ts, ev.size if ev.aggressor == "buy" else -ev.size))
        elif t is PerpState:
            if ev.mark > 0:
                perp_mark[ev.venue] = ev.mark
    while next_t < t1:
        if next_t >= t0 and math.isfinite(last_brti):
            emit(next_t)
        next_t += step
    cols = ["t", "brti", "g_med", "g_dw", "g_micro", "g_rep", "basis", "ofi_1s", "ofi_5s", "r_brti_1s", "r_brti_5s",
            "r_ven_0.2s", "r_ven_1s", "brti_age_s", "median_mid", "replica"]
    panel = pd.DataFrame(rows, columns=cols)
    bts = np.asarray(b_ts, dtype=np.int64)
    bv = np.asarray(b_val, dtype=float)
    if len(panel) and len(bts):
        tt = panel["t"].to_numpy(dtype=np.int64)
        for h in horizons_s:
            i = np.searchsorted(bts, tt + int(round(h * NS_PER_S)), side="right") - 1
            fut = np.where(i >= 0, bv[np.clip(i, 0, None)], np.nan)
            ok = tt + int(round(h * NS_PER_S)) <= bts[-1]
            panel[f"y_{h:g}s"] = np.where(ok, fut - panel["brti"].to_numpy(), np.nan)
        j = np.searchsorted(bts, tt, side="right")
        panel["y_next"] = np.where(j < len(bts), bv[np.clip(j, 0, len(bv) - 1)] - panel["brti"].to_numpy(), np.nan)
    wr = pd.DataFrame(wrows, columns=["T", "t", "k_fixed", "sum_fixed", "m", "n", "brti"])
    if len(wr):
        truth = {}
        for T in wr["T"].unique():
            ws = tracker.window_state(SettlementSpec(), int(T), int(T) + 30 * NS_PER_S)
            truth[T] = ws.settlement_value if ws.is_final else math.nan
        wr["A"] = wr["T"].map(truth)
    return PanelResult(panel, wr, pd.DataFrame({"ts": bts, "value": bv}))


# ============================================================================ walk-forward
def time_folds(t: np.ndarray, n_folds: int = 5) -> np.ndarray:
    """Fold id per row: UTC day when the span covers >= 3 days, else equal-duration blocks."""
    if not len(t):
        return np.zeros(0, dtype=int)
    span = (t.max() - t.min()) / NS_PER_S
    if span >= 3 * 86400:
        day = t // (86400 * NS_PER_S)
        return (day - day.min()).astype(int)
    edges = np.linspace(t.min(), t.max() + 1, n_folds + 1)
    return np.clip(np.searchsorted(edges, t, side="right") - 1, 0, n_folds - 1).astype(int)


def _ridge(alpha: float = 1.0):
    from sklearn.linear_model import Ridge
    from sklearn.pipeline import make_pipeline
    from sklearn.preprocessing import StandardScaler

    return make_pipeline(StandardScaler(), Ridge(alpha=alpha))


def _lgbm(seed: int = 0, n_estimators: int = 150):
    from lightgbm import LGBMRegressor

    return LGBMRegressor(n_estimators=n_estimators, learning_rate=0.05, num_leaves=15, min_child_samples=100,
                         subsample=0.8, subsample_freq=1, colsample_bytree=0.8, random_state=seed, n_jobs=1,
                         verbose=-1, deterministic=True, force_row_wise=True)


def walk_forward_predict(panel: pd.DataFrame, target: str, features: Sequence[str], model: str, *, n_folds: int = 5,
                         embargo_s: float = 6.0, seed: int = 0, min_train: int = 200) -> np.ndarray:
    """Out-of-sample predictions (NaN for the first fold / rows without a fit)."""
    y = panel[target].to_numpy(dtype=float)
    X = panel[list(features)].to_numpy(dtype=float)
    t = panel["t"].to_numpy(dtype=np.int64)
    folds = time_folds(t, n_folds)
    pred = np.full(len(panel), np.nan)
    emb = int(embargo_s * NS_PER_S)
    for k in np.unique(folds):
        te = folds == k
        if not te.any():
            continue
        t_start = t[te].min()
        tr = (t < t_start - emb) & np.isfinite(y)
        if tr.sum() < min_train:
            continue
        if model == "ridge":
            m = _ridge()
            m.fit(np.nan_to_num(X[tr]), y[tr])
            pred[te] = m.predict(np.nan_to_num(X[te]))
        elif model == "lgbm":
            m = _lgbm(seed)
            m.fit(X[tr], y[tr])
            pred[te] = m.predict(X[te])
        else:
            raise ValueError(model)
    return pred


def _block_ci(err_model: np.ndarray, err_base: np.ndarray, t: np.ndarray, block_s: float = 60.0, n_boot: int = 300,
              seed: int = 5) -> tuple[float, float]:
    """95% CI of the RMSE gain (1 - rmse_model / rmse_base) resampling time blocks."""
    ok = np.isfinite(err_model) & np.isfinite(err_base)
    if ok.sum() < 20:
        return math.nan, math.nan
    em, eb, tt = err_model[ok] ** 2, err_base[ok] ** 2, t[ok]
    blk = (tt // int(block_s * NS_PER_S)).astype(np.int64)
    keys, inv = np.unique(blk, return_inverse=True)
    sm = np.bincount(inv, weights=em)
    sb = np.bincount(inv, weights=eb)
    k = len(keys)
    if k < 3:
        return math.nan, math.nan
    rng = np.random.default_rng(seed)
    idx = rng.integers(0, k, size=(n_boot, k))
    g = 1.0 - np.sqrt(sm[idx].sum(axis=1) / np.maximum(sb[idx].sum(axis=1), 1e-300))
    lo, hi = np.percentile(g, [2.5, 97.5])
    return float(lo), float(hi)


def evaluate_nowcasts(panel: pd.DataFrame, horizons_s: Sequence[float] = HORIZONS_S, *, n_folds: int = 5,
                      models: Sequence[str] = ("ridge", "lgbm"), seed: int = 0) -> tuple[pd.DataFrame, pd.DataFrame]:
    """(metrics table, OOS predictions frame). Every model is scored on the same OOS rows
    (those where the walk-forward models have predictions)."""
    feats = [f for f in FEATURES if f in panel and panel[f].notna().any()]
    preds = pd.DataFrame({"t": panel["t"]})
    rows = []
    t = panel["t"].to_numpy(dtype=np.int64)
    for h in [*horizons_s, "next"]:
        tgt = f"y_{h:g}s" if h != "next" else "y_next"
        if tgt not in panel:
            continue
        y = panel[tgt].to_numpy(dtype=float)
        cand: dict[str, np.ndarray] = {"last_print": np.zeros(len(y))}
        if "g_med" in panel:
            cand["median_mid"] = panel["g_med"].to_numpy(dtype=float)
        if "g_rep" in panel and panel["g_rep"].notna().any():
            cand["replica"] = panel["g_rep"].to_numpy(dtype=float)
        for m in models:
            try:
                cand[m] = walk_forward_predict(panel, tgt, feats, m, n_folds=n_folds, seed=seed,
                                               embargo_s=(h if h != "next" else 1.0) + 5.0)
            except ImportError:  # lightgbm missing
                continue
        oos = np.isfinite(y) & np.logical_and.reduce([np.isfinite(cand[m]) for m in models if m in cand]) \
            if any(m in cand for m in models) else np.isfinite(y)
        base_err = y - cand["last_print"]
        rmse_base = float(np.sqrt(np.mean(base_err[oos] ** 2))) if oos.any() else math.nan
        for name, p in cand.items():
            err = y - p
            ok = oos & np.isfinite(p)
            rmse = float(np.sqrt(np.mean(err[ok] ** 2))) if ok.any() else math.nan
            lo, hi = _block_ci(np.where(ok, err, np.nan), np.where(ok, base_err, np.nan), t)
            rows.append({"horizon": f"{h:g}s" if h != "next" else "next_print", "model": name, "n_oos": int(ok.sum()),
                         "rmse_usd": rmse, "mae_usd": float(np.mean(np.abs(err[ok]))) if ok.any() else math.nan,
                         "rmse_gain_pct": 100.0 * (1.0 - rmse / rmse_base) if rmse_base > 0 else math.nan,
                         "gain_lo_pct": 100.0 * lo, "gain_hi_pct": 100.0 * hi})
            preds[f"{name}_{tgt}"] = p
    return pd.DataFrame(rows), preds


def window_average_eval(wr: pd.DataFrame, panel: pd.DataFrame, preds: pd.DataFrame, pred_col: str) -> pd.DataFrame:
    """Settlement-average error (A - A_hat, $) by time-to-expiry bucket: baseline vs model."""
    if wr is None or not len(wr) or pred_col not in preds:
        return pd.DataFrame()
    p = pd.DataFrame({"t": panel["t"].to_numpy(), "yhat": preds[pred_col].to_numpy()})
    d = wr.merge(p, on="t", how="left")
    d = d[d["A"].notna() & (d["m"] > 0)]
    if not len(d):
        return pd.DataFrame()
    d["tau_s"] = (d["T"] - d["t"]) / NS_PER_S
    base = (d["sum_fixed"] + d["m"] * d["brti"]) / d["n"]
    mod = (d["sum_fixed"] + d["m"] * (d["brti"] + d["yhat"].fillna(0.0))) / d["n"]
    d["err_base"] = d["A"] - base
    d["err_model"] = d["A"] - mod
    d["tau_b"] = pd.cut(d["tau_s"], [0, 15, 30, 60, 120.01], labels=["0-15s", "15-30s", "30-60s", "60-120s"], right=True)
    rows = []
    for b, g in d.groupby("tau_b", observed=True):
        rows.append({"tau_bucket": b, "n": len(g), "expirations": int(g["T"].nunique()),
                     "rmse_base_usd": float(np.sqrt(np.mean(g.err_base ** 2))),
                     "rmse_model_usd": float(np.sqrt(np.mean(g.err_model ** 2))),
                     "mae_base_usd": float(np.mean(np.abs(g.err_base))), "mae_model_usd": float(np.mean(np.abs(g.err_model)))})
    out = pd.DataFrame(rows)
    out["rmse_gain_pct"] = 100.0 * (1 - out.rmse_model_usd / out.rmse_base_usd)
    return out


def fit_beta(panel: pd.DataFrame, horizon_s: float = 0.5, feature: str = "g_med", t_end: int | None = None) -> float:
    """OLS slope (no intercept) of y_h on the venue gap using rows before t_end: the P&L hook's
    nowcast weight (share of the venue-vs-BRTI gap that the next prints close)."""
    tgt = f"y_{horizon_s:g}s"
    d = panel if t_end is None else panel[panel["t"] < t_end]
    d = d[np.isfinite(d[tgt]) & np.isfinite(d[feature])]
    x, y = d[feature].to_numpy(), d[tgt].to_numpy()
    den = float(x @ x)
    return float(x @ y / den) if den > 0 else 0.0


# ============================================================================ P&L hook
class NowcastMarketMaker(MarketMaker):
    """MarketMaker whose benchmark nowcast is brti_last + beta * (median venue mid - brti_last).

    Research hook for E2 -> E1/E3 (the production config key fair_value.nowcast =
    'brti_plus_composite' is not implemented in dh.strategy.mm; see the E2 report)."""

    def __init__(self, cfg: StrategyConfig, specs, *, nowcast_beta: float = 0.0, max_venue_age_s: float = 2.0,
                 spot_venues: Sequence[str] = SPOT_CONSTITUENTS, **kw: Any) -> None:
        super().__init__(cfg, specs, **kw)
        self.nowcast_beta = float(nowcast_beta)
        self.max_venue_age_ns = int(max_venue_age_s * NS_PER_S)
        self.spot_venues = set(spot_venues)

    def _nowcast(self, now: int):  # type: ignore[override]
        S, sd = super()._nowcast(now)
        if S is None or self.nowcast_beta == 0.0:
            return S, sd
        mids = [b.top().mid for v, b in self.ext.items()
                if v in self.spot_venues and b.top() is not None and now - b.ts <= self.max_venue_age_ns]
        if len(mids) < 2:
            return S, sd
        return S + self.nowcast_beta * (statistics.median(mids) - S), sd


def pnl_hook(root: str | Path, t_split: int, t1: int, cfg: StrategyConfig, beta: float, *, policies=("B", "C"),
             universe: Universe | None = None, warm: str = "recorded", seed: int = 1, n_boot: int = 400,
             n_jobs: int = 1, progress=None) -> pd.DataFrame:
    """Replay baseline vs NowcastMarketMaker(beta) on [t_split, t1) under each policy; paired
    event-bootstrap CI of the net c/contract difference and of the 1 s / 10 s markouts."""
    from dh.research.replay_grid import Variant, run_variants

    uni = universe or build_universe(root, t_split, t1)
    runs = run_variants(root, t_split, t1, [Variant("base", cfg),
                                            Variant("nowcast", cfg, strategy_factory=NowcastMarketMaker,
                                                    factory_kwargs={"nowcast_beta": beta})],
                        policies, universe=uni, warm=warm, seed=seed, n_jobs=n_jobs, progress=progress)
    by = {(r.variant, r.policy): r for r in runs}
    rows = []
    warns = sorted({w for r in runs for w in r.summary.get("warnings", []) or []})
    for p in policies:
        L = policy_letter(p)
        base, nc = by[("base", L)], by[("nowcast", L)]
        row = {"policy": L, "beta": beta, "fills_base": len(base.df), "fills_nowcast": len(nc.df),
               "net_c_base": base.summary.get("net_c_per_contract", math.nan),
               "net_c_nowcast": nc.summary.get("net_c_per_contract", math.nan),
               "usd_day_base": base.summary.get("net_usd_per_day", 0.0),
               "usd_day_nowcast": nc.summary.get("net_usd_per_day", 0.0)}
        a, b = _settled(base.df), _settled(nc.df)
        d = paired_diff_ci(a, b, "net_c_per_ct", n_boot=n_boot) if len(a) and len(b) else None
        row.update({"d_net_c": d.mean if d else math.nan, "d_net_lo_c": d.lo if d else math.nan,
                    "d_net_hi_c": d.hi if d else math.nan})
        # adverse selection on a COMMON reference: gross P&L to settlement per contract minus the edge
        # at the fill (to_settle_c = (settle - F_fill) s uses each run's own F, so compare the net)
        for name, df_ in (("base", a), ("nowcast", b)):
            row[f"fees_c_{name}"] = 100.0 * float(df_["fee"].sum() / df_["contracts"].sum()) if len(df_) else math.nan
        row["events"] = n_events(a, b)
        row["warnings"] = "; ".join(warns)
        rows.append(row)
    return pd.DataFrame(rows)


def _settled(df: pd.DataFrame) -> pd.DataFrame:
    return df[df["settle"].notna()] if len(df) and "settle" in df else df


# ============================================================================ runner
def run(root: str | Path, t0: int, t1: int, out: str | Path, *, cfg: StrategyConfig | None = None, step_ms: int = 200,
        horizons_s: Sequence[float] = HORIZONS_S, n_folds: int = 5, replica: bool = True, pnl: bool = True,
        policies=("B", "C"), split_frac: float = 0.5, hook_horizon_s: float = 0.5, warm: str = "recorded",
        seed: int = 0, universe: Universe | None = None, n_jobs: int = 1, progress=None) -> dict[str, Any]:
    """Full E2: panel -> walk-forward metrics -> window-average errors -> P&L hook. Writes CSV +
    markdown to ``out``; returns the tables."""
    Path(out).mkdir(parents=True, exist_ok=True)
    cfg = cfg or StrategyConfig()
    uni = universe or build_universe(root, t0, t1)
    pr = build_nowcast_panel(iter_index_and_venues(root, t0, t1), t0, t1, step_ms=step_ms, horizons_s=horizons_s,
                             expirations=uni.expirations(), replica=replica)
    metrics, preds = evaluate_nowcasts(pr.panel, horizons_s, n_folds=n_folds, seed=seed)
    wa = window_average_eval(pr.window_rows, pr.panel, preds, "ridge_y_1s")
    t_split = t0 + int(split_frac * (t1 - t0))
    beta = fit_beta(pr.panel, hook_horizon_s, "g_med", t_end=t_split) if len(pr.panel) else 0.0
    hook = (pnl_hook(root, t_split, t1, cfg, beta, policies=policies, universe=uni, warm=warm, n_jobs=n_jobs,
                     progress=progress) if pnl else pd.DataFrame())
    rep = Report("e2_nowcast", "E2 — Nowcasting the next BRTI print and the settlement average", Path(out),
                 synthetic=uni.synthetic, rule=RULE_E2,
                 meta={"root": str(root), "window": f"{fmt_ns(t0)} .. {fmt_ns(t1)}", "panel_rows": len(pr.panel),
                       "step_ms": step_ms, "folds": n_folds, "brti_ticks": len(pr.brti),
                       "hook_beta(g_med, h=%gs)" % hook_horizon_s: beta,
                       **({"P&L hook " + k: v for k, v in inputs_meta(uni, t_split)[0].items()} if pnl else {})})
    short = metrics[metrics["horizon"].isin([f"{h:g}s" for h in horizons_s])]
    best = short[short["model"].isin(["ridge", "lgbm"])]
    gain_ok = bool(len(best)) and bool((best.groupby("horizon")["rmse_gain_pct"].max() >= 10.0).all())
    gain_bad = bool(len(best)) and bool((best.groupby("horizon")["rmse_gain_pct"].max() < 5.0).all())
    pnl_ok = bool(len(hook)) and bool((hook["d_net_lo_c"] > 0).all())
    rep.decision_events = int(hook["events"].min()) if len(hook) and "events" in hook else None
    if gain_ok and pnl_ok:
        rep.verdict = "ACCEPT (RMSE gain >= 10% at every short horizon and replay P&L improves under B and C)"
    elif gain_bad or (len(hook) and (hook["d_net_hi_c"] < 0).any()):
        rep.verdict = "REJECT"
    else:
        rep.verdict = "INCONCLUSIVE (RMSE gain %s; P&L gain %s)" % ("ok" if gain_ok else "insufficient",
                                                                   "ok" if pnl_ok else "not established")
    rep.table("forecast", metrics, "Walk-forward OOS errors of the BRTI change over each horizon ($); gain vs the "
                                   "last print with a 60 s moving-block bootstrap CI.")
    rep.table("window_average", wa, "Error of the settlement-average estimate in the last 120 s before expiry "
                                     "(model: brti_last + ridge 1 s nowcast for every unfixed print).")
    rep.table("pnl_hook", hook, "Replay on the second part of the window: NowcastMarketMaker(beta) minus baseline "
                                "(paired by event, realized net c/contract to settlement: the common yardstick; "
                                "fair-value markouts are not comparable across nowcast definitions).")
    rep.write()
    return {"forecast": metrics, "window_average": wa, "pnl_hook": hook, "beta": beta, "panel": pr.panel}
