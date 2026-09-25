"""Experiment 3: is fill toxicity predictable, and does a toxicity-aware cancel rule pay?

Fills: shadow fills of the production MarketMaker replayed over the recording under fill policies B
and C (dh.research.replay_env.run_replay), or our live fills recorded in the window
(``live_fill_table``: private `fill` messages, fair value from the strategy's own FvProbe).

Markouts (dh.execution.markout.compute_markouts): signed cents per contract vs the FILL PRICE at
0.1, 0.5, 1, 5, 10, 30, 60 s (fair value = the strategy's logged F series) and to settlement; net =
markout - fee. Toxic = the fair value moved against the fill within 10 s (s * (F(t+10s) - F(t)) < 0:
adverse selection, excluding the spread captured); the regression target is the net 10 s markout
vs the fill price.

Features at fill time (causal; computed identically at decision time by ToxicityGuardMM):
queue ahead (strategy's queue estimate), quote age, adverse external move over 0.1/0.5/1/5 s
(median venue mid change in the direction that hurts the quote, sign-adjusted for the market's
delta), Kalshi touch imbalance on our side, venue top-of-book imbalance against us, public taker
volume in the market over 10 s / 60 s (and against our side over 10 s), time to expiry, |z|, YES
price, spread, sigma (annualized), fair-value edge at fill.

Models: walk-forward (folds of consecutive settlement events; never shuffled) logistic regression
and LightGBM for P(toxic), ridge and LightGBM for the 10 s markout. OOS AUC / log loss / Brier vs
the base rate; OOS R^2 vs the training mean.

Economic test: a cancel rule fitted on the first part of the window (logistic P(toxic) >=
threshold, threshold chosen on training fills with <= 20% of fills removed) is replayed on the
rest: ToxicityGuardMM pulls and does not place quotes on a market side whose score is above the
threshold. Reported under B and C: net c/contract change vs the baseline (paired event bootstrap)
and fill loss.

Decision rule (docs/TEST_MATRIX.md E3): accept if the toxicity-aware cancel policy raises net
c/contract by >= 0.1c (CI > 0) at <= 20% fill loss; reject on no OOS lift or if the lift vanishes
under policy C.
"""

from __future__ import annotations

import heapq
import math
import statistics
from collections import defaultdict, deque
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from dh.core.actions import PlaceOrder
from dh.core.events import ExtBBO, ExtBookDelta, ExtBookSnapshot, KalshiFill, KalshiTrade
from dh.core.units import NS_PER_S, PX_SCALE, QTY_SCALE
from dh.execution.markout import DEFAULT_HORIZONS_S, AsOf, compute_markouts
from dh.feeds.registry import SPOT_CONSTITUENTS
from dh.research.exp_common import (
    Report,
    cluster_mean_ci,
    fmt_ns,
    n_events,
    paired_diff_ci,
    policy_letter,
    write_csv,
)
from dh.research.replay_env import Universe, build_universe, inputs_meta, prime_probe, probe_for_window, run_replay
from dh.strategy.config import StrategyConfig
from dh.strategy.mm import MarketMaker

SEC_YR = 365.0 * 24 * 3600
RULE_E3 = ("accept if the toxicity-aware cancel policy raises net c/contract by >= 0.1c (CI > 0) at <= 20% fill "
           "loss; reject if there is no OOS lift or the lift vanishes under policy C")
FEATURES = ("queue_at_place_ct", "quote_age_s", "adv_ext_0.1s", "adv_ext_0.5s", "adv_ext_1s", "adv_ext_5s",
            "kalshi_imb_side", "venue_imb_adv", "taker_ct_10s", "taker_ct_60s", "takers_against_10s", "log_tau",
            "abs_z", "yes_px", "spread_ticks", "sigma_ann", "fv_edge_c")
LABEL_H_S = 10.0


# ============================================================================ features
class ToxicityState:
    """Histories needed by the toxicity features: median spot-venue mid (sampled on every venue
    update, throttled) and public Kalshi trades per market."""

    def __init__(self, spot_venues: Sequence[str] = SPOT_CONSTITUENTS, keep_s: float = 65.0, sample_ms: int = 50) -> None:
        self.spot = set(spot_venues)
        self.keep = int(keep_s * NS_PER_S)
        self.sample = sample_ms * 1_000_000
        self.mids: deque[tuple[int, float]] = deque()
        self.trades: dict[str, deque[tuple[int, float, str]]] = defaultdict(deque)

    def update(self, ev: Any, ext_books: dict) -> None:
        t = type(ev)
        if t is ExtBBO or t is ExtBookSnapshot or t is ExtBookDelta:
            if ev.venue not in self.spot or (self.mids and ev.ts - self.mids[-1][0] < self.sample):
                return
            ms = [b.top().mid for v, b in ext_books.items() if v in self.spot and b.top() is not None]
            if ms:
                self.mids.append((ev.ts, statistics.median(ms)))
                while self.mids and ev.ts - self.mids[0][0] > self.keep:
                    self.mids.popleft()
        elif t is KalshiTrade:
            dq = self.trades[ev.ticker]
            dq.append((ev.ts, ev.qty / QTY_SCALE, ev.taker_side))
            while dq and ev.ts - dq[0][0] > self.keep:
                dq.popleft()

    def mid_at(self, t: int) -> float:
        for ts, v in reversed(self.mids):
            if ts <= t:
                return v
        return math.nan


def queue_at_place(mm: MarketMaker, ticker: str, side: str, px: int) -> float:
    """Contracts that must trade before a new order at px: displayed qty at px plus better-priced
    displayed qty on our side (the strategy's own q_eff at placement)."""
    b = mm.books.get(ticker)
    if b is None or not b.valid:
        return math.nan
    if side == "bid":
        q = sum(v for p, v in b.yes_bids.items() if p >= px)
    else:
        q = sum(v for p, v in b.no_bids.items() if PX_SCALE - p <= px)
    return q / QTY_SCALE


def features_at(mm: MarketMaker, st: ToxicityState, now: int, ticker: str, side: str, px: int,
                queue_ahead_ct: float, age_s: float) -> dict[str, float]:
    """Toxicity features of a (hypothetical) fill of our `side` quote at YES price `px`;
    queue_ahead_ct = contracts ahead of the quote when it was placed (queue_at_place)."""
    spec = mm.specs.get(ticker)
    f = mm.fvc.get(ticker)
    s_side = 1.0 if side == "bid" else -1.0
    s_delta = 1.0 if (spec is None or spec.is_upper_tail) else -1.0
    s = s_side * s_delta
    m_now = st.mid_at(now)
    out: dict[str, float] = {"queue_at_place_ct": queue_ahead_ct, "quote_age_s": age_s}
    for h in (0.1, 0.5, 1.0, 5.0):
        m0 = st.mid_at(now - int(h * NS_PER_S))
        out[f"adv_ext_{h:g}s"] = -s * (m_now - m0) if math.isfinite(m_now) and math.isfinite(m0) else math.nan
    b = mm.books.get(ticker)
    if b is not None and b.valid:
        qb, qa = b.best_bid_qty() / QTY_SCALE, b.best_ask_qty() / QTY_SCALE
        ours, opp = (qb, qa) if side == "bid" else (qa, qb)
        out["kalshi_imb_side"] = (ours - opp) / (ours + opp) if ours + opp > 0 else 0.0
        sp = b.spread()
        out["spread_ticks"] = sp / 100.0 if sp is not None else math.nan
    else:
        out["kalshi_imb_side"] = math.nan
        out["spread_ticks"] = math.nan
    imbs = []
    for v, eb in mm.ext.items():
        top = eb.top()
        if v in st.spot and top is not None and top.bid_size + top.ask_size > 0:
            imbs.append((top.bid_size - top.ask_size) / (top.bid_size + top.ask_size))
    out["venue_imb_adv"] = -s * statistics.median(imbs) if imbs else math.nan
    dq = st.trades.get(ticker, ())
    t10, t60, against = 0.0, 0.0, 0.0
    hit_side = "no" if side == "bid" else "yes"  # takers selling YES hit our bid; buying YES lift our ask
    for ts, q, tside in dq:
        if now - ts <= 60 * NS_PER_S:
            t60 += q
            if now - ts <= 10 * NS_PER_S:
                t10 += q
                if tside == hit_side:
                    against += q
    out.update({"taker_ct_10s": t10, "taker_ct_60s": t60, "takers_against_10s": against})
    tau = (spec.expiration_ts - now) / NS_PER_S if spec is not None else math.nan
    out["log_tau"] = math.log(max(tau, 1.0)) if math.isfinite(tau) else math.nan
    out["abs_z"] = abs(f.z) if f is not None else math.nan
    out["yes_px"] = px / PX_SCALE
    S = mm._spot()
    try:
        out["sigma_ann"] = mm._sigma_1s(now, S) / S * math.sqrt(SEC_YR) if S else math.nan
    except Exception:  # noqa: BLE001 - vol not ready
        out["sigma_ann"] = math.nan
    out["fv_edge_c"] = 100.0 * s_side * (f.F - px / PX_SCALE) if f is not None else math.nan
    return out


class ToxicityCollector:
    """run_replay collector: features of every simulated fill, captured before the strategy
    processes the fill (queue at placement and order age as the strategy knew them)."""

    def __init__(self, mm: MarketMaker) -> None:
        self.mm = mm
        self.state = ToxicityState()
        self.rows: list[dict[str, Any]] = []
        self.q_place: dict[str, float] = {}

    def on_action(self, ts: int, a: Any) -> None:
        if type(a) is PlaceOrder:
            self.q_place[a.client_order_id] = queue_at_place(self.mm, a.ticker, a.book_side, a.px)

    def on_event(self, ev: Any) -> None:
        if type(ev) is KalshiFill:
            mm = self.mm
            w = mm.om.order(ev.client_order_id)
            age = (ev.ts - w.created_ns) / NS_PER_S if w is not None else math.nan
            row = features_at(mm, self.state, ev.ts, ev.ticker, ev.book_side, ev.yes_px,
                              self.q_place.get(ev.client_order_id, math.nan), age)
            row["trade_id"] = ev.trade_id
            self.rows.append(row)
        else:
            self.state.update(ev, self.mm.ext)

    def result(self) -> pd.DataFrame:
        return pd.DataFrame(self.rows)


# ============================================================================ fill tables
def _fv_lookup(ledger) -> dict[str, AsOf]:
    return {t: AsOf(s.ts, s.F) for t, s in ledger.fv.items() if s.ts}


def add_fill_markouts(res) -> None:
    """run_replay postprocess: merge the ToxicityCollector features into res.df and add markouts vs
    the fill price (dh.execution.markout, strategy fair value) at DEFAULT_HORIZONS_S and to
    settlement, the fee in c/contract, the net 10 s markout and the toxic label."""
    df = res.df
    if not len(df) or res.ledger is None:
        return
    feats = next((c for c in res.extras.get("collectors", []) if isinstance(c, pd.DataFrame) and "adv_ext_1s" in c),
                 pd.DataFrame())
    if len(feats):
        df = df.merge(feats, on="trade_id", how="left", suffixes=("", "_tox"))
    fills = [KalshiFill(ts=int(r.ts), ts_exch=0, ticker=r.ticker, trade_id=r.trade_id, order_id="", client_order_id="",
                        book_side="bid" if r.side > 0 else "ask", yes_px=int(round(r.px * PX_SCALE)),
                        qty=int(round(r.contracts * QTY_SCALE)), is_taker=bool(r.is_taker),
                        fee_micros=int(round(r.fee * 1e6)), post_position=0) for r in df.itertuples()]
    mk = compute_markouts(fills, _fv_lookup(res.ledger), DEFAULT_HORIZONS_S, settlement=dict(res.ledger.settle),
                          time_basis="recv")
    for j, h in enumerate(DEFAULT_HORIZONS_S):
        df[f"mkpx_{h:g}s_c"] = [m.markouts_c[j] for m in mk]
    df["mkpx_settle_c"] = [m.settle_c for m in mk]
    df["fee_c"] = [m.fee_c for m in mk]
    _label(df)
    res.df = df


def _label(df: pd.DataFrame) -> None:
    """net_mk_*: markout vs the fill price after fees (economic); adverse_10s_c: fair-value move
    against the fill over 10 s, s * (F(t+10s) - F(t)) (the adverse-selection component, excluding
    the spread captured). toxic = adverse_10s_c < 0."""
    df["net_mk_10s_c"] = df[f"mkpx_{LABEL_H_S:g}s_c"] - df["fee_c"]
    df["net_mk_60s_c"] = df["mkpx_60s_c"] - df["fee_c"] if "mkpx_60s_c" in df else np.nan
    adv = df[f"mo_{LABEL_H_S:g}s_c"] if f"mo_{LABEL_H_S:g}s_c" in df else df.get(f"mkfv_{LABEL_H_S:g}s_c")
    df["adverse_10s_c"] = adv if adv is not None else np.nan
    df["toxic"] = (df["adverse_10s_c"] < 0).astype(float).where(df["adverse_10s_c"].notna())


def replay_fill_table(root: str | Path, t0: int, t1: int, cfg: StrategyConfig, policy: str, *,
                      universe: Universe | None = None, warm: str = "recorded", seed: int = 1,
                      strategy_factory=None, factory_kwargs: dict | None = None) -> tuple[pd.DataFrame, dict]:
    """Shadow fills of one replay with features and markouts vs fill price (cents/contract)."""
    res = run_replay(root, t0, t1, cfg, policy, warm=warm, seed=seed, universe=universe,
                     collectors=[ToxicityCollector], strategy_factory=strategy_factory, factory_kwargs=factory_kwargs,
                     postprocess=add_fill_markouts)
    return res.df, res.summary


def live_fill_table(root: str | Path, t0: int, t1: int, cfg: StrategyConfig, universe: Universe, *,
                    warm: str = "recorded", horizons_s: Sequence[float] = DEFAULT_HORIZONS_S) -> pd.DataFrame:
    """Our LIVE fills in [t0, t1) (private fill messages of the recording) with the same features
    (queue ahead / quote age unknown: NaN) and markouts vs the strategy's fair value (FvProbe)."""
    fills = sorted((f for f in universe.own_fills.values() if t0 <= f.ts < t1), key=lambda f: (f.ts, f.trade_id))
    if not fills:
        return pd.DataFrame()
    probe, feed, _ = probe_for_window(root, t0, t1, cfg, universe, warm=warm)
    st = ToxicityState()
    rows: list[dict[str, Any]] = []
    marks: list[tuple[int, int, float, dict]] = []
    i = 0
    seq = 0
    first = True
    for ev in feed.events(on_add=probe.add):
        if first:
            first = False
            prime_probe(probe, feed.stream)
        while i < len(fills) and fills[i].ts <= ev.ts:
            f = fills[i]
            i += 1
            fv = probe.fair(f.ticker, f.ts)
            if fv is not None:
                probe.mm.fvc[f.ticker] = fv
            row = features_at(probe.mm, st, f.ts, f.ticker, f.book_side, f.yes_px, math.nan, math.nan)
            spec = probe.mm.specs.get(f.ticker)
            row.update({"ts": f.ts, "ticker": f.ticker, "event": spec.event_ticker if spec else f.ticker,
                        "side": 1 if f.book_side == "bid" else -1, "px": f.yes_px / PX_SCALE,
                        "contracts": f.qty / QTY_SCALE, "fee_c": 100.0 * f.fee_micros / 1e6 / (f.qty / QTY_SCALE),
                        "F": fv.F if fv else math.nan, "tau_s": (spec.expiration_ts - f.ts) / NS_PER_S if spec else math.nan,
                        "is_taker": f.is_taker, "trade_id": f.trade_id})
            rows.append(row)
            for h in horizons_s:
                seq += 1
                heapq.heappush(marks, (f.ts + int(h * NS_PER_S), seq, h, row))
        while marks and marks[0][0] <= ev.ts:
            tm, _, h, row = heapq.heappop(marks)
            fv = probe.fair(row["ticker"], tm)
            row[f"mkpx_{h:g}s_c"] = 100.0 * row["side"] * (fv.F - row["px"]) if fv is not None else math.nan
            row[f"mkfv_{h:g}s_c"] = 100.0 * row["side"] * (fv.F - row["F"]) if fv is not None else math.nan
        probe.on_event(ev)
        st.update(ev, probe.mm.ext)
    df = pd.DataFrame(rows)
    settle = universe.settlement_values()
    df["settle"] = df["ticker"].map(settle)
    df["mkpx_settle_c"] = 100.0 * df["side"] * (df["settle"] - df["px"])
    df["net_c_per_ct"] = df["mkpx_settle_c"] - df["fee_c"]
    for h in DEFAULT_HORIZONS_S:
        if f"mkpx_{h:g}s_c" not in df:
            df[f"mkpx_{h:g}s_c"] = np.nan
    _label(df)
    df["policy"] = "live"
    return df


# ============================================================================ models
def event_folds(df: pd.DataFrame, n_folds: int = 5) -> np.ndarray:
    """Fold id per fill: consecutive settlement events in expiration order (time-ordered)."""
    order = df.groupby("event")["ts"].min().sort_values()
    evs = list(order.index)
    k = max(1, min(n_folds, len(evs)))
    fold_of = {e: min(i * k // max(len(evs), 1), k - 1) for i, e in enumerate(evs)}
    return df["event"].map(fold_of).to_numpy()


def _make(kind: str, seed: int = 0):
    from sklearn.linear_model import LogisticRegression, Ridge
    from sklearn.pipeline import make_pipeline
    from sklearn.preprocessing import StandardScaler

    if kind == "logistic":
        return make_pipeline(StandardScaler(), LogisticRegression(C=0.5, max_iter=2000))
    if kind == "ridge":
        return make_pipeline(StandardScaler(), Ridge(alpha=5.0))
    from lightgbm import LGBMClassifier, LGBMRegressor

    kw = dict(n_estimators=100, learning_rate=0.05, num_leaves=7, min_child_samples=20, subsample=0.8, subsample_freq=1,
              colsample_bytree=0.8, random_state=seed, n_jobs=1, verbose=-1, deterministic=True, force_row_wise=True)
    return LGBMClassifier(**kw) if kind == "gbm_cls" else LGBMRegressor(**kw)


def _X(df: pd.DataFrame, feats: Sequence[str], fill_nan: bool) -> np.ndarray:
    X = df[list(feats)].to_numpy(dtype=float)
    return np.nan_to_num(X, nan=0.0, posinf=0.0, neginf=0.0) if fill_nan else X


def walk_forward_models(df: pd.DataFrame, feats: Sequence[str] = FEATURES, n_folds: int = 5, seed: int = 0,
                        min_train: int = 25) -> tuple[pd.DataFrame, pd.DataFrame]:
    """OOS classification/regression metrics and predictions (event-ordered folds; each fold is
    predicted by models trained on earlier events' fills whose 10 s label ended before the fold's
    first fill: no training label overlaps the test period)."""
    from sklearn.metrics import brier_score_loss, log_loss, roc_auc_score

    d = df[df["toxic"].notna()].copy()
    feats = [f for f in feats if f in d and d[f].notna().any()]
    if len(d) < 2 * min_train or not feats:
        return pd.DataFrame([{"model": "insufficient_data", "n_oos": 0, "fills": len(d)}]), d
    folds = event_folds(d, n_folds)
    d["fold"] = folds
    for m in ("logistic", "gbm_cls", "ridge", "gbm_reg"):
        d[f"pred_{m}"] = np.nan
    d["pred_base_rate"] = np.nan
    d["pred_mean"] = np.nan
    emb = int(LABEL_H_S * NS_PER_S)
    for k in sorted(set(folds)):
        te = d["fold"] == k
        # earlier events only, and only fills whose label horizon ended before the test fold starts
        tr = (d["fold"] < k) & (d["ts"] + emb <= d.loc[te, "ts"].min())
        if tr.sum() < min_train or d.loc[tr, "toxic"].nunique() < 2:
            continue
        y = d.loc[tr, "toxic"].to_numpy()
        r = d.loc[tr, "net_mk_10s_c"].to_numpy()
        d.loc[te, "pred_base_rate"] = float(y.mean())
        d.loc[te, "pred_mean"] = float(r.mean())
        for m in ("logistic", "gbm_cls"):
            try:
                mod = _make(m, seed)
                mod.fit(_X(d[tr], feats, m == "logistic"), y)
                d.loc[te, f"pred_{m}"] = mod.predict_proba(_X(d[te], feats, m == "logistic"))[:, 1]
            except ImportError:
                continue
        for m in ("ridge", "gbm_reg"):
            try:
                mod = _make(m, seed)
                mod.fit(_X(d[tr], feats, m == "ridge"), r)
                d.loc[te, f"pred_{m}"] = mod.predict(_X(d[te], feats, m == "ridge"))
            except ImportError:
                continue
    oos = d["pred_base_rate"].notna()
    rows = []
    y = d.loc[oos, "toxic"].to_numpy()
    for m in ("base_rate", "logistic", "gbm_cls"):
        p = d.loc[oos, f"pred_{m}"].to_numpy(dtype=float)
        ok = np.isfinite(p)
        if ok.sum() < 10:
            continue
        pp = np.clip(p[ok], 1e-4, 1 - 1e-4)
        auc = roc_auc_score(y[ok], pp) if len(set(y[ok])) > 1 and m != "base_rate" else 0.5
        rows.append({"model": m, "target": "toxic(fair value moved against the fill within 10 s)", "n_oos": int(ok.sum()),
                     "toxic_rate": float(y[ok].mean()), "auc": auc, "log_loss": log_loss(y[ok], pp, labels=[0, 1]),
                     "brier": brier_score_loss(y[ok], pp)})
    r = d.loc[oos, "net_mk_10s_c"].to_numpy(dtype=float)
    base = d.loc[oos, "pred_mean"].to_numpy(dtype=float)
    for m in ("ridge", "gbm_reg"):
        p = d.loc[oos, f"pred_{m}"].to_numpy(dtype=float)
        ok = np.isfinite(p) & np.isfinite(r)
        if ok.sum() < 10:
            continue
        sse, sst = float(np.sum((r[ok] - p[ok]) ** 2)), float(np.sum((r[ok] - base[ok]) ** 2))
        rows.append({"model": m, "target": "net 10s markout (c)", "n_oos": int(ok.sum()),
                     "r2_oos_vs_train_mean": 1 - sse / sst if sst > 0 else math.nan,
                     "corr": float(np.corrcoef(p[ok], r[ok])[0, 1]) if ok.sum() > 2 else math.nan})
    return pd.DataFrame(rows), d


def univariate_table(df: pd.DataFrame, feats: Sequence[str] = FEATURES) -> pd.DataFrame:
    """Spearman correlation of each feature with the net 10 s markout (sign diagnostics)."""
    d = df[df["net_mk_10s_c"].notna()]
    rows = []
    for f in feats:
        if f not in d or d[f].notna().sum() < 10 or d[f].nunique() < 2:
            continue
        ok = d[f].notna()
        rho = d.loc[ok, f].rank().corr(d.loc[ok, "net_mk_10s_c"].rank())
        rows.append({"feature": f, "n": int(ok.sum()), "spearman_vs_net_10s": rho})
    return pd.DataFrame(rows)


# ============================================================================ cancel rule
@dataclass
class ToxicityRule:
    """Fitted logistic P(toxic) and the pull threshold (research object; picklable)."""

    model: Any
    features: tuple[str, ...]
    threshold: float
    train_fills: int = 0
    removed_share_train: float = 0.0
    gain_train_c: float = 0.0

    def score(self, row: dict[str, float]) -> float:
        x = np.nan_to_num(np.array([[row.get(f, math.nan) for f in self.features]], dtype=float))
        return float(self.model.predict_proba(x)[0, 1])


def fit_rule(train: pd.DataFrame, feats: Sequence[str] = FEATURES, max_removed: float = 0.20,
             seed: int = 0, min_fills: int = 25) -> ToxicityRule | None:
    """Logistic P(toxic) on training fills; threshold maximizing the training net 60 s markout
    (c/contract, after fees; less noisy than settlement P&L) of the kept fills with at most
    `max_removed` of fills (contract-weighted) removed."""
    d = train[train["toxic"].notna()]
    feats = tuple(f for f in feats if f in d and d[f].notna().any())
    if len(d) < min_fills or d["toxic"].nunique() < 2 or not feats:
        return None
    m = _make("logistic", seed)
    m.fit(_X(d, feats, True), d["toxic"].to_numpy())
    p = m.predict_proba(_X(d, feats, True))[:, 1]
    w = d["contracts"].to_numpy()
    v = (d["net_mk_60s_c"] if "net_mk_60s_c" in d and d["net_mk_60s_c"].notna().any() else d["net_mk_10s_c"]).to_numpy()
    ok = np.isfinite(v)
    base = float(np.sum(v[ok] * w[ok]) / np.sum(w[ok]))
    best = (1.01, 0.0, 0.0)
    for thr in np.unique(np.quantile(p, np.linspace(0.5, 0.99, 25))):
        keep = (p < thr) & ok
        removed = 1.0 - w[keep].sum() / w[ok].sum()
        if removed > max_removed or not keep.any():
            continue
        gain = float(np.sum(v[keep] * w[keep]) / np.sum(w[keep])) - base
        if gain > best[1]:
            best = (float(thr), gain, removed)
    return ToxicityRule(m, feats, best[0], len(d), best[2], best[1])


class ToxicityGuardMM(MarketMaker):
    """MarketMaker that pulls (cancels and does not place) a market side whose toxicity score for a
    fill of the current/candidate quote is >= rule.threshold. Research hook overriding the private
    per-market decision step (proposed public hook: a pre-admission quote filter)."""

    def __init__(self, cfg: StrategyConfig, specs, *, rule: ToxicityRule, **kw: Any) -> None:
        super().__init__(cfg, specs, **kw)
        self.rule = rule
        self.tox_state = ToxicityState()
        self.q_place: dict[str, float] = {}
        self.guard_stats = {"side_checks": 0, "side_pulls": 0}

    def on_event(self, ev):  # type: ignore[override]
        out = super().on_event(ev)
        self.tox_state.update(ev, self.ext)
        for a in out:
            if type(a) is PlaceOrder:
                self.q_place[a.client_order_id] = queue_at_place(self, a.ticker, a.book_side, a.px)
        return out

    def _quote_market(self, now, s, f, grid, ev, base, S, D, c_h, health):  # type: ignore[override]
        out, props = super()._quote_market(now, s, f, grid, ev, base, S, D, c_h, health)
        if self.rule.threshold > 1.0:
            return out, props
        b = self.books.get(s.ticker)
        if b is None or not b.valid:
            return out, props
        pulled = set()
        for side in ("bid", "ask"):
            ws = [w for w in self.om.working(s.ticker) if w.book_side == side and not w.cancel_requested and w.remaining_qty > 0]
            if ws:
                w = ws[0]
                px, q, age = w.px, self.q_place.get(w.client_order_id, math.nan), (now - w.created_ns) / NS_PER_S
            else:
                cand = [p for p in props if p[2] == side]
                if not cand:
                    continue
                px = cand[0][3].px
                q = queue_at_place(self, s.ticker, side, px)
                age = 0.0
            self.guard_stats["side_checks"] += 1
            if self.rule.score(features_at(self, self.tox_state, now, s.ticker, side, px, q, age)) >= self.rule.threshold:
                pulled.add(side)
                self.guard_stats["side_pulls"] += 1
                self.stats.bump("toxicity_pull")
                for w in ws:
                    out += self._cancel(now, w, "toxicity")
        if pulled:
            props = [p for p in props if p[2] not in pulled]
        return out, props


# ============================================================================ runner
def run(root: str | Path, t0: int, t1: int, out: str | Path, *, cfg: StrategyConfig | None = None,
        policies=("B", "C"), split_frac: float = 0.5, n_folds: int = 5, warm: str = "recorded",
        universe: Universe | None = None, live: bool = False, seed: int = 1, n_boot: int = 400,
        n_jobs: int = 1, progress=None) -> dict[str, Any]:
    Path(out).mkdir(parents=True, exist_ok=True)
    from dh.research.replay_grid import Variant, run_variants, run_warnings

    cfg = cfg or StrategyConfig()
    uni = universe or build_universe(root, t0, t1)
    t_split = t0 + int(split_frac * (t1 - t0))
    pols = [policy_letter(p) for p in policies]
    runs = run_variants(root, t0, t1, [Variant("baseline", cfg, postprocess=add_fill_markouts)], pols, universe=uni,
                        warm=warm, seed=seed, n_jobs=n_jobs, collectors=[ToxicityCollector], progress=progress)
    fills: dict[str, pd.DataFrame] = {r.policy: r.df for r in runs}
    metrics, uni_tabs, cancel_rows = [], [], []
    for L, df in fills.items():
        if len(df):
            m, _pred = walk_forward_models(df, n_folds=n_folds)
            m.insert(0, "policy", L)
            metrics.append(m)
            u = univariate_table(df)
            u.insert(0, "policy", L)
            uni_tabs.append(u)
    if live and uni.own_fills:
        lv = live_fill_table(root, t0, t1, cfg, uni, warm=warm)
        fills["live"] = lv
        if len(lv):
            m, _ = walk_forward_models(lv, n_folds=n_folds)
            m.insert(0, "policy", "live")
            metrics.append(m)
    # cancel rule: fit on [t0, t_split) shadow fills of policy B, replay on [t_split, t1)
    train_src = fills.get("B") if "B" in fills else next(iter(fills.values()), pd.DataFrame())
    rule = fit_rule(train_src[train_src["ts"] < t_split]) if len(train_src) else None
    if rule is not None and rule.threshold <= 1.0:
        ev_runs = run_variants(root, t_split, t1, [Variant("base", cfg),
                                                    Variant("guard", cfg, strategy_factory=ToxicityGuardMM,
                                                            factory_kwargs={"rule": rule})],
                               pols, universe=uni, warm=warm, seed=seed, n_jobs=n_jobs, progress=progress)
        by = {(r.variant, r.policy): r for r in ev_runs}
        for L in pols:
            base, guard = by[("base", L)], by[("guard", L)]
            a = base.df[base.df["settle"].notna()] if len(base.df) else base.df
            b = guard.df[guard.df["settle"].notna()] if len(guard.df) else guard.df
            d = paired_diff_ci(a, b, "net_c_per_ct", n_boot=n_boot) if len(a) and len(b) else None
            fa = float(a["contracts"].sum()) if len(a) else 0.0
            fb = float(b["contracts"].sum()) if len(b) else 0.0
            cancel_rows.append({"policy": L, "threshold": rule.threshold, "fills_base": len(a), "fills_guard": len(b),
                                "fill_loss_pct": 100.0 * (1 - fb / fa) if fa > 0 else math.nan,
                                "net_c_base": base.summary.get("net_c_per_contract", math.nan),
                                "net_c_guard": guard.summary.get("net_c_per_contract", math.nan),
                                "d_net_c": d.mean if d else math.nan, "d_net_lo_c": d.lo if d else math.nan,
                                "d_net_hi_c": d.hi if d else math.nan,
                                "markout_10s_base_c": base.summary.get("markout_10s_c", math.nan),
                                "markout_10s_guard_c": guard.summary.get("markout_10s_c", math.nan),
                                "usd_day_base": base.summary.get("net_usd_per_day", 0.0),
                                "usd_day_guard": guard.summary.get("net_usd_per_day", 0.0),
                                "events": n_events(a, b)})
    cancel = pd.DataFrame(cancel_rows)
    met = pd.concat(metrics, ignore_index=True) if metrics else pd.DataFrame()
    unit = pd.concat(uni_tabs, ignore_index=True) if uni_tabs else pd.DataFrame()
    mk_rows = []
    for L, df in fills.items():
        if not len(df):
            continue
        for h in [*DEFAULT_HORIZONS_S, "settle"]:
            col = f"mkpx_{h:g}s_c" if h != "settle" else "mkpx_settle_c"
            if col not in df:
                continue
            ok = df[df[col].notna()]
            ci = cluster_mean_ci(ok[col] - ok["fee_c"], ok["contracts"], ok["event"], n_boot) if len(ok) else None
            mk_rows.append({"policy": L, "horizon": f"{h:g}s" if h != "settle" else "settle", "fills": len(ok),
                            "net_markout_c": ci.mean if ci else math.nan, "lo_c": ci.lo if ci else math.nan,
                            "hi_c": ci.hi if ci else math.nan})
    mko = pd.DataFrame(mk_rows)
    rep = Report("e3_toxicity", "E3 — Predictability of fill toxicity and a toxicity-aware cancel rule", Path(out),
                 synthetic=uni.synthetic, rule=RULE_E3,
                 meta={"root": str(root), "window": f"{fmt_ns(t0)} .. {fmt_ns(t1)}", "rule_fit_until": fmt_ns(t_split),
                       **inputs_meta(uni, t0)[0],
                       "label": f"toxic = fair value moved against the fill within {LABEL_H_S:g}s",
                       "rule_threshold": rule.threshold if rule else "n/a (too few training fills)",
                       "rule_train_fills": rule.train_fills if rule else 0,
                       "rule_removed_share_train": rule.removed_share_train if rule else math.nan})
    rep.decision_events = int(cancel["events"].min()) if len(cancel) else None
    if len(cancel):
        good = all((r.d_net_lo_c > 0) and (r.d_net_c >= 0.1) and (r.fill_loss_pct <= 20.0) for r in cancel.itertuples())
        vanish_c = any(r.policy == "C" and not (r.d_net_lo_c > 0) for r in cancel.itertuples())
        rep.verdict = "ACCEPT" if good else ("REJECT (lift absent or vanishes under C)" if vanish_c else "INCONCLUSIVE")
    elif rule is not None:
        rep.verdict = "REJECT (no threshold improves the training markout within 20% fill loss)"
    else:
        rep.verdict = "INCONCLUSIVE (no cancel rule could be fitted: too few training fills)"
    for w in run_warnings(runs):
        rep.line(f"WARNING: {w}")
    rep.table("markouts", mko, "Net markout (vs fill price, after fees) of replayed shadow fills, c/contract, "
                               "event-bootstrap CI.")
    rep.table("models", met, "Walk-forward OOS metrics (folds = consecutive settlement events).")
    rep.table("univariate", unit, "Spearman correlation of each fill-time feature with the net 10 s markout.")
    rep.table("cancel_rule", cancel, "Replay of the toxicity-aware cancel rule on the held-out part vs baseline "
                                     "(paired by event).")
    for L, df in fills.items():
        if len(df):
            write_csv(df, Path(out) / f"e3_toxicity_fills_{L}.csv", uni.synthetic)
    rep.write()
    return {"fills": fills, "models": met, "cancel_rule": cancel, "markouts": mko, "rule": rule, "univariate": unit}
