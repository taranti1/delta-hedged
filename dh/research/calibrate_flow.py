"""Calibrate the M1 fill-intensity model (taker flow per segment) from public Kalshi trades.

Taker orders are reconstructed from the trade tape: prints with the same
(ticker, ts_ms, taker_side) are one aggressive order sweeping one or more levels.
Segments = (tau bucket, |z| bucket, maker side), matching dh.strategy.fill_model.segment_key.

Exposure (market-seconds per segment) is EXACT in time to expiry: each 60 s cell of a market's
open window is split at the tau-bucket boundaries (30/60/300/600/1800 s), so every bucket gets
its true duration (audit M3). |z| is evaluated causally at each cell start from the BTC
reference (price at or before t; cells before the first print are skipped: audit minor 7).

Rates use gamma-Poisson shrinkage toward the pooled rate of the same (tau bucket, side)
(audit M4), so thin segments (far strikes) are estimated from their own exposure instead of
silently inheriting a global default:
    order_rate(seg) = (N_seg + r_pool * prior_s) / (E_seg + prior_s)
    contract_rate   = order_rate * size_mean(seg or pooled if N_seg < min_orders)
Pooled per-side defaults are returned under keys ('*', '*', side) for unseen segments.

BTC reference (causal, audit): ``btc.ts_ms`` is the OPEN time of ``btc_bar_ms`` bars (default
60 000: Bitstamp 1-minute OHLC closes), so a bar's price is used only from its close
(kalshi_data.btc_price_asof convention); ``btc_bar_ms = 0`` for point-in-time prices (BRTI ticks).

Out-of-sample grading (audit): ``calibrate`` is an in-sample fit. ``calibrate_split`` fits on the
markets that expire before a chronological split (default: the first 70 % of expirations) and
evaluates on the later ones; ``walk_forward_by_day`` refits on all days before each test day.
Both report in-sample AND out-of-sample predicted vs realized taker flow per segment
(``evaluate_flow``/``flow_metrics``: contracts ratio, WAPE, Poisson deviance of order counts vs a
pooled-per-side null). Splits are market-disjoint and causal: every training market expired
before the split, so the parameters use only data from before it; with ``purge`` (default) the
test markets' data before the split are dropped as well. Segments fitted this way are what a
replay may use (``save_segments``/``load_segments``; dh.research.flow_recording fits them from a
recording strictly before the replay's t0).

CLI:  python -m dh.research.calibrate_flow --trades T.parquet --markets M.parquet --btc B.parquet
          --out DIR [--split 0.7 | --walk-forward-days 1] [--btc-bar-ms 60000]
"""

from __future__ import annotations

import argparse
import json
import math
from collections import defaultdict
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from dh.research.kalshi_data import DEFAULT_BTC_BAR_MS, normalize_markets, normalize_trades
from dh.strategy.fill_model import SegmentFlow, segment_key

SEC_YR = 365.0 * 24 * 3600
TAU_BOUNDS = (30.0, 60.0, 300.0, 600.0, 1800.0)


def taker_orders(trades: pd.DataFrame) -> pd.DataFrame:
    """Group prints into taker orders: ticker, ts_ms, taker_side, contracts, n_prints, yes_px."""
    t = trades.copy()
    t["contracts"] = t.qty / 100.0
    g = t.groupby(["ticker", "ts_ms", "taker_side"], sort=True).agg(
        contracts=("contracts", "sum"), n_prints=("contracts", "size"), yes_px=("yes_px", "first"))
    return g.reset_index()


def _z(S: float, K: float, tau_s: float, vol_ann: float) -> float:
    sd = S * vol_ann * math.sqrt(max(tau_s - 40.0, 20.0) / SEC_YR)
    return abs(K - S) / sd


def split_tau(tau_hi: float, tau_lo: float) -> list[tuple[float, float]]:
    """Split the tau interval (tau_lo, tau_hi] at bucket boundaries -> [(duration, tau_mid)]."""
    cuts = [tau_lo] + [b for b in TAU_BOUNDS if tau_lo < b < tau_hi] + [tau_hi]
    return [(b - a, 0.5 * (a + b)) for a, b in zip(cuts, cuts[1:]) if b > a]


def exposure_seconds(markets: pd.DataFrame, price_at, vol_ann: float, grid_s: int = 60,
                     t_min_ms: int | None = None) -> dict:
    """Market-seconds per segment; cells start at the market open (or ``t_min_ms`` if later)."""
    exposure: dict[tuple[str, str, str], float] = defaultdict(float)
    for m in markets.itertuples():
        K = m.floor_strike if not pd.isna(m.floor_strike) else m.cap_strike
        if pd.isna(K):
            continue
        start, end = int(m.open_ts_ms), int(m.expiration_ts_ms)
        if t_min_ms is not None:
            start = max(start, int(t_min_ms))
        for t in range(start, end, grid_s * 1000):
            S = price_at(t)
            if S is None:
                continue
            t_end = min(t + grid_s * 1000, end)
            tau_hi, tau_lo = (end - t) / 1000.0, (end - t_end) / 1000.0
            z = _z(S, float(K), tau_hi, vol_ann)
            for dur, tau_mid in split_tau(tau_hi, tau_lo):
                for side in ("bid", "ask"):
                    exposure[segment_key(tau_mid, z, side)] += dur
    return exposure


def _price_fn(btc: pd.DataFrame, btc_bar_ms: int):
    """Causal scalar lookup (kalshi_data.btc_price_asof convention)."""
    avail = (btc["close_ts_ms"] if "close_ts_ms" in btc else btc["ts_ms"] + int(btc_bar_ms)).to_numpy(dtype=np.int64)
    px = btc["price"].to_numpy(dtype=float)
    order = np.argsort(avail, kind="stable")
    b_ts, b_px = avail[order], px[order]

    def price_at(ts_ms: int) -> float | None:
        i = int(np.searchsorted(b_ts, ts_ms, side="right")) - 1
        return float(b_px[i]) if i >= 0 else None

    return price_at


def order_sizes(orders: pd.DataFrame, price_at, vol_ann: float, t_min_ms: int | None = None) -> dict:
    """Taker-order sizes (contracts) per segment of the maker side they fill. ``orders`` =
    taker_orders(...) merged with the market columns (expiration_ts_ms, floor/cap strike)."""
    sizes: dict[tuple[str, str, str], list[float]] = defaultdict(list)
    for r in orders.itertuples():
        if t_min_ms is not None and r.ts_ms < t_min_ms:
            continue
        K = r.floor_strike if not pd.isna(r.floor_strike) else r.cap_strike
        S = price_at(int(r.ts_ms))
        if S is None or pd.isna(K):
            continue
        tau = (r.expiration_ts_ms - r.ts_ms) / 1000.0
        if tau <= 0:
            continue
        z = _z(S, float(K), tau, vol_ann)
        # a taker selling YES ('no') hits resting YES bids -> fills makers' BIDS
        maker_side = "bid" if r.taker_side == "no" else "ask"
        sizes[segment_key(tau, z, maker_side)].append(float(r.contracts))
    return sizes


@dataclass
class FlowStats:
    """Additive sufficient statistics of a sample: exposure seconds and order sizes per segment."""

    exposure: dict[tuple[str, str, str], float] = field(default_factory=lambda: defaultdict(float))
    sizes: dict[tuple[str, str, str], list[float]] = field(default_factory=lambda: defaultdict(list))
    markets: int = 0

    def add(self, other: FlowStats) -> None:
        for k, v in other.exposure.items():
            self.exposure[k] += v
        for k, v in other.sizes.items():
            self.sizes[k].extend(v)
        self.markets += other.markets


def _prepared(trades: pd.DataFrame, markets: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    markets = normalize_markets(markets)
    orders = taker_orders(normalize_trades(trades))
    orders = orders.merge(markets[["ticker", "expiration_ts_ms", "floor_strike", "cap_strike"]], on="ticker")
    return orders, markets


def flow_stats(orders: pd.DataFrame, markets: pd.DataFrame, price_at, vol_ann: float = 0.40, grid_s: int = 60,
               t_min_ms: int | None = None) -> FlowStats:
    """FlowStats of ``markets`` (and the orders in them), optionally only from ``t_min_ms`` on."""
    o = orders[orders.ticker.isin(set(markets.ticker))]
    return FlowStats(exposure=exposure_seconds(markets, price_at, vol_ann, grid_s, t_min_ms),
                     sizes=order_sizes(o, price_at, vol_ann, t_min_ms), markets=len(markets))


def fit_segments(stats: FlowStats, min_orders: int = 30, prior_s: float = 1800.0) -> dict[tuple[str, str, str], SegmentFlow]:
    """Gamma-Poisson-shrunk segment rates from sufficient statistics (module docstring)."""
    exposure, sizes = stats.exposure, stats.sizes

    def pooled(pred) -> tuple[float, list[float]]:
        E = sum(v for k, v in exposure.items() if pred(k))
        s = [x for k, v in sizes.items() if pred(k) for x in v]
        return (len(s) / E if E > 0 else 0.0), s

    def fit_sizes(arr: list[float]) -> tuple[float, float]:
        a = np.asarray(arr, dtype=float)
        mean = float(a.mean())
        cv = float(a.std(ddof=1) / mean) if len(a) > 1 and mean > 0 else 1.0
        return mean, max(cv, 0.1)

    out: dict[tuple[str, str, str], SegmentFlow] = {}
    for side in ("bid", "ask"):
        r_all, s_all = pooled(lambda k, sd=side: k[2] == sd)
        if s_all:
            mean, cv = fit_sizes(s_all)
            out[("*", "*", side)] = SegmentFlow(r_all * mean, mean, cv)
    for key, E in exposure.items():
        if E <= 0:
            continue
        tb, _, side = key
        r_pool, s_pool = pooled(lambda k, tb=tb, sd=side: k[0] == tb and k[2] == sd)
        n = len(sizes.get(key, []))
        order_rate = (n + r_pool * prior_s) / (E + prior_s)
        src = sizes.get(key, []) if n >= min_orders else (s_pool or [x for v in sizes.values() for x in v])
        if not src:
            continue
        mean, cv = fit_sizes(src)
        out[key] = SegmentFlow(rate_contracts_per_s=order_rate * mean, size_mean=mean, size_cv=cv)
    return out


def calibrate(
    trades: pd.DataFrame,
    markets: pd.DataFrame,
    btc: pd.DataFrame,
    vol_ann: float = 0.40,
    grid_s: int = 60,
    min_orders: int = 30,
    prior_s: float = 1800.0,
    btc_bar_ms: int = DEFAULT_BTC_BAR_MS,
) -> dict[tuple[str, str, str], SegmentFlow]:
    """trades/markets: downloader schema or minimal schema (dh.research.kalshi_data);
    btc: ts_ms (bar OPEN time of btc_bar_ms bars; 0 = point-in-time prices), price, optional
    close_ts_ms; joined causally (a bar is used only once closed). Returns {segment_key:
    SegmentFlow} incl. ('*','*',side) pooled defaults. In-sample fit: use ``calibrate_split``
    to grade it out of sample before feeding it to replays (E3/E4)."""
    orders, markets = _prepared(trades, markets)
    stats = flow_stats(orders, markets, _price_fn(btc, btc_bar_ms), vol_ann, grid_s)
    return fit_segments(stats, min_orders, prior_s)


# ============================================================================ out-of-sample grading
def evaluate_flow(seg: Mapping[tuple[str, str, str], SegmentFlow], stats: FlowStats) -> pd.DataFrame:
    """Predicted vs realized taker flow per segment of an evaluation sample.

    predicted contracts = rate_contracts_per_s x exposure seconds (unseen segments use the pooled
    ('*','*',side) entry, as the fill model does); predicted orders = order rate x exposure;
    ``pooled_orders`` is the same prediction from the pooled per-side rate only (null model)."""
    rows = []
    keys = set(stats.exposure) | set(stats.sizes)
    for key in sorted(keys):
        E = float(stats.exposure.get(key, 0.0))
        sz = stats.sizes.get(key, [])
        f = seg.get(key)
        src = "segment"
        if f is None:
            f, src = seg.get(("*", "*", key[2])), "pooled"
        null = seg.get(("*", "*", key[2]))
        rate = f.rate_contracts_per_s if f is not None else 0.0
        orate = f.order_rate if f is not None else 0.0
        rows.append({"tau_b": key[0], "z_b": key[1], "side": key[2], "exposure_s": E, "orders": len(sz),
                     "realized_ct": float(sum(sz)), "pred_ct": rate * E, "pred_orders": orate * E,
                     "pooled_orders": (null.order_rate if null is not None else 0.0) * E,
                     "source": src if f is not None else "none"})
    return pd.DataFrame(rows, columns=["tau_b", "z_b", "side", "exposure_s", "orders", "realized_ct", "pred_ct",
                                       "pred_orders", "pooled_orders", "source"])


def _poisson_dev(y: np.ndarray, mu: np.ndarray) -> float:
    mu = np.maximum(mu, 1e-9)
    t = np.where(y > 0, y * np.log(np.where(y > 0, y, 1.0) / mu), 0.0)
    return float(2.0 * np.sum(t - (y - mu)))


def flow_metrics(tab: pd.DataFrame) -> dict[str, Any]:
    """Aggregate calibration of an ``evaluate_flow`` table: contracts ratio (pred/realized), WAPE
    over segments, Poisson deviance of order counts (model and pooled null) and the share of the
    null deviance the segmentation explains (<= 0: segments do not beat the pooled rate)."""
    if tab is None or not len(tab):
        return {"segments": 0, "exposure_h": 0.0, "orders": 0, "realized_ct": 0.0, "pred_ct": 0.0,
                "ratio_ct": math.nan, "wape_ct": math.nan, "dev_model": math.nan, "dev_pooled": math.nan,
                "dev_explained": math.nan}
    y = tab.orders.to_numpy(dtype=float)
    real, pred = float(tab.realized_ct.sum()), float(tab.pred_ct.sum())
    d_m = _poisson_dev(y, tab.pred_orders.to_numpy(dtype=float))
    d_0 = _poisson_dev(y, tab.pooled_orders.to_numpy(dtype=float))
    return {"segments": int(len(tab)), "exposure_h": float(tab.exposure_s.sum() / 3600.0), "orders": int(y.sum()),
            "realized_ct": real, "pred_ct": pred, "ratio_ct": pred / real if real > 0 else math.nan,
            "wape_ct": float((tab.pred_ct - tab.realized_ct).abs().sum() / real) if real > 0 else math.nan,
            "dev_model": d_m, "dev_pooled": d_0, "dev_explained": 1.0 - d_m / d_0 if d_0 > 0 else math.nan}


@dataclass
class FlowSplit:
    """Result of a time-split calibration (see calibrate_split / walk_forward_by_day)."""

    segments: dict[tuple[str, str, str], SegmentFlow]  # fitted on the training period only (graded OOS)
    metrics: pd.DataFrame  # one row per sample: in_sample / out_of_sample (+ per test day)
    per_segment: pd.DataFrame  # evaluate_flow rows with a 'sample' column
    split_ms: int | None = None
    train_end_ms: int | None = None  # every training datum is before this time
    segments_all: dict[tuple[str, str, str], SegmentFlow] | None = None  # every market of the sample
    all_end_ms: int | None = None  # every datum of segments_all is before this time (use for later replays)
    meta: dict[str, Any] = field(default_factory=dict)


def calibrate_split(trades: pd.DataFrame, markets: pd.DataFrame, btc: pd.DataFrame, *, train_frac: float = 0.7,
                    split_ms: int | None = None, purge: bool = True, vol_ann: float = 0.40, grid_s: int = 60,
                    min_orders: int = 30, prior_s: float = 1800.0, btc_bar_ms: int = DEFAULT_BTC_BAR_MS) -> FlowSplit:
    """Chronological split by market expiration: fit on markets expiring at or before ``split_ms``
    (default: the ``train_frac`` quantile of expirations), evaluate in sample (training markets)
    and out of sample (markets expiring later; with ``purge`` only their data from ``split_ms``
    on). Training data all precede the split, so the out-of-sample numbers are causal."""
    orders, mk = _prepared(trades, markets)
    price_at = _price_fn(btc, btc_bar_ms)
    exp = np.sort(mk.expiration_ts_ms.to_numpy(dtype=np.int64))
    if split_ms is None:
        if not len(exp):
            raise ValueError("no markets")
        split_ms = int(exp[min(len(exp) - 1, max(0, int(math.ceil(train_frac * len(exp))) - 1))])
    train_m = mk[mk.expiration_ts_ms <= split_ms]
    test_m = mk[mk.expiration_ts_ms > split_ms]
    st_tr = flow_stats(orders, train_m, price_at, vol_ann, grid_s)
    st_te_full = flow_stats(orders, test_m, price_at, vol_ann, grid_s)
    st_te = flow_stats(orders, test_m, price_at, vol_ann, grid_s, t_min_ms=split_ms) if purge else st_te_full
    seg = fit_segments(st_tr, min_orders, prior_s)
    tabs = {"in_sample": evaluate_flow(seg, st_tr), "out_of_sample": evaluate_flow(seg, st_te)}
    rows = [{"sample": k, "fit_on": "train", "eval_on": "train" if k == "in_sample" else "test",
             "markets": st_tr.markets if k == "in_sample" else st_te.markets, **flow_metrics(t)} for k, t in tabs.items()]
    per = pd.concat([t.assign(sample=k) for k, t in tabs.items()], ignore_index=True)
    st_all = FlowStats()
    st_all.add(st_tr)
    st_all.add(st_te_full)
    return FlowSplit(seg, pd.DataFrame(rows), per, split_ms=int(split_ms), train_end_ms=int(split_ms),
                     segments_all=fit_segments(st_all, min_orders, prior_s),
                     all_end_ms=int(exp[-1]) if len(exp) else None,
                     meta={"method": "chronological", "train_frac": train_frac, "purge": purge,
                           "train_markets": st_tr.markets, "test_markets": st_te.markets, "btc_bar_ms": btc_bar_ms})


def walk_forward_by_day(trades: pd.DataFrame, markets: pd.DataFrame, btc: pd.DataFrame, *, min_train_days: int = 1,
                        purge: bool = True, vol_ann: float = 0.40, grid_s: int = 60, min_orders: int = 30,
                        prior_s: float = 1800.0, btc_bar_ms: int = DEFAULT_BTC_BAR_MS) -> FlowSplit:
    """Walk-forward by UTC day of market expiration: for each day after ``min_train_days`` days,
    fit on every market that expired on an earlier day and evaluate on that day's markets (with
    ``purge``, only their data from the day start). Rows: each test day, the pooled out-of-sample
    total, and the in-sample fit on all days for comparison. ``segments`` = fit on all days."""
    orders, mk = _prepared(trades, markets)
    price_at = _price_fn(btc, btc_bar_ms)
    day_ms = 86_400_000
    mk = mk.assign(_day=(mk.expiration_ts_ms - 1) // day_ms)
    days = sorted(mk._day.unique())
    full: dict[int, FlowStats] = {}
    test: dict[int, FlowStats] = {}
    for d in days:
        m = mk[mk._day == d]
        full[d] = flow_stats(orders, m, price_at, vol_ann, grid_s)
        test[d] = flow_stats(orders, m, price_at, vol_ann, grid_s, t_min_ms=int(d * day_ms)) if purge else full[d]
    rows, per = [], []
    cum = FlowStats()
    oos_tabs = []
    for i, d in enumerate(days):
        if i >= min_train_days and cum.markets:
            seg = fit_segments(cum, min_orders, prior_s)
            tab = evaluate_flow(seg, test[d])
            day = pd.Timestamp(int(d * day_ms), unit="ms", tz="UTC").strftime("%Y-%m-%d")
            rows.append({"sample": f"oos_day {day}", "fit_on": f"{i} earlier day(s)", "eval_on": day,
                         "markets": test[d].markets, **flow_metrics(tab)})
            oos_tabs.append(tab.assign(sample="out_of_sample", day=day))
        cum.add(full[d])
    seg_all = fit_segments(cum, min_orders, prior_s)
    tab_is = evaluate_flow(seg_all, cum)
    if oos_tabs:
        oos = pd.concat(oos_tabs, ignore_index=True)
        rows.append({"sample": "out_of_sample", "fit_on": "walk-forward (earlier days)", "eval_on": "each later day",
                     "markets": int(sum(test[d].markets for d in days[min_train_days:])), **flow_metrics(oos)})
        per.append(oos)
    rows.append({"sample": "in_sample", "fit_on": "all days", "eval_on": "all days", "markets": cum.markets,
                 **flow_metrics(tab_is)})
    per.append(tab_is.assign(sample="in_sample"))
    end = int(mk.expiration_ts_ms.max()) if len(mk) else None
    return FlowSplit(seg_all, pd.DataFrame(rows), pd.concat(per, ignore_index=True), split_ms=None, train_end_ms=end,
                     segments_all=seg_all, all_end_ms=end,
                     meta={"method": "walk_forward_day", "min_train_days": min_train_days, "purge": purge,
                           "days": len(days), "btc_bar_ms": btc_bar_ms})


# ============================================================================ persistence
def save_segments(seg: Mapping[tuple[str, str, str], SegmentFlow], path: str | Path, *,
                  meta: Mapping[str, Any] | None = None) -> Path:
    """JSON {"meta": {...}, "segments": {"tau|z|side": {rate_contracts_per_s, size_mean, size_cv}}}.
    ``meta.fit_end_ms`` (every training datum precedes it) lets a replay check causality."""
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    body = {"meta": dict(meta or {}),
            "segments": {"|".join(k): {"rate_contracts_per_s": f.rate_contracts_per_s, "size_mean": f.size_mean,
                                       "size_cv": f.size_cv} for k, f in sorted(seg.items())}}
    p.write_text(json.dumps(body, indent=1, sort_keys=True, default=str))
    return p


def load_segments(path: str | Path) -> tuple[dict[tuple[str, str, str], SegmentFlow], dict[str, Any]]:
    body = json.loads(Path(path).read_text())
    seg = {tuple(k.split("|")): SegmentFlow(float(v["rate_contracts_per_s"]), float(v["size_mean"]), float(v["size_cv"]))
           for k, v in body.get("segments", {}).items()}
    return seg, dict(body.get("meta") or {})  # type: ignore[return-value]


def write_split_report(res: FlowSplit, out: str | Path, *, title: str = "Flow calibration (time split)",
                       note: str = "", synthetic: bool = False) -> Path:
    """flow_metrics.csv, flow_segments_eval.csv, flow_calibration.md (in-sample vs out-of-sample
    rows), flow_segments.json (fit on the WHOLE sample: for replays that start after
    ``all_end_ms``) and flow_segments_train.json (the graded training fit, ``train_end_ms``)."""
    from dh.research.exp_common import SYNTHETIC_BANNER, markdown_table, write_csv

    out = Path(out)
    out.mkdir(parents=True, exist_ok=True)
    write_csv(res.metrics, out / "flow_metrics.csv", synthetic)
    write_csv(res.per_segment, out / "flow_segments_eval.csv", synthetic)
    save_segments(res.segments_all if res.segments_all is not None else res.segments, out / "flow_segments.json",
                  meta={**res.meta, "fit_end_ms": res.all_end_ms if res.segments_all is not None else res.train_end_ms,
                        "fit_on": "whole sample", "synthetic": synthetic})
    save_segments(res.segments, out / "flow_segments_train.json",
                  meta={**res.meta, "fit_end_ms": res.train_end_ms, "split_ms": res.split_ms, "fit_on": "training period",
                        "synthetic": synthetic})
    lines = [f"# {title}", ""]
    if synthetic:
        lines += [f"**{SYNTHETIC_BANNER}**", ""]
    if note:
        lines += [note, ""]
    lines += [f"Method: {res.meta.get('method')}; training data end (UTC ms): {res.train_end_ms}; whole-sample fit "
              f"(flow_segments.json) data end: {res.all_end_ms} -- use it only for replays that start later. Rows `in_sample` grade "
              "the fit on its own training data; `out_of_sample` rows grade it on later data only. ratio_ct = predicted / "
              "realized taker contracts; wape_ct = sum |pred - realized| / realized over segments; dev_explained = 1 - "
              "Poisson deviance(model) / deviance(pooled per-side rate) of order counts.", "",
              markdown_table(res.metrics), ""]
    md = out / "flow_calibration.md"
    md.write_text("\n".join(lines))
    return md


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--trades", required=True)
    ap.add_argument("--markets", required=True)
    ap.add_argument("--btc", required=True, help="ts_ms (bar open), price[, close_ts_ms]")
    ap.add_argument("--btc-bar-ms", type=int, default=DEFAULT_BTC_BAR_MS, help="0 = point-in-time prices")
    ap.add_argument("--out", required=True)
    ap.add_argument("--split", type=float, default=0.7, help="chronological train fraction of market expirations")
    ap.add_argument("--walk-forward-days", type=int, default=0, help="> 0: walk-forward by day instead of one split")
    ap.add_argument("--vol-ann", type=float, default=0.40)
    a = ap.parse_args(argv)
    rd: Callable[[str], pd.DataFrame] = pd.read_parquet
    tr, mk, bt = rd(a.trades), rd(a.markets), rd(a.btc)
    if a.walk_forward_days > 0:
        res = walk_forward_by_day(tr, mk, bt, min_train_days=a.walk_forward_days, vol_ann=a.vol_ann, btc_bar_ms=a.btc_bar_ms)
    else:
        res = calibrate_split(tr, mk, bt, train_frac=a.split, vol_ann=a.vol_ann, btc_bar_ms=a.btc_bar_ms)
    md = write_split_report(res, a.out)
    print(res.metrics.to_string(index=False))
    print(f"wrote {md}")


if __name__ == "__main__":
    main()
