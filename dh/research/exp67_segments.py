"""Experiments 6 and 7: where is the net edge? By time to expiry, |z| and YES price.

Input: ledger DataFrames of replays under fill policies B and C (A for reference only), or
ledger CSVs from earlier runs / live sessions (``--ledger``; columns ts, event, contracts,
net_c_per_ct, tau_s, px, fee, gross_edge_c and optionally z, mo_10s_c, mo_60s_c, policy).

Buckets (docs/TEST_MATRIX.md regime splits): tau (>30m, 10-30m, 5-10m, 1-5m, 30-60s, <30s);
|z| (0-0.5 ... >3; z of the strategy's last quote cycle before the fill); YES price (1-5c ...
95-99c). Per bucket: fills, contracts/day, fills/day, realized net c/contract with an event
bootstrap CI, gross edge, fees, markouts at 10 s / 60 s (vs the fair value at fill), toxic share
(net 10 s markout < 0), $/day.

Decision rule (E6/E7): quote only buckets whose net CI lower bound > 0; disable buckets whose CI
upper bound < 0 (here required under BOTH B and C to enable, under EITHER to disable);
everything else 'insufficient data'.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Sequence

import numpy as np
import pandas as pd

from dh.research.exp_common import (
    Report,
    flag_only_A,
    fmt_ns,
    price_bucket,
    segment_rows,
    tau_bucket,
    write_csv,
    z_bucket,
)
from dh.research.replay_grid import Variant, run_variants, run_warnings, settled
from dh.research.replay_env import Universe, build_universe, inputs_meta
from dh.strategy.config import StrategyConfig

RULE_E67 = ("quote only buckets whose realized net c/contract CI lower bound > 0 (under B and C); disable buckets "
            "whose CI upper bound < 0 (under B or C)")


def add_buckets(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()
    d["tau_bucket"] = tau_bucket(d["tau_s"]).to_numpy()
    d["z_bucket"] = z_bucket(d["z"]).to_numpy() if "z" in d else pd.Categorical([np.nan] * len(d))
    d["price_bucket"] = price_bucket(d["px"]).to_numpy()
    return d


def segment_tables(dfs: dict[str, pd.DataFrame], days: float, n_boot: int = 300) -> dict[str, pd.DataFrame]:
    out: dict[str, list[pd.DataFrame]] = {"tau": [], "z": [], "price": [], "tau_z": []}
    for pol, df in dfs.items():
        d = add_buckets(settled(df))
        if not len(d):
            continue
        for key, by in (("tau", ["tau_bucket"]), ("z", ["z_bucket"]), ("price", ["price_bucket"]),
                        ("tau_z", ["tau_bucket", "z_bucket"])):
            t = segment_rows(d, by, days, n_boot)
            if len(t):
                t.insert(0, "policy", pol)
                out[key].append(t)
    res = {}
    for k, parts in out.items():
        t = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
        if len(t):
            keys = [c for c in ("tau_bucket", "z_bucket", "price_bucket") if c in t]
            t = flag_only_A(t, keys)
            t = add_recommendation(t, keys)
        res[k] = t
    return res


def add_recommendation(t: pd.DataFrame, keys: Sequence[str]) -> pd.DataFrame:
    t = t.copy()
    t["recommendation"] = "insufficient data"
    for _, g in t.groupby(list(keys), observed=True, dropna=False):
        bc = g[g["policy"].isin(["B", "C"])]
        if not len(bc):
            continue
        rec = "insufficient data"
        if (bc["net_hi_c"] < 0).any():
            rec = "disable"
        elif len(bc) == bc["policy"].nunique() and bc["policy"].nunique() >= 1 and (bc["net_lo_c"] > 0).all():
            rec = "quote"
        t.loc[g.index, "recommendation"] = rec
    return t


def load_ledgers(paths: Sequence[str]) -> dict[str, pd.DataFrame]:
    out: dict[str, pd.DataFrame] = {}
    for p in paths:
        df = pd.read_csv(p)
        pol = str(df["policy"].iloc[0]) if "policy" in df and len(df) else Path(p).stem
        out[pol] = pd.concat([out[pol], df]) if pol in out else df
    return out


def run(root: str | Path | None, t0: int, t1: int, out: str | Path, *, cfg: StrategyConfig | None = None,
        policies=("A", "B", "C"), warm: str = "recorded", n_jobs: int = 1, universe: Universe | None = None,
        ledgers: Sequence[str] = (), seed: int = 1, progress=None) -> dict[str, Any]:
    Path(out).mkdir(parents=True, exist_ok=True)
    cfg = cfg or StrategyConfig()
    synthetic = False
    warns: list[str] = []
    imeta: dict[str, Any] = {"FV parameters": "as recorded in the given ledgers"}
    if ledgers:
        dfs = load_ledgers(ledgers)
        days = max((t1 - t0) / 86_400e9, 1e-9)
    else:
        uni = universe or build_universe(root, t0, t1)
        synthetic = uni.synthetic
        runs = run_variants(root, t0, t1, [Variant("config", cfg)], policies, universe=uni, warm=warm, seed=seed,
                            n_jobs=n_jobs, progress=progress)
        dfs = {r.policy: r.df for r in runs}
        warns = run_warnings(runs)
        imeta = inputs_meta(uni, t0)[0]
        days = runs[0].summary["days"] if runs else 1.0
    tabs = segment_tables(dfs, days)
    rep = Report("e67_segments", "E6/E7 — Net edge by time to expiry, |z| and YES price", Path(out),
                 synthetic=synthetic, rule=RULE_E67,
                 meta={"source": ", ".join(ledgers) if ledgers else str(root), "window": f"{fmt_ns(t0)} .. {fmt_ns(t1)}",
                       "policies": ",".join(dfs), "days": days, **imeta})
    tau = tabs.get("tau", pd.DataFrame())
    if len(tau):
        q = sorted(set(tau.loc[tau.recommendation == "quote", "tau_bucket"].astype(str)))
        dis = sorted(set(tau.loc[tau.recommendation == "disable", "tau_bucket"].astype(str)))
        rep.verdict = f"tau buckets to quote: {q or 'none'}; to disable: {dis or 'none'}"
    else:
        rep.verdict = "INCONCLUSIVE (no settled fills)"
    for w in warns:
        rep.line(f"WARNING: {w}")
    rep.table("tau", tabs.get("tau"), "E6: by time to expiry at fill.")
    rep.table("abs_z", tabs.get("z"), "E7: by |z| of the strategy's last quote cycle before the fill.")
    rep.table("yes_price", tabs.get("price"), "E7: by YES price of the fill.")
    rep.table("tau_x_z", tabs.get("tau_z"), "Joint split (small cells are noisy).")
    rep.write()
    for pol, df in dfs.items():
        if len(df):
            write_csv(df, Path(out) / f"e67_segments_ledger_{pol}.csv", synthetic)
    return tabs
