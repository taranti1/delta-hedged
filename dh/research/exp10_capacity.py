"""Experiment 10: capacity. How does net edge decay with quote size?

The production MarketMaker is replayed with clip size x k (default k in 1, 2, 5, 10, 20, 50) under
fill policies B and C. Size-denominated risk limits scale with k (``--no-scale-limits`` keeps them
fixed) so the limits do not cap size before the market does. Queue dilution is the simulator's:
our larger orders wait behind the same displayed queue and only fill from prints beyond it; other
participants do not react to us (no market impact: capacity is therefore an UPPER bound).

Per k x policy: net c/contract (event CI), fills/day, contracts/day, $/day, share of the taker
flow in the quoted markets (our filled contracts / recorded public traded contracts: in the
simulator our maker fills are carved out of the recorded prints), inventory sd (net YES
position, 1 s samples), mean |position|.
Capacity (per policy): the largest clip multiple (log-linear interpolation on the grid) at which
net c/contract stays above 1.0, 0.75, 0.5, 0.05 c and 0 (breakeven), with contracts/day and $/day
at that size. docs/TEST_MATRIX.md E10: measurement only (no accept/reject).
"""

from __future__ import annotations

import math
from pathlib import Path
from typing import Any, Sequence

import numpy as np
import pandas as pd

from dh.research.exp_common import Report, fmt_ns
from dh.research.replay_env import PortfolioSampler, Universe, build_universe
from dh.research.replay_grid import Variant, interp_capacity, run_variants, run_warnings, scaled_cfg, variant_table
from dh.strategy.config import StrategyConfig

RULE_E10 = "measurement: report capacity at > 1.0, 0.75, 0.5, 0.05 c/contract and breakeven under B and C"
LEVELS_C = (1.0, 0.75, 0.5, 0.05, 0.0)


def run(root: str | Path, t0: int, t1: int, out: str | Path, *, cfg: StrategyConfig | None = None,
        policies=("B", "C"), multipliers: Sequence[float] = (1, 2, 5, 10, 20, 50), scale_limits: bool = True,
        warm: str = "recorded", n_jobs: int = 1, universe: Universe | None = None, seed: int = 1,
        progress=None) -> dict[str, Any]:
    Path(out).mkdir(parents=True, exist_ok=True)
    cfg = cfg or StrategyConfig()
    uni = universe or build_universe(root, t0, t1)
    variants = [Variant(f"x{k:g}", scaled_cfg(cfg, float(k), scale_limits)) for k in multipliers]
    runs = run_variants(root, t0, t1, variants, policies, universe=uni, warm=warm, seed=seed, n_jobs=n_jobs,
                        collectors=[PortfolioSampler], progress=progress)
    tab = variant_table(runs, ref=variants[0].name)
    extra = []
    for r in runs:
        ps = r.collectors[0] if r.collectors else pd.DataFrame()
        ours = float(r.df["contracts"].sum()) if len(r.df) else 0.0
        pub = float(r.summary.get("public_contracts_quoted", 0.0))
        extra.append({"variant": r.variant, "policy": r.policy, "clip_multiple": float(r.variant[1:]),
                      "clip_contracts": cfg.quoting.clip_contracts * float(r.variant[1:]),
                      # our simulated fills are carved out of the recorded public prints (queue model):
                      # the recorded taker volume is the whole flow
                      "flow_share": ours / pub if pub > 0 else math.nan,
                      "inventory_sd_ct": float(ps["net_position_ct"].std()) if len(ps) > 1 else 0.0,
                      "mean_abs_position_ct": float(ps["abs_position_ct"].mean()) if len(ps) else 0.0})
    tab = tab.merge(pd.DataFrame(extra), on=["variant", "policy"], how="left")
    cap_rows = []
    for p in sorted(tab["policy"].unique()):
        t = tab[tab.policy == p].sort_values("clip_multiple")
        for lvl in LEVELS_C:
            for col, label in (("net_c", "point"), ("net_lo_c", "ci_lower")):
                k = interp_capacity(t["clip_multiple"], t[col], lvl)
                row = {"policy": p, "level_c": lvl, "basis": label, "max_clip_multiple": k,
                       "max_clip_contracts": k * cfg.quoting.clip_contracts if math.isfinite(k) else math.nan}
                if math.isfinite(k):
                    for c in ("contracts_per_day", "usd_per_day"):
                        row[c] = float(np.interp(math.log(k), np.log(t["clip_multiple"]), t[c]))
                cap_rows.append(row)
    cap = pd.DataFrame(cap_rows)
    rep = Report("e10_capacity", "E10 — Capacity: net edge vs quote size", Path(out), synthetic=uni.synthetic,
                 rule=RULE_E10, meta={"root": str(root), "window": f"{fmt_ns(t0)} .. {fmt_ns(t1)}",
                                      "base_clip_contracts": cfg.quoting.clip_contracts,
                                      "limits_scaled_with_size": scale_limits})
    bk = cap[(cap.level_c == 0.0) & (cap.basis == "point")]
    rep.verdict = "MEASUREMENT — breakeven clip multiple: " + ", ".join(
        f"{r.policy}: {r.max_clip_multiple:.3g}" for r in bk.itertuples()) if len(bk) else "MEASUREMENT"
    for w in run_warnings(runs):
        rep.line(f"WARNING: {w}")
    rep.table("by_size", tab, "Per clip multiple x policy (upper bound: no market impact modeled).")
    rep.table("capacity", cap, "Largest clip multiple with net c/contract above each level (point estimate and "
                               "CI lower bound); contracts/day and $/day interpolated at that size.")
    rep.write()
    return {"table": tab, "capacity": cap, "runs": runs}
