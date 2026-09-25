"""Experiment 9: does quoting many strikes improve netting and capital efficiency?

Paired replays of the production MarketMaker quoting, per settlement event, the 1 or 3 strikes
nearest the benchmark at the event's first quotable time (causal: dh.research.replay_env.
NearestStrikes; the strategy receives only those specs) versus all strikes, under fill policies B
and C.

Per variant x policy: net $/day, realized net c/contract (event CI), contracts/day, and from a
1 s portfolio sampler: peak and mean collateral ($ tied in open positions), mean |portfolio delta|
(BTC), mean gross delta sum|q_i delta_i| and the netting ratio mean|D| / mean gross, delta turnover
sum|dD| per filled contract (what a continuous hedger would have to trade; the M1 hedge is off).

Decision rule (docs/TEST_MATRIX.md E9): accept multi-strike quoting if $/day is up AND delta
(hedge) turnover per contract is down vs the single best strike; otherwise no improvement.
Inference (audit M8): paired CIs vs the reference under BOTH B and C: net $/day difference over
settlement events (expirations), delta turnover per contract as a paired ratio over 1 h time
blocks; a variant improves if both CIs exclude 0 in the right direction under B and C, with Holm
across the variants (p = max of the two one-sided p-values). Regime splits: tau bucket, vol
tercile, weekday.
"""

from __future__ import annotations

import math
from pathlib import Path
from typing import Any, Sequence

import numpy as np
import pandas as pd

from dh.research.exp_common import CI, Report, fmt_ns, holm, paired_ratio_ci, sum_diff_ci
from dh.research.replay_env import (
    NearestStrikes,
    PortfolioSampler,
    Universe,
    build_universe,
    describe_latency,
    inputs_meta,
    inputs_status,
    research_latency,
)
from dh.research.replay_grid import (
    GridRun,
    Variant,
    paired_regimes,
    policies_with_results,
    run_variants,
    run_warnings,
    settled,
    variant_table,
)
from dh.strategy.config import StrategyConfig

RULE_E9 = ("accept if $/day is up and hedge (delta) turnover per contract is down vs the single best strike (paired "
           "CIs under B and C, Holm across variants)")
BLOCK_NS = 3600 * 10**9


def turnover_blocks(ps: pd.DataFrame | None, fills: pd.DataFrame, block_ns: int = BLOCK_NS) -> dict[int, tuple[float, float]]:
    """{time block: (delta turnover sum|dD| in the block, settled contracts filled in the block)}."""
    out: dict[int, list[float]] = {}
    if ps is not None and len(ps) > 1:
        ts = ps["ts"].to_numpy(dtype=np.int64)
        dD = np.abs(np.diff(ps["delta_btc"].to_numpy(dtype=float)))
        for b, v in zip(ts[1:] // block_ns, dD):
            out.setdefault(int(b), [0.0, 0.0])[0] += float(v)
    f = settled(fills) if fills is not None else fills
    if f is not None and len(f):
        for b, c in zip(f["ts"].to_numpy(dtype=np.int64) // block_ns, f["contracts"].to_numpy(dtype=float)):
            out.setdefault(int(b), [0.0, 0.0])[1] += float(c)
    return {k: (v[0], v[1]) for k, v in out.items()}


def paired_e9(ref: GridRun, var: GridRun, n_boot: int = 400) -> tuple[CI, CI]:
    """(Delta net $/day over settlement events, Delta delta-turnover per contract over 1 h blocks)."""
    days = float(var.summary.get("days", math.nan))
    usd = sum_diff_ci(settled(ref.df), settled(var.df), "net", scale=1.0 / days if days > 0 else math.nan, n_boot=n_boot)
    ta = turnover_blocks(ref.collectors[0] if ref.collectors else None, ref.df)
    tb = turnover_blocks(var.collectors[0] if var.collectors else None, var.df)
    keys = sorted(set(ta) | set(tb))
    A = np.array([ta.get(k, (0.0, 0.0)) for k in keys], dtype=float).reshape(-1, 2)
    B = np.array([tb.get(k, (0.0, 0.0)) for k in keys], dtype=float).reshape(-1, 2)
    turn = paired_ratio_ci(A[:, 0], A[:, 1], B[:, 0], B[:, 1], n_boot) if len(keys) else CI(math.nan, math.nan, math.nan, 0)
    return usd, turn


def portfolio_metrics(ps: pd.DataFrame, contracts: float) -> dict[str, float]:
    if ps is None or not len(ps):
        return {"peak_collateral_usd": 0.0, "mean_collateral_usd": 0.0, "mean_abs_delta_btc": 0.0,
                "mean_gross_delta_btc": 0.0, "netting_ratio": math.nan, "delta_turnover_btc": 0.0,
                "delta_turnover_per_ct": math.nan}
    D = ps["delta_btc"].to_numpy()
    turn = float(np.abs(np.diff(D)).sum()) if len(D) > 1 else 0.0
    g = float(ps["gross_delta_btc"].mean())
    return {"peak_collateral_usd": float(ps["collateral_usd"].max()), "mean_collateral_usd": float(ps["collateral_usd"].mean()),
            "mean_abs_delta_btc": float(np.abs(D).mean()), "mean_gross_delta_btc": g,
            "netting_ratio": float(np.abs(D).mean() / g) if g > 0 else math.nan, "delta_turnover_btc": turn,
            "delta_turnover_per_ct": turn / contracts if contracts > 0 else math.nan}


def run(root: str | Path, t0: int, t1: int, out: str | Path, *, cfg: StrategyConfig | None = None,
        policies=("B", "C"), counts: Sequence[int] = (1, 3, 0), warm: str = "recorded", n_jobs: int = 1,
        universe: Universe | None = None, seed: int = 1, progress=None, latency=None, n_boot: int = 400) -> dict[str, Any]:
    Path(out).mkdir(parents=True, exist_ok=True)
    cfg = cfg or StrategyConfig()
    uni = universe or build_universe(root, t0, t1)
    lat = research_latency(uni, latency)
    names = {n: ("all" if n <= 0 else f"nearest_{n}") for n in counts}
    variants = [Variant(names[n], cfg, spec_filter=NearestStrikes(n, cfg.quoting.max_tau_s)) for n in counts]
    runs = run_variants(root, t0, t1, variants, policies, universe=uni, warm=warm, seed=seed, n_jobs=n_jobs,
                        collectors=[PortfolioSampler], progress=progress, latency=lat)
    ref = names[counts[0]]
    tab = variant_table(runs, ref=ref)
    extra = []
    for r in runs:
        ps = r.collectors[0] if r.collectors else None
        ct = float(r.df.loc[r.df["settle"].notna(), "contracts"].sum()) if len(r.df) else 0.0
        extra.append({"variant": r.variant, "policy": r.policy, "strikes_quoted": r.summary.get("n_specs", 0),
                      **portfolio_metrics(ps, ct)})
    tab = tab.merge(pd.DataFrame(extra), on=["variant", "policy"], how="left")
    by = {(r.variant, r.policy): r for r in runs}
    prow = []
    for r in runs:
        if r.variant == ref or (ref, r.policy) not in by:
            continue
        usd, turn = paired_e9(by[(ref, r.policy)], r, n_boot)
        p = max(usd.p_greater(0.0), turn.p_less(0.0)) if math.isfinite(usd.mean) and math.isfinite(turn.mean) else math.nan
        prow.append({"variant": r.variant, "policy": r.policy, "d_usd_day": usd.mean, "d_usd_day_lo": usd.lo,
                     "d_usd_day_hi": usd.hi, "events": usd.clusters, "d_turnover_per_ct": turn.mean,
                     "d_turnover_lo": turn.lo, "d_turnover_hi": turn.hi, "time_blocks": turn.clusters, "p_joint": p})
    paired = pd.DataFrame(prow)
    others = [names[n] for n in counts[1:]]
    improves: dict[str, bool] = {v: True for v in others}  # AND over B and C; starts True, any miss -> False
    worse: dict[str, bool] = {v: False for v in others}
    for p in ("B", "C"):
        rows = paired[paired.policy == p].set_index("variant") if len(paired) else pd.DataFrame()
        rej = holm(rows["p_joint"].to_numpy(dtype=float)) if len(rows) else []
        passed = {v for v, rj in zip(rows.index, rej) if rj and rows.loc[v, "d_usd_day_lo"] > 0
                  and rows.loc[v, "d_turnover_hi"] < 0}
        for v in others:
            improves[v] = improves[v] and v in passed
            if v in rows.index:
                worse[v] = worse[v] or bool(rows.loc[v, "d_usd_day_hi"] < 0 or rows.loc[v, "d_turnover_lo"] > 0)
    rep = Report("e9_multistrike", "E9 — Quoting 1 vs 3 vs all strikes: netting and capital efficiency", Path(out),
                 synthetic=uni.synthetic, rule=RULE_E9,
                 meta={"root": str(root), "window": f"{fmt_ns(t0)} .. {fmt_ns(t1)}", "reference": ref,
                       **inputs_meta(uni, t0)[0], "latency": describe_latency(lat, uni)})
    good = [v for v in others if improves.get(v)]
    if good:
        rep.verdict = f"ACCEPT: {', '.join(good)} improve(s) on {ref} ($/day up and turnover per contract down, CIs under B and C)"
    elif others and all(worse.get(v) for v in others):
        rep.verdict = f"REJECT (no improvement: every variant is worse than {ref} on $/day or turnover under B or C)"
    else:
        rep.verdict = f"INCONCLUSIVE (no variant shows $/day up and turnover down vs {ref} with CIs under both B and C)"
    rep.decision_events = int(paired["events"].min()) if len(paired) else None
    rep.policies = policies_with_results(runs)
    rep.in_sample, rep.in_sample_why = inputs_status(uni, t0)
    for w in run_warnings(runs):
        rep.line(f"WARNING: {w}")
    rep.table("variants", tab, "Per variant x policy. d_* = paired difference in net c/contract vs the reference; "
                               "collateral/delta from a 1 s portfolio sampler of the replayed strategy.")
    rep.table("paired", paired, "Decision table: paired CIs vs the reference (net $/day over settlement events; delta "
                                "turnover per contract over 1 h blocks); p_joint = max of the one-sided p-values.")
    rep.table("regimes", paired_regimes(runs, ref), "Paired net c/contract difference vs the reference by regime.")
    rep.write()
    return {"table": tab, "paired": paired, "runs": runs, "verdict": rep.final_verdict(), "rule_outcome": rep.verdict}
