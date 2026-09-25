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
"""

from __future__ import annotations

import math
from pathlib import Path
from typing import Any, Sequence

import numpy as np
import pandas as pd

from dh.research.exp_common import Report, fmt_ns
from dh.research.replay_env import NearestStrikes, PortfolioSampler, Universe, build_universe, inputs_meta
from dh.research.replay_grid import Variant, run_variants, run_warnings, variant_table
from dh.strategy.config import StrategyConfig

RULE_E9 = "accept if $/day is up and hedge (delta) turnover per contract is down vs the single best strike"


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
        universe: Universe | None = None, seed: int = 1, progress=None) -> dict[str, Any]:
    Path(out).mkdir(parents=True, exist_ok=True)
    cfg = cfg or StrategyConfig()
    uni = universe or build_universe(root, t0, t1)
    names = {n: ("all" if n <= 0 else f"nearest_{n}") for n in counts}
    variants = [Variant(names[n], cfg, spec_filter=NearestStrikes(n, cfg.quoting.max_tau_s)) for n in counts]
    runs = run_variants(root, t0, t1, variants, policies, universe=uni, warm=warm, seed=seed, n_jobs=n_jobs,
                        collectors=[PortfolioSampler], progress=progress)
    ref = names[counts[0]]
    tab = variant_table(runs, ref=ref)
    extra = []
    for r in runs:
        ps = r.collectors[0] if r.collectors else None
        ct = float(r.df.loc[r.df["settle"].notna(), "contracts"].sum()) if len(r.df) else 0.0
        extra.append({"variant": r.variant, "policy": r.policy, "strikes_quoted": r.summary.get("n_specs", 0),
                      **portfolio_metrics(ps, ct)})
    tab = tab.merge(pd.DataFrame(extra), on=["variant", "policy"], how="left")
    rep = Report("e9_multistrike", "E9 — Quoting 1 vs 3 vs all strikes: netting and capital efficiency", Path(out),
                 synthetic=uni.synthetic, rule=RULE_E9,
                 meta={"root": str(root), "window": f"{fmt_ns(t0)} .. {fmt_ns(t1)}", "reference": ref,
                       **inputs_meta(uni, t0)[0]})
    verdicts = []
    for v in tab["variant"].unique():
        if v == ref:
            continue
        ok = True
        for p in [p for p in policies if p in ("B", "C")]:
            a = tab[(tab.variant == ref) & (tab.policy == p)]
            b = tab[(tab.variant == v) & (tab.policy == p)]
            if not len(a) or not len(b):
                ok = False
                continue
            a, b = a.iloc[0], b.iloc[0]
            ok &= bool(b.usd_per_day > a.usd_per_day and b.delta_turnover_per_ct < a.delta_turnover_per_ct)
        verdicts.append(f"{v}: {'IMPROVES' if ok else 'no improvement'}")
    rep.decision_events = int(tab["events"].min()) if len(tab) and "events" in tab else None
    rep.verdict = "; ".join(verdicts) + f" (vs {ref})"
    for w in run_warnings(runs):
        rep.line(f"WARNING: {w}")
    rep.table("variants", tab, "Per variant x policy. d_* = paired difference in net c/contract vs the reference; "
                               "collateral/delta from a 1 s portfolio sampler of the replayed strategy.")
    rep.write()
    return {"table": tab, "runs": runs}
