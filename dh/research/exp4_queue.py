"""Experiment 4: is queue priority worth more than continuous repricing? (policy replays)

Variants of the production MarketMaker replayed on the same recording under fill policies B and C
(A optional, reference only):

  recenter_always     replace_rel = 0, min_order_age_ms = 0, kappa_replace_per_s = 0: every EV
                      improvement moves the quote (benchmark: "always re-center at fair")
  config              the configuration as given (hysteresis defaults)
  hysteresis_strong   replace_rel = 1.0, min_order_age_ms = 10 000, kappa_replace_per_s = 0.001
  age_only_5s         replace_rel = 0, min_order_age_ms = 5 000, kappa = 0
  join_only           fill.improve_rate_mult = 0 (improving one tick never has value: join/behind)
  improve_x2          fill.improve_rate_mult = 2 (favour stepping ahead of the queue)
  touch_only          max_ticks_from_touch = 0 (no quotes behind the touch)
  wide_ladder         max_ticks_from_touch = 6
A custom grid replaces these: JSON/YAML {name: {section: {field: value}}}.

Per variant x policy: realized net c/contract (event-bootstrap CI), $/day, fills/day,
contracts/day, quote-hours and net $ per quote-hour, quotes/cancels, markouts, fill share by
quote position at placement (touch / improve / behind); paired difference vs recenter_always.

Decision rule (docs/TEST_MATRIX.md E4): accept "keep priority" if it beats re-centering by
> 0.05c/contract (CI > 0) and in $/day under B and C; differences within the CI -> no decision.
"""

from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from dh.research.exp_common import Report, flag_only_A, fmt_ns
from dh.research.replay_env import Universe, build_universe, inputs_meta
from dh.research.replay_grid import Variant, apply_overrides, run_variants, run_warnings, settled, variant_table
from dh.strategy.config import StrategyConfig

RULE_E4 = ("accept keep-priority if it beats always-re-centering by > 0.05c/contract (paired CI > 0) and in $/day "
           "under B and C; differences within the CI -> no decision")
REF = "recenter_always"

DEFAULT_GRID: dict[str, dict[str, dict[str, Any]]] = {
    REF: {"quoting": {"replace_rel": 0.0, "min_order_age_ms": 0, "kappa_replace_per_s": 0.0}},
    "config": {},
    "hysteresis_strong": {"quoting": {"replace_rel": 1.0, "min_order_age_ms": 10_000, "kappa_replace_per_s": 0.001}},
    "age_only_5s": {"quoting": {"replace_rel": 0.0, "min_order_age_ms": 5_000, "kappa_replace_per_s": 0.0}},
    "join_only": {"fill": {"improve_rate_mult": 0.0}},
    "improve_x2": {"fill": {"improve_rate_mult": 2.0}},
    "touch_only": {"quoting": {"max_ticks_from_touch": 0}},
    "wide_ladder": {"quoting": {"max_ticks_from_touch": 6}},
}


def load_grid(spec: str | None) -> dict[str, dict[str, dict[str, Any]]]:
    if not spec:
        return DEFAULT_GRID
    p = Path(spec)
    txt = p.read_text() if p.exists() else spec
    g = yaml.safe_load(txt) if p.suffix in (".yaml", ".yml") else json.loads(txt)
    if REF not in g:
        g = {REF: DEFAULT_GRID[REF], **g}
    return g


def position_mix(runs) -> pd.DataFrame:
    rows = []
    for r in runs:
        d = settled(r.df)
        if not len(d) or "quote_position" not in d:
            continue
        for pos, g in d.groupby("quote_position"):
            rows.append({"variant": r.variant, "policy": r.policy, "quote_position": pos or "unknown",
                         "fills": len(g), "contract_share": float(g.contracts.sum() / d.contracts.sum()),
                         "net_c": float((g.net_c_per_ct * g.contracts).sum() / g.contracts.sum())})
    return pd.DataFrame(rows)


def run(root: str | Path, t0: int, t1: int, out: str | Path, *, cfg: StrategyConfig | None = None,
        policies=("B", "C"), grid: str | None = None, warm: str = "recorded", n_jobs: int = 1,
        universe: Universe | None = None, seed: int = 1, progress=None) -> dict[str, Any]:
    Path(out).mkdir(parents=True, exist_ok=True)
    cfg = cfg or StrategyConfig()
    uni = universe or build_universe(root, t0, t1)
    g = load_grid(grid)
    variants = [Variant(name, apply_overrides(cfg, over)) for name, over in g.items()]
    runs = run_variants(root, t0, t1, variants, policies, universe=uni, warm=warm, seed=seed, n_jobs=n_jobs,
                        progress=progress)
    tab = flag_only_A(variant_table(runs, ref=REF), ["variant"])
    mix = position_mix(runs)
    rep = Report("e4_queue", "E4 — Queue priority vs continuous repricing", Path(out), synthetic=uni.synthetic,
                 rule=RULE_E4, meta={"root": str(root), "window": f"{fmt_ns(t0)} .. {fmt_ns(t1)}", **inputs_meta(uni, t0)[0],
                                     "variants": len(variants), "policies": ",".join(policies)})
    winners = []
    need = [p for p in policies if p in ("B", "C")]
    for v in g:
        if v == REF or not need:
            continue
        rows = tab[(tab.variant == v) & tab.policy.isin(need)]
        if len(rows) == len(need) and all(
                (r.get("d_lo_c", math.nan) > 0) and (r.get("d_net_vs_ref_c", math.nan) > 0.05)
                and (r.get("d_usd_per_day_vs_ref", math.nan) > 0) for _, r in rows.iterrows()):
            winners.append(v)
    rep.verdict = (f"ACCEPT: {', '.join(winners)} beat re-centering under B and C" if winners else
                   "NO DECISION: no variant beats always-re-centering by > 0.05c/contract with CI > 0 under both B and C")
    for w in run_warnings(runs):
        rep.line(f"WARNING: {w}")
    rep.table("variants", tab, "Net c/contract with event-bootstrap CI; d_* = paired difference vs recenter_always "
                               "(same events). usd_per_quote_hour = net $ per hour of resting quotes.")
    rep.table("fill_position_mix", mix, "Fills by the quote's position at placement (touch/improve/behind).")
    rep.write()
    return {"table": tab, "mix": mix, "runs": runs}
