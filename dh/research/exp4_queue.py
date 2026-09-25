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
> 0.05c/contract (CI > 0) and in $/day under B and C; differences within the CI -> reject.
Multiplicity (audit M8): only the PRE-REGISTERED keep-priority variants (config,
hysteresis_strong, age_only_5s; every non-reference variant of a custom grid) are tested, with
Holm across them (one-sided p-value of d > 0 at family level 2.5 %, required under B and C);
the other variants are descriptive. Clusters = settlement events (expirations); day blocks
are a second check from 5 days on. Regime splits: tau bucket, vol tercile, weekday.
"""

from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from dh.research.exp_common import Report, fmt_ns, holm
from dh.research.replay_env import Universe, build_universe, describe_latency, inputs_meta, inputs_status, research_latency
from dh.research.replay_grid import (
    Variant,
    apply_overrides,
    paired_regimes,
    policies_with_results,
    run_variants,
    run_warnings,
    settled,
    variant_table,
)
from dh.strategy.config import StrategyConfig

KEEP_PRIORITY = ("config", "hysteresis_strong", "age_only_5s")  # pre-registered hypotheses of the default grid
RULE_E4 = ("accept keep-priority if it beats always-re-centering by > 0.05c/contract (paired CI > 0, Holm across the "
           "pre-registered keep-priority variants) and in $/day "
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


def e4_decision(tab: pd.DataFrame, tested: list[str], alpha: float = 0.025) -> tuple[list[str], dict[str, bool]]:
    """(winners, {policy: all-tested-present}): a tested variant wins if, under BOTH B and C, it is
    Holm-significant (d > 0 across the tested variants of that policy), d > 0.05c, $/day higher and
    the day-block check (when >= 5 days) agrees."""
    ok: dict[str, set[str]] = {}
    present: dict[str, bool] = {}
    for p in ("B", "C"):
        rows = tab[(tab.policy == p) & tab.variant.isin(tested)].set_index("variant")
        present[p] = len(rows) == len(tested) and "d_p" in rows
        if not len(rows) or "d_p" not in rows:
            ok[p] = set()
            continue
        rej = holm(rows["d_p"].to_numpy(dtype=float), alpha)
        good = set()
        for (v, r), rj in zip(rows.iterrows(), rej):
            day = r.get("d_day_lo_c", math.nan)
            day_ok = not (int(r.get("day_blocks", 0) or 0) >= 5 and not day > 0)
            if rj and r.get("d_net_vs_ref_c", math.nan) > 0.05 and r.get("d_usd_per_day_vs_ref", math.nan) > 0 and day_ok:
                good.add(str(v))
        ok[p] = good
    return sorted(ok["B"] & ok["C"]), present


def run(root: str | Path, t0: int, t1: int, out: str | Path, *, cfg: StrategyConfig | None = None,
        policies=("B", "C"), grid: str | None = None, warm: str = "recorded", n_jobs: int = 1,
        universe: Universe | None = None, seed: int = 1, progress=None, latency=None) -> dict[str, Any]:
    Path(out).mkdir(parents=True, exist_ok=True)
    cfg = cfg or StrategyConfig()
    uni = universe or build_universe(root, t0, t1)
    lat = research_latency(uni, latency)
    g = load_grid(grid)
    variants = [Variant(name, apply_overrides(cfg, over)) for name, over in g.items()]
    runs = run_variants(root, t0, t1, variants, policies, universe=uni, warm=warm, seed=seed, n_jobs=n_jobs,
                        progress=progress, latency=lat)
    tab = variant_table(runs, ref=REF)
    mix = position_mix(runs)
    tested = [v for v in g if v != REF and (grid or v in KEEP_PRIORITY)]
    rep = Report("e4_queue", "E4 — Queue priority vs continuous repricing", Path(out), synthetic=uni.synthetic,
                 rule=RULE_E4, meta={"root": str(root), "window": f"{fmt_ns(t0)} .. {fmt_ns(t1)}", **inputs_meta(uni, t0)[0],
                                     "latency": describe_latency(lat, uni), "variants": len(variants),
                                     "tested (pre-registered, Holm)": ", ".join(tested) or "none",
                                     "policies": ",".join(policies)})
    winners, _ = e4_decision(tab, tested)
    rep.decision_events = int(tab["events"].min()) if len(tab) and "events" in tab else None
    rep.policies = policies_with_results(runs, [REF, *tested])
    rep.in_sample, rep.in_sample_why = inputs_status(uni, t0)
    rep.verdict = (f"ACCEPT: {', '.join(winners)} beat re-centering under B and C (Holm across {len(tested)} "
                   "pre-registered variants)" if winners else
                   "REJECT (differences within the CI: no pre-registered keep-priority variant beats always-re-centering "
                   "by > 0.05c/contract with Holm-adjusted CI > 0 and higher $/day under both B and C)")
    for w in run_warnings(runs):
        rep.line(f"WARNING: {w}")
    rep.table("variants", tab, "Net c/contract with settlement-event CI; d_* = paired difference vs recenter_always "
                               "(same events; d_p = one-sided p-value of d > 0, day-block CI as a second check). "
                               "usd_per_quote_hour = net $ per hour of resting quotes.")
    rep.table("fill_position_mix", mix, "Fills by the quote's position at placement (touch/improve/behind).")
    rep.table("regimes", paired_regimes(runs, REF), "Paired net c/contract difference vs recenter_always by regime "
                                                    "(tau bucket, vol tercile, weekday).")
    rep.write()
    return {"table": tab, "mix": mix, "runs": runs, "verdict": rep.final_verdict(), "rule_outcome": rep.verdict}
