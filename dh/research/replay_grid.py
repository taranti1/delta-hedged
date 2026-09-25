"""Run a grid of replay variants x fill policies (optionally in parallel) and tabulate them.

    runs = run_variants(root, t0, t1, [Variant("base", cfg), Variant("x5", cfg5)], ("B", "C"), n_jobs=4)
    table = variant_table(runs, ref="base")

Every run is an independent, seeded, deterministic ``run_replay``; parallel execution (fork) gives
the same numbers as serial execution. Paired comparisons resample settlement events jointly across
the two variants (same recording, same events).
"""

from __future__ import annotations

import math
import multiprocessing as mp
from collections.abc import Callable, Sequence
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from dh.research.exp_common import cluster_mean_ci, paired_diff_ci, policy_letter
from dh.research.replay_env import Universe, build_universe, run_replay
from dh.strategy.config import StrategyConfig


@dataclass(frozen=True, eq=False)
class Variant:
    name: str
    cfg: StrategyConfig
    spec_filter: Any = None
    strategy_factory: Any = None
    factory_kwargs: dict[str, Any] | None = None
    postprocess: Any = None  # module-level function(ReplayResult) (picklable)
    note: str = ""


@dataclass
class GridRun:
    variant: str
    policy: str
    df: pd.DataFrame
    summary: dict[str, Any]
    collectors: list[Any] = field(default_factory=list)


def _job(args: tuple) -> GridRun:
    (root, t0, t1, v, policy, warm, seed, uni, collectors, latency) = args
    res = run_replay(root, t0, t1, v.cfg, policy, latency=latency, warm=warm, seed=seed, universe=uni,
                     spec_filter=v.spec_filter, strategy_factory=v.strategy_factory, factory_kwargs=v.factory_kwargs,
                     collectors=collectors, postprocess=v.postprocess)
    s = dict(res.summary)
    s["variant"] = v.name
    return GridRun(v.name, policy_letter(policy), res.df, s, list(res.extras.get("collectors", [])))


def run_variants(root: str | Path, t0: int, t1: int, variants: Sequence[Variant], policies: Sequence[str] = ("B", "C"),
                 *, universe: Universe | None = None, warm: str = "recorded", seed: int = 1, n_jobs: int = 1,
                 collectors: Sequence[Callable] = (), latency: Any = None,
                 progress: Callable[[str], None] | None = None) -> list[GridRun]:
    uni = universe or build_universe(root, t0, t1)
    prime_cache(uni, t0, warm, variants)
    jobs = [(str(root), t0, t1, v, policy_letter(p), warm, seed, uni, tuple(collectors), latency)
            for v in variants for p in policies]
    out: list[GridRun] = []
    if n_jobs <= 1 or len(jobs) == 1:
        for j in jobs:
            r = _job(j)
            if progress:
                progress(f"{r.variant}/{r.policy}: fills={len(r.df)} net={r.summary.get('net_c_per_contract', math.nan):.3f}c")
            out.append(r)
        return out
    ctx = mp.get_context("fork")
    with ProcessPoolExecutor(max_workers=n_jobs, mp_context=ctx) as ex:
        for r in ex.map(_job, jobs):
            if progress:
                progress(f"{r.variant}/{r.policy}: fills={len(r.df)} net={r.summary.get('net_c_per_contract', math.nan):.3f}c")
            out.append(r)
    return out


def prime_cache(uni: Universe, t0: int, warm: str, variants: Sequence[Variant] = (),
                fv_warm_s: float = 1.5 * 86400) -> None:
    """Scan the recorded BRTI ticks needed by every job once, before forking the workers."""
    from dh.research.replay_env import NearestStrikes, _ns, brti_ticks, parse_warm

    if "recorded" in parse_warm(warm):
        brti_ticks(uni.root, t0 - _ns(fv_warm_s), t0, cache=uni.cache)
    if any(isinstance(v.spec_filter, NearestStrikes) for v in variants):
        brti_ticks(uni.root, uni.t0 - 2 * 3600 * 10**9, uni.t1, cache=uni.cache)


def run_warnings(runs: Sequence[GridRun]) -> list[str]:
    """Unique replay warnings (fair value not warm, synthetic warm-up, no specs) across runs."""
    out: list[str] = []
    for r in runs:
        for w in r.summary.get("warnings", []) or []:
            if w not in out:
                out.append(w)
    return out


def settled(df: pd.DataFrame) -> pd.DataFrame:
    return df[df["settle"].notna()] if len(df) and "settle" in df else df


def variant_table(runs: Sequence[GridRun], ref: str | None = None, n_boot: int = 400) -> pd.DataFrame:
    """One row per (variant, policy): fills, contracts, net c/contract (event CI), $/day, per-day
    rates, markouts, quoting activity; paired difference vs the reference variant."""
    by = {(r.variant, r.policy): r for r in runs}
    rows = []
    for r in runs:
        s = r.summary
        d = settled(r.df)
        ci = cluster_mean_ci(d["net_c_per_ct"], d["contracts"], d["event"], n_boot) if len(d) else None
        days = s.get("days", math.nan)
        row = {"variant": r.variant, "policy": r.policy, "fills": len(d),
               "contracts": float(d["contracts"].sum()) if len(d) else 0.0,
               "net_c": ci.mean if ci else math.nan, "net_lo_c": ci.lo if ci else math.nan,
               "net_hi_c": ci.hi if ci else math.nan, "events": ci.clusters if ci else 0,
               "usd_per_day": s.get("net_usd_per_day", 0.0), "fills_per_day": s.get("fills_per_day", 0.0),
               "contracts_per_day": s.get("contracts_per_day", 0.0),
               "gross_edge_c": s.get("gross_edge_c_per_contract", math.nan),
               "fee_c": s.get("fees_c_per_contract", math.nan),
               "markout_1s_c": s.get("markout_1s_c", math.nan), "markout_10s_c": s.get("markout_10s_c", math.nan),
               "quotes_placed": s.get("mm_quotes_placed", 0), "cancels": s.get("mm_cancels", 0),
               "quote_hours": s.get("quote_hours", math.nan), "usd_per_quote_hour": s.get("net_usd_per_quote_hour", math.nan),
               "days": days}
        if ref is not None and r.variant != ref and (ref, r.policy) in by:
            a = settled(by[(ref, r.policy)].df)
            if len(a) and len(d):
                dd = paired_diff_ci(a, d, "net_c_per_ct", n_boot=n_boot)
                row.update({"d_net_vs_ref_c": dd.mean, "d_lo_c": dd.lo, "d_hi_c": dd.hi})
            row["d_usd_per_day_vs_ref"] = s.get("net_usd_per_day", 0.0) - by[(ref, r.policy)].summary.get("net_usd_per_day", 0.0)
        rows.append(row)
    return pd.DataFrame(rows)


def scaled_cfg(cfg: StrategyConfig, k: float, scale_limits: bool = True) -> StrategyConfig:
    """Clip size x k; with scale_limits, every size-denominated risk limit x k as well (so the
    limits do not bind before capacity does: Experiment 10)."""
    q = replace(cfg.quoting, clip_contracts=cfg.quoting.clip_contracts * k)
    if not scale_limits:
        return replace(cfg, quoting=q)
    r = cfg.risk
    r2 = replace(r, max_pos_per_market=r.max_pos_per_market * k, max_event_worst_loss=r.max_event_worst_loss * k,
                 max_total_worst_loss=r.max_total_worst_loss * k, tail_budget=r.tail_budget * k,
                 max_abs_delta_btc=r.max_abs_delta_btc * k, daily_loss_halt=r.daily_loss_halt * k,
                 settlement_loss_halt=r.settlement_loss_halt * k,
                 order_group_limit_contracts=r.order_group_limit_contracts * k, risk_capital=r.risk_capital * k)
    return replace(cfg, quoting=q, risk=r2)


def apply_overrides(cfg: StrategyConfig, over: dict[str, dict[str, Any]]) -> StrategyConfig:
    """{'quoting': {...}, 'fill': {...}, ...} -> cfg with those fields replaced."""
    out = cfg
    for section, fields in (over or {}).items():
        sub = getattr(out, section)
        out = replace(out, **{section: replace(sub, **{k: (tuple(v) if isinstance(v, list) else v)
                                                      for k, v in fields.items()})})
    return out


def interp_capacity(xs: Sequence[float], ys: Sequence[float], level: float) -> float:
    """Largest x (log-linear interpolation) with y > level, scanning x ascending; nan if y(x_min)
    <= level; x_max if y > level everywhere."""
    pts = [(float(x), float(y)) for x, y in sorted(zip(xs, ys)) if np.isfinite(y)]
    if not pts or pts[0][1] <= level:
        return math.nan
    for (x0, y0), (x1, y1) in zip(pts, pts[1:]):
        if y1 <= level < y0:
            w = (y0 - level) / (y0 - y1)
            return float(math.exp(math.log(x0) + w * (math.log(x1) - math.log(x0))))
    return pts[-1][0]
