"""Experiments 6 and 7: where is the net edge? By time to expiry, |z| and YES price.

Input: ledger DataFrames of replays under fill policies B and C (A for reference only), or
ledger CSVs from earlier runs / live sessions (``--ledger``; columns ts, event, contracts,
net_c_per_ct, tau_s, px, fee, gross_edge_c and optionally z, mo_10s_c, mo_60s_c, policy).

Buckets (docs/TEST_MATRIX.md regime splits): tau (>30m, 10-30m, 5-10m, 1-5m, 30-60s, <30s);
|z| (0-0.5 ... >3; z of the strategy's last quote cycle before the fill); YES price (1-5c ...
95-99c). Per bucket: fills, contracts/day, fills/day, realized net c/contract with an event
bootstrap CI, gross edge, fees, markouts at 10 s / 60 s (vs the fair value at fill), toxic share
(net 10 s markout < 0), $/day.

Decision rule (E6/E7), with multiplicity control and out-of-sample confirmation (audit C5):
  1. selection on the earlier part of the data: per table family (tau, |z|, YES price, tau x |z|)
     and policy, one-sided p-values (jackknife-t over settlement events) with Holm within the
     family; a bucket is a CANDIDATE when Holm-significant > 0 under BOTH B and C (>= 20 events);
  2. confirmation on a DISJOINT LATER window (``--confirm-t0/--confirm-t1``, or the built-in
     chronological split: settlement events expiring before / after the split): a candidate is
     'quote' only if its net CI lower bound > 0 there under B and C (Holm across the family's
     candidates, >= 20 events);
  3. 'disable' when Holm-significant < 0 under B or C in the selection sample.
The verdict states that selection was in-sample and where it was confirmed. Ledgers carry the
replay's status columns (synthetic, fv_status, flow_status); a pooled ledger without them has an
unknown status, which caps an ACCEPT (audit M7).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Sequence

import numpy as np
import pandas as pd

from dh.research.exp_common import (
    MIN_DECISION_EVENTS,
    Report,
    flag_only_A,
    fmt_ns,
    holm,
    price_bucket,
    segment_rows,
    tau_bucket,
    write_csv,
    z_bucket,
)
from dh.research.replay_env import (
    Universe,
    build_universe,
    describe_latency,
    inputs_meta,
    inputs_status,
    research_latency,
)
from dh.research.replay_grid import Variant, run_variants, run_warnings, settled
from dh.strategy.config import StrategyConfig

RULE_E67 = ("quote only buckets whose realized net c/contract is > 0 with Holm-controlled significance under B and C "
            "in the selection sample AND CI lower bound > 0 under B and C on a disjoint later window; disable buckets "
            "significantly < 0 (Holm) under B or C")
FAMILIES = (("tau", ["tau_bucket"]), ("z", ["z_bucket"]), ("price", ["price_bucket"]), ("tau_z", ["tau_bucket", "z_bucket"]))
KEYS = ("tau_bucket", "z_bucket", "price_bucket")


def add_buckets(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()
    d["tau_bucket"] = tau_bucket(d["tau_s"]).to_numpy()
    d["z_bucket"] = z_bucket(d["z"]).to_numpy() if "z" in d else pd.Categorical([np.nan] * len(d))
    d["price_bucket"] = price_bucket(d["px"]).to_numpy()
    return d


def _family_rows(dfs: dict[str, pd.DataFrame], days: float, n_boot: int) -> dict[str, pd.DataFrame]:
    out: dict[str, list[pd.DataFrame]] = {k: [] for k, _ in FAMILIES}
    for pol, df in dfs.items():
        d = add_buckets(settled(df)) if df is not None and len(df) else pd.DataFrame()
        if not len(d):
            continue
        for key, by in FAMILIES:
            t = segment_rows(d, by, days, n_boot)
            if len(t):
                t.insert(0, "policy", pol)
                out[key].append(t)
    return {k: (pd.concat(v, ignore_index=True) if v else pd.DataFrame()) for k, v in out.items()}


def _holm_flags(t: pd.DataFrame, col: str, min_events: int, alpha: float = 0.025) -> pd.Series:
    """Holm within the table family, separately per policy, over buckets with >= min_events."""
    flag = pd.Series(False, index=t.index)
    for _, g in t.groupby("policy"):
        g = g[g["events"] >= min_events]
        if len(g):
            flag.loc[g.index] = holm(g[col].to_numpy(dtype=float), alpha)
    return flag


def segment_tables(dfs: dict[str, pd.DataFrame], days: float, n_boot: int = 300,
                   confirm: dict[str, pd.DataFrame] | None = None, confirm_days: float | None = None,
                   min_events: int = MIN_DECISION_EVENTS) -> dict[str, pd.DataFrame]:
    """Per family: selection-sample rows (per policy x bucket) with Holm flags and the
    recommendation; with ``confirm`` (a disjoint later sample) candidates are re-tested there.
    Recommendations: 'quote' (confirmed), 'candidate (not confirmed)', 'disable', 'no edge',
    'insufficient data'."""
    sel = _family_rows(dfs, days, n_boot)
    conf = _family_rows(confirm, confirm_days or days, n_boot) if confirm else {}
    res = {}
    for k, by in FAMILIES:
        t = sel.get(k, pd.DataFrame())
        if not len(t):
            res[k] = t
            continue
        keys = [c for c in KEYS if c in t]
        t = flag_only_A(t, keys)
        t["holm_pos"] = _holm_flags(t, "p_pos", min_events)
        t["holm_neg"] = _holm_flags(t, "p_neg", min_events)
        ct = conf.get(k, pd.DataFrame()) if conf else pd.DataFrame()
        t = add_recommendation(t, keys, min_events, ct)
        res[k] = t
    return res


def add_recommendation(t: pd.DataFrame, keys: Sequence[str], min_events: int = MIN_DECISION_EVENTS,
                       confirm: pd.DataFrame | None = None) -> pd.DataFrame:
    """Candidate: Holm-significant > 0 under B AND C (each with >= min_events); 'quote' only if a
    ``confirm`` table (same family, later window) shows net CI lower bound > 0 under B and C with
    Holm across the candidates; 'disable': Holm-significant < 0 under B or C."""
    t = t.copy()
    for c in ("holm_pos", "holm_neg"):
        if c not in t:  # plain CI flags when called on a bare segment table
            t[c] = t["net_lo_c" if c == "holm_pos" else "net_hi_c"].map(lambda v: v > 0 if c == "holm_pos" else v < 0)
    t["recommendation"] = "insufficient data"
    t["confirm_lo_c"] = np.nan
    cand: list[tuple] = []
    for key, g in t.groupby(list(keys), observed=True, dropna=False):
        bc = g[g["policy"].isin(["B", "C"])]
        if not len(bc) or not ({"B", "C"} <= set(bc["policy"])) or int(bc["events"].min()) < min_events:
            continue
        if bc["holm_neg"].any():
            t.loc[g.index, "recommendation"] = "disable"
        elif bc["holm_pos"].all():
            t.loc[g.index, "recommendation"] = "candidate (not confirmed)"
            cand.append(key if isinstance(key, tuple) else (key,))
        else:
            t.loc[g.index, "recommendation"] = "no edge"
    if confirm is not None and len(confirm) and cand:
        ok_by_pol = {}
        for pol in ("B", "C"):
            c = confirm[confirm["policy"] == pol]
            rows = []
            for key in cand:
                m = np.ones(len(c), dtype=bool)
                for col, v in zip(keys, key):
                    m &= (c[col].astype(str) == str(v)).to_numpy()
                r = c[m]
                rows.append((key, r.iloc[0] if len(r) else None))
            pv = [float(r["p_pos"]) if r is not None and r["events"] >= min_events else np.nan for _, r in rows]
            rej = holm(pv)
            ok_by_pol[pol] = {key: bool(rj and r is not None and r["net_lo_c"] > 0) for (key, r), rj in zip(rows, rej)}
            for key, r in rows:
                if r is not None:
                    m = np.ones(len(t), dtype=bool)
                    for col, v in zip(keys, key):
                        m &= (t[col].astype(str) == str(v)).to_numpy()
                    t.loc[m & (t["policy"] == pol).to_numpy(), "confirm_lo_c"] = float(r["net_lo_c"])
        for key in cand:
            if ok_by_pol.get("B", {}).get(key) and ok_by_pol.get("C", {}).get(key):
                m = np.ones(len(t), dtype=bool)
                for col, v in zip(keys, key):
                    m &= (t[col].astype(str) == str(v)).to_numpy()
                t.loc[m, "recommendation"] = "quote"
    return t


def ledger_status(dfs: dict[str, pd.DataFrame]) -> tuple[bool, bool, str]:
    """(synthetic, in_sample, why) from the status columns of ledger frames (audit M7). A frame
    without fv_status / flow_status columns has an UNKNOWN status (treated as in-sample)."""
    synthetic, ins, why = False, False, []
    for pol, d in dfs.items():
        if d is None or not len(d):
            continue
        if "synthetic" in d and d["synthetic"].astype(str).str.lower().isin(["true", "1"]).any():
            synthetic = True
        if "fv_status" not in d or "flow_status" not in d:
            ins = True
            why.append(f"ledger {pol} lacks the fv_status / flow_status columns (status unknown)")
            continue
        if (d["fv_status"].astype(str) == "in_sample").any():
            ins = True
            why.append(f"ledger {pol}: FV parameters in sample")
        if (d["flow_status"].astype(str) == "in_sample").any():
            ins = True
            why.append(f"ledger {pol}: taker flow in sample")
    return synthetic, ins, "; ".join(dict.fromkeys(why))


def split_by_expiration(dfs: dict[str, pd.DataFrame], t_split: int) -> tuple[dict[str, pd.DataFrame], dict[str, pd.DataFrame]]:
    """Selection / confirmation samples: fills of settlement events expiring before / at or after
    t_split (clusters never straddle the split)."""
    a, b = {}, {}
    for pol, d in dfs.items():
        if d is None or not len(d):
            a[pol], b[pol] = d, d
            continue
        exp = d["expiration_ns"] if "expiration_ns" in d else d["ts"] + (d["tau_s"] * 1e9).astype("int64")
        a[pol], b[pol] = d[exp < t_split], d[exp >= t_split]
    return a, b


def load_ledgers(paths: Sequence[str]) -> dict[str, pd.DataFrame]:
    out: dict[str, pd.DataFrame] = {}
    for p in paths:
        df = pd.read_csv(p)
        pol = str(df["policy"].iloc[0]) if "policy" in df and len(df) else Path(p).stem
        out[pol] = pd.concat([out[pol], df]) if pol in out else df
    return out


def run(root: str | Path | None, t0: int, t1: int, out: str | Path, *, cfg: StrategyConfig | None = None,
        policies=("A", "B", "C"), warm: str = "recorded", n_jobs: int = 1, universe: Universe | None = None,
        ledgers: Sequence[str] = (), seed: int = 1, progress=None, latency=None, confirm_t0: int | None = None,
        confirm_t1: int | None = None, select_frac: float = 0.5) -> dict[str, Any]:
    """E6/E7 segment study (module docstring). Without a confirmation window the data are split
    chronologically by settlement-event expiration at t0 + select_frac * (t1 - t0)."""
    Path(out).mkdir(parents=True, exist_ok=True)
    cfg = cfg or StrategyConfig()
    warns: list[str] = []
    imeta: dict[str, Any] = {"FV parameters": "as recorded in the ledgers' status columns"}
    conf_dfs: dict[str, pd.DataFrame] = {}
    lat = None
    if (confirm_t0 is None) != (confirm_t1 is None):
        raise ValueError("give both --confirm-t0 and --confirm-t1 (or neither: built-in chronological split)")
    if confirm_t0 is not None and confirm_t0 < t1:
        raise ValueError("the confirmation window must start at or after the selection window's end (disjoint, later)")
    if ledgers and confirm_t0 is not None:
        raise ValueError("a confirmation window needs replays: with --ledger the built-in chronological split is used")
    if ledgers:
        dfs = load_ledgers(ledgers)
        synthetic, in_sample, why = ledger_status(dfs)
        uni = None
    else:
        uni = universe or build_universe(root, t0, t1)
        lat = research_latency(uni, latency)
        runs = run_variants(root, t0, t1, [Variant("config", cfg)], policies, universe=uni, warm=warm, seed=seed,
                            n_jobs=n_jobs, progress=progress, latency=lat)
        dfs = {r.policy: r.df for r in runs}
        warns = run_warnings(runs)
        imeta = {**inputs_meta(uni, t0)[0], "latency": describe_latency(lat, uni)}
        synthetic = uni.synthetic
        in_sample, why = inputs_status(uni, t0)
        if confirm_t0 is not None and confirm_t1 is not None:
            cuni = build_universe(root, confirm_t0, confirm_t1)
            cuni.fv_config, cuni.fv_config_source = uni.fv_config, uni.fv_config_source
            cuni.flow_segments, cuni.flow_meta, cuni.flow_source = uni.flow_segments, uni.flow_meta, uni.flow_source
            cruns = run_variants(root, confirm_t0, confirm_t1, [Variant("config", cfg)], policies, universe=cuni, warm=warm,
                                 seed=seed, n_jobs=n_jobs, progress=progress, latency=research_latency(cuni, latency))
            conf_dfs = {r.policy: r.df for r in cruns}
            warns += [w for w in run_warnings(cruns) if w not in warns]
            ci_s, ci_why = inputs_status(cuni, confirm_t0)
            in_sample, why = in_sample or ci_s, "; ".join(x for x in (why, ci_why) if x)
    if conf_dfs:
        sel_dfs, sel_win, conf_win = dfs, f"{fmt_ns(t0)} .. {fmt_ns(t1)}", f"{fmt_ns(confirm_t0)} .. {fmt_ns(confirm_t1)}"
        days_sel = max((t1 - t0) / 86_400e9, 1e-9)
        days_conf = max((confirm_t1 - confirm_t0) / 86_400e9, 1e-9)
    else:
        t_split = t0 + int(select_frac * (t1 - t0))
        sel_dfs, conf_dfs = split_by_expiration(dfs, t_split)
        sel_win = f"settlement events expiring before {fmt_ns(t_split)}"
        conf_win = f"settlement events expiring from {fmt_ns(t_split)} (chronological split)"
        days_sel = max((t_split - t0) / 86_400e9, 1e-9)
        days_conf = max((t1 - t_split) / 86_400e9, 1e-9)
    tabs = segment_tables(sel_dfs, days_sel, confirm=conf_dfs, confirm_days=days_conf)
    rep = Report("e67_segments", "E6/E7 — Net edge by time to expiry, |z| and YES price", Path(out),
                 synthetic=synthetic, rule=RULE_E67,
                 meta={"source": ", ".join(ledgers) if ledgers else str(root), "window": f"{fmt_ns(t0)} .. {fmt_ns(t1)}",
                       "selection (in-sample)": sel_win, "confirmation (later, disjoint)": conf_win,
                       "policies": ",".join(dfs), **imeta})
    quote = {k: sorted({"/".join(str(r[c]) for c in KEYS if c in t) for _, r in t[t.recommendation == "quote"].iterrows()})
             for k, t in tabs.items() if len(t)}
    cands = {k: sorted({"/".join(str(r[c]) for c in KEYS if c in t)
                        for _, r in t[t.recommendation == "candidate (not confirmed)"].iterrows()}) for k, t in tabs.items() if len(t)}
    dis = {k: sorted({"/".join(str(r[c]) for c in KEYS if c in t) for _, r in t[t.recommendation == "disable"].iterrows()})
           for k, t in tabs.items() if len(t)}
    sel_note = f"buckets selected in-sample on {sel_win} (Holm within each table), confirmed on {conf_win}"
    if any(quote.values()):
        rep.verdict = ("ACCEPT: quote " + "; ".join(f"{k}: {v}" for k, v in quote.items() if v)
                       + (f"; disable {'; '.join(f'{k}: {v}' for k, v in dis.items() if v)}" if any(dis.values()) else "")
                       + f" ({sel_note})")
    elif not any(len(t) for t in tabs.values()):
        rep.verdict = "INCONCLUSIVE (no settled fills)"
    else:
        rep.verdict = ("INCONCLUSIVE (no bucket confirmed on the later window; in-sample candidates: "
                       + ("; ".join(f"{k}: {v}" for k, v in cands.items() if v) or "none")
                       + ("; disable " + "; ".join(f"{k}: {v}" for k, v in dis.items() if v) if any(dis.values()) else "")
                       + f"; {sel_note})")
    n_conf = [int(d["event"].nunique()) for d in conf_dfs.values() if d is not None and len(d)]
    rep.decision_events = min(n_conf) if n_conf else 0
    rep.policies = [p for p, d in dfs.items() if d is not None and len(settled(d))]
    rep.in_sample, rep.in_sample_why = in_sample, why
    for w in warns:
        rep.line(f"WARNING: {w}")
    rep.table("tau", tabs.get("tau"), "E6: by time to expiry at fill (selection sample; confirm_lo_c = CI lower bound on "
                                      "the confirmation sample).")
    rep.table("abs_z", tabs.get("z"), "E7: by |z| of the strategy's last quote cycle before the fill.")
    rep.table("yes_price", tabs.get("price"), "E7: by YES price of the fill.")
    rep.table("tau_x_z", tabs.get("tau_z"), "Joint split (small cells are noisy).")
    rep.write()
    for pol, df in dfs.items():
        if df is not None and len(df):
            write_csv(df, Path(out) / f"e67_segments_ledger_{pol}.csv", synthetic)
    return {**tabs, "verdict": rep.final_verdict(), "rule_outcome": rep.verdict}
