"""Single entry point for the fair-value calibration study.

    python -m dh.research.fv_study.run --out docs/research [--jobs 4] [--quick]

Writes docs/research/tables/fv_*.csv, docs/research/figures/fv_*.png, the production config
dh/models/data/fv_recommended.json and a markdown digest of the key numbers
(data/cache/fv_study/digest.md, not committed).  Deterministic for a given data snapshot.
"""

from __future__ import annotations

import argparse
import json
import math
import time
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import numpy as np
import pandas as pd

from dh.models.calibration import ece, murphy
from dh.models.tails import GAUSS, StudentT
from dh.models.vol import SeasonalVol, annualized
from dh.research.fv_study import figures as figs
from dh.research.fv_study.config import CACHE_DIR, CFG, DATA_DIR, REPO, SEED, utc
from dh.research.fv_study.data import Minutes, add_months, load_minutes
from dh.research.fv_study.evaluate import (
    exceedance_at_own_sd,
    group_keys,
    make_cells,
    predict,
    reliability_by_tau,
    score_table,
    tail_calibration,
)
from dh.research.fv_study.panel import build_panel, estimate_kappa, hour_features, raw_ewmas
from dh.research.fv_study.proxy import simulate_proxy
from dh.research.fv_study.q4_predictability import decision_level, drift_oos, minute_level
from dh.research.fv_study.q5_greeks import greeks_table, window_vs_naive
from dh.research.fv_study.walkforward import (
    HL_LABEL,
    ModelDef,
    SeasonalSpec,
    fit_seasonal,
    main_models,
    norm_sigma_series,
    run_period,
)

BASE = "G-raw-2h"
KEY_MODELS = ["G-raw-2h", "G-blend", "T-blend", "MIX-blend", "EMP-blend"]

# shared state for forked workers (set before the pool is created)
_G: dict = {}


def _log(msg: str) -> None:
    print(f"[fv_study {time.strftime('%H:%M:%S')}] {msg}", flush=True)


def _run_job(args):
    period, start, end, models, seas = args
    return run_period(_G["M"], _G["P"], _G["raw"], _G["ns"], period, start, end, models, seas)


def _run_panel_job(args):
    _label, Pv, period, start, end, models, seas, cfg = args
    return run_period(_G["M"], Pv, _G["raw"], _G["ns"], period, start, end, models, seas, cfg)


def _parallel_panels(jobs: list, n_jobs: int) -> list:
    if n_jobs <= 1:
        return [_run_panel_job(j) for j in jobs]
    import multiprocessing as mp

    with ProcessPoolExecutor(max_workers=min(n_jobs, len(jobs)), mp_context=mp.get_context("fork")) as ex:
        return list(ex.map(_run_panel_job, jobs))


def _parallel(jobs: list, n_jobs: int) -> list:
    if n_jobs <= 1:
        return [_run_job(j) for j in jobs]
    import multiprocessing as mp

    with ProcessPoolExecutor(max_workers=n_jobs, mp_context=mp.get_context("fork")) as ex:
        return list(ex.map(_run_job, jobs))


def _md(df: pd.DataFrame, floatfmt: str = ".4f") -> str:
    """Markdown table (no external deps)."""
    cols = list(df.columns)
    out = ["| " + " | ".join(str(c) for c in cols) + " |", "|" + "|".join("---" for _ in cols) + "|"]
    for _, r in df.iterrows():
        cells = []
        for c in cols:
            v = r[c]
            if isinstance(v, (float, np.floating)):
                cells.append(format(v, floatfmt) if np.isfinite(v) else "")
            else:
                cells.append(str(v))
        out.append("| " + " | ".join(cells) + " |")
    return "\n".join(out)


def data_summary(M: Minutes, P, feats) -> pd.DataFrame:
    rows = []
    for name, start, end in CFG.periods:
        sel = (P.T > utc(start)) & (P.T <= utc(end))
        s1d = feats["sig1d"][:, 0]
        inc, exc = sel & P.valid, sel & ~P.valid
        i0, i1 = int(M.idx(utc(start))), int(M.idx(utc(end)))
        rows.append({
            "period": name, "start": start, "end": end, "hours_total": int(sel.sum()), "hours_valid": int(inc.sum()),
            "excl_outage": int((sel & P.excl_outage).sum()), "excl_no_trade_settle_min": int((sel & P.excl_notrade & ~P.excl_outage).sum()),
            "outage_minutes": int(M.outage[i0:i1].sum()), "zero_volume_minutes": int(M.zero_flat[i0:i1].sum()),
            "ann_vol_included_hours": annualized(float(np.nanmedian(s1d[inc]))) if inc.any() else np.nan,
            "ann_vol_excluded_hours": annualized(float(np.nanmedian(s1d[exc]))) if exc.any() else np.nan,
            "kappa_in_period": estimate_kappa(M, utc(start), utc(end)),
            "ann_vol_realized_1m": float(np.nanstd(M.r[i0:i1][M.r_valid[i0:i1]]) * math.sqrt(525960)),
        })
    return pd.DataFrame(rows)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", default=str(REPO / "docs" / "research"))
    ap.add_argument("--data-dir", default=str(DATA_DIR))
    ap.add_argument("--jobs", type=int, default=4)
    ap.add_argument("--quick", action="store_true", help="fewer layouts/bootstraps (debug only)")
    ap.add_argument("--config-out", default=str(REPO / "dh" / "models" / "data" / "fv_recommended.json"))
    args = ap.parse_args(argv)
    out = Path(args.out)
    tdir, fdir = out / "tables", out / "figures"
    tdir.mkdir(parents=True, exist_ok=True)
    fdir.mkdir(parents=True, exist_ok=True)
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    digest: list[str] = ["# fv_study digest (generated)\n"]
    t_start = time.time()

    # ------------------------------------------------------------------ data
    _log("loading data")
    M = load_minutes(Path(args.data_dir))
    P = build_panel(M)
    raw = raw_ewmas(M, CFG.half_lives_min)
    ns = norm_sigma_series(M)
    feats = hour_features(M, P, raw)
    _G.update(M=M, P=P, raw=raw, ns=ns)
    ds = data_summary(M, P, feats)
    ds.to_csv(tdir / "fv_data_summary.csv", index=False, float_format="%.5g")
    digest += ["## data summary", _md(ds, ".4g")]
    _log(f"data: {M.n} minutes, {P.n_hours} hours ({time.time() - t_start:.0f}s)")

    # ------------------------------------------------------------------ proxy error
    px = simulate_proxy(lams=(0.1, 0.3, 1.0), half_spread=(0.0, 0.05), n_paths=10_000 if args.quick else 20_000)
    px.to_csv(tdir / "fv_proxy_error.csv", index=False, float_format="%.5g")
    digest += ["## proxy error (simulation; units of 1-minute sd)", _md(px, ".4f")]

    # ------------------------------------------------------------------ Q3: seasonal layouts (validation picks)
    layouts = [SeasonalSpec(layout=l, bucket_s=b, tz=tz) for (l, b, tz) in CFG.seasonal_layouts]
    if args.quick:
        layouts = layouts[:2] + layouts[-1:]
    lay_models = [ModelDef("G-blend", "blend", "gauss"), ModelDef("T-blend", "blend", "t")]
    jobs = [(name, s, e, lay_models, sp) for sp in layouts for (name, s, e) in CFG.periods]
    _log(f"Q3 layouts: {len(jobs)} walk-forward runs")
    lay_res = _parallel(jobs, args.jobs)
    lay_rows = []
    for name, _, _ in CFG.periods:
        # identical cells across layouts (grid placement uses the layout-free reference vol),
        # so every layout/model pair is scored against the same outcomes: paired CIs
        runs = [(sp, R) for (n, _, _, _, sp), R in zip(jobs, lay_res) if n == name]
        C = make_cells(P, runs[0][1], "z")
        preds = {}
        for sp, R in runs:
            Cr = make_cells(P, R, "z")
            assert Cr.n == C.n and np.array_equal(Cr.K, C.K)
            for m in R.sd:
                preds[f"{sp.label}|{m}"] = predict(P, R, C, m)
        k = group_keys(P, runs[0][1], C, feats)
        tab = score_table(C, preds, {"all": k["all"], "tau": k["tau"]}, base="flat|T-blend",
                          n_boot=200 if args.quick else CFG.n_boot)
        tab[["layout", "model"]] = tab["model"].str.split("|", expand=True)
        tab.insert(0, "period", name)
        lay_rows.append(tab)
    lay = pd.concat(lay_rows, ignore_index=True)
    lay.to_csv(tdir / "fv_q3_layouts.csv", index=False, float_format="%.5g")
    v = lay[(lay.period == "validation") & (lay.model == "T-blend") & (lay.group == "all")].sort_values("log_loss")
    best = v.iloc[0]["layout"]
    chosen = next(sp for sp in layouts if sp.label == best)
    digest += ["## Q3 layouts (all taus, z-grid)",
               _md(lay[lay.group == "all"][["period", "layout", "model", "log_loss", "d_ll_vs_base", "d_ll_lo", "d_ll_hi", "brier"]].sort_values(["period", "model", "log_loss"]), ".5f"),
               f"\nchosen on validation: **{best}**"]
    _log(f"chosen seasonal layout (validation T-blend log loss): {best}")

    # ------------------------------------------------------------------ main walk-forward
    models = main_models()
    _log("main walk-forward runs")
    main_res = _parallel([(name, s, e, models, chosen) for (name, s, e) in CFG.periods], min(args.jobs, len(CFG.periods)))
    RES = {R.period: R for R in main_res}
    params = pd.concat([pd.DataFrame(R.params) for R in main_res], ignore_index=True)
    params.to_csv(CACHE_DIR / "fv_q1_params_monthly.csv", index=False, float_format="%.5g")
    par_sum = (params.groupby(["period", "model", "param", "tau_min"])["value"].agg(["mean", "min", "max"]).reset_index())
    par_sum.to_csv(tdir / "fv_q1_params.csv", index=False, float_format="%.4g")
    digest += ["## fitted parameters (mean over walk-forward months)",
               _md(par_sum[par_sum.period == "test"].pivot_table(index=["model", "param"], columns="tau_min", values="mean").reset_index(), ".3f")]

    # ------------------------------------------------------------------ Q1 scores
    score_frames, rel_frames, murphy_rows = [], [], []
    tail_frames, own_frames = [], []
    for name, R in RES.items():
        for grid in ("z", "dollar"):
            C = make_cells(P, R, grid)
            preds = {m: predict(P, R, C, m) for m in R.sd}
            keys = group_keys(P, R, C, feats)
            if grid == "dollar":
                keys = {k: keys[k] for k in ("all", "tau", "absz", "vol_regime")}
            tab = score_table(C, preds, keys, base=BASE, n_boot=200 if args.quick else CFG.n_boot)
            tab.insert(0, "period", name)
            score_frames.append(tab)
            if grid == "z":
                for m in KEY_MODELS:
                    rt = reliability_by_tau(C, preds[m], m)
                    rt.insert(0, "period", name)
                    rel_frames.append(rt)
                for m in preds:
                    mu = murphy(preds[m], C.y, bins=20)
                    murphy_rows.append({"period": name, "grid": grid, "model": m, "tau_min": "all", **mu})
                    for k_i, tau in enumerate(CFG.taus_min):
                        sel = C.k == k_i
                        mu = murphy(preds[m][sel], C.y[sel], bins=20)
                        murphy_rows.append({"period": name, "grid": grid, "model": m, "tau_min": str(tau), **mu,
                                            "ece": ece(preds[m][sel], C.y[sel], bins=20)})
                _log(f"{name}: z-grid cells {C.n}")
        # Q2 tail grid
        C = make_cells(P, R, "tail")
        preds = {m: predict(P, R, C, m) for m in KEY_MODELS + ["G-seas-2h", "T-raw-2h"]}
        taus = np.asarray(CFG.taus_min)[C.k]
        splits = {"all": np.ones(C.n, bool), "up": C.zref > 0, "down": C.zref < 0,
                  "tau_2-5m": taus <= 5, "tau_10-20m": (taus >= 10) & (taus <= 20), "tau_30-60m": taus >= 30}
        tc = tail_calibration(C, preds, splits, n_boot=200 if args.quick else CFG.n_boot)
        tc.insert(0, "period", name)
        tail_frames.append(tc)
        for m in ("G-raw-2h", "G-blend", "T-blend", "MIX-blend", "EMP-blend"):
            oe = exceedance_at_own_sd(P, R, m, n_boot=200 if args.quick else CFG.n_boot)
            oe.insert(0, "period", name)
            own_frames.append(oe)
    scores = pd.concat(score_frames, ignore_index=True)
    scores.to_csv(tdir / "fv_q1_scores.csv", index=False, float_format="%.5g")
    rel = pd.concat(rel_frames, ignore_index=True)
    rel.to_csv(tdir / "fv_q1_reliability.csv", index=False, float_format="%.5g")
    mur = pd.DataFrame(murphy_rows)
    mur.to_csv(tdir / "fv_q1_murphy.csv", index=False, float_format="%.5g")
    tails = pd.concat(tail_frames, ignore_index=True)
    tails.to_csv(tdir / "fv_q2_tail_calibration.csv", index=False, float_format="%.5g")
    own = pd.concat(own_frames, ignore_index=True)
    own.to_csv(tdir / "fv_q2_fair_price_at_z.csv", index=False, float_format="%.5g")

    for name in RES:
        for grid in ("z", "dollar"):
            s = scores[(scores.period == name) & (scores.grid == grid) & (scores.group == "all")].sort_values("log_loss")
            digest += [f"## Q1 {name} {grid}-grid overall",
                       _md(s[["model", "n", "brier", "log_loss", "d_ll_vs_base", "d_ll_lo", "d_ll_hi", "d_brier_vs_base", "d_brier_lo", "d_brier_hi"]], ".5f")]
        s = scores[(scores.period == name) & (scores.grid == "z") & (scores.group == "tau")]
        digest += [f"## Q1 {name} z-grid log loss by tau",
                   _md(s.pivot_table(index="model", columns="level", values="log_loss").reset_index(), ".4f"),
                   f"## Q1 {name} z-grid d_ll vs base by tau (x1000)",
                   _md((s.pivot_table(index="model", columns="level", values="d_ll_vs_base") * 1000).reset_index(), ".2f")]
        for g in ("absz", "vol_regime", "trend", "weekend", "vol_burst", "fomc", "expost_big_move"):
            s = scores[(scores.period == name) & (scores.grid == "z") & (scores.group == g) & scores.model.isin(KEY_MODELS + ["G-seas-2h", "G-raw-10m", "G-raw-1d"])]
            digest += [f"## Q1 {name} z-grid by {g}: log loss (d vs base x1000 [lo,hi])",
                       _md(s.assign(txt=lambda d: d.log_loss.map("{:.4f}".format) + " (" + (1000 * d.d_ll_vs_base).map("{:+.1f}".format)
                                    + " [" + (1000 * d.d_ll_lo).map("{:+.1f}".format) + "," + (1000 * d.d_ll_hi).map("{:+.1f}".format) + "])")
                           .pivot_table(index="model", columns="level", values="txt", aggfunc="first").reset_index())]
    digest += ["## Murphy decomposition (z grid, all taus)", _md(mur[mur.tau_min == "all"], ".5f"),
               "## reliability term x1e4 by tau (z grid)",
               _md((mur[mur.tau_min != "all"].pivot_table(index=["period", "model"], columns="tau_min", values="reliability") * 1e4).reset_index(), ".2f"),
               "## ECE x100 by tau (z grid, 20 bins)",
               _md((mur[mur.tau_min != "all"].pivot_table(index=["period", "model"], columns="tau_min", values="ece") * 100).reset_index(), ".3f")]
    for name in RES:
        t = tails[(tails.period == name)]
        for split in ("all", "up", "down", "tau_2-5m", "tau_10-20m", "tau_30-60m"):
            tt = t[t.split == split]
            digest += [f"## Q2 {name} tail calibration split={split}",
                       _md(tt[["model", "q_bucket", "n", "n_events", "mean_model_p", "freq", "freq_lo", "freq_hi", "ratio_freq_to_p", "ratio_lo", "ratio_hi"]], ".4f")]
        o = own[(own.period == name) & (own.z == 2.0)]
        digest += [f"## Q2 {name} realized frequency beyond +/-2 own sd", _md(o, ".4f")]

    # ------------------------------------------------------------------ Q3 profile description
    prof_rows = []
    for name, start, end in CFG.periods:
        sv = fit_seasonal(M, utc(start), utc(end), ns, SeasonalSpec(layout="day_type", bucket_s=3600, tz="America/New_York"))
        for b, f in enumerate(sv.factors):
            prof_rows.append({"period": name, "day_type": "weekday" if b < 24 else "weekend", "hour_et": b % 24, "factor": f})
        svu = fit_seasonal(M, utc(start), utc(end), ns, SeasonalSpec(layout="day_type", bucket_s=3600, tz="UTC"))
        for b, f in enumerate(svu.factors):
            prof_rows.append({"period": name, "day_type": ("weekday" if b < 24 else "weekend") + "_utc", "hour_et": b % 24, "factor": f})
    prof = pd.DataFrame(prof_rows)
    prof.to_csv(tdir / "fv_q3_seasonal_profile.csv", index=False, float_format="%.5g")
    pv = prof[prof.period == "validation"].set_index(["day_type", "hour_et"])["factor"]
    pt = prof[prof.period == "test"].set_index(["day_type", "hour_et"])["factor"]
    corr = float(np.corrcoef(pv.reindex(pt.index), pt)[0, 1])
    digest += ["## Q3 profile (test, ET)", _md(prof[prof.period == "test"].pivot_table(index="hour_et", columns="day_type", values="factor").reset_index(), ".3f"),
               f"\nprofile correlation validation vs test: {corr:.3f}"]

    # ------------------------------------------------------------------ Q4 predictability
    raw30 = raw[30]
    q4m = pd.concat([minute_level(M, raw30, s, e).assign(period=n) for (n, s, e) in CFG.periods], ignore_index=True)
    q4m.to_csv(tdir / "fv_q4_minute.csv", index=False, float_format="%.5g")
    q4d = pd.concat([decision_level(M, P, RES[n], "T-blend", raw30, n_boot=200 if args.quick else CFG.n_boot).assign(period=n)
                     for (n, _, _) in CFG.periods], ignore_index=True)
    q4d.to_csv(tdir / "fv_q4_decision.csv", index=False, float_format="%.5g")
    q4o = []
    for n, R in RES.items():
        C = make_cells(P, R, "z")
        for k in (1, 5, 15):
            q4o.append(drift_oos(M, P, R, C, "T-blend", raw30, past_min=k).assign(period=n))
    q4o = pd.concat(q4o, ignore_index=True)
    q4o.to_csv(tdir / "fv_q4_drift_oos.csv", index=False, float_format="%.5g")
    digest += ["## Q4 minute level", _md(q4m, ".4f"), "## Q4 decision level", _md(q4d, ".4f"), "## Q4 drift OOS", _md(q4o, ".5f")]

    # ------------------------------------------------------------------ robustness / adversarial checks
    _log("robustness checks")
    import dataclasses

    rob = []
    t_name, t_start_s, t_end_s = CFG.periods[-1]
    Rt = RES[t_name]
    Ct = make_cells(P, Rt, "z")
    rob_models = ["G-raw-2h", "G-blend", "T-blend", "MIX-blend", "EMP-blend"]
    pr_t = {m: predict(P, Rt, Ct, m) for m in rob_models}
    for blk, div in (("day", 1), ("week", 7), ("month(30d)", 30)):
        Cb = dataclasses.replace(Ct, day=Ct.day // div)
        tb = score_table(Cb, pr_t, {"all": (np.zeros(Ct.n, dtype=np.int64), ["all"])}, base="G-blend",
                         n_boot=200 if args.quick else CFG.n_boot)
        rob.append(tb.assign(variant=f"bootstrap blocks = {blk}"))
    rob_models2 = [ModelDef("G-blend", "blend", "gauss"), ModelDef("T-blend", "blend", "t")]
    # (b) keep hours whose settlement minute had no Bitstamp trade (proxy = stale close)
    P_inc = dataclasses.replace(P, valid=~P.excl_outage & np.isfinite(P.A) & np.all(np.isfinite(P.spot), axis=1))
    # (c) look-ahead placebo: spot and vol features taken from the candle that STARTS at t
    j0_la = P.j0 + 1
    P_la = dataclasses.replace(P, j0=j0_la, spot=M.c[j0_la])
    variants = [
        ("incl. no-trade settlement minutes", P_inc, CFG),
        ("LOOK-AHEAD PLACEBO (spot = close at t+60s)", P_la, CFG),
        ("training window 6 months", P, dataclasses.replace(CFG, train_months=6)),
        ("training window 24 months", P, dataclasses.replace(CFG, train_months=24)),
    ]
    var_res = _parallel_panels([(lab, Pv, t_name, t_start_s, t_end_s, rob_models2, chosen, cv) for lab, Pv, cv in variants], args.jobs)
    for (lab, Pv, _cv), Rv in zip(variants, var_res):
        Cv = make_cells(Pv, Rv, "z")
        pv = {m: predict(Pv, Rv, Cv, m) for m in Rv.sd}
        kv = group_keys(Pv, Rv, Cv, feats)
        tb = score_table(Cv, pv, {"all": kv["all"], "tau": kv["tau"]}, base="G-blend", n_boot=200 if args.quick else CFG.n_boot)
        rob.append(tb.assign(variant=lab))
    # reference: the main run on the same keys
    kt = group_keys(P, Rt, Ct, feats)
    tb = score_table(Ct, {m: pr_t[m] for m in ("G-blend", "T-blend")}, {"all": kt["all"], "tau": kt["tau"]}, base="G-blend",
                     n_boot=200 if args.quick else CFG.n_boot)
    rob.append(tb.assign(variant="main"))
    rob = pd.concat(rob, ignore_index=True)
    rob.insert(0, "period", t_name)
    rob.to_csv(tdir / "fv_robustness.csv", index=False, float_format="%.5g")
    digest += ["## robustness (test, z grid)",
               _md(rob[rob.group == "all"][["variant", "model", "n", "log_loss", "ll_lo", "ll_hi", "d_ll_vs_base", "d_ll_lo", "d_ll_hi"]], ".5f"),
               _md(rob[(rob.group == "tau") & (rob.model == "T-blend")].pivot_table(index="variant", columns="level", values="log_loss").reset_index(), ".4f")]

    # ------------------------------------------------------------------ recommended production config
    test = RES["test"]
    last_mi = len(test.months) - 1
    last_ms = test.months[last_mi]
    pr = params[(params.period == "test") & (params.month == last_ms)]
    taus_s = [int(t) * 60 for t in CFG.taus_min]
    w_by_tau = {}
    for t in CFG.taus_min:
        row = pr[(pr.model == "blend") & (pr.tau_min == t)].set_index("param")["value"]
        w_by_tau[str(t * 60)] = [round(float(row[f"w_{HL_LABEL[h]}"]), 6) for h in CFG.half_lives_min]
    t_by_tau = {}
    for t in CFG.taus_min:
        row = pr[(pr.model == "T-blend") & (pr.tau_min == t)].set_index("param")["value"]
        t_by_tau[str(t * 60)] = {"nu": round(float(row["nu"]), 4), "scale": round(float(row["c"]), 5)}
    # the production seasonal profile: chosen layout fitted on the latest train window
    data_end = int(M.ts[-1]) + 60
    sv_prod = fit_seasonal(M, data_end - 365 * 86400, data_end, ns, chosen)
    rec = {
        "generated_by": "python -m dh.research.fv_study.run",
        "data_end_utc": int(M.ts[-1]) + 60,
        "train_window_months": CFG.train_months,
        "fit_month_utc": last_ms,
        "note": "Blend weights and tail parameters by decision horizon (seconds to settlement), fitted walk-forward on the "
                "latest 12 months of Bitstamp 1-minute data (proxy settlement). Re-fit on BRTI once captured.",
        "vol": {"half_lives_s": [h * 60 for h in CFG.half_lives_min], "weights_by_horizon_s": w_by_tau,
                "min_dt_s": 60.0, "max_dt_s": 600.0, "units": "per-second log variance weights on deseasonalized EWMAs"},
        "seasonal": {**sv_prod.to_dict(), "factors": [round(f, 5) for f in sv_prod.factors]},
        "tail": {"kind": "student_t", "by_horizon_s": t_by_tau},
    }
    cfg_path = Path(args.config_out)
    cfg_path.parent.mkdir(parents=True, exist_ok=True)
    cfg_path.write_text(json.dumps(rec, indent=1, sort_keys=True))
    digest += ["## recommended config", "```json", json.dumps(rec, indent=1, sort_keys=True)[:4000], "```"]

    # ------------------------------------------------------------------ Q5 greeks at current spot / vol
    spot_now = float(M.c[-1])
    e_last = int(np.flatnonzero(test.month_of == last_mi)[-1])
    sig_now = float(np.sqrt(test.blend_sigma2[e_last, 0]))  # blend per-second log vol at the last eval hour, 60m horizon
    t60 = t_by_tau[str(3600)]
    tails_q5 = {"gauss": GAUSS, f"student_t({t60['nu']:.2f})": StudentT(t60["nu"])}
    q5 = pd.concat([
        greeks_table(spot_now, sig_now, tails_q5).assign(vol_label=f"current blend ({annualized(sig_now):.0%} ann.)", sigma_ann=annualized(sig_now)),
        greeks_table(spot_now, 0.35 / math.sqrt(365.25 * 86400), {"gauss": GAUSS}).assign(vol_label="35% ann.", sigma_ann=0.35),
        greeks_table(spot_now, 0.60 / math.sqrt(365.25 * 86400), {"gauss": GAUSS}).assign(vol_label="60% ann.", sigma_ann=0.60),
    ], ignore_index=True)
    q5.insert(0, "spot", spot_now)
    q5.to_csv(tdir / "fv_q5_greeks.csv", index=False, float_format="%.5g")
    wn = window_vs_naive(spot_now, sig_now)
    wn.to_csv(tdir / "fv_q5_window_vs_naive.csv", index=False, float_format="%.5g")
    digest += ["## Q5 exact window vs naive (Gauss, current vol)", _md(wn, ".3f")]
    q5d = q5[(q5["tail_model"] == "gauss") & (q5.vol_label.str.startswith("current"))]
    digest += [f"## Q5 greeks at spot {spot_now:.2f}, sigma {annualized(sig_now):.3f} ann.",
               _md(q5d.pivot_table(index="state", columns="z", values="hedge_notional_usd", sort=False).reset_index(), ".1f"),
               "p_yes", _md(q5d.pivot_table(index="state", columns="z", values="p_yes", sort=False).reset_index(), ".4f"),
               "delta change per 1 sd (BTC)", _md(q5d.pivot_table(index="state", columns="z", values="delta_change_per_1sd_btc", sort=False).reset_index(), ".5f")]

    # ------------------------------------------------------------------ figures
    rel_t = rel[rel.period == "test"]
    figs.reliability_panels(rel_t, ["G-raw-2h", "G-blend", "T-blend"], ["Gauss, EWMA 2h", "Gauss, seasonal blend", "Student-t, seasonal blend"],
                            path=fdir / "fv_reliability_by_tau.png")
    figs.tail_ratio(tails[tails.period == "test"], ["G-blend", "T-blend", "EMP-blend"], ["Gauss (blend)", "Student-t (blend)", "empirical (blend)"],
                    path=fdir / "fv_tail_calibration.png")
    figs.seasonality(prof[prof.period == "test"], path=fdir / "fv_seasonality.png")
    figs.dll_by_tau(scores[(scores.period == "test") & (scores.grid == "z")], ["G-seas-2h", "G-blend", "T-blend"],
                    ["Gauss, seasonal EWMA 2h", "Gauss, seasonal blend", "Student-t, seasonal blend"], path=fdir / "fv_logloss_by_tau.png")

    (CACHE_DIR / "digest.md").write_text("\n\n".join(digest))
    _log(f"done in {time.time() - t_start:.0f}s; digest at {CACHE_DIR / 'digest.md'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
