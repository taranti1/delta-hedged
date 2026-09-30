"""Quick screen (2026-09-30): would three candidate quote rules have blocked more losing than
winning fills?  Fixed-opportunity analysis: every recorded fill is kept or removed; the knock-on
effects (freed risk budget, other quotes placed instead) are NOT modeled.

Rules (primaries declared before any outcome was examined; the grids are context):
  final   block a quote with tau < T s and |z| < 2.5                  primary T=150
  jump    after a 1 s BRTI move >= k x trailing 1 s sigma (10 min), no quote placed and resting
          quotes pulled for P s ('pull': jump in [quote-P, fill-0.3 s])    primary k=4, P=10
  disagree  |F - Kalshi mid| > D at quote time                          primary D=10c
Inputs: paper logs (extracted *.sel), baseline replay ledgers, the Kalshi ticker/BRTI scan.
"""
from __future__ import annotations

import glob
import json
import os
import sys
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
from scipy.special import ndtri

SEL, SCAN, OUT = sys.argv[1], sys.argv[2], sys.argv[3]
SIM = "data/results/sim_20260927"
NS = 10**9
ET = timezone(timedelta(hours=-4))


def expiry_ns(ticker: str) -> int:
    # KXBTCD-26SEP2914-T82999.99: expiry 2026-09-29 14:00 ET
    code = ticker.split("-")[1]
    d = datetime.strptime(code[:7], "%y%b%d").replace(hour=int(code[7:9]), tzinfo=ET)
    return int(d.timestamp()) * NS


# ------------------------------------------------------------------ settlements
settle: dict[str, float] = {}
for f in glob.glob(f"{SEL}/pl_*.sel"):
    for line in open(f):
        r = json.loads(line)
        if r["k"] == "log.settle":
            settle[r["ticker"]] = r["px"] / 1e4
for t, v in json.load(open(f"{SIM}/official_outcomes.json")).items():
    if not t.startswith("_"):
        settle.setdefault(t, float(v))

# ------------------------------------------------------------------ paper fills
rows = []
for f in sorted(glob.glob(f"{SEL}/pl_*.sel")):
    session = os.path.basename(f)[3:-4]
    quotes, by_key, fills = {}, {}, []
    for line in open(f):
        r = json.loads(line)
        if r["k"] == "log.quote":
            if r.get("coid"):
                quotes[r["coid"]] = r
            by_key.setdefault((r["ticker"], r["side"], r["px"]), []).append(r)
        elif r["k"] == "log.fill":
            fills.append(r)
    for fl in fills:
        q = quotes.get(fl["coid"])
        if q is None:  # legacy sessions: the latest quote of this market / side / price
            cand = [x for x in by_key.get((fl["ticker"], fl["side"], fl["px"]), []) if x["t"] <= fl["t"]]
            q = cand[-1] if cand else None
        cfg = fl["cfg"].split("/")[0]
        rows.append(dict(src="paper", run=session, cfg=cfg, ticker=fl["ticker"], t_fill=fl["t"],
                         t_quote=q["t"] if q else np.nan, side=1 if fl["side"] == "bid" else -1,
                         px=fl["px"] / 1e4, ct=fl["qty"] / 100, fee=(fl.get("fee") or 0) / 1e6,
                         F_fill=fl["F"], F_quote=q.get("F", np.nan) if q else np.nan,
                         position=q.get("position") if q else None))
paper = pd.DataFrame(rows)

# ------------------------------------------------------------------ replay fills (baselines)
rrows = []
for run in ("s1_day0926_base_B", "s1_day0926_base_C", "s3_later_B", "s3_later_C",
            "s4_small_B", "s4_small_C"):
    pol = run[-1]
    d = pd.read_csv(f"{SIM}/{run}/ledger_{pol}.csv")
    for r in d.itertuples():
        rrows.append(dict(src="replay_" + pol, run=run, cfg="m1_small" if "small" in run else "m1",
                          ticker=r.ticker, t_fill=int(r.ts), t_quote=int(r.ts - r.order_age_s * NS),
                          side=int(r.side), px=float(r.px), ct=float(r.contracts), fee=float(r.fee),
                          F_fill=float(r.F), F_quote=float(r.F_cycle), position=r.quote_position))
fills = pd.concat([paper, pd.DataFrame(rrows)], ignore_index=True)
fills["expiry"] = fills.ticker.map(expiry_ns)
fills["settle"] = fills.ticker.map(settle)
fills["t_quote"] = fills.t_quote.fillna(fills.t_fill)
fills["F_q"] = fills.F_quote.fillna(fills.F_fill)
fills["tau_q"] = (fills.expiry - fills.t_quote) / NS
fills["z_q"] = np.abs(ndtri(fills.F_q.clip(1e-9, 1 - 1e-9)))
fills["net"] = fills.side * fills.ct * (fills.settle - fills.px) - fills.fee

# ------------------------------------------------------------------ Kalshi mid + BRTI jumps
tk = pd.concat([pd.read_parquet(p) for p in glob.glob(f"{SCAN}/tk_*.parquet")]).dropna()
tk = tk.astype({"t": "int64", "bid": "float64", "ask": "float64", "bsz": "float64", "asz": "float64"}).sort_values("t")
tk = tk[tk.ticker.isin(set(fills.ticker))]
tk["mid"] = np.where((tk.bsz > 0) & (tk.asz > 0) & (tk.ask > tk.bid), (tk.bid + tk.ask) / 2, np.nan)
tk["spread"] = np.where(tk.mid.notna(), tk.ask - tk.bid, np.nan)


def mid_at(ticker_col, t_col, max_age_s=300):
    out_mid = np.full(len(fills), np.nan)
    out_spr = np.full(len(fills), np.nan)
    g = {k: v for k, v in tk.groupby("ticker")}
    for i, (tick, t) in enumerate(zip(fills[ticker_col], fills[t_col])):
        d = g.get(tick)
        if d is None:
            continue
        j = np.searchsorted(d.t.values, t, side="right") - 1
        if j >= 0 and t - d.t.values[j] <= max_age_s * NS:
            out_mid[i] = d.mid.values[j]
            out_spr[i] = d.spread.values[j]
    return out_mid, out_spr


fills["mid_q"], fills["spread_q"] = mid_at("ticker", "t_quote")
fills["t_fill_m"] = fills.t_fill - NS // 2
fills["mid_f"], _ = mid_at("ticker", "t_fill_m")
fills["dis_q"] = (fills.F_q - fills.mid_q).abs() * 100
fills["dis_f"] = (fills.F_fill - fills.mid_f).abs() * 100

br = pd.concat([pd.read_parquet(p) for p in glob.glob(f"{SCAN}/br_*.parquet")])
br = br.dropna().astype({"t": "int64", "v": "float64"}).sort_values("t").drop_duplicates("t")
s = pd.Series(br.v.values, index=pd.to_datetime(br.t.values))
s1 = s.resample("1s").last().ffill(limit=5)
d1 = s1.diff()
sig = d1.rolling("600s", min_periods=120).std().shift(1)
ratio = (d1.abs() / sig)
jump_times = {k: ratio.index[ratio >= k].asi8 for k in (3, 4, 5, 6)}
covered = s1.notna()
cov_t = covered.index.asi8[covered.values]


def jump_in(k, lo, hi):
    jt = jump_times[k]
    a = np.searchsorted(jt, lo, side="left")
    b = np.searchsorted(jt, hi, side="right")
    return b > a


def has_brti(t):
    j = np.searchsorted(cov_t, t) - 1
    return j >= 0 and t - cov_t[j] <= 5 * NS


fills["brti_ok"] = [has_brti(t) for t in fills.t_quote]
fills["mid_ok"] = fills.mid_q.notna()

# ------------------------------------------------------------------ rules
rules: dict[str, np.ndarray] = {}
for T in (120, 150, 180, 300):
    rules[f"final_T{T}"] = ((fills.tau_q < T) & (fills.z_q < 2.5)).values
for k in (3, 4, 5):
    for P in (5, 10, 30):
        lo = (fills.t_quote - P * NS).values
        rules[f"jump_block_k{k}_P{P}"] = np.array([jump_in(k, a, b) for a, b in zip(lo, fills.t_quote.values)])
        rules[f"jump_pull_k{k}_P{P}"] = np.array([jump_in(k, a, b) for a, b in
                                                   zip(lo, (fills.t_fill - int(0.3 * NS)).values)])
for D in (5, 10, 15, 20):
    rules[f"dis_q_D{D}"] = (fills.dis_q > D).fillna(False).values
    rules[f"dis_q_near_D{D}"] = ((fills.dis_q > D) & (fills.tau_q < 900)).fillna(False).values
    rules[f"dis_pull_D{D}"] = ((fills.dis_q > D) | (fills.dis_f > D)).fillna(False).values
PRIMARY = ["final_T150", "jump_pull_k4_P10", "dis_q_D10"]
rules["combo_primary"] = rules["final_T150"] | rules["jump_pull_k4_P10"] | rules["dis_q_D10"]
for k, v in rules.items():
    fills["r_" + k] = v
fills.to_parquet(f"{OUT}/fills.parquet")

# ------------------------------------------------------------------ evaluation
DATASETS = {
    "paper_all": fills.src.eq("paper"),
    "paper_m1_small": fills.src.eq("paper") & fills.cfg.eq("860cfc8faa25933c"),
    "paper_m1_corrected": fills.src.eq("paper") & fills.cfg.eq("ece7ceef2c224a55"),
    "replay_B_m1": fills.run.isin(["s1_day0926_base_B", "s3_later_B"]),
    "replay_C_m1": fills.run.isin(["s1_day0926_base_C", "s3_later_C"]),
    "replay_B_small": fills.run.eq("s4_small_B"),
    "replay_C_small": fills.run.eq("s4_small_C"),
}
rng = np.random.default_rng(20260930)


def exp_series(d, keep):
    x = d.assign(k=np.where(keep, d.net, 0.0)).groupby("expiry").k.sum().sort_index()
    return x


def stats(d, blocked):
    keep = ~blocked
    base = exp_series(d, np.ones(len(d), bool))
    kept = exp_series(d, keep)
    diff = kept - base  # = -(blocked P&L) per expiration
    boots = [diff.sample(len(diff), replace=True, random_state=rng.integers(1 << 31)).sum() for _ in range(4000)]
    lo, hi = np.percentile(boots, [2.5, 97.5])

    def risk(x):
        cum = x.cumsum()
        return dict(sharpe=(x.mean() / x.std(ddof=1)) if x.std(ddof=1) > 0 else np.nan, worst=x.min(),
                    maxdd=(cum.cummax().clip(lower=0) - cum).max())

    rb, rk = risk(base), risk(kept)
    return dict(n_exp=len(base), fills=int(len(d)), blocked_fills=int(blocked.sum()),
                ct_kept_pct=100 * d.ct[keep].sum() / d.ct.sum(), base_net=base.sum(), kept_net=kept.sum(),
                delta=diff.sum(), delta_lo=lo, delta_hi=hi, blocked_net=d.net[blocked].sum(),
                blocked_ct=d.ct[blocked].sum(),
                base_c_ct=100 * base.sum() / d.ct.sum(), kept_c_ct=100 * kept.sum() / max(d.ct[keep].sum(), 1e-9),
                base_sharpe=rb["sharpe"], kept_sharpe=rk["sharpe"], base_worst=rb["worst"], kept_worst=rk["worst"],
                base_maxdd=rb["maxdd"], kept_maxdd=rk["maxdd"])


res = []
for dn, m in DATASETS.items():
    d = fills[m & fills.settle.notna()].reset_index(drop=True)
    for rn in rules:
        st = stats(d, d["r_" + rn].values)
        res.append(dict(dataset=dn, rule=rn, primary=rn in PRIMARY or rn == "combo_primary", **st))
res = pd.DataFrame(res)
res.to_csv(f"{OUT}/rule_results.csv", index=False)

cov = fills.groupby("src").agg(fills=("net", "size"), settled=("settle", lambda x: x.notna().mean()),
                               mid_ok=("mid_ok", "mean"), brti_ok=("brti_ok", "mean"))
cov.to_csv(f"{OUT}/coverage.csv")
print(cov)
pd.set_option("display.width", 250)
cols = ["dataset", "rule", "fills", "blocked_fills", "ct_kept_pct", "base_net", "delta", "delta_lo", "delta_hi",
        "base_sharpe", "kept_sharpe", "base_worst", "kept_worst", "base_maxdd", "kept_maxdd"]
print(res[res.primary][cols].round(2).to_string(index=False))
