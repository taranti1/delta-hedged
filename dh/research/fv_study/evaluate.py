"""Strike grids, model probabilities and scoring with day-block bootstrap CIs.

Every contract evaluated is a KXBTCD-style 'greater' threshold: YES iff A_T > K.
Probabilities come from the production library (dh.models.fairvalue.digital_vec) with a
full 60-print window still ahead (k_fixed = 0): at every tested decision time the settlement
window has not started, so P(YES) = sf((K - spot) / sd) with sd the model sd of the proxy.

Grids (strike placement uses only the parameter-free reference sd at t):
  'z'       K = spot + z * sd_ref,  z in [-3, 3] step 0.25
  'dollar'  K = multiples of $250 within spot +/- 4 sd_ref (Kalshi-like fixed-dollar strikes)
  'tail'    K = spot +/- |z| * sd_ref, |z| in [1.5, 3.5] step 0.25
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from dh.models.fairvalue import digital_vec
from dh.research.fv_study.config import CFG, SEED, StudyConfig
from dh.research.fv_study.panel import Panel
from dh.research.fv_study.walkforward import PeriodResult

EPS = 1e-6


@dataclass
class Cells:
    grid: str
    e: np.ndarray  # eval-hour position (index into PeriodResult.eval_h)
    k: np.ndarray  # tau index
    K: np.ndarray  # strike ($)
    zref: np.ndarray  # (K - spot) / sd_ref
    y: np.ndarray  # 1.0 if A > K
    day: np.ndarray  # day id (for block bootstrap)
    mi: np.ndarray  # month index

    @property
    def n(self) -> int:
        return int(self.K.size)


def make_cells(P: Panel, R: PeriodResult, grid: str, cfg: StudyConfig = CFG) -> Cells:
    n_e, nk = R.sd_ref.shape
    h = R.eval_h
    spot = P.spot[h]  # (n_e, K)
    A = P.A[h]
    if grid in ("z", "tail"):
        zs = np.asarray(cfg.z_grid if grid == "z" else np.r_[-np.asarray(cfg.tail_abs_z)[::-1], np.asarray(cfg.tail_abs_z)], dtype=np.float64)
        e = np.repeat(np.arange(n_e), nk * zs.size)
        k = np.tile(np.repeat(np.arange(nk), zs.size), n_e)
        z = np.tile(zs, n_e * nk)
        K = spot[e, k] + z * R.sd_ref[e, k]
    elif grid == "dollar":
        lo = np.ceil((spot - cfg.dollar_max_sd * R.sd_ref) / cfg.dollar_step).astype(np.int64)
        hi = np.floor((spot + cfg.dollar_max_sd * R.sd_ref) / cfg.dollar_step).astype(np.int64)
        cnt = np.maximum(hi - lo + 1, 0)
        e = np.repeat(np.repeat(np.arange(n_e), nk), cnt.ravel())
        k = np.repeat(np.tile(np.arange(nk), n_e), cnt.ravel())
        starts = np.repeat(lo.ravel(), cnt.ravel())
        offs = np.arange(cnt.sum()) - np.repeat(np.cumsum(cnt.ravel()) - cnt.ravel(), cnt.ravel())
        K = (starts + offs) * cfg.dollar_step
        z = (K - spot[e, k]) / R.sd_ref[e, k]
    else:
        raise ValueError(grid)
    ok = np.isfinite(K) & np.isfinite(z)
    e, k, K, z = e[ok], k[ok], K[ok], z[ok]
    y = (A[e] > K).astype(np.float64)
    return Cells(grid=grid, e=e, k=k, K=K, zref=z, y=y, day=P.day[h][e], mi=R.month_of[e])


def predict(P: Panel, R: PeriodResult, cells: Cells, model: str) -> np.ndarray:
    """P(YES) for every cell under ``model`` (tail objects vary by month and tau)."""
    h = R.eval_h
    spot = P.spot[h][cells.e, cells.k]
    sd = R.sd[model][cells.e, cells.k]
    p = np.full(cells.n, np.nan)
    key = cells.mi * 1000 + cells.k
    order = np.argsort(key, kind="stable")
    ks = key[order]
    bounds = np.flatnonzero(np.r_[True, ks[1:] != ks[:-1], True])
    for a, b in zip(bounds[:-1], bounds[1:]):
        idx = order[a:b]
        mi, k = int(ks[a] // 1000), int(ks[a] % 1000)
        tail = R.tails[model][(mi, k)]
        p[idx] = digital_vec("greater", spot[idx], sd[idx], tail, floor=cells.K[idx]).p_yes
    return p


def loss_terms(p: np.ndarray, y: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    pc = np.clip(p, EPS, 1 - EPS)
    return (p - y) ** 2, -(y * np.log(pc) + (1 - y) * np.log1p(-pc))


# ----------------------------------------------------------------------------- grouping keys
def zbucket(z: np.ndarray) -> tuple[np.ndarray, list[str]]:
    edges = np.array([0.0, 0.5, 1.0, 1.5, 2.0, 2.5, 3.01, np.inf])
    labels = ["0-0.5", "0.5-1", "1-1.5", "1.5-2", "2-2.5", "2.5-3", ">3"]
    return np.clip(np.searchsorted(edges, np.abs(z), side="right") - 1, 0, len(labels) - 1), labels


def group_keys(P: Panel, R: PeriodResult, cells: Cells, feats: dict[str, np.ndarray], cfg: StudyConfig = CFG) -> dict[str, tuple[np.ndarray, list[str]]]:
    """Reporting keys (all known at decision time t except 'expost_big_move')."""
    h = R.eval_h
    keys: dict[str, tuple[np.ndarray, list[str]]] = {}
    keys["all"] = (np.zeros(cells.n, dtype=np.int64), ["all"])
    keys["tau"] = (cells.k.astype(np.int64), [f"{t}m" for t in cfg.taus_min])
    keys["absz"] = zbucket(cells.zref)
    # vol regime terciles of trailing 1-day vol at t (thresholds over this period's eval hours)
    s1d = feats["sig1d"][h][cells.e, cells.k]
    q = np.nanquantile(feats["sig1d"][h][:, 0], [1 / 3, 2 / 3])
    keys["vol_regime"] = (np.searchsorted(q, s1d, side="right").astype(np.int64), ["low", "mid", "high"])
    tr = feats["trend"][h][cells.e, cells.k]
    keys["trend"] = (np.searchsorted(np.array([0.5, 1.5]), tr, side="right").astype(np.int64), ["chop(<0.5sd)", "mid", "trend(>1.5sd)"])
    keys["weekend"] = (P.weekend[h][cells.e].astype(np.int64), ["weekday", "weekend"])
    burst = feats["burst"][h][cells.e, cells.k]
    bq = np.nanquantile(feats["burst"][h][:, 0], 0.9)
    keys["vol_burst"] = ((burst > bq).astype(np.int64), ["normal", "burst(top10% sig10m/sig1d)"])
    keys["fomc"] = (P.fomc[h][cells.e].astype(np.int64), ["other", "FOMC 15:00ET"])
    # ex-post (outcome-conditioned, descriptive only): |A_T - spot(60m)| > 3 sd_ref(60m)
    big = np.abs(P.A[h] - P.spot[h][:, 0]) > 3.0 * R.sd_ref[:, 0]
    keys["expost_big_move"] = (big[cells.e].astype(np.int64), ["normal", "big move (ex post)"])
    return keys


# ----------------------------------------------------------------------------- scoring
def score_table(
    cells: Cells,
    preds: dict[str, np.ndarray],
    keys: dict[str, tuple[np.ndarray, list[str]]],
    base: str,
    n_boot: int = CFG.n_boot,
    seed: int = SEED,
) -> pd.DataFrame:
    """Brier / log loss (and paired differences vs ``base``) per group with day-block CIs."""
    days, d_idx = np.unique(cells.day, return_inverse=True)
    D = days.size
    rng = np.random.default_rng(seed)
    boot = rng.integers(0, D, size=(n_boot, D))
    terms = {m: loss_terms(p, cells.y) for m, p in preds.items()}
    rows = []
    for gname, (codes, labels) in keys.items():
        G = len(labels)
        flat = codes * D + d_idx
        cnt = np.bincount(flat, minlength=G * D).reshape(G, D).astype(np.float64)
        cnt_b = cnt[:, boot].sum(axis=2)  # (G, B)
        n_tot = cnt.sum(axis=1)
        sums = {}
        for m, (b, l) in terms.items():
            sums[m] = (
                np.bincount(flat, weights=b, minlength=G * D).reshape(G, D),
                np.bincount(flat, weights=l, minlength=G * D).reshape(G, D),
                np.bincount(flat, weights=preds[m], minlength=G * D).reshape(G, D),
            )
        yb = np.bincount(flat, weights=cells.y, minlength=G * D).reshape(G, D)
        Bb, Lb, _ = sums[base]
        for m, (B, L, Pm) in sums.items():
            with np.errstate(invalid="ignore", divide="ignore"):
                est_b = B.sum(1) / n_tot
                est_l = L.sum(1) / n_tot
                bs_l = L[:, boot].sum(axis=2) / cnt_b
                bs_b = B[:, boot].sum(axis=2) / cnt_b
                dl = (L - Lb)
                db = (B - Bb)
                est_dl = dl.sum(1) / n_tot
                est_db = db.sum(1) / n_tot
                bs_dl = dl[:, boot].sum(axis=2) / cnt_b
                bs_db = db[:, boot].sum(axis=2) / cnt_b
                citl = (yb.sum(1) - Pm.sum(1)) / n_tot
            for g in range(G):
                if n_tot[g] == 0:
                    continue
                rows.append({
                    "grid": cells.grid, "group": gname, "level": labels[g], "model": m, "n": int(n_tot[g]),
                    "n_days": int((cnt[g] > 0).sum()),
                    "brier": est_b[g], "brier_lo": np.nanquantile(bs_b[g], 0.025), "brier_hi": np.nanquantile(bs_b[g], 0.975),
                    "log_loss": est_l[g], "ll_lo": np.nanquantile(bs_l[g], 0.025), "ll_hi": np.nanquantile(bs_l[g], 0.975),
                    "d_ll_vs_base": est_dl[g], "d_ll_lo": np.nanquantile(bs_dl[g], 0.025), "d_ll_hi": np.nanquantile(bs_dl[g], 0.975),
                    "d_brier_vs_base": est_db[g], "d_brier_lo": np.nanquantile(bs_db[g], 0.025), "d_brier_hi": np.nanquantile(bs_db[g], 0.975),
                    "citl": citl[g], "base": base,
                })
    return pd.DataFrame(rows)


def reliability_by_tau(cells: Cells, p: np.ndarray, model: str, bins=None, taus=CFG.taus_min) -> pd.DataFrame:
    """Reliability table per decision time (Wilson CIs assume independence: indicative only)."""
    from dh.models.calibration import reliability

    bins = bins if bins is not None else (0.0, 0.01, 0.02, 0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95, 0.98, 0.99, 1.0)
    out = []
    for k, tau in enumerate(taus):
        m = cells.k == k
        t = reliability(p[m], cells.y[m], bins)
        t.insert(0, "tau_min", tau)
        t.insert(0, "model", model)
        out.append(t)
    return pd.concat(out, ignore_index=True)


TAIL_Q_EDGES = (0.0, 0.005, 0.01, 0.02, 0.05, 0.10, 0.20)
TAIL_Q_LABELS = ("<0.5%", "0.5-1%", "1-2%", "2-5%", "5-10%", "10-20%")


def tail_calibration(
    cells: Cells,
    preds: dict[str, np.ndarray],
    splits: dict[str, np.ndarray] | None = None,
    n_boot: int = CFG.n_boot,
    seed: int = 1,
) -> pd.DataFrame:
    """Tail-event frequency vs model probability, bucketed by the model's tail probability.

    Tail event: A > K for strikes above spot (YES), A <= K for strikes below spot (NO).
    q = model probability of the tail event.  CIs: day-block bootstrap of the frequency.
    """
    up = cells.zref > 0
    ev = np.where(up, cells.y, 1.0 - cells.y)
    days, d_idx = np.unique(cells.day, return_inverse=True)
    D = days.size
    rng = np.random.default_rng(seed)
    boot = rng.integers(0, D, size=(n_boot, D))
    splits = splits or {"all": np.ones(cells.n, dtype=bool)}
    rows = []
    edges = np.asarray(TAIL_Q_EDGES)
    for m, p in preds.items():
        q = np.where(up, p, 1.0 - p)
        qb = np.searchsorted(edges, q, side="right") - 1
        for sname, smask in splits.items():
            for b, lab in enumerate(TAIL_Q_LABELS):
                sel = smask & (qb == b)
                n = int(sel.sum())
                if n == 0:
                    continue
                num = np.bincount(d_idx[sel], weights=ev[sel], minlength=D)
                den = np.bincount(d_idx[sel], minlength=D).astype(np.float64)
                qs = np.bincount(d_idx[sel], weights=q[sel], minlength=D)
                bs = num[boot].sum(1) / np.maximum(den[boot].sum(1), 1e-12)
                bq = qs[boot].sum(1) / np.maximum(den[boot].sum(1), 1e-12)
                freq = num.sum() / den.sum()
                qm = qs.sum() / den.sum()
                ratio = bs / np.maximum(bq, 1e-12)
                rows.append({
                    "model": m, "split": sname, "q_bucket": lab, "n": n, "n_events": int(num.sum()),
                    "mean_model_p": qm, "freq": freq,
                    "freq_lo": float(np.quantile(bs, 0.025)), "freq_hi": float(np.quantile(bs, 0.975)),
                    "ratio_freq_to_p": freq / qm if qm > 0 else np.nan,
                    "ratio_lo": float(np.quantile(ratio, 0.025)), "ratio_hi": float(np.quantile(ratio, 0.975)),
                })
    return pd.DataFrame(rows)


def exceedance_at_own_sd(
    P: Panel, R: PeriodResult, model: str, zs=(1.5, 2.0, 2.5, 3.0), n_boot: int = CFG.n_boot, seed: int = 2,
    taus=CFG.taus_min,
) -> pd.DataFrame:
    """Empirical frequency that the proxy lands beyond spot +/- z * (model's own sd).

    This is the 'fair price' of a z-sd strike implied by realized outcomes (both sides pooled
    and per side), next to the model's own probability for that strike.
    """
    h = R.eval_h
    spot, sd, A = P.spot[h], R.sd[model], P.A[h][:, None]
    days, d_idx = np.unique(P.day[h], return_inverse=True)
    D = days.size
    rng = np.random.default_rng(seed)
    boot = rng.integers(0, D, size=(n_boot, D))
    rows = []
    for k, tau in enumerate(taus):
        for z in zs:
            upe = (A[:, 0] > spot[:, k] + z * sd[:, k]).astype(float)
            dne = (A[:, 0] <= spot[:, k] - z * sd[:, k]).astype(float)
            # model probability of each tail at exactly z of its own sd (tails vary by month)
            by_month = np.array([R.tails[model][(mi, k)].sf(z) if (mi, k) in R.tails[model] else np.nan
                                 for mi in range(len(R.months))])
            pm_up = by_month[R.month_of]
            for side, ev in (("up", upe), ("down", dne), ("pooled", 0.5 * (upe + dne))):
                num = np.bincount(d_idx, weights=ev, minlength=D)
                den = np.bincount(d_idx, minlength=D).astype(np.float64)
                bs = num[boot].sum(1) / den[boot].sum(1)
                rows.append({
                    "model": model, "tau_min": tau, "z": z, "side": side, "n_hours": int(den.sum()),
                    "freq": num.sum() / den.sum(), "freq_lo": float(np.quantile(bs, 0.025)), "freq_hi": float(np.quantile(bs, 0.975)),
                    "model_p": float(np.mean(pm_up)),
                })
    return pd.DataFrame(rows)
