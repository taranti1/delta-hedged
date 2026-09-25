"""Q4: short-horizon return predictability (momentum / reversal) at 1-minute resolution.

(a) Minute level: standardized returns u_i = r_i / sigma_{i-1} (causal 30-minute EWMA scale).
    Past window X = sum_{j<k} u_{i-j} / sqrt(k), future Y = sum_{j=1..h} u_{i+j} / sqrt(h);
    slope beta (== correlation for unit-variance inputs), Newey-West t-stat (overlap), R^2.
(b) Decision level (what the contract cares about): at each (hour, tau) regress the
    standardized proxy outcome (A_T - spot) / sd_model on the past k-minute standardized
    return at t.  Effect on an at-the-money fair value for a 1-sd / 2-sd past move:
    dP = phi(0) * beta * x  (probability; x 100 = cents).  Walk-forward OOS check: add
    drift = beta_train * x * sd to the fair value and compare OOS log loss.
"""

from __future__ import annotations

import math

import numpy as np
import pandas as pd

from dh.models.fairvalue import digital_vec
from dh.research.fv_study.config import CFG, SEED, utc
from dh.research.fv_study.data import Minutes
from dh.research.fv_study.evaluate import Cells, loss_terms, predict
from dh.research.fv_study.panel import Panel
from dh.research.fv_study.walkforward import PeriodResult

PHI0 = 1.0 / math.sqrt(2.0 * math.pi)


def _nw_se(x: np.ndarray, e: np.ndarray, lags: int) -> float:
    """Newey-West standard error of the OLS slope (no intercept, centered x)."""
    xe = x * e
    n = xe.size
    s = np.dot(xe, xe)
    for L in range(1, lags + 1):
        w = 1.0 - L / (lags + 1.0)
        s += 2.0 * w * np.dot(xe[L:], xe[:-L])
    sxx = np.dot(x, x)
    return math.sqrt(s) / sxx


def minute_level(M: Minutes, raw30: np.ndarray, start: str, end: str, ks=(1, 2, 3, 5), hs=(1, 2, 5, 10, 30)) -> pd.DataFrame:
    """Minute-level predictive regressions on [start, end) (overlapping samples, NW errors)."""
    i0, i1 = int(M.idx(utc(start))), int(M.idx(utc(end)))
    sig = np.sqrt(np.r_[np.nan, raw30[:-1]] * 60.0)  # per-minute sigma known before minute i
    u = np.where(M.r_valid, M.r / sig, np.nan)
    u = np.clip(u, -10, 10)  # limit the leverage of a handful of flash moves
    cs = np.r_[0.0, np.nancumsum(np.nan_to_num(u))]
    bad = np.r_[0, np.cumsum(~np.isfinite(u))]
    rows = []
    for k in ks:
        for h in hs:
            i = np.arange(max(i0, k + 1), min(i1, M.n - h - 1))
            X = (cs[i + 1] - cs[i + 1 - k]) / math.sqrt(k)
            Y = (cs[i + 1 + h] - cs[i + 1]) / math.sqrt(h)
            ok = (bad[i + 1] - bad[i + 1 - k] == 0) & (bad[i + 1 + h] - bad[i + 1] == 0)
            X, Y = X[ok], Y[ok]
            Xc, Yc = X - X.mean(), Y - Y.mean()
            beta = float(np.dot(Xc, Yc) / np.dot(Xc, Xc))
            e = Yc - beta * Xc
            se = _nw_se(Xc, e, lags=h + k)
            r2 = 1.0 - float(np.dot(e, e) / np.dot(Yc, Yc))
            rows.append({
                "past_min": k, "future_min": h, "n": int(ok.sum()), "beta": beta, "t_nw": beta / se,
                "beta_lo": beta - 1.96 * se, "beta_hi": beta + 1.96 * se, "r2": r2,
                "atm_dP_cents_1sd": 100 * PHI0 * beta, "atm_dP_cents_2sd": 100 * PHI0 * beta * 2,
            })
    return pd.DataFrame(rows)


def decision_level(
    M: Minutes, P: Panel, R: PeriodResult, model: str, raw30: np.ndarray, ks=(1, 5, 15, 60), n_boot: int = CFG.n_boot,
) -> pd.DataFrame:
    """Predictability of the settlement outcome from past returns at each decision time."""
    h = R.eval_h
    spot = P.spot[h]
    u_out = (P.A[h][:, None] - spot) / R.sd[model]
    lc = np.log(M.c)
    days, d_idx = np.unique(P.day[h], return_inverse=True)
    D = days.size
    rng = np.random.default_rng(SEED)
    boot = rng.integers(0, D, size=(n_boot, D))
    rows = []
    for k_i, tau in enumerate(CFG.taus_min):
        j0 = P.j0[h, k_i]
        for k in ks:
            x = (lc[j0] - lc[j0 - k]) / np.sqrt(raw30[j0] * 60.0 * k)
            y = u_out[:, k_i]
            ok = np.isfinite(x) & np.isfinite(y)
            xx, yy, dd = x[ok], y[ok], d_idx[ok]
            sxy = np.bincount(dd, weights=xx * yy, minlength=D)
            sxx = np.bincount(dd, weights=xx * xx, minlength=D)
            beta = sxy.sum() / sxx.sum()
            bs = sxy[boot].sum(1) / sxx[boot].sum(1)
            lo, hi = np.quantile(bs, [0.025, 0.975])
            rows.append({
                "tau_min": tau, "past_min": k, "n": int(ok.sum()), "beta": beta, "beta_lo": lo, "beta_hi": hi,
                "corr": float(np.corrcoef(xx, yy)[0, 1]),
                "atm_dP_cents_1sd": 100 * PHI0 * beta, "atm_dP_cents_2sd": 100 * PHI0 * beta * 2,
            })
    return pd.DataFrame(rows)


def drift_oos(
    M: Minutes, P: Panel, R: PeriodResult, cells: Cells, model: str, raw30: np.ndarray, past_min: int = 5,
) -> pd.DataFrame:
    """Walk-forward: beta fitted on the 12 months before each eval month, applied as a drift."""
    from dh.research.fv_study.data import add_months

    h_all = np.flatnonzero(P.valid)
    lc = np.log(M.c)
    base_p = predict(P, R, cells, model)
    h = R.eval_h
    spot = P.spot[h][cells.e, cells.k]
    sd = R.sd[model][cells.e, cells.k]
    drift = np.zeros(cells.n)
    betas = []
    for mi, ms in enumerate(R.months):
        lo = add_months(ms, -CFG.train_months)
        tr = h_all[(P.T[h_all] - 60 >= lo) & (P.T[h_all] <= ms)]
        for k_i in range(len(CFG.taus_min)):
            j0 = P.j0[tr, k_i]
            x = (lc[j0] - lc[j0 - past_min]) / np.sqrt(raw30[j0] * 60.0 * past_min)
            # outcome standardized by the model-free reference scale of the same horizon
            sref = np.sqrt(raw30[j0] * (CFG.taus_s[k_i] - 60 + 0.35 * 60)) * P.spot[tr, k_i]
            yv = (P.A[tr] - P.spot[tr, k_i]) / sref
            ok = np.isfinite(x) & np.isfinite(yv)
            b = float(np.dot(x[ok], yv[ok]) / np.dot(x[ok], x[ok]))
            betas.append({"month": ms, "tau_min": CFG.taus_min[k_i], "beta": b})
            sel = (cells.mi == mi) & (cells.k == k_i)
            if not np.any(sel):
                continue
            j0c = P.j0[h[cells.e[sel]], k_i]
            xc = (lc[j0c] - lc[j0c - past_min]) / np.sqrt(raw30[j0c] * 60.0 * past_min)
            sref_c = np.sqrt(raw30[j0c] * (CFG.taus_s[k_i] - 60 + 0.35 * 60)) * spot[sel]
            drift[sel] = np.nan_to_num(b * xc * sref_c)
    p_drift = np.empty(cells.n)
    key = cells.mi * 1000 + cells.k
    for kk in np.unique(key):
        idx = np.flatnonzero(key == kk)
        tail = R.tails[model][(int(kk // 1000), int(kk % 1000))]
        p_drift[idx] = digital_vec("greater", spot[idx], sd[idx], tail, floor=cells.K[idx], drift_abs=drift[idx]).p_yes
    _, l0 = loss_terms(base_p, cells.y)
    _, l1 = loss_terms(p_drift, cells.y)
    days, d_idx = np.unique(cells.day, return_inverse=True)
    D = days.size
    rng = np.random.default_rng(SEED + 4)
    boot = rng.integers(0, D, size=(CFG.n_boot, D))
    rows = []
    for k_i, tau in enumerate(CFG.taus_min):
        m = cells.k == k_i
        num = np.bincount(d_idx[m], weights=(l1 - l0)[m], minlength=D)
        den = np.bincount(d_idx[m], minlength=D).astype(float)
        bs = num[boot].sum(1) / den[boot].sum(1)
        rows.append({"tau_min": tau, "past_min": past_min, "d_log_loss": num.sum() / den.sum(),
                     "lo": float(np.quantile(bs, 0.025)), "hi": float(np.quantile(bs, 0.975)),
                     "mean_abs_dp_cents": float(100 * np.mean(np.abs(p_drift[m] - base_p[m])))})
    bt = pd.DataFrame(betas).groupby("tau_min")["beta"].agg(["mean", "std"]).reset_index()
    out = pd.DataFrame(rows).merge(bt.rename(columns={"mean": "beta_train_mean", "std": "beta_train_sd"}), on="tau_min")
    return out
