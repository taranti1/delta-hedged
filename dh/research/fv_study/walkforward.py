"""Monthly walk-forward: fit on the trailing window, predict the next month (never shuffled).

For evaluation month [ms, me) every fitted quantity (proxy kappa, seasonal profile, blend
weights, tail parameters, empirical residual distribution) uses only hours whose settlement
time T < ms and minutes before ms.  The EWMA vol features are causal by construction (the
value used at decision time t folds in returns up to the close of the candle ending at t).
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field

import numpy as np

from dh.models.tails import GAUSS, EmpiricalTail, StudentT, TailModel, VolMixture
from dh.models.vol import FLAT_SEASONAL, SeasonalVol, ewma_regular
from dh.research.fv_study.config import CFG, StudyConfig, utc
from dh.research.fv_study.data import Minutes, add_months, month_starts
from dh.research.fv_study.fit import fit_blend_qlike, fit_gauss_scale, fit_student_t, fit_vol_mixture
from dh.research.fv_study.panel import Panel, estimate_kappa, raw_var_samples, v_eff

HL_LABEL = {10: "10m", 30: "30m", 120: "2h", 360: "6h", 1440: "1d"}
KAPPA_REF = 0.35  # fixed kappa used only to place the strike grids (parameter-free grids)


@dataclass(frozen=True)
class ModelDef:
    name: str
    vol: str  # 'raw:<hl>' | 'seas:<hl>' | 'blend' | 'blend_raw'
    tail: str  # 'gauss' | 'gauss_cal' | 't' | 'mix' | 'emp'
    family: str = ""


def main_models(cfg: StudyConfig = CFG) -> list[ModelDef]:
    out: list[ModelDef] = []
    for h in cfg.half_lives_min:
        out.append(ModelDef(f"G-raw-{HL_LABEL[h]}", f"raw:{h}", "gauss", "gauss raw ewma"))
    for h in cfg.half_lives_min:
        out.append(ModelDef(f"G-seas-{HL_LABEL[h]}", f"seas:{h}", "gauss", "gauss seasonal ewma"))
    out += [
        ModelDef("G-blend-raw", "blend_raw", "gauss", "gauss blend"),
        ModelDef("G-blend", "blend", "gauss", "gauss blend"),
        ModelDef("G-blend-cal", "blend", "gauss_cal", "gauss blend"),
        ModelDef("T-raw-2h", "raw:120", "t", "fat tail"),
        ModelDef("T-blend", "blend", "t", "fat tail"),
        ModelDef("MIX-blend", "blend", "mix", "fat tail"),
        ModelDef("EMP-blend", "blend", "emp", "empirical"),
    ]
    return out


@dataclass
class SeasonalSpec:
    layout: str = CFG.seasonal_layout
    bucket_s: int = CFG.seasonal_bucket_s
    tz: str = CFG.seasonal_tz
    method: str = CFG.seasonal_method
    shrink: float = CFG.seasonal_shrink

    @property
    def label(self) -> str:
        if self.layout == "flat":
            return "flat"
        tz = "ET" if self.tz != "UTC" else "UTC"
        return f"{self.layout}/{self.bucket_s // 60}m/{tz}"


@dataclass
class PeriodResult:
    period: str
    months: list[int]
    eval_h: np.ndarray  # panel hour indices evaluated
    month_of: np.ndarray  # month index of each eval hour
    sd: dict[str, np.ndarray]  # model -> (n_eval, K) sd of the proxy outcome ($)
    tails: dict[str, dict[tuple[int, int], TailModel]]
    sd_ref: np.ndarray  # (n_eval, K) grid-placement sd ($)
    params: list[dict] = field(default_factory=list)
    seasonal: dict[int, SeasonalVol] = field(default_factory=dict)
    blend_sigma2: np.ndarray | None = None  # (n_eval, K) blend per-second variance (log units)
    veff: dict[int, np.ndarray] = field(default_factory=dict)


def fit_seasonal(M: Minutes, lo_ts: int, hi_ts: int, norm_sigma: np.ndarray, spec: SeasonalSpec) -> SeasonalVol:
    """Seasonal profile from minute returns in [lo, hi) (training only)."""
    if spec.layout == "flat":
        return FLAT_SEASONAL
    i0, i1 = int(M.idx(lo_ts)), int(M.idx(hi_ts))
    sl = slice(max(i0, 1), i1)
    return SeasonalVol.fit(
        M.ts[sl], M.r[sl], 60.0, valid=M.r_valid[sl], layout=spec.layout, bucket_s=spec.bucket_s, tz=spec.tz,
        normalizer=norm_sigma[sl], method=spec.method, shrink=spec.shrink,
    )


def run_period(
    M: Minutes,
    P: Panel,
    raw: dict[int, np.ndarray],
    norm_sigma: np.ndarray,
    period: str,
    start: str,
    end: str,
    models: list[ModelDef],
    seas: SeasonalSpec | None = None,
    cfg: StudyConfig = CFG,
    verbose: bool = False,
) -> PeriodResult:
    """Walk-forward over the months of [start, end); returns per-model forecasts for eval hours."""
    seas = seas or SeasonalSpec()
    hls = list(cfg.half_lives_min)
    need = {m.vol for m in models}
    tails_needed = {m.tail for m in models}
    taus_s = P.taus_s
    K = taus_s.size
    months = month_starts(start, end)
    end_ts = utc(end)
    in_period = P.valid & (P.T > utc(start)) & (P.T <= end_ts)
    # eval hour T belongs to the month containing its settlement minute [T-60, T)
    eval_h = np.flatnonzero(in_period)
    mstarts = np.array(months + [end_ts], dtype=np.int64)
    month_of = np.searchsorted(mstarts, P.T[eval_h] - 60, side="right") - 1
    n_e = eval_h.size
    sd = {m.name: np.full((n_e, K), np.nan) for m in models}
    tails: dict[str, dict[tuple[int, int], TailModel]] = {m.name: {} for m in models}
    ref = raw[cfg.ref_half_life_min]
    sd_ref = P.spot[eval_h] * np.sqrt(ref[P.j0[eval_h]] * v_eff(taus_s, KAPPA_REF)[None, :])
    blend_sigma2 = np.full((n_e, K), np.nan)
    res = PeriodResult(period=period, months=months, eval_h=eval_h, month_of=month_of, sd=sd, tails=tails, sd_ref=sd_ref,
                       blend_sigma2=blend_sigma2)
    x_raw, xv = raw_var_samples(M)
    for mi, ms in enumerate(months):
        me = int(mstarts[mi + 1])
        lo = add_months(ms, -cfg.train_months)
        tr_h = np.flatnonzero(P.valid & (P.T - 60 >= lo) & (P.T <= ms))
        ev_sel = np.flatnonzero(month_of == mi)
        ev_h = eval_h[ev_sel]
        if ev_h.size == 0:
            continue
        kappa = estimate_kappa(M, lo, ms)
        ve = v_eff(taus_s, kappa)
        res.veff[mi] = ve
        # ---------------------------------------------------------------- seasonal EWMAs
        sv = fit_seasonal(M, lo, ms, norm_sigma, seas)
        res.seasonal[mi] = sv
        i_lo = max(int(M.idx(lo)) - 14 * 1440, 1)
        i_hi = int(M.idx(me)) + 1
        ds: dict[int, np.ndarray] = {}
        if any(v.startswith("seas") or v == "blend" for v in need):
            f2 = sv.factors_at(M.ts[i_lo:i_hi] + 30.0) ** 2
            xs = x_raw[i_lo:i_hi] / f2
            for h in hls:
                ds[h] = ewma_regular(xs, xv[i_lo:i_hi], float(h))

        def feats(hidx: np.ndarray, k: int):
            j0 = P.j0[hidx, k]
            t = P.T[hidx] - taus_s[k]
            Xr = np.column_stack([raw[h][j0] for h in hls])
            if ds:
                ratio = sv.mean_var_factor_vec(t, P.T[hidx]) if sv.layout != "flat" else np.ones(hidx.size)
                Xs = np.column_stack([ds[h][j0 - i_lo] for h in hls]) * ratio[:, None]
            else:
                Xs = None
            return Xr, Xs

        for k in range(K):
            spot_tr = P.spot[tr_h, k]
            y_tr = ((P.A[tr_h] - spot_tr) / spot_tr) ** 2 / ve[k]
            Xr_tr, Xs_tr = feats(tr_h, k)
            Xr_ev, Xs_ev = feats(ev_h, k)
            spot_ev = P.spot[ev_h, k]
            base_row = {"period": period, "month": ms, "tau_min": int(taus_s[k] // 60), "kappa": kappa, "n_train": int(tr_h.size)}
            w_s = w_r = None
            if "blend" in need or any(m.vol == "blend" for m in models):
                w_s = fit_blend_qlike(Xs_tr, y_tr)
                s2_tr = Xs_tr @ w_s
                s2_ev = Xs_ev @ w_s
                blend_sigma2[ev_sel, k] = s2_ev
                for h, w in zip(hls, w_s):
                    res.params.append({**base_row, "model": "blend", "param": f"w_{HL_LABEL[h]}", "value": float(w)})
                u_tr = (P.A[tr_h] - spot_tr) / (spot_tr * np.sqrt(s2_tr * ve[k]))
                sd_blend_ev = spot_ev * np.sqrt(s2_ev * ve[k])
                if "gauss_cal" in tails_needed:
                    c_g = fit_gauss_scale(u_tr)
                    res.params.append({**base_row, "model": "G-blend-cal", "param": "c", "value": c_g})
                if "t" in tails_needed:
                    c_t, nu = fit_student_t(u_tr)
                    res.params += [{**base_row, "model": "T-blend", "param": "c", "value": c_t},
                                   {**base_row, "model": "T-blend", "param": "nu", "value": nu}]
                if "mix" in tails_needed:
                    c_m, cv = fit_vol_mixture(u_tr)
                    res.params += [{**base_row, "model": "MIX-blend", "param": "c", "value": c_m},
                                   {**base_row, "model": "MIX-blend", "param": "cv", "value": cv}]
                if "emp" in tails_needed:
                    emp = EmpiricalTail(u_tr)
                    res.params.append({**base_row, "model": "EMP-blend", "param": "rms_u", "value": float(np.sqrt(np.mean(u_tr**2)))})
            if "blend_raw" in need:
                w_r = fit_blend_qlike(Xr_tr, y_tr)
                for h, w in zip(hls, w_r):
                    res.params.append({**base_row, "model": "blend_raw", "param": f"w_{HL_LABEL[h]}", "value": float(w)})
            for m in models:
                if m.vol.startswith("raw:"):
                    h = int(m.vol.split(":")[1])
                    s2 = Xr_ev[:, hls.index(h)]
                    s2_train = Xr_tr[:, hls.index(h)]
                elif m.vol.startswith("seas:"):
                    h = int(m.vol.split(":")[1])
                    s2 = Xs_ev[:, hls.index(h)]
                    s2_train = Xs_tr[:, hls.index(h)]
                elif m.vol == "blend":
                    s2, s2_train = s2_ev, s2_tr
                elif m.vol == "blend_raw":
                    s2, s2_train = Xr_ev @ w_r, Xr_tr @ w_r
                else:  # pragma: no cover
                    raise ValueError(m.vol)
                base_sd = spot_ev * np.sqrt(s2 * ve[k])
                tail: TailModel = GAUSS
                scale = 1.0
                if m.tail == "gauss_cal":
                    scale = c_g if m.vol == "blend" else fit_gauss_scale((P.A[tr_h] - spot_tr) / (spot_tr * np.sqrt(s2_train * ve[k])))
                elif m.tail == "t":
                    if m.vol == "blend":
                        scale, nu_m = c_t, nu
                    else:
                        u = (P.A[tr_h] - spot_tr) / (spot_tr * np.sqrt(s2_train * ve[k]))
                        scale, nu_m = fit_student_t(u)
                        res.params += [{**base_row, "model": m.name, "param": "c", "value": scale},
                                       {**base_row, "model": m.name, "param": "nu", "value": nu_m}]
                    tail = StudentT(nu_m)
                elif m.tail == "mix":
                    scale, tail = c_m, VolMixture(cv)
                elif m.tail == "emp":
                    tail = emp
                sd[m.name][ev_sel, k] = base_sd * scale
                tails[m.name][(mi, k)] = tail
        if verbose:
            print(f"  {period} {mi}: month {ms} eval_hours={ev_h.size} train_hours={tr_h.size} kappa={kappa:.3f}", flush=True)
    return res


def norm_sigma_series(M: Minutes, half_life_min: int = CFG.seasonal_norm_half_life_min) -> np.ndarray:
    """Slow causal vol level (per sqrt s) used to normalize returns when fitting the profile.

    Value at i uses returns up to i-1 only (lagged one minute)."""
    x, v = raw_var_samples(M)
    e = ewma_regular(x, v, float(half_life_min))
    return np.sqrt(np.r_[np.nan, e[:-1]])
