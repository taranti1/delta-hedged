"""Hour x decision-time panel: spot, settlement proxy, causal vol features, reporting keys.

Timing (all UTC seconds; candles labelled by open time):
  settlement time T          every top of the hour
  decision time t            T - tau*60 for tau in taus_min
  spot(t)                    close of the candle [t-60, t)    -> index j0 = idx(t - 60)
                             (never the candle that starts at t: that would be look-ahead)
  settlement proxy A_T       OHLC4 of the candle [T-60, T)    -> index j1 = idx(T - 60)
  vol features at t          EWMA values after folding in the return of candle j0
  A_T - spot = (C_{j1-1} - C_{j0}) + (OHLC4_{j1} - C_{j1-1}):  (tau - 1) close-to-close
  minutes plus the proxy term, so the proxy's variance time in close-to-close units is
      v_eff(tau) = (tau - 1) * 60 + kappa * 60   seconds,
  kappa = E[(OHLC4 - C_prev)^2] / E[(C - C_prev)^2], estimated on training minutes.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass

import numpy as np

from dh.models.vol import ewma_regular
from dh.research.fv_study.config import CFG, StudyConfig
from dh.research.fv_study.data import Minutes

ET = "America/New_York"


@dataclass
class Panel:
    T: np.ndarray  # int64 [H] settlement times (s)
    taus_s: np.ndarray  # int64 [K]
    j0: np.ndarray  # int64 [H, K] spot candle index
    j1: np.ndarray  # int64 [H] settlement candle index
    spot: np.ndarray  # float [H, K]
    A: np.ndarray  # float [H] settlement proxy
    valid: np.ndarray  # bool [H]
    excl_outage: np.ndarray  # bool [H]
    excl_notrade: np.ndarray  # bool [H]
    day: np.ndarray  # int64 [H] UTC day index of the settlement minute
    weekend: np.ndarray  # bool [H] settlement minute on Sat/Sun UTC
    fomc: np.ndarray  # bool [H] hour ending 15:00 ET on an FOMC statement day
    hour_et: np.ndarray  # int [H] local ET hour of T

    @property
    def n_hours(self) -> int:
        return int(self.T.size)


def raw_var_samples(M: Minutes) -> tuple[np.ndarray, np.ndarray]:
    """Per-second variance samples x_i = r_i^2 / 60 and validity mask."""
    x = np.where(M.r_valid, M.r * M.r / 60.0, 0.0)
    return x, M.r_valid.copy()


def raw_ewmas(M: Minutes, half_lives_min) -> dict[int, np.ndarray]:
    """Causal raw EWMA per-second variance series for each half-life (index i includes r_i)."""
    x, v = raw_var_samples(M)
    return {int(h): ewma_regular(x, v, float(h)) for h in half_lives_min}


def build_panel(M: Minutes, cfg: StudyConfig = CFG, min_history_min: int = 3 * 1440) -> Panel:
    """All top-of-hour settlement times with enough history, with validity flags."""
    taus_s = cfg.taus_s
    first_T = M.t0 + (min_history_min + int(taus_s.max()) // 60 + 2) * 60
    first_T = ((first_T + 3599) // 3600) * 3600
    last_T = int(M.ts[-1]) + 60  # the last candle covers [ts[-1], ts[-1] + 60)
    last_T = (last_T // 3600) * 3600
    T = np.arange(first_T, last_T + 1, 3600, dtype=np.int64)
    j1 = M.idx(T - 60)
    j0 = M.idx(T[:, None] - taus_s[None, :] - 60)
    spot = M.c[j0]
    A = M.ohlc4[j1]
    # outage anywhere in the candles from the earliest spot candle to the settlement candle
    cum = np.r_[0, np.cumsum(M.outage.astype(np.int64))]
    lo = j0.min(axis=1)
    excl_outage = (cum[j1 + 1] - cum[lo]) > 0
    excl_notrade = M.zero_flat[j1]
    valid = ~excl_outage & ~excl_notrade & np.isfinite(A) & np.all(np.isfinite(spot), axis=1)
    day = (T - 60) // 86400
    weekend = ((day + 3) % 7) >= 5
    # ET hour and FOMC reaction hour (hour ending 15:00 ET on a statement day)
    import zoneinfo

    z = zoneinfo.ZoneInfo(ET)
    hour_et = np.empty(T.size, dtype=np.int64)
    date_et = np.empty(T.size, dtype=object)
    for i, t in enumerate(T):
        d = dt.datetime.fromtimestamp(int(t), tz=dt.timezone.utc).astimezone(z)
        hour_et[i] = d.hour
        date_et[i] = d.strftime("%Y-%m-%d")
    fomc_set = set(cfg.fomc_dates)
    fomc = np.array([(de in fomc_set) and (he == 15) for de, he in zip(date_et, hour_et)], dtype=bool)
    return Panel(
        T=T,
        taus_s=taus_s,
        j0=j0,
        j1=j1,
        spot=spot,
        A=A,
        valid=valid,
        excl_outage=excl_outage,
        excl_notrade=excl_notrade,
        day=day,
        weekend=weekend,
        fomc=fomc,
        hour_et=hour_et,
    )


def estimate_kappa(M: Minutes, lo_ts: int, hi_ts: int) -> float:
    """kappa = sum log(OHLC4_i/C_{i-1})^2 / sum log(C_i/C_{i-1})^2 over traded, valid minutes in [lo, hi)."""
    i0, i1 = int(M.idx(lo_ts)), int(M.idx(hi_ts))
    i0 = max(i0, 1)
    sl = slice(i0, i1)
    ok = M.r_valid[sl] & (M.v[sl] > 0)
    num = np.log(M.ohlc4[sl] / M.c[i0 - 1 : i1 - 1]) ** 2
    den = M.r[sl] ** 2
    return float(num[ok].sum() / den[ok].sum())


def v_eff(taus_s: np.ndarray, kappa: float) -> np.ndarray:
    """Proxy variance time (s) in close-to-close units for each decision time."""
    return (np.asarray(taus_s, dtype=np.float64) - 60.0) + kappa * 60.0


def hour_features(M: Minutes, P: Panel, raw: dict[int, np.ndarray]) -> dict[str, np.ndarray]:
    """Causal reporting features at each (hour, tau): trailing vol level, trend score, burst."""
    j0 = P.j0
    s1d = np.sqrt(raw[1440][j0])
    s10 = np.sqrt(raw[10][j0])
    lc = np.log(M.c)
    ret6h = lc[j0] - lc[np.maximum(j0 - 360, 0)]
    trend = np.abs(ret6h) / (s1d * np.sqrt(6 * 3600.0))
    return {"sig1d": s1d, "trend": trend, "burst": s10 / s1d}
