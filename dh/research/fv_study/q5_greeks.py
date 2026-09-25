"""Q5: delta / gamma profile of KXBTCD contracts at current spot and vol (production functions).

Everything is computed with dh.models.fairvalue.digital on exact WindowStates:
  * before the window: k_fixed = 0, tau_first = tau*60 - 59 s
  * inside the final minute: k prints fixed (their average equal to spot, i.e. the market did
    not move during the window so far), m = 60 - k remaining, next print 0.5 s away.
Strikes are placed at z standard deviations of the SETTLEMENT AVERAGE (not of spot), so the
table reads "a strike z sd away from where the average is heading".
"""

from __future__ import annotations

import math

import pandas as pd

from dh.core.market import MarketSpec
from dh.models.fairvalue import digital, remaining_avg_variance_time
from dh.models.tails import GAUSS, TailModel
from dh.settlement.window import WindowState


def _spec(K: float) -> MarketSpec:
    return MarketSpec(ticker="KXBTCD-SIM", event_ticker="KXBTCD-SIM", series_ticker="KXBTCD", strike_type="greater",
                      floor_strike=K, cap_strike=None, open_ts=0, close_ts=0, expiration_ts=0)


def greeks_table(
    spot: float,
    sigma_log: float,
    tails: dict[str, TailModel] | None = None,
    taus_min=(60, 30, 15, 10, 5, 2, 1.5),
    in_window_k=(0, 15, 30, 45, 55, 59),
    zs=(-2.0, -1.0, -0.5, 0.0, 0.5, 1.0, 2.0),
) -> pd.DataFrame:
    """Rows: state x z x tail -> p, delta (BTC/contract), notional ($), gamma, delta move per 1 sd."""
    tails = tails or {"gauss": GAUSS}
    sigma_abs = spot * sigma_log
    states: list[tuple[str, WindowState]] = []
    for tau in taus_min:
        states.append((f"T-{tau:g}m", WindowState(60, 0, 0.0, 60, max(tau * 60 - 59, 0.0), 1.0)))
    for k in in_window_k:
        # k prints fixed at spot; next print due in 0.5 s (k = 0: window starts in 0.5 s)
        states.append((f"window k={k}", WindowState(60, k, k * spot, 60 - k, 0.5, 1.0)))
    rows = []
    for name, ws in states:
        sd_R = sigma_abs * math.sqrt(remaining_avg_variance_time(ws))
        sd_A = sd_R * ws.m_remaining / ws.n_obs  # sd of the settlement average itself
        for z in zs:
            K = spot + z * sd_A
            for tname, tail in tails.items():
                d = digital(_spec(K), ws, spot, sigma_abs, tail)
                rows.append({
                    "state": name, "k_fixed": ws.k_fixed, "var_time_s": remaining_avg_variance_time(ws),
                    "sd_avg_usd": sd_A, "z": z, "strike": K, "tail": tname,
                    "p_yes": d.p_yes, "delta_btc": d.delta, "hedge_notional_usd": d.delta * spot,
                    "gamma_per_usd": d.gamma, "delta_change_per_1sd_btc": d.gamma * sd_A,
                    "hedge_notional_per_100_usd": 100 * d.delta * spot,
                })
    return pd.DataFrame(rows)
