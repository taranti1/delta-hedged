"""Experiment 5 on REAL BTC paths: which hedge policy maximizes net P&L utility?

Data: Bitstamp BTC/USD 1-minute bars (2025-01-07 .. 2026-09-25), a BRTI constituent.
Synthetic part: the Kalshi fill flow (times, strikes, sides, sizes) — clearly labeled.

Design
  * One KXBTCD-like event per UTC hour, expiring at the hour. Strike ladder every $250.
  * Fills arrive as a Poisson process during minutes 0..57 of the hour (no fills in the final
    2 minutes), at strikes drawn around the money, side +/-1 (buy/sell YES), size = clip.
  * Fill price = model fair value at the fill minute (zero spread edge), so the Kalshi leg has
    zero expected P&L and differences between policies isolate the hedge's cost vs risk.
    Variant 'informed': the fill side is chosen to be adverse to the NEXT 5 minutes' move with
    probability p_inf (a crude adverse-selection flow) to test whether fast hedging recoups
    adverse selection.
  * Settlement proxy A_T = OHLC4 of the minute candle [T-60s, T). Delta uses the Gaussian digital
    with the averaging-window variance time and a trailing EWMA vol (no look-ahead).
  * Hedge: perp at the minute close; cost per trade = |qty| * S * (fee_bps + half_spread_bps)/1e4.
    The hedge rolls across hours (portfolio view); P&L is booked per hour.

Policies: none | fill_only | continuous | band(B) | mv_band(lam) | timed(k).
Every hedging policy unwinds its residual hedge when the event settles (the binary delta
vanishes at settlement; carrying the hedge would be a naked BTC position).

Output: docs/research/tables/hedge_policy_*.csv and docs/research/05_hedge_policy.md numbers.
Run: python -m dh.research.hedge_study --csv <bitstamp_1m.csv> --out docs/research
"""

from __future__ import annotations

import argparse
import math
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.special import ndtr

SEC_YR = 365.0 * 24 * 3600
STRIKE_STEP = 250.0


@dataclass(frozen=True)
class FlowSpec:
    fills_per_hour: float = 20.0
    clip: float = 10.0  # contracts per fill
    strike_sd_steps: float = 1.5  # strikes drawn ~ N(0, sd) steps from the money
    p_informed: float = 0.0  # prob. a fill is on the side adverse to the next-5-min move


def load_minutes(csv: str | Path) -> pd.DataFrame:
    df = pd.read_csv(csv)
    df["t"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)
    df = df.set_index("t").sort_index()
    df["ohlc4"] = (df.open + df.high + df.low + df.close) / 4.0
    # outage filter: flat zero-volume runs >= 10 minutes mark their hours as bad
    flat = (df.volume == 0) & (df.high == df.low)
    run_id = (~flat).cumsum()
    run_len = flat.groupby(run_id).transform("sum")
    df["bad"] = flat & (run_len >= 10)
    return df


def ewma_sigma_per_min(close: np.ndarray, half_life_min: float = 60.0) -> np.ndarray:
    """Trailing EWMA std of 1-minute log returns known at each minute's close (no look-ahead)."""
    r = np.diff(np.log(close), prepend=np.log(close[0]))
    a = 1.0 - math.exp(math.log(0.5) / half_life_min)
    out = np.empty_like(r)
    v = (0.35 / math.sqrt(525600)) ** 2
    for i, x in enumerate(r):
        v = (1 - a) * v + a * x * x
        out[i] = math.sqrt(v)
    return out


def digital_delta(S: float, K: np.ndarray, sig_min: float, minutes_left: float) -> tuple[np.ndarray, np.ndarray]:
    """(P(A > K), dP/dS) for the 60-print average settling in `minutes_left` minutes."""
    t_first = max(minutes_left * 60.0 - 59.0, 0.0)
    var_time_s = t_first + 19.5025  # (61*121/360 - 1) seconds, full 60-print window unfixed
    sd = S * sig_min / math.sqrt(60.0) * math.sqrt(var_time_s)
    z = (S - K) / sd
    p = ndtr(z)
    delta = np.exp(-0.5 * z * z) / math.sqrt(2 * math.pi) / sd
    return p, delta


def simulate(
    df: pd.DataFrame,
    flow: FlowSpec,
    policies: list[tuple[str, float]],
    fee_bps: float,
    half_spread_bps: float,
    seed: int = 7,
) -> pd.DataFrame:
    """Returns per-hour P&L rows for every policy (identical fills for all policies)."""
    rng = np.random.default_rng(seed)
    close = df.close.to_numpy()
    ohlc4 = df.ohlc4.to_numpy()
    bad = df.bad.to_numpy()
    sig = ewma_sigma_per_min(close)
    idx = df.index
    minute = idx.minute.to_numpy()
    starts = np.flatnonzero(minute == 0)
    starts = starts[(starts >= 1) & (starts + 65 < len(df))]
    cost_rate = (fee_bps + half_spread_bps) / 1e4
    rows = []
    hedge_pos = {(name, param): 0.0 for name, param in policies}
    mins = np.arange(60)
    for s0 in starts:
        if bad[s0 - 1 : s0 + 60].any():
            continue
        S_path = close[s0 - 1 : s0 + 59]  # price known at decision minute m (close of candle m-1)
        sig_path = sig[s0 - 1 : s0 + 59]
        A_T = ohlc4[s0 + 59]  # settlement proxy: candle [T-60s, T)
        n = rng.poisson(flow.fills_per_hour)
        f_min = np.sort(rng.integers(0, 58, size=n))
        f_off = np.round(rng.normal(0.0, flow.strike_sd_steps, size=n))
        f_side = rng.choice([-1.0, 1.0], size=n)
        if flow.p_informed > 0 and n:
            inf = rng.random(n) < flow.p_informed
            nxt = np.sign(close[s0 + 4 + f_min] - S_path[f_min])  # next ~5-minute move
            f_side = np.where(inf & (nxt != 0), -nxt, f_side)
        f_K = np.round(S_path[f_min] / STRIKE_STEP) * STRIKE_STEP + f_off * STRIKE_STEP
        f_q = f_side * flow.clip
        # delta matrix [minute, fill]; fills are live from their fill minute to expiry
        if n:
            S_m = S_path[:, None]
            tleft = (60 - mins)[:, None] * 60.0
            var_t = np.maximum(tleft - 59.0, 0.0) + 19.5025
            sd = S_m * (sig_path[:, None] / math.sqrt(60.0)) * np.sqrt(var_t)
            z = (S_m - f_K[None, :]) / sd
            dl = np.exp(-0.5 * z * z) / math.sqrt(2 * math.pi) / sd
            live = mins[:, None] >= f_min[None, :]
            D_book = (dl * live * f_q[None, :]).sum(axis=1)
            at_fill = mins[:, None] == f_min[None, :]
            new_delta = (dl * at_fill * f_q[None, :]).sum(axis=1)
            f_px = ndtr(z[f_min, np.arange(n)])
            kalshi_pnl = float(np.sum(f_q * ((A_T > f_K).astype(float) - f_px)))
            contracts = float(np.sum(np.abs(f_q)))
        else:
            D_book = np.zeros(60)
            new_delta = np.zeros(60)
            kalshi_pnl = 0.0
            contracts = 0.0
        dS = np.diff(S_path, prepend=S_path[0])
        sig_abs = S_path * sig_path / math.sqrt(60.0)
        for name, param in policies:
            H = hedge_pos[(name, param)]
            hedge_pnl = 0.0
            cost = 0.0
            turnover = 0.0
            if H and name != "none":
                # the previous event settled: its hedge has no purpose any more -> unwind now
                cost += abs(H) * S_path[0] * cost_rate
                turnover += abs(H)
                H = 0.0
            for m in range(60):
                if m:
                    hedge_pnl += H * dS[m]
                D = D_book[m] + H
                if name == "none":
                    trade = -H
                elif name == "continuous":
                    trade = -D
                elif name == "fill_only":
                    trade = -new_delta[m]
                elif name == "band":
                    trade = -(D - math.copysign(param, D)) if abs(D) > param else 0.0
                elif name == "mv_band":
                    B = 2 * cost_rate * S_path[m] / (param * sig_abs[m] ** 2 * max((60 - m) * 60.0, 60.0))
                    trade = -(D - math.copysign(B, D)) if abs(D) > B else 0.0
                elif name == "timed":
                    trade = -D if m % int(param) == 0 else 0.0
                else:
                    raise ValueError(name)
                if trade:
                    H += trade
                    cost += abs(trade) * S_path[m] * cost_rate
                    turnover += abs(trade)
            hedge_pnl += H * (A_T - S_path[59])
            hedge_pos[(name, param)] = H
            rows.append((idx[s0], name, param, kalshi_pnl, hedge_pnl, cost, turnover, contracts))
    out = pd.DataFrame(rows, columns=["hour", "policy", "param", "kalshi_pnl", "hedge_pnl", "hedge_cost",
                                      "turnover_btc", "contracts"])
    out["net"] = out.kalshi_pnl + out.hedge_pnl - out.hedge_cost
    return out


def summarize(res: pd.DataFrame, lams: tuple[float, ...] = (1e-4, 1e-3, 1e-2)) -> pd.DataFrame:
    g = res.groupby(["policy", "param"])
    s = pd.DataFrame({
        "hours": g.size(),
        "mean_net": g.net.mean(),
        "sd_net": g.net.std(),
        "mean_hedge_cost": g.hedge_cost.mean(),
        "turnover_btc_per_h": g.turnover_btc.mean(),
        "contracts_per_h": g.contracts.mean(),
    })
    s["cost_c_per_contract"] = 100 * s.mean_hedge_cost / s.contracts_per_h
    s["se_mean"] = s.sd_net / np.sqrt(s.hours)
    for lam in lams:
        s[f"util_lam{lam:g}"] = s.mean_net - 0.5 * lam * s.sd_net**2
    return s.reset_index()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True)
    ap.add_argument("--out", default="docs/research")
    ap.add_argument("--start", default="2025-01-08")
    ap.add_argument("--end", default="2026-09-24")
    args = ap.parse_args()
    df = load_minutes(args.csv)
    df = df[(df.index >= args.start) & (df.index < args.end)]
    out = Path(args.out) / "tables"
    out.mkdir(parents=True, exist_ok=True)
    policies = [("none", 0.0), ("fill_only", 0.0), ("continuous", 0.0), ("timed", 5.0), ("timed", 15.0),
                ("band", 0.02), ("band", 0.05), ("band", 0.1), ("band", 0.25), ("band", 0.5),
                ("mv_band", 1e-4), ("mv_band", 1e-3), ("mv_band", 1e-2)]
    summaries = []
    for scale in (1.0, 5.0, 25.0):
        for p_inf in (0.0, 0.3):
            flow = FlowSpec(fills_per_hour=20.0, clip=10.0 * scale, p_informed=p_inf)
            for fee, hs in ((0.6, 0.5), (5.0, 0.5), (12.0, 0.5)):
                res = simulate(df, flow, policies, fee_bps=fee, half_spread_bps=hs)
                s = summarize(res)
                s.insert(0, "p_informed", p_inf)
                s.insert(0, "fee_bps", fee)
                s.insert(0, "contracts_per_fill", flow.clip)
                summaries.append(s)
                print(f"scale={scale} p_inf={p_inf} fee={fee}: done ({len(res)} rows)", flush=True)
    allsum = pd.concat(summaries, ignore_index=True)
    allsum.to_csv(out / "hedge_policy_summary.csv", index=False, float_format="%.6g")
    print(allsum.to_string())


if __name__ == "__main__":
    main()
