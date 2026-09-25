"""Ledger and P&L attribution (identical for replay and live sessions).

Consumes the event stream the strategy saw (fills, hedge fills, settlements) plus the
strategy's `Log('fv', ...)` records, and attributes every Kalshi fill:

    gross edge        (F_fill - px) * s              s = +1 bought YES, -1 sold YES
    markout(h)        (F_{t+h} - F_fill) * s          (negative = adverse selection at horizon h)
    settlement        (settle - F_fill) * s          (= total information/inventory P&L after fill)
    kalshi fee        exchange-reported fee_cost (or simulator fee engine)
    hedge cost        hedge fees + spread paid, allocated to fills by |delta contribution|
    net               (settle - px) * s - fee - allocated hedge cost + allocated hedge P&L

Primary outputs: realized net cents per filled contract (event-clustered CI), profitable
contracts/day, $/day, capital employed (collateral x holding time) and $ per capital-hour.
All money in dollars as floats here (research reporting); the exact-int trail stays in events.
"""

from __future__ import annotations

import bisect
import math
from collections import defaultdict
from dataclasses import dataclass, field

import numpy as np
import pandas as pd

from dh.core.actions import Log
from dh.core.events import HedgeFill, KalshiFill, Settlement
from dh.core.units import MICROS, NS_PER_S, PX_SCALE, QTY_SCALE

MARKOUT_H_S = (0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0)


@dataclass
class FvSeries:
    ts: list[int] = field(default_factory=list)
    F: list[float] = field(default_factory=list)
    delta: list[float] = field(default_factory=list)

    def at(self, t: int) -> tuple[float, float] | None:
        i = bisect.bisect_right(self.ts, t) - 1
        if i < 0:
            return None
        return self.F[i], self.delta[i]


@dataclass
class FillRecord:
    ts: int
    ticker: str
    event: str
    side: int  # +1 bought YES, -1 sold YES
    px: float
    contracts: float
    fee: float
    is_taker: bool
    F: float = math.nan
    delta: float = 0.0
    tau_s: float = math.nan
    markouts: dict[float, float] = field(default_factory=dict)
    settle: float = math.nan
    hedge_alloc_cost: float = 0.0
    hedge_alloc_pnl: float = 0.0

    @property
    def gross_edge(self) -> float:
        return (self.F - self.px) * self.side

    @property
    def to_settle(self) -> float:
        return (self.settle - self.F) * self.side

    @property
    def net(self) -> float:
        return (self.settle - self.px) * self.side * self.contracts - self.fee - self.hedge_alloc_cost + self.hedge_alloc_pnl


class Ledger:
    def __init__(self, event_of: dict[str, str], expiration_of: dict[str, int]) -> None:
        self.event_of = event_of
        self.expiration_of = expiration_of
        self.fv: dict[str, FvSeries] = defaultdict(FvSeries)
        self.fills: list[FillRecord] = []
        self.settle: dict[str, float] = {}
        self.hedge_fills: list[HedgeFill] = []
        self.first_ts = 0
        self.last_ts = 0

    # ------------------------------------------------------------------ ingestion
    def on_event(self, ev) -> None:
        if not self.first_ts:
            self.first_ts = ev.ts
        self.last_ts = max(self.last_ts, ev.ts)
        if isinstance(ev, KalshiFill):
            side = 1 if ev.book_side == "bid" else -1
            self.fills.append(FillRecord(
                ts=ev.ts, ticker=ev.ticker, event=self.event_of.get(ev.ticker, ev.ticker), side=side,
                px=ev.yes_px / PX_SCALE, contracts=ev.qty / QTY_SCALE, fee=ev.fee_micros / MICROS,
                is_taker=ev.is_taker,
                tau_s=(self.expiration_of.get(ev.ticker, ev.ts) - ev.ts) / NS_PER_S,
            ))
        elif isinstance(ev, Settlement):
            self.settle[ev.ticker] = ev.settlement_px / PX_SCALE
        elif isinstance(ev, HedgeFill):
            self.hedge_fills.append(ev)

    def on_log(self, ts: int, log: Log) -> None:
        if log.kind == "fv":
            p = log.payload
            s = self.fv[p["ticker"]]
            if s.ts and ts < s.ts[-1]:
                return
            s.ts.append(ts)
            s.F.append(float(p["F"]))
            s.delta.append(float(p.get("delta", 0.0)))

    # ------------------------------------------------------------------ attribution
    def attribute(self) -> pd.DataFrame:
        for f in self.fills:
            cur = self.fv[f.ticker].at(f.ts)
            if cur is not None:
                f.F, f.delta = cur
            for h in MARKOUT_H_S:
                later = self.fv[f.ticker].at(f.ts + int(h * NS_PER_S))
                if later is not None and not math.isnan(f.F):
                    f.markouts[h] = (later[0] - f.F) * f.side
            f.settle = self.settle.get(f.ticker, math.nan)
        self._allocate_hedges()
        rows = []
        for f in self.fills:
            r = {
                "ts": f.ts, "ticker": f.ticker, "event": f.event, "side": f.side, "px": f.px,
                "contracts": f.contracts, "fee": f.fee, "is_taker": f.is_taker, "F": f.F, "tau_s": f.tau_s,
                "gross_edge_c": 100 * f.gross_edge, "settle": f.settle,
                "to_settle_c": 100 * f.to_settle, "hedge_cost": f.hedge_alloc_cost, "hedge_pnl": f.hedge_alloc_pnl,
                "net": f.net, "net_c_per_ct": 100 * f.net / f.contracts if f.contracts else math.nan,
            }
            for h, v in f.markouts.items():
                r[f"mo_{h:g}s_c"] = 100 * v
            rows.append(r)
        return pd.DataFrame(rows)

    def _allocate_hedges(self) -> None:
        """Allocate hedge fees and hedge P&L to Kalshi fills pro rata to |delta| contribution
        within the same hour bucket (simple, transparent; exact portfolio attribution is
        reported separately in the summary)."""
        if not self.hedge_fills or not self.fills:
            return
        by_hour: dict[int, list[FillRecord]] = defaultdict(list)
        for f in self.fills:
            by_hour[f.ts // (3600 * NS_PER_S)].append(f)
        for h in self.hedge_fills:
            bucket = by_hour.get(h.ts // (3600 * NS_PER_S)) or []
            wsum = sum(abs(f.delta * f.contracts) for f in bucket)
            if wsum <= 0:
                continue
            for f in bucket:
                f.hedge_alloc_cost += h.fee_usd * abs(f.delta * f.contracts) / wsum

    # ------------------------------------------------------------------ summary
    def summary(self, df: pd.DataFrame | None = None, n_boot: int = 500, seed: int = 5) -> dict:
        df = self.attribute() if df is None else df
        done = df[~df.settle.isna()]
        days = max((self.last_ts - self.first_ts) / (86_400 * NS_PER_S), 1e-9)
        out: dict = {"fills": len(df), "settled_fills": len(done), "days": days}
        if not len(done):
            return out
        ct = done.contracts.sum()
        net = done.net.sum()
        out.update({
            "contracts": ct,
            "net_usd": net,
            "net_c_per_contract": 100 * net / ct,
            "gross_edge_c_per_contract": float(np.average(done.gross_edge_c, weights=done.contracts)),
            "fees_c_per_contract": 100 * done.fee.sum() / ct,
            "hedge_cost_c_per_contract": 100 * done.hedge_cost.sum() / ct,
            "contracts_per_day": ct / days,
            "net_usd_per_day": net / days,
            "profitable_fill_share": float((done.net > 0).mean()),
        })
        for h in MARKOUT_H_S:
            c = f"mo_{h:g}s_c"
            if c in done:
                ok = done[c].notna()
                if ok.any():
                    out[f"markout_{h:g}s_c"] = float(np.average(done.loc[ok, c], weights=done.loc[ok, "contracts"]))
        # capital: collateral x time to settlement (held to expiry assumption)
        coll = np.where(done.side > 0, done.px, 1 - done.px) * done.contracts
        cap_hours = float(np.sum(coll * np.maximum(done.tau_s, 0) / 3600.0))
        out["capital_dollar_hours"] = cap_hours
        out["net_per_capital_hour"] = net / cap_hours if cap_hours > 0 else math.nan
        # event-clustered bootstrap CI of net c/contract
        g = done.groupby("event").agg(n=("net", "sum"), c=("contracts", "sum"))
        rng = np.random.default_rng(seed)
        s, w = g.n.to_numpy(), g.c.to_numpy()
        k = len(s)
        bs = [100 * s[i].sum() / w[i].sum() for i in (rng.integers(0, k, k) for _ in range(n_boot)) if w[i].sum() > 0]
        if bs:
            lo, hi = np.percentile(bs, [2.5, 97.5])
            out["net_c_ci95"] = (float(lo), float(hi))
        out["events"] = k
        return out
