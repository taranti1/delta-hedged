"""Ledger and P&L attribution (identical for replay and live sessions).

Consumes the event stream the strategy saw (fills, hedge fills, settlements) plus the
strategy's `Log('fv', ...)` records, and attributes every Kalshi fill:

    gross edge        (F_fill - px) * s              s = +1 bought YES, -1 sold YES
    markout(h)        (F_{t+h} - F_fill) * s          (negative = adverse selection at horizon h)
    settlement        (settle - F_fill) * s          (= total information/inventory P&L after fill)
    kalshi fee        exchange-reported fee_cost (or simulator fee engine)
    hedge cost/P&L    hedge book marked to the benchmark (IndexTick 'BRTI'); fees and P&L of each
                      hedge interval allocated to the Kalshi fills outstanding at its start by
                      |delta x contracts|; amounts with nothing outstanding are reported as
                      unallocated (never dropped)
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
from dh.core.events import HedgeFill, IndexTick, KalshiFill, Settlement
from dh.core.units import MICROS, NS_PER_S, PX_SCALE, QTY_SCALE

MARKOUT_H_S = (0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0)
# columns of attribute() (also used for an empty ledger, so summary() works with no fills)
ATTRIBUTION_COLUMNS = ("ts", "ticker", "event", "side", "px", "contracts", "fee", "is_taker", "F", "tau_s",
                       "gross_edge_c", "settle", "to_settle_c", "hedge_cost", "hedge_pnl", "net", "net_c_per_ct")


@dataclass
class FvSeries:
    ts: list[int] = field(default_factory=list)
    F: list[float] = field(default_factory=list)
    delta: list[float] = field(default_factory=list)

    def at(self, t: int, max_age_ns: int | None = None) -> tuple[float, float] | None:
        """Last observation at or before t; None if none, or if older than max_age_ns
        (a markout must not silently reuse a stale value after logging stopped)."""
        i = bisect.bisect_right(self.ts, t) - 1
        if i < 0:
            return None
        if max_age_ns is not None and t - self.ts[i] > max_age_ns:
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
    def __init__(self, event_of: dict[str, str], expiration_of: dict[str, int],
                 mark_index_id: str = "BRTI", markout_max_age_s: float = 5.0) -> None:
        self.event_of = event_of
        self.expiration_of = expiration_of
        self.fv: dict[str, FvSeries] = defaultdict(FvSeries)
        self.fills: list[FillRecord] = []
        self.settle: dict[str, float] = {}
        self.hedge_fills: list[HedgeFill] = []
        self.mark_index_id = mark_index_id
        self.mark_ts: list[int] = []
        self.mark_px: list[float] = []
        self.markout_max_age_ns = int(markout_max_age_s * NS_PER_S)
        self.hedge_totals: dict[str, float] = {}
        self.first_ts = 0
        self.last_ts = 0

    # ------------------------------------------------------------------ ingestion
    def on_event(self, ev) -> None:
        if not self.first_ts:
            self.first_ts = ev.ts
        self.last_ts = max(self.last_ts, ev.ts)
        if isinstance(ev, KalshiFill):
            side = 1 if ev.book_side == "bid" else -1
            t_fill = ev.ts_exch if ev.ts_exch else ev.ts  # match time, not delivery time (audit m4)
            self.fills.append(FillRecord(
                ts=t_fill, ticker=ev.ticker, event=self.event_of.get(ev.ticker, ev.ticker), side=side,
                px=ev.yes_px / PX_SCALE, contracts=ev.qty / QTY_SCALE, fee=ev.fee_micros / MICROS,
                is_taker=ev.is_taker,
                tau_s=(self.expiration_of.get(ev.ticker, t_fill) - t_fill) / NS_PER_S,
            ))
        elif isinstance(ev, Settlement):
            self.settle[ev.ticker] = ev.settlement_px / PX_SCALE
        elif isinstance(ev, HedgeFill):
            self.hedge_fills.append(ev)
        elif isinstance(ev, IndexTick) and ev.index_id == self.mark_index_id:
            if not self.mark_ts or ev.ts >= self.mark_ts[-1]:
                self.mark_ts.append(ev.ts)
                self.mark_px.append(ev.value)

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
        """Idempotent: recomputes every derived field from the raw inputs."""
        for f in self.fills:
            f.F, f.delta = math.nan, 0.0
            f.markouts = {}
            f.hedge_alloc_cost = 0.0
            f.hedge_alloc_pnl = 0.0
            cur = self.fv[f.ticker].at(f.ts, self.markout_max_age_ns)
            if cur is not None:
                f.F, f.delta = cur
            for h in MARKOUT_H_S:
                later = self.fv[f.ticker].at(f.ts + int(h * NS_PER_S), self.markout_max_age_ns)
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
        return pd.DataFrame(rows, columns=None if rows else list(ATTRIBUTION_COLUMNS))

    def _mark(self, t: int) -> float | None:
        i = bisect.bisect_right(self.mark_ts, t) - 1
        return self.mark_px[i] if i >= 0 else None

    def _allocate_hedges(self) -> None:
        """Mark the hedge book to the benchmark and allocate its P&L and costs to Kalshi fills.

        Hedge P&L is exact at portfolio level: execution vs mark at each hedge fill, plus the
        position held between consecutive hedge fills marked to the benchmark, plus the final
        position marked at the last mark. Each piece is allocated pro rata to |delta x contracts|
        of the Kalshi fills outstanding at its start (filled before, expiring after). Pieces with no
        outstanding fill go to `unallocated` (reported, never dropped).
        """
        tot = {"hedge_pnl": 0.0, "hedge_fees": 0.0, "unallocated_pnl": 0.0, "unallocated_fees": 0.0,
               "missing_marks": 0.0}
        self.hedge_totals = tot
        if not self.hedge_fills:
            return
        hf = sorted(self.hedge_fills, key=lambda h: h.ts)
        fills = sorted(self.fills, key=lambda f: f.ts)

        def outstanding(t: int) -> list[FillRecord]:
            return [f for f in fills if f.ts <= t and self.expiration_of.get(f.ticker, 0) > t]

        def allocate(t: int, pnl: float, fee: float) -> None:
            live = outstanding(t)
            wsum = sum(abs(f.delta * f.contracts) for f in live)
            if wsum <= 0:
                tot["unallocated_pnl"] += pnl
                tot["unallocated_fees"] += fee
                return
            for f in live:
                w = abs(f.delta * f.contracts) / wsum
                f.hedge_alloc_pnl += pnl * w
                f.hedge_alloc_cost += fee * w

        pos = 0.0
        end_t = self.mark_ts[-1] if self.mark_ts else hf[-1].ts
        for i, h in enumerate(hf):
            sgn = 1.0 if h.side == "buy" else -1.0
            m = self._mark(h.ts)
            exec_pnl = sgn * h.qty_btc * ((m if m is not None else h.price) - h.price)
            pos += sgn * h.qty_btc
            t_next = hf[i + 1].ts if i + 1 < len(hf) else end_t
            m0, m1 = self._mark(h.ts), self._mark(t_next)
            carry = pos * (m1 - m0) if (m0 is not None and m1 is not None) else 0.0
            if pos and (m0 is None or m1 is None):
                tot["missing_marks"] += 1  # hedge interval could not be marked: P&L understated
            piece = exec_pnl + carry
            tot["hedge_pnl"] += piece
            tot["hedge_fees"] += h.fee_usd
            allocate(h.ts, piece, h.fee_usd)

    # ------------------------------------------------------------------ summary
    def summary(self, df: pd.DataFrame | None = None, n_boot: int = 500, seed: int = 5) -> dict:
        df = self.attribute() if df is None else df
        done = df[~df.settle.isna()]
        out_h = dict(self.hedge_totals)
        days = max((self.last_ts - self.first_ts) / (86_400 * NS_PER_S), 1e-9)
        out: dict = {"fills": len(df), "settled_fills": len(done), "days": days, **{f"hedge_{k}" if not k.startswith("hedge") else k: v for k, v in out_h.items()}}
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
