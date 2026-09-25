"""Experiment 5 on replayed or recorded fills: which hedge policy maximizes utility on the SAME
realized Kalshi fill stream and the recorded BTC path?

    python scripts/run_experiment.py e5 --root data --t0 A --t1 B              # replays under B and C
    python scripts/run_experiment.py e5 ... --session-fills live|paper|L.csv  # a session's fills

The standalone study (dh.research.hedge_study: real 1-minute BTC bars, SYNTHETIC fill flow) fixed
the policy family; this module re-runs it on real fills, as BUILD_PLAN H requires before the
hedge engine may turn on.

Fill streams (one per label; every hedge policy is evaluated on the identical stream):
  * replay (default): the production MarketMaker replayed over the recording under fill policies
    B and C (dh.research.replay_env.run_replay) with the hedge engine OFF (the configuration E5
    decides on). ``HedgeInputCollector`` records, every ``step_s`` of event time, the strategy's own
    Kalshi delta per settlement event D_e = sum_i q_i Delta_i (BTC; the MarketMaker's last quote
    cycle), the benchmark S, sigma ($/sqrt s: the strategy's _sigma_1s, the production band input)
    and the time to expiry; each fill's delta change q Delta when the fill is DELIVERED to the
    strategy; the recorded BRTI ticks; optionally a recorded hedge venue's top of book.
  * session fills: our fills of a live session (private `fill` frames recorded in kalshi.ws), of a
    paper session (`events.paper`), or of a replay ledger CSV; the same inputs come from the
    strategy's fair-value probe (replay_env.FvProbe) replayed over the window, with positions
    rebuilt from the fills. The hedge execution is still simulated, under B and C latency.

Kalshi leg: realized net P&L per settlement event (settlement - price, fees), identical for every
hedge policy. Events that expire after the window (no BTC path to their settlement) or with
unsettled fills are excluded (counted in the report).

Hedge policies (docs/research/05_hedge_policy.md):
  none              never hedge
  per_fill@c        hedge each fill's delta when the fill is known (venue minimum lot:
                    cfg.hedge.min_order_btc; smaller remainders carry to the next fill)
  band(lam)@c       dh.strategy.hedging.decide_hedge at every step: mean-variance no-trade band
                    B = 2 c S / (lam sigma^2 h), h = the event's time to expiry, trade back to the
                    band edge (band_min 0: the formula itself)
  band_configured   the production engine as configured (cfg.hedge incl. band_min_btc) at the
                    configured lambda cfg.lam and the achieved fee tier: THE decision policy
  c = fee bps (charged by the venue simulator) + half spread. Each settlement event has its own
  hedge book, unwound when the event settles (binary deltas vanish at settlement); netting of
  hedge trades across overlapping events is not modeled (an upper bound on hedge cost).
  Execution: dh.execution.hedge_sim.HedgeVenueSim (market orders, the research latency model,
  policy C x1.5, venue fee) against a proxy top of book at BRTI +- half spread (deep), or a
  recorded venue's top of book (``hedge_venue``; one level: larger orders fill partially).

Per (fill policy, scale, hedge policy), with settlement events as clusters: P&L per event (Kalshi +
hedge - hedge fees): mean and s.d., variance ratio vs no hedge, utility U = mean - lam/2 var
(TEST_MATRIX E5; lam in 1/$) and the paired utility gain vs no hedge (CIs: hull of the jackknife-t
and the percentile bootstrap over settlement events), net c/contract (cluster CI), hedge P&L, fees
and execution cost (vs BRTI) in c/contract, turnover. ``scale`` > 1 multiplies the Kalshi leg and
its delta (a what-if on the same fills; E10 replays real size). Regimes: vol tercile, weekday.

Decision rule (docs/TEST_MATRIX.md E5, docs/BUILD_PLAN.md H): ACCEPT (hedge engine on) only if
band_configured beats no hedge in utility (paired CI lower bound > 0) under BOTH B and C; REJECT
(hedge stays disabled) if the utility-gain CI upper bound <= 0 under B or C (a band that never
trades has gain exactly 0: no hedge is optimal); otherwise INCONCLUSIVE. Evidence guards
(exp_common.Report): < 20 settlement events -> INCONCLUSIVE; a synthetic recording or in-sample
fitted inputs (FV / flow / fill-intensity / adverse-selection parameters) cap an ACCEPT.
"""

from __future__ import annotations

import math
import multiprocessing as mp
from collections import defaultdict
from collections.abc import Sequence
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass, field, replace
from functools import partial
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from dh.core.actions import PlaceHedge
from dh.core.book import ExtBook
from dh.core.events import (
    ExtBBO,
    ExtBookDelta,
    ExtBookSnapshot,
    HedgeFill,
    HedgeOrderUpdate,
    IndexTick,
    KalshiFill,
    Timer,
)
from dh.core.units import NS_PER_S, PX_SCALE, QTY_SCALE
from dh.execution.hedge_sim import HedgeVenueSim
from dh.execution.latency import LatencyModel
from dh.research.exp_common import (
    CI,
    Report,
    _tq,
    fmt_ns,
    policy_letter,
    ratio_ci,
    vol_cuts,
)
from dh.research.replay_env import (
    POLICY_FULL,
    Universe,
    build_universe,
    describe_latency,
    inputs_meta,
    inputs_status,
    prime_probe,
    probe_for_window,
    realized_vol_asof,
    research_latency,
    settlement_cluster,
)
from dh.research.replay_grid import Variant, run_variants, run_warnings
from dh.strategy.config import HedgeCfg, StrategyConfig
from dh.strategy.hedging import decide_hedge, no_trade_band

HEDGE_VENUE = "hedge_sim"
HEDGE_SYMBOL = "BTC-USD"
PROXY_DEPTH_BTC = 1e6  # proxy book depth: the proxy never runs out of liquidity (no impact modeled)
DEFAULT_FEES_BPS = (0.6, 1.0, 5.0, 12.0)  # hedge_study's tiers + 1 bp (BUILD_PLAN H: "about 1 bp or less")
DEFAULT_LAMS = (1e-4, 1e-3, 1e-2)  # 1/$ (hedge_study grid); the configured cfg.lam is always added
END_NS = 2**62
_EPS = 1e-12
MD, FILL, GRID, EXPIRE = 0, 1, 2, 3  # item kinds; also the order of items with equal timestamps
CI_METHOD_U = "hull of jackknife-t and percentile bootstrap over settlement events (95 %)"
RULE_E5 = ("ACCEPT (hedge engine on) only if the configured mean-variance band (cfg.hedge at cfg.lam and the achieved "
           "hedge fee tier) beats no hedge in utility = mean - lambda/2 var of settlement-event P&L on the same fills "
           "(paired CI lower bound > 0 over settlement events) under BOTH fill policies B and C; REJECT (hedge stays "
           "disabled) if the utility-gain CI upper bound <= 0 under B or C; else INCONCLUSIVE")


# ============================================================================ policies
@dataclass(frozen=True)
class HedgePolicy:
    """kind: none | per_fill | band. fee_bps: venue fee per trade (bps of notional); lam: band risk
    aversion (1/$); band_min_btc: band floor (0 = the formula; the configured engine uses cfg.hedge's)."""

    kind: str
    fee_bps: float = 0.0
    lam: float = math.nan
    band_min_btc: float = 0.0
    tag: str = ""

    def __post_init__(self) -> None:
        if self.kind not in ("none", "per_fill", "band"):
            raise ValueError(f"unknown hedge policy kind {self.kind!r}")

    @property
    def name(self) -> str:
        if self.kind == "none":
            return "none"
        if self.kind == "per_fill":
            return f"per_fill@{self.fee_bps:g}bp"
        return f"band{'_' + self.tag if self.tag else ''}(lam={self.lam:g})@{self.fee_bps:g}bp"


def policy_grid(cfg: StrategyConfig, *, fees_bps: Sequence[float] = DEFAULT_FEES_BPS,
                lams: Sequence[float] = DEFAULT_LAMS, decision_fee_bps: float | None = None) -> list[HedgePolicy]:
    """none; per_fill at every fee tier; the pure band at every (lambda, fee tier) with the configured
    lambda added; and the decision policy band_configured (cfg.hedge, cfg.lam, the achieved tier)."""
    fee_dec = cfg.hedge.fee_bps_maker if decision_fee_bps is None else float(decision_fee_bps)
    fees = sorted({float(f) for f in fees_bps} | {fee_dec})
    ls = sorted({float(x) for x in lams} | {float(cfg.lam)})
    out = [HedgePolicy("none")]
    out += [HedgePolicy("per_fill", f) for f in fees]
    out += [HedgePolicy("band", f, lam) for lam in ls for f in fees]
    out.append(decision_policy(cfg, fee_dec))
    return out


def decision_policy(cfg: StrategyConfig, fee_bps: float | None = None) -> HedgePolicy:
    """The hedge engine as configured (band floor cfg.hedge.band_min_btc) at the configured lambda."""
    fee = cfg.hedge.fee_bps_maker if fee_bps is None else float(fee_bps)
    return HedgePolicy("band", fee, float(cfg.lam), float(cfg.hedge.band_min_btc), tag="configured")


# ============================================================================ inputs
@dataclass
class HedgeInputs:
    """Everything a hedge policy needs for ONE realized fill stream (a label such as 'B', 'C', 'live').

    grid     ts, event, D_btc (Kalshi delta of the event's positions), S, sigma_1s ($/sqrt s), tau_s
    fills    ts (known to the strategy), event, ticker, contracts (signed: + bought YES), delta_btc, dD_btc
    ticks    ts, value: BRTI (receive time), the hedge proxy's mid and the mark
    events   event, expiration_ns, kalshi_net_usd, contracts, included, reason
    venue    optional recorded hedge-venue top of book: ts, bid, bid_size, ask, ask_size
    """

    label: str
    grid: pd.DataFrame
    fills: pd.DataFrame
    ticks: pd.DataFrame
    events: pd.DataFrame
    venue: pd.DataFrame | None = None
    source: str = "replay"
    meta: dict[str, Any] = field(default_factory=dict)

    @property
    def included(self) -> pd.DataFrame:
        return self.events[self.events["included"]] if len(self.events) else self.events


GRID_COLS = ["ts", "event", "D_btc", "S", "sigma_1s", "tau_s"]
FILL_COLS = ["ts", "event", "ticker", "contracts", "delta_btc", "dD_btc"]
EVENT_COLS = ["event", "expiration_ns", "kalshi_net_usd", "contracts", "included", "reason"]
TOP_COLS = ["ts", "bid", "bid_size", "ask", "ask_size"]


class _TopTracker:
    """Top of book of one recorded venue (ExtBBO / snapshot / delta), recorded on change."""

    def __init__(self, venue: str) -> None:
        self.venue = venue
        self.book = ExtBook(venue, "")
        self.rows: list[tuple[int, float, float, float, float]] = []
        self._last: tuple[float, float, float, float] | None = None

    def on_event(self, ev: Any) -> None:
        t = type(ev)
        if t not in (ExtBBO, ExtBookSnapshot, ExtBookDelta) or ev.venue != self.venue:
            return
        if t is ExtBBO:
            self.book.apply_bbo(ev)
        elif t is ExtBookSnapshot:
            self.book.apply_snapshot(ev)
        else:
            self.book.apply(ev)
        top = self.book.top()
        if top is None:
            return
        cur = (top.bid, top.bid_size, top.ask, top.ask_size)
        if cur != self._last:
            self._last = cur
            self.rows.append((ev.ts, *cur))

    def frame(self) -> pd.DataFrame:
        return pd.DataFrame(self.rows, columns=TOP_COLS)


class HedgeInputCollector:
    """run_replay collector: hedge inputs of the replayed strategy (module docstring). Sampled on
    the strategy's Timers every ``step_s`` (the collector sees each event just BEFORE the strategy:
    D uses the positions so far and the deltas of the last quote cycle)."""

    def __init__(self, mm: Any, step_s: float = 1.0, venue: str | None = None) -> None:
        self.mm = mm
        self.period = max(1, int(round(step_s * NS_PER_S)))
        self.next_t = 0
        self.exp_of: dict[str, int] = {}
        self.tickers: dict[str, set[str]] = defaultdict(set)
        self.grid: list[tuple[int, str, float, float, float, float]] = []
        self.fills: list[tuple[int, str, str, float, float, float]] = []
        self.ticks: list[tuple[int, float]] = []
        self.top = _TopTracker(venue) if venue else None

    def on_event(self, ev: Any) -> None:
        t = type(ev)
        if t is Timer:
            if ev.ts >= self.next_t:
                self.next_t = ev.ts + self.period
                self._sample(ev.ts)
        elif t is IndexTick:
            if ev.index_id == "BRTI" and ev.feed in ("1hz", "5hz") and (not self.ticks or ev.ts >= self.ticks[-1][0]):
                self.ticks.append((ev.ts, float(ev.value)))
        elif t is KalshiFill:
            spec = self.mm.specs.get(ev.ticker)
            if spec is None:
                return
            e = settlement_cluster(spec.expiration_ts)
            self.exp_of[e] = spec.expiration_ts
            self.tickers[e].add(ev.ticker)
            f = self.mm.fvc.get(ev.ticker)
            d = float(f.delta) if f is not None else math.nan
            q = (1.0 if ev.book_side == "bid" else -1.0) * ev.qty / QTY_SCALE
            self.fills.append((ev.ts, e, ev.ticker, q, d, q * d if math.isfinite(d) else 0.0))
        elif self.top is not None:
            self.top.on_event(ev)

    def _sample(self, now: int) -> None:
        mm = self.mm
        S = mm._spot()
        if S is None:
            return
        sig = float(mm._sigma_1s(now, S))
        for e, T in self.exp_of.items():
            if T <= now:
                continue
            D = 0.0
            for tk in self.tickers[e]:
                q = mm.om.position(tk) / QTY_SCALE
                if q:
                    f = mm.fvc.get(tk)
                    if f is not None:
                        D += q * float(f.delta)
            self.grid.append((now, e, D, float(S), sig, (T - now) / NS_PER_S))

    def result(self) -> dict[str, Any]:
        return {"grid": pd.DataFrame(self.grid, columns=GRID_COLS), "fills": pd.DataFrame(self.fills, columns=FILL_COLS),
                "ticks": pd.DataFrame(self.ticks, columns=["ts", "value"]), "expirations": dict(self.exp_of),
                "venue": self.top.frame() if self.top is not None else None}


def _events_table(exp_of: dict[str, int], kalshi: pd.DataFrame, t_end: int, unsettled: set[str]) -> pd.DataFrame:
    """One row per settlement event with fills: its Kalshi leg and whether E5 can evaluate it."""
    rows = []
    g = kalshi.groupby("event").agg(net=("net", "sum"), ct=("contracts", "sum")) if len(kalshi) else pd.DataFrame()
    for e, T in sorted(exp_of.items(), key=lambda kv: kv[1]):
        net = float(g.loc[e, "net"]) if len(g) and e in g.index else 0.0
        ct = float(g.loc[e, "ct"]) if len(g) and e in g.index else 0.0
        reason = ""
        if T > t_end:
            reason = "expires after the window (no BTC path to its settlement)"
        elif e in unsettled:
            reason = "unsettled fills"
        elif ct <= 0:
            reason = "no settled contracts"
        rows.append((e, int(T), net, ct, not reason, reason))
    return pd.DataFrame(rows, columns=EVENT_COLS)


def inputs_from_replay(label: str, collected: dict[str, Any], ledger: pd.DataFrame, t_end: int) -> HedgeInputs:
    """HedgeInputs of one replay: collector output + the replay ledger (Kalshi leg per event)."""
    df = ledger if ledger is not None else pd.DataFrame()
    unsettled = set(df.loc[df["settle"].isna(), "event"]) if len(df) and "settle" in df else set()
    done = df[df["settle"].notna()] if len(df) and "settle" in df else df
    ev = _events_table(collected.get("expirations", {}), done, t_end, unsettled)
    return HedgeInputs(label, collected["grid"], collected["fills"], collected["ticks"], ev, collected.get("venue"),
                       source="replay")


# ----------------------------------------------------------------------------- session fills
def session_fill_table(root: str | Path, t0: int, t1: int, uni: Universe, source: str) -> pd.DataFrame:
    """Our fills of a session in [t0, t1): 'live' (private `fill` frames of kalshi.ws), 'paper'
    (KalshiFill events on events.paper) or a ledger CSV path (replay_env ledger columns).
    Columns: ts, ticker, side (+1 bought YES), px ($), contracts, fee ($), [net ($)]."""
    rows: list[dict[str, Any]] = []
    if source == "live":
        fills = [f for f in uni.own_fills.values() if t0 <= f.ts < t1]
    elif source == "paper":
        from dh.store.codec import decode_event
        from dh.store.replay import iter_raw, list_streams

        fills = []
        if "events.paper" in list_streams(root):
            seen: set[str] = set()
            for rec in iter_raw(root, ["events.paper"], t0, t1):
                try:
                    ev = decode_event(rec.data)
                except (ValueError, KeyError, TypeError):
                    continue
                if isinstance(ev, KalshiFill) and ev.trade_id not in seen:
                    seen.add(ev.trade_id)
                    fills.append(ev)
    else:
        d = pd.read_csv(source)
        need = {"ts", "ticker", "side", "px", "contracts", "fee"}
        if not need <= set(d.columns):
            raise ValueError(f"ledger {source} lacks columns {sorted(need - set(d.columns))}")
        d = d[(d["ts"] >= t0) & (d["ts"] < t1)]
        keep = [c for c in ("ts", "ticker", "side", "px", "contracts", "fee", "net") if c in d]
        return d[keep].sort_values(["ts", "ticker"], kind="stable").reset_index(drop=True)
    for f in sorted(fills, key=lambda f: (f.ts, f.trade_id)):
        rows.append({"ts": f.ts, "ticker": f.ticker, "side": 1 if f.book_side == "bid" else -1, "px": f.yes_px / PX_SCALE,
                     "contracts": f.qty / QTY_SCALE, "fee": f.fee_micros / 1e6})
    return pd.DataFrame(rows, columns=["ts", "ticker", "side", "px", "contracts", "fee"])


def session_inputs(root: str | Path, t0: int, t1: int, cfg: StrategyConfig, uni: Universe, fills: pd.DataFrame, *,
                   label: str = "session", step_s: float = 1.0, warm: str = "recorded", venue: str | None = None,
                   source: str = "session") -> HedgeInputs:
    """HedgeInputs of a fixed list of fills (session_fill_table): the strategy's fair-value probe is
    replayed over [t0, t1) and D_e = sum q_i Delta_i is recomputed from the fills every ``step_s``."""
    specs = {r.ticker: r.spec for r in uni.markets.values() if r.spec is not None}
    f = fills[fills["ticker"].isin(specs)].sort_values("ts", kind="stable").reset_index(drop=True)
    exp_of: dict[str, int] = {}
    ev_of: dict[str, str] = {}
    for tk in sorted(set(f["ticker"])):
        e = settlement_cluster(specs[tk].expiration_ts)
        exp_of[e], ev_of[tk] = specs[tk].expiration_ts, e
    probe, feed, winfo = probe_for_window(root, t0, t1, cfg, uni, warm=warm,
                                          specs=[specs[tk] for tk in sorted(set(f["ticker"]))])
    pos: dict[str, float] = defaultdict(float)
    tickers: dict[str, set[str]] = defaultdict(set)
    grid, frows, ticks = [], [], []
    top = _TopTracker(venue) if venue else None
    period = max(1, int(round(step_s * NS_PER_S)))
    next_t, i, first = 0, 0, True
    fv = f.to_dict("records")
    for ev in feed.events(on_add=probe.add):
        if first:
            first = False
            prime_probe(probe, feed.stream)
        while i < len(fv) and fv[i]["ts"] <= ev.ts:
            r = fv[i]
            i += 1
            tk, e = r["ticker"], ev_of[r["ticker"]]
            q = float(r["side"]) * float(r["contracts"])
            fair = probe.fair(tk, int(r["ts"]))
            d = float(fair.delta) if fair is not None else math.nan
            pos[tk] += q
            tickers[e].add(tk)
            frows.append((int(r["ts"]), e, tk, q, d, q * d if math.isfinite(d) else 0.0))
        probe.on_event(ev)
        if type(ev) is IndexTick and ev.index_id == "BRTI" and ev.feed in ("1hz", "5hz"):
            if not ticks or ev.ts >= ticks[-1][0]:
                ticks.append((ev.ts, float(ev.value)))
        if top is not None:
            top.on_event(ev)
        if ev.ts >= next_t and tickers:
            next_t = ev.ts + period
            S = probe.spot()
            if S is None or not probe.mm.fv.ready:
                continue
            sig = float(probe.mm._sigma_1s(ev.ts, S))
            for e, tks in tickers.items():
                T = exp_of[e]
                if T <= ev.ts:
                    continue
                D = 0.0
                for tk in tks:
                    if pos[tk]:
                        fair = probe.fair(tk, ev.ts)
                        if fair is not None:
                            D += pos[tk] * float(fair.delta)
                grid.append((ev.ts, e, D, float(S), sig, (T - ev.ts) / NS_PER_S))
    settle = uni.settlement_values()
    k = f.copy()
    k["event"] = k["ticker"].map(ev_of)
    if "net" not in k:
        k["settle"] = k["ticker"].map(settle)
        k["net"] = k["side"] * (k["settle"] - k["px"]) * k["contracts"] - k["fee"]
    unsettled = set(k.loc[k["net"].isna(), "event"])
    ev_tab = _events_table(exp_of, k[k["net"].notna()], t1, unsettled)
    return HedgeInputs(label, pd.DataFrame(grid, columns=GRID_COLS), pd.DataFrame(frows, columns=FILL_COLS),
                       pd.DataFrame(ticks, columns=["ts", "value"]), ev_tab, top.frame() if top else None, source=source,
                       meta={"fv_warm": winfo.source, "fv_ready_at_t0": winfo.ready, "fills": len(f)})


# ============================================================================ hedge simulation
def build_items(inp: HedgeInputs, half_spread_bps: float) -> list[tuple]:
    """Time-ordered simulation items of the included events: hedge-venue market data (a recorded
    venue's top of book, else a proxy BBO at BRTI +- half spread), fills, decision steps and
    expirations. Items with equal timestamps: market data, fills, decisions, then settlement."""
    inc = set(inp.included["event"])
    items: list[tuple] = []
    seq = 0
    if inp.venue is not None and len(inp.venue):
        for r in inp.venue.itertuples(index=False):
            ev = ExtBBO(int(r.ts), int(r.ts), HEDGE_VENUE, HEDGE_SYMBOL, float(r.bid), float(r.bid_size), float(r.ask),
                        float(r.ask_size))
            items.append((int(r.ts), MD, seq, ev))
            seq += 1
    else:
        h = half_spread_bps / 1e4
        for ts, v in zip(inp.ticks["ts"].to_numpy(dtype=np.int64), inp.ticks["value"].to_numpy(dtype=float)):
            items.append((int(ts), MD, seq, ExtBBO(int(ts), int(ts), HEDGE_VENUE, HEDGE_SYMBOL, v * (1 - h), PROXY_DEPTH_BTC,
                                                   v * (1 + h), PROXY_DEPTH_BTC)))
            seq += 1
    for r in inp.fills.itertuples(index=False):
        if r.event in inc:
            items.append((int(r.ts), FILL, seq, (r.event, float(r.dD_btc))))
            seq += 1
    for r in inp.grid.itertuples(index=False):
        if r.event in inc:
            items.append((int(r.ts), GRID, seq, (r.event, float(r.D_btc), float(r.S), float(r.sigma_1s), float(r.tau_s))))
            seq += 1
    for r in inp.included.itertuples(index=False):
        items.append((int(r.expiration_ns), EXPIRE, seq, r.event))
        seq += 1
    items.sort(key=lambda x: (x[0], x[1], x[2]))
    return items


def simulate_hedge(inp: HedgeInputs, pol: HedgePolicy, hedge_cfg: HedgeCfg, *, scale: float = 1.0,
                   latency: LatencyModel | None = None, sim_policy: str = "B", seed: int = 0,
                   half_spread_bps: float = 1.0, items: list[tuple] | None = None) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Run one hedge policy over one fill stream (module docstring). Returns (per-event frame:
    event, hedge_pnl (cash + residual marked to the last BRTI tick; before fees), hedge_fees,
    exec_cost (vs BRTI at execution), turnover_btc, trades, residual_btc; trade log)."""
    items = build_items(inp, half_spread_bps) if items is None else items
    lat = latency if latency is not None else LatencyModel(seed)
    sim = HedgeVenueSim(HEDGE_VENUE, pol.fee_bps, pol.fee_bps, lat, seed=seed, symbol=HEDGE_SYMBOL,
                        fill_policy=POLICY_FULL[policy_letter(sim_policy)])
    hcfg = replace(hedge_cfg, enabled=True, venue=HEDGE_VENUE, symbol=HEDGE_SYMBOL, fee_bps_maker=pol.fee_bps,
                   fee_bps_taker=pol.fee_bps, half_spread_bps=half_spread_bps, band_min_btc=pol.band_min_btc)
    min_lot = float(hedge_cfg.min_order_btc)
    tick_ts = inp.ticks["ts"].to_numpy(dtype=np.int64)
    tick_v = inp.ticks["value"].to_numpy(dtype=float)
    H: dict[str, float] = defaultdict(float)
    pend: dict[str, float] = defaultdict(float)
    cash: dict[str, float] = defaultdict(float)
    fees: dict[str, float] = defaultdict(float)
    xcost: dict[str, float] = defaultdict(float)
    turn: dict[str, float] = defaultdict(float)
    ntr: dict[str, int] = defaultdict(int)
    target: dict[str, float] = defaultdict(float)
    closed: set[str] = set()
    retry: set[str] = set()
    orders: dict[str, list] = {}
    log: list[tuple] = []
    n = 0

    def mark(t: int) -> float | None:
        i = int(np.searchsorted(tick_ts, t, side="right")) - 1
        return float(tick_v[i]) if i >= 0 else None

    def apply(m: Any) -> None:
        o = orders.get(m.client_order_id)
        if o is None:
            return
        e, sgn = o[0], o[1]
        if type(m) is HedgeFill:
            q = sgn * m.qty_btc
            H[e] += q
            pend[e] -= q
            o[3] += m.qty_btc
            cash[e] -= q * m.price
            fees[e] += m.fee_usd
            px = mark(m.ts_exch + sim.md_offset)
            if px is not None:
                xcost[e] += q * (m.price - px)
        elif type(m) is HedgeOrderUpdate and m.status in ("filled", "canceled", "rejected") and not o[4]:
            # final state: the part the venue did not fill never will. Its fills are counted by their
            # own HedgeFill messages, which may arrive before OR after this update (ws vs REST latency)
            o[4] = True
            pend[e] -= sgn * max(0.0, o[2] - m.filled_btc)

    def submit(ts: int, e: str, trade: float, why: str) -> None:
        nonlocal n
        if abs(trade) <= _EPS:
            return
        n += 1
        coid = f"h{n}"
        sgn = 1.0 if trade > 0 else -1.0
        orders[coid] = [e, sgn, abs(trade), 0.0, False]
        pend[e] += trade
        turn[e] += abs(trade)
        ntr[e] += 1
        log.append((ts, e, trade, why, H[e], pend[e]))
        sim.submit(PlaceHedge(coid, HEDGE_VENUE, HEDGE_SYMBOL, "buy" if trade > 0 else "sell", abs(trade),
                              order_type="market", reason=why), ts)

    for ts, kind, _, payload in items:
        for m in sim.pop_due(ts - 1):
            apply(m)
        if kind == MD:
            for m in sim.on_market_event(payload):
                apply(m)
            for e in list(retry):  # unwind remainders (a one-level recorded book may fill partially)
                if abs(pend[e]) <= _EPS:
                    if abs(H[e]) <= 1e-9:
                        retry.discard(e)
                    else:
                        submit(ts, e, -H[e], "unwind_retry")
        elif kind == FILL:
            e, dD = payload
            if pol.kind == "per_fill" and e not in closed:
                target[e] -= scale * dD
                tr = target[e] - (H[e] + pend[e])
                if abs(tr) >= min_lot:
                    submit(ts, e, tr, "fill")
        elif kind == GRID:
            e, D, S, sig, tau = payload
            if pol.kind == "band" and e not in closed:
                dec = decide_hedge(hcfg, D_btc=scale * D + H[e], spot=S, sigma_abs_per_sqrt_s=sig, h_eff_s=tau,
                                   lam=pol.lam, pending_btc=pend[e])
                if dec.target_btc:
                    submit(ts, e, dec.target_btc, "band")
        else:  # EXPIRE: the event settles, its hedge has no purpose any more
            closed.add(payload)
            submit(ts, payload, -(H[payload] + pend[payload]), "unwind")
            retry.add(payload)
    for m in sim.pop_due(END_NS):
        apply(m)
    last = float(tick_v[-1]) if len(tick_v) else 0.0
    rows = []
    for e in inp.included["event"]:
        rows.append({"event": e, "hedge_pnl": cash[e] + H[e] * last, "hedge_fees": fees[e], "exec_cost": xcost[e],
                     "turnover_btc": turn[e], "trades": ntr[e], "residual_btc": H[e]})
    trades = pd.DataFrame(log, columns=["ts", "event", "trade_btc", "reason", "H_before", "pending_after"])
    return pd.DataFrame(rows, columns=["event", "hedge_pnl", "hedge_fees", "exec_cost", "turnover_btc", "trades",
                                       "residual_btc"]), trades


_CTX: dict[str, Any] = {}  # fork-shared simulation inputs (parallel evaluate)


def _sim_job(args: tuple) -> tuple[str, float, int, pd.DataFrame, pd.DataFrame]:
    label, scale, j = args
    inp, items, pols, hcfg, lat, seed, hs, sim_policy = _CTX[label]
    pe, tr = simulate_hedge(inp, pols[j], hcfg, scale=scale, latency=lat, sim_policy=sim_policy, seed=seed,
                            half_spread_bps=hs, items=items)
    return label, scale, j, pe, tr


def evaluate(inputs: Sequence[HedgeInputs], policies: Sequence[HedgePolicy], hedge_cfg: HedgeCfg, *,
             scales: Sequence[float] = (1.0,), latency: LatencyModel | None = None, seed: int = 1,
             half_spread_bps: float = 1.0, sim_policies: dict[str, str] | None = None,
             n_jobs: int = 1) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Every hedge policy x scale on every fill stream. Returns (per-event frame: policy (fill-stream
    label), scale, hedge_policy, kind, lam, fee_bps, event, expiration_ns, kalshi_usd, contracts,
    hedge_pnl, hedge_fees, exec_cost, turnover_btc, trades, residual_btc, total_usd; trade log)."""
    sim_policies = sim_policies or {}
    for inp in inputs:
        _CTX[inp.label] = (inp, build_items(inp, half_spread_bps), list(policies), hedge_cfg, latency, seed,
                           half_spread_bps, sim_policies.get(inp.label, inp.label))
    jobs = [(inp.label, float(k), j) for inp in inputs for k in scales for j in range(len(policies))]
    try:
        if n_jobs > 1 and len(jobs) > 1:
            with ProcessPoolExecutor(max_workers=min(n_jobs, len(jobs)), mp_context=mp.get_context("fork")) as ex:
                res = list(ex.map(_sim_job, jobs, chunksize=max(1, len(jobs) // (4 * n_jobs))))
        else:
            res = [_sim_job(j) for j in jobs]
    finally:
        for inp in inputs:
            _CTX.pop(inp.label, None)
    by = {inp.label: inp for inp in inputs}
    parts, logs = [], []
    for label, k, j, pe, tr in res:
        pol = policies[j]
        ev = by[label].included[["event", "expiration_ns", "kalshi_net_usd", "contracts"]]
        d = ev.merge(pe, on="event", how="left")
        d[["hedge_pnl", "hedge_fees", "exec_cost", "turnover_btc", "trades", "residual_btc"]] = d[
            ["hedge_pnl", "hedge_fees", "exec_cost", "turnover_btc", "trades", "residual_btc"]].fillna(0.0)
        d["kalshi_usd"] = k * d.pop("kalshi_net_usd")
        d["contracts"] = k * d["contracts"]
        d["total_usd"] = d["kalshi_usd"] + d["hedge_pnl"] - d["hedge_fees"]
        d.insert(0, "fee_bps", pol.fee_bps)
        d.insert(0, "lam", pol.lam)
        d.insert(0, "kind", pol.kind)
        d.insert(0, "hedge_policy", pol.name)
        d.insert(0, "scale", k)
        d.insert(0, "policy", label)
        parts.append(d)
        if len(tr):
            logs.append(tr.assign(policy=label, scale=k, hedge_policy=pol.name))
    per_event = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
    return per_event, (pd.concat(logs, ignore_index=True) if logs else pd.DataFrame())


# ============================================================================ inference
def _util(x: np.ndarray, lam: float) -> np.ndarray:
    """mean - lam/2 var (ddof 1) along the last axis."""
    return x.mean(axis=-1) - 0.5 * lam * x.var(axis=-1, ddof=1)


def paired_stat_ci(stat, a: np.ndarray, b: np.ndarray, n_boot: int = 1000, seed: int = 23) -> CI:
    """CI of stat(a, b) (vectorized over the last axis) with settlement events as the resampling
    unit (a and b aligned by event): hull of the delete-one-event jackknife-t and the percentile
    bootstrap (95 %). ``se`` = jackknife SE (one-sided p-values: CI.p_greater / p_less)."""
    a = np.asarray(a, dtype=float)
    b = np.asarray(b, dtype=float)
    k = len(a)
    if k < 3:
        return CI(float(stat(a, b)) if k >= 2 else math.nan, math.nan, math.nan, k)
    theta = float(stat(a, b))
    keep = np.ones(k, dtype=bool)
    jack = np.empty(k)
    for i in range(k):
        keep[i] = False
        jack[i] = stat(a[keep], b[keep])
        keep[i] = True
    se = math.sqrt((k - 1) / k * float(np.sum((jack - jack.mean()) ** 2)))
    tq = _tq(k)
    los, his = [theta - tq * se], [theta + tq * se]
    rng = np.random.default_rng(seed)
    idx = rng.integers(0, k, size=(n_boot, k))
    bs = stat(a[idx], b[idx])
    bs = bs[np.isfinite(bs)]
    if len(bs) >= 20:
        ql, qh = np.quantile(bs, [0.025, 0.975])
        los.append(float(ql))
        his.append(float(qh))
    return CI(theta, float(min(los)), float(max(his)), k, se, CI_METHOD_U)


def _var_ratio(a: np.ndarray, b: np.ndarray) -> np.ndarray:
    va = a.var(axis=-1, ddof=1)
    with np.errstate(invalid="ignore", divide="ignore"):
        return np.where(va > 0, b.var(axis=-1, ddof=1) / va, np.nan)


def summarize(per_event: pd.DataFrame, lam_cfg: float, lams: Sequence[float] = (), *, n_boot: int = 1000) -> pd.DataFrame:
    """One row per (fill stream, scale, hedge policy); paired against 'none' on the same events
    (module docstring). Utility: the band's own lambda, else the configured lambda (lam_u)."""
    rows = []
    if per_event is None or not len(per_event):
        return pd.DataFrame()
    for (L, k), g in per_event.groupby(["policy", "scale"], sort=True):
        base = g[g["hedge_policy"] == "none"].set_index("event").sort_index()
        x0 = base["total_usd"].to_numpy(dtype=float)
        for name, h in g.groupby("hedge_policy", sort=False):
            h = h.set_index("event").reindex(base.index)
            x = h["total_usd"].to_numpy(dtype=float)
            kind = str(h["kind"].iloc[0])
            lam_pol = float(h["lam"].iloc[0])
            lam_u = lam_pol if kind == "band" and math.isfinite(lam_pol) else float(lam_cfg)
            ct = h["contracts"].to_numpy(dtype=float)
            ne = len(x)
            net = ratio_ci(100.0 * x, ct) if ne else CI(math.nan, math.nan, math.nan, 0)
            U = paired_stat_ci(lambda a, b, lu=lam_u: _util(b, lu), x0, x, n_boot)
            dU = paired_stat_ci(lambda a, b, lu=lam_u: _util(b, lu) - _util(a, lu), x0, x, n_boot)
            vr = paired_stat_ci(_var_ratio, x0, x, n_boot)
            cts = float(ct.sum())
            row = {"policy": L, "scale": k, "hedge_policy": name, "kind": kind, "lam": lam_pol,
                   "fee_bps": float(h["fee_bps"].iloc[0]), "events": ne, "contracts": cts,
                   "net_c": net.mean, "net_lo_c": net.lo, "net_hi_c": net.hi,
                   "mean_usd_event": float(x.mean()) if ne else math.nan,
                   "sd_usd_event": float(x.std(ddof=1)) if ne > 1 else math.nan,
                   "var_ratio_vs_none": vr.mean, "var_ratio_lo": vr.lo, "var_ratio_hi": vr.hi,
                   "hedge_pnl_c": 100.0 * float(h["hedge_pnl"].sum()) / cts if cts > 0 else math.nan,
                   "hedge_fees_c": 100.0 * float(h["hedge_fees"].sum()) / cts if cts > 0 else math.nan,
                   "exec_cost_c": 100.0 * float(h["exec_cost"].sum()) / cts if cts > 0 else math.nan,
                   "turnover_btc_per_event": float(h["turnover_btc"].mean()) if ne else math.nan,
                   "trades_per_event": float(h["trades"].mean()) if ne else math.nan,
                   "residual_btc_max": float(h["residual_btc"].abs().max()) if ne else math.nan,
                   "lam_u": lam_u, "util_usd": U.mean, "util_lo": U.lo, "util_hi": U.hi,
                   "d_util_usd": dU.mean, "d_util_lo": dU.lo, "d_util_hi": dU.hi, "d_util_p": dU.p_greater(0.0)}
            for lm in sorted(set(float(v) for v in lams) | {float(lam_cfg)}):
                row[f"util_lam{lm:g}"] = float(_util(x, lm)) if ne > 1 else math.nan
            rows.append(row)
    return pd.DataFrame(rows)


def e5_verdict(dec: pd.DataFrame) -> str:
    """Decision rule on the band_configured rows (one per fill policy) of summarize()."""
    if dec is None or not len(dec):
        return "INCONCLUSIVE (no settled fills of events that settle inside the window)"
    d = dec[dec["policy"].isin(["B", "C"]) & np.isfinite(dec["d_util_usd"].to_numpy(dtype=float))]
    txt = "; ".join(f"{r.policy}: utility gain {r.d_util_usd:.4g} [{r.d_util_lo:.4g}, {r.d_util_hi:.4g}] $/event, "
                    f"turnover {r.turnover_btc_per_event:.3g} BTC/event, {r.events} events" for r in d.itertuples())
    never = bool(len(d)) and bool((d["turnover_btc_per_event"] == 0).all())
    if {"B", "C"} <= set(d["policy"]) and (d["d_util_lo"] > 0).all() and not never:
        return f"ACCEPT (the configured hedge band beats no hedge in utility under B and C: {txt})"
    if never:  # no hedge trade at all: the P&L IS the unhedged P&L, the utility gain is exactly 0
        return f"REJECT (hedge stays disabled: the band never trades at this scale and cost, no hedge is optimal; {txt})"
    if len(d) and (np.isfinite(d["d_util_hi"].to_numpy(dtype=float)) & (d["d_util_hi"] <= 0)).any():
        return f"REJECT (hedge stays disabled: no utility gain; {txt})"
    return f"INCONCLUSIVE (utility gain not established under B and C: {txt or 'no results'})"


def regime_rows(per_event: pd.DataFrame, ticks: dict[str, pd.DataFrame], decision: str, n_boot: int = 300) -> pd.DataFrame:
    """Per fill stream: utility-relevant comparison of the decision policy vs no hedge by regime of
    the settlement event (vol tercile of BRTI realized vol over the hour before expiry, causal;
    weekday / weekend of the expiry): mean paired P&L difference $/event (CI) and s.d. of both."""
    rows = []
    if per_event is None or not len(per_event):
        return pd.DataFrame()
    d0 = per_event[(per_event["scale"] == 1.0)]
    for L, g in d0.groupby("policy"):
        a = g[g["hedge_policy"] == "none"].set_index("event")
        b = g[g["hedge_policy"] == decision].set_index("event").reindex(a.index)
        if not len(a) or b["total_usd"].isna().all():
            continue
        tk = ticks.get(L)
        exp = a["expiration_ns"].to_numpy(dtype=np.int64)
        rv = (realized_vol_asof(tk["ts"].to_numpy(dtype=np.int64), tk["value"].to_numpy(dtype=float), exp)
              if tk is not None and len(tk) else np.full(len(a), np.nan))
        c1, c2 = vol_cuts(rv)
        vt = np.where(~np.isfinite(rv) | (not math.isfinite(c1)), "n/a", np.where(rv <= c1, "low", np.where(rv <= c2, "mid", "high")))
        wd = np.where(pd.to_datetime(exp, unit="ns", utc=True).dayofweek >= 5, "weekend", "weekday")
        diff = (b["total_usd"] - a["total_usd"]).to_numpy(dtype=float)
        for fam, lab in (("vol_tercile", vt), ("weekday", wd)):
            for r in sorted(set(lab)):
                m = lab == r
                ci = paired_stat_ci(lambda x, y: (y - x).mean(axis=-1), a["total_usd"].to_numpy(dtype=float)[m],
                                    b["total_usd"].to_numpy(dtype=float)[m], n_boot)
                rows.append({"policy": L, "family": fam, "regime": r, "events": int(m.sum()),
                             "d_usd_event": float(diff[m].mean()), "d_lo": ci.lo, "d_hi": ci.hi,
                             "sd_none": float(a["total_usd"][m].std(ddof=1)) if m.sum() > 1 else math.nan,
                             "sd_hedged": float(b["total_usd"][m].std(ddof=1)) if m.sum() > 1 else math.nan})
    return pd.DataFrame(rows)


# ============================================================================ experiment
def _grid(values: Sequence[float] | str | None, default: Sequence[float]) -> list[float]:
    if values is None or values == "":
        return list(default)
    if isinstance(values, str):
        return [float(x) for x in values.split(",") if x.strip()]
    return [float(x) for x in values]


def run(root: str | Path, t0: int, t1: int, out: str | Path, *, cfg: StrategyConfig | None = None,
        policies: Sequence[str] = ("B", "C"), warm: str = "recorded", universe: Universe | None = None, seed: int = 1,
        n_jobs: int = 1, progress=None, latency: LatencyModel | None = None,
        fees_bps: Sequence[float] | str | None = None, decision_fee_bps: float | None = None,
        lams: Sequence[float] | str | None = None, scales: Sequence[float] | str | None = None, step_s: float = 1.0,
        half_spread_bps: float | None = None, hedge_venue: str | None = None, session_fills: str | None = None,
        n_boot: int = 1000) -> dict[str, Any]:
    """E5 on a recording (module docstring). ``session_fills`` = 'live' | 'paper' | ledger CSV path
    evaluates those fills instead of replaying the strategy (hedge execution under B and C)."""
    Path(out).mkdir(parents=True, exist_ok=True)
    cfg = cfg or StrategyConfig()
    uni = universe or build_universe(root, t0, t1)
    lat = research_latency(uni, latency, seed=seed)
    pols = [policy_letter(p) for p in policies]
    hs = cfg.hedge.half_spread_bps if half_spread_bps is None else float(half_spread_bps)
    fee_dec = cfg.hedge.fee_bps_maker if decision_fee_bps is None else float(decision_fee_bps)
    grid = policy_grid(cfg, fees_bps=_grid(fees_bps, DEFAULT_FEES_BPS), lams=_grid(lams, DEFAULT_LAMS),
                       decision_fee_bps=fee_dec)
    dec_name = decision_policy(cfg, fee_dec).name
    ks = sorted({1.0, *_grid(scales, (1.0,))})
    warns: list[str] = []
    runs = []
    if session_fills:
        fl = session_fill_table(root, t0, t1, uni, session_fills)
        src = session_fills if session_fills in ("live", "paper") else f"ledger {Path(session_fills).name}"
        if not len(fl):
            warns.append(f"no {src} fills in the window")
        base = session_inputs(root, t0, t1, cfg, uni, fl, label="session", step_s=step_s, warm=warm, venue=hedge_venue,
                              source=src)
        if not base.meta.get("fv_ready_at_t0", True):
            warns.append("fair-value probe NOT warm at t0: deltas are missing until it is (record CF history before t0)")
        inputs = [replace(base, label=p) for p in pols]  # same fills; hedge execution under each policy
        fills_desc = f"{src} fills ({len(fl)}): positions rebuilt from the fills, deltas from the strategy's FvProbe"
    else:
        rcfg = replace(cfg, hedge=replace(cfg.hedge, enabled=False))  # the fills E5 decides on: hedge engine off
        runs = run_variants(root, t0, t1, [Variant("config", rcfg)], pols, universe=uni, warm=warm, seed=seed,
                            n_jobs=n_jobs, collectors=[partial(HedgeInputCollector, step_s=step_s, venue=hedge_venue)],
                            progress=progress, latency=lat)
        warns += run_warnings(runs)
        inputs = [inputs_from_replay(r.policy, r.collectors[0], r.df, t1) for r in runs]
        fills_desc = "replayed MarketMaker fills (hedge engine off), per fill policy"
    venue_ok = bool(hedge_venue) and any(i.venue is not None and len(i.venue) for i in inputs)
    if hedge_venue and not venue_ok:
        warns.append(f"hedge venue {hedge_venue!r} has no recorded book in the window: hedges execute against the "
                     f"proxy book at BRTI +- {hs:g} bp instead")
    per_event, trades = evaluate(inputs, grid, cfg.hedge, scales=ks, latency=lat, seed=seed, half_spread_bps=hs,
                                 n_jobs=n_jobs)
    tab = summarize(per_event, cfg.lam, _grid(lams, DEFAULT_LAMS), n_boot=n_boot)
    dec = tab[(tab["hedge_policy"] == dec_name) & (tab["scale"] == 1.0)] if len(tab) else tab
    evt = pd.concat([i.events.assign(policy=i.label) for i in inputs], ignore_index=True) if inputs else pd.DataFrame()
    excluded = evt[~evt["included"]] if len(evt) else evt
    reg = regime_rows(per_event, {i.label: i.ticks for i in inputs}, dec_name)
    band_edge = no_trade_band_example(cfg, inputs[0] if inputs else None, fee_dec, hs)
    rep = Report("e5_hedge", "E5 — Hedge policy on realized fills (none / per fill / mean-variance band)", Path(out),
                 synthetic=uni.synthetic, rule=RULE_E5,
                 meta={"root": str(root), "window": f"{fmt_ns(t0)} .. {fmt_ns(t1)}", "fills": fills_desc,
                       **inputs_meta(uni, t0, cfg, t1)[0], "latency": describe_latency(lat, uni),
                       "hedge venue": (f"recorded {hedge_venue} top of book (one level)" if venue_ok
                                       else f"proxy BBO at BRTI +- {hs:g} bp (deep)"
                                       + (f"; {hedge_venue} has no recorded book" if hedge_venue else "")),
                       "decision policy": f"{dec_name}: cfg.hedge (band_min {cfg.hedge.band_min_btc:g} BTC, min order "
                                          f"{cfg.hedge.min_order_btc:g} BTC) at cfg.lam = {cfg.lam:g}/$ and "
                                          f"{fee_dec:g} bp + {hs:g} bp half spread",
                       "band edge at the median decision state (decision policy)": band_edge,
                       "decision step": f"{step_s:g} s", "scales (what-if on the same fills)": ",".join(f"{k:g}" for k in ks),
                       "utility": "U = mean - lam/2 var of P&L per settlement event ($); CIs: " + CI_METHOD_U,
                       "settlement events evaluated / excluded": "; ".join(
                           f"{i.label}: {int(i.events['included'].sum()) if len(i.events) else 0} / "
                           f"{int((~i.events['included']).sum()) if len(i.events) else 0}" for i in inputs) or "none"})
    rep.decision_events = int(dec["events"].min()) if len(dec) else 0
    rep.policies = sorted(set(dec.loc[dec["events"] > 0, "policy"])) if len(dec) else []
    rep.in_sample, rep.in_sample_why = inputs_status(uni, t0, cfg, t1)
    rep.verdict = e5_verdict(dec)
    for w in warns:
        rep.line(f"WARNING: {w}")
    if len(excluded):
        rep.line(f"{len(excluded)} settlement event(s) excluded: " + "; ".join(
            f"{r.event} ({r.reason})" for r in excluded.head(6).itertuples()) + (" ..." if len(excluded) > 6 else ""))
    rep.line("Every hedge policy is evaluated on the identical fill stream of its fill policy (paired by settlement "
             "event); the Kalshi leg is the realized net P&L incl. fees. Per-fill and band hedges are unwound when "
             "their event settles.")
    rep.table("decision", dec, "Decision policy vs no hedge per fill policy (scale 1): utility gain d_util_usd "
                               "($/event, paired CI), variance ratio vs no hedge, net c/contract.")
    rep.table("policies", tab, "Every hedge policy x fee tier x lambda x scale (descriptive; the decision uses the "
                               "`decision` rows only). util_usd at lam_u (the band's own lambda, else cfg.lam).")
    rep.table("regimes", reg, "Decision policy minus no hedge, $/event, by regime of the settlement event.")
    rep.table("events", per_event, "Per settlement event and hedge policy: Kalshi leg, hedge P&L, fees, execution cost "
                                   "vs BRTI, turnover.")
    rep.write()
    return {"table": tab, "decision": dec, "per_event": per_event, "trades": trades, "inputs": inputs, "runs": runs,
            "regimes": reg, "verdict": rep.final_verdict(), "rule_outcome": rep.verdict}


def no_trade_band_example(cfg: StrategyConfig, inp: HedgeInputs | None, fee_bps: float, half_spread_bps: float) -> str:
    """The decision band's half-width at the median decision state of the first fill stream, next to
    the median |D| (why a band never trades at small size)."""
    if inp is None or not len(inp.grid):
        return "n/a (no decision steps)"
    g = inp.grid[inp.grid["event"].isin(set(inp.included["event"]))]
    if not len(g):
        return "n/a (no decision steps of evaluated events)"
    hc = replace(cfg.hedge, enabled=True, fee_bps_maker=fee_bps, half_spread_bps=half_spread_bps)
    S, sig, tau = (float(g[c].median()) for c in ("S", "sigma_1s", "tau_s"))
    b = no_trade_band(hc, spot=S, sigma_abs_per_sqrt_s=sig, h_eff_s=tau, lam=cfg.lam)
    return (f"{b:.4g} BTC (S {S:.0f}, sigma {sig:.3g} $/sqrt s, tau {tau:.0f} s) vs median abs(D) "
            f"{float(g['D_btc'].abs().median()):.3g} BTC, max abs(D) {float(g['D_btc'].abs().max()):.3g} BTC")


__all__ = ["HedgePolicy", "HedgeInputs", "HedgeInputCollector", "policy_grid", "decision_policy", "inputs_from_replay",
           "session_fill_table", "session_inputs", "build_items", "simulate_hedge", "evaluate", "summarize",
           "paired_stat_ci", "e5_verdict", "regime_rows", "run", "RULE_E5"]
