"""Experiment 8: is selectively TAKING stale Kalshi quotes profitable after the taker fee?

Scan (causal, on a ``step_ms`` decision grid of receive time) every quotable market: with the
strategy's own fair-value band (FvProbe = MarketMaker's nowcast, vol, tails, settlement window
and band), an opportunity exists when

    buy YES at best ask a :  F_lo - a - taker_fee(a) >= threshold
    sell YES at best bid b:  b - F_hi - taker_fee(b) >= threshold

taker_fee = the EXACT per-order Kalshi fee per contract of the intended take (trade fee plus
balance rounding with carry: dh.kalshi.fees.FeeSchedule.single_fill_fees at the take size; audit
M3), from the market's recorded fee type/multiplier; executed takes are charged through an
OrderFeeAccumulator per order (one order may fill across several levels). An episode = consecutive grid points where the condition holds for a market side; the
first grid point of an episode sends ONE marketable order (limit = the touch seen, size = min(clip,
displayed touch qty)). It reaches the book after a sampled submit latency (LatencyModel; policy C
scales latency x1.5) and executes at exactly that time, after every recorded event up to it: the
fill walks the book AS RECORDED at arrival up to the limit (queue-empty check: the stale quote may
be gone), consuming displayed size once (our earlier takes are remembered per level, capped by the
displayed size, as in KalshiExchangeSim). Fill P&L per contract, net of the taker fee:
markout to the fair value 60 s later (F centre) and to settlement (recorded result).

Also reported: opportunities/day by threshold, fill rate after latency, P&L by staleness of the
Kalshi touch (time since it last changed) and by the preceding external move.

Every threshold is its own policy (audit M2): ``run`` scans once per threshold (an episode starts
when the edge first reaches THAT threshold) and decides on the 0.5c scan. Latency: research
placeholders with the recording's measured Kalshi market-data latency (audit M4). Clusters for
CIs = settlement events (expirations, audit M5); day blocks as a second check from 5 days on.

Decision rule (docs/TEST_MATRIX.md E8): accept if net > 0.5c/contract after fees with CI > 0 and
>= 20 opportunities/day, under BOTH fill policies B and C; otherwise taking stays disabled.
"""

from __future__ import annotations

import heapq
import math
import statistics
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Sequence

import numpy as np
import pandas as pd

from dh.core.events import KalshiBookDelta, KalshiBookSnapshot
from dh.core.units import NS_PER_MS, NS_PER_S, PX_SCALE, QTY_SCALE
from dh.execution.latency import LatencyModel
from dh.kalshi.fees import FeeEngine
from dh.research.exp_common import (
    Report,
    add_regimes,
    cluster_mean_ci,
    day_block_ok,
    fmt_ns,
    policy_letter,
    regime_table,
    with_day,
    write_csv,
)
from dh.research.replay_env import (
    HOUR_S,
    Universe,
    _ns,
    brti_ticks,
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
from dh.strategy.config import StrategyConfig

RULE_E8 = ("accept if taker net > 0.5c/contract after fees with a 95% CI > 0 and >= 20 opportunities/day; "
           "otherwise taking stays disabled")
THRESHOLDS_C = (0.0, 0.5, 1.0, 2.0)
MARK_H_S = (1.0, 5.0, 10.0, 60.0)


@dataclass
class _Take:
    t_dec: int
    t_exec: int
    ticker: str
    event: str
    side: int  # +1 buy YES, -1 sell YES
    limit_px: int
    size: int  # qty units
    edge_c: float
    F: float
    tau_s: float
    touch_age_s: float
    ext_move_1s: float
    fill_qty: int = 0
    fill_cost: float = 0.0  # $ paid (buy) or received (sell) for YES
    fee: float = 0.0
    F_mark: dict = field(default_factory=dict)  # horizon_s -> fair value at t_exec + h


@dataclass
class ScanResult:
    opportunities: pd.DataFrame
    takes: pd.DataFrame
    days: float
    synthetic: bool
    grid_points: int
    info: dict[str, Any] = field(default_factory=dict)


def scan(root: str | Path, t0: int, t1: int, cfg: StrategyConfig | None = None, *, universe: Universe | None = None,
         policy: str = "B", latency: LatencyModel | None = None, step_ms: int = 250, min_threshold_c: float = 0.0,
         clip_contracts: float = 10.0, mark_h_s: Sequence[float] = MARK_H_S, warm: str = "recorded", seed: int = 3,
         use_band: bool = True, min_tau_s: float = 5.0) -> ScanResult:
    cfg = cfg or StrategyConfig()
    uni = universe or build_universe(root, t0, t1)
    base_lat = research_latency(uni, latency, seed=seed)
    lat = base_lat.fork(seed, base_lat.multiplier * (1.5 if policy_letter(policy) == "C" else 1.0))
    probe, feed, winfo = probe_for_window(root, t0, t1, cfg, uni, warm=warm, seed=seed)
    mm = probe.mm
    fees = FeeEngine.from_config()
    sched: dict[str, Any] = {}
    step = step_ms * NS_PER_MS
    next_g = (t0 // step + 1) * step
    clip = int(round(clip_contracts * QTY_SCALE))
    in_episode: dict[tuple[str, int], bool] = {}
    opps: list[dict[str, Any]] = []
    pend_exec: list[tuple[int, int, _Take]] = []  # heap (t_exec, seq, take)
    pend_mark: list[tuple[int, int, float, _Take]] = []  # heap (t_mark, seq, horizon, take)
    mark_seq = 0
    done: list[_Take] = []
    consumed: dict[tuple[str, str, int], int] = {}
    touch: dict[str, tuple[int | None, int | None]] = {}
    touch_ts: dict[str, int] = {}
    mids: deque[tuple[int, float]] = deque()
    grid_points = 0
    first = True

    def fee_c(t: str, px: int, qty: int, book_side: str) -> float:
        """Exact per-contract fee of ONE taker order of qty at px (trade fee + balance rounding)."""
        s = sched.get(t)
        if s is None:
            spec = mm.specs[t]
            s = sched[t] = fees.schedule_for_spec(spec.fee_type, spec.fee_multiplier)
        if not s.supported or qty <= 0:
            return math.inf
        return 100.0 * s.single_fill_fees(px, qty, True, book_side).net_micros / 1e6 / (qty / QTY_SCALE)

    def ext_mid() -> float:
        ms = [b.top().mid for b in mm.ext.values() if b.top() is not None]
        return statistics.median(ms) if ms else math.nan

    def execute(tk: _Take) -> None:
        b = mm.books.get(tk.ticker)
        if b is None or not b.valid:
            return
        left = tk.size
        if tk.side > 0:  # lift YES asks = NO bids at NO px >= 1 - limit
            levels = [(px, q) for px, q in reversed(b.no_bids.items()) if PX_SCALE - px <= tk.limit_px]
            book = "no"
        else:
            levels = [(px, q) for px, q in reversed(b.yes_bids.items()) if px >= tk.limit_px]
            book = "yes"
        s = sched.get(tk.ticker)
        acc = s.order_accumulator("bid" if tk.side > 0 else "ask") if s is not None and s.supported else None
        for px, q in levels:
            avail = q - consumed.get((tk.ticker, book, px), 0)
            if avail <= 0:
                continue
            take = min(avail, left)
            yes_px = PX_SCALE - px if book == "no" else px
            consumed[(tk.ticker, book, px)] = consumed.get((tk.ticker, book, px), 0) + take
            tk.fill_qty += take
            tk.fill_cost += yes_px / PX_SCALE * take / QTY_SCALE
            tk.fee += (acc.apply_fill(yes_px, take, True).net_micros / 1e6) if acc is not None else 0.0
            left -= take
            if left <= 0:
                break

    def decide(g: int) -> None:
        nonlocal grid_points, mark_seq
        grid_points += 1
        m = ext_mid()
        if math.isfinite(m):
            mids.append((g, m))
            while mids and g - mids[0][0] > 2 * NS_PER_S:
                mids.popleft()
        past = next((v for ts, v in reversed(mids) if ts <= g - NS_PER_S), math.nan)
        ext_move = m - past if math.isfinite(m) and math.isfinite(past) else math.nan
        for t, spec in list(mm.specs.items()):
            if t in mm.settled or g >= spec.close_ts:
                continue
            tau = (spec.expiration_ts - g) / NS_PER_S
            if tau <= min_tau_s or tau > cfg.quoting.max_tau_s or spec.series_ticker not in cfg.quoting.enabled_series:
                continue
            b = mm.books.get(t)
            if b is None or not b.valid:
                continue
            bb, ba = b.best_bid(), b.best_ask()
            if bb is None and ba is None:
                continue
            f = probe.fair(t, g)
            if f is None:
                continue
            lo, hi = (f.F_lo, f.F_hi) if use_band else (f.F, f.F)
            for side, px in ((1, ba), (-1, bb)):
                key = (t, side)
                if px is None:
                    in_episode[key] = False
                    continue
                disp0 = b.best_ask_qty() if side > 0 else b.best_bid_qty()
                size0 = min(clip, disp0) if disp0 > 0 else clip
                edge = ((100.0 * (lo - px / PX_SCALE) if side > 0 else 100.0 * (px / PX_SCALE - hi))
                        - fee_c(t, px, size0, "bid" if side > 0 else "ask"))
                if edge < min_threshold_c:
                    in_episode[key] = False
                    continue
                if in_episode.get(key):
                    continue
                in_episode[key] = True
                disp = b.best_ask_qty() if side > 0 else b.best_bid_qty()
                age = (g - touch_ts.get(t, g)) / NS_PER_S
                opps.append({"t": g, "ticker": t, "event": settlement_cluster(spec.expiration_ts),
                             "event_ticker": spec.event_ticker, "side": side, "px": px / PX_SCALE,
                             "edge_c": edge, "F": f.F, "tau_s": tau, "touch_age_s": age, "ext_move_1s": ext_move,
                             "displayed_ct": disp / QTY_SCALE})
                tk = _Take(g, g + lat.submit_ns() + lat.md_offset_ns, t, settlement_cluster(spec.expiration_ts), side, px,
                           min(clip, disp) if disp > 0 else clip, edge, f.F, tau, age, ext_move)
                mark_seq += 1
                heapq.heappush(pend_exec, (tk.t_exec, mark_seq, tk))

    inf = 2**63

    for ev in feed.events(on_add=probe.add):
        if first:
            first = False
            prime_probe(probe, feed.stream)
        ts = ev.ts
        # everything scheduled strictly before this event, in time order (recorded events first at
        # ties, like the exchange simulator): order arrivals, markouts, decision-grid points
        while True:
            te = pend_exec[0][0] if pend_exec else inf
            tm = pend_mark[0][0] if pend_mark else inf
            tg = next_g if next_g < t1 else inf
            tn = min(te, tm, tg)
            if tn >= ts:
                break
            if tn == te:
                _, _, tk = heapq.heappop(pend_exec)
                execute(tk)
                done.append(tk)
                if tk.fill_qty > 0:
                    for h in mark_h_s:
                        mark_seq += 1
                        heapq.heappush(pend_mark, (tk.t_exec + int(h * NS_PER_S), mark_seq, h, tk))
            elif tn == tm:
                t_m, _, h, tk = heapq.heappop(pend_mark)
                f = probe.fair(tk.ticker, t_m)
                tk.F_mark[h] = f.F if f is not None else math.nan
            else:
                decide(next_g)
                next_g += step
        probe.on_event(ev)
        t = type(ev)
        if t is KalshiBookDelta or t is KalshiBookSnapshot:
            b = mm.books.get(ev.ticker)
            if b is not None:
                cur = (b.best_bid(), b.best_ask())
                if touch.get(ev.ticker) != cur:
                    touch[ev.ticker] = cur
                    touch_ts[ev.ticker] = ts
            if t is KalshiBookSnapshot:
                for k in [k for k in consumed if k[0] == ev.ticker]:
                    del consumed[k]
            else:
                k = (ev.ticker, ev.side, ev.px)
                if k in consumed and b is not None:
                    # the recorded level still shows what we took (no market impact): what we consumed
                    # can never exceed what is displayed (same rule as KalshiExchangeSim)
                    lvl = (b.yes_bids if ev.side == "yes" else b.no_bids).get(ev.px, 0)
                    if lvl < consumed[k]:
                        consumed[k] = lvl
    done += [tk for _, _, tk in pend_exec]
    settle = uni.settlement_values()
    rows = []
    for tk in done:
        n = tk.fill_qty / QTY_SCALE
        px = tk.fill_cost / n if n else math.nan
        s = settle.get(tk.ticker, math.nan)
        fee_ct = 100.0 * tk.fee / n if n else math.nan
        row = {"t_decision": tk.t_dec, "t_exec": tk.t_exec, "ticker": tk.ticker, "event": tk.event, "side": tk.side,
               "limit_px": tk.limit_px / PX_SCALE, "edge_c": tk.edge_c, "tau_s": tk.tau_s,
               "touch_age_s": tk.touch_age_s, "ext_move_1s": tk.ext_move_1s, "attempt_ct": tk.size / QTY_SCALE,
               "contracts": n, "avg_px": px, "fee_c": fee_ct, "F_decision": tk.F, "settle": s,
               "net_settle_c": 100.0 * tk.side * (s - px) - fee_ct if n else math.nan}
        for h in mark_h_s:
            fh = tk.F_mark.get(h, math.nan)
            row[f"net_{h:g}s_c"] = 100.0 * tk.side * (fh - px) - fee_ct if n else math.nan
        rows.append(row)
    days = max((t1 - t0) / (86400 * NS_PER_S), 1e-9)
    takes = pd.DataFrame(rows)
    if len(takes):
        bt = [e for e in brti_ticks(root, t0 - _ns(HOUR_S), t1, include_rest=False, cache=uni.cache)
              if e.feed in ("1hz", "5hz")]
        takes["rv_1h"] = realized_vol_asof(np.array([e.ts for e in bt], dtype=np.int64),
                                           np.array([e.value for e in bt], dtype=float),
                                           takes["t_decision"].to_numpy(dtype=np.int64))
        takes["ts"] = takes["t_exec"]
    return ScanResult(pd.DataFrame(opps), takes, days, uni.synthetic, grid_points,
                      {"fv_warm": winfo.source, "fv_ready_at_t0": winfo.ready, "policy": policy_letter(policy),
                       "threshold_c": min_threshold_c, "latency": describe_latency(lat, uni)})


def summarize(res: ScanResult, thresholds_c: Sequence[float] = THRESHOLDS_C, n_boot: int = 400) -> pd.DataFrame:
    rows = []
    op, tk = res.opportunities, res.takes
    for thr in thresholds_c:
        o = op[op["edge_c"] >= thr] if len(op) else op
        t = tk[tk["edge_c"] >= thr] if len(tk) else tk
        filled = t[t["contracts"] > 0] if len(t) else t
        row = {"threshold_c": thr, "opportunities": len(o), "opportunities_per_day": len(o) / res.days,
               "attempts": len(t), "filled": len(filled),
               "fill_rate": len(filled) / len(t) if len(t) else math.nan,
               "contracts": float(filled["contracts"].sum()) if len(filled) else 0.0,
               "events": int(filled["event"].nunique()) if len(filled) and "event" in filled else 0}
        for col, name in (("net_5s_c", "net_5s"), ("net_60s_c", "net_60s"), ("net_settle_c", "net_settle")):
            if len(filled) and col not in filled:
                continue
            ok = filled[filled[col].notna()] if len(filled) else filled
            ci = cluster_mean_ci(ok[col], ok["contracts"], ok["event"], n_boot) if len(ok) else None
            row[f"{name}_c"] = ci.mean if ci else math.nan
            row[f"{name}_lo_c"] = ci.lo if ci else math.nan
            row[f"{name}_hi_c"] = ci.hi if ci else math.nan
        ok = filled[filled["net_settle_c"].notna()] if len(filled) else filled
        row["usd_per_day_settle"] = float((ok["net_settle_c"] * ok["contracts"]).sum() / 100.0 / res.days) if len(ok) else 0.0
        rows.append(row)
    return pd.DataFrame(rows)


def by_staleness(res: ScanResult) -> pd.DataFrame:
    tk = res.takes
    if not len(tk):
        return pd.DataFrame()
    f = tk[tk["contracts"] > 0].copy()
    if not len(f):
        return pd.DataFrame()
    f["touch_age_b"] = pd.cut(f["touch_age_s"], [-0.01, 0.5, 1, 2, 5, 1e9], labels=["<0.5s", "0.5-1s", "1-2s", "2-5s", ">5s"])
    rows = []
    for b, g in f.groupby("touch_age_b", observed=True):
        ci = cluster_mean_ci(g["net_settle_c"], g["contracts"], g["event"], 300)
        rows.append({"touch_age": b, "takes": len(g), "contracts": g.contracts.sum(),
                     "net_60s_c": float(np.average(g.net_60s_c.fillna(0), weights=g.contracts)),
                     "net_settle_c": ci.mean, "net_settle_lo_c": ci.lo, "net_settle_hi_c": ci.hi})
    return pd.DataFrame(rows)


def _scan_job(args: tuple) -> ScanResult:
    root, t0, t1, cfg, uni, p, latency, step_ms, clip, warm, seed, thr = args
    return scan(root, t0, t1, cfg, universe=uni, policy=p, latency=latency, step_ms=step_ms, clip_contracts=clip,
                warm=warm, seed=seed, min_threshold_c=thr)


def e8_verdict(summ: pd.DataFrame, decision_c: float = 0.5) -> tuple[str, list[str], int | None]:
    """(rule outcome, policies with results, events) on the decision-threshold scans."""
    if summ is None or not len(summ):
        return "INCONCLUSIVE (no scan results)", [], None
    dec = summ[(summ.threshold_c == decision_c) & summ.policy.isin(["A", "B", "C"])]
    have = sorted(dec.loc[dec["filled"] > 0, "policy"].astype(str))
    rows = {r.policy: r for r in dec.itertuples()}
    reasons, good, bad = [], True, False
    for p in ("B", "C"):
        r = rows.get(p)
        if r is None or not r.filled:
            good = False
            continue
        day = day_block_ok(_DayCI(getattr(r, "net_settle_day_lo_c", math.nan), int(getattr(r, "day_blocks", 0) or 0)))
        ok = r.net_settle_lo_c > 0 and r.net_settle_c > 0.5 and r.opportunities_per_day >= 20 and day is not False
        good &= bool(ok)
        bad |= bool((math.isfinite(r.net_settle_hi_c) and r.net_settle_hi_c < 0.5) or r.opportunities_per_day < 20)
        reasons.append(f"{p}: net {r.net_settle_c:.2f}c [{r.net_settle_lo_c:.2f}, {r.net_settle_hi_c:.2f}], "
                       f"{r.opportunities_per_day:.0f} opp/day")
    ev = [int(r.events) for r in dec.itertuples() if r.policy in ("B", "C") and r.filled]
    events = min(ev) if ev else None
    detail = "; ".join(reasons) or "no fills under B/C"
    if good:
        return f"ACCEPT ({detail})", have, events
    if bad:
        return f"REJECT (taking stays disabled: {detail})", have, events
    return f"INCONCLUSIVE (taking stays disabled: {detail})", have, events


class _DayCI:
    def __init__(self, lo: float, clusters: int) -> None:
        self.lo, self.clusters = lo, clusters


def run(root: str | Path, t0: int, t1: int, out: str | Path, *, cfg: StrategyConfig | None = None,
        policies=("B", "C"), latency: LatencyModel | None = None, step_ms: int = 250, clip_contracts: float = 10.0,
        warm: str = "recorded", universe: Universe | None = None, seed: int = 3, n_jobs: int = 1,
        thresholds_c: Sequence[float] = THRESHOLDS_C) -> dict[str, Any]:
    """One scan per (policy, threshold): each threshold is its own taking policy (audit M2); the
    verdict uses the 0.5c scans under B and C."""
    Path(out).mkdir(parents=True, exist_ok=True)
    uni = universe or build_universe(root, t0, t1)
    lat = research_latency(uni, latency, seed=seed)
    ths = sorted(set(float(x) for x in thresholds_c) | {0.5})
    jobs = [(str(root), t0, t1, cfg, uni, p, lat, step_ms, clip_contracts, warm, seed, thr) for p in policies for thr in ths]
    if n_jobs > 1 and len(jobs) > 1:
        import multiprocessing as mp
        from concurrent.futures import ProcessPoolExecutor

        with ProcessPoolExecutor(max_workers=min(n_jobs, len(jobs)), mp_context=mp.get_context("fork")) as ex:
            results = list(ex.map(_scan_job, jobs))
    else:
        results = [_scan_job(j) for j in jobs]
    tables, res_by, regs = [], {}, []
    stale = pd.DataFrame()
    for job, r in zip(jobs, results):
        p, thr = policy_letter(job[5]), job[11]
        res_by[(p, thr)] = r
        s = summarize(r, thresholds_c=(thr,))
        s.insert(0, "policy", p)
        tk = r.takes[r.takes["contracts"] > 0] if len(r.takes) else r.takes
        if len(tk) and "net_settle_c" in tk:
            ok = tk[tk["net_settle_c"].notna()]
            dd = cluster_mean_ci(ok["net_settle_c"], ok["contracts"], with_day(ok)["day"], 300) if len(ok) else None
            s["net_settle_day_lo_c"] = dd.lo if dd else math.nan
            s["day_blocks"] = dd.clusters if dd else 0
            if thr == 0.5 and len(ok):
                rt = regime_table(add_regimes(ok).assign(policy=p), "net_settle_c", n_boot=200)
                if len(rt):
                    regs.append(rt)
        tables.append(s)
        if p == "B" and thr == 0.5:
            stale = by_staleness(r)
    summ = pd.concat(tables, ignore_index=True) if tables else pd.DataFrame()
    verdict, have, events = e8_verdict(summ)
    any_r = next(iter(res_by.values()), None)
    rep = Report("e8_taker", "E8 — Selective taking of stale Kalshi quotes", Path(out), synthetic=uni.synthetic,
                 rule=RULE_E8, meta={"root": str(root), "window": f"{fmt_ns(t0)} .. {fmt_ns(t1)}",
                                     "decision_grid_ms": step_ms, "clip_contracts": clip_contracts,
                                     "thresholds (one scan each)": ", ".join(f"{x:g}c" for x in ths),
                                     "fee": "exact per-order fee incl. balance rounding (OrderFeeAccumulator)",
                                     "latency": describe_latency(lat, uni),
                                     "fv_warm": any_r.info.get("fv_warm") if any_r else "",
                                     **inputs_meta(uni, t0)[0]})
    rep.verdict = verdict
    rep.policies = have
    rep.decision_events = events
    rep.in_sample, rep.in_sample_why = inputs_status(uni, t0)
    for w in inputs_meta(uni, t0)[1]:
        rep.line(f"WARNING: {w}")
    if any(not r.info.get("fv_ready_at_t0", True) for r in res_by.values()):
        rep.line("WARNING: fair-value model NOT warm at t0 (no opportunities are evaluated until it is): start t0 "
                 "later, record CF history before t0, or use --warm recorded+gbm / csv:<prices>")
    rep.table("summary", summ, "One scan per edge threshold (c/contract after the exact per-order taker fee, "
                               "band-conservative): opportunities (episodes), fills after latency, net P&L to the 60 s "
                               "fair value and to settlement (settlement-event CI; day blocks as a second check).")
    rep.table("by_touch_staleness", stale, "Policy B fills of the 0.5c scan by the age of the Kalshi touch at decision time.")
    rep.table("regimes", pd.concat(regs, ignore_index=True) if regs else pd.DataFrame(),
              "0.5c scans: net c/contract to settlement by regime (tau bucket, vol tercile, weekday).")
    for (p, thr), r in res_by.items():
        if len(r.takes) and thr == 0.5:
            write_csv(r.takes, Path(out) / f"e8_taker_takes_{p}.csv", uni.synthetic)
    rep.write()
    return {"summary": summ, "staleness": stale, "results": res_by, "verdict": rep.final_verdict(), "rule_outcome": verdict}
