"""Experiment 8: is selectively TAKING stale Kalshi quotes profitable after the taker fee?

Scan (causal, on a ``step_ms`` decision grid of receive time) every quotable market: with the
strategy's own fair-value band (FvProbe = MarketMaker's nowcast, vol, tails, settlement window
and band), an opportunity exists when

    buy YES at best ask a :  F_lo - a - taker_fee(a) >= threshold
    sell YES at best bid b:  b - F_hi - taker_fee(b) >= threshold

taker_fee from the market's exact FeeSchedule (dh.kalshi.fees; series fee type/multiplier as
recorded). An episode = consecutive grid points where the condition holds for a market side; the
first grid point of an episode sends ONE marketable order (limit = the touch seen, size = min(clip,
displayed touch qty)). It reaches the book after a sampled submit latency (LatencyModel; policy C
scales latency x1.5) and executes at exactly that time, after every recorded event up to it: the
fill walks the book AS RECORDED at arrival up to the limit (queue-empty check: the stale quote may
be gone), consuming displayed size once (our earlier takes are remembered per level, capped by the
displayed size, as in KalshiExchangeSim). Fill P&L per contract, net of the taker fee:
markout to the fair value 60 s later (F centre) and to settlement (recorded result).

Also reported: opportunities/day by threshold, fill rate after latency, P&L by staleness of the
Kalshi touch (time since it last changed) and by the preceding external move.

Decision rule (docs/TEST_MATRIX.md E8): accept if net > 0.5c/contract after fees with CI > 0 and
>= 20 opportunities/day; otherwise taking stays disabled.
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
from dh.research.exp_common import Report, cluster_mean_ci, fmt_ns, policy_letter, write_csv
from dh.research.replay_env import Universe, build_universe, inputs_meta, prime_probe, probe_for_window
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
    lat = (latency or LatencyModel(seed)).fork(seed, (latency.multiplier if latency else 1.0)
                                                 * (1.5 if policy_letter(policy) == "C" else 1.0))
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

    def fee_c(t: str, px: int) -> float:
        s = sched.get(t)
        if s is None:
            spec = mm.specs[t]
            s = sched[t] = fees.schedule_for_spec(spec.fee_type, spec.fee_multiplier)
        return 100.0 * s.expected_fee_per_contract(px, True) if s.supported else math.inf

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
        for px, q in levels:
            avail = q - consumed.get((tk.ticker, book, px), 0)
            if avail <= 0:
                continue
            take = min(avail, left)
            yes_px = PX_SCALE - px if book == "no" else px
            consumed[(tk.ticker, book, px)] = consumed.get((tk.ticker, book, px), 0) + take
            tk.fill_qty += take
            tk.fill_cost += yes_px / PX_SCALE * take / QTY_SCALE
            tk.fee += (s.trade_fee_micros(yes_px, take, True) / 1e6) if s is not None else 0.0
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
                edge = (100.0 * (lo - px / PX_SCALE) if side > 0 else 100.0 * (px / PX_SCALE - hi)) - fee_c(t, px)
                if edge < min_threshold_c:
                    in_episode[key] = False
                    continue
                if in_episode.get(key):
                    continue
                in_episode[key] = True
                disp = b.best_ask_qty() if side > 0 else b.best_bid_qty()
                age = (g - touch_ts.get(t, g)) / NS_PER_S
                opps.append({"t": g, "ticker": t, "event": spec.event_ticker, "side": side, "px": px / PX_SCALE,
                             "edge_c": edge, "F": f.F, "tau_s": tau, "touch_age_s": age, "ext_move_1s": ext_move,
                             "displayed_ct": disp / QTY_SCALE})
                tk = _Take(g, g + lat.submit_ns() + lat.md_offset_ns, t, spec.event_ticker, side, px,
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
    return ScanResult(pd.DataFrame(opps), pd.DataFrame(rows), days, uni.synthetic, grid_points,
                      {"fv_warm": winfo.source, "fv_ready_at_t0": winfo.ready, "policy": policy_letter(policy)})


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
               "contracts": float(filled["contracts"].sum()) if len(filled) else 0.0}
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
    root, t0, t1, cfg, uni, p, latency, step_ms, clip, warm, seed = args
    return scan(root, t0, t1, cfg, universe=uni, policy=p, latency=latency, step_ms=step_ms, clip_contracts=clip,
                warm=warm, seed=seed)


def run(root: str | Path, t0: int, t1: int, out: str | Path, *, cfg: StrategyConfig | None = None,
        policies=("B", "C"), latency: LatencyModel | None = None, step_ms: int = 250, clip_contracts: float = 10.0,
        warm: str = "recorded", universe: Universe | None = None, seed: int = 3, n_jobs: int = 1) -> dict[str, Any]:
    Path(out).mkdir(parents=True, exist_ok=True)
    uni = universe or build_universe(root, t0, t1)
    tables = []
    stale = pd.DataFrame()
    res_by = {}
    jobs = [(str(root), t0, t1, cfg, uni, p, latency, step_ms, clip_contracts, warm, seed) for p in policies]
    if n_jobs > 1 and len(jobs) > 1:
        import multiprocessing as mp
        from concurrent.futures import ProcessPoolExecutor

        with ProcessPoolExecutor(max_workers=min(n_jobs, len(jobs)), mp_context=mp.get_context("fork")) as ex:
            results = list(ex.map(_scan_job, jobs))
    else:
        results = [_scan_job(j) for j in jobs]
    for p, r in zip(policies, results):
        res_by[policy_letter(p)] = r
        s = summarize(r)
        s.insert(0, "policy", policy_letter(p))
        tables.append(s)
        if policy_letter(p) == "B":
            stale = by_staleness(r)
    summ = pd.concat(tables, ignore_index=True) if tables else pd.DataFrame()
    rep = Report("e8_taker", "E8 — Selective taking of stale Kalshi quotes", Path(out), synthetic=uni.synthetic,
                 rule=RULE_E8, meta={"root": str(root), "window": f"{fmt_ns(t0)} .. {fmt_ns(t1)}",
                                     "decision_grid_ms": step_ms, "clip_contracts": clip_contracts,
                                     "fv_warm": next(iter(res_by.values())).info.get("fv_warm") if res_by else "",
                                     **inputs_meta(uni, t0)[0]})
    ok_all = True
    reasons = []
    for p in ("B", "C"):
        row = summ[(summ.policy == p) & (summ.threshold_c == 0.5)] if len(summ) else summ
        if not len(row):
            ok_all = False
            continue
        r = row.iloc[0]
        good = r.net_settle_lo_c > 0 and r.net_settle_c > 0.5 and r.opportunities_per_day >= 20
        ok_all &= bool(good)
        reasons.append(f"{p}: net {r.net_settle_c:.2f}c [{r.net_settle_lo_c:.2f}, {r.net_settle_hi_c:.2f}], "
                       f"{r.opportunities_per_day:.0f} opp/day")
    rep.verdict = ("ACCEPT" if ok_all else "REJECT/INCONCLUSIVE (taking stays disabled)") + " — " + "; ".join(reasons)
    for w in inputs_meta(uni, t0)[1]:
        rep.line(f"WARNING: {w}")
    if any(not r.info.get("fv_ready_at_t0", True) for r in res_by.values()):
        rep.line("WARNING: fair-value model NOT warm at t0 (no opportunities are evaluated until it is): start t0 "
                 "later, record CF history before t0, or use --warm recorded+gbm / csv:<prices>")
    rep.table("summary", summ, "Per edge threshold (c/contract after the taker fee, band-conservative): "
                               "opportunities (episodes), fills after latency, net P&L to the 60 s fair value and "
                               "to settlement (event-bootstrap CI).")
    rep.table("by_touch_staleness", stale, "Policy B fills by the age of the Kalshi touch at decision time.")
    for p, r in res_by.items():
        if len(r.takes):
            write_csv(r.takes, Path(out) / f"e8_taker_takes_{p}.csv", uni.synthetic)
    rep.write()
    return {"summary": summ, "staleness": stale, "results": res_by}
