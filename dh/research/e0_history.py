"""Experiment 0 (M1.1) at scale on the downloaded history (scripts/download_kalshi_history.py).

    python -m dh.research.e0_history --history data/external/kalshi --out docs/research/tables/e0

Same statistic as ``dh.research.exp0_maker_pnl`` (every public print is a maker fill; maker P&L
to settlement per contract, net of the exact per-order maker fee of the series' fee type at the
account's balance precision; event-clustered CIs; Holm across every segment examined), made to
run on tens of millions of prints: trades are processed event by event and reduced to cells
(series, tau bucket, price bucket, |z| bucket, maker side, policy-C flag, settlement cluster)
holding contract-weighted sums. The cluster CI (``exp_common.cluster_mean_ci``) depends only on
per-cluster sums, so the reduction is exact.

Fill policies on a public tape (analogues of the simulator's B / C, see docs/EXECUTION_MODEL.md):
  B  every print is a fill of the maker at that price: the average maker (front and back of the
     queue alike). This is E0's classic statistic.
  C  only the prints a LAST-in-queue maker would get: the last print at a price level before the
     next print on the same side, within ``c_window_ms`` (2 s), trades THROUGH that level (the
     level was exhausted). These are the fills of a maker who joined behind everyone; they are
     more adverse by construction.

Settlement cluster = the expiration time T = close_time (every KXBTC* market closing together
settles on one BRTI average; dh.settlement.convention). Tau = T - trade time. |z| = distance of
the nearest strike from the BRTI print known at the trade (1 Hz prints from the CF history,
lagged 100 ms), in units of the benchmark's expected move to T with a causal 60-minute realized
vol (floored at 20 %/yr; 40 % when no BRTI is available the bucket is left empty).

Decision (docs/BUILD_PLAN.md F, M1.1): keep a segment only if its maker net P&L CI lower bound is
above 0.15c/contract (with >= 200 settlement events) under BOTH B and C; STOP if none. Bounds
reported: per-segment 95 % CI and the simultaneous (Bonferroni over every examined segment of
that policy) bound used with Holm. Everything here is in-sample descriptive statistics over the
whole period. The chronological split selects on the first half and checks those same
candidates in the second half. Neither establishes an untouched holdout or executable edge:
all public-tape results remain exploratory, tradable=false, with explicit cost/provenance
blockers. Current fee snapshots never establish historical fees before their fetch time.
"""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from dh.research.exp0_maker_pnl import (
    EDGE_C,
    MIN_SEGMENT_EVENTS,
    PRICE_BUCKETS,
    TAU_BUCKETS,
    TAU_LABELS,
    Z_BUCKETS,
    default_fee_engine,
    e0_multiplicity,
    maker_fee_per_contract,
)
from dh.research.exp_common import cluster_mean_ci
from dh.research.historical_fees import HistoricalFees
from dh.research.evidence_manifest import input_manifest, manifest_matches

SEC_YR = 365.0 * 24 * 3600
PX_LABELS = [f"{a / 100:g}-{min(b, 10000) / 100:g}c" for a, b in zip(PRICE_BUCKETS[:-1], PRICE_BUCKETS[1:])]
Z_LABELS = [f"{a:g}-{b:g}" if b < 1e8 else f">{a:g}" for a, b in zip(Z_BUCKETS[:-1], Z_BUCKETS[1:])]
CELL_KEYS = ["series", "tau_b", "px_b", "z_b", "maker_side", "c_fill", "cluster"]


# ============================================================================ inputs
def load_brti(root: Path) -> tuple[np.ndarray, np.ndarray]:
    """(seconds, cents) of the on-the-second BRTI prints in brti/hourly (sorted)."""
    files = sorted((root / "brti" / "hourly").glob("*/*.parquet"))
    if not files:
        return np.array([], dtype=np.int64), np.array([], dtype=np.int64)
    parts = []
    for f in files:
        t = pq.read_table(f).to_pandas()
        t = t[t.t_ms % 1000 == 0]
        parts.append(t)
    df = pd.concat(parts, ignore_index=True).drop_duplicates("t_ms").sort_values("t_ms")
    return (df.t_ms.to_numpy() // 1000).astype(np.int64), df.cents.to_numpy().astype(np.int64)


def realized_vol(secs: np.ndarray, cents: np.ndarray, window_min: int = 60) -> tuple[np.ndarray, np.ndarray]:
    """(minute start s, annualized realized vol usable FROM the end of that minute): 1-minute log
    returns over the trailing ``window_min`` minutes (>= 30 returns required)."""
    if not len(secs):
        return np.array([], dtype=np.int64), np.array([])
    s = pd.Series(cents / 100.0, index=pd.to_datetime(secs, unit="s"))
    m = s.resample("1min").last().dropna()
    r = np.log(m).diff()
    vol = r.rolling(window_min, min_periods=30).std() * math.sqrt(SEC_YR / 60.0)
    t = (m.index.astype("int64") // 10**9).to_numpy()
    return t + 60, vol.to_numpy()  # known once the minute has closed


def fee_types(root: Path) -> dict[str, tuple[str, float]]:
    """Series -> (fee_type, multiplier) from the downloaded GET /series/{s}; scheduled historical
    changes (fees/series_fee_changes) are reported by ``fee_notes``."""
    out = {}
    for f in sorted((root / "series").glob("*.json")):
        s = json.loads(f.read_text())
        s = s.get("series", s)
        out[str(s["ticker"])] = (str(s.get("fee_type") or ""), float(s.get("fee_multiplier") if s.get("fee_multiplier") is not None else 1.0))
    return out


def fee_notes(root: Path) -> list[str]:
    notes = []
    for f in sorted((root / "fees" / "series_fee_changes").glob("*.json")):
        arr = json.loads(f.read_text()).get("series_fee_change_arr") or []
        notes.append(f"{f.stem}: {len(arr)} scheduled/historical series fee change(s)"
                     + (": " + "; ".join(f"{c.get('scheduled_ts')} -> {c.get('fee_type')} x{c.get('fee_multiplier')}" for c in arr) if arr else ""))
    p = root / "fees" / "event_fee_changes.parquet"
    if p.is_file():
        n = pq.read_metadata(p).num_rows
        notes.append(f"event fee overrides for these series: {n}")
    return notes


# ============================================================================ per event
def c_flags(t: pd.DataFrame, window_ms: int = 2000) -> np.ndarray:
    """Policy-C proxy: the print is the last one at its level before the NEXT print on the same
    side (same market, same taker side) within ``window_ms`` trades through the level
    (a taker buying YES at a higher price / selling at a lower one). ``t`` sorted by ts_ms and,
    within a millisecond, in sweep order (event_cells)."""
    out = np.zeros(len(t), dtype=bool)
    if not len(t):
        return out
    g = t.groupby(["ticker", "taker_side"], sort=False)
    nxt_px = g["yes_px"].shift(-1).to_numpy()
    nxt_ts = g["ts_ms"].shift(-1).to_numpy()
    px = t["yes_px"].to_numpy()
    ts = t["ts_ms"].to_numpy()
    buy = (t["taker_side"] == "yes").to_numpy()
    ok = np.isfinite(nxt_px) & (nxt_ts - ts <= window_ms)
    out = ok & np.where(buy, nxt_px > px, nxt_px < px)
    return out


def event_cells(trades: pd.DataFrame, markets: pd.DataFrame, series: str, ftypes: dict, fe,
                brti: tuple[np.ndarray, np.ndarray], vol: tuple[np.ndarray, np.ndarray],
                c_window_ms: int = 2000, fee_resolver: HistoricalFees | None = None) -> tuple[pd.DataFrame, dict]:
    """Cells (CELL_KEYS + sums) for one event's trades."""
    st = {"trades": 0, "blocks": 0, "no_market": 0, "unknown_fee_trades": 0}
    if not len(trades):
        return pd.DataFrame(), st
    t = trades.rename(columns={"taker_outcome_side": "taker_side"})
    st["blocks"] = int(t["is_block_trade"].sum()) if "is_block_trade" in t else 0
    if "is_block_trade" in t:
        t = t[~t["is_block_trade"].astype(bool)]
    t = t[t.taker_side.isin(["yes", "no"])]
    cols = ["ticker", "result", "strike_type", "floor_strike", "cap_strike", "close_ts_ms"]
    cols += [c for c in ("event_ticker",) if c in markets]
    mk = markets[markets.result.isin(["yes", "no"])][cols]
    t = t[["ticker", "ts_ms", "yes_px", "qty", "taker_side"]].merge(mk, on="ticker", how="inner")
    st["no_market"] = int(len(trades) - st["blocks"] - len(t))
    if not len(t):
        return pd.DataFrame(), st
    # within one millisecond a sweep walks the book: buys at ascending, sells at descending YES
    # prices (the tape carries no sub-ms order), which the policy-C flag relies on
    t["_po"] = np.where(t.taker_side == "yes", t.yes_px, -t.yes_px)
    t = t.sort_values(["ts_ms", "ticker", "taker_side", "_po"], kind="stable").drop(columns="_po").reset_index(drop=True)
    st["trades"] = len(t)
    p = t.yes_px.to_numpy() / 1e4
    settle = (t.result == "yes").to_numpy().astype(float)
    buy = (t.taker_side == "yes").to_numpy()
    t["maker_side"] = np.where(buy, "sold_yes", "bought_yes")
    gross = np.where(buy, p - settle, settle - p)
    if fee_resolver is not None:
        st["unknown_fee_trades"] = int((~fee_resolver.apply(t, series, ftypes)).sum())
    fee = maker_fee_per_contract(t, ftypes, fe)
    contracts = t.qty.to_numpy() / 100.0
    tau = (t.close_ts_ms.to_numpy() - t.ts_ms.to_numpy()) / 1000.0
    t["tau_b"] = pd.cut(tau, TAU_BUCKETS, labels=TAU_LABELS, right=False).astype(str)
    t["px_b"] = pd.cut(t.yes_px, PRICE_BUCKETS, labels=PX_LABELS, right=False).astype(str)
    secs, cents = brti
    zb = np.full(len(t), "n/a", dtype=object)
    if len(secs):
        avail = (t.ts_ms.to_numpy() - 100) // 1000
        i = np.searchsorted(secs, avail, side="right") - 1
        okS = (i >= 0) & (avail - secs[np.clip(i, 0, None)] <= 60)
        S = np.where(okS, cents[np.clip(i, 0, None)] / 100.0, np.nan)
        vt, vv = vol
        j = np.searchsorted(vt, t.ts_ms.to_numpy() // 1000, side="right") - 1
        sig = np.where(j >= 0, vv[np.clip(j, 0, None)], np.nan) if len(vt) else np.full(len(t), np.nan)
        sig = np.where(np.isfinite(sig), np.maximum(sig, 0.20), 0.40)
        F = t.floor_strike.to_numpy(dtype=float)
        C = t.cap_strike.to_numpy(dtype=float)
        d = np.fmin(np.abs(F - S), np.abs(C - S))  # nearest strike (fmin ignores a missing side)
        sd = S * sig * np.sqrt(np.maximum(tau - 40.0, 20.0) / SEC_YR)
        z = d / sd
        lab = pd.cut(z, Z_BUCKETS, labels=Z_LABELS, right=False).astype(str)
        zb = np.where(np.isfinite(z), lab, "n/a")
    t["z_b"] = zb
    t["c_fill"] = c_flags(t, c_window_ms)
    t["series"] = series
    t["cluster"] = t.close_ts_ms.astype("int64")
    t["w"] = contracts
    t["g"] = contracts * gross
    t["n"] = contracts * (gross - fee)
    t["f"] = contracts * fee
    t["k"] = 1
    cells = t.groupby(CELL_KEYS, observed=True, sort=False)[["w", "g", "n", "f", "k"]].sum().reset_index()
    return cells, st


# ============================================================================ tables
def seg_table(cells: pd.DataFrame, by: list[str], n_boot: int, min_events: int) -> pd.DataFrame:
    rows = []
    for key, g in cells.groupby(by, observed=True):
        cl = g.groupby("cluster")[["w", "g", "n", "f", "k"]].sum()
        n_ev = len(cl)
        if n_ev < min_events or cl.w.sum() <= 0:
            continue
        gross = cluster_mean_ci(cl.g / cl.w, cl.w, cl.index.to_numpy(), n_boot)
        net = cluster_mean_ci(cl.n / cl.w, cl.w, cl.index.to_numpy(), n_boot)
        key = key if isinstance(key, tuple) else (key,)
        rows.append({**dict(zip(by, key)), "contracts": float(cl.w.sum()), "trades": int(cl.k.sum()), "events": n_ev,
                     "maker_gross_c": 100 * gross.mean, "gross_lo_c": 100 * gross.lo, "gross_hi_c": 100 * gross.hi,
                     "maker_fee_c": 100 * float(cl.f.sum() / cl.w.sum()),
                     "maker_net_c": 100 * net.mean, "net_lo_unadj_c": 100 * net.lo, "net_hi_unadj_c": 100 * net.hi,
                     "net_se_c": 100 * net.se, "p_edge": net.p_greater(EDGE_C / 100.0)})
    return pd.DataFrame(rows)


TABLES = {
    "series": ["series"],
    "series_tau": ["series", "tau_b"],
    "series_price": ["series", "px_b"],
    "series_tau_price": ["series", "tau_b", "px_b"],
    "series_tau_z": ["series", "tau_b", "z_b"],
    "series_price_side": ["series", "px_b", "maker_side"],
}


def policy_tables(cells: pd.DataFrame, n_boot: int, min_events: int) -> dict[str, pd.DataFrame]:
    tabs = {k: seg_table(cells if "z_b" not in by else cells[cells.z_b != "n/a"], by, n_boot, min_events)
            for k, by in TABLES.items()}
    return e0_multiplicity({k: v for k, v in tabs.items()})


def decision(tb: dict[str, pd.DataFrame], tc: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Segments whose simultaneous net lower bound > 0.15c under BOTH B and C (M1.1 keep)."""
    rows = []
    for k, by in TABLES.items():
        b, c = tb.get(k), tc.get(k)
        if b is None or c is None or not len(b) or not len(c):
            continue
        m = b.merge(c, on=by, suffixes=("_B", "_C"))
        for r in m.itertuples(index=False):
            d = r._asdict()
            rows.append({"table": k, "segment": " / ".join(str(d[x]) for x in by),
                         "events_B": d["events_B"], "net_B_c": d["maker_net_c_B"], "net_lo_B_c": d["net_lo_c_B"],
                         "net_lo_unadj_B_c": d["net_lo_unadj_c_B"],
                         "events_C": d["events_C"], "net_C_c": d["maker_net_c_C"], "net_lo_C_c": d["net_lo_c_C"],
                         "net_lo_unadj_C_c": d["net_lo_unadj_c_C"],
                         "keep": bool(d["net_lo_c_B"] > EDGE_C and d["net_lo_c_C"] > EDGE_C),
                         "keep_unadjusted": bool(d["net_lo_unadj_c_B"] > EDGE_C and d["net_lo_unadj_c_C"] > EDGE_C)})
    return pd.DataFrame(rows)


def confirmed_candidates(train: pd.DataFrame, later: pd.DataFrame) -> list[dict]:
    """A later-sample winner must have been selected before that sample was examined."""
    if not len(train) or not len(later):
        return []
    selected = train.loc[train.keep, ["table", "segment"]]
    return selected.merge(later[later.keep], on=["table", "segment"]).to_dict("records")


# ============================================================================ driver
def build_cells(root: Path, series_list: list[str], start_ms: int | None = None, end_ms: int | None = None,
                c_window_ms: int = 2000, log=print) -> tuple[pd.DataFrame, dict]:
    ftypes = fee_types(root)
    fee_resolver = HistoricalFees(root)
    fe = default_fee_engine()
    brti = load_brti(root)
    vol = realized_vol(*brti)
    log(f"BRTI prints: {len(brti[0])} seconds" + (f" {pd.Timestamp(brti[0][0], unit='s')} .. {pd.Timestamp(brti[0][-1], unit='s')}" if len(brti[0]) else ""))
    allc = []
    st_all = {"events": 0, "trades": 0, "blocks": 0, "no_market": 0, "unknown_fee_trades": 0, "fee_types": {k: list(v) for k, v in ftypes.items()}}
    for s in series_list:
        tdir = root / "trades" / f"series={s}"
        files = sorted(tdir.glob("*.parquet"))
        log(f"{s}: {len(files)} event files")
        for i, f in enumerate(files):
            mpath = root / "markets" / f"series={s}" / f.name
            if not mpath.is_file():
                continue
            mk = pq.read_table(mpath, columns=["ticker", "event_ticker", "result", "strike_type", "floor_strike", "cap_strike", "close_ts_ms"]).to_pandas()
            if not len(mk):
                continue
            T = int(mk.close_ts_ms.max())
            if (start_ms is not None and T < start_ms) or (end_ms is not None and T >= end_ms):
                continue
            tr = pq.read_table(f, columns=["ticker", "ts_ms", "yes_px", "qty", "taker_outcome_side", "is_block_trade"]).to_pandas()
            cells, st = event_cells(tr, mk, s, ftypes, fe, brti, vol, c_window_ms, fee_resolver)
            for k in ("trades", "blocks", "no_market", "unknown_fee_trades"):
                st_all[k] += st[k]
            st_all["events"] += 1
            if len(cells):
                allc.append(cells)
            if (i + 1) % 200 == 0:
                log(f"  {s}: {i + 1}/{len(files)} events, {st_all['trades']} prints so far")
    cells = pd.concat(allc, ignore_index=True) if allc else pd.DataFrame(columns=CELL_KEYS + ["w", "g", "n", "f", "k"])
    return cells, st_all


def run(root: Path, out: Path, series_list: list[str], n_boot: int = 300, min_events: int = MIN_SEGMENT_EVENTS,
        c_window_ms: int = 2000, split: bool = True, log=print) -> dict:
    out.mkdir(parents=True, exist_ok=True)
    manifest = input_manifest(root)
    cells, st = build_cells(root, series_list, c_window_ms=c_window_ms, log=log)
    cells.to_parquet(out / "e0_cells.parquet")
    res = {"stats": st, "fee_notes": fee_notes(root), "input_manifest": manifest,
           "tradable": False, "feature_definition": "descriptive spot/realized-vol proxy; not production z",
           "fee_history_complete": st.get("unknown_fee_trades", 0) == 0,
           "promotion_status": "exploratory_only", "confirmation_candidates": [],
           "run_parameters": {"n_boot": n_boot, "min_events": min_events, "c_window_ms": c_window_ms,
                              "split": split, "series": series_list}}
    if not len(cells):
        res["verdict"] = "INCONCLUSIVE (no data)"
        (out / "e0_summary.json").write_text(json.dumps(res, indent=1, default=str))
        return res
    cl = cells.cluster.to_numpy()
    res["period_utc"] = [str(pd.Timestamp(int(cl.min()), unit="ms")), str(pd.Timestamp(int(cl.max()), unit="ms"))]
    res["events_by_series"] = {s: int(g.cluster.nunique()) for s, g in cells.groupby("series")}
    res["prints_by_series"] = {s: int(g.k.sum()) for s, g in cells.groupby("series")}
    res["contracts_by_series"] = {s: float(g.w.sum()) for s, g in cells.groupby("series")}
    tb = policy_tables(cells, n_boot, min_events)
    tc = policy_tables(cells[cells.c_fill], n_boot, min_events)
    for name, tabs in (("B", tb), ("C", tc)):
        for k, t in tabs.items():
            t.to_csv(out / f"e0_{name}_{k}.csv", index=False, float_format="%.4f")
    dec = decision(tb, tc)
    dec.to_csv(out / "e0_decision.csv", index=False, float_format="%.4f")
    res["segments_examined"] = {"B": int(sum(len(t) for t in tb.values())), "C": int(sum(len(t) for t in tc.values()))}
    res["keep"] = dec[dec.keep].to_dict("records") if len(dec) else []
    res["keep_unadjusted"] = dec[dec.keep_unadjusted].to_dict("records") if len(dec) else []
    res["verdict"] = ("EXPLORATORY CANDIDATES: " + ", ".join(f"{r['table']}:{r['segment']}" for r in res["keep"])) if res["keep"] else \
        "STOP: no segment has a net lower bound > 0.15c/contract under both B and C"
    if split and len(cells):
        mid = int(np.median(np.unique(cl)))
        halves = {}
        for name, sub in (("first", cells[cells.cluster <= mid]), ("second", cells[cells.cluster > mid])):
            halves[name] = {"B": policy_tables(sub, n_boot, min_events),
                            "C": policy_tables(sub[sub.c_fill], n_boot, min_events)}
            for pol in ("B", "C"):
                for k, t in halves[name][pol].items():
                    t.to_csv(out / f"e0_{name}_{pol}_{k}.csv", index=False, float_format="%.4f")
        res["split_at_utc"] = str(pd.Timestamp(mid, unit="ms"))
        train = decision(halves["first"]["B"], halves["first"]["C"])
        later = decision(halves["second"]["B"], halves["second"]["C"])
        res["confirmation_candidates"] = confirmed_candidates(train, later)
    res["inputs_unchanged"] = manifest_matches(root, manifest)
    res["promotion_blockers"] = ["public tape proxies do not establish executable strategy profit",
                                  "no preregistered locked policy/untouched holdout attestation"]
    if not res["fee_history_complete"]:
        res["promotion_blockers"].append("historical fees unknown for some trades; net figures are estimates")
    if not res["inputs_unchanged"]:
        res["promotion_blockers"].append("inputs changed during run; rerun on an immutable snapshot")
    (out / "e0_summary.json").write_text(json.dumps(res, indent=1, default=str))
    return res


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--history", default="data/external/kalshi")
    ap.add_argument("--out", default="data/results/e0")
    ap.add_argument("--series", nargs="+", default=["KXBTCD", "KXBTC", "KXBTC15M"])
    ap.add_argument("--n-boot", type=int, default=300)
    ap.add_argument("--min-events", type=int, default=MIN_SEGMENT_EVENTS)
    ap.add_argument("--c-window-ms", type=int, default=2000)
    a = ap.parse_args(argv)
    res = run(Path(a.history), Path(a.out), a.series, a.n_boot, a.min_events, a.c_window_ms)
    print(json.dumps({k: v for k, v in res.items() if k not in ("keep", "keep_unadjusted")}, indent=1, default=str))
    print("keep:", len(res.get("keep", [])), "keep_unadjusted:", len(res.get("keep_unadjusted", [])))


if __name__ == "__main__":
    main()


# ============================================================================ report
def _fmt_ci(m: float, lo: float, hi: float) -> str:
    return f"{m:+.2f} [{lo:+.2f}, {hi:+.2f}]"


def markdown_tables(out: Path, tables: tuple[str, ...] = ("series", "series_tau", "series_price", "series_tau_price",
                                                          "series_tau_z")) -> str:
    """Markdown: per segment, events and net c/contract [per-segment 95 % CI] under B and C, plus
    the simultaneous lower bound used for the M1.1 decision."""
    parts = []
    for k in tables:
        by = TABLES[k]
        b = pd.read_csv(out / f"e0_B_{k}.csv") if (out / f"e0_B_{k}.csv").stat().st_size > 1 else pd.DataFrame()
        c = pd.read_csv(out / f"e0_C_{k}.csv") if (out / f"e0_C_{k}.csv").stat().st_size > 1 else pd.DataFrame()
        if not len(b):
            continue
        m = b.merge(c, on=by, how="left", suffixes=("_B", "_C"))
        order = {"tau_b": TAU_LABELS, "px_b": PX_LABELS, "z_b": Z_LABELS + ["n/a"]}
        keys = [m[x].map({v: i for i, v in enumerate(order[x])}) if x in order else m[x] for x in by]
        m = m.assign(**{f"_k{i}": k for i, k in enumerate(keys)}).sort_values([f"_k{i}" for i in range(len(keys))])
        lines = [f"**{k}** ({len(m)} segments with >= {MIN_SEGMENT_EVENTS} settlement events under B)", "",
                 "| " + " | ".join(by) + " | events B | prints B | gross B c | net B c [95% CI] | simult. lo B | events C | net C c [95% CI] | simult. lo C |",
                 "|" + "---|" * (len(by) + 8)]
        for r in m.itertuples(index=False):
            d = r._asdict()
            cB = _fmt_ci(d["maker_net_c_B"], d["net_lo_unadj_c_B"], d["net_hi_unadj_c_B"])
            if pd.notna(d.get("maker_net_c_C")):
                cC = _fmt_ci(d["maker_net_c_C"], d["net_lo_unadj_c_C"], d["net_hi_unadj_c_C"])
                evC, loC = f"{int(d['events_C'])}", f"{d['net_lo_c_C']:+.2f}"
            else:
                cC, evC, loC = "n/a (< 200 events)", "", ""
            lines.append("| " + " | ".join(str(d[x]) for x in by) +
                         f" | {int(d['events_B'])} | {int(d['trades_B']):,} | {d['maker_gross_c_B']:+.2f} | {cB} | {d['net_lo_c_B']:+.2f} | {evC} | {cC} | {loC} |")
        parts.append("\n".join(lines))
    return "\n\n".join(parts)
