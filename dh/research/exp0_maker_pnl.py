"""Experiment 0: where do makers earn on Kalshi BTC markets? (public trades vs settlement)

For every public trade, the maker's gross P&L to settlement per contract is

    taker bought YES at p  (maker sold YES):   p - settle
    taker sold YES at p    (maker bought YES): settle - p          settle in {0, 1}

Net of the maker fee (only for series whose fee type charges makers):
    maker_net = maker_gross - maker_rate * p * (1 - p)

Segments: YES price bucket, maker side, time to expiry, normalized strike distance (needs a
BTC reference series), hour of day, weekday/weekend, trade size. All trades of one EXPIRATION share
one settlement average (every KXBTC* series expiring at the same time settles on the same BRTI
60-print average), so inference clusters by expiration time (audit M5); CIs are the hull of a
studentized cluster bootstrap and a jackknife-t interval (exp_common).

Maker fee (audit M3): the exact Kalshi fee of the maker's order per print, by the series' fee type
(``fee_types``: series -> fee_type[:multiplier]; a markets ``fee_type`` column wins), including the
balance rounding (dh.kalshi.fees.FeeSchedule.single_fill_fees: an order pays its exact fee rounded
up to the cent). Each print is priced as one complete maker order: an UPPER bound on the fee of a
maker whose order filled in several prints (the per-order carry rebates part of the rounding).

Verdict (audit C5, docs/TEST_MATRIX.md E0): only segments with >= 200 settlement events are
examined; each tests H0 "net <= 0.15c" (one-sided jackknife-t p-value) with Holm across EVERY
segment of every table (family-wise 2.5 %). ACCEPT if some segment rejects; REJECT if every
examined segment's simultaneous (Bonferroni) upper bound is below 0.15c; else INCONCLUSIVE.
``net_lo_c`` / ``net_hi_c`` in the tables are those simultaneous bounds; ``net_lo_unadj_c`` /
``net_hi_unadj_c`` the per-segment 95 % CI.

This is an UNCONDITIONAL average over all makers (front-of-queue professionals and slow
back-of-queue makers alike). It bounds where edge can exist; it is not our fill-conditioned
P&L (Experiments 3/4).

Inputs (Parquet or DataFrames):
  trades:  ticker, ts_ms, yes_px (int, 1e-4 $), qty (int, 0.01 contracts), taker_side ('yes'|'no')
  markets: ticker, event_ticker, expiration_ts_ms, result ('yes'|'no'), strike_type,
           floor_strike, cap_strike
  btc (optional): ts_ms, price  (any BRTI proxy; used for z = distance / expected move). ts_ms is
           the bar OPEN time of --btc-bar-ms bars (default 60 000: 1-minute OHLC closes); the
           join is causal (the bar is used only after it closed; kalshi_data.btc_price_asof)
"""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

from dh.research.exp_common import Report, cluster_mean_ci, holm
from dh.research.kalshi_data import DEFAULT_BTC_BAR_MS, btc_price_asof, normalize_markets, normalize_trades

PRICE_BUCKETS = [0, 500, 1000, 2000, 3500, 5000, 6500, 8000, 9000, 9500, 10001]  # px units
TAU_BUCKETS = [0, 30, 60, 300, 600, 1800, 3600, 1e9]  # seconds
TAU_LABELS = ["<30s", "30-60s", "1-5m", "5-10m", "10-30m", "30-60m", ">60m"]
Z_BUCKETS = [0, 0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 1e9]
MIN_SEGMENT_EVENTS = 200  # TEST_MATRIX E0
EDGE_C = 0.15  # net c/contract a segment must beat
ALPHA = 0.025  # family-wise, one-sided
RATE_TO_TYPE = {0.0: "quadratic", 0.0175: "quadratic_with_maker_fees", 0.035: "quadratic_with_combo_maker_fees"}


def parse_fee_types(items) -> dict[str, tuple[str, float]]:
    """['KXBTCD=quadratic_with_maker_fees', 'KXBTC=quadratic:1'] -> {series: (fee_type, multiplier)};
    series '*' is the default."""
    out: dict[str, tuple[str, float]] = {}
    for it in items or ():
        k, _, v = str(it).partition("=")
        ft, _, m = v.partition(":")
        out[k.strip()] = (ft.strip(), float(m) if m else 1.0)
    return out


def default_fee_engine():
    """The account's fee engine: config/fees.yaml rates with ``fees.balance_precision_dollars`` from
    config/kalshi.yaml ($0.0001, verified on 40/40 account fills by scripts/verify_fee_schedule.py);
    falls back to config/fees.yaml alone."""
    try:
        from dh.kalshi.config import load_config

        return load_config(None).fee_engine()
    except Exception:  # noqa: BLE001 - research fallback (no kalshi config present)
        from dh.kalshi.fees import FeeEngine

        return FeeEngine.from_config()


def maker_fee_per_contract(df: pd.DataFrame, fee_types: dict[str, tuple[str, float]], fee_engine=None) -> np.ndarray:
    """Exact maker fee per contract (dollars) of each print priced as one maker order (module doc).

    Computed once per distinct (fee type, multiplier, price, size, side); series whose fee type
    charges no maker fee (``quadratic``: KXBTCD, KXBTC and KXBTC15M on 2026-09-25) cost 0."""
    fe = fee_engine or default_fee_engine()
    out = np.zeros(len(df))
    if not len(df):
        return out
    series = df["ticker"].astype(str).str.split("-").str[0].to_numpy()
    ft = np.empty(len(df), dtype=object)
    fm = np.ones(len(df))
    for ser in np.unique(series):
        t, m = fee_types.get(ser) or fee_types.get("*") or ("quadratic_with_maker_fees", 1.0)
        sel = series == ser
        ft[sel] = t
        fm[sel] = m
    if "fee_type" in df:
        col = df["fee_type"].to_numpy()
        has = np.array([isinstance(x, str) and bool(x) for x in col])
        ft[has] = col[has]
        if "fee_multiplier" in df:
            mcol = pd.to_numeric(df["fee_multiplier"], errors="coerce").to_numpy()
            fm[has] = np.where(np.isfinite(mcol[has]), mcol[has], 1.0)
    key = pd.DataFrame({"ft": ft, "fm": fm, "px": df["yes_px"].to_numpy(), "qty": df["qty"].to_numpy(),
                        "side": df["maker_side"].to_numpy()})
    scheds: dict[tuple[str, float], object] = {}
    fees: dict[tuple, float] = {}
    for k in key.drop_duplicates().itertuples(index=False):
        q = int(k.qty)
        if q <= 0:
            continue
        sk = (str(k.ft), float(k.fm))
        sc = scheds.get(sk)
        if sc is None:
            sc = scheds[sk] = fe.schedule_for_spec(*sk)
        net = sc.single_fill_fees(int(k.px), q, False, "bid" if k.side == "bought_yes" else "ask").net_micros  # type: ignore[attr-defined]
        fees[(sk[0], sk[1], int(k.px), q, k.side)] = net / 1e6 / (q / 100.0)
    if fees:
        idx = pd.MultiIndex.from_frame(key.assign(ft=key.ft.astype(str), fm=key.fm.astype(float),
                                                  px=key.px.astype(int), qty=key.qty.astype(int)))
        ser_f = pd.Series(fees)
        ser_f.index = pd.MultiIndex.from_tuples(list(fees.keys()))
        out = ser_f.reindex(idx).fillna(0.0).to_numpy(dtype=float)
    return out


def prepare(trades: pd.DataFrame, markets: pd.DataFrame, btc: pd.DataFrame | None = None,
            vol_ann: float = 0.40, maker_rate: float | None = None, btc_bar_ms: int = DEFAULT_BTC_BAR_MS,
            fee_types: dict[str, tuple[str, float]] | None = None) -> pd.DataFrame:
    """Join trades to settlements and compute per-trade maker P&L columns (dollars/contract).

    Accepts the downloader's schema (see dh.research.kalshi_data); block trades are dropped.
    The BTC reference for z is joined causally (kalshi_data.btc_price_asof): ``btc.ts_ms`` is a
    bar OPEN time and the bar's price is usable only from its close (``btc_bar_ms`` later; 0 for
    point-in-time prices). Fees: ``fee_types`` (series -> (fee_type, multiplier)); the legacy
    ``maker_rate`` maps to a fee type (0 -> quadratic, 0.0175 -> quadratic_with_maker_fees,
    0.035 -> quadratic_with_combo_maker_fees) for every series.
    """
    if fee_types is None:
        ft = RATE_TO_TYPE.get(round(float(maker_rate), 6)) if maker_rate is not None else "quadratic_with_maker_fees"
        if ft is None:
            raise ValueError(f"maker_rate {maker_rate} has no Kalshi fee type (use fee_types)")
        fee_types = {"*": (ft, 1.0)}
    trades = normalize_trades(trades)
    markets = normalize_markets(markets)
    cols = ["ticker", "event_ticker", "expiration_ts_ms", "result", "strike_type", "floor_strike", "cap_strike"]
    cols += [c for c in ("fee_type", "fee_multiplier") if c in markets]
    df = trades.merge(markets[cols], on="ticker", how="inner")
    df = df[df.result.isin(["yes", "no"])].copy()
    df["p"] = df.yes_px / 1e4
    df["contracts"] = df.qty / 100.0
    df["settle"] = (df.result == "yes").astype(float)
    df["maker_side"] = np.where(df.taker_side == "yes", "sold_yes", "bought_yes")
    df["maker_gross"] = np.where(df.taker_side == "yes", df.p - df.settle, df.settle - df.p)
    df["maker_fee"] = maker_fee_per_contract(df, fee_types) if len(df) else 0.0
    df["maker_net"] = df.maker_gross - df.maker_fee
    df["cluster"] = df.expiration_ts_ms.astype("int64")  # settlement event = expiration time (audit M5)
    df["tau_s"] = (df.expiration_ts_ms - df.ts_ms) / 1000.0
    df["tau_b"] = pd.cut(df.tau_s, TAU_BUCKETS, labels=TAU_LABELS, right=False)
    df["px_b"] = pd.cut(df.yes_px, PRICE_BUCKETS, right=False)
    ts = pd.to_datetime(df.ts_ms, unit="ms", utc=True)
    df["hour"] = ts.dt.hour
    df["weekend"] = ts.dt.dayofweek >= 5
    df["size_b"] = pd.cut(df.contracts, [0, 5, 25, 100, 500, 1e9], right=False)
    if btc is not None and len(btc):
        S = btc_price_asof(btc, df.ts_ms.to_numpy(), btc_bar_ms)
        K = df.floor_strike.fillna(df.cap_strike).to_numpy(dtype=float)
        sd = S * vol_ann * np.sqrt(np.maximum(df.tau_s.to_numpy() - 40.0, 20.0) / (365 * 24 * 3600.0))
        df["z"] = np.abs(K - S) / sd
        df["z_b"] = pd.cut(df.z, Z_BUCKETS, right=False)
    return df


def _cluster_col(df: pd.DataFrame) -> str:
    return "cluster" if "cluster" in df else "event_ticker"


def event_bootstrap(df: pd.DataFrame, col: str, n_boot: int = 500, seed: int = 11) -> tuple[float, float, float]:
    """Contract-weighted mean of `col` with a 95% CI over settlement events (expirations)."""
    ci = cluster_mean_ci(df[col], df["contracts"], df[_cluster_col(df)], n_boot, seed)
    return ci.mean, ci.lo, ci.hi


def segment_table(df: pd.DataFrame, by: list[str], n_boot: int = 300, min_events: int = MIN_SEGMENT_EVENTS) -> pd.DataFrame:
    """Segments with >= min_events settlement events: gross/net c/contract with per-segment CIs and
    the one-sided p-value of H0 'net <= 0.15c' (``p_edge``). run() adds the simultaneous bounds."""
    rows = []
    cc = _cluster_col(df)
    for key, g in df.groupby(by, observed=True):
        n_ev = g[cc].nunique()
        if n_ev < min_events:
            continue
        gross = cluster_mean_ci(g["maker_gross"], g["contracts"], g[cc], n_boot)
        net = cluster_mean_ci(g["maker_net"], g["contracts"], g[cc], n_boot)
        key = key if isinstance(key, tuple) else (key,)
        rows.append({**dict(zip(by, key)), "contracts": g.contracts.sum(), "trades": len(g), "events": n_ev,
                     "maker_gross_c": 100 * gross.mean, "gross_lo_c": 100 * gross.lo, "gross_hi_c": 100 * gross.hi,
                     "maker_net_c": 100 * net.mean, "net_lo_unadj_c": 100 * net.lo, "net_hi_unadj_c": 100 * net.hi,
                     "net_se_c": 100 * net.se, "p_edge": net.p_greater(EDGE_C / 100.0)})
    return pd.DataFrame(rows)


def run(trades: pd.DataFrame, markets: pd.DataFrame, btc: pd.DataFrame | None, out: Path,
        maker_rate: float | None = None, n_boot: int = 300, btc_bar_ms: int = DEFAULT_BTC_BAR_MS,
        fee_types: dict[str, tuple[str, float]] | None = None,
        min_events: int = MIN_SEGMENT_EVENTS) -> dict[str, pd.DataFrame]:
    df = prepare(trades, markets, btc, maker_rate=maker_rate, btc_bar_ms=btc_bar_ms, fee_types=fee_types)
    out = Path(out)
    out.mkdir(parents=True, exist_ok=True)
    tables = {
        "overall": segment_table(df.assign(all="all"), ["all"], n_boot, min_events),
        "price": segment_table(df, ["px_b"], n_boot, min_events),
        "price_side": segment_table(df, ["px_b", "maker_side"], n_boot, min_events),
        "tau": segment_table(df, ["tau_b"], n_boot, min_events),
        "tau_price": segment_table(df, ["tau_b", "px_b"], n_boot, min_events),
        "hour": segment_table(df, ["hour"], n_boot, min_events),
        "weekend": segment_table(df, ["weekend"], n_boot, min_events),
        "size": segment_table(df, ["size_b"], n_boot, min_events),
    }
    if "z_b" in df:
        tables["z"] = segment_table(df, ["z_b"], n_boot, min_events)
        tables["tau_z"] = segment_table(df, ["tau_b", "z_b"], n_boot, min_events)
    tables = e0_multiplicity(tables)
    verdict = e0_verdict(tables)
    rep = Report("exp0", "E0 — Where do makers earn on Kalshi BTC markets (public trades vs settlement)", out,
                 rule="accept if some segment's net c/contract exceeds 0.15c with >= 200 settlement events (Holm across "
                      "every segment examined); reject if every segment's simultaneous upper bound < 0.15c",
                 meta={"trades": len(df), "settlement events (expirations)": int(df["cluster"].nunique()) if len(df) else 0,
                       "segments examined (>= %d events)" % min_events: int(sum(len(t) for t in tables.values())),
                       "maker fee": "exact per-print order fee incl. balance rounding, by series fee type",
                       "clusters": "expiration time (all series expiring together settle on one BRTI average)"})
    rep.verdict = verdict
    for k, t in tables.items():
        rep.table(k, t)
    rep.write()
    for k, t in tables.items():
        t.to_csv(out / f"exp0_{k}.csv", index=False, float_format="%.4f")
    return tables


def e0_multiplicity(tables: dict[str, pd.DataFrame], alpha: float = ALPHA) -> dict[str, pd.DataFrame]:
    """Holm across EVERY examined segment of every table (H0 net <= 0.15c) and simultaneous
    (Bonferroni) bounds net_lo_c / net_hi_c: the family is all segment rows."""
    from scipy import stats

    rows = [(k, i) for k, t in tables.items() for i in range(len(t))]
    m = max(len(rows), 1)
    p = [float(tables[k]["p_edge"].iloc[i]) for k, i in rows]
    rej = holm(p, alpha)
    out = {k: t.copy() for k, t in tables.items()}
    for k in out:
        out[k]["holm_edge"] = False
    for (k, i), rj in zip(rows, rej):
        out[k].iloc[i, out[k].columns.get_loc("holm_edge")] = bool(rj)
    for k, t in out.items():
        if not len(t):
            continue
        q = stats.t.ppf(1 - alpha / m, np.maximum(t["events"].to_numpy() - 1, 1))
        t["net_lo_c"] = t["maker_net_c"] - q * t["net_se_c"]
        t["net_hi_c"] = t["maker_net_c"] + q * t["net_se_c"]
    return out


def e0_verdict(tables: dict[str, pd.DataFrame]) -> str:
    allr = pd.concat([t.assign(table=k) for k, t in tables.items() if len(t)], ignore_index=True) \
        if any(len(t) for t in tables.values()) else pd.DataFrame()
    if not len(allr):
        return "INCONCLUSIVE (no segment with >= 200 settlement events)"
    hits = allr[allr["holm_edge"]]
    if len(hits):
        top = hits.sort_values("maker_net_c", ascending=False).iloc[0]
        return (f"ACCEPT ({len(hits)} segment(s) with net > 0.15c after Holm across {len(allr)} segments; e.g. "
                f"{top['table']}: {top['maker_net_c']:.2f}c, {int(top['events'])} events)")
    if (allr["net_hi_c"] < EDGE_C).all():
        return f"REJECT (every one of {len(allr)} segments has a simultaneous upper bound < 0.15c)"
    return f"INCONCLUSIVE (no segment beats 0.15c after Holm across {len(allr)} segments)"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--trades", required=True, help="Parquet of trades (schema in module doc)")
    ap.add_argument("--markets", required=True)
    ap.add_argument("--btc", default=None)
    ap.add_argument("--out", default="docs/research/tables")
    ap.add_argument("--maker-rate", type=float, default=None,
                    help="legacy: one maker fee rate for every series (0 | 0.0175 | 0.035); prefer --fee-type")
    ap.add_argument("--fee-type", action="append", default=[],
                    help="SERIES=FEE_TYPE[:MULTIPLIER], e.g. KXBTCD=quadratic_with_maker_fees ('*' = default series)")
    ap.add_argument("--btc-bar-ms", type=int, default=DEFAULT_BTC_BAR_MS,
                    help="length of the BTC bars whose OPEN time is ts_ms (0 = point-in-time prices)")
    a = ap.parse_args()
    tables = run(pd.read_parquet(a.trades), pd.read_parquet(a.markets),
                 pd.read_parquet(a.btc) if a.btc else None, Path(a.out), a.maker_rate, btc_bar_ms=a.btc_bar_ms,
                 fee_types=parse_fee_types(a.fee_type) or None)
    for k, t in tables.items():
        print(f"\n== {k}\n{t.to_string(index=False)}")


if __name__ == "__main__":
    main()
