"""M1.2 settlement-convention check: BRTI 60-print averages vs published ``expiration_value``.

For every settled KXBTCD / KXBTC / KXBTC15M expiration, the published ``expiration_value`` is
compared (to the cent) with the average of the BRTI once-per-second prints over several
candidate windows. The production convention (``dh.settlement.convention``) is

    close_(T-60,T]        prints stamped T-59 s .. T with T = close_time, average rounded half up

and the alternatives show that no other plausible reading matches:

    close_[T-60,T)        T-60 .. T-1 (start tick included, close tick excluded)
    close_[T-60,T]        61 prints T-60 .. T
    close_(T-59,T+1]      two seconds late (T-58 .. T+1)
    close_[T-61,T-1)      one second early (T-61 .. T-2)
    expected_[E-60,E)     E = expected_expiration_time = close + 5 min (the old, wrong T)
    5hz_mean_[T-60,T)     mean of every 5 Hz tick in the window (not 60 once-per-second prints)

Prints are integer cents (BRTI is published with 2 decimals), so each average is an exact
rational: ``sum / n`` cents, rounded half up as ``(2 * sum + n) // (2 * n)``; the ``diff``
columns are the unrounded average minus the published value in dollars.

Inputs
------
* prints: {second (Unix s): value in integer cents}. From the recorder (``prints_from_recording``:
  1 Hz ``cfbenchmarks_value``, else the 5 Hz tick stamped exactly on the second) or from the CF
  Benchmarks history (``prints_from_history``: the 5 Hz row stamped exactly on the second).
* expirations: one row per settlement time T with the published value(s) and the markets'
  ``expected_expiration_time`` (``expirations_from_markets``).

``python -m dh.research.settlement_check live --root data`` (recorder) or
``... history --history data/external/kalshi`` (downloaded markets + BRTI) writes a CSV per
expiration and prints the pass rate per convention (M1.2 criterion: >= 99 % of events match
to $0.01 under the production convention).
"""

from __future__ import annotations

import argparse
import math
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import orjson
import pandas as pd

from dh.core.units import NS_PER_MS, NS_PER_S
from dh.kalshi.wire import opt_iso_to_ns

PRODUCTION = "close_[T-60,T)"


@dataclass(frozen=True)
class Window:
    name: str
    anchor: str  # 'close' | 'expected'
    first: int  # offset (s) of the first print from the anchor
    last: int  # offset (s) of the last print (inclusive)


WINDOWS: tuple[Window, ...] = (
    Window(PRODUCTION, "close", -60, -1),
    Window("close_(T-60,T]", "close", -59, 0),
    Window("close_[T-60,T]", "close", -60, 0),
    Window("close_(T-59,T+1]", "close", -58, 1),
    Window("close_[T-61,T-1)", "close", -61, -2),
    Window("expected_[E-60,E)", "expected", -60, -1),
)


def cents(v: Any) -> int | None:
    """'83950.62' / 83950.62 -> 8395062 (None if not a finite number on the cent grid)."""
    try:
        f = float(str(v).replace(",", "")) if isinstance(v, str) else float(v)  # one market published "77,362.10"
    except (TypeError, ValueError):
        return None
    if not math.isfinite(f):
        return None
    c = round(f * 100)
    return int(c) if abs(f * 100 - c) < 1e-4 else None


def round_half_up_cents(total: int, n: int) -> int:
    """round(total / n) with halves up, exact for non-negative integers."""
    return (2 * total + n) // (2 * n)


# ============================================================================ prints
def prints_from_recording(root: str | Path, t0_ns: int, t1_ns: int) -> tuple[dict[int, int], dict[int, int], pd.DataFrame, dict]:
    """(1 Hz prints, 5 Hz on-the-second prints, 5 Hz ticks frame, stats) from ``kalshi.ws``.

    1 Hz: ``cfbenchmarks_value`` frames; the second is the CF source ``time`` inside ``data``
    (ceil to the second, the ``dh.settlement.window`` rule; whole-second stamps in practice).
    5 Hz: ``cfbenchmarks_value_5hz`` ``source_ts_ms``/``value_usd``; the on-the-second print is the
    tick stamped exactly on the second. The 5 Hz frame also keeps every tick (for the 5 Hz-mean
    alternative) and the quarter-hour running averages are returned in ``stats['qh']``
    ({close_s: (avg_str, n)} of Kalshi's ``last_60s_windowed_average_15min`` at its largest n).
    """
    from dh.store.replay import iter_raw

    p1: dict[int, int] = {}
    p5: dict[int, int] = {}
    t5: list[int] = []
    v5: list[int] = []
    qh: dict[int, tuple[str, int, int]] = {}
    st = {"1hz": 0, "5hz": 0, "1hz_conflicts": 0, "1hz_offgrid_ms": 0, "5hz_offcent": 0, "1hz_offcent": 0}
    for r in iter_raw(root, ["kalshi.ws"], t0_ns, t1_ns):
        if b"cfbenchmarks_value" not in r.data[:48]:
            continue
        j = orjson.loads(r.data)
        typ = j.get("type")
        m = j.get("msg") or {}
        if m.get("index_id") != "BRTI":
            continue
        if typ == "cfbenchmarks_value":
            try:
                d = orjson.loads(m.get("data") or b"{}")
            except orjson.JSONDecodeError:
                continue
            t_ms = int(d.get("time") or 0)
            c = cents(d.get("value"))
            if not t_ms or c is None:
                st["1hz_offcent"] += c is None
                continue
            st["1hz"] += 1
            if t_ms % 1000:
                st["1hz_offgrid_ms"] += 1
            sec = -((-t_ms) // 1000)
            if sec in p1 and p1[sec] != c:
                st["1hz_conflicts"] += 1
            p1[sec] = c
            q = m.get("last_60s_windowed_average_15min")
            if isinstance(q, dict) and q.get("value") is not None:
                close_s = int(q.get("window_start_ts_ms", 0)) // 1000 + 60
                n = int(q.get("window_size") or 0)
                if close_s not in qh or n >= qh[close_s][1]:
                    qh[close_s] = (str(q["value"]), n, int(q.get("window_end_ts_exclusive", 0)))
        elif typ == "cfbenchmarks_value_5hz":
            t_ms = int(m.get("source_ts_ms") or 0)
            c = cents(m.get("value_usd"))
            if not t_ms or c is None:
                st["5hz_offcent"] += c is None
                continue
            st["5hz"] += 1
            t5.append(t_ms)
            v5.append(c)
            if t_ms % 1000 == 0:
                p5[t_ms // 1000] = c
    ticks = pd.DataFrame({"t_ms": np.asarray(t5, dtype=np.int64), "cents": np.asarray(v5, dtype=np.int64)})
    ticks = ticks.drop_duplicates("t_ms").sort_values("t_ms").reset_index(drop=True)
    st["qh"] = qh
    return p1, p5, ticks, st


def prints_from_history(brti_dir: str | Path) -> tuple[dict[int, int], pd.DataFrame]:
    """(on-the-second prints, all ticks) from the downloader's BRTI store
    (``brti/hourly/<YYYY-MM-DD>.parquet`` with t_ms, cents)."""
    files = sorted(Path(brti_dir).glob("*/*.parquet")) + sorted(Path(brti_dir).glob("*.parquet"))
    if not files:
        return {}, pd.DataFrame({"t_ms": [], "cents": []})
    df = pd.concat([pd.read_parquet(f, columns=["t_ms", "cents"]) for f in files], ignore_index=True)
    df = df.drop_duplicates("t_ms").sort_values("t_ms").reset_index(drop=True)
    on = df[df.t_ms % 1000 == 0]
    return dict(zip((on.t_ms // 1000).astype(int).tolist(), on.cents.astype(int).tolist())), df


# ============================================================================ expirations
def expirations_from_markets(markets: pd.DataFrame) -> pd.DataFrame:
    """One row per settlement time T (= close_time) from settled market rows (any series).

    Columns: T_s, E_s (expected_expiration_time, s), series (sorted, '+'-joined), events,
    ev (the published expiration_value; the most common one), ev_values (distinct values across
    the event's markets and series: must be 1), markets, results.
    """
    m = markets.copy()
    if "close_ts_ms" not in m:
        m["close_ts_ms"] = m["close_time"].map(lambda s: (opt_iso_to_ns(s) or 0) // NS_PER_MS)
    if "expected_expiration_ts_ms" not in m:
        m["expected_expiration_ts_ms"] = m["expected_expiration_time"].map(lambda s: (opt_iso_to_ns(s) or 0) // NS_PER_MS)
    m = m[m["expiration_value"].notna() & (m["expiration_value"].astype(str) != "")]
    m = m[m["result"].isin(["yes", "no"])]
    if "series_ticker" not in m:
        m["series_ticker"] = m["event_ticker"].str.split("-").str[0]
    rows = []
    for T_ms, g in m.groupby("close_ts_ms"):
        vals = g["expiration_value"].astype(str).value_counts()
        rows.append({
            "T_s": int(T_ms) // 1000,
            "E_s": int(g["expected_expiration_ts_ms"].max() or 0) // 1000,
            "series": "+".join(sorted(g["series_ticker"].unique())),
            "events": ",".join(sorted(g["event_ticker"].unique())),
            "ev": vals.index[0],
            "ev_values": len(vals),
            "markets": len(g),
        })
    return pd.DataFrame(rows).sort_values("T_s").reset_index(drop=True) if rows else pd.DataFrame(
        columns=["T_s", "E_s", "series", "events", "ev", "ev_values", "markets"])


# ============================================================================ check
def _window_avg(prints: dict[int, int], secs: Iterable[int]) -> tuple[int | None, int, int]:
    """(sum cents or None if any second is missing, n prints, n missing)."""
    tot, n, miss = 0, 0, 0
    for s in secs:
        v = prints.get(s)
        if v is None:
            miss += 1
            continue
        tot += v
        n += 1
    return (tot if miss == 0 else None), n, miss


def check(expirations: pd.DataFrame, prints: dict[int, int], ticks: pd.DataFrame | None = None,
          windows: tuple[Window, ...] = WINDOWS) -> pd.DataFrame:
    """Per expiration and window: reconstructed rounded average, match flag and diff ($)."""
    out = []
    t_ms = ticks["t_ms"].to_numpy() if ticks is not None and len(ticks) else np.array([], dtype=np.int64)
    t_c = ticks["cents"].to_numpy() if ticks is not None and len(ticks) else np.array([], dtype=np.int64)
    for r in expirations.itertuples(index=False):
        evc = cents(r.ev)
        row: dict[str, Any] = {"T_s": r.T_s, "T": pd.Timestamp(r.T_s, unit="s", tz="UTC").isoformat(), "series": r.series,
                               "ev": r.ev, "ev_values": r.ev_values}
        for w in windows:
            anchor = r.T_s if w.anchor == "close" else (r.E_s or r.T_s + 300)
            tot, n, miss = _window_avg(prints, range(anchor + w.first, anchor + w.last + 1))
            key = w.name
            if tot is None or evc is None:
                row[f"{key}:match"] = np.nan
                row[f"{key}:diff"] = np.nan
                row[f"{key}:missing"] = miss
                continue
            rc = round_half_up_cents(tot, n)
            row[f"{key}:match"] = float(rc == evc)
            row[f"{key}:diff"] = tot / n / 100 - evc / 100
            row[f"{key}:missing"] = 0
        # 5 Hz mean of every tick in [T-60 s, T)
        key = "5hz_mean_[T-60,T)"
        if len(t_ms):
            lo, hi = np.searchsorted(t_ms, (r.T_s - 60) * 1000, "left"), np.searchsorted(t_ms, r.T_s * 1000, "left")
            if hi - lo >= 250 and evc is not None:  # at least ~5/6 of the 300 ticks
                tot, n = int(t_c[lo:hi].sum()), int(hi - lo)
                row[f"{key}:match"] = float(round_half_up_cents(tot, n) == evc)
                row[f"{key}:diff"] = tot / n / 100 - evc / 100
            else:
                row[f"{key}:match"] = np.nan
                row[f"{key}:diff"] = np.nan
        out.append(row)
    return pd.DataFrame(out)


def summarize(res: pd.DataFrame) -> pd.DataFrame:
    """Pass rate per window over expirations where that window is fully observed."""
    rows = []
    for c in [c for c in res.columns if c.endswith(":match")]:
        name = c[:-6]
        v = res[c].dropna()
        d = res[f"{name}:diff"].dropna().abs() if f"{name}:diff" in res else pd.Series(dtype=float)
        rows.append({"window": name, "events_checked": int(len(v)), "matches": int(v.sum()),
                     "pass_rate": float(v.mean()) if len(v) else np.nan,
                     "median_abs_diff": float(d.median()) if len(d) else np.nan,
                     "max_abs_diff": float(d.max()) if len(d) else np.nan,
                     "not_observable": int(res[c].isna().sum())})
    return pd.DataFrame(rows)


def rounding_rule_check(res: pd.DataFrame, prints: dict[int, int]) -> dict[str, int]:
    """Events whose production-window average is exactly half a cent (sum % 60 == 30): how
    each tie-breaking rule fares (half up / half down / half even / truncate)."""
    out = {"ties": 0, "half_up": 0, "half_down": 0, "half_even": 0, "truncate": 0, "float_round": 0}
    for r in res.itertuples(index=False):
        tot, n, miss = _window_avg(prints, range(r.T_s - 60, r.T_s))
        evc = cents(r.ev)
        if tot is None or evc is None or (tot * 2) % n != 0 or tot % n == 0:
            continue
        if (2 * tot) % (2 * n) != n:  # not an exact half
            continue
        out["ties"] += 1
        lo = tot // n
        out["half_up"] += evc == lo + 1
        out["half_down"] += evc == lo
        out["half_even"] += evc == (lo if lo % 2 == 0 else lo + 1)
        out["truncate"] += evc == lo
        # round() of the binary double average (sum of the dollar floats / 60): the rule that
        # explains every tie before 2026-08-21 ~21:00Z (docs/research/M1_2_SETTLEMENT_CHECK.md)
        vals = [prints[s] / 100 for s in range(r.T_s - 60, r.T_s)]
        out["float_round"] += evc == round(round(sum(vals) / len(vals), 2) * 100)
    return out


# ============================================================================ CLI
def _live(args: argparse.Namespace) -> None:
    import asyncio

    from dh.kalshi.rest import KalshiRest

    root = Path(args.root)
    p1, p5, ticks, st = prints_from_recording(root, 0, 2**62)
    prints = dict(p5)
    prints.update(p1)  # the 1 Hz print wins where both exist
    agree = sum(1 for s, v in p1.items() if s in p5 and p5[s] == v)
    both = sum(1 for s in p1 if s in p5)
    lo_s, hi_s = min(prints), max(prints)

    async def fetch() -> list[dict]:
        rows = []
        async with KalshiRest(read_only=True) as rest:  # unsigned public GETs: no account tokens
            for s in ("KXBTCD", "KXBTC", "KXBTC15M"):
                async for m in rest.iter_markets(series_ticker=s, status="settled", min_close_ts=lo_s):
                    rows.append({**m, "series_ticker": s})
        return rows

    mk = pd.DataFrame(asyncio.run(fetch()))
    ex = expirations_from_markets(mk)
    ex = ex[(ex.T_s - 61 >= lo_s) & (ex.T_s <= hi_s)]
    res = check(ex, prints, ticks)
    qh = st.pop("qh")
    res["kalshi_qh_avg"] = res.T_s.map(lambda T: qh.get(T, (None,))[0])
    res["kalshi_qh_n"] = res.T_s.map(lambda T: qh.get(T, (None, None))[1])
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    res.to_csv(out / "settlement_check_live.csv", index=False)
    mk.drop(columns=[c for c in mk.columns if c in ("rules_primary", "rules_secondary")], errors="ignore").to_csv(
        out / "settlement_check_live_markets.csv", index=False)
    print(orjson.dumps({**st, "prints_1hz": len(p1), "prints_5hz_on_second": len(p5),
                        "seconds_both": both, "seconds_both_equal": agree,
                        "span": [lo_s, hi_s]}).decode())
    print(summarize(res).to_string(index=False))
    print("ties:", rounding_rule_check(res, prints))


async def fetch_settled(series: str, rate: float = 1.0) -> pd.DataFrame:
    """Every settled market of a series from GET /markets (status=settled) and
    GET /historical/markets (UNSIGNED public requests, ``rate`` per second)."""
    import time as _time

    from dh.kalshi.rest import KalshiRest

    class _Pace:
        def __init__(self) -> None:
            self.next = 0.0

        async def acquire(self, method: str, path: str, n_items: int = 1) -> float:
            import asyncio

            now = _time.monotonic()
            if now < self.next:
                await asyncio.sleep(self.next - now)
            self.next = max(now, self.next) + 1.0 / rate
            return 0.0

        def on_429(self, method: str) -> None:
            self.next = _time.monotonic() + 10.0

    cols = ["ticker", "event_ticker", "close_time", "expected_expiration_time", "result", "expiration_value",
            "floor_strike", "strike_type", "settlement_ts", "status"]
    rows = []
    async with KalshiRest(limiter=_Pace(), read_only=True, max_get_retries=10, backoff_max_s=60.0) as rest:
        async for m in rest.iter_markets(series_ticker=series, status="settled"):
            rows.append({**{c: m.get(c) for c in cols}, "source": "live"})
        async for m in rest.iter_historical_markets(series_ticker=series):
            rows.append({**{c: m.get(c) for c in cols}, "source": "historical"})
    df = pd.DataFrame(rows).drop_duplicates("ticker")
    df["series_ticker"] = series
    return df


def _history(args: argparse.Namespace) -> None:
    import asyncio

    root = Path(args.history)
    frames = []
    if args.fetch_series:
        for s in args.fetch_series:
            cache = root / "settlement" / f"{s}_settled.parquet"
            if cache.is_file() and not args.refresh:
                df = pd.read_parquet(cache)
            else:
                df = asyncio.run(fetch_settled(s))
                cache.parent.mkdir(parents=True, exist_ok=True)
                df.to_parquet(cache)
            frames.append(df[["ticker", "event_ticker", "series_ticker", "close_time", "expected_expiration_time", "result",
                              "expiration_value"]])
    files = sorted((root / "markets").glob("series=*/*.parquet"))
    if files:
        frames += [pd.read_parquet(f, columns=["ticker", "event_ticker", "series_ticker", "close_time", "close_ts_ms",
                                               "expected_expiration_ts_ms", "result", "expiration_value"]) for f in files]
    mk = pd.concat(frames, ignore_index=True)
    for c in ("close_ts_ms", "expected_expiration_ts_ms"):
        src = "close_time" if c == "close_ts_ms" else "expected_expiration_time"
        if c not in mk:
            mk[c] = np.nan
        miss = mk[c].isna()
        mk.loc[miss, c] = mk.loc[miss, src].map(lambda x: (opt_iso_to_ns(x) or 0) // NS_PER_MS)
    mk["close_ts_ms"] = mk["close_ts_ms"].astype("int64")
    mk["expected_expiration_ts_ms"] = mk["expected_expiration_ts_ms"].astype("int64")
    ex = expirations_from_markets(mk)
    prints, ticks = prints_from_history(root / "brti" / "hourly")
    if prints:
        lo_s, hi_s = min(prints), max(prints)
        ex = ex[(ex.T_s - 61 >= lo_s) & (ex.T_s + 1 <= hi_s)]
    res = check(ex, prints, ticks)
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    res.to_csv(out / "settlement_check_history.csv", index=False)
    print(summarize(res).to_string(index=False))
    print("ties:", rounding_rule_check(res, prints))


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description="M1.2 settlement convention check")
    sub = ap.add_subparsers(dest="cmd", required=True)
    a = sub.add_parser("live")
    a.add_argument("--root", default="data")
    a.add_argument("--out", default="data/results/settlement_check")
    b = sub.add_parser("history")
    b.add_argument("--history", default="data/external/kalshi")
    b.add_argument("--out", default="data/results/settlement_check")
    b.add_argument("--fetch-series", nargs="*", default=["KXBTC15M"],
                   help="also fetch every settled market of these series (unsigned, cached under <history>/settlement)")
    b.add_argument("--refresh", action="store_true", help="re-fetch the cached settled-market lists")
    args = ap.parse_args(argv)
    (_live if args.cmd == "live" else _history)(args)


if __name__ == "__main__":
    main()


__all__ = ["WINDOWS", "PRODUCTION", "check", "summarize", "expirations_from_markets", "prints_from_recording",
           "prints_from_history", "rounding_rule_check", "round_half_up_cents", "cents", "NS_PER_S", "defaultdict"]
