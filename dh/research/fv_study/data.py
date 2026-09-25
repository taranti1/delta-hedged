"""Bitstamp BTC/USD 1-minute bars: loading, outage flags, minute returns.

Candles are labelled by OPEN time: the candle with timestamp s covers [s, s + 60) and its
close is the last trade before s + 60.  Missing minutes were filled upstream with flat
zero-volume candles (O = H = L = C = previous close); isolated ones are genuine no-trade
minutes, long runs are feed outages.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import polars as pl

from dh.research.fv_study.config import BULK_FILE, BULK_URL, CACHE_DIR, CFG, DATA_DIR, LATEST_FILE, utc


@dataclass
class Minutes:
    ts: np.ndarray  # int64 open time (UTC s), regular 60 s grid
    o: np.ndarray
    h: np.ndarray
    l: np.ndarray
    c: np.ndarray
    v: np.ndarray
    zero_flat: np.ndarray  # no trade in the minute (carried-forward candle)
    outage: np.ndarray  # part of a zero-flat run >= outage_run_min minutes
    r: np.ndarray  # log(c_i / c_{i-1}); r[0] = nan
    r_valid: np.ndarray  # both candles outside outages

    @property
    def n(self) -> int:
        return int(self.ts.size)

    @property
    def t0(self) -> int:
        return int(self.ts[0])

    def idx(self, ts_s) -> np.ndarray:
        """Index of the candle with open time ts_s (vectorized; caller ensures range)."""
        return (np.asarray(ts_s, dtype=np.int64) - self.t0) // 60

    @property
    def ohlc4(self) -> np.ndarray:
        return (self.o + self.h + self.l + self.c) / 4.0


def _runs_mask(flag: np.ndarray, min_len: int) -> np.ndarray:
    """True for elements belonging to runs of True of length >= min_len."""
    d = np.diff(np.r_[0, flag.astype(np.int8), 0])
    starts = np.flatnonzero(d == 1)
    ends = np.flatnonzero(d == -1)
    out = np.zeros(flag.size, dtype=bool)
    for s, e in zip(starts, ends):
        if e - s >= min_len:
            out[s:e] = True
    return out


def load_minutes(
    data_dir: Path = DATA_DIR,
    start: str = CFG.data_start,
    end: str | None = None,
    outage_run_min: int = CFG.outage_run_min,
    use_cache: bool = True,
) -> Minutes:
    """Load, merge (latest file wins on overlap), validate and flag the minute bars."""
    data_dir = Path(data_dir)
    bulk = data_dir / BULK_FILE
    latest = data_dir / LATEST_FILE
    if not latest.exists():
        raise FileNotFoundError(f"{latest} missing (copy the 2025-2026 Bitstamp minute file there)")
    if not bulk.exists():
        raise FileNotFoundError(f"{bulk} missing; download it from {BULK_URL}")
    cache = CACHE_DIR / f"minutes_{start}.parquet"
    src_mtime = max(bulk.stat().st_mtime, latest.stat().st_mtime)
    if use_cache and cache.exists() and cache.stat().st_mtime >= src_mtime:
        df = pl.read_parquet(cache)
    else:
        t_start = utc(start)
        b = pl.read_csv(bulk).filter(pl.col("timestamp") >= t_start)
        lt = pl.read_csv(latest).filter(pl.col("timestamp") >= t_start)
        cols = ["timestamp", "open", "high", "low", "close", "volume"]
        df = (
            pl.concat([b.select(cols).with_columns(pl.lit(0).alias("src")), lt.select(cols).with_columns(pl.lit(1).alias("src"))])
            .sort(["timestamp", "src"])
            .unique("timestamp", keep="last", maintain_order=True)
            .sort("timestamp")
            .drop("src")
        )
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        df.write_parquet(cache)
    if end is not None:
        df = df.filter(pl.col("timestamp") < utc(end))
    ts = df["timestamp"].to_numpy().astype(np.int64)
    if not np.all(np.diff(ts) == 60):
        raise ValueError("minute grid is not regular; gaps must be filled before use")
    o, h, l, c, v = (df[k].to_numpy().astype(np.float64) for k in ("open", "high", "low", "close", "volume"))
    zero_flat = (v == 0) & (o == h) & (h == l) & (l == c)
    outage = _runs_mask(zero_flat, outage_run_min)
    r = np.r_[np.nan, np.diff(np.log(c))]
    r_valid = np.r_[False, ~outage[1:] & ~outage[:-1]]
    return Minutes(ts=ts, o=o, h=h, l=l, c=c, v=v, zero_flat=zero_flat, outage=outage, r=r, r_valid=r_valid)


def month_starts(start: str, end: str) -> list[int]:
    """UTC epoch seconds of the first instant of each month in [start, end)."""
    s = dt.datetime.strptime(start, "%Y-%m-%d").replace(tzinfo=dt.timezone.utc)
    e = dt.datetime.strptime(end, "%Y-%m-%d").replace(tzinfo=dt.timezone.utc)
    out = []
    cur = s.replace(day=1)
    if cur < s:
        cur = s
    while cur < e:
        out.append(int(cur.timestamp()))
        y, m = (cur.year + (cur.month // 12), cur.month % 12 + 1)
        cur = dt.datetime(y, m, 1, tzinfo=dt.timezone.utc)
    return out


def add_months(ts_s: int, k: int) -> int:
    """Shift a UTC month start by k months."""
    d = dt.datetime.fromtimestamp(ts_s, tz=dt.timezone.utc)
    m0 = d.year * 12 + (d.month - 1) + k
    return int(dt.datetime(m0 // 12, m0 % 12 + 1, 1, tzinfo=dt.timezone.utc).timestamp())
