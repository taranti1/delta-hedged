"""Point-in-time fee reconstruction with explicit unknown history.

A downloaded current schedule does not establish the schedule before its fetch time.
Unknown intervals keep a descriptive current-schedule estimate but block promotion.
"""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq


def _ms(value) -> int | None:
    try:
        return int(pd.Timestamp(value).timestamp() * 1000) if value else None
    except (ValueError, TypeError, OverflowError):
        return None



def _mult(value) -> float:
    """fee_multiplier; an explicit null means the default 1 (0 stays 0)."""
    return 1.0 if value is None else float(value)

class HistoricalFees:
    def __init__(self, root: Path):
        self.series = {}
        files = list((root / "fees" / "snapshots" / "series").glob("*/*.json")) + list((root / "series").glob("*.json"))
        for f in sorted(files):
            raw = json.loads(f.read_text())
            obj = raw.get("series", raw)
            rows = []
            at = raw.get("fetched_ns")
            if at and obj.get("fee_type"):
                rows.append((int(at) // 1_000_000, obj["fee_type"], _mult(obj.get("fee_multiplier"))))
            changes = root / "fees" / "series_fee_changes" / f"{obj['ticker']}.json"
            if changes.exists():
                for r in json.loads(changes.read_text()).get("series_fee_change_arr", []):
                    t = _ms(r.get("scheduled_ts"))
                    if t is not None and r.get("fee_type"):
                        rows.append((t, r["fee_type"], _mult(r.get("fee_multiplier"))))
            self.series[obj["ticker"]] = sorted(set(self.series.get(obj["ticker"], []) + rows))
        p = root / "fees" / "event_fee_changes.parquet"
        self.events = {}
        self.static_events = {}
        for path in sorted((root / "events").glob("series=*/events.parquet")):
            for row in pq.ParquetFile(path).read().to_pylist():
                if row.get("fee_type_override") or row.get("fee_multiplier_override") is not None:
                    self.static_events[row["event_ticker"]] = row
        if p.is_file():
            for r in pq.ParquetFile(p).read().to_pylist():
                at = _ms(r.get("scheduled_ts"))
                if at is not None:
                    self.events.setdefault(r["event_ticker"], []).append((at, r))
        for rows in self.events.values():
            rows.sort(key=lambda r: r[0])

    def apply(self, trades: pd.DataFrame, series: str, fallback: dict) -> np.ndarray:
        default = fallback.get(series, ("quadratic_with_maker_fees", 1.0))
        ft = np.full(len(trades), default[0], dtype=object)
        fm = np.full(len(trades), default[1], dtype=float)
        known = np.zeros(len(trades), dtype=bool)
        ts = trades.ts_ms.to_numpy()
        for at, typ, mult in self.series.get(series, []):
            mask = ts >= at
            ft[mask], fm[mask], known[mask] = typ, mult, True
        if "event_ticker" in trades:
            for event in trades.event_ticker.unique():
                static = self.static_events.get(event)
                if static:
                    mask = trades.event_ticker.to_numpy() == event
                    if static.get("fee_type_override"):
                        ft[mask] = static["fee_type_override"]
                    if static.get("fee_multiplier_override") is not None:
                        fm[mask] = float(static["fee_multiplier_override"])
                    known[mask] = False  # current override alone does not date its effective start
                for at, row in self.events.get(event, []):
                    mask = (trades.event_ticker.to_numpy() == event) & (ts >= at)
                    # An explicit type with explicit multiplier fully defines the override.
                    if row.get("fee_type_override"):
                        ft[mask] = row["fee_type_override"]
                        if row.get("fee_multiplier_override") is not None:
                            fm[mask] = float(row["fee_multiplier_override"])
                            known[mask] = True
                    if row.get("fee_multiplier_override") is not None:
                        fm[mask] = float(row["fee_multiplier_override"])
                    if not row.get("fee_type_override"):
                        # Reset/inheritance semantics need a complete underlying schedule.
                        known[mask] = False
        trades["fee_type"], trades["fee_multiplier"] = ft, fm
        return known
