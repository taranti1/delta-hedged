"""BRTI back-fill with the CF passthrough format VERIFIED LIVE on 2026-09-25:
timespan=HOUR & timestamp=<hour start ISO ms> -> {"data": {"serverTime", "payload": [{time, value}]}}."""

from __future__ import annotations

import asyncio
import datetime as dt
from pathlib import Path
from typing import Any

import orjson

from dh.core.units import NS_PER_MS, NS_PER_S
from dh.kalshi.normalize import cf_history_to_ticks
from dh.live.config import BackfillCfg
from dh.live.startup import fetch_benchmark_history, resample

REAL = orjson.loads((Path(__file__).resolve().parents[1] / "kalshi" / "fixtures" / "live_cf_history_2026-09-25.json").read_bytes())
H = 3600 * NS_PER_S


def test_real_passthrough_body_parses():
    ticks = cf_history_to_ticks(REAL["body"], 1, "BRTI")
    assert len(ticks) == 10 and ticks[0].ts_exch == 1790355600000 * NS_PER_MS and ticks[0].value == 83737.5
    assert ticks[1].ts_exch - ticks[0].ts_exch == 200 * NS_PER_MS  # 5 Hz
    assert REAL["request_params"] == {"id": "BRTI", "timespan": "HOUR", "timestamp": "2026-09-25T17:00:00.000Z"}


class HourlyRest:
    """Serves the verified shape: one hour of 1-minute rows starting at `timestamp`; the newest
    (current) hour is empty (CF publication delay)."""

    def __init__(self, now_ns: int) -> None:
        self.now_ns = now_ns
        self.calls: list[dict[str, Any]] = []

    async def get_cfbenchmarks_history(self, index_id: str, *, timespan=None, timestamp=None, extra_params=None):
        self.calls.append({"timespan": timespan, "timestamp": timestamp})
        assert timespan == "HOUR"
        start = dt.datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=dt.timezone.utc)
        s_ms = int(start.timestamp() * 1000)
        assert s_ms % 3_600_000 == 0  # truncated to the hour, as CF requires
        if s_ms * NS_PER_MS + H > self.now_ns:
            return {"data": {"serverTime": "x", "payload": []}}  # not published yet
        return {"data": {"serverTime": "x", "payload": [{"time": t, "value": "84000.00"}
                                                        for t in range(s_ms, s_ms + 3_600_000, 60_000)]}}


def test_aligned_hourly_chunks_skip_the_unpublished_current_hour():
    now = 1790364000 * NS_PER_S + 25 * 60 * NS_PER_S  # 2026-09-25 19:45 UTC
    rest = HourlyRest(now)
    cfg = BackfillCfg()  # the verified defaults: HOUR, {start_iso}, align, 3600 s chunks
    ticks, n, errors = asyncio.run(fetch_benchmark_history(rest, now, cfg))
    assert rest.calls[0]["timestamp"] == "2026-09-25T19:00:00.000Z"  # newest (current, partial) hour first
    assert rest.calls[1]["timestamp"] == "2026-09-25T18:00:00.000Z"
    assert n == 49 and "skipped" in errors[0] and len(errors) == 1
    pts = resample(ticks, now - int(cfg.days * 86400) * NS_PER_S, now, cfg.step_s)
    assert len(pts) / (int(cfg.days * 86400) // cfg.step_s) > 0.98
