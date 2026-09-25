"""scripts/record.py metadata recording -> dh.research.replay_env rebuild (offline, fake transport)."""

from __future__ import annotations

import asyncio
import importlib.util
import sys
from pathlib import Path
from typing import Any

import orjson

from dh.kalshi.rest import HttpResponse, KalshiRest
from dh.research.replay_env import build_universe
from dh.store.recorder import Recorder

REPO = Path(__file__).resolve().parents[2]
T0 = 1_790_251_200 * 10**9  # 2026-09-24T12:00Z
SERIES = {"ticker": "KXBTCD", "fee_type": "quadratic", "fee_multiplier": 1}
EVENT = "KXBTCD-26SEP2413"


def _market(k: float) -> dict[str, Any]:
    return {"ticker": f"{EVENT}-T{k:g}", "event_ticker": EVENT, "market_type": "binary", "status": "active",
            "open_time": "2026-09-24T12:00:00Z", "close_time": "2026-09-24T13:00:00Z",
            "expected_expiration_time": "2026-09-24T13:00:00Z", "strike_type": "greater", "floor_strike": k,
            "price_ranges": [{"start": "0.0000", "end": "1.0000", "step": "0.0100"}],
            "rules_primary": "simple average of the sixty seconds of CF Benchmarks BRTI"}


def _load_record():
    spec = importlib.util.spec_from_file_location("_script_record_meta", REPO / "scripts" / "record.py")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


class Router:
    """Answers GETs by path like the Kalshi API (openapi shapes)."""

    def __init__(self) -> None:
        self.paths: list[str] = []

    async def __call__(self, method, url, headers, params, data, timeout_s):
        path = url.split("/trade-api/v2", 1)[1]
        self.paths.append(path)
        if path == "/markets":
            body = {"markets": [_market(84000.0), _market(84250.0)], "cursor": ""}
        elif path == "/series/KXBTCD":
            body = {"series": SERIES}
        elif path == "/series/fee_changes":
            body = {"series_fee_change_arr": [{"id": "c1", "series_ticker": "KXBTCD", "fee_type": "quadratic_with_maker_fees",
                                               "fee_multiplier": 1, "scheduled_ts": "2026-09-24T12:30:00Z"}]}
        elif path == "/events/fee_changes":
            body = {"event_fee_changes": [], "cursor": ""}
        elif path == f"/events/{EVENT}":
            body = {"event": {"event_ticker": EVENT, "series_ticker": "KXBTCD", "fee_type_override": None},
                    "markets": [_market(84000.0), _market(84250.0)]}
        else:
            return HttpResponse(404, {}, orjson.dumps({"error": {"code": "not_found"}}))
        return HttpResponse(200, {}, orjson.dumps(body))


def test_record_metadata_is_rebuildable(tmp_path):
    rec_mod = _load_record()
    clock = iter(range(T0 + 10**9, T0 + 10**12, 10**9))
    recorder = Recorder(tmp_path, start=False, clock_ns=lambda: T0)
    router = Router()
    src = rec_mod.KalshiSource({"series": ["KXBTCD"]}, recorder, rec_mod.Monitor(recorder), asyncio.Event())
    src.rest = KalshiRest("https://x.test/trade-api/v2", transport=router, on_raw=recorder.write,
                          clock_ns=lambda: next(clock))

    async def go():
        src.tickers = await src.discover()
        await src.record_metadata()
        await src.record_metadata()  # a refresh: series/fees again, the event only once

    asyncio.run(go())
    recorder.close()
    assert router.paths.count(f"/events/{EVENT}") == 1 and router.paths.count("/series/KXBTCD") == 2
    assert "/series/fee_changes" in router.paths and "/events/fee_changes" in router.paths
    u = build_universe(tmp_path, T0, T0 + 3600 * 10**9, own_fill_scan=False)
    specs = u.specs(["KXBTCD"])
    assert {s.floor_strike for s in specs} == {84000.0, 84250.0}
    assert all(s.fee_type == "quadratic" for s in specs)  # in force at availability (12:00)
    t = specs[0].ticker
    assert u.fee_fields(t, T0 + 2400 * 10**9)[0] == "quadratic_with_maker_fees"  # scheduled change at 12:30
    assert u.fee_changes_in_window() and u.events[EVENT]
