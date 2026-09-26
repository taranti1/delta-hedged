"""Re-review probes (post-fix d83d737): runner / venue / watchdog / rest guard / clock. Offline."""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import sys
import time
from pathlib import Path
from typing import Any

import orjson
import pytest

REPO = Path("/Users/thomast/Desktop/delta-hedged")
sys.path.insert(0, str(REPO))

from dh.kalshi.rest import HttpResponse, KalshiHTTPError, KalshiRest, UnscopedWriteError  # noqa: E402
from dh.live.config import LiveConfig, VenueCfg, live_config_problems, load_live_config  # noqa: E402

SHARED = VenueCfg(subaccount=1, shared_account=True, key_restricted_to_subaccount=True, exchange_indexes=(2,))


class Transport:
    def __init__(self, handler) -> None:
        self.calls: list[tuple[str, str, list, Any]] = []
        self.handler = handler

    async def __call__(self, method, url, headers, params, data, timeout_s):
        path = url.split("/trade-api/v2", 1)[1]
        self.calls.append((method, path, list(params), orjson.loads(data) if data else None))
        st, body = self.handler(method, path, list(params), orjson.loads(data) if data else None)
        return HttpResponse(st, {}, orjson.dumps(body) if body is not None else b"")


def rest_for(handler, **kw) -> tuple[KalshiRest, Transport]:
    tr = Transport(handler)
    return KalshiRest("https://fake.invalid/trade-api/v2", None, None, transport=tr, **kw), tr


# 1. guard: shard VALUE now enforced for non-reducing writes; reducing writes may name any shard / -1
async def test_guard_shard_value():
    r, tr = rest_for(lambda *a: (200, {"order_id": "x"}), write_subaccount=1, write_shards=(2,), forbid_bulk_cancel=True)
    for sh in (0, -1):
        with pytest.raises(UnscopedWriteError):
            await r.create_order({"ticker": "T", "subaccount": 1, "exchange_index": sh})
    with pytest.raises(UnscopedWriteError):
        await r.batch_create_orders([{"subaccount": 1, "exchange_index": 2}, {"subaccount": 1, "exchange_index": 0}])
    with pytest.raises(UnscopedWriteError):
        await r.reset_order_group("g", subaccount=1, exchange_index=0)
    with pytest.raises(UnscopedWriteError):
        await r.cancel_all_orders(subaccount=1)  # bulk forbidden
    assert tr.calls == []
    await r.cancel_order("o", market_ticker="T", subaccount=1, exchange_index=0)  # reducing: any shard
    await r.cancel_order("o", market_ticker="T", subaccount=1, exchange_index=-1)
    with pytest.raises(UnscopedWriteError):
        await r.cancel_order("o", market_ticker="T", subaccount=0, exchange_index=2)
    assert len(tr.calls) == 2


# 3/9. fail-closed config: defaults refused everywhere
async def test_defaults_refused():
    from dh.live.venue_kalshi import KalshiVenue

    assert live_config_problems(LiveConfig(mode="live"))  # non-empty
    with pytest.raises(ValueError):
        KalshiVenue(object(), sink=lambda e: None, cfg=VenueCfg())
    with pytest.raises(ValueError):
        KalshiVenue(object(), sink=lambda e: None, cfg=VenueCfg(subaccount=0, shared_account=False))
    ok = KalshiVenue(object(), sink=lambda e: None, cfg=VenueCfg(subaccount=0, shared_account=False, allow_primary_account=True))
    assert ok.sub == 0 and ok.bulk_cancel_allowed  # explicit opt-in on a NON-shared account only
    sys.path.insert(0, str(REPO / "scripts"))
    import importlib

    wd = importlib.import_module("watchdog")
    calls: list[Any] = []

    class Fake:
        async def cancel_all_orders(self, **k):
            calls.append(k)
            return {}

        async def close(self):
            pass

    a = argparse.Namespace(live_config="", heartbeat="/nonexistent", once=False, cancel_now=True, arm_on_start=False, max_age_s=0.0)
    assert await wd.amain(a, rest=Fake()) == 2 and calls == []


# 5. key proof
@pytest.mark.parametrize("status,expect", [(200, False), (403, True), (401, True), (503, False), (429, False)])
async def test_key_proof(status, expect):
    from dh.live.startup import verify_key_restriction

    class R:
        async def get_api_keys(self):
            raise KalshiHTTPError("GET", "/api_keys", 503, {})

        async def get_balance(self, **kw):
            assert kw == {"subaccount": 0}
            if status == 200:
                return {"balance": 1}
            raise KalshiHTTPError("GET", "/portfolio/balance", status, {})

    ok, why = await verify_key_restriction(R(), "kid", 1, [{"balance": 1}])
    assert ok is expect, why


# 5b. foreign filter: System 2 fill dropped; OUR fill without client_order_id before the ack is dropped too
def test_foreign_filter():
    from dh.core.events import KalshiFill
    from dh.live.runner import SeenIds, note_own_order, own_order_ok

    known = SeenIds()
    sys2 = KalshiFill(1, 1, "KXBTCD-X", "t1", "o-sys2", "sys2-abc", "bid", 5000, 100, False, 0, 0)
    ours_no_coid = KalshiFill(2, 2, "KXBTCD-X", "t2", "o-ours", "", "bid", 5000, 100, False, 0, 0)
    assert not own_order_ok(sys2, "dhm1-", known)
    assert not own_order_ok(ours_no_coid, "dhm1-", known)  # dropped if it beats the OrderAck
    from dh.core.events import OrderAck

    note_own_order(OrderAck(3, 3, "dhm1-tok-1", "o-ours", "KXBTCD-X", 0, 100), known)
    assert own_order_ok(ours_no_coid, "dhm1-", known)


# 7. schedule: 00:00 close handled; plausibility over the NEXT day
def test_schedule_fixed():
    from zoneinfo import ZoneInfo

    from dh.live.startup import schedule_closures

    ny = ZoneInfo("America/New_York")
    ok = [{"open_time": "00:00", "close_time": "23:59"}]
    week = {"start_time": "2026-01-01T00:00:00Z", "end_time": "2027-01-01T00:00:00Z", "monday": ok, "tuesday": ok,
            "wednesday": ok, "thursday": [{"open_time": "00:00", "close_time": "03:00"},
                                          {"open_time": "05:00", "close_time": "00:00"}],
            "friday": ok, "saturday": ok, "sunday": ok}
    now = int(dt.datetime(2026, 9, 28, 12, 0, tzinfo=ny).timestamp() * 1e9)
    cl, _ = schedule_closures({"schedule": {"standard_hours": [week], "maintenance_windows": []}},
                              now - 86_400 * 10**9, now + 8 * 86_400 * 10**9, now_ns=now)
    assert [(b - a) / 3.6e12 for a, b, _ in cl] == [2.0]


# NEW: clock slew monotonic, absorbs ppm drift, a wall step still shows
def test_clock_slew():
    from dh.live.clock import AnchoredClock

    st = {"mono": 0, "wall": 1_000_000_000_000}

    c = AnchoredClock(lambda: st["wall"], lambda: st["mono"])
    last = c()
    for _ in range(200_000):  # 200 s at 1 ms steps; wall runs +3.2 ppm fast vs mono
        st["mono"] += 1_000_000
        st["wall"] += 1_000_003  # 3 ppm
        t = c()
        assert t > last
        last = t
    assert abs(c.drift_ns()) < 50_000  # absorbed (< 50 us)
    st["wall"] += 1_000_000_000  # +1 s step
    st["mono"] += 1_000_000
    assert c.drift_ns() > 998_000_000  # still visible
    last = c()
    for _ in range(1000):
        st["mono"] += 1_000_000
        st["wall"] += 1_000_000
        t = c()
        assert t > last and t - last <= 1_000_000 + 60  # never stepped: <= 50 ppm extra
        last = t
    st["wall"] -= 5_000_000_000  # backwards wall step: clock keeps advancing
    for _ in range(1000):
        st["mono"] += 1_000_000
        st["wall"] += 1_000_000
        t = c()
        assert t > last
        last = t


# NEW: watchdog beat from the FUTURE is accepted as fresh
def test_future_beat_accepted():
    from dh.live.monitor import watchdog_beat_problem

    now = time.time_ns()
    beat = {"t": now + 3600 * 10**9, "subaccount": 1, "state": "ARMED", "armed": [1, "s"]}
    assert watchdog_beat_problem(beat, now_ns=now, subaccount=1, max_age_s=10) == ""


# NEW: scoped cancel-all inside a venue task waits on ITSELF in wait_idle (full wait_s per round);
# pagination of the resting list is followed
async def test_scoped_cancel_self_wait_and_pagination():
    from dh.core.actions import CancelAll
    from dh.live.venue_kalshi import KalshiVenue

    state = {"resting": {f"o{i}": 2 for i in range(5)}, "lists": 0}

    def handler(method, path, params, body):
        if method == "GET" and path == "/portfolio/orders":
            state["lists"] += 1
            p = dict(params)
            assert p.get("subaccount") == "1"
            ids = sorted(state["resting"])
            if p.get("cursor") == "c2":
                page, cur = ids[3:], ""
            else:
                page, cur = ids[:3], ("c2" if len(ids) > 3 else "")
            return 200, {"orders": [{"order_id": i, "ticker": "KXBTCD-X", "exchange_index": 2, "client_order_id": "dhm1-a-1",
                                     "status": "resting"} for i in page], "cursor": cur}
        if method == "DELETE" and path == "/portfolio/events/orders/batched":
            out = []
            for it in body["orders"]:
                assert it["subaccount"] == 1 and it["exchange_index"] == 2
                state["resting"].pop(it["order_id"], None)
                out.append({"order_id": it["order_id"], "client_order_id": "dhm1-a-1", "reduced_by": "1.00"})
            return 200, {"orders": out}
        if method == "DELETE" and path.startswith("/portfolio/events/orders/"):
            state["resting"].pop(path.rsplit("/", 1)[1], None)
            return 200, {"order_id": path.rsplit("/", 1)[1], "reduced_by": "1.00"}
        if method == "GET" and path.startswith("/portfolio/orders/"):
            return 404, {"error": {"code": "not_found"}}
        raise AssertionError((method, path))

    rest, tr = rest_for(handler, write_subaccount=1, write_shards=(2,), forbid_bulk_cancel=True)

    async def nosleep(s):
        return None

    v = KalshiVenue(rest, sink=lambda e: None, cfg=SHARED, sleep=nosleep)
    t0 = time.monotonic()
    v.submit([CancelAll(reason="kill")], 1)
    await asyncio.sleep(0)
    await v.wait_idle(20)
    el = time.monotonic() - t0
    print("elapsed", round(el, 2), "lists", state["lists"], "left", state["resting"])
    assert state["resting"] == {}
    assert not any(c[0] == "DELETE" and c[1] == "/portfolio/events/orders" for c in tr.calls)
    assert el >= 1.9  # the kill task waited wait_s on itself
