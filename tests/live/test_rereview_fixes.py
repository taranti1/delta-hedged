"""Regression tests for the pre-live RE-review (docs/research/prelive_review_2026-09-25/
PRELIVE_REREVIEW.md), runner side: NEW-1 (our fills parked, never dropped, while their order id
is unknown), NEW-2 (the watchdog proves it can reach the API), NEW-3 (future-stamped beats and
heartbeats are not fresh), NEW-5 (a scoped cancel-all inside a venue task never waits on itself),
NEW-6 (the key proof accepts only 403). Offline: fakes only, nothing reaches Kalshi.

Each test turns one review probe (test_rereview_runner.py) into the FIXED behaviour.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import replace
from pathlib import Path
from typing import Any

import orjson
import pytest

from dh.core.actions import CancelAll
from dh.core.events import FeedStatus, KalshiFill, KalshiOrderUpdate, OrderAck
from dh.core.units import NS_PER_MS, NS_PER_S
from dh.kalshi.rest import HttpResponse, KalshiHTTPError, KalshiRest
from dh.live.config import LiveConfig, VenueCfg, WatchdogCfg, live_config_problems
from dh.live.monitor import read_heartbeat, watchdog_beat_path, watchdog_beat_problem, write_heartbeat
from dh.live.replay import UniverseReplay
from dh.live.runner import UNKNOWN_ORDER_REASON, order_row_owner
from dh.live.startup import verify_key_restriction
from dh.live.venue_kalshi import KalshiVenue
from dh.live.watchdog import Watchdog, rest_api_probe, rest_scoped_cancel_all

from .fakes import T0, FakeRest, RecordingStrategy, fill_row, order_row
from .test_runner import TK, OrderingStrategy, cfg, live_runner

PREFIX = "dhm1-"


class Capture:
    """Recorder stand-in: what the runner records (events.live in processing order)."""

    def __init__(self) -> None:
        self.events: list[tuple[str, Any]] = []

    def write_event(self, stream: str, ev: Any) -> None:
        self.events.append((stream, ev))

    def write(self, stream: str, ts: int, data: bytes) -> None:
        pass

    def live(self) -> list[Any]:
        return [e for st, e in self.events if st == "events.live"]


def _shared(**venue: Any) -> LiveConfig:
    v = dict(subaccount=1, shared_account=True, key_restricted_to_subaccount=True, allow_primary_account=False,
             unknown_order_lookup_delay_s=0.0, unknown_order_lookup_retry_s=0.01)
    v.update(venue)
    return cfg(**v)


def _runner(rest: FakeRest | None = None, clock: Any = None, **venue: Any):
    s = OrderingStrategy(n=0)
    c = _shared(**venue)
    rec = Capture()
    kw = {"clock_ns": clock} if clock is not None else {}
    r, v, rest = live_runner(s, rest=rest, config=c, venue_cfg=c.venue, own_id_prefix=PREFIX, series=("KXBTCD",),
                             recorder=rec, **kw)
    return s, r, rest, rec


def _fill(t: int, coid: str, oid: str, trade: str) -> KalshiFill:
    return KalshiFill(t, t, TK, trade, oid, coid, "bid", 4500, 100, False, 0, 0, False)


def _delivered(s: RecordingStrategy) -> list[tuple[str, str]]:
    return [(type(e).__name__, getattr(e, "trade_id", "") or getattr(e, "order_id", ""))
            for e in s.events if isinstance(e, (OrderAck, KalshiFill, KalshiOrderUpdate))]


async def _drain(r, rounds: int = 20) -> None:
    """Let the runner's background lookups run, then process what they queued."""
    for _ in range(rounds):
        await asyncio.sleep(0.005)
        r.process_pending()


def _replay(raw: list[Any], live: list[Any]) -> RecordingStrategy:
    """UniverseReplay over the raw WS events and the recorded events.live (ranked last at equal ts)."""
    s2 = RecordingStrategy()
    rep = UniverseReplay(s2, [], live=True, subaccount=1, key_restricted=True, series=("KXBTCD",), own_id_prefix=PREFIX)
    merged = sorted([(e.ts, 0, i, e) for i, e in enumerate(raw)] + [(e.ts, 1, i, e) for i, e in enumerate(live)],
                    key=lambda x: x[:3])
    for *_, e in merged:
        rep.on_event(e)
    return s2


# ============================================================================ NEW-1
async def test_our_fill_that_beats_the_create_ack_is_delivered_after_the_ack_and_replays():
    """Probe test_foreign_filter: a WS fill without client_order_id, queued before our create's
    response, was DROPPED as foreign. Now it is parked and delivered right after the ack (with the
    order's client_order_id), recorded on events.live; replay delivers exactly the same."""
    s, r, rest, rec = _runner(unknown_order_lookup_delay_s=5.0)  # the ack comes first: no lookup needed
    t = time.time_ns()
    raw = [_fill(t, "", "o-2", "t1")]
    r.push(raw[0])
    r.process_pending()
    assert _delivered(s) == [] and "o-2" in r._parked  # noqa: SLF001
    assert r.metrics.get("dh_foreign_order_events_total", type="KalshiFill", source="ws") is None
    r.push_result(OrderAck(t + 1, 0, "dhm1-tok-2", "o-2", TK, 0, 200))
    r.process_pending()
    assert _delivered(s) == [("OrderAck", "o-2"), ("KalshiFill", "t1")]
    f = s.events[-1]
    assert f.client_order_id == "dhm1-tok-2" and f.ts >= t + 1 and s.om.position(TK) == 100
    assert r._parked == {} and not rest.of("get_order")  # noqa: SLF001
    live = rec.live()
    assert [type(e).__name__ for e in live] == ["OrderAck", "KalshiFill"]  # the release is recorded after the ack
    assert _delivered(_replay(raw, live)) == _delivered(s)
    # a late duplicate (WS or REST copy) is never delivered twice
    r.push(_fill(t + 2, "", "o-2", "t1"))
    r.process_pending()
    assert [x for x in _delivered(s) if x[0] == "KalshiFill"] == [("KalshiFill", "t1")]


async def test_rest_fill_without_client_id_for_a_known_order_is_delivered():
    s, r, rest, _ = _runner()
    t = time.time_ns()
    r.push_result(OrderAck(t, 0, "dhm1-tok-1", "o-1", TK, 0, 200))
    r.process_pending()
    tf = t + 50 * NS_PER_MS
    r.push_side("fills", {"rows": [fill_row("f-1", "o-1", TK, created_ns=tf)], "fetched_ns": t + 10, "since_ns": 0})
    r.process_pending()
    assert ("KalshiFill", "f-1") in _delivered(s) and s.om.position(TK) == 100
    assert r.metrics.get("dh_foreign_order_events_total", type="KalshiFill", source="rest") is None


async def test_rest_fill_of_a_create_being_reconciled_is_released_by_the_reconciliation():
    """A create with an unknown outcome: its REST back-filled fill (no client id) arrives before the
    reconciliation finds the order. Parked, then released by the reconciled order update."""
    s, r, rest, rec = _runner(unknown_order_lookup_delay_s=5.0)
    t = time.time_ns()
    tf = t + 50 * NS_PER_MS
    r.push_side("fills", {"rows": [fill_row("f-7", "o-7", TK, created_ns=tf)], "fetched_ns": t + 10, "since_ns": 0})
    r.process_pending()
    assert _delivered(s) == [] and "o-7" in r._parked  # noqa: SLF001
    # the venue's reconciliation (find_created by client_order_id) emits the order with our client id
    r.push_result(KalshiOrderUpdate(t + 20, 0, TK, "o-7", "dhm1-tok-7", "executed", "bid", 4500, 100, 100, 0))
    r.process_pending()
    assert _delivered(s) == [("KalshiOrderUpdate", "o-7"), ("KalshiFill", "f-7")]
    assert s.events[-1].client_order_id == "dhm1-tok-7" and r._parked == {}  # noqa: SLF001
    assert [type(e).__name__ for e in rec.live()][-2:] == ["KalshiOrderUpdate", "KalshiFill"]


async def test_lookup_proving_the_order_ours_releases_the_parked_fill():
    rest = FakeRest()
    rest.orders["o-3"] = order_row("dhm1-tok-3", "o-3", TK, subaccount=1)
    s, r, rest, rec = _runner(rest)
    t = time.time_ns()
    raw = [_fill(t, "", "o-3", "t3")]
    r.push(raw[0])
    await _drain(r)
    assert rest.of("get_order") == [(("o-3",), {})]
    assert _delivered(s) == [("KalshiFill", "t3")] and s.events[-1].client_order_id == "dhm1-tok-3"
    assert r.metrics.get("dh_unknown_order_events_released_total", why="lookup") == 1.0
    # recorded: the replay delivers it too (the raw copy is dropped there, as it was parked live)
    assert _delivered(_replay(raw, rec.live())) == _delivered(s)
    # the order is known now: its next fill passes at once
    r.push(_fill(t + 1, "", "o-3", "t4"))
    r.process_pending()
    assert [x for x in _delivered(s)] == [("KalshiFill", "t3"), ("KalshiFill", "t4")]


@pytest.mark.parametrize("row", [
    order_row("sys2-abc", "o-9", TK),  # System 2's order (another client id; primary: no subaccount field)
    order_row("dhm1-tok-9", "o-9", TK, subaccount=5),  # our prefix, but another subaccount
])
async def test_a_proven_foreign_fill_is_dropped(row):
    rest = FakeRest()
    rest.orders["o-9"] = row
    s, r, rest, _ = _runner(rest)
    r.push(_fill(time.time_ns(), "", "o-9", "t9"))
    await _drain(r)
    assert _delivered(s) == [] and r._parked == {}  # noqa: SLF001
    assert r.metrics.get("dh_foreign_order_events_total", type="KalshiFill", source="ws") == 1.0
    assert "reconciling" not in r.gate.reasons  # proven foreign: no pause
    # later events of that order are dropped at once, without another lookup
    r.push_side("fills", {"rows": [fill_row("f-9", "o-9", TK, created_ns=time.time_ns())], "fetched_ns": 0, "since_ns": 0})
    r.process_pending()
    assert len(rest.of("get_order")) == 1 and _delivered(s) == []


async def test_a_fill_whose_order_stays_unknown_times_out_pauses_quoting_and_reconciles():
    """GET order keeps answering 404: after unknown_order_park_s the fill is dropped with an ERROR,
    and quoting pauses through the reconcile path (gate 'reconciling', strategy told
    kalshi.reconcile stale; fills, positions and resting orders re-read), never silently."""
    s, r, rest, rec = _runner(unknown_order_park_s=0.05)
    t = time.time_ns()
    r.push(_fill(t, "", "o-x", "tx"))
    await _drain(r, 5)
    assert "o-x" in r._parked and len(rest.of("get_order")) >= 1  # noqa: SLF001
    await asyncio.sleep(0.06)
    r.push_side("persist")  # any item: the housekeeping sees the timeout
    r.process_pending()
    assert r._parked == {} and _delivered(s) == []  # noqa: SLF001
    assert r.metrics.get("dh_unknown_order_events_dropped_total", type="KalshiFill", source="ws") == 1.0
    assert "reconciling" in r.gate.reasons and UNKNOWN_ORDER_REASON in r._recon  # noqa: SLF001
    stale = [e for e in s.events if isinstance(e, FeedStatus) and e.stream == "kalshi.reconcile"]
    assert stale and stale[-1].status == "stale" and stale[-1].detail == UNKNOWN_ORDER_REASON
    await _drain(r)
    assert rest.of("iter_fills") and rest.of("get_all_positions")  # the reconcile re-read fills + positions
    assert "reconciling" not in r.gate.reasons  # nothing differed: resumed
    assert [e.status for e in s.events if isinstance(e, FeedStatus) and e.stream == "kalshi.reconcile"][-1] == "resynced"


async def test_a_parked_fill_never_confirms_a_position_mismatch():
    """While our fill is parked the exchange position already includes it: the positions check
    must not halt on that 'mismatch' (review NEW-1 consequence); once released, positions agree."""
    from dh.core.events import KalshiPositionSnapshot

    clock = {"t": T0}
    s, r, rest, _ = _runner(clock=lambda: clock["t"], unknown_order_lookup_delay_s=5.0, position_confirm_s=5.0)
    r.push(_fill(T0, "", "o-2", "t1"))
    r.push_side("positions", {TK: 100})  # suspect: exchange 100, ours 0
    r.process_pending()
    clock["t"] += 6 * NS_PER_S
    r.push_side("fills", {"rows": [], "fetched_ns": clock["t"], "since_ns": 0})
    r.push_side("positions", {TK: 100})  # would confirm (-> snapshot -> halt) without the parked fill
    r.process_pending()
    assert not [e for e in s.events if isinstance(e, KalshiPositionSnapshot)]
    assert r.metrics.get("dh_position_confirm_parked_total") == 1.0
    r.push_result(OrderAck(clock["t"], 0, "dhm1-tok-2", "o-2", TK, 0, 200))
    r.push_side("positions", {TK: 100})
    r.process_pending()
    snaps = [e for e in s.events if isinstance(e, KalshiPositionSnapshot)]
    assert s.om.position(TK) == 100 and [x.position for x in snaps] == [100]  # agreement, not a mismatch
    assert r.metrics.get("dh_position_mismatches_total") is None


def test_order_row_owner():
    assert order_row_owner(order_row("dhm1-a", "o", TK, subaccount=1), PREFIX, 1, key_restricted=True)["verdict"] == "ours"
    # the endpoint takes no subaccount: an absent field is ours only for a key restricted to our subaccount
    assert order_row_owner(order_row("dhm1-a", "o", TK), PREFIX, 1, key_restricted=True)["verdict"] == "ours"
    assert order_row_owner(order_row("dhm1-a", "o", TK), PREFIX, 1, key_restricted=False)["verdict"] == "foreign"
    assert order_row_owner(order_row("sys2-a", "o", TK, subaccount=1), PREFIX, 1, key_restricted=True)["verdict"] == "foreign"
    assert order_row_owner(order_row("", "o", TK, subaccount=1), PREFIX, 1, key_restricted=True)["verdict"] == "foreign"
    assert order_row_owner(None, PREFIX, 1, key_restricted=True)["verdict"] == "not_found"


def test_park_config_is_validated_live():
    bad = replace(_shared(), mode="live")
    bad = replace(bad, venue=replace(bad.venue, unknown_order_park_s=0.0))
    assert any("unknown_order_park_s" in p for p in live_config_problems(bad))


# ============================================================================ NEW-2 / NEW-3: watchdog
NOW = 1_790_000_000 * NS_PER_S


def _beat(**kw: Any) -> dict[str, Any]:
    b = {"t": NOW, "pid": 1, "subaccount": 1, "state": "ARMED", "armed": [42, "s"], "api_ok": True, "api_ok_ns": NOW,
         "step_ok": True}
    b.update(kw)
    return b


def test_future_watchdog_beat_is_not_fresh():
    """Probe test_future_beat_accepted: a beat 1 h in the future was accepted as fresh."""
    assert watchdog_beat_problem(_beat(), now_ns=NOW, subaccount=1, max_age_s=10) == ""
    assert watchdog_beat_problem(_beat(t=NOW + 1 * NS_PER_S), now_ns=NOW, subaccount=1, max_age_s=10) == ""  # skew
    p = watchdog_beat_problem(_beat(t=NOW + 3600 * NS_PER_S), now_ns=NOW, subaccount=1, max_age_s=10)
    assert "FUTURE" in p
    assert "FUTURE" in watchdog_beat_problem(_beat(t=NOW + 3 * NS_PER_S), now_ns=NOW, subaccount=1, max_age_s=10)


def test_watchdog_beat_must_prove_api_capability():
    assert "cannot prove" in watchdog_beat_problem(_beat(api_ok=False, api_error="HTTP 401"), now_ns=NOW, subaccount=1,
                                                   max_age_s=10)
    b = _beat()
    del b["api_ok"]
    assert "cannot prove" in watchdog_beat_problem(b, now_ns=NOW, subaccount=1, max_age_s=10)  # an old watchdog
    assert "probe is" in watchdog_beat_problem(_beat(api_ok_ns=NOW - 600 * NS_PER_S), now_ns=NOW, subaccount=1,
                                               max_age_s=10, api_max_age_s=180)
    assert "keeps failing" in watchdog_beat_problem(_beat(step_ok=False), now_ns=NOW, subaccount=1, max_age_s=10)


async def _nosleep(_s: float) -> None:
    await asyncio.sleep(0)


async def test_watchdog_probes_its_key_at_start_and_reports_it_in_the_beat(tmp_path: Path):
    hbp = tmp_path / "hb.json"
    rest = FakeRest()
    calls: list[dict] = []
    fail = {"on": True}

    async def probe():
        calls.append({})
        if fail["on"]:
            raise KalshiHTTPError("GET", "/portfolio/orders", 401, {"error": {"code": "unauthorized"}})
        return {"orders": []}

    w = Watchdog(hbp, rest_scoped_cancel_all(rest, 1, sleep=_nosleep),
                 WatchdogCfg(poll_s=0.01, beat_interval_s=0.01, api_probe_interval_s=0.02), subaccount=1, api_probe=probe)
    write_heartbeat(hbp, {"mode": "live", "state": "running", "pid": 77, "session": "live-a", "subaccount": 1})
    stop = asyncio.Event()
    seen: dict[str, Any] = {}

    async def stopper():
        await asyncio.sleep(0.08)
        seen["bad"] = read_heartbeat(watchdog_beat_path(hbp))
        fail["on"] = False
        await asyncio.sleep(0.08)
        seen["good"] = read_heartbeat(watchdog_beat_path(hbp))
        stop.set()

    await asyncio.gather(w.run(stop), stopper())
    bad, good = seen["bad"], seen["good"]
    assert bad["api_ok"] is False and "401" in bad["api_error"]
    assert "cannot prove" in watchdog_beat_problem(bad, now_ns=bad["t"], subaccount=1, max_age_s=10)
    assert good["api_ok"] is True and good["api_ok_ns"] > 0 and good["step_ok"] is True
    assert watchdog_beat_problem(good, now_ns=good["t"], subaccount=1, max_age_s=10) == ""
    assert len(calls) >= 3  # at start, then every api_probe_interval_s


async def test_watchdog_beat_reports_a_step_that_keeps_failing(tmp_path: Path):
    hbp = tmp_path / "hb.json"

    async def probe():
        return {"orders": []}

    w = Watchdog(hbp, lambda: None, WatchdogCfg(poll_s=0.005, beat_interval_s=0.005), subaccount=1, api_probe=probe)

    async def boom():
        raise OSError("heartbeat dir unreadable")

    w.step = boom  # type: ignore[method-assign]
    stop = asyncio.Event()

    async def stopper():
        await asyncio.sleep(0.08)
        stop.set()

    beats: list[dict] = []
    orig = w.write_beat

    def spy(**kw):
        orig(**kw)
        b = read_heartbeat(watchdog_beat_path(hbp))
        if b:
            beats.append(b)

    w.write_beat = spy  # type: ignore[method-assign]
    await asyncio.gather(w.run(stop), stopper())
    assert any(b.get("step_ok") is False and "unreadable" in b.get("step_error", "") for b in beats)


async def test_rest_api_probe_is_a_scoped_read():
    class R:
        def __init__(self):
            self.kw = None

        async def get_orders(self, **kw):
            self.kw = kw
            return {"orders": [], "cursor": ""}

    r = R()
    await rest_api_probe(r, 1)()
    assert r.kw == {"subaccount": 1, "status": "resting", "limit": 1}


async def test_a_future_stamped_runner_heartbeat_fires_the_watchdog(tmp_path: Path):
    """NEW-3 on the watchdog side: the watched runner's heartbeat jumps 1 h into the future (a wrong
    clock). It must NOT count as fresh forever: once the last trusted heartbeat is stale, it fires."""
    hbp = tmp_path / "hb.json"
    clock = {"t": NOW}
    fired: list[int] = []

    async def cancel() -> bool:
        fired.append(clock["t"])
        return True

    w = Watchdog(hbp, cancel, WatchdogCfg(stale_s=2.0), subaccount=1, clock_ns=lambda: clock["t"])
    write_heartbeat(hbp, {"mode": "live", "state": "running", "pid": 77, "session": "s", "subaccount": 1}, now_ns=NOW)
    assert await w.step() == "ARMED"
    write_heartbeat(hbp, {"mode": "live", "state": "running", "pid": 77, "session": "s", "subaccount": 1},
                    now_ns=NOW + 3600 * NS_PER_S)
    clock["t"] += 1 * NS_PER_S
    assert await w.step() == "ARMED" and not fired  # the last trusted beat is 1 s old
    clock["t"] += 2 * NS_PER_S
    assert await w.step() == "TRIGGERED" and fired  # was: never (last_hb_ns jumped 1 h ahead)
    # a future-stamped heartbeat never (re-)arms either
    w2 = Watchdog(tmp_path / "hb2.json", cancel, WatchdogCfg(stale_s=2.0), subaccount=1, clock_ns=lambda: clock["t"])
    write_heartbeat(tmp_path / "hb2.json", {"mode": "live", "state": "running", "pid": 7, "session": "x", "subaccount": 1},
                    now_ns=clock["t"] + 60 * NS_PER_S)
    assert await w2.step() == "DISARMED"


def test_watchdog_config_capability_keys_are_validated_live():
    c = replace(_shared(), mode="live", watchdog=WatchdogCfg(api_probe_interval_s=60, api_max_age_s=30))
    assert any("api_max_age_s" in p for p in live_config_problems(c))


# ============================================================================ NEW-5
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


async def test_scoped_cancel_all_inside_a_venue_task_does_not_wait_on_itself():
    """Probe test_scoped_cancel_self_wait_and_pagination: the kill ran inside a venue task and
    wait_idle waited the full 2 s on ITSELF every round. Now a round is fast; pagination is still
    followed and the bulk cancel-all is never sent."""
    state = {"resting": {f"o{i}": 2 for i in range(5)}, "lists": 0}

    def handler(method, path, params, body):
        if method == "GET" and path == "/portfolio/orders":
            state["lists"] += 1
            p = dict(params)
            assert p.get("subaccount") == "1"
            ids = sorted(state["resting"])
            page, cur = (ids[3:], "") if p.get("cursor") == "c2" else (ids[:3], "c2" if len(ids) > 3 else "")
            return 200, {"orders": [{"order_id": i, "ticker": "KXBTCD-X", "exchange_index": 2, "client_order_id": "dhm1-a-1",
                                     "status": "resting"} for i in page], "cursor": cur}
        if method == "DELETE" and path == "/portfolio/events/orders/batched":
            out = []
            for it in body["orders"]:
                assert it["subaccount"] == 1 and it["exchange_index"] == 2
                state["resting"].pop(it["order_id"], None)
                out.append({"order_id": it["order_id"], "client_order_id": "dhm1-a-1", "reduced_by": "1.00"})
            return 200, {"orders": out}
        raise AssertionError((method, path))

    tr = Transport(handler)
    rest = KalshiRest("https://fake.invalid/trade-api/v2", None, None, transport=tr, write_subaccount=1, write_shards=(2,),
                      forbid_bulk_cancel=True)

    async def nosleep(_s):
        return None

    v = KalshiVenue(rest, sink=lambda e: None, cfg=SHARED, sleep=nosleep)
    t0 = time.monotonic()
    v.submit([CancelAll(reason="kill")], 1)
    await asyncio.sleep(0)
    assert await v.wait_idle(20)
    el = time.monotonic() - t0
    assert state["resting"] == {}
    assert not any(c[0] == "DELETE" and c[1] == "/portfolio/events/orders" for c in tr.calls)
    assert el < 0.5, el  # was >= 2.0 s per round


async def test_wait_idle_never_waits_on_the_calling_task():
    v = KalshiVenue(object(), sink=lambda e: None, cfg=SHARED)

    async def inner():
        t0 = time.monotonic()
        ok = await v.wait_idle(1.0)
        return ok, time.monotonic() - t0

    t = v._spawn(inner(), "self")  # noqa: SLF001
    ok, el = await t
    assert ok and el < 0.2


# ============================================================================ NEW-6: key proof
@pytest.mark.parametrize("status,expect", [(200, False), (403, True), (401, False), (503, False), (429, False)])
async def test_key_proof_accepts_only_403(status, expect):
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
    if status == 401:
        assert "key rejected, not proven restricted" in why

