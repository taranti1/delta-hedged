"""Regression tests for the pre-live FINAL check (docs/research/prelive_review_2026-09-25/
PRELIVE_FINAL_CHECK.md): F1 (an order that can never be proven ours looped park -> timeout ->
reconcile -> re-park forever, its market exempt from the mismatch confirmation), F2 (the
watchdog's API probe proved READ access only) and F3 (a backward wall-clock step fired the
watchdog on a healthy runner). Offline: fakes only, nothing reaches Kalshi.

Each test turns one review probe (test_final_park_loop.py, test_trace_park.py,
test_final_watchdog.py) into the FIXED behaviour; each fails on the code before the fix.
"""

from __future__ import annotations

import argparse
import asyncio
import time
import uuid
from dataclasses import replace
from pathlib import Path
from typing import Any

import orjson
import pytest

from dh.core.events import KalshiFill, KalshiPositionSnapshot, OrderAck, RiskStateSeed
from dh.core.units import NS_PER_MS, NS_PER_S
from dh.kalshi.rest import HttpResponse, KalshiHTTPError, KalshiRest, UnscopedWriteError
from dh.live.config import (
    RECONCILE_CONFIRM_ROUNDS,
    WatchdogCfg,
    live_config_problems,
    load_live_config,
)
from dh.live.monitor import (
    read_heartbeat,
    watchdog_beat_path,
    watchdog_beat_problem,
    write_heartbeat,
)
from dh.live.replay import UniverseReplay
from dh.live.riskstate import RiskStateStore
from dh.live.runner import UNKNOWN_ORDER_LOOP_REASON, UNKNOWN_ORDER_REASON, LiveRunner
from dh.live.venue_kalshi import KalshiVenue
from dh.live.watchdog import Watchdog, WriteProbeError, rest_api_probe, rest_write_probe

from .fakes import (
    T0,
    FakeRest,
    RecordingStrategy,
    fill_row,
    http_error,
    kxbtcd_spec,
    order_row,
)
from .test_rereview_fixes import (
    PREFIX,
    TK,
    Capture,
    _delivered,
    _drain,
    _fill,
    _replay,
    _runner,
    _shared,
)
from .test_runner import P_MS, OrderingStrategy, cfg

REPO = Path(__file__).resolve().parents[2]
VERIFY = "get_order_by_id_finds_shard_orders"


async def _spin(r, secs: float) -> None:
    """What the probes did: let background lookups / reconciliations run, keep items flowing."""
    t_end = time.monotonic() + secs
    while time.monotonic() < t_end:
        await asyncio.sleep(0.01)
        r.push_side("persist")
        r.process_pending()


def _fills(s: RecordingStrategy) -> list[KalshiFill]:
    return [e for e in s.events if isinstance(e, KalshiFill)]


# ============================================================================ F1 (a): deliver, never loop
def _live(tmp_path: Path, *, restricted: bool = True, **venue: Any):
    """A live runner on the REAL clock trading a market that is open now (so the positions check
    runs, unlike the probes' fixture market, which had closed on the wall clock). ``restricted``:
    System 1's deployment (subaccount 1, key restricted to it, proven at start). Otherwise a
    primary-account runner with an UNRESTRICTED key (explicit opt-in, non-shared account): a REST
    fill is not ours by construction there, so an unprovable order keeps re-parking."""
    hour = 3600 * NS_PER_S
    spec = kxbtcd_spec(hour_ns=(time.time_ns() // hour + 2) * hour)
    assert spec.ticker == TK
    if restricted:
        v = dict(subaccount=1, shared_account=True, key_restricted_to_subaccount=True, allow_primary_account=False)
    else:
        v = dict(subaccount=0, shared_account=False, key_restricted_to_subaccount=False, allow_primary_account=True)
    v.update(unknown_order_lookup_delay_s=0.0, unknown_order_lookup_retry_s=0.01)
    v.update(venue)
    c = cfg(**v)
    rec = Capture()
    store = RiskStateStore(tmp_path / "risk.json")
    rest = FakeRest()
    s = OrderingStrategy(n=0)
    venue_ = KalshiVenue(rest, sink=lambda e: None, cfg=c.venue)
    r = LiveRunner(s, mode="live", period_ns=P_MS * NS_PER_MS, cfg=c, venue=venue_, universe=[spec],
                   own_id_prefix=PREFIX, series=("KXBTCD",), recorder=rec, risk_store=store)
    venue_.sink = r.push_result
    return s, r, rest, rec, store


def _our_fill_whose_order_stays_unknown(r, rest: FakeRest) -> list[KalshiFill]:
    """The probes' scenario: our WS fill (no client id) of order o-x; GET order keeps 404-ing, the
    list does not show it either, REST fills carry its copy, the exchange position includes it."""
    t = time.time_ns() + 10**6
    rest.fills = [fill_row("f-x", "o-x", TK, created_ns=t)]
    rest.positions = {TK: "1.00"}
    raw = [_fill(t, "", "o-x", "f-x")]
    r.push(raw[0])
    r.process_pending()
    return raw


@pytest.mark.parametrize("park,confirm", [(0.05, 0.02), (0.10, 0.05), (0.30, 0.05)])
async def test_f1_rest_fill_of_our_subaccount_is_delivered_after_the_first_park_timeout(tmp_path: Path, park, confirm):
    """Probe test_unknown_order_loop: GET order keeps 404-ing (e.g. it never sees shard-2 orders),
    the REST copy was re-parked by every 'unknown_order' reconciliation: 0 fills delivered, gate
    closed (or open on the wrong inventory), no halt, forever. Now the REST copy, read with
    GET /portfolio/fills?subaccount=1 under the proven restricted key, is delivered ONCE after the
    first timeout; the loop ends, positions agree, and replay delivers the same."""
    s, r, rest, rec, _ = _live(tmp_path, unknown_order_park_s=park, position_confirm_s=confirm)
    raw = _our_fill_whose_order_stays_unknown(r, rest)
    await _spin(r, 2 * park + 1.0)
    assert [(f.trade_id, f.client_order_id) for f in _fills(s)] == [("f-x", "dhm1-orphan-o-x")]
    assert s.om.position(TK) == 100
    assert r._park_timeouts == {"o-x": 1} and r._parked == {}  # noqa: SLF001 - one timeout, never re-parked
    assert r.metrics.get("dh_unknown_order_events_dropped_total", type="KalshiFill", source="ws") == 1.0
    assert r.metrics.get("dh_unknown_order_events_dropped_total", type="KalshiFill", source="rest") is None
    assert r.metrics.get("dh_unknown_order_fills_delivered_total") == 1.0
    assert "reconciling" not in r.gate.reasons and UNKNOWN_ORDER_REASON not in r._recon  # noqa: SLF001
    assert not [k for k in r.gate.reasons if k.startswith("halt")]
    assert r.metrics.get("dh_position_mismatches_total") is None  # the exchange's 100 is explained
    assert [e.position for e in s.events if isinstance(e, KalshiPositionSnapshot)][-1:] == [100]
    # the loop is over: no more lookups / reconciliations
    n_get, n_fills = len(rest.of("get_order")), len(rest.of("iter_fills"))
    await _spin(r, 0.3)
    assert len(rest.of("get_order")) == n_get and len(rest.of("iter_fills")) == n_fills
    # replay: the raw WS copy is dropped (unknown order id), the recorded delivery is fed at its point
    live = rec.live()
    assert [e for e in live if isinstance(e, KalshiFill)] == _fills(s)
    assert _delivered(_replay(raw, live)) == _delivered(s)


async def test_f1_the_delivered_fill_is_booked_once_even_if_the_ack_turns_up_later(tmp_path: Path):
    s, r, rest, _, _ = _live(tmp_path, unknown_order_park_s=0.05, position_confirm_s=0.02)
    raw = _our_fill_whose_order_stays_unknown(r, rest)
    await _spin(r, 0.5)
    assert len(_fills(s)) == 1
    r.push_result(OrderAck(time.time_ns(), 0, "dhm1-tok-x", "o-x", TK, 0, 100))
    r.push(replace(raw[0], ts=time.time_ns()))  # a late WS copy
    r.process_pending()
    assert len(_fills(s)) == 1 and s.om.position(TK) == 100


# ============================================================================ F1 (c): no exemption, bounded loop
async def test_f1_unprovable_order_confirms_the_mismatch_and_halts_after_n_park_cycles(tmp_path: Path):
    """Probe test_trace_park / test_unknown_order_loop on a key that is not restricted: the order is
    never proven; before, the market stayed exempt from the mismatch confirmation and nothing ever
    escalated. Now, after the first timeout the market confirms normally (the snapshot reaches the
    strategy) and the 3rd timeout of the same order halts: Halt(all) 'unknown_order_loop', gate
    closed, persisted (sticky), a halting RiskStateSeed recorded; then the loop stops."""
    s, r, rest, rec, store = _live(tmp_path, restricted=False, unknown_order_park_s=0.05, position_confirm_s=0.02)
    raw = _our_fill_whose_order_stays_unknown(r, rest)
    await _spin(r, 1.5)
    assert _fills(s) == []  # never proven ours: never delivered on this key
    snaps = [e for e in s.events if isinstance(e, KalshiPositionSnapshot)]
    assert snaps and snaps[0].position == 100  # the mismatch confirmed (was: never, the ticker stayed exempt)
    assert r.metrics.get("dh_position_mismatches_total") >= 1.0
    assert r._park_timeouts["o-x"] == 3  # noqa: SLF001 - exactly the configured cycles
    assert r.metrics.get("dh_unknown_order_loops_total") == 1.0
    assert "halt:all" in r.gate.reasons and r._halt_info["halt:all"][0] == UNKNOWN_ORDER_LOOP_REASON  # noqa: SLF001
    st = store.load()
    assert st.halted and st.halt_reason == UNKNOWN_ORDER_LOOP_REASON and st.halt_scope == "all"
    seeds = [e for e in s.events if isinstance(e, RiskStateSeed)]
    assert [(x.halted, x.halt_reason) for x in seeds] == [(True, UNKNOWN_ORDER_LOOP_REASON)]
    # halted: the order is never parked / looked up / reconciled again
    n_get = len(rest.of("get_order"))
    await _spin(r, 0.4)
    assert r._parked == {} and UNKNOWN_ORDER_REASON not in r._recon  # noqa: SLF001
    assert len(rest.of("get_order")) == n_get and r._park_timeouts["o-x"] == 3  # noqa: SLF001
    # replay: the halting seed and the confirmed snapshots come back from events.live at their points
    s2 = RecordingStrategy()
    rep = UniverseReplay(s2, [], live=True, subaccount=0, key_restricted=False, series=("KXBTCD",), own_id_prefix=PREFIX)
    merged = sorted([(e.ts, 0, i, e) for i, e in enumerate(raw)] + [(e.ts, 1, i, e) for i, e in enumerate(rec.live())],
                    key=lambda x: x[:3])
    for *_, e in merged:
        rep.on_event(e)

    def key(evs):
        return [(type(e).__name__, e.ts) for e in evs if isinstance(e, (KalshiFill, KalshiPositionSnapshot, RiskStateSeed))]

    assert key(s2.events) == key(s.events)


async def test_f1_restricted_key_never_halts_on_a_ws_only_event_before_n_cycles(tmp_path: Path):
    """An order UPDATE (WS, no client id) is not a REST fill: it is never delivered by construction.
    Its order stays unknown: it is dropped once (reconciled) and, never re-read by REST, cannot loop."""
    from dh.core.events import KalshiOrderUpdate

    s, r, rest, _, _ = _live(tmp_path, unknown_order_park_s=0.05, position_confirm_s=0.02)
    r.push(KalshiOrderUpdate(time.time_ns(), 0, TK, "o-u", "", "canceled", "bid", 4500, 0, 0, 100))
    r.process_pending()
    await _spin(r, 0.6)
    assert r._park_timeouts == {"o-u": 1} and r._parked == {}  # noqa: SLF001
    assert not [k for k in r.gate.reasons if k.startswith("halt")] and "reconciling" not in r.gate.reasons


async def test_f1_only_a_first_park_is_exempt_from_the_mismatch_confirmation():
    """_check_positions: a parked event exempts its market only before its order's first timeout."""
    clock = {"t": T0}
    s, r, rest, _ = _runner(clock=lambda: clock["t"], unknown_order_lookup_delay_s=50.0, position_confirm_s=5.0)
    r.push(_fill(T0, "", "o-2", "t1"))
    r.push_side("positions", {TK: 100})
    r.process_pending()
    clock["t"] += 6 * NS_PER_S
    r.push_side("fills", {"rows": [], "fetched_ns": clock["t"], "since_ns": 0})
    r.push_side("positions", {TK: 100})
    r.process_pending()
    assert not [e for e in s.events if isinstance(e, KalshiPositionSnapshot)]  # first park: exempt
    assert r.metrics.get("dh_position_confirm_parked_total") == 1.0
    r._park_timeouts["o-2"] = 1  # noqa: SLF001 - as if it had timed out once and been re-parked
    clock["t"] += NS_PER_S
    r.push_side("fills", {"rows": [], "fetched_ns": clock["t"], "since_ns": 0})
    r.push_side("positions", {TK: 100})
    r.process_pending()
    assert [e.position for e in s.events if isinstance(e, KalshiPositionSnapshot)] == [100]
    assert r.metrics.get("dh_position_mismatches_total") == 1.0


# ============================================================================ F1 (b): list lookup + verify_live
async def test_f1_lookup_falls_back_to_the_subaccount_list_and_reports_get_by_id():
    """GET /portfolio/orders/{id} (no subaccount / shard parameter) never finds our shard-2 order;
    the list GET /portfolio/orders?subaccount=1&ticker=<t> does: the parked fill is released with
    the order's client id, and verify_live reports that the by-id endpoint missed it."""
    rest = FakeRest()
    rest.orders["o-7"] = order_row("dhm1-tok-7", "o-7", TK, subaccount=1, exchange_index=2)
    rest.on("get_order", *[http_error(404, "not_found", "order not found", "GET") for _ in range(20)])
    s, r, rest, rec = _runner(rest)
    raw = [_fill(time.time_ns(), "", "o-7", "t7")]
    r.push(raw[0])
    await _drain(r)
    assert [(f.trade_id, f.client_order_id) for f in _fills(s)] == [("t7", "dhm1-tok-7")]
    assert rest.of("iter_orders")[0][1] == {"subaccount": 1, "ticker": TK, "status": "resting"}  # review L2
    assert r.metrics.get("dh_verify_live", check=VERIFY) == 0.0 and r._verified[VERIFY] is False  # noqa: SLF001
    assert _delivered(_replay(raw, rec.live())) == _delivered(s)


async def test_f1_lookup_by_id_ok_reports_verified():
    rest = FakeRest()
    rest.orders["o-3"] = order_row("dhm1-tok-3", "o-3", TK, subaccount=1, exchange_index=2)
    s, r, rest, _ = _runner(rest)
    r.push(_fill(time.time_ns(), "", "o-3", "t3"))
    await _drain(r)
    assert len(_fills(s)) == 1 and not rest.of("iter_orders")  # found by id: no list read
    assert r.metrics.get("dh_verify_live", check=VERIFY) == 1.0


@pytest.mark.parametrize("by_id", [True, False])
async def test_f1_one_time_session_check_of_get_order_by_id(by_id):
    rest = FakeRest()
    rest.orders["o-1"] = order_row("dhm1-tok-1", "o-1", TK, subaccount=1, exchange_index=2)
    if not by_id:
        rest.on("get_order", *[http_error(404, "not_found", "order not found", "GET") for _ in range(5)])
    s, r, rest, _ = _runner(rest, verify_get_order_after_s=0.01)
    t = time.time_ns()
    r.push_result(OrderAck(t, 0, "dhm1-tok-1", "o-1", TK, 0, 200))
    r.push_result(OrderAck(t + 1, 0, "dhm1-tok-2", "o-2", TK, 0, 200))  # a second ack: no second check
    r.process_pending()
    await _drain(r)
    assert [a for a, _ in rest.of("get_order")] == [("o-1",)]
    assert r.metrics.get("dh_verify_live", check=VERIFY) == (1.0 if by_id else 0.0)


# ============================================================================ F1 (d): config relation
def test_f1_park_and_confirm_relation_is_enforced_live():
    base = replace(_shared(), mode="live")
    assert not [p for p in live_config_problems(base) if "unknown_order" in p]  # defaults: 10 < 3 x 5

    def probs(**v: Any) -> list[str]:
        return [p for p in live_config_problems(replace(base, venue=replace(base.venue, **v))) if "unknown_order" in p]

    assert RECONCILE_CONFIRM_ROUNDS == 3
    assert probs(unknown_order_park_s=15.0, position_confirm_s=5.0)  # == 3 x confirm: refused
    assert probs(unknown_order_park_s=0.30, position_confirm_s=0.05, unknown_order_lookup_delay_s=0.0)  # probe timing
    assert not probs(unknown_order_park_s=14.9, position_confirm_s=5.0)
    assert probs(unknown_order_max_park_cycles=1)


def test_f1_example_config_passes_the_live_checks():
    c = load_live_config(REPO / "config" / "live.example.yaml")
    c = replace(c, mode="live")
    assert c.venue.unknown_order_max_park_cycles >= 2
    assert c.venue.unknown_order_park_s < RECONCILE_CONFIRM_ROUNDS * c.venue.position_confirm_s
    assert c.watchdog.api_probe_interval_s <= c.watchdog.api_write_probe_interval_s
    assert not [p for p in live_config_problems(c) if "unknown_order" in p or "api_write" in p]


# ============================================================================ F2: write capability
class ReadOnlyKey:
    """Probe test_api_probe_passes_with_a_read_only_key: reads succeed, every cancel is 403."""

    def __init__(self, status: int = 403) -> None:
        self.status = status
        self.cancels: list[tuple[str, dict]] = []

    async def get_orders(self, **kw):
        return {"orders": [], "cursor": ""}

    async def cancel_order(self, order_id, **kw):
        self.cancels.append((order_id, kw))
        raise KalshiHTTPError("DELETE", f"/portfolio/events/orders/{order_id}", self.status,
                              {"error": {"code": "forbidden", "message": "missing write scope"}})


async def test_f2_a_read_only_key_no_longer_passes_the_capability_probe(tmp_path: Path):
    key = ReadOnlyKey(403)
    assert isinstance(await rest_api_probe(key, 1)(), dict)  # the read alone still passes ...
    w = Watchdog(tmp_path / "hb.json", lambda: None, WatchdogCfg(), subaccount=1, api_probe=rest_api_probe(key, 1),
                 api_write_probe=rest_write_probe(key, 1, 2))
    assert await w.probe_api() is False  # ... but the watchdog's capability is not proven
    assert w.api_ok is False and "403" in w.api_error and w.write_ok is False
    beat = read_heartbeat(watchdog_beat_path(tmp_path / "hb.json"))
    assert beat["api_ok"] is False and beat["api_write_ok"] is False and "403" in beat["api_write_error"]
    assert watchdog_beat_problem(beat, now_ns=beat["t"], subaccount=1, max_age_s=10)
    (oid, kw), = key.cancels
    assert uuid.UUID(oid).version == 4 and kw == {"subaccount": 1, "exchange_index": 2}


@pytest.mark.parametrize("status", [401, 403])
async def test_f2_write_probe_401_403_is_not_ok(status):
    with pytest.raises(WriteProbeError, match=str(status)):
        await rest_write_probe(ReadOnlyKey(status), 1, 2)()


class Transport:
    def __init__(self, handler) -> None:
        self.calls: list[tuple[str, str, list, Any]] = []
        self.handler = handler

    async def __call__(self, method, url, headers, params, data, timeout_s):
        path = url.split("/trade-api/v2", 1)[1]
        self.calls.append((method, path, list(params), orjson.loads(data) if data else None))
        st, body = self.handler(method, path, list(params))
        return HttpResponse(st, {}, orjson.dumps(body) if body is not None else b"")


def _scoped_rest(handler) -> tuple[KalshiRest, Transport]:
    tr = Transport(handler)
    # exactly the watchdog's client: writes must name subaccount 1 and a shard; no bulk cancel-all
    return KalshiRest("https://fake.invalid/trade-api/v2", None, None, transport=tr, write_subaccount=1,
                      write_shards=(2,), forbid_bulk_cancel=True), tr


async def test_f2_write_probe_through_the_scoped_write_client():
    """The probe passes the write guard (explicit subaccount + shard), is a DELETE of a FRESH random
    uuid4 each time (never a real order id), and a 404 means write capability."""

    def handler(method, path, params):
        assert method == "DELETE" and path.startswith("/portfolio/events/orders/")
        return 404, {"error": {"code": "not_found", "message": "order not found"}}

    rest, tr = _scoped_rest(handler)
    probe = rest_write_probe(rest, 1, 2)
    r1 = await probe()
    await probe()
    assert r1["write_ok"] and r1["status"] == 404
    ids = [c[1].rsplit("/", 1)[1] for c in tr.calls]
    assert len(ids) == 2 and ids[0] != ids[1] and all(uuid.UUID(i).version == 4 for i in ids)
    for method, _path, params, body in tr.calls:
        assert method == "DELETE" and body is None
        assert dict(params) == {"subaccount": "1", "exchange_index": "2"}  # no market_ticker: no auto-routing


@pytest.mark.parametrize("status,body", [(200, {"order_id": "x", "reduced_by": "1.00"}), (500, None), (400, {})])
async def test_f2_write_probe_anything_but_404_is_not_ok(status, body):
    rest, _ = _scoped_rest(lambda m, p, q: (status, body))
    with pytest.raises(WriteProbeError):
        await rest_write_probe(rest, 1, 2)()


async def test_f2_write_probe_is_refused_by_the_guard_for_another_subaccount():
    rest, tr = _scoped_rest(lambda m, p, q: (404, {}))
    with pytest.raises(UnscopedWriteError):
        await rest_write_probe(rest, 0, 2)()  # this client writes only for subaccount 1
    assert tr.calls == []
    with pytest.raises(ValueError):
        rest_write_probe(rest, 1, -1)  # auto-routing needs a ticker: never
    with pytest.raises(ValueError):
        rest_write_probe(rest, None, 2)  # type: ignore[arg-type]


async def test_f2_write_probe_cadence():
    """At start, then every api_write_probe_interval_s while OK; on every read probe while failing."""
    clock = {"t": 1_790_000_000 * NS_PER_S}
    calls: list[int] = []
    fail = {"on": False}

    async def read():
        return {"orders": []}

    async def write():
        calls.append(clock["t"])
        if fail["on"]:
            raise WriteProbeError("HTTP 403")
        return {"write_ok": True}

    w = Watchdog(Path("/nonexistent/hb.json"), lambda: None, WatchdogCfg(api_probe_interval_s=60, api_write_probe_interval_s=600),
                 subaccount=1, api_probe=read, api_write_probe=write, clock_ns=lambda: clock["t"])
    w.write_beat = lambda **kw: None  # type: ignore[method-assign]
    for _ in range(10):  # 10 read probes, 60 s apart
        assert await w.probe_api() is True
        clock["t"] += 60 * NS_PER_S
    assert len(calls) == 1  # the write probe ran at start only (600 s not yet elapsed)
    assert await w.probe_api() is True and len(calls) == 2  # t0 + 600 s
    fail["on"] = True
    clock["t"] += 600 * NS_PER_S
    assert await w.probe_api() is False and w.api_ok is False and "write" in w.api_error
    fail["on"] = False
    clock["t"] += 60 * NS_PER_S
    assert await w.probe_api() is True and len(calls) == 4  # retried at the READ interval while failing


def test_f2_runner_requires_the_write_proof_in_the_beat():
    now = 1_790_000_000 * NS_PER_S
    b = {"t": now, "pid": 1, "subaccount": 1, "state": "ARMED", "armed": None, "api_ok": True, "api_ok_ns": now,
         "step_ok": True, "api_write_ok": True}
    assert watchdog_beat_problem(b, now_ns=now, subaccount=1, max_age_s=10) == ""
    assert "CANCEL" in watchdog_beat_problem({**b, "api_write_ok": False, "api_write_error": "HTTP 403"}, now_ns=now,
                                             subaccount=1, max_age_s=10)
    b.pop("api_write_ok")
    assert "CANCEL" in watchdog_beat_problem(b, now_ns=now, subaccount=1, max_age_s=10)  # an old watchdog


def test_f2_write_probe_interval_is_validated_live():
    base = replace(_shared(), mode="live")
    bad = replace(base, watchdog=WatchdogCfg(api_probe_interval_s=60, api_write_probe_interval_s=30))
    assert any("api_write_probe_interval_s" in p for p in live_config_problems(bad))
    assert not any("api_write_probe_interval_s" in p for p in live_config_problems(base))


async def test_f2_watchdog_script_wires_the_write_probe(tmp_path: Path, monkeypatch):
    import importlib
    import sys

    sys.path.insert(0, str(REPO / "scripts"))
    mod = importlib.import_module("watchdog")
    seen: dict[str, Any] = {}
    real = mod.Watchdog

    def spy(*a, **kw):
        seen.update(kw)
        return real(*a, **kw)

    monkeypatch.setattr(mod, "Watchdog", spy)
    rest = FakeRest()
    rest.on("cancel_order", http_error(404, "not_found", "order not found", "DELETE"))
    write_heartbeat(tmp_path / "hb.json", {"mode": "live", "state": "stopped", "subaccount": 1}, now_ns=1)
    args = argparse.Namespace(live_config=str(REPO / "config" / "live.example.yaml"), heartbeat=str(tmp_path / "hb.json"),
                              once=True, cancel_now=False, arm_on_start=False, max_age_s=0.0)
    assert await mod.amain(args, rest=rest) == 0
    assert (await seen["api_write_probe"]())["write_ok"]
    (oid,), kw = rest.of("cancel_order")[0]
    assert uuid.UUID(oid).version == 4 and kw == {"subaccount": 1, "exchange_index": 2}


# ============================================================================ F3: backward clock step
WALL0 = 1_790_000_000 * NS_PER_S


def _hb(path: Path, t: int, pid: int = 4242, session: str = "live-s1") -> None:
    write_heartbeat(path, {"pid": pid, "mode": "live", "state": "running", "session": session, "subaccount": 1,
                           "shutdown_timeout_s": 10.0, "order_groups": []}, now_ns=t)


async def _nosleep(_s: float) -> None:
    return None


def _wd(tmp_path: Path, clock: dict, calls: list) -> tuple[Watchdog, Path]:
    async def cancel():
        calls.append(clock["t"])
        return True

    hb = tmp_path / "heartbeat.json"
    return Watchdog(hb, cancel, WatchdogCfg(), clock_ns=lambda: clock["t"], sleep=_nosleep, subaccount=1), hb


async def test_f3_backward_wall_step_with_heartbeats_continuing_does_not_fire(tmp_path: Path):
    """Probe test_backward_wall_step_fires_even_though_heartbeats_stay_fresh: a 3 s BACKWARD step on
    the host (both processes share the clock) fired the watchdog on a healthy runner (cancel-all +
    a marker naming it -> sticky halt). Now the runner's next fresh heartbeat resets the stored
    time; even a poll that lands between the step and that heartbeat does not fire."""
    clock = {"t": WALL0}
    calls: list[int] = []
    wd, hb = _wd(tmp_path, clock, calls)
    for _ in range(8):
        _hb(hb, clock["t"])
        await wd.step()
        clock["t"] += NS_PER_S // 4
    assert wd.st.state == "ARMED"
    clock["t"] -= 3 * NS_PER_S  # NTP / timed steps the wall clock back 3 s
    assert await wd.step() == "ARMED"  # the old heartbeat is 'in the future' now: wait for the next one
    clock["t"] += NS_PER_S // 4
    for _ in range(40):  # 10 s: the runner keeps beating, with the stepped clock
        _hb(hb, clock["t"])
        assert await wd.step() == "ARMED"
        clock["t"] += NS_PER_S // 4
    assert calls == [] and not (tmp_path / "heartbeat.json.cancel_all").exists()
    assert wd.back_steps == 1 and wd.st.last_hb_ns <= clock["t"]


async def test_f3_backward_wall_step_with_heartbeats_stopped_fires(tmp_path: Path):
    """The runner died at the step: no fresh heartbeat of it arrives -> fire within stale_s."""
    clock = {"t": WALL0}
    calls: list[int] = []
    wd, hb = _wd(tmp_path, clock, calls)
    for _ in range(8):
        _hb(hb, clock["t"])
        await wd.step()
        clock["t"] += NS_PER_S // 4
    clock["t"] -= 3 * NS_PER_S
    t_step = clock["t"]
    states = []
    for _ in range(16):  # 4 s of polls, the heartbeat file never changes again
        states.append(await wd.step())
        clock["t"] += NS_PER_S // 4
    assert "TRIGGERED" in states and calls
    assert calls[0] - t_step <= int((WatchdogCfg().stale_s + 0.5) * NS_PER_S)
    assert (tmp_path / "heartbeat.json.cancel_all").exists()


async def test_f3_backward_step_then_the_runner_stops_beating_still_fires(tmp_path: Path):
    clock = {"t": WALL0}
    calls: list[int] = []
    wd, hb = _wd(tmp_path, clock, calls)
    for _ in range(8):
        _hb(hb, clock["t"])
        await wd.step()
        clock["t"] += NS_PER_S // 4
    clock["t"] -= 3 * NS_PER_S
    for _ in range(4):  # alive after the step: reset, no fire
        _hb(hb, clock["t"])
        assert await wd.step() == "ARMED"
        clock["t"] += NS_PER_S // 4
    for _ in range(12):  # then it dies
        await wd.step()
        clock["t"] += NS_PER_S // 4
    assert wd.st.state == "TRIGGERED" and calls


async def test_f3_another_writers_heartbeat_never_resets_the_stored_time(tmp_path: Path):
    """Only the watched runner (pid + session) can vouch after a step."""
    clock = {"t": WALL0}
    calls: list[int] = []
    wd, hb = _wd(tmp_path, clock, calls)
    for _ in range(8):
        _hb(hb, clock["t"])
        await wd.step()
        clock["t"] += NS_PER_S // 4
    clock["t"] -= 3 * NS_PER_S
    for _ in range(16):
        _hb(hb, clock["t"], pid=999, session="live-other")  # a second process writing the file
        await wd.step()
        clock["t"] += NS_PER_S // 4
    assert calls
