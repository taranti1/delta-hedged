"""LiveRunner safety paths from the independent live review (tests/live/test_runner.py has
the basics): consumer backlog, exchange-time lag, stalls, halts in the same cycle, cancel-all
semantics, reconnect reconciliation, fill de-duplication, subaccount scoping, stuck cancels,
position persistence, clock offset, the watchdog's cancel-all marker, risk-state persistence,
a hung shutdown."""

from __future__ import annotations

import asyncio
import json
import time

from dh.core.actions import AmendOrder, CancelAll, CancelOrder, Halt, PlaceOrder
from dh.core.events import (
    FeedStatus,
    IndexTick,
    KalshiFill,
    KalshiOrderUpdate,
    KalshiPositionSnapshot,
    OrderAck,
    OrderReject,
    Timer,
)
from dh.core.units import NS_PER_MS, NS_PER_S
from dh.kalshi.fees import FeeEngine
from dh.live.config import WatchdogCfg
from dh.live.monitor import read_heartbeat, write_json_atomic
from dh.live.riskstate import RiskStateStore
from dh.live.runner import LAG_STREAM, RECONCILE_STREAM
from dh.live.watchdog import Watchdog, rest_cancel_all
from dh.store.codec import decode_event
from dh.store.recorder import Recorder
from dh.store.replay import iter_raw

from .fakes import T0, FakeRest, RecordingStrategy, fill_row, order_row, unknown
from .test_runner import SPEC, TK, OrderingStrategy, cfg, live_runner


def statuses(s, stream):
    return [e.status for e in s.events if isinstance(e, FeedStatus) and e.stream == stream]


def resting(s: OrderingStrategy, coid: str, oid: str, ticker: str = TK, qty: int = 200) -> None:
    s.om.request_place(PlaceOrder(coid, ticker, "bid", 4500, qty), 1)
    s.om.on_event(OrderAck(2, 0, coid, oid, ticker, 0, qty))


# ============================================================================ M2: backlog
async def test_backlog_does_not_starve_cancels_or_heartbeat(tmp_path):
    """1,500 queued events at 1 ms each: a cancel decided on the first one reaches REST at
    once (not after the whole backlog) and the heartbeat keeps being written meanwhile."""
    n = 1500
    marks: dict = {}

    def respond(ev):
        time.sleep(0.001)
        if isinstance(ev, IndexTick) and ev.value == 1.0:
            marks["decided"] = time.monotonic()
            return [CancelOrder("c-1", TK, "oid-1", reason="react")]
        return []

    s = RecordingStrategy(respond)
    rest = FakeRest()
    orig = rest.cancel_order

    async def cancel_order(order_id, **kw):
        marks.setdefault("sent", time.monotonic())
        marks.setdefault("processed", len(s.events))
        return await orig(order_id, **kw)

    rest.cancel_order = cancel_order
    hb = tmp_path / "hb.json"
    beats: set[int] = set()
    r, _, _ = live_runner(s, rest=rest, heartbeat_path=hb, config=cfg(loop={"max_lag_s": 0.0}))

    async def burst():
        t = time.time_ns()
        r.push(IndexTick(t, t, "BRTI", 1.0, "5hz"))
        for i in range(n):
            r.push(IndexTick(t + i + 1, t + i + 1, "BRTI", 2.0, "5hz"))
        while len(s.events) < n:
            h = read_heartbeat(hb)
            if h is not None:
                beats.add(h["t"])
            await asyncio.sleep(0.02)

    r.add_source("burst", burst)
    await r.run(duration_s=n * 0.001 + 1.5)
    assert marks["sent"] - marks["decided"] < 0.1, "a cancel must not wait for the backlog to drain"
    assert marks["processed"] < 100
    assert len(beats) >= 3, "the heartbeat must keep beating while a backlog drains"


# ============================================================================ M1 / m1: lag
async def test_exchange_time_lag_closes_gate_tells_strategy_and_resumes(tmp_path):
    """Frames piling up in the WebSocket buffer are invisible to the runner queue (each event is
    stamped when read, so its queue lag is ~0): the exchange timestamps show the backlog."""
    clock = {"t": T0}

    def respond(ev):
        if isinstance(ev, IndexTick):
            return [PlaceOrder(f"q-{ev.ts}", TK, "bid", 4500, 100)]
        return []

    s = RecordingStrategy(respond)
    rec = Recorder(tmp_path / "data")
    r, venue, rest = live_runner(s, config=cfg(loop={"max_lag_s": 1.0, "lag_resume_s": 1.0, "lag_confirm_s": 0.0}),
                                 clock_ns=lambda: clock["t"], recorder=rec)

    def tick(age_ms: int) -> None:
        clock["t"] += 200 * NS_PER_MS
        r.push(IndexTick(clock["t"], clock["t"] - age_ms * NS_PER_MS, "BRTI", 84000.0, "5hz"))
        r.process_pending()

    for _ in range(5):
        tick(50)  # baseline: 50 ms relay latency
    assert "lag" not in r.gate.reasons and statuses(s, LAG_STREAM) == []
    for k in range(1, 8):
        tick(50 + 300 * k)  # the backlog grows 300 ms per tick; the queue lag stays 0
    assert "lag" in r.gate.reasons and statuses(s, LAG_STREAM) == ["stale"]
    await venue.wait_idle(1.0)
    r.process_pending()
    rej = [e for e in s.events if isinstance(e, OrderReject)]
    assert rej and all(e.reason == "gate:lag" for e in rej)
    for _ in range(3):
        tick(60)  # caught up, but not for lag_resume_s yet ...
    assert "lag" in r.gate.reasons
    for _ in range(4):
        tick(60)  # ... now it has been fresh for >= 1 s
    assert "lag" not in r.gate.reasons and statuses(s, LAG_STREAM) == ["stale", "resumed"]
    rec.close()
    live = [decode_event(x.data) for x in iter_raw(tmp_path / "data", ["events.live"], 0, 2**62)]
    assert [e.status for e in live if isinstance(e, FeedStatus)] == ["stale", "resumed"]  # replayable


async def test_stall_followed_by_an_event_is_gated_before_catch_up_timers():
    def respond(ev):
        if isinstance(ev, Timer):
            return [PlaceOrder(client_order_id=f"s-{ev.ts}", ticker=TK, book_side="bid", px=4500, qty=100)]
        return []

    s = RecordingStrategy(respond)
    clock = {"t": T0}
    r, venue, rest = live_runner(s, config=cfg(), clock_ns=lambda: clock["t"])  # max_lag_s = 5
    r.push(IndexTick(T0, T0, "BRTI", 84000.0, "5hz"))
    r.process_pending()
    clock["t"] = T0 + 8 * NS_PER_S  # the loop stalled 8 s; the first item after it is an EVENT
    r.push(IndexTick(clock["t"], clock["t"] - 7 * NS_PER_S, "BRTI", 84000.0, "5hz"))
    r.process_pending()
    await venue.wait_idle(1.0)
    assert rest.of("create_order") == [] and rest.of("batch_create_orders") == []
    lag_idx = next(i for i, e in enumerate(s.events) if isinstance(e, FeedStatus) and e.stream == LAG_STREAM)
    catch_up = [i for i, e in enumerate(s.events) if isinstance(e, Timer) and e.ts > T0]
    assert len(catch_up) >= 150 and lag_idx < min(catch_up), "the strategy is told before the catch-up cycles run"


# ============================================================================ m5 / m13 / cancel-all
async def test_halt_closes_the_gate_before_orders_of_the_same_cycle():
    def respond(ev):
        if isinstance(ev, IndexTick):
            return [PlaceOrder("p-1", TK, "bid", 4500, 100), CancelAll("daily_loss"), Halt("daily_loss", "all")]
        return []

    s = RecordingStrategy(respond)
    r, venue, rest = live_runner(s)
    r.push(IndexTick(time.time_ns(), 0, "BRTI", 84000.0, "5hz"))
    r.process_pending()
    await venue.wait_idle(1.0)
    r.process_pending()
    assert rest.of("create_order") == []
    rej = [e for e in s.events if isinstance(e, OrderReject)]
    assert [(e.client_order_id, e.reason) for e in rej] == [("p-1", "gate:halt:all")]
    assert rest.names().count("cancel_all_orders") == 1  # with a Halt: the REST cancel-all
    assert {"halt:all", "cancel_all_hold"} <= set(r.gate.reasons)


async def test_amend_is_gated_like_a_place():
    def respond(ev):
        if isinstance(ev, IndexTick):
            return [AmendOrder("a-1", "a-2", TK, "oid-1", "bid", 4600, 200)]
        return []

    s = RecordingStrategy(respond)
    r, venue, rest = live_runner(s)
    r.gate.close("kill", 1)
    r.push(IndexTick(time.time_ns(), 0, "BRTI", 84000.0, "5hz"))
    r.process_pending()
    await venue.wait_idle(1.0)
    r.process_pending()
    assert rest.of("amend_order") == []
    (rej,) = [e for e in s.events if isinstance(e, OrderReject)]
    assert rej.request == "amend" and rej.reason == "gate:kill"


async def test_cancel_all_without_halt_cancels_our_orders_and_sweeps_no_rest_cancel_all():
    s = OrderingStrategy(n=0)
    rest = FakeRest()
    r, venue, _ = live_runner(s, rest=rest)
    for coid, oid in (("c-1", "o-1"), ("c-2", "o-2")):
        resting(s, coid, oid)
        rest.orders[oid] = order_row(coid, oid, TK)
    rest.orders["o-9"] = order_row("ghost", "o-9", TK)  # e.g. a create whose outcome was unknown

    def respond(ev):
        if isinstance(ev, IndexTick):
            return [CancelOrder("c-1", TK, "o-1", reason="runner_lag"), CancelAll("runner_lag")]
        return []

    s.respond = respond
    r.push(IndexTick(time.time_ns(), 0, "BRTI", 84000.0, "5hz"))
    r.process_pending()
    for _ in range(3):
        await asyncio.sleep(0.02)
        await venue.wait_idle(1.0)
    assert "cancel_all_orders" not in rest.names(), "no REST cancel-all (it cancels orders placed in the next minute)"
    cancelled = {a[0] for a, _ in rest.of("cancel_order")} | {x["order_id"] for a, _ in rest.of("batch_cancel_orders") for x in a[0]}
    assert cancelled == {"o-1", "o-2", "o-9"}
    assert rest.of("iter_orders")[0][1] == {"status": "resting", "subaccount": 0}
    assert "cancel_all_hold" not in r.gate.reasons


# ============================================================================ M3: reconnect
async def test_reconnect_backfills_the_lost_fill_and_resyncs():
    """A fill during a WS outage is never re-sent and the private channels have no seq: the
    runner reconciles (strategy told: kalshi.reconcile stale ... resynced) and back-fills it."""
    s = OrderingStrategy(n=0)
    rest = FakeRest()
    clock = {"t": T0}
    r, venue, _ = live_runner(s, rest=rest, clock_ns=lambda: clock["t"], config=cfg(ghost_sweep=True))
    resting(s, "c-1", "o-1")
    rest.orders["o-1"] = order_row("c-1", "o-1", TK, filled="1.00", remaining="1.00")
    rest.fills = [fill_row("f-2", "o-1", TK, px="0.4500", count="1.00", created_ns=T0 + 2 * NS_PER_S, coid="c-1")]
    rest.positions = {TK: "1.00"}
    r.push(FeedStatus(T0 + NS_PER_S, 0, "kalshi.ws", "disconnected", "reset"))
    r.process_pending()
    assert statuses(s, RECONCILE_STREAM) == ["stale"] and "reconciling" in r.gate.reasons
    clock["t"] = T0 + 5 * NS_PER_S
    r.push(FeedStatus(clock["t"], 0, "kalshi.ws", "connected", "url"))
    r.process_pending()
    await asyncio.sleep(0.05)
    r.process_pending()
    (_, kw), = rest.of("iter_fills")
    assert kw == {"min_ts": (T0 + NS_PER_S) // NS_PER_S - 120, "subaccount": 0}  # since the disconnect, minus a margin
    fills = [e for e in s.events if isinstance(e, KalshiFill)]
    assert [f.trade_id for f in fills] == ["f-2"] and s.om.position(TK) == 100
    assert s.om.stats["position_mismatches"] == 0
    assert statuses(s, RECONCILE_STREAM) == ["stale", "resynced"] and "reconciling" not in r.gate.reasons
    # ordering: the back-filled fill reached the strategy BEFORE 'resynced'
    i_fill = next(i for i, e in enumerate(s.events) if isinstance(e, KalshiFill))
    i_res = max(i for i, e in enumerate(s.events) if isinstance(e, FeedStatus) and e.stream == RECONCILE_STREAM)
    assert i_fill < i_res


async def test_reconnect_waits_for_a_position_mismatch_to_resolve():
    s = OrderingStrategy(n=0)
    rest = FakeRest()
    rest.positions = {TK: "3.00"}  # the exchange holds 3, we know of none, and REST has no fill
    clock = {"t": T0}
    r, venue, _ = live_runner(s, rest=rest, clock_ns=lambda: clock["t"], config=cfg(position_confirm_s=0.0))
    r.push(FeedStatus(T0 + 1, 0, "kalshi.ws", "disconnected", ""))
    r.push(FeedStatus(T0 + 2, 0, "kalshi.ws", "connected", ""))
    for _ in range(5):
        r.process_pending()
        await asyncio.sleep(0.02)
        clock["t"] += NS_PER_S
    r.process_pending()
    snaps = [e for e in s.events if isinstance(e, KalshiPositionSnapshot)]
    assert snaps and snaps[-1].position == 300 and s.om.stats["position_mismatches"] == 1  # -> Halt(all) in the MM
    assert statuses(s, RECONCILE_STREAM)[-1] == "resynced"


async def test_rest_backfill_goes_through_fee_check_and_ws_copy_is_dropped():
    s = RecordingStrategy()
    r, venue, rest = live_runner(s, fee_engine=FeeEngine.from_config())
    # a maker fill whose REST fee is 0 under quadratic_with_maker_fees: a fee mismatch
    r.push_side("fills", [fill_row("f-1", "o-1", TK, px="0.4500", count="2.00", fee="0.000000", created_ns=T0, coid="c-1")])
    r.process_pending()
    assert "fee_mismatch" in r.gate.reasons, "back-filled fills get the same fee check as WS fills"
    ws_copy = KalshiFill(time.time_ns(), 0, TK, "f-1", "o-1", "c-1", "bid", 4500, 200, False, 0, 200)
    r.push(ws_copy)
    r.process_pending()
    assert [e.trade_id for e in s.events if isinstance(e, KalshiFill)] == ["f-1"]
    assert r.metrics.get("dh_duplicate_fills_dropped_total") == 1


async def test_periodic_fill_backfill_runs():
    s = RecordingStrategy()
    rest = FakeRest()
    rest.fills = [fill_row("f-7", "o-7", TK, px="0.4500", count="1.00", created_ns=time.time_ns())]
    r, venue, _ = live_runner(s, rest=rest, config=cfg(fills_backfill_interval_s=0.05))
    await r.run(duration_s=0.3)
    calls = rest.of("iter_fills")
    assert len(calls) >= 2 and all(k["subaccount"] == 0 for _, k in calls)
    assert [e.trade_id for e in s.events if isinstance(e, KalshiFill)] == ["f-7"]  # once, however often it is fetched


# ============================================================================ M4: subaccounts
async def test_other_subaccounts_events_are_dropped():
    s = OrderingStrategy(n=0)
    from dh.live.config import VenueCfg

    r, venue, rest = live_runner(s, config=cfg(subaccount=3), venue_cfg=VenueCfg(subaccount=3))
    t = time.time_ns()
    r.push(KalshiFill(t, 0, TK, "t-0", "x", "manual-1", "bid", 4500, 300, True, 0, 300, subaccount=0))
    r.push(KalshiFill(t + 1, 0, TK, "t-3", "o-3", "c-3", "bid", 4500, 100, False, 0, 100, subaccount=3))
    r.push(KalshiOrderUpdate(t + 2, 0, TK, "x", "manual-1", "resting", "bid", 4500, 300, 0, 300, subaccount=0))
    r.push(KalshiPositionSnapshot(t + 3, 0, TK, 300, subaccount=0))
    r.process_pending()
    assert [e.trade_id for e in s.events if isinstance(e, KalshiFill)] == ["t-3"]
    assert not any(isinstance(e, (KalshiOrderUpdate, KalshiPositionSnapshot)) for e in s.events)
    assert r.metrics.get("dh_foreign_subaccount_events_total", type="KalshiFill") == 1


# ============================================================================ C2: stuck cancels
async def test_sweep_recancels_an_order_still_resting_after_its_cancel():
    s = OrderingStrategy(n=0)
    rest = FakeRest()
    clock = {"t": T0}
    r, venue, _ = live_runner(s, rest=rest, clock_ns=lambda: clock["t"], config=cfg(ghost_sweep=True))
    resting(s, "c-1", "o-1")
    rest.orders["o-1"] = order_row("c-1", "o-1", TK)
    rest.on("cancel_order", *[unknown("DELETE")] * 3)

    def respond(ev):
        s.om.on_event(ev)
        if isinstance(ev, IndexTick):
            a = CancelOrder("c-1", TK, "o-1", reason="requote")
            return [a] if s.om.request_cancel(a, ev.ts) else []
        return []

    s.respond = respond
    r.push(IndexTick(T0, T0, "BRTI", 84000.0, "5hz"))
    r.process_pending()
    await venue.wait_idle(1.0)
    n0 = len(rest.of("cancel_order"))
    clock["t"] = T0 + 5 * NS_PER_S  # within the change timeout: the sweep leaves it alone
    r.push_side("resting", [{"order_id": "o-1", "client_order_id": "c-1", "ticker": TK}])
    r.process_pending()
    await venue.wait_idle(1.0)
    assert len(rest.of("cancel_order")) == n0
    clock["t"] = T0 + 11 * NS_PER_S  # still resting 11 s after the cancel went out
    r.push_side("resting", [{"order_id": "o-1", "client_order_id": "c-1", "ticker": TK}])
    r.process_pending()
    await venue.wait_idle(1.0)
    assert len(rest.of("cancel_order")) == n0 + 1 and "o-1" in venue._orders  # noqa: SLF001 - and the venue keeps checking


# ============================================================================ m3: positions
async def test_constant_discrepancy_is_confirmed_while_fills_keep_coming():
    s = OrderingStrategy(n=0)
    clock = {"t": T0}
    r, _, _ = live_runner(s, config=cfg(position_confirm_s=5.0), clock_ns=lambda: clock["t"])
    for k in range(3):  # one fill lost at the start; trading goes on (exchange and ours both move)
        r.push_side("positions", {TK: 100 * (k + 1)})
        r.process_pending()
        s.om.on_event(KalshiFill(clock["t"] + 1, 0, TK, f"tr-{k}", "o-1", "", "bid", 4500, 100, False, 0, 0, False))
        clock["t"] += 6 * NS_PER_S
    assert s.om.stats["position_mismatches"] >= 1


# ============================================================================ m9 / watchdog marker
async def test_persistent_clock_offset_blocks_orders():
    s = RecordingStrategy()
    r, _, _ = live_runner(s, config=cfg(loop={"clock_block_ms": 100.0, "clock_block_samples": 2}))
    r.note_clock_offset(0.2, T0)
    assert "clock" not in r.gate.reasons  # one bad sample is an alarm, not a block
    r.note_clock_offset(-0.3, T0 + 1)
    assert "clock" in r.gate.reasons
    r.note_clock_offset(0.001, T0 + 2)
    assert "clock" not in r.gate.reasons


async def test_watchdog_cancel_all_marker_holds_orders_and_reconciles(tmp_path):
    s = OrderingStrategy(n=0)
    clock = {"t": T0}
    marker = tmp_path / "hb.json.cancel_all"
    r, venue, rest = live_runner(s, clock_ns=lambda: clock["t"], cancel_all_marker=marker,
                                 config=cfg(cancel_all_hold_s=60.0))
    write_json_atomic(marker, {"t": T0, "ok": True, "by": "watchdog"})
    r.push(IndexTick(T0 + 1, T0, "BRTI", 84000.0, "5hz"))
    r.process_pending()
    r.process_pending()
    assert {"cancel_all_hold", "reconciling"} <= set(r.gate.reasons)
    assert statuses(s, RECONCILE_STREAM) == ["stale"]
    await asyncio.sleep(0.02)
    assert "iter_fills" in rest.names()  # our order view is reconciled too
    clock["t"] = T0 + 61 * NS_PER_S
    r.push(IndexTick(clock["t"], clock["t"], "BRTI", 84000.0, "5hz"))
    r.process_pending()
    r.process_pending()
    assert "cancel_all_hold" not in r.gate.reasons and statuses(s, RECONCILE_STREAM) == ["stale", "resynced"]


# ============================================================================ C1: persistence
async def test_halt_is_persisted_immediately(tmp_path):
    def respond(ev):
        return [CancelAll("x"), Halt("reconciliation:position_mismatch", "all")] if isinstance(ev, IndexTick) else []

    s = RecordingStrategy(respond)
    store = RiskStateStore(tmp_path / "risk.json")
    r, venue, _ = live_runner(s, risk_store=store)
    r.push(IndexTick(time.time_ns(), 0, "BRTI", 84000.0, "5hz"))
    r.process_pending()
    st = store.load()
    assert st.halted and st.halt_reason == "reconciliation:position_mismatch"
    await venue.wait_idle(1.0)


# ============================================================================ m12: hung shutdown
async def test_hung_shutdown_lets_the_heartbeat_go_stale_and_the_watchdog_trigger(tmp_path):
    s = RecordingStrategy()
    rest = FakeRest()
    rest.gate = asyncio.Event()  # every write hangs (the cancel-all at shutdown never returns)
    hb = tmp_path / "hb.json"
    r, venue, _ = live_runner(s, rest=rest, heartbeat_path=hb,
                              config=cfg(loop={"shutdown_timeout_s": 0.4, "heartbeat_interval_s": 0.05}))
    task = asyncio.create_task(r.run(duration_s=0.2))
    await asyncio.sleep(0.3)
    assert read_heartbeat(hb)["state"] == "stopping"
    await asyncio.sleep(0.6)  # > shutdown_timeout_s: no more 'stopping' beats
    h = read_heartbeat(hb)
    assert h["state"] == "stopping" and time.time_ns() - h["t"] > 0.3 * NS_PER_S
    w = Watchdog(hb, rest_cancel_all(FakeRest()), WatchdogCfg(stale_s=0.25), arm_on_start=True)
    assert await w.step() == "TRIGGERED"
    rest.gate.set()
    await asyncio.wait_for(task, 10)


async def test_heartbeat_payload_identifies_the_runner(tmp_path):
    s = RecordingStrategy()
    hb = tmp_path / "hb.json"
    r, _, _ = live_runner(s, heartbeat_path=hb, session_id="live-x-tok")
    await r.run(duration_s=0.2)
    h = json.loads(hb.read_text())
    assert h["session"] == "live-x-tok" and h["mode"] == "live" and h["shutdown_timeout_s"] == 2.0


def test_spec_used_by_tests_is_open():
    assert SPEC.close_ts > T0


# ============================================================================ more untested paths
async def test_fill_post_position_mismatch_reconciles_instead_of_halting():
    """A fill whose post_position disagrees (a fill was lost) no longer halts on the spot: the
    MarketMaker pauses and asks for reconciliation; the runner back-fills the lost fill from
    REST at once, positions agree again, nothing halts."""
    from pathlib import Path

    from dh.strategy.config import load_config
    from dh.strategy.mm import MarketMaker

    scfg = load_config(Path(__file__).resolve().parents[2] / "config" / "m1.yaml")
    mm = MarketMaker(scfg, [SPEC], fee_engine=FeeEngine.from_config(), book_includes_own=True, id_prefix="dhm1-test0001")
    rest = FakeRest()
    rest.fills = [fill_row("tr-1", "o-9", TK, px="0.4500", count="1.00", created_ns=time.time_ns())]  # the lost one
    r, venue, _ = live_runner(mm, rest=rest)
    t = time.time_ns()
    # we knew of no fill; this one says the position is now +2 (ours would be +1)
    r.push(KalshiFill(t, 0, TK, "tr-2", "o-9", "", "bid", 4500, 100, False, 0, 200))
    r.process_pending()
    for _ in range(3):
        await asyncio.sleep(0.02)
        r.process_pending()
    assert rest.of("iter_fills"), "the runner reconciled at once"
    assert mm.om.position(TK) == 200 and not mm.risk.halted_all and "halt:all" not in r.gate.reasons
    await venue.wait_idle(1.0)


async def test_blocked_loop_and_dead_consumer_let_the_heartbeat_go_stale(tmp_path):
    import threading

    hb = tmp_path / "hb.json"
    samples: list[tuple[float, float]] = []  # (seconds since start, heartbeat age)
    marks: dict[str, float] = {}

    def respond(ev):
        if isinstance(ev, IndexTick) and ev.value == 1.0:
            time.sleep(1.2)  # the whole event loop is blocked
        return []

    s = RecordingStrategy(respond)
    r, _, _ = live_runner(s, heartbeat_path=hb)
    stop = threading.Event()
    t0 = time.monotonic()

    def probe():  # outside the event loop, like the watchdog process
        while not stop.is_set():
            h = read_heartbeat(hb)
            if h is not None:
                samples.append((time.monotonic() - t0, (time.time_ns() - h["t"]) / NS_PER_S))
            time.sleep(0.05)

    th = threading.Thread(target=probe)
    th.start()

    async def feed():
        await asyncio.sleep(0.2)
        marks["block"] = time.monotonic() - t0
        r.push(IndexTick(time.time_ns(), 0, "BRTI", 1.0, "5hz"))
        await asyncio.sleep(0.6)
        marks["dead"] = time.monotonic() - t0
        r._consumer_task.cancel()  # noqa: SLF001 - the consumer dies (the loop itself is fine)
        await asyncio.sleep(5)

    r.add_source("feed", feed)
    await r.run(duration_s=3.6)
    stop.set()
    th.join()
    blocked = [a for t, a in samples if marks["block"] < t < marks["block"] + 1.4]
    dead = [a for t, a in samples if marks["dead"] + 0.5 < t < 3.5]
    assert blocked and max(blocked) > 1.0, "a blocked loop must let the heartbeat age (the watchdog then cancels)"
    assert dead and min(dead) > 0.4 and max(dead) > 1.0, "a dead consumer must not keep beating 'running'"


def test_runner_and_venue_bookkeeping_is_bounded():
    from dh.live.runner import SeenIds
    from dh.live.venue_kalshi import MAX_OID_MAP

    s = RecordingStrategy()
    r, venue, _ = live_runner(s)
    for i in range(60_000):
        r._note_cancel(f"c-{i}", i)  # noqa: SLF001
    assert len(r._cancel_sent) == 50_000 and "c-59999" in r._cancel_sent  # noqa: SLF001
    for i in range(MAX_OID_MAP + 10):
        venue._learn(f"c-{i}", f"o-{i}")  # noqa: SLF001
    assert len(venue._oid_by_coid) == MAX_OID_MAP  # noqa: SLF001
    seen = SeenIds(cap=100)
    seen.add(*[f"t-{i}" for i in range(150)])
    assert "t-149" in seen and "t-0" not in seen


def test_own_activity_frames_carry_no_seq_so_a_lost_fill_shows_no_gap():
    """Why the reconnect reconciliation exists: fill / user_order / market_position messages
    have no seq (asyncapi), so the sequencer cannot see a lost one."""
    import orjson

    from dh.kalshi.sequencer import KalshiWsState, normalize_ws_message

    from .fakes import WsFrames

    w = WsFrames()
    f1, f3 = w.fill(TK, "c-1", "o-1", "t-1"), w.fill(TK, "c-1", "o-1", "t-3")  # t-2 lost
    assert "seq" not in orjson.loads(f1)
    st = KalshiWsState()
    evs = normalize_ws_message(orjson.loads(w.subscribed("fill")), T0, st)
    evs += normalize_ws_message(orjson.loads(f1), T0, st) + normalize_ws_message(orjson.loads(f3), T0 + 1, st)
    assert not [e for e in evs if isinstance(e, FeedStatus) and e.status == "gap"]
    assert [e.trade_id for e in evs if isinstance(e, KalshiFill)] == ["t-1", "t-3"]
