"""LiveRunner integration tests (offline): one consumer, results through the queue, timers on
the grid, kill file, heartbeat, metrics endpoint, graceful shutdown, paper isolation."""

from __future__ import annotations

import asyncio
import json
import time
from dataclasses import replace

import aiohttp
import pytest

from dh.core.actions import CancelAll, Halt, Log, PlaceOrder
from dh.core.events import (
    IndexTick,
    KalshiBookSnapshot,
    KalshiFill,
    KalshiOrderGroupUpdate,
    KalshiPositionSnapshot,
    KalshiTrade,
    OrderAck,
    OrderReject,
    Timer,
)
from dh.core.units import NS_PER_MS, NS_PER_S
from dh.execution.exchange_sim import KalshiExchangeSim
from dh.execution.latency import LatencyModel
from dh.execution.order_manager import OrderManager
from dh.live.config import LiveConfig, LoopCfg, MetricsCfg, VenueCfg
from dh.live.monitor import KillFile, Metrics, read_heartbeat
from dh.live.runner import LiveRunner
from dh.live.venue_hedge import DisabledHedgeVenue
from dh.live.venue_kalshi import KalshiVenue
from dh.store.codec import decode_event
from dh.store.recorder import Recorder
from dh.store.replay import iter_raw

from .fakes import T0, FakeRest, RecordingStrategy, kxbtcd_spec

SPEC = kxbtcd_spec()
TK = SPEC.ticker
P_MS = 50


def cfg(mode: str = "live", **venue) -> LiveConfig:
    v = dict(positions_interval_s=0.0, queue_positions_interval_s=0.0, ghost_sweep=False)
    v.update(venue)
    return LiveConfig(mode=mode, loop=LoopCfg(heartbeat_interval_s=0.05, kill_check_interval_s=0.02, clock_sample_s=0.0,
                                              metrics_refresh_s=0.05, shutdown_timeout_s=2.0, max_lag_s=5.0),
                      metrics=MetricsCfg(enabled=False), venue=VenueCfg(**v))


def tick(v: float = 84_000.0) -> IndexTick:
    t = time.time_ns()
    return IndexTick(ts=t, ts_exch=t, index_id="BRTI", value=v, feed="5hz")


class OrderingStrategy(RecordingStrategy):
    """Places one order on the first BRTI tick and tracks it with the real OrderManager."""

    def __init__(self, n: int = 1) -> None:
        super().__init__(self._respond)
        self.om = OrderManager()
        self.n = n
        self.placed = 0

    def _respond(self, ev):
        self.om.on_event(ev)
        if isinstance(ev, IndexTick) and self.placed < self.n:
            self.placed += 1
            a = PlaceOrder(client_order_id=f"t-{self.placed}", ticker=TK, book_side="bid", px=4600, qty=200)
            self.om.request_place(a, ev.ts)
            return [a, Log("quote", {"coid": a.client_order_id})]
        return []


def live_runner(strategy, rest=None, tmp_path=None, **kw):
    rest = rest or FakeRest()
    venue = KalshiVenue(rest, sink=lambda e: None, cfg=kw.pop("venue_cfg", cfg().venue))
    r = LiveRunner(strategy, mode="live", period_ns=P_MS * NS_PER_MS, cfg=kw.pop("config", cfg()), venue=venue,
                   universe=[SPEC], **kw)
    venue.sink = r.push_result
    return r, venue, rest


async def _feeder(runner, events_fn, n: int, dt: float):
    for _ in range(n):
        runner.push(events_fn())
        await asyncio.sleep(dt)


async def test_actions_to_rest_results_back_through_queue_single_consumer(tmp_path):
    s = OrderingStrategy()
    rec = Recorder(tmp_path / "data")
    r, venue, rest = live_runner(s, recorder=rec)
    r.add_source("feed", lambda: _feeder(r, tick, 8, 0.02))
    code = await r.run(duration_s=0.4)
    rec.close()
    assert code == 0
    (args, _), = rest.of("create_order")
    assert args[0]["client_order_id"] == "t-1" and args[0]["price"] == "0.4600"
    acks = [e for e in s.events if isinstance(e, OrderAck)]
    assert len(acks) == 1 and s.om.order("t-1").order_id == "oid-1"
    place_ts = next(e.ts for e in s.events if isinstance(e, IndexTick))
    assert acks[0].ts >= place_ts
    # every strategy call ran inside the consumer task, never concurrently (asserted by the fake)
    assert s.tasks == {"runner:consumer"}
    ts = [e.ts for e in s.events]
    assert ts == sorted(ts)
    # the ack was recorded on events.live (replayable)
    recs = [decode_event(x.data) for x in iter_raw(tmp_path / "data", ["events.live"], 0, 2**62)]
    assert any(isinstance(e, OrderAck) and e.client_order_id == "t-1" for e in recs)
    # graceful shutdown: cancel-all via REST + resting check
    assert "cancel_all_orders" in rest.names() and "iter_orders" in rest.names()


async def test_timer_grid_monotonic_with_live_clock():
    s = RecordingStrategy()
    r, _, _ = live_runner(s)
    r.add_source("feed", lambda: _feeder(r, tick, 5, 0.03))
    await r.run(duration_s=0.6)
    timers = [e for e in s.events if isinstance(e, Timer)]
    assert len(timers) >= 6
    p = P_MS * NS_PER_MS
    assert all(t.ts % p == 0 and t.period_ns == p for t in timers)
    assert all(b.ts - a.ts == p for a, b in zip(timers, timers[1:])), "no missing / duplicate grid points"
    ts = [e.ts for e in s.events]
    assert ts == sorted(ts)
    first_input = next(e for e in s.events if not isinstance(e, Timer))
    assert timers[0].ts >= first_input.ts  # no timers before the first input event (replay rule)
    # timers kept flowing after the last input (quiet market)
    last_input = max(e.ts for e in s.events if not isinstance(e, Timer))
    assert timers[-1].ts > last_input + 3 * p


async def test_kill_file_cancels_all_and_stops(tmp_path):
    kf = tmp_path / "KILL"
    hb = tmp_path / "hb.json"
    s = OrderingStrategy()
    r, _, rest = live_runner(s, kill_file=KillFile(kf), heartbeat_path=hb)

    async def killer():
        r.push(tick())
        await asyncio.sleep(0.25)
        assert read_heartbeat(hb)["state"] == "running"
        kf.write_text("operator test")
        await asyncio.sleep(5)

    r.add_source("killer", killer)
    t0 = time.monotonic()
    code = await r.run(duration_s=5.0)
    assert time.monotonic() - t0 < 2.0, "the kill file must stop the runner promptly"
    assert code == 0 and r.stop_reason.startswith("kill file: operator test")
    assert rest.names().count("cancel_all_orders") >= 2  # immediate + shutdown
    assert "kill" in r.gate.reasons
    hbv = read_heartbeat(hb)
    assert hbv["state"] == "stopped" and hbv["mode"] == "live"


async def test_gate_rejects_new_orders_after_halt_but_not_cancels():
    def respond(ev):
        if isinstance(ev, IndexTick):
            if ev.value == 1.0:
                return [CancelAll("risk"), Halt("daily_loss", "all")]
            return [PlaceOrder(client_order_id=f"g-{ev.ts}", ticker=TK, book_side="bid", px=4500, qty=100)]
        return []

    s = RecordingStrategy(respond)
    r, _, rest = live_runner(s)

    async def feed():
        r.push(tick(1.0))
        await asyncio.sleep(0.05)
        r.push(tick(2.0))
        await asyncio.sleep(0.3)

    r.add_source("feed", feed)
    await r.run(duration_s=0.3)
    assert rest.of("create_order") == []
    rej = [e for e in s.events if isinstance(e, OrderReject)]
    assert len(rej) == 1 and rej[0].reason == "gate:halt:all" and rej[0].request == "create"
    assert rest.names().count("cancel_all_orders") >= 1


async def test_heartbeat_and_metrics_endpoint(tmp_path):
    s = RecordingStrategy()
    c = replace(cfg(), metrics=MetricsCfg(enabled=True, host="127.0.0.1", port=0))
    hb = tmp_path / "hb.json"
    r, _, _ = live_runner(s, config=c, heartbeat_path=hb, metrics=Metrics())
    seen: dict = {}

    async def probe():
        r.push(tick())
        await asyncio.sleep(0.2)
        port = r._metrics_server.bound_port  # noqa: SLF001
        async with aiohttp.ClientSession() as http:
            async with http.get(f"http://127.0.0.1:{port}/metrics") as resp:
                seen["metrics"] = (resp.status, await resp.text())
            async with http.get(f"http://127.0.0.1:{port}/health") as resp:
                seen["health"] = (resp.status, await resp.json())
        seen["hb"] = read_heartbeat(hb)
        await asyncio.sleep(1)

    r.add_source("probe", probe)
    await r.run(duration_s=0.5)
    status, text = seen["metrics"]
    assert status == 200 and "dh_events_total{type=\"IndexTick\"} 1" in text and "dh_timers_total" in text
    assert "dh_queue_depth" in text and "dh_gate_closed 0" in text
    hstatus, health = seen["health"]
    assert hstatus == 200 and health["ok"] and health["mode"] == "live" and health["consumer_alive"]
    assert seen["hb"]["state"] == "running" and seen["hb"]["mode"] == "live" and isinstance(seen["hb"]["t"], int)


async def test_order_group_updates_translated_to_logical_id_and_foreign_dropped():
    s = RecordingStrategy()
    r, venue, rest = live_runner(s)
    venue.groups["dh-main"] = "og-7"
    r.push(KalshiOrderGroupUpdate(time.time_ns(), 1, "og-7", "triggered"))
    r.push(KalshiOrderGroupUpdate(time.time_ns(), 2, "someone-else", "triggered"))
    r.process_pending()
    ups = [e for e in s.events if isinstance(e, KalshiOrderGroupUpdate)]
    assert [(u.order_group_id, u.event_type) for u in ups] == [("dh-main", "triggered")]


async def test_ws_position_snapshots_are_checked_not_forwarded():
    """A WS market_position can beat its fill message: it must never reach the strategy's
    OrderManager directly (that would be an instant false Halt(all))."""
    s = OrderingStrategy(n=0)
    clock = {"t": T0}
    r, _, _ = live_runner(s, config=cfg(position_confirm_s=5.0), clock_ns=lambda: clock["t"])
    r.push(KalshiPositionSnapshot(T0, 0, TK, 200, source="ws"))  # exchange already +2 ...
    r.process_pending()
    s.om.on_event(KalshiFill(T0 + 1, 0, TK, "tr-1", "o-1", "", "bid", 4500, 200, False, 0, 200))  # ... fill arrives
    r.push(KalshiPositionSnapshot(T0 + 2, 0, TK, 200, source="ws"))
    r.process_pending()
    snaps = [e for e in s.events if isinstance(e, KalshiPositionSnapshot)]
    assert [(x.position, x.source) for x in snaps] == [(200, "ws_checked")]
    assert s.om.stats["position_mismatches"] == 0
    # a persistent disagreement is confirmed after position_confirm_s
    clock["t"] = T0 + 10 * NS_PER_S
    r.push(KalshiPositionSnapshot(clock["t"], 0, TK, 500, source="ws"))
    clock["t"] = T0 + 16 * NS_PER_S
    r.push(KalshiPositionSnapshot(clock["t"], 0, TK, 500, source="ws"))
    r.process_pending()
    assert [x.position for x in s.events if isinstance(x, KalshiPositionSnapshot)][-1] == 500
    assert s.om.stats["position_mismatches"] == 1


async def test_scoped_cancel_all_uses_order_manager_view():
    s = OrderingStrategy(n=0)
    r, venue, rest = live_runner(s)
    other = "KXBTCD-26SEP2513-T84250.00"
    for coid, oid, t in (("a-1", "o-1", TK), ("a-2", "o-2", TK), ("a-3", "o-3", other)):
        s.om.request_place(PlaceOrder(coid, t, "bid", 4500, 100), 1)
        s.om.on_event(OrderAck(2, 0, coid, oid, t, 0, 100))

    def respond(ev):
        return [CancelAll("book_gap", tickers=(TK,))] if isinstance(ev, IndexTick) else []

    s.respond = respond
    r.push(tick())
    r.process_pending()
    await venue.wait_idle(1.0)
    (args, _), = rest.of("batch_cancel_orders")
    assert sorted(x["order_id"] for x in args[0]) == ["o-1", "o-2"]
    assert "cancel_all_orders" not in rest.names() and "iter_orders" not in rest.names()


async def test_reconcile_requested_backfills_missed_fills():
    s = OrderingStrategy(n=0)
    rest = FakeRest()
    fill_row = {"fill_id": "f-9", "trade_id": "f-9", "order_id": "o-9", "ticker": TK, "market_ticker": TK,
                "outcome_side": "yes", "book_side": "bid", "count_fp": "1.00", "yes_price_dollars": "0.4500",
                "no_price_dollars": "0.5500", "is_taker": False, "fee_cost": "0.000000", "exchange_index": 0,
                "created_time": "2026-09-25T12:00:00Z"}
    seen_row = dict(fill_row, fill_id="f-1", trade_id="f-1")
    rest.fills = [fill_row, seen_row]
    r, venue, _ = live_runner(s, rest=rest)

    def respond(ev):
        s.om.on_event(ev)
        if isinstance(ev, IndexTick):
            return [Log("risk", {"event": "reconcile_requested", "channel": "fill"})]
        return []

    s.respond = respond
    r.push(KalshiFill(time.time_ns(), 0, TK, "f-1", "o-1", "", "bid", 4500, 100, False, 0, 100))  # seen on WS
    r.push(tick())
    r.process_pending()
    await asyncio.sleep(0.05)
    r.process_pending()
    fills = [e for e in s.events if isinstance(e, KalshiFill)]
    assert [f.trade_id for f in fills] == ["f-1", "f-9"]  # only the missed one was added
    assert s.om.position(TK) == 200
    assert rest.of("iter_fills")[0][1]["min_ts"] > 0


async def test_position_reconciliation_two_strike():
    s = OrderingStrategy(n=0)
    c = cfg(position_confirm_s=5.0)
    clock = {"t": T0}  # before SPEC closes
    r, _, _ = live_runner(s, config=c, clock_ns=lambda: clock["t"])
    # our position: +2 contracts from a fill
    s.om.on_event(KalshiFill(1, 0, TK, "tr-1", "o-1", "", "bid", 4500, 200, False, 0, 0, False))
    r.push_side("positions", {TK: 300})  # exchange says 3: maybe a fill in flight
    r.process_pending()
    assert not [e for e in s.events if isinstance(e, KalshiPositionSnapshot)]
    clock["t"] += 6 * NS_PER_S
    r.push_side("positions", {TK: 300})  # still different 6 s later: confirmed
    r.process_pending()
    snaps = [e for e in s.events if isinstance(e, KalshiPositionSnapshot)]
    assert len(snaps) == 1 and snaps[0].position == 300 and snaps[0].source == "rest"
    # the in-flight case: exchange ahead, then our fill arrives -> no snapshot mismatch
    s2 = OrderingStrategy(n=0)
    r2, _, _ = live_runner(s2, config=c, clock_ns=lambda: clock["t"])
    r2.push_side("positions", {TK: 100})
    r2.process_pending()
    s2.om.on_event(KalshiFill(2, 0, TK, "tr-2", "o-2", "", "bid", 4500, 100, False, 0, 0, False))
    clock["t"] += 6 * NS_PER_S
    r2.push_side("positions", {TK: 100})
    r2.process_pending()
    snaps2 = [e for e in s2.events if isinstance(e, KalshiPositionSnapshot)]
    assert [x.position for x in snaps2] == [100]  # agreeing snapshot only
    assert s2.om.stats["position_mismatches"] == 0


async def test_order_reconciliation_ghosts_and_lost_updates():
    s = OrderingStrategy(n=0)
    r, venue, rest = live_runner(s, config=cfg(ghost_sweep=True))
    for coid, oid in (("k-1", "o-k"), ("k-2", "o-k2")):
        s.om.request_place(PlaceOrder(coid, TK, "bid", 4500, 100), 1)
        s.om.on_event(OrderAck(2, 0, coid, oid, TK, 0, 100))
    # the exchange: k-1 rests, k-2 was cancelled (its user_order message was lost), plus a
    # resting order from an old session nobody knows about
    from .fakes import order_row

    rest.orders["o-k2"] = order_row("k-2", "o-k2", TK, status="canceled", remaining="0.00", initial="1.00")
    r.push_side("resting", [{"order_id": "o-k", "client_order_id": "k-1", "ticker": TK},
                            {"order_id": "o-g", "client_order_id": "old-session-7", "ticker": TK}])
    r.process_pending()
    await venue.wait_idle(1.0)
    assert [a for a, _ in rest.of("cancel_order")] == [("o-g",)]
    venue._orders["o-k2"].next_ns = 0  # noqa: SLF001 - due now
    await venue.reconcile_due()
    assert rest.of("get_order")[0][0] == ("o-k2",)
    r.process_pending()
    assert s.om.order("k-2").state.name == "CANCELED" and s.om.order("k-1").state.name == "RESTING"


@pytest.mark.parametrize("reported, ok", [(8663, True), (10_000, True), (0, False), (50_000, False)])
async def test_fee_reconciliation_per_order_rounding(reported, ok):
    """2 contracts at 45c, maker, quadratic_with_maker_fees: trade fee $0.008663, net fee after
    the $0.01 balance rounding $0.01. Both conventions pass; a zero fee (wrong schedule:
    no maker fees) and a gross error close the gate and cancel everything."""
    s = RecordingStrategy()
    from dh.kalshi.fees import FeeEngine

    r, venue, rest = live_runner(s, fee_engine=FeeEngine.from_config())
    r.push(KalshiFill(time.time_ns(), 0, TK, "tr-1", "o-1", "c-1", "bid", 4500, 200, False, reported, 200))
    r.process_pending()
    await venue.wait_idle(1.0)
    assert (not r.gate.closed) is ok
    assert ("cancel_all_orders" in rest.names()) is (not ok)
    if not ok:
        assert r.gate.reasons.keys() == {"fee_mismatch"}


async def test_paper_mode_fills_from_simulator_and_no_rest_orders(tmp_path):
    """Paper: the strategy trades against the live book through the simulator; the REST
    order endpoints are never touched and the fills are the simulator's."""

    class PaperStrategy(RecordingStrategy):
        def __init__(self):
            super().__init__(self._respond)
            self.om = OrderManager()
            self.done = False

        def _respond(self, ev):
            self.om.on_event(ev)
            if isinstance(ev, KalshiBookSnapshot) and not self.done:
                self.done = True
                a = PlaceOrder("p-1", TK, "bid", 4600, 200)  # improves the 45c best bid: first in queue
                self.om.request_place(a, ev.ts)
                return [a]
            return []

    s = PaperStrategy()
    now_hour = (time.time_ns() // (3600 * NS_PER_S) + 2) * 3600 * NS_PER_S
    spec = kxbtcd_spec(hour_ns=now_hour, ticker=TK)  # still open on the real clock
    sim = KalshiExchangeSim(LatencyModel.fixed(submit_ms=5, response_ms=5, ws_ms=5), "conservative", lambda p, q, t: 0,
                            seed=1, id_prefix="paper")
    sim.register_market(spec)
    rest = FakeRest(forbid_all_writes=True)
    rec = Recorder(tmp_path / "data")
    r = LiveRunner(s, mode="paper", period_ns=P_MS * NS_PER_MS, cfg=cfg("paper"), sim=sim, recorder=rec,
                   hedge=DisabledHedgeVenue(lambda e: None), universe=[spec])

    async def feed():
        t = time.time_ns()
        r.push(KalshiBookSnapshot(t, 0, TK, 1, 1, ((4500, 10_000),), ((5300, 10_000),)))
        await asyncio.sleep(0.1)
        t = time.time_ns()
        r.push(KalshiTrade(t, t, TK, "pub-1", 4600, 500, "no"))  # a taker sells YES at 46c
        await asyncio.sleep(1)

    r.add_source("feed", feed)
    await r.run(duration_s=0.4)
    rec.close()
    assert rest.calls == []
    fills = [e for e in s.events if isinstance(e, KalshiFill)]
    assert len(fills) == 1 and fills[0].trade_id.startswith("paper-f") and fills[0].qty == 200
    assert s.om.position(TK) == 200 and sim.position(TK) == 200
    assert any(isinstance(e, OrderAck) and e.order_id.startswith("paper-o") for e in s.events)
    paper = [decode_event(x.data) for x in iter_raw(tmp_path / "data", ["events.paper"], 0, 2**62)]
    assert any(isinstance(e, KalshiFill) for e in paper)
    assert list(iter_raw(tmp_path / "data", ["events.live"], 0, 2**62)) == []


async def test_strategy_exception_fails_safe():
    def boom(ev):
        if isinstance(ev, IndexTick):
            raise RuntimeError("bug")
        return []

    s = RecordingStrategy(boom)
    r, _, rest = live_runner(s)
    r.add_source("feed", lambda: _feeder(r, tick, 3, 0.05))
    code = await r.run(duration_s=2.0)
    assert code == 4 and "strategy error" in r.stop_reason
    assert "cancel_all_orders" in rest.names() and "strategy_error" in r.gate.reasons


async def test_universe_roll_over_uses_add_markets_and_subscribes():
    class MM(RecordingStrategy):
        def __init__(self):
            super().__init__()
            self.specs = {SPEC.ticker: SPEC}

        def add_markets(self, specs):
            new = [s.ticker for s in specs if s.ticker not in self.specs]
            for s in specs:
                self.specs.setdefault(s.ticker, s)
            return new

    s = MM()
    subscribed: list = []

    async def sub(tickers):
        subscribed.append(list(tickers))

    r, _, _ = live_runner(s)
    r.subscribe_markets = sub
    nxt = kxbtcd_spec(84_250.0, hour_ns=SPEC.expiration_ts + 3600 * NS_PER_S,
                      ticker="KXBTCD-26SEP2514-T84250.00")
    r.push_side("universe_add", [SPEC, nxt])
    r.process_pending()
    await asyncio.sleep(0.01)
    assert set(r.universe) == {SPEC.ticker, nxt.ticker} and subscribed == [[nxt.ticker]]


@pytest.mark.parametrize("mode", ["paper", "live"])
def test_mode_requirements(mode):
    s = RecordingStrategy()
    with pytest.raises(ValueError):
        LiveRunner(s, mode=mode, period_ns=NS_PER_S)  # paper needs a sim, live needs a venue


async def test_json_log_gets_log_actions(tmp_path):
    from dh.live.monitor import JsonLog

    s = OrderingStrategy()
    jl = JsonLog(tmp_path / "log.jsonl", "cfg", "sha")
    r, _, _ = live_runner(s, jsonlog=jl)
    r.push(tick())
    r.process_pending()
    await asyncio.sleep(0.05)
    jl.close()
    lines = [json.loads(x) for x in (tmp_path / "log.jsonl").read_text().splitlines()]
    kinds = [x["k"] for x in lines]
    assert "log.quote" in kinds and "action" in kinds
    q = next(x for x in lines if x["k"] == "log.quote")
    assert q["coid"] == "t-1" and q["cfg"] == "cfg" and q["sha"] == "sha"


async def test_loop_stall_blocks_orders_until_fresh_events():
    """Timers caught up after a stall must not send orders decided on stale data."""
    def respond(ev):
        if isinstance(ev, Timer):
            return [PlaceOrder(client_order_id=f"s-{ev.ts}", ticker=TK, book_side="bid", px=4500, qty=100)]
        return []

    s = RecordingStrategy(respond)
    clock = {"t": T0}
    r, _, rest = live_runner(s, config=cfg(), clock_ns=lambda: clock["t"])
    r.push(IndexTick(T0, T0, "BRTI", 84000.0, "5hz"))
    r.process_pending()
    clock["t"] = T0 + 8 * NS_PER_S  # the loop stalled for 8 s (> max_lag_s = 5 s)
    r.queue.put_nowait(__import__("dh.live.runner", fromlist=["Wake"]).Wake(clock["t"]))
    r.process_pending()
    await asyncio.sleep(0.01)
    assert rest.of("create_order") == [] and "lag" in r.gate.reasons
    rejects = [e for e in s.events if isinstance(e, OrderReject)]
    assert rejects and all(e.reason == "gate:lag" for e in rejects)
    r.push(IndexTick(clock["t"], clock["t"], "BRTI", 84000.0, "5hz"))  # fresh data: gate reopens
    r.process_pending()
    assert "lag" not in r.gate.reasons
