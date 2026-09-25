"""Start-up / restart / shutdown safety of the live app (offline, fake REST + fake WS):
carried-over risk state, single instance, paper-only settings refused live, start-up failures
after side effects, SIGTERM, a shutdown that cannot confirm the cancel, the session id prefix,
and bit-for-bit replay across a WebSocket reconnect."""

from __future__ import annotations

import asyncio
import json
import os
import signal
from dataclasses import replace

import pytest

from dh.core.events import IndexTick, RiskStateSeed
from dh.core.units import NS_PER_S
from dh.live.app import LiveApp, Overrides
from dh.live.monitor import read_heartbeat, write_heartbeat
from dh.live.riskstate import DAY_NS, RiskState, RiskStateStore
from dh.store.codec import decode_event
from dh.store.replay import iter_raw

from .fakes import FakeRest, fill_row, http_error, order_row
from .test_app import _setup


def _store(lcfg, mode="live") -> RiskStateStore:
    return RiskStateStore(lcfg.paths.risk_state_for(mode))


def _live_records(tmp_path):
    return [decode_event(x.data) for x in iter_raw(tmp_path / "data", ["events.live"], 0, 2**62)]


async def test_restart_after_a_loss_halts_at_the_daily_limit_in_total(tmp_path):
    """Session 1 lost $24.99 today (persisted; REST agrees less pessimistically). After the
    restart the first event is the seed, and a further loss of 1 cent halts at -$25 in total."""
    rest, fake, lcfg, scfg, tickers = _setup(tmp_path, "live", forbid_writes=False)
    now = __import__("time").time_ns()
    day0 = now - now % DAY_NS
    _store(lcfg).save(RiskState(day0, -24.99, False, "", 0, "live-earlier", "live", now - 60 * NS_PER_S))
    rest.fills = [fill_row("f-1", "o-1", tickers[0], side="bid", px="0.6000", count="10.00", created_ns=now - 3600 * NS_PER_S),
                  fill_row("f-2", "o-2", tickers[0], side="ask", px="0.5000", count="10.00", created_ns=now - 1800 * NS_PER_S)]
    app = LiveApp(scfg, lcfg, "live", Overrides(rest=rest, ws_connect=fake.connect, install_signals=False))
    runner = await app.build()
    assert rest.of("iter_fills")[0][1] == {"min_ts": day0 // NS_PER_S, "subaccount": 0}
    assert app.info["day_pnl_rest"]["pnl_usd"] == pytest.approx(-1.0)  # 10 x (50c - 60c)
    runner.process_pending()  # the seed (and the start-up hold) reach the strategy
    mm = runner.strategy
    assert mm.risk.seed_day_pnl == pytest.approx(-24.99) and not mm.risk.halted_all
    t = runner.clock_ns()
    runner.push(IndexTick(t, t, "BRTI", 84000.0, "5hz"))
    runner.push(IndexTick(t + 400_000_000, t + 400_000_000, "BRTI", 84000.0, "5hz"))  # quote cycles at equity 0
    runner.process_pending()
    assert mm.stats.cycles > 0 and not mm.risk.halted_all
    mm.equity = lambda S=None: -0.01  # this session loses one more cent
    runner.push(IndexTick(t + 800_000_000, t + 800_000_000, "BRTI", 84000.0, "5hz"))
    runner.process_pending()
    assert mm.risk.halted_all and mm.risk.halt_reason == "daily_loss"
    assert "halt:all" in runner.gate.reasons
    st = _store(lcfg).load()
    assert st.halted and st.halt_reason == "daily_loss" and st.day_pnl_usd == pytest.approx(-25.0)
    await runner.shutdown()
    await app.close()
    recs = _live_records(tmp_path)
    assert isinstance(recs[0], RiskStateSeed) and recs[0].day_pnl_usd == pytest.approx(-24.99)


async def test_carried_halt_starts_halted_unless_the_operator_resets(tmp_path):
    rest, fake, lcfg, scfg, _ = _setup(tmp_path, "live", forbid_writes=False)
    now = __import__("time").time_ns()
    _store(lcfg).save(RiskState(now - now % DAY_NS, -3.0, True, "reconciliation:position_mismatch", 0, "x", "live", now))
    app = LiveApp(scfg, lcfg, "live", Overrides(rest=rest, ws_connect=fake.connect, install_signals=False))
    runner = await app.build()
    runner.process_pending()
    assert runner.strategy.risk.halted_all and "halt:all" in runner.gate.reasons
    assert runner.strategy.risk.halt_reason == "carried_over:reconciliation:position_mismatch"
    await runner.shutdown()
    await app.close()
    # the operator investigated: --reset-daily-halt
    (tmp_path / "b").mkdir()
    rest2, fake2, _, _, _ = _setup(tmp_path / "b", "live", forbid_writes=False)
    app2 = LiveApp(scfg, lcfg, "live", Overrides(rest=rest2, ws_connect=fake2.connect, install_signals=False),
                   reset_daily_halt=True)
    runner2 = await app2.build()
    runner2.process_pending()
    assert not runner2.strategy.risk.halted_all and "halt:all" not in runner2.gate.reasons
    assert app2.info["risk_seed"]["overridden"]["halted"] is True
    await runner2.shutdown()
    await app2.close()
    logs = [json.loads(x) for f in (tmp_path / "logs").iterdir() for x in f.read_text().splitlines()]
    assert any(x["k"] == "risk_reset_by_operator" for x in logs)


async def test_second_instance_is_refused(tmp_path):
    rest, fake, lcfg, scfg, _ = _setup(tmp_path, "paper", forbid_writes=True)
    a = LiveApp(scfg, lcfg, "paper", Overrides(rest=rest, ws_connect=fake.connect, install_signals=False))
    await a.build()
    b = LiveApp(scfg, lcfg, "paper", Overrides(rest=FakeRest(forbid_all_writes=True), ws_connect=fake.connect,
                                                install_signals=False))
    assert await b.run(duration_s=0.5) == 2  # the same data_root / heartbeat: refused
    await a.runner.shutdown()
    await a.close()
    # a heartbeat that is fresh from another (unlocked) process also refuses the start
    write_heartbeat(tmp_path / "run" / "hb.paper.json", {"pid": os.getpid() + 1, "mode": "paper", "state": "running"})
    c = LiveApp(scfg, lcfg, "paper", Overrides(rest=FakeRest(forbid_all_writes=True), ws_connect=fake.connect,
                                                install_signals=False))
    assert await c.run(duration_s=0.5) == 2


@pytest.mark.parametrize("change", [
    {"loop": {"strategy_error": "continue"}},
    {"venue": {"exclude_events_with_positions": False}},
    {"venue": {"startup_cancel_all": False}},
])
async def test_live_refuses_paper_only_settings(tmp_path, change):
    rest, fake, lcfg, scfg, _ = _setup(tmp_path, "live", forbid_writes=True)
    for section, kv in change.items():
        lcfg = replace(lcfg, **{section: replace(getattr(lcfg, section), **kv)})
    app = LiveApp(scfg, lcfg, "live", Overrides(rest=rest, ws_connect=fake.connect, install_signals=False))
    assert await app.run(duration_s=0.5) == 2 and rest.calls == [] and fake.conns == []


@pytest.mark.parametrize("failure", ["order_group", "positions", "leftover"])
async def test_startup_failure_after_side_effects_leaves_nothing_behind(tmp_path, failure):
    rest, fake, lcfg, scfg, _ = _setup(tmp_path, "live", forbid_writes=False)
    if failure == "order_group":
        rest.on("create_order_group", http_error(400, "bad", "no groups for you"))
    elif failure == "positions":
        rest.on("get_all_positions", ConnectionError("network down"))
    else:
        rest.orders["o-x"] = order_row("x", "o-x", "KXBTCD-X")

        async def cancel_order(order_id, **kw):  # a leftover that refuses to die
            rest.calls.append(("cancel_order", (order_id,), kw))
            raise http_error(500, "internal", "no", "DELETE")

        async def cancel_all(**kw):
            rest.calls.append(("cancel_all_orders", (), kw))
            return {}

        rest.cancel_order = cancel_order
        rest.cancel_all_orders = cancel_all
    app = LiveApp(scfg, lcfg, "live", Overrides(rest=rest, ws_connect=fake.connect, install_signals=False))
    assert await app.run(duration_s=1.0) == 2
    names = rest.names()
    assert "cancel_all_orders" in names and "create_order" not in names and "batch_create_orders" not in names
    assert fake.conns == []  # never subscribed, never quoted
    assert read_heartbeat(tmp_path / "run" / "hb.json")["state"] == "stopped"
    # locks released: the next start is not refused as a second instance
    (tmp_path / "again").mkdir()
    rest2, fake2, _, _, _ = _setup(tmp_path / "again", "live", forbid_writes=False)
    app2 = LiveApp(scfg, lcfg, "live", Overrides(rest=rest2, ws_connect=fake2.connect, install_signals=False))
    runner2 = await app2.build()
    await runner2.shutdown()
    await app2.close()


async def test_sigterm_stops_gracefully_with_cancel_all(tmp_path):
    rest, fake, lcfg, scfg, _ = _setup(tmp_path, "live", forbid_writes=False)
    app = LiveApp(scfg, lcfg, "live", Overrides(rest=rest, ws_connect=fake.connect, install_signals=True))
    task = asyncio.create_task(app.run(duration_s=20.0))
    for _ in range(100):
        await asyncio.sleep(0.05)
        if app.runner is not None and app.runner._stop_evt is not None:  # noqa: SLF001 - running
            break
    assert signal.getsignal(signal.SIGTERM) not in (signal.SIG_DFL, None), "the handler must be installed"
    n_before = rest.names().count("cancel_all_orders")
    os.kill(os.getpid(), signal.SIGTERM)
    code = await asyncio.wait_for(task, 15)
    assert code == 0 and app.runner.stop_reason == "signal SIGTERM"
    assert rest.names().count("cancel_all_orders") > n_before and "delete_order_group" in rest.names()
    assert read_heartbeat(tmp_path / "run" / "hb.json")["state"] == "stopped"


async def test_shutdown_that_cannot_confirm_the_cancel_exits_3_without_stopped_heartbeat(tmp_path):
    rest, fake, lcfg, scfg, _ = _setup(tmp_path, "live", forbid_writes=False)
    app = LiveApp(scfg, lcfg, "live", Overrides(rest=rest, ws_connect=fake.connect, install_signals=False))
    runner = await app.build()

    async def breakage():
        await asyncio.sleep(0.3)
        rest.orders["o-stuck"] = order_row("stuck", "o-stuck", "KXBTCD-X")

        async def fail(*a, **kw):
            raise http_error(503, "unavailable", "down", "DELETE")

        rest.cancel_all_orders = fail
        rest.cancel_order = fail
        rest.batch_cancel_orders = fail
        await asyncio.sleep(5)

    runner.add_source("breakage", breakage)
    code = await runner.run(duration_s=0.6)
    await app.close()
    assert code == 3 and not runner.shutdown_ok
    assert read_heartbeat(tmp_path / "run" / "hb.json")["state"] != "stopped"  # the watchdog keeps watching
    assert "delete_order_group" in rest.names()


async def test_session_prefix_is_unique_and_used_for_every_order(tmp_path):
    rest, fake, lcfg, scfg, tickers = _setup(tmp_path, "live", forbid_writes=False)
    a = LiveApp(scfg, lcfg, "live", Overrides(rest=rest, ws_connect=fake.connect, install_signals=False))
    b = LiveApp(scfg, lcfg, "live", Overrides(rest=rest, ws_connect=fake.connect, install_signals=False))
    assert a.id_prefix != b.id_prefix and a.id_prefix.startswith(scfg.run_prefix + "-") and len(a.token) == 8
    from .test_app import _brti

    runner = await a.build()
    runner.add_source("brti", lambda: _brti(runner, 5))
    await runner.run(duration_s=2.0)
    await a.close()
    bodies = [x[0] for x, _ in rest.of("create_order")] + [b for x, _ in rest.of("batch_create_orders") for b in x[0]]
    assert bodies and all(o["client_order_id"].startswith(a.id_prefix + "-") for o in bodies)
    meta = [json.loads(r.data) for r in iter_raw(tmp_path / "data", ["meta"], 0, 2**62)]
    start = next(m for m in meta if m["kind"] == "session_start")
    assert start["id_prefix"] == a.id_prefix and start["subaccount"] == 0


async def test_live_session_with_a_ws_reconnect_replays_bit_for_bit(tmp_path):
    """The reconnect reconciliation (kalshi.reconcile stale -> REST -> resynced) is recorded,
    so a live session with a WebSocket outage replays exactly."""
    import time

    from websockets.exceptions import ConnectionClosedError

    from dh.live.replay import load_session, logged_decisions, replay_session, replayed_decisions

    from ..kalshi.fake_ws import FakeKalshi

    rest, fake, lcfg, scfg, tickers = _setup(tmp_path, "live", forbid_writes=False)
    wide = fake.scripts[0][0]
    fake = FakeKalshi([[wide], [wide]])

    async def connect(url, headers):  # the first connection drops after 0.8 s (after the start-up hold)
        conn = await fake.connect(url, headers)
        if len(fake.conns) == 1:
            orig, deadline = conn.recv, time.monotonic() + 0.8

            async def recv():
                left = deadline - time.monotonic()
                if left <= 0:
                    raise ConnectionClosedError(None, None)
                try:
                    return await asyncio.wait_for(orig(), left)
                except TimeoutError:
                    raise ConnectionClosedError(None, None) from None

            conn.recv = recv
        return conn

    lcfg = replace(lcfg, venue=replace(lcfg.venue, reconnect_settle_s=0.0))
    app = LiveApp(scfg, lcfg, "live", Overrides(rest=rest, ws_connect=connect, install_signals=False))
    runner = await app.build()
    rec = runner.recorder

    async def brti():
        end = time.monotonic() + 5
        while time.monotonic() < end:
            t = runner.clock_ns()
            v = 84_000.0 + (0.5 if (t // 10**8) % 2 else -0.5)
            ev = IndexTick(ts=t, ts_exch=t, index_id="BRTI", value=v, feed="5hz")
            rec.write_event("events.test", ev)
            runner.push(ev)
            await asyncio.sleep(0.03)

    runner.add_source("brti", brti)
    await runner.run(duration_s=3.0)
    await app.close()
    assert len(fake.conns) == 2, "the WebSocket reconnected"
    recs = _live_records(tmp_path)
    rstat = [e.status for e in recs if getattr(e, "stream", "") == "kalshi.reconcile"]
    assert rstat[:2] == ["stale", "resynced"] and rstat.count("stale") >= 2  # start-up hold + the reconnect
    info = load_session(tmp_path / "data")
    res = replay_session(tmp_path / "data", scfg, extra_streams=("events.test",))
    log = next((tmp_path / "logs").iterdir())
    live_a, live_l = logged_decisions(log, info.last_ts)
    rep_a, rep_l = replayed_decisions(res, info.last_ts)
    assert any(a[1] == "PlaceOrder" for a in live_a)
    assert live_a == rep_a and live_l == rep_l

async def test_live_session_with_a_lag_episode_replays_bit_for_bit(tmp_path):
    """The runner's lag decisions (runner.lag stale/resumed, injected after the market event
    that caused them) are recorded, so a session with a data-lag episode replays exactly."""
    import time

    from dh.live.replay import load_session, logged_decisions, replay_session, replayed_decisions

    rest, fake, lcfg, scfg, tickers = _setup(tmp_path, "live", forbid_writes=False)
    lcfg = replace(lcfg, loop=replace(lcfg.loop, max_lag_s=1.0, lag_resume_s=0.3, lag_confirm_s=0.0))
    app = LiveApp(scfg, lcfg, "live", Overrides(rest=rest, ws_connect=fake.connect, install_signals=False))
    runner = await app.build()
    rec = runner.recorder

    async def brti():
        start = time.monotonic()
        while time.monotonic() - start < 5:
            t = runner.clock_ns()
            el = time.monotonic() - start
            behind = int(1.5e9) if 1.0 < el < 1.6 else 0  # 0.6 s of ticks whose source time is 1.5 s old
            ev = IndexTick(ts=t, ts_exch=t - behind, index_id="BRTI", value=84_000.0, feed="5hz")
            rec.write_event("events.test", ev)
            runner.push(ev)
            await asyncio.sleep(0.03)

    runner.add_source("brti", brti)
    await runner.run(duration_s=2.6)
    await app.close()
    recs = _live_records(tmp_path)
    assert [e.status for e in recs if getattr(e, "stream", "") == "runner.lag"] == ["stale", "resumed"]
    info = load_session(tmp_path / "data")
    res = replay_session(tmp_path / "data", scfg, extra_streams=("events.test",))
    log = next((tmp_path / "logs").iterdir())
    live_a, live_l = logged_decisions(log, info.last_ts)
    rep_a, rep_l = replayed_decisions(res, info.last_ts)
    assert any(a[1] == "PlaceOrder" for a in live_a)
    assert any(a[1] == "CancelAll" and a[2].get("reason") == "runner_lag" for a in live_a)
    assert live_a == rep_a and live_l == rep_l
