"""End-to-end (offline): the real start-up sequence, MarketMaker, KalshiWS (against the
in-process fake server), paper simulator / live venue, recorder and shutdown."""

from __future__ import annotations

import asyncio
import json
import time
from dataclasses import replace

import orjson
import pytest

from dh.core.events import IndexTick
from dh.core.units import NS_PER_S
from dh.live.app import LiveApp, Overrides, run_from_paths
from dh.live.config import (
    BackfillCfg,
    LiveConfig,
    LoopCfg,
    MetricsCfg,
    PathsCfg,
    UniverseCfg,
    VenueCfg,
)
from dh.live.monitor import read_heartbeat
from dh.store.replay import iter_raw
from dh.strategy.config import load_config

from ..kalshi import samples as S
from ..kalshi.fake_ws import FakeKalshi, dumps
from .fakes import FakeRest

REPO = __import__("pathlib").Path(__file__).resolve().parents[2]


def _iso(ns: int) -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ns // NS_PER_S))


def _setup(tmp_path, mode: str, *, forbid_writes: bool):
    now = time.time_ns()
    exp = (now // (3600 * NS_PER_S) + 1) * 3600 * NS_PER_S  # the current hour's event
    if exp - now < 600 * NS_PER_S:
        exp += 3600 * NS_PER_S  # keep clear of the final-minutes rules
    et = "KXBTCD-TEST" + str(exp // NS_PER_S)
    tickers = [f"{et}-T84000.00", f"{et}-T84500.00"]
    rest = FakeRest(forbid_all_writes=forbid_writes)
    rest.series["KXBTCD"] = dict(S.SERIES_KXBTCD, fee_type="quadratic_with_maker_fees")
    rest.events["KXBTCD"] = [dict(S.EVENT_KXBTCD, event_ticker=et, markets=[
        S.market(ticker=t, event_ticker=et, floor_strike=float(t.rsplit("T", 1)[1]), close_time=_iso(exp),
                 expected_expiration_time=_iso(exp), open_time=_iso(exp - 86400 * NS_PER_S)) for t in tickers])]

    def cf(timespan, timestamp):
        span, end_ms = int(str(timespan).rstrip("s")), int(timestamp)
        return {"payload": [{"time": t, "value": f"{84000 + 5 * ((t // 60000) % 7):.2f}"}
                            for t in range(end_ms - span * 1000, end_ms + 1, 60_000)]}

    rest.cf_history = cf

    def wide(conn):  # a wide book so the strategy has edge to quote
        sid = conn.sid_of("orderbook_delta")
        return dumps({"type": "orderbook_snapshot", "sid": sid, "seq": conn.next_seq(sid),
                      "msg": {"market_ticker": tickers[0], "market_id": "m", "yes_dollars_fp": [["0.3000", "50.00"]],
                              "no_dollars_fp": [["0.3000", "50.00"]]}})

    fake = FakeKalshi([[wide]])
    (tmp_path / "run").mkdir()
    lcfg = LiveConfig(
        mode=mode,
        paths=PathsCfg(data_root=str(tmp_path / "data"), log_dir=str(tmp_path / "logs"), kill_file=str(tmp_path / "run" / "KILL"),
                       heartbeat_file=str(tmp_path / "run" / "hb.json")),
        metrics=MetricsCfg(enabled=False),
        loop=LoopCfg(heartbeat_interval_s=0.05, clock_sample_s=60.0, shutdown_timeout_s=2.0, max_lag_s=5.0),
        venue=VenueCfg(positions_interval_s=0.0, queue_positions_interval_s=0.0, fills_backfill_interval_s=0.0,
                       cancel_all_hold_s=0.3),
        universe=UniverseCfg(horizon_s=7200.0, discovery_interval_s=3600.0),
        backfill=BackfillCfg(days=2.0, chunk_s=12 * 3600),
    )
    scfg = load_config(REPO / "config" / "m1.yaml")
    # max_tau_s covers the chosen expiry at any minute of the hour (10 min .. 70 min away)
    scfg = replace(scfg, risk=replace(scfg.risk, book_resume_after_s=0.1),
                   timers=replace(scfg.timers, quote_period_ms=100),
                   quoting=replace(scfg.quoting, max_tau_s=4500.0))
    return rest, fake, lcfg, scfg, tickers


async def _brti(runner, seconds: float):
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        t = runner.clock_ns()
        runner.push(IndexTick(ts=t, ts_exch=t, index_id="BRTI", value=84000.0, feed="5hz"))
        await asyncio.sleep(0.05)


def _subscribed_channels(fake: FakeKalshi) -> list[list[str]]:
    return [c["params"]["channels"] for c in fake.conns[0].sent if c["cmd"] == "subscribe"]


async def test_paper_session_end_to_end(tmp_path):
    rest, fake, lcfg, scfg, tickers = _setup(tmp_path, "paper", forbid_writes=True)
    app = LiveApp(scfg, lcfg, "paper", Overrides(rest=rest, ws_connect=fake.connect, install_signals=False))
    runner = await app.build()
    runner.add_source("brti", lambda: _brti(runner, 5))
    code = await runner.run(duration_s=2.0)
    await app.close()
    assert code == 0
    mm = runner.strategy
    assert mm.fv.ready, "2 days of back-filled BRTI must warm the fair-value model"
    assert set(mm.specs) == set(tickers)
    # the strategy quoted into the simulator; nothing reached a REST write endpoint
    assert mm.stats.quotes_placed > 0 and runner.sim.stats["orders"] > 0
    assert not any(n in rest.names() for n in ("create_order", "batch_create_orders", "cancel_all_orders",
                                                  "create_order_group", "cancel_order"))
    chans = _subscribed_channels(fake)
    assert ["orderbook_delta", "trade"] in chans and ["market_lifecycle_v2"] in chans
    assert ["cfbenchmarks_value"] in chans and ["cfbenchmarks_value_5hz"] in chans
    assert not any("fill" in c or "user_orders" in c for c in chans), "paper mode must not consume real fills"
    ob = next(c for c in fake.conns[0].sent if c["cmd"] == "subscribe" and "orderbook_delta" in c["params"]["channels"])
    assert sorted(ob["params"]["market_tickers"]) == sorted(tickers)
    data = tmp_path / "data"
    meta = [orjson.loads(r.data) for r in iter_raw(data, ["meta"], 0, 2**62)]
    kinds = [m["kind"] for m in meta]
    assert kinds[:2] == ["session_start", "fv_warmup"] and "session_end" in kinds
    start = meta[0]
    assert start["mode"] == "paper" and len(start["specs"]) == 2 and start["backfill"]["ready"]
    assert len(meta[1]["points"]) >= 2 * 1440 - 2
    assert sum(1 for _ in iter_raw(data, ["kalshi.ws"], 0, 2**62)) > 5
    assert sum(1 for _ in iter_raw(data, ["events.paper"], 0, 2**62)) > 0
    assert read_heartbeat(tmp_path / "run" / "hb.paper.json")["state"] == "stopped"  # paper: its own heartbeat file
    assert not (tmp_path / "run" / "hb.json").exists(), "a paper runner must never write the live (watchdog's) heartbeat"
    logs = [json.loads(x) for f in (tmp_path / "logs").iterdir() for x in f.read_text().splitlines()]
    assert any(x["k"] == "action" and x["type"] == "PlaceOrder" for x in logs)
    assert any(x["k"] == "log.fv" for x in logs)


async def test_live_session_end_to_end(tmp_path):
    rest, fake, lcfg, scfg, tickers = _setup(tmp_path, "live", forbid_writes=False)
    app = LiveApp(scfg, lcfg, "live", Overrides(rest=rest, ws_connect=fake.connect, install_signals=False))
    runner = await app.build()
    runner.add_source("brti", lambda: _brti(runner, 5))
    code = await runner.run(duration_s=2.0)
    await app.close()
    assert code == 0 and runner.shutdown_ok
    names = rest.names()
    # start-up: clean-slate cancel-all VERIFIED (resting list), THEN positions, today's fills and
    # settlements (risk seed), order group before any order
    assert (names.index("cancel_all_orders") < names.index("iter_orders") < names.index("get_all_positions")
            < names.index("iter_fills") < names.index("iter_settlements") < names.index("create_order_group"))
    assert all(k.get("subaccount") == 0 for n, _, k in rest.calls if n in ("cancel_all_orders", "iter_orders", "iter_fills",
                                                                         "get_all_positions", "iter_settlements"))
    first_order = min(names.index(n) for n in ("create_order", "batch_create_orders") if n in names)
    assert names.index("create_order_group") < first_order
    bodies = [a[0] for a, _ in rest.of("create_order")] + [b for a, _ in rest.of("batch_create_orders") for b in a[0]]
    assert bodies and all(b["order_group_id"] == "og-1" and b["post_only"] and b["cancel_order_on_pause"] for b in bodies)
    assert all(b["ticker"] in tickers for b in bodies)
    # private channels subscribed in live mode
    assert ["fill", "user_orders", "order_group_updates", "market_positions"] in _subscribed_channels(fake)
    # shutdown: cancel-all, verification, group deleted
    tail = names[names.index("cancel_all_orders", names.index("create_order_group") + 1):]
    assert "iter_orders" in tail and "delete_order_group" in tail
    assert all(o["status"] != "resting" for o in rest.orders.values())
    live = list(iter_raw(tmp_path / "data", ["events.live"], 0, 2**62))
    assert len(live) >= len(bodies)  # every ack recorded for replay
    assert read_heartbeat(tmp_path / "run" / "hb.json")["state"] == "stopped"


async def test_live_refuses_to_start_when_exchange_closed(tmp_path):
    rest, fake, lcfg, scfg, _ = _setup(tmp_path, "live", forbid_writes=True)
    rest.exchange = {"exchange_active": True, "trading_active": False}
    app = LiveApp(scfg, lcfg, "live", Overrides(rest=rest, ws_connect=fake.connect, install_signals=False))
    assert await app.run(duration_s=1.0) == 2
    assert fake.conns == []  # never connected, never traded


async def test_kill_file_present_refuses_start(tmp_path):
    rest, fake, lcfg, scfg, _ = _setup(tmp_path, "paper", forbid_writes=True)
    (tmp_path / "run" / "KILL").write_text("still investigating")
    app = LiveApp(scfg, lcfg, "paper", Overrides(rest=rest, ws_connect=fake.connect, install_signals=False))
    assert await app.run(duration_s=1.0) == 2 and rest.calls == []


async def test_run_from_paths_mode_guard(tmp_path):
    import pytest

    from dh.live.config import ModeError

    with pytest.raises(ModeError):
        await run_from_paths("config/m1.yaml", "config/live.example.yaml", cli_mode="live", confirmed=True)
    p = tmp_path / "live.yaml"
    p.write_text("mode: live\n")
    with pytest.raises(ModeError):
        await run_from_paths("config/m1.yaml", str(p), cli_mode=None, confirmed=False)


@pytest.mark.parametrize("mode", ["paper", "live"])
async def test_session_replays_bit_for_bit(tmp_path, mode):
    """Determinism contract (docs/ARCHITECTURE.md): a recorded session re-run through
    dh.backtest.runner.run makes exactly the same decisions (actions and Log records).
    Paper: the replay's simulator regenerates the fills; live: the venue results are
    replayed from events.live."""
    from dh.live.replay import load_session, logged_decisions, replay_session, replayed_decisions

    rest, fake, lcfg, scfg, tickers = _setup(tmp_path, mode, forbid_writes=(mode == "paper"))
    app = LiveApp(scfg, lcfg, mode, Overrides(rest=rest, ws_connect=fake.connect, install_signals=False))
    runner = await app.build()
    rec = runner.recorder

    async def brti():  # a recorded benchmark source (the fake WS script cannot pace frames)
        end = time.monotonic() + 5
        v = 84_000.0
        while time.monotonic() < end:
            t = runner.clock_ns()
            v = 84_000.0 + (0.5 if (t // 10**8) % 2 else -0.5)  # calm: no abnormal-move trips
            ev = IndexTick(ts=t, ts_exch=t, index_id="BRTI", value=v, feed="5hz")
            rec.write_event("events.test", ev)
            runner.push(ev)
            await asyncio.sleep(0.03)

    runner.add_source("brti", brti)
    await runner.run(duration_s=2.5)
    await app.close()
    info = load_session(tmp_path / "data")
    assert info.mode == mode and len(info.specs) == 2 and info.last_ts > info.t0
    res = replay_session(tmp_path / "data", scfg, extra_streams=("events.test",))
    log = next((tmp_path / "logs").iterdir())
    live_a, live_l = logged_decisions(log, info.last_ts)
    rep_a, rep_l = replayed_decisions(res, info.last_ts)
    assert any(a[1] == "PlaceOrder" for a in live_a), "the session must have traded to be a meaningful check"
    assert live_a == rep_a
    assert live_l == rep_l
    assert len(live_a) >= 2 and len(live_l) > 10  # quotes + timers' order-group creation, fv logs


async def test_startup_network_failure_exits_cleanly(tmp_path):
    rest, fake, lcfg, scfg, _ = _setup(tmp_path, "paper", forbid_writes=True)
    rest.on("iter_events", ConnectionError("network down"))
    app = LiveApp(scfg, lcfg, "paper", Overrides(rest=rest, ws_connect=fake.connect, install_signals=False))
    assert await app.run(duration_s=1.0) == 2
    assert fake.conns == [] and app.jsonlog is not None and app.jsonlog._f.closed  # noqa: SLF001
