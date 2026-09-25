"""Operator tools (python -m dh.live.tools ...): offline with fakes."""

from __future__ import annotations

import time
from dataclasses import asdict

import yaml

from dh.core.units import NS_PER_S
from dh.live.config import LiveConfig, VenueCfg
from dh.live.monitor import JsonLog
from dh.live.tools import cmd_backfill, cmd_ledger, cmd_orders, cmd_reconcile, cmd_replay

from .fakes import FakeRest, order_row
from .test_app import _setup
from .test_startup import _cf_history


async def test_backfill_orders_and_reconcile_commands(tmp_path):
    out: list[str] = []
    rest = FakeRest()
    rest.cf_history = _cf_history(time.time_ns(), 2.0, step_ms=30_000)
    assert await cmd_backfill(LiveConfig(), rest, out.append) == 0
    assert '"ok": true' in out[-1]
    rest.orders["o-1"] = order_row("c-1", "o-1", "KXBTCD-X")
    assert await cmd_orders(LiveConfig(venue=VenueCfg(subaccount=2)), rest, out.append) == 1  # not empty
    assert rest.of("iter_orders")[-1][1] == {"status": "resting", "subaccount": 2} and out[-1] == "1 resting orders"
    # reconcile: log.fill vs exchange fills
    log = tmp_path / "live-x.jsonl"
    jl = JsonLog(log, "c", "s")
    t0 = time.time_ns()
    jl.write("session_start", t0)
    jl.write("log.fill", t0 + NS_PER_S, ticker="KXBTCD-X", coid="c-1", side="bid", px=4500, qty=200, taker=False, fee=10_000)
    jl.close()
    rest.fills = [{"fill_id": "f", "trade_id": "f", "ticker": "KXBTCD-X", "count_fp": "2.00", "fee_cost": "0.010000"}]
    assert await cmd_reconcile(str(log), LiveConfig(), rest, out.append) == 0
    assert out[-1] == "0 tickers mismatched" and rest.of("iter_fills")[0][1]["min_ts"] == t0 // NS_PER_S - 60
    rest.fills[0]["count_fp"] = "3.00"
    assert await cmd_reconcile(str(log), LiveConfig(), rest, out.append) == 1


async def test_ledger_and_replay_commands_on_a_recorded_session(tmp_path):
    from dh.core.events import IndexTick
    from dh.live.app import LiveApp, Overrides

    rest, fake, lcfg, scfg, _ = _setup(tmp_path, "paper", forbid_writes=True)
    app = LiveApp(scfg, lcfg, "paper", Overrides(rest=rest, ws_connect=fake.connect, install_signals=False))
    runner = await app.build()

    async def brti():
        import asyncio

        for _ in range(40):
            t = time.time_ns()
            ev = IndexTick(ts=t, ts_exch=t, index_id="BRTI", value=84_000.0, feed="5hz")
            runner.recorder.write_event("events.test", ev)
            runner.push(ev)
            await asyncio.sleep(0.03)

    runner.add_source("brti", brti)
    await runner.run(duration_s=1.2)
    await app.close()
    log = next((tmp_path / "logs").iterdir())
    out: list[str] = []
    assert cmd_ledger(str(log), str(tmp_path / "data"), out.append) == 0 and '"fills"' in out[-1]
    cfg_path = tmp_path / "strategy.yaml"
    cfg_path.write_text(yaml.safe_dump(asdict(scfg)))
    assert cmd_replay(str(log), str(tmp_path / "data"), str(cfg_path), out.append, extra_streams=("events.test",)) == 0
    assert '"identical": true' in out[-1]
    # without the test's benchmark stream the replay diverges: the check reports it and where
    code = cmd_replay(str(log), str(tmp_path / "data"), str(cfg_path), out.append)
    assert '"identical": false' in out[-1] and code == 1
