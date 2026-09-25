"""Watchdog: a stale live heartbeat triggers cancel-all (never an order); clean stops and
paper runners do not."""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

import pytest

from dh.core.units import NS_PER_S
from dh.live.config import WatchdogCfg
from dh.live.monitor import write_heartbeat
from dh.live.watchdog import Watchdog, rest_cancel_all

from .fakes import FakeClock, FakeRest, http_error, unknown

REPO = Path(__file__).resolve().parents[2]


def wd(tmp_path, rest: FakeRest | None = None, **cfg):
    clock = FakeClock()
    rest = rest or FakeRest()
    c = WatchdogCfg(**{"stale_s": 2.0, "retry_s": 1.0, "repeat_s": 30.0, "max_repeats": 2, **cfg})
    w = Watchdog(tmp_path / "hb.json", rest_cancel_all(rest, subaccount=4), c, clock_ns=clock)
    return w, rest, clock


def beat(tmp_path, clock, mode="live", state="running"):
    write_heartbeat(tmp_path / "hb.json", {"mode": mode, "state": state, "pid": 1}, now_ns=clock())


async def test_stale_heartbeat_triggers_cancel_all_only(tmp_path):
    rest = FakeRest()
    rest.forbid_order_writes = True  # anything but cancel-all would raise
    w, rest, clock = wd(tmp_path, rest)
    assert await w.step() == "DISARMED"  # no heartbeat yet
    beat(tmp_path, clock)
    assert await w.step() == "ARMED"
    clock.advance(1 * NS_PER_S)
    assert await w.step() == "ARMED" and rest.calls == []
    clock.advance(int(1.5 * NS_PER_S))  # 2.5 s since the last beat
    assert await w.step() == "TRIGGERED"
    assert rest.names() == ["cancel_all_orders"] and rest.of("cancel_all_orders")[0][1] == {"subaccount": 4}
    clock.advance(5 * NS_PER_S)
    await w.step()
    assert rest.names() == ["cancel_all_orders"]  # no repeat before repeat_s
    for _ in range(5):
        clock.advance(31 * NS_PER_S)
        await w.step()
    assert rest.names().count("cancel_all_orders") == 3  # first + max_repeats(2)
    assert set(rest.names()) == {"cancel_all_orders"}


async def test_failed_cancel_is_retried_until_success(tmp_path):
    rest = FakeRest().on("cancel_all_orders", http_error(500, "internal", "x", "DELETE"), unknown("DELETE"), {})
    w, rest, clock = wd(tmp_path, rest)
    beat(tmp_path, clock)
    await w.step()
    clock.advance(3 * NS_PER_S)
    await w.step()  # attempt 1: HTTP 500 raised -> failure
    clock.advance(int(0.5 * NS_PER_S))
    await w.step()  # retry_s not elapsed
    assert rest.names().count("cancel_all_orders") == 1
    clock.advance(1 * NS_PER_S)
    await w.step()  # attempt 2: unknown outcome -> failure
    clock.advance(1 * NS_PER_S)
    await w.step()  # attempt 3: ok
    assert rest.names().count("cancel_all_orders") == 3
    assert w.st.successes == 1 and w.st.failures == 2


async def test_recovery_rearms_and_clean_stop_disarms(tmp_path):
    w, rest, clock = wd(tmp_path)
    beat(tmp_path, clock)
    await w.step()
    clock.advance(3 * NS_PER_S)
    assert await w.step() == "TRIGGERED"
    beat(tmp_path, clock)  # runner recovered (or restarted)
    assert await w.step() == "ARMED"
    beat(tmp_path, clock, state="stopped")  # clean shutdown with confirmed cancel-all
    assert await w.step() == "DISARMED"
    clock.advance(60 * NS_PER_S)
    assert await w.step() == "DISARMED" and rest.names().count("cancel_all_orders") == 1


async def test_paper_heartbeat_never_arms_and_missing_file_triggers(tmp_path):
    w, rest, clock = wd(tmp_path)
    beat(tmp_path, clock, mode="paper")
    assert await w.step() == "DISARMED"
    clock.advance(10 * NS_PER_S)
    assert await w.step() == "DISARMED" and rest.calls == []
    beat(tmp_path, clock, mode="live")
    assert await w.step() == "ARMED"
    (tmp_path / "hb.json").unlink()
    assert await w.step() == "TRIGGERED" and rest.names() == ["cancel_all_orders"]


async def test_arm_on_start_acts_on_stale_file(tmp_path):
    clock = FakeClock()
    write_heartbeat(tmp_path / "hb.json", {"mode": "live", "state": "running"}, now_ns=clock() - 60 * NS_PER_S)
    rest = FakeRest()
    w = Watchdog(tmp_path / "hb.json", rest_cancel_all(rest), WatchdogCfg(), clock_ns=clock, arm_on_start=True)
    assert await w.step() == "TRIGGERED" and rest.names() == ["cancel_all_orders"]


async def test_run_loop_stops(tmp_path):
    w, rest, clock = wd(tmp_path)
    stop = asyncio.Event()
    calls = {"n": 0}

    async def fake_sleep(dt):
        calls["n"] += 1
        if calls["n"] >= 3:
            stop.set()

    w._sleep = fake_sleep  # noqa: SLF001
    await asyncio.wait_for(w.run(stop), 1.0)
    assert calls["n"] == 3


def _script():
    sys.path.insert(0, str(REPO / "scripts"))
    import importlib

    return importlib.import_module("watchdog")


@pytest.mark.parametrize("flag", ["cancel_now", "once"])
async def test_watchdog_script_with_injected_rest(tmp_path, flag):
    mod = _script()
    rest = FakeRest()
    write_heartbeat(tmp_path / "hb.json", {"mode": "live", "state": "running"}, now_ns=1)  # ancient
    args = argparse.Namespace(live_config=str(REPO / "config" / "live.example.yaml"), heartbeat=str(tmp_path / "hb.json"),
                              once=flag == "once", cancel_now=flag == "cancel_now", arm_on_start=True, max_age_s=0.0)
    assert await mod.amain(args, rest=rest) == 0
    assert rest.names() == ["cancel_all_orders"]
