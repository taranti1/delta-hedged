"""Regression tests for the pre-live review (docs/research/prelive_review_2026-09-25): the runner,
the venue, the watchdog and the live app (offline: fake REST, fake transports, fake clocks).

Each test turns one review probe or finding into a check of the FIXED behaviour:
  H2  fail-closed configuration (explicit subaccount / shared flag; the watchdog too)
  M1  the restricted key is PROVEN (a subaccount-0 read must be refused) and fills / order
      updates of client_order_ids that are not ours are dropped, live and in replay
  M3  the watchdog's own beat: the live start and the order gate depend on it
  M4  position-confirmation deferral per market, capped
  M5  a shared account never calls the bulk cancel-all (dynamic and static checks)
  LOW the write guard's shards, the balance and status gates failing closed, the reducing-order
      collateral, the midnight schedule close, per-market pause rejects, the fee check's
      subaccount, the recorder's rate share
  CLOCK the session clock slews toward the wall clock (and replay stays bit-for-bit)
  DISK  the live start and the order gate need free disk
"""

from __future__ import annotations

import argparse
import ast
import asyncio
import datetime as dt
import json
import os
import sys
import time
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest

from dh.core.actions import CancelAll, Halt
from dh.core.events import (
    FeedStatus,
    IndexTick,
    KalshiFill,
    KalshiOrderUpdate,
    KalshiPositionSnapshot,
    OrderAck,
    OrderReject,
)
from dh.core.units import NS_PER_MS, NS_PER_S
from dh.kalshi.rest import HttpResponse, KalshiHTTPError, KalshiRest, UnscopedWriteError
from dh.live.app import LiveApp, Overrides
from dh.live.clock import MAX_SLEW_PPM, AnchoredClock
from dh.live.config import (
    DiskCfg,
    LiveConfig,
    VenueCfg,
    WatchdogCfg,
    live_config_problems,
    load_live_config,
    subaccount_problems,
    venue_scope_problems,
)
from dh.live.monitor import read_heartbeat, watchdog_beat_path, watchdog_beat_problem, write_heartbeat
from dh.live.replay import UniverseReplay
from dh.live.runner import (
    DISK_REASON,
    MARKET_PAUSE_REASON,
    PAUSE_STREAM_REASON,
    RECONCILE_STREAM,
    WATCHDOG_REASON,
    LiveRunner,
    pause_reject_scope,
)
from dh.live.startup import schedule_closures, verify_key_restriction
from dh.live.venue_kalshi import KalshiVenue
from dh.live.watchdog import Watchdog, rest_scoped_cancel_all

from .fakes import T0, FakeClock, FakeRest, RecordingStrategy, fill_row, kxbtcd_spec, order_row
from .test_runner import P_MS, SPEC, TK, OrderingStrategy, cfg, confirm_read, live_runner

REPO = Path(__file__).resolve().parents[2]
TK2 = "KXBTCD-26SEP2513-T84250.00"
SHARED = dict(subaccount=1, shared_account=True, key_restricted_to_subaccount=True)


async def _nosleep(dt_s: float) -> None:
    return None


class Recorder:
    """HTTP transport of a real KalshiRest: records every request (the probes' fake)."""

    def __init__(self, status: int = 200, body: bytes = b"{}") -> None:
        self.calls: list[tuple[str, str, list, bytes | None]] = []
        self.status, self.body = status, body

    async def __call__(self, method, url, headers, params, data, timeout_s):
        self.calls.append((method, url, list(params), data))
        return HttpResponse(self.status, {}, self.body)


def _rest(sub: int | None, tr: Recorder, **kw: Any) -> KalshiRest:
    return KalshiRest("https://fake.invalid/trade-api/v2", None, None, transport=tr, write_subaccount=sub, **kw)


def _script():
    sys.path.insert(0, str(REPO / "scripts"))
    import importlib

    return importlib.import_module("watchdog")


# ============================================================================ H2: fail-closed configuration
def test_a_live_config_without_venue_is_refused_and_targets_nothing():
    """Review probe 9: LiveConfig(mode='live') used to pass every live check and target
    subaccount 0 (the start-up cancel-all went out as DELETE ...?subaccount=0)."""
    problems = live_config_problems(LiveConfig(mode="live"))
    assert any("venue.subaccount is not set" in p for p in problems)
    assert any("venue.shared_account is not set" in p for p in problems)
    with pytest.raises(ValueError, match="venue.subaccount is not set"):
        KalshiVenue(FakeRest(), sink=lambda e: None, cfg=LiveConfig(mode="live").venue)


def test_live_yaml_must_state_subaccount_and_shared_account(tmp_path):
    p = tmp_path / "live.yaml"
    p.write_text("mode: live\n")
    assert len(live_config_problems(load_live_config(p))) >= 2
    p.write_text("mode: live\nvenue:\n  subaccount: 1\n")  # the shared flag missing
    assert any("shared_account is not set" in x for x in live_config_problems(load_live_config(p)))
    p.write_text("mode: live\nvenue:\n  subaccount: 1\n  shared_account: true\n")  # no restricted keys
    assert any("key_restricted_to_subaccount must be true" in x for x in live_config_problems(load_live_config(p)))
    p.write_text("mode: live\nvenue:\n  subaccount: 1\n  shared_account: true\n  key_restricted_to_subaccount: true\n")
    assert live_config_problems(load_live_config(p)) == []


@pytest.mark.parametrize("sub, shared, allow, ok", [
    (0, False, True, True),  # the primary account, explicitly, on an account shared with nobody
    (0, False, False, False),  # no opt-in
    (0, True, True, False),  # never on a shared account
    (0, None, True, False),  # the shared flag not stated
    (1, True, False, True),
    (None, True, False, False),
    (True, False, False, False),  # a bool is not a subaccount number
    (64, False, False, False),
])
def test_subaccount_rules(sub, shared, allow, ok):
    v = VenueCfg(subaccount=sub, shared_account=shared, allow_primary_account=allow)
    assert (subaccount_problems(v) == []) is ok


def test_bulk_cancel_is_allowed_only_on_an_account_declared_not_shared():
    assert VenueCfg(subaccount=1, shared_account=False).bulk_cancel_allowed
    assert not VenueCfg(subaccount=1, shared_account=True).bulk_cancel_allowed
    assert not VenueCfg(subaccount=1).bulk_cancel_allowed  # unknown = shared (fail closed)


async def test_watchdog_without_an_explicit_config_refuses(tmp_path):
    """Review probe 3: an empty --live-config loaded LiveConfig() defaults and cancelled
    subaccount 0 (System 2's) on --cancel-now."""
    mod = _script()
    calls: list[Any] = []

    class Rest:
        async def cancel_all_orders(self, *, subaccount):
            calls.append(("cancel_all", subaccount))
            return {}

        async def trigger_order_group(self, gid, *, subaccount, exchange_index):
            calls.append(("trigger", gid, subaccount, exchange_index))
            return {}

        async def close(self):
            pass

    base = dict(heartbeat=str(tmp_path / "hb.json"), once=False, cancel_now=True, arm_on_start=False, max_age_s=0.0)
    assert await mod.amain(argparse.Namespace(live_config="", **base), rest=Rest()) == 2
    assert await mod.amain(argparse.Namespace(live_config=str(tmp_path / "missing.yaml"), **base), rest=Rest()) == 2
    venue_less = tmp_path / "live.yaml"
    venue_less.write_text("mode: live\n")
    assert await mod.amain(argparse.Namespace(live_config=str(venue_less), **base), rest=Rest()) == 2
    no_key_rule = tmp_path / "shared.yaml"
    no_key_rule.write_text("venue:\n  subaccount: 1\n  shared_account: true\n")
    assert await mod.amain(argparse.Namespace(live_config=str(no_key_rule), **base), rest=Rest()) == 2
    assert calls == []


async def test_watchdog_never_arms_on_a_heartbeat_of_another_subaccount(tmp_path):
    clock = FakeClock()
    rest = FakeRest()
    hbp = tmp_path / "hb.json"
    w = Watchdog(hbp, rest_scoped_cancel_all(rest, 1, sleep=_nosleep), clock_ns=clock, subaccount=1)
    write_heartbeat(hbp, {"mode": "live", "state": "running", "pid": 5, "session": "s0", "subaccount": 0}, now_ns=clock())
    assert await w.step() == "DISARMED"
    write_heartbeat(hbp, {"mode": "live", "state": "running", "pid": 5, "session": "s0"}, now_ns=clock())  # none named
    assert await w.step() == "DISARMED"
    clock.advance(10 * NS_PER_S)
    assert await w.step() == "DISARMED" and rest.calls == []  # a stale runner of another subaccount: nothing sent
    write_heartbeat(hbp, {"mode": "live", "state": "running", "pid": 6, "session": "s1", "subaccount": 1}, now_ns=clock())
    assert await w.step() == "ARMED"


async def test_watchdog_needs_its_own_key_on_a_shared_account(tmp_path, monkeypatch):
    mod = _script()
    for v in ("KALSHI_WATCHDOG_KEY_ID", "KALSHI_WATCHDOG_PRIVATE_KEY_PATH", "KALSHI_KEY_ID", "KALSHI_PRIVATE_KEY_PATH"):
        monkeypatch.delenv(v, raising=False)
    lcfg = load_live_config(REPO / "config" / "live.example.yaml")
    with pytest.raises(mod.ConfigRefused, match="no watchdog key"):
        mod.build_rest(lcfg)
    # the explicit opt-in lets it try the runner's key (here: none configured -> still refused, differently)
    with pytest.raises(mod.ConfigRefused, match="no Kalshi credentials"):
        mod.build_rest(replace(lcfg, watchdog=replace(lcfg.watchdog, allow_runner_key=True)))
    # its own key: the client is bound to subaccount 1, shard 2, and can never send the bulk cancel-all
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import rsa

    pem = tmp_path / "wd.pem"
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem.write_bytes(key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.TraditionalOpenSSL,
                                      serialization.NoEncryption()))
    monkeypatch.setenv("KALSHI_WATCHDOG_KEY_ID", "wd-key")
    monkeypatch.setenv("KALSHI_WATCHDOG_PRIVATE_KEY_PATH", str(pem))
    rest = mod.build_rest(lcfg)
    try:
        assert rest.write_subaccount == 1 and rest.write_shards == frozenset({2}) and rest.forbid_bulk_cancel
        with pytest.raises(UnscopedWriteError, match="bulk cancel-all"):
            await rest.cancel_all_orders(subaccount=1)
    finally:
        await rest.close()


# ============================================================================ M1: proven restriction, foreign fills
async def test_key_restriction_needs_a_refused_subaccount_0_read():
    """Review probe 5: a 503 on /api_keys plus a balance body without balance_breakdown used to
    count as proof. Now only a refused (401/403) GET /portfolio/balance?subaccount=0 is."""

    class R:
        async def get_api_keys(self):
            raise KalshiHTTPError("GET", "/api_keys", 503, {"error": {"code": "x"}})

        async def get_balance(self, *, subaccount=None, exchange_index=None):
            return {"balance": 15000, "balance_dollars": "150.00"}  # an unrestricted key reads subaccount 0

    ok, why = await verify_key_restriction(R(), "kid", 1, [{"balance": 15000, "balance_dollars": "150.00"}])
    assert not ok and "ANSWERED" in why

    class Restricted(R):
        async def get_balance(self, *, subaccount=None, exchange_index=None):
            raise KalshiHTTPError("GET", "/portfolio/balance", 403, {"error": {"code": "forbidden"}})

    ok, why = await verify_key_restriction(Restricted(), "kid", 1, [])
    assert ok and "HTTP 403" in why

    class Flaky(R):
        async def get_balance(self, *, subaccount=None, exchange_index=None):
            raise KalshiHTTPError("GET", "/portfolio/balance", 500, {"error": {"code": "x"}})

    ok, why = await verify_key_restriction(Flaky(), "kid", 1, [])
    assert not ok and "no proof" in why


async def test_live_start_refuses_an_unrestricted_key_on_a_shared_account(tmp_path):
    from .test_shared_account import _live

    rest, fake, lcfg, scfg, _ = _live(tmp_path)
    rest.key_subaccount = None  # System 2's unrestricted key: the subaccount-0 read answers
    app = LiveApp(scfg, lcfg, "live", Overrides(rest=rest, ws_connect=fake.connect, install_signals=False))
    assert await app.run(duration_s=0.5) == 2
    assert not [n for n in rest.names() if n in ("cancel_order", "batch_cancel_orders", "cancel_all_orders",
                                                  "create_order_group", "create_order")]
    assert fake.conns == [] and "ANSWERED" in app.info["key_restriction"]["evidence"]


def _fill(t: int, coid: str, oid: str, trade: str, ticker: str = TK, sub: int = 0) -> KalshiFill:
    return KalshiFill(t, t, ticker, trade, oid, coid, "bid", 4500, 100, False, 0, 0, False, subaccount=sub)


async def test_a_foreign_fill_never_reaches_the_strategy_even_with_the_restricted_key_rule():
    """Review probe 5b: with key_restricted a fill without the subaccount field normalizes to 0
    and was taken as ours, System 2's included. Now it must also be OUR order."""
    s = OrderingStrategy(n=0)
    c = cfg(subaccount=1, shared_account=True, key_restricted_to_subaccount=True)
    r, _, _ = live_runner(s, config=c, venue_cfg=c.venue, own_id_prefix="dhm1-", series=("KXBTCD",))
    t = time.time_ns()
    r.push(_fill(t, "sys2-abc", "o-sys2", "t1"))  # another system's fill (no subaccount field)
    r.push(_fill(t + 1, "", "o-unknown", "t2"))  # no client_order_id, unknown order
    r.push(_fill(t + 2, "dhm1-tok-1", "o-1", "t3"))  # ours
    r.push_result(OrderAck(t + 3, 0, "dhm1-tok-2", "o-2", TK, 0, 200))  # our create's response
    r.push(_fill(t + 4, "", "o-2", "t4"))  # no client_order_id, but a known order of ours
    r.push(KalshiOrderUpdate(t + 5, 0, TK, "o-sys2", "sys2-abc", "resting", "bid", 4500, 100, 0, 100))
    r.process_pending()
    got = [e.trade_id for e in s.events if isinstance(e, KalshiFill)]
    assert got == ["t3", "t4"]
    assert not [e for e in s.events if isinstance(e, KalshiOrderUpdate)]
    assert s.om.position(TK) == 200
    # the System 2 fill is dropped as foreign; OUR-looking fill without a client id of an unknown order
    # is PARKED (review NEW-1: it may have beaten our create's response), never dropped silently
    assert r.metrics.get("dh_foreign_order_events_total", type="KalshiFill", source="ws") == 1.0
    assert "o-unknown" in r._parked  # noqa: SLF001
    # a REST back-fill (REST fills carry no client_order_id): only known order ids of this session
    tf = t + 50 * NS_PER_MS  # after the session started (a REST row carries ms)
    r.push_side("fills", {"rows": [fill_row("f-9", "o-sys2", TK, created_ns=tf), fill_row("f-10", "o-1", TK, created_ns=tf)],
                          "fetched_ns": t + 10, "since_ns": 0})
    r.process_pending()
    assert [e.trade_id for e in s.events if isinstance(e, KalshiFill)] == ["t3", "t4", "f-10"]
    assert r.metrics.get("dh_foreign_order_events_total", type="KalshiFill", source="rest") == 1.0
    # replay applies the same rule, from the same events in the same order
    s2 = RecordingStrategy()
    rep = UniverseReplay(s2, [], live=True, subaccount=1, key_restricted=True, series=("KXBTCD",), own_id_prefix="dhm1-")
    for e in [_fill(t, "sys2-abc", "o-sys2", "t1"), _fill(t + 1, "", "o-unknown", "t2"), _fill(t + 2, "dhm1-tok-1", "o-1", "t3"),
              OrderAck(t + 3, 0, "dhm1-tok-2", "o-2", TK, 0, 200), _fill(t + 4, "", "o-2", "t4")]:
        rep.on_event(e)
    assert [e.trade_id for e in s2.events if isinstance(e, KalshiFill)] == ["t3", "t4"]


async def test_the_live_app_filters_on_its_run_prefix_and_records_it_for_replay(tmp_path):
    from dh.live.replay import load_session

    from .test_app import _setup

    rest, fake, lcfg, scfg, _ = _setup(tmp_path, "live", forbid_writes=False)
    app = LiveApp(scfg, lcfg, "live", Overrides(rest=rest, ws_connect=fake.connect, install_signals=False))
    runner = await app.build()
    assert runner.own_id_prefix == f"{scfg.run_prefix}-" and app.id_prefix.startswith(runner.own_id_prefix)
    await runner.shutdown()
    await app.close()
    assert load_session(tmp_path / "data").own_id_prefix == f"{scfg.run_prefix}-"


# ============================================================================ M3: watchdog liveness
async def test_watchdog_writes_its_own_beat_and_marks_its_exit(tmp_path):
    hbp = tmp_path / "hb.json"
    rest = FakeRest()
    w = Watchdog(hbp, rest_scoped_cancel_all(rest, 1, sleep=_nosleep), WatchdogCfg(poll_s=0.01, beat_interval_s=0.01),
                 subaccount=1)
    write_heartbeat(hbp, {"mode": "live", "state": "running", "pid": 77, "session": "live-a", "subaccount": 1})
    stop = asyncio.Event()

    async def stopper():
        await asyncio.sleep(0.15)
        beat = read_heartbeat(watchdog_beat_path(hbp))
        assert beat["pid"] == os.getpid() and beat["subaccount"] == 1 and beat["state"] == "ARMED"
        assert beat["armed"] == [77, "live-a"] and beat["last_poll_ns"] > 0
        stop.set()

    await asyncio.gather(w.run(stop), stopper())
    assert read_heartbeat(watchdog_beat_path(hbp))["state"] == "EXITED"


def test_watchdog_beat_problems():
    now = T0
    good = {"t": now, "pid": 1, "subaccount": 1, "state": "ARMED", "armed": [42, "s"], "api_ok": True, "api_ok_ns": now}
    assert watchdog_beat_problem(good, now_ns=now, subaccount=1, max_age_s=10, runner=(42, "s")) == ""
    assert "no watchdog beat" in watchdog_beat_problem(None, now_ns=now, subaccount=1, max_age_s=10)
    assert "old" in watchdog_beat_problem(good, now_ns=now + 11 * NS_PER_S, subaccount=1, max_age_s=10)
    assert "subaccount" in watchdog_beat_problem(good, now_ns=now, subaccount=2, max_age_s=10)
    assert "not armed" in watchdog_beat_problem(good, now_ns=now, subaccount=1, max_age_s=10, runner=(43, "s"))
    assert "exited" in watchdog_beat_problem(dict(good, state="EXITED"), now_ns=now, subaccount=1, max_age_s=10)


@pytest.mark.real_guards
@pytest.mark.parametrize("beat", ["missing", "stale", "other_subaccount", "fresh"])
async def test_live_start_requires_a_fresh_watchdog_beat_for_its_subaccount(tmp_path, beat, monkeypatch):
    import dh.live.app as appmod

    from .test_shared_account import _live

    monkeypatch.setattr(appmod, "data_root_free_gb", lambda root: 500.0)
    rest, fake, lcfg, scfg, _ = _live(tmp_path)
    wd = watchdog_beat_path(tmp_path / "run" / "hb.json")
    now = time.time_ns()
    if beat != "missing":
        t = now - (60 * NS_PER_S if beat == "stale" else 0)
        write_heartbeat(wd, {"pid": 9, "subaccount": 0 if beat == "other_subaccount" else 1, "state": "DISARMED",
                             "armed": None, "api_ok": True, "api_ok_ns": t}, now_ns=t)
    app = LiveApp(scfg, lcfg, "live", Overrides(rest=rest, ws_connect=fake.connect, install_signals=False))
    if beat != "fresh":
        assert await app.run(duration_s=0.5) == 2
        assert rest.calls == [] and fake.conns == []  # refused before anything was read or sent
        return
    runner = await app.build()
    assert app.info["watchdog"]["ok"] and runner._watchdog_reader is not None  # noqa: SLF001
    await runner.shutdown()
    await app.close()


async def test_runner_gates_new_orders_while_the_watchdog_is_not_protecting_it():
    """The beat goes stale (or names another subaccount, or is not armed on this runner once it
    has been running a while): gate 'watchdog' + the strategy is told (kalshi.reconcile stale)."""
    s = RecordingStrategy()
    beat = {"t": time.time_ns(), "pid": 1, "subaccount": 0, "state": "ARMED", "armed": None, "api_ok": True,
            "api_ok_ns": time.time_ns()}
    alive = {"on": False}
    c = replace(cfg(), watchdog=WatchdogCfg(runner_max_age_s=0.3))
    r, _, _ = live_runner(s, config=c, watchdog_reader=lambda: dict(beat, t=time.time_ns()) if alive["on"] else dict(beat))

    async def probe():
        await asyncio.sleep(0.1)
        assert WATCHDOG_REASON not in r.gate.reasons  # fresh
        await asyncio.sleep(0.6)  # the beat is not refreshed: stale after 0.3 s
        assert WATCHDOG_REASON in r.gate.reasons
        alive["on"] = True  # fresh again (re-stamped on every read), armed on this runner
        beat["armed"] = [os.getpid(), r.session_id]
        await asyncio.sleep(0.4)
        assert WATCHDOG_REASON not in r.gate.reasons
        beat["armed"] = [os.getpid() + 1, "another"]  # running > 0.3 s: must be armed on THIS runner
        await asyncio.sleep(0.4)
        assert WATCHDOG_REASON in r.gate.reasons
        await asyncio.sleep(5)

    r.add_source("probe", probe)
    await r.run(duration_s=1.8)
    fs_ = [e for e in s.events if isinstance(e, FeedStatus) and e.stream == RECONCILE_STREAM]
    assert [e.status for e in fs_][:3] == ["stale", "resynced", "stale"] and fs_[0].detail == WATCHDOG_REASON
    # a gated place is rejected locally
    assert r.gate.check(TK) != ""


# ============================================================================ M4: per-market, capped deferral
def _two_market_runner(clock: dict[str, int]):
    s = OrderingStrategy(n=0)
    spec2 = kxbtcd_spec(84_250.0)
    rest = FakeRest()
    c = cfg(position_confirm_s=5.0)
    venue = KalshiVenue(rest, sink=lambda e: None, cfg=c.venue, clock_ns=lambda: clock["t"])
    r = LiveRunner(s, mode="live", period_ns=P_MS * NS_PER_MS, cfg=c, venue=venue, universe=[SPEC, spec2],
                   clock_ns=lambda: clock["t"])
    venue.sink = r.push_result
    return r, s, spec2.ticker


async def test_a_fill_in_another_market_does_not_defer_the_confirmation():
    """Review M4: the deferral compared the read's timestamp with the GLOBAL last WS fill."""
    clock = {"t": T0}
    r, s, tk2 = _two_market_runner(clock)
    r.push(KalshiFill(clock["t"], T0 - 10 * NS_PER_S, TK, "tr-1", "o-1", "", "bid", 4500, 200, False, 0, 0, False))
    r.process_pending()
    r.push_side("positions", {TK: 100})  # the exchange differs in TK
    r.process_pending()
    r.push(KalshiFill(clock["t"], T0 + 5 * NS_PER_S, tk2, "tr-2", "o-2", "", "bid", 4500, 200, False, 0, 0, False))
    r.process_pending()
    clock["t"] += 6 * NS_PER_S
    confirm_read(r, clock["t"])
    # validated after TK's last fill, before tk2's: TK's difference is confirmed
    r.push_side("positions_checked", {"positions": {TK: 100, tk2: 200}, "as_of_ns": T0})
    r.process_pending()
    snaps = [e for e in s.events if isinstance(e, KalshiPositionSnapshot) and e.ticker == TK]
    assert snaps and snaps[-1].position == 100
    assert not r.metrics.get("dh_position_reads_stale_total")


async def test_the_deferral_is_capped_while_fills_keep_arriving():
    """Review M4: fills that keep arriving in the market (both sides move, the difference stays)
    deferred the confirmation forever. After venue.position_defer_max deferrals the next read
    validated after the difference was first seen confirms it."""
    clock = {"t": T0}
    r, s, _ = _two_market_runner(clock)
    n = 0

    def fill_round() -> None:
        nonlocal n
        n += 1
        r.push(KalshiFill(clock["t"], clock["t"] - NS_PER_S, TK, f"tr-{n}", f"o-{n}", "", "bid", 4500, 200, False, 0, 0,
                          False))
        r.process_pending()

    fill_round()
    r.push_side("positions", {TK: 200 * n - 100})  # the exchange is 100 short, and stays so
    r.process_pending()
    first_seen = r._pos_suspect[TK][1]  # noqa: SLF001
    confirmed_at = None
    for rnd in range(12):
        clock["t"] += 6 * NS_PER_S
        fill_round()  # one more fill: both sides move, the difference stays
        confirm_read(r, clock["t"])
        r.push_side("positions_checked", {"positions": {TK: 200 * n - 100}, "as_of_ns": clock["t"] - 2 * NS_PER_S})
        r.process_pending()
        if [e for e in s.events if isinstance(e, KalshiPositionSnapshot) and e.position == 200 * n - 100]:
            confirmed_at = rnd
            break
    assert confirmed_at is not None, "a real mismatch must not be deferred forever"
    assert confirmed_at <= r.cfg.venue.position_defer_max
    assert clock["t"] - 2 * NS_PER_S > first_seen


# ============================================================================ M5: never the bulk cancel-all when shared
def _shared_venue(rest: FakeRest):
    clock = FakeClock()
    out: list = []
    v = KalshiVenue(rest, sink=out.append, cfg=VenueCfg(**SHARED, cancel_rounds=3), clock_ns=clock, monotonic=clock.mono,
                    sleep=_nosleep)
    v.register_markets([kxbtcd_spec(ticker=TK), kxbtcd_spec(ticker=TK2)])
    return v, out


async def test_shared_venue_cancels_by_id_until_the_list_is_empty():
    rest = FakeRest()
    rest.orders["o-1"] = order_row("dhm1-a-1", "o-1", TK)
    rest.orders["o-2"] = order_row("x", "o-2", TK2, exchange_index=2)
    rest.orders["o-3"] = order_row("y", "o-3", TK, exchange_index=3)  # a leftover on another shard: still ours
    v, _ = _shared_venue(rest)
    assert await v.cancel_all_verified("startup") == []
    assert await v.cancel_all_now("kill")
    v.submit([CancelAll(reason="halt")], 1)
    await v.wait_idle(1.0)
    assert "cancel_all_orders" not in rest.names()
    items = [it for a, _ in rest.of("batch_cancel_orders") for it in a[0]]
    single = rest.of("cancel_order")
    assert items or single
    for it in items:
        assert it["subaccount"] == 1 and it["exchange_index"] in (2, 3)
    assert all(k["subaccount"] == 1 for _, k in single)
    assert all(k.get("subaccount") == 1 and k.get("status") == "resting" for _, k in rest.of("iter_orders"))
    assert v.last_cancel_all_ns == 0  # no bulk call: no one-minute tail to wait out


async def test_shared_venue_alarms_when_orders_keep_resting():
    rest = FakeRest()
    rest.orders["o-1"] = order_row("dhm1-a-1", "o-1", TK)
    rest.on("batch_cancel_orders", *[{"orders": []}] * 10)
    rest.on("cancel_order", *[KalshiHTTPError("DELETE", "/x", 400, {"error": {"code": "x"}})] * 10)
    v, _ = _shared_venue(rest)
    left = await v.cancel_all_verified("shutdown", rounds=2, wait_s=0.1)
    assert [o["order_id"] for o in left] == ["o-1"]  # bounded: returned (the runner exits 3 / alarms)
    assert "cancel_all_orders" not in rest.names()
    with pytest.raises(RuntimeError, match="forbidden on a shared account"):
        await v._bulk_cancel_all("x")  # noqa: SLF001


@pytest.mark.parametrize("path", ["kill", "manual_halt", "timed_halt", "fee_mismatch", "watchdog_marker", "shutdown"])
async def test_no_runner_path_calls_the_bulk_cancel_all_on_a_shared_account(tmp_path, path):
    from dh.live.monitor import KillFile

    s = OrderingStrategy(n=0)
    c = cfg(**SHARED)
    rest = FakeRest()
    rest.on("cancel_all_orders", *[AssertionError("bulk cancel-all on a shared account")] * 5)
    rest.orders["o-1"] = order_row("dhm1-a-1", "o-1", TK)
    kill = tmp_path / "KILL"
    marker = tmp_path / "hb.json.cancel_all"
    r, v, _ = live_runner(s, rest=rest, config=c, venue_cfg=c.venue, kill_file=KillFile(kill),
                          cancel_all_marker=marker, started_ns=time.time_ns() - NS_PER_S)
    await v.ensure_order_group("dh-main", 2000)
    t = r.clock_ns()
    if path == "kill":
        r.kill("drill")
    elif path in ("manual_halt", "timed_halt"):
        r._on_actions(IndexTick(t, t, "BRTI", 1.0, "5hz"),  # noqa: SLF001
                      [Halt(reason="x", scope="all", until_ts=0 if path == "manual_halt" else t + 10**12),
                       CancelAll(reason="x")], [False, False], "strategy")
    elif path == "fee_mismatch":
        r._cancel_all_async("fee_mismatch")  # noqa: SLF001
    elif path == "watchdog_marker":
        marker.write_text(json.dumps({"t": time.time_ns(), "ok": True, "watched": [os.getpid(), r.session_id]}))
        r._check_marker(t)  # noqa: SLF001
        r.process_pending()
    else:
        await r.shutdown()
    await v.wait_idle(2.0)
    if path != "shutdown":
        await r.shutdown()
    assert "cancel_all_orders" not in rest.names()
    assert rest.orders["o-1"]["status"] == "canceled"  # cancelled by id
    assert "cancel_all_hold" not in r.gate.reasons  # no bulk call, no one-minute hold


async def test_watchdog_on_a_shared_account_triggers_then_cancels_by_id(tmp_path):
    clock = FakeClock()
    rest = FakeRest()
    rest.orders["o-1"] = order_row("dhm1-a-1", "o-1", TK, exchange_index=2)
    rest.orders["o-1"]["order_group_id"] = "og-7"
    rest.orders["o-2"] = order_row("dhm1-a-2", "o-2", TK)  # outside the group
    hbp = tmp_path / "hb.json"
    from dh.live.watchdog import rest_trigger_groups

    w = Watchdog(hbp, rest_scoped_cancel_all(rest, 1, sleep=_nosleep), clock_ns=clock, subaccount=1,
                 trigger_groups=rest_trigger_groups(rest, 1))
    write_heartbeat(hbp, {"mode": "live", "state": "running", "pid": 5, "session": "s", "subaccount": 1,
                          "order_groups": [{"id": "og-7", "exchange_index": 2, "subaccount": 1}]}, now_ns=clock())
    assert await w.step() == "ARMED"
    clock.advance(3 * NS_PER_S)
    assert await w.step() == "TRIGGERED"
    names = rest.names()
    assert "cancel_all_orders" not in names and names[0] == "trigger_order_group"
    assert all(o["status"] == "canceled" for o in rest.orders.values())
    items = [it for a, _ in rest.of("batch_cancel_orders") for it in a[0]]
    assert items == [{"order_id": "o-2", "market_ticker": TK, "subaccount": 1, "exchange_index": 2}]
    assert json.loads((tmp_path / "hb.json.cancel_all").read_text())["bulk"] is False


def _funcs(tree: ast.AST):
    """(qualified name, node) of every function / method in a module."""
    out = []

    def walk(node: ast.AST, prefix: str) -> None:
        for ch in ast.iter_child_nodes(node):
            if isinstance(ch, ast.ClassDef):
                walk(ch, prefix + ch.name + ".")
            elif isinstance(ch, (ast.FunctionDef, ast.AsyncFunctionDef)):
                out.append((prefix + ch.name, ch))
                walk(ch, prefix + ch.name + ".")
    walk(tree, "")
    return out


def test_static_no_code_path_reachable_on_a_shared_account_calls_the_bulk_cancel_all():
    """Static guard (review M5). The bulk endpoint (``KalshiRest.cancel_all_orders``) may be
    called only from:
      * ``KalshiVenue._bulk_cancel_all``, which must start by raising unless
        ``self.bulk_cancel_allowed`` (i.e. venue.shared_account is explicitly false), and
      * ``dh.live.watchdog.rest_cancel_all``, which scripts/watchdog.py may build only in the
        branch where ``bulk`` (= venue.bulk_cancel_allowed) is true;
    and every KalshiRest bound to a subaccount (runner, watchdog) is built with
    ``forbid_bulk_cancel`` (refused before signing on a shared account)."""
    allowed = {("dh/live/venue_kalshi.py", "KalshiVenue._bulk_cancel_all"),
               ("dh/live/watchdog.py", "rest_cancel_all._cancel")}
    files = sorted((REPO / "dh").rglob("*.py")) + sorted((REPO / "scripts").glob("*.py"))
    seen: set[tuple[str, str]] = set()
    for f in files:
        rel = str(f.relative_to(REPO))
        tree = ast.parse(f.read_text(), rel)
        for qual, fn in _funcs(tree):
            for node in ast.walk(fn):
                if not isinstance(node, ast.Call):
                    continue
                name = node.func.attr if isinstance(node.func, ast.Attribute) else getattr(node.func, "id", "")
                if name == "cancel_all_orders":
                    # the innermost function containing this call
                    inner = [q for q, g in _funcs(tree) if any(n is node for n in ast.walk(g))]
                    where = max(inner, key=len)
                    assert (rel, where) in allowed, f"bulk cancel-all called from {rel}:{node.lineno} ({where})"
                    seen.add((rel, where))
                if name == "rest_cancel_all" and rel == "scripts/watchdog.py":
                    parents = [p for p in ast.walk(fn) if isinstance(p, (ast.IfExp, ast.If))
                               and any(n is node for n in ast.walk(p.body if isinstance(p, ast.IfExp) else p))]
                    assert parents and all("bulk" in ast.unparse(p.test) for p in parents[:1]), \
                        f"rest_cancel_all built outside a `bulk` branch at {rel}:{node.lineno}"
                if name == "KalshiRest":
                    kws = {k.arg for k in node.keywords if k.arg}
                    if "write_subaccount" in kws:
                        assert {"forbid_bulk_cancel", "write_shards"} <= kws, \
                            f"{rel}:{node.lineno}: a subaccount-bound KalshiRest without forbid_bulk_cancel/write_shards"
    assert seen == allowed
    # the venue's bulk method refuses first thing on a shared account
    tree = ast.parse((REPO / "dh" / "live" / "venue_kalshi.py").read_text())
    fn = dict(_funcs(tree))["KalshiVenue._bulk_cancel_all"]
    body = [st for st in fn.body if not (isinstance(st, ast.Expr) and isinstance(st.value, ast.Constant))]
    first = body[0]
    assert isinstance(first, ast.If) and ast.unparse(first.test) == "not self.bulk_cancel_allowed"
    assert isinstance(first.body[0], ast.Raise)


# ============================================================================ LOW: write guard shards (L1)
async def test_the_write_guard_accepts_only_the_configured_shards():
    """Review probe 1: a create naming shard 0 passed (presence was checked, not the value)."""
    tr = Recorder(body=b'{"order_id":"x"}')
    r = _rest(1, tr, write_shards=(2,))
    with pytest.raises(UnscopedWriteError, match="not one of this client's shards"):
        await r.create_order({"ticker": TK, "subaccount": 1, "exchange_index": 0})
    with pytest.raises(UnscopedWriteError, match="-1"):
        await r.create_order({"ticker": TK, "subaccount": 1, "exchange_index": -1})
    with pytest.raises(UnscopedWriteError):
        await r.create_order_group(2000, subaccount=1, exchange_index=0)
    with pytest.raises(UnscopedWriteError):
        await r.batch_create_orders([{"ticker": TK, "subaccount": 1, "exchange_index": 2},
                                     {"ticker": TK, "subaccount": 1, "exchange_index": 0}])
    assert tr.calls == []
    await r.create_order({"ticker": TK, "subaccount": 1, "exchange_index": 2})
    # cancels reduce: any shard of the subaccount (a leftover elsewhere must be cancellable), or -1
    await r.cancel_order("o-1", market_ticker=TK, subaccount=1, exchange_index=0)
    await r.cancel_order("o-2", market_ticker=TK, subaccount=1, exchange_index=-1)
    await r.trigger_order_group("g", subaccount=1, exchange_index=3)
    assert len(tr.calls) == 4


async def test_the_guard_still_refuses_subaccount_0():
    """Review probe 1b (found correct; kept as a regression guard)."""
    tr = Recorder()
    r = _rest(1, tr)
    with pytest.raises(UnscopedWriteError):
        await r.cancel_all_orders(subaccount=0)
    with pytest.raises(UnscopedWriteError):
        await r.trigger_order_group("g", subaccount=None, exchange_index=2)
    assert tr.calls == []


# ============================================================================ LOW: balance gate (L2)
async def test_orders_closing_a_held_position_are_not_counted_as_collateral():
    rest = FakeRest()
    rest.balances = {2: "20.0000"}
    rest.positions = {TK: "10.00"}  # long 10 YES
    rest.exposure = {TK: "4.5000"}
    rest.orders["o-1"] = order_row("c-1", "o-1", TK, side="ask", px="0.6000", remaining="10.00")  # closes it: $0
    rest.orders["o-2"] = order_row("c-2", "o-2", TK, side="ask", px="0.6000", remaining="5.00")  # beyond it: 5 x 40c
    rest.orders["o-3"] = order_row("c-3", "o-3", TK, side="bid", px="0.4500", remaining="10.00")  # a bid: 10 x 45c
    v, _ = _shared_venue(rest)
    f = (await v.fetch_shard_funds())[2]
    assert f["resting"] == pytest.approx(2.0 + 4.5)  # was 4.0 + 2.0 + 4.5
    assert f["funds"] == pytest.approx(20.0 + 4.5 + 6.5)


async def test_failed_balance_reads_close_the_gate():
    s = RecordingStrategy()
    rest = FakeRest()
    rest.on("get_balance", *[ConnectionError("down")] * 50)
    c = cfg(balance_interval_s=0.05, balance_max_failures=2)
    r, _, _ = live_runner(s, rest=rest, config=c, venue_cfg=c.venue)
    r.balance_required_usd = 60.0

    async def probe():
        await asyncio.sleep(0.5)
        assert "balance" in r.gate.reasons
        await asyncio.sleep(5)

    r.add_source("probe", probe)
    await r.run(duration_s=0.8)
    assert "balance" in r.gate.reasons


# ============================================================================ LOW: exchange status / schedule (L3)
async def test_failed_status_polls_close_the_exchange_pause_gate():
    s = RecordingStrategy()
    rest = FakeRest()
    rest.on("get_exchange_status", *[ConnectionError("down")] * 50)
    c = cfg(exchange_status_interval_s=0.05, exchange_status_max_failures=3)
    r, _, _ = live_runner(s, rest=rest, config=c, venue_cfg=c.venue)

    async def probe():
        await asyncio.sleep(0.6)
        assert PAUSE_STREAM_REASON in r.gate.reasons and "unreadable" in r._pause  # noqa: SLF001
        await asyncio.sleep(5)

    r.add_source("probe", probe)
    await r.run(duration_s=0.9)
    assert "unreadable" in r._pause  # noqa: SLF001
    # a successful poll removes that reason
    r._on_exchange_status(r.clock_ns(), {"status": {"exchange_active": True, "trading_active": True}})  # noqa: SLF001
    assert "unreadable" not in r._pause  # noqa: SLF001


def test_a_midnight_close_is_the_end_of_the_day_not_a_closure():
    """Review probe 7: a session closing at "00:00" was dropped, leaving a ~21 h bogus
    Thursday closure that the plausibility check (looking at the PAST day) let through."""
    from zoneinfo import ZoneInfo

    ny = ZoneInfo("America/New_York")
    ok = [{"open_time": "00:00", "close_time": "23:59"}]
    week = {"start_time": "2026-01-01T00:00:00Z", "end_time": "2027-01-01T00:00:00Z",
            "monday": ok, "tuesday": ok, "wednesday": ok,
            "thursday": [{"open_time": "00:00", "close_time": "03:00"}, {"open_time": "05:00", "close_time": "00:00"}],
            "friday": [{"open_time": "00:00", "close_time": "00:00"}],  # a whole day written 00:00-00:00
            "saturday": ok, "sunday": ok}
    now = int(dt.datetime(2026, 9, 28, 12, 0, tzinfo=ny).timestamp() * 1e9)  # a Monday
    closures, notes = schedule_closures({"schedule": {"standard_hours": [week], "maintenance_windows": []}},
                                        now - 86_400 * 10**9, now + 8 * 86_400 * 10**9, now_ns=now)
    assert notes == []
    assert [(b - a) / 3.6e12 for a, b, _ in closures] == [2.0]  # only the Thursday 03:00-05:00 ET pause
    thu = int(dt.datetime(2026, 10, 1, 3, 0, tzinfo=ny).timestamp() * 1e9)
    assert closures[0][0] == thu


def test_the_plausibility_window_is_the_next_day():
    from zoneinfo import ZoneInfo

    ny = ZoneInfo("America/New_York")
    day = [{"open_time": "00:00", "close_time": "23:59"}]
    closed = {"start_time": "2026-01-01T00:00:00Z", "end_time": "2027-01-01T00:00:00Z", "monday": day, "tuesday": [],
              "wednesday": day, "thursday": day, "friday": day, "saturday": day, "sunday": day}
    mon = int(dt.datetime(2026, 9, 28, 20, 0, tzinfo=ny).timestamp() * 1e9)  # Monday 20:00 ET
    body = {"schedule": {"standard_hours": [closed]}}
    # the next 24 h (Monday 20:00 .. Tuesday 20:00) would be closed 20 h: implausible, ignored
    closures, notes = schedule_closures(body, mon - 86_400 * 10**9, mon + 8 * 86_400 * 10**9, now_ns=mon)
    assert closures == [] and any("standard_hours ignored" in n for n in notes)
    # judged on the PAST day (the start of the window, the old behaviour) it slipped through
    assert schedule_closures(body, mon - 86_400 * 10**9, mon + 8 * 86_400 * 10**9)[0] != []


@pytest.mark.parametrize("reason, scope", [
    ("market_paused", "market"), ("market is paused", "market"), ("paused", "market"),
    ("exchange_paused", "exchange"), ("trading_is_paused", "exchange"), ("exchange closed", "exchange"),
    ("outside trading hours", "exchange"), ("market_closed", ""), ("gate:exchange_pause", ""), ("insufficient_balance", ""),
])
def test_pause_reject_scope(reason, scope):
    assert pause_reject_scope(reason) == scope


async def test_a_market_level_pause_blocks_that_market_only_for_the_hold():
    s = OrderingStrategy(n=0)
    clock = {"t": T0}
    c = cfg(pause_reject_hold_s=30.0)
    r, _, _ = live_runner(s, config=c, venue_cfg=c.venue, clock_ns=lambda: clock["t"])
    r.push_result(OrderReject(clock["t"], 0, "c-1", TK, "market_paused", 400, "create"))
    r.process_pending()
    assert PAUSE_STREAM_REASON not in r.gate.reasons  # not global
    assert r.gate.check(TK) == MARKET_PAUSE_REASON and r.gate.check(TK2) == ""
    clock["t"] += 31 * NS_PER_S
    r.push_side("call", lambda ts: None)
    r.process_pending()
    assert r.gate.check(TK) == ""  # the hold ended
    r.push_result(OrderReject(clock["t"], 0, "c-2", TK, "exchange_paused", 400, "create"))
    r.process_pending()
    assert PAUSE_STREAM_REASON in r.gate.reasons  # an exchange-level code: global


# ============================================================================ LOW: fee check, recorder share
async def test_verify_fee_schedule_reads_only_the_configured_subaccount(tmp_path):
    sys.path.insert(0, str(REPO / "scripts"))
    import importlib

    mod = importlib.import_module("verify_fee_schedule")
    rest = FakeRest()
    await mod.check_fills(rest, None, 1.0, "KX", 5, subaccount=1)
    assert rest.of("iter_fills")[0][1]["subaccount"] == 1
    assert rest.of("iter_historical_fills")[0][1]["subaccount"] == 1
    live = tmp_path / "live.yaml"
    live.write_text("venue:\n  subaccount: 1\n  shared_account: true\n")
    assert mod.configured_subaccount(mod.parse_args(["--live-config", str(live)])) == 1
    assert mod.configured_subaccount(mod.parse_args(["--live-config", str(live), "--subaccount", "2"])) == 2
    assert mod.configured_subaccount(mod.parse_args(["--live-config", str(tmp_path / "none.yaml")])) is None


def test_the_recorder_takes_a_tenth_of_the_account_budget_by_default():
    sys.path.insert(0, str(REPO / "scripts"))
    import importlib

    rec = importlib.import_module("record")
    assert rec.RECORDER_ACCOUNT_SHARE == 0.1
    assert rec.recorder_account_share({}) == 0.1 and rec.recorder_account_share({"account_share": 0.05}) == 0.05
    import yaml

    feeds = yaml.safe_load((REPO / "config" / "feeds.yaml").read_text())
    assert rec.recorder_account_share(feeds["kalshi"]) <= 0.1


# ============================================================================ CLOCK
def test_the_session_clock_slews_the_macos_drift_away():
    """Review L4: mach_absolute_time runs ~3.2 ppm off the disciplined wall clock; the anchored
    clock's drift crossed the 250 ms block ~16 h into a session. Slewed at <= 50 ppm it tracks
    the wall clock (drift stays near zero) for 24 h."""
    wall = {"t": 1_790_000_000 * NS_PER_S}
    mono = {"t": 0}
    c = AnchoredClock(lambda: wall["t"], lambda: mono["t"])
    prev = c()
    worst = 0
    for _ in range(24 * 3600):  # one reading per second for 24 h
        mono["t"] += NS_PER_S
        wall["t"] += NS_PER_S + 3_200  # the wall clock gains 3.2 us per monotonic second
        t = c()
        assert 0 < t - prev <= NS_PER_S + 50_000  # strictly increasing, never a step
        prev = t
        worst = max(worst, abs(c.drift_ns()))
    assert worst < 1 * NS_PER_MS  # unslewed: 24 h x 11.5 ms/h = 276 ms


def test_a_wall_clock_step_still_shows_as_a_large_offset_for_hours():
    wall = {"t": 1_790_000_000 * NS_PER_S}
    mono = {"t": 0}
    c = AnchoredClock(lambda: wall["t"], lambda: mono["t"])
    c()
    wall["t"] += NS_PER_S  # a 1 s step (NTP makestep)
    for _ in range(3600):  # an hour later
        mono["t"] += NS_PER_S
        wall["t"] += NS_PER_S
        c()
    d = c.drift_ns()
    assert d >= NS_PER_S - 3600 * MAX_SLEW_PPM * 1_000 - 10  # decays at <= 50 us/s
    assert d > 250 * NS_PER_MS  # still blocks new orders (clock_block_ms 250)


@pytest.mark.parametrize("mode", ["paper", "live"])
async def test_a_session_on_a_slewing_clock_replays_bit_for_bit(tmp_path, mode):
    """Receive times are recorded: a clock that slews (here a wall clock running 20,000 ppm fast,
    so the 50 ppm slew is active the whole session) does not change the replay."""
    from dh.live.replay import load_session, logged_decisions, replay_session, replayed_decisions

    from .test_app import _setup

    rest, fake, lcfg, scfg, _ = _setup(tmp_path, mode, forbid_writes=(mode == "paper"))
    t0w, t0m = time.time_ns(), time.monotonic_ns()
    clock = AnchoredClock(lambda: t0w + int((time.monotonic_ns() - t0m) * 1.02), time.monotonic_ns)
    app = LiveApp(scfg, lcfg, mode, Overrides(rest=rest, ws_connect=fake.connect, install_signals=False, clock_ns=clock))
    runner = await app.build()
    rec = runner.recorder

    async def brti():
        end = time.monotonic() + 5
        while time.monotonic() < end:
            t = runner.clock_ns()
            ev = IndexTick(ts=t, ts_exch=t, index_id="BRTI", value=84_000.0 + (0.5 if (t // 10**8) % 2 else -0.5), feed="5hz")
            rec.write_event("events.test", ev)
            runner.push(ev)
            await asyncio.sleep(0.03)

    runner.add_source("brti", brti)
    await runner.run(duration_s=2.5)
    await app.close()
    assert clock.drift_ns() > 0  # the wall clock ran ahead: the slew was engaged
    info = load_session(tmp_path / "data")
    res = replay_session(tmp_path / "data", scfg, extra_streams=("events.test",))
    log = next((tmp_path / "logs").iterdir())
    live_a, live_l = logged_decisions(log, info.last_ts)
    rep_a, rep_l = replayed_decisions(res, info.last_ts)
    assert any(a[1] == "PlaceOrder" for a in live_a)
    assert live_a == rep_a and live_l == rep_l


# ============================================================================ DISK
@pytest.mark.real_guards
async def test_live_start_refuses_below_the_free_disk_minimum(tmp_path, monkeypatch):
    import dh.live.app as appmod

    from .conftest import healthy_watchdog_beat
    from .test_shared_account import _live

    monkeypatch.setattr(appmod, "watchdog_reader_for", lambda p, sub, clock: (lambda: healthy_watchdog_beat(sub, clock())))
    monkeypatch.setattr(appmod, "data_root_free_gb", lambda root: 9.5)
    rest, fake, lcfg, scfg, _ = _live(tmp_path)
    app = LiveApp(scfg, lcfg, "live", Overrides(rest=rest, ws_connect=fake.connect, install_signals=False))
    assert await app.run(duration_s=0.5) == 2 and rest.calls == [] and fake.conns == []


async def test_runner_gates_new_orders_below_the_disk_threshold():
    s = RecordingStrategy()
    free = {"gb": 50.0}
    c = replace(cfg(), disk=DiskCfg(min_free_gb_start=10.0, min_free_gb_gate=5.0, resume_margin_gb=1.0,
                                    check_interval_s=0.05))
    r, _, _ = live_runner(s, config=c, disk_free_gb=lambda: free["gb"])

    async def probe():
        await asyncio.sleep(0.15)
        assert DISK_REASON not in r.gate.reasons
        free["gb"] = 4.0
        await asyncio.sleep(0.2)
        assert DISK_REASON in r.gate.reasons
        free["gb"] = 5.5  # above the threshold, below threshold + margin: still closed
        await asyncio.sleep(0.2)
        assert DISK_REASON in r.gate.reasons
        free["gb"] = 7.0
        await asyncio.sleep(0.2)
        assert DISK_REASON not in r.gate.reasons
        await asyncio.sleep(5)

    r.add_source("probe", probe)
    await r.run(duration_s=1.0)
    st = [(e.status, e.detail) for e in s.events if isinstance(e, FeedStatus) and e.stream == RECONCILE_STREAM]
    assert ("stale", DISK_REASON) in st and ("resynced", DISK_REASON) in st
    assert r.metrics.get("dh_disk_free_gb") == 7.0


def test_live_example_config_states_every_new_key():
    cfg_ = load_live_config(REPO / "config" / "live.example.yaml")
    v = cfg_.venue
    assert v.subaccount == 1 and v.shared_account is True and v.key_restricted_to_subaccount and not v.allow_primary_account
    assert v.balance_max_failures >= 1 and v.exchange_status_max_failures >= 1 and v.cancel_rounds >= 1
    assert v.position_defer_max >= 1 and v.position_defer_max_s > 0
    assert cfg_.watchdog.runner_max_age_s == 10.0 and cfg_.watchdog.beat_interval_s > 0 and not cfg_.watchdog.allow_runner_key
    assert cfg_.disk.min_free_gb_start == 10.0 and cfg_.disk.min_free_gb_gate == 5.0 and cfg_.disk.check_interval_s == 60.0
    assert venue_scope_problems(v) == [] and live_config_problems(replace(cfg_, mode="live")) == []
    text = (REPO / "config" / "live.example.yaml").read_text()
    for key in ("allow_primary_account", "balance_max_failures", "exchange_status_max_failures", "position_defer_max",
                "cancel_rounds", "allow_runner_key", "beat_interval_s", "runner_max_age_s", "min_free_gb_start",
                "min_free_gb_gate"):
        assert key in text, key


def test_launchd_watchdog_template_uses_the_live_config_and_documents_the_beat():
    plist = (REPO / "deploy" / "launchd" / "com.dh.watchdog.plist").read_text()
    readme = (REPO / "deploy" / "launchd" / "README.md").read_text()
    assert "config/live.yaml" in plist and "--arm-on-start" in plist
    assert "heartbeat.json.watchdog" in readme and "heartbeat.json.watchdog" in plist
