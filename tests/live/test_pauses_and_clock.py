"""Exchange pauses, the exchange schedule, REST data recency (user_data_timestamp), the macOS
clock sampler, queue-position coverage and the live inbound rules for a subaccount-restricted
key (offline: fake REST, fake clock)."""

from __future__ import annotations

import asyncio
import datetime as dt
import time
from dataclasses import replace

import pytest

from dh.core.events import FeedStatus, KalshiFill, KalshiOrderUpdate, KalshiPositionSnapshot, OrderReject
from dh.core.units import NS_PER_S
from dh.live.config import LoopCfg, VenueCfg
from dh.live.replay import UniverseReplay
from dh.live.runner import (
    PAUSE_STREAM_REASON,
    RECONCILE_STREAM,
    is_pause_reject,
    own_series_ok,
    own_subaccount_ok,
    trusted_clock_sources,
)
from dh.live.startup import closure_at, schedule_closures, shard_status

from dh.store.recorder import sample_clock as _real_sample_clock

from .fakes import T0, RecordingStrategy
from .test_review2 import fs
from .test_runner import TK, OrderingStrategy, cfg, confirm_read, live_runner, tick

ET = "America/New_York"
REAL_SAMPLE_CLOCK = _real_sample_clock  # bound at import, before the autouse healthy-chrony patch


def status(trading: bool = True, exchange: bool = True, shard2: tuple[bool, bool] | None = None) -> dict:
    body = {"exchange_active": exchange, "trading_active": trading}
    if shard2 is not None:
        body["exchange_index_statuses"] = [
            {"exchange_index": 0, "description": "default", "exchange_active": True, "trading_active": True,
             "intra_exchange_transfers_active": True},
            {"exchange_index": 2, "description": "crypto", "exchange_active": shard2[0], "trading_active": shard2[1],
             "intra_exchange_transfers_active": True}]
    return {"status": shard_status(body, (2,))}


# ============================================================================ status of OUR shard
def test_shard_status_reads_the_shards_own_entry():
    # the top level describes shard 0 (openapi 3.31.0): shard 2's own entry decides
    s = shard_status({"exchange_active": True, "trading_active": True, "exchange_index_statuses": [
        {"exchange_index": 2, "exchange_active": True, "trading_active": False}]}, (2,))
    assert s["exchange_active"] and not s["trading_active"] and s["source"] == {"2": "exchange_index_statuses"}
    s = shard_status({"exchange_active": True, "trading_active": True}, (2,))  # no breakdown: top level
    assert s["trading_active"] and "top_level" in s["source"]["2"]
    s = shard_status({"exchange_active": False, "trading_active": True}, (2,))  # an exchange pause
    assert not s["exchange_active"] and not s["trading_active"]


async def test_live_start_refuses_when_our_shard_is_paused(tmp_path):
    from dh.live.app import LiveApp, Overrides

    from .test_app import _setup

    rest, fake, lcfg, scfg, _ = _setup(tmp_path, "live", forbid_writes=True)
    rest.exchange = {"exchange_active": True, "trading_active": True, "exchange_index_statuses": [
        {"exchange_index": 2, "exchange_active": True, "trading_active": False, "description": "x",
         "intra_exchange_transfers_active": True}]}
    app = LiveApp(scfg, lcfg, "live", Overrides(rest=rest, ws_connect=fake.connect, install_signals=False))
    assert await app.run(duration_s=0.5) == 2 and fake.conns == []


# ============================================================================ pauses in the runner
async def test_trading_pause_blocks_orders_pulls_quotes_and_reconciles_before_resuming():
    s = RecordingStrategy()
    r, v, rest = live_runner(s)
    r.push_side("exchange_status", status(shard2=(True, False)))
    r.process_pending()
    assert PAUSE_STREAM_REASON in r.gate.reasons and "reconciling" in r.gate.reasons
    assert fs(s, RECONCILE_STREAM) == [("stale", PAUSE_STREAM_REASON)]  # the strategy cancels its quotes
    assert r.metrics.get("dh_exchange_paused") == 1.0 and r.metrics.get("dh_trading_active") == 0.0
    n_before = len(rest.calls)
    r.push_side("exchange_status", status(shard2=(True, True)))
    r.process_pending()
    assert PAUSE_STREAM_REASON not in r.gate.reasons and "reconciling" in r.gate.reasons  # not before the re-read
    await asyncio.sleep(0.05)
    r.process_pending()
    reads = [n for n, _, _ in rest.calls[n_before:]]
    assert "iter_fills" in reads and "get_all_positions" in reads and "iter_orders" in reads
    assert "reconciling" not in r.gate.reasons
    assert fs(s, RECONCILE_STREAM) == [("stale", PAUSE_STREAM_REASON), ("resynced", PAUSE_STREAM_REASON)]


async def test_exchange_pause_is_logged_as_blocking_cancels_too(caplog):
    s = RecordingStrategy()
    r, _, _ = live_runner(s)
    r.push_side("exchange_status", status(exchange=False))
    r.process_pending()
    assert PAUSE_STREAM_REASON in r.gate.reasons and r.metrics.get("dh_exchange_active") == 0.0
    assert "cancels are rejected too" in caplog.text and "cancel_order_on_pause" in caplog.text


async def test_scheduled_closure_pulls_quotes_ahead_and_the_poll_ends_it():
    s = RecordingStrategy()
    clock = {"t": T0}
    r, _, _ = live_runner(s, clock_ns=lambda: clock["t"])
    start = T0 + 90 * NS_PER_S  # the weekly pause starts in 90 s; lead 60 s
    r.push_side("exchange_schedule", {"closures": [(start, start + 7200 * NS_PER_S, "standard_hours")], "notes": []})
    r.process_pending()
    assert PAUSE_STREAM_REASON not in r.gate.reasons and r.metrics.get("dh_next_closure_ts") == start / NS_PER_S
    clock["t"] += 31 * NS_PER_S  # 59 s before the start: inside the lead
    r.push_side("exchange_status", status())
    r.process_pending()
    assert PAUSE_STREAM_REASON in r.gate.reasons and "scheduled standard_hours closure" in r._pause["schedule"]  # noqa: SLF001
    clock["t"] = start + 30 * NS_PER_S  # the pause began: the status poll confirms it
    r.push_side("exchange_status", status(trading=False))
    r.process_pending()
    clock["t"] = start + 300 * NS_PER_S  # still paused (status), the schedule no longer needed
    r.push_side("exchange_status", status(trading=False))
    r.process_pending()
    assert PAUSE_STREAM_REASON in r.gate.reasons and set(r._pause) == {"status"}  # noqa: SLF001
    clock["t"] = start + 7201 * NS_PER_S  # over, and the exchange says trading again
    r.push_side("exchange_status", status())
    r.process_pending()
    assert PAUSE_STREAM_REASON not in r.gate.reasons
    # a standard-hours closure the exchange does not confirm (a misread schedule) costs minutes, not hours
    start2 = clock["t"] + 30 * NS_PER_S
    r.push_side("exchange_schedule", {"closures": [(start2, start2 + 7200 * NS_PER_S, "standard_hours")], "notes": []})
    r.process_pending()
    assert PAUSE_STREAM_REASON in r.gate.reasons
    clock["t"] = start2 + 61 * NS_PER_S
    r.push_side("exchange_status", status())
    r.process_pending()
    assert PAUSE_STREAM_REASON not in r.gate.reasons
    # a MAINTENANCE window (explicit datetimes) holds for its whole length
    start3 = clock["t"] + 30 * NS_PER_S
    r.push_side("exchange_schedule", {"closures": [(start3, start3 + 3600 * NS_PER_S, "maintenance")], "notes": []})
    clock["t"] = start3 + 600 * NS_PER_S
    r.push_side("exchange_status", status())
    r.process_pending()
    assert PAUSE_STREAM_REASON in r.gate.reasons and "maintenance" in r._pause["schedule"]  # noqa: SLF001


async def test_a_place_rejected_for_a_pause_blocks_and_polls_now():
    s = RecordingStrategy()
    clock = {"t": T0}
    r, _, _ = live_runner(s, clock_ns=lambda: clock["t"])
    r.push_result(OrderReject(clock["t"], 0, "c-1", TK, "trading_is_paused", 400, "create"))
    r.process_pending()
    assert PAUSE_STREAM_REASON in r.gate.reasons and r._status_now.is_set()  # noqa: SLF001
    # the strategy sees the reject FIRST, then the reconcile stale (replay order)
    kinds = [type(e).__name__ for e in s.events if isinstance(e, (OrderReject, FeedStatus))]
    assert kinds[:2] == ["OrderReject", "FeedStatus"] and fs(s, RECONCILE_STREAM)[0][0] == "stale"
    r.push_side("exchange_status", status())  # the poll says active, but the hold is not over
    r.process_pending()
    assert PAUSE_STREAM_REASON in r.gate.reasons
    clock["t"] += 31 * NS_PER_S
    r.push_side("exchange_status", status())
    r.process_pending()
    assert PAUSE_STREAM_REASON not in r.gate.reasons


@pytest.mark.parametrize("reason, pause", [
    ("trading_is_paused", True), ("exchange_paused: trading is paused", True), ("exchange_closed", True),
    ("trading_not_active", True), ("market_inactive", False), ("post_only_cross", False), ("gate:exchange_pause", False),
    ("market_closed", False), ("insufficient_balance", False)])
def test_pause_reject_reasons(reason, pause):
    assert is_pause_reject(reason) is pause


async def test_exchange_and_balance_loops_poll_and_gate():
    s = RecordingStrategy()
    r, v, rest = live_runner(s, config=cfg(exchange_status_interval_s=0.05, exchange_schedule_interval_s=0.1,
                                           balance_interval_s=0.05))
    rest.exchange = {"exchange_active": True, "trading_active": False}
    rest.balances = {2: "5.0000"}
    r.balance_required_usd = 60.0
    await r.run(duration_s=0.4)
    names = rest.names()
    assert "get_exchange_status" in names and "get_exchange_schedule" in names
    assert rest.of("get_balance")[0][1] == {"subaccount": 0, "exchange_index": 2}
    assert fs(s, RECONCILE_STREAM)[0] == ("stale", PAUSE_STREAM_REASON)
    assert r.metrics.get("dh_balance_dollars", exchange_index="2") == 5.0


# ============================================================================ the exchange schedule
def _ns(y, mo, d, h=0, mi=0, tz=ET) -> int:
    from zoneinfo import ZoneInfo

    return int(dt.datetime(y, mo, d, h, mi, tzinfo=ZoneInfo(tz)).timestamp() * 1e9)


def _week(thursday: list) -> dict:
    full = [{"open_time": "00:00", "close_time": "23:59"}]
    return {"start_time": "2026-01-01T00:00:00Z", "end_time": "2027-01-01T00:00:00Z", "monday": full, "tuesday": full,
            "wednesday": full, "thursday": thursday, "friday": full, "saturday": full, "sunday": full}


def test_weekly_thursday_pause_and_maintenance_windows_from_the_schedule():
    body = {"schedule": {"standard_hours": [_week([{"open_time": "00:00", "close_time": "03:00"},
                                                   {"open_time": "05:00", "close_time": "23:59"}])],
                         "maintenance_windows": [{"start_datetime": "2026-10-03T12:00:00Z",
                                                  "end_datetime": "2026-10-03T13:00:00Z"}]}}
    lo, hi = _ns(2026, 9, 28), _ns(2026, 10, 5)  # Monday .. Monday (Thursday = 2026-10-01)
    closures, notes = schedule_closures(body, lo, hi)
    assert notes == []
    thu = (_ns(2026, 10, 1, 3), _ns(2026, 10, 1, 5), "standard_hours")  # 07:00-09:00Z (EDT)
    maint = (_ns(2026, 10, 3, 12, tz="UTC"), _ns(2026, 10, 3, 13, tz="UTC"), "maintenance")
    assert closures == [thu, maint]  # 23:59 closes join the next day's 00:00 opens: no nightly gap
    assert closure_at(closures, thu[0] - 30 * NS_PER_S, 60 * NS_PER_S) == thu
    assert closure_at(closures, thu[0] - 90 * NS_PER_S, 60 * NS_PER_S) is None
    assert closure_at(closures, thu[1], 60 * NS_PER_S) is None


def test_implausible_or_missing_standard_hours_do_not_close_trading():
    body = {"schedule": {"standard_hours": [_week([]) | {"monday": [], "tuesday": [], "wednesday": []}],
                         "maintenance_windows": []}}
    closures, notes = schedule_closures(body, _ns(2026, 9, 28), _ns(2026, 10, 5))
    assert closures == [] and "implausible" not in notes and any("standard_hours ignored" in n for n in notes)
    assert schedule_closures({}, 0, 10**18) == ([], [])
    assert schedule_closures({"schedule": {"standard_hours": [], "maintenance_windows": [{"start_datetime": "x"}]}},
                             0, 10**18)[0] == []


# ============================================================================ REST recency (finding 12)
async def test_positions_older_than_the_last_ws_fill_never_confirm_a_mismatch():
    s = OrderingStrategy(n=0)
    clock = {"t": T0}
    r, _, _ = live_runner(s, config=cfg(position_confirm_s=5.0), clock_ns=lambda: clock["t"])
    tf = T0 - 2 * NS_PER_S
    r.push(KalshiFill(clock["t"], tf, TK, "tr-1", "o-1", "", "bid", 4500, 200, False, 0, 0, False))  # WS fill at tf
    r.process_pending()
    assert s.om.position(TK) == 200
    r.push_side("positions", {TK: 100})  # the exchange view lags
    r.process_pending()
    clock["t"] += 6 * NS_PER_S
    confirm_read(r, clock["t"])
    r.push_side("positions_checked", {"positions": {TK: 100}, "as_of_ns": tf - NS_PER_S})  # validated before the fill
    r.process_pending()
    assert not [e for e in s.events if isinstance(e, KalshiPositionSnapshot)]
    assert r.metrics.get("dh_position_reads_stale_total") == 1.0 and r._positions_now.is_set()  # noqa: SLF001
    r.push_side("positions_checked", {"positions": {TK: 100}, "as_of_ns": tf + NS_PER_S})  # recent enough: confirms
    r.process_pending()
    snaps = [e for e in s.events if isinstance(e, KalshiPositionSnapshot)]
    assert len(snaps) == 1 and snaps[0].position == 100


async def test_positions_reads_carry_the_user_data_timestamp():
    s = OrderingStrategy(n=0)
    r, v, rest = live_runner(s)
    rest.user_data_ts = "2026-09-25T12:00:00.500Z"
    pos, as_of = await r._read_positions()  # noqa: SLF001
    names = rest.names()
    assert names.index("get_user_data_timestamp") < names.index("get_all_positions")  # the timestamp first
    assert as_of == 1790337600_500_000_000 and pos == {}
    rest.user_data_ts = None  # unavailable: the read still happens, the other guards stay in force
    assert (await r._read_positions())[1] is None  # noqa: SLF001


# ============================================================================ queue positions (finding 13)
async def test_queue_positions_coverage_is_measured_and_verified_once():
    s = OrderingStrategy(n=1)
    r, v, rest = live_runner(s)
    r.push(tick())
    r.process_pending()
    await v.wait_idle(1.0)
    r.process_pending()
    w = s.om.working()[0]
    assert w.state.name == "RESTING"
    logged = []
    r.jlog = lambda kind, ts, **p: logged.append((kind, p))  # type: ignore[method-assign]
    r._queue_coverage([TK], [], w.created_ns + 2 * NS_PER_S)  # noqa: SLF001 - shard-2 orders missing
    assert r.metrics.get("dh_queue_positions_coverage") == 0.0
    assert r.metrics.get("dh_verify_live", check="queue_positions_covers_shard") == 0.0
    r._queue_coverage([TK], [(w.order_id, TK, 500)], w.created_ns + 2 * NS_PER_S)  # noqa: SLF001
    assert r.metrics.get("dh_queue_positions_coverage") == 1.0
    assert [p["exchange_indexes"] for k, p in logged if k == "verify_live"] == [[2]]  # logged once


# ============================================================================ macOS clock
def test_sntp_is_trusted_on_macos_only():
    assert trusted_clock_sources("darwin") == ("chronyc", "timedatectl", "sntp")
    assert trusted_clock_sources("linux") == ("chronyc", "timedatectl")
    r, _, _ = live_runner(RecordingStrategy())
    rec = {"src": "sntp", "offset_s": 0.042, "est_error_s": 0.021, "synced": None}
    r.platform = "darwin"
    assert r.clock_sample_problem(rec) == ""  # "synchronised" = sntp answered within the bound
    assert "error bound" in r.clock_sample_problem(rec | {"est_error_s": 0.4})
    assert "no offset" in r.clock_sample_problem(rec | {"offset_s": None})
    assert "not synchronised" in r.clock_sample_problem(rec | {"est_error_s": None})
    r.platform = "linux"
    assert "unmeasurable" in r.clock_sample_problem(rec)


async def test_a_mac_40ms_behind_ntp_keeps_trading():
    s = RecordingStrategy()
    loop = {"clock_sample_s": 0.05, "clock_resample_s": 0.05, "clock_block_samples": 1, "clock_alarm_ms": 100.0}
    r, _, _ = live_runner(s, config=cfg(loop=loop),
                          clock_sampler=lambda: {"src": "sntp", "offset_s": 0.042, "est_error_s": 0.02, "synced": None})
    r.platform = "darwin"
    await r.run(duration_s=0.3)
    assert "clock" not in r.gate.reasons and r.metrics.get("dh_clock_untrusted") == 0.0
    assert abs(r.metrics.get("dh_clock_offset_seconds") - 0.042) < 0.01 and not r.metrics.get("dh_clock_alarms_total")


def test_the_live_clock_loop_samples_sntp_on_a_mac(monkeypatch):
    """The runner's default sampler is the recorder's sample_clock: without chronyc /
    timedatectl / adjtimex it returns the query-only sntp measurement, which the live gate
    trusts on macOS."""
    import dh.store.recorder as rec

    monkeypatch.setattr(rec, "_chronyc", lambda: None)
    monkeypatch.setattr(rec, "_timedatectl", lambda: None)
    monkeypatch.setattr(rec, "_adjtimex", lambda: None)
    monkeypatch.setattr(rec, "_sntp", lambda: {"offset_s": 0.04, "est_error_s": 0.02, "server": "time.apple.com",
                                               "raw": "+0.040000 +/- 0.020000 time.apple.com 17.253.4.125"})
    sample = REAL_SAMPLE_CLOCK()  # the real function (tests/live/conftest.py patches the module attribute)
    assert sample["src"] == "sntp" and sample["offset_s"] == 0.04 and sample["synced"] is None
    r, _, _ = live_runner(RecordingStrategy())
    r.platform = "darwin"
    assert r.clock_sample_problem(sample) == ""


# ============================================================================ inbound rules (restricted key)
def test_own_subaccount_rule_for_restricted_keys():
    f = lambda sa: KalshiFill(1, 0, TK, "t", "o", "", "bid", 4500, 100, False, 0, 0, subaccount=sa)  # noqa: E731
    assert own_subaccount_ok(f(1), 1) and not own_subaccount_ok(f(0), 1)  # full-account key: no field = primary
    assert own_subaccount_ok(f(0), 1, key_restricted=True)  # restricted key: server-scoped, no field = ours
    assert not own_subaccount_ok(f(2), 1, key_restricted=True)  # an explicit other number never is
    assert own_subaccount_ok(f(0), 0, key_restricted=True) and not own_subaccount_ok(f(1), 0, key_restricted=True)
    assert own_series_ok(f(1), ("KXBTCD",)) and not own_series_ok(replace(f(1), ticker="KXNHL-X-Y"), ("KXBTCD",))
    assert own_series_ok(replace(f(1), ticker="KXNHL-X-Y"), ())


async def test_runner_and_replay_apply_the_same_inbound_rules():
    s = RecordingStrategy()
    c = cfg(subaccount=1, key_restricted_to_subaccount=True)
    r, _, _ = live_runner(s, config=c, venue_cfg=c.venue, series=("KXBTCD",))
    assert r.subaccount == 1 and r.key_restricted
    t = time.time_ns()
    evs = [KalshiFill(t, 0, TK, "t1", "o", "", "bid", 4500, 100, False, 0, 0, subaccount=0),  # no field: ours
           KalshiFill(t + 1, 0, TK, "t2", "o", "", "bid", 4500, 100, False, 0, 0, subaccount=5),  # other subaccount
           KalshiFill(t + 2, 0, "KXNHL-26SEP22-FLA", "t3", "o", "", "bid", 4500, 100, False, 0, 0, subaccount=1),
           KalshiOrderUpdate(t + 3, 0, TK, "o", "c", "resting", "bid", 4500, 100, 0, 100, subaccount=0)]
    for e in evs:
        r.push(e)
    r.process_pending()
    got = [(type(e).__name__, getattr(e, "trade_id", "")) for e in s.events if isinstance(e, (KalshiFill, KalshiOrderUpdate))]
    assert got == [("KalshiFill", "t1"), ("KalshiOrderUpdate", "")]
    assert r.metrics.get("dh_foreign_subaccount_events_total", type="KalshiFill") == 1.0
    assert r.metrics.get("dh_foreign_series_events_total", type="KalshiFill") == 1.0
    s2 = RecordingStrategy()
    rep = UniverseReplay(s2, [], live=True, subaccount=1, key_restricted=True, series=("KXBTCD",))
    for e in evs:
        rep.on_event(e)
    assert [(type(e).__name__, getattr(e, "trade_id", "")) for e in s2.events] == got


def test_loop_cfg_default_alarm_is_unchanged():
    assert LoopCfg().clock_alarm_ms == 5.0 and VenueCfg().exchange_indexes == (2,)
