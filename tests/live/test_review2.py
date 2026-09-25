"""Live review, round 2 (N1-N8): the paths the re-check found untested.

  * lag sustained beyond the baseline window, lag present from the start, the gate closing
    before the event that reveals the lag reaches the strategy (N2, nit)
  * a fill lost silently on the WebSocket during a positions check; a late REST fill during
    the reconnect confirmation (N4)
  * no reachable chronyd / unsynchronised / imprecise clock, exchange timestamps from the future
    (N5); a dying background loop (N6)
  * an excluded market settling during the session, a halted runner crossing midnight, a
    restart at 00:00, power loss right after a halt (N1, N3, N7)
"""

from __future__ import annotations

import asyncio
import time
from pathlib import Path

import pytest

from dh.core.actions import PlaceOrder
from dh.core.events import (
    FeedStatus,
    IndexTick,
    KalshiFill,
    KalshiMarketLifecycle,
    KalshiPositionSnapshot,
    OrderAck,
    OrderReject,
    RiskStateSeed,
    Settlement,
)
from dh.core.units import NS_PER_MS, NS_PER_S, PX_SCALE
from dh.live.config import LoopCfg
from dh.live.riskstate import DAY_NS, RiskBook, RiskStateStore, decide_seed, day_start
from dh.execution.exchange_sim import KalshiExchangeSim
from dh.execution.latency import LatencyModel
from dh.live.runner import CLOCK_STREAM, LAG_STREAM, LagMeter, LiveRunner
from dh.strategy.config import load_config
from dh.strategy.risk import RiskEngine

from .fakes import T0, RecordingStrategy, fill_row
from .test_runner import SPEC, TK, OrderingStrategy, cfg, live_runner

REPO = Path(__file__).resolve().parents[2]
RISK_CFG = load_config(REPO / "config" / "m1.yaml").risk
H = 3600 * NS_PER_S
D0 = T0 - T0 % DAY_NS


def fs(s, stream):
    return [(e.status, e.detail) for e in s.events if isinstance(e, FeedStatus) and e.stream == stream]


class RiskStrategy(RecordingStrategy):
    """A strategy with the real RiskEngine: seeds, and the daily-loss check on every BRTI tick."""

    def __init__(self) -> None:
        super().__init__(self._respond)
        self.risk = RiskEngine(RISK_CFG)
        self.eq = 0.0

    def equity(self, S=None):
        return self.eq

    def _respond(self, ev):
        if isinstance(ev, RiskStateSeed):
            return self.risk.on_seed(ev)
        if isinstance(ev, IndexTick):
            return self.risk.on_equity(ev.ts, self.eq)
        return []


# ============================================================================ N2: lag baseline
def _lag_series(lag_of_t, seconds, *, net=0.05, hz=5):
    lc = LoopCfg()
    m = LagMeter(int(lc.lag_window_s * NS_PER_S), int(lc.lag_confirm_s * NS_PER_S),
                 cap_ns=int(lc.baseline_cap_ms() * NS_PER_MS))
    out = {}
    for i in range(int(seconds * hz)):
        src = T0 + i * NS_PER_S // hz
        now = src + int((net + lag_of_t(i / hz)) * NS_PER_S)
        out[i // hz] = m.observe(IndexTick(now, src, "BRTI", 84000.0, "5hz"), now)
    return m, out


def test_lag_sustained_beyond_the_window_is_still_lag():
    m, out = _lag_series(lambda t: 3.0 if t >= 60 else 0.0, 13 * 60)
    assert out[30] == pytest.approx(0.0)
    assert out[120] == pytest.approx(3.0, abs=0.01) and out[600] == pytest.approx(3.0, abs=0.01)
    # the window no longer holds a fresh minute: the baseline is the cap (350 ms), not 3.05 s
    assert out[700] == pytest.approx(3.05 - 0.35, abs=0.01) and out[700] > LoopCfg().max_lag_s
    assert m.over_cap("idx:BRTI:5hz") == pytest.approx(3.05 * NS_PER_S, rel=1e-3)


def test_lag_present_from_the_start_is_lag():
    m, out = _lag_series(lambda t: 3.0, 120)
    assert out[0] > LoopCfg().max_lag_s and out[119] == pytest.approx(2.7, abs=0.01)
    # a normal latency below the cap is still absorbed (no false lag)
    _, calm = _lag_series(lambda t: 0.0, 60, net=0.2)
    assert max(calm.values()) == pytest.approx(0.0)


async def test_lag_from_start_closes_the_gate_before_the_strategy_sees_the_tick_and_alarms():
    clock = {"t": T0}

    def respond(ev):
        if isinstance(ev, IndexTick):
            return [PlaceOrder(f"q-{ev.ts}", TK, "bid", 4500, 100)]
        return []

    s = RecordingStrategy(respond)
    r, venue, rest = live_runner(s, config=cfg(loop={"max_lag_s": 1.0, "lag_confirm_s": 0.0}),
                                 clock_ns=lambda: clock["t"])
    clock["t"] += 200 * NS_PER_MS
    r.push(IndexTick(clock["t"], clock["t"] - 3 * NS_PER_S, "BRTI", 84000.0, "5hz"))  # 3 s behind from the start
    r.process_pending()
    await venue.wait_idle(1.0)
    r.process_pending()
    assert "lag" in r.gate.reasons and [x[0] for x in fs(s, LAG_STREAM)] == ["stale"]
    i_tick = next(i for i, e in enumerate(s.events) if isinstance(e, IndexTick))
    i_stale = next(i for i, e in enumerate(s.events) if isinstance(e, FeedStatus))
    assert i_tick < i_stale  # the FeedStatus follows the tick (its frame is not split) ...
    rej = [e for e in s.events if isinstance(e, OrderReject)]
    assert [e.reason for e in rej] == ["gate:lag"]  # ... but the order decided on it never went out
    assert rest.of("create_order") == [] and rest.of("batch_create_orders") == []
    assert r.metrics.get("dh_lag_baseline_over_cap_total", source="idx:BRTI:5hz") == 1
    r.refresh_metrics()
    assert r.metrics.get("dh_lag_baseline_seconds", source="idx:BRTI:5hz") == pytest.approx(3.0)


# ============================================================================ N4: fills before a confirming positions read
@pytest.mark.parametrize("ws_position_msg", [False, True])
async def test_silent_fill_loss_is_backfilled_before_any_position_check_confirms(ws_position_msg):
    s = OrderingStrategy(n=0)
    c = cfg(positions_interval_s=0.2, position_confirm_s=0.05, fills_backfill_interval_s=30.0,
            fills_backfill_min_age_s=10.0, fills_backfill_margin_s=2.0)
    start = time.time_ns()
    clock = lambda: T0 + (time.time_ns() - start)  # noqa: E731 - real time on the test market's day
    r, venue, rest = live_runner(s, config=c, clock_ns=clock)
    t0 = clock()
    s.om.request_place(PlaceOrder("c-1", TK, "bid", 4500, 200), t0)
    s.om.on_event(OrderAck(t0, 0, "c-1", "o-1", TK, 0, 200))
    rest.fills = [fill_row("t-9", "o-1", TK, px="0.4500", count="1.00", created_ns=t0, coid="c-1")]  # lost on the WS
    rest.positions = {TK: "1.00"}
    if ws_position_msg:  # the market_position message that followed the lost fill message
        r.push(KalshiPositionSnapshot(t0, 0, TK, 100, source="ws"))
    await r.run(duration_s=0.9)
    assert [e.trade_id for e in s.events if isinstance(e, KalshiFill)] == ["t-9"]
    assert s.om.position(TK) == 100 and s.om.stats["position_mismatches"] == 0
    reads = [n for n in rest.names() if n in ("iter_fills", "get_all_positions")]
    i_first_fills = reads.index("iter_fills")
    assert "get_all_positions" in reads[i_first_fills:]  # a fills read preceded the confirming positions read
    snaps = [e for e in s.events if isinstance(e, KalshiPositionSnapshot)]
    assert snaps and all(e.position == 100 for e in snaps)  # only agreeing snapshots reached the strategy


async def test_reconnect_confirmation_refetches_fills_before_confirming():
    """n4: the fill made during the outage is in positions but not yet in GET /portfolio/fills when
    the reconcile reads them; the confirmation round reads fills again and finds it."""
    s = OrderingStrategy(n=0)
    clock = {"t": T0}
    r, venue, rest = live_runner(s, config=cfg(position_confirm_s=0.05), clock_ns=lambda: clock["t"])
    s.om.request_place(PlaceOrder("c-1", TK, "bid", 4500, 200), T0)
    s.om.on_event(OrderAck(T0, 0, "c-1", "o-1", TK, 0, 200))
    rest.positions = {TK: "1.00"}
    r.push(FeedStatus(T0 + 1, 0, "kalshi.ws", "disconnected", ""))
    r.process_pending()
    clock["t"] = T0 + 5 * NS_PER_S
    r.push(FeedStatus(clock["t"], 0, "kalshi.ws", "connected", ""))
    r.process_pending()
    await asyncio.sleep(0.02)
    r.process_pending()
    assert r._pos_suspect  # noqa: SLF001 - positions ahead of fills: suspected, not confirmed
    rest.fills = [fill_row("t-2", "o-1", TK, px="0.4500", count="1.00", created_ns=T0 + 2 * NS_PER_S, coid="c-1")]
    clock["t"] += NS_PER_S
    await asyncio.sleep(0.2)
    r.process_pending()
    assert rest.names().count("iter_fills") == 2 and s.om.position(TK) == 100
    assert s.om.stats["position_mismatches"] == 0 and "reconciling" not in r.gate.reasons


# ============================================================================ N5: the clock gate fails closed
def _sampler(**rec):
    def f():
        return {"src": "chronyc", "offset_s": 0.0001, "est_error_s": 0.001, "synced": True, **rec}
    return f


@pytest.mark.parametrize("bad, why", [
    ({"src": "adjtimex"}, "unmeasurable"),
    ({"src": "unknown", "offset_s": None, "synced": None}, "unmeasurable"),
    ({"synced": False}, "not synchronised"),
    ({"synced": None}, "not synchronised"),
    ({"est_error_s": 0.4}, "estimated error"),
    ({"src": "timedatectl", "offset_s": None, "est_error_s": None}, "no offset"),
])
async def test_untrustworthy_clock_blocks_live_orders_and_tells_the_strategy(bad, why):
    s = RecordingStrategy()
    loop = {"clock_sample_s": 0.05, "clock_resample_s": 0.05, "clock_block_samples": 2}
    r, _, _ = live_runner(s, config=cfg(loop=loop), clock_sampler=_sampler(**bad))
    await r.run(duration_s=0.4)
    got = fs(s, CLOCK_STREAM)
    assert got and got[0][0] == "stale" and why in got[0][1] and "clock" in r.gate.reasons
    assert r.metrics.get("dh_clock_untrusted") == 1.0
    # paper mode: nothing to protect, the sample is informational
    s2 = RecordingStrategy()
    sim = KalshiExchangeSim(LatencyModel.fixed(submit_ms=5, response_ms=5, ws_ms=5), "conservative", lambda p, q, t: 0,
                            seed=1, id_prefix="paper")
    sim.register_market(SPEC)
    p = LiveRunner(s2, mode="paper", period_ns=50 * NS_PER_MS, cfg=cfg("paper", loop=loop), sim=sim, universe=[SPEC],
                   clock_sampler=_sampler(**bad))
    await p.run(duration_s=0.3)
    assert fs(s2, CLOCK_STREAM) == []


async def test_clock_gate_reopens_on_a_good_sample():
    seq = [_sampler(src="adjtimex")] * 3
    calls = {"n": 0}

    def sampler():
        calls["n"] += 1
        return seq[calls["n"] - 1]() if calls["n"] <= len(seq) else _sampler()()

    s = RecordingStrategy()
    loop = {"clock_sample_s": 0.05, "clock_resample_s": 0.05, "clock_block_samples": 2}
    r, _, _ = live_runner(s, config=cfg(loop=loop), clock_sampler=sampler)
    await r.run(duration_s=0.6)
    assert [x[0] for x in fs(s, CLOCK_STREAM)] == ["stale", "resumed"] and "clock" not in r.gate.reasons


async def test_exchange_timestamps_from_the_future_prove_the_clock_behind():
    """chronyd says all is well, but every market-data source is stamped 600 ms in our future:
    the local clock is behind by at least that much (latencies are never negative)."""
    s = RecordingStrategy()
    loop = {"clock_sample_s": 0.05, "clock_resample_s": 0.05, "clock_block_samples": 2, "max_lag_s": 5.0}
    r, _, _ = live_runner(s, config=cfg(loop=loop), clock_sampler=_sampler())

    async def feed():
        while True:
            t = r.clock_ns()
            r.push(IndexTick(t, t + 600 * NS_PER_MS, "BRTI", 84000.0, "5hz"))
            await asyncio.sleep(0.02)

    r.add_source("brti", feed)
    await r.run(duration_s=0.5)
    got = fs(s, CLOCK_STREAM)
    assert got and got[0][0] == "stale" and "behind exchange time" in got[0][1]


# ============================================================================ N6: supervised loops
@pytest.mark.parametrize("how", ["raises", "returns"])
async def test_a_dying_background_loop_stops_the_runner_with_exit_4(tmp_path, how):
    s = RecordingStrategy()
    store = RiskStateStore(tmp_path / "risk.json")
    r, venue, rest = live_runner(s, config=cfg(loop={"risk_state_interval_s": 0.05}), risk_store=store)

    async def dies():
        await asyncio.sleep(0.05)
        if how == "raises":
            raise RuntimeError("disk on fire")

    r._risk_state_loop = dies  # noqa: SLF001
    t0 = time.monotonic()
    code = await r.run(duration_s=5.0)
    assert code == 4 and time.monotonic() - t0 < 3.0
    assert "runner:risk_state" in r.stop_reason and ("disk on fire" in r.stop_reason or "returned" in r.stop_reason)
    assert r.metrics.get("dh_loop_deaths_total", loop="runner:risk_state") == 1
    assert "cancel_all_orders" in rest.names()  # the normal, cancelling shutdown


# ============================================================================ N1: excluded markets settle
@pytest.mark.parametrize("result, expect", [("no", 2.0), ("yes", -8.0)])
async def test_excluded_market_settlement_reaches_the_strategy_and_the_state(tmp_path, result, expect):
    """10 short YES sold today at 20c in an event excluded from the session, valued at the 20c ask
    at start-up (seed 0). It settles: +$2 (NO) or -$8 (YES) -- the strategy's loss limit and the
    persisted state see it right after the Settlement event."""
    ex = "KXBTCD-26SEP2513-T90000.00"
    now = D0 + 14 * H
    dec = decide_seed(now, None, type("P", (), {"pnl_usd": 0.0, "open_usd": -2.0})())
    book = RiskBook.from_decision(dec, {ex: (-1000, 2000)})
    s = RiskStrategy()
    store = RiskStateStore(tmp_path / "risk.json")
    clock = {"t": now}
    r, _, _ = live_runner(s, risk_store=store, risk_book=book, clock_ns=lambda: clock["t"])
    r.push_result(RiskStateSeed(now, 0, dec.day_start_ns, dec.day_pnl_usd))
    r.push(IndexTick(now + 1, now + 1, "BRTI", 84000.0, "5hz"))
    r.process_pending()
    assert s.risk.seed_day_pnl == pytest.approx(0.0)
    r.persist_risk_state(now + 2)
    st = store.load()
    assert st.day_pnl_usd == pytest.approx(0.0) and st.mark_usd == pytest.approx(-2.0) and st.realized_usd == pytest.approx(2.0)
    clock["t"] = now + H
    px = PX_SCALE if result == "yes" else 0
    r.push(KalshiMarketLifecycle(clock["t"], 0, ex, "determined", result=result))
    r.push(Settlement(clock["t"], 0, ex, result, None, px))
    r.process_pending()
    seeds = [e for e in s.events if isinstance(e, RiskStateSeed)]
    assert len(seeds) == 2 and seeds[1].day_pnl_usd == pytest.approx(expect)
    i_set = next(i for i, e in enumerate(s.events) if isinstance(e, Settlement))
    assert s.events.index(seeds[1]) == i_set + 1  # right after the event that caused it
    assert s.risk.seed_day_pnl == pytest.approx(expect)
    r.persist_risk_state(clock["t"] + 1)
    st = store.load()
    assert st.day_pnl_usd == pytest.approx(expect) and st.mark_usd == 0.0 and st.realized_usd == pytest.approx(expect)
    # a later 'settled' message for the same market changes nothing
    r.push(KalshiMarketLifecycle(clock["t"] + 5, 0, ex, "settled", result=result))
    r.process_pending()
    assert len([e for e in s.events if isinstance(e, RiskStateSeed)]) == 2


async def test_excluded_settlement_known_only_from_the_settled_message():
    ex = "KXBTCD-26SEP2513-T90000.00"
    now = D0 + 14 * H
    dec = decide_seed(now, None, type("P", (), {"pnl_usd": 0.0, "open_usd": -2.0})())
    s = RiskStrategy()
    r, _, _ = live_runner(s, risk_book=RiskBook.from_decision(dec, {ex: (-1000, 2000)}), clock_ns=lambda: now + H)
    r.push(KalshiMarketLifecycle(now + H, 0, ex, "settled", result="no", settlement_value="0"))
    r.process_pending()
    assert [e.day_pnl_usd for e in s.events if isinstance(e, RiskStateSeed)] == [pytest.approx(2.0)]


# ============================================================================ N3: a halted runner crosses midnight
async def test_daily_loss_halt_in_force_after_midnight_is_not_carried_into_the_next_day(tmp_path):
    s = RiskStrategy()
    store = RiskStateStore(tmp_path / "risk.json")
    clock = {"t": D0 + 21 * H}
    r, venue, _ = live_runner(s, risk_store=store, clock_ns=lambda: clock["t"])
    r.push(IndexTick(clock["t"], clock["t"], "BRTI", 84000.0, "5hz"))  # day D starts at equity 0
    r.process_pending()
    s.eq = -25.0
    clock["t"] = D0 + 22 * H
    r.push(IndexTick(clock["t"], clock["t"], "BRTI", 84000.0, "5hz"))  # -> Halt(all) daily_loss, persisted at once
    r.process_pending()
    assert s.risk.halted_all and store.load().halt_day_ns == D0
    clock["t"] = D0 + DAY_NS + 2 * NS_PER_S  # still running (halted) after midnight: periodic persist
    r.push_side("persist")
    r.process_pending()
    st = store.load()
    assert st.day_start_ns == D0 + DAY_NS and st.halted and st.halt_reason == "daily_loss" and st.halt_day_ns == D0
    d = decide_seed(D0 + DAY_NS + 8 * H, st, None)  # the operator restarts next morning
    assert not d.halted and d.day_pnl_usd == pytest.approx(0.0)
    await venue.wait_idle(1.0)
    # control: a sticky halt decided on day D is still carried
    st.halt_reason = "reconciliation:position_mismatch"
    assert decide_seed(D0 + DAY_NS + 8 * H, st, None).halted


async def test_power_loss_right_after_a_halt_keeps_it(tmp_path, monkeypatch):
    """The halt is on disk (file AND directory entry fsynced) before anything else happens: a
    crash before the next periodic persist cannot lose it."""
    import os

    import dh.live.riskstate as rs

    synced: list[str] = []
    real_fsync = os.fsync
    monkeypatch.setattr(rs, "fsync_dir", lambda p: synced.append(f"dir:{p}"))
    monkeypatch.setattr(rs.os, "fsync", lambda fd: (synced.append("file"), real_fsync(fd)))
    s = RiskStrategy()
    path = tmp_path / "state" / "risk.json"
    r, venue, _ = live_runner(s, risk_store=RiskStateStore(path), clock_ns=lambda: D0 + 10 * H,
                              config=cfg(loop={"risk_state_interval_s": 0.0}))  # no periodic persist at all
    r.push(IndexTick(D0 + 10 * H, D0 + 10 * H, "BRTI", 84000.0, "5hz"))
    r.process_pending()
    s.eq = -30.0
    r.push(IndexTick(D0 + 10 * H + 1, D0 + 10 * H + 1, "BRTI", 84000.0, "5hz"))
    r.process_pending()
    assert synced == ["file", f"dir:{path.parent}"]
    st = RiskStateStore(path).load()  # a new process after the power comes back
    assert st.halted and st.halt_reason == "daily_loss" and st.day_pnl_usd == pytest.approx(-30.0)
    assert decide_seed(D0 + 11 * H, st, None).halted
    await venue.wait_idle(1.0)


# ============================================================================ N7: a restart at 00:00
async def test_restart_at_midnight_derives_the_new_day():
    from dh.live.app import LiveApp, Overrides

    from .test_app import _setup

    import tempfile

    with tempfile.TemporaryDirectory() as d:
        rest, fake, lcfg, scfg, _ = _setup(Path(d), "live", forbid_writes=True)
    clock = {"t": D0 + DAY_NS - 50 * NS_PER_MS}  # 23:59:59.950 of day D
    app = LiveApp(scfg, lcfg, "live", Overrides(rest=rest, clock_ns=lambda: clock["t"]))
    app.rest = rest
    rest.fills = [fill_row("f-y", "o-y", TK, side="bid", px="0.5000", count="10.00", created_ns=D0 + 23 * H)]
    orig = rest.iter_settlements

    async def slow_settlements(**kw):
        clock["t"] += 100 * NS_PER_MS  # midnight passes while the day is derived
        async for x in orig(**kw):
            yield x

    rest.iter_settlements = slow_settlements
    pnl, seed_ts = await app._derive_today({}, 0)  # noqa: SLF001
    assert day_start(seed_ts) == D0 + DAY_NS and pnl.day_start_ns == D0 + DAY_NS
    assert pnl.fills == 0 and pnl.pnl_usd == 0.0  # yesterday's -$5 is not today's
    assert [k["min_ts"] for _, k in rest.of("iter_fills")] == [D0 // NS_PER_S, (D0 + DAY_NS) // NS_PER_S]
