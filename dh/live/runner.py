"""LiveRunner: one asyncio process, ONE ordered event queue, ONE strategy consumer.

    runner = LiveRunner(mm, mode="paper", period_ns=200_000_000, cfg=live_cfg, sim=sim, ...)
    runner.add_source("kalshi.ws", lambda: ws.run(runner.push))
    exit_code = await runner.run()

Data path
  * Sources (KalshiWS, external FeedClients) record every inbound frame raw (Recorder) and
    normalize it; the normalized events go to ``push`` -> the queue. Timestamps are local
    receive times; ``push`` keeps them non-decreasing in queue order (a backwards clock step
    is clamped and counted).
  * Adapter results (order acks/rejects, cancel acks, reconciliation updates, gate rejects,
    hedge updates) go to ``push_result``; events the runner derives itself (checked position
    snapshots, back-filled fills) are injected in queue order. Both are recorded as
    codec-encoded events on stream ``events.live`` (so a live session replays bit-for-bit,
    see dh.live.replay); results are queued with ts = receive time.
  * Live inbound rules: exchange order-group ids are translated to the strategy's logical id
    (foreign groups dropped); WS market_position snapshots go through the same persistence
    check as the REST positions instead of reaching the strategy directly.
  * The consumer task takes items in order and hands events to ``EventPump`` (dh.live.pump),
    which calls ``strategy.on_event`` synchronously, never concurrently, with ``Timer``
    events synthesized on the grid ``k * period_ns`` exactly as the backtest runner does.
    A wake-up is scheduled for the next grid point (and the next simulator delivery in
    paper mode), so timers keep flowing when the market is quiet.
  * Actions returned by the strategy never block the consumer: live mode hands Kalshi
    actions to ``KalshiVenue.submit`` (asyncio tasks), paper mode to the simulator inside
    the pump, hedge actions to the hedge venue; ``Log`` actions go to the JSON log.

Safety
  * Order gate: new orders are refused (as OrderReject events, reason 'gate:<why>') while the
    kill file exists, after a strategy Halt, on a fee mismatch, on consumer lag > max_lag_s
    or a loop stall, during shutdown, and per market after a close-time / tick-grid change
    or a spec change found by re-discovery. Cancels always pass.
  * Kill file: checked every ``kill_check_interval_s`` on the consumer loop -> cancel all
    (REST) and stop.
  * Heartbeat file every ``heartbeat_interval_s`` while the consumer is alive (watchdog).
  * Graceful shutdown: gate closed, consumer drained, in-flight requests awaited, cancel-all
    via REST + a resting-order check, order group deleted, sources stopped, recorder flushed.
"""

from __future__ import annotations

import asyncio
import dataclasses
import logging
import os
import time
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import orjson

from dh.core.actions import (
    Action,
    CancelAll,
    CancelHedge,
    CancelOrder,
    Halt,
    Log,
    PlaceHedge,
    PlaceOrder,
    Resume,
)
from dh.core.events import (
    Event,
    ExtBBO,
    ExtBookDelta,
    ExtBookSnapshot,
    ExtTrade,
    FeedStatus,
    IndexTick,
    KalshiBookDelta,
    KalshiBookSnapshot,
    KalshiFeeUpdate,
    KalshiFill,
    KalshiMarketLifecycle,
    KalshiOrderGroupUpdate,
    KalshiPositionSnapshot,
    KalshiTicker,
    KalshiTrade,
    OrderReject,
    Timer,
)
from dh.core.market import MarketSpec
from dh.core.units import NS_PER_S, PX_SCALE
from dh.live.config import LiveConfig
from dh.live.monitor import JsonLog, KillFile, Metrics, MetricsServer, write_heartbeat
from dh.live.pump import EventPump, OrderingError
from dh.live.startup import spec_to_dict

log = logging.getLogger("dh.live.runner")

RESULT_STREAM = "events.live"
PAPER_STREAM = "events.paper"
META_STREAM = "meta"
LIVE_ORDER_STATES = ("PENDING_NEW", "RESTING", "PENDING_CANCEL", "PENDING_AMEND")
MARKET_DATA = (IndexTick, KalshiBookDelta, KalshiBookSnapshot, KalshiTrade, KalshiTicker, ExtBBO, ExtBookDelta,
               ExtBookSnapshot, ExtTrade)


@dataclass(frozen=True, slots=True)
class Wake:
    """Consumer wake-up at local time ``ts`` (timers / simulator deliveries / housekeeping)."""

    ts: int


@dataclass(frozen=True, slots=True)
class Side:
    """Side-channel input processed in queue order at ``ts`` (not a strategy event)."""

    ts: int
    kind: str  # universe_add | spec_changed | positions | positions_ws | fills | resting | queue_positions | call
    payload: Any = None


_STOP = object()


class OrderGate:
    """Runner-level interlock for NEW orders (cancels are never gated)."""

    def __init__(self) -> None:
        self.reasons: dict[str, int] = {}  # reason -> since (ns)
        self.tickers: dict[str, str] = {}  # blocked ticker -> reason

    @property
    def closed(self) -> bool:
        return bool(self.reasons)

    def close(self, reason: str, ts: int) -> bool:
        if reason in self.reasons:
            return False
        self.reasons[reason] = ts
        return True

    def open(self, reason: str) -> bool:
        return self.reasons.pop(reason, None) is not None

    def block(self, tickers: Iterable[str], reason: str) -> list[str]:
        new = [t for t in tickers if t not in self.tickers]
        for t in new:
            self.tickers[t] = reason
        return new

    def check(self, ticker: str) -> str:
        """'' if a new order in ``ticker`` may be sent, else the reason."""
        if self.reasons:
            return next(iter(self.reasons))
        return self.tickers.get(ticker, "")


class LiveRunner:
    """See module docstring. All I/O objects are injected (tests use fakes)."""

    def __init__(
        self,
        strategy: Any,
        *,
        mode: str,
        period_ns: int,
        cfg: LiveConfig | None = None,
        venue: Any = None,
        sim: Any = None,
        paper_fees: Any = None,
        hedge: Any = None,
        recorder: Any = None,
        jsonlog: JsonLog | None = None,
        metrics: Metrics | None = None,
        clock_ns: Callable[[], int] = time.time_ns,
        monotonic: Callable[[], float] = time.monotonic,
        kill_file: KillFile | None = None,
        heartbeat_path: str | Path | None = None,
        fee_engine: Any = None,
        universe: Iterable[MarketSpec] = (),
        discover: Callable[[dict[str, MarketSpec]], Awaitable[Any]] | None = None,
        subscribe_markets: Callable[[list[str]], Awaitable[None]] | None = None,
        unsubscribe_markets: Callable[[list[str]], Awaitable[None]] | None = None,
        series: Iterable[str] = (),
        session_id: str = "",
    ) -> None:
        if mode not in ("paper", "live"):
            raise ValueError(f"mode {mode!r}")
        if mode == "paper" and sim is None:
            raise ValueError("paper mode needs a simulator")
        if mode == "live" and venue is None:
            raise ValueError("live mode needs a Kalshi venue")
        self.strategy = strategy
        self.mode = mode
        self.cfg = cfg or LiveConfig(mode=mode)
        self.venue = venue if mode == "live" else None
        self.sim = sim if mode == "paper" else None
        self.paper_fees = paper_fees
        self.hedge = hedge
        self.recorder = recorder
        self.jsonlog = jsonlog
        self.metrics = metrics or Metrics()
        self._clock = clock_ns
        self._mono = monotonic
        self.kill_file = kill_file
        self.heartbeat_path = Path(heartbeat_path) if heartbeat_path else None
        self.fee_engine = fee_engine
        self.universe: dict[str, MarketSpec] = {s.ticker: s for s in universe}
        self._discover = discover
        self.subscribe_markets = subscribe_markets
        self.unsubscribe_markets = unsubscribe_markets
        self.series = tuple(series)
        self.session_id = session_id
        self.period_ns = int(period_ns)
        self.pump = EventPump(strategy, self.period_ns, sim=self.sim, on_actions=self._on_actions,
                              on_delivered=self._on_delivered)
        self.gate = OrderGate()
        self.queue: asyncio.Queue[Any] = asyncio.Queue()
        self.exit_code = 0
        self.stop_reason = ""
        self._last_push_ts = 0
        self._sources: list[tuple[str, Callable[[], Awaitable[Any]]]] = []
        self._source_stoppers: list[Callable[[], Any]] = []
        self._tasks: list[asyncio.Task[Any]] = []
        self._bg: set[asyncio.Task[Any]] = set()
        self._stop_evt: asyncio.Event | None = None
        self._stopping = False
        self._consumer_task: asyncio.Task[Any] | None = None
        self._consumer_beat = self._mono()
        self._wake_handle: asyncio.TimerHandle | None = None
        self._wake_at: int | None = None
        self._next_kill_check = 0
        self._next_metrics = 0
        self._next_prune = 0
        self._killed = False
        self._halts: list[Halt] = []
        self._pos_suspect: dict[str, tuple[int, int]] = {}  # ticker -> (exchange pos, first seen ns)
        self._positions_now = asyncio.Event()
        self._discover_now = asyncio.Event()
        self._fee_acc: dict[str, Any] = {}  # order id -> OrderFeeAccumulator
        self._fee_eff: dict[str, tuple[str, float]] = {}  # ticker -> fee override in force
        self._seen_fills: dict[str, None] = {}  # trade/fill ids delivered (bounded, FIFO)
        self._halt_until: dict[str, int] = {}  # gate reason -> reopen ts (0 = manual)
        self._last_brti_ns = 0
        self._last_event_ts = 0
        self._lag_s = 0.0
        self._metrics_server: MetricsServer | None = None
        self.shutdown_ok = True

    # ================================================================== producers
    def _stamp(self) -> int:
        """Current local time, never below the last queued timestamp."""
        t = self._clock()
        if t < self._last_push_ts:
            self.metrics.inc("dh_clock_regressions_total")
            t = self._last_push_ts
        self._last_push_ts = t
        return t

    def push(self, ev: Event) -> None:
        """Queue an inbound event (already recorded raw by its source).

        Live-mode inbound rules (mirrored by dh.live.replay so sessions replay exactly):
          * KalshiOrderGroupUpdate: the exchange group id is translated to the strategy's
            logical id; updates of groups that are not ours are dropped.
          * KalshiPositionSnapshot from the WS (source 'ws') never reaches the strategy
            directly: it races with the fill message and with settlement, so it goes through
            the same persistence check as the REST positions (``_check_positions``)."""
        if ev.ts < self._last_push_ts:
            self.metrics.inc("dh_ts_clamped_total")
            ev = dataclasses.replace(ev, ts=self._last_push_ts)
        else:
            self._last_push_ts = ev.ts
        if self.mode == "live":
            if isinstance(ev, KalshiOrderGroupUpdate):
                tr = self._translate_group(ev)
                if tr is None:
                    return
                ev = tr
            elif isinstance(ev, KalshiPositionSnapshot) and ev.source == "ws":
                self.queue.put_nowait(Side(ev.ts, "positions_ws", {ev.ticker: ev.position}))
                return
        self.queue.put_nowait(ev)

    def _translate_group(self, ev: KalshiOrderGroupUpdate) -> KalshiOrderGroupUpdate | None:
        v = self.venue
        if v is None:
            return ev
        if ev.order_group_id in v.groups:  # already logical
            return ev
        logical = v.logical_group_of(ev.order_group_id)
        if not logical:
            self.metrics.inc("dh_foreign_group_updates_total")
            return None
        return dataclasses.replace(ev, order_group_id=logical)

    def push_result(self, ev: Event) -> None:
        """Queue an adapter-generated event, recording it on ``events.live`` first."""
        if ev.ts < self._last_push_ts:
            self.metrics.inc("dh_ts_clamped_total")
            ev = dataclasses.replace(ev, ts=self._last_push_ts)
        else:
            self._last_push_ts = ev.ts
        self._record_event(RESULT_STREAM, ev)
        self.queue.put_nowait(ev)

    def push_side(self, kind: str, payload: Any = None) -> None:
        """Queue a side-channel item stamped now (processed in order by the consumer)."""
        self.queue.put_nowait(Side(self._stamp(), kind, payload))

    def add_source(self, name: str, factory: Callable[[], Awaitable[Any]], stop: Callable[[], Any] | None = None) -> None:
        """Register a supervised input task (restarted with backoff if it crashes)."""
        self._sources.append((name, factory))
        if stop is not None:
            self._source_stoppers.append(stop)

    # ================================================================== recording / logs
    def _record_event(self, stream: str, ev: Event) -> None:
        if self.recorder is not None:
            try:
                self.recorder.write_event(stream, ev)
            except Exception:  # noqa: BLE001 - never let capture stop trading decisions
                self.metrics.inc("dh_record_errors_total")
                log.exception("recording %s failed", stream)

    def meta(self, kind: str, ts: int, /, **payload: Any) -> None:
        """Session record on the 'meta' stream (universe changes, warm-up, gate...)."""
        if self.recorder is not None:
            try:
                self.recorder.write(META_STREAM, ts, orjson.dumps({"kind": kind, "t": ts, "session": self.session_id, **payload},
                                                                 default=str))
            except Exception:  # noqa: BLE001
                self.metrics.inc("dh_record_errors_total")
                log.exception("meta record failed")

    def jlog(self, kind: str, ts: int, /, **payload: Any) -> None:
        if self.jsonlog is not None:
            try:
                self.jsonlog.write(kind, ts, **payload)
            except Exception:  # noqa: BLE001
                self.metrics.inc("dh_log_errors_total")

    # ================================================================== consumer
    async def _consume(self) -> None:
        q = self.queue
        while True:
            item = await q.get()
            if item is _STOP:
                return
            try:
                self._process(item)
                self._housekeeping(item.ts)
            except Exception as exc:  # noqa: BLE001 - fail safe
                log.exception("consumer error on %s", type(item).__name__)
                self.metrics.inc("dh_strategy_errors_total")
                self.jlog("error", self._clock(), where="consumer", item=type(item).__name__, error=f"{type(exc).__name__}: {exc}"[:500])
                if self.cfg.loop.strategy_error != "continue":
                    self.gate.close("strategy_error", self._clock())
                    self.request_stop(f"strategy error: {type(exc).__name__}: {exc}", code=4)
                    return
            self._consumer_beat = self._mono()
            if q.empty():
                self._schedule_wake()

    def process_pending(self) -> int:
        """Synchronously process everything queued (tests / shutdown drain)."""
        n = 0
        while not self.queue.empty():
            item = self.queue.get_nowait()
            if item is _STOP:
                continue
            self._process(item)
            self._housekeeping(item.ts)
            n += 1
        return n

    def _process(self, item: Any) -> None:
        if isinstance(item, Wake):
            self._on_wake(item.ts)
        elif isinstance(item, Side):
            self.pump.advance(item.ts)
            self._on_side(item)
        else:
            self._on_event(item)

    def _on_event(self, ev: Event) -> None:
        now = self._clock()
        self._last_event_ts = ev.ts
        lag = (now - ev.ts) / NS_PER_S
        self._lag_s = lag
        max_lag = self.cfg.loop.max_lag_s
        if self.mode == "live" and max_lag > 0:
            if lag > max_lag:
                if self.gate.close("lag", ev.ts):
                    self.jlog("gate", ev.ts, action="close", reason="lag", lag_s=round(lag, 3))
            elif lag < max_lag / 2 and isinstance(ev, MARKET_DATA) and self.gate.open("lag"):
                # only fresh MARKET data proves the consumer caught up (our own results are
                # always stamped 'now')
                self.jlog("gate", ev.ts, action="open", reason="lag", lag_s=round(lag, 3))
        if self.venue is not None:
            self.venue.observe(ev)
        self._pre_event(ev)
        try:
            self.pump.feed(ev)
        except OrderingError:
            self.metrics.inc("dh_ts_clamped_total")
            self.pump.feed(dataclasses.replace(ev, ts=self.pump.last_ts))

    def _pre_event(self, ev: Event) -> None:
        """Runner-side bookkeeping BEFORE the strategy sees ``ev`` (never changes it)."""
        if isinstance(ev, IndexTick):
            if ev.index_id == "BRTI":
                self._last_brti_ns = ev.ts
        elif isinstance(ev, KalshiFill):
            self.metrics.inc("dh_fills_total", side=ev.book_side, taker=str(ev.is_taker).lower())
            self._note_fill_ids(ev.trade_id, ev.fill_id)
            if self.mode == "live":
                self._check_fee(ev)
        elif isinstance(ev, FeedStatus):
            if ev.status in ("gap", "disconnected", "stale", "error"):
                self.metrics.inc("dh_feed_status_total", stream=ev.stream.split(":", 1)[0], status=ev.status)
                self.jlog("feed_status", ev.ts, stream=ev.stream, status=ev.status, detail=ev.detail[:200])
            if ev.stream == "kalshi.ws":
                self.metrics.set("dh_kalshi_ws_ok", 1.0 if ev.status in ("connected", "resynced", "resumed") else 0.0)
        elif isinstance(ev, KalshiFeeUpdate):
            self._on_fee_update(ev)
        elif isinstance(ev, KalshiMarketLifecycle):
            self._on_lifecycle(ev)

    def _on_wake(self, ts: int) -> None:
        """Deliver due timers / simulator messages. If the timer grid is far behind the wake
        (the loop stalled: VM pause, GC, a slow cycle), the catch-up cycles would decide on
        pre-stall data: in live mode new orders are blocked (gate 'lag') until an event
        arrives with a small lag again."""
        nxt = self.pump.next_timer_ns
        max_lag = self.cfg.loop.max_lag_s
        if self.mode == "live" and max_lag > 0 and nxt is not None and (ts - nxt) / NS_PER_S > max_lag:
            if self.gate.close("lag", ts):
                self.metrics.inc("dh_loop_stalls_total")
                self.jlog("gate", ts, action="close", reason="lag", stall_s=round((ts - nxt) / NS_PER_S, 3))
        self.pump.advance(ts)

    def _housekeeping(self, ts: int) -> None:
        """Every consumer iteration (cheap unless something is due): kill-file check,
        metrics refresh, pruning of settled markets, expiry of timed halts."""
        if self._halt_until:
            for key, until in list(self._halt_until.items()):
                if until and self.pump.last_ts >= until and self.gate.open(key):
                    self._halt_until.pop(key, None)
                    self.metrics.set("dh_halted", 0.0, scope=key.split(":", 1)[1])
                    self.jlog("gate", ts, action="open", reason=key, note="timed halt expired")
        mono_ns = int(self._mono() * NS_PER_S)
        if self.kill_file is not None and mono_ns >= self._next_kill_check:
            self._next_kill_check = mono_ns + int(self.cfg.loop.kill_check_interval_s * NS_PER_S)
            if self.kill_file.triggered():
                self.kill(self.kill_file.reason())
        if mono_ns >= self._next_metrics:
            self._next_metrics = mono_ns + int(self.cfg.loop.metrics_refresh_s * NS_PER_S)
            self.refresh_metrics()
        if mono_ns >= self._next_prune:
            self._next_prune = mono_ns + 300 * NS_PER_S
            if self.pump.stats.events:
                self._prune(max(ts, self.pump.last_ts))

    def _schedule_wake(self) -> None:
        """Arm one wake-up for the next deadline (timer grid / simulator / housekeeping)."""
        if self._stopping:
            return
        deadline = self.pump.next_deadline_ns()
        now = self._clock()
        house = now + int(min(self.cfg.loop.kill_check_interval_s, self.cfg.loop.metrics_refresh_s) * NS_PER_S)
        if deadline is None or deadline > house:
            deadline = house
        deadline += 1  # timers are delivered strictly before the wake stamp
        if self._wake_handle is not None and self._wake_at is not None and self._wake_at <= deadline:
            return
        if self._wake_handle is not None:
            self._wake_handle.cancel()
        delay = max(0.0, (deadline - now) / NS_PER_S)
        self._wake_at = deadline
        self._wake_handle = asyncio.get_running_loop().call_later(delay, self._fire_wake)

    def _fire_wake(self) -> None:
        self._wake_handle = None
        self._wake_at = None
        if not self._stopping:
            self.queue.put_nowait(Wake(self._stamp()))

    # ================================================================== pump hooks
    def _on_delivered(self, ev: Event, origin: str) -> None:
        if origin == "sim":
            self._record_event(PAPER_STREAM, ev)
            self.metrics.inc("dh_paper_events_total", type=type(ev).__name__)
        elif origin == "timer":
            self.metrics.inc("dh_timers_total")
        else:
            self.metrics.inc("dh_events_total", type=type(ev).__name__)

    def _on_actions(self, ev: Event, actions: list[Action], handled: list[bool], origin: str) -> None:
        ts = ev.ts
        to_venue: list[Action] = []
        to_hedge: list[Action] = []
        scoped: list[CancelAll] = []
        for a, done in zip(actions, handled, strict=True):
            if isinstance(a, Log):
                self.jlog("log." + a.kind, ts, **a.payload)
                if a.kind == "risk" and a.payload.get("event") == "reconcile_requested":
                    self._reconcile_now(ts, str(a.payload.get("channel", "")))
                continue
            name = type(a).__name__
            self.metrics.inc("dh_actions_total", type=name)
            self.jlog("action", ts, type=name, origin=origin, **_action_fields(a))
            if isinstance(a, Halt):
                self._on_halt(a, ts)
            elif isinstance(a, Resume):
                self.jlog("resume_ignored", ts, reason=a.reason, note="restart the runner to resume after a halt")
            elif isinstance(a, (PlaceHedge, CancelHedge)):
                to_hedge.append(a)
            elif done:
                continue  # paper mode: the simulator took it
            elif self.mode == "live":
                if isinstance(a, CancelAll) and a.tickers:
                    scoped.append(a)
                    continue
                if isinstance(a, PlaceOrder):
                    why = self.gate.check(a.ticker)
                    if why:
                        self.metrics.inc("dh_gate_rejects_total", reason=why.split(":", 1)[0])
                        self.push_result(OrderReject(self._stamp(), 0, a.client_order_id, a.ticker, f"gate:{why}", 0, "create"))
                        continue
                to_venue.append(a)
        if to_venue and self.venue is not None:
            self.venue.submit(to_venue, ts)
        if scoped:
            sent = {a.order_id for a in to_venue if isinstance(a, CancelOrder) and a.order_id}
            for a in scoped:
                self._cancel_scoped(a.tickers, a.reason, ts, sent)
        if to_hedge and self.hedge is not None:
            self.hedge.submit(to_hedge, ts)

    def _on_halt(self, a: Halt, ts: int) -> None:
        """Close the gate for new orders. until_ts == 0 (every M1 halt): until the runner is
        restarted by an operator; a timed halt reopens at until_ts (event time)."""
        self._halts.append(a)
        self.metrics.set("dh_halted", 1.0, scope=a.scope)
        key = f"halt:{a.scope}"
        if a.until_ts:
            self._halt_until[key] = max(self._halt_until.get(key, 0), a.until_ts)
        else:
            self._halt_until[key] = 0  # manual: never reopens by itself
        if self.gate.close(key, ts):
            log.error("STRATEGY HALT scope=%s reason=%s", a.scope, a.reason)
            self.jlog("halt", ts, scope=a.scope, reason=a.reason, until_ts=a.until_ts)
            self.meta("halt", ts, scope=a.scope, reason=a.reason)

    # ================================================================== runner-side checks
    def _check_fee(self, f: KalshiFill) -> None:
        """Reconcile the fill's reported fee_cost with the exact fee model, including Kalshi's
        per-order balance-precision rounding (OrderFeeAccumulator per order id). A plain
        per-fill comparison with a one-cent tolerance could not detect a wrong maker-fee
        schedule at M1 sizes (a 2-contract maker fee is below one cent)."""
        spec = self.universe.get(f.ticker)
        if spec is None or self.fee_engine is None:
            return
        ftype, mult = self._fee_eff.get(f.ticker, (spec.fee_type, spec.fee_multiplier))
        if not ftype:
            return
        from dh.kalshi.fees import reconcile_fill_fee

        try:
            acc = self._fee_acc.get(f.order_id)
            if acc is None:
                sched = self.fee_engine.schedule_for_spec(ftype, mult)
                if not getattr(sched, "supported", True):
                    return
                acc = self._fee_acc[f.order_id] = sched.order_accumulator(f.book_side)
                while len(self._fee_acc) > 50_000:
                    self._fee_acc.pop(next(iter(self._fee_acc)))
            bd = acc.apply_fill(f.yes_px, f.qty, f.is_taker)
        except Exception:  # noqa: BLE001 - unsupported schedule: the strategy never quotes it
            return
        chk = reconcile_fill_fee(bd.net_micros, f"{f.fee_micros / 1_000_000:.6f}", breakdown=bd)
        self.metrics.observe("dh_fee_diff_micros", abs(chk.diff_micros))
        if not chk.ok:
            expected = bd.net_micros
            tol = chk.tolerance_micros
            self.metrics.inc("dh_fee_mismatch_total")
            self.jlog("fee_mismatch", f.ts, ticker=f.ticker, trade_id=f.trade_id, order_id=f.order_id, px=f.yes_px,
                      qty=f.qty, taker=f.is_taker, reported=f.fee_micros, expected_net=expected, expected_trade=bd.trade_micros,
                      tolerance=tol)
            if self.cfg.venue.halt_on_fee_mismatch and self.gate.close("fee_mismatch", f.ts):
                log.error("FEE MISMATCH %s reported=%d expected=%d: new orders blocked", f.ticker, f.fee_micros, expected)
                self.meta("gate", f.ts, action="close", reason="fee_mismatch")
                self._cancel_all_async("fee_mismatch")

    def _event_tickers(self, event_ticker: str) -> list[str]:
        return sorted(t for t, s in self.universe.items() if s.event_ticker == event_ticker)

    def _block(self, tickers: list[str], reason: str, ts: int) -> None:
        new = self.gate.block(tickers, reason)
        if not new:
            return
        log.warning("blocking new orders in %s: %s", ",".join(new), reason)
        self.jlog("block", ts, tickers=new, reason=reason)
        self.meta("block", ts, tickers=new, reason=reason)
        self.metrics.inc("dh_blocked_markets_total", reason=reason.split(":", 1)[0])
        self._cancel_scoped(new, f"blocked:{reason}", ts)

    def _on_fee_update(self, ev: KalshiFeeUpdate) -> None:
        """Event-level fee override. The MarketMaker re-resolves its own schedules on this
        event; the runner only tracks the effective (type, multiplier) the same way
        (override > series base; None clears) so its exact per-fill fee check stays right.
        An unparseable multiplier blocks the event's markets (never guess a fee)."""
        tickers = self._event_tickers(ev.event_ticker)
        if not tickers:
            return
        for t in tickers:
            spec = self.universe[t]
            ftype = ev.fee_type_override if ev.fee_type_override is not None else spec.fee_type
            try:
                mult = float(ev.fee_multiplier_override) if ev.fee_multiplier_override not in (None, "") else spec.fee_multiplier
            except ValueError:
                self._block([t], f"fee_update_unparseable:{ev.fee_multiplier_override}", ev.ts)
                continue
            self._fee_eff[t] = (ftype, mult)
            self._fee_acc.clear()  # accumulators were built with the old schedule
            if self.paper_fees is not None and ftype:
                self.paper_fees.set_fee(t, ftype, mult)
        self.jlog("fee_update", ev.ts, event=ev.event_ticker, fee_type=ev.fee_type_override,
                  multiplier=ev.fee_multiplier_override, tickers=tickers)

    def _on_lifecycle(self, ev: KalshiMarketLifecycle) -> None:
        if ev.event_type == "created" and (not self.series or ev.ticker.split("-", 1)[0] in self.series):
            self._discover_now.set()
            return
        spec = self.universe.get(ev.ticker)
        if spec is None:
            return
        if ev.event_type == "close_date_updated" and ev.close_ts and ev.close_ts != spec.close_ts:
            self._block([ev.ticker], "close_date_updated", ev.ts)
        elif ev.event_type == "price_level_structure_updated" and ev.price_ranges:
            if tuple((r.start_px, r.end_px, r.step_px) for r in spec.price_ranges) != tuple(ev.price_ranges):
                self._block([ev.ticker], "tick_grid_changed", ev.ts)
        elif ev.event_type == "metadata_updated":
            self._discover_now.set()  # discovery compares the refreshed spec (spec_changed)

    # ================================================================== side channel
    def _on_side(self, item: Side) -> None:
        k = item.kind
        if k == "universe_add":
            self._universe_add(item.ts, list(item.payload or ()))
        elif k == "spec_changed":
            for t, why in sorted((item.payload or {}).items()):
                self._block([t], f"spec_changed:{why}", item.ts)
        elif k == "positions":
            self._check_positions(item.ts, dict(item.payload or {}))
        elif k == "positions_ws":
            self._check_positions(item.ts, dict(item.payload or {}), partial=True)
        elif k == "fills":
            self._backfill_fills(item.ts, list(item.payload or ()))
        elif k == "resting":
            self._check_resting(item.ts, list(item.payload or ()))
        elif k == "queue_positions":
            self._ingest_queue_positions(item.ts, list(item.payload or ()))
        elif k == "call":
            item.payload(item.ts)

    def _universe_add(self, ts: int, specs: list[MarketSpec]) -> None:
        add = getattr(self.strategy, "add_markets", None)
        fresh = [s for s in specs if s.ticker not in self.universe]
        if not fresh:
            return
        added = list(add(fresh)) if add is not None else []
        if add is None:
            log.error("strategy has no add_markets(): %d new markets ignored", len(fresh))
            self.jlog("universe_error", ts, error="strategy has no add_markets", tickers=[s.ticker for s in fresh])
            return
        new_specs = [s for s in fresh if s.ticker in set(added)]
        for s in new_specs:
            self.universe[s.ticker] = s
            if self.sim is not None:
                self.sim.register_market(s)
        if self.paper_fees is not None:
            self.paper_fees.add_specs(new_specs)
        if not new_specs:
            return
        tickers = [s.ticker for s in new_specs]
        self.meta("universe_add", ts, specs=[spec_to_dict(s) for s in new_specs])
        self.jlog("universe_add", ts, tickers=tickers)
        self.metrics.inc("dh_universe_added_total", len(tickers))
        log.info("universe +%d markets (now %d)", len(tickers), len(self.universe))
        if self.subscribe_markets is not None:
            self._spawn(self.subscribe_markets(tickers), "subscribe")

    def _prune(self, ts: int) -> None:
        prune = getattr(self.strategy, "prune_settled", None)
        specs = getattr(self.strategy, "specs", None)
        if prune is None or specs is None:
            return
        before = set(specs)
        before_ns = ts - int(self.cfg.universe.prune_after_s * NS_PER_S)
        prune(before_ns)
        gone = sorted(before - set(specs))
        if not gone:
            return
        for t in gone:
            self.universe.pop(t, None)
        self.meta("universe_prune", ts, tickers=gone, before_ns=before_ns)
        self.jlog("universe_prune", ts, tickers=gone)
        if self.unsubscribe_markets is not None:
            self._spawn(self.unsubscribe_markets(gone), "unsubscribe")

    def _position_of(self, ticker: str) -> int:
        om = getattr(self.strategy, "om", None)
        return int(om.position(ticker)) if om is not None else 0

    def _check_positions(self, ts: int, exch: dict[str, int], *, partial: bool = False) -> None:
        """Compare exchange positions with the strategy's fill-derived positions for markets
        still trading. A mismatch must persist ``position_confirm_s`` (a fill may be in
        flight on the WebSocket) before a KalshiPositionSnapshot is fed, which makes the
        strategy's OrderManager flag it (-> Halt(all)). ``partial``: ``exch`` holds only the
        markets it names (a WS market_position message), not the whole account."""
        settled = getattr(self.strategy, "settled", {}) or {}
        src = "ws_checked" if partial else "rest"
        ours_nonzero = set() if partial else {t for t in self.universe if self._position_of(t)}
        cands = {t for t in set(exch) | ours_nonzero
                 if t in self.universe and self.universe[t].close_ts > ts and t not in settled}
        confirm_ns = int(self.cfg.venue.position_confirm_s * NS_PER_S)
        for t in sorted(cands):
            ex, ours = int(exch.get(t, 0)), self._position_of(t)
            if ex == ours:
                if self._pos_suspect.pop(t, None) is not None:
                    self.jlog("position_ok", ts, ticker=t, position=ex, note="mismatch resolved")
                if ex:
                    self._inject(KalshiPositionSnapshot(ts, 0, t, ex, source=src))
                continue
            s = self._pos_suspect.get(t)
            if s is None or s[0] != ex:
                self._pos_suspect[t] = (ex, ts)
                self.metrics.inc("dh_position_suspects_total")
                self.jlog("position_suspect", ts, ticker=t, exchange=ex, ours=ours)
                self._positions_now.set()  # confirm soon
            elif ts - s[1] >= confirm_ns:
                self._pos_suspect.pop(t, None)
                self.metrics.inc("dh_position_mismatches_total")
                log.error("POSITION MISMATCH %s exchange=%d ours=%d", t, ex, ours)
                self.jlog("position_mismatch", ts, ticker=t, exchange=ex, ours=ours, source=src)
                self._inject(KalshiPositionSnapshot(ts, 0, t, ex, source=src))
        if not partial:
            for t in [t for t in self._pos_suspect if t not in cands]:
                self._pos_suspect.pop(t, None)

    def _check_resting(self, ts: int, rows: list[dict[str, Any]]) -> None:
        """Order reconciliation against GET /portfolio/orders?status=resting:
          * ghost sweep: cancel resting orders the strategy does not consider live;
          * lost updates: every order the strategy considers live (with an exchange id) that
            is NOT resting is looked up (GET order) and its final state fed back as a
            KalshiOrderUpdate (a canceled/executed user_order message may have been lost)."""
        if self.venue is None or not self.cfg.venue.ghost_sweep:
            return
        om = getattr(self.strategy, "om", None)
        if om is not None:
            resting_ids = {str(o.get("order_id") or "") for o in rows}
            for w in om.working():
                if w.order_id and w.order_id not in resting_ids and w.created_ns < ts - NS_PER_S:
                    self.metrics.inc("dh_order_state_checks_total")
                    self.venue.check_order(w.client_order_id, w.order_id, w.ticker, recancel=False, reason="not_resting")
        ghosts: list[CancelOrder] = []
        for o in rows:
            coid = str(o.get("client_order_id") or "")
            oid = str(o.get("order_id") or "")
            if not oid:
                continue
            w = om.order(coid) if (om is not None and coid) else None
            if w is not None and getattr(getattr(w, "state", None), "name", "") in LIVE_ORDER_STATES:
                continue
            ghosts.append(CancelOrder(coid, str(o.get("ticker") or ""), oid, reason="ghost"))
        if ghosts:
            self.metrics.inc("dh_ghost_orders_total", len(ghosts))
            self.jlog("ghost_orders", ts, orders=[(g.client_order_id, g.order_id, g.ticker) for g in ghosts])
            log.warning("cancelling %d resting orders unknown to the strategy", len(ghosts))
            self.venue.cancel_orders(ghosts)

    def _ingest_queue_positions(self, ts: int, rows: list[tuple[str, str, int]]) -> None:
        """Exchange queue positions -> the strategy's queue estimator (calibration sample;
        overwrite only with venue.queue_positions_resync, which is recorded in 'meta')."""
        q = getattr(self.strategy, "queue", None)
        om = getattr(self.strategy, "om", None)
        if q is None or om is None:
            return
        by_oid = {w.order_id: w.client_order_id for w in om.working() if w.order_id}
        resync = bool(self.cfg.venue.queue_positions_resync)
        applied = []
        for oid, ticker, qty in rows:
            coid = by_oid.get(oid)
            if coid is None:
                continue
            est = q.estimated_position(coid)
            s = q.ingest_exchange_queue_position(coid, int(qty), ts, resync=resync)
            if s is None:
                continue
            self.metrics.observe("dh_queue_error_contracts", abs(s.estimated - s.reported) / 100.0)
            applied.append((coid, ticker, int(qty), est))
        if applied:
            self.jlog("queue_positions", ts, rows=applied, resync=resync)
            if resync:
                self.meta("queue_resync", ts, rows=applied)

    def _note_fill_ids(self, *ids: str) -> None:
        seen = self._seen_fills
        for i in ids:
            if i and i not in seen:
                seen[i] = None
        while len(seen) > 200_000:
            seen.pop(next(iter(seen)))

    def _backfill_fills(self, ts: int, rows: list[dict[str, Any]]) -> None:
        """REST fills after an own-channel gap: feed those the WebSocket never delivered.

        Dedupe: a REST Fill's ``trade_id`` is documented as the legacy name of ``fill_id``;
        both are checked against every fill already seen. If the WS trade ids ever differed
        from the REST ids, fills would be counted twice and the position reconciliation
        would halt trading (a loud failure, never a silent one)."""
        from dh.kalshi.normalize import rest_fill_to_event

        n = 0
        for row in rows:
            tid = str(row.get("trade_id") or "")
            fid = str(row.get("fill_id") or "")
            if (tid and tid in self._seen_fills) or (fid and fid in self._seen_fills):
                continue
            try:
                ev = rest_fill_to_event(row, ts)
            except (KeyError, ValueError, TypeError):
                continue
            ev = dataclasses.replace(ev, trade_id=tid or fid, fill_id=fid)
            self._note_fill_ids(tid, fid)
            self.jlog("fill_backfilled", ts, ticker=ev.ticker, trade_id=ev.trade_id, order_id=ev.order_id, qty=ev.qty)
            self._inject(ev)
            n += 1
        if n:
            log.warning("back-filled %d fills missed by the WebSocket", n)
            self.metrics.inc("dh_fills_backfilled_total", n)

    def _cancel_scoped(self, tickers: Iterable[str], reason: str, ts: int, skip_oids: Iterable[str] = ()) -> None:
        """CancelAll(tickers): the REST cancel-all cannot filter by market, so cancel our own
        working orders there (the OrderManager's view) in batches; if a create with unknown
        outcome is pending in those markets, also sweep the REST resting orders."""
        if self.venue is None:
            return
        tset = set(tickers)
        skip = set(skip_oids)
        om = getattr(self.strategy, "om", None)
        acts: list[CancelOrder] = []
        if om is not None:
            for w in om.working():
                if w.ticker in tset and w.order_id and w.order_id not in skip:
                    acts.append(CancelOrder(w.client_order_id, w.ticker, w.order_id, reason=f"scoped:{reason}"))
                    skip.add(w.order_id)
        if acts:
            self.venue.cancel_orders(acts)
        if self.venue.has_pending_creates(tset) or om is None:
            self._spawn(self.venue.sweep_resting(sorted(tset), reason), "sweep")

    def _reconcile_now(self, ts: int, reason: str) -> None:
        """Immediate REST reconciliation (own-channel gap): fills since the gap, then
        positions and orders (the positions loop runs at once)."""
        if self.venue is None:
            return
        self.jlog("reconcile_now", ts, reason=reason)
        self.metrics.inc("dh_reconcile_requests_total")

        async def fills() -> None:
            try:
                rows = await self.venue.fetch_fills(ts // NS_PER_S - 120)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                self.jlog("reconcile_error", self._clock(), what="fills", error=f"{type(exc).__name__}: {exc}"[:300])
                return
            self.push_side("fills", rows)

        self._spawn(fills(), "fills")
        self._positions_now.set()

    def _inject(self, ev: Event) -> None:
        """Feed a runner-generated event now (inside the consumer), recorded for replay."""
        self._record_event(RESULT_STREAM, ev)
        self.pump.feed(ev)

    # ================================================================== kill / halt / stop
    def kill(self, reason: str) -> None:
        """Manual kill: close the gate, cancel everything, stop the process."""
        if self._killed:
            return
        self._killed = True
        ts = self._clock()
        log.critical("KILL: %s", reason)
        self.gate.close("kill", ts)
        self.metrics.set("dh_killed", 1.0)
        self.jlog("kill", ts, reason=reason)
        self.meta("kill", ts, reason=reason)
        self._cancel_all_async(f"kill:{reason}")
        self.request_stop(f"kill file: {reason}", code=0)

    def _cancel_all_async(self, reason: str) -> None:
        if self.venue is not None:
            self.venue.submit([CancelAll(reason=reason)], self._clock())

    def request_stop(self, reason: str, code: int = 0) -> None:
        if not self.stop_reason:
            self.stop_reason = reason
            self.exit_code = code
        if self._stop_evt is not None:
            self._stop_evt.set()

    def _spawn(self, coro: Awaitable[Any], name: str) -> asyncio.Task[Any]:
        t = asyncio.ensure_future(coro)
        t.set_name(f"runner:{name}")
        self._bg.add(t)
        t.add_done_callback(self._bg_done)
        return t

    def _bg_done(self, t: asyncio.Task[Any]) -> None:
        self._bg.discard(t)
        if not t.cancelled() and t.exception() is not None:
            log.error("background task %s failed: %r", t.get_name(), t.exception())
            self.metrics.inc("dh_task_errors_total", task=t.get_name())

    # ================================================================== background loops
    async def _supervise(self, name: str, factory: Callable[[], Awaitable[Any]]) -> None:
        delay = 1.0
        assert self._stop_evt is not None
        while not self._stop_evt.is_set():
            started = self._mono()
            try:
                await factory()
                if self._stop_evt.is_set():
                    return
                detail = "source returned"
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                detail = f"{type(exc).__name__}: {exc}"[:300]
                log.exception("source %s crashed", name)
            self.metrics.inc("dh_source_restarts_total", source=name)
            self.jlog("source_restart", self._clock(), source=name, detail=detail)
            if self._mono() - started > 300:
                delay = 1.0
            try:
                await asyncio.wait_for(self._stop_evt.wait(), timeout=delay)
            except TimeoutError:
                pass
            delay = min(60.0, delay * 2)

    async def _heartbeat_loop(self) -> None:
        if self.heartbeat_path is None:
            return
        iv = self.cfg.loop.heartbeat_interval_s
        alive_s = max(2.0 * iv, 1.0)
        while True:
            # running: only while the consumer makes progress (a dead or stuck consumer lets
            # the heartbeat go stale -> watchdog). stopping: the process is busy cancelling.
            if self._stopping:
                beat, state = True, "stopping"
            else:
                beat = self._mono() - self._consumer_beat <= alive_s and not (self._consumer_task and self._consumer_task.done())
                state = "running"
            if beat:
                try:
                    write_heartbeat(self.heartbeat_path, self._heartbeat_payload(state))
                    self.metrics.set("dh_heartbeat_ts", self._clock() / NS_PER_S)
                except OSError as exc:
                    self.metrics.inc("dh_heartbeat_errors_total")
                    log.error("heartbeat write failed: %s", exc)
            await asyncio.sleep(iv)

    def _heartbeat_payload(self, state: str) -> dict[str, Any]:
        return {"pid": os.getpid(), "mode": self.mode, "state": state, "session": self.session_id,
                "queue": self.queue.qsize(), "lag_s": round(self._lag_s, 3), "last_event_ts": self._last_event_ts,
                "gate": sorted(self.gate.reasons)}

    async def _discovery_loop(self) -> None:
        if self._discover is None:
            return
        iv = self.cfg.universe.discovery_interval_s
        while not self._stopping:
            try:
                await asyncio.wait_for(self._discover_now.wait(), timeout=iv)
                await asyncio.sleep(2.0)  # debounce bursts of lifecycle messages
            except TimeoutError:
                pass
            self._discover_now.clear()
            if self._stopping:
                return
            try:
                res = await self._discover(dict(self.universe))
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                self.metrics.inc("dh_discovery_errors_total")
                self.jlog("discovery_error", self._clock(), error=f"{type(exc).__name__}: {exc}"[:300])
                continue
            specs = list(getattr(res, "specs", res) or [])
            changed = dict(getattr(res, "changed", {}) or {})
            if specs:
                self.push_side("universe_add", specs)
            if changed:
                self.push_side("spec_changed", changed)

    async def _positions_loop(self) -> None:
        v = self.venue
        iv = self.cfg.venue.positions_interval_s
        if v is None or iv <= 0:
            return
        while not self._stopping:
            try:
                await asyncio.wait_for(self._positions_now.wait(), timeout=iv)
                await asyncio.sleep(self.cfg.venue.position_confirm_s)
            except TimeoutError:
                pass
            self._positions_now.clear()
            if self._stopping:
                return
            try:
                pos = await v.fetch_positions()
                self.push_side("positions", pos)
                if self.cfg.venue.ghost_sweep:
                    self.push_side("resting", await v.resting_orders())
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                self.metrics.inc("dh_reconcile_errors_total", kind="positions")
                self.jlog("reconcile_error", self._clock(), what="positions", error=f"{type(exc).__name__}: {exc}"[:300])

    async def _queue_positions_loop(self) -> None:
        v = self.venue
        iv = self.cfg.venue.queue_positions_interval_s
        om = getattr(self.strategy, "om", None)
        if v is None or iv <= 0 or om is None:
            return
        while not self._stopping:
            await asyncio.sleep(iv)
            tickers = sorted({w.ticker for w in om.working() if w.order_id and w.state.name == "RESTING"})
            if not tickers:
                continue
            if not v.read_budget_ok(("/portfolio/orders/queue_positions",)):
                self.metrics.inc("dh_polls_skipped_total", poll="queue_positions")
                continue
            try:
                rows = await v.fetch_queue_positions(tickers)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                self.metrics.inc("dh_reconcile_errors_total", kind="queue_positions")
                self.jlog("reconcile_error", self._clock(), what="queue_positions", error=f"{type(exc).__name__}: {exc}"[:300])
                continue
            if rows:
                self.push_side("queue_positions", rows)

    async def _clock_loop(self) -> None:
        iv = self.cfg.loop.clock_sample_s
        if iv <= 0 or self.recorder is None:
            return
        from dh.store.recorder import sample_clock

        while not self._stopping:
            try:
                rec = await asyncio.to_thread(sample_clock)
                self.recorder.write("clock", self._clock(), orjson.dumps(rec, default=str))
                off = rec.get("offset_s")
                if isinstance(off, (int, float)):
                    self.metrics.set("dh_clock_offset_seconds", float(off))
                    if abs(off) * 1000 > self.cfg.loop.clock_alarm_ms:
                        self.metrics.inc("dh_clock_alarms_total")
                        log.warning("clock offset %.1f ms > %.1f ms", off * 1000, self.cfg.loop.clock_alarm_ms)
            except Exception:  # noqa: BLE001
                log.exception("clock sample failed")
            await asyncio.sleep(iv)

    # ================================================================== metrics / health
    def refresh_metrics(self) -> None:
        m = self.metrics
        now = self._clock()
        st = self.pump.stats
        m.set("dh_queue_depth", float(self.queue.qsize()))
        m.set("dh_consumer_lag_seconds", self._lag_s)
        m.set("dh_pump_events", float(st.events))
        m.set("dh_pump_timers", float(st.timers))
        m.set("dh_pump_sim_events", float(st.sim_events))
        m.set("dh_gate_closed", 1.0 if self.gate.closed else 0.0)
        m.set("dh_blocked_markets", float(len(self.gate.tickers)))
        m.set("dh_universe_markets", float(len(self.universe)))
        m.set("dh_brti_age_seconds", (now - self._last_brti_ns) / NS_PER_S if self._last_brti_ns else -1.0)
        s = self.strategy
        fv = getattr(s, "fv", None)
        if fv is not None:
            m.set("dh_fv_ready", 1.0 if getattr(fv, "ready", False) else 0.0)
        stats = getattr(s, "stats", None)
        if stats is not None:
            for k in ("cycles", "quotes_placed", "cancels", "fills", "halted_cycles"):
                if hasattr(stats, k):
                    m.set(f"dh_mm_{k}", float(getattr(stats, k)))
            for k, v in sorted(getattr(stats, "reasons", {}).items()):
                m.set("dh_mm_reason", float(v), reason=k)
        om = getattr(s, "om", None)
        if om is not None:
            m.set("dh_working_orders", float(len(om.working())))
            for t in self.universe:
                q = om.position(t)
                if q:
                    m.set("dh_position_contracts", q / 100.0, ticker=t)
            m.set("dh_fees_paid_dollars", om.fees_micros() / 1e6)
        eq = getattr(s, "equity", None)
        if callable(eq):
            try:
                S = s.tracker.latest_value() if hasattr(s, "tracker") else None
                m.set("dh_equity_dollars", float(eq(S)))
            except Exception:  # noqa: BLE001
                pass
        if self.venue is not None:
            m.set("dh_venue_inflight", float(self.venue.inflight))
            m.set("dh_venue_pending_reconciliations", float(self.venue.pending_reconciliations))
            m.set("dh_venue_unknown_outcomes", float(self.venue.stats.unknown))
        rec = self.recorder
        if rec is not None and hasattr(rec, "stats"):
            m.set("dh_recorder_records", float(rec.stats.records))
            m.set("dh_recorder_write_errors", float(rec.stats.write_errors))

    def health(self) -> dict[str, Any]:
        alive = self._mono() - self._consumer_beat <= max(2.0 * self.cfg.loop.heartbeat_interval_s, 1.0)
        fv = getattr(self.strategy, "fv", None)
        return {"ok": alive and not self._stopping, "mode": self.mode, "consumer_alive": alive,
                "queue": self.queue.qsize(), "lag_s": round(self._lag_s, 3), "gate": sorted(self.gate.reasons),
                "blocked_markets": len(self.gate.tickers), "universe": len(self.universe),
                "fv_ready": bool(getattr(fv, "ready", False)) if fv is not None else None,
                "halts": [h.reason for h in self._halts], "stopping": self._stopping}

    # ================================================================== lifecycle
    async def run(self, *, duration_s: float | None = None) -> int:
        """Run until stop is requested (signal, kill file, error) or ``duration_s``."""
        self._stop_evt = asyncio.Event()
        loop = asyncio.get_running_loop()
        self._consumer_beat = self._mono()
        self._consumer_task = asyncio.create_task(self._consume(), name="runner:consumer")
        self._consumer_task.add_done_callback(self._consumer_done)
        self._tasks.append(self._consumer_task)
        self._tasks.append(asyncio.create_task(self._heartbeat_loop(), name="runner:heartbeat"))
        self._tasks.append(asyncio.create_task(self._discovery_loop(), name="runner:discovery"))
        self._tasks.append(asyncio.create_task(self._clock_loop(), name="runner:clock"))
        if self.venue is not None:
            self._tasks.append(asyncio.create_task(self.venue.run_reconciler(), name="venue:reconciler"))
            self._tasks.append(asyncio.create_task(self._positions_loop(), name="runner:positions"))
            self._tasks.append(asyncio.create_task(self._queue_positions_loop(), name="runner:queue_positions"))
        for name, factory in self._sources:
            self._tasks.append(asyncio.create_task(self._supervise(name, factory), name=f"source:{name}"))
        if self.cfg.metrics.enabled:
            self._metrics_server = MetricsServer(self.metrics, self.cfg.metrics.host, self.cfg.metrics.port,
                                                 health=self.health, before_scrape=self.refresh_metrics)
            try:
                port = await self._metrics_server.start()
                log.info("metrics on http://%s:%d/metrics", self.cfg.metrics.host, port)
            except OSError as exc:
                log.error("metrics endpoint failed to start: %s", exc)
                self._metrics_server = None
        loop.call_soon(self._schedule_wake)
        self.meta("runner_start", self._clock(), mode=self.mode, period_ns=self.period_ns,
                  universe=sorted(self.universe))
        try:
            if duration_s is not None:
                try:
                    await asyncio.wait_for(self._stop_evt.wait(), timeout=duration_s)
                except TimeoutError:
                    self.request_stop("duration elapsed")
            else:
                await self._stop_evt.wait()
        finally:
            await self.shutdown()
        return self.exit_code

    def _consumer_done(self, t: asyncio.Task[Any]) -> None:
        if not self._stopping and not t.cancelled():
            self.request_stop("consumer exited", code=self.exit_code or 4)

    async def shutdown(self) -> None:
        """Graceful stop (see module docstring). Safe to call twice."""
        if self._stopping:
            return
        self._stopping = True
        ts = self._clock()
        reason = self.stop_reason or "shutdown"
        log.info("shutting down: %s", reason)
        self.gate.close("shutdown", ts)
        self.meta("shutdown_begin", ts, reason=reason)
        self.jlog("shutdown", ts, reason=reason)
        if self._wake_handle is not None:
            self._wake_handle.cancel()
            self._wake_handle = None
        tmo = self.cfg.loop.shutdown_timeout_s
        # 1. let the consumer finish what is queued (bounded), then stop it
        if self._consumer_task is not None and not self._consumer_task.done():
            self.queue.put_nowait(_STOP)
            try:
                await asyncio.wait_for(asyncio.shield(self._consumer_task), timeout=tmo / 2)
            except Exception:  # noqa: BLE001 - includes TimeoutError
                self._consumer_task.cancel()
        # 2. live: finish in-flight writes, cancel everything, verify, delete the group
        cancel_ok = True
        if self.venue is not None:
            try:
                await self.venue.wait_idle(tmo / 2)
                cancel_ok = await self.venue.cancel_all_now(f"shutdown:{reason}")
                for attempt in range(3):  # the read API can lag the cancel by a moment
                    left = await self.venue.resting_orders()
                    if not left:
                        break
                    log.warning("%d orders still resting after cancel-all: cancelling individually", len(left))
                    self.venue.cancel_orders([CancelOrder(str(o.get("client_order_id") or ""), str(o.get("ticker") or ""),
                                                          str(o["order_id"]), reason="shutdown") for o in left if o.get("order_id")])
                    await self.venue.wait_idle(tmo / 4)
                    await asyncio.sleep(0.5 * (attempt + 1))
                else:
                    left = await self.venue.resting_orders()
                cancel_ok = not left  # verified by the exchange's own resting-order list
                if self.cfg.venue.shutdown_delete_group:
                    for logical in list(self.venue.groups):
                        await self.venue._retry_group("delete", logical, attempts=2)  # noqa: SLF001
            except Exception as exc:  # noqa: BLE001
                cancel_ok = False
                log.exception("shutdown cancel failed")
                self.jlog("shutdown_error", self._clock(), error=f"{type(exc).__name__}: {exc}"[:300])
        self.shutdown_ok = cancel_ok
        if not cancel_ok:
            log.critical("SHUTDOWN COULD NOT CONFIRM ALL ORDERS CANCELLED: the watchdog will keep trying; "
                         "check the Kalshi UI")
            self.exit_code = self.exit_code or 3
        # 3. stop sources and background tasks
        for stop in self._source_stoppers:
            try:
                r = stop()
                if asyncio.iscoroutine(r):
                    await asyncio.wait_for(r, timeout=5.0)
            except Exception:  # noqa: BLE001
                log.exception("source stop failed")
        for t in self._tasks:
            if not t.done():
                t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        for t in list(self._bg):
            t.cancel()
        await asyncio.gather(*list(self._bg), return_exceptions=True)
        if self.venue is not None:
            await self.venue.close()
        if self.hedge is not None:
            try:
                await self.hedge.close()
            except Exception:  # noqa: BLE001
                log.exception("hedge venue close failed")
        if self._metrics_server is not None:
            await self._metrics_server.stop()
        # 4. final heartbeat: 'stopped' only when every order is confirmed cancelled
        if self.heartbeat_path is not None and cancel_ok:
            try:
                write_heartbeat(self.heartbeat_path, self._heartbeat_payload("stopped"))
            except OSError:
                pass
        end = self._clock()
        self.refresh_metrics()
        self.meta("session_end", end, reason=reason, exit_code=self.exit_code, cancel_ok=cancel_ok,
                  pump=dataclasses.asdict(self.pump.stats), last_ts=self.pump.last_ts)
        self.jlog("session_end", end, reason=reason, exit_code=self.exit_code, cancel_ok=cancel_ok)


def _action_fields(a: Action) -> dict[str, Any]:
    """Compact JSON-safe fields of an action for the audit log."""
    d = {f.name: getattr(a, f.name) for f in dataclasses.fields(a)}  # type: ignore[arg-type]
    if isinstance(a, PlaceOrder):
        d["px_dollars"] = a.px / PX_SCALE
    return d


__all__ = ["META_STREAM", "PAPER_STREAM", "RESULT_STREAM", "LiveRunner", "OrderGate", "Side", "Timer", "Wake"]
