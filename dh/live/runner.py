"""LiveRunner: one asyncio process, ONE ordered event queue, ONE strategy consumer.

    runner = LiveRunner(mm, mode="paper", period_ns=200_000_000, cfg=live_cfg, sim=sim, ...)
    runner.add_source("kalshi.ws", lambda: ws.run(runner.push))
    exit_code = await runner.run()

Data path
  * Sources (KalshiWS, external FeedClients) record every inbound frame raw (Recorder) and
    normalize it; the normalized events go to ``push`` -> the queue. Timestamps are receive
    times from ONE monotonic, strictly increasing clock (dh.live.clock.AnchoredClock), so
    queue items have unique, increasing timestamps.
  * Adapter results (order acks/rejects, cancel acks, reconciliation updates, gate rejects,
    hedge updates, the start-up RiskStateSeed) go to ``push_result``; events the runner
    derives itself (lag / reconciliation FeedStatus, checked position snapshots, back-filled
    fills) are injected in queue order. Both are recorded as codec-encoded events on stream
    ``events.live`` WHEN THE CONSUMER TAKES THEM, i.e. in processing order, so a live session
    replays bit-for-bit (dh.live.replay ranks events.live after every other stream: an
    injected event shares the timestamp of the item that caused it and comes after it).
  * Live inbound rules (mirrored by dh.live.replay): events of another Kalshi subaccount are
    dropped (the private channels are account-wide); order-group ids are translated to the
    strategy's logical id (foreign groups dropped); WS market_position snapshots go through
    the same persistence check as the REST positions instead of reaching the strategy; a fill
    already delivered (WS or REST back-fill, by trade/fill id) is never delivered twice.
  * The consumer task takes items in order and hands events to ``EventPump`` (dh.live.pump),
    which calls ``strategy.on_event`` synchronously, never concurrently, with ``Timer``
    events synthesized on the grid ``k * period_ns`` exactly as the backtest runner does.
    A wake-up is scheduled for the next grid point (and the next simulator delivery in
    paper mode), so timers keep flowing when the market is quiet. The consumer yields to
    the event loop after every item that dispatched orders and at least every
    ``loop.yield_items`` items / ``loop.yield_ms`` of work, so cancels, heartbeats and the
    reconciler never wait for a backlog to drain.
  * Actions returned by the strategy never block the consumer: live mode hands Kalshi
    actions to ``KalshiVenue.submit`` (asyncio tasks), paper mode to the simulator inside
    the pump, hedge actions to the hedge venue; ``Log`` actions go to the JSON log.

Safety
  * Order gate: new orders (PlaceOrder, AmendOrder) are refused (as OrderReject events,
    reason 'gate:<why>') while the kill file exists, after a strategy Halt (closed BEFORE the
    orders decided in the same cycle go out), on a fee mismatch, on data lag or a loop
    stall, while own-activity state is being reconciled, during the minute after a global
    cancel-all, on a persistent clock offset, during shutdown, and per market after a
    close-time / tick-grid change or a spec change found by re-discovery. Cancels always pass.
  * Data lag: the larger of the runner-queue lag and the exchange-time lag of Kalshi market
    data (BRTI source time, trade / book-delta ts_ms, relative to a trailing latency
    baseline, so frames piling up in the WebSocket receive buffer are seen). Above
    ``max_lag_s`` the gate closes and the strategy gets FeedStatus('runner.lag', 'stale')
    (it cancels its quotes); FeedStatus('runner.lag', 'resumed') once fresh data has kept
    the lag below max_lag_s/2 for ``lag_resume_s``.
  * Own-activity reconciliation (FeedStatus 'kalshi.reconcile' stale ... resynced): after a
    Kalshi WS disconnect (fills and order updates of the outage are lost: the private
    channels carry no sequence numbers) the runner back-fills GET /portfolio/fills since the
    disconnect, checks positions and resting orders, then resumes; also during the hold
    after a global cancel-all (start-up, watchdog). Fills are also back-filled every
    ``fills_backfill_interval_s``.
  * Global CancelAll: with a Halt in the same cycle -> DELETE /portfolio/events/orders (then
    new orders are held ``cancel_all_hold_s``: Kalshi may cancel orders placed during the
    minute after a cancel-all); without one -> the OrderManager's working orders are
    cancelled in batches and every other resting order found on REST is swept (no
    one-minute tail).
  * Kill file: checked every ``kill_check_interval_s`` on the consumer loop -> cancel all
    (REST) and stop.
  * Heartbeat file every ``heartbeat_interval_s`` while the consumer is alive (watchdog);
    'stopping' only for ``shutdown_timeout_s`` (a hung shutdown goes stale).
  * Risk state (day P&L, carried halt, pause) persisted every ``risk_state_interval_s``, on
    every Halt and at shutdown (dh.live.riskstate); the next session's seed reads it.
  * Graceful shutdown: gate closed, consumer drained, in-flight requests awaited, cancel-all
    via REST verified with the resting-order list, order group deleted, sources stopped.
"""

from __future__ import annotations

import asyncio
import dataclasses
import json
import logging
import os
import time
from collections import deque
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import orjson

from dh.core.actions import (
    Action,
    AmendOrder,
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
    KalshiOrderUpdate,
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
LAG_STREAM = "runner.lag"  # RiskCfg.lag_stream
RECONCILE_STREAM = "kalshi.reconcile"  # RiskCfg.reconcile_stream
LIVE_ORDER_STATES = ("PENDING_NEW", "RESTING", "PENDING_CANCEL", "PENDING_AMEND")
MARKET_DATA = (IndexTick, KalshiBookDelta, KalshiBookSnapshot, KalshiTrade, KalshiTicker, ExtBBO, ExtBookDelta,
               ExtBookSnapshot, ExtTrade)
# Kalshi market data that is always the LAST event of its WebSocket frame: the lag state may
# change (and inject a FeedStatus) right after these without splitting a frame's events
LAG_TYPES = (IndexTick, KalshiBookDelta, KalshiTrade, KalshiTicker)
OWN_TYPES = (KalshiFill, KalshiOrderUpdate, KalshiPositionSnapshot)
DAY_NS = 86_400 * NS_PER_S


@dataclass(frozen=True, slots=True)
class Wake:
    """Consumer wake-up at local time ``ts`` (timers / simulator deliveries / housekeeping)."""

    ts: int


@dataclass(frozen=True, slots=True)
class Side:
    """Side-channel input processed in queue order at ``ts`` (not a strategy event)."""

    ts: int
    kind: str  # universe_add | spec_changed | positions | positions_ws | fills | resting | queue_positions |
    #            reconcile_done | hold | hold_end | persist | call
    payload: Any = None


@dataclass(frozen=True, slots=True)
class Result:
    """An adapter result queued by ``push_result``; recorded on events.live when taken."""

    ev: Any

    @property
    def ts(self) -> int:
        return int(self.ev.ts)


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


class SeenIds:
    """Bounded FIFO set of delivered fill ids (trade_id / fill_id). The live runner and
    dh.live.replay apply the same rule: a KalshiFill with an id already delivered is dropped."""

    def __init__(self, cap: int = 200_000) -> None:
        self.cap = cap
        self._d: dict[str, None] = {}

    def __contains__(self, i: str) -> bool:
        return i in self._d

    def seen(self, f: KalshiFill) -> bool:
        return any(i and i in self._d for i in (f.trade_id, getattr(f, "fill_id", "")))

    def add(self, *ids: str) -> None:
        d = self._d
        for i in ids:
            if i and i not in d:
                d[i] = None
        while len(d) > self.cap:
            d.pop(next(iter(d)))


class LagMeter:
    """Exchange-time data lag. age = now - ts_exch; the per-source baseline is the smallest
    age seen over a trailing window (network + relay latency + clock offset, bucketed per
    minute); excess = age - baseline. The lag is the SMALLEST excess over the last
    ``confirm_ns`` (batched relays deliver some ticks late; a real backlog delays them all)."""

    def __init__(self, window_ns: int, confirm_ns: int, bucket_ns: int = 60 * NS_PER_S) -> None:
        self.window_ns = max(bucket_ns, int(window_ns))
        self.confirm_ns = max(0, int(confirm_ns))
        self.bucket_ns = bucket_ns
        self._base: dict[str, deque[list[int]]] = {}
        self._recent: deque[tuple[int, float]] = deque()

    @staticmethod
    def key(ev: Event) -> str:
        if isinstance(ev, IndexTick):
            return f"idx:{ev.index_id}:{ev.feed}"
        return type(ev).__name__

    def observe(self, ev: Event, now: int) -> float:
        te = int(getattr(ev, "ts_exch", 0) or 0)
        if te <= 0:
            return self.current(now)
        age = now - te
        dq = self._base.setdefault(self.key(ev), deque())
        b = now - now % self.bucket_ns
        if dq and dq[-1][0] == b:
            dq[-1][1] = min(dq[-1][1], age)
        else:
            dq.append([b, age])
        while dq and dq[0][0] < now - self.window_ns:
            dq.popleft()
        base = min(x[1] for x in dq)
        ex = max(0, age - base) / NS_PER_S
        self._recent.append((now, ex))
        return self.current(now)

    def current(self, now: int) -> float:
        r = self._recent
        while len(r) > 1 and r[0][0] < now - self.confirm_ns:
            r.popleft()
        return min(e for _, e in r) if r else 0.0


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
        risk_store: Any = None,
        cancel_all_marker: str | Path | None = None,
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
        self.cancel_all_marker = Path(cancel_all_marker) if cancel_all_marker else None
        self.fee_engine = fee_engine
        self.universe: dict[str, MarketSpec] = {s.ticker: s for s in universe}
        self._discover = discover
        self.subscribe_markets = subscribe_markets
        self.unsubscribe_markets = unsubscribe_markets
        self.series = tuple(series)
        self.session_id = session_id
        self.risk_store = risk_store
        self.subaccount = int(getattr(self.venue, "sub", 0) or 0) if self.venue is not None else 0
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
        self._stopping_since = 0.0  # monotonic
        self._consumer_task: asyncio.Task[Any] | None = None
        self._consumer_beat = self._mono()
        self._wake_handle: asyncio.TimerHandle | None = None
        self._wake_at: int | None = None
        self._dispatched = False
        self._next_kill_check = 0
        self._next_metrics = 0
        self._next_prune = 0
        self._next_marker_check = 0
        self._marker_seen = 0
        self._killed = False
        self._halts: list[Halt] = []
        self._pos_suspect: dict[str, tuple[int, int]] = {}  # ticker -> (exchange - ours, first seen ns)
        self._positions_now = asyncio.Event()
        self._discover_now = asyncio.Event()
        self._fee_acc: dict[str, Any] = {}  # order id -> OrderFeeAccumulator
        self._fee_eff: dict[str, tuple[str, float]] = {}  # ticker -> fee override in force
        self.fills_seen = SeenIds()
        self._cancel_sent: dict[str, int] = {}  # coid -> last cancel dispatch ts (bounded)
        self._halt_until: dict[str, int] = {}  # gate reason -> reopen ts (0 = manual)
        # data lag (M1)
        lc = self.cfg.loop
        self.lag_meter = LagMeter(int(lc.lag_window_s * NS_PER_S), int(lc.lag_confirm_s * NS_PER_S))
        self._lag_ok_since = 0
        # own-activity reconciliation (M3) and cancel-all holds
        self._recon: dict[str, int] = {}  # reason -> since ns
        self._recon_gen = 0
        self._ws_down_since = 0
        self._hold_until = 0
        self._clock_bad = 0
        self._last_brti_ns = 0
        self._last_event_ts = 0
        self._lag_s = 0.0
        self._metrics_server: MetricsServer | None = None
        self.shutdown_ok = True

    # ================================================================== producers
    @property
    def clock_ns(self) -> Callable[[], int]:
        return self._clock

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
          * fills / order updates / positions of another subaccount are dropped;
          * KalshiOrderGroupUpdate: the exchange group id is translated to the strategy's
            logical id; updates of groups that are not ours are dropped;
          * KalshiPositionSnapshot from the WS (source 'ws') never reaches the strategy
            directly: it races with the fill message and with settlement, so it goes through
            the same persistence check as the REST positions (``_check_positions``)."""
        if ev.ts < self._last_push_ts:
            self.metrics.inc("dh_ts_clamped_total")
            ev = dataclasses.replace(ev, ts=self._last_push_ts)
        else:
            self._last_push_ts = ev.ts
        if self.mode == "live":
            if isinstance(ev, OWN_TYPES) and getattr(ev, "subaccount", 0) != self.subaccount:
                self.metrics.inc("dh_foreign_subaccount_events_total", type=type(ev).__name__)
                return
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
        """Queue an adapter-generated event; it is recorded on ``events.live`` when the
        consumer takes it (processing order)."""
        if ev.ts < self._last_push_ts:
            self.metrics.inc("dh_ts_clamped_total")
            ev = dataclasses.replace(ev, ts=self._last_push_ts)
        else:
            self._last_push_ts = ev.ts
        self.queue.put_nowait(Result(ev))

    def push_side(self, kind: str, payload: Any = None) -> None:
        """Queue a side-channel item stamped now (processed in order by the consumer)."""
        self.queue.put_nowait(Side(self._stamp(), kind, payload))

    def hold(self, until_ns: int, why: str) -> None:
        """Hold NEW orders until ``until_ns`` (after a global cancel-all: Kalshi may cancel
        orders placed during the following minute); the strategy is told it is reconciling."""
        self.push_side("hold", {"until": int(until_ns), "why": why})

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
        lc = self.cfg.loop
        every = max(1, int(lc.yield_items))
        slice_s = max(0.0, lc.yield_ms / 1000.0)
        n = 0
        t0 = self._mono()
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
            n += 1
            if q.empty():
                self._schedule_wake()
            # never let a backlog starve the order requests just dispatched, the heartbeat or
            # the reconciler: asyncio.Queue.get() does not suspend while items are queued
            if self._dispatched or n >= every or self._mono() - t0 >= slice_s:
                self._dispatched = False
                n = 0
                await asyncio.sleep(0)
                t0 = self._mono()

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
        if isinstance(item, Result):
            self._record_event(RESULT_STREAM, item.ev)
            self._on_event(item.ev)
        elif isinstance(item, Wake):
            self._on_wake(item.ts)
        elif isinstance(item, Side):
            self.pump.advance(item.ts)
            self._on_side(item)
        else:
            self._on_event(item)

    def _on_event(self, ev: Event) -> None:
        live = self.mode == "live"
        if live and isinstance(ev, KalshiFill) and self.fills_seen.seen(ev):
            # delivered already (a REST back-fill beat the WS message, or a duplicate)
            self.metrics.inc("dh_duplicate_fills_dropped_total")
            self.jlog("fill_duplicate_dropped", ev.ts, ticker=ev.ticker, trade_id=ev.trade_id, order_id=ev.order_id)
            return
        self._last_event_ts = ev.ts
        max_lag = self.cfg.loop.max_lag_s
        lag_after: str = ""
        if live and max_lag > 0:
            nxt = self.pump.next_timer_ns
            if nxt is not None and (ev.ts - nxt) / NS_PER_S > max_lag:
                # the loop stalled: the catch-up timers delivered with this event would decide
                # on pre-stall data -> gate closed and the strategy told BEFORE they run
                self._lag_stale(self.pump.last_ts + 1, "stall", stall_s=round((ev.ts - nxt) / NS_PER_S, 3))
            if isinstance(ev, LAG_TYPES):
                lag_after = self._lag_update(ev)
        if self.venue is not None:
            self.venue.observe(ev)
        self._pre_event(ev)
        try:
            self.pump.feed(ev)
        except OrderingError:
            self.metrics.inc("dh_ts_clamped_total")
            ev = dataclasses.replace(ev, ts=self.pump.last_ts)
            self.pump.feed(ev)
        # state changes that tell the strategy something come AFTER the event that caused them
        if lag_after == "stale":
            self._lag_stale(ev.ts, "lag", lag_s=round(self._lag_s, 3))
        elif lag_after == "resumed":
            self._lag_resumed(ev.ts)
        if live and isinstance(ev, FeedStatus) and ev.stream == "kalshi.ws":
            self._on_ws_status(ev)

    def _lag_update(self, ev: Event) -> str:
        """Measure the data lag on Kalshi market data; returns 'stale' / 'resumed' / ''."""
        now = self._clock()
        q = max(0.0, (now - ev.ts) / NS_PER_S)
        x = self.lag_meter.observe(ev, now)
        lag = max(q, x)
        self._lag_s = lag
        lc = self.cfg.loop
        closed = "lag" in self.gate.reasons
        if lag > lc.max_lag_s:
            self._lag_ok_since = 0
            return "" if closed else "stale"
        if not closed:
            return ""
        if lag < lc.max_lag_s / 2:
            if not self._lag_ok_since:
                self._lag_ok_since = ev.ts
            if ev.ts - self._lag_ok_since >= int(lc.lag_resume_s * NS_PER_S):
                return "resumed"
        else:
            self._lag_ok_since = 0
        return ""

    def _lag_stale(self, inject_ts: int, why: str, **info: Any) -> None:
        if not self.gate.close("lag", inject_ts):
            return
        self._lag_ok_since = 0
        if why == "stall":
            self.metrics.inc("dh_loop_stalls_total")
        self.metrics.inc("dh_lag_episodes_total", why=why)
        log.warning("data lag (%s %s): new orders blocked, quotes cancelled", why, info)
        self.jlog("gate", inject_ts, action="close", reason="lag", why=why, **info)
        self._inject(FeedStatus(inject_ts, 0, LAG_STREAM, "stale", f"{why} {info}"[:200]))

    def _lag_resumed(self, ts: int) -> None:
        if not self.gate.open("lag"):
            return
        self._lag_ok_since = 0
        self.jlog("gate", ts, action="open", reason="lag", lag_s=round(self._lag_s, 3))
        self._inject(FeedStatus(ts, 0, LAG_STREAM, "resumed", f"lag {self._lag_s:.3f}s"))

    def _pre_event(self, ev: Event) -> None:
        """Runner-side bookkeeping BEFORE the strategy sees ``ev`` (never changes it)."""
        if isinstance(ev, IndexTick):
            if ev.index_id == "BRTI":
                self._last_brti_ns = ev.ts
        elif isinstance(ev, KalshiFill):
            self.metrics.inc("dh_fills_total", side=ev.book_side, taker=str(ev.is_taker).lower())
            self.fills_seen.add(ev.trade_id, getattr(ev, "fill_id", ""))
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
        pre-stall data: in live mode new orders are blocked and the strategy is told (lag
        stale, before those timers) until fresh data arrives."""
        nxt = self.pump.next_timer_ns
        max_lag = self.cfg.loop.max_lag_s
        if self.mode == "live" and max_lag > 0 and nxt is not None and (ts - nxt) / NS_PER_S > max_lag:
            self._lag_stale(self.pump.last_ts + 1, "stall", stall_s=round((ts - nxt) / NS_PER_S, 3))
        self.pump.advance(ts)

    def _housekeeping(self, ts: int) -> None:
        """Every consumer iteration (cheap unless something is due): kill-file check,
        metrics refresh, pruning of settled markets, expiry of timed halts and holds, the
        watchdog's cancel-all marker."""
        if self._halt_until:
            for key, until in list(self._halt_until.items()):
                if until and self.pump.last_ts >= until and self.gate.open(key):
                    self._halt_until.pop(key, None)
                    self.metrics.set("dh_halted", 0.0, scope=key.split(":", 1)[1])
                    self.jlog("gate", ts, action="open", reason=key, note="timed halt expired")
        if self._hold_until and ts >= self._hold_until:
            self._hold_until = 0
            self.push_side("hold_end")
        mono_ns = int(self._mono() * NS_PER_S)
        if self.kill_file is not None and mono_ns >= self._next_kill_check:
            self._next_kill_check = mono_ns + int(self.cfg.loop.kill_check_interval_s * NS_PER_S)
            if self.kill_file.triggered():
                self.kill(self.kill_file.reason())
        if self.cancel_all_marker is not None and self.mode == "live" and mono_ns >= self._next_marker_check:
            self._next_marker_check = mono_ns + NS_PER_S
            self._check_marker(ts)
        if mono_ns >= self._next_metrics:
            self._next_metrics = mono_ns + int(self.cfg.loop.metrics_refresh_s * NS_PER_S)
            self.refresh_metrics()
        if mono_ns >= self._next_prune:
            self._next_prune = mono_ns + 300 * NS_PER_S
            if self.pump.stats.events:
                self._prune(max(ts, self.pump.last_ts))

    def _check_marker(self, ts: int) -> None:
        """The watchdog cancelled everything (its marker file): hold new orders for the
        cancel-all tail and reconcile (our view of the orders is stale)."""
        p = self.cancel_all_marker
        try:
            st = p.stat()
        except OSError:
            return
        if st.st_mtime_ns == self._marker_seen:
            return
        self._marker_seen = st.st_mtime_ns
        try:
            rec = json.loads(p.read_text())
            t = int(rec.get("t", 0))
        except (OSError, ValueError, TypeError, AttributeError):
            t = st.st_mtime_ns
        until = t + int(self.cfg.venue.cancel_all_hold_s * NS_PER_S)
        if until <= ts:
            return
        log.error("the watchdog cancelled every order (%s): holding new orders, reconciling", p)
        self.jlog("watchdog_cancel_all_seen", ts, marker_t=t, hold_until=until)
        self.metrics.inc("dh_watchdog_cancel_alls_seen_total")
        self.hold(until, "watchdog_cancel_all")
        self._reconcile_now(ts, "watchdog_cancel_all")

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
        """Route the strategy's actions (never feeds events back: the pump is mid-item)."""
        ts = ev.ts
        halts = [a for a in actions if isinstance(a, Halt)]
        for h in halts:  # the gate closes BEFORE any order decided in the same cycle goes out
            self._on_halt(h, ts)
        to_venue: list[Action] = []
        to_hedge: list[Action] = []
        scoped: list[CancelAll] = []
        soft: list[CancelAll] = []
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
                continue
            if isinstance(a, Resume):
                self.jlog("resume_ignored", ts, reason=a.reason, note="restart the runner to resume after a halt")
            elif isinstance(a, (PlaceHedge, CancelHedge)):
                to_hedge.append(a)
            elif done:
                continue  # paper mode: the simulator took it
            elif self.mode == "live":
                if isinstance(a, CancelAll):
                    if a.tickers:
                        scoped.append(a)
                        continue
                    if not halts:
                        soft.append(a)
                        continue
                    self._note_global_cancel_all(ts)
                elif isinstance(a, (PlaceOrder, AmendOrder)):
                    why = self.gate.check(a.ticker)
                    if why:
                        self.metrics.inc("dh_gate_rejects_total", reason=why.split(":", 1)[0])
                        req = "create" if isinstance(a, PlaceOrder) else "amend"
                        self.push_result(OrderReject(self._stamp(), 0, a.client_order_id, a.ticker, f"gate:{why}", 0, req))
                        continue
                elif isinstance(a, CancelOrder):
                    self._note_cancel(a.client_order_id, ts)
                to_venue.append(a)
        if to_venue and self.venue is not None:
            self.venue.submit(to_venue, ts)
            self._dispatched = True
        sent = {a.order_id for a in to_venue if isinstance(a, CancelOrder) and a.order_id}
        for a in scoped:
            self._cancel_scoped(a.tickers, a.reason, ts, sent)
        for a in soft:
            self._cancel_soft(a.reason, ts, sent)
        if to_hedge and self.hedge is not None:
            self.hedge.submit(to_hedge, ts)
            self._dispatched = True

    def _note_cancel(self, coid: str, ts: int) -> None:
        if not coid:
            return
        d = self._cancel_sent
        d.pop(coid, None)
        d[coid] = ts
        while len(d) > 50_000:
            d.pop(next(iter(d)))

    def _note_global_cancel_all(self, ts: int) -> None:
        """A REST cancel-all is going out: Kalshi may cancel orders placed during the next
        minute, so new orders are held (the gate; a halt closes it anyway)."""
        hold = self.cfg.venue.cancel_all_hold_s
        if hold > 0:
            until = self._clock() + int(hold * NS_PER_S)
            self._hold_until = max(self._hold_until, until)
            if self.gate.close("cancel_all_hold", ts):
                self.jlog("gate", ts, action="close", reason="cancel_all_hold", until=self._hold_until)

    def _on_halt(self, a: Halt, ts: int) -> None:
        """Close the gate for new orders. until_ts == 0 (every M1 halt): until the runner is
        restarted by an operator; a timed halt reopens at until_ts (event time). The halt is
        persisted at once (a restart must not clear it)."""
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
            if not a.until_ts:
                self.persist_risk_state(ts, fsync=True, halt_reason=a.reason)

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
                self.persist_risk_state(f.ts, fsync=True, halt_reason="fee_mismatch")

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
        event; the runner tracks the effective (type, multiplier) the same way (override >
        the market's BASE fee, i.e. without any override; None clears) so its exact per-fill
        fee check stays right. An unparseable multiplier blocks the event's markets."""
        tickers = self._event_tickers(ev.event_ticker)
        if not tickers:
            return
        for t in tickers:
            spec = self.universe[t]
            eff = effective_fee(spec, ev)
            if eff is None:
                self._block([t], f"fee_update_unparseable:{ev.fee_multiplier_override}", ev.ts)
                continue
            ftype, mult = eff
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

    # ================================================================== reconciliation state
    def _on_ws_status(self, ev: FeedStatus) -> None:
        """Kalshi WS connection status (live): a disconnect loses fills / order updates that
        nothing re-sends (the private channels carry no sequence numbers) -> reconciling until
        the REST back-fill after the reconnect completes."""
        if ev.status == "disconnected":
            if not self._ws_down_since:  # the EARLIEST disconnect not reconciled yet
                self._ws_down_since = ev.ts
            self._recon_gen += 1
            self._recon_begin("ws_reconnect", ev.ts)
        elif ev.status == "connected" and self._ws_down_since:
            self._spawn(self._reconnect_reconcile(self._recon_gen, self._ws_down_since), "reconcile")

    def _recon_begin(self, reason: str, ts: int) -> None:
        if self.mode != "live":
            return
        first = not self._recon
        self._recon.setdefault(reason, ts)
        if first:
            self.gate.close("reconciling", ts)
            self.metrics.set("dh_reconciling", 1.0)
            self.jlog("reconcile", ts, action="begin", reason=reason)
            self._inject(FeedStatus(ts, 0, RECONCILE_STREAM, "stale", reason))

    def _recon_end(self, reason: str, ts: int) -> None:
        if self._recon.pop(reason, None) is None or self._recon:
            return
        self.gate.open("reconciling")
        self.metrics.set("dh_reconciling", 0.0)
        self.jlog("reconcile", ts, action="end", reason=reason)
        self._inject(FeedStatus(ts, 0, RECONCILE_STREAM, "resynced", reason))

    async def _reconnect_reconcile(self, gen: int, since_ns: int) -> None:
        """After a reconnect: fills since the disconnect (minus a margin), positions and the
        resting orders, in that order through the queue, then 'reconcile_done'."""
        v = self.venue
        vc = self.cfg.venue
        await asyncio.sleep(max(0.0, vc.reconnect_settle_s))
        attempt = 0
        while not self._stopping and gen == self._recon_gen:
            try:
                rows = await v.fetch_fills(since_ns // NS_PER_S - int(vc.fills_backfill_margin_s))
                pos = await v.fetch_positions()
                resting = await v.resting_orders()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - keep reconciling (quoting stays off)
                attempt += 1
                self.metrics.inc("dh_reconcile_errors_total", kind="reconnect")
                self.jlog("reconcile_error", self._clock(), what="reconnect", attempt=attempt,
                          error=f"{type(exc).__name__}: {exc}"[:300])
                await asyncio.sleep(min(vc.reconcile_retry_max_s, 0.5 * 2 ** min(attempt, 8)))
                continue
            if gen != self._recon_gen:
                return
            self.jlog("reconcile_fetch", self._clock(), since_ns=since_ns, fills=len(rows), positions=len(pos),
                      resting=len(resting))
            self.push_side("fills", rows)
            self.push_side("positions", pos)
            self.push_side("resting_all", resting)
            self.push_side("reconcile_done", {"gen": gen, "reason": "ws_reconnect", "round": 0})
            return

    async def _confirm_positions(self, gen: int, rnd: int) -> None:
        await asyncio.sleep(max(0.0, self.cfg.venue.position_confirm_s))
        attempt = 0
        while not self._stopping and gen == self._recon_gen:
            try:
                pos = await self.venue.fetch_positions()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                attempt += 1
                self.jlog("reconcile_error", self._clock(), what="positions", error=f"{type(exc).__name__}: {exc}"[:300])
                await asyncio.sleep(min(self.cfg.venue.reconcile_retry_max_s, 0.5 * 2 ** min(attempt, 8)))
                continue
            self.push_side("positions", pos)
            self.push_side("reconcile_done", {"gen": gen, "reason": "ws_reconnect", "round": rnd})
            return

    def _on_reconcile_done(self, ts: int, d: dict[str, Any]) -> None:
        if d.get("gen") != self._recon_gen:
            return  # superseded by a newer disconnect
        if self._pos_suspect and int(d.get("round", 0)) < 3:
            # a position still differs: confirm (-> mismatch -> Halt) or clear before quoting
            self._spawn(self._confirm_positions(self._recon_gen, int(d.get("round", 0)) + 1), "confirm_positions")
            return
        self._ws_down_since = 0
        self._recon_end(str(d.get("reason", "ws_reconnect")), ts)

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
        elif k == "fills_periodic":
            self._backfill_fills(item.ts, list(item.payload or ()),
                                 min_age_ns=int(self.cfg.venue.fills_backfill_min_age_s * NS_PER_S))
        elif k == "resting":
            self._check_resting(item.ts, list(item.payload or ()))
        elif k == "resting_all":
            self._check_resting(item.ts, list(item.payload or ()), force=True)
        elif k == "queue_positions":
            self._ingest_queue_positions(item.ts, list(item.payload or ()))
        elif k == "reconcile_done":
            self._on_reconcile_done(item.ts, dict(item.payload or {}))
        elif k == "hold":
            self._start_hold(item.ts, int(item.payload["until"]), str(item.payload.get("why", "")))
        elif k == "hold_end":
            if not self._hold_until:
                if self.gate.open("cancel_all_hold"):
                    self.jlog("gate", item.ts, action="open", reason="cancel_all_hold")
                self._recon_end("cancel_all_hold", item.ts)
        elif k == "persist":
            self.persist_risk_state(item.ts)
        elif k == "call":
            item.payload(item.ts)

    def _start_hold(self, ts: int, until: int, why: str) -> None:
        if self.mode != "live" or until <= ts:
            return
        self._hold_until = max(self._hold_until, until)
        if self.gate.close("cancel_all_hold", ts):
            self.jlog("gate", ts, action="close", reason="cancel_all_hold", why=why, until=self._hold_until)
        self.meta("hold", ts, why=why, until=self._hold_until)
        self._recon_begin("cancel_all_hold", ts)

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
        still trading. A DISCREPANCY (exchange - ours) must persist unchanged for
        ``position_confirm_s`` (a fill may be in flight on the WebSocket) before a
        KalshiPositionSnapshot is fed, which makes the strategy's OrderManager flag it
        (-> Halt(all)); new fills that move both sides keep the clock running. ``partial``:
        ``exch`` holds only the markets it names (a WS market_position message)."""
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
                    self._inject(KalshiPositionSnapshot(ts, 0, t, ex, source=src, subaccount=self.subaccount))
                continue
            d = ex - ours
            s = self._pos_suspect.get(t)
            if s is None or s[0] != d:
                self._pos_suspect[t] = (d, ts)
                self.metrics.inc("dh_position_suspects_total")
                self.jlog("position_suspect", ts, ticker=t, exchange=ex, ours=ours, diff=d)
                self._positions_now.set()  # confirm soon
            elif ts - s[1] >= confirm_ns:
                self._pos_suspect.pop(t, None)
                self.metrics.inc("dh_position_mismatches_total")
                log.error("POSITION MISMATCH %s exchange=%d ours=%d", t, ex, ours)
                self.jlog("position_mismatch", ts, ticker=t, exchange=ex, ours=ours, source=src)
                self._inject(KalshiPositionSnapshot(ts, 0, t, ex, source=src, subaccount=self.subaccount))
        if not partial:
            for t in [t for t in self._pos_suspect if t not in cands]:
                self._pos_suspect.pop(t, None)

    def _check_resting(self, ts: int, rows: list[dict[str, Any]], *, force: bool = False) -> None:
        """Order reconciliation against GET /portfolio/orders?status=resting:
          * ghost sweep: cancel resting orders the strategy does not consider live;
          * stuck cancels: an order the strategy is cancelling (PENDING_CANCEL) that still
            rests although the cancel went out more than the OrderManager's change timeout
            ago is cancelled again (cancels are idempotent);
          * lost updates: every order the strategy considers live (with an exchange id) that
            is NOT resting is looked up (GET order) and its final state fed back as a
            KalshiOrderUpdate (a canceled/executed user_order message may have been lost)."""
        if self.venue is None or not (self.cfg.venue.ghost_sweep or force):
            return
        om = getattr(self.strategy, "om", None)
        if om is not None:
            resting_ids = {str(o.get("order_id") or "") for o in rows}
            for w in om.working():
                if w.order_id and w.order_id not in resting_ids and w.created_ns < ts - NS_PER_S:
                    self.metrics.inc("dh_order_state_checks_total")
                    self.venue.check_order(w.client_order_id, w.order_id, w.ticker, recancel=False, reason="not_resting")
        change_ns = int(getattr(om, "change_timeout_ns", 10 * NS_PER_S)) if om is not None else 10 * NS_PER_S
        ghosts: list[CancelOrder] = []
        again: list[CancelOrder] = []
        for o in rows:
            coid = str(o.get("client_order_id") or "")
            oid = str(o.get("order_id") or "")
            if not oid:
                continue
            w = om.order(coid) if (om is not None and coid) else None
            state = getattr(getattr(w, "state", None), "name", "")
            if w is not None and state in LIVE_ORDER_STATES:
                # last cancel sent: the runner's own record (every cancel it dispatched), or the
                # OrderManager's when it exposes one; unknown -> long ago. (Not updated_ns: the
                # reconciler's "still resting" updates refresh it.)
                sent = max(self._cancel_sent.get(coid, 0), int(getattr(w, "cancel_sent_ns", 0) or 0))
                if state == "PENDING_CANCEL" and ts - sent > change_ns:
                    again.append(CancelOrder(coid, str(o.get("ticker") or w.ticker), oid, reason="sweep_recancel"))
                continue
            ghosts.append(CancelOrder(coid, str(o.get("ticker") or ""), oid, reason="ghost"))
        if ghosts:
            self.metrics.inc("dh_ghost_orders_total", len(ghosts))
            self.jlog("ghost_orders", ts, orders=[(g.client_order_id, g.order_id, g.ticker) for g in ghosts])
            log.warning("cancelling %d resting orders unknown to the strategy", len(ghosts))
        if again:
            self.metrics.inc("dh_cancel_resends_total", len(again))
            self.jlog("cancel_resend", ts, orders=[(g.client_order_id, g.order_id, g.ticker) for g in again])
            log.warning("re-cancelling %d orders still resting after their cancel", len(again))
            for g in again:
                self._note_cancel(g.client_order_id, ts)
                self.venue.check_order(g.client_order_id, g.order_id, g.ticker, recancel=True, reason="sweep_recancel")
        if ghosts or again:
            self.venue.cancel_orders(ghosts + again)
            self._dispatched = True

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

    def _backfill_fills(self, ts: int, rows: list[dict[str, Any]], *, min_age_ns: int = 0) -> None:
        """REST fills: feed those the WebSocket never delivered (after a reconnect, an
        own-channel gap, and periodically as a safety net), through the same bookkeeping as a
        WS fill (fee check, metrics). ``min_age_ns`` (periodic pass): fills younger than this
        are left to the WebSocket (the next pass overlaps and takes them if they never come).

        Dedupe: a REST Fill's ``trade_id`` is documented as the legacy name of ``fill_id``;
        both are checked against every fill already delivered, and a WS fill arriving after
        its REST copy is dropped (dh.live.replay applies the same rule). If the WS trade ids
        ever differed from the REST ids, fills would be counted twice and the position
        reconciliation would halt trading (a loud failure, never a silent one)."""
        from dh.kalshi.normalize import rest_fill_to_event
        from dh.live.riskstate import row_in_subaccount

        n = 0
        for row in sorted(rows, key=lambda r: (str(r.get("created_time") or ""), str(r.get("fill_id") or r.get("trade_id") or ""))):
            tid = str(row.get("trade_id") or "")
            fid = str(row.get("fill_id") or "")
            if (tid and tid in self.fills_seen) or (fid and fid in self.fills_seen):
                continue
            if not row_in_subaccount(row, self.subaccount):
                self.metrics.inc("dh_foreign_subaccount_events_total", type="RestFill")
                continue
            try:
                ev = rest_fill_to_event(row, ts)
            except (KeyError, ValueError, TypeError):
                continue
            if min_age_ns and (not ev.ts_exch or ts - ev.ts_exch < min_age_ns):
                continue
            ev = dataclasses.replace(ev, trade_id=tid or fid, fill_id=fid, subaccount=self.subaccount)
            self.jlog("fill_backfilled", ts, ticker=ev.ticker, trade_id=ev.trade_id, order_id=ev.order_id, qty=ev.qty)
            self._pre_event(ev)
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
            for a in acts:
                self._note_cancel(a.client_order_id, ts)
            self.venue.cancel_orders(acts)
            self._dispatched = True
        if self.venue.has_pending_creates(tset) or om is None:
            self._spawn(self.venue.sweep_resting(sorted(tset), reason, skip), "sweep")

    def _cancel_soft(self, reason: str, ts: int, skip_oids: Iterable[str] = ()) -> None:
        """CancelAll() without a Halt: every working order of the OrderManager (except
        cancels sent within the last second), then every other order still resting on REST
        (ghosts, creates with unknown outcome). No DELETE /portfolio/events/orders, so no
        one-minute tail of cancelled new orders when quoting resumes."""
        if self.venue is None:
            return
        skip = set(skip_oids)
        om = getattr(self.strategy, "om", None)
        acts: list[CancelOrder] = []
        if om is not None:
            for w in om.working():
                if not w.order_id or w.order_id in skip:
                    continue
                if ts - self._cancel_sent.get(w.client_order_id, 0) < NS_PER_S:
                    skip.add(w.order_id)
                    continue
                acts.append(CancelOrder(w.client_order_id, w.ticker, w.order_id, reason=f"all:{reason}"))
                skip.add(w.order_id)
        if acts:
            for a in acts:
                self._note_cancel(a.client_order_id, ts)
            self.venue.cancel_orders(acts)
            self._dispatched = True
        self.jlog("cancel_all_soft", ts, reason=reason, cancels=len(acts))
        self._spawn(self.venue.sweep_resting(None, reason, skip), "sweep")

    def _reconcile_now(self, ts: int, reason: str) -> None:
        """Immediate REST reconciliation (own-channel gap, a fill whose post_position
        disagreed, the watchdog's cancel-all): fills since the gap, then positions and orders
        (the positions loop runs at once)."""
        if self.venue is None:
            return
        self.jlog("reconcile_now", ts, reason=reason)
        self.metrics.inc("dh_reconcile_requests_total")
        margin = int(self.cfg.venue.fills_backfill_margin_s)

        async def fills() -> None:
            try:
                rows = await self.venue.fetch_fills(ts // NS_PER_S - margin)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                self.jlog("reconcile_error", self._clock(), what="fills", error=f"{type(exc).__name__}: {exc}"[:300])
                return
            self.push_side("fills", rows)

        self._spawn(fills(), "fills")
        self._positions_now.set()

    def _inject(self, ev: Event) -> None:
        """Feed a runner-generated event now (inside the consumer, never from an action
        hook), recorded for replay."""
        self._record_event(RESULT_STREAM, ev)
        self.pump.feed(ev)

    # ================================================================== risk state (C1)
    def risk_snapshot(self, ts: int, *, halt_reason: str = "") -> Any:
        """Current per-day risk state for persistence (dh.live.riskstate.RiskState)."""
        from dh.live.riskstate import RiskState, base_reason, day_start

        s = self.strategy
        risk = getattr(s, "risk", None)
        prev = getattr(self.risk_store, "last", None)
        pnl = float(getattr(prev, "day_pnl_usd", 0.0)) if prev is not None and prev.day_start_ns == day_start(ts) else 0.0
        halted = False
        reason = ""
        pause = 0
        if risk is not None:
            eq = getattr(s, "equity", None)
            try:
                if callable(eq) and hasattr(risk, "day_pnl"):
                    S = s.tracker.latest_value() if hasattr(s, "tracker") else None
                    pnl = float(risk.day_pnl(ts, float(eq(S))))
            except Exception:  # noqa: BLE001 - keep the last known P&L
                log.exception("day P&L for the risk state failed")
            halted = bool(getattr(risk, "halted_all", False) or getattr(risk, "halted_quoting", False))
            reason = str(getattr(risk, "halt_reason", "") or "")
            pause = int(getattr(risk, "pause_until_ns", 0) or 0)
        manual = [k for k, until in self._halt_until.items() if not until]
        if "fee_mismatch" in self.gate.reasons or manual:
            halted = True
            reason = reason or halt_reason or ("fee_mismatch" if "fee_mismatch" in self.gate.reasons else manual[0])
        if halt_reason and not reason:
            reason = halt_reason
        return RiskState(day_start_ns=day_start(ts), day_pnl_usd=pnl, halted=halted, halt_reason=base_reason(reason),
                         pause_until_ns=pause, session=self.session_id, mode=self.mode, updated_ns=ts)

    def persist_risk_state(self, ts: int, *, fsync: bool = False, halt_reason: str = "") -> None:
        if self.risk_store is None:
            return
        try:
            st = self.risk_snapshot(ts, halt_reason=halt_reason)
            self.risk_store.save(st, fsync=fsync)
            self.metrics.set("dh_day_pnl_dollars", st.day_pnl_usd)
        except Exception as exc:  # noqa: BLE001
            self.metrics.inc("dh_risk_state_errors_total")
            log.error("risk state persistence failed: %s", exc)

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
            self._note_global_cancel_all(self._clock())
            self.venue.submit([CancelAll(reason=reason)], self._clock())
            self._dispatched = True

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
        tmo = self.cfg.loop.shutdown_timeout_s
        warned = False
        while True:
            # running: only while the consumer makes progress (a dead or stuck consumer lets
            # the heartbeat go stale -> watchdog). stopping: the process is busy cancelling,
            # but only for shutdown_timeout_s: a hung shutdown must go stale too.
            if self._stopping:
                beat, state = self._mono() - self._stopping_since <= tmo, "stopping"
                if not beat and not warned:
                    warned = True
                    log.critical("shutdown exceeded %.1fs: heartbeat stopped (the watchdog takes over)", tmo)
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
        d = {"pid": os.getpid(), "mode": self.mode, "state": state, "session": self.session_id,
             "queue": self.queue.qsize(), "lag_s": round(self._lag_s, 3), "last_event_ts": self._last_event_ts,
             "gate": sorted(self.gate.reasons), "shutdown_timeout_s": self.cfg.loop.shutdown_timeout_s}
        if self._stopping:
            d["stopping_for_s"] = round(self._mono() - self._stopping_since, 3)
        return d

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

    async def _fills_loop(self) -> None:
        """Safety net for fills the WebSocket lost without a visible gap: GET /portfolio/fills
        every ``fills_backfill_interval_s`` (overlapping windows; duplicates are dropped)."""
        v = self.venue
        vc = self.cfg.venue
        iv = vc.fills_backfill_interval_s
        if v is None or iv <= 0:
            return
        while not self._stopping:
            await asyncio.sleep(iv)
            if self._stopping:
                return
            if not v.read_budget_ok(("/portfolio/fills",)):
                self.metrics.inc("dh_polls_skipped_total", poll="fills")
                continue
            try:
                rows = await v.fetch_fills(self._clock() // NS_PER_S - int(iv + vc.fills_backfill_margin_s))
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                self.metrics.inc("dh_reconcile_errors_total", kind="fills")
                self.jlog("reconcile_error", self._clock(), what="fills", error=f"{type(exc).__name__}: {exc}"[:300])
                continue
            if rows:
                self.push_side("fills_periodic", rows)

    async def _risk_state_loop(self) -> None:
        iv = self.cfg.loop.risk_state_interval_s
        if self.risk_store is None or iv <= 0:
            return
        while not self._stopping:
            await asyncio.sleep(iv)
            if not self._stopping:
                self.push_side("persist")

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

    def clock_offset_s(self, chrony_offset_s: float | None) -> float:
        """Offset of the receive clock from true time: the anchored clock's drift from the
        wall clock plus the wall clock's own (chrony) offset."""
        drift = getattr(self._clock, "drift_ns", None)
        d = drift() / NS_PER_S if callable(drift) else 0.0
        return d + (float(chrony_offset_s) if chrony_offset_s is not None else 0.0)

    def note_clock_offset(self, eff_s: float, ts: int) -> None:
        """Alarm above clock_alarm_ms; block new orders while |offset| > clock_block_ms on
        clock_block_samples samples in a row (reopens on the first good sample)."""
        lc = self.cfg.loop
        self.metrics.set("dh_clock_offset_seconds", eff_s)
        ms = abs(eff_s) * 1000
        if ms > lc.clock_alarm_ms:
            self.metrics.inc("dh_clock_alarms_total")
            log.warning("clock offset %.1f ms > %.1f ms", eff_s * 1000, lc.clock_alarm_ms)
        if ms > lc.clock_block_ms:
            self._clock_bad += 1
            if self._clock_bad >= max(1, lc.clock_block_samples) and self.gate.close("clock", ts):
                log.error("clock offset %.1f ms persists: new orders blocked (restart re-anchors the clock)", eff_s * 1000)
                self.jlog("gate", ts, action="close", reason="clock", offset_ms=round(eff_s * 1000, 3))
                self.meta("gate", ts, action="close", reason="clock", offset_ms=round(eff_s * 1000, 3))
        else:
            self._clock_bad = 0
            if self.gate.open("clock"):
                self.jlog("gate", ts, action="open", reason="clock", offset_ms=round(eff_s * 1000, 3))

    async def _clock_loop(self) -> None:
        iv = self.cfg.loop.clock_sample_s
        if iv <= 0:
            return
        from dh.store.recorder import sample_clock

        step = min(iv, 5.0)
        next_sample = 0.0
        last_off: float | None = None
        while not self._stopping:
            try:
                now = self._mono()
                if now >= next_sample:
                    next_sample = now + iv
                    rec = await asyncio.to_thread(sample_clock)
                    if self.recorder is not None:
                        self.recorder.write("clock", self._clock(), orjson.dumps(rec, default=str))
                    off = rec.get("offset_s")
                    last_off = float(off) if isinstance(off, (int, float)) else None
                self.note_clock_offset(self.clock_offset_s(last_off), self._clock())
            except Exception:  # noqa: BLE001
                log.exception("clock sample failed")
            await asyncio.sleep(step)

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
        for r in ("lag", "reconciling", "cancel_all_hold", "clock"):
            m.set("dh_gate_reason", 1.0 if r in self.gate.reasons else 0.0, reason=r)
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
            m.set("dh_venue_stuck_cancels", float(len(self.venue.stuck_orders)))
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
                "reconciling": sorted(self._recon), "stuck_cancels": list(getattr(self.venue, "stuck_orders", []) or []),
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
        self._tasks.append(asyncio.create_task(self._risk_state_loop(), name="runner:risk_state"))
        if self.venue is not None:
            self._tasks.append(asyncio.create_task(self.venue.run_reconciler(), name="venue:reconciler"))
            self._tasks.append(asyncio.create_task(self._positions_loop(), name="runner:positions"))
            self._tasks.append(asyncio.create_task(self._queue_positions_loop(), name="runner:queue_positions"))
            self._tasks.append(asyncio.create_task(self._fills_loop(), name="runner:fills"))
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
        self._stopping_since = self._mono()
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
        self.persist_risk_state(self._clock(), fsync=True)
        # 2. live: finish in-flight writes, cancel everything, verify, delete the group
        cancel_ok = True
        if self.venue is not None:
            try:
                await self.venue.wait_idle(tmo / 2)
                try:
                    left = await self.venue.cancel_all_verified(f"shutdown:{reason}", wait_s=tmo / 4)
                except RuntimeError:  # the cancel-all itself failed: cancel what we can see
                    left = await self.venue.resting_orders()
                    if left:
                        self.venue.cancel_orders([CancelOrder(str(o.get("client_order_id") or ""), str(o.get("ticker") or ""),
                                                              str(o["order_id"]), reason="shutdown") for o in left if o.get("order_id")])
                        await self.venue.wait_idle(tmo / 4)
                        left = await self.venue.resting_orders()
                    else:
                        left = []
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


def effective_fee(spec: MarketSpec, ev: KalshiFeeUpdate) -> tuple[str, float] | None:
    """(fee type, multiplier) in force after ``ev`` for ``spec``: the override, else the
    market's base fee (without any override); None if the multiplier is unparseable."""
    base_type, base_mult = getattr(spec, "base_fee", (spec.fee_type, spec.fee_multiplier))
    ftype = ev.fee_type_override if ev.fee_type_override is not None else base_type
    if ev.fee_multiplier_override in (None, ""):
        return ftype, float(base_mult)
    try:
        return ftype, float(ev.fee_multiplier_override)
    except ValueError:
        return None


def _action_fields(a: Action) -> dict[str, Any]:
    """Compact JSON-safe fields of an action for the audit log."""
    d = {f.name: getattr(a, f.name) for f in dataclasses.fields(a)}  # type: ignore[arg-type]
    if isinstance(a, PlaceOrder):
        d["px_dollars"] = a.px / PX_SCALE
    return d


__all__ = ["LAG_STREAM", "META_STREAM", "PAPER_STREAM", "RECONCILE_STREAM", "RESULT_STREAM", "LagMeter", "LiveRunner",
           "OrderGate", "Result", "SeenIds", "Side", "Timer", "Wake", "effective_fee"]
