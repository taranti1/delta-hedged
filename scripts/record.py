#!/usr/bin/env python
"""Market-data collector daemon: records every configured feed append-only.

    python scripts/record.py                          # config/feeds.yaml
    python scripts/record.py --only coinbase,kraken --no-kalshi --duration 120

What runs (each in its own supervised asyncio task: one venue crashing never stops others):
  * every enabled external feed (config/feeds.yaml ``feeds``) -> dh.feeds FeedClient.run(
    recorder.write): reconnect/backoff/staleness/resync handled per feed;
  * Kalshi: dh.kalshi.ws.KalshiWS(url, signer, subscriptions, on_raw=recorder.write,
    on_event=<monitor only>) subscribed to orderbook_delta + trade + ticker on the open
    KXBTCD/KXBTC/KXBTC15M markets, market_lifecycle_v2 (all markets) and
    cfbenchmarks_value + cfbenchmarks_value_5hz for BRTI; plus periodic REST snapshots via
    dh.kalshi.rest.KalshiRest(on_raw=recorder.write). New markets are added from lifecycle
    events and a periodic REST re-discovery. At start-up and on every refresh the series
    objects, scheduled series/event fee changes and each newly discovered event (GET
    /events/{e}) are recorded too, so specs, strikes and fees can be rebuilt offline
    (dh.research.replay_env). (dh.kalshi is imported lazily.)
  * clock-health sampler -> stream 'clock'; a 'meta' record with the session configuration.
  * a status line every ``status_interval_s``: msgs/s per stream, last message age, gaps,
    reconnects, stale events, free disk space and shed streams.
  * a low-disk guard (dh.store.recorder.DiskGuard, config ``recorder.*``) every
    ``disk_check_interval_s`` (and right after a disk-full write error): below
    ``min_free_gb_shed`` the non-essential ``shed_streams`` stop being recorded (``meta``
    records mark it; Kalshi, coinbase/kraken/bitstamp, clock and meta keep recording), back
    above it + ``shed_hysteresis_gb`` they resume (their feeds reconnect for a fresh snapshot);
    below ``min_free_gb_stop`` the collector shuts down cleanly and exits 5. It also refuses to
    start (exit 5) below ``min_free_gb_stop``.

Status recording: venue clients write connection markers into their own streams and the
Kalshi client writes synthetic status frames into 'kalshi.ws', so replay reproduces outages
from those streams. The 'status' stream only carries what nothing else records: supervisor
restarts of crashed tasks and Kalshi start-up failures.

Shutdown (SIGINT/SIGTERM, --duration, or low disk): stop clients, cancel tasks, flush, fsync
and write segment indexes (Recorder.close()). Exit status: 0 normal, 2 another recorder holds
the store lock or the recorder config is invalid, 5 (EXIT_LOW_DISK) free space below
``min_free_gb_stop`` (at start-up or while running).
"""

from __future__ import annotations

import argparse
import asyncio
import fcntl
import logging
import os
import platform
import signal
import socket
import subprocess
import sys
import time
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

import orjson

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from dh.core.events import Event, FeedStatus, KalshiMarketLifecycle  # noqa: E402
from dh.feeds.base import FeedClient, make_marker, parse_rfc3339_ns  # noqa: E402
from dh.feeds.registry import build_feeds, load_feeds_config  # noqa: E402
from dh.store.recorder import EXIT_LOW_DISK, ClockSampler, DiskGuard, Recorder  # noqa: E402

log = logging.getLogger("record")

# Kalshi REST budgets are per ACCOUNT, shared with the other live system and with every System 1
# process (live runner 0.2, paper runner, watchdog, tools): the recorder takes 0.1 by default
# (config/feeds.yaml kalshi.account_share; measured use ~1.2 tokens/s of the read budget). Keep the
# sum over all System 1 processes <= 0.5 (docs/RUNBOOK.md 1.3).
RECORDER_ACCOUNT_SHARE = 0.1


def recorder_account_share(kcfg: dict[str, Any]) -> float:
    """The recorder's share of the account's REST budget: feeds.yaml kalshi.account_share, else 0.1."""
    v = (kcfg or {}).get("account_share")
    share = RECORDER_ACCOUNT_SHARE if v in (None, "") else float(v)
    if not 0.0 < share <= 1.0:
        raise ValueError(f"kalshi.account_share must be in (0, 1], got {share}")
    return share


# ============================================================================ monitoring
class Monitor:
    """Counters for the status line (never recorded)."""

    def __init__(self, recorder: Recorder) -> None:
        self.recorder = recorder
        self.feeds: dict[str, FeedClient] = {}
        self.kalshi_ws: Any = None
        self.kalshi_status: dict[str, int] = {}
        self.kalshi_events = 0
        self.restarts: dict[str, int] = {}
        self._last_counts: dict[str, int] = {}
        self._last_time = time.monotonic()

    def on_feed_status(self, ev: FeedStatus) -> None:
        if ev.status in ("gap", "error"):
            log.warning("%s %s %s", ev.stream, ev.status, ev.detail[:200])

    def on_kalshi_event(self, ev: Event) -> None:
        self.kalshi_events += 1
        if isinstance(ev, FeedStatus):
            key = ev.status
            self.kalshi_status[key] = self.kalshi_status.get(key, 0) + 1
            if ev.status in ("gap", "error", "disconnected", "stale"):
                log.warning("kalshi %s %s %s", ev.stream, ev.status, ev.detail[:200])

    def status_line(self) -> str:
        now_ns = time.time_ns()
        now = time.monotonic()
        dt = max(1e-9, now - self._last_time)
        self._last_time = now
        rows = []
        streams = {k: (v.count, v.last_t) for k, v in self.recorder.stream_stats().items()}
        shed = self.recorder.shed_streams
        by_stream = {f.name: f for f in self.feeds.values()}
        for s in sorted(streams):
            count, last_t = streams[s]
            rate = (count - self._last_counts.get(s, 0)) / dt
            self._last_counts[s] = count
            age = (now_ns - last_t) / 1e9 if last_t else float("nan")
            extra = ""
            f = by_stream.get(s)
            if f is not None:
                m = f.metrics
                extra = f" gaps={m.gaps} rsync={m.resyncs} reconn={m.reconnects} stale={m.stale} err={m.errors}"
            elif s == "kalshi.ws" and self.kalshi_ws is not None:
                st = self.kalshi_ws.state.counters
                extra = f" gaps={st.get('gaps', 0)} dups={st.get('dups', 0)} connects={self.kalshi_ws.connects}"
            if s in shed:
                extra += " SHED (not recorded: low disk)"
            rows.append(f"{s:<22} {rate:8.1f}/s age={age:6.1f}s n={count}{extra}")
        rs = self.recorder.stats
        head = f"recorder records={rs.records} MB={rs.bytes / 1e6:.1f} frames={rs.frames} write_errors={rs.write_errors}"
        head += f" free_GB={rs.free_gb:.1f}" if rs.free_gb is not None else " free_GB=?"
        if rs.shed_streams:
            head += f" LOW-DISK SHED {','.join(rs.shed_streams)} not_recorded={rs.shed_records}"
        if rs.dropped_records or rs.disk_full_errors:
            head += f" dropped={rs.dropped_records} disk_full_errors={rs.disk_full_errors}"
        if self.restarts:
            head += f" restarts={self.restarts}"
        return head + "\n  " + "\n  ".join(rows)


# ============================================================================ supervision
async def supervise(
    name: str,
    stream: str,
    factory: Callable[[], Awaitable[None]],
    recorder: Recorder,
    monitor: Monitor,
    stop: asyncio.Event,
    backoff0: float,
    backoff_max: float,
) -> None:
    """Run ``factory()`` forever; a crash is recorded to 'status' and the task restarted."""
    delay = backoff0
    while not stop.is_set():
        started = time.monotonic()
        try:
            await factory()
            if stop.is_set():
                return
            detail = "task returned unexpectedly"
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            detail = f"task crashed: {type(exc).__name__}: {exc}"[:300]
            log.exception("%s crashed", name)
        monitor.restarts[name] = monitor.restarts.get(name, 0) + 1
        recorder.write_event("status", FeedStatus(time.time_ns(), 0, stream, "error", detail))
        if time.monotonic() - started > 300:
            delay = backoff0
        try:
            await asyncio.wait_for(stop.wait(), timeout=delay)
        except asyncio.TimeoutError:
            pass
        delay = min(backoff_max, delay * 2)


# ============================================================================ Kalshi source
class KalshiSource:
    """KalshiWS + periodic REST snapshots, coded to dh.kalshi's public interface."""

    def __init__(self, kcfg: dict[str, Any], recorder: Recorder, monitor: Monitor, stop: asyncio.Event) -> None:
        self.kcfg = kcfg
        self.recorder = recorder
        self.monitor = monitor
        self.stop = stop
        self.tickers: list[str] = []
        self.ws: Any = None
        self.rest: Any = None
        self._refresh_now = asyncio.Event()
        # 'created' lifecycle events for markets that open LATER (Kalshi creates a day of
        # KXBTC15M markets at once, found live 2026-09-25): ticker -> open time (ns). The
        # refresh loop wakes shortly after each open instead of re-discovering on every event.
        self.pending_open: dict[str, int] = {}
        self._last_refresh_mono = 0.0
        self.series = [str(s) for s in kcfg.get("series") or []]
        self.events_found: set[str] = set()  # event tickers of discovered markets
        self.events_recorded: set[str] = set()  # events whose GET /events/{e} was recorded

    async def run(self) -> None:
        try:
            from dh.kalshi.config import load_config
            from dh.kalshi.rest import KalshiRest
            from dh.kalshi.ws import KalshiWS, Subscription, shard_market_subscriptions
        except ImportError as exc:
            log.error("Kalshi source disabled: dh.kalshi not importable (%s)", exc)
            self.recorder.write_event("status", FeedStatus(time.time_ns(), 0, "kalshi.ws", "error", f"dh.kalshi import failed: {exc}"[:300]))
            await self.stop.wait()
            return
        kc = load_config(self.kcfg.get("config"), env=self.kcfg.get("env"))
        if kc.env_file_report is not None:
            log.info("kalshi: %s", kc.env_file_report.summary())  # variable names only
        self.series = self.series or list(kc.series)
        signer = kc.signer()
        # read_only: the recorder never writes (any non-GET raises before it is signed or sent)
        self.rest = KalshiRest(kc.rest_url, signer, kc.limiter(account_share=recorder_account_share(self.kcfg)),
                               on_raw=self.recorder.write, read_only=True, **kc.rest_kwargs())
        tasks: list[asyncio.Task[Any]] = []
        try:
            if signer is not None:
                try:  # the account's real limits, scaled by kalshi.account_share (recorder default 0.1)
                    await self.rest.configure_rate_limits()
                except Exception as exc:  # noqa: BLE001 - keep the (scaled) defaults
                    log.warning("kalshi: account limits unavailable (%s): using configured defaults", exc)
            log.info("kalshi: REST rate limiter %s", self.rest.limiter.describe())
            self.tickers = await self.discover()
            log.info("kalshi: %d open markets in %s", len(self.tickers), ",".join(self.series))
            await self.record_metadata()
            tasks.append(asyncio.create_task(self.refresh_loop(), name="kalshi:refresh"))
            if float(self.kcfg.get("rest_orderbook_interval_s", 60) or 0) > 0:
                tasks.append(asyncio.create_task(self.orderbook_loop(), name="kalshi:orderbooks"))
            if float(self.kcfg.get("cf_history_interval_s", 0) or 0) > 0:
                tasks.append(asyncio.create_task(self.cf_history_loop(), name="kalshi:cf"))
            if signer is None:
                log.error("kalshi: no API credentials (%s): the WebSocket requires authentication, "
                          "recording REST snapshots only", kc.credentials_hint())
            else:
                cap = int(self.kcfg.get("max_markets_per_subscription", 100) or 0)
                subs = shard_market_subscriptions(
                    list(self.kcfg.get("market_channels") or ["orderbook_delta", "trade", "ticker"]),
                    list(self.tickers), cap)
                for ch in self.kcfg.get("lifecycle_channels") or ["market_lifecycle_v2"]:
                    subs.append(Subscription([ch]))
                for ch in self.kcfg.get("index_channels") or ["cfbenchmarks_value", "cfbenchmarks_value_5hz"]:
                    subs.append(Subscription([ch], index_ids=list(self.kcfg.get("index_ids") or kc.index_ids)))
                ws_kw = _ws_kwargs(kc.ws)
                self.ws = KalshiWS(kc.ws_url, signer, subs, on_raw=self.recorder.write, on_event=self.on_event,
                                   max_markets_per_subscription=cap, **ws_kw)
                self.monitor.kalshi_ws = self.ws
                tasks.append(asyncio.create_task(self.ws.run(), name="kalshi:ws"))
            stopper = asyncio.create_task(self.stop.wait(), name="kalshi:stop")
            tasks.append(stopper)
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for t in done:
                if t is not stopper and not t.cancelled() and t.exception() is not None:
                    raise t.exception()  # type: ignore[misc]
        finally:
            if self.ws is not None:
                await self.ws.stop()
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await self.rest.close()

    def on_event(self, ev: Event) -> None:
        self.monitor.on_kalshi_event(ev)
        if isinstance(ev, KalshiMarketLifecycle) and any(ev.ticker.startswith(s + "-") for s in self.series):
            if ev.ticker in self.tickers or ev.event_type not in ("created", "activated"):
                return
            if ev.event_type == "created" and ev.open_ts > time.time_ns() + 5 * 10**9:
                self.pending_open[ev.ticker] = ev.open_ts  # not open yet: refresh when it opens
            else:
                self._refresh_now.set()

    def pending_due(self, retry_s: float = 10.0, give_up_s: float = 120.0) -> bool:
        """True when a pending market opened >= 2 s ago and is not subscribed yet (retried at
        most every ``retry_s``; given up ``give_up_s`` after its open time)."""
        now = time.time_ns()
        for t, open_ns in list(self.pending_open.items()):
            if t in self.tickers or now > open_ns + int(give_up_s * 1e9):
                del self.pending_open[t]
        opened = any(now >= open_ns + 2 * 10**9 for open_ns in self.pending_open.values())
        return opened and time.monotonic() - self._last_refresh_mono >= retry_s

    async def discover(self) -> list[str]:
        """Open markets of the configured series closing within the horizon."""
        horizon_h = float(self.kcfg.get("market_horizon_h", 0) or 0)
        limit_ns = time.time_ns() + int(horizon_h * 3600e9) if horizon_h > 0 else None
        out: set[str] = set()
        for s in self.series:
            async for m in self.rest.iter_markets(series_ticker=s, status="open"):
                t = m.get("ticker")
                if not t:
                    continue
                if limit_ns is not None and m.get("close_time"):
                    try:
                        if parse_rfc3339_ns(str(m["close_time"])) > limit_ns:
                            continue
                    except ValueError:
                        pass
                out.add(str(t))
                if m.get("event_ticker"):
                    self.events_found.add(str(m["event_ticker"]))
            # Markets that open soon (next KXBTC15M, next hourly KXBTCD/KXBTC event, which opens
            # one hour before it closes): refresh right after they open instead of waiting for
            # the periodic refresh. Markets CLOSING within the look-ahead, any status (one page
            # per series). status=unopened would list every future hourly market (5,640 per
            # series on 2026-09-25, ~35 MB per scan) and cannot be combined with close-time
            # filters (openapi GET /markets compatibility table).
            ahead_s = float(self.kcfg.get("pending_lookahead_h", 3)) * 3600
            now_ns = time.time_ns()
            try:
                async for m in self.rest.iter_markets(series_ticker=s, min_close_ts=now_ns // 10**9,
                                                      max_close_ts=int(now_ns // 10**9 + ahead_s)):
                    t, ot = m.get("ticker"), m.get("open_time")
                    if not t or not ot or str(t) in out:
                        continue
                    try:
                        open_ns = parse_rfc3339_ns(str(ot))
                    except ValueError:
                        continue
                    if open_ns > now_ns:
                        self.pending_open[str(t)] = open_ns
            except Exception as exc:  # noqa: BLE001 - best effort; the periodic refresh still runs
                log.debug("kalshi: upcoming markets of %s unavailable: %s", s, exc)
        return sorted(out)

    async def record_metadata(self) -> None:
        """Record what offline replay needs to rebuild specs and fees (dh.research.replay_env):
        GET /series/{s} per configured series (fee_type, fee_multiplier) -> 'kalshi.rest.series';
        GET /series/fee_changes and /events/fee_changes (scheduled changes) -> 'kalshi.rest.fees';
        GET /events/{e} (event + nested markets: strikes, rules, fee overrides) once per newly
        discovered event -> 'kalshi.rest.events'. Recorded through KalshiRest(on_raw=...);
        failures are logged and never stop the collector. Runs at start-up and every refresh."""
        for s in self.series:
            for what, call in ((f"series {s}", lambda s=s: self.rest.get_series(s)),
                               (f"series fee changes {s}", lambda s=s: self.rest.get_series_fee_changes(s))):
                try:
                    await call()
                except Exception as exc:  # noqa: BLE001 - recorded by KalshiRest; keep going
                    log.warning("kalshi: %s failed: %s", what, exc)
        try:
            async for _ in self.rest.iter_event_fee_changes():
                pass
        except Exception as exc:  # noqa: BLE001
            log.warning("kalshi: event fee changes failed: %s", exc)
        for e in sorted(self.events_found - self.events_recorded):
            try:
                await self.rest.get_event(e, with_nested_markets=True)
                self.events_recorded.add(e)
            except Exception as exc:  # noqa: BLE001
                log.warning("kalshi: GET /events/%s failed: %s", e, exc)

    async def refresh_loop(self) -> None:
        period = float(self.kcfg.get("market_refresh_s", 300))
        min_gap = float(self.kcfg.get("min_refresh_gap_s", 15))  # event-triggered refreshes at most this often
        self._last_refresh_mono = time.monotonic()  # run() discovered just before starting this loop
        while True:
            try:
                await asyncio.wait_for(self._refresh_now.wait(), timeout=2.0)
                gap = self._last_refresh_mono + min_gap - time.monotonic()
                await asyncio.sleep(max(2.0, gap))  # debounce bursts of lifecycle events
            except asyncio.TimeoutError:
                # periodic refresh, or a market announced by a 'created' event has opened
                if time.monotonic() - self._last_refresh_mono < period and not self.pending_due():
                    continue
            self._refresh_now.clear()
            self._last_refresh_mono = time.monotonic()
            try:
                fresh = await self.discover()
            except Exception as exc:  # noqa: BLE001
                log.warning("kalshi: market discovery failed: %s", exc)
                continue
            add = [t for t in fresh if t not in set(self.tickers)]
            gone = [t for t in self.tickers if t not in set(fresh)]
            self.tickers = fresh
            await self.record_metadata()
            if self.ws is not None:
                chans = self.kcfg.get("market_channels") or ["orderbook_delta"]
                if add:
                    await self.ws.add_markets(add, channel=chans[0])
                if gone:
                    await self.ws.delete_markets(gone, channel=chans[0])
            if add or gone:
                log.info("kalshi: markets +%d -%d (now %d)", len(add), len(gone), len(self.tickers))

    async def orderbook_loop(self) -> None:
        period = float(self.kcfg.get("rest_orderbook_interval_s", 60))
        while True:
            tickers = list(self.tickers)
            for i in range(0, len(tickers), 100):
                try:
                    await self.rest.get_orderbooks(tickers[i : i + 100])
                except Exception as exc:  # noqa: BLE001 - recorded by KalshiRest; keep going
                    log.warning("kalshi: REST orderbooks failed: %s", exc)
            await asyncio.sleep(period)

    async def cf_history_loop(self) -> None:
        period = float(self.kcfg.get("cf_history_interval_s", 0))
        while True:
            for idx in self.kcfg.get("index_ids") or ["BRTI"]:
                try:
                    await self.rest.get_cfbenchmarks_history(idx, timespan=self.kcfg.get("cf_history_timespan"))
                except Exception as exc:  # noqa: BLE001
                    log.warning("kalshi: CF history failed: %s", exc)
            await asyncio.sleep(period)


def _ws_kwargs(ws: dict[str, Any]) -> dict[str, Any]:
    kw: dict[str, Any] = {}
    for k in ("stale_after_s", "resync_timeout_s", "backoff_initial_s", "backoff_max_s", "healthy_reset_s"):
        if k in ws:
            kw[k] = float(ws[k]) if ws[k] is not None else None
    if "use_yes_price" in ws:
        kw["use_yes_price"] = bool(ws["use_yes_price"])
    if "ping_interval_s" in ws or "ping_timeout_s" in ws:
        from dh.kalshi.ws import websockets_connect_factory

        kw["connect"] = websockets_connect_factory(ping_interval=ws.get("ping_interval_s", 10), ping_timeout=ws.get("ping_timeout_s", 10))
    return kw


# ============================================================================ main
def acquire_single_instance_lock(root: Path) -> Any:
    """Exclusive, non-blocking flock on ``<root>/recorder.lock`` held for the process lifetime:
    two recorders appending to the same store would interleave segment files (e.g. a manual
    ``nohup`` run and the launchd agent). Returns the open file, or None if another holds it."""
    root.mkdir(parents=True, exist_ok=True)
    f = open(root / "recorder.lock", "a+")  # noqa: SIM115 - must stay open while running
    try:
        fcntl.flock(f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        f.close()
        return None
    f.seek(0)
    f.truncate()
    f.write(f"{os.getpid()}\n")
    f.flush()
    return f


def session_meta(args: argparse.Namespace, cfg: dict[str, Any], disk: dict[str, Any] | None = None) -> bytes:
    try:
        commit = subprocess.run(["git", "-C", str(REPO), "rev-parse", "HEAD"], capture_output=True, text=True, timeout=5).stdout.strip()
    except (OSError, subprocess.SubprocessError):
        commit = ""
    return orjson.dumps({
        "kind": "session_start", "pid": os.getpid(), "host": socket.gethostname(), "argv": sys.argv,
        "python": platform.python_version(), "git_commit": commit, "config": cfg, "disk": disk,
    }, default=str)


def feed_shed_hooks(recorder: Recorder, feeds: dict[str, FeedClient]) -> tuple[Any, Any]:
    """DiskGuard callbacks keeping shed venue streams self-describing for replay.

    before_shed: a 'disconnected' status marker (dh.feeds.base) into each shed stream whose feed
    is connected, so replay of that stream alone invalidates its books at the shed (no delta
    is ever applied across the unrecorded interval). after_unshed: reconnect those feeds, so
    the resumed recording starts with 'connected' + a fresh snapshot."""
    by_stream = {f.name: f for f in feeds.values()}

    def before_shed(streams: tuple[str, ...], info: dict[str, Any]) -> None:
        now = time.time_ns()
        detail = (f"recorder shed stream: low disk ({info['free_gb']:.1f} GB free < "
                  f"min_free_gb_shed {info['min_free_gb_shed']:g} GB)")
        for s in streams:
            f = by_stream.get(s)
            if f is not None and f.metrics.connects > f.metrics.disconnects:
                recorder.write(s, now, make_marker("status", f.conn_id, status="disconnected", detail=detail))

    def after_unshed(streams: tuple[str, ...], info: dict[str, Any]) -> None:
        for s in streams:
            f = by_stream.get(s)
            if f is not None:
                f.request_reconnect("recording resumed after low-disk shed: fresh snapshot")

    return before_shed, after_unshed


async def amain(args: argparse.Namespace) -> int:
    cfg = load_feeds_config(args.config)
    root = Path(args.root or cfg.get("root") or "data")
    if not root.is_absolute() and not args.root:
        root = REPO / root  # config paths are relative to the repository
    rcfg = cfg.get("recorder") or {}
    try:
        guard = DiskGuard.from_config(rcfg, root)
    except (ValueError, TypeError) as exc:
        log.error("invalid recorder disk-guard config: %s", exc)
        return 2
    # Low-disk start-up refusal, before anything is created or written under the root. The
    # filesystem is shared with another system: a full disk must never corrupt this store.
    disk: dict[str, Any] | None = None
    try:
        free_gb, total_gb = guard.measure()
    except OSError as exc:
        log.warning("disk: cannot measure free space of %s (%s): guard retries every %g s", guard.path(), exc,
                    guard.interval_s)
    else:
        disk = guard.info(free_gb, total_gb)
        log.info("disk: %.1f GB free of %.1f GB on %s (%s)", free_gb, total_gb, guard.path(), guard.describe())
        if guard.below_stop(free_gb):
            log.error("disk: only %.1f GB free on %s, below min_free_gb_stop %g GB: refusing to start (exit %d). "
                      "Archive or move data/raw off this disk (docs/RUNBOOK.md section 3), then restart.",
                      free_gb, guard.path(), guard.min_free_gb_stop, EXIT_LOW_DISK)
            return EXIT_LOW_DISK
    lock = acquire_single_instance_lock(root)
    if lock is None:
        log.error("another recorder already holds %s: refusing to start (stop it first)", root / "recorder.lock")
        return 2
    recorder = Recorder(root, flush_interval_s=float(rcfg.get("flush_interval_s", 1.0)),
                        fsync_interval_s=float(rcfg.get("fsync_interval_s", 30.0)), zstd_level=int(rcfg.get("zstd_level", 3)))
    recorder.write("meta", time.time_ns(), session_meta(args, cfg, disk))
    guard.recorder = recorder
    monitor = Monitor(recorder)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:  # pragma: no cover - non-POSIX
            pass

    only = [s for s in args.only.split(",") if s] if args.only else None
    feeds = build_feeds(cfg, only=only, status_cb=monitor.on_feed_status)
    monitor.feeds = feeds
    guard.before_shed, guard.after_unshed = feed_shed_hooks(recorder, feeds)
    exit_code = 0
    # first check before any feed starts: sheds at once when already below min_free_gb_shed
    if guard.check().action == "stop":  # (or fell below min_free_gb_stop since the measure above)
        exit_code = EXIT_LOW_DISK
        stop.set()
    sup = cfg.get("supervisor") or {}
    b0, bmax = float(sup.get("restart_backoff_initial_s", 1)), float(sup.get("restart_backoff_max_s", 60))
    tasks: list[asyncio.Task[Any]] = []
    for name, feed in feeds.items():
        if not feed.implemented:
            log.warning("%s: stub feed skipped", name)
            continue
        log.info("feed %s", feed.describe())
        tasks.append(asyncio.create_task(
            supervise(name, feed.name, (lambda f=feed: f.run(recorder.write)), recorder, monitor, stop, b0, bmax), name=f"feed:{name}"))
    kcfg = cfg.get("kalshi") or {}
    run_kalshi = bool(kcfg.get("enabled", False)) and not args.no_kalshi and (only is None or "kalshi" in only)
    if run_kalshi:
        ks = KalshiSource(kcfg, recorder, monitor, stop)
        tasks.append(asyncio.create_task(supervise("kalshi", "kalshi.ws", ks.run, recorder, monitor, stop, b0, bmax), name="kalshi"))
    sampler = ClockSampler(recorder, interval_s=float((cfg.get("clock") or {}).get("sample_interval_s", 60)))
    tasks.append(asyncio.create_task(sampler.run(), name="clock"))

    async def status_loop() -> None:
        period = float(args.status_interval or cfg.get("status_interval_s", 60))
        while True:
            await asyncio.sleep(period)
            log.info("status\n%s", monitor.status_line())
            if sampler.last:
                log.info("clock src=%s offset_s=%s synced=%s", sampler.last.get("src"), sampler.last.get("offset_s"), sampler.last.get("synced"))

    async def disk_loop() -> None:
        """DiskGuard every disk_check_interval_s, and at once after a disk-full write error."""
        nonlocal exit_code
        last, seen_full = time.monotonic(), recorder.stats.disk_full_errors
        while True:
            await asyncio.sleep(min(1.0, guard.interval_s))
            full = recorder.stats.disk_full_errors
            if time.monotonic() - last < guard.interval_s and full == seen_full:
                continue
            last, seen_full = time.monotonic(), full
            try:
                res = guard.check()
            except Exception:  # noqa: BLE001 - the guard must keep running
                log.exception("disk guard check failed")
                continue
            if res.action == "stop":
                exit_code = EXIT_LOW_DISK
                stop.set()
                return

    tasks.append(asyncio.create_task(status_loop(), name="status"))
    tasks.append(asyncio.create_task(disk_loop(), name="disk"))
    log.info("recording %d feeds%s to %s", len(feeds), " + kalshi" if run_kalshi else "", root / "raw")
    try:
        if args.duration:
            try:
                await asyncio.wait_for(stop.wait(), timeout=args.duration)
            except asyncio.TimeoutError:
                pass
        else:
            await stop.wait()
    finally:
        log.info("shutting down")
        stop.set()
        for f in feeds.values():
            f.stop()
        await asyncio.sleep(0.2)  # let clients write their 'disconnected' markers
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        end: dict[str, Any] = {"kind": "session_end", "pid": os.getpid()}
        if exit_code == EXIT_LOW_DISK:
            end.update(reason="low_disk", exit_code=exit_code, free_gb=recorder.stats.free_gb,
                       min_free_gb_stop=guard.min_free_gb_stop)
        recorder.write("meta", time.time_ns(), orjson.dumps(end))  # the final record, then close
        recorder.close()
        log.info("final\n%s", monitor.status_line())
        if exit_code == EXIT_LOW_DISK:
            log.error("disk: STOPPED: %.1f GB free on %s < min_free_gb_stop %g GB. Store flushed, fsync'ed and "
                      "closed cleanly; exit %d. Archive or move data/raw off this disk (docs/RUNBOOK.md section 3), "
                      "then restart.", recorder.stats.free_gb or 0.0, guard.path(), guard.min_free_gb_stop, EXIT_LOW_DISK)
    return exit_code


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--config", default=str(REPO / "config" / "feeds.yaml"))
    p.add_argument("--root", default=None, help="data root (overrides config 'root')")
    p.add_argument("--only", default="", help="comma list of feed keys/venues/streams to run ('kalshi' for Kalshi)")
    p.add_argument("--no-kalshi", action="store_true")
    p.add_argument("--duration", type=float, default=0.0, help="stop after N seconds (0 = run until signalled)")
    p.add_argument("--status-interval", type=float, default=0.0)
    p.add_argument("--log-level", default="INFO")
    args = p.parse_args(argv)
    logging.basicConfig(level=args.log_level.upper(), format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    return asyncio.run(amain(args))


if __name__ == "__main__":
    sys.exit(main())
