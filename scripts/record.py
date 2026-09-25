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
    reconnects, stale events.

Status recording: venue clients write connection markers into their own streams and the
Kalshi client writes synthetic status frames into 'kalshi.ws', so replay reproduces outages
from those streams. The 'status' stream only carries what nothing else records: supervisor
restarts of crashed tasks and Kalshi start-up failures.

Shutdown (SIGINT/SIGTERM or --duration): stop clients, cancel tasks, flush + fsync + write
segment indexes (Recorder.close()).
"""

from __future__ import annotations

import argparse
import asyncio
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
from dh.feeds.base import FeedClient, parse_rfc3339_ns  # noqa: E402
from dh.feeds.registry import build_feeds, load_feeds_config  # noqa: E402
from dh.store.recorder import ClockSampler, Recorder  # noqa: E402

log = logging.getLogger("record")


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
            rows.append(f"{s:<22} {rate:8.1f}/s age={age:6.1f}s n={count}{extra}")
        rs = self.recorder.stats
        head = f"recorder records={rs.records} MB={rs.bytes / 1e6:.1f} frames={rs.frames} write_errors={rs.write_errors}"
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
        self.series = self.series or list(kc.series)
        signer = kc.signer()
        self.rest = KalshiRest(kc.rest_url, signer, kc.limiter(), on_raw=self.recorder.write, **kc.rest_kwargs())
        tasks: list[asyncio.Task[Any]] = []
        try:
            self.tickers = await self.discover()
            log.info("kalshi: %d open markets in %s", len(self.tickers), ",".join(self.series))
            await self.record_metadata()
            tasks.append(asyncio.create_task(self.refresh_loop(), name="kalshi:refresh"))
            if float(self.kcfg.get("rest_orderbook_interval_s", 60) or 0) > 0:
                tasks.append(asyncio.create_task(self.orderbook_loop(), name="kalshi:orderbooks"))
            if float(self.kcfg.get("cf_history_interval_s", 0) or 0) > 0:
                tasks.append(asyncio.create_task(self.cf_history_loop(), name="kalshi:cf"))
            if signer is None:
                log.error("kalshi: no API credentials (KALSHI_KEY_ID / KALSHI_PRIVATE_KEY_PATH): the WebSocket "
                          "requires authentication, recording REST snapshots only")
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
            if ev.ticker not in self.tickers and ev.event_type in ("created", "activated"):
                self._refresh_now.set()

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
        while True:
            try:
                await asyncio.wait_for(self._refresh_now.wait(), timeout=period)
                await asyncio.sleep(2.0)  # debounce bursts of lifecycle events
            except asyncio.TimeoutError:
                pass
            self._refresh_now.clear()
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
def session_meta(args: argparse.Namespace, cfg: dict[str, Any]) -> bytes:
    try:
        commit = subprocess.run(["git", "-C", str(REPO), "rev-parse", "HEAD"], capture_output=True, text=True, timeout=5).stdout.strip()
    except (OSError, subprocess.SubprocessError):
        commit = ""
    return orjson.dumps({
        "kind": "session_start", "pid": os.getpid(), "host": socket.gethostname(), "argv": sys.argv,
        "python": platform.python_version(), "git_commit": commit, "config": cfg,
    }, default=str)


async def amain(args: argparse.Namespace) -> int:
    cfg = load_feeds_config(args.config)
    root = Path(args.root or cfg.get("root") or "data")
    if not root.is_absolute() and not args.root:
        root = REPO / root  # config paths are relative to the repository
    rcfg = cfg.get("recorder") or {}
    recorder = Recorder(root, flush_interval_s=float(rcfg.get("flush_interval_s", 1.0)),
                        fsync_interval_s=float(rcfg.get("fsync_interval_s", 30.0)), zstd_level=int(rcfg.get("zstd_level", 3)))
    recorder.write("meta", time.time_ns(), session_meta(args, cfg))
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

    tasks.append(asyncio.create_task(status_loop(), name="status"))
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
        recorder.write("meta", time.time_ns(), orjson.dumps({"kind": "session_end", "pid": os.getpid()}))
        recorder.close()
        log.info("final\n%s", monitor.status_line())
    return 0


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
