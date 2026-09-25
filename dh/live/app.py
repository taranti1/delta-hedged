"""Wiring for scripts/run_live.py: configs -> start-up sequence -> LiveRunner.

Start-up sequence (live and paper; docs/RUNBOOK.md explains each step to operators):
  1. load StrategyConfig (config/m1.yaml) + LiveConfig (config/live.yaml) + Kalshi config
     (config/kalshi.yaml: endpoints, key id / private key path from the environment);
     resolve the mode (live needs ``mode: live`` AND --i-understand-this-sends-real-orders)
  2. open the Recorder (raw capture), the JSON log and the metrics registry
  3. REST client with the account's rate limits (GET /account/limits, endpoint costs)
  4. exchange status (live refuses to start unless exchange_active and trading_active)
  5. live only: account positions (events already held are excluded from this session) and a
     clean-slate cancel-all of leftover resting orders
  6. discover open markets of the configured series expiring within the horizon
     (dh.kalshi.metadata + FeeEngine; unresolved/unsupported fee types stay untradable)
  7. back-fill the benchmark history and warm the FairValueModel (dh.live.startup)
  8. construct the MarketMaker (book_includes_own in live mode)
  9. paper: KalshiExchangeSim (policy C); live: KalshiVenue + the exchange order group
 10. hedge venue (disabled in M1)
 11. Kalshi WS subscriptions (+ optional external feeds) -> runner sources
 12. 'meta' session_start record (configs, digests, git SHA, universe, warm-up points), run
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import socket
import time
from collections.abc import Callable
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import orjson

from dh.core.units import NS_PER_MS, QTY_SCALE
from dh.live.config import LiveConfig, load_live_config, resolve_mode
from dh.live.monitor import JsonLog, KillFile, Metrics, git_sha, write_heartbeat
from dh.live.runner import META_STREAM, LiveRunner
from dh.live.startup import (
    backfill_fair_value,
    build_paper_sim,
    discover_universe,
    events_with_positions,
    exchange_status,
    spec_to_dict,
)
from dh.live.venue_hedge import build_hedge_venue
from dh.live.venue_kalshi import KalshiVenue

log = logging.getLogger("dh.live.app")
REPO_ROOT = Path(__file__).resolve().parents[2]


class StartupError(RuntimeError):
    """The runner refuses to start (exit code 2); the message says why."""


@dataclass
class Overrides:
    """Injection points for tests (no network): REST client, WS connect factory, signer..."""

    rest: Any = None
    ws_connect: Any = None
    signer: Any = None
    clock_ns: Callable[[], int] = time.time_ns
    feeds: dict[str, Any] | None = None
    install_signals: bool = True
    recorder: Any = None


def _resolve(path: str) -> Path:
    p = Path(path).expanduser()
    return p if p.is_absolute() else REPO_ROOT / p


def ws_kwargs(ws: dict[str, Any]) -> dict[str, Any]:
    """KalshiWS keyword arguments from config/kalshi.yaml's ``ws`` section."""
    kw: dict[str, Any] = {}
    for k in ("stale_after_s", "resync_timeout_s", "backoff_initial_s", "backoff_max_s", "healthy_reset_s"):
        if k in ws:
            kw[k] = float(ws[k]) if ws[k] is not None else None
    if "use_yes_price" in ws:
        kw["use_yes_price"] = bool(ws["use_yes_price"])
    return kw


def build_subscriptions(mode: str, tickers: list[str], lcfg: LiveConfig) -> list[Any]:
    """Kalshi WS subscriptions: market data for the universe, lifecycle, BRTI 1 Hz + 5 Hz,
    and (live only) the private channels. Paper mode must NOT consume the account's real
    fills/orders: the simulator is its only source of own-order events."""
    from dh.kalshi.ws import Subscription, shard_market_subscriptions

    u = lcfg.universe
    subs = shard_market_subscriptions(list(u.market_channels), list(tickers), int(u.max_markets_per_subscription))
    subs.append(Subscription(["market_lifecycle_v2"]))
    subs.append(Subscription(["cfbenchmarks_value"], index_ids=list(u.index_ids)))
    subs.append(Subscription(["cfbenchmarks_value_5hz"], index_ids=list(u.index_ids)))
    if mode == "live":
        subs.append(Subscription(["fill", "user_orders", "order_group_updates", "market_positions"]))
    return subs


def check_runtime_paths(lcfg: LiveConfig, mode: str) -> None:
    """Kill-file directory must exist, no kill file may be present, the heartbeat must be
    writable (the watchdog depends on it)."""
    kill = _resolve(lcfg.paths.kill_file)
    if not kill.parent.is_dir():
        raise StartupError(f"kill-file directory {kill.parent} does not exist: "
                           f"sudo mkdir -p {kill.parent} && sudo chown $USER {kill.parent}")
    if kill.exists():
        raise StartupError(f"kill file {kill} is present: investigate, then remove it to start")
    hb = _resolve(lcfg.paths.heartbeat_file)
    try:
        write_heartbeat(hb, {"pid": os.getpid(), "mode": mode, "state": "starting"})
    except OSError as exc:
        raise StartupError(f"heartbeat file {hb} is not writable ({exc})") from exc


class LiveApp:
    """Builds and runs one session. ``build()`` performs the start-up sequence."""

    def __init__(self, scfg: Any, lcfg: LiveConfig, mode: str, overrides: Overrides | None = None) -> None:
        self.scfg = scfg
        self.lcfg = lcfg
        self.mode = mode
        self.ov = overrides or Overrides()
        self.clock = self.ov.clock_ns
        self.session_id = f"{mode}-{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime(self.clock() // 1_000_000_000))}-{os.getpid()}"
        self.recorder: Any = None
        self.jsonlog: JsonLog | None = None
        self.rest: Any = None
        self.ws: Any = None
        self.runner: LiveRunner | None = None
        self.metrics = Metrics()
        self.info: dict[str, Any] = {}

    # ------------------------------------------------------------------ helpers
    def _log(self, kind: str, /, **payload: Any) -> None:
        if self.jsonlog is not None:
            self.jsonlog.write(kind, self.clock(), **payload)

    def _meta(self, kind: str, /, **payload: Any) -> None:
        if self.recorder is not None:
            ts = self.clock()
            self.recorder.write(META_STREAM, ts, orjson.dumps({"kind": kind, "t": ts, "session": self.session_id, **payload},
                                                              default=str))

    # ------------------------------------------------------------------ build
    async def build(self) -> LiveRunner:
        from dh.kalshi.config import load_config as load_kalshi_config
        from dh.kalshi.rest import KalshiRest
        from dh.kalshi.ws import KalshiWS, websockets_connect_factory
        from dh.models.fvmodel import FairValueModel, load_recommended_config
        from dh.store.recorder import Recorder
        from dh.strategy.mm import MarketMaker

        scfg, lcfg, mode = self.scfg, self.lcfg, self.mode
        kc = load_kalshi_config(_resolve(lcfg.kalshi_config) if lcfg.kalshi_config else None, env=lcfg.kalshi_env or None)
        signer = self.ov.signer if self.ov.signer is not None else kc.signer()
        if signer is None and self.ov.ws_connect is None:
            raise StartupError("no Kalshi API credentials (KALSHI_KEY_ID / KALSHI_PRIVATE_KEY_PATH): the WebSocket "
                               "requires authentication even for market data")
        paths = lcfg.paths
        check_runtime_paths(lcfg, mode)
        self.recorder = self.ov.recorder if self.ov.recorder is not None else Recorder(_resolve(paths.data_root))
        sha = git_sha(REPO_ROOT)
        digest = f"{scfg.digest()}/{lcfg.digest()}"
        log_dir = _resolve(paths.log_dir)
        self.jsonlog = JsonLog(log_dir / f"{self.session_id}.jsonl", digest, sha)
        self._log("session_start", mode=mode, session=self.session_id, git_sha=sha, strategy_digest=scfg.digest(),
                  live_digest=lcfg.digest(), host=socket.gethostname(), pid=os.getpid())
        log.info("session %s mode=%s strategy=%s live=%s sha=%s", self.session_id, mode, scfg.digest(), lcfg.digest(), sha)

        # 3. REST
        self.rest = self.ov.rest or KalshiRest(kc.rest_url, signer, kc.limiter(), on_raw=self.recorder.write,
                                               clock_ns=self.clock, **kc.rest_kwargs())
        try:
            limits = await self.rest.configure_rate_limits()
            self.info["rate_limits"] = limits
        except Exception as exc:  # noqa: BLE001 - conservative defaults stay in force
            log.warning("could not load account rate limits (%s): conservative defaults in force", exc)
            self.info["rate_limits"] = {"error": str(exc)[:200]}

        # 4. exchange status
        try:
            status = await exchange_status(self.rest)
        except Exception as exc:  # noqa: BLE001
            status = {"exchange_active": False, "trading_active": False, "error": f"{type(exc).__name__}: {exc}"[:200]}
        self.info["exchange_status"] = status
        if not (status["exchange_active"] and status["trading_active"]):
            msg = f"exchange not trading: {status}"
            if mode == "live":
                raise StartupError(msg)
            log.warning("%s (paper mode continues: no fills while trading is paused)", msg)

        fee_engine = kc.fee_engine()
        venue: KalshiVenue | None = None
        excluded: set[str] = set()
        if mode == "live":
            venue = KalshiVenue(self.rest, sink=lambda ev: None, cfg=lcfg.venue, clock_ns=self.clock)
            # 5. positions already held + clean slate
            positions = await venue.fetch_positions()
            if lcfg.venue.exclude_events_with_positions:
                excluded = events_with_positions(positions)
            self.info["startup_positions"] = positions
            if positions:
                log.warning("account holds positions at start-up: %s (events excluded: %s)", positions, sorted(excluded))
            if lcfg.venue.startup_cancel_all:
                if not await venue.cancel_all_now("startup"):
                    raise StartupError("start-up cancel-all failed: not trading with unknown resting orders")

        # 6. universe
        series = tuple(lcfg.universe.series) or tuple(scfg.quoting.enabled_series)
        now = self.clock()
        sel = await discover_universe(self.rest, series, fee_engine, now, lcfg.universe.horizon_s, exclude_events=excluded)
        for t, why in sorted(sel.skipped.items()):
            log.info("market %s: %s", t, why)
        if not sel.specs:
            raise StartupError(f"no tradable markets in {series} within {lcfg.universe.horizon_s:.0f}s "
                               f"(skipped: {len(sel.skipped)})")
        specs = sel.specs
        log.info("universe: %d markets in %d events", len(specs), len({s.event_ticker for s in specs}))

        # 7. fair-value warm-up
        fv = FairValueModel.from_config(load_recommended_config())
        bf = await backfill_fair_value(self.rest, fv, self.clock(), lcfg.backfill, clock_ns=self.clock)
        if bf.ready:
            log.info("fair-value model warm: %d points from %s (coverage %.1f%%)", len(bf.points), bf.source, 100 * bf.coverage)
        else:
            log.error("fair-value model NOT ready (%s): the strategy will not quote until >= 1 day of live BRTI ticks "
                      "has been observed; errors: %s", bf.source, bf.errors[:3])

        # 8. strategy
        mm = MarketMaker(scfg, specs, fv_model=fv, fee_engine=fee_engine, book_includes_own=(mode == "live"))

        # 9. execution
        sim = paper_fees = None
        if mode == "paper":
            sim, paper_fees = build_paper_sim(lcfg.paper, specs, fee_engine)
        if scfg.hedge.enabled:
            if mode == "live":
                raise StartupError("strategy config enables hedging but no hedge adapter exists (M1: hedge.enabled=false)")
            log.warning("hedge.enabled=true but no hedge adapter exists: every PlaceHedge is rejected (paper)")
        hedge = build_hedge_venue(False, scfg.hedge.venue, sink=lambda ev: None, clock_ns=self.clock)

        async def discover(known: dict[str, Any]) -> Any:
            return await discover_universe(self.rest, series, fee_engine, self.clock(), lcfg.universe.horizon_s,
                                           exclude_events=excluded, known=known)

        runner = LiveRunner(
            mm, mode=mode, period_ns=int(scfg.timers.quote_period_ms) * NS_PER_MS, cfg=lcfg, venue=venue, sim=sim,
            paper_fees=paper_fees, hedge=hedge, recorder=self.recorder, jsonlog=self.jsonlog, metrics=self.metrics,
            clock_ns=self.clock, kill_file=KillFile(_resolve(paths.kill_file)), heartbeat_path=_resolve(paths.heartbeat_file),
            fee_engine=fee_engine, universe=specs, discover=discover, series=series, session_id=self.session_id)
        hedge.sink = runner.push_result
        if venue is not None:
            venue.sink = runner.push_result
            venue.log_fn = lambda k, p: runner.jlog("venue." + k, self.clock(), **p)
            venue.observe_rtt = lambda op, dt, outcome: self.metrics.observe("dh_rest_rtt_seconds", dt, op=op, outcome=outcome)
            venue.on_group_map = lambda logical, gid: runner.meta("order_group_map", self.clock(), logical=logical, id=gid)
            limit = round(scfg.risk.order_group_limit_contracts * QTY_SCALE)
            gid = await venue.ensure_order_group(MarketMaker.ORDER_GROUP_ID, limit)
            if gid is None:
                raise StartupError("could not create the exchange order group (fill-burst breaker): refusing to trade")
            self.info["order_group"] = {"logical": MarketMaker.ORDER_GROUP_ID, "id": gid, "limit": limit}

        # 11. market data
        subs = build_subscriptions(mode, [s.ticker for s in specs], lcfg)
        kw = ws_kwargs(kc.ws)
        connect = self.ov.ws_connect or websockets_connect_factory(ping_interval=kc.ws.get("ping_interval_s", 10),
                                                                   ping_timeout=kc.ws.get("ping_timeout_s", 10))
        self.ws = KalshiWS(kc.ws_url, signer, subs, on_raw=self.recorder.write, on_event=runner.push,
                           connect=connect, clock_ns=self.clock,
                           max_markets_per_subscription=int(lcfg.universe.max_markets_per_subscription), **kw)
        ws = self.ws
        runner.add_source("kalshi.ws", ws.run, stop=ws.stop)
        runner.subscribe_markets = lambda tickers: ws.add_markets(tickers, channel=lcfg.universe.market_channels[0])
        runner.unsubscribe_markets = lambda tickers: ws.delete_markets(tickers, channel=lcfg.universe.market_channels[0])
        self._add_feeds(runner)

        # 12. session record
        self._meta("session_start", mode=mode, git_sha=sha, strategy_config=asdict(scfg), strategy_digest=scfg.digest(),
                   live_config=lcfg.as_dict(), live_digest=lcfg.digest(), specs=[spec_to_dict(s) for s in specs],
                   skipped=sel.skipped, excluded_events=sorted(excluded), backfill=bf.summary(),
                   paper=asdict(lcfg.paper) if mode == "paper" else None, info=self.info)
        self._meta("fv_warmup", source=bf.source, points=bf.points)
        self._log("startup", universe=[s.ticker for s in specs], skipped=sel.skipped, backfill=bf.summary(), info=self.info)
        self.runner = runner
        return runner

    def _add_feeds(self, runner: LiveRunner) -> None:
        only = list(self.lcfg.feeds.only)
        if not only and self.ov.feeds is None:
            return
        feeds = self.ov.feeds
        if feeds is None:
            from dh.feeds.registry import build_feeds, load_feeds_config

            feeds = build_feeds(load_feeds_config(_resolve(self.lcfg.feeds.config)), only=only, on_event=runner.push)
        for name, feed in feeds.items():
            if not getattr(feed, "implemented", True):
                log.warning("feed %s is a stub: skipped", name)
                continue
            runner.add_source(name, (lambda f=feed: f.run(self.recorder.write)), stop=feed.stop)

    # ------------------------------------------------------------------ run
    async def run(self, duration_s: float | None = None) -> int:
        try:
            runner = await self.build()
        except StartupError as exc:
            log.error("refusing to start: %s", exc)
            self._log("startup_refused", error=str(exc))
            await self.close()
            return 2
        except Exception as exc:  # noqa: BLE001 - network / auth / parsing: never half-start
            log.exception("start-up failed")
            self._log("startup_failed", error=f"{type(exc).__name__}: {exc}"[:500])
            await self.close()
            return 2
        if self.ov.install_signals:
            loop = asyncio.get_running_loop()
            for sig in (signal.SIGINT, signal.SIGTERM):
                try:
                    loop.add_signal_handler(sig, runner.request_stop, f"signal {sig.name}")
                except (NotImplementedError, RuntimeError):  # pragma: no cover - non-POSIX
                    pass
        try:
            code = await runner.run(duration_s=duration_s)
        finally:
            await self.close()
        return code

    async def close(self) -> None:
        if self.rest is not None and self.ov.rest is None:
            try:
                await self.rest.close()
            except Exception:  # noqa: BLE001
                pass
        if self.recorder is not None and self.ov.recorder is None:
            self.recorder.close()
        if self.jsonlog is not None:
            self.jsonlog.close()


async def run_from_paths(strategy_config: str | None, live_config: str | None, *, cli_mode: str | None,
                         confirmed: bool, duration_s: float | None = None, overrides: Overrides | None = None) -> int:
    """Entry point used by scripts/run_live.py (mode checks happen before anything connects)."""
    from dh.strategy.config import load_config

    lcfg = load_live_config(_resolve(live_config) if live_config else None)
    mode = resolve_mode(cli_mode, lcfg.mode, confirmed)
    scfg = load_config(_resolve(strategy_config) if strategy_config else None)
    if mode == "live" and scfg.hedge.enabled:
        raise StartupError("strategy config enables hedging but no hedge adapter exists (M1: hedge.enabled=false)")
    app = LiveApp(scfg, lcfg, mode, overrides)
    return await app.run(duration_s)
