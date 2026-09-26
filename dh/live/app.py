"""Wiring for scripts/run_live.py: configs -> start-up sequence -> LiveRunner.

Start-up sequence (live and paper; docs/RUNBOOK.md explains each step to operators):
  1. load StrategyConfig (config/m1.yaml) + LiveConfig (config/live.yaml) + Kalshi config
     (config/kalshi.yaml: endpoints, key id / private key path from the environment);
     resolve the mode (live needs ``mode: live`` AND --i-understand-this-sends-real-orders);
     live refuses paper-only settings (dh.live.config.live_config_problems)
  2. single instance: flock on <data_root>/runner.lock and <heartbeat>.lock, refuse if the
     mode's heartbeat file is fresh from another process; kill file absent; heartbeat
     'starting' (paper and live use different heartbeat files)
  3. open the Recorder (raw capture), the JSON log and the metrics registry; the session's
     client_order_id prefix <run_prefix>-<token> (never repeats across restarts)
  4. REST client with the account's rate limits (GET /account/limits, endpoint costs); live:
     every write must name venue.subaccount (and a shard) or it is refused before sending;
     paper: read-only. Shared account: rate_limits.account_share must be <= 0.5
  5. exchange status of the configured shards (live refuses to start unless exchange_active
     and trading_active there); the exchange schedule's closures (logged, handed to the runner)
  6. live only: collateral: GET /portfolio/balance?subaccount=<n>&exchange_index=<shard> must
     cover the worst-case total loss + margin on every configured shard (proves the subaccount
     exists and is funded on that shard); a key declared restricted to the subaccount is
     verified (GET /api_keys, else the balance breakdown). A watchdog cancel-all marker left
     from before this start is renamed (the runner halts only on markers written after it
     started); clean-slate cancel-all of leftover resting orders, VERIFIED with the
     resting-order list, THEN account positions (events already held are excluded from this
     session; positions outside the series are reported and ignored; a malformed position row
     refuses the start); new orders are held for venue.cancel_all_hold_s after that cancel-all
  7. risk state: today's P&L re-derived from Kalshi (live: the historical cutoff, today's
     fills and settlements, the positions valued at exchange prices; any malformed row or
     failed read refuses the start) and the persisted state -> RiskStateSeed
     (dh.live.riskstate), the first event; recomputed if the UTC day changed meanwhile
  8. discover open markets of the configured series expiring within the horizon, on a known
     exchange shard listed in venue.exchange_indexes (dh.kalshi.metadata + FeeEngine;
     unresolved/unsupported fee types stay untradable)
  9. back-fill the benchmark history and warm the FairValueModel (dh.live.startup)
 10. construct the MarketMaker (book_includes_own in live mode, the session id prefix)
 11. paper: KalshiExchangeSim (policy C); live: KalshiVenue + one exchange order group per
     shard in use (explicit subaccount and exchange_index)
 12. hedge venue (disabled in M1)
 13. Kalshi WS subscriptions (+ optional external feeds) -> runner sources
 14. 'meta' session_start record (configs, digests, git SHA, universe, warm-up points,
     id prefix, risk seed), run
"""

from __future__ import annotations

import asyncio
import fcntl
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

from dh.core.units import NS_PER_MS, NS_PER_S, QTY_SCALE
from dh.live.clock import AnchoredClock, session_token
from dh.live.config import LiveConfig, account_share_problem, live_config_problems, load_live_config, resolve_mode
from dh.live.monitor import (
    JsonLog,
    KillFile,
    Metrics,
    cancel_all_marker_path,
    git_sha,
    read_heartbeat,
    read_watchdog_beat,
    watchdog_beat_path,
    watchdog_beat_problem,
    write_heartbeat,
)
from dh.live.riskstate import (
    RiskBook,
    RiskStateError,
    RiskStateStore,
    day_start,
    decide_seed,
    derive_day_pnl,
    make_seed,
    state_from_decision,
)
from dh.live.runner import META_STREAM, LiveRunner
from dh.live.startup import (
    backfill_fair_value,
    build_paper_sim,
    discover_universe,
    events_with_positions,
    exchange_status,
    required_balance_usd,
    schedule_closures,
    series_of_ticker,
    spec_to_dict,
    verify_key_restriction,
)
from dh.live.venue_hedge import build_hedge_venue
from dh.live.venue_kalshi import KalshiVenue

log = logging.getLogger("dh.live.app")
REPO_ROOT = Path(__file__).resolve().parents[2]


class StartupError(RuntimeError):
    """The runner refuses to start (exit code 2); the message says why."""




@dataclass
class Overrides:
    """Injection points for tests (no network): REST client, WS connect factory, signer...
    ``clock_ns`` None = a fresh AnchoredClock (monotonic, strictly increasing)."""

    rest: Any = None
    ws_connect: Any = None
    signer: Any = None
    clock_ns: Callable[[], int] | None = None
    feeds: dict[str, Any] | None = None
    install_signals: bool = True
    recorder: Any = None
    session_token: str | None = None
    clock_sampler: Callable[[], dict[str, Any]] | None = None  # None = dh.store.recorder.sample_clock


def _resolve(path: str) -> Path:
    p = Path(path).expanduser()
    return p if p.is_absolute() else REPO_ROOT / p


def quarantine_stale_marker(marker: Path, now_ns: int) -> Path | None:
    """A watchdog cancel-all marker present before this runner starts is about an earlier
    runner: rename it (kept for the post-mortem, logged) so it can never halt this one."""
    if not marker.exists():
        return None
    try:
        content = marker.read_text()[:300]
    except OSError:
        content = "?"
    dest = marker.with_name(f"{marker.name}.stale-{now_ns // NS_PER_S}")
    try:
        os.replace(marker, dest)
    except OSError as exc:
        raise StartupError(f"cannot move the old watchdog marker {marker} aside ({exc})") from exc
    log.warning("watchdog cancel-all marker from before this start (%s) moved to %s", content.strip(), dest)
    return dest


def data_root_free_gb(root: str | Path) -> float:
    """Free space (decimal GB, available to this user) of the filesystem holding the session
    store ``root`` (``<root>/raw`` when it exists: it may be a symlink to another volume)."""
    import shutil

    from dh.store.recorder import disk_usage_path

    return shutil.disk_usage(disk_usage_path(root)).free / 1e9


def watchdog_reader_for(path: Path, subaccount: int, clock: Callable[[], int]) -> Callable[[], dict[str, Any] | None]:
    """Reader of the watchdog's beat file ``<heartbeat>.watchdog`` (``subaccount`` / ``clock``
    are for test stand-ins; the file names its own subaccount and time)."""
    return lambda: read_watchdog_beat(path)


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


class WsFillProbe:
    """Raw-frame hook of the live Kalshi WebSocket (records every frame, like the recorder's
    ``write``) that settles an open question on the FIRST own fill: does the ``fill`` message
    carry ``subaccount`` (asyncapi: optional)? One ``verify_live`` line + metric
    (``dh_verify_live{check="ws_fill_subaccount_field"}``): as expected when the field names
    our subaccount, or, with a subaccount-restricted key, when it is absent (the server scopes
    the channel; the runner then attributes the message to us)."""

    def __init__(self, write: Callable[[str, int, Any], None], runner: Any, subaccount: int, key_restricted: bool,
                 id_prefix: str = "") -> None:
        self.write = write
        self.runner = runner
        self.sub = int(subaccount)
        self.restricted = bool(key_restricted)
        self.id_prefix = id_prefix  # with a full-account key only fills of our own orders count
        self.done = False

    def __call__(self, stream: str, ts: int, data: Any) -> None:
        self.write(stream, ts, data)
        if self.done:
            return
        if (b'"fill"' not in data) if isinstance(data, (bytes, bytearray)) else ('"fill"' not in str(data)):
            return
        try:
            frame = orjson.loads(data)
        except (orjson.JSONDecodeError, TypeError):
            return
        if not isinstance(frame, dict) or frame.get("type") != "fill" or not isinstance(frame.get("msg"), dict):
            return
        msg = frame["msg"]
        coid = str(msg.get("client_order_id") or "")
        if not self.restricted and not (self.id_prefix and coid.startswith(self.id_prefix + "-")):
            return  # a full-account key also delivers other subaccounts' fills: only ours settle it
        self.done = True
        present = msg.get("subaccount") not in (None, "")
        value = msg.get("subaccount")
        try:
            ours = present and int(value) == self.sub
        except (TypeError, ValueError):
            ours = False
        ok = ours or (not present and (self.restricted or self.sub == 0))
        self.runner.verify_live("ws_fill_subaccount_field", ok, present=present, value=value, subaccount=self.sub,
                                key_restricted=self.restricted, exchange_index=msg.get("exchange_index"),
                                ticker=msg.get("market_ticker"))
        # review NEW-1: fillPayload.client_order_id is optional; without it a fill that beats our
        # create's response is parked until the order id is known (logged once, informational)
        self.runner.verify_live("ws_fill_client_order_id", bool(coid), present=bool(coid),
                                order_id=msg.get("order_id"), ticker=msg.get("market_ticker"))


class InstanceLock:
    """Exclusive, non-blocking flock held for the life of the process (released on exit,
    even on a crash)."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self._fd: int | None = None

    def acquire(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(self.path, os.O_RDWR | os.O_CREAT, 0o644)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            try:
                holder = os.pread(fd, 64, 0).decode(errors="replace").strip()
            finally:
                os.close(fd)
            raise StartupError(f"another runner holds {self.path} ({holder or 'unknown pid'}): one runner per "
                               "data_root / heartbeat file") from exc
        os.ftruncate(fd, 0)
        os.pwrite(fd, f"pid {os.getpid()}".encode(), 0)
        self._fd = fd

    def release(self) -> None:
        if self._fd is not None:
            try:
                fcntl.flock(self._fd, fcntl.LOCK_UN)
            finally:
                os.close(self._fd)
                self._fd = None


def heartbeat_path(lcfg: LiveConfig, mode: str) -> Path:
    return _resolve(lcfg.paths.heartbeat_for(mode))


def check_runtime_paths(lcfg: LiveConfig, mode: str, *, now_ns: int | None = None) -> list[InstanceLock]:
    """Kill-file directory must exist, no kill file may be present; single instance (flock on
    <data_root>/runner.lock and <heartbeat>.lock; a heartbeat of this mode that is fresh and
    from another pid refuses the start); the heartbeat must be writable (the watchdog
    depends on it). Returns the held locks (release them on exit)."""
    kill = _resolve(lcfg.paths.kill_file)
    if not kill.parent.is_dir() and not Path(lcfg.paths.kill_file).expanduser().is_absolute():
        # a repository-relative runtime directory (the macOS default data/run): create it
        kill.parent.mkdir(parents=True, exist_ok=True)
        log.info("created the runtime directory %s (kill file, heartbeat, locks)", kill.parent)
    if not kill.parent.is_dir():
        raise StartupError(f"kill-file directory {kill.parent} does not exist: "
                           f"sudo mkdir -p {kill.parent} && sudo chown $USER {kill.parent}")
    if kill.exists():
        raise StartupError(f"kill file {kill} is present: investigate, then remove it to start")
    hb = heartbeat_path(lcfg, mode)
    locks = [InstanceLock(_resolve(lcfg.paths.data_root) / "runner.lock"), InstanceLock(hb.with_name(hb.name + ".lock"))]
    held: list[InstanceLock] = []
    try:
        for lk in locks:
            lk.acquire()
            held.append(lk)
        prev = read_heartbeat(hb)
        now = time.time_ns() if now_ns is None else now_ns
        fresh_s = max(5.0, 2 * lcfg.watchdog.stale_s)
        if (prev is not None and not prev.get("unparsed") and prev.get("pid") not in (None, os.getpid())
                and str(prev.get("state", "")) in ("starting", "running", "stopping")
                and now - int(prev.get("t", 0)) < fresh_s * NS_PER_S):
            raise StartupError(f"heartbeat {hb} is fresh from pid {prev.get('pid')} (state {prev.get('state')}): "
                               f"another runner is alive; stop it (or wait {fresh_s:.0f}s after it died)")
        try:
            write_heartbeat(hb, {"pid": os.getpid(), "mode": mode, "state": "starting"})
        except OSError as exc:
            raise StartupError(f"heartbeat file {hb} is not writable ({exc})") from exc
    except BaseException:
        for lk in held:
            lk.release()
        raise
    return held


class LiveApp:
    """Builds and runs one session. ``build()`` performs the start-up sequence."""

    def __init__(self, scfg: Any, lcfg: LiveConfig, mode: str, overrides: Overrides | None = None, *,
                 reset_daily_halt: bool = False) -> None:
        self.scfg = scfg
        self.lcfg = lcfg
        self.mode = mode
        self.ov = overrides or Overrides()
        self.clock: Callable[[], int] = self.ov.clock_ns or AnchoredClock()
        self.reset_daily_halt = reset_daily_halt
        now = self.clock()
        self.token = self.ov.session_token or session_token(now)
        self.id_prefix = f"{scfg.run_prefix}-{self.token}"
        # every client_order_id of this system starts with this (live: the own-activity filter, review M1)
        self.own_id_prefix = f"{scfg.run_prefix}-"
        self.session_id = f"{mode}-{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime(now // 1_000_000_000))}-{self.token}"
        self.recorder: Any = None
        self.jsonlog: JsonLog | None = None
        self.rest: Any = None
        self.ws: Any = None
        self.venue: Any = None
        self.runner: LiveRunner | None = None
        self.metrics = Metrics()
        self.info: dict[str, Any] = {}
        self.locks: list[InstanceLock] = []

    # ------------------------------------------------------------------ helpers
    def _log(self, kind: str, /, **payload: Any) -> None:
        if self.jsonlog is not None:
            self.jsonlog.write(kind, self.clock(), **payload)

    def _meta(self, kind: str, /, *, at: int | None = None, **payload: Any) -> None:
        if self.recorder is not None:
            ts = self.clock() if at is None else at
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
        t_start = self.clock()  # the session's replay window starts here (before the risk seed)
        if mode == "live":
            problems = live_config_problems(lcfg)
            if problems:
                raise StartupError("; ".join(problems))
        kc = load_kalshi_config(_resolve(lcfg.kalshi_config) if lcfg.kalshi_config else None, env=lcfg.kalshi_env or None)
        if mode == "live":
            share = account_share_problem(lcfg, kc.account_share)
            if share:
                raise StartupError(share)
        signer = self.ov.signer if self.ov.signer is not None else kc.signer()
        if signer is None and self.ov.ws_connect is None:
            raise StartupError(f"no Kalshi API credentials ({kc.credentials_hint()}): the WebSocket "
                               "requires authentication even for market data")
        paths = lcfg.paths
        self.locks = check_runtime_paths(lcfg, mode, now_ns=self.clock())
        hb = heartbeat_path(lcfg, mode)
        watchdog_reader = None
        disk_free = None
        if mode == "live":
            # before anything is sent: enough disk for the session store, and a LIVE watchdog for
            # this subaccount (review M3: never trade with the dead-man switch down)
            root = _resolve(paths.data_root)
            disk_free = lambda: data_root_free_gb(root)  # noqa: E731
            try:
                free = disk_free()
            except OSError as exc:
                raise StartupError(f"cannot measure the free disk space of {root} ({exc})") from exc
            self.info["disk_free_gb"] = round(free, 3)
            if free < lcfg.disk.min_free_gb_start:
                raise StartupError(f"only {free:.1f} GB free on the disk of {root} (< disk.min_free_gb_start "
                                   f"{lcfg.disk.min_free_gb_start:g} GB): free space first (RUNBOOK 3 'Low disk')")
            wd_path = watchdog_beat_path(hb)
            watchdog_reader = watchdog_reader_for(wd_path, lcfg.venue.sub, self.clock)
            problem = watchdog_beat_problem(watchdog_reader(), now_ns=self.clock(), subaccount=lcfg.venue.sub,
                                            max_age_s=lcfg.watchdog.runner_max_age_s,
                                            api_max_age_s=lcfg.watchdog.api_max_age_s,
                                            max_future_s=lcfg.watchdog.max_future_s)
            if problem:
                raise StartupError(f"{problem} ({wd_path}): start the watchdog for subaccount {lcfg.venue.sub} first "
                                   "(RUNBOOK 5.2 step 2; macOS: the launchd agent, deploy/launchd/README.md)")
            self.info["watchdog"] = {"beat": str(wd_path), "ok": True}
        marker = cancel_all_marker_path(hb) if mode == "live" else None
        if marker is not None:
            moved = quarantine_stale_marker(marker, self.clock())
            if moved is not None:
                self.info["stale_watchdog_marker"] = str(moved)
        started_ns = self.clock()  # the runner honours only watchdog markers written after this
        self.recorder = self.ov.recorder if self.ov.recorder is not None else Recorder(_resolve(paths.data_root))
        sha = git_sha(REPO_ROOT)
        digest = f"{scfg.digest()}/{lcfg.digest()}"
        log_dir = _resolve(paths.log_dir)
        self.jsonlog = JsonLog(log_dir / f"{self.session_id}.jsonl", digest, sha)
        self._log("session_start", mode=mode, session=self.session_id, git_sha=sha, strategy_digest=scfg.digest(),
                  live_digest=lcfg.digest(), host=socket.gethostname(), pid=os.getpid(), id_prefix=self.id_prefix)
        log.info("session %s mode=%s strategy=%s live=%s sha=%s ids=%s-*", self.session_id, mode, scfg.digest(),
                 lcfg.digest(), sha, self.id_prefix)
        store = RiskStateStore(_resolve(paths.risk_state_for(mode)))
        try:
            prev_state = store.load()
        except RiskStateError as exc:
            raise StartupError(str(exc)) from exc

        # 3. REST
        # paper can never send a write (read_only); live refuses, before signing, any write that
        # does not name this runner's subaccount and one of its shards explicitly (write_subaccount,
        # write_shards), and on a shared account the bulk cancel-all (forbid_bulk_cancel)
        live = mode == "live"
        self.rest = self.ov.rest or KalshiRest(kc.rest_url, signer, kc.limiter(), on_raw=self.recorder.write,
                                               clock_ns=self.clock, read_only=not live,
                                               write_subaccount=lcfg.venue.sub if live else None,
                                               write_shards=tuple(lcfg.venue.exchange_indexes) if live else None,
                                               forbid_bulk_cancel=live and not lcfg.venue.bulk_cancel_allowed,
                                               **kc.rest_kwargs())
        try:
            limits = await self.rest.configure_rate_limits()
            self.info["rate_limits"] = limits
        except Exception as exc:  # noqa: BLE001 - conservative defaults stay in force
            log.warning("could not load account rate limits (%s): conservative defaults in force", exc)
            self.info["rate_limits"] = {"error": str(exc)[:200]}

        # 4. exchange status of the shards this runner trades, and the schedule's closures
        shards = tuple(lcfg.venue.exchange_indexes)
        try:
            status = await exchange_status(self.rest, shards)
        except Exception as exc:  # noqa: BLE001
            status = {"exchange_active": False, "trading_active": False, "error": f"{type(exc).__name__}: {exc}"[:200]}
        self.info["exchange_status"] = status
        if not (status["exchange_active"] and status["trading_active"]):
            msg = f"exchange not trading on shard(s) {list(shards)}: {status}"
            if mode == "live":
                raise StartupError(msg)
            log.warning("%s (paper mode continues: no fills while trading is paused)", msg)
        closures: list[tuple[int, int, str]] = []
        if mode == "live":
            try:
                now = self.clock()
                closures, notes = schedule_closures(await self.rest.get_exchange_schedule(), now - 86_400 * NS_PER_S,
                                                    now + 8 * 86_400 * NS_PER_S, now_ns=now)
                self.info["exchange_schedule"] = {"closures": closures[:20], "notes": notes}
                nxt = next((c for c in closures if c[1] > now), None)
                log.info("exchange schedule: %d closure(s) in the next week; next %s", len(closures), nxt)
            except Exception as exc:  # noqa: BLE001 - the status poll still catches every pause
                log.warning("GET /exchange/schedule failed (%s): pauses are caught by the status poll only", exc)
                self.info["exchange_schedule"] = {"error": f"{type(exc).__name__}: {exc}"[:200]}

        series = tuple(lcfg.universe.series) or tuple(scfg.quoting.enabled_series)
        balance_need = required_balance_usd(scfg.risk, lcfg.venue.min_balance_margin_dollars)
        balances: dict[int, float | None] = {}  # shard -> funds (available + positions at cost + resting)
        available: dict[int, float | None] = {}

        fee_engine = kc.fee_engine()
        venue: KalshiVenue | None = None
        excluded: set[str] = set()
        positions: dict[str, int] = {}
        rest_pnl = None
        hold_until = 0
        if mode == "live":
            try:
                venue = self.venue = KalshiVenue(self.rest, sink=lambda ev: None, cfg=lcfg.venue, clock_ns=self.clock)
            except ValueError as exc:
                raise StartupError(str(exc)) from exc
            # 5b. collateral on every traded shard (read-only; before any write): proves the
            # subaccount exists and is funded where its orders' collateral is checked. Funds =
            # available balance + open positions at cost + resting orders' collateral (leftovers
            # of a crashed session or held positions do not make a funded shard look empty)
            try:
                funds = await venue.fetch_shard_funds()
            except Exception as exc:  # noqa: BLE001
                raise StartupError(f"GET /portfolio/balance?subaccount={venue.sub} failed ({type(exc).__name__}: "
                                   f"{exc}): the subaccount must exist and be funded on shard(s) {list(shards)}") from exc
            bodies = {sh: f["body"] for sh, f in funds.items()}
            balances = {sh: f["funds"] for sh, f in funds.items()}
            available = {sh: f["available"] for sh, f in funds.items()}
            self.info["balances"] = {"shards": {str(sh): {k: f[k] for k in ("available", "positions", "resting", "funds")}
                                                for sh, f in funds.items()}, "required_usd": balance_need}
            low = {sh: usd for sh, usd in balances.items() if usd is None or usd < balance_need}
            if low:
                raise StartupError(f"funds of subaccount {venue.sub} on shard(s) {low} do not cover the worst-case "
                                   f"loss + margin ${balance_need:.2f} (fund the subaccount on that shard: RUNBOOK 1.2)")
            log.info("funds of subaccount %d per shard: %s (required $%.2f)", venue.sub, self.info["balances"]["shards"],
                     balance_need)
            if lcfg.venue.key_restricted_to_subaccount:
                # positive proof (review M1): a read of subaccount 0 must be REFUSED with this key
                ok, why = await verify_key_restriction(self.rest, str(getattr(signer, "key_id", "") or ""), venue.sub,
                                                       bodies.values())
                self.info["key_restriction"] = {"ok": ok, "evidence": why}
                if not ok:
                    raise StartupError(f"venue.key_restricted_to_subaccount is true but {why}: WS messages without a "
                                       "subaccount field would be attributed to this runner")
                log.info("API key restriction verified: %s", why)
            # 6. clean slate FIRST (verified), then positions: an order resting while the
            # positions are read could fill unseen and escape the event exclusion. A shared account
            # never uses the bulk cancel-all: the venue lists subaccount 1's resting orders and
            # cancels them by id until the list is empty
            try:
                left = await venue.cancel_all_verified("startup")
            except RuntimeError as exc:
                raise StartupError(f"start-up cancel-all failed ({exc}): not trading with unknown resting orders") from exc
            if left:
                raise StartupError(f"{len(left)} orders still resting after the start-up cancel-all: "
                                   f"{[o.get('order_id') for o in left][:10]}")
            if venue.last_cancel_all_ns:  # only a BULK cancel-all has the one-minute tail to wait out
                hold_until = venue.last_cancel_all_ns + int(lcfg.venue.cancel_all_hold_s * NS_PER_S)
            try:
                all_positions = await venue.fetch_positions(strict=True)
            except ValueError as exc:
                raise StartupError(f"malformed position row ({exc}): not trading with an unknown inventory") from exc
            positions = {t: q for t, q in all_positions.items() if series_of_ticker(t) in series}
            foreign = {t: q for t, q in all_positions.items() if t not in positions}
            if foreign:
                log.warning("subaccount %d holds positions outside %s (not managed, not counted): %s", venue.sub,
                            list(series), foreign)
                self.info["foreign_positions"] = foreign
            excluded = events_with_positions(positions)
            self.info["startup_positions"] = positions
            if positions:
                log.warning("account holds positions at start-up: %s (events excluded: %s)", positions, sorted(excluded))
            # 7. today's P&L from Kalshi (fills, settlements, positions incl. excluded events, at
            # exchange prices), for the UTC day of the seed (re-derived if midnight passed, N7)
            rest_pnl, seed_ts = await self._derive_today(positions, venue.sub, series)
            self.info["day_pnl_rest"] = rest_pnl.summary()
            for fb in rest_pnl.fallbacks:
                log.warning("risk state: %s", fb)
            for tk, q in sorted(rest_pnl.positions_now.items()):  # how each open position is valued
                src = rest_pnl.mark_source.get(tk, "") or "unknown"
                log.info("risk state: open %s %+.2f valued at $%.4f (%s)", tk, q / 100,
                         rest_pnl.open_px.get(tk, 0) / 1e4, rest_pnl.price_src.get(f"open:{tk}", src))
                self.metrics.inc("dh_position_marks_total", scope="startup", source=src)
            if rest_pnl.window_backfill:
                wb = rest_pnl.window_backfill
                (log.warning if wb.get("errors") else log.info)(
                    "risk state: closed markets awaiting their result %s: window prints back-filled (%d CF call(s), "
                    "%d ticks, errors %s)", wb.get("markets"), wb.get("calls", 0), wb.get("ticks", 0), wb.get("errors"))
            if rest_pnl.foreign:
                log.warning("risk state: fill/settlement rows outside %s skipped: %s", list(series), rest_pnl.foreign)
        else:
            seed_ts = self.clock()  # the seed's decision time AND event time (one UTC day, even at midnight)
        seed = decide_seed(seed_ts, prev_state, rest_pnl, reset=self.reset_daily_halt)
        self.info["risk_seed"] = {"day_start_ns": seed.day_start_ns, "day_pnl_usd": round(seed.day_pnl_usd, 6),
                                  "real_pnl_usd": round(seed.real_pnl_usd, 6), "realized_usd": round(seed.realized_usd, 6),
                                  "mark_usd": round(seed.mark_usd, 6), "budget_base_usd": round(seed.budget_base_usd, 6),
                                  "halted": seed.halted, "halt_reason": seed.halt_reason, "halt_scope": seed.halt_scope,
                                  "halt_day_ns": seed.halt_day_ns, "pause_until_ns": seed.pause_until_ns,
                                  "notes": seed.notes, "overridden": seed.overridden}
        for n in seed.notes:
            log.warning("risk state: %s", n) if seed.halted or seed.overridden else log.info("risk state: %s", n)
        log.info("risk state: real day P&L %+.2f (realized %+.2f, open %+.2f); the daily-loss limit counts %+.2f",
                 seed.real_pnl_usd, seed.realized_usd, seed.mark_usd, seed.day_pnl_usd)
        if seed.overridden:
            log.warning("risk state: OPERATOR RESET: real day P&L %+.2f stays recorded; the loss budget starts at %+.2f "
                        "(was %+.2f counted, halted=%s %s)", seed.real_pnl_usd, seed.budget_base_usd,
                        seed.overridden.get("day_pnl_usd", 0.0), seed.overridden.get("halted"),
                        seed.overridden.get("halt_reason") or "")
            self._log("risk_reset_by_operator", **self.info["risk_seed"])

        # 6. universe (markets on a known, configured exchange shard only)
        now = self.clock()
        sel = await discover_universe(self.rest, series, fee_engine, now, lcfg.universe.horizon_s, exclude_events=excluded,
                                      exchange_indexes=shards)
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
        mm = MarketMaker(scfg, specs, fv_model=fv, fee_engine=fee_engine, book_includes_own=(mode == "live"),
                         id_prefix=self.id_prefix)

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
                                           exclude_events=excluded, known=known, exchange_indexes=shards)

        # what the strategy's equity does not cover: earlier sessions' realized P&L, the excluded
        # positions at their start-up marks (settled during the session -> updated seed)
        ex_marks = rest_pnl.open_px if rest_pnl is not None else {}
        book = RiskBook.from_decision(seed, {t: (q, ex_marks.get(t, 0)) for t, q in positions.items() if q},
                                      specs=rest_pnl.specs if rest_pnl is not None else None,
                                      sources=rest_pnl.mark_source if rest_pnl is not None else None, now_ns=seed_ts)
        runner = LiveRunner(
            mm, mode=mode, period_ns=int(scfg.timers.quote_period_ms) * NS_PER_MS, cfg=lcfg, venue=venue, sim=sim,
            paper_fees=paper_fees, hedge=hedge, recorder=self.recorder, jsonlog=self.jsonlog, metrics=self.metrics,
            clock_ns=self.clock, kill_file=KillFile(_resolve(paths.kill_file)), heartbeat_path=hb,
            fee_engine=fee_engine, universe=specs, discover=discover, series=series, session_id=self.session_id,
            risk_store=store, cancel_all_marker=marker, risk_book=book, started_ns=started_ns,
            clock_sampler=self.ov.clock_sampler, own_id_prefix=self.own_id_prefix if mode == "live" else "",
            watchdog_reader=watchdog_reader, disk_free_gb=disk_free)
        hedge.sink = runner.push_result
        # the first event: the day's risk state (before any Timer; recorded for replay)
        runner.push_result(make_seed(seed_ts, seed))
        store.save(state_from_decision(seed, session=self.session_id, mode=mode, now_ns=self.clock()), fsync=True)
        if hold_until:
            runner.hold(hold_until, "startup_cancel_all")
        if mode == "live":
            runner.balance_required_usd = balance_need
            runner.push_side("balance", {"balances": balances, "available": available})  # metrics from the first second
            runner.push_side("exchange_schedule", {"closures": closures, "notes": []})
        if venue is not None:
            venue.sink = runner.push_result
            venue.log_fn = lambda k, p: runner.jlog("venue." + k, self.clock(), **p)
            venue.observe_rtt = lambda op, dt, outcome: self.metrics.observe("dh_rest_rtt_seconds", dt, op=op, outcome=outcome)
            venue.on_group_map = lambda logical, gid: runner.meta("order_group_map", self.clock(), logical=logical, id=gid)
            limit = round(scfg.risk.order_group_limit_contracts * QTY_SCALE)
            gid = await venue.ensure_order_group(MarketMaker.ORDER_GROUP_ID, limit)
            if gid is None:
                raise StartupError(f"could not create the exchange order group (fill-burst breaker) on shard(s) "
                                   f"{venue.shards_in_use()}: refusing to trade")
            self.info["order_group"] = {"logical": MarketMaker.ORDER_GROUP_ID, "id": gid, "limit": limit,
                                        "groups": venue.group_refs()}

        # 11. market data
        subs = build_subscriptions(mode, [s.ticker for s in specs], lcfg)
        kw = ws_kwargs(kc.ws)
        connect = self.ov.ws_connect or websockets_connect_factory(ping_interval=kc.ws.get("ping_interval_s", 10),
                                                                   ping_timeout=kc.ws.get("ping_timeout_s", 10))
        on_raw = self.recorder.write
        if mode == "live":  # the first own fill settles whether the WS fill names the subaccount
            on_raw = WsFillProbe(self.recorder.write, runner, lcfg.venue.sub, lcfg.venue.key_restricted_to_subaccount,
                                 self.id_prefix)
        self.ws = KalshiWS(kc.ws_url, signer, subs, on_raw=on_raw, on_event=runner.push,
                           connect=connect, clock_ns=self.clock,
                           max_markets_per_subscription=int(lcfg.universe.max_markets_per_subscription), **kw)
        ws = self.ws
        runner.add_source("kalshi.ws", ws.run, stop=ws.stop)
        runner.subscribe_markets = lambda tickers: ws.add_markets(tickers, channel=lcfg.universe.market_channels[0])
        runner.unsubscribe_markets = lambda tickers: ws.delete_markets(tickers, channel=lcfg.universe.market_channels[0])
        self._add_feeds(runner)

        # 12. session record
        self._meta("session_start", at=t_start, mode=mode, git_sha=sha, strategy_config=asdict(scfg), strategy_digest=scfg.digest(),
                   live_config=lcfg.as_dict(), live_digest=lcfg.digest(), specs=[spec_to_dict(s) for s in specs],
                   skipped=sel.skipped, excluded_events=sorted(excluded), backfill=bf.summary(),
                   paper=asdict(lcfg.paper) if mode == "paper" else None, info=self.info,
                   id_prefix=self.id_prefix, session_token=self.token, subaccount=lcfg.venue.sub, series=list(series),
                   own_id_prefix=self.own_id_prefix if mode == "live" else "",
                   exchange_indexes=list(shards), shared_account=lcfg.venue.shared_account,
                   key_restricted_to_subaccount=lcfg.venue.key_restricted_to_subaccount)
        self._meta("fv_warmup", source=bf.source, points=bf.points)
        self._log("startup", universe=[s.ticker for s in specs], skipped=sel.skipped, backfill=bf.summary(), info=self.info)
        self.runner = runner
        return runner

    async def _derive_today(self, positions: dict[str, int], sub: int, series: tuple[str, ...] = ()) -> tuple[Any, int]:
        """(today's DayPnl from Kalshi, the seed time), both on the same UTC day: when midnight
        passes during the derivation it is done again for the new day (a restart at 00:00 must
        not seed the new day with yesterday's P&L). Failures refuse the start."""
        for _ in range(3):
            ds = day_start(self.clock())
            try:
                pnl = await derive_day_pnl(self.rest, ds, positions, subaccount=sub, series=series or None,
                                           now_ns=self.clock(), window_backfill=True)
            except RiskStateError as exc:
                raise StartupError(f"risk state: {exc}") from exc
            seed_ts = self.clock()
            if day_start(seed_ts) == ds:
                return pnl, seed_ts
            log.warning("the UTC day changed while today's P&L was derived: deriving it for the new day")
        raise StartupError("the UTC day kept changing while today's P&L was derived")

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
            # FeedClient._reader yields once per frame (dh.feeds.base), so a busy feed cannot
            # starve the consumer, order requests or the heartbeat (audit live M2)
            runner.add_source(name, (lambda f=feed: f.run(self.recorder.write)), stop=feed.stop)

    # ------------------------------------------------------------------ run
    async def run(self, duration_s: float | None = None) -> int:
        try:
            runner = await self.build()
        except StartupError as exc:
            log.error("refusing to start: %s", exc)
            self._log("startup_refused", error=str(exc))
            await self._abort()
            return 2
        except Exception as exc:  # noqa: BLE001 - network / auth / parsing: never half-start
            log.exception("start-up failed")
            self._log("startup_failed", error=f"{type(exc).__name__}: {exc}"[:500])
            await self._abort()
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

    async def _abort(self) -> None:
        """Start-up failed after side effects: delete an order group we created (no order was
        placed yet), mark the heartbeat 'stopped' (nothing of ours rests), release everything."""
        v = self.venue
        if v is not None:
            for logical in list(v.groups):
                try:
                    await v._retry_group("delete", logical, attempts=2)  # noqa: SLF001
                except Exception:  # noqa: BLE001
                    log.exception("order group delete failed")
            try:
                await v.close()
            except Exception:  # noqa: BLE001
                pass
        if self.locks:
            try:
                write_heartbeat(heartbeat_path(self.lcfg, self.mode), {"pid": os.getpid(), "mode": self.mode,
                                                                      "state": "stopped", "session": self.session_id})
            except OSError:
                pass
        await self.close()

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
        for lk in self.locks:
            lk.release()
        self.locks = []


async def run_from_paths(strategy_config: str | None, live_config: str | None, *, cli_mode: str | None,
                         confirmed: bool, duration_s: float | None = None, overrides: Overrides | None = None,
                         reset_daily_halt: bool = False) -> int:
    """Entry point used by scripts/run_live.py (mode checks happen before anything connects)."""
    from dh.strategy.config import load_config

    lcfg = load_live_config(_resolve(live_config) if live_config else None)
    mode = resolve_mode(cli_mode, lcfg.mode, confirmed)
    scfg = load_config(_resolve(strategy_config) if strategy_config else None)
    if mode == "live" and scfg.hedge.enabled:
        raise StartupError("strategy config enables hedging but no hedge adapter exists (M1: hedge.enabled=false)")
    if mode == "live":
        problems = live_config_problems(lcfg)
        if problems:
            raise StartupError("; ".join(problems))
    app = LiveApp(scfg, lcfg, mode, overrides, reset_daily_halt=reset_daily_halt)
    return await app.run(duration_s)
