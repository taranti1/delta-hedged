"""Live / paper runner configuration (config/live.yaml; template config/live.example.yaml).

The STRATEGY configuration (quoting, risk limits, timers) stays in config/m1.yaml
(dh.strategy.config); this file only holds what the runner needs to talk to the world:
mode, Kalshi endpoints and key location (via config/kalshi.yaml), data/log paths, the kill
file and heartbeat, the metrics port, venue behaviour (batching, reconciliation, polls),
the market universe (series, horizon, roll-over), the benchmark back-fill and the paper
simulator. Secrets are never stored here (see dh.kalshi.config: environment variables).

Mode safety: ``mode: live`` in this file AND ``--i-understand-this-sends-real-orders`` on
the command line are both required to send real orders (``resolve_mode``).
"""

from __future__ import annotations

import hashlib
import json
import sys
from dataclasses import asdict, dataclass, field, fields, is_dataclass
from pathlib import Path
from typing import Any

import yaml

MODES = ("paper", "live")
LIVE_CONFIRM_FLAG = "--i-understand-this-sends-real-orders"
# a shared Kalshi account (another live system on subaccount 0): this process may use at most
# this fraction of the account's REST budget (rate limits are per account)
SHARED_ACCOUNT_MAX_SHARE = 0.5


def default_run_dir(platform: str | None = None) -> str:
    """Directory of the kill file, the heartbeat and their locks when the config leaves them
    empty: ``/run/dh`` on Linux (tmpfs, created by the operator after every boot) and
    ``data/run`` under the repository on macOS (there is no /run; created on demand)."""
    return "data/run" if (platform or sys.platform) == "darwin" else "/run/dh"


@dataclass(frozen=True)
class PathsCfg:
    # Recorder root: <data_root>/raw/<stream>/... Must NOT be the collector's root
    # (scripts/record.py): two processes writing kalshi.ws into one store would interleave
    # two connections' sequence numbers. One runner per data_root: the runner holds
    # <data_root>/runner.lock (flock) while it runs.
    data_root: str = "data/live"
    log_dir: str = "data/live_logs"  # JSON-lines decision/action logs
    # '' = <default_run_dir()>/KILL: /run/dh/KILL (Linux) or data/run/KILL (macOS)
    kill_file: str = ""  # touch it to cancel everything and stop (live AND paper)
    heartbeat_file: str = ""  # LIVE runner; read by scripts/watchdog.py ('' = <run dir>/heartbeat.json)
    paper_heartbeat_file: str = ""  # paper runner; '' = <heartbeat_file stem>.paper.json (never the watchdog's file)
    # per-UTC-day risk state (day P&L, carried halt, pause); '' = <data_root>/state/risk_state.<mode>.json
    risk_state_file: str = ""

    def __post_init__(self) -> None:
        run = default_run_dir()
        if not self.kill_file:
            object.__setattr__(self, "kill_file", f"{run}/KILL")
        if not self.heartbeat_file:
            object.__setattr__(self, "heartbeat_file", f"{run}/heartbeat.json")

    def heartbeat_for(self, mode: str) -> str:
        """The heartbeat file of a runner in ``mode``: paper never writes the live file (a paper
        runner beating into it would look like the live runner to the watchdog)."""
        if mode == "live":
            return self.heartbeat_file
        if self.paper_heartbeat_file and self.paper_heartbeat_file != self.heartbeat_file:
            return self.paper_heartbeat_file
        p = Path(self.heartbeat_file)
        return str(p.with_name(f"{p.stem}.paper{p.suffix or '.json'}"))

    def risk_state_for(self, mode: str) -> str:
        """Risk-state file of ``mode`` (paper and live never share one)."""
        if self.risk_state_file:
            p = Path(self.risk_state_file)
            return str(p if mode == "live" else p.with_name(f"{p.stem}.paper{p.suffix or '.json'}"))
        return str(Path(self.data_root) / "state" / f"risk_state.{mode}.json")


@dataclass(frozen=True)
class MetricsCfg:
    enabled: bool = True
    host: str = "127.0.0.1"  # never expose on a public interface
    port: int = 9108  # 0 = ephemeral (tests)


@dataclass(frozen=True)
class LoopCfg:
    heartbeat_interval_s: float = 0.5
    kill_check_interval_s: float = 0.2  # checked on the consumer loop at least this often
    metrics_refresh_s: float = 1.0
    # Data lag above this (the larger of the runner-queue lag and the exchange-time lag of
    # Kalshi market data, see LiveRunner) closes the order gate AND tells the strategy
    # (FeedStatus 'runner.lag' stale -> it cancels its quotes); both reopen once fresh data
    # has shown a lag below max_lag_s / 2 for lag_resume_s.
    max_lag_s: float = 1.0
    lag_resume_s: float = 2.0
    lag_window_s: float = 600.0  # trailing window of the exchange-time latency baseline
    lag_confirm_s: float = 0.5  # exchange-time lag = the SMALLEST excess age over this window
    # The latency baseline never exceeds this cap (0 = clock_block_ms + 100): a backlog present
    # since start-up, or lasting longer than lag_window_s, cannot become "normal latency".
    # A source whose smallest age exceeds the cap raises an alarm (dh_lag_baseline_seconds).
    lag_baseline_cap_ms: float = 0.0
    yield_items: int = 64  # the consumer yields to the event loop at least every N items ...
    yield_ms: float = 5.0  # ... or N ms of work, and always after dispatching orders
    shutdown_timeout_s: float = 10.0
    strategy_error: str = "stop"  # stop (fail safe) | continue (paper debugging only; refused live)
    clock_sample_s: float = 60.0  # chrony/timedatectl sample to the 'clock' stream; 0 = off (refused live)
    clock_resample_s: float = 5.0  # while the last sample was bad or unmeasurable: sample this often
    clock_alarm_ms: float = 5.0  # |offset| above this: alarm (metric + log)
    clock_block_ms: float = 250.0  # |offset| above this on clock_block_samples samples in a row:
    clock_block_samples: int = 2  # new orders blocked until it recovers
    # live: a sample counts as bad (blocks like an offset) unless it comes from chronyc or
    # timedatectl and says synchronised (macOS: a query-only `sntp` answer; "synchronised" = it
    # answered), and its estimated error is at most this (0 = clock_block_ms)
    clock_max_est_error_ms: float = 0.0
    risk_state_interval_s: float = 2.0  # persist day P&L / halt / pause this often (0 = off)

    def __post_init__(self) -> None:
        if self.strategy_error not in ("stop", "continue"):
            raise ValueError(f"loop.strategy_error must be 'stop' or 'continue', got {self.strategy_error!r}")

    def baseline_cap_ms(self) -> float:
        """Cap of the exchange-time latency baseline (ms)."""
        return self.lag_baseline_cap_ms if self.lag_baseline_cap_ms > 0 else self.clock_block_ms + 100.0

    def max_est_error_ms(self) -> float:
        """Largest acceptable estimated clock error of a sample (ms)."""
        return self.clock_max_est_error_ms if self.clock_max_est_error_ms > 0 else self.clock_block_ms


@dataclass(frozen=True)
class VenueCfg:
    # Kalshi subaccount (null / 0 = primary). ALWAYS sent explicitly (0 for primary): Kalshi
    # reads an omitted subaccount as "all subaccounts" on GET orders/fills and cancel-all.
    subaccount: int | None = None
    # The Kalshi account is SHARED with another live system (which owns subaccount 0): live mode
    # refuses subaccount null/0 and rate_limits.account_share > 0.5, and the REST client refuses
    # any write that does not name this subaccount explicitly.
    shared_account: bool = False
    # The API key(s) are restricted to ``subaccount`` (Kalshi scopes the private WebSocket
    # channels server-side): a fill / user_order / market_position without a subaccount field is
    # this subaccount's. Verified at live start-up (GET /api_keys, else the balance breakdown).
    key_restricted_to_subaccount: bool = False
    # Exchange shards this runner trades and has funded (all KXBTC* markets: shard 2). A market on
    # an unknown or other shard is never traded; one order group per shard in use; the balance of
    # every listed shard is checked at start-up and every balance_interval_s.
    exchange_indexes: tuple[int, ...] = (2,)
    # funds per shard (available + our positions at cost + resting collateral) >= worst-case loss (m1) + this
    min_balance_margin_dollars: float = 10.0
    balance_interval_s: float = 60.0  # GET /portfolio/balance re-read; below the requirement -> new orders blocked
    exchange_status_interval_s: float = 10.0  # GET /exchange/status poll (trading pauses); live: > 0
    exchange_schedule_interval_s: float = 3600.0  # GET /exchange/schedule (maintenance, weekly pause)
    pause_lead_s: float = 60.0  # pull quotes this long before a scheduled closure starts
    pause_reject_hold_s: float = 30.0  # a place rejected for a pause keeps the pause gate closed this long
    max_batch: int = 20  # places / cancels per batched request (also capped by the write bucket)
    max_place_wait_s: float = 0.5  # never send a quote that would wait longer than this for write tokens
    self_trade_prevention: str = "taker_at_cross"
    reconcile_backoff_s: tuple[float, ...] = (0.5, 1.0, 2.0, 4.0, 8.0, 15.0, 30.0)
    reconcile_missing_after_s: float = 30.0  # unknown create not found this long -> rejected
    reconcile_min_attempts: int = 3
    create_match_skew_s: float = 2.0  # a lookup match must be created >= request time - this
    missing_recheck_s: tuple[float, ...] = (10.0, 30.0)  # re-look for a create declared missing
    max_cancel_retries: int = 5  # re-cancels of a still-resting order before the stuck alarm
    recancel_backoff_max_s: float = 5.0  # (re-cancelling never stops while the order rests)
    queue_positions_interval_s: float = 2.0  # GET /portfolio/orders/queue_positions; 0 = off
    queue_positions_resync: bool = False  # True: overwrite the estimator (breaks replay parity)
    queue_positions_max_tickers: int = 20
    read_reserve_tokens: float = 30.0  # skip optional polls when the read bucket is below this
    positions_interval_s: float = 30.0  # GET /portfolio/positions reconciliation; 0 = off
    position_confirm_s: float = 5.0  # a mismatch must persist this long before it halts
    ghost_sweep: bool = True  # cancel resting orders the strategy does not know about
    fills_backfill_interval_s: float = 60.0  # GET /portfolio/fills safety net (0 = off)
    fills_backfill_margin_s: float = 120.0  # look-back overlap of every fill back-fill
    fills_backfill_min_age_s: float = 10.0  # periodic pass: leave younger fills to the WebSocket
    reconnect_settle_s: float = 2.0  # after a WS reconnect, let subscriptions settle, then reconcile
    reconcile_retry_max_s: float = 30.0
    startup_cancel_all: bool = True  # clean slate: cancel leftover resting orders at start (live: required)
    cancel_all_hold_s: float = 60.0  # Kalshi may cancel orders placed within 1 min of a cancel-all
    exclude_events_with_positions: bool = True  # never trade events we already hold at start (live: required)
    shutdown_delete_group: bool = True
    halt_on_fee_mismatch: bool = True

    @property
    def sub(self) -> int:
        """The subaccount number sent on every request (0 = primary)."""
        return int(self.subaccount) if self.subaccount is not None else 0


@dataclass(frozen=True)
class UniverseCfg:
    series: tuple[str, ...] = ()  # empty = StrategyConfig.quoting.enabled_series
    horizon_s: float = 7200.0  # markets expiring within now + horizon are traded (current + next hour)
    discovery_interval_s: float = 120.0  # REST re-discovery (hourly roll-over)
    prune_after_s: float = 900.0  # settled markets are dropped this long after expiration
    market_channels: tuple[str, ...] = ("orderbook_delta", "trade")
    max_markets_per_subscription: int = 100
    index_ids: tuple[str, ...] = ("BRTI",)


@dataclass(frozen=True)
class BackfillCfg:
    """Benchmark history for the fair-value warm-up (dh.models.fvmodel needs >= 1 day).

    Exact call, per chunk, newest first:
        GET <rest>/cfbenchmarks/history/values?id=BRTI&timespan=<timespan>&timestamp=<timestamp>
    with the templates below filled from {start_ms, end_ms, start_s, end_s, span_s, span_ms,
    start_iso, end_iso} of the chunk.

    VERIFIED LIVE 2026-09-25 (docs.kalshi.com "CF Benchmarks REST Passthrough" + CF Benchmarks
    /api/v1/history/values): ``timespan=HOUR&timestamp=<hour START, ISO ms, e.g.
    2026-09-25T17:00:00.000Z>`` returns {"data":{"serverTime":..,"payload":[{"time":<ms>,
    "value":"83737.50"}, ...]}}: the hour's ticks at 5 Hz (18,000 rows, ~750 kB), ascending.
    The timestamp must be truncated to the timespan granularity (``align``: chunk boundaries at
    multiples of chunk_s). Recent values can be delayed up to 15 min: an empty chunk ending
    within ``recent_delay_s`` of now is skipped instead of stopping the back-fill. Each request
    costs 50 read tokens (48 for 2 days). Ticks are down-sampled to one print per ``step_s``
    before feeding the EWMAs.
    """

    enabled: bool = True
    index_id: str = "BRTI"
    days: float = 2.0
    step_s: int = 60
    chunk_s: int = 3600  # must equal the timespan below (HOUR = 3600 s)
    timespan: str = "HOUR"
    timestamp: str = "{start_iso}"
    align: bool = True  # chunk boundaries at multiples of chunk_s (CF: timestamp truncated to the timespan)
    recent_delay_s: float = 1800.0  # an EMPTY chunk ending this close to now is skipped, not fatal
    extra_params: dict[str, Any] = field(default_factory=dict)
    min_coverage: float = 0.9  # fraction of the requested grid that must be present


@dataclass(frozen=True)
class PaperCfg:
    policy: str = "conservative"  # optimistic | realistic | conservative (C = default)
    seed: int = 7
    submit_ms: float = 40.0  # lognormal medians (ms); replace with measured RTTs
    response_ms: float = 40.0
    ws_ms: float = 25.0
    sigma: float = 0.4  # lognormal sigma; 0 = fixed delays
    latency_multiplier: float | None = None  # None = simulator default (1.5 under policy C)


@dataclass(frozen=True)
class FeedsCfg:
    config: str = "config/feeds.yaml"
    only: tuple[str, ...] = ()  # external venues to run (keys/venues/streams); empty = none


@dataclass(frozen=True)
class WatchdogCfg:
    stale_s: float = 2.0  # heartbeat older than this -> cancel all
    poll_s: float = 0.25
    retry_s: float = 1.0  # retry a failed cancel-all
    repeat_s: float = 30.0  # repeat while still stale (orders placed in flight)
    max_repeats: int = 10
    only_live: bool = True  # ignore heartbeats written by paper runners
    stopping_grace_s: float = 5.0  # a 'stopping' runner older than its shutdown_timeout_s + this is stuck
    key_id_env: str = "KALSHI_WATCHDOG_KEY_ID"  # optional separate API key (falls back to the main one)
    private_key_path_env: str = "KALSHI_WATCHDOG_PRIVATE_KEY_PATH"


@dataclass(frozen=True)
class LiveConfig:
    mode: str = "paper"
    session: str = ""  # free label written into logs / meta
    kalshi_config: str = ""  # path to config/kalshi.yaml ('' = default lookup)
    kalshi_env: str = ""  # prod | demo ('' = file default)
    paths: PathsCfg = field(default_factory=PathsCfg)
    metrics: MetricsCfg = field(default_factory=MetricsCfg)
    loop: LoopCfg = field(default_factory=LoopCfg)
    venue: VenueCfg = field(default_factory=VenueCfg)
    universe: UniverseCfg = field(default_factory=UniverseCfg)
    backfill: BackfillCfg = field(default_factory=BackfillCfg)
    paper: PaperCfg = field(default_factory=PaperCfg)
    feeds: FeedsCfg = field(default_factory=FeedsCfg)
    watchdog: WatchdogCfg = field(default_factory=WatchdogCfg)

    def __post_init__(self) -> None:
        if self.mode not in MODES:
            raise ValueError(f"mode must be one of {MODES}, got {self.mode!r}")

    def digest(self) -> str:
        blob = json.dumps(asdict(self), sort_keys=True, default=str).encode()
        return hashlib.sha256(blob).hexdigest()[:16]

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def _build(cls: type, data: dict[str, Any] | None, where: str) -> Any:
    kwargs: dict[str, Any] = {}
    known = {f.name: f for f in fields(cls)}
    for k, v in (data or {}).items():
        if k not in known:
            raise KeyError(f"unknown live config key {where}.{k}")
        f = known[k]
        default = f.default_factory() if callable(f.default_factory) else f.default  # type: ignore[misc]
        if is_dataclass(default):
            kwargs[k] = _build(type(default), v or {}, f"{where}.{k}")
        elif isinstance(default, tuple):
            kwargs[k] = tuple(v or ())
        else:
            kwargs[k] = v
    return cls(**kwargs)


def load_live_config(path: str | Path | None) -> LiveConfig:
    """Parse config/live.yaml (unknown keys are errors: a typo must never be ignored)."""
    if path is None:
        return LiveConfig()
    data = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
    if not isinstance(data, dict):
        raise ValueError(f"{path}: expected a mapping")
    return _build(LiveConfig, data, "live")


class ModeError(SystemExit):
    """Refusal to start in the requested mode (exit code 2)."""

    def __init__(self, message: str) -> None:
        super().__init__(2)
        self.message = message

    def __str__(self) -> str:
        return self.message


def live_config_problems(cfg: LiveConfig) -> list[str]:
    """Settings that are refused in live mode (each is safe only for paper debugging)."""
    out = []
    if cfg.loop.strategy_error != "stop":
        out.append("loop.strategy_error must be 'stop' in live mode (a strategy exception must stop trading)")
    if not cfg.venue.exclude_events_with_positions:
        out.append("venue.exclude_events_with_positions must be true in live mode (positions held at start-up "
                   "are not seeded into the strategy: its first fill there would be a position mismatch)")
    if not cfg.venue.startup_cancel_all:
        out.append("venue.startup_cancel_all must be true in live mode (leftover orders must be cancelled "
                   "before positions are read)")
    if cfg.loop.clock_sample_s <= 0:
        out.append("loop.clock_sample_s must be > 0 in live mode (the clock-offset gate must measure the clock; "
                   "it blocks new orders when it cannot)")
    v = cfg.venue
    if v.shared_account and v.sub == 0:
        out.append("venue.shared_account is true: venue.subaccount must be a dedicated subaccount (1-63), never "
                   "null/0 (subaccount 0 belongs to the other live system on this account)")
    if not v.exchange_indexes or any(isinstance(x, bool) or not isinstance(x, int) or x < 0 for x in v.exchange_indexes):
        out.append("venue.exchange_indexes must list the exchange shard(s) this runner trades and has funded "
                   "(KXBTC*: [2])")
    if v.exchange_status_interval_s <= 0:
        out.append("venue.exchange_status_interval_s must be > 0 in live mode (trading pauses must be detected)")
    if v.balance_interval_s <= 0:
        out.append("venue.balance_interval_s must be > 0 in live mode (the shard balance must be re-checked)")
    return out


def account_share_problem(cfg: LiveConfig, account_share: float) -> str:
    """Live refusal of the Kalshi config's rate_limits.account_share on a shared account ('' = ok)."""
    if cfg.venue.shared_account and float(account_share) > SHARED_ACCOUNT_MAX_SHARE:
        return (f"venue.shared_account is true: rate_limits.account_share {account_share:g} in the Kalshi config "
                f"must be <= {SHARED_ACCOUNT_MAX_SHARE:g} (REST budgets are per account; the other system needs "
                "its share)")
    return ""


def resolve_mode(cli_mode: str | None, config_mode: str, confirmed: bool) -> str:
    """Effective mode. Live needs ``mode: live`` in the live config AND the CLI flag.

    ``--mode paper`` always wins (downgrading is safe); ``--mode live`` with a paper config,
    or a live config without the confirmation flag, refuses to start.
    """
    if cli_mode is not None and cli_mode not in MODES:
        raise ModeError(f"--mode must be one of {MODES}")
    mode = cli_mode or config_mode
    if mode == "paper":
        return "paper"
    if config_mode != "live":
        raise ModeError("live mode requires `mode: live` in the live config (config/live.yaml); refusing to start")
    if not confirmed:
        raise ModeError(f"live mode sends REAL orders: pass {LIVE_CONFIRM_FLAG} to confirm; refusing to start")
    return "live"
