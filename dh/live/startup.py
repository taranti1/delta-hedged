"""Start-up helpers shared by live and paper mode (all offline-testable with fake REST).

    status  = await exchange_status(rest)
    sel     = await discover_universe(rest, ("KXBTCD",), fee_engine, now_ns, horizon_s)
    result  = await backfill_fair_value(rest, fv_model, now_ns, BackfillCfg())
    sim     = build_paper_sim(PaperCfg(), sel.specs, fee_engine)

Fair-value warm-up: ``FairValueModel.ready`` needs one half-life of history in EVERY EWMA,
the longest being 1 day (dh/models/data/fv_recommended.json, max_dt_s 600 s), so the model
is fed ``BackfillCfg.days`` (default 2) of BRTI history down-sampled to one print per
``step_s`` (default 60 s, the sampling the EWMAs were validated on). The history comes from
Kalshi's CF Benchmarks passthrough, fetched in ``chunk_s`` pieces, newest first:

    GET /trade-api/v2/cfbenchmarks/history/values?id=BRTI&timespan=3600s&timestamp=<chunk_end_ms>

(templates ``BackfillCfg.timespan`` / ``timestamp``; the passthrough is not in the openapi
spec, so its parameter formats must be confirmed live). If the history is unavailable or too
sparse, the model stays not-ready and the MarketMaker refuses to quote until live ticks have
warmed it (>= 1 day): the runner never substitutes a guess.
"""

from __future__ import annotations

import math
from collections.abc import Callable, Iterable
from dataclasses import asdict, dataclass, field
from typing import Any

from dh.core.events import IndexTick
from dh.core.market import MarketSpec, PriceRange, SettlementSpec
from dh.core.units import NS_PER_MS, NS_PER_S
from dh.kalshi.metadata import MarketRegistry, discover_markets, parse_market_ticker
from dh.kalshi.normalize import cf_history_to_ticks
from dh.live.config import BackfillCfg, PaperCfg

TRADABLE_STATUSES = ("", "active", "open", "initialized")


# ============================================================================ exchange
async def exchange_status(rest: Any) -> dict[str, Any]:
    """GET /exchange/status -> {'exchange_active', 'trading_active', ...} (booleans coerced)."""
    body = await rest.get_exchange_status()
    out = dict(body or {})
    out["exchange_active"] = bool(out.get("exchange_active", False))
    out["trading_active"] = bool(out.get("trading_active", False))
    return out


# ============================================================================ specs <-> json
def spec_to_dict(spec: MarketSpec) -> dict[str, Any]:
    """JSON-safe MarketSpec (recorded in 'meta' so replay rebuilds the same universe)."""
    return asdict(spec)


def spec_from_dict(d: dict[str, Any]) -> MarketSpec:
    d = dict(d)
    d["settlement"] = SettlementSpec(**d.get("settlement", {}))
    d["price_ranges"] = tuple(PriceRange(**r) if isinstance(r, dict) else PriceRange(*r) for r in d.get("price_ranges", ()))
    return MarketSpec(**d)


def spec_signature(spec: MarketSpec) -> tuple:
    """Everything that changes what a quote is worth or whether it is valid."""
    return (spec.strike_type, spec.floor_strike, spec.cap_strike, spec.close_ts, spec.expiration_ts,
            spec.price_ranges, spec.fee_type, spec.fee_multiplier, spec.settlement)


# ============================================================================ universe
@dataclass
class UniverseSelection:
    specs: list[MarketSpec]
    skipped: dict[str, str] = field(default_factory=dict)  # ticker -> reason (tradability notes)
    changed: dict[str, str] = field(default_factory=dict)  # known ticker -> what changed
    registry: MarketRegistry | None = None


def select_specs(
    registry: MarketRegistry,
    now_ns: int,
    horizon_s: float,
    *,
    series: Iterable[str] = (),
    exclude_events: Iterable[str] = (),
    known: dict[str, MarketSpec] | None = None,
) -> UniverseSelection:
    """Markets to trade: in ``series``, not closed, expiring within ``now + horizon_s``, with
    clean rules text (metadata.BLOCKING_FLAGS), an open status, not paused, and not in an
    excluded event. Markets with an unresolved / unsupported fee type ARE included: the
    MarketMaker keeps them untradable itself (it never assumes a fee schedule).
    ``known`` markets are not returned again; their spec changes are reported in ``changed``."""
    series_set = set(series)
    excluded = set(exclude_events)
    known = known or {}
    horizon_ns = int(horizon_s * NS_PER_S)
    out: list[MarketSpec] = []
    skipped: dict[str, str] = {}
    changed: dict[str, str] = {}
    for t, reason in sorted(registry.rejected.items()):
        if not series_set or parse_market_ticker(t).series in series_set:
            skipped[t] = f"unsupported: {reason}"
    for t in sorted(registry.specs):
        spec = registry.specs[t]
        if series_set and spec.series_ticker not in series_set:
            continue
        if t in known:
            if spec_signature(spec) != spec_signature(known[t]):
                changed[t] = "spec changed"
            continue
        if spec.close_ts <= now_ns or spec.expiration_ts <= now_ns:
            continue
        if spec.expiration_ts - now_ns > horizon_ns:
            continue
        if spec.event_ticker in excluded:
            skipped[t] = "event already holds a position at start-up"
            continue
        flags = registry.blocking_flags(t)
        if flags:
            skipped[t] = "rules check: " + ",".join(flags)
            continue
        status = registry.status.get(t, "")
        if status not in TRADABLE_STATUSES:
            skipped[t] = f"status {status!r}"
            continue
        if t in registry.paused:
            skipped[t] = "paused"
            continue
        if not spec.fee_type:
            skipped[t] = "fee type unresolved (kept, untradable)"
        out.append(spec)
    return UniverseSelection(out, skipped, changed, registry)


async def discover_universe(
    rest: Any,
    series: Iterable[str],
    fee_engine: Any,
    now_ns: int,
    horizon_s: float,
    *,
    exclude_events: Iterable[str] = (),
    known: dict[str, MarketSpec] | None = None,
) -> UniverseSelection:
    """REST discovery (dh.kalshi.metadata.discover_markets on open events) + select_specs."""
    series = tuple(series)
    reg = await discover_markets(rest, series, status="open", fee_engine=fee_engine)
    return select_specs(reg, now_ns, horizon_s, series=series, exclude_events=exclude_events, known=known)


def events_with_positions(positions: dict[str, int]) -> set[str]:
    """Event tickers of markets with a non-zero position."""
    return {parse_market_ticker(t).event_ticker for t, q in positions.items() if q}


# ============================================================================ back-fill
@dataclass
class BackfillResult:
    points: list[tuple[int, float]]  # (source ts ns, value) fed to the model, ascending
    requested: int  # grid points requested
    coverage: float
    ready: bool  # FairValueModel.ready after the warm-up
    source: str
    requests: int = 0
    errors: list[str] = field(default_factory=list)

    def summary(self) -> dict[str, Any]:
        return {"source": self.source, "points": len(self.points), "requested": self.requested,
                "coverage": round(self.coverage, 4), "ready": self.ready, "requests": self.requests,
                "first_ns": self.points[0][0] if self.points else 0,
                "last_ns": self.points[-1][0] if self.points else 0, "errors": self.errors[:5]}


def _fill_template(tpl: str, start_ns: int, end_ns: int) -> str:
    return tpl.format(start_ms=start_ns // NS_PER_MS, end_ms=end_ns // NS_PER_MS, start_s=start_ns // NS_PER_S,
                      end_s=end_ns // NS_PER_S, span_s=(end_ns - start_ns) // NS_PER_S,
                      span_ms=(end_ns - start_ns) // NS_PER_MS)


async def fetch_benchmark_history(
    rest: Any,
    end_ns: int,
    cfg: BackfillCfg,
    *,
    clock_ns: Callable[[], int] | None = None,
) -> tuple[list[IndexTick], int, list[str]]:
    """BRTI ticks in [end - days, end] from the CF passthrough, newest chunk first; stops at
    the first failing chunk. Returns (ticks ascending by source time, requests, errors)."""
    span_ns = int(cfg.days * 86400 * NS_PER_S)
    chunk_ns = max(1, int(cfg.chunk_s)) * NS_PER_S
    start_all = end_ns - span_ns
    ticks: dict[int, IndexTick] = {}
    errors: list[str] = []
    n = 0
    c_end = end_ns
    while c_end > start_all:
        c_start = max(start_all, c_end - chunk_ns)
        n += 1
        try:
            body = await rest.get_cfbenchmarks_history(
                cfg.index_id,
                timespan=_fill_template(cfg.timespan, c_start, c_end) if cfg.timespan else None,
                timestamp=_fill_template(cfg.timestamp, c_start, c_end) if cfg.timestamp else None,
                extra_params=dict(cfg.extra_params) or None,
            )
        except Exception as exc:  # noqa: BLE001 - unavailable history is an expected outcome
            errors.append(f"{type(exc).__name__}: {exc}"[:300])
            break
        recv = clock_ns() if clock_ns is not None else end_ns
        got = 0
        for t in cf_history_to_ticks(body, recv, cfg.index_id):
            if t.index_id == cfg.index_id and start_all <= t.ts_exch <= end_ns and math.isfinite(t.value) and t.value > 0:
                ticks[t.ts_exch] = t
                got += 1
        if got == 0:
            errors.append(f"chunk ending {c_end // NS_PER_S}: no ticks")
            break
        c_end = c_start
    return [ticks[k] for k in sorted(ticks)], n, errors


def resample(ticks: list[IndexTick] | list[tuple[int, float]], start_ns: int, end_ns: int, step_s: int,
             max_stale_s: float = 600.0) -> list[tuple[int, float]]:
    """One (grid ts, last value at or before it) per ``step_s`` grid point in [start, end];
    a grid point whose last tick is older than ``max_stale_s`` is skipped (outage)."""
    pts = [(t.ts_exch, t.value) if isinstance(t, IndexTick) else (int(t[0]), float(t[1])) for t in ticks]
    pts.sort()
    step = int(step_s) * NS_PER_S
    stale = int(max_stale_s * NS_PER_S)
    g = (start_ns // step + (1 if start_ns % step else 0)) * step
    out: list[tuple[int, float]] = []
    i = 0
    last: tuple[int, float] | None = None
    while g <= end_ns:
        while i < len(pts) and pts[i][0] <= g:
            last = pts[i]
            i += 1
        if last is not None and g - last[0] <= stale:
            out.append((g, last[1]))
        g += step
    return out


def warm_fv(fv: Any, points: Iterable[tuple[int, float]]) -> None:
    """Feed (source ts ns, value) points to FairValueModel.update, in time order."""
    for ts, v in points:
        fv.update(int(ts), float(v))


async def backfill_fair_value(
    rest: Any,
    fv: Any,
    now_ns: int,
    cfg: BackfillCfg,
    *,
    clock_ns: Callable[[], int] | None = None,
) -> BackfillResult:
    """Fetch, down-sample and feed the benchmark history; see the module docstring."""
    step = int(cfg.step_s) * NS_PER_S
    start = now_ns - int(cfg.days * 86400 * NS_PER_S)
    requested = max(1, (now_ns - start) // step)
    if not cfg.enabled:
        return BackfillResult([], requested, 0.0, bool(getattr(fv, "ready", False)), "disabled")
    ticks, n_req, errors = await fetch_benchmark_history(rest, now_ns, cfg, clock_ns=clock_ns)
    points = resample(ticks, start, now_ns, cfg.step_s)
    coverage = len(points) / requested
    if points and coverage >= cfg.min_coverage:
        warm_fv(fv, points)
        source = "cfbenchmarks_rest"
    else:
        if points:
            errors.append(f"coverage {coverage:.2%} < {cfg.min_coverage:.0%}: history not used")
        points = []
        source = "none"
    return BackfillResult(points, requested, coverage, bool(getattr(fv, "ready", False)), source, n_req, errors)


# ============================================================================ paper simulator
class PaperFees:
    """fee_fn for KalshiExchangeSim, which does not pass the ticker: the most expensive of
    the universe's (supported) fee schedules, i.e. paper P&L is never flattered by fees."""

    def __init__(self, fee_engine: Any) -> None:
        self.fee_engine = fee_engine
        self.schedules: dict[tuple[str, float], Any] = {}

    def add_specs(self, specs: Iterable[MarketSpec]) -> None:
        for s in specs:
            if not s.fee_type or (s.fee_type, s.fee_multiplier) in self.schedules:
                continue
            try:
                sched = self.fee_engine.schedule_for_spec(s.fee_type, s.fee_multiplier)
            except Exception:  # noqa: BLE001 - unsupported: the strategy will not quote it
                continue
            if getattr(sched, "supported", True):
                self.schedules[(s.fee_type, s.fee_multiplier)] = sched

    def __call__(self, px: int, qty: int, is_taker: bool) -> int:
        if not self.schedules:
            return 0
        return max(s.trade_fee_micros(px, qty, is_taker) for s in self.schedules.values())


def build_paper_sim(cfg: PaperCfg, specs: Iterable[MarketSpec], fee_engine: Any) -> tuple[Any, PaperFees]:
    """KalshiExchangeSim for paper mode (policy C by default, seeded latency)."""
    from dh.execution.exchange_sim import KalshiExchangeSim
    from dh.execution.latency import LatencyModel, LogNormal

    if cfg.sigma > 0:
        lat = LatencyModel(cfg.seed, submit=LogNormal(cfg.submit_ms, cfg.sigma), response=LogNormal(cfg.response_ms, cfg.sigma),
                           ws=LogNormal(cfg.ws_ms, cfg.sigma), md=0.0)
    else:
        lat = LatencyModel.fixed(cfg.submit_ms, cfg.response_ms, cfg.ws_ms, seed=cfg.seed)
    specs = list(specs)
    fees = PaperFees(fee_engine)
    fees.add_specs(specs)
    sim = KalshiExchangeSim(lat, cfg.policy, fees, seed=cfg.seed, latency_multiplier=cfg.latency_multiplier,
                            id_prefix="paper")
    for s in specs:
        sim.register_market(s)
    return sim, fees
