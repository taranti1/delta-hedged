"""Start-up helpers shared by live and paper mode (all offline-testable with fake REST).

    status  = await exchange_status(rest, shards=(2,))       # the shards' own trading status
    sel     = await discover_universe(rest, ("KXBTCD",), fee_engine, now_ns, horizon_s, exchange_indexes=(2,))
    closures, notes = schedule_closures(await rest.get_exchange_schedule(), start_ns, end_ns)
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
def shard_status(body: Any, shards: Iterable[int] = ()) -> dict[str, Any]:
    """ExchangeStatus -> the status that applies to ``shards``.

    openapi 3.31.0: the top-level ``exchange_active`` / ``trading_active`` describe the DEFAULT
    shard (0); ``exchange_index_statuses`` (absent when the breakdown is unavailable) has one
    entry per shard. For each of ``shards`` its entry is used, else the top level (noted in
    ``source``); the result is active only if every shard is. ``exchange_active`` false is an
    exchange pause (cancels are rejected too); ``trading_active`` false alone is a trading pause
    (cancels still work)."""
    out = dict(body or {})
    top_ex = bool(out.get("exchange_active", False))
    top_tr = bool(out.get("trading_active", False))
    per: dict[int, dict[str, Any]] = {}
    for e in out.get("exchange_index_statuses") or []:
        if isinstance(e, dict):
            try:
                per[int(e.get("exchange_index"))] = e
            except (TypeError, ValueError):
                continue
    ex, tr = True, True
    src: dict[str, str] = {}
    shards = sorted(set(shards))
    for sh in shards:
        e = per.get(sh)
        if e is not None:
            ex = ex and bool(e.get("exchange_active", False))
            tr = tr and bool(e.get("trading_active", False))
            src[str(sh)] = "exchange_index_statuses"
        else:
            ex, tr = ex and top_ex, tr and top_tr
            src[str(sh)] = "top_level (no per-shard entry)"
    if not shards:
        ex, tr = top_ex, top_tr
    out["exchange_active"] = ex
    out["trading_active"] = ex and tr
    out["shards"] = shards
    out["source"] = src
    return out


async def exchange_status(rest: Any, shards: Iterable[int] = ()) -> dict[str, Any]:
    """GET /exchange/status -> ``shard_status`` for ``shards`` (booleans coerced)."""
    return shard_status(await rest.get_exchange_status(), shards)


def required_balance_usd(risk: Any, margin_usd: float) -> float:
    """Collateral each traded shard must hold: the strategy's worst-case total loss (the larger
    of ``risk.max_total_worst_loss`` and the daily-loss halt, config/m1.yaml) plus a margin."""
    worst = max(float(getattr(risk, "max_total_worst_loss", 0.0) or 0.0), float(getattr(risk, "daily_loss_halt", 0.0) or 0.0))
    return worst + max(0.0, float(margin_usd))


PROBE_SUBACCOUNT = 0  # a subaccount a key restricted to ours must NOT be able to read (the primary)


async def verify_key_restriction(rest: Any, key_id: str, subaccount: int,
                                 balance_bodies: Iterable[Any] = ()) -> tuple[bool, str]:
    """Is the runner's API key restricted to ``subaccount`` (``venue.key_restricted_to_subaccount``
    makes a private WS message without a subaccount field count as ours, so it must be true)?

    POSITIVE proof (review M1): a read-only probe the restricted key must be REFUSED,
    ``GET /portfolio/balance?subaccount=0`` (another subaccount's balance; Kalshi: naming any
    other subaccount is rejected for a restricted key), must answer HTTP 403 (a scope refusal).
    A 2xx (the key can read subaccount 0: unrestricted) refuses the start; a 401 is an
    AUTHENTICATION failure (key rejected, signature / clock wrong), not a scope refusal, and
    refuses too ("key rejected, not proven restricted", review NEW-6); any other failure (5xx,
    429, network) proves nothing and refuses too (retry the start later).
    Secondary evidence, never a substitute: GET /api_keys, when it lists the key, must show it
    restricted to ``subaccount`` (a listed unrestricted key or another subaccount refuses);
    GET /api_keys failing is expected for a restricted key and only noted. The balance bodies'
    ``balance_breakdown`` is noted, never trusted. Returns (ok, evidence); key ids are never logged."""
    from dh.kalshi.rest import KalshiHTTPError

    if int(subaccount) == PROBE_SUBACCOUNT:
        return False, "a key restricted to subaccount 0 cannot be proven with the subaccount-0 probe"
    notes: list[str] = []
    try:
        body = await rest.get_api_keys()
        listed = False
        for k in (body or {}).get("api_keys") or []:
            if isinstance(k, dict) and key_id and str(k.get("api_key_id")) == key_id:
                listed = True
                s = k.get("subaccount")
                if s is None:
                    return False, "GET /api_keys: the runner key is NOT restricted to a subaccount"
                if int(s) != int(subaccount):
                    return False, f"GET /api_keys: the runner key is restricted to subaccount {s}, not {subaccount}"
                notes.append(f"GET /api_keys: restricted to subaccount {s}")
        if not listed:
            notes.append("GET /api_keys does not list the runner key")
    except Exception as exc:  # noqa: BLE001 - expected for a restricted key: secondary evidence only
        notes.append(f"GET /api_keys failed ({type(exc).__name__}{': HTTP ' + str(exc.status) if isinstance(exc, KalshiHTTPError) else ''})")
    bodies = [b for b in balance_bodies if isinstance(b, dict)]
    if bodies:
        notes.append("balance_breakdown " + ("absent" if all("balance_breakdown" not in b for b in bodies) else "PRESENT"))
    try:
        await rest.get_balance(subaccount=PROBE_SUBACCOUNT)
    except KalshiHTTPError as exc:
        if exc.status == 403:
            return True, (f"GET /portfolio/balance?subaccount={PROBE_SUBACCOUNT} refused with HTTP 403 (the key "
                          f"cannot read another subaccount); " + "; ".join(notes))
        if exc.status == 401:
            return False, (f"GET /portfolio/balance?subaccount={PROBE_SUBACCOUNT} answered HTTP 401: key rejected, not "
                           f"proven restricted (an authentication failure is not a scope refusal: check the key, its "
                           f"signature and the clock, then retry); " + "; ".join(notes))
        return False, (f"GET /portfolio/balance?subaccount={PROBE_SUBACCOUNT} failed with HTTP {exc.status}, not 403: "
                       f"no proof the key is restricted (retry the start); " + "; ".join(notes))
    except Exception as exc:  # noqa: BLE001 - network / transport: no proof
        return False, (f"GET /portfolio/balance?subaccount={PROBE_SUBACCOUNT} failed ({type(exc).__name__}): no proof the "
                       f"key is restricted (retry the start); " + "; ".join(notes))
    return False, (f"GET /portfolio/balance?subaccount={PROBE_SUBACCOUNT} ANSWERED with the runner key: it is NOT "
                   f"restricted to subaccount {subaccount}; " + "; ".join(notes))


_WEEKDAYS = ("monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday")
SCHEDULE_TZ = "America/New_York"  # Schedule.standard_hours: "All times are expressed in ET"


def _hhmm(v: Any, *, close: bool = False) -> int | None:
    """'HH:MM' (ET) -> minutes after midnight; a CLOSING time of 00:00, 23:59 or 24:00 = the end
    of the day (midnight of the next day): a session "05:00-00:00" runs to midnight, and one
    written "00:00-00:00" is the whole day (review L3: a 00:00 close was dropped, leaving a
    bogus closure)."""
    try:
        h, m = str(v).strip().split(":")[:2]
        mins = int(h) * 60 + int(m)
    except (TypeError, ValueError):
        return None
    if close and (mins == 0 or mins >= 23 * 60 + 59):
        return 24 * 60
    return mins if 0 <= mins <= 24 * 60 else None


def _merge(iv: list[tuple[int, int]], gap_ns: int = 0) -> list[tuple[int, int]]:
    out: list[tuple[int, int]] = []
    for a, b in sorted(iv):
        if out and a <= out[-1][1] + gap_ns:
            out[-1] = (out[-1][0], max(out[-1][1], b))
        else:
            out.append((a, b))
    return out


def schedule_closures(body: Any, start_ns: int, end_ns: int, *, min_gap_s: float = 120.0,
                      tz_name: str = SCHEDULE_TZ, now_ns: int | None = None) -> tuple[list[tuple[int, int, str]], list[str]]:
    """GET /exchange/schedule -> the intervals in [start_ns, end_ns) when trading is scheduled to
    be unavailable: (start ns, end ns, why), merged and sorted, plus notes.

    * ``maintenance_windows`` (start_datetime / end_datetime) are taken as given;
    * ``standard_hours``: each WeeklySchedule, within its own [start_time, end_time), lists the
      ET trading sessions per weekday; the time between sessions (sessions closer than
      ``min_gap_s`` are joined, so a 23:59 / 00:00 close and a 00:00 open are continuous) is a
      closure. This is how the weekly Thursday 03:00-05:00 ET trading pause is expected to appear.
    A standard-hours reading that would close more than half of the NEXT day (from ``now_ns``,
    default ``start_ns``) is implausible for 24/7 crypto markets: it is ignored (noted); the
    live status poll stays authoritative."""
    import datetime as dt

    from dh.kalshi.wire import opt_iso_to_ns

    sched = body.get("schedule", body) if isinstance(body, dict) else {}
    notes: list[str] = []
    maint: list[tuple[int, int]] = []
    for w in sched.get("maintenance_windows") or []:
        if not isinstance(w, dict):
            continue
        try:
            a, b = opt_iso_to_ns(w.get("start_datetime")), opt_iso_to_ns(w.get("end_datetime"))
        except (TypeError, ValueError):
            notes.append(f"unparseable maintenance window {w!r:.120}")
            continue
        if a and b and b > a and b > start_ns and a < end_ns:
            maint.append((a, b))
    std: list[tuple[int, int]] = []
    weeks = [w for w in sched.get("standard_hours") or [] if isinstance(w, dict)]
    if weeks:
        try:
            from zoneinfo import ZoneInfo

            tz = ZoneInfo(tz_name)
        except Exception as exc:  # noqa: BLE001 - no tz database: maintenance windows only
            notes.append(f"standard_hours ignored: no time zone {tz_name} ({type(exc).__name__})")
            weeks = []
        gap = int(min_gap_s * 1e9)
        for wk in weeks:
            try:
                ws = opt_iso_to_ns(wk.get("start_time")) or 0
                we = opt_iso_to_ns(wk.get("end_time")) or 2**62
            except (TypeError, ValueError):
                notes.append("standard_hours entry with unparseable start/end ignored")
                continue
            lo, hi = max(ws, start_ns), min(we, end_ns)
            if lo >= hi:
                continue
            d0 = dt.datetime.fromtimestamp(lo / 1e9, tz).date() - dt.timedelta(days=1)
            d1 = dt.datetime.fromtimestamp(hi / 1e9, tz).date() + dt.timedelta(days=1)
            opens: list[tuple[int, int]] = []
            d = d0
            while d <= d1:
                for sess in wk.get(_WEEKDAYS[d.weekday()]) or []:
                    if not isinstance(sess, dict):
                        continue
                    o, c = _hhmm(sess.get("open_time")), _hhmm(sess.get("close_time"), close=True)
                    if o is None or c is None or c <= o:
                        continue
                    base = dt.datetime(d.year, d.month, d.day, tzinfo=tz)
                    a = int((base + dt.timedelta(minutes=o)).timestamp() * 1e9)
                    b = int((base + dt.timedelta(minutes=c)).timestamp() * 1e9)
                    opens.append((a, b))
                d += dt.timedelta(days=1)
            cur = lo
            for a, b in _merge(opens, gap):
                if b <= cur:
                    continue
                if a > cur + gap:
                    std.append((cur, min(a, hi)))
                cur = max(cur, b)
                if cur >= hi:
                    break
            if cur < hi:
                std.append((cur, hi))
        t0 = start_ns if now_ns is None else int(now_ns)
        day = [(max(a, t0), min(b, t0 + 86_400 * 10**9)) for a, b in std]
        closed = sum(max(0, b - a) for a, b in day)
        if closed > 43_200 * 10**9:
            notes.append(f"standard_hours ignored: would close {closed / 3.6e12:.1f} h of the next 24 h")
            std = []
    merged = _merge(maint + [(a, b) for a, b in std if b > a])
    res = [(a, b, "maintenance" if any(ma < b and mb > a for ma, mb in maint) else "standard_hours")
           for a, b in merged]
    return res, notes


def closure_at(closures: Iterable[tuple[int, int, str]], now_ns: int, lead_ns: int = 0) -> tuple[int, int, str] | None:
    """The scheduled closure in force at ``now_ns`` or starting within ``lead_ns`` (None)."""
    for a, b, why in closures:
        if a - lead_ns <= now_ns < b:
            return a, b, why
    return None


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
    """Everything that changes what a quote is worth or whether it is valid. The fee enters as
    the BASE fee (series / market, without event overrides): an event override is applied by
    the KalshiFeeUpdate event (strategy and runner follow it), so it must not make a re-discovered
    market look changed (which would block it for the session)."""
    return (spec.strike_type, spec.floor_strike, spec.cap_strike, spec.close_ts, spec.expiration_ts,
            spec.price_ranges, spec.base_fee, spec.settlement, spec.exchange_index)


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
    exchange_indexes: Iterable[int] | None = None,
) -> UniverseSelection:
    """Markets to trade: in ``series``, not closed, expiring within ``now + horizon_s``, with
    clean rules text (metadata.BLOCKING_FLAGS), an open status, not paused, not in an excluded
    event, and on a KNOWN exchange shard (in ``exchange_indexes`` when given: the shards the
    runner has funded and creates order groups on). Markets with an unresolved / unsupported
    fee type ARE included: the MarketMaker keeps them untradable itself (it never assumes a
    fee schedule). ``known`` markets are not returned again; their spec changes (a shard move
    included) are reported in ``changed``."""
    series_set = set(series)
    excluded = set(exclude_events)
    shards = None if exchange_indexes is None else {int(x) for x in exchange_indexes}
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
        if spec.exchange_index is None:
            skipped[t] = "exchange shard unknown (no exchange_index on market/event/series): not traded"
            continue
        if shards is not None and spec.exchange_index not in shards:
            skipped[t] = f"exchange shard {spec.exchange_index} not in venue.exchange_indexes {sorted(shards)}"
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
    exchange_indexes: Iterable[int] | None = None,
) -> UniverseSelection:
    """REST discovery (dh.kalshi.metadata.discover_markets on open events) + select_specs."""
    series = tuple(series)
    reg = await discover_markets(rest, series, status="open", fee_engine=fee_engine)
    return select_specs(reg, now_ns, horizon_s, series=series, exclude_events=exclude_events, known=known,
                        exchange_indexes=exchange_indexes)


def series_of_ticker(ticker: str) -> str:
    """Series of a market ticker (``KXBTCD-26SEP2513-T84000.00`` -> ``KXBTCD``)."""
    return parse_market_ticker(ticker).series


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


def _iso_ms(t_ns: int) -> str:
    """UTC ISO-8601 with milliseconds and 'Z' (CF Benchmarks timestamp format)."""
    import datetime as _dt

    d = _dt.datetime.fromtimestamp(t_ns // NS_PER_MS / 1000, tz=_dt.timezone.utc)
    return d.strftime("%Y-%m-%dT%H:%M:%S.") + f"{(t_ns // NS_PER_MS) % 1000:03d}Z"


def _fill_template(tpl: str, start_ns: int, end_ns: int) -> str:
    return tpl.format(start_ms=start_ns // NS_PER_MS, end_ms=end_ns // NS_PER_MS, start_s=start_ns // NS_PER_S,
                      end_s=end_ns // NS_PER_S, span_s=(end_ns - start_ns) // NS_PER_S,
                      span_ms=(end_ns - start_ns) // NS_PER_MS, start_iso=_iso_ms(start_ns), end_iso=_iso_ms(end_ns))


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
    align = bool(getattr(cfg, "align", False))
    recent_ns = int(float(getattr(cfg, "recent_delay_s", 0.0)) * NS_PER_S)
    # aligned: the newest chunk is the (partial) one containing end_ns, e.g. [18:00, 19:00)
    c_end = -(-end_ns // chunk_ns) * chunk_ns if align else end_ns
    while c_end > start_all:
        c_start = c_end - chunk_ns if align else max(start_all, c_end - chunk_ns)
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
            if recent_ns and c_end > end_ns - recent_ns:  # publication delay (CF: up to 15 min)
                errors.append(f"chunk ending {c_end // NS_PER_S}: no ticks yet (recent; skipped)")
                c_end = c_start
                continue
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
    """Fees for the paper simulator.

    ``order_fee`` (KalshiExchangeSim ``order_fee_fn``): the market's own schedule (the sim's
    order id gives the ticker; event fee overrides via ``set_fee``) through an
    OrderFeeAccumulator per order, i.e. the net fee including Kalshi's per-order balance
    rounding and carry, which the strategy prices too. ``__call__`` (the per-fill ``fee_fn``
    fallback, no ticker available): the most expensive schedule of the universe, so paper
    P&L is never flattered by fees."""

    def __init__(self, fee_engine: Any) -> None:
        self.fee_engine = fee_engine
        self.schedules: dict[tuple[str, float], Any] = {}
        self.by_ticker: dict[str, tuple[str, float]] = {}
        self.sim: Any = None
        self._accs: dict[str, Any] = {}

    def _ensure(self, key: tuple[str, float]) -> Any:
        if key in self.schedules:
            return self.schedules[key]
        try:
            sched = self.fee_engine.schedule_for_spec(key[0], key[1])
        except Exception:  # noqa: BLE001 - unsupported: the strategy will not quote it
            return None
        if not getattr(sched, "supported", True):
            return None
        self.schedules[key] = sched
        return sched

    def add_specs(self, specs: Iterable[MarketSpec]) -> None:
        for s in specs:
            if s.fee_type:
                self.by_ticker.setdefault(s.ticker, (s.fee_type, s.fee_multiplier))
                self._ensure((s.fee_type, s.fee_multiplier))

    def set_fee(self, ticker: str, fee_type: str, multiplier: float) -> None:
        """Event fee override in force for ``ticker`` (new orders use it)."""
        self.by_ticker[ticker] = (fee_type, multiplier)
        self._ensure((fee_type, multiplier))

    def __call__(self, px: int, qty: int, is_taker: bool) -> int:
        if not self.schedules:
            return 0
        return max(s.trade_fee_micros(px, qty, is_taker) for s in self.schedules.values())

    def order_fee(self, order_key: str, book_side: str, px: int, qty: int, is_taker: bool) -> int:
        acc = self._accs.get(order_key)
        if acc is None:
            o = self.sim.orders.get(order_key) if self.sim is not None else None
            key = self.by_ticker.get(o.ticker) if o is not None else None
            sched = self._ensure(key) if key is not None else None
            if sched is None:
                return self(px, qty, is_taker)
            acc = self._accs[order_key] = sched.order_accumulator(book_side)
        return int(acc.apply_fill(px, qty, is_taker).net_micros)


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
    sim = KalshiExchangeSim(lat, cfg.policy, fees, seed=cfg.seed, order_fee_fn=fees.order_fee,
                            latency_multiplier=cfg.latency_multiplier, id_prefix="paper")
    fees.sim = sim
    for s in specs:
        sim.register_market(s)
    return sim, fees
