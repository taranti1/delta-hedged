"""Kalshi BTC market discovery, MarketSpec registry, rule sanity checks, lifecycle tracking.

    reg = await discover_markets(rest, ("KXBTCD", "KXBTC", "KXBTC15M"), fee_engine=FeeEngine())
    for ev in events: reg.on_event(ev)          # KalshiMarketLifecycle / KalshiFeeUpdate
    await refresh_markets(reg, rest)            # re-read markets flagged needs_refresh

Specs come from dh.kalshi.normalize.rest_market_to_spec (strikes, times -> ns, tick grid
from price_ranges, fee type/multiplier with event > series precedence). Markets that cannot
be modeled are kept in ``rejected`` with the reason (e.g. KXBTC15M before its strike is set).

``rules_flags(market)`` sanity-checks the contract text: the settlement model assumes the
simple average of 60 one-second BRTI prints before expiration; a market whose rules do not
mention an average, BRTI/CF Benchmarks and a 60-second window is flagged and should not be
traded until a human has read the rules.
"""

from __future__ import annotations

import dataclasses
import re
from collections.abc import Iterable
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from typing import Any

from dh.core.events import Event, KalshiFeeUpdate, KalshiMarketLifecycle
from dh.core.market import MarketSpec, PriceRange
from dh.kalshi.fees import FeeEngine, FeeSchedule, resolve_fee_fields
from dh.kalshi.normalize import UnsupportedMarket, rest_market_to_spec
from dh.kalshi.wire import as_dict, opt_iso_to_ns

BTC_SERIES = ("KXBTCD", "KXBTC", "KXBTC15M")
# Flags that make a market untradable until a human has checked it; others are informational.
BLOCKING_FLAGS = frozenset(
    {
        "rules_no_average",
        "rules_no_brti",
        "rules_no_60s_window",
        "no_expected_expiration_time",
        "ticker_strike_mismatch",
    }
)
_RX_60S = re.compile(r"\b(60|sixty)[\s-]*seconds?\b|\b(one|1)[\s-]*minute\b")


# ============================================================================ parsing / checks
@dataclass(frozen=True, slots=True)
class TickerParts:
    """'KXBTCD-25AUG0517-T114999.99' -> series 'KXBTCD', event 'KXBTCD-25AUG0517',
    strike_code 'T', strike_value Decimal('114999.99'). Informational only: the market's
    floor_strike/cap_strike fields are authoritative."""

    series: str
    event_ticker: str
    suffix: str
    strike_code: str
    strike_value: Decimal | None


def parse_market_ticker(ticker: str) -> TickerParts:
    parts = ticker.split("-")
    series = parts[0]
    event = "-".join(parts[:-1]) if len(parts) > 1 else ticker
    suffix = parts[-1] if len(parts) > 1 else ""
    m = re.match(r"^([A-Za-z]*)(-?\d+(?:\.\d+)?)$", suffix)
    code, value = "", None
    if m:
        code = m.group(1)
        try:
            value = Decimal(m.group(2))
        except InvalidOperation:
            value = None
    return TickerParts(series, event, suffix, code, value)


def rules_flags(market: dict[str, Any]) -> list[str]:
    """Contract-text / field sanity flags ([] = consistent with the BRTI 60 s average model)."""
    flags: list[str] = []
    text = f"{market.get('rules_primary') or ''} {market.get('rules_secondary') or ''}".lower()
    if "averag" not in text:
        flags.append("rules_no_average")
    if not any(k in text for k in ("brti", "real-time index", "real time index", "cf benchmarks")):
        flags.append("rules_no_brti")
    if not _RX_60S.search(text):
        flags.append("rules_no_60s_window")
    if not market.get("expected_expiration_time"):
        flags.append("no_expected_expiration_time")
    elif market.get("close_time") and opt_iso_to_ns(market["expected_expiration_time"]) != opt_iso_to_ns(market["close_time"]):
        flags.append("expiration_differs_from_close")
    if market.get("market_type") not in (None, "binary"):
        flags.append(f"market_type_{market.get('market_type')}")
    if market.get("fee_waiver_expiration_time"):
        flags.append("fee_waiver_present")
    tp = parse_market_ticker(str(market.get("ticker", "")))
    st = market.get("strike_type")
    if tp.strike_value is not None and tp.strike_code == "T" and st in ("greater", "greater_or_equal"):
        fs = market.get("floor_strike")
        if fs is not None and Decimal(repr(fs) if isinstance(fs, float) else str(fs)) != tp.strike_value:
            flags.append("ticker_strike_mismatch")
    return flags


# ============================================================================ registry
@dataclass
class SeriesBundle:
    """One series with its events (EventData without nested markets) and markets."""

    series: dict[str, Any]
    events: list[dict[str, Any]] = field(default_factory=list)
    markets: list[dict[str, Any]] = field(default_factory=list)
    fee_changes: list[dict[str, Any]] = field(default_factory=list)


class MarketRegistry:
    """Current MarketSpecs + raw metadata + lifecycle state, updated from events."""

    def __init__(self, fee_engine: FeeEngine | None = None) -> None:
        self.fee_engine = fee_engine
        self.specs: dict[str, MarketSpec] = {}
        self.markets: dict[str, dict[str, Any]] = {}
        self.events: dict[str, dict[str, Any]] = {}
        self.series: dict[str, dict[str, Any]] = {}
        self.rejected: dict[str, str] = {}
        self.flags: dict[str, list[str]] = {}
        self.status: dict[str, str] = {}
        self.paused: set[str] = set()
        self.needs_refresh: set[str] = set()
        self.log: list[str] = []

    # ------------------------------------------------------------------ building
    @classmethod
    def from_bundles(cls, bundles: Iterable[SeriesBundle], fee_engine: FeeEngine | None = None) -> MarketRegistry:
        reg = cls(fee_engine)
        for b in bundles:
            reg.add_series(b.series)
            for ev in b.events:
                reg.add_event(ev)
            for m in b.markets:
                reg.add_market(m)
        return reg

    def add_series(self, series: dict[str, Any]) -> None:
        self.series[str(series["ticker"])] = dict(series)

    def add_event(self, event: dict[str, Any]) -> None:
        ev = {k: v for k, v in event.items() if k != "markets"}
        self.events[str(ev["event_ticker"])] = ev

    def add_market(self, market: dict[str, Any]) -> MarketSpec | None:
        """Register/replace a raw Market; returns its spec or None (reason in rejected)."""
        t = str(market["ticker"])
        self.markets[t] = dict(market)
        self.status[t] = str(market.get("status") or "")
        self.flags[t] = rules_flags(market)
        self.needs_refresh.discard(t)
        return self._rebuild(t)

    def _context(self, market: dict[str, Any]) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
        event = self.events.get(str(market.get("event_ticker") or ""))
        series_t = (event or {}).get("series_ticker") or str(market.get("event_ticker", "")).split("-", 1)[0]
        return self.series.get(str(series_t)), event

    def _rebuild(self, ticker: str) -> MarketSpec | None:
        market = self.markets[ticker]
        series, event = self._context(market)
        try:
            spec = rest_market_to_spec(market, series, event)
        except UnsupportedMarket as exc:
            self.specs.pop(ticker, None)
            self.rejected[ticker] = str(exc)
            return None
        self.rejected.pop(ticker, None)
        self.specs[ticker] = spec
        return spec

    def fee_schedule(self, ticker: str) -> FeeSchedule:
        """Resolved fee schedule for a registered market (needs a FeeEngine)."""
        if self.fee_engine is None:
            raise RuntimeError("registry has no FeeEngine")
        market = self.markets[ticker]
        series, event = self._context(market)
        return self.fee_engine.schedule_for(series, event, market)

    # ------------------------------------------------------------------ queries
    def blocking_flags(self, ticker: str) -> list[str]:
        """Flags that block trading (rules text / strike inconsistencies, non-binary)."""
        return [f for f in self.flags.get(ticker, []) if f in BLOCKING_FLAGS or f.startswith("market_type_")]

    def tradable(self, ticker: str, now_ns: int) -> bool:
        """Spec'd, rules clean, not paused, status open/active, not yet closed."""
        spec = self.specs.get(ticker)
        if spec is None or ticker in self.paused or self.blocking_flags(ticker):
            return False
        if self.status.get(ticker) not in ("", "active", "open", "initialized"):
            return False
        return spec.open_ts <= now_ns < spec.close_ts

    def active_tickers(self, now_ns: int, series: str | None = None) -> list[str]:
        return sorted(
            t for t, s in self.specs.items() if (series is None or s.series_ticker == series) and self.tradable(t, now_ns)
        )

    # ------------------------------------------------------------------ updates
    def on_event(self, ev: Event) -> list[str]:
        """Apply a lifecycle / fee-override event; returns human-readable change notes."""
        if isinstance(ev, KalshiMarketLifecycle):
            notes = self._on_lifecycle(ev)
        elif isinstance(ev, KalshiFeeUpdate):
            notes = self._on_fee_update(ev)
        else:
            return []
        self.log.extend(notes)
        return notes

    def _on_lifecycle(self, ev: KalshiMarketLifecycle) -> list[str]:
        t = ev.ticker
        et = ev.event_type
        notes: list[str] = []
        if et == "created":
            self.needs_refresh.add(t)
            notes.append(f"{t}: created -> refresh")
        elif et == "activated":
            self.paused.discard(t)
            notes.append(f"{t}: activated")
        elif et == "deactivated":
            self.paused.add(t)
            notes.append(f"{t}: deactivated (paused)")
        elif et == "close_date_updated":
            spec = self.specs.get(t)
            if spec is not None and ev.close_ts and ev.close_ts != spec.close_ts:
                self.specs[t] = dataclasses.replace(spec, close_ts=ev.close_ts)
                notes.append(f"{t}: close_ts {spec.close_ts} -> {ev.close_ts}")
            self.needs_refresh.add(t)  # expected_expiration_time may have moved too
        elif et in ("determined", "settled"):
            self.status[t] = et
            notes.append(f"{t}: {et} result={ev.result!r}")
        elif et == "price_level_structure_updated":
            spec = self.specs.get(t)
            if ev.price_ranges and spec is not None:
                ranges = tuple(PriceRange(s, e, st) for s, e, st in ev.price_ranges)
                if ranges != spec.price_ranges:
                    self.specs[t] = dataclasses.replace(spec, price_ranges=ranges)
                    notes.append(f"{t}: tick grid -> {ev.price_level_structure} {ev.price_ranges}")
            elif not ev.price_ranges:
                self.needs_refresh.add(t)
        elif et == "metadata_updated":
            self.needs_refresh.add(t)
            notes.append(f"{t}: metadata updated -> refresh")
        if ev.is_deactivated is not None:
            (self.paused.add if ev.is_deactivated else self.paused.discard)(t)
        return notes

    def _on_fee_update(self, ev: KalshiFeeUpdate) -> list[str]:
        event = self.events.setdefault(ev.event_ticker, {"event_ticker": ev.event_ticker})
        event["fee_type_override"] = ev.fee_type_override
        event["fee_multiplier_override"] = None if ev.fee_multiplier_override is None else Decimal(ev.fee_multiplier_override)
        notes = []
        for t, m in self.markets.items():
            if m.get("event_ticker") == ev.event_ticker:
                self._rebuild(t)
                series, evd = self._context(m)
                ft, mult, src = resolve_fee_fields(series, evd, m)
                notes.append(f"{t}: fee -> {ft} x{mult} ({src})")
        return notes

    def apply_metadata_update(self, body: dict[str, Any]) -> list[str]:
        """Raw WS market_lifecycle_v2 'metadata_updated' (or 'created' additional_metadata)
        body: patch strike fields / subtitle into the raw market and rebuild its spec."""
        t = str(body.get("market_ticker") or "")
        m = self.markets.get(t)
        if m is None:
            self.needs_refresh.add(t)
            return [f"{t}: unknown market -> refresh"]
        src = as_dict(body.get("additional_metadata")) or body
        changed = []
        for k in ("strike_type", "floor_strike", "cap_strike", "custom_strike", "yes_sub_title", "rules_primary", "rules_secondary"):
            if k in src and m.get(k) != src[k]:
                m[k] = src[k]
                changed.append(k)
        if not changed:
            return []
        self.flags[t] = rules_flags(m)
        self._rebuild(t)
        note = f"{t}: metadata {', '.join(changed)} updated"
        self.log.append(note)
        return [note]


# ============================================================================ discovery (REST)
async def fetch_series_bundle(rest: Any, series_ticker: str, *, status: str | None = "open", with_fee_changes: bool = True) -> SeriesBundle:
    """GET /series/{s}, all events (status filter, nested markets), series fee changes."""
    series = (await rest.get_series(series_ticker))["series"]
    bundle = SeriesBundle(series=series)
    async for ev in rest.iter_events(series_ticker=series_ticker, status=status, with_nested_markets=True):
        markets = ev.get("markets") or []
        bundle.events.append({k: v for k, v in ev.items() if k != "markets"})
        for m in markets:
            m = dict(m)
            m.setdefault("event_ticker", ev.get("event_ticker"))
            bundle.markets.append(m)
    if with_fee_changes:
        body = await rest.get_series_fee_changes(series_ticker)
        bundle.fee_changes = list(body.get("series_fee_change_arr") or [])
    return bundle


async def discover_markets(
    rest: Any,
    series: Iterable[str] = BTC_SERIES,
    *,
    status: str | None = "open",
    fee_engine: FeeEngine | None = None,
) -> MarketRegistry:
    """Discover open events/markets for the given series and build a MarketRegistry."""
    bundles = [await fetch_series_bundle(rest, s, status=status) for s in series]
    return MarketRegistry.from_bundles(bundles, fee_engine)


async def refresh_markets(reg: MarketRegistry, rest: Any) -> list[str]:
    """Re-read every market in reg.needs_refresh via GET /markets/{t}; returns tickers done."""
    done = []
    for t in sorted(reg.needs_refresh):
        body = await rest.get_market(t)
        m = body.get("market")
        if isinstance(m, dict):
            et = str(m.get("event_ticker") or "")
            if et and et not in reg.events:
                ev = await rest.get_event(et)
                if isinstance(ev.get("event"), dict):
                    reg.add_event(ev["event"])
            reg.add_market(m)
            done.append(t)
    return done
