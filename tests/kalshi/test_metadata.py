from __future__ import annotations

from decimal import Decimal
from typing import Any

from dh.core.events import KalshiFeeUpdate, KalshiMarketLifecycle
from dh.core.market import PriceRange
from dh.kalshi.fees import FeeEngine
from dh.kalshi.metadata import (
    MarketRegistry,
    SeriesBundle,
    discover_markets,
    parse_market_ticker,
    refresh_markets,
    rules_flags,
)
from dh.kalshi.wire import iso_to_ns

from . import samples as S

T = S.MARKET_KXBTCD["ticker"]
NOW = iso_to_ns("2025-08-05T20:30:00Z")
M15 = S.market(ticker="KXBTC15M-25AUG051715-15", event_ticker="KXBTC15M-25AUG051715", floor_strike=None,
               close_time="2025-08-05T21:15:00Z", expected_expiration_time="2025-08-05T21:15:00Z")
SERIES_15M = dict(S.SERIES_KXBTCD, ticker="KXBTC15M")
EVENT_15M = dict(S.EVENT_KXBTCD, event_ticker="KXBTC15M-25AUG051715", series_ticker="KXBTC15M")


def lc(event_type: str, **kw: Any) -> KalshiMarketLifecycle:
    return KalshiMarketLifecycle(ts=NOW, ts_exch=0, ticker=kw.pop("ticker", T), event_type=event_type, **kw)


def registry() -> MarketRegistry:
    return MarketRegistry.from_bundles(
        [SeriesBundle(S.SERIES_KXBTCD, [S.EVENT_KXBTCD], [S.MARKET_KXBTCD]),
         SeriesBundle(SERIES_15M, [EVENT_15M], [M15])],
        FeeEngine(),
    )


def test_parse_ticker_and_rules_flags():
    tp = parse_market_ticker(T)
    assert (tp.series, tp.event_ticker, tp.strike_code, tp.strike_value) == ("KXBTCD", "KXBTCD-25AUG0517", "T", Decimal("114999.99"))
    assert parse_market_ticker("KXBTC15M-26APR160100-00").strike_value == Decimal("0")
    assert rules_flags(S.MARKET_KXBTCD) == []
    alt = S.market(rules_primary="If the average BRTI over the one minute before 5 PM EDT is above 114999.99, Yes.")
    assert rules_flags(alt) == []
    bad = S.market(rules_primary="Resolves Yes if BTC trades above 115000 at any time.", floor_strike=115000.0)
    assert set(rules_flags(bad)) == {"rules_no_average", "rules_no_brti", "rules_no_60s_window", "ticker_strike_mismatch"}
    info = S.market(close_time="2025-08-05T20:59:00Z", fee_waiver_expiration_time="2025-08-06T00:00:00Z")
    assert rules_flags(info) == ["expiration_differs_from_close", "fee_waiver_present"]


def test_registry_build_rejections_and_tradability():
    reg = registry()
    assert set(reg.specs) == {T}
    assert "KXBTC15M-25AUG051715-15" in reg.rejected and "floor_strike" in reg.rejected["KXBTC15M-25AUG051715-15"]
    assert reg.tradable(T, NOW) and not reg.tradable(T, iso_to_ns("2025-08-05T21:00:00Z"))
    assert reg.active_tickers(NOW) == [T] and reg.active_tickers(NOW, series="KXBTC") == []
    assert reg.fee_schedule(T).fee_type == "quadratic" and reg.fee_schedule(T).source == "series"
    reg.flags[T] = ["fee_waiver_present"]  # informational flags do not block
    assert reg.tradable(T, NOW)
    reg.flags[T] = ["rules_no_brti"]
    assert not reg.tradable(T, NOW)


def test_lifecycle_updates():
    reg = registry()
    reg.on_event(lc("deactivated", is_deactivated=True))
    assert T in reg.paused and not reg.tradable(T, NOW)
    reg.on_event(lc("activated", is_deactivated=False))
    assert reg.tradable(T, NOW)
    new_close = iso_to_ns("2025-08-05T20:45:00Z")
    notes = reg.on_event(lc("close_date_updated", close_ts=new_close))
    assert reg.specs[T].close_ts == new_close and T in reg.needs_refresh and notes
    reg.on_event(lc("price_level_structure_updated", price_level_structure="deci_cent", price_ranges=((0, 10000, 10),)))
    assert reg.specs[T].price_ranges == (PriceRange(0, 10000, 10),) and reg.specs[T].is_valid_px(4510)
    reg.on_event(lc("determined", result="yes"))
    assert reg.status[T] == "determined" and not reg.tradable(T, NOW)
    reg.on_event(lc("metadata_updated"))
    assert T in reg.needs_refresh
    assert reg.on_event(KalshiFeeUpdate(ts=0, ts_exch=0, event_ticker="X", fee_type_override=None, fee_multiplier_override=None)) == []


def test_fee_override_event_rebuilds_specs():
    reg = registry()
    notes = reg.on_event(KalshiFeeUpdate(ts=NOW, ts_exch=0, event_ticker="KXBTCD-25AUG0517",
                                         fee_type_override="quadratic_with_maker_fees", fee_multiplier_override="0.5"))
    assert reg.specs[T].fee_type == "quadratic_with_maker_fees" and reg.specs[T].fee_multiplier == 0.5
    sched = reg.fee_schedule(T)
    assert sched.source == "event_override" and sched.trade_fee_micros(5000, 100, False) == 2188
    assert notes and "event_override" in notes[0]
    reg.on_event(KalshiFeeUpdate(ts=NOW, ts_exch=0, event_ticker="KXBTCD-25AUG0517", fee_type_override=None, fee_multiplier_override=None))
    assert reg.specs[T].fee_type == "quadratic" and reg.fee_schedule(T).source == "series"


def test_metadata_update_sets_strike_and_builds_spec():
    reg = registry()
    t15 = "KXBTC15M-25AUG051715-15"
    notes = reg.apply_metadata_update({"event_type": "metadata_updated", "market_ticker": t15,
                                       "strike_type": "greater", "floor_strike": 115321.5})
    assert notes and t15 in reg.specs and reg.specs[t15].floor_strike == 115321.5
    assert t15 not in reg.rejected
    assert reg.apply_metadata_update({"event_type": "metadata_updated", "market_ticker": t15, "floor_strike": 115321.5}) == []
    assert reg.apply_metadata_update({"market_ticker": "UNKNOWN-1"}) and "UNKNOWN-1" in reg.needs_refresh


class FakeRest:
    def __init__(self) -> None:
        self.calls: list[str] = []

    async def get_series(self, s: str) -> dict:
        self.calls.append(f"series:{s}")
        return {"series": {"KXBTCD": S.SERIES_KXBTCD, "KXBTC15M": SERIES_15M}[s]}

    async def iter_events(self, **kw: Any):
        self.calls.append(f"events:{kw['series_ticker']}:{kw['status']}:{kw['with_nested_markets']}")
        if kw["series_ticker"] == "KXBTCD":
            yield dict(S.EVENT_KXBTCD, markets=[S.MARKET_KXBTCD])
        else:
            yield dict(EVENT_15M, markets=[M15])

    async def get_series_fee_changes(self, s: str) -> dict:
        return {"series_fee_change_arr": []}

    async def get_market(self, t: str) -> dict:
        self.calls.append(f"market:{t}")
        return {"market": dict(M15, floor_strike=115000.0)}

    async def get_event(self, e: str) -> dict:
        self.calls.append(f"event:{e}")
        return {"event": EVENT_15M, "markets": []}


async def test_discovery_and_refresh():
    rest = FakeRest()
    reg = await discover_markets(rest, ("KXBTCD", "KXBTC15M"), fee_engine=FeeEngine())
    assert "events:KXBTCD:open:True" in rest.calls and set(reg.specs) == {T}
    t15 = "KXBTC15M-25AUG051715-15"
    reg.needs_refresh.add(t15)
    assert await refresh_markets(reg, rest) == [t15]
    assert t15 in reg.specs and not reg.needs_refresh
