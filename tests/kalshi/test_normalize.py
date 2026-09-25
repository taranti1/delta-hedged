from __future__ import annotations

import copy
from decimal import Decimal

import orjson
import pytest

from dh.core.events import (
    FeedStatus,
    IndexTick,
    KalshiBookDelta,
    KalshiBookSnapshot,
    KalshiFeeUpdate,
    KalshiFill,
    KalshiMarketLifecycle,
    KalshiOrderUpdate,
    KalshiTicker,
    KalshiTrade,
    Settlement,
)
from dh.core.market import PriceRange, SettlementSpec
from dh.core.units import NS_PER_MS, NS_PER_S, UnitError
from dh.kalshi.normalize import (
    UnsupportedMarket,
    book_levels,
    book_side_of,
    cf_history_to_ticks,
    market_position,
    normalize_rest_record,
    order_group_update,
    order_to_update,
    rest_fill_to_event,
    rest_market_to_spec,
    rest_orderbook_to_snapshot,
    rest_trade_to_event,
    taker_outcome_side,
    ws_message_to_events,
)
from dh.kalshi.wire import epoch_to_ns, iso_to_ns, number_to_str

from . import samples as S

R = 1_000_000_000_000  # arbitrary recv_ns


def ex(examples, name, i=0):
    return copy.deepcopy(examples[name][i])


# ------------------------------------------------------------------------------ wire helpers
def test_iso_to_ns_variants():
    assert iso_to_ns("1970-01-01T00:00:01Z") == NS_PER_S
    assert iso_to_ns("2025-08-05T21:00:00Z") == 1754427600 * NS_PER_S
    assert iso_to_ns("2025-08-05T17:00:00-04:00") == 1754427600 * NS_PER_S
    assert iso_to_ns("2025-08-05T21:00:00.123456789123Z") == 1754427600 * NS_PER_S + 123456789
    assert iso_to_ns("2025-08-05T21:00:00.5+00:00") == 1754427600 * NS_PER_S + 500_000_000
    with pytest.raises(ValueError):
        iso_to_ns("yesterday")


def test_epoch_units_and_number_strings():
    assert epoch_to_ns(1710000000) == 1710000000 * NS_PER_S
    assert epoch_to_ns(1710000000123) == 1710000000123 * NS_PER_MS
    assert epoch_to_ns("1710000000123456") == 1710000000123456 * 1000
    assert epoch_to_ns("2024-03-09T16:00:00Z") == iso_to_ns("2024-03-09T16:00:00Z")
    assert number_to_str(1) == "1" and number_to_str(1.0) == "1" and number_to_str(0.25) == "0.25"
    assert number_to_str(None) is None


# ------------------------------------------------------------------------------ WS examples
def test_orderbook_snapshot_example(asyncapi_examples):
    (ev,) = ws_message_to_events(ex(asyncapi_examples, "orderbookSnapshot"), R)
    assert ev == KalshiBookSnapshot(
        ts=R, ts_exch=0, ticker="FED-23DEC-T3.00", sid=2, seq=2,
        yes_bids=((800, 30000), (2200, 33300)), no_bids=((5400, 2000), (5600, 14600)),
    )


def test_orderbook_snapshot_missing_side_and_yes_price_flip(asyncapi_examples):
    m = ex(asyncapi_examples, "orderbookSnapshot")
    del m["msg"]["no_dollars_fp"]
    (ev,) = ws_message_to_events(m, R)
    assert ev.no_bids == ()
    m = ex(asyncapi_examples, "orderbookSnapshot")
    (ev,) = ws_message_to_events(m, R, use_yes_price=True)  # NO side reported in YES pricing
    assert ev.no_bids == ((4400, 14600), (4600, 2000))


def test_orderbook_delta_example(asyncapi_examples):
    (ev,) = ws_message_to_events(ex(asyncapi_examples, "orderbookDelta"), R)
    assert ev == KalshiBookDelta(
        ts=R, ts_exch=1669149841000 * NS_PER_MS, ticker="FED-23DEC-T3.00", sid=2, seq=3,
        side="yes", px=9600, delta=-5400, own_client_order_id="",
    )
    m = ex(asyncapi_examples, "orderbookDelta")
    m["msg"].update(side="no", client_order_id="dhA-7")
    (ev,) = ws_message_to_events(m, R, use_yes_price=True)
    assert ev.px == 400 and ev.own_client_order_id == "dhA-7"


def test_trade_example_and_taker_mapping(asyncapi_examples):
    (ev,) = ws_message_to_events(ex(asyncapi_examples, "trade"), R)
    assert ev == KalshiTrade(
        ts=R, ts_exch=1669149841000 * NS_PER_MS, ticker="HIGHNY-22DEC23-B53.5",
        trade_id="d91bc706-ee49-470d-82d8-11418bda6fed", yes_px=3600, qty=13600,
        taker_side="no", is_block=False, sid=11, seq=2,
    )
    assert taker_outcome_side({"taker_book_side": "bid"}) == "yes"
    assert taker_outcome_side({"taker_book_side": "ask"}) == "no"
    assert taker_outcome_side({"taker_side": "yes"}) == "yes"  # legacy fallback only
    with pytest.raises(ValueError):
        taker_outcome_side({"taker_outcome_side": "yes", "taker_book_side": "ask"})
    with pytest.raises(ValueError):
        taker_outcome_side({})


def test_ticker_example(asyncapi_examples):
    (ev,) = ws_message_to_events(ex(asyncapi_examples, "ticker"), R)
    assert ev == KalshiTicker(
        ts=R, ts_exch=1669149841000 * NS_PER_MS, ticker="FED-23DEC-T3.00", yes_bid=4500, yes_ask=5300,
        yes_bid_qty=30000, yes_ask_qty=15000, last_px=4800, volume=3389600, open_interest=2042200,
    )


def test_fill_example_uses_book_side_not_deprecated_fields(asyncapi_examples):
    (ev,) = ws_message_to_events(ex(asyncapi_examples, "fill"), R)
    assert ev == KalshiFill(
        ts=R, ts_exch=1671899397000 * NS_PER_MS, ticker="HIGHNY-22DEC23-B53.5",
        trade_id="d91bc706-ee49-470d-82d8-11418bda6fed", order_id="ee587a1c-8b87-4dcf-b721-9f6f790619fa",
        client_order_id="my-order-1", book_side="bid", yes_px=7500, qty=27800, is_taker=True,
        fee_micros=10000, post_position=50000, has_post_position=True,
    )
    m = ex(asyncapi_examples, "fill")
    m["msg"].update(side="no", action="sell")  # deprecated fields must be ignored
    assert ws_message_to_events(m, R)[0].book_side == "bid"
    m = ex(asyncapi_examples, "fill")
    del m["msg"]["book_side"]
    m["msg"]["outcome_side"] = "no"
    assert ws_message_to_events(m, R)[0].book_side == "ask"
    del m["msg"]["outcome_side"]
    with pytest.raises(ValueError):
        ws_message_to_events(m, R)
    with pytest.raises(ValueError):
        book_side_of({"book_side": "bid", "outcome_side": "no"})


def test_user_order_example(asyncapi_examples):
    (ev,) = ws_message_to_events(ex(asyncapi_examples, "userOrder"), R)
    assert ev == KalshiOrderUpdate(
        ts=R, ts_exch=1733047200000 * NS_PER_MS, ticker="FED-23DEC-T3.00",
        order_id="ee587a1c-8b87-4dcf-b721-9f6f790619fa", client_order_id="my-order-1", status="resting",
        book_side="bid", yes_px=3500, initial_qty=1000, fill_qty=0, remaining_qty=1000,
        maker_fees_micros=0, taker_fees_micros=0,
    )


def test_lifecycle_examples(asyncapi_examples):
    (created,) = ws_message_to_events(ex(asyncapi_examples, "marketLifecycleV2", 0), R)
    assert isinstance(created, KalshiMarketLifecycle)
    assert created.event_type == "created" and created.close_ts == 1694721600 * NS_PER_S
    assert created.price_level_structure == "linear_cent" and created.is_deactivated is None
    (plsu,) = ws_message_to_events(ex(asyncapi_examples, "marketLifecycleV2", 1), R)
    assert plsu.event_type == "price_level_structure_updated"
    assert plsu.price_ranges == ((0, 10000, 10),) and plsu.price_level_structure == "deci_cent"
    for i in (0, 1):
        (md,) = ws_message_to_events(ex(asyncapi_examples, "marketMetadataUpdated", i), R)
        assert md.event_type == "metadata_updated"


def test_lifecycle_determined_emits_settlement():
    msg = {"type": "market_lifecycle_v2", "sid": 13, "seq": 9, "msg": {
        "market_ticker": "KXBTCD-X", "event_type": "determined", "determination_ts": 1754427660,
        "result": "yes", "settlement_value": "1.0000"}}
    lc, st = ws_message_to_events(msg, R)
    assert lc.determination_ts == 1754427660 * NS_PER_S and lc.result == "yes"
    assert st == Settlement(ts=R, ts_exch=1754427660 * NS_PER_S, ticker="KXBTCD-X", result="yes",
                            expiration_value=None, settlement_px=10000)
    msg["msg"].update(event_type="deactivated", is_deactivated=True)
    (lc,) = ws_message_to_events(msg, R)
    assert lc.is_deactivated is True


def test_event_fee_update_examples(asyncapi_examples):
    (s,) = ws_message_to_events(ex(asyncapi_examples, "eventFeeUpdate", 0), R)
    assert s == KalshiFeeUpdate(ts=R, ts_exch=0, event_ticker="KXBTCD-26MAY2018",
                                fee_type_override="quadratic", fee_multiplier_override="1")
    (c,) = ws_message_to_events(ex(asyncapi_examples, "eventFeeUpdate", 1), R)
    assert c.fee_type_override is None and c.fee_multiplier_override is None


def test_cfbenchmarks_1hz_example(asyncapi_examples):
    (t,) = ws_message_to_events(ex(asyncapi_examples, "cfbenchmarksValue"), R)
    assert t == IndexTick(
        ts=R, ts_exch=1710000000123 * NS_PER_MS, index_id="BRTI", value=68000.12, feed="1hz",
        kalshi_recv_ns=1710000000123 * NS_PER_MS, avg60=68000.12, avg60_n=3, qh_avg=68000.23, qh_n=14,
    )


def test_cfbenchmarks_1hz_defensive_parsing(asyncapi_examples):
    m = ex(asyncapi_examples, "cfbenchmarksValue")
    del m["msg"]["last_60s_windowed_average_15min"]
    m["msg"]["data"] = orjson.dumps({"type": "value", "id": "BRTI", "value": 68001.5}).decode()  # no time, numeric value
    (t,) = ws_message_to_events(m, R)
    assert t.value == 68001.5 and t.qh_avg is None and t.qh_n == 0
    assert t.ts_exch == 1710000000123 * NS_PER_MS  # from avg_60s window_end_ts_exclusive
    m["msg"]["data"] = {"time": "1710000001000", "value": "68002"}  # already an object, string time
    (t,) = ws_message_to_events(m, R)
    assert t.ts_exch == 1710000001000 * NS_PER_MS and t.value == 68002.0
    m["msg"]["data"] = "not json"
    (fs,) = ws_message_to_events(m, R)
    assert isinstance(fs, FeedStatus) and fs.status == "error"


def test_cfbenchmarks_5hz_example(asyncapi_examples):
    (t,) = ws_message_to_events(ex(asyncapi_examples, "cfbenchmarksValue5Hz"), R)
    assert t == IndexTick(ts=R, ts_exch=1710000000323 * NS_PER_MS, index_id="BRTI", value=68000.12,
                          feed="5hz", kalshi_recv_ns=1710000000341 * NS_PER_MS)
    m = ex(asyncapi_examples, "cfbenchmarksValue5Hz")
    del m["msg"]["value_usd"]
    del m["msg"]["source_ts_ms"]
    (t,) = ws_message_to_events(m, R)
    assert t.value == 68000.12 and t.ts_exch == 1710000000323 * NS_PER_MS  # from raw CF frame


def test_control_and_ignored_messages(asyncapi_examples):
    for name in ("subscribedResponse", "unsubscribedResponse", "okResponse", "listSubscriptionsResponse",
                 "cfbenchmarksIndexList", "cfbenchmarks5HzIndexList", "eventLifecycle",
                 "pythValue", "rfqCreated", "quoteExecuted"):
        for p in asyncapi_examples[name]:
            assert ws_message_to_events(copy.deepcopy(p), R) == [], name
    for p in asyncapi_examples["errorResponse"]:
        (fs,) = ws_message_to_events(copy.deepcopy(p), R)
        assert fs.stream == "kalshi.ws" and fs.status == "error" and f"code={p['msg']['code']}" in fs.detail


def test_order_group_updates_become_core_events(asyncapi_examples):
    """Audit M4: live order-group updates use the same core event as the simulator."""
    from dh.core.events import KalshiOrderGroupUpdate, KalshiPositionSnapshot

    m = ex(asyncapi_examples, "orderGroupUpdates")
    (lu,) = ws_message_to_events(copy.deepcopy(m), R)
    assert isinstance(lu, KalshiOrderGroupUpdate) and lu.event_type == "limit_updated" and lu.contracts_limit == 15000
    m["msg"]["event_type"] = "triggered"
    (tg,) = ws_message_to_events(m, R)
    assert isinstance(tg, KalshiOrderGroupUpdate) and tg.order_group_id == "og_123" and tg.event_type == "triggered"
    assert tg.ts_exch == 1733047200000 * NS_PER_MS
    m["msg"]["event_type"] = "reset"
    assert ws_message_to_events(m, R)[0].event_type == "reset"
    (ps,) = ws_message_to_events(ex(asyncapi_examples, "marketPosition"), R)
    assert isinstance(ps, KalshiPositionSnapshot) and ps.position == 10000 and ps.cost_micros == 50_000_000
    g = order_group_update(ex(asyncapi_examples, "orderGroupUpdates")["msg"])
    assert g.contracts_limit == 15000 and g.event_type == "limit_updated"


def test_market_position_helper(asyncapi_examples):
    p = market_position(ex(asyncapi_examples, "marketPosition")["msg"])
    assert p.position == 10000 and p.position_cost_micros == 50_000_000 and p.fees_paid_micros == 1_000_000


def test_malformed_values_raise():
    with pytest.raises(UnitError):
        book_levels([["0.00005", "1.00"]])
    with pytest.raises(ValueError):
        book_levels([["0.5000", "1.00"], ["0.50", "2.00"]])  # duplicate price level
    with pytest.raises(ValueError):
        book_levels([["0.5000", "-1.00"]])
    assert book_levels([["0.5000", "0.00"]]) == ()


# ------------------------------------------------------------------------------ REST
def test_rest_orderbook_and_record():
    snap = rest_orderbook_to_snapshot("T", S.ORDERBOOK_BODY, R)
    assert snap == KalshiBookSnapshot(ts=R, ts_exch=0, ticker="T", sid=0, seq=0,
                                      yes_bids=((4400, 2550), (4500, 10000)), no_bids=((5000, 1000), (5300, 5000)))
    rec = {"method": "GET", "path": "/trade-api/v2/markets/T/orderbook", "params": {}, "status": 200, "body": S.ORDERBOOK_BODY}
    assert normalize_rest_record(rec, R) == [snap]
    multi = {"method": "GET", "path": "/markets/orderbooks", "params": {"tickers": ["A", "B"]}, "status": 200,
             "body": {"orderbooks": [{"ticker": "A", **S.ORDERBOOK_BODY}, {"ticker": "B", "orderbook_fp": {"yes_dollars": [], "no_dollars": []}}]}}
    a, b = normalize_rest_record(multi, R)
    assert a.ticker == "A" and a.yes_bids == snap.yes_bids and b.yes_bids == () and b.no_bids == ()
    assert normalize_rest_record({**rec, "status": 500}, R) == []
    assert normalize_rest_record({**rec, "method": "POST"}, R) == []


def test_rest_trades_record_sorted_ascending():
    body = {"trades": [S.trade_row("t2", "2025-08-05T20:31:00Z", taker="no"), S.trade_row("t1", "2025-08-05T20:30:00.25Z")], "cursor": ""}
    t1, t2 = normalize_rest_record({"method": "GET", "path": "/historical/trades", "status": 200, "body": body}, R)
    assert (t1.trade_id, t2.trade_id) == ("t1", "t2")
    assert t1 == KalshiTrade(ts=R, ts_exch=iso_to_ns("2025-08-05T20:30:00.25Z"), ticker=S.MARKET_KXBTCD["ticker"],
                             trade_id="t1", yes_px=4600, qty=300, taker_side="yes")
    assert t2.taker_side == "no"
    assert rest_trade_to_event(S.trade_row("t3", "2025-08-05T20:30:00Z", yes="0.0550", count="0.25"), R).qty == 25


def test_rest_market_settlement_record():
    m = S.settled_market()
    (s,) = normalize_rest_record({"method": "GET", "path": f"/historical/markets/{m['ticker']}", "status": 200, "body": {"market": m}}, R)
    assert s == Settlement(ts=R, ts_exch=iso_to_ns("2025-08-05T21:05:00Z"), ticker=m["ticker"], result="yes",
                           expiration_value=115123.45, settlement_px=10000)
    assert normalize_rest_record({"method": "GET", "path": f"/markets/{m['ticker']}", "status": 200, "body": {"market": S.market()}}, R) == []


@pytest.mark.parametrize(
    "body",
    [
        {"serverTime": "2024-03-09T16:00:00Z", "payload": [{"value": "68000.10", "time": 1710000000000}, {"value": "68000.20", "time": 1710000000200}]},
        [[1710000000000, "68000.10"], [1710000000200, "68000.20"]],
        {"data": {"values": [{"v": 68000.10, "t": "2024-03-09T16:00:00Z"}, {"v": 68000.20, "t": "2024-03-09T16:00:00.2Z"}]}},
        {"values": [{"price": "68000.20", "timestamp": 1710000000.2}, {"price": "68000.10", "timestamp": 1710000000}, {"bad": 1}]},
    ],
)
def test_cf_history_shapes(body):
    ticks = cf_history_to_ticks(body, R)
    assert [t.value for t in ticks] == [68000.10, 68000.20]
    assert [t.ts_exch for t in ticks] == [1710000000000 * NS_PER_MS, 1710000000200 * NS_PER_MS]
    assert all(t.feed == "rest" and t.index_id == "BRTI" and t.ts == R for t in ticks)
    rec = {"method": "GET", "path": "/cfbenchmarks/history/values", "params": {"id": "BRTI"}, "status": 200, "body": body}
    assert normalize_rest_record(rec, R) == ticks
    assert cf_history_to_ticks({"unexpected": True}, R) == []


def test_rest_fill_and_order_helpers():
    f = rest_fill_to_event(S.FILL_ROW, R)
    assert f.book_side == "ask" and f.yes_px == 4700 and f.qty == 200 and not f.has_post_position
    assert f.trade_id == "f-1" and f.ts_exch == iso_to_ns("2025-08-05T20:40:00Z")
    o = order_to_update(S.ORDER_ROW, R)
    assert (o.fill_qty, o.remaining_qty, o.initial_qty, o.maker_fees_micros) == (100, 900, 1000, 4331)
    assert o.ts_exch == iso_to_ns("2025-08-05T20:31:00.5Z") and o.book_side == "bid"
    # fills/orders are reconciliation data, never replayed as events from REST records
    assert normalize_rest_record({"method": "GET", "path": "/portfolio/fills", "status": 200, "body": {"fills": [S.FILL_ROW]}}, R) == []


# ------------------------------------------------------------------------------ market specs
def test_rest_market_to_spec_kxbtcd():
    spec = rest_market_to_spec(S.MARKET_KXBTCD, S.SERIES_KXBTCD, S.EVENT_KXBTCD)
    assert spec.ticker == "KXBTCD-25AUG0517-T114999.99" and spec.series_ticker == "KXBTCD"
    assert spec.strike_type == "greater" and spec.floor_strike == 114999.99 and spec.cap_strike is None
    assert spec.open_ts == iso_to_ns("2025-08-05T20:00:00Z")
    assert spec.close_ts == spec.expiration_ts == iso_to_ns("2025-08-05T21:00:00Z")
    assert spec.price_ranges == (PriceRange(0, 10000, 100),)
    assert spec.settlement == SettlementSpec(index_id="BRTI", n_obs=60)
    assert (spec.fee_type, spec.fee_multiplier) == ("quadratic", 1.0)
    assert spec.is_valid_px(4500) and not spec.is_valid_px(4550)
    assert spec.yes_wins(115000.0) and not spec.yes_wins(114999.99)


def test_spec_fee_precedence_event_over_series():
    ev = dict(S.EVENT_KXBTCD, fee_type_override="quadratic_with_maker_fees", fee_multiplier_override=0.5)
    spec = rest_market_to_spec(S.MARKET_KXBTCD, S.SERIES_KXBTCD, ev)
    assert (spec.fee_type, spec.fee_multiplier) == ("quadratic_with_maker_fees", 0.5)
    ev = dict(S.EVENT_KXBTCD, fee_type_override=None, fee_multiplier_override=2)
    spec = rest_market_to_spec(S.MARKET_KXBTCD, S.SERIES_KXBTCD, ev)
    assert (spec.fee_type, spec.fee_multiplier) == ("quadratic", 2.0)
    spec = rest_market_to_spec(S.MARKET_KXBTCD)  # no series/event: unresolved, series from ticker
    assert spec.fee_type == "" and spec.series_ticker == "KXBTCD"


def test_spec_between_tapered_grid_and_expiration_fallback():
    m = S.market(
        ticker="KXBTC-25AUG0517-B115125", event_ticker="KXBTC-25AUG0517", strike_type="between", floor_strike=115000, cap_strike=115249.99,
        price_ranges=[{"start": "0.0000", "end": "0.1000", "step": "0.0010"}, {"start": "0.1000", "end": "0.9000", "step": "0.0100"},
                      {"start": "0.9000", "end": "1.0000", "step": "0.0010"}],
        expected_expiration_time=None,
    )
    spec = rest_market_to_spec(m)
    assert spec.series_ticker == "KXBTC" and spec.floor_strike == 115000.0 and spec.cap_strike == 115249.99
    assert spec.is_valid_px(50) and spec.is_valid_px(1500) and not spec.is_valid_px(1510)
    assert spec.expiration_ts == spec.close_ts


def test_spec_rejections():
    with pytest.raises(UnsupportedMarket):
        rest_market_to_spec(S.market(strike_type="functional"))
    with pytest.raises(UnsupportedMarket):
        rest_market_to_spec(S.market(price_ranges=[]))
    with pytest.raises(UnsupportedMarket):  # KXBTC15M before its strike is set
        rest_market_to_spec(S.market(ticker="KXBTC15M-26APR160100-00", event_ticker="KXBTC15M-26APR160100", floor_strike=None))
    with pytest.raises(UnsupportedMarket):  # no settlement model for other series
        rest_market_to_spec(S.market(ticker="KXETHD-X-T1", event_ticker="KXETHD-X"))
    spec = rest_market_to_spec(S.market(ticker="KXETHD-X-T1", event_ticker="KXETHD-X"),
                               settlement=SettlementSpec(index_id="ETHUSD_RTI"))
    assert spec.settlement.index_id == "ETHUSD_RTI"
    assert Decimal(str(spec.floor_strike)) == Decimal("114999.99")


def test_lifecycle_created_carries_strike_fields(asyncapi_examples):
    """Audit m5: 'created' lifecycle events carry strike and time fields for replay."""
    for p in asyncapi_examples["marketLifecycleV2"]:
        if p["msg"].get("event_type") != "created":
            continue
        (lc,) = ws_message_to_events(copy.deepcopy(p), R)
        md = p["msg"]["additional_metadata"]
        assert lc.strike_type == md["strike_type"] and lc.floor_strike == float(md["floor_strike"])
        assert lc.event_ticker == md["event_ticker"] and lc.expected_expiration_ts == md["expected_expiration_ts"] * 10**9
