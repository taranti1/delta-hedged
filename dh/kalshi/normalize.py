"""Pure mapping of Kalshi WebSocket / REST JSON into normalized events (dh.core.events).

No I/O, no clocks: the local receive time ``recv_ns`` (int ns since epoch) is passed in and
becomes every event's ``ts``. ``ts_exch`` is the exchange timestamp when the message carries
one (``ts_ms``, ``created_time``, CF source time ...), else 0.

Units: prices -> Px int (1e-4 $, YES scale unless noted), counts -> Qty int (0.01 contract),
fees -> Micros int (1e-6 $), all via dh.core.units (exact; malformed values raise).

WebSocket message types (asyncapi 2.0.0) and their mapping:

  orderbook_snapshot        -> KalshiBookSnapshot (YES bids, NO bids on the NO price scale)
  orderbook_delta           -> KalshiBookDelta (signed delta; own client_order_id if ours)
  trade                     -> KalshiTrade (taker_side = taker_outcome_side; book 'bid'=='yes')
  ticker                    -> KalshiTicker
  fill                      -> KalshiFill (book_side/outcome_side; deprecated side/action unused)
  user_order                -> KalshiOrderUpdate
  market_lifecycle_v2       -> KalshiMarketLifecycle (+ Settlement on 'determined');
                               'metadata_updated' -> KalshiMarketLifecycle(event_type=
                               'metadata_updated') only: the core event has no strike fields,
                               consumers must refresh the market spec (dh.kalshi.metadata).
                               Likewise 'created' strikes (additional_metadata) are not
                               representable: the registry refreshes created markets.
  event_fee_update          -> KalshiFeeUpdate
  order_group_updates       -> FeedStatus(stream='kalshi.order_group:<id>') for 'triggered'
                               (status 'error': group orders canceled, entry blocked) and
                               'reset' (status 'resynced'); other group events -> []
                               (full payload: ``order_group_update()``).
  cfbenchmarks_value        -> IndexTick(feed='1hz') incl. avg60 / quarter-hour averages
  cfbenchmarks_value_5hz    -> IndexTick(feed='5hz')
  market_position           -> KalshiPositionSnapshot (reconciliation; was [] before audit M4); the
                               may act on (positions come from fills; the live runner
                               reconciles with ``market_position()`` / REST positions).
  event_lifecycle           -> [] : event creation is picked up by metadata discovery (REST).
  subscribed/unsubscribed/ok/list_subscriptions/*_indexlist -> []
  error                     -> FeedStatus(stream='kalshi.ws', status='error')
  anything else             -> [] (e.g. pyth_value, communications: not used)

Order book price convention: subscriptions must NOT set ``use_yes_price=true`` (dh.kalshi.ws
sends ``use_yes_price: false`` explicitly). If a recording was made with yes-leg pricing,
pass ``use_yes_price=True`` so NO-side prices are converted back to the NO scale.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

import orjson

from dh.core.events import (
    Event,
    FeedStatus,
    IndexTick,
    KalshiBookDelta,
    KalshiBookSnapshot,
    KalshiFeeUpdate,
    KalshiFill,
    KalshiMarketLifecycle,
    KalshiOrderGroupUpdate,
    KalshiOrderUpdate,
    KalshiPositionSnapshot,
    KalshiTicker,
    KalshiTrade,
    Settlement,
)
from dh.core.market import (
    SUPPORTED_STRIKE_TYPES,
    MarketSpec,
    PriceRange,
    SettlementSpec,
)
from dh.core.units import PX_SCALE, px_from_dollars, qty_from_fp
from dh.kalshi.wire import (
    as_dict,
    epoch_to_ns,
    iso_to_ns,
    ms_to_ns,
    normalize_route,
    number_to_str,
    opt_iso_to_ns,
    opt_micros,
    opt_px,
    opt_qty,
    s_to_ns,
    to_float,
)

WS_STREAM = "kalshi.ws"
_BOOK_TO_OUTCOME = {"bid": "yes", "ask": "no"}
_OUTCOME_TO_BOOK = {"yes": "bid", "no": "ask"}
CONTROL_TYPES = frozenset(
    {
        "subscribed",
        "unsubscribed",
        "ok",
        "list_subscriptions",
        "cfbenchmarks_value_indexlist",
        "cfbenchmarks_value_5hz_indexlist",
        "pyth_value_underlying_list",
    }
)


class UnsupportedMarket(ValueError):
    """The market cannot be modeled (strike type, missing tick grid, unknown settlement)."""


# ============================================================================ WebSocket
def ws_message_to_events(msg: dict, recv_ns: int, *, use_yes_price: bool = False) -> list[Event]:
    """One parsed WS message -> normalized events (stateless; see module docstring).

    recv_ns: local receive time, int ns. Sequence/gap handling is NOT done here (it needs
    state): see dh.kalshi.sequencer.normalize_ws_frame, which wraps this function.
    Raises ValueError/UnitError on malformed payloads of known types.
    """
    typ = msg.get("type")
    body = msg.get("msg")
    if typ in CONTROL_TYPES:
        return []
    if typ == "error":
        return [ws_error_status(msg, recv_ns)]
    if not isinstance(body, dict):
        return []
    handler = _WS_HANDLERS.get(str(typ))
    if handler is None:
        return []
    return handler(msg, body, recv_ns, use_yes_price)


def ws_error_status(msg: dict, recv_ns: int) -> FeedStatus:
    """WS error response -> FeedStatus('error') with code/message/id/sid in detail."""
    body = as_dict(msg.get("msg"))
    parts = [f"code={body.get('code')}", f"msg={body.get('msg')!s}"]
    for k in ("id", "sid"):
        if msg.get(k) is not None:
            parts.append(f"{k}={msg.get(k)}")
    for k in ("market_ticker", "market_tickers"):
        if body.get(k):
            parts.append(f"{k}={body.get(k)}")
    return FeedStatus(ts=recv_ns, ts_exch=0, stream=WS_STREAM, status="error", detail=" ".join(parts))


def _sid_seq(msg: dict) -> tuple[int, int]:
    return int(msg.get("sid") or 0), int(msg.get("seq") or 0)


def _exch_ts(m: dict) -> int:
    """Exchange time from ts_ms (preferred) or legacy ts (seconds)."""
    if m.get("ts_ms") is not None:
        return ms_to_ns(m["ts_ms"])
    ts = m.get("ts")
    if isinstance(ts, (int, float)) or (isinstance(ts, str) and ts.isdigit()):
        return s_to_ns(int(ts))
    if isinstance(ts, str) and ts:
        return iso_to_ns(ts)
    return 0


def book_levels(raw: Any, *, flip: bool = False) -> tuple[tuple[int, int], ...]:
    """[[price_dollars, count_fp], ...] -> ((px, qty), ...) ascending by px, zero sizes dropped.

    flip=True maps a yes-leg-priced NO level back to the NO scale (px -> 1 - px).
    Duplicate prices or negative sizes raise ValueError.
    """
    if raw is None:
        return ()
    if not isinstance(raw, list):
        raise ValueError("book side must be a list of [price, count] levels")
    out: dict[int, int] = {}
    for lvl in raw:
        if not isinstance(lvl, (list, tuple)) or len(lvl) < 2:
            raise ValueError(f"bad book level {lvl!r}")
        px = px_from_dollars(str(lvl[0]))
        if flip:
            px = PX_SCALE - px
        q = qty_from_fp(str(lvl[1]))
        if q < 0:
            raise ValueError(f"negative level size {lvl!r}")
        if q == 0:
            continue
        if px in out:
            raise ValueError(f"duplicate price level {lvl!r}")
        out[px] = q
    return tuple(sorted(out.items()))


def _ob_snapshot(msg: dict, m: dict, recv_ns: int, yes_priced: bool) -> list[Event]:
    sid, seq = _sid_seq(msg)
    return [
        KalshiBookSnapshot(
            ts=recv_ns,
            ts_exch=0,
            ticker=str(m["market_ticker"]),
            sid=sid,
            seq=seq,
            yes_bids=book_levels(m.get("yes_dollars_fp")),
            no_bids=book_levels(m.get("no_dollars_fp"), flip=yes_priced),
        )
    ]


def _ob_delta(msg: dict, m: dict, recv_ns: int, yes_priced: bool) -> list[Event]:
    sid, seq = _sid_seq(msg)
    side = m.get("side")
    if side not in ("yes", "no"):
        raise ValueError(f"orderbook_delta side {side!r}")
    px = px_from_dollars(str(m["price_dollars"]))
    if yes_priced and side == "no":
        px = PX_SCALE - px
    return [
        KalshiBookDelta(
            ts=recv_ns,
            ts_exch=_exch_ts(m),
            ticker=str(m["market_ticker"]),
            sid=sid,
            seq=seq,
            side=side,
            px=px,
            delta=qty_from_fp(str(m["delta_fp"])),
            own_client_order_id=str(m.get("client_order_id") or ""),
        )
    ]


def taker_outcome_side(m: dict) -> str:
    """'yes' if the taker bought YES (lifted YES asks), 'no' if the taker sold YES/bought NO.

    Uses taker_outcome_side, cross-checked with taker_book_side ('bid' == 'yes'); the
    deprecated taker_side (same bit) is only a fallback for old recordings.
    """
    outcome = m.get("taker_outcome_side")
    book = m.get("taker_book_side")
    from_book = _BOOK_TO_OUTCOME.get(str(book)) if book is not None else None
    if outcome in ("yes", "no"):
        if from_book is not None and from_book != outcome:
            raise ValueError(f"inconsistent taker sides outcome={outcome} book={book}")
        return str(outcome)
    if from_book is not None:
        return from_book
    legacy = m.get("taker_side")
    if legacy in ("yes", "no"):
        return str(legacy)
    raise ValueError("trade without taker side")


def book_side_of(m: dict) -> str:
    """Order/fill direction on the YES book from book_side (or outcome_side): 'bid'|'ask'.

    The deprecated side/action/is_yes fields are deliberately ignored.
    """
    book = m.get("book_side")
    outcome = m.get("outcome_side")
    from_outcome = _OUTCOME_TO_BOOK.get(str(outcome)) if outcome is not None else None
    if book in ("bid", "ask"):
        if from_outcome is not None and from_outcome != book:
            raise ValueError(f"inconsistent book_side={book} outcome_side={outcome}")
        return str(book)
    if from_outcome is not None:
        return from_outcome
    raise ValueError("payload has neither book_side nor outcome_side")


def _yes_px(m: dict) -> int:
    if m.get("yes_price_dollars") not in (None, ""):
        return px_from_dollars(str(m["yes_price_dollars"]))
    if m.get("no_price_dollars") not in (None, ""):
        return PX_SCALE - px_from_dollars(str(m["no_price_dollars"]))
    raise ValueError("payload has no yes/no price")


def _trade(msg: dict, m: dict, recv_ns: int, _yp: bool) -> list[Event]:
    sid, seq = _sid_seq(msg)
    return [
        KalshiTrade(
            ts=recv_ns,
            ts_exch=_exch_ts(m),
            ticker=str(m.get("market_ticker") or m.get("ticker")),
            trade_id=str(m["trade_id"]),
            yes_px=_yes_px(m),
            qty=qty_from_fp(str(m["count_fp"])),
            taker_side=taker_outcome_side(m),  # type: ignore[arg-type]
            is_block=bool(m.get("is_block_trade", False)),
            sid=sid,
            seq=seq,
        )
    ]


def _ticker(msg: dict, m: dict, recv_ns: int, _yp: bool) -> list[Event]:
    return [
        KalshiTicker(
            ts=recv_ns,
            ts_exch=_exch_ts(m),
            ticker=str(m["market_ticker"]),
            yes_bid=opt_px(m.get("yes_bid_dollars")),
            yes_ask=opt_px(m.get("yes_ask_dollars")),
            yes_bid_qty=opt_qty(m.get("yes_bid_size_fp")),
            yes_ask_qty=opt_qty(m.get("yes_ask_size_fp")),
            last_px=opt_px(m.get("price_dollars")),
            volume=opt_qty(m.get("volume_fp")),
            open_interest=opt_qty(m.get("open_interest_fp")),
        )
    ]


def subaccount_of(m: dict) -> int:
    """Subaccount number of an own-activity message or REST object: `subaccount` (fill,
    market_position) or `subaccount_number` (user_order, Order); missing -> 0 (primary)."""
    v = m.get("subaccount")
    if v in (None, ""):
        v = m.get("subaccount_number")
    try:
        return int(v) if v not in (None, "") else 0
    except (TypeError, ValueError):
        return -1  # unparseable: never matches a configured subaccount, so it is dropped


def _fill(msg: dict, m: dict, recv_ns: int, _yp: bool) -> list[Event]:
    has_pos = m.get("post_position_fp") not in (None, "")
    return [
        KalshiFill(
            ts=recv_ns,
            ts_exch=_exch_ts(m),
            ticker=str(m.get("market_ticker") or m.get("ticker")),
            trade_id=str(m["trade_id"]),
            order_id=str(m["order_id"]),
            client_order_id=str(m.get("client_order_id") or ""),
            book_side=book_side_of(m),  # type: ignore[arg-type]
            yes_px=_yes_px(m),
            qty=qty_from_fp(str(m["count_fp"])),
            is_taker=bool(m["is_taker"]),
            fee_micros=opt_micros(m.get("fee_cost")),
            post_position=opt_qty(m.get("post_position_fp")),
            has_post_position=has_pos,
            subaccount=subaccount_of(m),
        )
    ]


def _user_order(msg: dict, m: dict, recv_ns: int, _yp: bool) -> list[Event]:
    return [order_to_update(m, recv_ns)]


def order_to_update(m: dict, recv_ns: int) -> KalshiOrderUpdate:
    """WS user_order msg or REST Order object -> KalshiOrderUpdate (ts=recv_ns)."""
    ts_exch = ms_to_ns(m.get("last_updated_ts_ms")) or ms_to_ns(m.get("created_ts_ms"))
    if not ts_exch:
        ts_exch = opt_iso_to_ns(m.get("last_update_time")) or opt_iso_to_ns(m.get("created_time"))
    return KalshiOrderUpdate(
        ts=recv_ns,
        ts_exch=ts_exch,
        ticker=str(m.get("ticker") or m.get("market_ticker")),
        order_id=str(m["order_id"]),
        client_order_id=str(m.get("client_order_id") or ""),
        status=str(m.get("status") or "unknown"),
        book_side=book_side_of(m),  # type: ignore[arg-type]
        yes_px=_yes_px(m),
        initial_qty=opt_qty(m.get("initial_count_fp")),
        fill_qty=opt_qty(m.get("fill_count_fp")),
        remaining_qty=opt_qty(m.get("remaining_count_fp")),
        maker_fees_micros=opt_micros(m.get("maker_fees_dollars")),
        taker_fees_micros=opt_micros(m.get("taker_fees_dollars")),
        subaccount=subaccount_of(m),
    )


def price_ranges_to_tuples(raw: Any) -> tuple[tuple[int, int, int], ...]:
    """[{start,end,step}] (dollar strings) -> ((start_px, end_px, step_px), ...)."""
    out = []
    for r in raw or []:
        start, end, step = (px_from_dollars(str(r[k])) for k in ("start", "end", "step"))
        if step <= 0 or end < start:
            raise ValueError(f"invalid price range {r!r}")
        out.append((start, end, step))
    return tuple(out)


def _strike_fields(m: dict) -> dict:
    """Strike/metadata fields from a lifecycle message (in `additional_metadata` on
    'created', possibly top-level on 'metadata_updated'); absent fields stay default."""
    md = as_dict(m.get("additional_metadata"))
    src = {**m, **md}
    out: dict = {}
    if src.get("event_ticker"):
        out["event_ticker"] = str(src["event_ticker"])
    if src.get("strike_type"):
        out["strike_type"] = str(src["strike_type"])
    for k in ("floor_strike", "cap_strike"):
        v = src.get(k)
        if v not in (None, ""):
            out[k] = float(v)
    if src.get("expected_expiration_ts") not in (None, ""):
        out["expected_expiration_ts"] = s_to_ns(src.get("expected_expiration_ts"))
    if m.get("open_ts") not in (None, ""):
        out["open_ts"] = s_to_ns(m.get("open_ts"))
    return out


def _lifecycle(msg: dict, m: dict, recv_ns: int, _yp: bool) -> list[Event]:
    ticker = str(m["market_ticker"])
    et = str(m.get("event_type") or "")
    if et == "metadata_updated":
        return [KalshiMarketLifecycle(ts=recv_ns, ts_exch=0, ticker=ticker, event_type=et, **_strike_fields(m))]
    det_ns = s_to_ns(m.get("determination_ts"))
    set_ns = s_to_ns(m.get("settled_ts"))
    ts_exch = det_ns if et == "determined" else set_ns if et == "settled" else 0
    result = str(m.get("result") or "")
    sval = m.get("settlement_value")
    is_deact = m.get("is_deactivated")
    events: list[Event] = [
        KalshiMarketLifecycle(
            ts=recv_ns,
            ts_exch=ts_exch,
            ticker=ticker,
            event_type=et,
            close_ts=s_to_ns(m.get("close_ts")),
            determination_ts=det_ns,
            settled_ts=set_ns,
            result=result,
            settlement_value="" if sval is None else str(sval),
            is_deactivated=None if is_deact is None else bool(is_deact),
            price_level_structure=str(m.get("price_level_structure") or ""),
            price_ranges=price_ranges_to_tuples(m.get("price_ranges")),
            **_strike_fields(m),
        )
    ]
    if et == "determined":
        events.append(
            Settlement(
                ts=recv_ns,
                ts_exch=det_ns,
                ticker=ticker,
                result=result,
                expiration_value=None,
                settlement_px=_settlement_px(result, sval),
            )
        )
    return events


def _settlement_px(result: str, settlement_value: Any) -> int:
    if settlement_value not in (None, ""):
        return px_from_dollars(str(settlement_value))
    return PX_SCALE if result == "yes" else 0


def _fee_update(msg: dict, m: dict, recv_ns: int, _yp: bool) -> list[Event]:
    fto = m.get("fee_type_override")
    return [
        KalshiFeeUpdate(
            ts=recv_ns,
            ts_exch=0,
            event_ticker=str(m["event_ticker"]),
            fee_type_override=None if fto is None else str(fto),
            fee_multiplier_override=number_to_str(m.get("fee_multiplier_override")),
        )
    ]


def _order_group(msg: dict, m: dict, recv_ns: int, _yp: bool) -> list[Event]:
    """order_group_updates -> core KalshiOrderGroupUpdate (created|triggered|reset|deleted|
    limit_updated), the same event the simulator emits, so live and replay take the same
    path through the OrderManager and the strategy (audit M4)."""
    lim = m.get("contracts_limit_fp")
    return [
        KalshiOrderGroupUpdate(
            ts=recv_ns,
            ts_exch=ms_to_ns(m.get("ts_ms")),
            order_group_id=str(m.get("order_group_id") or ""),
            event_type=str(m.get("event_type") or ""),
            contracts_limit=opt_qty(lim) if lim not in (None, "") else -1,
        )
    ]


def _market_position(msg: dict, m: dict, recv_ns: int, _yp: bool) -> list[Event]:
    """market_positions -> core KalshiPositionSnapshot (reconciliation input)."""
    snap = market_position(m)
    return [
        KalshiPositionSnapshot(
            ts=recv_ns, ts_exch=0, ticker=snap.ticker, position=snap.position,
            cost_micros=snap.position_cost_micros, realized_pnl_micros=snap.realized_pnl_micros,
            fees_paid_micros=snap.fees_paid_micros, source="ws", subaccount=subaccount_of(m),
        )
    ]


def cf_frame(data: Any) -> dict:
    """Parse the raw CF Benchmarks frame carried in msg.data (JSON string or object)."""
    if isinstance(data, dict):
        return data
    if isinstance(data, (str, bytes)) and data:
        try:
            obj = orjson.loads(data)
        except orjson.JSONDecodeError:
            return {}
        return obj if isinstance(obj, dict) else {}
    return {}


def _cf_1hz(msg: dict, m: dict, recv_ns: int, _yp: bool) -> list[Event]:
    frame = cf_frame(m.get("data"))
    value = to_float(frame.get("value"))
    if value is None:
        value = to_float(m.get("value_usd", m.get("value")))
    avg = as_dict(m.get("avg_60s_data"))
    qh = m.get("last_60s_windowed_average_15min")
    qh = qh if isinstance(qh, dict) else None
    src_ns = epoch_to_ns(frame.get("time")) if frame.get("time") not in (None, "") else 0
    if not src_ns and avg.get("window_end_ts_exclusive") is not None:
        # avg_60s window is [source_ts_ms - 60000, source_ts_ms): its exclusive end IS the tick time.
        src_ns = ms_to_ns(avg["window_end_ts_exclusive"])
    if not src_ns:
        src_ns = ms_to_ns(m.get("source_ts_ms")) or ms_to_ns(m.get("received_at"))
    index_id = str(m.get("index_id") or frame.get("id") or "")
    if value is None or not index_id:
        return [
            FeedStatus(ts=recv_ns, ts_exch=0, stream=WS_STREAM, status="error", detail="cfbenchmarks_value without value/index_id")
        ]
    return [
        IndexTick(
            ts=recv_ns,
            ts_exch=src_ns,
            index_id=index_id,
            value=value,
            feed="1hz",
            kalshi_recv_ns=ms_to_ns(m.get("received_at")),
            avg60=to_float(avg.get("value")),
            avg60_n=int(avg.get("window_size") or 0),
            qh_avg=to_float(qh.get("value")) if qh else None,
            qh_n=int(qh.get("window_size") or 0) if qh else 0,
        )
    ]


def _cf_5hz(msg: dict, m: dict, recv_ns: int, _yp: bool) -> list[Event]:
    value = to_float(m.get("value_usd"))
    src_ns = ms_to_ns(m.get("source_ts_ms"))
    frame: dict = {}
    if value is None or not src_ns:
        frame = cf_frame(m.get("data"))
        if value is None:
            value = to_float(frame.get("value"))
        if not src_ns and frame.get("time") not in (None, ""):
            src_ns = epoch_to_ns(frame.get("time"))
    index_id = str(m.get("index_id") or frame.get("id") or "")
    if value is None or not index_id:
        return [
            FeedStatus(ts=recv_ns, ts_exch=0, stream=WS_STREAM, status="error", detail="cfbenchmarks_value_5hz without value/index_id")
        ]
    return [
        IndexTick(
            ts=recv_ns,
            ts_exch=src_ns,
            index_id=index_id,
            value=value,
            feed="5hz",
            kalshi_recv_ns=ms_to_ns(m.get("received_at")),
        )
    ]


def _ignore(msg: dict, m: dict, recv_ns: int, _yp: bool) -> list[Event]:
    return []


_WS_HANDLERS = {
    "orderbook_snapshot": _ob_snapshot,
    "orderbook_delta": _ob_delta,
    "trade": _trade,
    "ticker": _ticker,
    "fill": _fill,
    "user_order": _user_order,
    "market_lifecycle_v2": _lifecycle,
    "event_fee_update": _fee_update,
    "order_group_updates": _order_group,
    "cfbenchmarks_value": _cf_1hz,
    "cfbenchmarks_value_5hz": _cf_5hz,
    "market_position": _market_position,
    "event_lifecycle": _ignore,
}


# ---------------------------------------------------------------- non-event payload helpers
@dataclass(frozen=True, slots=True)
class MarketPositionSnapshot:
    """WS market_position / REST MarketPosition (reconciliation only; not a core Event).

    position: signed Qty (YES > 0, NO < 0); money fields in Micros.
    """

    ticker: str
    position: int
    position_cost_micros: int
    realized_pnl_micros: int
    fees_paid_micros: int
    volume: int
    subaccount: int | None = None


def market_position(m: dict) -> MarketPositionSnapshot:
    """WS market_position msg (or REST MarketPosition) -> MarketPositionSnapshot."""
    return MarketPositionSnapshot(
        ticker=str(m.get("market_ticker") or m.get("ticker")),
        position=opt_qty(m.get("position_fp")),
        position_cost_micros=opt_micros(m.get("position_cost_dollars", m.get("market_exposure_dollars"))),
        realized_pnl_micros=opt_micros(m.get("realized_pnl_dollars")),
        fees_paid_micros=opt_micros(m.get("fees_paid_dollars")),
        volume=opt_qty(m.get("volume_fp")),
        subaccount=m.get("subaccount"),
    )


@dataclass(frozen=True, slots=True)
class OrderGroupUpdate:
    """order_group_updates payload. contracts_limit in Qty units (None if absent)."""

    event_type: str
    order_group_id: str
    contracts_limit: int | None
    ts_exch: int


def order_group_update(m: dict) -> OrderGroupUpdate:
    lim = m.get("contracts_limit_fp")
    return OrderGroupUpdate(
        event_type=str(m.get("event_type") or ""),
        order_group_id=str(m.get("order_group_id") or ""),
        contracts_limit=None if lim in (None, "") else qty_from_fp(str(lim)),
        ts_exch=ms_to_ns(m.get("ts_ms")),
    )


# ============================================================================ REST
def rest_orderbook_to_snapshot(ticker: str, body: dict, recv_ns: int) -> KalshiBookSnapshot:
    """GET /markets/{ticker}/orderbook body (or one MarketOrderbookFp) -> snapshot (sid=seq=0)."""
    ob = body.get("orderbook_fp")
    if not isinstance(ob, dict):
        raise ValueError("orderbook response without orderbook_fp")
    return KalshiBookSnapshot(
        ts=recv_ns,
        ts_exch=0,
        ticker=ticker,
        sid=0,
        seq=0,
        yes_bids=book_levels(ob.get("yes_dollars")),
        no_bids=book_levels(ob.get("no_dollars")),
    )


def rest_trade_to_event(row: dict, recv_ns: int) -> KalshiTrade:
    """openapi Trade -> KalshiTrade (ts_exch from created_time; sid=seq=0)."""
    ts_exch = opt_iso_to_ns(row.get("created_time"))
    if not ts_exch:
        ts_exch = _exch_ts(row)
    return KalshiTrade(
        ts=recv_ns,
        ts_exch=ts_exch,
        ticker=str(row.get("ticker") or row.get("market_ticker")),
        trade_id=str(row["trade_id"]),
        yes_px=_yes_px(row),
        qty=qty_from_fp(str(row["count_fp"])),
        taker_side=taker_outcome_side(row),  # type: ignore[arg-type]
        is_block=bool(row.get("is_block_trade", False)),
    )


def rest_fill_to_event(row: dict, recv_ns: int) -> KalshiFill:
    """openapi Fill -> KalshiFill (reconciliation; has_post_position=False)."""
    ts_exch = opt_iso_to_ns(row.get("created_time")) or s_to_ns(row.get("ts"))
    return KalshiFill(
        ts=recv_ns,
        ts_exch=ts_exch,
        ticker=str(row.get("ticker") or row.get("market_ticker")),
        trade_id=str(row.get("fill_id") or row.get("trade_id")),
        order_id=str(row["order_id"]),
        client_order_id=str(row.get("client_order_id") or ""),
        book_side=book_side_of(row),  # type: ignore[arg-type]
        yes_px=_yes_px(row),
        qty=qty_from_fp(str(row["count_fp"])),
        is_taker=bool(row["is_taker"]),
        fee_micros=opt_micros(row.get("fee_cost")),
        post_position=0,
        has_post_position=False,
        subaccount=subaccount_of(row),
    )


def cf_history_to_ticks(body: Any, recv_ns: int, index_id: str = "BRTI") -> list[IndexTick]:
    """CF Benchmarks REST passthrough body -> IndexTick(feed='rest'), ascending by source time.

    The passthrough is not in the openapi spec, so the shape is parsed defensively: rows are
    looked up under 'payload' | 'values' | 'data' | 'history' | 'results' (or the body is a
    list); a row is {value|v|price, time|t|timestamp|ts|source_ts_ms} or [time, value]; times
    may be s/ms/us/ns epochs or ISO strings. Unparseable rows are skipped.
    """
    rows: Any = body
    if isinstance(body, dict):
        rows = None
        for key in ("payload", "values", "data", "history", "results"):
            v = body.get(key)
            if isinstance(v, dict):
                v = next((v[k] for k in ("values", "data", "history") if isinstance(v.get(k), list)), None)
            if isinstance(v, list):
                rows = v
                break
    if not isinstance(rows, list):
        return []
    ticks: dict[int, IndexTick] = {}
    for row in rows:
        t_raw: Any = None
        v_raw: Any = None
        rid = index_id
        if isinstance(row, dict):
            v_raw = next((row[k] for k in ("value", "v", "price", "value_usd") if row.get(k) not in (None, "")), None)
            t_raw = next(
                (row[k] for k in ("time", "t", "timestamp", "ts", "source_ts_ms") if row.get(k) not in (None, "")), None
            )
            rid = str(row.get("id") or row.get("index_id") or index_id)
        elif isinstance(row, (list, tuple)) and len(row) >= 2:
            t_raw, v_raw = row[0], row[1]
        value = to_float(v_raw)
        if value is None or t_raw is None:
            continue
        try:
            t_ns = epoch_to_ns(t_raw)
        except (ValueError, ArithmeticError):
            continue
        ticks[t_ns] = IndexTick(ts=recv_ns, ts_exch=t_ns, index_id=rid, value=value, feed="rest")
    return [ticks[k] for k in sorted(ticks)]


def market_settlement(market: dict, recv_ns: int) -> Settlement | None:
    """REST Market with a result -> Settlement (expiration_value as float), else None."""
    result = str(market.get("result") or "")
    if result not in ("yes", "no", "scalar"):
        return None
    ev = to_float(market.get("expiration_value"))
    ts_exch = opt_iso_to_ns(market.get("settlement_ts"))
    return Settlement(
        ts=recv_ns,
        ts_exch=ts_exch,
        ticker=str(market["ticker"]),
        result=result,
        expiration_value=ev,
        settlement_px=_settlement_px(result, market.get("settlement_value_dollars")),
    )


_RX_ORDERBOOK = re.compile(r"^/markets/([^/]+)/orderbook$")
_RX_MARKET = re.compile(r"^/(?:historical/)?markets/([^/]+)$")


def normalize_rest_record(record: dict, recv_ns: int) -> list[Event]:
    """Recorded REST record {method, path, params, status, body} -> events.

    Emits: orderbook -> KalshiBookSnapshot(sid=0, seq=0); /markets/orderbooks -> one snapshot
    per market; /markets/trades and /historical/trades -> KalshiTrade (ascending time);
    /cfbenchmarks/... -> IndexTick(feed='rest'); a single market GET with a result ->
    Settlement. Everything else (including own fills/orders, which the WS already delivers,
    and non-2xx responses) -> [].
    """
    status = int(record.get("status") or 0)
    if not 200 <= status < 300 or str(record.get("method", "GET")).upper() != "GET":
        return []
    body = record.get("body")
    if not isinstance(body, (dict, list)):
        return []
    path = normalize_route(str(record.get("path", "")))
    params = record.get("params") or {}
    m = _RX_ORDERBOOK.match(path)
    if m and isinstance(body, dict):
        return [rest_orderbook_to_snapshot(m.group(1), body, recv_ns)]
    if path == "/markets/orderbooks" and isinstance(body, dict):
        return [
            rest_orderbook_to_snapshot(str(ob["ticker"]), ob, recv_ns)
            for ob in body.get("orderbooks") or []
        ]
    if path in ("/markets/trades", "/historical/trades") and isinstance(body, dict):
        trades = [rest_trade_to_event(r, recv_ns) for r in body.get("trades") or []]
        trades.sort(key=lambda t: (t.ts_exch, t.trade_id))
        return list(trades)
    if path.startswith("/cfbenchmarks"):
        idx = params.get("id") if isinstance(params, dict) else None
        return list(cf_history_to_ticks(body, recv_ns, index_id=str(idx or "BRTI")))
    m = _RX_MARKET.match(path)
    if m and isinstance(body, dict) and isinstance(body.get("market"), dict):
        s = market_settlement(body["market"], recv_ns)
        return [s] if s is not None else []
    return []


# ============================================================================ market specs
BTC_INDEX_BY_SERIES_PREFIX = (("KXBTC", "BRTI"),)


def default_settlement(series_ticker: str) -> SettlementSpec | None:
    """BRTI 60 x 1s average for KXBTC* series (KXBTCD, KXBTC, KXBTC15M); None otherwise.

    MUST be validated per series against rules_primary (dh.kalshi.metadata.rules_flags) and
    settled markets' expiration_value before trading.
    """
    for prefix, index_id in BTC_INDEX_BY_SERIES_PREFIX:
        if series_ticker.startswith(prefix):
            return SettlementSpec(index_id=index_id, n_obs=60)
    return None


def series_of(market: dict, series: dict | None, event: dict | None) -> str:
    """Series ticker from series/event dicts, else the event ticker's prefix before '-'."""
    if series and series.get("ticker"):
        return str(series["ticker"])
    if event and event.get("series_ticker"):
        return str(event["series_ticker"])
    et = str(market.get("event_ticker") or "")
    return et.split("-", 1)[0] if et else ""


def rest_market_to_spec(
    market: dict,
    series: dict | None = None,
    event: dict | None = None,
    *,
    settlement: SettlementSpec | None = None,
) -> MarketSpec:
    """openapi Market (+ optional Series, EventData) -> MarketSpec.

    Times: open_time, close_time, expected_expiration_time (fallback close_time) -> ns.
    Tick grid: market.price_ranges (authoritative; price_level_structure is only a label).
    Fees: event fee_type_override/fee_multiplier_override > series fee_type/fee_multiplier >
    market fields; unresolved -> fee_type ''. Settlement: explicit arg, else BRTI/60 obs for
    KXBTC* series, else UnsupportedMarket. Raises UnsupportedMarket for unsupported strike
    types, missing strikes or a missing tick grid.
    """
    from dh.kalshi.fees import (
        resolve_fee_fields,  # local import: fees does not import us
    )

    ticker = str(market["ticker"])
    strike_type = str(market.get("strike_type") or "")
    if strike_type not in SUPPORTED_STRIKE_TYPES:
        raise UnsupportedMarket(f"{ticker}: unsupported strike_type {strike_type!r}")
    ranges = price_ranges_to_tuples(market.get("price_ranges"))
    if not ranges:
        raise UnsupportedMarket(f"{ticker}: no price_ranges (tick grid unknown)")
    series_ticker = series_of(market, series, event)
    settle = settlement or default_settlement(series_ticker)
    if settle is None:
        raise UnsupportedMarket(f"{ticker}: no settlement model for series {series_ticker!r}")
    close_ns = iso_to_ns(str(market["close_time"]))
    exp_raw = market.get("expected_expiration_time")
    exp_ns = iso_to_ns(str(exp_raw)) if exp_raw else close_ns
    fee_type, mult, _src = resolve_fee_fields(series, event, market)
    base_type, base_mult, _bsrc = resolve_fee_fields(series, None, market)  # without event override
    floor = market.get("floor_strike")
    cap = market.get("cap_strike")
    try:
        return MarketSpec(
            ticker=ticker,
            event_ticker=str(market.get("event_ticker") or (event or {}).get("event_ticker") or ""),
            series_ticker=series_ticker,
            strike_type=strike_type,
            floor_strike=None if floor is None else float(floor),
            cap_strike=None if cap is None else float(cap),
            open_ts=opt_iso_to_ns(market.get("open_time")),
            close_ts=close_ns,
            expiration_ts=exp_ns,
            settlement=settle,
            price_ranges=tuple(PriceRange(s, e, st) for s, e, st in ranges),
            fee_type=fee_type,
            fee_multiplier=float(mult) if mult is not None else 1.0,
            title=str(market.get("title") or market.get("yes_sub_title") or ""),
            base_fee_type=base_type,
            base_fee_multiplier=float(base_mult) if base_mult is not None else None,
        )
    except ValueError as exc:  # MarketSpec validation (e.g. strike missing)
        raise UnsupportedMarket(str(exc)) from exc
