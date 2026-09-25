"""V2 order request bodies from dh.core.actions, and REST results -> normalized events.

Bodies follow openapi 3.30.0 (CreateOrderV2Request, AmendOrderV2Request,
DecreaseOrderV2Request, cancel params): ``side`` is 'bid'|'ask' on the YES book, ``price`` is
a 4-decimal dollar string from the Px int, ``count`` a 2-decimal count string from the Qty
int, ``expiration_time`` Unix SECONDS (floored from the action's ns so an order never
outlives the strategy's intent). The adapter never invents prices or sizes.

Result mapping (for the live runner; the simulator emits the same event types):
  create  2xx -> OrderAck(request='create')     KalshiHTTPError -> OrderReject
  amend   2xx -> OrderAck(request='amend')      (remaining_qty = -1 when the response omits
                                                 remaining_count, i.e. size unchanged/unknown)
  decrease 2xx -> OrderAck(request='decrease')
  cancel  2xx -> CancelAck(canceled_qty = reduced_by)
  UnknownOutcome -> [] : the order stays pending until reconciliation (GET /portfolio/orders
                   by client_order_id -> normalize.order_to_update) resolves it.
"""

from __future__ import annotations

from typing import Any

from dh.core.actions import AmendOrder, CancelOrder, DecreaseOrder, PlaceOrder
from dh.core.events import CancelAck, Event, OrderAck, OrderReject
from dh.core.units import NS_PER_S, PX_SCALE, px_to_dollars, qty_to_fp
from dh.kalshi.rest import KalshiHTTPError, UnknownOutcome
from dh.kalshi.wire import ms_to_ns, opt_qty

SELF_TRADE_PREVENTION = ("taker_at_cross", "maker")
TIME_IN_FORCE = ("good_till_canceled", "immediate_or_cancel", "fill_or_kill")


def _check_px_qty(px: int, qty: int) -> None:
    if not 0 < px < PX_SCALE:
        raise ValueError(f"order px {px} outside (0, {PX_SCALE})")
    if qty <= 0:
        raise ValueError(f"order qty {qty} must be > 0")


def expiration_seconds(expiration_ns: int) -> int:
    """Action expiration (ns) -> Unix seconds for the API, floored (never later than asked)."""
    return expiration_ns // NS_PER_S


TIF_FROM_ACTION = {"gtc": "good_till_canceled", "ioc": "immediate_or_cancel", "fok": "fill_or_kill"}


def place_order_body(
    a: PlaceOrder,
    *,
    self_trade_prevention: str = "taker_at_cross",
    time_in_force: str | None = None,
    subaccount: int | None = None,
    exchange_index: int | None = None,
    reduce_only: bool | None = None,
) -> dict[str, Any]:
    """CreateOrderV2Request for a PlaceOrder (px 1e-4 $, qty 0.01 contracts).

    time_in_force defaults to the action's own PlaceOrder.time_in_force ('gtc' | 'ioc' |
    'fok' mapped to the API values); an explicit argument overrides it (audit m6)."""
    _check_px_qty(a.px, a.qty)
    if time_in_force is None:
        time_in_force = TIF_FROM_ACTION.get(a.time_in_force, a.time_in_force)
    if self_trade_prevention not in SELF_TRADE_PREVENTION:
        raise ValueError(f"self_trade_prevention_type {self_trade_prevention!r}")
    if time_in_force not in TIME_IN_FORCE:
        raise ValueError(f"time_in_force {time_in_force!r}")
    if a.book_side not in ("bid", "ask"):
        raise ValueError(f"book_side {a.book_side!r}")
    body: dict[str, Any] = {
        "ticker": a.ticker,
        "client_order_id": a.client_order_id,
        "side": a.book_side,
        "count": qty_to_fp(a.qty),
        "price": px_to_dollars(a.px),
        "time_in_force": time_in_force,
        "self_trade_prevention_type": self_trade_prevention,
        "post_only": bool(a.post_only),
        "cancel_order_on_pause": bool(a.cancel_on_pause),
    }
    if a.expiration_ts:
        if time_in_force == "immediate_or_cancel":
            raise ValueError("immediate_or_cancel cannot carry expiration_time")
        body["expiration_time"] = expiration_seconds(a.expiration_ts)
    if a.order_group_id:
        body["order_group_id"] = a.order_group_id
    if subaccount is not None:
        body["subaccount"] = subaccount
    if exchange_index is not None:
        body["exchange_index"] = exchange_index
    if reduce_only is not None:
        body["reduce_only"] = bool(reduce_only)
    return body


def amend_order_body(a: AmendOrder, *, exchange_index: int | None = None) -> dict[str, Any]:
    """AmendOrderV2Request: count = total fillable (already filled + desired remaining)."""
    _check_px_qty(a.px, a.total_qty)
    body: dict[str, Any] = {
        "ticker": a.ticker,
        "side": a.book_side,
        "price": px_to_dollars(a.px),
        "count": qty_to_fp(a.total_qty),
        "client_order_id": a.client_order_id,
        "updated_client_order_id": a.new_client_order_id,
    }
    if exchange_index is not None:
        body["exchange_index"] = exchange_index
    return body


def decrease_order_kwargs(a: DecreaseOrder) -> dict[str, Any]:
    """Keyword args for KalshiRest.decrease_order (reduce_to in Qty units, auto-routed)."""
    if a.reduce_to < 0:
        raise ValueError("reduce_to must be >= 0")
    return {"reduce_to": a.reduce_to, "market_ticker": a.ticker}


def cancel_order_kwargs(a: CancelOrder) -> dict[str, Any]:
    """Keyword args for KalshiRest.cancel_order (requires the exchange order_id)."""
    if not a.order_id:
        raise ValueError(f"cancel of {a.client_order_id} needs the exchange order_id")
    return {"order_id": a.order_id, "market_ticker": a.ticker}


def canonical_reason(err: KalshiHTTPError) -> str:
    """Map a Kalshi error to the OrderManager's reject vocabulary (audit M5).

    not_found | already_filled | already_canceled | expired | rate_limited | http_5xx
    | post_only_cross | <lower-cased error code> | http_<status>.  The full error body is kept
    in the recorded REST stream; the reason must be a bare token so state logic matches it.
    """
    code = (err.code or "").lower()
    text = f"{code} {(err.message or '').lower()}"
    if "already_filled" in text or "executed" in text or "fully filled" in text or "order_filled" in text:
        return "already_filled"
    if "cancel" in text and ("already" in text or code in ("order_canceled", "order_cancelled", "canceled")):
        return "already_canceled"
    if "expired" in text:
        return "expired"
    if err.status == 404 or "not_found" in text or "not found" in text:
        return "not_found"
    if err.status == 429:
        return "rate_limited"
    if err.status >= 500:
        return "http_5xx"
    if "post_only" in text or "would cross" in text or "cross" in code:
        return "post_only_cross"
    return code or f"http_{err.status}"


def _reject(err: KalshiHTTPError, coid: str, ticker: str, recv_ns: int, request: str) -> OrderReject:
    return OrderReject(ts=recv_ns, ts_exch=0, client_order_id=coid, ticker=ticker, reason=canonical_reason(err),
                       http_status=err.status, request=request)


def create_result_to_events(res: dict[str, Any] | UnknownOutcome | KalshiHTTPError, a: PlaceOrder, recv_ns: int) -> list[Event]:
    """CreateOrderV2Response / error -> [OrderAck] / [OrderReject] / [] (unknown)."""
    if isinstance(res, KalshiHTTPError):
        return [_reject(res, a.client_order_id, a.ticker, recv_ns, "create")]
    if isinstance(res, UnknownOutcome):
        return []
    return [
        OrderAck(
            ts=recv_ns,
            ts_exch=ms_to_ns(res.get("ts_ms")),
            client_order_id=str(res.get("client_order_id") or a.client_order_id),
            order_id=str(res["order_id"]),
            ticker=a.ticker,
            fill_qty=opt_qty(res.get("fill_count")),
            remaining_qty=opt_qty(res.get("remaining_count")),
            request="create",
        )
    ]


def batch_create_results_to_events(res: dict[str, Any] | UnknownOutcome | KalshiHTTPError, actions: list[PlaceOrder], recv_ns: int) -> list[Event]:
    """BatchCreateOrdersV2Response (per-order results, same order as the request)."""
    if isinstance(res, KalshiHTTPError):
        return [_reject(res, a.client_order_id, a.ticker, recv_ns, "create") for a in actions]
    if isinstance(res, UnknownOutcome):
        return []
    by_coid = {a.client_order_id: a for a in actions}
    out: list[Event] = []
    for i, r in enumerate(res.get("orders") or []):
        coid = r.get("client_order_id") or (actions[i].client_order_id if i < len(actions) else "")
        a = by_coid.get(str(coid)) or (actions[i] if i < len(actions) else None)
        if a is None:
            continue
        err = r.get("error")
        if err:
            out.append(
                OrderReject(
                    ts=recv_ns,
                    ts_exch=0,
                    client_order_id=a.client_order_id,
                    ticker=a.ticker,
                    reason=f"{err.get('code', '')}: {err.get('message', '')}".strip(": "),
                    http_status=0,
                    request="create",
                )
            )
        elif r.get("order_id"):
            out.append(
                OrderAck(
                    ts=recv_ns,
                    ts_exch=ms_to_ns(r.get("ts_ms")),
                    client_order_id=a.client_order_id,
                    order_id=str(r["order_id"]),
                    ticker=a.ticker,
                    fill_qty=opt_qty(r.get("fill_count")),
                    remaining_qty=opt_qty(r.get("remaining_count")),
                    request="create",
                )
            )
    return out


def amend_result_to_events(res: dict[str, Any] | UnknownOutcome | KalshiHTTPError, a: AmendOrder, recv_ns: int) -> list[Event]:
    """AmendOrderV2Response -> OrderAck(request='amend', client_order_id = new id)."""
    if isinstance(res, KalshiHTTPError):
        return [_reject(res, a.client_order_id, a.ticker, recv_ns, "amend")]
    if isinstance(res, UnknownOutcome):
        return []
    rem = res.get("remaining_count")
    return [
        OrderAck(
            ts=recv_ns,
            ts_exch=ms_to_ns(res.get("ts_ms")),
            client_order_id=str(res.get("client_order_id") or a.new_client_order_id),
            order_id=str(res.get("order_id") or a.order_id),
            ticker=a.ticker,
            fill_qty=opt_qty(res.get("fill_count")),
            remaining_qty=-1 if rem in (None, "") else opt_qty(rem),
            request="amend",
        )
    ]


def decrease_result_to_events(res: dict[str, Any] | UnknownOutcome | KalshiHTTPError, a: DecreaseOrder, recv_ns: int) -> list[Event]:
    """DecreaseOrderV2Response -> OrderAck(request='decrease')."""
    if isinstance(res, KalshiHTTPError):
        return [_reject(res, a.client_order_id, a.ticker, recv_ns, "decrease")]
    if isinstance(res, UnknownOutcome):
        return []
    return [
        OrderAck(
            ts=recv_ns,
            ts_exch=ms_to_ns(res.get("ts_ms")),
            client_order_id=str(res.get("client_order_id") or a.client_order_id),
            order_id=str(res.get("order_id") or a.order_id),
            ticker=a.ticker,
            fill_qty=0,
            remaining_qty=opt_qty(res.get("remaining_count")),
            request="decrease",
        )
    ]


def cancel_result_to_events(res: dict[str, Any] | UnknownOutcome | KalshiHTTPError, a: CancelOrder, recv_ns: int) -> list[Event]:
    """CancelOrderV2Response -> CancelAck(canceled_qty = reduced_by, Qty units)."""
    if isinstance(res, KalshiHTTPError):
        return [_reject(res, a.client_order_id, a.ticker, recv_ns, "cancel")]
    if isinstance(res, UnknownOutcome):
        return []
    return [
        CancelAck(
            ts=recv_ns,
            ts_exch=ms_to_ns(res.get("ts_ms")),
            client_order_id=str(res.get("client_order_id") or a.client_order_id),
            order_id=str(res.get("order_id") or a.order_id),
            ticker=a.ticker,
            canceled_qty=opt_qty(res.get("reduced_by")),
        )
    ]
