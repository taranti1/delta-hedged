from __future__ import annotations

import pytest

from dh.core.actions import AmendOrder, CancelOrder, DecreaseOrder, PlaceOrder
from dh.core.events import CancelAck, OrderAck, OrderReject
from dh.core.units import NS_PER_MS, NS_PER_S
from dh.kalshi.orders import (
    amend_order_body,
    amend_result_to_events,
    batch_create_results_to_events,
    cancel_order_kwargs,
    cancel_result_to_events,
    create_result_to_events,
    decrease_order_kwargs,
    decrease_result_to_events,
    expiration_seconds,
    place_order_body,
)
from dh.kalshi.rest import KalshiHTTPError, UnknownOutcome

R = 5_000
PLACE = PlaceOrder(client_order_id="dhA-1", ticker="KXBTCD-X", book_side="ask", px=4550, qty=250,
                   post_only=True, expiration_ts=1_754_427_599_999_999_999, order_group_id="g1", cancel_on_pause=True)


def test_place_order_body_matches_create_order_v2_request():
    assert place_order_body(PLACE) == {
        "ticker": "KXBTCD-X",
        "client_order_id": "dhA-1",
        "side": "ask",
        "count": "2.50",
        "price": "0.4550",
        "time_in_force": "good_till_canceled",
        "self_trade_prevention_type": "taker_at_cross",
        "post_only": True,
        "cancel_order_on_pause": True,
        "expiration_time": 1_754_427_599,  # floored seconds
        "order_group_id": "g1",
    }
    b = place_order_body(PlaceOrder("c", "T", "bid", 1, 1, post_only=False), time_in_force="immediate_or_cancel",
                         self_trade_prevention="maker", subaccount=2, exchange_index=0, reduce_only=True)
    assert b["price"] == "0.0001" and b["count"] == "0.01" and b["post_only"] is False
    assert b["subaccount"] == 2 and b["exchange_index"] == 0 and b["reduce_only"] is True and "expiration_time" not in b
    assert expiration_seconds(3 * NS_PER_S - 1) == 2


@pytest.mark.parametrize("bad", [dict(px=0), dict(px=10_000), dict(qty=0)])
def test_place_order_body_rejects_bad_units(bad):
    kw = dict(client_order_id="c", ticker="T", book_side="bid", px=5000, qty=100)
    kw.update(bad)
    with pytest.raises(ValueError):
        place_order_body(PlaceOrder(**kw))


def test_place_order_body_rejects_ioc_with_expiry_and_bad_enums():
    with pytest.raises(ValueError):
        place_order_body(PLACE, time_in_force="immediate_or_cancel")
    with pytest.raises(ValueError):
        place_order_body(PLACE, self_trade_prevention="none")


def test_amend_decrease_cancel_builders():
    a = AmendOrder("dhA-1", "dhA-2", "KXBTCD-X", "o1", "bid", 4600, 800)
    assert amend_order_body(a) == {"ticker": "KXBTCD-X", "side": "bid", "price": "0.4600", "count": "8.00",
                                   "client_order_id": "dhA-1", "updated_client_order_id": "dhA-2"}
    assert decrease_order_kwargs(DecreaseOrder("dhA-1", "KXBTCD-X", "o1", 300)) == {"reduce_to": 300, "market_ticker": "KXBTCD-X"}
    assert cancel_order_kwargs(CancelOrder("dhA-1", "KXBTCD-X", "o1")) == {"order_id": "o1", "market_ticker": "KXBTCD-X"}
    with pytest.raises(ValueError):
        cancel_order_kwargs(CancelOrder("dhA-1", "KXBTCD-X"))


def test_create_results():
    ok = {"order_id": "o1", "client_order_id": "dhA-1", "fill_count": "0.50", "remaining_count": "2.00", "ts_ms": 1715793600123}
    assert create_result_to_events(ok, PLACE, R) == [
        OrderAck(ts=R, ts_exch=1715793600123 * NS_PER_MS, client_order_id="dhA-1", order_id="o1", ticker="KXBTCD-X",
                 fill_qty=50, remaining_qty=200, request="create")
    ]
    err = KalshiHTTPError("POST", "/portfolio/events/orders", 400, {"code": "post_only_cross", "message": "would cross"})
    (rej,) = create_result_to_events(err, PLACE, R)
    assert rej == OrderReject(ts=R, ts_exch=0, client_order_id="dhA-1", ticker="KXBTCD-X",
                              reason="post_only_cross", http_status=400, request="create")
    assert create_result_to_events(UnknownOutcome("POST", "/p", {}, "timeout"), PLACE, R) == []


def test_batch_create_results():
    a1 = PlaceOrder("c1", "T", "bid", 4500, 100)
    a2 = PlaceOrder("c2", "T", "ask", 4700, 100)
    res = {"orders": [
        {"order_id": "o1", "client_order_id": "c1", "fill_count": "0.00", "remaining_count": "1.00", "ts_ms": 1},
        {"client_order_id": "c2", "error": {"code": "insufficient_balance", "message": "no money"}},
    ]}
    ack, rej = batch_create_results_to_events(res, [a1, a2], R)
    assert isinstance(ack, OrderAck) and ack.order_id == "o1" and ack.remaining_qty == 100
    assert isinstance(rej, OrderReject) and rej.client_order_id == "c2" and "insufficient_balance" in rej.reason


def test_amend_decrease_cancel_results():
    a = AmendOrder("dhA-1", "dhA-2", "T", "o1", "bid", 4600, 800)
    (ack,) = amend_result_to_events({"order_id": "o1", "client_order_id": "dhA-2", "ts_ms": 2}, a, R)
    assert ack.client_order_id == "dhA-2" and ack.remaining_qty == -1 and ack.request == "amend"
    (ack,) = amend_result_to_events({"order_id": "o1", "remaining_count": "8.00", "fill_count": "0.00", "ts_ms": 2}, a, R)
    assert ack.remaining_qty == 800
    d = DecreaseOrder("dhA-1", "T", "o1", 300)
    (ack,) = decrease_result_to_events({"order_id": "o1", "client_order_id": "dhA-1", "remaining_count": "3.00", "ts_ms": 3}, d, R)
    assert ack.request == "decrease" and ack.remaining_qty == 300
    c = CancelOrder("dhA-1", "T", "o1")
    assert cancel_result_to_events({"order_id": "o1", "client_order_id": "dhA-1", "reduced_by": "10.00", "ts_ms": 4}, c, R) == [
        CancelAck(ts=R, ts_exch=4 * NS_PER_MS, client_order_id="dhA-1", order_id="o1", ticker="T", canceled_qty=1000)
    ]
    (rej,) = cancel_result_to_events(KalshiHTTPError("DELETE", "/x", 404, {"code": "not_found", "message": ""}), c, R)
    assert rej.request == "cancel" and rej.http_status == 404


def test_rejects_use_order_manager_vocabulary():
    """Audit M5: a rejected cancel must not bring the order back to RESTING."""
    from dh.core.actions import CancelOrder, PlaceOrder
    from dh.core.events import OrderAck
    from dh.execution.order_manager import OrderManager, OrderState
    from dh.kalshi.orders import cancel_result_to_events

    for body, st, final in [({"error": {"code": "not_found", "message": "order not found"}}, 404, OrderState.CANCELED),
                            ({"error": {"code": "order_already_filled", "message": "x"}}, 400, OrderState.FILLED),
                            ({"error": {"code": "order_already_canceled", "message": "already canceled"}}, 400,
                             OrderState.CANCELED)]:
        om = OrderManager()
        p = PlaceOrder("c1", "T", "bid", 5000, 500)
        om.request_place(p, 1)
        om.on_event(OrderAck(ts=2, ts_exch=0, client_order_id="c1", order_id="o1", ticker="T", fill_qty=0,
                             remaining_qty=500))
        c = CancelOrder("c1", "T", order_id="o1")
        om.request_cancel(c, 3)
        (rej,) = cancel_result_to_events(KalshiHTTPError("DELETE", "/x", st, body), c, 4)
        om.on_event(rej)
        assert om.order("c1").state == final, (body, om.order("c1").state)
