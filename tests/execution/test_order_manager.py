from __future__ import annotations

import dataclasses

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from dh.core.actions import AmendOrder, CancelOrder, DecreaseOrder, PlaceOrder
from dh.core.events import Timer
from dh.execution.order_manager import EVENT_KINDS, TERMINAL, OrderManager, OrderState
from tests.execution.helpers import S, T, ack, cancel_ack, fill, reject, update

P = PlaceOrder("c1", T, "bid", 4500, 1000)


def kinds(evs):
    return [e.kind for e in evs]


def placed(om=None):
    om = om or OrderManager()
    om.request_place(P, 0)
    return om


def test_happy_path_position_cash_fees():
    om = placed()
    assert om.order("c1").state is OrderState.PENDING_NEW
    assert om.worst_case_exposure(T, "bid") == 1000 and om.worst_case_exposure(T, "ask") == 0
    assert kinds(om.on_event(ack(1, "c1", "X1", 0, 1000))) == ["accepted"]
    assert om.order("c1").state is OrderState.RESTING
    evs = om.on_event(fill(2, "c1", "X1", "bid", 4500, 400, "t1", post=400, fee=1000))
    assert kinds(evs) == ["fill"] and evs[0].qty == 400 and evs[0].remaining_qty == 600
    evs = om.on_event(fill(3, "c1", "X1", "bid", 4500, 600, "t2", post=1000, fee=1500))
    assert kinds(evs) == ["fill", "filled"]
    assert om.position(T) == 1000 and om.working(T) == []
    assert om.cash_micros(T) == -4500 * 1000 and om.fees_micros(T) == 2500
    assert om.settled_pnl_micros(T, 10_000) == -4_500_000 - 2500 + 10_000_000


def test_fill_before_ack_and_ack_after_full_fill():
    om = placed()
    evs = om.on_event(fill(1, "c1", "X1", "bid", 4500, 1000, "t1", post=1000))
    assert kinds(evs) == ["fill", "filled"]
    evs = om.on_event(ack(2, "c1", "X1", 0, 1000))  # ack reflects placement time: must not regress
    assert kinds(evs) == ["accepted"] and evs[0].state is OrderState.FILLED
    o = om.order("c1")
    assert o.state is OrderState.FILLED and o.order_id == "X1" and o.remaining_qty == 0
    assert om.position(T) == 1000


def test_orphan_fill_without_client_id_attached_on_ack():
    om = placed()
    evs = om.on_event(fill(1, "", "X1", "bid", 4500, 300, "t1"))
    assert kinds(evs) == ["orphan_fill"] and om.position(T) == 300
    assert om.stats["orphan_fills"] == 1
    evs = om.on_event(ack(2, "c1", "X1", 0, 1000))
    assert kinds(evs) == ["accepted", "fill"]
    o = om.order("c1")
    assert o.filled_qty == 300 and o.remaining_qty == 700 and om.position(T) == 300


def test_fill_during_pending_cancel_then_cancel_ack_with_fills_in_flight():
    om = placed()
    om.on_event(ack(1, "c1", "X1", 0, 1000))
    assert om.request_cancel(CancelOrder("c1", T, "X1"), 2) is True
    assert om.order("c1").state is OrderState.PENDING_CANCEL
    assert kinds(om.on_event(fill(3, "c1", "X1", "bid", 4500, 200, "t1"))) == ["fill"]
    # exchange: another 300 filled before the cancel; that fill message is still in flight
    evs = om.on_event(cancel_ack(4, "c1", "X1", 500))
    assert kinds(evs) == ["canceled"] and evs[0].qty == 500
    o = om.order("c1")
    assert o.state is OrderState.CANCELED and o.filled_qty == 500 and o.inflight_fill_qty == 300
    assert om.position(T) == 200 and om.worst_case_exposure(T, "bid") == 500
    om.on_event(fill(5, "c1", "X1", "bid", 4500, 300, "t2"))
    assert om.position(T) == 500 and om.worst_case_exposure(T, "bid") == 500
    assert om.order("c1").inflight_fill_qty == 0 and om.order("c1").state is OrderState.CANCELED


def test_cancel_rejected_already_filled_before_fills_arrive():
    om = placed()
    om.on_event(ack(1, "c1", "X1", 0, 1000))
    om.request_cancel(CancelOrder("c1", T, "X1"), 2)
    evs = om.on_event(reject(3, "c1", "already_filled", "cancel"))
    assert kinds(evs) == ["cancel_rejected", "filled"]
    o = om.order("c1")
    assert o.state is OrderState.FILLED and o.inflight_fill_qty == 1000
    assert om.position(T) == 0 and om.worst_case_exposure(T, "bid") == 1000
    om.on_event(fill(4, "c1", "X1", "bid", 4500, 1000, "t1"))
    assert om.position(T) == 1000 and om.order("c1").inflight_fill_qty == 0


def test_duplicate_fill_is_ignored():
    om = placed()
    f = fill(1, "c1", "X1", "bid", 4500, 300, "t1")
    om.on_event(f)
    assert om.on_event(f) == [] and om.on_event(fill(9, "", "X1", "bid", 4500, 300, "t1")) == []
    assert om.position(T) == 300 and om.stats["duplicate_fills"] == 2


def test_out_of_order_order_updates():
    om = placed()
    om.on_event(update(1, "c1", "X1", "resting", "bid", 4500, 1000, 0, 1000, ts_exch=10))
    assert om.order("c1").state is OrderState.RESTING
    om.on_event(update(2, "c1", "X1", "canceled", "bid", 4500, 1000, 400, 0, ts_exch=30))
    assert om.order("c1").state is OrderState.CANCELED and om.order("c1").filled_qty == 400
    # a stale 'resting' snapshot and an older fill count arrive late: ignored
    om.on_event(update(3, "c1", "X1", "resting", "bid", 4500, 1000, 100, 900, ts_exch=20))
    o = om.order("c1")
    assert o.state is OrderState.CANCELED and o.filled_qty == 400 and o.inflight_fill_qty == 400
    assert om.worst_case_exposure(T, "bid") == 400


def test_stale_update_by_exchange_time_does_not_shrink_cap():
    om = placed()
    om.on_event(ack(1, "c1", "X1", 0, 1000, ts_exch=5))
    om.on_event(update(2, "c1", "X1", "resting", "bid", 4500, 1000, 300, 700, ts_exch=20))
    om.on_event(update(3, "c1", "X1", "resting", "bid", 4500, 1000, 0, 400, ts_exch=10))  # stale
    assert om.order("c1").remaining_qty == 700 and om.stats["stale_updates"] == 1


def test_unknown_outcome_create_and_reconciliation():
    om = placed()
    evs = om.on_event(reject(5, "c1", "timeout"))
    assert kinds(evs) == ["reconcile_needed"]
    o = om.order("c1")
    assert o.state is OrderState.PENDING_NEW and o.unknown_outcome
    assert om.worst_case_exposure(T, "bid") == 1000  # might be live
    evs = om.reconcile_missing("c1", 6)
    assert kinds(evs) == ["rejected"] and om.order("c1").state is OrderState.REJECTED
    assert om.worst_case_exposure(T, "bid") == 0
    # second order: timeout then the exchange shows it resting
    om.request_place(PlaceOrder("c2", T, "ask", 4700, 500), 10)
    om.on_event(reject(11, "c2", "timeout"))
    evs = om.on_event(update(12, "c2", "X2", "resting", "ask", 4700, 500, 0, 500))
    assert kinds(evs) == ["accepted"] and om.order("c2").state is OrderState.RESTING
    assert not om.order("c2").unknown_outcome


def test_timer_timeouts_flag_once():
    om = OrderManager(ack_timeout_ns=1 * S, change_timeout_ns=1 * S)
    om.request_place(P, 0)
    assert om.on_event(Timer(S // 2)) == []
    assert kinds(om.on_event(Timer(2 * S))) == ["reconcile_needed"]
    assert om.on_event(Timer(3 * S)) == []
    om.on_event(ack(3 * S, "c1", "X1", 0, 1000))
    om.request_cancel(CancelOrder("c1", T, "X1"), 4 * S)
    evs = om.on_event(Timer(6 * S))
    assert kinds(evs) == ["reconcile_needed"] and evs[0].detail == "cancel_timeout"


def test_post_position_mismatch_and_reconcile_position():
    om = OrderManager(initial_positions={T: 500})
    om.request_place(P, 0)
    assert kinds(om.on_event(fill(1, "c1", "X1", "bid", 4500, 100, "t1", post=600))) == ["fill"]
    evs = om.on_event(fill(2, "c1", "X1", "bid", 4500, 100, "t2", post=900))
    assert kinds(evs) == ["fill", "position_mismatch"] and om.exchange_position(T) == 900
    evs = om.reconcile_position(T, 900, 3, adopt=True)
    assert kinds(evs) == ["position_mismatch"] and om.position(T) == 900
    assert om.reconcile_position(T, 900, 4) == []


def test_deferred_cancel_and_cancel_overtaking_create():
    om = placed()
    assert om.request_cancel(CancelOrder("c1", T), 1) is False  # no order_id yet: deferred
    assert om.order("c1").state is OrderState.PENDING_CANCEL
    evs = om.on_event(ack(2, "c1", "X1", 0, 1000))
    assert kinds(evs) == ["accepted", "cancel_ready"] and evs[1].order_id == "X1"
    # a cancel that reached the exchange before the create comes back not_found
    om2 = placed()
    om2.request_cancel(CancelOrder("c1", T), 1)
    evs = om2.on_event(reject(2, "c1", "not_found", "cancel"))
    assert kinds(evs) == ["cancel_rejected"] and evs[0].detail == "not_found_before_ack"
    assert om2.order("c1").state is OrderState.PENDING_CANCEL
    assert kinds(om2.on_event(ack(3, "c1", "X1", 0, 1000))) == ["accepted", "cancel_ready"]


def test_transient_cancel_reject_reverts_and_gone_reject_needs_reconcile():
    om = placed()
    om.on_event(ack(1, "c1", "X1", 0, 1000))
    om.request_cancel(CancelOrder("c1", T, "X1"), 2)
    evs = om.on_event(reject(3, "c1", "rate_limited", "cancel"))
    assert kinds(evs) == ["cancel_rejected"] and om.order("c1").state is OrderState.RESTING
    om.request_cancel(CancelOrder("c1", T, "X1"), 4)
    evs = om.on_event(reject(5, "c1", "not_found", "cancel"))
    assert kinds(evs) == ["cancel_rejected", "reconcile_needed"]
    o = om.order("c1")
    assert o.state is OrderState.CANCELED and o.unresolved and om.worst_case_exposure(T, "bid") == 1000
    om.on_event(update(6, "c1", "X1", "canceled", "bid", 4500, 1000, 0, 0))
    assert not om.order("c1").unresolved and om.worst_case_exposure(T, "bid") == 0


def test_create_reject_and_duplicate_client_id():
    om = placed()
    evs = om.on_event(reject(1, "c1", "post_only_cross"))
    assert kinds(evs) == ["rejected"] and om.order("c1").state is OrderState.REJECTED
    assert om.worst_case_exposure(T, "bid") == 0
    with pytest.raises(ValueError):
        om.request_place(P, 2)


def test_amend_flow_with_old_and_new_ids():
    om = placed()
    om.on_event(ack(1, "c1", "X1", 0, 1000))
    om.on_event(fill(2, "c1", "X1", "bid", 4500, 200, "t1"))
    ok = om.request_amend(AmendOrder("c1", "c1b", T, "X1", "bid", 4600, 1500), 3)
    assert ok and om.order("c1").state is OrderState.PENDING_AMEND
    assert om.worst_case_exposure(T, "bid") == 1500  # amend-up could fill 1300 more (+200 held)
    om.on_event(fill(4, "c1", "X1", "bid", 4500, 100, "t2"))  # old id, before the amend applied
    evs = om.on_event(ack(5, "c1b", "X1", 0, 1200, request="amend"))
    assert kinds(evs) == ["amended"]
    o = om.order("c1b")
    assert o is not None and o.px == 4600 and o.total_qty == 1500 and o.remaining_qty == 1200
    assert om.order("c1").client_order_id == "c1b"  # alias still resolves
    om.on_event(fill(6, "c1b", "X1", "bid", 4600, 1200, "t3"))
    assert om.order("c1b").state is OrderState.FILLED and om.position(T) == 1500
    # amend refused because the order was already filled
    om.request_place(PlaceOrder("d1", T, "ask", 4800, 300), 7)
    om.on_event(ack(8, "d1", "X2", 0, 300))
    om.request_amend(AmendOrder("d1", "d1b", T, "X2", "ask", 4900, 300), 9)
    evs = om.on_event(reject(10, "d1b", "already_filled", "amend"))
    assert kinds(evs) == ["amend_rejected", "filled"] and om.order("d1b") is None
    assert om.order("d1").state is OrderState.FILLED


def test_decrease_keeps_order_and_counts_prior_fills():
    om = placed()
    om.on_event(ack(1, "c1", "X1", 0, 1000, ts_exch=1))
    assert om.request_decrease(DecreaseOrder("c1", T, "X1", 300), 2)
    evs = om.on_event(ack(3, "c1", "X1", 0, 300, request="decrease", ts_exch=30))
    assert kinds(evs) == ["decreased"] and om.order("c1").remaining_qty == 300
    # a fill that happened before the decrease (exchange time 20) arrives late
    om.on_event(fill(4, "c1", "X1", "bid", 4500, 100, "t1", ts_exch=20))
    assert om.order("c1").remaining_qty == 300 and om.position(T) == 100


# ------------------------------------------------------------------ permutation property
def _truth(kind: str):
    """Exchange messages for one order under a scenario; returns (messages, final qty, final state)."""
    a = ack(0, "c1", "X1", 0, 1000, ts_exch=1)
    u0 = update(0, "c1", "X1", "resting", "bid", 4500, 1000, 0, 1000, ts_exch=1)
    f1 = fill(0, "c1", "X1", "bid", 4500, 300, "t1", ts_exch=2, post=300)
    u1 = update(0, "c1", "X1", "resting", "bid", 4500, 1000, 300, 700, ts_exch=2)
    f2 = fill(0, "", "X1", "bid", 4500, 200, "t2", ts_exch=3, post=500)  # no client id: orphan-able
    u2 = update(0, "c1", "X1", "resting", "bid", 4500, 1000, 500, 500, ts_exch=3)
    if kind == "cancel":
        return [a, u0, f1, u1, f2, u2, f1, cancel_ack(0, "c1", "X1", 500),
                update(0, "c1", "X1", "canceled", "bid", 4500, 1000, 500, 0, ts_exch=4)], 500, OrderState.CANCELED
    f3 = fill(0, "c1", "X1", "bid", 4500, 500, "t3", ts_exch=4, post=1000)
    u3 = update(0, "c1", "X1", "executed", "bid", 4500, 1000, 1000, 0, ts_exch=4)
    msgs = [a, u0, f1, u1, f2, u2, f3, u3, f2]
    if kind == "cancel_after_fill":
        msgs.append(reject(0, "c1", "already_filled", "cancel"))
    return msgs, 1000, OrderState.FILLED


@settings(max_examples=300, deadline=None)
@given(kind=st.sampled_from(["cancel", "filled", "cancel_after_fill"]), data=st.data(),
       cancel_at=st.integers(0, 12))
def test_any_delivery_order_converges(kind, data, cancel_at):
    msgs, final, final_state = _truth(kind)
    perm = data.draw(st.permutations(msgs))
    om = placed()
    was_terminal = False
    for i, m in enumerate(perm):
        if i == cancel_at and kind != "filled":
            om.request_cancel(CancelOrder("c1", T, om.order("c1").order_id), i)
        evs = om.on_event(dataclasses.replace(m, ts=i + 1))
        assert all(e.kind in EVENT_KINDS for e in evs)
        o = om.order("c1")
        assert 0 <= om.position(T) <= final
        assert om.worst_case_exposure(T, "bid") >= final
        assert o.remaining_qty >= 0 and o.filled_qty <= 1000
        if was_terminal:
            assert o.state in TERMINAL  # never resurrected
        was_terminal = o.state in TERMINAL
    o = om.order("c1")
    assert o.state is final_state
    assert om.position(T) == final == o.filled_qty
    assert om.working(T) == [] and o.inflight_fill_qty == 0
    assert om.worst_case_exposure(T, "bid") == final and om.worst_case_exposure(T, "ask") == final


def test_order_group_updates_and_position_snapshots():
    from dh.core.events import KalshiOrderGroupUpdate, KalshiPositionSnapshot

    om = placed()
    assert kinds(om.on_event(KalshiOrderGroupUpdate(1, 0, "g1", "triggered"))) == ["group_triggered"]
    assert om.group_blocked("g1") and not om.group_blocked("g2")
    assert kinds(om.on_event(KalshiOrderGroupUpdate(2, 0, "g1", "reset"))) == ["group_reset"]
    assert not om.group_blocked("g1")
    assert om.on_event(KalshiOrderGroupUpdate(3, 0, "g1", "limit_updated", 500)) == []
    om.on_event(fill(4, "c1", "X1", "bid", 4500, 300, "t1"))
    assert om.on_event(KalshiPositionSnapshot(5, 0, T, 300)) == []
    evs = om.on_event(KalshiPositionSnapshot(6, 0, T, 400))
    assert kinds(evs) == ["position_mismatch"] and om.position(T) == 300  # never adopted silently


def test_exchange_evidence_overrides_a_wrong_reject():
    om = placed()
    om.on_event(reject(1, "c1", "bad_request"))
    assert om.order("c1").state is OrderState.REJECTED
    evs = om.on_event(fill(2, "c1", "X1", "bid", 4500, 100, "t1"))
    assert kinds(evs) == ["reconcile_needed", "fill"] and om.order("c1").state is OrderState.RESTING
    assert om.worst_case_exposure(T, "bid") == 1000
    om2 = placed()
    om2.on_event(reject(1, "c1", "bad_request"))
    evs = om2.on_event(update(2, "c1", "X1", "resting", "bid", 4500, 1000, 0, 1000))
    assert kinds(evs)[0] == "reconcile_needed" and om2.order("c1").state is OrderState.RESTING
