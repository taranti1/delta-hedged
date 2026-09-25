"""KalshiVenue: actions -> exact REST calls/bodies; results -> events; unknown outcomes ->
reconciliation; everything cross-checked against the real OrderManager."""

from __future__ import annotations

import pytest

from dh.core.actions import (
    CancelAll,
    CancelOrder,
    CreateOrderGroup,
    DeleteOrderGroup,
    PlaceHedge,
    PlaceOrder,
    ResetOrderGroup,
    UpdateOrderGroupLimit,
)
from dh.core.events import CancelAck, KalshiOrderUpdate, OrderAck, OrderReject
from dh.core.units import NS_PER_S
from dh.execution.order_manager import OrderManager, OrderState
from dh.kalshi.orders import place_order_body
from dh.kalshi.rate_limit import BucketLimit, KalshiRateLimiter
from dh.kalshi.rest import NotSentError
from dh.live.config import VenueCfg
from dh.live.venue_kalshi import KalshiVenue, batch_cancel_results_to_events, cancel_reject_reason

from .fakes import FakeClock, FakeRest, http_error, order_row, unknown

TK = "KXBTCD-26SEP2513-T84000.00"
TK2 = "KXBTCD-26SEP2513-T84250.00"


async def _nosleep(dt: float) -> None:
    return None


def make(rest: FakeRest | None = None, **cfg) -> tuple[KalshiVenue, FakeRest, list, FakeClock]:
    rest = rest or FakeRest()
    clock = FakeClock()
    out: list = []
    v = KalshiVenue(rest, sink=out.append, cfg=VenueCfg(**cfg), clock_ns=clock, monotonic=clock.mono, sleep=_nosleep)
    return v, rest, out, clock


def po(coid: str, ticker: str = TK, side: str = "bid", px: int = 4500, qty: int = 200, group: str = "") -> PlaceOrder:
    return PlaceOrder(client_order_id=coid, ticker=ticker, book_side=side, px=px, qty=qty, order_group_id=group)


async def test_single_place_exact_body_and_ack():
    v, rest, out, clock = make(subaccount=3)
    a = po("dhm1-1")
    v.submit([a], clock())
    assert await v.wait_idle(1.0)
    (args, _), = rest.of("create_order")
    assert args[0] == place_order_body(a, subaccount=3)
    assert args[0] == {"ticker": TK, "client_order_id": "dhm1-1", "side": "bid", "count": "2.00", "price": "0.4500",
                       "time_in_force": "good_till_canceled", "self_trade_prevention_type": "taker_at_cross",
                       "post_only": True, "cancel_order_on_pause": True, "subaccount": 3}
    assert len(out) == 1 and isinstance(out[0], OrderAck)
    assert (out[0].client_order_id, out[0].order_id, out[0].remaining_qty, out[0].ts) == ("dhm1-1", "oid-1", 200, clock())


async def test_simultaneous_places_are_batched_in_order():
    v, rest, out, clock = make(max_batch=2)
    acts = [po("c-1"), po("c-2", TK2, "ask", 5600), po("c-3")]
    v.submit(acts, clock())
    await v.wait_idle(1.0)
    batches = rest.of("batch_create_orders")
    singles = rest.of("create_order")
    assert [[b["client_order_id"] for b in args[0]] for args, _ in batches] == [["c-1", "c-2"]]
    assert [args[0]["client_order_id"] for args, _ in singles] == ["c-3"]
    assert sorted(e.client_order_id for e in out if isinstance(e, OrderAck)) == ["c-1", "c-2", "c-3"]


async def test_order_group_mapping_create_reset_limit_delete():
    v, rest, out, clock = make()
    gid = await v.ensure_order_group("dh-main", 2000)
    assert gid == "og-1" and rest.of("create_order_group")[0][0] == (2000,)
    # the strategy's own CreateOrderGroup at its first timer is a no-op (same limit)
    v.submit([CreateOrderGroup("dh-main", 2000)], clock())
    await v.wait_idle(1.0)
    assert len(rest.of("create_order_group")) == 1
    v.submit([po("c-1", group="dh-main")], clock())
    await v.wait_idle(1.0)
    assert rest.of("create_order")[0][0][0]["order_group_id"] == "og-1"
    rest.on("reset_order_group", http_error(429, "rate_limited"), {})
    v.submit([ResetOrderGroup("dh-main"), UpdateOrderGroupLimit("dh-main", 3000)], clock())
    await v.wait_idle(1.0)
    assert [a for a, _ in rest.of("reset_order_group")] == [("og-1",), ("og-1",)]  # retried after 429
    assert rest.of("update_order_group_limit")[0][0] == ("og-1", 3000) and v.group_limits["dh-main"] == 3000
    v.submit([DeleteOrderGroup("dh-main")], clock())
    await v.wait_idle(1.0)
    assert rest.of("delete_order_group")[0][0] == ("og-1",) and "dh-main" not in v.groups
    assert v.logical_group_of("og-1") == ""


async def test_unknown_group_create_adopts_new_group():
    rest = FakeRest()
    rest.groups = [{"id": "og-old", "contracts_limit_fp": "20.00", "is_auto_cancel_enabled": True}]

    def lost(args, kwargs):
        rest.groups.append({"id": "og-new", "contracts_limit_fp": "20.00", "is_auto_cancel_enabled": True})
        return unknown("POST", "/portfolio/order_groups/create")

    rest.on("create_order_group", lost)
    v, _, out, _ = make(rest)
    assert await v.ensure_order_group("dh-main", 2000) == "og-new"
    assert len(rest.of("create_order_group")) == 1  # never re-created blindly


async def test_place_without_exchange_group_is_rejected_locally():
    v, rest, out, clock = make()
    v.submit([po("c-1", group="dh-main")], clock())
    await v.wait_idle(1.0)
    assert rest.of("create_order") == []
    assert isinstance(out[0], OrderReject) and out[0].reason.startswith("invalid_order: order group")


async def test_create_http_error_and_not_sent_are_definite_rejects():
    rest = FakeRest().on("create_order", http_error(400, "invalid_price", "bad tick"), NotSentError("connect refused"))
    v, _, out, clock = make(rest)
    v.submit([po("c-1")], clock())
    await v.wait_idle(1.0)
    v.submit([po("c-2")], clock())
    await v.wait_idle(1.0)
    assert [(e.client_order_id, e.reason, e.http_status) for e in out] == [
        ("c-1", "invalid_price", 400), ("c-2", "not_sent", 0)]
    assert v.pending_reconciliations == 0
    om = OrderManager()
    for c in ("c-1", "c-2"):
        om.request_place(po(c), 0)
    for e in out:
        om.on_event(e)
    assert [om.order(c).state for c in ("c-1", "c-2")] == [OrderState.REJECTED, OrderState.REJECTED]


async def test_unknown_create_reconciled_found_never_resubmitted():
    rest = FakeRest()

    def lost(args, kwargs):  # the order DID reach the book, the response was lost
        body = args[0]
        rest.orders["oid-9"] = order_row(body["client_order_id"], "oid-9", body["ticker"])
        return unknown()

    rest.on("create_order", lost)
    v, _, out, clock = make(rest)
    a = po("c-1")
    v.submit([a], clock())
    await v.wait_idle(1.0)
    assert out == [] and v.pending_reconciliations == 1
    clock.advance(1 * NS_PER_S)
    await v.reconcile_due()
    assert len(rest.of("create_order")) == 1, "a create with unknown outcome is never resubmitted"
    (args, kw), = rest.of("find_order_by_client_id")
    assert args == ("c-1",) and kw["ticker"] == TK and kw["min_ts"] == clock.t // NS_PER_S - 1 - 60
    (ev,) = out
    assert isinstance(ev, KalshiOrderUpdate) and (ev.client_order_id, ev.order_id, ev.status) == ("c-1", "oid-9", "resting")
    om = OrderManager()
    om.request_place(a, 0)
    om.on_event(ev)
    w = om.order("c-1")
    assert w.state is OrderState.RESTING and w.order_id == "oid-9"


async def test_unknown_create_missing_becomes_reject_after_grace():
    rest = FakeRest().on("create_order", unknown())
    v, _, out, clock = make(rest, reconcile_missing_after_s=30.0, reconcile_min_attempts=3)
    v.submit([po("c-1")], clock())
    await v.wait_idle(1.0)
    for _ in range(20):
        clock.advance(5 * NS_PER_S)
        await v.reconcile_due()
        if out:
            break
    assert len(rest.of("find_order_by_client_id")) >= 3
    (ev,) = out
    assert isinstance(ev, OrderReject) and ev.reason == "reconciled_missing" and ev.request == "create"
    om = OrderManager()
    om.request_place(po("c-1"), 0)
    om.on_event(ev)
    assert om.order("c-1").state is OrderState.REJECTED


async def test_batch_response_missing_an_order_is_reconciled():
    rest = FakeRest()
    rest.on("batch_create_orders", lambda args, kw: {"orders": [
        {"order_id": "oid-1", "client_order_id": "c-1", "fill_count": "0.00", "remaining_count": "2.00", "ts_ms": 1},
        {"client_order_id": "c-2", "error": {"code": "post_only_cross", "message": "would cross"}}]})
    v, _, out, clock = make(rest)
    v.submit([po("c-1"), po("c-2"), po("c-3")], clock())
    await v.wait_idle(1.0)
    kinds = sorted((type(e).__name__, e.client_order_id) for e in out)
    assert kinds == [("OrderAck", "c-1"), ("OrderReject", "c-2")]
    assert v.pending_reconciliations == 1 and "c-3" in v._creates  # noqa: SLF001


async def test_cancel_single_and_batch():
    rest = FakeRest()
    rest.orders["o-1"] = order_row("c-1", "o-1", TK)
    rest.orders["o-2"] = order_row("c-2", "o-2", TK2)
    rest.orders["o-3"] = order_row("c-3", "o-3", TK2)
    v, _, out, clock = make(rest)
    v.submit([CancelOrder("c-1", TK, "o-1")], clock())
    await v.wait_idle(1.0)
    assert rest.of("cancel_order")[0] == (("o-1",), {"market_ticker": TK, "subaccount": None})
    assert isinstance(out[0], CancelAck) and out[0].canceled_qty == 200
    v.submit([CancelOrder("c-2", TK2, "o-2"), CancelOrder("c-3", TK2, "o-3")], clock())
    await v.wait_idle(1.0)
    (args, _), = rest.of("batch_cancel_orders")
    assert args[0] == [{"order_id": "o-2", "market_ticker": TK2}, {"order_id": "o-3", "market_ticker": TK2}]
    assert sorted(e.client_order_id for e in out[1:] if isinstance(e, CancelAck)) == ["c-2", "c-3"]


async def test_cancel_404_normalized_and_reconciled_to_filled():
    rest = FakeRest()
    rest.orders["o-1"] = order_row("c-1", "o-1", TK, status="executed", filled="2.00", remaining="0.00")
    rest.on("cancel_order", http_error(404, "not_found", "order not found", "DELETE"))
    v, _, out, clock = make(rest)
    om = OrderManager()
    a = po("c-1")
    om.request_place(a, 0)
    om.on_event(OrderAck(1, 0, "c-1", "o-1", TK, 0, 200))
    assert om.request_cancel(CancelOrder("c-1", TK, "o-1"), 2)
    v.submit([CancelOrder("c-1", TK, "o-1")], clock())
    await v.wait_idle(1.0)
    assert [(e.reason, e.http_status, e.request) for e in out] == [("not_found", 404, "cancel")]
    clock.advance(NS_PER_S)
    await v.reconcile_due()
    assert rest.of("get_order")[0][0] == ("o-1",)
    upd = out[-1]
    assert isinstance(upd, KalshiOrderUpdate) and upd.status == "executed" and upd.fill_qty == 200
    for e in out:
        om.on_event(e)
    w = om.order("c-1")
    assert w.state is OrderState.FILLED and w.filled_qty == 200 and not w.unresolved


async def test_cancel_unknown_outcome_recancels_while_resting():
    rest = FakeRest()
    rest.orders["o-1"] = order_row("c-1", "o-1", TK)
    rest.on("cancel_order", unknown("DELETE"))  # first cancel: lost; the re-cancel succeeds
    v, _, out, clock = make(rest)
    v.submit([CancelOrder("c-1", TK, "o-1")], clock())
    await v.wait_idle(1.0)
    assert out == []
    clock.advance(NS_PER_S)
    await v.reconcile_due()
    await v.wait_idle(1.0)
    assert [type(e).__name__ for e in out] == ["KalshiOrderUpdate", "CancelAck"]
    assert out[0].status == "resting" and len(rest.of("cancel_order")) == 2
    clock.advance(5 * NS_PER_S)
    await v.reconcile_due()
    assert out[-1].status == "canceled" and v.pending_reconciliations == 0


async def test_cancel_before_ack_and_rate_limited_cancel():
    rest = FakeRest().on("cancel_order", http_error(429, "rate_limited", "slow down", "DELETE"))
    v, _, out, clock = make(rest)
    v.submit([CancelOrder("c-9", TK, "")], clock())  # no order id yet
    await v.wait_idle(1.0)
    assert rest.of("cancel_order") == [] and out[0].reason == "not_found"
    v.observe(OrderAck(1, 0, "c-9", "o-9", TK, 0, 200))  # the ack arrives: the id is learned
    v.submit([CancelOrder("c-9", TK, "")], clock())
    await v.wait_idle(1.0)
    assert rest.of("cancel_order")[0][0] == ("o-9",)
    assert out[-1].reason == "rate_limited" and v.pending_reconciliations == 0  # 429: nothing to reconcile


async def test_cancel_all_global_and_scoped_sweep():
    rest = FakeRest()
    rest.orders["o-1"] = order_row("c-1", "o-1", TK)
    rest.orders["o-2"] = order_row("x-2", "o-2", TK2)
    v, _, out, clock = make(rest, subaccount=2)
    v.submit([CancelAll("book_gap", tickers=(TK2,))], clock())
    await v.wait_idle(1.0)
    assert rest.of("iter_orders")[0][1] == {"status": "resting", "ticker": TK2, "subaccount": 2}
    assert rest.of("cancel_order")[0][0] == ("o-2",) and rest.orders["o-1"]["status"] == "resting"
    rest.on("cancel_all_orders", unknown("DELETE"), http_error(429, "rate_limited"), {})
    v.submit([CancelAll("kalshi_disconnected")], clock())
    await v.wait_idle(1.0)
    assert [k for _, k in rest.of("cancel_all_orders")] == [{"subaccount": 2}] * 3  # retried: idempotent


async def test_rate_budget_drops_stale_quotes_not_cancels():
    lim = KalshiRateLimiter(write=BucketLimit(refill_rate=10.0, bucket_capacity=20.0))
    rest = FakeRest(limiter=lim)
    v, _, out, clock = make(rest, max_batch=1, max_place_wait_s=0.5)
    lim.write.drain()  # empty bucket: a 10-token create waits 1 s
    v.submit([po("c-1"), CancelOrder("c-0", TK, "o-0")], clock())
    await v.wait_idle(1.0)
    assert any(isinstance(e, OrderReject) and e.reason == "rate_budget" for e in out)
    assert rest.of("create_order") == [] and len(rest.of("cancel_order")) == 1


async def test_non_kalshi_actions_are_returned():
    v, rest, out, clock = make()
    h = PlaceHedge("h-1", "kalshi_perp", "KXBTCPERP", "buy", 0.01)
    assert v.submit([h], clock()) == [h] and rest.calls == []


def test_cancel_reason_normalization_and_batch_cancel_results():
    assert cancel_reject_reason(404, "", "") == "not_found"
    assert cancel_reject_reason(400, "order_not_found", "") == "not_found"
    assert cancel_reject_reason(429, "too_many_requests", "") == "rate_limited"
    assert cancel_reject_reason(400, "market_closed", "closed") == "market_closed"
    assert cancel_reject_reason(400, "order_already_executed", "") == "already_filled"
    acts = [CancelOrder("c-1", TK, "o-1"), CancelOrder("c-2", TK, "o-2"), CancelOrder("c-3", TK, "o-3")]
    evs, recon = batch_cancel_results_to_events({"orders": [
        {"order_id": "o-1", "reduced_by": "2.00", "ts_ms": 5},
        {"order_id": "o-2", "reduced_by": "0.00", "error": {"code": "not_found", "message": "gone"}}]}, acts, 7)
    assert isinstance(evs[0], CancelAck) and evs[0].canceled_qty == 200 and evs[0].ts == 7
    assert isinstance(evs[1], OrderReject) and evs[1].reason == "not_found"
    assert [a.client_order_id for a in recon] == ["c-2", "c-3"]


async def test_position_and_queue_position_reads():
    rest = FakeRest()
    rest.positions = {TK: "3.00", TK2: "-2.50"}
    rest.orders["o-1"] = order_row("c-1", "o-1", TK)
    rest.queue_positions = {"o-1": "12.00"}
    v, _, _, _ = make(rest, subaccount=1)
    assert await v.fetch_positions() == {TK: 300, TK2: -250}
    assert rest.of("get_all_positions")[0][1] == {"count_filter": "position", "subaccount": 1}
    assert await v.fetch_queue_positions([TK]) == [("o-1", TK, 1200)]


@pytest.mark.parametrize("n", [1, 3])
async def test_not_sent_batch(n):
    rest = FakeRest().on("create_order", NotSentError("x")).on("batch_create_orders", NotSentError("x"))
    v, _, out, clock = make(rest)
    v.submit([po(f"c-{i}") for i in range(n)], clock())
    await v.wait_idle(1.0)
    assert [e.reason for e in out] == ["not_sent"] * n and v.pending_reconciliations == 0


async def test_rate_budget_counts_chunks_of_the_same_dispatch():
    lim = KalshiRateLimiter(write=BucketLimit(refill_rate=10.0, bucket_capacity=30.0))  # full: 30 tokens
    rest = FakeRest(limiter=lim)
    v, _, out, clock = make(rest, max_batch=1, max_place_wait_s=0.5)
    v.submit([po(f"c-{i}") for i in range(4)], clock())  # 4 creates x 10 tokens: the 4th would wait 1 s
    await v.wait_idle(1.0)
    rej = [e.client_order_id for e in out if isinstance(e, OrderReject)]
    assert rej == ["c-3"] and len(rest.of("create_order")) == 3
    assert v._reserved_write == 0.0  # noqa: SLF001 - every reservation released
