from __future__ import annotations

from dh.core.actions import AmendOrder, CancelAll, CancelOrder, DecreaseOrder, PlaceHedge, PlaceOrder
from dh.core.events import (
    CancelAck,
    KalshiFill,
    KalshiMarketLifecycle,
    KalshiOrderUpdate,
    OrderAck,
    OrderReject,
)
from dh.core.market import MarketSpec, PriceRange
from dh.execution import LatencyModel, OrderState, run_interleaved
from tests.execution.helpers import MS, S, T, Scripted, delta, make_sim, no_fee, simple_fee, snap, trade


def run(sim, events, schedule, **kw):
    strat = Scripted(schedule)
    out = run_interleaved(events, strat, [sim], **kw)
    return strat, out


def of(out, cls, **match):
    return [e for e in out if isinstance(e, cls) and all(getattr(e, k) == v for k, v in match.items())]


def test_post_only_cross_rejected_and_inside_spread_accepted():
    sim = make_sim()
    evs = [snap(0), delta(10 * MS, "yes", 4400, 100)]
    sched = [(0, PlaceOrder("x", T, "bid", 4700, 100)), (0, PlaceOrder("y", T, "bid", 4600, 100)),
             (0, PlaceOrder("z", T, "ask", 4500, 100))]
    strat, out = run(sim, evs, sched)
    rej = of(out, OrderReject)
    assert sorted((r.client_order_id, r.reason) for r in rej) == [("x", "post_only_cross"), ("z", "post_only_cross")]
    assert of(out, OrderAck, client_order_id="y")[0].remaining_qty == 100
    assert strat.om.order("x").state is OrderState.REJECTED and strat.om.order("y").state is OrderState.RESTING


def test_post_only_rejected_when_crossing_own_resting_order():
    sim = make_sim()
    sched = [(0, PlaceOrder("ask", T, "ask", 4600, 100)), (1, PlaceOrder("bid", T, "bid", 4600, 100))]
    strat, out = run(sim, [snap(0), delta(1, "yes", 4400, 1)], sched)
    assert [(r.client_order_id, r.reason) for r in of(out, OrderReject)] == [("bid", "post_only_cross")]


def test_taker_walks_displayed_levels_with_fees_and_phantom_consumption():
    sim = make_sim(fee=simple_fee)
    ev0 = snap(0, yes=((4400, 500),), no=((5300, 700), (5200, 200)))  # asks 47 x7, 48 x2
    sched = [(0, PlaceOrder("t1", T, "bid", 4800, 1000, post_only=False)),
             (1, PlaceOrder("t2", T, "bid", 4800, 300, post_only=False))]
    strat, out = run(sim, [ev0, delta(1, "yes", 4400, 1)], sched)
    f1 = of(out, KalshiFill, client_order_id="t1")
    assert [(f.yes_px, f.qty, f.is_taker) for f in f1] == [(4700, 700, True), (4800, 200, True)]
    assert f1[0].fee_micros == simple_fee(4700, 700, True)
    a1 = of(out, OrderAck, client_order_id="t1")[0]
    assert (a1.fill_qty, a1.remaining_qty) == (900, 100)
    # same displayed liquidity cannot be taken twice: t2 does not cross anything any more
    assert of(out, KalshiFill, client_order_id="t2") == []
    assert of(out, OrderAck, client_order_id="t2")[0].remaining_qty == 300
    assert strat.om.position(T) == 900 == sim.position(T)
    assert sim.stats["taker"] == 900


def test_queue_fill_beyond_queue_with_trade_and_delta_in_both_orders():
    for pol in "ABC":
        sim = make_sim(pol)
        evs = [snap(0), delta(1, "yes", 4400, 1),
               trade(10 * MS, 4500, 600, "no"), delta(10 * MS, "yes", 4500, -600),  # trade first
               delta(20 * MS, "yes", 4500, -300), trade(21 * MS, 4500, 300, "no"),  # delta first
               trade(2 * S, 4500, 300, "no"), delta(2 * S, "yes", 4500, -100)]
        strat, out = run(sim, evs, [(0, PlaceOrder("b", T, "bid", 4500, 500))])
        fills = of(out, KalshiFill)
        assert [(f.qty, f.is_taker) for f in fills] == [(200, False)], pol  # q 1000 -> 100 -> fill 200
        assert strat.om.order("b").remaining_qty == 300


def test_sweep_through_fill():
    sim = make_sim("B")
    evs = [snap(0, yes=((4500, 1000), (4400, 500))), delta(1, "yes", 4300, 1),
           trade(5 * MS, 4500, 1000, "no"), trade(5 * MS, 4400, 250, "no"),
           delta(5 * MS, "yes", 4500, -1000), delta(5 * MS, "yes", 4400, -250)]
    strat, out = run(sim, evs, [(0, PlaceOrder("b", T, "bid", 4500, 400))])
    assert [(f.qty, f.yes_px) for f in of(out, KalshiFill)] == [(250, 4500)]  # at OUR price
    assert sim.stats["sweep"] == 250 and strat.om.position(T) == 250


def test_crossing_level_inference_A_B_only():
    for pol, expect in (("A", 200), ("B", 200), ("C", 0)):
        sim = make_sim(pol)
        evs = [snap(0, yes=((4400, 1000),), no=((5300, 700),)), delta(1, "yes", 4300, 1),
               delta(5 * MS, "no", 5500, 200)]  # a YES ask at 45 appears below our 46 bid
        strat, out = run(sim, evs, [(0, PlaceOrder("b", T, "bid", 4600, 500))])
        assert sum(f.qty for f in of(out, KalshiFill)) == expect, pol


def test_fill_before_ack_race_from_latency():
    lat = LatencyModel.fixed(10, 50, 2)  # submit 10ms, REST response 50ms, WS 2ms
    sim = make_sim("B", lat=lat)
    evs = [snap(0, yes=((4400, 100),)), delta(1, "yes", 4300, 1), trade(15 * MS, 4500, 300, "no"),
           delta(15 * MS, "yes", 4400, 0)]
    strat, out = run(sim, evs, [(0, PlaceOrder("b", T, "bid", 4500, 500))])
    kinds = [(type(e).__name__, e.ts) for e in out]
    assert kinds[0] == ("KalshiOrderUpdate", 12 * MS)  # user_orders beats the REST ack
    assert ("KalshiFill", 17 * MS) in kinds and ("OrderAck", 60 * MS) in kinds
    assert strat.om.order("b").state is OrderState.RESTING and strat.om.order("b").filled_qty == 300


def test_fill_before_cancel_race_emerges_from_latency():
    lat = LatencyModel.fixed(10, 30, 2)
    for full, expect_cancel in ((False, True), (True, False)):
        sim = make_sim("B", lat=lat)
        q = 500 if full else 300
        evs = [snap(0, yes=((4400, 100),)), delta(1, "yes", 4300, 1), trade(105 * MS, 4500, q, "no")]
        sched = [(0, PlaceOrder("b", T, "bid", 4500, 500)), (100 * MS, CancelOrder("b", T))]
        # the strategy decides to cancel at 100ms (event at 100ms drives it)
        evs.insert(2, delta(100 * MS, "yes", 4300, 1))
        strat, out = run(sim, evs, sched)
        om = strat.om
        assert om.position(T) == q
        if expect_cancel:
            ca = of(out, CancelAck)
            assert ca and ca[0].canceled_qty == 200 and ca[0].ts == 140 * MS
            assert om.order("b").state is OrderState.CANCELED and om.order("b").filled_qty == 300
        else:
            rj = of(out, OrderReject)
            assert rj and rj[0].reason == "already_filled" and rj[0].request == "cancel"
            assert om.order("b").state is OrderState.FILLED


def test_cancel_unknown_and_cancel_all():
    sim = make_sim()
    sched = [(0, CancelOrder("nope", T)), (0, PlaceOrder("a", T, "bid", 4400, 100)),
             (0, PlaceOrder("b", T, "ask", 4800, 100)), (5, CancelAll("halt"))]
    strat, out = run(sim, [snap(0), delta(5, "yes", 4300, 1)], sched)
    assert [(r.client_order_id, r.reason) for r in of(out, OrderReject)] == [("nope", "not_found")]
    ups = of(out, KalshiOrderUpdate, status="canceled")
    assert sorted(u.client_order_id for u in ups) == ["a", "b"]
    assert strat.om.working(T) == []


def test_gtt_expiry():
    sim = make_sim()
    sched = [(0, PlaceOrder("g", T, "bid", 4500, 500, expiration_ts=2 * S + 500 * MS))]  # floored to 2s
    evs = [snap(0), delta(1, "yes", 4300, 1), trade(3 * S, 4500, 5000, "no")]
    strat, out = run(sim, evs, sched)
    assert of(out, KalshiFill) == []
    up = of(out, KalshiOrderUpdate, status="canceled")
    assert up and up[0].ts == 2 * S and sim.order_status("g")["cancel_reason"] == "expired"
    assert strat.om.order("g").state is OrderState.CANCELED


def test_market_close_auto_cancel_no_fills_after_close_and_rejects():
    sim = make_sim()
    life = KalshiMarketLifecycle(0, 0, T, "created", close_ts=5 * S)
    sched = [(0, PlaceOrder("a", T, "bid", 4500, 500)), (6 * S, PlaceOrder("late", T, "bid", 4500, 100))]
    evs = [life, snap(0), delta(1, "yes", 4300, 1), trade(6 * S, 4500, 5000, "no")]
    strat, out = run(sim, evs, sched)
    assert of(out, KalshiFill) == []
    assert sim.order_status("a")["cancel_reason"] == "market_closed"
    assert [(r.client_order_id, r.reason) for r in of(out, OrderReject)] == [("late", "market_closed")]


def test_pause_cancels_cancel_on_pause_orders_and_blocks_new_ones():
    sim = make_sim()
    sched = [(0, PlaceOrder("p", T, "bid", 4500, 100)),
             (0, PlaceOrder("k", T, "bid", 4400, 100, cancel_on_pause=False)),
             (2 * S, PlaceOrder("n1", T, "bid", 4400, 100)), (4 * S, PlaceOrder("n2", T, "bid", 4400, 100))]
    evs = [snap(0), delta(1, "yes", 4300, 1),
           KalshiMarketLifecycle(1 * S, 0, T, "deactivated", is_deactivated=True), delta(2 * S, "yes", 4300, 1),
           KalshiMarketLifecycle(3 * S, 0, T, "activated", is_deactivated=False), delta(4 * S, "yes", 4300, 1)]
    strat, out = run(sim, evs, sched)
    assert sim.order_status("p")["cancel_reason"] == "market_paused"
    assert sim.order_status("k")["status"] == "resting"
    assert [(r.client_order_id, r.reason) for r in of(out, OrderReject)] == [("n1", "market_paused")]
    assert sim.order_status("n2")["status"] == "resting"


def test_order_group_rolling_limit_auto_cancel_and_reset():
    sim = make_sim()
    sim.create_order_group("g", 500)
    sched = [(0, PlaceOrder("a", T, "bid", 4500, 1000, order_group_id="g")),
             (0, PlaceOrder("b", T, "ask", 4800, 1000, order_group_id="g")),
             (2 * S, PlaceOrder("c", T, "bid", 4500, 100, order_group_id="g")),
             (0, PlaceOrder("u", T, "bid", 4500, 100, order_group_id="missing"))]
    evs = [snap(0, yes=((4400, 100),)), delta(1, "yes", 4300, 1),
           trade(1 * S, 4500, 300, "no"), trade(1 * S + 1, 4500, 400, "no"), delta(2 * S, "yes", 4300, 1)]
    strat, out = run(sim, evs, sched)
    assert [f.qty for f in of(out, KalshiFill)] == [300, 200]  # capped at the 5-contract limit
    assert sim.order_status("a")["cancel_reason"] == "order_group_triggered"
    assert sim.order_status("b")["cancel_reason"] == "order_group_triggered"
    reasons = {(r.client_order_id, r.reason) for r in of(out, OrderReject)}
    assert reasons == {("c", "order_group_triggered"), ("u", "order_group_not_found")}
    sim.reset_order_group("g")
    strat2 = Scripted([(3 * S, PlaceOrder("d", T, "bid", 4500, 100, order_group_id="g"))])
    run_interleaved([delta(3 * S, "yes", 4300, 1)], strat2, [sim])
    assert sim.order_status("d")["status"] == "resting"


def test_order_group_window_rolls_after_15s():
    sim = make_sim()
    sim.create_order_group("g", 500)
    sched = [(0, PlaceOrder("a", T, "bid", 4500, 1000, order_group_id="g"))]
    evs = [snap(0, yes=((4400, 100),)), delta(1, "yes", 4300, 1), trade(1 * S, 4500, 300, "no"),
           trade(17 * S, 4500, 300, "no")]
    strat, out = run(sim, evs, sched)
    # 300 + 300 > 500 in total, but the first 300 left the rolling 15 s window before the second
    assert [f.qty for f in of(out, KalshiFill)] == [300, 300] and sim.stats["group_triggers"] == 0
    assert sim.order_status("a")["status"] == "resting" and sim.groups["g"].matched == 300


def test_amend_priority_rules_and_decrease():
    sim = make_sim("C")
    sched = [(0, PlaceOrder("a", T, "bid", 4500, 1000)),
             (10 * MS, AmendOrder("a", "a2", T, "", "bid", 4500, 600)),  # size down: keeps queue
             (20 * MS, DecreaseOrder("a2", T, "", 400)),
             (30 * MS, AmendOrder("a2", "a3", T, "", "bid", 4400, 400))]  # price change: back of queue
    evs = [snap(0, yes=((4500, 1000), (4400, 800))), delta(5 * MS, "yes", 4500, 300),
           delta(15 * MS, "yes", 4300, 1), delta(25 * MS, "yes", 4300, 1), delta(35 * MS, "yes", 4300, 1)]
    strat = Scripted(sched)
    qs = []
    for i in range(len(evs)):
        run_interleaved(evs[i:i + 1], strat, [sim], until_ns=evs[i].ts)
        st = sim.order_status("a")
        qs.append((st["client_order_id"], st["remaining"], st["queue_ahead"], st["px"]))
    assert qs[1] == ("a", 1000, 1000, 4500)
    assert qs[2] == ("a2", 600, 1000, 4500)  # amend down kept priority
    assert qs[3] == ("a2", 400, 1000, 4500)  # decrease kept priority
    assert qs[4] == ("a3", 400, 800, 4400)  # new price: joins behind 800
    o = strat.om.order("a3")
    assert o.state is OrderState.RESTING and o.px == 4400 and o.remaining_qty == 400


def test_invalid_price_and_duplicate_ids():
    sim = make_sim()
    spec = MarketSpec(T, "E", "KXBTCD", "greater", 85000.0, None, 0, 10 * S, 10 * S,
                      price_ranges=(PriceRange(100, 9900, 100),))
    sim.register_market(spec)
    sched = [(0, PlaceOrder("a", T, "bid", 4450, 100)), (0, PlaceOrder("b", T, "bid", 4400, 100)),
             (1, PlaceOrder("b", T, "bid", 4300, 100))]
    strat = Scripted([])
    for t, a in sched:
        sim.submit(a, t)
    run_interleaved([snap(0), delta(5, "yes", 4300, 1)], strat, [sim])
    rej = [(e.client_order_id, e.reason) for e in strat.events if isinstance(e, OrderReject)]
    assert rej == [("a", "invalid_price"), ("b", "duplicate_client_order_id")]


def test_non_kalshi_actions_are_ignored():
    sim = make_sim()
    assert sim.submit(PlaceHedge("h", "kalshi_perp", "KXBTCPERP", "buy", 0.1), 0) is False
    assert sim.next_due_ns() is None


def test_determinism_same_seed_identical_output():
    from tests.execution.helpers import gen_schedule, gen_stream

    lat = LatencyModel(5)  # lognormal defaults

    def once(seed):
        sim = make_sim("B", lat=lat, fee=simple_fee, seed=seed)
        strat = Scripted(gen_schedule(11, 12, 20 * S))
        out = run_interleaved(gen_stream(11, 400), strat, [sim])
        return out, sim.fill_log

    a, fa = once(1)
    b, fb = once(1)
    c, _ = once(2)
    assert a == b and fa == fb and len(a) > 20
    assert [e.ts for e in a] != [e.ts for e in c]
    assert no_fee(1, 1, True) == 0


def test_order_group_messages_reach_order_manager():
    from dh.core.events import KalshiOrderGroupUpdate

    sim = make_sim()
    sim.create_order_group("g", 300)
    sched = [(0, PlaceOrder("a", T, "bid", 4500, 1000, order_group_id="g"))]
    evs = [snap(0, yes=((4400, 100),)), delta(1, "yes", 4300, 1), trade(1 * S, 4500, 500, "no")]
    strat, out = run(sim, evs, sched)
    msgs = [(m.event_type, m.contracts_limit) for m in out if isinstance(m, KalshiOrderGroupUpdate)]
    assert msgs == [("created", 300), ("triggered", -1)]
    assert strat.om.group_blocked("g") and strat.om.order("a").state is OrderState.CANCELED
    assert strat.om.position(T) == 300
    sim.reset_order_group("g")
    run_interleaved([delta(2 * S, "yes", 4300, 1)], strat, [sim])
    assert not strat.om.group_blocked("g")
