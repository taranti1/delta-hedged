"""Regression cases from the September 26 profit/edge review."""
from dataclasses import replace
from types import SimpleNamespace

import pytest

from dh.core.actions import CancelOrder, Log, PlaceOrder
from dh.core.events import CancelAck, KalshiFill, OrderAck
from dh.core.units import NS_PER_S
from dh.strategy.config import FairValueCfg, StrategyConfig
from dh.strategy.fill_model import AdverseSelectionModel, FillIntensityModel
from dh.strategy.mm import MarketMaker, MarketFV
from dh.strategy.quoting import ExistingOrder, _candidate_prices, decide_side
from tests.strategy.mm_driver import T0, spec
from tests.strategy.test_strategy_parts import _ctx


def decision(ctx, **kwargs):
    cfg = StrategyConfig()
    return decide_side(ctx, "bid", FillIntensityModel(cfg.fill), AdverseSelectionModel(cfg.adverse),
                       v_min=.001, kappa_replace=0, **kwargs)


def test_full_capacity_replacement_cancels_before_placing():
    ctx = _ctx(existing={"bid": [ExistingOrder("old", 4000, 500, 0)]}, capacity=5)
    d = decision(ctx)
    assert d.cancel == ["old"] and not d.keep
    assert d.best is not None and d.best.px != 4000
    assert d.place is None  # old can still fill before cancel acknowledgement
    ctx.existing = {}  # acknowledgment removes exposure on the next decision
    assert decision(ctx).place is not None


def test_full_capacity_young_order_retains_priority():
    ctx = _ctx(existing={"bid": [ExistingOrder("young", 4000, 500, 0, age_ns=1)]}, capacity=5)
    d = decision(ctx, min_age_ns=100)
    assert d.keep == ["young"] and not d.cancel and d.place is None


def test_touch_only_has_no_improving_or_distant_fair_candidates():
    ctx = _ctx(F=.3)
    ctx.touch_only = True
    assert _candidate_prices(ctx, "bid") == [(4800, "touch")]
    assert _candidate_prices(ctx, "ask") == [(5200, "touch")]
    ctx.book.yes_bids.clear()
    assert _candidate_prices(ctx, "bid") == []


@pytest.mark.parametrize("field,value", [("tail", "student_t"), ("student_nu", 8),
    ("vol_half_life_s", 100), ("use_seasonality", False), ("nowcast", "brti_plus_composite"),
    ("nowcast_beta", .4), ("mixture_cv", .9)])
def test_unused_model_controls_fail_loudly(field, value):
    cfg = replace(StrategyConfig(), fair_value=replace(FairValueCfg(), **{field: value}))
    with pytest.raises(ValueError, match=field):
        MarketMaker(cfg, [spec()])


def fill(ticker, trade, fee, order="order", **kwargs):
    return KalshiFill(T0 + 2, T0 + 1, ticker, trade, order, "", "bid", 4500, 100, False,
                      fee, 0, False, **kwargs)


def test_fee_reconciliation_accepts_split_fill_carry_and_deduplicates():
    s = spec()
    mm = MarketMaker(StrategyConfig(), [s])
    acc = mm.fee_sched[s.ticker].order_accumulator("bid")
    for i in range(12):
        f = fill(s.ticker, str(i), acc.apply_fill(4500, 100, False).net_micros)
        assert mm._reconcile_fee(f) == []
        assert mm._reconcile_fee(f) == []
    assert mm._fee_accumulators[(s.ticker, "order", "bid")].fills == 12
    assert not mm.risk.fee_mismatch


def test_repeated_micro_fee_residuals_cannot_accumulate_without_bound_within_an_order():
    s = spec()
    mm = MarketMaker(StrategyConfig(), [s])
    expected = mm.fee_sched[s.ticker].trade_fee_micros(4500, 100, False)
    precision = mm.fee_sched[s.ticker].rates.balance_precision_micros
    for i in range(precision):
        mm._reconcile_fee(fill(s.ticker, str(i), expected + 1))
    assert mm.risk.fee_mismatch
    assert mm._fee_residual[(s.ticker, "order", "bid")] >= precision


def test_micro_fee_residuals_do_not_accumulate_across_orders():
    # a session-wide sum would halt a long, healthy session on benign one-micro differences
    s = spec()
    mm = MarketMaker(StrategyConfig(), [s])
    expected = mm.fee_sched[s.ticker].trade_fee_micros(4500, 100, False)
    for i in range(200):
        mm._reconcile_fee(fill(s.ticker, str(i), expected + 1, order=f"o{i}"))
    assert not mm.risk.fee_mismatch


def test_unexplained_fee_beyond_rounding_tolerance_halts():
    s = spec()
    mm = MarketMaker(StrategyConfig(), [s])
    bd = mm.fee_sched[s.ticker].order_accumulator("bid").apply_fill(4500, 100, False)
    tolerance = max(1, abs(bd.rounding_micros - bd.rebate_micros))
    mm._reconcile_fee(fill(s.ticker, "unexpected", max(bd.trade_micros, bd.net_micros) + tolerance + 1))
    assert mm.risk.fee_mismatch


def test_fill_log_preserves_execution_and_receipt_time():
    s = spec()
    mm = MarketMaker(StrategyConfig(), [s])
    fee = mm.fee_sched[s.ticker].trade_fee_micros(4500, 100, False)
    acts = mm.on_event(fill(s.ticker, "timestamp", fee))
    payload = next(a.payload for a in acts if isinstance(a, Log) and a.kind == "fill")
    assert payload["ts_exch"] == T0 + 1 and payload["ts_recv"] == T0 + 2


def candidate(ticker, px=4000, size=2., ev_rate=.01, score=.01):
    c = SimpleNamespace(ticker=ticker, px=px, size=size, ev_rate=ev_rate, score=score)
    c.as_log = lambda: {"ticker": ticker, "px": px, "size": size, "ev_rate": ev_rate, "score": score}
    return c


def fv():
    return MarketFV(T0, .5, .5, .5, 0., 0., 0., 100., 5.)


def test_same_settlement_multiple_series_is_counted_once():
    a, b = spec("a", event="event-a"), spec("b", event="event-b", series="KXBTC")
    cfg = StrategyConfig()
    cfg = replace(cfg, quoting=replace(cfg.quoting, enabled_series=("KXBTCD", "KXBTC")))
    mm = MarketMaker(cfg, [a, b])
    groups = mm._risk_groups(T0, 84000, [])
    assert len(groups) == 1
    assert set(next(iter(groups.values()))["specs"]) == {"a", "b"}
    c = replace(b, settlement=replace(b.settlement, include_close_tick=True))
    assert mm._settlement_key(c) != mm._settlement_key(a)


def test_scenario_admission_nets_positions_but_never_assumes_two_orders_fill():
    a, b = spec("a", K=83000), spec("b", K=85000)
    cfg = StrategyConfig()
    cfg = replace(cfg, risk=replace(cfg.risk, max_event_worst_loss=.25, max_total_worst_loss=.25))
    mm = MarketMaker(cfg, [a, b])
    mm.om.on_event(replace(fill("a", "position", 0), yes_px=2000))  # one long lower strike at 20c
    groups = mm._risk_groups(T0, 84000, [])
    ewc = {k: mm._group_loss(g) for k, g in groups.items()}
    c = candidate("b", px=8000, size=1)
    acts = mm._admit(T0, [(c.score, b, "ask", c, fv())], groups, ewc, sum(ewc.values()), 0, 0)
    assert any(isinstance(a, PlaceOrder) for a in acts)  # worst loss remains 20c, not 40c
    # Two opposite orders at one strike cannot be netted as though both fill.
    empty = MarketMaker(cfg, [a])
    groups = empty._risk_groups(T0, 84000, [])
    g = next(iter(groups.values()))
    g["admitted"] = [("a", "bid", .5, 1), ("a", "ask", .5, 1)]
    assert empty._group_loss(g) == pytest.approx(.5)


def test_better_opportunity_evicts_only_after_safe_recheck():
    a, b = spec("a"), spec("b", K=85000)
    cfg = StrategyConfig()
    cfg = replace(cfg, quoting=replace(cfg.quoting, min_order_age_ms=0, kappa_replace_per_s=0),
                  risk=replace(cfg.risk, max_event_worst_loss=1, max_total_worst_loss=1))
    mm = MarketMaker(cfg, [a, b])
    order = PlaceOrder(client_order_id="old", ticker="a", book_side="bid", px=9000, qty=100)
    mm.om.request_place(order, T0)
    mm.om.on_event(OrderAck(T0, 0, "old", "exchange-old", "a", 0, 100))
    old = candidate("a", px=9000, size=1, ev_rate=.000001, score=.000001)
    mm._retained_candidates = {"old": old}
    mm.fvc["a"] = fv()
    new = candidate("b")
    groups = mm._risk_groups(T0, 84000, mm.om.working())
    ewc = {k: mm._group_loss(g) for k, g in groups.items()}
    acts = mm._admit(T0 + NS_PER_S, [(new.score, b, "bid", new, fv())], groups, ewc,
                     sum(ewc.values()), 0, 0)
    assert any(isinstance(a, CancelOrder) for a in acts)
    assert not any(isinstance(a, PlaceOrder) for a in acts)
    assert mm.om.order("old").cancel_requested
    assert any(isinstance(a, Log) and a.kind == "opportunity_rejected" for a in acts)
    # An unacknowledged cancellation continues to reserve risk.
    groups = mm._risk_groups(T0, 84000, mm.om.working())
    assert sum(mm._group_loss(g) for g in groups.values()) == pytest.approx(.9)


def test_touch_only_pulls_resting_order_after_touch_moves():
    ctx = _ctx(existing={"bid": [ExistingOrder("old", 4000, 500, 0)]}, capacity=5)
    ctx.touch_only = True
    d = decision(ctx, min_age_ns=10**30)
    assert d.cancel == ["old"] and not d.keep and d.place is None


def test_model_artifact_and_effective_hashes_preserve_identity():
    from dh.models.fvmodel import FairValueModel, load_recommended_config
    raw = load_recommended_config()
    first = FairValueModel.from_config(raw)
    second = FairValueModel.from_config({**raw, "note": "changed provenance only"})
    assert len(first.artifact_hash) == 64
    assert first.artifact_hash != second.artifact_hash
    assert first.effective_hash == second.effective_hash
    assert first.fitted_to_utc == raw["data_end_utc"]


def test_quote_log_links_prediction_to_order_and_model():
    a = spec("a")
    mm = MarketMaker(StrategyConfig(), [a])
    groups = mm._risk_groups(T0, 84000, [])
    ewc = {k: mm._group_loss(g) for k, g in groups.items()}
    c = candidate("a")
    actions = mm._admit(T0, [(c.score, a, "bid", c, fv())], groups, ewc, sum(ewc.values()), 0, 0)
    order = next(a for a in actions if isinstance(a, PlaceOrder))
    payload = next(a.payload for a in actions if isinstance(a, Log) and a.kind == "quote")
    assert payload["coid"] == order.client_order_id
    assert payload["ts_decision"] == T0 and payload["F"] == .5
    assert payload["fv_model_hash"] == mm.fv.effective_hash
    assert payload["fv_artifact_hash"] == mm.fv.artifact_hash


def test_closed_disabled_position_is_still_in_total_risk():
    closed = replace(spec("closed", series="DISABLED"), close_ts=T0-1, expiration_ts=T0-1)
    mm = MarketMaker(StrategyConfig(), [closed])
    mm.om.on_event(replace(fill("closed", "p", 0), yes_px=9000))
    groups = mm._risk_groups(T0, 84000, [])
    assert sum(mm._group_loss(g) for g in groups.values()) == pytest.approx(.9)


def test_scenario_netting_never_bypasses_cash_collateral_budget():
    a, b = spec("a", K=83000), spec("b", K=85000)
    cfg = StrategyConfig()
    cfg = replace(cfg, risk=replace(cfg.risk, risk_capital=1.1))
    mm = MarketMaker(cfg, [a, b])
    mm.om.on_event(replace(fill("a", "position", 0), yes_px=2000))
    groups = mm._risk_groups(T0, 84000, [])
    ewc = {k: mm._group_loss(g) for k, g in groups.items()}
    c = candidate("b", px=8000, size=1)
    acts = mm._admit(T0, [(c.score, b, "ask", c, fv())], groups, ewc, sum(ewc.values()), 0, 0)
    assert not any(isinstance(a, PlaceOrder) for a in acts)
    assert any(isinstance(a, Log) and a.payload.get("constraint") == "collateral" for a in acts)


def test_unknown_cancel_outcome_keeps_scenario_and_cash_risk_reserved():
    from dh.core.events import OrderReject
    a = spec("a")
    mm = MarketMaker(StrategyConfig(), [a])
    order = PlaceOrder(client_order_id="old", ticker="a", book_side="bid", px=9000, qty=100)
    mm.om.request_place(order, T0)
    mm.om.on_event(OrderAck(T0, 0, "old", "oid", "a", 0, 100))
    mm.om.request_cancel(CancelOrder(client_order_id="old", ticker="a", order_id="oid"), T0+1)
    mm.om.on_event(OrderReject(T0+2, 0, "old", "a", "not_found", 0, "cancel"))
    old = mm.om.order("old")
    assert old.unresolved and old.remaining_qty == 0 and old.could_fill_qty == 100
    assert old.could_fill_qty == mm.om.worst_case_exposure("a", "bid")
    groups = mm._risk_groups(T0+2, 84000, [w for w in mm.om.all_orders() if w.could_fill_qty])
    assert sum(mm._group_loss(g) for g in groups.values()) == pytest.approx(.9)
    assert mm._reserved_collateral() == pytest.approx(.9)


def test_amend_up_reserves_adverse_price_and_quantity():
    from dh.core.actions import AmendOrder
    a = spec("a")
    mm = MarketMaker(StrategyConfig(), [a])
    order = PlaceOrder(client_order_id="old", ticker="a", book_side="bid", px=4000, qty=100)
    mm.om.request_place(order, T0)
    mm.om.on_event(OrderAck(T0, 0, "old", "oid", "a", 0, 100))
    mm.om.request_amend(AmendOrder(client_order_id="old", ticker="a", order_id="oid",
                                  new_client_order_id="amended", book_side="bid", px=9000, total_qty=200), T0+1)
    old = mm.om.order("old")
    assert old.could_fill_qty == 200 and old.worst_case_px == 9000
    mm.om.request_cancel(CancelOrder(client_order_id="old", ticker="a", order_id="oid"), T0+2)
    old = mm.om.order("old")
    assert old.could_fill_qty == 200 and old.worst_case_px == 9000
    assert old.could_fill_qty == mm.om.worst_case_exposure("a", "bid")
    groups = mm._risk_groups(T0+2, 84000, mm.om.all_orders())
    assert sum(mm._group_loss(g) for g in groups.values()) == pytest.approx(1.8)
    assert mm._reserved_collateral() == pytest.approx(1.8)
