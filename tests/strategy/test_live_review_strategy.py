"""Strategy-side fixes from the independent live-runner review.

C1 (risk state across restarts), C2 (re-cancel after a cancel timeout), M1/M3 (runner lag and
reconciliation streams), M5 (bounded order book-keeping), M6 (session id prefix), m3 (a fill's
post_position disagreement pauses and reconciles instead of halting), m5 (a cycle that halts
places nothing), m6 (a declared-missing order found resting is pulled), m7 (base fee restored
when an event override is cleared), m11 (order-group reset re-sent until confirmed).
"""
from __future__ import annotations

from dataclasses import replace

from dh.core.actions import CancelAll, CancelOrder, Halt, Log, PlaceOrder, ResetOrderGroup
from dh.core.events import (
    CancelAck,
    FeedStatus,
    KalshiBookSnapshot,
    KalshiFeeUpdate,
    KalshiFill,
    KalshiOrderGroupUpdate,
    KalshiOrderUpdate,
    OrderAck,
    OrderReject,
    RiskStateSeed,
    Timer,
)
from dh.core.units import NS_PER_S

from .mm_driver import T0, TICK, Driver, spec

DAY_START = T0 - T0 % (86_400 * NS_PER_S)


def _ready(d: Driver) -> None:
    d.feed(FeedStatus(T0, 0, "kalshi.ws", "connected"))
    d.feed(KalshiBookSnapshot(T0, 0, TICK, 1, 1, ((4500, 10000),), ((4500, 10000),)))
    d.inputs(T0)


def _places(acts):
    return [a for a in acts if isinstance(a, PlaceOrder)]


def _rest_all(d: Driver, acts) -> list[PlaceOrder]:
    """Ack every PlaceOrder in acts (they rest on the exchange)."""
    ps = _places(acts)
    for a in ps:
        d.feed(OrderAck(d.now, 0, a.client_order_id, "X" + a.client_order_id, a.ticker, 0, a.qty, "create"))
    return ps


def _loss_fill(d: Driver, ts: int, px: int = 9900, qty: int = 400) -> None:
    """An orphan fill buying YES far above fair value: an immediate mark-to-fair loss."""
    d.feed(KalshiFill(ts, 0, TICK, f"t{ts}", "oid-x", "", "bid", px, qty, False, 0, 0, False))


# ---------------------------------------------------------------------------------- C1
def test_seeded_day_loss_counts_toward_the_daily_halt():
    d = Driver([spec()])
    lim = d.cfg.risk.daily_loss_halt
    d.feed(RiskStateSeed(T0, 0, DAY_START, day_pnl_usd=-(lim - 1.0)))
    _ready(d)
    assert _places(d.advance(T0 + 6 * NS_PER_S))  # $1 of today's budget left: still trading
    _loss_fill(d, d.now + 1)  # about -$2 more at fair ~0.5
    acts = d.advance(d.now + 1 * NS_PER_S)
    assert d.mm.halted_all and any(isinstance(a, Halt) for a in acts)
    assert d.mm.risk.day_pnl(d.now, d.mm.equity(84000.0)) <= -lim


def test_same_session_loss_without_seed_does_not_halt():
    d = Driver([spec()])
    _ready(d)
    d.advance(T0 + 6 * NS_PER_S)
    _loss_fill(d, d.now + 1)
    d.advance(d.now + 1 * NS_PER_S)
    assert not d.mm.halted_all  # ~-$2 alone is far from the $25 limit


def test_carried_over_halt_and_pause():
    d = Driver([spec()])
    acts = d.feed(RiskStateSeed(T0, 0, DAY_START, day_pnl_usd=-3.0, halted=True, halt_reason="daily_loss"))
    assert d.mm.halted_all and any(isinstance(a, Halt) for a in acts)
    _ready(d)
    assert not _places(d.advance(T0 + 6 * NS_PER_S))
    d2 = Driver([spec()])
    d2.feed(RiskStateSeed(T0, 0, DAY_START, pause_until_ns=T0 + 60 * NS_PER_S))
    _ready(d2)
    assert not _places(d2.advance(T0 + 30 * NS_PER_S))
    assert _places(d2.advance(T0 + 65 * NS_PER_S))


def test_seed_for_another_day_is_ignored():
    d = Driver([spec()])
    d.feed(RiskStateSeed(T0, 0, DAY_START - 86_400 * NS_PER_S, day_pnl_usd=-30.0, halted=True))
    assert not d.mm.halted_all
    _ready(d)
    assert _places(d.advance(T0 + 6 * NS_PER_S))


# ---------------------------------------------------------------------------------- m5
def test_the_cycle_that_halts_places_nothing():
    d = Driver([spec()])
    d.feed(RiskStateSeed(T0, 0, DAY_START, day_pnl_usd=-(d.cfg.risk.daily_loss_halt - 0.5)))
    _ready(d)
    d.advance(T0 + 6 * NS_PER_S)
    _loss_fill(d, d.now + 1)
    d.now += 600_000_000  # one quote period (500 ms in the test config)
    d.inputs(d.now)
    acts = d.feed(Timer(d.now))
    assert any(isinstance(a, Halt) for a in acts) and not _places(acts)


# ---------------------------------------------------------------------------------- M1 / M3
def test_runner_lag_and_reconcile_streams_pull_quotes_and_block_quoting():
    for stream in ("runner.lag", "kalshi.reconcile"):
        d = Driver([spec()])
        _ready(d)
        _rest_all(d, d.advance(T0 + 6 * NS_PER_S))
        acts = d.feed(FeedStatus(d.now + 1, 0, stream, "stale"))
        assert any(isinstance(a, CancelAll) for a in acts)
        assert any(isinstance(a, CancelOrder) for a in acts)
        assert not _places(d.advance(d.now + 3 * NS_PER_S))
        d.feed(FeedStatus(d.now + 1, 0, stream, "resumed" if stream == "runner.lag" else "resynced"))
        assert _places(d.advance(d.now + 3 * NS_PER_S)), stream


# ---------------------------------------------------------------------------------- C2
def test_cancel_without_answer_is_resent_until_it_resolves():
    d = Driver([spec()])
    _ready(d)
    ps = _rest_all(d, d.advance(T0 + 6 * NS_PER_S))
    acts = d.feed(FeedStatus(d.now + 1, 0, "runner.lag", "stale"))  # pulls every quote
    coids = {a.client_order_id for a in acts if isinstance(a, CancelOrder)}
    assert coids == {p.client_order_id for p in ps}
    retries = []
    for _ in range(3):  # no answer: re-sent every change timeout (10 s)
        acts = d.advance(d.now + 11 * NS_PER_S)
        retries.append({a.client_order_id for a in acts if isinstance(a, CancelOrder) and a.reason == "cancel_retry"})
    assert retries == [coids] * 3
    for c in coids:
        d.feed(CancelAck(d.now + 1, 0, c, "X" + c, TICK, 0))
    acts = d.advance(d.now + 25 * NS_PER_S)
    assert not [a for a in acts if isinstance(a, CancelOrder) and a.reason == "cancel_retry"]


# ---------------------------------------------------------------------------------- m6
def test_declared_missing_order_found_resting_is_pulled():
    d = Driver([spec()])
    _ready(d)
    (p, *_) = _places(d.advance(T0 + 6 * NS_PER_S))
    d.feed(OrderReject(d.now + 1, 0, p.client_order_id, TICK, "missing_after_lookup", 0, "create"))
    acts = d.feed(KalshiOrderUpdate(d.now + 2, 0, TICK, "X1", p.client_order_id, "resting", p.book_side, p.px,
                                    p.qty, 0, p.qty))
    assert [a.reason for a in acts if isinstance(a, CancelOrder) and a.client_order_id == p.client_order_id] == ["revived"]


# ---------------------------------------------------------------------------------- m3
def test_fill_post_position_mismatch_pauses_and_requests_reconciliation():
    d = Driver([spec()])
    _ready(d)
    _rest_all(d, d.advance(T0 + 6 * NS_PER_S))
    acts = d.mm.on_event(KalshiFill(d.now + 1, 0, TICK, "tz", "oid-y", "", "bid", 4500, 100, False, 0, 700, True))
    assert not d.mm.halted_all and not any(isinstance(a, Halt) for a in acts)
    assert any(isinstance(a, CancelAll) for a in acts)
    assert any(isinstance(a, Log) and a.payload.get("event") == "reconcile_requested" for a in acts)
    assert not _places(d.advance(d.now + 10 * NS_PER_S))  # paused (own_gap_pause_s)


# ---------------------------------------------------------------------------------- M5 / M6
def test_order_bookkeeping_is_pruned_and_ids_carry_the_session_prefix():
    d = Driver([spec()], id_prefix="dhm1-k3x9")
    _ready(d)
    ps = _places(d.advance(T0 + 6 * NS_PER_S))
    assert ps and all(p.client_order_id.startswith("dhm1-k3x9-") for p in ps)
    # churn: every order rests, is pulled and acked canceled; old terminal orders are forgotten
    for _ in range(40):
        for p in _rest_all(d, d.advance(d.now + 2 * NS_PER_S)):
            d.feed(CancelAck(d.now + 1, 0, p.client_order_id, "X" + p.client_order_id, TICK, p.qty))
        d.feed(FeedStatus(d.now + 2, 0, "runner.lag", "stale"))
        d.feed(FeedStatus(d.now + 3, 0, "runner.lag", "resumed"))
    placed = d.mm.ids.n
    d.advance(d.now + 700 * NS_PER_S, step_ms=5000)
    assert placed > 40 and len(d.mm.om.all_orders()) < 20, (len(d.mm.om.all_orders()), placed)


# ---------------------------------------------------------------------------------- m7
def test_clearing_an_event_override_restores_the_series_fee():
    s = replace(spec(), fee_type="quadratic", base_fee_type="quadratic_with_maker_fees", base_fee_multiplier=1.0)
    d = Driver([s])
    _ready(d)
    assert d.mm._order_fee(TICK, 5000, 5.0, "bid") == 0.0  # override in force at start-up
    d.feed(KalshiFeeUpdate(T0, 0, "KXBTCD-TEST", None, None))  # override cleared
    assert d.mm.fee_sched[TICK].fee_type == "quadratic_with_maker_fees"
    assert d.mm._order_fee(TICK, 5000, 5.0, "bid") > 0.0


# ---------------------------------------------------------------------------------- m11
def test_order_group_reset_is_resent_until_confirmed():
    d = Driver([spec()])
    _ready(d)
    d.advance(T0 + 6 * NS_PER_S)
    d.feed(KalshiOrderGroupUpdate(d.now + 1, 0, "dh-main", "triggered"))
    cool = int(d.cfg.risk.order_group_cooldown_s * NS_PER_S)
    resets = [a for a in d.advance(d.now + cool + 2 * NS_PER_S, step_ms=1000) if isinstance(a, ResetOrderGroup)]
    assert len(resets) == 1
    resets = [a for a in d.advance(d.now + cool + 2 * NS_PER_S, step_ms=1000) if isinstance(a, ResetOrderGroup)]
    assert len(resets) == 1 and resets[0].reason == "reset not confirmed"
    d.feed(KalshiOrderGroupUpdate(d.now + 1, 0, "dh-main", "reset"))
    resets = [a for a in d.advance(d.now + 2 * cool, step_ms=1000) if isinstance(a, ResetOrderGroup)]
    assert resets == []


def test_clock_gate_stream_pulls_quotes_and_blocks_quoting():
    d = Driver([spec()])
    _ready(d)
    _rest_all(d, d.advance(T0 + 6 * NS_PER_S))
    acts = d.feed(FeedStatus(d.now + 1, 0, "runner.clock", "stale", "clock offset 300 ms"))
    assert any(isinstance(a, CancelAll) and a.reason == "clock_offset" for a in acts)
    assert not _places(d.advance(d.now + 3 * NS_PER_S))
    assert "clock_offset" in d.mm.risk.health(d.now).reasons
    d.feed(FeedStatus(d.now + 1, 0, "runner.clock", "resumed"))
    assert _places(d.advance(d.now + 3 * NS_PER_S))


def test_order_expiry_is_spread_deterministically():
    """Live review nit: quotes placed together must not all expire in the same instant; the
    spread is a stable function of the client_order_id, so replay reproduces it."""
    from dataclasses import replace as dc_replace

    from dh.backtest.kat import default_kat_config

    base = default_kat_config()
    cfg = dc_replace(base, quoting=dc_replace(base.quoting, order_expiry_s=120.0))
    d = Driver([spec()], cfg=cfg)
    _ready(d)
    ps = _places(d.advance(T0 + 6 * NS_PER_S))
    assert len(ps) >= 2
    life = [(p.expiration_ts - d.mm.om.order(p.client_order_id).created_ns) / NS_PER_S for p in ps]
    assert all(120.0 <= x <= 150.0 for x in life), life
    assert len({round(x, 3) for x in life}) == len(life)  # spread, not all the same
    assert d.mm._expiry_ns(T0, "x-1") == d.mm._expiry_ns(T0, "x-1")  # deterministic
    d0 = Driver([spec()])  # order_expiry_s = 0 -> good till canceled
    assert d0.mm._expiry_ns(T0, "x-1") == 0
