"""Risk state across restarts (audit live C1; review N1, N3, N7, N8): the persisted state, the
day's P&L from Kalshi (fills, settlements, open positions at exchange prices, positions held at
midnight at the last trade), fail-closed row handling and the historical cutoff, the seed
decision (realized from both sources, a fresh mark, the halt's UTC day, the operator's fresh
budget), the runner's RiskBook; the session clock and client_order_id token."""

from __future__ import annotations

import time

import pytest

from dh.core.events import RiskStateSeed
from dh.core.units import NS_PER_S, PX_SCALE
from dh.live.clock import AnchoredClock, session_token, to_base36
from dh.live.riskstate import (
    DAY_NS,
    RiskBook,
    RiskState,
    RiskStateError,
    RiskStateStore,
    day_pnl_from_rows,
    decide_seed,
    derive_day_pnl,
    historical_row_owner,
    make_seed,
    mark_px,
    row_in_subaccount,
    sticky,
)

from .fakes import T0, FakeRest, fill_row, market_row, trade_row

TK = "KXBTCD-26SEP2513-T84000.00"
TK2 = "KXBTCD-26SEP2513-T84250.00"
DAY0 = T0 - T0 % DAY_NS
H = 3600 * NS_PER_S


def settle_row(ticker: str, result: str, yes: str = "0.00", no: str = "0.00", t_ns: int = T0, **kw) -> dict:
    return {"ticker": ticker, "event_ticker": ticker.rsplit("-", 1)[0], "market_result": result, "yes_count_fp": yes,
            "no_count_fp": no, "yes_total_cost_dollars": "0", "no_total_cost_dollars": "0",
            "revenue": 0, "settled_time": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(t_ns // NS_PER_S)),
            "fee_cost": "0", "exchange_index": 0, **kw}


# ============================================================================ the day's P&L
def test_day_pnl_realized_round_trip_is_exact():
    # bought 10 YES at 40c, sold 10 at 45c today, 2c fees: +$0.48; flat now
    fills = [fill_row("f1", "o1", TK, side="bid", px="0.4000", count="10.00", fee="0.010000", created_ns=T0),
             fill_row("f2", "o2", TK, side="ask", px="0.4500", count="10.00", fee="0.010000", created_ns=T0 + 1)]
    d = day_pnl_from_rows(DAY0, fills, [], {})
    assert d.fills == 2 and d.positions_now == {} and d.positions_midnight == {} and d.fallbacks == []
    assert d.pnl_usd == pytest.approx(0.48) and d.realized_usd == pytest.approx(0.48) and d.open_usd == 0.0


def test_open_positions_are_valued_at_exchange_prices_with_logged_fallbacks():
    # 5 NO bought today (= YES sold at 60c: +$3.00 in YES terms), NO wins: YES-terms payout 0 ->
    # +$3.00 = Kalshi's -$2.00 cost + $5.00 revenue. 2 YES bought at 30c still open, YES bid 29c.
    fills = [fill_row("f1", "o1", TK, side="ask", px="0.6000", count="5.00", created_ns=T0),
             fill_row("f2", "o2", TK2, side="bid", px="0.3000", count="2.00", created_ns=T0)]
    settles = [settle_row(TK, "no", no="5.00", revenue=500)]
    d = day_pnl_from_rows(DAY0, fills, settles, {TK2: 200}, open_px={TK2: 2900})
    assert d.settlements_usd == pytest.approx(0.0) and d.cash_usd == pytest.approx(3.0 - 0.6)
    assert d.open_usd == pytest.approx(0.58) and d.pnl_usd == pytest.approx(2.98) and d.fallbacks == []
    assert d.realized_usd == pytest.approx(2.4)
    # no price at all: the worst case, and it is listed (the app logs it)
    d = day_pnl_from_rows(DAY0, fills, settles, {TK2: 200})
    assert d.open_usd == 0.0 and d.pnl_usd == pytest.approx(2.4) and "no exchange price" in d.fallbacks[0]
    short = day_pnl_from_rows(DAY0, [fill_row("f3", "o3", TK, side="ask", px="0.6000", count="5.00", created_ns=T0)],
                              [], {TK: -500})
    assert short.open_usd == pytest.approx(-5.0) and short.pnl_usd == pytest.approx(3.0 - 5.0) and short.fallbacks
    # at its YES ask instead: 5 short at 61c
    short = day_pnl_from_rows(DAY0, [fill_row("f3", "o3", TK, side="ask", px="0.6000", count="5.00", created_ns=T0)],
                              [], {TK: -500}, open_px={TK: 6100})
    assert short.pnl_usd == pytest.approx(3.0 - 3.05)


def test_positions_held_at_midnight_are_valued_at_the_last_trade_before_it():
    # 4 YES held from yesterday settled YES today: +$4 today, minus their value at midnight
    # (last trade 70c) -> +$1.20 for today
    s = [settle_row(TK, "yes", yes="4.00", revenue=400)]
    d = day_pnl_from_rows(DAY0, [], s, {}, midnight_px={TK: 7000})
    assert d.positions_midnight == {TK: 400} and d.midnight_usd == pytest.approx(2.8)
    assert d.pnl_usd == pytest.approx(1.2) and d.fallbacks == []
    # no trade before midnight: worst case for a long held then ($1) -> 0, listed
    d = day_pnl_from_rows(DAY0, [], s, {})
    assert d.pnl_usd == pytest.approx(0.0) and "no trade at or before midnight" in d.fallbacks[0]
    # fills before midnight are not today's
    old = fill_row("f0", "o0", TK, side="bid", px="0.5000", count="1.00", created_ns=DAY0 - NS_PER_S)
    assert day_pnl_from_rows(DAY0, [old], [], {}).fills == 0


def test_settlement_fee_cost_is_not_a_second_charge():
    """Settlement.fee_cost is 'Total fees paid' (next to the cost basis): the fees were paid by
    the fills, which count them once."""
    fills = [fill_row("f1", "o1", TK, side="bid", px="0.4000", count="1.00", fee="0.020000", created_ns=T0)]
    s = [settle_row(TK, "yes", yes="1.00", fee_cost="0.0200")]
    assert day_pnl_from_rows(DAY0, fills, s, {}).pnl_usd == pytest.approx(1.0 - 0.4 - 0.02)


@pytest.mark.parametrize("bad", [
    lambda: fill_row("f1", "o1", TK, created_ns=T0) | {"created_time": None},
    lambda: fill_row("f1", "o1", TK, created_ns=T0) | {"count_fp": "abc"},
    lambda: fill_row("f1", "o1", TK, created_ns=T0) | {"yes_price_dollars": None, "no_price_dollars": None},
    lambda: {k: v for k, v in fill_row("f1", "o1", TK, created_ns=T0).items() if k != "order_id"},
    lambda: fill_row("", "o1", TK, created_ns=T0),
    lambda: "not a row",
])
def test_malformed_or_timeless_fill_rows_fail_closed(bad):
    with pytest.raises(RiskStateError):
        day_pnl_from_rows(DAY0, [bad()], [], {})


@pytest.mark.parametrize("bad", [
    settle_row(TK, "yes", yes="1.00") | {"settled_time": ""},
    settle_row(TK, "scalar", yes="1.00"),  # no value: payout unknown
    settle_row(TK, "yes", yes="x"),
    settle_row("", "yes", yes="1.00"),
])
def test_malformed_or_timeless_settlement_rows_fail_closed(bad):
    with pytest.raises(RiskStateError):
        day_pnl_from_rows(DAY0, [], [bad], {})


def test_subaccount_rows():
    assert row_in_subaccount({}, 0) and not row_in_subaccount({"subaccount_number": 3}, 0)
    assert row_in_subaccount({"subaccount_number": 3}, 3) and row_in_subaccount({}, 3)  # omitted: trust the query
    assert not row_in_subaccount({"subaccount_number": 0}, 3) and not row_in_subaccount({"subaccount": "x"}, 3)
    rows = [fill_row("f1", "o1", TK, side="bid", px="0.5000", count="1.00", created_ns=T0),
            fill_row("f2", "o2", TK, side="bid", px="0.5000", count="1.00", created_ns=T0, subaccount=5)]
    assert day_pnl_from_rows(DAY0, rows, [], {}).fills == 1
    # /historical/fills has no subaccount filter: only rows that say whose they are count
    assert historical_row_owner({}, 0) is True and historical_row_owner({"subaccount_number": 3}, 3) is True
    assert historical_row_owner({"subaccount_number": 3}, 0) is False and historical_row_owner({}, 3) is None
    with pytest.raises(RiskStateError, match="cannot be attributed"):
        day_pnl_from_rows(DAY0, [], [], {}, subaccount=3, historical_fills=[fill_row("h1", "o1", TK, created_ns=T0)])


def test_mark_px_rules():
    assert mark_px(market_row(TK, result="yes", status="determined"), 100) == (PX_SCALE, "payout (yes)")
    assert mark_px(market_row(TK, result="no", status="finalized"), -100)[0] == 0
    assert mark_px(market_row(TK, result="scalar", settlement_value="0.3700"), 100)[0] == 3700
    assert mark_px(market_row(TK, bid="0.4100", ask="0.4300"), 100) == (4100, "YES bid")
    assert mark_px(market_row(TK, bid="0.4100", ask="0.4300"), -100) == (4300, "YES ask")
    assert mark_px(market_row(TK, bid="0.0000", status="closed", last="0.3900"), 100) == (3900, "last trade (closed)")
    p, why = mark_px(market_row(TK, bid="0.0000"), 100)  # active without a bid: no price
    assert p is None and "no YES bid" in why
    assert mark_px(market_row(TK, ask="1.0000"), -100)[0] is None


async def test_derive_day_pnl_uses_the_cutoff_prices_and_explicit_subaccount():
    rest = FakeRest()
    cut = DAY0 + H  # the first hour of today is historical already
    rest.cutoff = {"trades_created_ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(cut // NS_PER_S)),
                   "market_settled_ts": "2026-01-01T00:00:00Z", "orders_updated_ts": "2026-01-01T00:00:00Z"}
    early = fill_row("f0", "o0", TK, side="bid", px="0.5000", count="2.00", fee="0.010000", created_ns=DAY0 + 600 * NS_PER_S)
    late = fill_row("f1", "o1", TK, side="bid", px="0.5000", count="2.00", fee="0.010000", created_ns=T0)
    rest.historical_fills = [early]
    rest.fills = [late, early]  # the boundary may overlap: de-duplicated by fill id
    rest.markets[TK] = market_row(TK, bid="0.4800", ask="0.5200")
    # 1 YES held at midnight in TK2 (sold today at 55c); its last trade before 00:00 is historical
    rest.fills.append(fill_row("f2", "o2", TK2, side="ask", px="0.5500", count="1.00", created_ns=T0))
    rest.historical_trades = [trade_row("t1", TK2, px="0.5100", created_ns=DAY0 - 120 * NS_PER_S),
                              trade_row("t0", TK2, px="0.4000", created_ns=DAY0 - 900 * NS_PER_S)]
    rest.trades = [trade_row("t2", TK2, px="0.9000", created_ns=DAY0 + 60 * NS_PER_S)]  # after midnight: ignored
    d = await derive_day_pnl(rest, DAY0, {TK: 400}, subaccount=0)
    assert rest.of("iter_fills")[0][1] == {"min_ts": DAY0 // NS_PER_S, "subaccount": 0}
    assert rest.of("iter_historical_fills")[0][1] == {"min_ts": DAY0 // NS_PER_S, "max_ts": cut // NS_PER_S}
    assert rest.of("iter_settlements")[0][1] == {"min_ts": DAY0 // NS_PER_S, "subaccount": 0}
    assert d.fills == 3 and d.historical_fills == 0  # 'early' counted once (from the live page)
    assert d.open_px == {TK: 4800} and d.positions_midnight == {TK2: 100} and d.midnight_px == {TK2: 5100}
    assert all(k["historical"] for _, k in rest.of("iter_trades"))  # midnight is before the cutoff
    # cash -2.00 + 0.55, fees 0.02, open 4 x 48c, midnight 1 x 51c
    assert d.pnl_usd == pytest.approx(-2.0 + 0.55 - 0.02 + 1.92 - 0.51) and d.fallbacks == []


async def test_derive_day_pnl_fails_closed():
    rest = FakeRest().on("get_historical_cutoff", ConnectionError("down"))
    with pytest.raises(RiskStateError, match="historical/cutoff"):
        await derive_day_pnl(rest, DAY0, {})
    rest = FakeRest()
    rest.fills = [fill_row("f1", "o1", TK, created_ns=T0) | {"created_time": ""}]
    with pytest.raises(RiskStateError, match="without created_time"):
        await derive_day_pnl(rest, DAY0, {})
    rest = FakeRest().on("get_markets", ConnectionError("down"))
    with pytest.raises(RiskStateError, match="could not be derived"):
        await derive_day_pnl(rest, DAY0, {TK: 100})


# ============================================================================ persistence
def test_store_roundtrip_unreadable_and_directory_fsync(tmp_path, monkeypatch):
    import dh.live.riskstate as rs

    synced: list[str] = []
    monkeypatch.setattr(rs, "fsync_dir", lambda p: synced.append(str(p)))
    st = RiskStateStore(tmp_path / "s" / "risk.json")
    assert st.load() is None
    s = RiskState(DAY0, -3.5, True, "daily_loss", 7, "sess", "live", T0, realized_usd=-1.5, mark_usd=-2.0,
                  budget_base_usd=0.5, halt_scope="all", halt_day_ns=DAY0)
    st.save(s, fsync=True)
    assert st.load() == s and synced == [str(tmp_path / "s")]  # the rename is made durable too
    st.save(s)
    assert synced == [str(tmp_path / "s")]  # periodic saves skip the fsyncs
    (tmp_path / "s" / "risk.json").write_text('{"day_start_ns": 1, "day_pnl_usd": -2.0}')
    assert st.load().realized_usd is None  # written before the realized / mark split
    (tmp_path / "s" / "risk.json").write_text("{not json")
    with pytest.raises(RiskStateError):
        st.load()


# ============================================================================ the seed
def _prev(pnl=-1.0, realized=None, mark=0.0, halted=False, reason="", pause=0, halt_day=0, base=0.0, scope=""):
    return RiskState(DAY0, pnl, halted, reason, pause, "s1", "live", DAY0 + H, realized_usd=realized, mark_usd=mark,
                     budget_base_usd=base, halt_scope=scope, halt_day_ns=halt_day)


def test_seed_takes_the_lower_realized_plus_a_fresh_mark():
    now = DAY0 + 10 * H
    # REST: 10 YES bought at 50c today, bid 48c now -> realized -5, open +4.80
    rest = day_pnl_from_rows(DAY0, [fill_row("f1", "o1", TK, px="0.5000", count="10.00", created_ns=now)], [],
                             {TK: 1000}, open_px={TK: 4800})
    # persisted by the previous session: realized -5.30 (it also paid fees REST did not see), and a
    # pessimistic mark of +1.00 that must NOT be locked in
    d = decide_seed(now, _prev(pnl=-4.30, realized=-5.30, mark=1.0), rest)
    assert d.realized_usd == pytest.approx(-5.30) and d.mark_usd == pytest.approx(4.80)
    assert d.real_pnl_usd == d.day_pnl_usd == pytest.approx(-0.50) and not d.halted
    # a state file from before the split: its total caps the result
    d = decide_seed(now, _prev(pnl=-3.0), rest)
    assert d.real_pnl_usd == pytest.approx(-3.0)
    # paper (no REST): the persisted realized and mark
    d = decide_seed(now, _prev(pnl=-4.30, realized=-5.30, mark=1.0), None)
    assert d.real_pnl_usd == pytest.approx(-4.30)


def test_seed_halt_rules_and_the_halt_day():
    now = DAY0 + 10 * H
    prev = _prev(pnl=-24.99, realized=-24.99, halted=True, reason="carried_over:daily_loss",
                 pause=now + H, halt_day=DAY0)
    d = decide_seed(now, prev, None)
    assert d.halted and d.halt_reason == "daily_loss" and d.pause_until_ns == now + H and d.halt_day_ns == DAY0
    assert d.day_pnl_usd == pytest.approx(-24.99) and d.halt_scope == "all"
    # next UTC day: the daily-loss halt and P&L do not carry; a reconciliation halt does
    tomorrow = now + DAY_NS
    assert not decide_seed(tomorrow, prev, None).halted and decide_seed(tomorrow, prev, None).day_pnl_usd == 0.0
    recon = _prev(pnl=0.0, realized=0.0, halted=True, reason="reconciliation:position_mismatch", halt_day=DAY0)
    assert decide_seed(tomorrow, recon, None).halted and sticky("fee_mismatch:x") and not sticky("daily_loss")
    assert sticky("watchdog_cancel_all")
    # N3: a daily-loss halt decided on day D, still in force (runner up) after midnight, is
    # persisted with day_start D+1 -- it keeps its own day and is not carried on D+1
    redated = RiskState(DAY0 + DAY_NS, 0.0, True, "daily_loss", 0, "s1", "live", DAY0 + DAY_NS + 2 * NS_PER_S,
                        realized_usd=0.0, halt_scope="all", halt_day_ns=DAY0)
    d = decide_seed(DAY0 + DAY_NS + 8 * H, redated, None)
    assert not d.halted and any("not carried" in n for n in d.notes)
    # the scope of a quoting halt is kept
    fee = _prev(realized=0.0, halted=True, reason="fee_mismatch", scope="quoting", halt_day=DAY0)
    assert decide_seed(now, fee, None).halt_scope == "quoting"


def test_operator_reset_starts_a_fresh_budget_from_the_real_pnl():
    now = DAY0 + 10 * H
    prev = _prev(pnl=-26.0, realized=-26.0, halted=True, reason="daily_loss", pause=now + H, halt_day=DAY0)
    r = decide_seed(now, prev, None, reset=True)
    assert (r.day_pnl_usd, r.halted, r.pause_until_ns) == (0.0, False, 0)
    assert r.real_pnl_usd == pytest.approx(-26.0) and r.budget_base_usd == pytest.approx(-26.0)  # stays recorded
    assert r.overridden["halted"] and r.overridden["day_pnl_usd"] == pytest.approx(-26.0)
    # a later restart the same day keeps the base: only losses after the reset count
    later = RiskState(DAY0, -30.0, False, "", 0, "s2", "live", now + H, realized_usd=-30.0, budget_base_usd=-26.0)
    d = decide_seed(now + 2 * H, later, None)
    assert d.real_pnl_usd == pytest.approx(-30.0) and d.day_pnl_usd == pytest.approx(-4.0)
    # ... and the base lapses at midnight
    assert decide_seed(now + DAY_NS, later, None).budget_base_usd == 0.0


def test_make_seed_is_the_first_event():
    d = decide_seed(DAY0 + H, _prev(realized=-2.0, halted=True, reason="reconciliation:x", halt_day=DAY0), None)
    ev = make_seed(DAY0 + H, d)
    assert isinstance(ev, RiskStateSeed) and ev.halted and ev.day_pnl_usd == pytest.approx(-2.0)
    assert ev.day_start_ns == DAY0 and ev.halt_reason == "reconciliation:x"


# ============================================================================ the runner's book
def test_riskbook_realizes_excluded_settlements_and_rolls_at_midnight():
    now = DAY0 + 14 * H
    d = decide_seed(now, None, day_pnl_from_rows(DAY0, [fill_row("f1", "o1", TK, side="ask", px="0.2000", count="10.00",
                                                                  created_ns=now - H)], [], {TK: -1000},
                                                 open_px={TK: 2000}))
    assert d.real_pnl_usd == pytest.approx(0.0)  # sold at 20c, valued at the 20c ask
    b = RiskBook.from_decision(d, {TK: (-1000, 2000)})
    assert b.realized_usd == pytest.approx(2.0) and b.mark_usd() == pytest.approx(-2.0) and b.seed_value(now) == 0.0
    assert b.settle(TK, 0, now + H) == (-1000, 2000)  # settled NO: the short keeps the $2
    assert b.seed_value(now + H) == pytest.approx(2.0) and b.excluded == {}
    # a position held over midnight: the new day starts from its value at midnight
    b2 = RiskBook.from_decision(d, {TK: (-1000, 2000)})
    b2.base_usd = -5.0
    assert b2.roll(DAY0 + DAY_NS + 1) and b2.seed_value(DAY0 + DAY_NS + 1) == 0.0 and b2.base_usd == 0.0
    b2.settle(TK, PX_SCALE, DAY0 + DAY_NS + H)  # settled YES the next day: -$8 vs its midnight value
    assert b2.seed_value(DAY0 + DAY_NS + H) == pytest.approx(-8.0)
    b2.note_seed(RiskStateSeed(DAY0 + DAY_NS + H, 0, DAY0 + DAY_NS, -8.0))
    assert b2.carried_seed(DAY0 + DAY_NS + 2 * H) == -8.0 and b2.carried_seed(DAY0 + 2 * DAY_NS) == 0.0
    b2.note_seed(RiskStateSeed(DAY0 + 2 * DAY_NS, 0, DAY0, -1.0))  # a seed for another day is ignored (as the strategy does)
    assert b2.carried_seed(DAY0 + DAY_NS + 2 * H) == -8.0


# ============================================================================ review N1: the reproducer's cases
def test_restart_seeds_are_realistic_for_the_reviewed_cases():
    """n1b: A open shorts at 14:00:10, B longs held over midnight into a market that settled NO,
    C one event short (then it settles NO). Seeds match the true day P&L; nothing halts."""
    E14a, E14b, E15, E15b = ("KXBTCD-26SEP2514-T84000.00", "KXBTCD-26SEP2514-T84500.00",
                             "KXBTCD-26SEP2515-T84000.00", "KXBTCD-26SEP2515-T84500.00")
    E01 = "KXBTCD-26SEP2501-T84000.00"

    def rt(i, t, qty, buy, sell, tk=TK):
        return [fill_row(f"r{i}a", f"o{i}a", tk, side="bid", px=buy, count=qty, created_ns=t),
                fill_row(f"r{i}b", f"o{i}b", tk, side="ask", px=sell, count=qty, created_ns=t + 60 * NS_PER_S)]

    # A: true -2 (round trip -2; 30 shorts sold at 20c, valued at 20c: the 14:00 markets closed
    # (last trade 20c), the 15:00 one at its 20c ask)
    fa = rt(1, DAY0 + 10 * H, "10.00", "0.5000", "0.3000") + [
        fill_row(f"s{i}", f"p{i}", tk, side="ask", px="0.2000", count="10.00", created_ns=DAY0 + 13 * H + 40 * 60 * NS_PER_S)
        for i, tk in enumerate((E14a, E14b, E15))]
    rest = day_pnl_from_rows(DAY0, fa, [], {E14a: -1000, E14b: -1000, E15: -1000},
                             open_px={E14a: 2000, E14b: 2000, E15: 2000})
    prev = RiskState(DAY0, -2.0, False, "", 0, "prev", "live", DAY0 + 14 * H, realized_usd=4.0, mark_usd=-6.0)
    d = decide_seed(DAY0 + 14 * H + 10 * NS_PER_S, prev, rest)
    assert d.day_pnl_usd == pytest.approx(-2.0) and not d.halted
    # B: true -18 (round trip -15; 10 YES held at midnight worth 30c, settled NO)
    fb = rt(2, DAY0 + 3 * H, "30.00", "0.6000", "0.1000")
    sb = [settle_row(E01, "no", yes="10.00", t_ns=DAY0 + H + 30 * NS_PER_S, value=0)]
    rest = day_pnl_from_rows(DAY0, fb, sb, {}, midnight_px={E01: 3000})
    prev = RiskState(DAY0, -18.0, False, "", 0, "prev", "live", DAY0 + 7 * H, realized_usd=-18.0)
    d = decide_seed(DAY0 + 8 * H, prev, rest)
    assert d.day_pnl_usd == pytest.approx(-18.0) and not d.halted
    # C: true -2 at 14:00; +2 after the NO settlement (later restart: persisted realized +2, mark gone)
    fc = rt(3, DAY0 + 10 * H, "10.00", "0.5000", "0.3000") + [
        fill_row("s4", "p4", E15, side="ask", px="0.2000", count="10.00", created_ns=DAY0 + 13 * H),
        fill_row("s5", "p5", E15b, side="ask", px="0.2000", count="10.00", created_ns=DAY0 + 13 * H)]
    rest = day_pnl_from_rows(DAY0, fc, [], {E15: -1000, E15b: -1000}, open_px={E15: 2000, E15b: 2000})
    prev = RiskState(DAY0, -2.0, False, "", 0, "prev", "live", DAY0 + 14 * H, realized_usd=2.0, mark_usd=-4.0)
    assert decide_seed(DAY0 + 14 * H + 10 * NS_PER_S, prev, rest).day_pnl_usd == pytest.approx(-2.0)
    sc = [settle_row(tk, "no", no="10.00", t_ns=DAY0 + 15 * H + 30 * NS_PER_S, value=0) for tk in (E15, E15b)]
    rest = day_pnl_from_rows(DAY0, fc, sc, {})
    assert decide_seed(DAY0 + 16 * H, prev, rest).day_pnl_usd == pytest.approx(2.0)


# ============================================================================ clock / token
def test_anchored_clock_is_monotonic_and_strict_across_wall_steps():
    wall = {"t": 1_000 * NS_PER_S}
    mono = {"t": 5 * NS_PER_S}
    c = AnchoredClock(lambda: wall["t"], lambda: mono["t"])
    a = c()
    assert a == 1_000 * NS_PER_S
    assert c() == a + 1 and c() == a + 2  # strictly increasing even when time stands still
    mono["t"] += NS_PER_S
    wall["t"] -= 30 * NS_PER_S  # the wall clock is stepped back 30 s
    b = c()
    assert b == a + NS_PER_S  # follows the monotonic clock, never the step
    assert c.drift_ns() == pytest.approx(-31 * NS_PER_S, abs=10)


def test_session_token():
    t = session_token(T0, 0)
    assert len(t) == 8 and t.isalnum() and t == t.lower()
    assert session_token(T0, 1) != t and session_token(T0 + NS_PER_S, 0) != t
    assert session_token(T0 + NS_PER_S, 0) > t  # sortable by start second
    assert to_base36(0) == "0" and to_base36(35) == "z" and to_base36(36, 3) == "010"
