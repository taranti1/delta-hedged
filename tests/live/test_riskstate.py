"""Risk state across restarts (audit live C1): persisted state, the REST re-derivation of the
day's P&L (a strict lower bound), the seed decision and the operator override; the session
clock and client_order_id token."""

from __future__ import annotations

import pytest

from dh.core.units import NS_PER_S
from dh.live.clock import AnchoredClock, session_token, to_base36
from dh.live.riskstate import (
    DAY_NS,
    RiskState,
    RiskStateError,
    RiskStateStore,
    day_pnl_from_rows,
    decide_seed,
    derive_day_pnl,
    row_in_subaccount,
    sticky,
)

from .fakes import T0, FakeRest, fill_row

TK = "KXBTCD-26SEP2513-T84000.00"
TK2 = "KXBTCD-26SEP2513-T84250.00"
DAY0 = T0 - T0 % DAY_NS


def settle_row(ticker: str, result: str, yes: str = "0.00", no: str = "0.00", t_ns: int = T0, **kw) -> dict:
    import time

    return {"ticker": ticker, "event_ticker": ticker.rsplit("-", 1)[0], "market_result": result, "yes_count_fp": yes,
            "no_count_fp": no, "yes_total_cost_dollars": "0", "no_total_cost_dollars": "0",
            "revenue": 0, "settled_time": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(t_ns // NS_PER_S)),
            "fee_cost": "0", "exchange_index": 0, **kw}


def test_day_pnl_realized_round_trip_is_exact():
    # bought 10 YES at 40c, sold 10 at 45c today, 2c fees: +$0.48; flat now
    fills = [fill_row("f1", "o1", TK, side="bid", px="0.4000", count="10.00", fee="0.010000", created_ns=T0),
             fill_row("f2", "o2", TK, side="ask", px="0.4500", count="10.00", fee="0.010000", created_ns=T0 + 1)]
    d = day_pnl_from_rows(DAY0, fills, [], {})
    assert d.fills == 2 and d.positions_now == {} and d.positions_midnight == {}
    assert d.pnl_usd == pytest.approx(0.48)


def test_day_pnl_settlement_in_yes_terms_and_worst_case_open():
    # 5 NO bought today (= YES sold at 60c: +$3.00 in YES terms), NO wins: YES-terms payout 0 ->
    # +$3.00 = Kalshi's -$2.00 cost + $5.00 revenue. Plus 2 YES bought at 30c still open: worst
    # case worth 0 -> -$0.60.
    fills = [fill_row("f1", "o1", TK, side="ask", px="0.6000", count="5.00", created_ns=T0),
             fill_row("f2", "o2", TK2, side="bid", px="0.3000", count="2.00", created_ns=T0)]
    settles = [settle_row(TK, "no", no="5.00", revenue=500)]
    d = day_pnl_from_rows(DAY0, fills, settles, {TK2: 200})
    assert d.settlements_usd == pytest.approx(0.0) and d.cash_usd == pytest.approx(3.0 - 0.6)
    assert d.worst_open_usd == pytest.approx(0.0) and d.pnl_usd == pytest.approx(2.4)
    # a short YES still open counts its worst case (-$1 per contract)
    d2 = day_pnl_from_rows(DAY0, [fill_row("f3", "o3", TK, side="ask", px="0.6000", count="5.00", created_ns=T0)], [],
                           {TK: -500})
    assert d2.worst_open_usd == pytest.approx(-5.0) and d2.pnl_usd == pytest.approx(3.0 - 5.0)


def test_day_pnl_positions_held_at_midnight_are_pessimistic():
    # 4 YES held from yesterday (no fills today) settled YES today: payout $4 today, but they
    # could have been worth $4 at midnight -> 0 for today (never an optimistic +$4)
    d = day_pnl_from_rows(DAY0, [], [settle_row(TK, "yes", yes="4.00", revenue=400)], {})
    assert d.positions_midnight == {TK: 400} and d.pnl_usd == pytest.approx(0.0)
    # fills before midnight are ignored
    old = fill_row("f0", "o0", TK, side="bid", px="0.5000", count="1.00", created_ns=DAY0 - NS_PER_S)
    assert day_pnl_from_rows(DAY0, [old], [], {}).fills == 0


def test_subaccount_rows():
    assert row_in_subaccount({}, 0) and not row_in_subaccount({"subaccount_number": 3}, 0)
    assert row_in_subaccount({"subaccount_number": 3}, 3) and row_in_subaccount({}, 3)  # omitted: trust the query
    assert not row_in_subaccount({"subaccount_number": 0}, 3) and not row_in_subaccount({"subaccount": "x"}, 3)
    rows = [fill_row("f1", "o1", TK, side="bid", px="0.5000", count="1.00", created_ns=T0),
            fill_row("f2", "o2", TK, side="bid", px="0.5000", count="1.00", created_ns=T0, subaccount=5)]
    assert day_pnl_from_rows(DAY0, rows, [], {}).fills == 1


async def test_derive_day_pnl_queries_since_midnight_with_explicit_subaccount():
    rest = FakeRest()
    rest.fills = [fill_row("f1", "o1", TK, side="bid", px="0.5000", count="2.00", fee="0.010000", created_ns=T0)]
    d = await derive_day_pnl(rest, DAY0, {TK: 200}, subaccount=0)
    assert rest.of("iter_fills")[0][1] == {"min_ts": DAY0 // NS_PER_S, "subaccount": 0}
    assert rest.of("iter_settlements")[0][1] == {"min_ts": DAY0 // NS_PER_S, "subaccount": 0}
    assert d.pnl_usd == pytest.approx(-1.0 - 0.01)  # $1 paid, worth 0 in the worst case, 1c fee


def test_store_roundtrip_and_unreadable(tmp_path):
    st = RiskStateStore(tmp_path / "s" / "risk.json")
    assert st.load() is None
    st.save(RiskState(DAY0, -3.5, True, "daily_loss", 7, "sess", "live", T0), fsync=True)
    assert st.load() == RiskState(DAY0, -3.5, True, "daily_loss", 7, "sess", "live", T0)
    (tmp_path / "s" / "risk.json").write_text("{not json")
    with pytest.raises(RiskStateError):
        st.load()


def test_decide_seed_rules():
    now = DAY0 + 10 * 3600 * NS_PER_S
    rest = day_pnl_from_rows(DAY0, [fill_row("f1", "o1", TK, side="bid", px="0.5000", count="10.00", created_ns=now)], [],
                             {TK: 1000})  # -$5 lower bound
    prev = RiskState(DAY0, -1.0, False, "", 0, "s1", "live", now - 1)
    d = decide_seed(now, prev, rest)
    assert d.day_pnl_usd == pytest.approx(-5.0) and not d.halted  # the lower of the two
    prev = RiskState(DAY0, -24.99, True, "carried_over:daily_loss", now + 3600 * NS_PER_S, "s1", "live", now - 1)
    d = decide_seed(now, prev, None)
    assert d.halted and d.halt_reason == "daily_loss" and d.pause_until_ns == now + 3600 * NS_PER_S
    assert d.day_pnl_usd == pytest.approx(-24.99)
    # next UTC day: the daily-loss halt and P&L do not carry; a reconciliation halt does
    tomorrow = now + DAY_NS
    assert not decide_seed(tomorrow, prev, None).halted and decide_seed(tomorrow, prev, None).day_pnl_usd == 0.0
    recon = RiskState(DAY0, 0.0, True, "reconciliation:position_mismatch", 0, "s1", "live", now)
    assert decide_seed(tomorrow, recon, None).halted and sticky("fee_mismatch:x") and not sticky("daily_loss")
    # operator override: everything forgiven, and recorded
    r = decide_seed(now, prev, rest, reset=True)
    assert (r.day_pnl_usd, r.halted, r.pause_until_ns) == (0.0, False, 0)
    assert r.overridden["halted"] and r.overridden["day_pnl_usd"] == pytest.approx(-24.99)


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
