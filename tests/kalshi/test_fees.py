from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from dh.core.units import PX_SCALE, micros_from_dollars
from dh.kalshi.fees import (
    DEFAULT_FEES_YAML,
    FeeBreakdown,
    FeeEngine,
    FeeRates,
    UnsupportedFeeType,
    apply_scheduled_changes,
    reconcile_fill_fee,
    resolve_fee_fields,
)
from dh.kalshi.wire import iso_to_ns

from . import samples as S

ENGINE = FeeEngine()  # config/fees.yaml
QUAD_TYPES = ("quadratic", "quadratic_with_maker_fees", "quadratic_with_combo_maker_fees")


def test_rates_loaded_from_config_as_decimals():
    r = ENGINE.rates
    assert r.source == str(DEFAULT_FEES_YAML)
    assert r.taker == Decimal("0.07")
    assert r.maker == {"quadratic": Decimal("0"), "quadratic_with_maker_fees": Decimal("0.0175"),
                       "quadratic_with_combo_maker_fees": Decimal("0.035")}
    assert r.balance_precision_micros == 10_000 and r.verified_against_live_fills is False


def test_config_rejects_floats(tmp_path: Path):
    p = tmp_path / "fees.yaml"
    p.write_text("rates:\n  taker: 0.07\n  maker: {quadratic: '0'}\n")
    with pytest.raises(ValueError):
        FeeRates.from_yaml(p)


def test_fee_rounding_docs_example():
    """docs fee_rounding: buy 1 @ $0.055 taker -> trade 0.003639, rounding 0.001361, net 0.005."""
    s = ENGINE.schedule("quadratic_with_maker_fees", 1)
    assert s.unrounded_fee(550, 100, True) == Decimal("0.00363825")
    assert s.order_fee_micros(550, 100, True) == FeeBreakdown(3639, 1361, 0, 5000, -60000)


# Published fee-schedule tables (ceil to the cent). For whole-cent prices a single buy fill
# at $0.01 balance precision gives exactly the tabled fee as its NET fee.
ONE_CONTRACT = [("0.01", "0.01"), ("0.05", "0.01"), ("0.10", "0.01"), ("0.25", "0.02"), ("0.50", "0.02"),
                ("0.75", "0.02"), ("0.90", "0.01"), ("0.95", "0.01"), ("0.99", "0.01")]
HUNDRED_CONTRACTS = [("0.01", "0.07"), ("0.50", "1.75"), ("0.99", "0.07")]


@pytest.mark.parametrize("price,fee", ONE_CONTRACT)
def test_taker_table_one_contract(price, fee):
    s = ENGINE.schedule("quadratic", 1)
    px = int(Decimal(price) * PX_SCALE)
    assert s.order_fee_micros(px, 100, True).net_micros == micros_from_dollars(fee)


@pytest.mark.parametrize("price,fee", HUNDRED_CONTRACTS)
def test_taker_table_hundred_contracts(price, fee):
    s = ENGINE.schedule("quadratic", 1)
    px = int(Decimal(price) * PX_SCALE)
    assert s.order_fee_micros(px, 10_000, True).net_micros == micros_from_dollars(fee)


def test_trade_fee_values_and_roles():
    q = ENGINE.schedule("quadratic", 1)
    mk = ENGINE.schedule("quadratic_with_maker_fees", 1)
    combo = ENGINE.schedule("quadratic_with_combo_maker_fees", 1)
    assert q.trade_fee_micros(5000, 100, True) == 17_500
    assert q.trade_fee_micros(5000, 100, False) == 0  # no maker fee on 'quadratic'
    assert mk.trade_fee_micros(5000, 100, False) == 4_375
    assert combo.trade_fee_micros(5000, 100, False) == 8_750
    assert mk.trade_fee_micros(5000, 1, False) == 44  # 0.4375 micros -> ceil
    assert ENGINE.schedule("quadratic", "0.5").trade_fee_micros(5000, 100, True) == 8_750
    assert mk.expected_fee_per_contract(5000, False) == pytest.approx(0.004375)
    assert q.expected_fee_per_contract(5000, True) == pytest.approx(0.0175)
    # quadratic maker fill at a whole-cent price: no fee at all (cash already aligned)
    assert q.order_fee_micros(4500, 300, False) == FeeBreakdown(0, 0, 0, 0, -1_350_000)


def test_unsupported_types_never_price_as_zero():
    for ft in ("flat", "margin_market_maker_program_fees", "bogus"):
        s = ENGINE.schedule(ft, 1)
        assert not s.supported
        with pytest.raises(UnsupportedFeeType):
            s.trade_fee_micros(5000, 100, True)
        with pytest.raises(UnsupportedFeeType):
            s.expected_fee_per_contract(5000, False)
    unresolved = ENGINE.schedule_for(None, None, None)
    assert unresolved.source == "unresolved" and not unresolved.supported
    with pytest.raises(UnsupportedFeeType):
        unresolved.trade_fee_micros(5000, 100, True)
    with pytest.raises(UnsupportedFeeType):
        ENGINE.schedule("quadratic", "-1")


def test_schedule_resolution_precedence():
    series = S.SERIES_KXBTCD
    assert resolve_fee_fields(series) == ("quadratic", Decimal(1), "series")
    ev = {"fee_type_override": "quadratic_with_maker_fees", "fee_multiplier_override": None}
    assert resolve_fee_fields(series, ev) == ("quadratic_with_maker_fees", Decimal(1), "event_override")
    ev = {"fee_type_override": None, "fee_multiplier_override": 0.5}
    assert resolve_fee_fields(series, ev) == ("quadratic", Decimal("0.5"), "event_override")
    assert resolve_fee_fields(None, None, {"fee_type": "quadratic", "fee_multiplier": 2}) == ("quadratic", Decimal(2), "market")
    assert resolve_fee_fields({}, {}, {}) == ("", None, "unresolved")
    s = ENGINE.schedule_for(series, {"fee_type_override": "flat"}, S.MARKET_KXBTCD)
    assert s.fee_type == "flat" and s.source == "event_override" and not s.supported


def test_fee_waiver_flagged_not_applied_by_default():
    m = S.market(fee_waiver_expiration_time="2025-08-05T20:30:00Z")
    before, after = iso_to_ns("2025-08-05T20:00:00Z"), iso_to_ns("2025-08-05T20:45:00Z")
    s = ENGINE.schedule_for(S.SERIES_KXBTCD, None, m)
    assert s.waiver_active(before) and not s.waiver_active(after)
    assert s.trade_fee_micros(5000, 100, True, now_ns=before) == 17_500  # conservative default
    opt_in = FeeEngine(ENGINE.rates, apply_fee_waiver=True).schedule_for(S.SERIES_KXBTCD, None, m)
    assert opt_in.trade_fee_micros(5000, 100, True, now_ns=before) == 0
    assert opt_in.trade_fee_micros(5000, 100, True, now_ns=after) == 17_500


def test_accumulator_carry_and_rebate():
    s = ENGINE.schedule("quadratic", 1)
    acc = s.order_accumulator("bid")
    f1 = acc.apply_fill(5050, 100, True)
    assert f1 == FeeBreakdown(17_499, 7_501, 0, 25_000, -530_000)
    f2 = acc.apply_fill(5050, 100, True)
    assert f2 == FeeBreakdown(17_499, 7_501, 10_000, 15_000, -520_000)
    assert acc.carry_micros == 5_002 and acc.total.net_micros == 40_000
    # one 2-contract fill costs the same in total
    assert s.order_fee_micros(5050, 200, True).net_micros == 40_000


def test_accumulator_rebate_never_makes_net_negative():
    s = ENGINE.schedule("quadratic_with_maker_fees", 1)
    acc = s.order_accumulator("bid")
    nets = [acc.apply_fill(550, 100, True).net_micros for _ in range(9)]
    assert nets == [5000] * 9  # carry grows past a cent but a rebate would make net < 0
    assert acc.carry_micros == 9 * 1361


def test_ask_side_cash_equivalence_and_direct_member_precision():
    s = ENGINE.schedule("quadratic", 1)
    ask = s.order_fee_micros(4500, 100, True, book_side="ask")
    assert ask == FeeBreakdown(17_325, 2_675, 0, 20_000, -570_000)  # buys NO at $0.55
    direct = s.order_accumulator("bid", balance_precision_micros=100).apply_fill(2700, 100, True)
    assert direct.trade_micros == 13_797 and direct.net_micros == 13_800  # $0.0138 at $0.0001 precision
    with pytest.raises(ValueError):
        s.order_accumulator("buy")


def test_reconcile_fill_fee():
    b = FeeBreakdown(3639, 1361, 0, 5000, -60000)
    ok = reconcile_fill_fee(5000, "0.005000", breakdown=b)
    assert ok.ok and ok.exact and ok.matched == "net" and ok.tolerance_micros == 1361
    tr = reconcile_fill_fee(5000, "0.003639", breakdown=b)
    assert tr.ok and not tr.exact and tr.matched == "trade" and tr.diff_micros == -1361
    bad = reconcile_fill_fee(5000, "0.020000", breakdown=b)
    assert not bad.ok and bad.detail == "MISMATCH"
    assert reconcile_fill_fee(17_500, "0.0175").ok
    assert reconcile_fill_fee(0, "0.009999").ok and not reconcile_fill_fee(0, "0.010001").ok
    junk = reconcile_fill_fee(0, "n/a")
    assert not junk.ok and junk.reported_micros is None


def test_apply_scheduled_changes():
    series = dict(S.SERIES_KXBTCD)
    changes = [
        {"id": "1", "series_ticker": "KXBTCD", "fee_type": "quadratic_with_maker_fees", "fee_multiplier": 1, "scheduled_ts": "2025-08-01T00:00:00Z"},
        {"id": "2", "series_ticker": "KXBTCD", "fee_type": "quadratic_with_maker_fees", "fee_multiplier": 2, "scheduled_ts": "2025-09-01T00:00:00Z"},
    ]
    fetched, now = iso_to_ns("2025-07-01T00:00:00Z"), iso_to_ns("2025-08-15T00:00:00Z")
    out = apply_scheduled_changes(series, changes, fetched, now)
    assert (out["fee_type"], out["fee_multiplier"]) == ("quadratic_with_maker_fees", 1)
    assert apply_scheduled_changes(series, changes, now, now)["fee_type"] == "quadratic"


def test_schedule_for_spec():
    s = ENGINE.schedule_for_spec("quadratic_with_maker_fees", 0.5)
    assert s.multiplier == Decimal("0.5") and s.trade_fee_micros(5000, 100, False) == 2_188
    assert not ENGINE.schedule_for_spec("", 1.0).supported


# ------------------------------------------------------------------------------ properties
px_st = st.integers(min_value=0, max_value=PX_SCALE)
qty_st = st.integers(min_value=0, max_value=5_000_000)
type_st = st.sampled_from(QUAD_TYPES)
mult_st = st.sampled_from(["0", "0.25", "0.5", "1", "1.5", "2"])


@settings(max_examples=300, deadline=None)
@given(px=px_st, qty=qty_st, ft=type_st, mult=mult_st, taker=st.booleans())
def test_prop_symmetric_in_p_and_zero_at_bounds(px, qty, ft, mult, taker):
    s = ENGINE.schedule(ft, mult)
    assert s.trade_fee_micros(px, qty, taker) == s.trade_fee_micros(PX_SCALE - px, qty, taker)
    assert s.trade_fee_micros(0, qty, taker) == 0 == s.trade_fee_micros(PX_SCALE, qty, taker)
    exact = Decimal(mult) * s.rates.rate(ft, taker) * qty * px * (PX_SCALE - px) / PX_SCALE
    assert exact <= s.trade_fee_micros(px, qty, taker) < exact + 1


@settings(max_examples=300, deadline=None)
@given(px=px_st, q1=qty_st, q2=qty_st, ft=type_st, taker=st.booleans())
def test_prop_monotone_in_qty_and_taker_ge_maker(px, q1, q2, ft, taker):
    s = ENGINE.schedule(ft, 1)
    lo, hi = sorted((q1, q2))
    assert s.trade_fee_micros(px, lo, taker) <= s.trade_fee_micros(px, hi, taker)
    assert s.trade_fee_micros(px, hi, True) >= s.trade_fee_micros(px, hi, False)


fill_st = st.tuples(st.integers(1, PX_SCALE - 1), st.integers(1, 100_000), st.booleans())


@settings(max_examples=300, deadline=None)
@given(fills=st.lists(fill_st, min_size=1, max_size=25), ft=type_st, side=st.sampled_from(["bid", "ask"]),
       bp=st.sampled_from([100, 10_000]))
def test_prop_accumulator_never_negative_and_conserves(fills, ft, side, bp):
    s = ENGINE.schedule(ft, 1)
    acc = s.order_accumulator(side, balance_precision_micros=bp)
    rounding = rebate = 0
    for px, qty, taker in fills:
        b = acc.apply_fill(px, qty, taker)
        assert b.trade_micros >= 0 and 0 <= b.rounding_micros < bp
        assert b.rebate_micros >= 0 and b.rebate_micros % bp == 0
        assert b.net_micros == b.trade_micros + b.rounding_micros - b.rebate_micros >= 0
        assert (b.cash_micros - b.rebate_micros) % bp == 0  # cash lands on the balance grid
        rounding += b.rounding_micros
        rebate += b.rebate_micros
        assert acc.carry_micros == rounding - rebate >= 0
