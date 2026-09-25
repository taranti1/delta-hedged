"""Settlement convention on REAL Kalshi markets (docs/kalshi_specs/samples_2026-09-25, public GETs).

T = close_time (not expected_expiration_time = close + 5 min); window (T-60 s, T]; the published
expiration value is the average rounded to cents; KXBTC15M strikes are the previous quarter's
expiration value with >= semantics; a benchmark gap inside the window is a No-risk condition.
"""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pytest

from dh.core.events import IndexTick
from dh.core.market import MarketSpec, SettlementSpec, round_half_up
from dh.core.units import NS_PER_S
from dh.kalshi.metadata import rules_flags
from dh.kalshi.normalize import UnsupportedMarket, rest_market_to_spec
from dh.kalshi.wire import iso_to_ns
from dh.models.fairvalue import digital
from dh.settlement.convention import (
    check_settlement_time,
    rules_settlement_time_ns,
    rules_strike_reference_time_ns,
    ticker_settlement_time_ns,
)
from dh.settlement.window import SettlementTracker, WindowState, window_state_from_prints

SAMPLES = Path(__file__).resolve().parents[2] / "docs" / "kalshi_specs" / "samples_2026-09-25"
SERIES = ("KXBTCD", "KXBTC", "KXBTC15M")


def _load(name: str) -> list[dict]:
    body = json.loads((SAMPLES / name).read_text())
    return body["markets"] if isinstance(body, dict) and "markets" in body else body


def _series(s: str) -> dict:
    body = json.loads((SAMPLES / f"series_{s}.json").read_text())
    return body.get("series", body)


def _all_markets() -> list[tuple[str, dict]]:
    out = []
    for s in SERIES:
        for kind in ("settled", "open"):
            out += [(s, m) for m in _load(f"markets_{kind}_{s}.json")]
    return out


REAL = _all_markets()


@pytest.mark.parametrize("series,m", REAL, ids=[m["ticker"] for _, m in REAL])
def test_real_market_T_is_close_time(series, m):
    close = iso_to_ns(m["close_time"])
    expected = iso_to_ns(m["expected_expiration_time"])
    assert expected - close == 300 * NS_PER_S  # every real KXBTC* market: expected = close + 5 min
    assert rules_settlement_time_ns(m["rules_primary"]) == close
    assert ticker_settlement_time_ns(m["event_ticker"]) == close
    assert check_settlement_time(m, close, series) == ("verified", [])
    assert "settlement_time_mismatch" not in rules_flags(m)
    if m.get("floor_strike") is None and m.get("cap_strike") is None:
        return
    spec = rest_market_to_spec(m, _series(series))
    assert spec.expiration_ts == spec.close_ts == close
    assert spec.expected_expiration_ts == expected  # metadata only
    assert spec.settlement.round_decimals == 2 and spec.settlement.n_obs == 60
    obs = spec.settlement.obs_times(spec.expiration_ts)
    assert obs[0] == close - 59 * NS_PER_S and obs[-1] == close and len(obs) == 60  # (T-60 s, T]


def test_same_close_same_expiration_value_across_series():
    """KXBTCD-26SEP2515, KXBTC-26SEP2515 and KXBTC15M-26SEP251500 all close 19:00Z and settle on the
    same BRTI average: one settlement event; the 15-minute market was settled BEFORE close + 5 min."""
    by = {s: {m["event_ticker"]: m for m in _load(f"markets_settled_{s}.json")} for s in SERIES}
    d, r, q = by["KXBTCD"]["KXBTCD-26SEP2515"], by["KXBTC"]["KXBTC-26SEP2515"], by["KXBTC15M"]["KXBTC15M-26SEP251500"]
    assert d["close_time"] == r["close_time"] == q["close_time"] == "2026-09-25T19:00:00Z"
    assert d["expiration_value"] == r["expiration_value"] == q["expiration_value"] == "83950.62"
    assert iso_to_ns(q["settlement_ts"]) < iso_to_ns(q["expected_expiration_time"])
    assert iso_to_ns(q["settlement_ts"]) - iso_to_ns(q["close_time"]) < 10 * NS_PER_S


def test_kxbtc15m_strike_is_previous_quarter_value_at_least():
    settled = {m["event_ticker"]: m for m in _load("markets_settled_KXBTC15M.json")}
    (nxt,) = _load("markets_open_KXBTC15M.json")
    prev = settled["KXBTC15M-26SEP251500"]
    assert nxt["event_ticker"] == "KXBTC15M-26SEP251515"
    assert nxt["floor_strike"] == float(prev["expiration_value"])  # 83950.62
    assert rules_strike_reference_time_ns(nxt["rules_primary"]) == iso_to_ns(prev["close_time"])
    spec = rest_market_to_spec(nxt, _series("KXBTC15M"))
    assert spec.strike_type == "greater_or_equal"
    # ">=" on the ROUNDED value: a tie at the cent is YES; half a cent below rounds up to the strike
    assert spec.yes_wins(83950.62) and spec.yes_wins(83950.615) and not spec.yes_wins(83950.6149)
    assert spec.settle_thresholds == (pytest.approx(83950.615, abs=1e-9), None)


@pytest.mark.parametrize("series", SERIES)
def test_settled_results_follow_rounded_expiration_value(series):
    for m in _load(f"markets_settled_{series}.json"):
        if m.get("floor_strike") is None and m.get("cap_strike") is None:
            continue
        spec = rest_market_to_spec(m, _series(series))
        assert spec.yes_wins(float(m["expiration_value"])) == (m["result"] == "yes"), m["ticker"]


def test_kxbtcd_above_is_strict_on_the_rounded_value():
    (m,) = [x for x in _load("markets_open_KXBTCD.json")][:1]
    spec = rest_market_to_spec(m, _series("KXBTCD"))
    K = spec.floor_strike
    assert spec.strike_type == "greater" and abs(K * 100 - round(K * 100)) < 1e-6  # X.99 strikes
    assert not spec.yes_wins(K) and not spec.yes_wins(K + 0.0049) and spec.yes_wins(K + 0.005)
    lo, hi = spec.settle_thresholds
    assert lo == pytest.approx(K + 0.005, abs=1e-9) and hi is None
    A = np.array([K + 0.004, K + 0.0051, K + 1])
    assert spec.yes_wins_vec(A).tolist() == [False, True, True]


def test_threshold_shift_in_the_pricing_model():
    """The continuous model prices P(round(avg) > K) = P(avg >= K + 0.005): exact at a fixed window."""
    base = dict(ticker="X", event_ticker="E", series_ticker="KXBTCD", strike_type="greater", floor_strike=100_000.0,
                cap_strike=None, open_ts=0, close_ts=0, expiration_ts=0)
    rounded = MarketSpec(**base, settlement=SettlementSpec(round_decimals=2))
    raw = MarketSpec(**base)
    ws = WindowState(n_obs=60, k_fixed=0, sum_fixed=0.0, m_remaining=60, tau_first_s=0.0, step_s=1.0)
    # spot exactly at K + 0.005 is the new at-the-money point
    assert digital(rounded, ws, 100_000.005, 1.0, "gauss").p_yes == pytest.approx(0.5, abs=1e-9)
    assert digital(rounded, ws, 100_000.0, 1.0, "gauss").p_yes < 0.4999
    assert digital(raw, ws, 100_000.0, 1.0, "gauss").p_yes == pytest.approx(0.5, abs=1e-12)
    # determined outcomes use the rounded comparison
    fixed = WindowState(n_obs=60, k_fixed=60, sum_fixed=60 * 100_000.004, m_remaining=0, tau_first_s=0.0, step_s=1.0)
    assert digital(rounded, fixed, 0.0, 1.0).p_yes == 0.0 and digital(raw, fixed, 0.0, 1.0).p_yes == 1.0


def test_round_half_up_matches_exact_cent_ties():
    # 60 prints in integer cents whose sum is 30 mod 60: the average ends in exactly half a cent
    cents = np.full(60, 8_395_062, dtype=np.int64)
    cents[:30] += 1  # sum = 60 * 8395062 + 30 -> average 83950.625 exactly
    avg = float(cents.sum()) / 60 / 100
    assert round_half_up(avg) == 83950.63
    assert round_half_up(83950.6249999) == 83950.62


def test_moved_close_time_is_refused():
    (m,) = _load("markets_open_KXBTCD.json")[:1]
    bad = dict(m, close_time="2026-09-25T20:05:00Z")  # e.g. a parser that picked expected_expiration_time
    with pytest.raises(UnsupportedMarket, match="settlement time ambiguous"):
        rest_market_to_spec(bad, _series("KXBTCD"))
    assert "settlement_time_mismatch" in rules_flags(bad)
    # rules text without a parseable time: the event ticker becomes binding (blocking flag)
    vague = dict(bad, rules_primary="the simple average of the sixty seconds of CF Benchmarks' BRTI")
    assert rest_market_to_spec(vague, _series("KXBTCD")).expiration_ts == iso_to_ns(vague["close_time"])
    assert "settlement_time_mismatch" in rules_flags(vague)
    # rules verify close_time but the ticker disagrees: the rules are binding, the flag informational
    odd = dict(m, event_ticker="KXBTCD-26SEP2517")
    assert rest_market_to_spec(odd, _series("KXBTCD")).expiration_ts == iso_to_ns(m["close_time"])
    assert "ticker_time_differs" in rules_flags(odd) and "settlement_time_mismatch" not in rules_flags(odd)


def test_ticker_time_formats_and_dst():
    assert ticker_settlement_time_ns("KXBTCD-26SEP2515") == iso_to_ns("2026-09-25T19:00:00Z")  # EDT
    assert ticker_settlement_time_ns("KXBTC15M-26SEP251545") == iso_to_ns("2026-09-25T19:45:00Z")
    assert ticker_settlement_time_ns("KXBTCD-26JAN0517") == iso_to_ns("2026-01-05T22:00:00Z")  # EST
    assert ticker_settlement_time_ns("KXBTCD-26SEP251545") is None  # wrong digit count for hourly
    assert ticker_settlement_time_ns("KXBTC15M-26SEP2515") is None
    assert ticker_settlement_time_ns("KXETHD-26SEP2515") is None  # unknown series format
    assert rules_settlement_time_ns("... BRTI before 5 PM EST on Jan 5, 2026 is above 1") == iso_to_ns("2026-01-05T22:00:00Z")
    assert rules_settlement_time_ns("no time here") is None


def test_gap_in_window_is_flagged_not_hidden():
    """A missing 1 Hz print in the window (a later print exists, no 5 Hz either) is counted in
    n_missing whatever the gap policy; the contract resolves such data problems to No."""
    T = 1_790_000_000 * NS_PER_S
    spec = SettlementSpec(round_decimals=2)
    obs = spec.obs_times(T)
    prints = {t: 100.0 for t in obs if t != obs[10]}
    ws = window_state_from_prints(spec, T, T, prints)
    assert ws.n_missing == 1 and ws.n_filled == 1 and ws.incomplete and ws.k_fixed == 60
    ws_skip = window_state_from_prints(spec, T, T, prints, gap_policy="skip")
    assert ws_skip.n_missing == 1 and ws_skip.n_obs == 59
    tr = SettlementTracker(use_5hz=False)
    for t in obs:
        if t != obs[10]:
            tr.on_index(IndexTick(t, t, "BRTI", 100.0, "1hz"))
    ws = tr.window_state(spec, T, T)
    assert ws.n_missing == 1 and ws.incomplete
    full = SettlementTracker(use_5hz=False)
    full.on_ticks(IndexTick(t, t, "BRTI", 100.0, "1hz") for t in obs)
    assert full.window_state(spec, T, T).n_missing == 0
    assert full.settlement_value(spec, T, rounded=True) == 100.0
