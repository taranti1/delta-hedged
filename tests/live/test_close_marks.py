"""Marks of positions held between a market's close and its result (dh.settlement.closemark):

  1. a result (WS determined / REST result) -> the payout;
  2. closed, no result -> the exact payout from our own BRTI prints of the window
     (own_benchmark), the worst case when a print is missing or the rounded value is within
     $0.01 of the strike (KXBTC15M >= ties included);
  3. REST bid / ask never after the close; the last trade only when the window cannot be
     evaluated, then the worst case;
  4. at start-up the window is back-filled with ONE CF passthrough call (timespan=HOUR, hour START);
  5. metrics / logs name the mark source; replay reproduces the marks.

Real prints: tests/settlement/fixtures/brti_live_2026-09-25.json (recorder 1 Hz BRTI and the
published expiration values of 13 expirations)."""

from __future__ import annotations

import datetime as dt
import json
import time
from dataclasses import replace
from pathlib import Path

import pytest

from dh.core.actions import Log
from dh.core.events import (
    IndexTick,
    KalshiFill,
    KalshiMarketLifecycle,
    KalshiTrade,
    RiskStateSeed,
    Settlement,
    Timer,
)
from dh.core.market import MarketSpec, PriceRange, SettlementSpec
from dh.core.units import NS_PER_MS, NS_PER_S, PX_SCALE
from dh.live.monitor import JsonLog
from dh.live.riskstate import (
    DAY_NS,
    RiskBook,
    backfill_window_prints,
    decide_seed,
    derive_day_pnl,
    mark_px,
    open_marks,
)
from dh.settlement.closemark import (
    LAST_TRADE,
    OWN_BENCHMARK,
    WORST_CASE,
    close_mark,
    evaluate_window,
    expiration_value,
)
from dh.settlement.window import SettlementTracker
from dh.store.codec import decode_event
from dh.store.recorder import Recorder
from dh.store.replay import iter_raw
from dh.strategy.risk import RiskEngine

from ..strategy.mm_driver import T0 as MM_T0
from ..strategy.mm_driver import Driver
from ..strategy.mm_driver import spec as mm_spec
from .fakes import FakeRest, RecordingStrategy, fill_row
from .test_review2 import RISK_CFG
from .test_runner import live_runner

FIX = json.loads((Path(__file__).resolve().parents[1] / "settlement" / "fixtures" / "brti_live_2026-09-25.json")
                 .read_text())["expirations"]
CENTS = SettlementSpec(round_decimals=2)  # the production convention (default_settlement)
H = 3600 * NS_PER_S


def _t(iso: str) -> int:
    return int(dt.datetime.fromisoformat(iso.replace("Z", "+00:00")).timestamp())


def _iso(s: int) -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(s))


def mspec(strike_type: str, close_s: int, floor: float | None = None, cap: float | None = None,
          series: str = "KXBTCD", ticker: str | None = None) -> MarketSpec:
    t = ticker or f"{series}-X{close_s}-{strike_type[:2].upper()}{floor or cap}"
    return MarketSpec(ticker=t, event_ticker=t.rsplit("-", 1)[0], series_ticker=series, strike_type=strike_type,
                      floor_strike=floor, cap_strike=cap, open_ts=(close_s - 3600) * NS_PER_S, close_ts=close_s * NS_PER_S,
                      expiration_ts=close_s * NS_PER_S, settlement=CENTS, price_ranges=(PriceRange(100, 9900, 100),),
                      fee_type="quadratic_with_maker_fees")


def tracker(prints_cents: dict[int, int], feed: str = "1hz") -> SettlementTracker:
    tr = SettlementTracker()
    for s, c in sorted(prints_cents.items()):
        tr.on_index(IndexTick(s * NS_PER_S, s * NS_PER_S, "BRTI", c / 100, feed))
    return tr


def flat_window(close_s: int, cents: int, *, before: int = 5, after: int = 2) -> dict[int, int]:
    """1 Hz prints at ``cents`` from T-60-before .. T+after (the window T-60 .. T-1 included)."""
    return {s: cents for s in range(close_s - 60 - before, close_s + after + 1)}


def fixture_prints(ex: dict) -> tuple[int, dict[int, int]]:
    return _t(ex["close_time"]), {int(k): int(v) for k, v in ex["brti_1hz_cents"].items()}


# ============================================================================ rule 2: own prints
@pytest.mark.parametrize("ex", FIX, ids=[e["close_time"] for e in FIX])
def test_recorded_windows_reproduce_the_published_value_and_outcome(ex):
    T, prints = fixture_prints(ex)
    v = float(ex["expiration_value"].replace(",", ""))
    tr = tracker(prints)
    above = evaluate_window(mspec("greater", T, floor=v - 10.01), tr)
    below = evaluate_window(mspec("greater", T, floor=v + 9.99), tr)
    assert above.status == "yes" and below.status == "no" and above.final
    assert f"{above.value:.2f}" == ex["expiration_value"].replace(",", "")  # to the cent, from our prints
    m = close_mark(10.0, above)
    assert (m.px, m.source) == (PX_SCALE, OWN_BENCHMARK)
    assert close_mark(-10.0, below).px == 0 and close_mark(-10.0, below).source == OWN_BENCHMARK


def test_kxbtc15m_greater_or_equal_chain_on_recorded_prints():
    """Each quarter's strike is the previous quarter's published value (>=). The recorded
    quarters decide YES / NO from our own prints exactly as Kalshi did."""
    quarters = sorted((e for e in FIX if "KXBTC15M" in e["series"]), key=lambda e: e["close_time"])
    seen = 0
    for prev, cur in zip(quarters, quarters[1:], strict=False):
        T, prints = fixture_prints(cur)
        if _t(cur["close_time"]) - _t(prev["close_time"]) != 900:
            continue
        K, v = float(prev["expiration_value"]), float(cur["expiration_value"])
        oc = evaluate_window(mspec("greater_or_equal", T, floor=K, series="KXBTC15M"), tracker(prints))
        assert oc.status == ("yes" if v >= K else "no") and oc.value == pytest.approx(v)
        seen += 1
    assert seen >= 5


def test_greater_or_equal_tie_and_near_strike_fall_back_to_the_worst_case():
    T = 1_790_366_400
    tr = tracker(flat_window(T, 8_396_317))  # every print 83963.17: value exactly 83963.17
    tie = evaluate_window(mspec("greater_or_equal", T, floor=83963.17, series="KXBTC15M"), tr)
    assert tie.status == "near_strike" and tie.value == pytest.approx(83963.17)  # >= would say YES: not trusted
    assert (close_mark(5.0, tie).px, close_mark(5.0, tie).source) == (0, WORST_CASE)  # long: $0
    assert close_mark(-5.0, tie).px == PX_SCALE  # short: $1
    # one cent away either side: still within $0.01 -> worst case; two cents: decided
    assert evaluate_window(mspec("greater_or_equal", T, floor=83963.18, series="KXBTC15M"), tr).status == "near_strike"
    assert evaluate_window(mspec("greater_or_equal", T, floor=83963.16, series="KXBTC15M"), tr).status == "near_strike"
    assert evaluate_window(mspec("greater_or_equal", T, floor=83963.15, series="KXBTC15M"), tr).status == "yes"
    assert evaluate_window(mspec("greater_or_equal", T, floor=83963.19, series="KXBTC15M"), tr).status == "no"
    # KXBTCD strict 'greater' at X.99: 83963.17 vs 83963.16 / 83963.18 near, 83962.99 decided
    assert evaluate_window(mspec("greater", T, floor=83963.16), tr).status == "near_strike"
    assert evaluate_window(mspec("greater", T, floor=83962.99), tr).status == "yes"
    assert evaluate_window(mspec("greater", T, floor=83964.99), tr).status == "no"


def test_half_cent_tie_rounds_up_exactly_and_is_near_the_strike():
    """59 prints at 84000.00 and one at 84000.30: sum = 30 mod 60 cents -> average 84000.005
    rounds HALF UP to 84000.01 (integer cents, no float noise)."""
    T = 1_790_366_400
    prints = flat_window(T, 8_400_000)
    prints[T - 30] = 8_400_030
    assert expiration_value(mspec("greater", T, floor=1.0), [c / 100 for s, c in sorted(prints.items())
                                                           if T - 60 <= s < T]) == 84000.01
    tr = tracker(prints)
    assert evaluate_window(mspec("greater", T, floor=84000.00), tr).status == "near_strike"
    assert evaluate_window(mspec("greater", T, floor=83999.98), tr).status == "yes"


def test_between_market_near_either_bound_is_the_worst_case():
    T = 1_790_366_400
    tr = tracker(flat_window(T, 8_424_999))  # 84249.99
    assert evaluate_window(mspec("between", T, floor=84000.0, cap=84249.99, series="KXBTC"), tr).status == "near_strike"
    assert evaluate_window(mspec("between", T, floor=84000.0, cap=84499.99, series="KXBTC"), tr).status == "yes"
    assert evaluate_window(mspec("between", T, floor=84250.0, cap=84499.99, series="KXBTC"), tr).status == "near_strike"
    assert evaluate_window(mspec("between", T, floor=84500.0, cap=84749.99, series="KXBTC"), tr).status == "no"


@pytest.mark.parametrize("feed", ["1hz", "5hz"])
def test_a_missing_print_falls_back_to_the_worst_case(feed):
    T = 1_790_366_400
    prints = flat_window(T, 8_500_000)
    del prints[T - 30]  # a hole INSIDE the window; later prints exist: missing, not pending
    oc = evaluate_window(mspec("greater", T, floor=84000.0), tracker(prints, feed))
    assert oc.status == "missing" and oc.n_missing == 1 and oc.final and "1 of 60 prints missing" in oc.detail
    m = close_mark(3.0, oc, last_trade_px=9900)  # the last trade is NOT used for an incomplete window
    assert (m.px, m.source) == (0, WORST_CASE) and m.detail.startswith("missing")


def test_a_window_that_cannot_be_evaluated_uses_the_last_trade_then_the_worst_case():
    T = 1_790_366_400
    sp = mspec("greater", T, floor=84000.0)
    # the feed stalled at T-10: the last prints are pending (they may still arrive)
    early = {s: c for s, c in flat_window(T, 8_500_000).items() if s <= T - 10}
    oc = evaluate_window(sp, tracker(early))
    assert oc.status == "unavailable" and oc.n_pending == 9 and not oc.final
    assert (close_mark(2.0, oc, last_trade_px=6100).px, close_mark(2.0, oc, last_trade_px=6100).source) == (6100, LAST_TRADE)
    assert close_mark(2.0, oc).px == 0 and close_mark(-2.0, oc).px == PX_SCALE and close_mark(2.0, oc).source == WORST_CASE
    # prints held only from mid-window on (a session started then): unknown, never "missing"
    late = {s: c for s, c in flat_window(T, 8_500_000).items() if s >= T - 20}
    oc = evaluate_window(sp, tracker(late))
    assert oc.status == "unavailable" and oc.n_before == 40 and oc.n_missing == 0 and oc.final
    assert evaluate_window(sp, None).status == "unavailable"


# ============================================================================ rules 1-3 at start-up
def closed_row(ticker: str, close_s: int, *, strike_type: str, floor: float, bid: str = "0.9900", ask: str = "0.9950",
               last: str = "0.9700", status: str = "closed", result: str = "", **extra) -> dict:
    """A Market row between close and determination: status closed, stale pre-close quotes."""
    row = {"ticker": ticker, "event_ticker": ticker.rsplit("-", 1)[0], "status": status, "strike_type": strike_type,
           "floor_strike": floor, "close_time": _iso(close_s), "open_time": _iso(close_s - 900),
           "yes_bid_dollars": bid, "yes_ask_dollars": ask, "last_price_dollars": last, "result": result,
           "price_ranges": [{"start": "0.0000", "end": "1.0000", "step": "0.0100"}]}
    row.update(extra)
    return row


def cf_hour_server(prints_cents: dict[int, int], calls: list):
    """The verified passthrough shape: timespan=HOUR & timestamp=<hour START ISO ms> -> the hour's
    ticks (here: the whole-second prints, as 5 Hz rows would carry them)."""
    def cf(timespan, timestamp):
        calls.append((timespan, timestamp))
        assert timespan == "HOUR"
        start = _t(timestamp.replace(".000Z", "Z"))
        assert start % 3600 == 0
        return {"data": {"serverTime": "x", "payload": [{"time": s * 1000, "value": f"{c / 100:.2f}"}
                                                         for s, c in sorted(prints_cents.items())
                                                         if start <= s < start + 3600]}}
    return cf


async def test_startup_marks_a_closed_market_from_its_backfilled_window_not_the_stale_bid():
    """KXBTC15M 20:15Z: strike = the 20:00Z value 84028.57 (>=), published 83963.17 -> NO. A
    restart at T+30 s (closed, not determined, REST bid still a stale 0.99): ONE CF call for the
    20:00Z hour, the long is marked at $0 (own_benchmark), not at 99c."""
    ex = next(e for e in FIX if e["close_time"].startswith("2026-09-25T20:15"))
    T, prints = fixture_prints(ex)
    tk = "KXBTC15M-26SEP251615-15"
    rest = FakeRest()
    calls: list = []
    rest.cf_history = cf_hour_server(prints, calls)
    rest.markets[tk] = closed_row(tk, T, strike_type="greater_or_equal", floor=84028.57)
    day0 = T * NS_PER_S - T * NS_PER_S % DAY_NS
    rest.fills = [fill_row("f1", "o1", tk, side="bid", px="0.4000", count="10.00", created_ns=(T - 300) * NS_PER_S)]
    d = await derive_day_pnl(rest, day0, {tk: 1000}, now_ns=(T + 30) * NS_PER_S, window_backfill=True)
    assert calls == [("HOUR", "2026-09-25T20:00:00.000Z")]
    assert d.open_px == {tk: 0} and d.mark_source == {tk: OWN_BENCHMARK} and d.fallbacks == []
    assert d.price_src[f"open:{tk}"].startswith("own_benchmark: expiration value 83963.17")
    assert d.window_backfill["calls"] == 1 and d.window_backfill["outcomes"][tk]["status"] == "no"
    assert d.pnl_usd == pytest.approx(-4.0)  # paid $4.00 for a losing position (the stale bid said +$5.90)
    assert tk in d.specs and "specs" not in d.summary()
    # without the back-fill (or before the fix): never the stale bid, the last trade instead
    d0 = await derive_day_pnl(rest, day0, {tk: 1000}, now_ns=(T + 30) * NS_PER_S)
    assert d0.open_px == {tk: 9700} and d0.mark_source == {tk: LAST_TRADE}


async def test_startup_near_strike_missing_or_unavailable_windows_are_logged_fallbacks():
    T = 1_790_366_400 + 900
    rest = FakeRest()
    calls: list = []
    near, short, nodata = "KXBTC15M-A-15", "KXBTCD-B-T83999.99", "KXBTCD-C-T80000.00"
    prints = flat_window(T, 8_400_000)
    rest.cf_history = cf_hour_server(prints, calls)
    rest.markets[near] = closed_row(near, T, strike_type="greater_or_equal", floor=84000.0)
    rest.markets[short] = closed_row(short, T, strike_type="greater", floor=83989.99)
    rest.markets[nodata] = closed_row(nodata, T + 3600, strike_type="greater", floor=80000.0, last="0.0000")
    om = await open_marks(rest, {near: 500, short: -300, nodata: 200}, now_ns=(T + 7200) * NS_PER_S, window_backfill=True)
    assert om.outcomes[near].status == "near_strike" and near not in om.px
    assert om.src[f"open:{near}"].startswith("worst_case: near_strike: expiration value 84000.00")
    assert om.outcomes[short].status == "yes" and om.px[short] == PX_SCALE  # short: own_benchmark says YES wins
    # the next hour's window: the fake has no prints there -> not evaluable, no last trade -> worst case
    assert om.outcomes[nodata].status == "unavailable" and nodata not in om.px
    assert len(calls) == 2 and om.backfill["calls"] == 2  # one call per hour the windows touch
    d = await derive_day_pnl(rest, (T * NS_PER_S) - (T * NS_PER_S) % DAY_NS, {near: 500, short: -300, nodata: 200},
                             now_ns=(T + 7200) * NS_PER_S, window_backfill=True)
    assert d.mark_source == {near: WORST_CASE, short: OWN_BENCHMARK, nodata: WORST_CASE}
    assert any(near in f and "near_strike" in f for f in d.fallbacks)
    assert any(nodata in f and "not evaluable" in f for f in d.fallbacks)


async def test_startup_result_needs_no_backfill_and_cf_failure_falls_back():
    T = 1_790_366_400
    rest = FakeRest()  # cf_history None: the passthrough answers 404
    done, waiting = "KXBTCD-D-T84000.00", "KXBTCD-E-T84000.00"
    rest.markets[done] = closed_row(done, T, strike_type="greater", floor=84000.0, status="determined", result="yes")
    om = await open_marks(rest, {done: 100}, now_ns=(T + 100) * NS_PER_S, window_backfill=True)
    assert om.px == {done: PX_SCALE} and om.src[f"open:{done}"].startswith("result_rest")
    assert not rest.of("get_cfbenchmarks_history")
    rest.markets[waiting] = closed_row(waiting, T, strike_type="greater", floor=84000.0, last="0.4200")
    om = await open_marks(rest, {waiting: 100}, now_ns=(T + 100) * NS_PER_S, window_backfill=True)
    assert "404" in om.backfill["errors"][0] and om.outcomes[waiting].status == "unavailable"
    assert om.px == {waiting: 4200} and om.src[f"open:{waiting}"].startswith("last_trade")
    # still open (before close_time): the exchange quote as before
    rest.markets[waiting] = closed_row(waiting, T, strike_type="greater", floor=84000.0, status="active", bid="0.4100")
    om = await open_marks(rest, {waiting: 100}, now_ns=(T - 100) * NS_PER_S, window_backfill=True)
    assert om.px == {waiting: 4100} and om.src[f"open:{waiting}"] == "exchange_quote: YES bid"
    # past close_time by the clock while REST still says active with a stale bid: never the bid
    p, why = mark_px(rest.markets[waiting], 100, now_ns=(T + 1) * NS_PER_S)
    assert p == 9700 and why.startswith("last_trade")


async def test_backfill_window_prints_is_one_hour_start_call():
    T = 1_790_366_400  # 20:00:00Z: the window 19:59:00 .. 19:59:59 lies in the 19:00Z hour
    rest = FakeRest()
    calls: list = []
    rest.cf_history = cf_hour_server(flat_window(T, 8_400_000), calls)
    tr, info = await backfill_window_prints(rest, [mspec("greater", T, floor=1.0), mspec("greater", T, floor=2.0)])
    assert calls == [("HOUR", "2026-09-25T19:00:00.000Z")] and info["calls"] == 1 and info["ticks"] == 60 + 5
    assert tr.stats["ticks"] == 65


# ============================================================================ in session: the strategy
def _mm_close_scenario(strike: float = 83900.0, strike_type: str = "greater", series: str = "KXBTCD",
                       qty: int = 100, gap: bool = False):
    """A MarketMaker holding ``qty`` (1/100 contracts) bought at 50c in a market closing 100 s
    after start; BRTI flat at 84000 (5 Hz, optionally with a 2 s hole inside the window). Returns
    (driver, spec, the events fed, the Logs emitted)."""
    exp = MM_T0 + 100 * NS_PER_S
    sp = replace(mm_spec(K=strike, strike_type=strike_type, exp=exp, series=series,
                         event=f"{series}-TEST"), settlement=CENTS, ticker=f"{series}-TEST-T{strike}")
    d = Driver([sp])
    events: list = [KalshiFill(MM_T0 + 1, 0, sp.ticker, "t-1", "oid-x", "", "bid" if qty > 0 else "ask", 5000,
                               abs(qty), False, 0, 0, False),
                    KalshiTrade(MM_T0 + 2, 0, sp.ticker, "tr-1", 6200, 100, "yes")]
    t = MM_T0
    while t < exp + 3 * NS_PER_S:
        t += 200 * NS_PER_MS
        if not (gap and exp - 30 * NS_PER_S <= t < exp - 28 * NS_PER_S):
            events.append(IndexTick(t, t, "BRTI", 84000.0, "5hz"))
        events.append(Timer(t))
    logs: list = []
    for ev in events:
        logs += [(ev.ts, a) for a in d.mm.on_event(ev) if isinstance(a, Log) and a.kind == "close_mark"]
    return d, sp, events, logs


def test_strategy_marks_a_position_after_the_close_from_its_own_prints():
    d, sp, _, logs = _mm_close_scenario()
    cm = d.mm.close_marks[sp.ticker]
    assert (cm.px, cm.source) == (PX_SCALE, OWN_BENCHMARK) and cm.value == pytest.approx(84000.0)
    assert len(logs) == 1 and logs[0][0] >= sp.close_ts and logs[0][1].payload["source"] == OWN_BENCHMARK
    assert d.mm.equity() == pytest.approx(-0.5 + 1.0)  # 1 contract bought at 50c, worth $1 (not F)
    # the WS result arrives: the payout wins (and replaces the mark), whatever the mark said
    d.mm.on_event(KalshiMarketLifecycle(sp.close_ts + 90 * NS_PER_S, 0, sp.ticker, "determined", result="no"))
    assert sp.ticker not in d.mm.close_marks and d.mm.settled[sp.ticker] == 0
    assert d.mm.equity() == pytest.approx(-0.5)


@pytest.mark.parametrize("qty, worst", [(100, 0), (-100, PX_SCALE)])
def test_strategy_15m_tie_is_marked_at_the_worst_case(qty, worst):
    d, sp, _, logs = _mm_close_scenario(84000.0, "greater_or_equal", "KXBTC15M", qty=qty)
    cm = d.mm.close_marks[sp.ticker]
    assert (cm.px, cm.source) == (worst, WORST_CASE) and "near_strike" in cm.detail
    assert logs[-1][1].payload["source"] == WORST_CASE


def test_strategy_missing_print_is_the_worst_case_not_the_last_trade():
    d, sp, _, _ = _mm_close_scenario(gap=True)
    cm = d.mm.close_marks[sp.ticker]
    assert (cm.px, cm.source) == (0, WORST_CASE) and cm.detail.startswith("missing")
    assert d.mm.last_trade_px[sp.ticker] == 6200  # known, but not used for an incomplete window


def test_replay_of_the_recorded_events_reproduces_the_marks():
    """The marks are a pure function of the recorded events (prints, fills, results): a fresh
    MarketMaker fed the same events (what dh.live.replay does) emits the same close_mark logs
    and the same equity."""
    d, sp, events, logs = _mm_close_scenario(84000.0, "greater_or_equal", "KXBTC15M")
    d2 = Driver([sp])
    logs2 = []
    for ev in events:
        logs2 += [(ev.ts, a) for a in d2.mm.on_event(ev) if isinstance(a, Log) and a.kind == "close_mark"]
    assert [(t, a.payload) for t, a in logs] == [(t, a.payload) for t, a in logs2] and logs
    assert d.mm.equity() == d2.mm.equity() and d.mm.close_marks == d2.mm.close_marks


# ============================================================================ in session: excluded positions
class TrackerStrategy(RecordingStrategy):
    """The runner's view of a strategy: a SettlementTracker fed every BRTI tick, the real
    RiskEngine (seeds), a fixed equity of 0."""

    def __init__(self) -> None:
        super().__init__(self._respond)
        self.tracker = SettlementTracker()
        self.risk = RiskEngine(RISK_CFG)

    def equity(self, S=None):
        return 0.0

    def _respond(self, ev):
        if isinstance(ev, IndexTick):
            self.tracker.on_index(ev)
            return self.risk.on_equity(ev.ts, 0.0)
        if isinstance(ev, RiskStateSeed):
            return self.risk.on_seed(ev)
        return []


@pytest.mark.parametrize("strike, px, source, pnl", [(83000.0, PX_SCALE, OWN_BENCHMARK, 5.5),
                                                      (84000.0, 0, WORST_CASE, -4.5)])
async def test_excluded_position_open_at_startup_is_remarked_at_its_close(tmp_path, strike, px, source, pnl):
    """10 YES held at start-up in an excluded market, then open (marked at the 45c bid). It
    closes during the session: the runner re-marks it from the window's prints (own_benchmark,
    or the worst case at the strike), logs close_mark, counts the source, and the strategy gets
    an updated seed right after the tick (recorded on events.live for replay)."""
    T = (MM_T0 // H + 1) * H
    ex = f"KXBTCD-EXCL-T{strike:.2f}"
    sp = mspec("greater", T // NS_PER_S, floor=strike, ticker=ex)
    now = T - 120 * NS_PER_S
    dec = decide_seed(now, None, type("P", (), {"pnl_usd": 0.0, "open_usd": 4.5})())
    book = RiskBook.from_decision(dec, {ex: (1000, 4500)}, specs={ex: sp}, sources={ex: "exchange_quote"}, now_ns=now)
    assert book.watch == {ex}
    s = TrackerStrategy()
    rec = Recorder(tmp_path / "data")
    log = JsonLog(tmp_path / "log.jsonl", "cfg")
    r, _, _ = live_runner(s, risk_book=book, jsonlog=log, recorder=rec, clock_ns=lambda: T + 5 * NS_PER_S)
    r.push_result(RiskStateSeed(now, 0, dec.day_start_ns, dec.day_pnl_usd))
    for sec in range(T // NS_PER_S - 70, T // NS_PER_S + 3):
        r.push(IndexTick(sec * NS_PER_S, sec * NS_PER_S, "BRTI", 84000.0, "1hz"))
    r.process_pending()
    seeds = [e for e in s.events if isinstance(e, RiskStateSeed)]
    assert [round(e.day_pnl_usd, 6) for e in seeds] == [0.0, pytest.approx(pnl)]
    assert seeds[1].ts == T  # the first tick at the close; the window was complete one second earlier
    assert book.excluded[ex] == (1000, px) and book.mark_src[ex] == source and not book.watch
    assert r.metrics.get("dh_position_marks_total", scope="excluded", source=source) == 1
    log.close()
    recs = [json.loads(x) for x in (tmp_path / "log.jsonl").read_text().splitlines()]
    cm = [x for x in recs if x["k"] == "close_mark"]
    assert len(cm) == 1 and cm[0]["ticker"] == ex and cm[0]["source"] == source and cm[0]["prev_px"] == 4500
    r.refresh_metrics()
    assert r.metrics.get("dh_marked_positions", scope="excluded", source=source) == 1.0
    # the WS result then realizes it: nothing moves when the mark was the payout
    r.push(Settlement(T + 90 * NS_PER_S, 0, ex, "yes" if px else "no", None, px))
    r.process_pending()
    seeds = [e for e in s.events if isinstance(e, RiskStateSeed)]
    assert seeds[-1].day_pnl_usd == pytest.approx(pnl) and ex not in book.excluded and ex not in book.mark_src
    rec.close()
    live = [decode_event(x.data) for x in iter_raw(tmp_path / "data", ["events.live"], 0, 2**62)]
    assert [round(e.day_pnl_usd, 6) for e in live if isinstance(e, RiskStateSeed)][1] == pytest.approx(pnl)


async def test_excluded_market_closed_before_the_start_is_not_remarked_in_session():
    """Its window lies before the session's first print: the start-up mark (from the back-fill)
    stays until the result; the session's own prints would call the window 'unknown'."""
    T = (MM_T0 // H + 1) * H
    ex = "KXBTCD-EXCL-T83000.00"
    sp = mspec("greater", T // NS_PER_S, floor=83000.0, ticker=ex)
    now = T + 30 * NS_PER_S
    dec = decide_seed(now, None, type("P", (), {"pnl_usd": 0.0, "open_usd": 10.0})())
    book = RiskBook.from_decision(dec, {ex: (1000, PX_SCALE)}, specs={ex: sp}, sources={ex: OWN_BENCHMARK}, now_ns=now)
    assert not book.watch
    s = TrackerStrategy()
    r, _, _ = live_runner(s, risk_book=book, clock_ns=lambda: now)
    r.push(IndexTick(now, now, "BRTI", 84000.0, "1hz"))
    r.process_pending()
    assert book.excluded[ex] == (1000, PX_SCALE) and not [e for e in s.events if isinstance(e, RiskStateSeed)]


# ============================================================================ restart between close and result
async def test_restart_between_close_and_determination(tmp_path):
    """The account holds 10 YES (bought today at 60c) in a KXBTCD market that closed 40 s ago
    and is not determined yet (REST: closed, stale 30c bid). The restart back-fills the window
    with one CF call, marks the position at $1 (own_benchmark: the average is far above the
    strike) and seeds +$4.00, not the stale bid's -$3.00; the WS determination later changes
    nothing."""
    from .test_app import _setup
    from dh.live.app import LiveApp, Overrides

    rest, fake, lcfg, scfg, _ = _setup(tmp_path, "live", forbid_writes=False)
    now_s = time.time_ns() // NS_PER_S
    T = now_s - 40
    ex = f"KXBTCD-RESTART{T}-T80000.00"
    day0 = now_s * NS_PER_S - now_s * NS_PER_S % DAY_NS
    rest.positions = {ex: "10.00"}
    rest.fills = [fill_row("f-1", "o-1", ex, side="bid", px="0.6000", count="10.00",
                           created_ns=max(day0, (T - 120) * NS_PER_S))]
    rest.markets[ex] = closed_row(ex, T, strike_type="greater", floor=79999.99, bid="0.3000", last="0.3100")
    fv_cf = rest.cf_history
    calls: list = []
    hour_cf = cf_hour_server(flat_window(T, 8_400_000, after=0), calls)
    rest.cf_history = lambda span, ts: hour_cf(span, ts) if span == "HOUR" else fv_cf(span, ts)
    app = LiveApp(scfg, lcfg, "live", Overrides(rest=rest, ws_connect=fake.connect, install_signals=False))
    runner = await app.build()
    d = app.info["day_pnl_rest"]
    assert len(calls) == 1 and calls[0][1].endswith(":00:00.000Z")
    assert d["open_px"] == {ex: PX_SCALE} and d["mark_source"] == {ex: OWN_BENCHMARK} and d["fallbacks"] == []
    assert d["window_backfill"]["outcomes"][ex]["status"] == "yes"
    assert app.info["risk_seed"]["real_pnl_usd"] == pytest.approx(4.0)
    assert runner.riskbook.mark_src[ex] == OWN_BENCHMARK and not runner.riskbook.watch
    assert app.metrics.get("dh_position_marks_total", scope="startup", source=OWN_BENCHMARK) == 1
    runner.process_pending()
    assert runner.strategy.risk.seed_day_pnl == pytest.approx(4.0)
    t = runner.clock_ns()
    runner.push(KalshiMarketLifecycle(t, 0, ex, "determined", result="yes"))
    runner.push(Settlement(t, 0, ex, "yes", None, PX_SCALE))
    runner.process_pending()
    assert runner.strategy.risk.seed_day_pnl == pytest.approx(4.0) and ex not in runner.riskbook.excluded
    await runner.shutdown()
    await app.close()
