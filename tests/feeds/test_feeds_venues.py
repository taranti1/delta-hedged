"""Normalizer tests per venue on realistic fixture frames (snapshot + updates + trades)."""

from __future__ import annotations

import orjson
import pytest

from dh.core.book import ExtBook
from dh.core.events import (
    ExtBBO,
    ExtBookDelta,
    ExtBookSnapshot,
    ExtTrade,
    FeedStatus,
    IndexTick,
    Liquidation,
    OptionQuote,
    PerpState,
)
from dh.feeds.base import NormalizerState, safe_normalize
from dh.feeds.books import BookTracker
from dh.feeds.registry import normalizer_for
from tests.feeds.helpers import T0_NS, VENUE_FIXTURES, load_fixture, of_type, run_fixture


@pytest.mark.parametrize("name", VENUE_FIXTURES)
def test_deterministic_and_state_serializable(name):
    """Same frames -> same events; checkpointing the state mid-stream changes nothing."""
    ev1, st1 = run_fixture(name)
    ev2, st2 = run_fixture(name)
    assert ev1 == ev2 and st1.to_json() == st2.to_json()
    recs = load_fixture(name)
    norm, st = normalizer_for(recs[0][1])
    out = []
    half = len(recs) // 2
    for i, (t, _s, _q, raw) in enumerate(recs):
        if i == half:
            st = NormalizerState.from_dict(orjson.loads(st.to_json()))
        out += safe_normalize(norm, raw, t, st)
    assert out == ev1
    assert not [e for e in ev1 if isinstance(e, FeedStatus) and "normalize" in e.detail]


@pytest.mark.parametrize("name", VENUE_FIXTURES)
def test_event_invariants(name):
    events, _ = run_fixture(name)
    assert events, name
    for e in events:
        assert e.ts >= T0_NS
        if isinstance(e, ExtBookSnapshot):
            assert all(e.bids[i][0] > e.bids[i + 1][0] for i in range(len(e.bids) - 1))
            assert all(e.asks[i][0] < e.asks[i + 1][0] for i in range(len(e.asks) - 1))
            assert all(s > 0 for _, s in e.bids + e.asks)
            if e.bids and e.asks:
                assert e.bids[0][0] < e.asks[0][0]
        if isinstance(e, ExtBookDelta):
            assert e.changes and all(side in ("b", "a") and s >= 0 for side, _, s in e.changes)
        if isinstance(e, ExtTrade):
            assert e.aggressor in ("buy", "sell") and e.size > 0 and e.price > 0 and e.trade_id
            assert e.ts_exch > 0 and e.ts - e.ts_exch < 10**9  # history is filtered out
        if isinstance(e, (ExtTrade, ExtBookDelta, ExtBookSnapshot, ExtBBO)):
            assert e.ts_exch == 0 or abs(e.ts - e.ts_exch) < 5 * 10**9


def _replay_books(events):
    tr = BookTracker()
    for e in events:
        tr.on_event(e)
    return tr


def test_coinbase():
    ev, st = run_fixture("coinbase")
    snaps = of_type(ev, ExtBookSnapshot)
    assert snaps[0].bids[0] == (84500.01, 0.51) and snaps[0].asks[0] == (84500.02, 0.3)
    assert snaps[0].depth_limited is False and snaps[0].seq == 1
    assert snaps[0].ts_exch == T0_NS + 55_000_123  # message timestamp, ns precision
    trades = of_type(ev, ExtTrade)
    # snapshot trades are history; duplicate id 812345672 emitted once
    assert [t.trade_id for t in trades] == ["812345672", "812345673"]
    assert [t.aggressor for t in trades] == ["buy", "sell"]
    gaps = [e for e in ev if isinstance(e, FeedStatus) and e.status == "gap"]
    assert len(gaps) == 1 and gaps[0].stream == "coinbase.book:BTC-USD" and "7 -> 9" in gaps[0].detail
    assert st.stats["deltas_suppressed"] == 2
    assert [e.status for e in ev if isinstance(e, FeedStatus)] == ["connected", "gap", "resynced", "disconnected"]
    # book after the resync snapshot + one delta
    tr = BookTracker()
    for e in ev[:-1]:
        tr.on_event(e)
    b = tr.books[("coinbase", "BTC-USD")]
    assert list(b.bids.items())[::-1] == [(84500.01, 0.51), (84499.5, 1.2), (84499.0, 0.25)]
    assert b.top().ask == 84500.02


def test_kraken_book_state_matches_events():
    ev, st = run_fixture("kraken")
    assert st.stats["checksum_ok"] == 4 and st.stats["checksum_fail"] == 1
    deltas = of_type(ev, ExtBookDelta)
    assert deltas[0].changes == (("b", 45283.5, 0.0), ("b", 45275.0, 0.5))
    # truncation of the depth-10 book is emitted as an explicit removal
    assert deltas[1].changes == (("a", 45284.0, 0.25), ("a", 45299.5, 0.0))
    trades = of_type(ev, ExtTrade)
    assert [(t.aggressor, t.price, t.size) for t in trades] == [("buy", 45284.0, 0.0015), ("sell", 45283.4, 0.02)]
    statuses = [(e.stream, e.status) for e in ev if isinstance(e, FeedStatus)]
    assert statuses == [("kraken.ws", "connected"), ("kraken.book:BTC/USD", "gap"), ("kraken.book:BTC/USD", "resynced")]
    # the consumer book equals the normalizer's own book before the bad update
    tr = BookTracker()
    for e in ev[: ev.index(next(e for e in ev if isinstance(e, FeedStatus) and e.status == "gap"))]:
        tr.on_event(e)
    b = tr.books[("kraken", "BTC/USD")]
    assert len(b.bids) == 10 and len(b.asks) == 10
    assert b.top().bid == 45283.4 and b.top().ask == 45284.0


def test_bitstamp_snapshot_alignment():
    ev, st = run_fixture("bitstamp")
    snap = of_type(ev, ExtBookSnapshot)[0]
    # REST book @150ms folded with the buffered diff @290ms, not the one @90ms
    assert snap.bids == ((84494.0, 0.5), (84490.0, 0.1))
    assert snap.asks == ((84505.0, 0.4), (84510.0, 0.7), (84520.0, 2.0))
    assert snap.ts == T0_NS + 1_600_000_000 + 5 and snap.ts_exch == T0_NS + 290_000_000
    assert snap.depth_limited is False
    deltas = of_type(ev, ExtBookDelta)
    assert len(deltas) == 1 and deltas[0].changes == (("b", 84496.0, 0.3), ("a", 84505.0, 0.0))
    assert st.stats["diff_not_newer_than_book"] == 1
    assert [(t.aggressor, t.price) for t in of_type(ev, ExtTrade)] == [("buy", 84510.0), ("sell", 84496.0)]


def test_bitstamp_periodic_resnapshot_refolds_recent_diffs():
    _ev, st = run_fixture("bitstamp")
    norm, _ = normalizer_for("bitstamp.ws")
    body = orjson.dumps({"timestamp": "0", "microtimestamp": str(1790337600 * 10**6 + 1_650_000),
                         "bids": [["84495", "1.0"]], "asks": [["84505", "0.4"]]}).decode()
    rest = orjson.dumps({"_dh": "rest", "conn": 1, "kind": "order_book", "status": 200, "body": body, "symbol": "btcusd", "url": "u", "req_ns": 0})
    out = norm(rest, T0_NS + 2_000_000_000, st)
    snap = out[0]
    # the diff @1690ms (already emitted as a delta) is newer than this REST book: re-applied
    assert snap.bids == ((84496.0, 0.3), (84495.0, 1.0)) and snap.asks == ()
    bad = orjson.dumps({"_dh": "rest", "conn": 1, "kind": "order_book", "status": 503, "body": "", "symbol": "btcusd", "url": "u", "req_ns": 0})
    assert norm(bad, T0_NS + 3 * 10**9, st)[0].status == "error"


def test_gemini():
    ev, _ = run_fixture("gemini")
    snap = of_type(ev, ExtBookSnapshot)[0]
    assert snap.bids == ((84498.12, 0.25), (84497.0, 1.5)) and snap.asks == ((84500.5, 0.4), (84502.0, 0.9))
    assert of_type(ev, ExtBookDelta)[0].changes == (("b", 84498.12, 0.0), ("a", 84500.0, 0.1))
    trades = of_type(ev, ExtTrade)
    assert [t.trade_id for t in trades] == ["207000002"]  # snapshot trade = history, dup dropped


def test_gemini_first_message_without_connect_marker_is_not_a_snapshot():
    recs = load_fixture("gemini")[1:]  # replay starting mid-connection
    norm, st = normalizer_for("gemini.ws")
    ev = []
    for t, _s, _q, raw in recs:
        ev += norm(raw, t, st)
    assert not of_type(ev, ExtBookSnapshot) and not of_type(ev, ExtBookDelta)


def test_cryptocom_and_heartbeat_reply():
    ev, st = run_fixture("cryptocom")
    snap = of_type(ev, ExtBookSnapshot)[0]
    assert snap.seq == 900000100 and snap.bids[0] == (84499.9, 0.3) and snap.asks[0] == (84501.1, 0.21)
    d = of_type(ev, ExtBookDelta)
    assert len(d) == 1 and d[0].seq == 900000108 and d[0].prev_seq == 900000100
    t = of_type(ev, ExtTrade)[0]
    assert t.aggressor == "buy" and t.ts_exch == (1790337600000 + 1085) * 10**6 + 123
    gaps = [e for e in ev if isinstance(e, FeedStatus) and e.status == "gap"]
    assert gaps and "pu 900000110" in gaps[0].detail
    from dh.feeds.cryptocom import CryptoComFeed

    replies = CryptoComFeed().control_replies(b'{"id":1790337601080,"method":"public/heartbeat","code":0}')
    assert [orjson.loads(r) for r in replies] == [{"id": 1790337601080, "method": "public/respond-heartbeat"}]


def test_deribit():
    ev, _ = run_fixture("deribit")
    snap = of_type(ev, ExtBookSnapshot)[0]
    assert snap.symbol == "BTC-PERPETUAL" and snap.seq == 71000000100
    assert snap.bids[0][0] == 84510.0 and snap.bids[0][1] == pytest.approx(169000 / 84510.0)  # USD -> BTC
    delta = of_type(ev, ExtBookDelta)[0]
    assert delta.prev_seq == 71000000100 and delta.changes[1] == ("b", 84509.5, 0.0)
    perp = of_type(ev, PerpState)[0]
    assert perp.mark == 84500.0 and perp.index == 84480.0 and perp.funding_rate == 0.0001
    assert perp.funding_interval_s == 8 * 3600 and perp.open_interest == pytest.approx(1_014_000_000 / 84500.0)
    bbo = of_type(ev, ExtBBO)[0]
    assert bbo.bid_size == pytest.approx(1.0)
    trades = of_type(ev, ExtTrade)
    assert [t.aggressor for t in trades] == ["buy", "sell"] and trades[1].size == pytest.approx(1.0)
    liq = of_type(ev, Liquidation)
    assert len(liq) == 1 and liq[0].side == "sell"  # taker-side liquidation sold
    idx = of_type(ev, IndexTick)
    assert [(i.index_id, i.value, i.feed) for i in idx] == [("deribit:btc_usd", 84480.12, "deribit"), ("deribit:dvol_btc_usd", 0.425, "deribit")]
    assert all(i.index_id != "BRTI" for i in idx)
    opt = of_type(ev, OptionQuote)[0]
    assert opt.instrument == "BTC-26SEP26-85000-C" and opt.cp == "C" and opt.strike == 85000.0
    assert opt.expiry_ts == (1790337600 + 20 * 3600) * 10**9  # 2026-09-26 08:00 UTC
    assert (opt.mark_iv, opt.bid_iv, opt.ask_iv) == pytest.approx((0.385, 0.379, 0.392))
    assert (opt.bid, opt.ask, opt.underlying) == (0.0085, 0.0093, 84495.0)
    statuses = [(e.stream, e.status) for e in ev if isinstance(e, FeedStatus)]
    assert ("deribit.book:BTC-PERPETUAL", "gap") in statuses and ("deribit.ws", "error") in statuses


def test_deribit_option_selection_and_names():
    from dh.feeds.deribit import DeribitFeed, parse_option_name, select_options

    assert parse_option_name("BTC-PERPETUAL") is None
    assert parse_option_name("BTC-5OCT26-90000-P")[2] == "P"
    now_ms = 1790337600000
    inst = []
    for exp_off_h, tag in ((20, "26SEP26"), (44, "27SEP26"), (164, "2OCT26")):
        for k in range(70000, 100001, 5000):
            for cp in ("C", "P"):
                inst.append({"instrument_name": f"BTC-{tag}-{k}-{cp}", "expiration_timestamp": now_ms + exp_off_h * 3600_000,
                             "strike": float(k), "kind": "option", "is_active": True})
    inst.append({"instrument_name": "BTC-25SEP26-85000-C", "expiration_timestamp": now_ms - 1, "strike": 85000.0, "kind": "option"})
    names = select_options(inst, 84500.0, now_ms, n_expiries=2, moneyness=0.06, max_n=100)
    assert names == sorted(names) and all("2OCT26" not in n and "25SEP26" not in n for n in names)
    strikes = {int(n.split("-")[2]) for n in names}
    assert strikes == {80000, 85000}
    assert len(select_options(inst, 84500.0, now_ms, 2, 0.06, 3)) == 3
    # the client turns discovery responses into subscriptions
    f = DeribitFeed(None)
    f.clock_ns = lambda: now_ms * 1_000_000
    f.options.update(option_expiries=1, option_moneyness=0.02)
    f.channels = ("options",)
    msgs = f.subscribe_messages()
    reqs = [orjson.loads(m) for m in msgs]
    idx_id = next(r["id"] for r in reqs if r["method"] == "public/get_index_price")
    ins_id = next(r["id"] for r in reqs if r["method"] == "public/get_instruments")
    assert f.control_replies(orjson.dumps({"jsonrpc": "2.0", "id": idx_id, "result": {"index_price": 84500.0}})) == []
    out = f.control_replies(orjson.dumps({"jsonrpc": "2.0", "id": ins_id, "result": inst}))
    subs = [orjson.loads(m) for m in out]
    assert subs and subs[0]["method"] == "public/subscribe"
    assert subs[0]["params"]["channels"] == ["ticker.BTC-26SEP26-85000-C.100ms", "ticker.BTC-26SEP26-85000-P.100ms"]
    test_reply = f.control_replies(b'{"jsonrpc":"2.0","method":"heartbeat","params":{"type":"test_request"}}')
    assert orjson.loads(test_reply[0])["method"] == "public/test"


def test_binance_futures():
    ev, st = run_fixture("binance_futures")
    bbo = of_type(ev, ExtBBO)
    assert len(bbo) == 1 and bbo[0].bid == 84520.1 and bbo[0].ask_size == 1.004
    assert st.stats["bbo_out_of_order"] == 1
    assert [t.aggressor for t in of_type(ev, ExtTrade)] == ["buy", "sell"]  # m=False -> buyer took
    p = of_type(ev, PerpState)[0]
    assert p.mark == 84515.3 and p.index == 84490.52 and p.funding_rate == 0.0001
    assert p.next_funding_ts == T0_NS + 4 * 3600 * 10**9
    liq = of_type(ev, Liquidation)[0]
    assert liq.side == "sell" and liq.price == 84480.5 and liq.size == 0.5
    from dh.feeds.binance_futures import BinanceFuturesFeed

    url = BinanceFuturesFeed().url
    assert url.startswith("wss://fstream.binance.com/stream?streams=") and "btcusdt@markPrice@1s" in url


def test_bybit():
    ev, st = run_fixture("bybit")
    snap = of_type(ev, ExtBookSnapshot)[0]
    assert snap.seq == 5100001 and snap.bids[0] == (84525.1, 1.204)
    d = of_type(ev, ExtBookDelta)
    assert len(d) == 1 and d[0].changes == (("b", 84525.1, 0.0), ("a", 84525.3, 0.3))
    assert st.stats["stale_update_dropped"] == 1
    perps = of_type(ev, PerpState)
    assert len(perps) == 2 and perps[1].mark == 84523.0 and perps[1].index == 84497.8  # delta merged
    assert perps[0].open_interest == 51234.567 and perps[0].funding_interval_s == 8 * 3600
    liq = of_type(ev, Liquidation)[0]
    assert liq.side == "sell"  # a long (position side Buy) was liquidated
    assert of_type(ev, ExtTrade)[0].aggressor == "buy"


def test_bybit_contiguity_flag(monkeypatch):
    import dh.feeds.bybit as bybit

    norm, st = normalizer_for("bybit.ws")
    recs = load_fixture("bybit")
    for t, _s, _q, raw in recs[:3]:
        norm(raw, t, st)
    jump = orjson.dumps({"topic": "orderbook.50.BTCUSDT", "type": "delta", "ts": 1, "data": {"s": "BTCUSDT", "b": [["1", "1"]], "a": [], "u": 5100009}})
    st2 = NormalizerState.from_dict(st.to_dict())
    assert of_type(norm(jump, T0_NS + 10**9, st), ExtBookDelta)  # default: non-contiguous u tolerated
    assert st.stats["u_noncontiguous"] == 1
    monkeypatch.setattr(bybit, "U_CONTIGUOUS", True)
    out = norm(jump, T0_NS + 10**9, st2)
    assert out[0].status == "gap"


def test_okx():
    ev, _ = run_fixture("okx")
    snap = of_type(ev, ExtBookSnapshot)[0]
    assert snap.bids[0] == (84530.0, 2.5) and snap.asks[0] == (84530.1, 1.2)  # contracts * 0.01 BTC
    t = of_type(ev, ExtTrade)[0]
    assert t.size == pytest.approx(0.12) and t.aggressor == "buy"
    perps = of_type(ev, PerpState)
    assert len(perps) == 2  # emitted only once the mark is known
    assert perps[-1].mark == 84522.4 and perps[-1].index == 84501.2 and perps[-1].open_interest == 25000.0
    assert perps[-1].funding_interval_s == 8 * 3600 and perps[-1].next_funding_ts == T0_NS + 4 * 3600 * 10**9
    liq = of_type(ev, Liquidation)
    assert len(liq) == 1 and liq[0].symbol == "BTC-USDT-SWAP" and liq[0].size == pytest.approx(0.3)
    assert any(isinstance(e, FeedStatus) and e.status == "error" for e in ev)


def test_hyperliquid():
    ev, _ = run_fixture("hyperliquid")
    snap = of_type(ev, ExtBookSnapshot)[0]
    assert snap.bids == ((84540.0, 3.1204), (84539.0, 5.5)) and snap.asks[0] == (84541.0, 1.8)
    trades = of_type(ev, ExtTrade)
    assert [t.trade_id for t in trades] == ["900000000000002", "900000000000003"]  # history + dup dropped
    assert [t.aggressor for t in trades] == ["buy", "sell"]
    p = of_type(ev, PerpState)[0]
    assert p.mark == 84538.0 and p.index == 84510.0 and p.funding_interval_s == 3600


def test_all_books_consistent_after_fixture():
    for name in VENUE_FIXTURES:
        ev, _ = run_fixture(name)
        tr = _replay_books(ev)
        for (venue, sym), b in tr.books.items():
            assert isinstance(b, ExtBook)
            if b.valid:
                assert not b.crossed(), (name, venue, sym)
