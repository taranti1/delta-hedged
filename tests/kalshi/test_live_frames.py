"""Wire formats verified against REAL Kalshi prod frames captured 2026-09-25 (first live contact).

Fixtures (public market data and the public cost table only; no account data):
  fixtures/live_ws_2026-09-25.json          WS frames (control lists of market_tickers truncated)
  fixtures/live_endpoint_costs_2026-09-25.json   GET /account/endpoint_costs body
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import orjson

from dh.core.events import IndexTick, KalshiBookDelta, KalshiBookSnapshot, KalshiMarketLifecycle, KalshiTicker, KalshiTrade
from dh.core.units import NS_PER_MS, NS_PER_S
from dh.kalshi.normalize import ws_message_to_events
from dh.kalshi.rate_limit import BucketLimit, KalshiRateLimiter
from dh.kalshi.sequencer import KalshiWsState, normalize_ws_message
from dh.kalshi.ws import KalshiWS, shard_market_subscriptions

from .fake_ws import FakeKalshi

FIX = Path(__file__).parent / "fixtures"
WS = orjson.loads((FIX / "live_ws_2026-09-25.json").read_bytes())
COSTS = orjson.loads((FIX / "live_endpoint_costs_2026-09-25.json").read_bytes())


def one(msg: dict) -> object:
    evs = ws_message_to_events(msg, 1, use_yes_price=False)
    assert len(evs) == 1
    return evs[0]


# ---------------------------------------------------------------------------- sequencing
def test_merged_subscribe_ok_replies_consume_seq_no_false_gap():
    """One sid per channel per connection: shard subscribes 2..5 were answered with 'ok'
    carrying sid+seq (orderbook_delta/trade) or sid only (ticker); the first orderbook
    snapshot on sid 1 then had seq 5 (4 oks before it). No gap must be reported."""
    st = KalshiWsState()
    out = []
    for m in WS["control"]:
        out += normalize_ws_message(m, 1, st)
    oks = [m for m in WS["control"] if m["type"] == "ok"]
    assert {(m["sid"], "seq" in m) for m in oks} == {(1, True), (2, True), (3, False)}
    snap = WS["snapshot"]
    assert snap["sid"] == 1 and snap["seq"] == 5
    out += normalize_ws_message(snap, 2, st)
    out += normalize_ws_message(WS["trade"], 3, st)  # sid 2 seq 5 after 4 oks
    assert st.counters["gaps"] == 0 and st.counters["dups"] == 0
    assert [type(e).__name__ for e in out] == ["KalshiBookSnapshot", "KalshiTrade"]
    assert st.sids[1].channel == "orderbook_delta" and st.sids[2].channel == "trade"


# ---------------------------------------------------------------------------- normalizers
def test_orderbook_snapshot_and_delta_real_shape():
    snap = one(WS["snapshot"])
    assert isinstance(snap, KalshiBookSnapshot) and snap.ticker == "KXBTC-26SEP2516-B74250" and snap.sid == 1
    raw_yes = WS["snapshot"]["msg"].get("yes_dollars_fp") or []
    raw_no = WS["snapshot"]["msg"].get("no_dollars_fp") or []
    assert len(snap.yes_bids) == len(raw_yes) and len(snap.no_bids) == len(raw_no)
    d = one(WS["delta_same_market"])
    # {"price_dollars":"0.9600","delta_fp":"-2.00","side":"no","ts":"<ISO>","ts_ms":1790363624342}
    assert isinstance(d, KalshiBookDelta) and d.side == "no" and d.px == 9600 and d.delta == -200
    assert d.ts_exch == 1790363624342 * NS_PER_MS  # ts_ms preferred over the ISO 'ts'


def test_trade_real_shape():
    t = one(WS["trade"])
    m = WS["trade"]["msg"]
    assert isinstance(t, KalshiTrade) and t.trade_id == m["trade_id"]
    # {"yes_price_dollars":"0.9840","count_fp":"148.78","taker_outcome_side":"yes","is_block_trade":false}
    assert t.yes_px == 9840 and t.qty == 14878 and t.taker_side == "yes" and not t.is_block
    assert t.ts_exch == m["ts_ms"] * NS_PER_MS  # 'ts' is integer SECONDS on trades


def test_ticker_real_shape():
    t = one(WS["ticker"])
    assert isinstance(t, KalshiTicker) and t.ticker == "KXBTCD-26SEP2516-T83399.99"
    assert (t.yes_bid, t.yes_ask, t.last_px) == (9900, 10000, 9900)
    assert (t.yes_bid_qty, t.yes_ask_qty, t.volume, t.open_interest) == (104600, 0, 795052, 733362)
    assert t.ts_exch == 1790363592125 * NS_PER_MS
    assert "seq" not in WS["ticker"]  # the ticker channel is not sequenced


def test_lifecycle_real_shapes():
    s = one(WS["lifecycle_settled"])
    assert isinstance(s, KalshiMarketLifecycle) and s.event_type == "settled" and s.settled_ts == 1790363638 * NS_PER_S
    c = one(WS["lifecycle_created"])
    assert c.event_type == "created" and c.strike_type == "greater" and c.floor_strike == 6.5
    assert c.event_ticker == "KXAFCONTOTAL-26SEP25MLICPV" and c.open_ts == 1790363733 * NS_PER_S
    assert c.close_ts == 1790535600 * NS_PER_S and c.price_level_structure == "linear_cent"
    assert c.price_ranges == ((0, 10000, 100),)


def test_cfbenchmarks_1hz_real_shape():
    """1 Hz frames carry NO value_usd/source_ts_ms: value and source time come from the raw
    upstream 'data' JSON string; avg_60s_data.window_size counts prior ticks OF THIS
    SUBSCRIPTION (0 on the first tick after subscribing: the average warms up for 60 s)."""
    f = one(WS["cf_1hz_first"])
    assert isinstance(f, IndexTick) and f.feed == "1hz" and f.index_id == "BRTI"
    assert f.value == 84018.87 and f.ts_exch == 1790363593000 * NS_PER_MS
    assert f.kalshi_recv_ns == 1790363593053 * NS_PER_MS and f.avg60_n == 0 and f.qh_avg is None
    q = one(WS["cf_1hz_quarter_hour"])  # final minute before a quarter-hour close
    assert q.qh_avg is not None and q.qh_n >= 1 and q.avg60 is not None and q.avg60_n > 0


def test_cfbenchmarks_5hz_real_shape():
    t = one(WS["cf_5hz"])
    assert isinstance(t, IndexTick) and t.feed == "5hz" and t.value == 84017.28
    assert t.ts_exch == 1790363592200 * NS_PER_MS and t.kalshi_recv_ns == 1790363592237 * NS_PER_MS


# ---------------------------------------------------------------------------- rate limits
def test_real_endpoint_costs_and_account_share():
    lim = KalshiRateLimiter(account_share=0.2)
    lim.update_endpoint_costs(COSTS)
    # server paths carry the /trade-api/v2 prefix, ':param' and '*wildcard' segments
    assert lim.cost_for("GET", "/cfbenchmarks/history/values") == 50
    assert lim.cost_for("GET", "/cfbenchmarks") == 50
    assert lim.cost_for("GET", "/portfolio/orders/abc-123") == 2
    assert lim.cost_for("DELETE", "/portfolio/events/orders/abc") == 2
    assert lim.cost_for("GET", "/markets/orderbooks") == 10
    # Basic tier as returned by GET /account/limits on 2026-09-25
    lim.update_from_limits({"usage_tier": "basic", "read": {"refill_rate": 200, "bucket_capacity": 600},
                            "write": {"refill_rate": 100, "bucket_capacity": 100}})
    assert lim.read.limit == BucketLimit(40.0, 120.0)
    assert lim.write.limit == BucketLimit(20.0, 30.0)  # capacity floored at the largest write cost (30)
    assert lim.account_read == BucketLimit(200.0, 600.0)
    full = KalshiRateLimiter()
    full.update_from_limits({"read": {"refill_rate": 200, "bucket_capacity": 600},
                             "write": {"refill_rate": 100, "bucket_capacity": 100}})
    assert full.read.limit == BucketLimit(200.0, 600.0)  # share 1.0 = unchanged


# ---------------------------------------------------------------------------- WS client
def test_ws_maps_every_shard_to_the_merged_sid_and_updates_reach_the_server():
    subs = shard_market_subscriptions(["orderbook_delta", "trade", "ticker"], [f"M{i}" for i in range(5)], 2)
    fake = FakeKalshi([[]])
    ws = KalshiWS("wss://x/trade-api/ws/v2", None, subs, connect=fake.connect, stale_after_s=None,
                  max_markets_per_subscription=2)

    books: set[str] = set()

    async def go():
        task = asyncio.create_task(ws.run())
        try:
            async with asyncio.timeout(3.0):
                while len(ws._sub_sids) < 3 or any(len(v) < 3 for v in ws._sub_sids.values()):
                    await asyncio.sleep(0)
                await ws.add_markets(["N1"], channel="orderbook_delta")  # last shard has room (1 market)
                await ws.delete_markets(["M3"], channel="orderbook_delta")  # lives in shard 1
                while "N1" not in ws.state.book_tickers():  # N1's snapshot arrived on the merged sid
                    await asyncio.sleep(0)
                books.update(ws.state.book_tickers())
        finally:
            await ws.stop()
            await asyncio.wait_for(task, 3.0)

    asyncio.run(go())
    conn = fake.conns[0]
    ob, tr, tk = conn.sid_of("orderbook_delta"), conn.sid_of("trade"), conn.sid_of("ticker")
    assert ws._sub_sids == {i: {"orderbook_delta": ob, "trade": tr, "ticker": tk} for i in range(3)}
    upd = [(c["params"]["action"], c["params"]["sid"], tuple(c["params"]["market_tickers"]))
           for c in conn.sent if c["cmd"] == "update_subscription"]
    assert upd == [("add_markets", ob, ("N1",)), ("add_markets", tk, ("N1",)), ("add_markets", tr, ("N1",)),
                   ("delete_markets", ob, ("M3",)), ("delete_markets", tk, ("M3",)), ("delete_markets", tr, ("M3",))]
    assert ws.state.counters["gaps"] == 0
    assert books >= {"M0", "M1", "M2", "M3", "M4", "N1"}
