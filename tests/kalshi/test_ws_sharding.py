from __future__ import annotations

import asyncio

from dh.kalshi.ws import KalshiWS, Subscription, shard_market_subscriptions


def test_shard_market_subscriptions():
    subs = shard_market_subscriptions(["orderbook_delta", "trade"], [f"M{i}" for i in range(250)], 100)
    assert [len(s.market_tickers) for s in subs] == [100, 100, 50]
    assert all(s.channels == ["orderbook_delta", "trade"] for s in subs)
    assert len(shard_market_subscriptions(["orderbook_delta"], ["a", "b"], 0)) == 1


def test_add_and_delete_markets_respect_cap_offline():
    subs = shard_market_subscriptions(["orderbook_delta"], [f"M{i}" for i in range(95)], 100)
    subs.append(Subscription(["cfbenchmarks_value"], index_ids=["BRTI"]))
    ws = KalshiWS("wss://x", None, subs, max_markets_per_subscription=100)

    async def go():
        await ws.add_markets([f"N{i}" for i in range(12)] + ["M0"], channel="orderbook_delta")
        await ws.delete_markets(["M1", "N11"], channel="orderbook_delta")

    asyncio.run(go())
    market_subs = [s for s in ws.subscriptions if "orderbook_delta" in s.channels]
    sizes = [len(s.market_tickers) for s in market_subs]
    assert sizes == [99, 6]  # 95 + 5 filled the first shard to 100, 7 went to a new shard; 2 deleted
    all_t = [t for s in market_subs for t in s.market_tickers]
    assert len(all_t) == len(set(all_t)) and "M1" not in all_t and "N11" not in all_t and "M0" in all_t
