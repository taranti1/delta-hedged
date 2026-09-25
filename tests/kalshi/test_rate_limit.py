from __future__ import annotations

import asyncio

import pytest

from dh.kalshi.rate_limit import (
    BucketLimit,
    KalshiRateLimiter,
    RateLimitCostError,
    TokenBucket,
)


class FakeClock:
    """Deterministic clock; sleep() advances time instead of waiting."""

    def __init__(self) -> None:
        self.now = 1000.0
        self.sleeps: list[float] = []

    def __call__(self) -> float:
        return self.now

    async def sleep(self, dt: float) -> None:
        self.sleeps.append(dt)
        self.now += dt


def test_bucket_refill_and_try_acquire():
    c = FakeClock()
    b = TokenBucket(BucketLimit(10.0, 20.0), clock=c, sleep=c.sleep)
    assert b.tokens == 20.0  # starts full
    assert b.try_acquire(15) and not b.try_acquire(10)
    assert b.time_until(10) == pytest.approx(0.5)  # 5 left, deficit 5 at 10/s
    c.now += 0.5
    assert b.try_acquire(10)
    c.now += 100  # refill caps at capacity
    assert b.tokens == 20.0


async def test_acquire_waits_exact_deficit():
    c = FakeClock()
    b = TokenBucket(BucketLimit(10.0, 10.0), clock=c, sleep=c.sleep)
    assert await b.acquire(10) == 0.0
    waited = await b.acquire(4)
    assert waited == pytest.approx(0.4) and c.sleeps == [pytest.approx(0.4)]
    assert b.tokens == pytest.approx(0.0)


async def test_concurrent_acquires_are_fifo_and_total_time_is_exact():
    c = FakeClock()
    b = TokenBucket(BucketLimit(10.0, 10.0), clock=c, sleep=c.sleep)
    order: list[int] = []

    async def req(i: int) -> None:
        await b.acquire(10)
        order.append(i)

    await asyncio.gather(*(req(i) for i in range(4)))
    assert order == [0, 1, 2, 3]
    assert c.now - 1000.0 == pytest.approx(3.0)  # 40 tokens at 10/s, first 10 free


def test_cost_larger_than_capacity_raises():
    b = TokenBucket(BucketLimit(10.0, 10.0), clock=FakeClock())
    with pytest.raises(RateLimitCostError):
        b.time_until(11)


def test_limiter_bucket_selection_and_documented_costs():
    lim = KalshiRateLimiter(clock=FakeClock())
    assert lim.bucket_name("GET") == "read" and lim.bucket_name("delete") == "write"
    assert lim.cost_for("GET", "/markets") == 10
    assert lim.cost_for("GET", "/portfolio/orders/abc-123") == 2
    assert lim.cost_for("GET", "/trade-api/v2/portfolio/orders/abc-123?x=1") == 2
    assert lim.cost_for("GET", "/portfolio/orders") == 10  # list endpoint keeps default
    assert lim.cost_for("DELETE", "/portfolio/events/orders/abc") == 2
    assert lim.cost_for("DELETE", "/portfolio/events/orders") == 2  # cancel all
    assert lim.cost_for("POST", "/portfolio/events/orders") == 10
    assert lim.cost_for("POST", "/portfolio/events/orders/batched", n_items=5) == 50
    assert lim.cost_for("DELETE", "/portfolio/events/orders/batched", n_items=5) == 10


def test_limiter_updates_from_account_endpoints():
    c = FakeClock()
    lim = KalshiRateLimiter(clock=c, sleep=c.sleep)
    assert lim.limits_source == "default"
    lim.update_from_limits(
        {"usage_tier": "advanced", "read": {"refill_rate": 300, "bucket_capacity": 300},
         "write": {"refill_rate": 300, "bucket_capacity": 600}, "grants": []}
    )
    assert lim.usage_tier == "advanced" and lim.limits_source == "account"
    assert lim.write.limit == BucketLimit(300.0, 600.0)
    assert lim.write.tokens == pytest.approx(50.0)  # tokens kept (not refilled) on update
    lim.update_endpoint_costs(
        {"default_cost": 12, "endpoint_costs": [
            {"method": "GET", "path": "/trade-api/v2/markets/{ticker}/orderbook", "cost": 3},
            {"method": "POST", "path": "/portfolio/events/orders/batched", "cost": 8},
        ]}
    )
    assert lim.cost_for("GET", "/markets/KXBTCD-X/orderbook") == 3
    assert lim.cost_for("GET", "/markets") == 12
    assert lim.cost_for("POST", "/portfolio/events/orders/batched", 3) == 24
    assert lim.cost_for("GET", "/portfolio/orders/x") == 2  # documented default survives
    assert lim.max_batch_items("POST", "/portfolio/events/orders/batched") == 75


async def test_limiter_acquire_and_429_drain():
    c = FakeClock()
    lim = KalshiRateLimiter(read=BucketLimit(10, 20), write=BucketLimit(10, 10), clock=c, sleep=c.sleep)
    assert await lim.acquire("GET", "/markets") == 0.0
    assert await lim.acquire("GET", "/markets") == 0.0
    waited = await lim.acquire("GET", "/markets")
    assert waited == pytest.approx(1.0)
    lim.on_429("POST")
    assert lim.write.tokens == 0.0
    assert not lim.try_acquire("POST", "/portfolio/events/orders")
    with pytest.raises(ValueError):
        BucketLimit.from_json({"refill_rate": 0, "bucket_capacity": 5})
