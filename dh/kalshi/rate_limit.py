"""Client-side token buckets mirroring Kalshi's API rate limits.

Kalshi meters each account with two token buckets (openapi ``BucketLimit``,
``GET /account/limits``): a *read* bucket and a *write* bucket. Each request deducts its
endpoint cost in tokens; the bucket refills at ``refill_rate`` tokens/second up to
``bucket_capacity``; a request that finds too few tokens is rejected with HTTP 429.

Costs (``GET /account/endpoint_costs``): ``default_cost`` (currently 10) for every endpoint
not listed; the listed endpoints override it. Documented non-default costs (openapi x-mint
notes, used until the server list is loaded):

  GET    /portfolio/orders/{order_id}                2
  DELETE /portfolio/events/orders/{order_id}         2
  DELETE /portfolio/events/orders                    2   (cancel all)
  POST   /portfolio/events/orders/batched            10 per order in the batch
  DELETE /portfolio/events/orders/batched            2 per order in the batch
  POST   /account/api_usage_level/upgrade            30

Bucket selection: GET/HEAD -> read; POST/PUT/PATCH/DELETE -> write.

Conservative defaults (used only until ``update_from_limits`` is called with the account's
real limits): read 100 tokens/s (capacity 100), write 50 tokens/s (capacity 50), i.e. at most
10 default-cost reads/s and 5 order creates/s. Always load the account limits at start-up.

Units: tokens (dimensionless), seconds for all clock values. The clock is injectable
(``clock()`` -> float seconds, monotonic) and so is ``sleep`` so tests are deterministic.
"""

from __future__ import annotations

import asyncio
import re
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from dh.kalshi.wire import normalize_route

Clock = Callable[[], float]
Sleep = Callable[[float], Awaitable[None]]

DEFAULT_COST = 10
TIME_EPS_S = 1e-6  # waits below this are rounding noise
PER_ITEM_ENDPOINTS: tuple[tuple[str, str], ...] = (
    ("POST", "/portfolio/events/orders/batched"),
    ("DELETE", "/portfolio/events/orders/batched"),
    ("POST", "/portfolio/orders/batched"),
    ("DELETE", "/portfolio/orders/batched"),
)
DOCUMENTED_COSTS: tuple[tuple[str, str, int], ...] = (
    ("GET", "/portfolio/orders/{order_id}", 2),
    ("DELETE", "/portfolio/events/orders/{order_id}", 2),
    ("DELETE", "/portfolio/events/orders", 2),
    ("POST", "/portfolio/events/orders/batched", 10),
    ("DELETE", "/portfolio/events/orders/batched", 2),
    ("POST", "/account/api_usage_level/upgrade", 30),
)


@dataclass(frozen=True, slots=True)
class BucketLimit:
    """refill_rate: tokens added per second; bucket_capacity: max tokens held."""

    refill_rate: float
    bucket_capacity: float

    @classmethod
    def from_json(cls, obj: dict[str, Any]) -> BucketLimit:
        """openapi BucketLimit {refill_rate:int, bucket_capacity:int}."""
        rate = float(obj["refill_rate"])
        cap = float(obj["bucket_capacity"])
        if rate <= 0 or cap <= 0:
            raise ValueError(f"invalid bucket limit {obj!r}")
        return cls(rate, cap)


DEFAULT_READ = BucketLimit(refill_rate=100.0, bucket_capacity=100.0)
DEFAULT_WRITE = BucketLimit(refill_rate=50.0, bucket_capacity=50.0)


class RateLimitCostError(ValueError):
    """A single request costs more than the bucket can ever hold (split the batch)."""


class TokenBucket:
    """Continuous-refill token bucket. Starts full. FIFO for concurrent async waiters."""

    def __init__(self, limit: BucketLimit, clock: Clock = time.monotonic, sleep: Sleep = asyncio.sleep):
        self.limit = limit
        self._clock = clock
        self._sleep = sleep
        self._tokens = float(limit.bucket_capacity)
        self._t = clock()
        self._lock: asyncio.Lock | None = None
        self.waited_s = 0.0  # cumulative time spent waiting for tokens (s)
        self.acquired = 0  # number of successful acquisitions

    # ------------------------------------------------------------------ state
    def _refill(self) -> None:
        now = self._clock()
        dt = now - self._t
        if dt > 0:
            self._tokens = min(self.limit.bucket_capacity, self._tokens + dt * self.limit.refill_rate)
        self._t = now

    @property
    def tokens(self) -> float:
        """Tokens available now (after refill)."""
        self._refill()
        return self._tokens

    def update_limit(self, limit: BucketLimit) -> None:
        """Adopt new limits; current tokens are clamped to the new capacity."""
        self._refill()
        self.limit = limit
        self._tokens = min(self._tokens, limit.bucket_capacity)

    def drain(self) -> None:
        """Empty the bucket (after an HTTP 429 our view was too optimistic)."""
        self._refill()
        self._tokens = 0.0

    # ------------------------------------------------------------------ acquisition
    def time_until(self, cost: float) -> float:
        """Seconds until `cost` tokens are available (0 if available now).

        Deficits shorter than TIME_EPS_S are treated as available: after sleeping exactly
        the computed wait, float rounding can leave a residue too small to advance a clock.
        """
        if cost > self.limit.bucket_capacity:
            raise RateLimitCostError(f"cost {cost} exceeds bucket capacity {self.limit.bucket_capacity}")
        self._refill()
        deficit = cost - self._tokens
        if deficit <= 0:
            return 0.0
        wait = deficit / self.limit.refill_rate
        return 0.0 if wait < TIME_EPS_S else wait

    def try_acquire(self, cost: float) -> bool:
        """Deduct `cost` tokens if available now; never waits."""
        if self.time_until(cost) > 0:
            return False
        self._tokens -= cost
        self.acquired += 1
        return True

    async def acquire(self, cost: float) -> float:
        """Wait (FIFO) until `cost` tokens are available, deduct them; returns seconds waited."""
        if self._lock is None:
            self._lock = asyncio.Lock()
        async with self._lock:
            waited = 0.0
            while True:
                wait = self.time_until(cost)
                if wait <= 0:
                    self._tokens -= cost
                    self.acquired += 1
                    self.waited_s += waited
                    return waited
                await self._sleep(wait)
                waited += wait


def _route_regex(template: str) -> re.Pattern[str]:
    parts = []
    for seg in normalize_route(template).split("/"):
        if (seg.startswith("{") and seg.endswith("}")) or seg.startswith(":"):
            parts.append("[^/]+")
        else:
            parts.append(re.escape(seg))
    return re.compile("^" + "/".join(parts) + "$")


class KalshiRateLimiter:
    """Read/write token buckets + endpoint cost table.

    Use: ``await limiter.acquire(method, path, n_items)`` before sending a request.
    ``path`` may be concrete ('/portfolio/orders/abc') or a template; batch endpoints are
    billed per item (``n_items``).
    """

    def __init__(
        self,
        read: BucketLimit = DEFAULT_READ,
        write: BucketLimit = DEFAULT_WRITE,
        *,
        default_cost: int = DEFAULT_COST,
        endpoint_costs: list[tuple[str, str, int]] | None = None,
        clock: Clock = time.monotonic,
        sleep: Sleep = asyncio.sleep,
    ) -> None:
        self.read = TokenBucket(read, clock, sleep)
        self.write = TokenBucket(write, clock, sleep)
        self.default_cost = default_cost
        self.usage_tier = "unknown"
        self.limits_source = "default"  # 'default' until update_from_limits()
        self._costs: list[tuple[str, re.Pattern[str], int, str]] = []
        self.set_endpoint_costs(list(DOCUMENTED_COSTS) + list(endpoint_costs or []))

    # ------------------------------------------------------------------ configuration
    def set_endpoint_costs(self, costs: list[tuple[str, str, int]]) -> None:
        """Replace the cost table (method, route template, cost). Later entries win."""
        table: dict[tuple[str, str], tuple[str, re.Pattern[str], int, str]] = {}
        for method, template, cost in costs:
            key = (method.upper(), normalize_route(template))
            table[key] = (method.upper(), _route_regex(template), int(cost), key[1])
        self._costs = list(table.values())

    def update_from_limits(self, body: dict[str, Any]) -> None:
        """Apply GET /account/limits (GetAccountApiLimitsResponse)."""
        self.read.update_limit(BucketLimit.from_json(body["read"]))
        self.write.update_limit(BucketLimit.from_json(body["write"]))
        self.usage_tier = str(body.get("usage_tier", "unknown"))
        self.limits_source = "account"

    def update_endpoint_costs(self, body: dict[str, Any]) -> None:
        """Apply GET /account/endpoint_costs (GetAccountEndpointCostsResponse).

        Server-listed costs override the documented defaults; ``default_cost`` replaces 10.
        """
        if "default_cost" in body and body["default_cost"] is not None:
            self.default_cost = int(body["default_cost"])
        server = [
            (str(e["method"]), str(e["path"]), int(e["cost"]))
            for e in body.get("endpoint_costs") or []
        ]
        self.set_endpoint_costs(list(DOCUMENTED_COSTS) + server)

    # ------------------------------------------------------------------ lookup
    @staticmethod
    def bucket_name(method: str) -> str:
        """'read' for GET/HEAD, 'write' otherwise."""
        return "read" if method.upper() in ("GET", "HEAD") else "write"

    def bucket_for(self, method: str) -> TokenBucket:
        return self.read if self.bucket_name(method) == "read" else self.write

    def cost_for(self, method: str, path: str, n_items: int = 1) -> int:
        """Token cost of one request (per-item endpoints multiply by n_items >= 1)."""
        m = method.upper()
        route = normalize_route(path)
        unit = self.default_cost
        for cm, rx, cost, _tmpl in self._costs:
            if cm == m and rx.match(route):
                unit = cost
                break
        if (m, route) in PER_ITEM_ENDPOINTS:
            return unit * max(1, int(n_items))
        return unit

    # ------------------------------------------------------------------ use
    async def acquire(self, method: str, path: str, n_items: int = 1) -> float:
        """Wait for and deduct the request's cost; returns seconds waited."""
        return await self.bucket_for(method).acquire(self.cost_for(method, path, n_items))

    def try_acquire(self, method: str, path: str, n_items: int = 1) -> bool:
        return self.bucket_for(method).try_acquire(self.cost_for(method, path, n_items))

    def on_429(self, method: str) -> None:
        """The server throttled us: drain the corresponding bucket."""
        self.bucket_for(method).drain()

    def max_batch_items(self, method: str, path: str) -> int:
        """Largest batch whose cost fits the bucket capacity (per-item endpoints)."""
        unit = self.cost_for(method, path, 1)
        return max(1, int(self.bucket_for(method).limit.bucket_capacity // unit))
