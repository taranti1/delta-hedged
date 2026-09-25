"""Hedge venue adapters for the live runner.

M1 runs with hedging DISABLED by design: Experiment 5 (docs/research/05_hedge_policy.md) shows
that at M1 size the optimal hedge is none at every fee tier, and the Kalshi Perps API
specification (perps.openapi.yaml / perps.asyncapi.yaml) could not be obtained in the build
environment (docs/ENVIRONMENT.md). ``DisabledHedgeVenue`` therefore answers every
``PlaceHedge`` with ``HedgeOrderUpdate(status='rejected')`` carrying the reason, so the
strategy's hedge bookkeeping (``MarketMaker.hedge_pending``) never waits for a fill that
cannot come. ``KalshiPerpHedgeVenue`` is the clearly marked skeleton to fill in once the spec
is vendored.
"""

from __future__ import annotations

import time
from abc import ABC, abstractmethod
from collections.abc import Callable, Sequence
from typing import Any

from dh.core.actions import Action, CancelHedge, PlaceHedge
from dh.core.events import Event, HedgeOrderUpdate

Sink = Callable[[Event], None]

DISABLED_REASON = (
    "hedging disabled in M1: no hedge is optimal at M1 size (docs/research/05_hedge_policy.md) and the "
    "Kalshi Perps API spec is not available (docs/ENVIRONMENT.md)"
)


class HedgeVenue(ABC):
    """Executes PlaceHedge / CancelHedge; results come back as HedgeOrderUpdate / HedgeFill
    events through ``sink`` (stamped with the local receive time)."""

    name: str = "hedge"

    def __init__(self, sink: Sink, clock_ns: Callable[[], int] = time.time_ns) -> None:
        self.sink = sink
        self._clock = clock_ns

    @abstractmethod
    def submit(self, actions: Sequence[Action], decision_ns: int) -> None:
        """Dispatch hedge actions (must not block the consumer)."""

    async def start(self) -> None:  # noqa: B027 - optional hook
        """Connect / authenticate (no-op by default)."""

    async def close(self) -> None:  # noqa: B027 - optional hook
        """Cancel resting hedge orders and disconnect (no-op by default)."""


class DisabledHedgeVenue(HedgeVenue):
    """Rejects every hedge order immediately with a clear reason."""

    name = "disabled"

    def __init__(self, sink: Sink, clock_ns: Callable[[], int] = time.time_ns, reason: str = DISABLED_REASON) -> None:
        super().__init__(sink, clock_ns)
        self.reason = reason
        self.rejected: list[str] = []

    def submit(self, actions: Sequence[Action], decision_ns: int) -> None:
        for a in actions:
            if isinstance(a, PlaceHedge):
                self.rejected.append(a.client_order_id)
                self.sink(HedgeOrderUpdate(ts=self._clock(), ts_exch=0, venue=a.venue, client_order_id=a.client_order_id,
                                           status="rejected", filled_btc=0.0, remaining_btc=0.0, reason=self.reason))
            elif isinstance(a, CancelHedge):
                self.sink(HedgeOrderUpdate(ts=self._clock(), ts_exch=0, venue=a.venue, client_order_id=a.client_order_id,
                                           status="rejected", filled_btc=0.0, remaining_btc=0.0,
                                           reason="no such hedge order (hedging disabled)"))


class KalshiPerpHedgeVenue(HedgeVenue):
    """SKELETON -- Kalshi Perps (KXBTCPERP) hedge adapter. NOT IMPLEMENTED.

    TODO(kalshi-perps), once perps.openapi.yaml / perps.asyncapi.yaml are vendored into
    docs/kalshi_specs/ with provenance:
      1. REST client (signing like dh.kalshi.auth; separate rate-limit buckets if any):
         create order (limit post-only at the touch, IOC when urgent), cancel, get order by
         client id, positions, margin/collateral; UnknownOutcome semantics exactly as
         dh.kalshi.rest (never retry a create; reconcile by client_order_id).
      2. Private WS: fills -> HedgeFill(qty_btc, price, fee_usd, is_maker); order updates ->
         HedgeOrderUpdate; connection status -> FeedStatus(stream=RiskCfg.hedge_stream) so the
         risk engine stops quoting sides that increase |D| when the venue is down.
      3. Contract size -> BTC conversion, tick/lot rounding, reduce_only for unwinds,
         funding accounting (PerpState), fee tier from the account.
      4. Paper mode: dh.execution.hedge_sim.HedgeVenueSim fed with the perp book.
      5. Tests with spec example payloads (tests/live/) and a smoke script.
    Until then constructing it raises, so it can never be enabled by accident.
    """

    name = "kalshi_perp"

    def __init__(self, *args: Any, **kwargs: Any) -> None:  # noqa: D107
        raise NotImplementedError("Kalshi Perps hedge adapter: spec not available (see class TODO); "
                                  "use DisabledHedgeVenue")

    def submit(self, actions: Sequence[Action], decision_ns: int) -> None:  # pragma: no cover - unreachable
        raise NotImplementedError


def build_hedge_venue(enabled: bool, venue: str, sink: Sink, clock_ns: Callable[[], int] = time.time_ns) -> HedgeVenue:
    """The hedge adapter for the strategy's HedgeCfg. Only 'disabled' exists today: enabling
    hedging in the strategy config with no adapter is refused loudly."""
    if not enabled:
        return DisabledHedgeVenue(sink, clock_ns)
    if venue == "kalshi_perp":
        return KalshiPerpHedgeVenue(sink, clock_ns)  # raises NotImplementedError
    raise ValueError(f"no hedge adapter for venue {venue!r}")
