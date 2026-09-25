"""Actions emitted by the deterministic Strategy.

The Strategy never performs I/O. It returns actions; a venue adapter executes them:
  * live:     dh.live (Kalshi REST V2 order endpoints, hedge-venue client)
  * backtest: dh.execution.exchange_sim (queue-aware matching with latency)
Results come back as events (OrderAck / OrderReject / CancelAck / KalshiFill / ...).

client_order_id values are generated deterministically by the Strategy (run prefix + counter)
so that a replay of the same inputs produces byte-identical action streams.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

from dh.core.events import BookSide


@dataclass(frozen=True, slots=True)
class PlaceOrder:
    client_order_id: str
    ticker: str
    book_side: BookSide  # 'bid' = buy YES, 'ask' = sell YES (== buy NO at 1-px)
    px: int  # YES price, 1e-4 dollars, must be on the market's tick grid
    qty: int  # 0.01-contract units
    post_only: bool = True
    expiration_ts: int = 0  # ns; 0 = none (GTC). Converted to seconds for the API.
    order_group_id: str = ""
    cancel_on_pause: bool = True
    reason: str = ""
    time_in_force: str = "gtc"  # gtc | ioc | fok (maker quotes are always gtc + post_only)


@dataclass(frozen=True, slots=True)
class CancelOrder:
    client_order_id: str
    ticker: str
    order_id: str = ""  # exchange id if known (required by REST cancel)
    reason: str = ""


@dataclass(frozen=True, slots=True)
class AmendOrder:
    """Price and/or total-count amend (REST: count = already filled + desired remaining).

    Amend semantics for queue priority must be verified empirically (Experiment 4); the
    default policy uses cancel/replace unless amend is shown to preserve priority for
    size decreases.
    """

    client_order_id: str
    new_client_order_id: str
    ticker: str
    order_id: str
    book_side: BookSide
    px: int
    total_qty: int
    reason: str = ""


@dataclass(frozen=True, slots=True)
class DecreaseOrder:
    client_order_id: str
    ticker: str
    order_id: str
    reduce_to: int  # remaining qty after decrease
    reason: str = ""


@dataclass(frozen=True, slots=True)
class CancelAll:
    reason: str
    tickers: tuple[str, ...] = ()  # empty = everything


@dataclass(frozen=True, slots=True)
class CreateOrderGroup:
    """Exchange-side fill-burst breaker: auto-cancels the group's orders once more than
    contracts_limit contracts match within a rolling 15 seconds."""

    order_group_id: str
    contracts_limit: int  # qty units (0.01 contracts)
    reason: str = ""


@dataclass(frozen=True, slots=True)
class ResetOrderGroup:
    order_group_id: str
    reason: str = ""


@dataclass(frozen=True, slots=True)
class UpdateOrderGroupLimit:
    order_group_id: str
    contracts_limit: int
    reason: str = ""


@dataclass(frozen=True, slots=True)
class DeleteOrderGroup:
    order_group_id: str
    reason: str = ""


@dataclass(frozen=True, slots=True)
class PlaceHedge:
    client_order_id: str
    venue: str
    symbol: str
    side: Literal["buy", "sell"]
    qty_btc: float
    order_type: Literal["limit", "market"] = "limit"
    limit_px: float = 0.0
    post_only: bool = False
    reduce_only: bool = False
    reason: str = ""


@dataclass(frozen=True, slots=True)
class CancelHedge:
    client_order_id: str
    venue: str
    reason: str = ""


@dataclass(frozen=True, slots=True)
class Halt:
    """Kill switch. scope='quoting' cancels all Kalshi orders and stops new quotes;
    scope='all' also stops hedging (except risk-reducing flattening if configured)."""

    reason: str
    scope: Literal["quoting", "all"] = "quoting"
    until_ts: int = 0  # ns; 0 = until manual reset


@dataclass(frozen=True, slots=True)
class Resume:
    reason: str


@dataclass(frozen=True, slots=True)
class Log:
    """Structured decision record (candidate quotes, EV terms, hedge decisions...).

    Logged in live and backtest identically; research uses these to evaluate hypothetical
    quotes that were considered but not sent.
    """

    kind: str
    payload: dict[str, Any] = field(default_factory=dict)


Action = (
    PlaceOrder
    | CancelOrder
    | AmendOrder
    | DecreaseOrder
    | CancelAll
    | CreateOrderGroup
    | ResetOrderGroup
    | UpdateOrderGroupLimit
    | DeleteOrderGroup
    | PlaceHedge
    | CancelHedge
    | Halt
    | Resume
    | Log
)
