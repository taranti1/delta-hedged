"""Normalized event model shared by live trading, recording and replay.

Every event carries:
  ts       int ns since Unix epoch, LOCAL receive time (the event-time clock). Replay orders
           events by (ts, stream rank, per-stream record sequence). Strategy code must use
           ``ev.ts`` as "now"; it must never read the wall clock.
  ts_exch  int ns exchange/server timestamp when the venue supplies one, else 0.

Price/size conventions
  * Kalshi events use exact ints: px in 1e-4 dollars on the YES scale, qty in 0.01 contracts
    (see dh.core.units).  Kalshi's book is published as YES bids and NO bids; adapters keep
    both sides verbatim (``side='yes'|'no'``) and dh.core.book.KalshiBook exposes the YES-book view
    (a NO bid at q is a YES ask at 1-q).
  * External BTC venues use floats (USD price, BTC size) — they never touch the ledger.

Adding a new event type: subclass nothing, just define a frozen slotted dataclass with ``ts``
and ``ts_exch`` first and register it in EVENT_TYPES (used by the store codec).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

YesNo = Literal["yes", "no"]
BookSide = Literal["bid", "ask"]  # Kalshi V2 order/trade side on the YES book
Aggressor = Literal["buy", "sell", ""]  # "" = unknown


# ----------------------------------------------------------------------------- Kalshi market data
@dataclass(frozen=True, slots=True)
class KalshiBookSnapshot:
    ts: int
    ts_exch: int
    ticker: str
    sid: int
    seq: int
    yes_bids: tuple[tuple[int, int], ...]  # ((px, qty), ...) YES bids
    no_bids: tuple[tuple[int, int], ...]  # ((px, qty), ...) NO bids (NO-price scale)


@dataclass(frozen=True, slots=True)
class KalshiBookDelta:
    ts: int
    ts_exch: int
    ticker: str
    sid: int
    seq: int
    side: YesNo  # which bid book changed: YES bids or NO bids
    px: int  # on that side's own price scale (NO bids are on the NO scale)
    delta: int  # signed change in resting qty at px
    own_client_order_id: str = ""  # present only when our order caused the change


@dataclass(frozen=True, slots=True)
class KalshiTrade:
    ts: int
    ts_exch: int
    ticker: str
    trade_id: str
    yes_px: int
    qty: int
    taker_side: YesNo  # taker_outcome_side: 'yes' = taker bought YES (lifted YES asks)
    is_block: bool = False
    sid: int = 0
    seq: int = 0


@dataclass(frozen=True, slots=True)
class KalshiTicker:
    ts: int
    ts_exch: int
    ticker: str
    yes_bid: int
    yes_ask: int
    yes_bid_qty: int
    yes_ask_qty: int
    last_px: int
    volume: int
    open_interest: int


@dataclass(frozen=True, slots=True)
class KalshiMarketLifecycle:
    ts: int
    ts_exch: int
    ticker: str
    event_type: str  # created|activated|deactivated|close_date_updated|determined|settled|...
    close_ts: int = 0  # ns
    determination_ts: int = 0
    settled_ts: int = 0
    result: str = ""
    settlement_value: str = ""
    is_deactivated: bool | None = None
    price_level_structure: str = ""
    price_ranges: tuple[tuple[int, int, int], ...] = ()  # ((start_px, end_px, step_px), ...)
    # Strike/metadata fields (present on `created` / `metadata_updated`; KXBTC15M sets its
    # strike after open, so replay must carry them). Empty/None = not present in the message.
    event_ticker: str = ""
    strike_type: str = ""
    floor_strike: float | None = None
    cap_strike: float | None = None
    open_ts: int = 0  # ns
    expected_expiration_ts: int = 0  # ns


@dataclass(frozen=True, slots=True)
class KalshiFeeUpdate:
    ts: int
    ts_exch: int
    event_ticker: str
    fee_type_override: str | None
    fee_multiplier_override: str | None


# ----------------------------------------------------------------------------- Kalshi own activity
@dataclass(frozen=True, slots=True)
class KalshiFill:
    """Our fill (WS `fill` channel, or simulator)."""

    ts: int
    ts_exch: int
    ticker: str
    trade_id: str
    order_id: str
    client_order_id: str
    book_side: BookSide  # 'bid' = we bought YES, 'ask' = we sold YES (bought NO)
    yes_px: int
    qty: int
    is_taker: bool
    fee_micros: int  # exchange-reported fee_cost (simulator: fee engine)
    post_position: int  # signed YES position after the fill (qty units), if known
    has_post_position: bool = True
    fill_id: str = ""  # REST fill_id when known; dedupe key if WS/REST trade_ids ever differ
    subaccount: int = 0  # Kalshi subaccount (0 = primary); the live runner drops other accounts'


@dataclass(frozen=True, slots=True)
class KalshiOrderUpdate:
    """Order state change (WS `user_orders`, REST ack, or simulator)."""

    ts: int
    ts_exch: int
    ticker: str
    order_id: str
    client_order_id: str
    status: str  # resting|canceled|executed|pending(internal)
    book_side: BookSide
    yes_px: int
    initial_qty: int
    fill_qty: int
    remaining_qty: int
    maker_fees_micros: int = 0
    taker_fees_micros: int = 0
    subaccount: int = 0  # Kalshi subaccount (0 = primary)


@dataclass(frozen=True, slots=True)
class OrderAck:
    """Synchronous REST response to create/amend (or simulator equivalent)."""

    ts: int
    ts_exch: int
    client_order_id: str
    order_id: str
    ticker: str
    fill_qty: int  # immediately filled on entry (post_only orders should be 0)
    remaining_qty: int  # -1 = unknown (e.g. amend responses that omit remaining_count)
    request: str = "create"  # create|amend|decrease


@dataclass(frozen=True, slots=True)
class OrderReject:
    ts: int
    ts_exch: int
    client_order_id: str
    ticker: str
    reason: str
    http_status: int = 0
    request: str = "create"  # create|amend|cancel|decrease


@dataclass(frozen=True, slots=True)
class CancelAck:
    ts: int
    ts_exch: int
    client_order_id: str
    order_id: str
    ticker: str
    canceled_qty: int  # qty removed from the book by this cancel


@dataclass(frozen=True, slots=True)
class KalshiOrderGroupUpdate:
    """WS `order_group_updates`: created|triggered|reset|deleted|limit_updated."""

    ts: int
    ts_exch: int
    order_group_id: str
    event_type: str
    contracts_limit: int = -1  # qty units; -1 = not present


@dataclass(frozen=True, slots=True)
class KalshiPositionSnapshot:
    """Exchange-reported position (WS `market_positions` or REST /portfolio/positions),
    used only for reconciliation against the internal ledger."""

    ts: int
    ts_exch: int
    ticker: str
    position: int  # signed YES qty (0.01 units)
    cost_micros: int = 0
    realized_pnl_micros: int = 0
    fees_paid_micros: int = 0
    source: str = "ws"  # ws|rest
    subaccount: int = 0  # Kalshi subaccount (0 = primary)


# ----------------------------------------------------------------------------- settlement benchmark
@dataclass(frozen=True, slots=True)
class IndexTick:
    """CF Benchmarks index value via Kalshi WS `cfbenchmarks_value[_5hz]` or REST history.

    ts_exch = upstream source publication time (source_ts_ms), the time that defines which
    settlement-window second the print belongs to.
    """

    ts: int
    ts_exch: int
    index_id: str  # namespaced: 'BRTI' (Kalshi CF feed = settlement benchmark); external
    # indices use '<venue>:<name>' (e.g. 'deribit:btc_usd', 'deribit:dvol_btc_usd').
    # Settlement logic MUST filter on index_id == spec.settlement.index_id ('BRTI').
    value: float
    feed: str  # '1hz' | '5hz' | 'rest'
    kalshi_recv_ns: int = 0  # when Kalshi received the upstream frame
    avg60: float | None = None  # trailing [src-60s, src) average from Kalshi (1hz feed)
    avg60_n: int = 0
    qh_avg: float | None = None  # last_60s_windowed_average_15min (final minute of a quarter hour)
    qh_n: int = 0


# ----------------------------------------------------------------------------- external venues
@dataclass(frozen=True, slots=True)
class ExtBookSnapshot:
    ts: int
    ts_exch: int
    venue: str  # coinbase|kraken|bitstamp|gemini|cryptocom|bullish|... or perp venue
    symbol: str  # venue-native symbol, e.g. 'BTC-USD'
    bids: tuple[tuple[float, float], ...]  # ((price, size), ...) best first
    asks: tuple[tuple[float, float], ...]
    seq: int = 0
    depth_limited: bool = True  # True if the venue only publishes top-N levels


@dataclass(frozen=True, slots=True)
class ExtBookDelta:
    """Batch of absolute level updates: size 0 removes the level."""

    ts: int
    ts_exch: int
    venue: str
    symbol: str
    changes: tuple[tuple[str, float, float], ...]  # (('b'|'a', price, new_size), ...)
    seq: int = 0
    prev_seq: int = 0  # for venues that chain sequence numbers (0 = n/a)


@dataclass(frozen=True, slots=True)
class ExtBBO:
    ts: int
    ts_exch: int
    venue: str
    symbol: str
    bid: float
    bid_size: float
    ask: float
    ask_size: float
    seq: int = 0


@dataclass(frozen=True, slots=True)
class ExtTrade:
    ts: int
    ts_exch: int
    venue: str
    symbol: str
    price: float
    size: float
    aggressor: Aggressor
    trade_id: str = ""


@dataclass(frozen=True, slots=True)
class PerpState:
    ts: int
    ts_exch: int
    venue: str
    symbol: str
    mark: float = 0.0  # USD; 0.0 = unknown
    index: float = 0.0  # USD; 0.0 = unknown
    funding_rate: float = 0.0  # per funding interval, as a fraction; 0.0 = unknown
    funding_interval_s: int = 0
    next_funding_ts: int = 0
    open_interest: float = 0.0  # BTC (contracts converted); 0.0 = unknown


@dataclass(frozen=True, slots=True)
class Liquidation:
    ts: int
    ts_exch: int
    venue: str
    symbol: str
    side: Aggressor  # side of the liquidation order ('sell' = long liquidated)
    price: float
    size: float


@dataclass(frozen=True, slots=True)
class OptionQuote:
    ts: int
    ts_exch: int
    venue: str  # 'deribit'
    instrument: str
    expiry_ts: int  # ns
    strike: float
    cp: Literal["C", "P"]
    bid: float  # in underlying units for Deribit (BTC); convert with underlying
    ask: float
    mark_iv: float  # annualized, fraction (0.45 == 45%)
    bid_iv: float
    ask_iv: float
    underlying: float
    mark: float = 0.0  # option mark price (venue units, BTC for Deribit); 0.0 = unknown
    bid_size: float = 0.0
    ask_size: float = 0.0
    delta: float = 0.0


# ----------------------------------------------------------------------------- hedge venue
@dataclass(frozen=True, slots=True)
class HedgeFill:
    ts: int
    ts_exch: int
    venue: str
    symbol: str
    client_order_id: str
    side: Literal["buy", "sell"]
    qty_btc: float
    price: float
    fee_usd: float
    is_maker: bool


@dataclass(frozen=True, slots=True)
class HedgeOrderUpdate:
    ts: int
    ts_exch: int
    venue: str
    client_order_id: str
    status: str  # accepted|rejected|canceled|filled|partially_filled
    filled_btc: float
    remaining_btc: float
    reason: str = ""


# ----------------------------------------------------------------------------- infrastructure
@dataclass(frozen=True, slots=True)
class FeedStatus:
    """Connection / integrity status. Recorded, so replay reproduces outages and gaps."""

    ts: int
    ts_exch: int
    stream: str  # e.g. 'kalshi.ws', 'coinbase.ws', 'kalshi.book:KXBTCD-...'
    status: Literal["connected", "disconnected", "gap", "stale", "resynced", "resumed", "error"]
    detail: str = ""


@dataclass(frozen=True, slots=True)
class Timer:
    """Deterministic periodic tick injected by the runner (never recorded)."""

    ts: int
    ts_exch: int = 0
    period_ns: int = 0


@dataclass(frozen=True, slots=True)
class Settlement:
    """Market determination (from lifecycle or REST) with the benchmark value used."""

    ts: int
    ts_exch: int
    ticker: str
    result: str  # 'yes'|'no'|...
    expiration_value: float | None = None
    settlement_px: int = 0  # YES payout in px units (10_000 for YES)


@dataclass(frozen=True, slots=True)
class RiskStateSeed:
    """Risk state carried over from earlier sessions of the same UTC day (audit live C1).

    Pushed once by the live runner at start-up, before any Timer, from its persisted state and
    the day's REST fills/settlements; recorded, so replay reproduces it. Without it a restart
    would silently reset the daily-loss halt and any pause.

    day_pnl_usd: net P&L of the UTC day starting at ``day_start_ns`` before this session (fees,
    settlements and events excluded from this session included; negative = loss). It counts
    toward the daily-loss limit on that UTC day only. ``halted`` carries over a Halt(all);
    ``pause_until_ns`` carries over a pause (e.g. the settlement-loss pause).
    """

    ts: int
    ts_exch: int
    day_start_ns: int
    day_pnl_usd: float = 0.0
    halted: bool = False
    halt_reason: str = ""
    pause_until_ns: int = 0


EVENT_TYPES: dict[str, type] = {
    cls.__name__: cls
    for cls in (
        KalshiBookSnapshot,
        KalshiBookDelta,
        KalshiTrade,
        KalshiTicker,
        KalshiMarketLifecycle,
        KalshiFeeUpdate,
        KalshiFill,
        KalshiOrderUpdate,
        OrderAck,
        OrderReject,
        CancelAck,
        KalshiOrderGroupUpdate,
        KalshiPositionSnapshot,
        IndexTick,
        ExtBookSnapshot,
        ExtBookDelta,
        ExtBBO,
        ExtTrade,
        PerpState,
        Liquidation,
        OptionQuote,
        HedgeFill,
        HedgeOrderUpdate,
        FeedStatus,
        Timer,
        Settlement,
        RiskStateSeed,
    )
}

Event = (
    KalshiBookSnapshot
    | KalshiBookDelta
    | KalshiTrade
    | KalshiTicker
    | KalshiMarketLifecycle
    | KalshiFeeUpdate
    | KalshiFill
    | KalshiOrderUpdate
    | OrderAck
    | OrderReject
    | CancelAck
    | KalshiOrderGroupUpdate
    | KalshiPositionSnapshot
    | IndexTick
    | ExtBookSnapshot
    | ExtBookDelta
    | ExtBBO
    | ExtTrade
    | PerpState
    | Liquidation
    | OptionQuote
    | HedgeFill
    | HedgeOrderUpdate
    | FeedStatus
    | Timer
    | Settlement
    | RiskStateSeed
)

__all__ = [name for name in EVENT_TYPES] + ["Event", "EVENT_TYPES", "YesNo", "BookSide", "Aggressor", "field"]
