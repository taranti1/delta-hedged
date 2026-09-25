"""Spec-shaped REST payloads (openapi 3.30.0 field names/types) used as offline fixtures.

Values are illustrative (not captured data); field names, types and fixed-point formats
follow the vendored openapi.yaml exactly.
"""

from __future__ import annotations

import copy
from typing import Any

RULES_KXBTCD = (
    "If the simple average of the sixty seconds of CF Benchmarks' Bitcoin Real-Time Index (BRTI) "
    "before 5 PM EDT on Aug 5, 2025 is above 114999.99, then the market resolves to Yes."
)

SERIES_KXBTCD: dict[str, Any] = {
    "ticker": "KXBTCD",
    "frequency": "hourly",
    "title": "Bitcoin price Above/below",
    "category": "Crypto",
    "categories": ["Crypto"],
    "tags": ["BTC"],
    "settlement_sources": [{"name": "CF Benchmarks", "url": "https://www.cfbenchmarks.com/data/indices/BRTI"}],
    "contract_url": "https://kalshi.com/contract",
    "contract_terms_url": "https://kalshi.com/terms",
    "fee_type": "quadratic",
    "fee_multiplier": 1,
    "additional_prohibitions": [],
}

EVENT_KXBTCD: dict[str, Any] = {
    "event_ticker": "KXBTCD-25AUG0517",
    "series_ticker": "KXBTCD",
    "sub_title": "On Aug 5, 2025 at 5pm EDT",
    "title": "Bitcoin price on Aug 5, 2025 at 5pm EDT?",
    "collateral_return_type": "",
    "mutually_exclusive": False,
    "settlement_sources": [{"name": "CF Benchmarks", "url": "https://www.cfbenchmarks.com/data/indices/BRTI"}],
    "strike_date": "2025-08-05T21:00:00Z",
}

MARKET_KXBTCD: dict[str, Any] = {
    "ticker": "KXBTCD-25AUG0517-T114999.99",
    "event_ticker": "KXBTCD-25AUG0517",
    "market_type": "binary",
    "title": "Bitcoin price on Aug 5, 2025?",
    "yes_sub_title": "$115,000 or above",
    "no_sub_title": "$115,000 or above",
    "created_time": "2025-08-04T21:00:00Z",
    "updated_time": "2025-08-05T20:00:00Z",
    "open_time": "2025-08-05T20:00:00Z",
    "close_time": "2025-08-05T21:00:00Z",
    "expected_expiration_time": "2025-08-05T21:00:00Z",
    "latest_expiration_time": "2025-08-12T21:00:00Z",
    "settlement_timer_seconds": 60,
    "status": "active",
    "notional_value_dollars": "1.0000",
    "yes_bid_dollars": "0.4500",
    "yes_ask_dollars": "0.4700",
    "no_bid_dollars": "0.5300",
    "no_ask_dollars": "0.5500",
    "yes_bid_size_fp": "100.00",
    "yes_ask_size_fp": "50.00",
    "last_price_dollars": "0.4600",
    "previous_yes_bid_dollars": "0.4000",
    "previous_yes_ask_dollars": "0.4200",
    "previous_price_dollars": "0.4100",
    "volume_fp": "1234.00",
    "volume_24h_fp": "1234.00",
    "open_interest_fp": "800.00",
    "result": "",
    "can_close_early": False,
    "expiration_value": "",
    "rules_primary": RULES_KXBTCD,
    "rules_secondary": "",
    "price_level_structure": "linear_cent",
    "price_ranges": [{"start": "0.0000", "end": "1.0000", "step": "0.0100"}],
    "strike_type": "greater",
    "floor_strike": 114999.99,
}


def market(**overrides: Any) -> dict[str, Any]:
    m = copy.deepcopy(MARKET_KXBTCD)
    m.update(overrides)
    return m


def settled_market(**overrides: Any) -> dict[str, Any]:
    m = market(
        status="finalized",
        result="yes",
        expiration_value="115123.45",
        settlement_value_dollars="1.0000",
        settlement_ts="2025-08-05T21:05:00Z",
    )
    m.update(overrides)
    return m


ORDERBOOK_BODY: dict[str, Any] = {
    "orderbook_fp": {
        "yes_dollars": [["0.4500", "100.00"], ["0.4400", "25.50"]],
        "no_dollars": [["0.5300", "50.00"], ["0.5000", "10.00"]],
    }
}


def trade_row(trade_id: str, created: str, yes: str = "0.4600", count: str = "3.00", taker: str = "yes") -> dict[str, Any]:
    no = f"{1 - float(yes):.4f}"
    return {
        "trade_id": trade_id,
        "ticker": MARKET_KXBTCD["ticker"],
        "count_fp": count,
        "yes_price_dollars": yes,
        "no_price_dollars": no,
        "taker_side": taker,
        "taker_outcome_side": taker,
        "taker_book_side": "bid" if taker == "yes" else "ask",
        "created_time": created,
        "is_block_trade": False,
    }


FILL_ROW: dict[str, Any] = {
    "fill_id": "f-1",
    "exchange_index": 0,
    "trade_id": "f-1",
    "order_id": "o-1",
    "ticker": MARKET_KXBTCD["ticker"],
    "market_ticker": MARKET_KXBTCD["ticker"],
    "outcome_side": "no",
    "book_side": "ask",
    "count_fp": "2.00",
    "yes_price_dollars": "0.4700",
    "no_price_dollars": "0.5300",
    "is_taker": False,
    "created_time": "2025-08-05T20:40:00Z",
    "fee_cost": "0.000000",
    "ts": 1754426400,
}

ORDER_ROW: dict[str, Any] = {
    "order_id": "o-1",
    "user_id": "u-1",
    "client_order_id": "dhA-1",
    "ticker": MARKET_KXBTCD["ticker"],
    "outcome_side": "yes",
    "book_side": "bid",
    "type": "limit",
    "status": "resting",
    "yes_price_dollars": "0.4500",
    "no_price_dollars": "0.5500",
    "fill_count_fp": "1.00",
    "remaining_count_fp": "9.00",
    "initial_count_fp": "10.00",
    "taker_fees_dollars": "0.000000",
    "maker_fees_dollars": "0.004331",
    "taker_fill_cost_dollars": "0.000000",
    "maker_fill_cost_dollars": "0.450000",
    "created_time": "2025-08-05T20:30:00Z",
    "last_update_time": "2025-08-05T20:31:00.5Z",
}
