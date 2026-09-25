# Build environment, data access and reference provenance

## Network access in the build environment (2026-09-25)

The cloud container this system was built in could reach package registries (PyPI, npm,
crates, Go proxy) and `raw.githubusercontent.com`, but its egress policy **denied** every
market-data and Kalshi host, including:

`docs.kalshi.com`, `kalshi.com`, `api.elections.kalshi.com`, `external-api.kalshi.com`,
`demo-api.kalshi.co`, `api.exchange.coinbase.com`, `advanced-trade-ws.coinbase.com`,
`ws-feed.exchange.coinbase.com`, `api.kraken.com`, `ws.kraken.com`, `www.bitstamp.net`,
`api.gemini.com`, `api.crypto.com`, `www.deribit.com`, `api.binance.com`, `fapi.binance.com`,
`api.bybit.com`, `www.okx.com`, `api.hyperliquid.xyz`, `www.cfbenchmarks.com`, `huggingface.co`.

Consequences:
* No live Kalshi, exchange, or options data could be captured during the build. The collectors
  and adapters are written to the official specs and unit-tested against spec-shaped fixtures,
  but **each must pass `scripts/smoke_*.py` against the live endpoint before being trusted**.
* To run collection from Claude Code on the web, add those hosts to the environment's allowed
  domains (Environment settings -> Network access), or run the collectors on your own machine/VPS.

## Kalshi reference provenance (authoritative sources actually used)

| Required reference | How it was obtained | Version / hash |
|---|---|---|
| `openapi.yaml` (Predictions REST) | Copy dated 2026-09-24 from `taranti1/trading-strategy/Kalshi/specs/`, cross-checked endpoint-for-endpoint against Kalshi's official generated SDK `kalshi-typescript@3.30.0` (npm, published 2026-09-15 by Kalshi) — identical path set | 3.30.0, sha256 `75098a17...9e3199` -> `docs/kalshi_specs/openapi.yaml` |
| `asyncapi.yaml` (Predictions WebSocket) | Same repo copy (2026-09-24) | 2.0.0, sha256 `fdeb90ed...cc945ba` -> `docs/kalshi_specs/asyncapi.yaml` |
| `llms.txt` doc index | Blocked (`docs.kalshi.com`) | — |
| `perps.openapi.yaml`, `perps.asyncapi.yaml` | Blocked; no official SDK found on npm/PyPI | — (perp hedge adapter is interface-only until obtained) |
| Fee schedule (regulatory page + PDF, "Fee Schedule for July 2026 - 7.7.26 Update") | Blocked. Formula/rates cross-checked from: series `fee_type` enum in the spec (`quadratic`, `quadratic_with_maker_fees`, `quadratic_with_combo_maker_fees`, `flat`), the fee-rounding implementation in `taranti1/trading-strategy` (reconciled against live fills there), and public secondary sources | Rates live in `config/fees.yaml` with provenance; runtime reads fee type/multiplier per series and event and reconciles every fill's `fee_cost` |

Key mechanics confirmed from the specs (used throughout the code):
* REST base `https://external-api.kalshi.com/trade-api/v2` (also `api.elections.kalshi.com`);
  demo `https://external-api.demo.kalshi.co/trade-api/v2`. Auth headers `KALSHI-ACCESS-KEY`,
  `KALSHI-ACCESS-TIMESTAMP` (ms), `KALSHI-ACCESS-SIGNATURE` = base64 RSA-PSS(SHA256, MGF1-SHA256,
  salt = digest length) over `timestamp + METHOD + path` (path without query string).
* WebSocket `wss://external-api-ws.kalshi.com/trade-api/ws/v2`, authenticated at handshake;
  server pings every 10 s. Channels: `orderbook_delta` (snapshot then deltas, `seq` per sid),
  `trade`, `ticker`, `fill`, `user_orders`, `market_positions`, `market_lifecycle_v2`
  (includes `event_fee_update`), `order_group_updates`, **`cfbenchmarks_value`** (BRTI once per
  second, with trailing-60s and quarter-hour final-minute averages) and
  **`cfbenchmarks_value_5hz`** (BRTI up to 5 Hz). Historical BRTI (intra-second) via the
  CF Benchmarks REST passthrough `/cfbenchmarks/history/values?id=BRTI&timespan=...`.
* Orders V2: `POST /portfolio/events/orders` (`side` bid|ask on the YES book, `price` dollars,
  `count` fp, `post_only`, `time_in_force`, `expiration_time`, `order_group_id`,
  `cancel_order_on_pause`, `self_trade_prevention_type`), amend/decrease/cancel/batch variants,
  `GET /portfolio/orders/queue_positions` (our exact queue position), order groups with a
  rolling-15-second contracts limit and auto-cancel.
* Prices: fixed-point dollars; valid ticks per market from `price_level_structure` /
  `price_ranges` (1c, 0.1c, 0.01c steps). Counts: fixed-point with 2 decimals.
* Rate limits: token buckets (read/write) per usage tier from `GET /account/limits`.

## Real historical data used in this build

* Bitstamp BTC/USD 1-minute OHLCV, 2025-01-07 -> 2026-09-25 (901,557 rows), from the public
  `ff137/bitstamp-btcusd-minute-data` GitHub dataset (daily-updated). Bitstamp is a BRTI
  constituent; 1-minute closes are a proxy for BRTI at minute resolution (no tick data).
