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
| `llms.txt` doc index | Blocked in the build environment; read on 2026-09-25 from the macOS host (pages used: rate_limits, api_environments, cfbenchmarks/rest-passthrough, websockets/*) | docs.kalshi.com as of 2026-09-25 |
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

## First live contact: macOS host, 2026-09-25

The build environment above never reached an exchange. On 2026-09-25 the stack ran for the
first time against the REAL endpoints from the user's Mac (read-only; Kalshi prod on the same
account as the user's other, live-trading system). Reachable: Kalshi prod + demo
(`external-api.kalshi.com`, `external-api-ws.kalshi.com`, `api.elections.kalshi.com`),
`docs.kalshi.com`, Coinbase, Kraken, Bitstamp, Gemini, Crypto.com, Deribit, OKX, Hyperliquid,
CF Benchmarks docs. Geo-blocked from this US host (disabled in `config/feeds.yaml`, not bugs):
Binance (HTTP 451), Bybit (HTTP 403).

### Credentials and budget
`config/kalshi.yaml` (git-ignored) loads the other system's `prod.env` through `auth.env_file`
(names `KALSHI_PROD_API_KEY_ID` / `KALSHI_PROD_PRIVATE_KEY_PATH`; never copied, values never
printed, `ALLOW_*` never loaded) and sets `rate_limits.account_share: 0.2`. Account limits
observed: tier basic, read 200 tokens/s (cap 600), write 100/s (cap 100); this system uses at
most read 40/s (cap 120), write 20/s (cap 30) and never writes (read-only REST clients).
One Kalshi WebSocket per process (docs name no connection limit).

### Adapter verification status (smoke checks against the live endpoints)

| Adapter | Status | Fix / finding |
|---|---|---|
| Kalshi REST (auth signing, paths, fixed-point, orderbooks, series/events/markets, limits) | VERIFIED | none needed; `smoke_kalshi` 11/11 PASS first try |
| Kalshi `GET /account/endpoint_costs` | VERIFIED | server paths carry `/trade-api/v2`, `:param` and `*wildcard` segments (`GET /trade-api/v2/cfbenchmarks/*endpoint` = 50): `*` segments were matched literally, so the CF passthrough was under-billed at 10; fixed in `dh/kalshi/rate_limit.py` |
| Kalshi WS `orderbook_delta` / `trade` / `ticker` / `market_lifecycle_v2` | VERIFIED | **one sid per channel per connection**: a second `subscribe` for a channel (our 100-market shards) is merged and answered with `{"type":"ok","id","sid","seq","msg":{"market_tickers":[full list]}}` (seq consumed on sequenced channels, none on `ticker`). The client only mapped sids from `subscribed`, so runtime add/delete of markets on shards 2+ was silently not sent until a reconnect (new KXBTC15M/KXBTCD markets would not have been recorded); fixed in `dh/kalshi/ws.py` (`ok` replies map shards to the merged sid). 477 markets on one sid: no error 26 |
| Kalshi WS `cfbenchmarks_value` (1 Hz) | VERIFIED | carries NO `value_usd` / `source_ts_ms`: value and source time only inside the raw upstream `data` JSON string (parsed); `avg_60s_data.window_size` counts only ticks since THIS subscription started (0 on the first tick: the 60 s average warms up after every (re)subscribe); `last_60s_windowed_average_15min` appears in the final minute before each quarter hour |
| Kalshi WS `cfbenchmarks_value_5hz` | VERIFIED | `value_usd` (8 decimals), `source_ts_ms`, `received_at` as specified |
| Kalshi CF history passthrough | VERIFIED | `timespan=HOUR&timestamp=<hour start ISO ms>`, body `{"data":{"serverTime","payload":[{time,value}]}}` at 5 Hz; the parser did not look inside `data.payload` and the back-fill templates were guesses: both fixed (`dh/kalshi/normalize.py`, `BackfillCfg`) |
| `scripts/record.py` Kalshi discovery | FIXED | (1) Kalshi `created` a day of KXBTC15M markets at once (open times hours ahead); each event re-ran discovery (~1 REST call/s for minutes): future markets are now parked until their open time, event-triggered refreshes are spaced by `min_refresh_gap_s`, and markets closing within `pending_lookahead_h` are re-discovered right after they open. (2) `market_horizon_h: 30` excluded a weekly KXBTCD/KXBTC event: now 0 = all open markets. Measured: ~1.2 read tokens/s |
| Kalshi demo WS URL | docs | `wss://external-api-ws.demo.kalshi.co/trade-api/ws/v2` (was an unverified guess); demo not exercised |
| Coinbase Advanced Trade (`level2`, `market_trades`, `heartbeats`) | VERIFIED, no auth needed | `market_trades.side` is the **maker** side (aggressor check 0.04 with it read as taker, 0.81-0.98 after the flip); `sequence_num` is one counter per connection (0 gaps) |
| Kraken v2 (`book` depth 100 + CRC32, `trade`) | VERIFIED | none (checksums 2467/2467) |
| Bitstamp (`diff_order_book`, `live_trades` + REST snapshot) | VERIFIED | REST snapshot recording raised `TypeError` (`make_marker(kind, ...)` also received `kind=`): the book never became valid; fixed with positional-only parameters in `dh/feeds/base.py` |
| Gemini v2 `l2` | VERIFIED | none |
| Crypto.com `book` 50 + `trade` | VERIFIED | none |
| Deribit perp (`book`/`ticker`/`trades` 100ms, index, DVOL) + options tickers | VERIFIED | trade `direction` is the taker side, but 100 ms trade batches arrive AFTER the book update that already reflects them: `smoke_feeds` now also scores the aggressor in exchange-time order (0.94-0.98); research code must order Deribit events by exchange time |
| OKX swap (`books5`, trades, funding, mark, OI, index, liquidations) | VERIFIED | none |
| Hyperliquid (`l2Book`, `trades`, `activeAssetCtx`) | VERIFIED | `l2Book` now defaults to `fast: false` (a snapshot every 3-5 s); the adapter subscribes with `"fast": true` (~0.5 s, top 5 levels) |
| Binance futures, Bybit | not verifiable here | geo-blocked (451 / 403) |
| Paxos, Bullish, LMAX, Kalshi perps | not verified | stubs / disabled |

Every fix above has a unit test built on the REAL captured frame (public market data only):
`tests/kalshi/test_live_frames.py`, `tests/kalshi/fixtures/live_*_2026-09-25.json`,
`tests/feeds/test_feeds_live_frames.py`, `tests/feeds/fixtures/live/`,
`tests/live/test_backfill_live_format.py`.

### macOS specifics
* No `chronyc`/`timedatectl`/`adjtimex`: the recorder's clock sampler uses a query-only
  `sntp` exchange (`src: "sntp"`); this Mac measured +35 ms drifting to +50 ms within an
  hour (local clock behind NTP, +/- ~20 ms). Exchange-latency numbers from this host are biased by that offset.
* No GNU `timeout`; long-running jobs use `caffeinate -i` (idle-sleep prevention).
* A launchd agent for the recorder is provided but not installed: `deploy/launchd/`.
