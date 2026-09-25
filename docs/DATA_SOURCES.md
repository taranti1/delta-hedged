# B. Data-source map (free sources only)

Every source is captured raw (exact frames) with a local receive timestamp by `scripts/record.py`
into the append-only store, and normalized on replay (`dh.store.replay`). "Verify live" = adapter
written to documented formats but not yet exercised against the live endpoint from the build
environment (egress blocked); run `scripts/smoke_feeds.py` / `scripts/smoke_kalshi.py` first.

## 1. Kalshi (authoritative: `docs/kalshi_specs/openapi.yaml` 3.30.0, `asyncapi.yaml` 2.0.0)

| Channel / endpoint | Fields used | Rate | Purpose |
|---|---|---|---|
| WS `orderbook_delta` (snapshot then deltas, `seq` per sid) | `yes_dollars_fp`/`no_dollars_fp` levels, delta `price_dollars`, `delta_fp`, `side`, `ts_ms`, own `client_order_id` | every book change | L2 reconstruction, queue tracking, microstructure features |
| WS `trade` | `yes_price_dollars`, `count_fp`, `taker_outcome_side`/`taker_book_side`, `ts_ms`, `trade_id` | every match | taker flow, aggressor side, fill-model calibration, Experiment 0/3 |
| WS `ticker` | best bid/ask and sizes, last, volume, OI | on change | cross-check of book, OI |
| WS `market_lifecycle_v2` (+ `event_fee_update`) | open/close/determination/settlement, `settlement_value`, deactivation, `price_level_structure`, `price_ranges`, fee overrides | on change | market registry, halts, fee changes, tick grid |
| WS **`cfbenchmarks_value`** (index_ids `["BRTI"]`) | tick value, source ts, Kalshi receive ts, `avg_60s_data`, `last_60s_windowed_average_15min` | ~1 Hz | **the settlement benchmark itself**, plus Kalshi's own running window averages |
| WS **`cfbenchmarks_value_5hz`** (`BRTI`) | `value_usd`, `source_ts_ms`, `received_at` | up to 5 Hz | lowest-latency benchmark nowcast |
| WS `fill`, `user_orders`, `market_positions`, `order_group_updates` | own fills (price, count, fee_cost, post_position), order states, positions, group triggers | on change | order manager, reconciliation, fee reconciliation |
| REST `GET /markets`, `/markets/{t}`, `/series/{s}`, `/events/{e}`, `/series/fee_changes`, `/events/fee_changes` | strikes, strike_type, times, price ranges, fee_type/multiplier, overrides | discovery every 5 min + lifecycle-driven | registry and fee engine |
| REST `GET /markets/{t}/orderbook`, `/markets/orderbooks` | full book | on (re)connect / gap | resynchronization and WS-book verification |
| REST `GET /portfolio/orders/queue_positions` | our queue position per order | 1-2 s for resting orders (read budget) | **ground truth for the queue estimator** |
| REST `/historical/{markets,trades,candlesticks}`, `/markets/trades` | settled results, `expiration_value`, full trade tape | batch | Experiment 0, fill-model priors, settlement-convention check |
| REST `/cfbenchmarks/history/values?id=BRTI&timespan=...` (passthrough) | historical BRTI incl. intra-second | batch | settlement check and benchmark research on past windows |
| REST `/incentive_programs` | program type, market, period reward, target size, discount | daily | incentive economics (only when earned) |
| REST `/account/limits`, `/account/endpoint_costs`, `/exchange/status`, `/exchange/schedule` | token buckets, costs, trading status | startup + periodic | rate limiter, halts |

## 2. Settlement-benchmark constituents (BRTI proxies and nowcast inputs)

| Venue | Free WS channel(s) | Fields | Purpose |
|---|---|---|---|
| Coinbase (Advanced Trade) | `level2`, `market_trades`, `heartbeats` BTC-USD | L2 updates (absolute sizes), trades w/ side, sequence | largest USD venue; nowcast, lead-lag |
| Kraken (WS v2) | `book` depth 100 + CRC32 checksum, `trade` BTC/USD | L2 + checksum, trades | constituent; checksum-validated book |
| Bitstamp | `diff_order_book_btcusd`, `live_trades_btcusd` + REST snapshot | L2 diffs with microtimestamps, trades | constituent |
| Gemini (v2 marketdata) | `l2` BTCUSD | L2 + trades | constituent |
| Crypto.com Exchange | `book.BTC_USD`, `trade.BTC_USD` | L2 + trades | constituent |
| Others (Bullish, LMAX, itBit/Paxos) | stubs; availability of free feeds to confirm | | completeness of the BRTI replica |

## 3. Perpetuals and derivatives (hedge venue candidates and signals)

| Venue | Channels | Fields | Purpose / constraints |
|---|---|---|---|
| **Kalshi perps (KXBTCPERP)** | perps REST/WS (spec not obtainable in build env) | book, trades, mark (BRTI-referenced), funding (8h) | preferred US-legal, benchmark-aligned hedge; fees 5.0->0.6 bp maker / 12->2.6 bp taker |
| Deribit | `book`/`ticker`/`trades` BTC-PERPETUAL; option tickers (nearest expiries) | mark IV, bid/ask IV, underlying, DVOL | options-implied vol features (test, don't assume) |
| Binance USD-M futures | `bookTicker`, `aggTrade`, `markPrice@1s`, `forceOrder` BTCUSDT | BBO, trades, mark/index/funding, liquidations | lead-lag signal (data only; geo-restricted for trading) |
| Bybit linear | `orderbook.50`, `publicTrade`, `tickers`, liquidations | L2, trades, funding, OI | signal |
| OKX swap | `books5`/`bbo-tbt`, `trades`, funding, mark | BBO/L5, trades, funding | signal |
| Hyperliquid | `l2Book`, `trades` BTC | L2, trades | signal |

## 4. Historical data used offline in this build

| Dataset | Access | Use |
|---|---|---|
| Bitstamp BTC/USD 1-minute OHLCV 2012-2026 (`ff137/bitstamp-btcusd-minute-data`) | raw.githubusercontent.com (reachable) | fair-value calibration, vol seasonality, hedge-policy study |

## 5. Clock and integrity

* Host clock disciplined by chrony (PTP if available); offset sampled every 60 s into stream `clock`.
* Every frame stores local receive ns; venue timestamps are kept separately. Latency per venue =
  receive - venue ts (monitored; alarms on drift).
* Sequence checks: Kalshi per-sid `seq`; Coinbase `sequence_num`; Kraken CRC32; Binance `u/pu`;
  Bybit `u/seq`. A gap emits `FeedStatus(gap)`, invalidates the book and triggers a resnapshot;
  the strategy cancels quotes that depend on the invalid book.
* Duplicates are dropped by (venue, sequence/trade_id); reconnects are logged as FeedStatus events,
  so replay reproduces outages exactly.
