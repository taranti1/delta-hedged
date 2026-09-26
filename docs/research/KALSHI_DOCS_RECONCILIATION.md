# Kalshi official docs vs the `dh` Kalshi adapter: reconciliation

Date: 2026-09-25. Research only; no code, test or config file was changed.
Sources: https://docs.kalshi.com/llms.txt and the pages it indexes (fetched 19:03-19:12 UTC as
`.md`), the official specs (now vendored in `docs/kalshi_specs/`, see `PROVENANCE.md`), the
API changelog https://docs.kalshi.com/changelog/index.md, the series contract terms PDFs, and a
handful of public unauthenticated GETs (`/series/{t}`, `/markets?series_ticker=..`,
`/historical/cutoff`; saved under `docs/kalshi_specs/samples_2026-09-25/`). No authenticated or
write endpoint was called. The fee schedule (kalshi.com/fee-schedule, the PDF, and
/regulatory/fee-schedule) is behind a Vercel bot checkpoint (HTTP 429); it was not bypassed.

Line numbers are for the working tree at about 19:20 UTC; other agents were editing
`dh/kalshi/rest.py`, `rate_limit.py`, `dh/live/app.py`, `ws.py` and some scripts at the same
time, so symbol names are given too.

**Status update (later on 2026-09-25):**
* Finding 2 FIXED and VERIFIED: T = `close_time` everywhere (`dh/settlement/convention.py`,
  `rest_market_to_spec` refuses a market whose rules-text time differs from close_time;
  `expected_expiration_time` is metadata only). The settlement window turned out to be
  **[T-60 s, T)** (the prints stamped T-60 s .. T-1 s), NOT the (close-60 s, close] of section d:
  that window, which Kalshi's streamed `last_60s_windowed_average_15min` uses, matched 1 of 11
  recorded expirations; [T-60 s, T) rounded to cents matched all of them and the history
  (`docs/research/M1_2_SETTLEMENT_CHECK.md`).
* Finding 3 FIXED: `cf_history_to_ticks` unwraps `{"data": {"payload": [...]}}`; `timestamp` is
  the hour START (one live call: timestamp 20:00:00.000Z returned 18,000 ticks 20:00:00.000 ..
  20:59:59.800); `scripts/download_kalshi_history.py` fetches BRTI hour by hour this way.
* Finding 10 FIXED: the modelled average is rounded to cents before the strike comparison
  (`SettlementSpec.round_decimals`, `MarketSpec.settle_thresholds`); a missing print inside the
  window is counted (`WindowState.n_missing`) and pulls at-risk quotes (`brti_gap_in_window`).
* Finding 15 applied to the history downloader (public GETs unsigned, CF passthrough signed at
  10 % of the read budget).
* Section e: observed on the recorder capture, see `docs/research/M1_2_SETTLEMENT_CHECK.md` s.5.

## 1. Summary of discrepancies

Severity: **breaks-live** = live trading fails or runs with a wrong model; **wrong-assumption**
= a documented behaviour differs from what code or docs assume; **cosmetic** = harmless or an
optimisation.

| # | Severity | Finding (source) | Code location | Recommended change |
|---|---|---|---|---|
| 1 | **breaks-live** | **Exchange sharding.** Since 2026-08-24 12:00 ET new crypto events are created on **exchange shard 2**. Live public data: `KXBTCD`, `KXBTC` and `KXBTC15M` series and markets all return `exchange_index: 2`. Order groups "do not function across exchange instances", and `POST /portfolio/order_groups/create` has `exchange_index` "Defaults to 0" (also trigger/reset/delete/limit). The adapter creates the group without `exchange_index`, so it lands on shard 0 while every quote goes to shard 2 (getting_started/exchange_sharding FAQ; openapi `CreateOrderGroupRequest`) | `dh/live/venue_kalshi.py:774` (`ensure_order_group`), `:823-827` (`_retry_group`); `dh/kalshi/rest.py` `create_order_group` / `reset_` / `trigger_` / `update_order_group_limit` / `delete_order_group` (lines 1007-1064) already accept `exchange_index` but it is never passed | Add `exchange_index` to `MarketSpec` (from `Market.exchange_index`, fallback `Series.exchange_index`). Create the group with `exchange_index=2` (one group per shard in use; refuse to trade a market whose shard has no group) and pass the same `exchange_index` on every group op. Check on demo whether a place that references a group on another shard is rejected ("order group not found") or silently unprotected. Update RUNBOOK 5.2 / 5.3 / 6, which say every order is protected by the group |
| 2 | **breaks-live** | **Settlement time T is `close_time`, not `expected_expiration_time`.** On every BTC market today `expected_expiration_time = close_time + 5 min` (e.g. KXBTCD-26SEP2516: close 20:00:00Z, expected 20:05:00Z). `rules_primary`: the average of the sixty seconds of BRTI **before 4 PM EDT** (= close_time). KXBTCD-26SEP2515 and KXBTC15M-26SEP251500 (close 19:00Z) both settled at `expiration_value` 83950.62. The spec uses `expected_expiration_time` as T, so the modelled settlement window is (T+4 min, T+5 min]. Result: tau is 5 min too long, no print ever counts as fixed before the close, and the fair value is far too diffuse in the final minutes | `dh/kalshi/normalize.py:853-854, 869` (`rest_market_to_spec`); consumers `dh/strategy/mm.py:651-658` and `_band` (~608); `dh/kalshi/metadata.py:88-89` (flag `expiration_differs_from_close` is informational only); research `dh/research/kalshi_data.py:65`, `scripts/download_kalshi_history.py:534` | For KXBTC* use `close_time` as T (better: parse the time out of `rules_primary` or the ticker and require it to equal `close_time`, otherwise block the market). Make `expiration_differs_from_close` non-blocking but log it. Re-run the settlement-convention check and any research that keyed tau on `expected_expiration_ts_ms` |
| 3 | **breaks-live** | **CF Benchmarks passthrough shape.** The passthrough wraps the CF response: `{"data": {"serverTime": .., "payload": ..}}` (cfbenchmarks/rest-passthrough). `cf_history_to_ticks` looks for a list under `data.values/data/history`, so it returns `[]` for this envelope (checked: 0 ticks from `{"data":{"payload":[..]}}`, 2 ticks from the unwrapped body). The fair-value warm-up then gets no history and the model stays not-ready for 1 day; the history downloader writes empty BRTI files. Documented parameters: `timespan=HOUR` and an ISO-8601 `timestamp` (`2026-08-21T14:00:00.000Z` in the example). The code's templates are `"{span_s}s"` / `"{end_ms}"` | `dh/kalshi/normalize.py:699-708` (`cf_history_to_ticks`); `dh/live/config.py:178-179` (`BackfillCfg.timespan/timestamp`); `dh/live/startup.py:14`; `scripts/download_kalshi_history.py` `fetch_brti` | Unwrap first: `if isinstance(body, dict) and isinstance(body.get("data"), dict): body = body["data"]`, then read `payload`. Change the defaults to `timespan: "HOUR"`, `timestamp: "{start_iso}"` or `"{end_iso}"`. One authenticated call is needed to learn whether `timestamp` is the window start or end (CF Benchmarks docs, not fetched here). The cost is 50 read tokens per call (the working tree already added this to `rate_limit.DOCUMENTED_COSTS`) |
| 4 | **breaks-live** | **Collateral per shard.** Collateral checks run inside each matching engine. Programmatic traders must preallocate collateral on the shard before placing orders, and subaccount balances are local to a shard (exchange_sharding "Balance Management"). Nothing in code or RUNBOOK moves funds to shard 2, so orders are rejected for insufficient balance unless the account's balance already sits there (via a manual UI transfer or auto-rebalancing) | none (no `get_balance` call anywhere in `dh/live`); RUNBOOK 1.x / 5.1 | RUNBOOK: fund shard 2 (`POST /portfolio/intra_exchange_instance_transfer` with `source/destination: event_contract` and `destination_exchange_shard: 2`; the UI at kalshi.com/account/exchange-indexes; or a target balance allocation). Start-up: `GET /portfolio/balance?exchange_index=2&subaccount=<n>` must cover the worst case, else refuse to start. For a subaccount: create it with `exchange_index: 2` and fund it with `POST /portfolio/subaccounts/transfer` `exchange_index: 2` |
| 5 | wrong-assumption | `GET /historical/fills` (and `/historical/orders`) now take `subaccount`; omitted means all subaccounts (openapi 3.31.0; changelog 2026-09-24). Code and RUNBOOK say "no subaccount parameter" and refuse the start on rows without a subaccount field | `dh/live/riskstate.py:182-190` (`historical_row_owner`), `:207-210`, `:544` (`derive_day_pnl` calls `iter_historical_fills` without `subaccount`); RUNBOOK.md:47-49 | Pass `subaccount=sub` and drop the "cannot be attributed" refusal (keep the per-row check). Practically dormant: `trades_created_ts` is 2026-07-27, so today's fills never hit the historical path |
| 6 | wrong-assumption | **Balance precision.** Direct-member balances are aligned to $0.0001; the $0.01 example is labelled "FCM-cleared fill" (getting_started/fee_rounding; changelog 2026-05-28). A kalshi.com API account is a Direct account: subaccounts and cancel-all are described as features of "the authenticated Direct member" | `config/fees.yaml:27` (`balance_precision_dollars: "0.01"`); BUILD_PLAN.md:206 treats direct membership as future | Set `"0.0001"` once `scripts/verify_fee_schedule.py` confirms on real fills (`balance_dollars` in GET /portfolio/balance shows the account's precision) |
| 7 | wrong-assumption | **Auto-routing costs latency and rate budget.** Without `exchange_index`, single order writes auto-route by ticker, which "will incur an additional latency cost". They are also billed to the unscoped Write bucket and to every nonzero shard's Write bucket, so they compete with any other system on the account. With explicit `exchange_index=2` a write bills only shard 2's bucket, which carries the full tier budget. Batch writes always bill the unscoped bucket (getting_started/rate_limits, exchange_sharding FAQ) | `dh/live/venue_kalshi.py:462-467` (`place_body` never passes `exchange_index`); `_cancel_item` :594, `_cancel_one` :600, amend/decrease | Pass `spec.exchange_index` on create, cancel, amend, decrease and batch-cancel items. Prefer single writes over batches when the shared unscoped bucket is contended |
| 8 | wrong-assumption | **Maintenance and pauses.** Every Thursday 03:00-05:00 ET there is a trading pause (cancels allowed, no places). A rarer exchange pause blocks cancels too, so kill-file, watchdog and cancel-all all fail during it (getting_started/maintenance_and_pauses). The runner reads `/exchange/status` only at start-up | `dh/live/app.py:319`; RUNBOOK 6 and 8 | Poll `GET /exchange/status` and `/exchange/schedule` (e.g. every 10 s, plus on place rejects) and gate quoting on `trading_active`. Document in RUNBOOK 6/8 that `cancel_order_on_pause` is the only protection during an exchange pause and that sessions may disconnect in the Thursday window |
| 9 | wrong-assumption | `use_yes_price` on `orderbook_delta`: Kalshi will flip the default to `true` and later **remove** the flag, so NO-side levels will then only come in yes-leg pricing (asyncapi `subscribeCommandPayload.params.use_yes_price`, "Migration plan"). The adapter pins `false` and the normalizer assumes no-leg pricing | `config/kalshi.yaml:49`; `dh/kalshi/ws.py:104` (`Subscription.params`); the NO-side conversion in `dh/kalshi/normalize.py` | Migrate the normalizer to yes-leg pricing now (`use_yes_price: true`) with a test on both shapes, before Kalshi announces the flip |
| 10 | wrong-assumption | **Settlement edge rules.** BTC.pdf (KXBTCD, KXBTC): if no data is available at the expiration time the market resolves No. CRYPTO.pdf (KXBTC15M): "no data is available or incomplete" resolves the affected strikes No. KXBTC15M `rules_secondary`: the average is rounded to the nearest 2 decimals; `expiration_value` has 2 decimals on all three series. KXBTC15M is `greater_or_equal` against the previous quarter's `expiration_value`, so ties at the cent are YES | `dh/settlement/window.py` (`gap_policy` carry_forward/skip); `dh/core/market.py` strike semantics compare the unrounded average | Round the modelled average to cents before comparing (tie mass is tiny but exact matters for the 15M "at least" rule). Treat a BRTI outage inside the window as a No-risk scenario (pull quotes) instead of carry-forward |
| 11 | wrong-assumption | **Rate limits are per account**, shared by every key, process and FIX session (rate_limits: "Your tier sets your budget"; REST and FIX drain the same buckets). The config already models this (`rate_limits.account_share`), but RUNBOOK 1.2 justifies the watchdog's second key as protection against a "rate-limited runner key" | RUNBOOK.md:52 | Keep the second key (it helps against revocation) and fix the rationale. Cancel-all costs 2 tokens and 429s carry no penalty, so the watchdog's 1 s retry is enough. If System 1 moves to a subaccount, give runner and watchdog keys restricted to that subaccount: server-side scoping, and a restricted key cannot touch System 2's orders |
| 12 | wrong-assumption | REST portfolio reads lag the exchange; `GET /exchange/user_data_timestamp` gives the time up to which GetBalance, GetOrders, GetFills and GetPositions were last validated (api-reference/exchange/get-user-data-timestamp). Reconciliation does not use it | `dh/live/runner.py` positions/fills reconciliation | Before confirming a position discrepancy, require `user_data_timestamp` >= the last WS fill time |
| 13 | wrong-assumption (verify) | `GET /portfolio/orders/queue_positions` has no `exchange_index` parameter and defaults to subaccount 0. The docs do not say whether it covers shard-2 orders | `dh/live/venue_kalshi.py:1097` (explicit subaccount: OK) | Verify once live that shard-2 resting orders are returned |
| 14 | cosmetic | Spec 3.30.0 -> 3.31.0 (section 3): `liquidity_dollars` removed from Market (unused by `dh`), `max_updated_ts` on GET /markets, Ed25519 API keys (lower signing cost; `auth.py` is RSA-PSS only), OAuth partner tokens, FCM endpoints. Docstrings still say 3.30.0 | `dh/kalshi/__init__.py:1`, `rest.py:1`, `orders.py:3`, `core/units.py:3`, `tests/kalshi/samples.py:1`, DATA_SOURCES.md:8, ENVIRONMENT.md:26-29 | Bump the references; update the ENVIRONMENT provenance table (llms.txt and perps specs are now obtained, see `docs/kalshi_specs/PROVENANCE.md`) |
| 15 | cosmetic | Public GETs are signed, so they draw the account's read bucket. Only "authenticated" requests cost tokens; limits for unauthenticated requests are undocumented | `dh/kalshi/rest.py:315` (`_send`) | Optionally send public market-data GETs (markets, orderbooks, trades, historical, cutoff) unsigned from the recorder and downloader to spare the shared read budget |
| 16 | cosmetic | The demo WS host in code is the "also supported" host; the recommended one is `wss://external-api-ws.demo.kalshi.co/trade-api/ws/v2` (getting_started/api_environments). The WS message limit is 64 MiB and permessage-deflate is optional (changelog 2026-09-24); the client caps frames at 16 MiB | `dh/kalshi/ws.py:66`, `:131` | Switch the demo URL. 16 MiB is fine for this universe |
| 17 | cosmetic | After `close_time` every order operation, including cancels, is rejected with `MARKET_INACTIVE`, and the exchange cancels resting orders shortly after close (getting_started/market_lifecycle) | `dh/kalshi/orders.py` `canonical_reason` (maps it to `market_inactive`) | Treat `market_inactive` on a cancel as terminal-after-close (no re-cancel or STUCK flag) |

Confirmed consistent (no change needed): signing (RSA-PSS, MGF1-SHA256, salt = digest length,
path without query, ms timestamp); REST/WS production hosts; V2 order fields and cost table
(cancel 2, cancel-all 2, batch create 10/item, batch cancel 2/item, GET order 2); explicit
`subaccount` on every call where "omitted" means all subaccounts; `avg_60s_data` /
`last_60s_windowed_average_15min` window semantics; market status handling in `mark_px`;
fee rounding mechanics in `fees.py` (identical to getting_started/fee_rounding); maker rates
0.25x / 0.5x taker for `quadratic_with_maker_fees` / combo; 409 on create = duplicate id;
`fill` / `user_orders` / `market_positions` carry no `seq`, `order_group_updates` does.

## 2. Files saved

`docs/kalshi_specs/`: `openapi.yaml` (replaced: 3.31.0), `asyncapi.yaml` (replaced: 2.0.0,
newer content), `perps_openapi.yaml`, `perps_asyncapi.yaml`, `perps_scm_openapi.yaml`,
`llms.txt`, `contract_terms/BTC.pdf`, `contract_terms/CRYPTO.pdf`, `samples_2026-09-25/*.json`,
`PROVENANCE.md` (URLs, sha256, date). `tests/kalshi/test_normalize.py` and
`test_sequencer.py`, which consume the asyncapi examples, pass against the new asyncapi.yaml
(45 passed).

## 3. Spec diff (pinned -> official)

**openapi 3.30.0 -> 3.31.0** (structural diff):
* New operations (FCM only): `GET /fcm/fills`, `GET /fcm/subtraders`,
  `GET|PUT /fcm/subtraders/blocked_categories`.
* New parameters: `subaccount` on `GET /historical/fills` and `GET /historical/orders`
  (omitted = all subaccounts; restricted keys get only their subaccount); `max_updated_ts` on
  `GET /markets`.
* Removed field: `Market.liquidity_dollars` (always "0.0000" since Feb; removed from
  markets/events/historical responses in the 2026-10-01 release). `dh` does not read it.
* Enums: `ApiKeyScope` + `write::fcm_risk`; `RestingMarginReservation` + `none`; new
  `ApiKeyType` (`rsa`, `ed25519`); `OutcomeSide` factored into a schema.
* Semantics text: `EventData.collateral_return_type` (`MECNET` / `DIRECNET` / empty), OAuth
  `read::compliance_partner` tokens accepted on fills and positions, Ed25519 signing.
* Nothing that `dh/kalshi/*` or `dh/live/*` sends or parses changed shape.

**asyncapi 2.0.0 (same version label, content differs):** `subscribe.params.user_filter`
(communications only); `ticker.dollar_volume` / `dollar_open_interest` are now signed
integers (the `minimum: 0` constraint was dropped); Pyth channel documentation and examples.
No channel or message field used by `dh` changed.

**Present in both but newly relevant** (from the changelog, not the diff): exchange sharding
and `exchange_index` everywhere (Aug 2026), per-shard write buckets, cancel-all endpoint and
its 1-minute tail (2026-08-27), cancel-all cost cut to 2 tokens (2026-09-03), 5 Hz CF channel
(2026-09-03), subscription-ack race fix (2026-09-17), `orders_updated_ts` cutoff two weeks
back and advancing independently (2026-09-24), +20% read/write limits for Premier and above
(2026-09-24).

**Perps specs** (new): REST under `/trade-api/v2/margin/*` on the same hosts (production
"rolling out member by member"); WS `wss://external-api-margin-ws.kalshi.com/trade-api/ws/v2/margin`
with channels `orderbook_delta`, `ticker`, `trade`, `fill`, `user_orders`,
`order_group_updates`; fee tiers `GET /margin/fee_tiers` and `/margin/fee_tier_rates`, funding
`/margin/funding_rates/{historical,estimate}`, `/margin/funding_history`; perps rate limits in
separate buckets (`GET /account/limits/perps`); no batch orders and no queue positions on
margin. Enough to design `dh/feeds/kalshi_perp.py` / `dh/live/venue_hedge.py` against; the
placeholder fee tiers in `config/fees.yaml` should be replaced by `/margin/fee_tier_rates`.

## 4. Q&A

### a. Subaccounts

**Creation and funding.** API only: "Subaccounts are currently an API-only feature", not
supported in the web or mobile app (getting_started/subaccounts).
* `POST /portfolio/subaccounts` creates the next number (1-63; 0 is the primary). It needs a
  Direct account on the Advanced tier or above; Advanced is self-serve via
  `POST /account/api_usage_level/upgrade`. Body `exchange_index` defaults to 0, so a subaccount
  for crypto must be created with `exchange_index: 2` (api-reference/portfolio/create-subaccount;
  getting_started/exchange_sharding).
* Funding: `POST /portfolio/subaccounts/transfer` with `from_subaccount`, `to_subaccount`,
  `amount_cents`, a required `client_transfer_id` (idempotent: a retry returns 409) and
  `exchange_index` (default 0). Transfers net to zero inside the account.
* Across shards: `POST /portfolio/intra_exchange_instance_transfer` with amount in centicents,
  `source/destination_exchange_shard` and `source/destination_subaccount`. It is asynchronous
  and runs in up to three non-atomic steps, so a partial failure can leave funds in the
  primary account on either shard.
* Balances: `GET /portfolio/subaccounts/balances` returns one row per
  (subaccount, exchange_index).
* Subaccount balances are local to a shard; order groups do not work across shards.
* Netting is configured per subaccount (`GET|PUT /portfolio/subaccounts/netting`).

**`subaccount` parameter, and what omitting it means** (official openapi 3.31.0, per-endpoint
parameter text):

| Endpoint | Omitted means |
|---|---|
| GET /portfolio/orders, GET /portfolio/fills, GET /portfolio/settlements | **all subaccounts** |
| DELETE /portfolio/events/orders (cancel-all) | **all subaccounts**, across every shard |
| GET /portfolio/order_groups, GET /portfolio/order_groups/{id} | all subaccounts |
| GET /historical/fills, GET /historical/orders | all subaccounts (new in 3.31.0) |
| GET /portfolio/positions, GET /portfolio/balance | primary (0). Balance covers all shards unless `exchange_index` is given; positions and orders/fills also cover all shards unless `exchange_index` filters |
| DELETE /portfolio/events/orders/{id}, POST .../{id}/amend, POST .../{id}/decrease | 0 |
| PUT /portfolio/order_groups/{id}/trigger, /reset, /limit, DELETE /portfolio/order_groups/{id} | 0 (and `exchange_index` 0) |
| POST /portfolio/order_groups/create, POST /portfolio/events/orders(/batched) | body field; primary when omitted |
| DELETE /portfolio/events/orders/batched | per item `{order_id, subaccount?, exchange_index?, market_ticker?}`; the legacy `ids` form maps to 0 |
| GET /portfolio/orders/queue_positions | 0 |
| GET /historical/positions | 0 |
| GET /portfolio/orders/{id} | no parameter |

With a subaccount-restricted key, an omitted `subaccount` means the key's locked subaccount,
and naming another one is rejected (getting_started/subaccounts). `dh` already sends
`subaccount` explicitly everywhere except the historical fills read (finding 5).

**WebSocket.**
* `fill.msg.subaccount` is an optional integer ("Optional subaccount number for the fill";
  the example shows `3`).
* `market_positions.msg.subaccount` is optional.
* `user_orders.msg.subaccount_number` is optional (0 = primary). Note the different field
  name; `normalize.subaccount_of` handles both.
* The subscribe command has **no subaccount filter**. A full-account key receives every
  subaccount's private messages.
* A subaccount-restricted key (allowed on WS since 2026-07-23) gets `fill`, `user_orders`,
  `market_positions`, `order_group_updates` and `communications` scoped server-side to its
  subaccount. `orderbook_delta` still shows the full book but hides the own-order annotation
  for sibling subaccounts (changelog "Subaccount-restricted API keys can open WebSocket
  sessions").
* This makes a restricted key the robust way to run System 1 on a subaccount, instead of
  relying on an optional field.

### b. Rate limits

* **Scope:** per account (member). Your tier sets your budget. REST and FIX drain the same
  buckets. `/account/limits` returns the authenticated user's buckets and grants. WS
  connections are counted "per user". API keys share the account's budget
  (getting_started/rate_limits; api-reference/account/get-account-api-limits).
* **Unauthenticated requests:** only authenticated requests cost tokens. Public GETs sent
  without a signature do not draw the account's buckets; no IP limit is documented. `dh`
  signs everything (finding 15).
* **Buckets:** Read covers GET and anything not routed to Write. Write covers order place,
  amend and cancel, order groups, the RFQ quote flow and block-trade accepts. Continuous
  refill.
  * Capacity is 3 s of budget for Basic and Advanced read and for write above Basic. It is
    1 s for read above Advanced and for Basic write. The spec's `BucketLimit` text says 2 s
    for write, which contradicts the guide; `dh` reads `bucket_capacity` from the API, so this
    does not matter.
  * A 429 body is `{"error":"too many requests"}` with no `Retry-After` header and no penalty.
* **Tier budgets (tokens/s, read / write):**
  * Basic 200 / 100
  * Advanced 300 / 300
  * Expert 600 / 600
  * Premier 1,200 / 1,200
  * Paragon 2,400 / 2,400
  * Prime 4,800 / 4,800
  * Prestige 12,000 / 9,600

  Expert and above are earned from volume share (30-day grants) or assigned by Kalshi.
* **Token costs:** default 10 (`GET /account/endpoint_costs` lists the others). Documented
  non-default costs:
  * cancel 2
  * cancel-all 2
  * batch create 10 per order and batch cancel 2 per order (the whole batch must fit in the
    bucket at once)
  * `GET /portfolio/orders/{id}` 2
  * CF passthrough 50
  * API-level upgrade 30
  * legacy `/portfolio/orders` mutations 10x V2 (100)
* **Per-shard write buckets:**
  * A single REST write with `exchange_index >= 1` bills that shard's bucket, which carries
    the full tier budget.
  * Auto-routed writes bill the unscoped bucket plus every nonzero shard.
  * Batches and shard-0 writes bill the unscoped bucket.
  * Read is not shard-scoped.
* **WebSocket:**
  * Connections per user are limited by tier, starting at 200 (changelog 2025-09-18).
  * Sanity limits per session: at most 500k market subscriptions and 10k commands/s
    (changelog 2026-06-18).
  * Per-subscription market limit: error 26, limit value not published. Per-subscription
    command rate: error 27. Buffer overflow: error 25.
  * Messages up to 64 MiB. The server pings every 10 s.
  * Subscribing is idempotent; `list_subscriptions` exists.
* **Perps:** separate buckets (`GET /account/limits/perps`).

### c. Cancel-all and order groups

* **Exact wording** (api-reference/orders/cancel-all-orders): "Newly placed orders may also be
  cancelled during the minute after the request."
* **Scope:** all resting event-market orders of the Direct member across every shard, from
  any subaccount if `subaccount` is omitted, else only that subaccount. It costs 2 tokens and
  returns 204. The runner's 60 s hold is the right response to the tail.
* **Cancelling only one group's orders:** yes. None of these endpoints documents a
  follow-on tail like cancel-all's.
  * `PUT /portfolio/order_groups/{id}/trigger` cancels all orders in the group and rejects new
    ones until `reset`.
  * `DELETE /portfolio/order_groups/{id}` cancels all resting orders in the group and removes
    it.
  * `PUT .../limit` with a limit below the current rolling volume triggers the group.

  All take `subaccount` and `exchange_index` query parameters, both defaulting to 0, so they
  need `exchange_index=2` here (finding 1). A trigger is a good soft kill scoped to the bot's
  quotes, and it does not touch other systems' orders on the same subaccount.
* **Semantics** (getting_started/order_groups; openapi):
  * The contracts limit is 1-1,000,000 (fractional via `contracts_limit_fp`). It counts
    matched (filled) contracts over a rolling 15-second window.
  * When the rolling total exceeds the limit (the create endpoint says "hit") the group is
    triggered: all resting orders in it are cancelled and new orders in it are rejected until
    `reset`, which also zeroes the counter.
  * Up to 100,000 groups per user.
  * A group is bound to one subaccount (returned on create) and one exchange shard.
  * `order_group_updates` (WS, sequenced) emits `created`, `triggered`, `reset`, `deleted`
    and `limit_updated` with `ts_ms`.

### d. Settlement of KXBTCD / KXBTC / KXBTC15M

* **Definition:**
  * Contract terms BTC.pdf (KXBTCD and KXBTC `contract_terms_url`): the underlying is the
    simple average of BRTI for the minute (60 s) prior to the stated time; the expiration
    value is the value the source agency (CF Benchmarks) documents.
  * CRYPTO.pdf (KXBTC15M) has the same average over "the 60 seconds prior to <time>".
  * Series `product_metadata`: 60 RTI prices are collected in the last minute and averaged.
  * KXBTC15M `rules_secondary` adds rounding to the nearest 2 decimals. `expiration_value`
    is a 2-decimal string on all three series (e.g. "83950.62").
* **Window boundary:** Kalshi documents (close-60 s, close] (start tick excluded, close tick
  included) for its own `last_60s_windowed_average_15min` field. `dh` uses the same window.
  It is still unverified against `expiration_value`: compare the field's value at count 60
  with the published `expiration_value` for a few quarters.
* **Strikes and payout criteria:**
  * KXBTCD: `strike_type greater`, "above X" strictly, strikes at X.99.
  * KXBTC: range buckets with `greater` / `less` tails; "between" is inclusive on both ends
    per the contract terms.
  * KXBTC15M: `greater_or_equal` ("at least") versus the previous quarter's
    `expiration_value` (e.g. floor 83955.99 = the 14:45 EDT expiration value).
* **No or incomplete data:** resolves No (finding 10).
* **Timing** (public markets, 2026-09-25):
  * KXBTCD and KXBTC: close 19:00:00Z, `settlement_timer_seconds` 60, `settlement_ts`
    19:02:48Z, so determination at about 19:01:48Z. `expected_expiration_time` is close + 5
    min and `latest_expiration_time` close + 7 days.
  * KXBTC15M: timer 1 s, `settlement_ts` 19:00:08Z.
  * The contract terms allow settlement up to the day after expiration, or later if the
    outcome is under review (Rule 7.1).
* **Where the terms are:** `GET /series/{t}` returns `contract_terms_url` (assets.kalshi.com
  /contract_terms/BTC.pdf, CRYPTO.pdf), `contract_url` (the regulatory filing) and
  `settlement_sources`. The rulebook itself is not linked from the API docs.

### e. Valuing a position between close and settlement

* **Statuses:** REST `Market.status` is one of `initialized`, `active`, `inactive`, `closed`,
  `determined`, `disputed`, `amended`, `finalized`. `settled` exists only as a GET /markets
  filter value (it maps to `finalized`) and as a WS lifecycle event (getting_started/
  market_lifecycle).
* **Transitions:**
  * `active` to `closed` at `close_time`, with no WS event.
  * `closed` to `determined` with a `determined` event that carries `result`,
    `settlement_value` (fixed-point dollars) and `determination_ts`.
  * `determined` to `finalized` with a `settled` event, after `settlement_timer_seconds`;
    `settlement_ts` is set then.
* **During `closed`:**
  * Every order operation is rejected (`MARKET_INACTIVE`) and resting orders are cancelled
    shortly after close, so no executable prices exist.
  * The Market object's quote fields then show the empty book. Finalized BTC markets show
    `yes_bid_dollars` "0.0000" and `yes_ask_dollars` "1.0000"; the same is expected while
    `closed`, but that was not observed directly because the window lasts about 2 min.
  * `last_price_dollars` keeps the last trade. Your own fair value, given the partly fixed
    60-print average, is the only meaningful mark.
* **From `determined`:** `result` and `settlement_value_dollars` are populated before payout
  (the dispute window).
* **Portfolio fields:**
  * `MarketPosition.market_exposure_dollars` is the cost of the aggregate position (cost
    basis, not a mark).
  * The docs do not define how `GetBalance.portfolio_value` marks positions.
  * `GET /portfolio/positions` returns only unsettled positions; after payout the row moves
    to `GET /portfolio/settlements` (`revenue` in cents, `value` = YES payout in cents,
    `fee_cost` = total fees paid).
* **Code status:** `riskstate.mark_px` (determined: payout; else bid/ask; else last trade when
  not active) is consistent with this. With finding 2 fixed, the gap between close and
  determination is about 2 min (hourly) or about 7 s (15M).

### f. CF Benchmarks passthrough and WS channels

* **REST** (cfbenchmarks/rest-passthrough):
  * `GET /trade-api/v2/cfbenchmarks/<path>` forwards `<path>` and all query parameters
    except `includeVerification` to `https://www.cfbenchmarks.com/api/v1/<path>`. It is
    authenticated, requires an account entitlement and costs 50 read tokens.
  * History: `/cfbenchmarks/history/values?id=BRTI&timespan=HOUR&timestamp=2026-08-21T14:00:00.000Z`.
    Tick-level, with intra-second granularity on some indices. Other CF parameters (for
    example `maxResolution`) are forwarded unchanged.
  * Response: `{"data": {"serverTime": "...", "payload": ...}}`.
  * Errors: 404 not_found, 429 (upstream rate limit), 503 (upstream auth, server error or
    timeout), 400 with upstream detail.
  * Allowed `timespan` values, whether `timestamp` is the start or the end, and row fields
    are defined by CF Benchmarks' API docs, which were not fetched (out of scope). Sign the
    path `/trade-api/v2/cfbenchmarks/history/values` without the query.
* **`cfbenchmarks_value` (WS):**
  * Subscribe `{"channels":["cfbenchmarks_value"],"index_ids":["BRTI"]}` (or `["all"]`).
    Without `index_ids` nothing flows.
  * `update_subscription` takes `sid` and an `action` of `subscribe_indices`,
    `unsubscribe_indices` or `indexlist`; missing `index_ids` is error 24.
  * About 1 Hz. Duplicate or out-of-order source timestamps are dropped. Sequenced (`seq`).
  * `msg` fields:
    * `index_id`
    * `received_at` (ms)
    * `data` (the raw CF frame as a string; the source timestamp is inside it)
    * `avg_60s_data {value (8 dp), window_size, window_start_ts_ms, window_end_ts_exclusive}`
      over the trailing window [src-60 s, src), prior ticks only
    * `last_60s_windowed_average_15min`, only in the last minute before :00/:15/:30/:45,
      over (close-60 s, close]
* **`cfbenchmarks_value_5hz` (WS):**
  * Same subscribe and update scheme.
  * Covers BRTI, ETHUSD_RTI, SOLUSD_RTI, XRPUSD_RTI and DOGEUSD_RTI.
  * `msg` fields: `index_id`, `value_usd` (8 dp), `source_ts_ms`, `received_at`, `data`.
    No averages.
  * Up to 5 updates/s, sequenced.

### g. Historical data

* **Cutoffs:** `GET /historical/cutoff` (public) returns `market_settled_ts`,
  `trades_created_ts`, `orders_updated_ts` and `market_positions_last_updated_ts`. At
  19:11Z today they were 2026-07-27 for markets, trades and positions, and 2026-09-11 for
  orders (orders now sit two weeks back and advance on their own).
* **Live vs historical:**
  * Live endpoints omit data older than their cutoff: `/markets` and events with nested
    markets, `/markets/trades`, `/portfolio/fills`, `/portfolio/orders` (resting orders are
    always live) and `/portfolio/positions`.
  * Events and series stay live.
  * How far back history goes is not documented; the partition was introduced 2026-02-19.
* **Authentication** (per `security` in the spec):
  * Public: `/historical/cutoff`, `/historical/trades`, `/historical/markets`,
    `/historical/markets/{t}`, `/historical/markets/{t}/candlesticks`, `/markets/trades`.
  * Authenticated: `/historical/fills`, `/historical/orders`, `/historical/positions`.
* **Pagination:**
  * The response carries `cursor`; pass it back and stop on an empty or null cursor.
  * `limit` defaults to 100 and allows 1-1000 (0-1000 on trades and markets).
  * `min_ts`/`max_ts` are Unix timestamps. The spec says only "Unix timestamp" for these
    filters but "(in seconds)" for `min/max_updated_ts`; `dh` uses seconds.
  * `/historical/markets` filters are mutually exclusive.
  * `/markets/trades` and `/historical/trades` include block trades unless
    `is_block_trade=false`.

### h. Fees

* **`fee_type` values:**
  * REST enum: `quadratic`, `quadratic_with_maker_fees`, `quadratic_with_combo_maker_fees`,
    `flat`.
  * The WS `event_fee_update` enum also lists `margin_market_maker_program_fees`; `dh`
    correctly treats unknown types as unsupported.
  * `quadratic` follows the General Trading Fees Table (no maker fee);
    `quadratic_with_maker_fees` adds maker fees; the combo variant uses a 0.5 maker
    multiplier instead of 0.25; `flat` follows the Specific Trading Fees Table (Series
    `fee_type` description).
  * The 0.25 / 0.5 maker multipliers are confirmed by changelog 2026-08-22.
* **BTC series:** public `GET /series/{t}` today returns `fee_type: quadratic` and
  `fee_multiplier: 1` for KXBTCD, KXBTC and KXBTC15M. So there are taker fees only, and
  maker fills should show `fee_cost` 0 unless an event override or waiver applies.
* **Rates:** the 0.07 taker rate and the P(1-P) formula could not be re-verified: the fee
  schedule PDF and page are behind a Vercel checkpoint. `config/fees.yaml` rates remain
  "secondary source"; verify them on real fills.
* **Rounding** (getting_started/fee_rounding), identical to `dh/kalshi/fees.py`:
  * Trade fee = ceil to $0.000001.
  * Rounding fee = (revenue - trade fee) minus that amount floored to the balance precision.
  * A per-order accumulator rebates in precision units, capped so a fill's net fee is never
    negative.
  * Net fee = trade + rounding - rebate.
  * Precision is $0.0001 for direct members and $0.01 for FCM-cleared ones (finding 6).
* **Where fees are reported:**
  * REST `Fill.fee_cost` and WS `fill.msg.fee_cost` (fixed-point dollars)
  * V2 create and amend responses: `average_fee_paid`
  * `Settlement.fee_cost` (total fees paid)
  * `user_orders` `taker_fees_dollars` / `maker_fees_dollars` (6 dp)
* **Other fee notes:** `Market.fee_waiver_expiration_time` exists (null on BTC markets
  today). Scheduled series changes come from `/series/fee_changes` and event overrides from
  `/events/fee_changes` and the WS `event_fee_update`.

### i. Order V2 (`POST /portfolio/events/orders`)

* **Required fields:**
  * `ticker`
  * `side` (`bid` / `ask` on the YES book)
  * `count` (fixed-point string, 0.01 granularity)
  * `price` (fixed-point dollars; valid ticks per `price_ranges`)
  * `time_in_force` (`good_till_canceled` / `immediate_or_cancel` / `fill_or_kill`)
  * `self_trade_prevention_type`: `taker_at_cross` cancels the taker, keeping fills already
    matched; `maker` cancels the resting order and continues matching
* **Optional fields:**
  * `client_order_id`: no format or length is documented; the quick start uses a UUID; a
    duplicate returns 409.
  * `expiration_time`: Unix seconds, only with `good_till_canceled`. Rejected with IOC, and
    rejected if in the past.
  * `post_only`
  * `cancel_order_on_pause`: cancels on a trading or exchange pause.
  * `reduce_only`
  * `subaccount`
  * `order_group_id`
  * `exchange_index`: omitted with a ticker means auto-route; -1 forces auto-route;
    otherwise 0.
* **Response (201):** `order_id`, `client_order_id`, `fill_count`, `remaining_count`,
  `average_fill_price`, `average_fee_paid`, `ts_ms` (matching-engine time).
* **Amend:**
  * `count` = already filled + desired remaining. `ticker`, `side`, `price` and `count` are
    required; `client_order_id` and `updated_client_order_id` are optional.
  * Queue priority is kept only for a size decrease.
  * `subaccount` is a query parameter (default 0).
  * The response's `remaining_count` is the actual resting size.
* **Decrease:** exactly one of `reduce_by` / `reduce_to`; `market_ticker` for auto-routing.
* **Cancel:** `market_ticker` query parameter for auto-routing (an order id alone cannot
  identify the shard); response `reduced_by`, `ts_ms`.
* **Batches:**
  * Maximum size "scales with your tier's write budget" (no fixed number). The whole batch
    must fit in the bucket, is billed per item, always to the unscoped Write bucket.
  * Per-item `error` in the response.
* **Other:**
  * The legacy `/portfolio/orders` mutations are deprecated.
  * Post-only quotes that repeatedly cross may be temporarily rate limited (changelog
    2026-08-22, about RFQ quotes).

### j. RUNBOOK 5.2-5.3, 6, 8 against the docs

* **5.2 "After discovery the exchange order group is created"; 5.3 "Every order ... inside
  the order group"; 6 "Exchange-side protections always in force: order-group auto-cancel":**
  not true while the group is created on shard 0 and the markets are on shard 2
  (finding 1).
* **5.2 step 3 / 8, day P&L:** the historical fills step now can and should pass
  `subaccount` (finding 5). The mark rules match the docs (after close there is no bid or
  ask; `result` and `settlement_value_dollars` are set at determination). Settlement
  `fee_cost` is "total fees paid", so not adding it is right.
* **5.2 / 8 timing:** "restarting in the minutes between a held market's close and its
  determination" is about 2 min for hourly markets and about 7 s for KXBTC15M today. The
  fair-value clock is 5 min off (finding 2).
* **5.3 queue positions every 2 s:** fine for rate. Explicit subaccount OK; shard-2
  coverage unverified (finding 13).
* **5.3 HTTP 409 on create is a definite reject:** consistent (spec 409 "resource already
  exists").
* **5.3 own WS channels have no sequence numbers:** consistent (`fill`, `user_orders` and
  `market_positions` carry no `seq`; `order_group_updates` does).
* **6, cancel-all tail and 60 s hold:** consistent with the docs. Add group trigger as a
  scoped kill that does not cancel other systems' orders (section c).
* **6 / 8, pauses:** nothing covers the weekly Thursday 03:00-05:00 ET trading pause or
  exchange pauses. During an exchange pause even cancels (kill file, watchdog, UI) fail;
  only `cancel_order_on_pause` protects (finding 8).
* **1.2, referenced from 5.1 and 8:** "GET /historical/fills has no subaccount filter" is
  outdated (finding 5). "A revoked/rate-limited runner key" is half-wrong: budgets are per
  account (finding 11). The subaccount WS caveat (optional `subaccount` field) still stands
  for full-account keys; a restricted key removes it (section a).
* **5.1 / 1.x, missing step:** fund exchange shard 2, and create or fund any subaccount on
  shard 2 (finding 4).
