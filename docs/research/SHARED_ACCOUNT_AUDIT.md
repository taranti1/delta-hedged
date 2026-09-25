# Shared Kalshi account audit: delta-hedged (System 1) next to trading-strategy (System 2)

Date: 2026-09-25. Read-only code audit; no Kalshi calls were made, no System 2 file was touched.
System 1 = `/Users/thomast/Desktop/delta-hedged` (package `dh`, KXBTC* market maker, not live).
System 2 = `/Users/thomast/Desktop/trading-strategy/Kalshi` (live now; paths below are relative
to `Kalshi/`). `trading-strategy-reentry` is the same commit (349e1b5); its uncommitted diffs in
`two_leg_launcher.py`, `step4_pilot.py`, `two_leg_campaign.py` only regroup settlement accounting
and touch no subaccount or account-read call, so every finding applies to it too.

Running now (from `ps`, cwd `trading-strategy/Kalshi`):
`scripts/short_duration_screener.py --profile live --out-dir data/short_duration_screener/live-shared-20260925b`
and `python -m kalshi_m1.experiments.two_leg_launcher watch ... --allowlist data/two_leg_live/allowlist.json
--auto-launch --allow-live-orders --fast-regime`. Each admitted manifest is run by a child
`two_leg_launcher launch --manifest ... --allow-live-orders` (`two_leg_launcher.py:1024-1034`,
`:1060-1110`), which runs `run_step4_pilot` (`two_leg_launcher.py:2182`) with the two-leg config
from `two_leg.py:493-560`.

Kalshi spec facts used below (`docs/kalshi_specs/openapi.yaml`):

| Endpoint | `subaccount` omitted means | spec line |
|---|---|---|
| GET /portfolio/orders, DELETE /portfolio/events/orders (cancel-all), GET /portfolio/fills, GET /portfolio/settlements, GET /portfolio/order_groups | ALL subaccounts (`SubaccountQuery`, 4478-4483) | 913, 1076, 1972, 1858, 1311 |
| GET /portfolio/balance, GET /portfolio/positions, DELETE /portfolio/events/orders/{id}, order-group writes, queue_positions | primary = 0 (`SubaccountQueryDefaultPrimary`, 4485-4490) | 1532, 1824, 1192, 1396-1495, 983 |
| GET /historical/fills | no subaccount parameter at all | 4057-4073 |
| Cancel-all | "matching orders may come from any subaccount" if omitted; only that subaccount if given; orders placed in the next minute may also be cancelled | 1067 |
| API key `subaccount` | "restricts the API key to a single sub-account ... may only read and trade on that sub-account; it cannot act on other sub-accounts, transfer funds ..., or create sub-accounts" | 4829-4834, 4873-4877 |
| Create subaccount | Advanced API tier and above; numbered 1-63 | 1652 |
| Rate limits | token buckets per user ("authenticated user's ... token-bucket limits"); System 1's own note: per account, shared by every key/process | 2821; `dh/kalshi/rate_limit.py:22-27` |

---------------------------------------------------------------------------------------------
## A. System 2's footprint on the account

### A1. Universe: can it touch KXBTC / KXBTCD / KXBTC15M?

**In practice no; by code, nothing forbids it.** There is no series, category, prefix or crypto
denylist anywhere in the screener, sweep, watch, preflight, policy, campaign or executor (grep for
KXBTC/KXETH/crypto finds no gating code). Crypto is excluded only by the generic basket rules:

* Screener `live` profile (`scripts/short_duration_screener.py:2665-2683`): close within 16 days,
  first expiration 1.5-48 h, 2-3 legs per event, maker-fee series skipped; no series filter.
  It lists ALL markets (`GET /markets`, no `series_ticker`, `:858-865`), groups by event, drops
  events with > 3 listed markets (`:886-922`), then fetches events and requires
  `verify_no_me_event` (`src/kalshi_m0/markets/event_rules.py:36-78`: `mutually_exclusive` true,
  supported collateral-return type, >= 2 binary open markets).
* Watch/preflight: 2-3 legs, 1.5-48 h, price tiers, recent-trade flow (`two_leg_screen.py:366-373,
  426-431, 600-621`; `two_leg_launcher.py:223-224, 246, 267-268`). No series gate.
* Allowlist `data/two_leg_live/allowlist.json` shape
  `{"events": [{"event", "start_time", "start_source", "auto_launch"?}]}`
  (`two_leg_watch.py:269-355`); current content: one stale row (KXNHLGAME-26SEP22FLACAR). It is
  **advisory, not a gate**: an unlisted event resolves from its ticker (`two_leg_watch.py:242-244`)
  and under the fast rule may auto-launch unless a row says `"auto_launch": false`
  (`two_leg_watch.py:179-192`). The current watch journal shows 4 auto-launches, none allowlisted.
* Executor: one event per session, orders only on that event's markets, MECNET re-verified
  (`two_leg.py:588-591`, `step4_pilot.py:2992`, `basket_executor.py:3395-3399`).

Why KXBTC* never qualifies today: KXBTC15M events have one binary market and expire in < 1.5 h;
KXBTCD strike ladders are not mutually exclusive; KXBTC range events have far more than 3
buckets. The screener log shows KXBTC15M/KXETH15M/KXBTCMAX100/... events fetched and none
reaching the universe; no crypto ticker appears in `data/two_leg_live/` (manifests, `orders.jsonl`,
`campaign.json`) or the screener universe; every manifest in `prepared/` is sports/esports.
**Residual:** a future 2-3 leg mutually exclusive crypto event expiring in 1.5-48 h (e.g. a
2-coin "crypto lead" event; a 5-leg KXCRYPTOLEAD15M is already archived, `DECISIONS.md:1455-1456`)
could be auto-launched without any allowlist entry. KXBTC/KXBTCD/KXBTC15M themselves cannot.
The screener's global public trade tape does contain crypto trades (read-only data).

### A2. Subaccount: every portfolio call site

All live clients are `LiveProdTradingClient` (subclass of `LiveDemoTradingClient`, field
`subaccount: int = 0`, `execution/client.py:470`), always constructed with `subaccount=0`
(`two_leg_launcher.py:2011-2013`, `step4_pilot.py:1013-1020`). Scope string
`key:<suffix>/subaccount:0` (`risk/netting_provenance.py:98-101`). No `/historical/*` call exists.
No batch cancel and no order groups are used.

| Call | Where | `subaccount` | Notes |
|---|---|---|---|
| POST /portfolio/events/orders | `execution/client.py:934-947`, body `order_bodies.py:144`, taker `:186` | explicit 0 (body) | |
| DELETE /portfolio/events/orders/{id} | `execution/client.py:949-972`, params `order_bodies.py:195-212` | explicit 0 | routed by ticker/shard |
| DELETE /portfolio/events/orders (cancel-all) | `execution/client.py:1070-1072`, `prod_client.py:172-176` | explicit 0 | fallback list+cancel also explicit (`client.py:1081-1084`, `prod_client.py:183-186`) |
| GET /portfolio/orders (list) | `execution/client.py:610-631` | explicit 0 | used by recovery, drift check, preflight, netting |
| GET /portfolio/orders (auth check) | `prod_client.py:280-283` | explicit 0 | |
| GET /portfolio/orders/{id}, /{id}/queue_position | `client.py:768, 1112, 1121`; `settlement_followup.py:119` | none (by id) | only its own ids |
| GET /portfolio/fills | `execution/client.py:633-657` | explicit 0 | |
| GET /portfolio/settlements | `execution/client.py:659-672` | explicit 0 | |
| GET /portfolio/positions | `execution/client.py:593-609` | explicit 0 | |
| GET /portfolio/balance (unscoped) | `client.py:565-574` (`get_balance()`), `client.py:1231`, `prod_client.py:248`, `settlement_followup.py:342` | **omitted** | spec default = primary (0). System 2 treats it as "the account total" (D-059, `DECISIONS.md:1882-1886`, comment `two_leg_campaign.py:838-840`); that finding was about shards, never tested with a funded subaccount |
| GET /portfolio/balance?exchange_index | `client.py:575-578` | explicit 0 | per-shard preflight read |
| GET /portfolio/subaccounts/netting | `client.py:580-585` | n/a (all rows) | every consumer filters `subaccount_number == 0` (`account_observations.py:53-56, 78-82, 150-153, 193-197`) |
| GET /portfolio/subaccounts/balances | `client.py:587-591` | n/a | auth check only |

Verdict: every call where an omitted subaccount means "all subaccounts" passes `subaccount=0`
explicitly. The only omission is the balance read, which the spec scopes to the primary account.

### A3. Account-wide reads in System 2 and what System 1 activity does to them

"Same sub" = System 1 trading KXBTC* on subaccount 0; "other sub" = System 1 on subaccount 1.

| System 2 check | Where | Same sub (0) | Other sub (1) |
|---|---|---|---|
| Launch preflight: any resting order on the account refuses | `two_leg_launcher.py:319-321` (`account_has_resting_orders`) | **Every launch refused** while System 1 quotes (a market maker always rests orders) | invisible (explicit sub 0) |
| Preflight cash `>= SESSION_BUDGET` ($50) | `two_leg_launcher.py:330-333`, per shard `:340-348`; `two_leg_policy.py:21` | System 1's margin draws the same cash | invisible, but funding sub 1 lowers sub-0 cash |
| Startup recovery: foreign resting orders | `recovery.py:357-367, 477-481`; `allow_foreign_resting=False` (`step4_pilot.py:181`, not set in `two_leg.py:514-560`) | **RecoveryBlocked, session refuses** | invisible |
| Startup position seed into RiskDebt ledger | `recovery.py:430-441`, `seed_positions.py:57-133`, `gates.py:83-88, 221-224` | System 1's KXBTC positions consume System 2's aggregate RiskDebt cap ($50 x stage, `limits.py:78-96`); a negative `market_exposure` (collateral return on sub 0 is ON, D-062) raises `PositionSeedError` (`seed_positions.py:115-116`) -> start refused | invisible |
| Cost-basis reconcile vs journal | `recovery.py:126-218` | markets not in the journal are skipped (`:159-160`): no refusal | n/a |
| Daily realized loss (`daily_realized_loss.json`) | `risk/daily_loss.py:243-268` (all fills + settlements of sub 0 today, no ticker filter), at start `step4_pilot.py:1153-1168` and every 60 s `step4_pilot.py:3052-3072` | System 1's fees and settlement losses count toward System 2's $40 day limit (`two_leg_policy.py:33`) -> `daily_realized_loss_breached`; System 1's gains mask System 2 losses | invisible |
| In-session drift check (every `account_recheck_seconds`=60, `two_leg.py:559`) | `session_control.py:162-215`; reaction `basket_executor.py:4066-4069` | `foreign_resting_order` / `position_drift` -> **basket halted "account_drift" mid-session** (can leave a naked leg) | invisible |
| Session cash parity (balance start/end vs own spend + all sub-0 settlements in window) | `step4_pilot.py:1440-1495`, `account_observations.py:464-530`; balances `step4_pilot.py:767-780` (unscoped) | any System 1 fill or settlement inside a System 2 session -> `cash_reconciliation_mismatch` -> run unreconciled -> campaign stop (`step4_pilot.py:1562-1586`) | invisible **if** the unscoped balance really is primary-only (spec); a sub0->sub1 transfer inside a session breaks it |
| Settlement capital release (`_cash_since_anchor`) | `two_leg_campaign.py:803-925` (fill ownership `:843-853`), used at `:750-752` | any sub-0 fill not in System 2's journal -> `unattributed_or_duplicate_new_fill` -> capital never released | only undeclared transfers to/from sub 1 break it (declare with `two_leg_launcher declare-transfer`, D-059 `DECISIONS.md:1872-1880`) |
| Pending-run reconcile | `two_leg_campaign.py:1057-1069` | `exchange_activity_requires_shared_order_recovery`, `initial_positions_changed` | invisible |
| Netting provenance ("orders the evidence does not explain") | `account_observations.py:238-283` | per event only; System 1 never orders System 2's events -> no effect | no effect |
| Netting check reads ALL sub-0 order history on every preflight | `account_observations.py:241-243` -> `client.py:610-631` (`paginate`, 50-page cap `client.py:265-287`) | System 1's thousands of orders/day push it past 50 x 200 orders -> `RuntimeError` -> every preflight fails | invisible |
| Netting config per shard | `account_observations.py:38-95` | no effect | sub-1 rows filtered out |
| Final position check | `step4_pilot.py:1507-1528` | only the session's candidate markets -> no effect | no effect |

### A4. System 2 cancel-all

* Default ownership-scoped cleanup: `router.cleanup_owned` (`step4_pilot.py:1386-1389`).
* Account-wide cancel-all only when `kill_scope == "subaccount"` and the kill switch engaged
  (`step4_pilot.py:1395-1399`; `campaign.py:485-489`). Default `"owned"` (`step4_pilot.py:180`,
  `campaign.py:87-89`, CLI defaults `cli.py:660-664, 717-721`); the launcher's `pilot_config` does
  not set it (`two_leg.py:514-560`), so the live two-leg sessions never cancel-all.
* The smoke tools call `cancel_all_resting` unconditionally (`prod_smoke.py:421, 454, 463`;
  `live_smoke.py:381, 436, 445`): manual tools, not running.
* Every cancel-all is `DELETE /portfolio/events/orders?subaccount=0` (explicit). It would
  cancel System 1's orders only if System 1 is on subaccount 0.

### A5. API usage (keys, request rate, WebSockets)

All three processes sign with ONE production key (`KALSHI_PROD_API_KEY_ID` +
`KALSHI_PROD_PRIVATE_KEY_PATH|PEM` from `.secrets/prod.env`, present, not read;
`prod_client.py:29-93`). Session logs show its non-secret suffix as the account scope
`key:f32264a7/subaccount:0` (`netting_provenance.py:98-101`). Numbers below are from code plus
the run logs (`screener.log`, watch journal, session files).

| Process | Host | REST (signed) | WebSocket | 429 handling |
|---|---|---|---|---|
| Screener `--profile live` (realtime collector, `short_duration_screener.py:2682, 2988`) | `external-api.kalshi.com`, signed with the prod key (`live_collector.py:60-88`) | one shared limiter at 0.25 s = cap 4 req/s (`live_collector.py:78`); discovery ~every 2 min (paged `GET /markets` + `GET /events`), global `/markets/trades` poller, `/markets/orderbooks` 100 tickers/call every 60 s. **Measured 3.83 req/s, pinned at the cap**, 0 x 429 | 1 signed connection to `wss://external-api-ws.kalshi.com/trade-api/ws/v2`: `orderbook_delta` + `trade` for ~3,860 tickers + unfiltered `market_lifecycle_v2`; reconnects on every ticker-set change (~2 min) (`live_books.py:194-287`) | `max_retries=0`: skip/keep old data, no halt (`live_collector.py:83-84, 142-146`) |
| Launcher `watch --fast-regime` | `api.elections.kalshi.com` (signed); fee/milestone reads unsigned on `external-api` | wakes on tape changes, 1-5 s (`two_leg_watch.py:1350-1362`); ~16 signed GETs per 2-leg preflight (incl. `list_orders(status=None)` paging ALL sub-0 order history, <= 50 pages, `client.py:265-287`). **~1.4 req/s average, 3-4 req/s peak**; no client-side limiter | none | 3 quick tries (0.1/0.2 s, `Retry-After` ignored, `client.py:542-560`); 5 failed ticks stop the watch |
| Launched session (`launch` -> `step4_pilot` -> `BasketExecutor`) | `api.elections.kalshi.com` | `get_order` + queue position per resting leg every 2 s; reassess 60 s (15 s once a leg is held); drift + daily-loss reads (all pages) every 60 s; start-up burst ~30-40 GETs; <= ~15 writes per session. **~2-3.5 req/s**, mostly not concurrent with the watch (`two_leg_launcher.py:1446-1452`) | none | create 429 = rejected; cancel 429 -> uncertain -> halt; > 30 s failed reads -> `data_outage` |

Total on the key: about 5-7.5 signed req/s, peaks near 8, before System 1 adds anything.
Whether Kalshi meters `external-api` and `api.elections` traffic in one bucket, and what a
100-ticker `/markets/orderbooks` call costs, is not answerable from the code: read
`GET /account/limits` and `GET /account/endpoint_costs` once. System 1 must leave that
budget: start with `rate_limits.account_share` <= 0.5 (reads are the contested bucket; System 2
writes little), and watch both systems' 429 counts. System 2 does not honour `Retry-After` on
signed GETs, so a 429 storm caused by System 1 would halt System 2 sessions (cancel 429 ->
uncertain -> halt).

Observed while auditing (not a code finding): the running watch predates commit 349e1b5, so
since 18:52:09Z every auto-launch is refused with `SourceChangedError`
(`prepared/watch-20260925T183212Z.jsonl`) while its signed preflights continue.

---------------------------------------------------------------------------------------------
## B. System 1's account-wide actions

System 1 already sends `subaccount` explicitly on every request (`venue_kalshi.py:48-49, 285`;
`VenueCfg.sub`, `dh/live/config.py:113-147`; watchdog `dh/live/watchdog.py:225-237`,
`scripts/watchdog.py:83`; tools `dh/live/tools.py:72, 130`). It is therefore already safe on its own
subaccount. On the shared primary account (sub 0, where System 2 has orders/positions in
non-BTC markets):

| # | Path | file:line | On shared sub 0 | Scoped today? |
|---|---|---|---|---|
| 1 | Start-up clean slate: cancel-all + verify, leftovers cancelled one by one (3 rounds), else exit | `app.py:338-345` -> `venue_kalshi.py:628-685` | **Cancels all of System 2's resting orders**, plus any it places in the next minute (spec 1067); if System 2 re-posts, start fails "still resting" | subaccount only |
| 2 | Start-up positions (strict) -> excluded events | `app.py:347-354`, `venue_kalshi.py:1109-1130`, `startup.py:156-158` | System 2's rows read; a malformed/odd row refuses the start; its events are "excluded" (harmless: not in universe) | subaccount only |
| 3 | Day P&L at start (fills, historical fills, settlements, open marks, midnight marks) | `app.py:355-361, 473-487`, `riskstate.py:292-330, 445-476, 533-576` | **System 2's fills, settlements and positions enter System 1's day P&L** and its -$25 daily-loss limit; an unparseable System 2 settlement row refuses the start | subaccount only (historical: none) |
| 4 | RiskBook "excluded" positions and their settlement realization | `app.py:421-424`; `runner.py:744-758, 1076-1079` (lifecycle is subscribed for ALL markets, `app.py:145`) | System 2's positions are carried and their settlements realized into System 1's day P&L | no |
| 5 | Order group create / list | `app.py:437-446`, `venue_kalshi.py:759-805` | harmless (System 2 uses no order groups) | subaccount |
| 6 | Ghost sweep every 30 s (and after every reconnect, `force=True`) | `runner.py:1772-1796` -> `_check_resting` `runner.py:1363-1413`; `runner.py:1135, 1152, 1234-1237`; `ghost_sweep: true` (`config/live.example.yaml:80`) | **Cancels every System 2 resting order within 30 s** | subaccount only |
| 7 | Soft CancelAll (lag, disconnect, pauses, fill_position_mismatch) -> sweep of ALL resting orders | `runner.py:1501-1526` -> `venue_kalshi.py:695-716` (`tickers=None`) | **Cancels all System 2 orders** | subaccount only |
| 8 | Scoped CancelAll(tickers) | `runner.py:1478-1499` | only BTC tickers | ticker |
| 9 | Global cancel-all on Halt, kill file, fee mismatch, watchdog marker | `runner.py:924-931, 959-971, 1034, 859, 1642-1660` -> `venue_kalshi.py:376-390, 662-665` | **Cancels all System 2 orders**; Halt can be triggered by System 2's own fills (row 11) | subaccount only |
| 10 | Shutdown: cancel-all verified, then cancel every visible resting order | `runner.py:2120-2140` | **Cancels System 2's orders**; exit 3 if System 2 keeps resting | subaccount only |
| 11 | Fills: WS `fill` channel (account-wide) + REST back-fill every 60 s / reconnect / reconcile | `app.py:149`, `runner.py:479-481, 1440-1476, 1798-1822, 1133, 1538-1550` | System 2's fills pass the subaccount filter and reach the OrderManager as orphan fills: position/cash/fees updated for their tickers (`order_manager.py:340-371`), equity counts their cash but marks only BTC specs (`mm.py:883-895`) -> phantom loss -> daily-loss Halt -> row 9. A fill whose `post_position` (System 2's whole position) differs -> `fill_position_mismatch` -> CancelAll -> row 7 | subaccount only |
| 12 | WS user_orders / market_positions | `runner.py:479`, `order_manager.py:479-481` (unknown_order: benign); positions `runner.py:1331-1334` (universe only) | benign | universe |
| 13 | Positions reconciliation (halts on mismatch) | `runner.py:1320-1361` | only markets in the universe: no effect | universe |
| 14 | Queue positions, create-by-coid lookup | `venue_kalshi.py:1091-1103, 926-940` | own tickers / coid | yes |
| 15 | Watchdog cancel-all (separate process, own key) | `dh/live/watchdog.py:225-237`, `scripts/watchdog.py:83` | **Cancels all System 2 orders** whenever the runner's heartbeat is stale >2 s, repeated every 30 s | subaccount only |
| 16 | `tools orders` / `reconcile` | `dh/live/tools.py:70-77, 128-131` | lists System 2 orders ("must print 0" in RUNBOOK section 6/9 no longer holds) | subaccount only |
| 17 | Balance | none (System 1 never reads balance live) | - | - |
| 18 | Rate limiter | `dh/kalshi/rate_limit.py:22-31`, `config/kalshi.example.yaml:50-56` (`account_share: 1.0`) | System 1 would plan on the whole account budget | configurable |

---------------------------------------------------------------------------------------------
## C. Recommendation

### Option (i): System 1 on its own subaccount (recommended)

Residual cross-effects, given A:

* System 2 -> System 1: none on orders/fills/positions/settlements/cancel-all (all explicit
  sub 0, A2). System 2's cancel-all can never reach sub 1.
* System 1 -> System 2: none on orders (all explicit sub N). System 2's sub-0 fills/orders still
  arrive on System 1's account-wide private WebSocket and are dropped by the subaccount filter
  (`runner.py:479-481`; `normalize.py:308-317` maps a missing field to 0).
* **Funding transfer.** Moving cash sub 0 -> sub 1 lowers System 2's unscoped balance: an
  undeclared transfer breaks `_cash_since_anchor` (settled capital stays reserved) and, inside a
  running session, the session parity (campaign stop). Do the transfer while no System 2 session
  runs and declare it with `two_leg_launcher declare-transfer` (D-059). Keep >= $50 per traded
  shard on sub 0 (preflight) and System 2's `STRATEGY_CAPITAL` allocation in mind.
* **Must verify once (read-only, after funding):** unscoped `GET /portfolio/balance` equals the
  sub-0 row of `GET /portfolio/subaccounts/balances` (spec: primary only). System 2 calls the
  unscoped read "the account total" (D-059, shards). If it turned out to include sub 1, every
  System 1 trade would break System 2's cash parity; then option (i) also needs System 2 to pass
  `subaccount=0` on its balance reads.
* **Must verify once:** System 1's own WS `fill` / `market_position` / `user_orders` payloads carry
  `subaccount: 1` (RUNBOOK 1.2). If not, System 1 drops its own WS fills and depends on the 60 s
  REST back-fill (position suspects, slower).
* `GET /historical/fills` has no subaccount filter: System 1 on sub 1 refuses to start if today
  (UTC) reaches before the historical cutoff and System 2's primary rows lack a subaccount field
  (`riskstate.py:182-214`). Rare (cutoff is normally far behind), but possible.
* Shared rate-limit buckets (A5): set `rate_limits.account_share` below 1.
* Netting is per subaccount; a new subaccount defaults to OFF, which System 1's accounting
  (counts-based settlement, `riskstate.py:230-276`) does not care about either way.

Code changes in delta-hedged (small; most of this is configuration):

| Change | file / function |
|---|---|
| `venue.subaccount: 1`; `rate_limits.account_share: <= 0.5` (see A5) | `config/live.yaml`, `config/kalshi.yaml` (already supported: `dh/live/config.py:113-147`, `dh/kalshi/config.py:61-81`) |
| Fail closed on a shared account: new `venue.shared_account: true` makes live mode refuse `subaccount` null/0 and refuse `rate_limits.account_share == 1.0` | `dh/live/config.py:live_config_problems` (`:280-300`) |
| Start-up guard: `GET /portfolio/balance?subaccount=N` must succeed and be > 0 (proves the subaccount exists and is funded) | `dh/live/app.py:LiveApp.build` before step 6 (`:336`) |
| Historical fills on a shared account: skip rows whose ticker is outside System 1's series instead of raising; still raise on an unattributable KXBTC* row | `dh/live/riskstate.py:parse_fill_row / day_flows` (`:199-227, 292-310`), pass `series` from `derive_day_pnl` (`:533-545`) |
| Optional belt-and-braces: drop WS/REST own-channel events whose ticker series is not System 1's | `dh/live/runner.py:push` (`:463-490`), `_backfill_fills` (`:1440-1476`) |
| RUNBOOK 1.2 / 1.3 / 5.2: subaccount deployment steps, restricted keys, transfer declaration for System 2 | `docs/RUNBOOK.md` |

What the user must do manually:

1. Confirm the account is on the Advanced API tier or above (subaccount creation, spec 1652).
2. Create subaccount 1 (Kalshi UI or `POST /portfolio/subaccounts`); leave its netting OFF.
3. Create two NEW API keys **restricted to subaccount 1** (runner + watchdog; spec 4873-4877).
   A restricted key makes the exchange itself refuse any System 1 action on sub 0, even a bug that
   omits `subaccount`. Keep System 2's existing unrestricted key untouched.
4. Between System 2 sessions, transfer the System 1 allocation sub 0 -> sub 1, bracket it with
   balance readings, and record it in System 2 with `two_leg_launcher declare-transfer`.
5. Run the two one-time checks above (unscoped balance == sub-0 balance; WS payloads carry
   `subaccount`), in paper mode / read-only.

### Option (ii): shared primary account with ownership scoping (not recommended)

System 1 changes needed (every row marked "subaccount only" in B):

| Change | file / function |
|---|---|
| Remove every global cancel-all: start-up clean slate, Halt/kill/fee-mismatch, shutdown -> trigger/delete System 1's order group (`PUT /portfolio/order_groups/{id}/trigger` cancels only the group's orders, spec 1447-1451) + cancel own orders by list | `app.py:338-345`; `runner.py:_on_actions (924-931), kill/_cancel_all_async (1642-1660), _check_fee (1034), _watchdog_halt (859), shutdown (2120-2140)`; `venue_kalshi.py:cancel_all_now / cancel_all_verified (628-685)` |
| Start-up clean slate = cancel resting orders whose `client_order_id` starts with `run_prefix-` (`dhm1-`, `config/m1.yaml:3`) or whose ticker is in System 1's series | `venue_kalshi.py:cancel_all_verified`, `resting_orders` (`:687-693`) |
| Ghost sweep, soft sweep, verify leftovers, shutdown leftovers: only own coid prefix / own series | `runner.py:_check_resting (1363-1413)`, `_cancel_soft (1501-1526)`; `venue_kalshi.py:sweep_resting (695-716)` |
| Drop fills / order updates of other series before the strategy (WS and REST) | `runner.py:push (463-490)`, `_backfill_fills (1440-1476)` |
| Positions, day P&L, excluded set, historical fills: filter to own series | `app.py:347-361, 421-424`; `riskstate.py:day_flows (292-330)`, `derive_day_pnl (533-576)`, `open_marks (445-476)` |
| Watchdog: no DELETE /portfolio/events/orders; trigger the order group whose id the runner writes into the heartbeat, then list+cancel by coid prefix | `dh/live/watchdog.py:rest_cancel_all (225-237)`, `scripts/watchdog.py:83`, heartbeat payload `runner.py:1736-1742` |
| Tools filter by prefix/series | `dh/live/tools.py:70-77, 128-131` |
| `rate_limits.account_share` | `config/kalshi.yaml` |

Why it still fails: even a perfectly scoped System 1 breaks System 2, because System 2 treats
any other activity on sub 0 as foreign (A3): every launch is refused while System 1 rests an
order (`two_leg_launcher.py:319-321`), recovery blocks on foreign resting orders
(`recovery.py:477-481`), the 60 s drift check halts baskets mid-session
(`session_control.py:162-215`, `basket_executor.py:4066-4069`), System 1's fees/losses feed
System 2's $40 daily limit (`daily_loss.py:243-268`), session cash parity and settlement capital
release fail on System 1's fills (`step4_pilot.py:1453-1495`, `two_leg_campaign.py:842-852`),
and System 1's positions eat System 2's RiskDebt cap (`gates.py:83-88`). Fixing that needs
series/prefix scoping inside the live System 2 code, and System 1 also loses its fastest
safety net (global cancel-all and the one-minute kill) and gets System 2's collateral-return
setting (ON on sub 0, D-062) on its KXBTC range events.

User manual steps for (ii): allow System 2 changes (out of scope here), set
`account_share`, and accept that System 1's kill path becomes list-and-cancel.

### Verdict

Use option (i): System 1 on subaccount 1 with API keys restricted to subaccount 1. It needs
almost no code change (System 1 is already explicit on every call; System 2 is already explicit
on every "omitted = all" call), leaves System 2 untouched, and keeps System 1's global
cancel-all, ghost sweep and watchdog intact within its own subaccount. Top residual risks: the
funding transfer must be declared to System 2, the unscoped-balance semantics must be verified
once, and the rate-limit budget is shared.
