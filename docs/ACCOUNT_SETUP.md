# Kalshi subaccount setup for System 1 (M1)

System 1 (this repository's KXBTC* market maker) is to trade on its **own subaccount**, next to
the user's other, live system ("System 2", `trading-strategy/Kalshi`), which trades on the
primary account, subaccount 0. Why: `docs/research/SHARED_ACCOUNT_AUDIT.md` section C (option
(i)). The Kalshi facts are in `docs/research/KALSHI_DOCS_RECONCILIATION.md` section 4a and
findings 1 and 4, re-checked below against the official spec (`docs/kalshi_specs/openapi.yaml`
3.31.0) and docs.kalshi.com on 2026-09-25.

You run every step yourself with `scripts/account_setup.py`. The tool:
* is **read-only by default**: `status` (the default command) only sends GETs, and every write
  command is a **dry run** unless you add `--execute`. A dry run uses a REST client that cannot
  send anything but GETs;
* with `--execute`, prints the exact request (method, URL, JSON body) and sends it only after you
  **type** the confirmation it asks for (the amount, the key name, `advanced`, `CREATE 2`,
  `netting off 1`), at an interactive terminal;
* never chains steps, and never writes to subaccount 0 except as the **source** of a transfer.
  Moving money back into subaccount 0 is deliberately not supported;
* refuses a transfer out of subaccount 0 while a System 2 process runs, and checks again just
  before sending;
* saves the transfer's idempotency key before sending, so re-running the same command after a
  timeout cannot apply the transfer twice;
* logs every write attempt to `data/logs/account_setup.jsonl` (git-ignored). No key material is
  logged, and key ids are masked. Unresolved transfers are kept in
  `data/logs/account_setup_state.json`.

Exit codes: `0` done or dry run, `1` failed, outcome unknown or partial, `2` refused or bad
arguments (nothing sent), `3` the typed confirmation did not match (nothing sent).

---------------------------------------------------------------------------------------------
## Facts the procedure rests on

| What | Endpoint and body (openapi 3.31.0 operationId) | Notes |
|---|---|---|
| API tier | `GET /account/limits` (GetAccountApiLimits) | `usage_tier`; the account is **basic** today |
| Upgrade to Advanced | `POST /account/api_usage_level/upgrade`, no body (UpgradeAccountApiUsageLevel) | self-serve, permanent grant. Criterion: at least 1 of the account's last 100 Predictions orders was created via the API (System 2's are). 403 otherwise. Costs 30 write tokens. Account-wide: budgets go from Basic read 200 / write 100 to Advanced 300 / 300 tokens/s |
| Create subaccount | `POST /portfolio/subaccounts` `{"exchange_index": 2}` (CreateSubaccount) | Advanced tier or above. API only: the web and mobile apps do not support subaccounts. Numbers 1-63, assigned in sequence. `exchange_index` defaults to 0, and all KXBTC* markets are on shard 2. **Not idempotent**: each call creates the next number |
| Same-shard transfer | `POST /portfolio/subaccounts/transfer` `{client_transfer_id (uuid, required), from_subaccount, to_subaccount, amount_cents, exchange_index}` (ApplySubaccountTransfer) | amounts in **cents**. `exchange_index` defaults to 0. Idempotent on `client_transfer_id`: a retry with the same id returns 409 (getting_started/subaccounts) |
| Cross-shard transfer | `POST /portfolio/intra_exchange_instance_transfer` `{source: "event_contract", destination: "event_contract", amount, source_exchange_shard, destination_exchange_shard, source_subaccount, destination_subaccount}` (IntraExchangeInstanceTransfer) | `amount` in **centicents** ($150 = 1,500,000). Asynchronous: the response is `{transfer_id}`, and `GET /portfolio/intra_exchange_instance_transfers/{transfer_id}` reports `status` `pending` or `complete`. Across shards with subaccounts it runs in up to **three non-atomic steps**; a failed later step leaves the money in the primary account on the source or destination shard. No idempotency key |
| Balances | `GET /portfolio/subaccounts/balances` (GetSubaccountBalances) | one row per (subaccount, exchange_index), balance in dollars. Subaccount balances are local to a shard |
| System 2's balance read | `GET /portfolio/balance` with no parameters (GetBalance) | the **primary** account's aggregate over all shards (changelog 2026-08-13). So a transfer out of subaccount 0 lowers it, and a move between shards of subaccount 0 does not (System 2 D-059) |
| Netting | `GET` / `PUT /portfolio/subaccounts/netting` `{subaccount_number, enabled}` | per subaccount |
| Restricted API key | `POST /api_keys/generate` `{name, key_type, scopes, subaccount}` (GenerateApiKey) | see the next section |

### Restricted API keys: how they are created and how they authenticate

**Creation.** The account owner creates them over the API, signed with an **unrestricted** key.
Setting the `subaccount` field (0-63) restricts the new key to that one subaccount:
* **`POST /trade-api/v2/api_keys/generate`**: Kalshi generates the key pair and returns the
  private key **once**. `key_type` is `rsa` when omitted. No tier requirement is documented.
  This is what `account_setup.py create-key` sends, with `key_type: "rsa"`, because
  `dh.kalshi.auth` signs RSA-PSS only.
* `POST /trade-api/v2/api_keys` registers your own public key and takes the same `subaccount`
  field. It requires the **Premier** or Market Maker usage level, so it is not available to
  this account.
* **Web UI**: the documented "Create New API Key" button (kalshi.com/account/profile) generates
  an unrestricted RSA key pair. No subaccount option is documented for it, and subaccounts are
  an API-only feature.

Sources:
* docs.kalshi.com/getting_started/subaccounts, section "Restricted API keys": you can restrict a
  key to a single subaccount when generating it.
* openapi 3.31.0: `GenerateApiKeyRequest.subaccount` and `CreateApiKeyRequest.subaccount`, with
  the Premier / Market Maker requirement in the CreateApiKey description.
* docs.kalshi.com/getting_started/api_keys: web UI key creation, and both API routes.
* docs.kalshi.com/getting_started/sub_users_vs_subaccounts: to give a bot a fixed amount of
  capital, fund a subaccount and use a key restricted to it; only the account owner can create
  API keys.

**Authentication.** A restricted key signs requests exactly like any other key: the same
`KALSHI-ACCESS-KEY`, `KALSHI-ACCESS-TIMESTAMP` and `KALSHI-ACCESS-SIGNATURE` headers, and RSA-PSS
SHA-256 over `timestamp + METHOD + path` (getting_started/api_keys). Nothing changes in
`dh.kalshi.auth`. The restriction is enforced by the server:
* A request that omits `subaccount` acts on the key's locked subaccount. Naming any **other**
  subaccount, **including 0**, is rejected. So `config/live.yaml` must say
  `venue.subaccount: 1`.
* The key can place and manage orders, read the portfolio (balance, positions, fills,
  settlements), manage order groups and run the RFQ flow (getting_started/subaccounts).
* Queue positions, batch order endpoints and order-group management were opened to
  restricted keys on 2026-07-30, and historical fills and orders on 2026-09-24 (changelog).
* It can open WebSocket sessions (changelog 2026-07-23). There, `fill`, `user_orders`,
  `market_positions`, `order_group_updates` and `communications` are scoped to its subaccount.
  `orderbook_delta` still carries the full book.
* It **cannot** transfer funds, manage subaccounts or manage API keys. Endpoints outside its
  allowed set answer 403 ("restricted to a single sub-account").
* `GET /portfolio/balance` omits `balance_breakdown` for a restricted key.

**Not documented, checked in step 7:** whether a restricted key's signature is accepted on
market-data GETs, `/account/limits`, `/account/endpoint_costs`, `/exchange/status` and the CF
Benchmarks passthrough. The runner uses all of them.

---------------------------------------------------------------------------------------------
## Step 0: before you start

In the main checkout (`/Users/thomast/Desktop/delta-hedged`), with the venv active
(`. .venv/bin/activate`):
```sh
cp config/kalshi.yaml config/kalshi.admin.yaml   # git-ignored; keeps the UNRESTRICTED key
python scripts/account_setup.py                  # = status, read-only
```
* `config/kalshi.yaml` on this Mac points `auth.env_file` at System 2's credentials file, read
  in place (RUNBOOK 1.3). That unrestricted key is the only one that can run steps 1-5 and 8.
* `account_setup.py` uses `config/kalshi.admin.yaml` automatically while it exists (or
  `--config PATH`), so step 3 may switch `config/kalshi.yaml` to the restricted keys.
* The status header shows which key is in use and whether it is restricted.

`status` prints:
* the API tier and limits;
* every (subaccount, shard) balance;
* the unscoped primary balance next to the sum of the subaccount-0 rows;
* the netting rows;
* the shard of KXBTCD, KXBTC and KXBTC15M;
* the API keys (masked) with their restriction;
* recent transfers and any System 2 process;
* unresolved local transfers, and whether each withdrawal is already in System 2's ledger;
* a **checklist** with the next command.

Optional rehearsal: with demo credentials in a separate config, add
`--demo --config config/kalshi.demo.yaml` to every command.

## Step 1: upgrade the API tier to Advanced
```sh
python scripts/account_setup.py upgrade-tier --to advanced             # dry run
python scripts/account_setup.py upgrade-tier --to advanced --execute   # type: advanced
```
If the account is already Advanced or above, nothing is sent. Check the result with `status`:
`usage_tier advanced`. System 1's rate limiter reads the new budgets at its next start, and
`rate_limits.account_share` stays 0.2.

## Step 2: create the subaccount on shard 2
```sh
python scripts/account_setup.py create-subaccount --exchange-index 2             # dry run
python scripts/account_setup.py create-subaccount --exchange-index 2 --execute   # type: CREATE 2
```
* Expect `Created subaccount 1 on shard 2`. If Kalshi assigns another number, use it in place
  of `1` everywhere below (`status --subaccount N`).
* The tool refuses when a numbered subaccount already exists (`--another` overrides).
* After an unknown outcome, run `status` **before** retrying: a retry creates one more
  subaccount.

## Step 3: create the restricted API keys (runner and watchdog)
```sh
mkdir -p ~/.kalshi && chmod 700 ~/.kalshi
python scripts/account_setup.py create-key --subaccount 1 --name dh-sub1-runner \
  --pem ~/.kalshi/dh-sub1-runner.pem --env-file ~/.kalshi/dh-sub1.env \
  --id-var KALSHI_KEY_ID --path-var KALSHI_PRIVATE_KEY_PATH --execute        # type: dh-sub1-runner
python scripts/account_setup.py create-key --subaccount 1 --name dh-sub1-watchdog \
  --pem ~/.kalshi/dh-sub1-watchdog.pem --env-file ~/.kalshi/dh-sub1.env \
  --id-var KALSHI_WATCHDOG_KEY_ID --path-var KALSHI_WATCHDOG_PRIVATE_KEY_PATH --execute  # type: dh-sub1-watchdog
```
(Leave out `--execute` first to see the dry run.)

What the tool does with each key:
* It sends `POST /api_keys/generate` with
  `{"name", "key_type": "rsa", "scopes": ["read", "write"], "subaccount": 1}`.
* It writes the private key, which Kalshi shows only once, to the `--pem` file. The file is
  created before the request with mode 0600 and must not already exist.
* It appends `<id-var>=<key id>` and `<path-var>=<pem path>` to the env file (mode 0600).
* It prints neither the private key nor the key id.
* It refuses paths inside the repository, and variable names the env file already defines.

The key pair is valid only for subaccount 1: even a bug that sends `subaccount: 0` is refused
by the exchange, and neither key can move money. Keep System 2's key untouched.

Then point the Kalshi config that the runner and the watchdog load at the env file. Edit
the `auth` block of `config/kalshi.yaml`:
```yaml
auth:
  env_file: ~/.kalshi/dh-sub1.env   # outside the repo, chmod 600, read in place
  key_id_env: KALSHI_KEY_ID
  private_key_path_env: KALSHI_PRIVATE_KEY_PATH
```
* The watchdog finds `KALSHI_WATCHDOG_KEY_ID` / `KALSHI_WATCHDOG_PRIVATE_KEY_PATH` in the same
  file: `load_config` loads the whole file.
* Variables already exported in your shell win over the file. Make sure the four names are not
  exported (`env | grep KALSHI_`).
* The recorder also reads `config/kalshi.yaml`: after its next restart it signs with the runner
  key. Step 7 checks that this works.

## Step 4: stop System 2 sessions

Required for steps 5 and 6. Why:
* A running session's cash parity would see the withdrawal. The result is a
  `cash_reconciliation_mismatch` and a campaign stop.
* A run's balance anchor falling inside the transfer's bracket makes System 2 fail closed
  (`cash_transfer_straddles_anchor`).

In System 2's checkout:
```sh
ps axo pid,command | grep -E 'kalshi_m1|two_leg_launcher|short_duration_screener' | grep -v grep
touch <manifest-dir>/WATCH_STOP   # <manifest-dir> = the watch's --manifest-dir (see the ps line)
```
* `WATCH_STOP` stops the watch once a running session has ended. It is one-shot: the watch
  deletes it (System 2 docs/TWO_LEG_PILOT.md).
* Ctrl-C in the watch's terminal also works: it is forwarded to a running session, which then
  cancels and reconciles its orders.
* Do not use `data/two_leg_live/STOP` unless you mean it. It is System 2's persistent kill
  switch and stays until removed.
* Wait until `python scripts/account_setup.py status` shows `[System 2 processes] none`.
* The screener only collects market data. If you leave it running, pass `--i-stopped-system2`
  to the transfer. That flag only says that no System 2 **session** runs.

## Step 5: transfer the M1 allocation to subaccount 1 on shard 2

**Amount: $150.**
* M1's worst-case limits are $50 total (`risk.max_total_worst_loss`) with a daily halt at $25
  (`risk.daily_loss_halt`).
* $150 leaves headroom for fees and for the start-up balance check. That check
  (reconciliation finding 4, audit C) requires
  `GET /portfolio/balance?subaccount=1&exchange_index=2` to cover the worst case.
* What stays on subaccount 0 must still meet System 2's launch preflight: at least $50 on every
  shard it trades. Its $250 allocation ceiling is not a balance. The tool warns when the
  remainder on the source shard drops below $50.

`status` shows where subaccount 0's cash is.

**A. Subaccount 0 already holds at least $150 on shard 2:**
```sh
python scripts/account_setup.py transfer --from 0 --to 1 --amount-dollars 150 --exchange-index 2            # dry run
python scripts/account_setup.py transfer --from 0 --to 1 --amount-dollars 150 --exchange-index 2 --execute  # type: 150
```

**B. The usual case: the primary account's cash is on shard 0.** This funds shard 2 and
subaccount 1 in one cross-shard transfer:
```sh
python scripts/account_setup.py shard-transfer --from-subaccount 0 --from-shard 0 \
  --to-subaccount 1 --to-shard 2 --amount-dollars 150              # dry run; add --execute, type: 150
```
The tool then polls the transfer's status every 2 s for up to 180 s (`--timeout-s`) and
compares the balances before and after:
* **Complete**: subaccount 1 on shard 2 received $150.
* **PARTIAL**: Kalshi reports the transfer complete, but the money stopped part-way (the
  non-atomic steps). The tool shows where it went. If it sits in subaccount 0 on shard 2,
  finish with the command of case A, which the tool prints. If it is still on shard 0, nothing
  moved.
* **Still pending** at the timeout: the transfer stays unresolved. Check later with
  `python scripts/account_setup.py shard-transfer --resume <transfer_id>`.

Instead of case B you can move primary cash to shard 2 in the Kalshi web app
(kalshi.com/account/exchange-indexes) and then use case A. A move between shards of subaccount
0 needs no System 2 declaration (D-059).

**Unknown outcome** (timeout, 5xx):
* `transfer`: re-run the **same** command. It reuses the saved `client_transfer_id`; if the
  first attempt was applied, Kalshi answers 409 and the tool records the transfer as applied.
  Any other transfer is refused until this one is resolved.
* `shard-transfer`: this API has no idempotency key, so the tool refuses any further transfer.
  Run `status` and look at "recent transfers". If the transfer is listed, run
  `shard-transfer --resume <id>`. If it is not, run `forget-pending --id <local id>`, which
  changes local state only.

## Step 6: declare the withdrawal to System 2

After a transfer out of subaccount 0, the tool:
* reads the unscoped `GET /portfolio/balance`, the read System 2's cash parity uses, just
  before and just after the transfer;
* prints the exact command below, with the real times filled in;
* shows it again in `status` until System 2's ledger holds the row.

Run it in System 2's checkout **before restarting the watch**:
```sh
cd /Users/thomast/Desktop/trading-strategy/Kalshi && .venv/bin/python -m kalshi_m1.experiments.two_leg_launcher declare-transfer --amount=-150.00 --after 2026-09-25T21:03:11.123456+00:00 --before 2026-09-25T21:03:14.456789+00:00 --note 'dh account_setup: subaccount 0->1 shard 2 $150.00 client_transfer_id ...'
```

Arguments (System 2 `src/kalshi_m1/experiments/two_leg_launcher.py`, `declare-transfer`;
`CampaignStore.declare_cash_transfer` in `two_leg_campaign.py`; DECISIONS.md D-059):

| Argument | Meaning | Here |
|---|---|---|
| `--amount` | dollars as the balance saw them: + deposit, - withdrawal | `-150.00` (written `--amount=-150.00`) |
| `--after` | ISO time, with timezone, of the last balance reading WITHOUT the change | the tool's reading just before sending |
| `--before` | ISO time of the first balance reading WITH the change (must be later than `--after` and not in the future) | the tool's reading right after the transfer |
| `--note` | required text | transfer id and route |

* The command is offline (no network). It validates the row and appends it to System 2's
  `data/two_leg_live/cash_transfers.json` (`cash_transfers/1`). It refuses duplicates and
  brackets that end in the future.
* **Precondition:** no System 2 session ran between `--after` and `--before`, and none has
  started since (step 4).
* A declared transfer counts once its bracket lies after a run's balance anchor. A bracket that
  straddles an anchor fails closed.
* If the tool reports that the unscoped balance did **not** change, do not declare anything.
  That contradicts the documented semantics: re-check with `status` first.
* If the balance changed by a different amount (a settlement landed in the bracket), the tool
  says so. Still declare the transfer amount: System 2 counts its own settlements.
* Then restart System 2 as usual.

## Step 7: verify
```sh
python scripts/account_setup.py status
```
Expected:
* tier Advanced;
* subaccount 1 on shard 2 holding about $150;
* 2 API keys restricted to subaccount 1;
* the withdrawal shown as declared to System 2;
* the unscoped primary balance **equal** to the sum of the subaccount-0 rows (the one-time
  check of audit C).

Then check the restricted runner key, which `config/kalshi.yaml` now uses. These commands are
read-only:
```sh
cp -n config/live.example.yaml config/live.yaml   # if absent; then set venue.subaccount: 1 in it
python scripts/smoke_kalshi.py --seconds 20       # exchange status, account limits, markets, orderbooks, WS, CF
python -m dh.live.tools orders                    # 0 resting orders on subaccount 1
python -m dh.live.tools backfill                  # CF Benchmarks passthrough with the restricted key
```
If any of them answers 403 "restricted to a single sub-account":
1. Put the old key back for the recorder: `cp config/kalshi.admin.yaml config/kalshi.yaml`.
2. Give the runner its own Kalshi config (`config/live.yaml` `kalshi_config:`) that points at
   `~/.kalshi/dh-sub1.env`.
3. Report the endpoint: the runner then needs a design change.

## Step 8: netting OFF on subaccount 1
```sh
python scripts/account_setup.py set-netting --subaccount 1 --off             # dry run
python scripts/account_setup.py set-netting --subaccount 1 --off --execute   # type: netting off 1
python scripts/account_setup.py status                                       # every item [x]
```
* A new subaccount is not listed, which means Kalshi's default: off. Setting it explicitly makes
  the row visible.
* System 1's accounting is count-based and settles without netting.
* System 2 filters the netting rows to subaccount 0, so it is unaffected (audit A2).

---------------------------------------------------------------------------------------------
## After the setup: what to check

* `status` ends with `Next: nothing`.
* **System 2:** its next settlement check shows a zero parity residual (the declaration was
  right). Its launch preflight still finds at least $50 on each shard it trades.
* **System 1 configuration:**
  * `config/live.yaml`: `venue.subaccount: 1`.
  * `config/kalshi.yaml`: points at `~/.kalshi/dh-sub1.env`; `rate_limits.account_share` stays
    0.2 (budgets are per account and shared).
  * The start-up balance check (reconciliation finding 4) should read
    `GET /portfolio/balance?subaccount=1&exchange_index=2`.
* **Open code items before live** (KALSHI_DOCS_RECONCILIATION):
  * create the order group with `exchange_index: 2` (finding 1);
  * pass `exchange_index` on order writes (finding 7);
  * pass `subaccount` on historical fills (finding 5).
* **Verify live on the first paper or live session:**
  * own `fill`, `user_orders` and `market_positions` messages carry subaccount 1 (a restricted
    key scopes them server-side anyway);
  * queue positions cover shard-2 orders (finding 13).
* **Secrets:**
  * `ls -l ~/.kalshi` shows `-rw-------` on both PEMs and the env file.
  * Nothing under the repository holds a key (`*.pem`, `*.env` and `config/kalshi*.yaml` are
    git-ignored).
  * Once the setup is finished, delete `config/kalshi.admin.yaml`: System 1 must not hold the
    unrestricted key. `account_setup.py` then falls back to `config/kalshi.yaml` and refuses
    admin steps with the restricted key.
* **Audit trail:** keep `data/logs/account_setup.jsonl` and `data/logs/account_setup_state.json`.
* **Money back to subaccount 0** (a later reallocation or shutdown) is not supported by this
  tool. It is a manual transfer, and System 2 needs its own declaration: a deposit, with a
  positive `--amount`.

## Command reference

| Command | Sends | Type to confirm | Guards |
|---|---|---|---|
| `status` (default) | GETs only | none | none |
| `upgrade-tier --to advanced` | `POST /account/api_usage_level/upgrade` | `advanced` | skipped when already Advanced+ |
| `create-subaccount --exchange-index N [--another]` | `POST /portfolio/subaccounts` | `CREATE N` | tier Advanced+; refuses when a numbered subaccount exists |
| `create-key --subaccount N --name X --pem P --env-file E --id-var A --path-var B` | `POST /api_keys/generate` | the key name | N >= 1 exists; P and E outside the repo; P new; A and B not already in E |
| `transfer --from F --to T --amount-dollars D --exchange-index N [--i-stopped-system2]` | `POST /portfolio/subaccounts/transfer` | the amount | T >= 1; System 2 check when F = 0; balance check; saved `client_transfer_id` |
| `shard-transfer --from-subaccount F --from-shard S --to-subaccount T --to-shard U --amount-dollars D` | `POST /portfolio/intra_exchange_instance_transfer`, then polls | the amount | T >= 1; S != U; System 2 check when F = 0; refuses while anything is unresolved |
| `shard-transfer --resume ID` | GETs only | none | none |
| `set-netting --subaccount N --on/--off` | `PUT /portfolio/subaccounts/netting` | `netting off N` | N >= 1 exists |
| `forget-pending --id ID` | nothing (local state) | the id | none |

Global options: `--config PATH`, `--demo`, `--system2-root PATH`. All write commands take
`--execute`. Amounts are dollars with at most 2 decimals, at most $1,000 (typo guard).

Tests: `tests/kalshi/test_account_setup.py` (fake REST client; request bodies validated
against the openapi schemas).
