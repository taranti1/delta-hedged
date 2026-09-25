# Pre-live review: System 1 (Kalshi subaccount 1, shard 2)

Scope: `git diff 1f0ec5a 80d4dbc` (live runner), `git show cfd4ff5` (account_setup.py), and at lower priority the settlement convention (`1f0ec5a..6d45556`) and WS commit 1bbb0fd.

Method: I read the code paths end to end and wrote throwaway probes that run offline against fakes (no Kalshi calls):
- `scratchpad/review/test_prelive_runner.py`: 8 probes
- `scratchpad/review/test_prelive_account_setup.py`: 5 probes
- `scratchpad/review/clock_drift.py`: reads the local `data/raw/clock` samples

All 13 probes pass, and each one demonstrates the behaviour described below. The repo suite also passes: `pytest tests/live tests/kalshi`, 496 passed.

One disclosure: I ran `sntp -t 2 time.apple.com` once to see the output format. That is Apple's NTP server, not Kalshi. No repo files were edited.

**Bottom line:** I found no path where the documented configuration sends a write to subaccount 0 or without an explicit subaccount. The `write_subaccount` guard holds. The two HIGH findings are:
1. `account_setup.py transfer` can move System 2's money twice.
2. The configuration defaults fail open to subaccount 0. Right now `config/kalshi.yaml` authenticates with System 2's unrestricted key, so the server would not stop this either.

---

## HIGH

### H1. `account_setup.py transfer`: a retry can drop the saved `client_transfer_id`, move money twice, and leave a withdrawal undeclared to System 2

**Where:** `scripts/account_setup.py:1003-1011`, `:1020-1026`, `:559-568`

**Failure scenario A5 (network flap; most likely):**
1. Attempt 1 is applied, but its response is lost. The tool reports "UNRESOLVED, re-run this exact command: it cannot apply twice".
2. The operator re-runs while the network is still down. `NotSentError` takes the `oc.kind in ("rejected","not_sent")` branch, which pops the pending entry (`:1004-1007`) without checking `attempts > 1`.
3. The next re-run generates a fresh uuid, and the transfer is applied a second time.
4. Only the second move gets a System 2 declaration. The first withdrawal from subaccount 0 is never declared, so System 2's exact cash parity breaks and its campaign stops.

**Variant A1:** Kalshi answers the duplicate id with 400 instead of 409. The openapi 3.31.0 spec for `POST /portfolio/subaccounts/transfer` lists only 200, 400, 401 and 500; the 409 comes from the getting-started guide. The result is the same: "definitely not applied", the pending entry is dropped, and a re-run moves the money twice.

**Variant A4:** the retry is answered 200 as an idempotent replay.
- The bracket starts at the retry's reading, which is already after the change. The observed delta is 0, so the declaration is marked `required: False` with "Do NOT declare anything yet".
- `status` then reports "(0 missing)", so System 2 is never told.

**Variant A2:** the retry is answered 409 but the earlier attempt was not applied (for example, the id was recorded but the transfer failed).
- The tool records `applied_earlier_409` and emits a REQUIRED declaration.
- Its warning even advises "declare the transfer amount" although the balance moved by 0. A declared-but-absent withdrawal also breaks System 2's parity.

**Evidence:** all four variants reproduce with the repo's own `FakeRest`/`Harness`:
- `test_retry_not_sent_drops_pending_then_double_move` shows `(1,2)` balance 300 and two distinct client ids.
- `test_retry_duplicate_answered_400_loses_idempotency`
- `test_retry_answered_200_declaration_dropped`
- `test_409_on_retry_trusted_without_balance_evidence`

**Partial mitigation:** the tool prints subaccount 0 and 1 balances before each confirmation. An attentive operator could notice that subaccount 1 already holds $150.

**Fix:**
- Once `attempts > 1`, never pop a pending transfer on `not_sent` or any 4xx.
- Persist the subaccount-balance rows at the first attempt (as `shard-transfer` already does) and resolve by evidence. That means balance deltas of `(from,idx)` and `(to,idx)` exactly equal to ∓amount, plus a matching row in `GET /portfolio/subaccounts/transfers` (from, to, amount_cents, exchange_index, created_ts ≥ first attempt).
- For any outcome of a retry, bracket from `first_pre`.
- Treat "200 on retry" like "409 on retry".
- Only declare when the balance evidence confirms the move.

### H2. Fail-open configuration defaults: a live config without `venue:` targets subaccount 0, cancel-all included, and the watchdog can too

**Where:**
- `dh/live/config.py:135-143` (defaults `subaccount=None→0`, `shared_account=False`, `key_restricted_to_subaccount=False`)
- `live_config_problems` `:329-354`
- `dh/live/app.py:377-380` (`write_subaccount=lcfg.venue.sub`)
- `scripts/watchdog.py:88-97` (`--live-config ""` loads `LiveConfig()`)

**Failure scenario:** a `config/live.yaml` with `mode: live` whose `venue:` block is missing or incomplete passes every live check (`live_config_problems(LiveConfig(mode="live")) == []`). Then:
- The REST guard is bound to subaccount 0.
- The start-up clean slate sends `DELETE /portfolio/events/orders?subaccount=0`, which cancels all of System 2's resting orders.
- The runner then trades on subaccount 0.

The watchdog has the same failure: run with an empty or venue-less config, `--cancel-now` or a trigger cancels subaccount 0.

The host's `config/kalshi.yaml` currently loads System 2's `.secrets/prod.env` with `KALSHI_PROD_API_KEY_ID`, an unrestricted key. So until the switch to restricted keys, nothing server-side prevents this. The watchdog also falls back to that key when `KALSHI_WATCHDOG_*` is not set.

**Evidence:**
- `test_live_config_defaults_target_subaccount_0`: the venue's startup cancel-all goes out as `DELETE ... subaccount=0`.
- `test_watchdog_default_config_targets_subaccount_0`: `calls == [("cancel_all", 0)]`.

**Fix:**
- In live mode, require an explicit `venue.subaccount` and `venue.shared_account`. Refuse subaccount 0 unless an explicit `allow_primary_subaccount: true` is set. Require `key_restricted_to_subaccount: true` when `shared_account` is true.
- The watchdog should refuse to run without an explicit config file and non-zero subaccount under the same rule.
- The watchdog should also refuse to act when the heartbeat's `"subaccount"` differs from its config.
- Consider hard-coding a `FORBIDDEN_SUBACCOUNTS = {0}` in the runner/watchdog REST guard for this deployment.

---

## MEDIUM

### M1. Key-restriction check: the fallback accepts a *missing field* as proof, and the restricted-key rule then ingests System 2's own-activity messages

**Where:** `dh/live/startup.py:93-120`; `dh/live/runner.py:205-211` (`own_subaccount_ok`)

**How the check works:**
- When `GET /api_keys` raises (after its 429/5xx retries) or does not list the key, the start-up accepts any balance bodies that lack `balance_breakdown`.
- A genuinely restricted key probably gets 403 on `GET /api_keys`. So this weakest path is the normal path, and its correctness rests on one doc sentence about `balance_breakdown`.

**Failure scenario:**
1. An unrestricted key (for example System 2's, which `config/kalshi.yaml` loads today, or a shell `KALSHI_KEY_ID` that wins over the env file) meets a transient `GET /api_keys` failure, or Kalshi omits `balance_breakdown` on `subaccount+exchange_index` reads.
2. The key passes the check.
3. With `key_restricted=True`, every primary-account `fill` / `user_order` / `market_position` without a `subaccount` field normalizes to 0 and is accepted as System 1's.
4. System 2's KXBTC* fills become orphan fills that move System 1's inventory until the REST positions check halts.

The `WsFillProbe` only logs; it does not enforce.

**Evidence:**
- `test_key_restriction_fallback_accepts_without_positive_evidence` (503 on `/api_keys` plus a body without breakdown gives `ok=True`).
- `test_primary_fill_attributed_with_restricted_flag`.

**Fix:**
- Require positive evidence: `GET /portfolio/balance?subaccount=0` (a read) must answer 403 with the runner key.
- Treat an `/api_keys` error as "retry", not "fallback".
- With `shared_account`, also drop and alarm on own-activity WS events whose `client_order_id` lacks this session's `id_prefix` or an order id the venue does not know.

### M2. `shard-transfer`: completion is `>=`, so an over-move is reported "Complete"; the amount unit is unverified

**Where:** `scripts/account_setup.py:1080` (`arrived = changes >= amount`), `:148` (`int(d*10_000)`)

**Scenario:**
- The request `amount` is centicents per the spec, while the response and listing `amount` are FixedPointDollars. That is an unusual mix.
- If Kalshi reads the field in cents, a $150 request moves $15,000 whenever subaccount 0 on shard 0 holds that much. The tool prints "Complete: subaccount 1 on shard 2 received $150.00".

**Evidence:** `test_shard_transfer_overmove_reported_complete` ends with the `(1,2)` balance at 15000 and exit 0.

**Fix:**
- Require the destination delta and the source delta to equal ±amount exactly, and alarm otherwise.
- Do a $1 canary `shard-transfer` first; `ACCOUNT_SETUP.md` step 5 should say so.

### M3. No watchdog liveness check: the runner can trade with the dead-man switch down

**Where:** `dh/live/app.py` / `runner.py`. Nothing reads any watchdog state; only the RUNBOOK checklist covers it.

**Scenario:** the launchd watchdog crash-loops, for example:
- TCC blocks `~/Desktop`,
- no credentials causes `SystemExit` in `build_rest`, or
- it exits with code 2.

Launchd restarts it every 10 s, silently. The runner starts and quotes anyway. The remaining backstops are the 120-150 s order expiry and the order-group limit.

**Fix:**
- The watchdog writes its own state file (`<heartbeat>.watchdog`: t, state, armed pid/session).
- The live runner refuses to start if that file is not fresh.
- The runner closes the gate if the watchdog is not ARMED on this pid/session within N seconds of `running`, or goes stale later.

### M4. Position-mismatch confirmation can be deferred indefinitely while fills keep arriving

**Where:** `dh/live/runner.py:1639-1670`

- `stale_read` compares Kalshi's `user_data_timestamp` with the *global* latest WS fill time across all markets.
- In a busy period every positions read can be "stale", so a real mismatch in any market is never confirmed and never halts.
- Each deferral also re-arms the confirming read (fills + timestamp + positions + resting) roughly every 5 s, which spends shared read budget.

**Fix:**
- Compare `as_of` with the suspicion's `first_seen` or the last fill *in that ticker*.
- Cap deferrals, for example by count or at 60 s, then halt.

### M5. Cancel-all "tail" scope is an unverified assumption with System 2 exposure

**Where:**
- `venue_kalshi.py:714-746`
- `watchdog.py:198-203`: repeats every 30 s, up to 11 cancel-alls over about 5 min
- start-up, kill, halt and shutdown in `runner.py`

Kalshi: "Newly placed orders may also be cancelled during the minute after the request." Nothing verifies that this tail honours the `subaccount` filter. If it is account-wide, every System 1 cancel-all can cancel System 2's orders placed in the next minute, which would leave a naked leg for a two-leg strategy.

**Fix:**
- Verify on subaccounts 1 and 2 only (never 0): cancel-all `subaccount=1`, then place on 2 within the minute.
- Otherwise prefer the already-implemented scoped paths: group trigger/delete, and list + batch-cancel of `GET /portfolio/orders?status=resting&subaccount=1`. Keep cancel-all as a last resort, and cut the watchdog's repeats down to trigger + list-based cancels.

---

## LOW

### L1. The guard enforces the *presence* of `exchange_index`, not its value
`rest.py:447-456` accepts any value ≥ -1, so a create naming shard 0 passes (`test_guard_accepts_any_explicit_shard`). The venue computes shards correctly today (`trade_shard`). Consider allowing only the configured shards, plus -1 for cancels.

### L2. Balance gate
`venue_kalshi.py:1283-1323`:
- Resting asks are counted at (1−p)·remaining even when they close a held YES position. Kalshi likely reserves nothing for those, so funds are over-counted by up to about the inventory's NO value.
- A failed balance read leaves the gate unchanged (fail-open).

Impact is limited: Kalshi rejects under-collateralized orders.

### L3. Exchange-pause gating
- Failed status polls keep the last state (fail-open).
- If `exchange_index_statuses` is absent, the shard-0 top level stands in for shard 2.
- A session whose `close_time` is "00:00" is dropped, producing a 21 h bogus closure (`test_schedule_close_midnight_and_plausibility_window`). The "12 h" plausibility check looks at [now−24 h, now), the *past* day (`startup.py:226` with `app.py:404`). The damage is bounded to roughly 2 min without quotes by the `c[0]+60 s` rule (`runner.py:1382-1386`).
- Gaps between WeeklySchedules are ignored.
- `_PAUSE_REJECT` (`runner.py:190`) matches any "paus", so a *market*-level pause reject triggers a global 30 s pause each time.

### L4. macOS clock drift blocks new orders mid-session
The `AnchoredClock` rides `mach_absolute_time`, which timed does not discipline. Measured from this Mac's `data/raw/clock` samples over 3.7 h, its error versus true time is 65 ms at start and grows by +11.5 ms/h (3.2 ppm). It crosses `clock_block_ms` (250 ms) about 16 h into a session. This fails closed: new orders are blocked until a restart.

The rest of the clock gate is sound:
- sntp formats other than the current macOS one, and no network, give "unmeasurable" and the gate blocks (`test_sntp_regex_variants`).
- The error bound is applied.
- Resampling every 5 s while the clock is bad may trip NTP rate limiting.

Fix: slew the anchor toward wall time at a bounded rate, or document a session limit of about 8 h.

### L5. `account_setup` operational gaps
- System 2 detection is local `ps` only; it misses a remote or containerized System 2.
- `--resume` does not recheck System 2, and the bracket can straddle a new System 2 anchor. System 2 fails closed on that.
- The pending-transfer state lives per checkout.
- `create-subaccount --another` relies on the new subaccount showing up in balances or netting. After an unknown outcome, a retry could create subaccount 2.

### L6. Rate budgets are shared
`account_share` (0.2) applies per process. The live runner, paper runner, recorder, watchdog and tools each take up to 0.2 of the account's read budget, so up to 0.8 in total. Only the live runner's own share is checked (`config.py:357-363`).

### L7. Kill-latch race
A group create that is in flight when the kill latches gets mapped after `trigger_groups` has iterated (`venue_kalshi.py:881-909`), so that group is never triggered. Impact: none. The gate is closed and the group is deleted at shutdown.

### L8. `scripts/verify_fee_schedule.py:85-87` reads fills without `subaccount`
This returns all subaccounts, System 2's included. It is read-only, but its fee verification would mix System 2's fills.

---

## Checked and found correct

- **Write guard** (`rest.py:381-457`). Every write goes through `KalshiRest.write`, and the guard refuses before signing or sending. It covers:
  - create, and batch create per item
  - amend, decrease, cancel, and batch cancel per item
  - cancel-all
  - group create, reset, trigger, limit and delete
  - body vs query conflicts, and list-valued or bool params

  `_send` is used only by GET and `write`, and writes are never retried inside the client.

  Every non-read-only client is accounted for:
  - the runner and the watchdog are both guarded;
  - `account_setup` is unguarded by design and never targets subaccount 0: `transfer`/`shard-transfer` refuse 0 as destination, and `create-key` and `set-netting` refuse subaccount 0;
  - tools, recorder, smoke and research clients are `read_only`.
- **Portfolio reads** in the runner, venue, riskstate and tools all pass `subaccount` explicitly. The only exception is `GET /portfolio/orders/{id}`, which has no such parameter and is used only for the session's own order ids.
- **Watchdog trigger** skips heartbeat groups whose subaccount does not match or whose shard is missing. Its cancel-all names the subaccount explicitly, and `shared_account` plus subaccount 0 refuses with exit 2.
- **Kill paths.** The group trigger is spawned before the cancel-all. A failed or stale trigger does not block the cancel-all. The latch forbids any group create or reset afterwards. Shutdown verifies the cancel against the resting list and writes `stopped` only when it is confirmed. Exit codes: 0 / 2 refused / 3 unconfirmed cancel / 4 loop or consumer died. The watchdog marker logic (mine means halt, another runner's means a 60 s hold) is sound.
- **Balance units.** `balance_dollars` is a fixed-point string with a cents fallback; px (1e-4 $) × qty (1e-2) = 1e-6 $; the requirement is $60. The startup check is fail-closed.
- **`shard_status`** matches the spec; startup refuses unless shard 2 is active. `schedule_closures` handles the realistic Thursday 03:00-05:00 ET schedule, DST weeks and 23:59 closes correctly (probed).
- **Key restriction.** An unrestricted key listed in `GET /api_keys` is refused, and any shape mismatch fails closed.
- **`account_setup` request bodies and flow:**
  - Bodies match openapi 3.31.0: `amount_cents`, uuid `client_transfer_id`, `exchange_index`, the intra-transfer fields, the netting body, and `GenerateApiKey` with `subaccount`.
  - A dry run uses a read-only client. `--execute` needs a TTY plus a typed confirmation.
  - The transfer id is fsynced before sending. `shard-transfer` refuses while anything is pending. The PEM is written `O_EXCL` 0600, and key material is sanitized out of the logs.
- **System 2 `declare-transfer` command** matches the real CLI (`two_leg_launcher.py:1970-1996`, `two_leg_campaign.py:762-798`):
  - `--amount=-X.XX` with the withdrawal as negative
  - tz-aware ISO `--after` (last reading without the change) and `--before` (first reading with it)
  - a quoted `--note`
  - the ledger path `data/two_leg_live/cash_transfers.json` and the duplicate check line up with `is_declared`
- **WS (1bbb0fd):** it yields once per frame. The `use_yes_price` proof is sound (the true reading is always uncrossed, so a unique proof is never wrong). REST books default to no-leg.
- **Replay** mirrors the new inbound rules (`key_restricted` and `series` come from the session meta).
- **Pre-existing live safety survives the merge:** post_only (`m1.yaml`), `cancel_order_on_pause` defaults True and is present in the body, the 120 s spread expiry, unknown-outcome reconciliation, persisted and carried halts, and fail-closed start-up.
