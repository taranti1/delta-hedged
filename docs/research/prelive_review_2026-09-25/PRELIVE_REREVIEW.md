# Pre-live re-review: fixes at d83d737 (merged at 5db5f7c)

**Scope:** `git diff 2262c1e d83d737` (37 files).

**Method.**
- I adapted the original probes to the new APIs and re-ran them.
- I wrote new adversarial probes for the new code.
- Everything ran offline (no Kalshi calls). No repo edits, no commits.
- Probe files are in `scratchpad/review/`:
  - `test_rereview_runner.py`: 12 probes, all pass.
  - `test_rereview_account_setup.py`: 6 probes, all pass.
- Repo suite `pytest tests/live tests/kalshi`: 577 passed.

**Bottom line.** Every original finding is actually closed; each one was re-probed against the new code, not just against its new tests. The new code opens no path to subaccount 0 or to the bulk cancel-all on the shared account. I found 1 MEDIUM and 5 LOW. None is CRITICAL or HIGH.

---

## Original findings: status

| # | Finding | Status | How verified |
|---|---|---|---|
| H1 A5 | NotSent on a retry drops the id, so the money moves twice | **closed** | A retry never drops the id (`attempts > 1`). A re-run first checks evidence and sends nothing. Probe: one client id, $150 moved once. |
| H1 A1 | 400 on a retry | **closed** | Id kept. Evidence (exact balance deltas since the first attempt, plus the transfer list) completes it. Probe: single move. |
| H1 A4 | 200 replay on a retry drops the declaration | **closed** | The bracket always starts at the first attempt's reading. Probe: declaration `required`, `-150.00`. |
| H1 A2 | 409 on a retry declared although nothing moved | **closed** | Verdict `not_seen`: stays pending, nothing declared (probe rc 1, completed `[]`). |
| H2 | Defaults target subaccount 0 | **closed** | `subaccount` and `shared_account` must be explicit. Subaccount 0 only with `allow_primary_account` on a non-shared account. `LiveConfig(mode="live")` is refused. `KalshiVenue(VenueCfg())` raises. The watchdog with `--live-config ""`, or without a venue section, exits 2 and sends nothing. The example-config fallback is removed. `tools` refuses an unset subaccount. |
| M1 | Key proof accepted a missing field | **closed** | Positive proof: `GET /portfolio/balance?subaccount=0` must answer 401 or 403. A 200 is refused; 5xx, 429 and network errors are refused ("retry"). Second layer: the own-order filter drops a System 2 fill (`sys2-…` client id, unknown order id) even with a restricted flag. See NEW-1 for its cost. |
| M2 | Shard-transfer over-move reported "Complete" | **closed** | A `--probe-dollars` transfer (≤ $5) is mandatory per route. Completion needs exact deltas (±1 cent), nothing else moving, and the listed amount matching. Probe: the full transfer is refused before the probe; a 100x probe alarms (rc 1); the full transfer stays refused. |
| M3 | No watchdog liveness check | **closed (liveness only)** | The `<heartbeat>.watchdog` beat is required at start (≤ 10 s, same subaccount). A runner gate closes on a stale/missing/`EXITED`/other-subaccount beat, or when the watchdog is not armed on this runner after 10 s of running. Capability is not checked: see NEW-2. |
| M4 | Unbounded mismatch deferral | **closed** | Compared per ticker. Capped at 5 deferrals or 60 s; after the cap only a read newer than the first sight defers-to-confirm; any read confirms after 120 s. |
| M5 | Cancel-all tail scope | **closed by avoidance** | `forbid_bulk_cancel` in the REST guard (runner and watchdog), shared account only. The runner uses a scoped cancel-all: list + batch cancel by id, 3 rounds. The watchdog uses `rest_scoped_cancel_all`. Probe: the bulk DELETE raises before sending; a scoped kill never sends it and follows list pagination. The 60 s holds are removed for shared accounts, which is correct. |
| L1 | Guard checked shard presence, not value | **closed** | Creates, amends and group create/reset/limit must name a configured shard; -1 is refused. Reducing writes (cancel, batch cancel, decrease, group trigger/delete) may name any shard or -1. Probe: shard 0 and -1 creates and a mixed batch are refused; cancels on shard 0 and -1 pass; subaccount 0 is still refused. |
| L2 | Balance gate | **closed** | Closing orders are excluded from collateral. 3 failed reads close the gate. |
| L3 | Pause gating | **closed** | 3 unreadable status polls close the gate. A "00:00" close means end of day. The plausibility check now covers the NEXT 24 h. Market-level pause rejects block only that market. Probe: a Thursday 05:00–00:00 session gives just the 2 h pause. |
| L4 | macOS clock drift | **closed** | `AnchoredClock` slews toward wall time at ≤ 50 ppm. Probe: 3 ppm drift is absorbed (< 50 µs over 200 s). It stays strictly increasing, never stepped (≤ 50 ppm extra per interval). A +1 s wall step still shows as about 1 s of drift, so the gate blocks. A backward step keeps the clock advancing. Replay-safe: receive times are recorded. |
| L5 | account_setup operations | **closed** | Per-user state file with legacy migration. `--resume` rechecks System 2. `--again` is needed to repeat a completed transfer. |
| L6 | Rate-share sum | **closed** | The recorder takes 0.1 and the docs keep the total ≤ 0.5. Only the runner's share is enforced in code. |
| L7 | Kill-latch race | not changed | Harmless: gate closed, group deleted at shutdown. |
| L8 | `verify_fee_schedule` read all subaccounts | **closed** | Takes `--subaccount` or `venue.subaccount`. |
| sntp formats | | unchanged, fail-closed (correct) | |

---

## New findings

### MEDIUM

**NEW-1. The own-order filter can drop OUR fills. Recovery is automatic, but inventory is wrong for up to about 60 s.**
`dh/live/runner.py:250-272, :778-782, :2002-2009`

**How it works:**
- A fill passes if its `client_order_id` starts with `dhm1-`, or if its `order_id` is already in `known_oids`.
- `known_oids` is filled by an OrderAck (the REST create response) or an order update carrying our client id.

**Where it drops our own fills:**
- **WS fill without a client id.** asyncapi `fillPayload.client_order_id` is *optional*. If a post-only quote fills right after it rests, the WS fill can be queued before the REST create response. It is then dropped as "foreign" and not added to `fills_seen`.
- **REST fills never carry a client id.** A create with an unknown outcome is not in `known_oids` until reconciliation finds it. Until then its REST back-filled fills are dropped too.

Probe `test_foreign_filter`: our fill without a client id and with an unknown order id is refused; after `OrderAck` it is accepted.

**Consequence (fail-safe but costly):**
- Nothing re-delivers the WS copy.
- The REST copy is picked up by the next periodic or confirming fills read once the order id is known: within 60 s, or at the positions check about 5 s after the mismatch shows.
- In that window the strategy quotes on a wrong inventory. Worst case, the positions check halts all trading on a false "mismatch".
- Each drop also logs a misleading "does this key see another system's activity?" error.

With a **proven** restricted key, this filter only protects against a scenario Kalshi already excludes server-side. So it adds risk without adding protection in the deployed configuration.

**Fix:**
- Do not drop an unknown-id fill on a restricted-key session. Park it (for example for 5 s, or until the next `OrderAck` or `user_order` names its order id), then deliver or drop.
- Alternatively, on an unknown `order_id` do `GET /portfolio/orders/{id}` (subaccount-scoped by the restricted key) and accept if its `client_order_id` has our prefix.
- Also add the `client_order_id` presence to the `WsFillProbe` verify_live check.

### LOW

**NEW-2. Watchdog liveness is not watchdog capability.**
- `scripts/watchdog.py` never exercises its key. Its beat stays fresh even if the key is wrong or revoked, lacks permissions, or the network is down, so the runner trades behind a watchdog that cannot cancel.
- The beat is also written after every `step()`, even when `step()` raises every time.

Fix:
- At startup, do `GET /portfolio/balance?subaccount=<n>` plus the subaccount-0 403 probe, then repeat a cheap read every few minutes.
- Put `rest_ok` and `last_step_ok` in the beat, and have `watchdog_beat_problem` require them.

**NEW-3. A watchdog beat from the future counts as fresh.**
`watchdog_beat_problem` tests `age > max_age` only (`monitor.py`), so a negative age passes. Probe `test_future_beat_accepted`: a beat 1 h in the future is accepted.

Triggers: a watchdog wall clock ahead of the runner, or a beat written just before an NTP step back. After a macOS sleep the anchored clock lags wall time, so a *dead* watchdog's last beat also looks future. The clock gate blocks in that case, but the watchdog gate alone would not.

Fix: use `abs(age) > max_age`. The same pre-existing issue applies to the watchdog's runner-heartbeat freshness (`now - t <= stale`).

**NEW-4. `account_setup` "listed" evidence can match an EARLIER identical transfer.**
`scripts/account_setup.py` `transfer_evidence` / `Evidence.verdict`.

Scenario:
1. T1 completes.
2. The operator sends T2 with `--again` within 120 s (`TRANSFER_LIST_SKEW_S`).
3. T2's outcome is unknown and it was not applied.
4. Any unrelated balance change on subaccount 0 (for example a System 2 settlement) makes the balance verdict `mixed`.
5. T1's list row matches, so the verdict is `applied`.
6. T2 is completed and a $150 withdrawal that never happened is declared to System 2 (probe `test_N1_listed_false_positive`: rc 0, `required: True`). System 2's cash parity then fails closed.

This needs `--again` plus balance noise, hence LOW.

Fix:
- Store the matched `transfer_id` in each completed record and exclude already-claimed ids.
- Shrink the skew to a few seconds.
- Never return `applied` on `mixed` alone; require `applied` balances, or a listed row not claimed by another record.

**NEW-5. The scoped cancel-all inside a venue task waits on itself.**
`venue_kalshi.py` `scoped_cancel_all` → `wait_idle(wait_s)`.

On the kill and halt paths, `cancel_all_now` runs inside a venue task, so `wait_idle` includes itself and always runs its full 2 s per round (probe: 2.0 s for one round). With orders that keep resting (for example during an exchange pause):
- the kill task takes about 9 s,
- shutdown's own scoped pass takes about 10 s,
- the `stopping` heartbeat stops at 10 s, so the watchdog fires "shutdown hung" at 15 s.

That is correct when orders are really stuck, but it slows the first scoped round on every kill.

Fix: exclude `asyncio.current_task()` from `wait_idle`.

**NEW-6. Minor.**
- The key proof accepts **401** as proof of restriction. 401 is an authentication failure, not a scope refusal. It is unlikely here, since the start-up already read subaccount 1 with the same key. Accept 403 only.
- On a `mismatch` shard-transfer result, the System 2 declaration amount is taken from all subaccount-0 row changes since `rows_before`, so unrelated settlement noise enters it. The ALARM tells the operator to stop.
- `--state-file` / `DH_ACCOUNT_SETUP_STATE` can point a run at an empty state and bypass idempotency. That is an operator choice.

---

## Adversarial checks with no new hole found

**Scoped cancel-all as the only kill path (shared account):**
- Pagination: the `iter_orders` cursor walk is followed (probe: 2 pages, then an empty list). A repeated cursor raises, which fails closed: start-up exit 2, shutdown exit 3 with the watchdog taking over.
- Rounds are bounded (`cancel_rounds` 3, plus a final list; the watchdog does the same).
- Orders in flight during the loop:
  - the group trigger (`latch_kill`) cancels the group's orders or rejects them on arrival;
  - shutdown waits for in-flight writes (`wait_idle`) before its own scoped pass;
  - the 120–150 s expiry remains the backstop.
- Unknown shards: the order's own `exchange_index` is learned from the REST row; otherwise -1 with the ticker, which the guard allows for reducing writes only.
- Every item names subaccount 1.

**Watchdog beat gate:**
- It cannot deadlock the kill path: only new orders are gated; cancels, group trigger, kill file and shutdown are independent.
- A beat naming another subaccount, `EXITED`, a stale beat, or a watchdog not armed on this pid/session after 10 s all close the gate.
- A foreign watchdog on another heartbeat file writes next to that file, not ours.
- The watchdog locks only onto heartbeats naming its own subaccount.
- It refuses to run without its own key on a shared account (unless `allow_runner_key`).

**Restricted-key proof:** an unrestricted key gets a 200 on the subaccount-0 read and is refused. A transient error refuses rather than falling back. A key restricted to another subaccount already fails the subaccount-1 balance read. `key_restricted_to_subaccount` is mandatory when `shared_account` is true.

**Previous-session fills:** an unknown-order REST fill with `ts_exch < started_ns` is skipped (its position was read at start and its event excluded). A WS fill with an older session's `dhm1-` prefix passes the prefix test. Replay mirrors the filter by building `known_oids` from the same events in the same order.

**Fail-closed config:** I found no live path that reaches subaccount 0 by default:
- `VenueCfg.sub` still maps None to 0, but every consumer refuses unset values first: live mode, `KalshiVenue`, watchdog, tools. `run_live.py`'s example fallback is `mode: paper` with subaccount 1.
- A YAML string, float or bool for `subaccount` or `shared_account` is refused.

**Account setup cannot send twice:**
- A retry reuses the same client id. Any answer to a retry keeps it.
- A completed identical transfer needs `--again`.
- Shard-transfer stays blocked while anything is pending, and needs a probe first.
- Declarations are only built for evidence-`applied` transfers (NEW-4 is the one false-positive route) and are bracketed from the first attempt's reading.

**Clock slew:** monotonic and strictly increasing; the offset gate still sees steps; replay does not depend on the clock.

**Repo configs:** `config/live.yaml`, `paper.yaml` and `live.example.yaml` state subaccount 1, `shared_account: true`, `key_restricted_to_subaccount: true`, `allow_primary_account: false`, `allow_runner_key: false`.
