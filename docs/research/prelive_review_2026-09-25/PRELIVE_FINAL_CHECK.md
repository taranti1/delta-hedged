# Pre-live final check: fixes for NEW-1..NEW-6

**Scope.** The fixes are commit 560852e, merged at 010a6f4 (`git diff 0f540a6 560852e`).

**How this was checked.** Offline and read-only: no Kalshi calls, no repo edits, no commits.
- Repo suites `tests/live` + `tests/kalshi` at 010a6f4: **608 passed**.
- Probes in `scratchpad/review/`:

| Probe file | What it checks | Result |
|---|---|---|
| `test_final_runner.py` | 8 probes: parking, owner verdicts, key proof, future beats | all pass |
| `test_final_account_setup.py` | earlier transfer probes A1–A5 plus NEW-4 | 6/6 pass (NEW-4 now asserts the fixed behaviour) |
| `test_final_watchdog.py` | 3 probes: future-stamp rules, API probe | pass (2 demonstrate findings F2 and F3) |
| `test_final_park_loop.py` + `test_trace_park.py` | unknown-order loop | pass (demonstrate F1) |
| `test_rereview_runner.py` (old) | earlier probes of NEW-3, NEW-5, NEW-6 | those 3 now FAIL their old assertions: the behaviour changed as intended |

**Bottom line.**
- NEW-1..NEW-6 are closed. I re-probed each fix against the code itself, not only through its new tests.
- The parking mechanism never delivers a fill twice and replays exactly.
- On a subaccount-restricted key it cannot release a System 2 fill.
- There is one MEDIUM gap (F1) in how parking handles an order that can never be proven: it stalls without escalating, and can trade on a wrong position if the config is changed.
- There are two LOW items and one item to verify live.
- Nothing is CRITICAL or HIGH.

---

## NEW-1..6: re-probed

| Finding | Status | Evidence |
|---|---|---|
| NEW-1: our fill dropped as foreign | **closed** | See below. |
| NEW-2: watchdog liveness is not capability | **closed (read capability only)** | The beat now carries `api_ok`, `api_ok_ns` (at most 180 s old), `step_ok` (false after 3 failed polls) and `api_error`. The runner refuses to start, or closes the gate, when these are missing or stale. Limitation: F2. |
| NEW-3: future beats counted as fresh | **closed** | Runner side: a beat more than 1 h in the future is refused; one 1 s in the future is accepted. `api_ok_ns` from the future is refused. Watchdog side: a runner heartbeat more than 2 s in the future is never trusted. Side effect: F3. |
| NEW-4: "listed" evidence matched an earlier identical transfer | **closed** | Old probe N1 (T2 sent with `--again` inside the skew, plus unrelated balance noise): now rc 1, T2 stays pending, nothing is declared. Rules now: `applied` needs exact deltas on the route; at most one unclaimed listed row; claimed ids are stored; skew is 5 s; rows before the last identical completion are excluded. A1–A5 still pass (single move, correct declarations). |
| NEW-5: scoped cancel-all waited on itself | **closed** | Old probe: kill round took 2.0 s, now 0.0003 s. Each round waits only for its own cancel tasks. Pagination is still followed and the bulk DELETE is never sent. |
| NEW-6: minor items | **closed** | 401 no longer counts as proof (probe: 200, 401, 503 are refused; only 403 passes). A `mismatch` shard-transfer declares the listed or requested amount instead of the noisy subaccount-0 delta. `--state-file` / `DH_ACCOUNT_SETUP_STATE` are refused when they lack records of the per-user file (override: `--i-know-state-file`). |

**NEW-1 in detail.** A fill with no client_order_id, for an order id not yet known, is now parked rather than dropped.
- Probe `test_ws_and_rest_copies_parked_released_once_and_replay`: a WS copy and a REST copy of the same fill both arrive before the ack.
  - Both are parked (`_parked_n == 2`).
  - After the ack, exactly ONE fill is delivered; the position becomes 100.
  - Late WS and REST copies are dropped.
  - Replay delivers the same fills.
- `test_foreign_by_lookup`: a System 2 order found by lookup (other client id) is dropped. Inventory is untouched and there is no pause.

---

## Parking mechanism: adversarial checks

### Double delivery: none
- The WS copy is keyed `trade|` and the REST copy `trade|fill`, so both can be parked.
- `_flush_release` checks `fills_seen` for each item, and `_pre_event` adds both ids. The second copy of a batch is therefore dropped.
- A copy arriving after the release is caught at the door (`fills_seen` / `_backfill_fills`).
- Probe confirms: 2 parked, 1 delivered, 2 late copies dropped.

### WS reconnect and restart: nothing lost
- The park lives in the runner, not in the WS session.
- The reconnect reconciliation's REST copies are de-duplicated by `_parked_ids`.
- A park that times out is re-derived from REST by the `unknown_order` reconciliation.
- At shutdown, parked events are logged. The next session reads positions at start-up and excludes that event.

### Replay: exact
- The raw copy is dropped in replay (its order id is unknown there too; `known_oids` is built from the same events in the same order).
- The released copy is recorded on `events.live` with the order's client_order_id, at the time of the item that proved it ours, so replay delivers it at the same point.
- Verified for release by ack (repo test and my probe) and by lookup (repo test).

### Can a lookup release a System 2 fill?
**Restricted key: no.** `order_row_owner` requires our `dhm1-` client-id prefix, whatever the subaccount field says. A System 2 row (no `subaccount_number`, client id `sys2-…`) is `foreign`. An empty client id is `foreign`. `test_lookup_verdicts` covers all five cases.

**Unrestricted key: cannot occur on the shared account.** Live mode refuses `shared_account: true` without `key_restricted_to_subaccount: true`, and the start-up 403 proof rejects an unrestricted key. Even so, `order_row_owner` treats a row without a subaccount field as foreign when the key is not restricted, and the subaccount rule in `push` drops primary-account messages before parking. On a non-shared account on subaccount 0 (explicit opt-in), the prefix alone decides, which is correct.

### MEDIUM F1: an order that can never be proven ours loops forever and never halts

**Where:** `runner.py` `_park_timeout` → `_unknown_dropped` → `_reconnect_reconcile(reason="unknown_order")` → `_backfill_fills` → `_unknown_order_event`, plus `_check_positions` (`position_confirm_parked`).

**Scenario.** Our fill's order stays unknown: no ack, the create reconciliation does not find it, and `GET /portfolio/orders/{id}` keeps answering 404. Then:
1. The park times out and the fill is dropped; the `unknown_order` reconciliation re-reads fills.
2. The REST copy (a real fill of subaccount 1) is **parked again**, because `_unpark` cleared its id from `_parked_ids`.
3. A new lookup, a new timeout, and so on: forever.
4. Meanwhile `_check_positions` skips confirmation for any ticker that has a parked event, so the position mismatch that would halt never confirms.

**Outcome depends on timing:**
- **Default timings** (`unknown_order_park_s` 10 s < 3 confirm rounds × `position_confirm_s` 5 s): the reconciliation never ends, so the `reconciling` gate stays closed forever.
  - Probe with the same ratio (park 0.10 s, confirm 0.05 s) and park 0.05 / confirm 0.02 s: gate closed 100% of the time after the first drop; 26 and 55 drop cycles; 0 fills delivered; 0 position snapshots; no halt.
  - Result: a permanent, silent stall. Quoting is off, which is safe, but nothing escalates beyond an ERROR log every 100 drops and a `GET /portfolio/orders/{id}` every 2 s.
- **`park_s` larger than about 3 × `position_confirm_s`** (not validated): the reconciliation ends between cycles and the gate reopens.
  - Probe (park 0.30 s, confirm 0.05 s): gate open 42% of the time.
  - Result: the strategy quotes on a position that lacks the fill, and the mismatch halt is disabled for that ticker.

**Why it is plausible.** For shard-2 orders, `GET /portfolio/orders/{id}` (no subaccount or exchange_index parameter) may simply never find them. The spec's cancel endpoint says "An order_id alone cannot identify the exchange shard." If so, the lookup path can never prove a shard-2 order ours, and the only release routes are the ack and the create reconciliation. The trigger is still narrow: it needs an order that neither the ack nor the list-based reconciliation (`find_created` with its tombstone rechecks) ever identifies. Hence MEDIUM, not HIGH.

**Fix (any of):**
- Deliver instead of re-parking. A REST fill fetched with `GET /portfolio/fills?subaccount=1` under a proven restricted key is subaccount 1's fill: after the first park timeout, deliver the REST copy as an orphan fill, not re-park it.
- Or stop exempting the ticker from mismatch confirmation after the first timeout, so the mismatch halts.
- Or count park cycles per fill id and halt after N.
- Also validate `unknown_order_park_s < 3 × position_confirm_s` in `live_config_problems`, or make the confirmation independent of the park.
- Replace or supplement the lookup with the list `GET /portfolio/orders?subaccount=1&ticker=…` (which supports `exchange_index`). Add a `verify_live` check of `GET /portfolio/orders/{id}` on a shard-2 order.

---

## Watchdog API probe (NEW-2 fix)

**LOW F2: the probe proves read access only.** `rest_api_probe` is `GET /portfolio/orders?subaccount=1&status=resting&limit=1`.
- **Would pass while cancels fail:** a key without the `write` scope (for example one created in the web UI or with read-only scopes). Probe `test_api_probe_passes_with_a_read_only_key`: the read succeeds while the batch cancel returns 403.
- **Would correctly fail:** a key restricted to another subaccount (the read returns 403); a revoked key (401); no network.
- **Undetectable by any probe:** during an exchange pause, cancels are rejected anyway.
- **Why only LOW:** `create-key` always requests `["read","write"]`.
- **Fix:** add a harmless *write* capability check at start and periodically: `DELETE /portfolio/events/orders/{random uuid}?subaccount=1&exchange_index=2`. A 404 means authorized; a 403 means no write scope; it costs 2 write tokens. Alternatively, check the key's `scopes` in `GET /api_keys` when it is listed.

**Correct:**
- The runner requires `api_ok is True` and an `api_ok_ns` no older than 180 s and not in the future.
- A successful cancel-all refreshes `api_ok`.
- The probe is bounded by `api_probe_timeout_s`.
- Config limits are validated in live mode.

---

## Future-stamp rules (NEW-3 fix): spurious firing?

**Normal operation: no.** Runner and watchdog on one host stamp and read heartbeats with the same `time.time_ns()`, so a heartbeat is never in the future. The runner's slewed session clock stays within milliseconds of wall time (3 ppm absorbed), far below the 2 s tolerance.
- Probe `test_same_host_small_skew_never_fires`: 100 s of polling, even with heartbeats stamped 1.5 s ahead; the watchdog stays ARMED with 0 cancels.

**LOW F3: a backward wall-clock step of more than 2 s fires the watchdog on a healthy runner.**
- Why: `st.last_hb_ns` is a running max, so after the step the stored value lies in the future. The age check `age < -max_future_s` then triggers even though fresh heartbeats keep arriving.
- Probe `test_backward_wall_step_fires_even_though_heartbeats_stay_fresh`: 1 cancel-all, and a marker naming the live runner. The runner then halts with a sticky halt, and the operator must restart.
- Impact: fail-safe, and rare on macOS (`timed` slews small offsets). The runner's own clock gate would block new orders for hours after such a step anyway (slew 50 ppm).
- Before the fix, the same step blinded the watchdog instead, which was worse.
- Fix if wanted: when a fresh, trusted heartbeat from the watched runner arrives after a backward step, reset `last_hb_ns` to it instead of firing. Fire only when no trusted heartbeat is newer than `now - stale_s`.

---

## Other things verified correct
- **`wait_idle` / `_wait_tasks`:** never waits on the calling task. Shutdown and in-flight create handling are unchanged. The kill takes about one RTT per round.
- **Parking bounds:**
  - `unknown_order_park_max` (1000) evicts the oldest order through the same timeout and reconcile path.
  - The park config is validated live (`park_s > 0`, `retry > 0`, `0 ≤ delay < park_s`).
  - A parked ticker is exempt only from *confirmation*; other tickers reconcile normally.
- **`_recon_gen_of`:** the `unknown_order` reconciliation has its own generation, so a WS disconnect cannot orphan it, and vice versa.
- **Fail-closed config:** no live path reaches subaccount 0 by default. The watchdog, venue and live checks are unchanged from the re-review.
- **`account_setup`:**
  - A retry never drops a saved id.
  - Every declaration is built only for exact route deltas, and is bracketed from the first attempt.
  - Claimed transfer ids prevent re-use.
  - State overrides are refused unless they are complete.
