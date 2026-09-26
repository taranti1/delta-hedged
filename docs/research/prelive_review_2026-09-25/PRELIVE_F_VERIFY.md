# Pre-live verification of F1-F3 (fix a6fdadd, merged at 3eb8c44)

**Scope:** `git diff c94df1e a6fdadd`.

**Method:** reviewer only, offline, no edits or commits.
- Repo suites `tests/live` + `tests/kalshi` at 3eb8c44: **637 passed**.
- New probes in `scratchpad/review/`, all pass:
  - `test_fverify_park.py`: 3 probes.
  - `test_fverify_watchdog.py`: 11 probes.
- Earlier probes re-run: the ones that demonstrated F1 and F3 now fail their old "bug present" assertions, as expected.
  - `test_final_park_loop`: the fill is now delivered.
  - `test_backward_wall_step...`: the watchdog no longer fires.
  - `test_future_beat_rules` also fails its old assertion, because the beat must now include `api_write_ok`.

## Verdict

F1, F2 and F3 are closed. I found **no CRITICAL, HIGH or MEDIUM issue** in the new code.

Remaining:
- One LOW item that affects analytics only.
- One LOW efficiency item.
- Two assumptions about Kalshi's behaviour to confirm on the first live start. If they are wrong, the system fails closed.

---

## F1: the unknown-order loop. **Closed.**

### What the fix does
1. After an order's FIRST park timeout, a REST fill of that order is delivered instead of being parked again. This applies only when all of these hold:
   - live mode;
   - a key restricted to our subaccount (proven by the start-up 403);
   - `subaccount != 0`;
   - `source == "rest"`.

   The fill is delivered as `dhm1-orphan-<oid>` and recorded.
2. From the first timeout on, that order's market takes part in the position-mismatch confirmation again.
3. The `unknown_order_max_park_cycles`-th timeout (default 3) of the same order halts trading. The halt is `Halt(all)` with reason `unknown_order_loop`: persisted, sticky, and recorded through the RiskStateSeed.
4. Live mode refuses `unknown_order_park_s >= 3 × position_confirm_s`.

### Probes
- **Restricted key, 1 s run** (`test_orphan_delivered_once_then_late_ack_no_double`), with park 0.05/confirm 0.02 and 0.10/0.05:
  - the fill is delivered exactly once, as `dhm1-orphan-o-x`;
  - position becomes 100;
  - the order gate reopens (no reasons left) and there is no halt;
  - one park timeout, nothing left parked.
- **Same run, then a late ack plus WS and REST copies:** still exactly one fill, position still 100.
- **Replay:** delivers the same fills.
- **Configuration without the orphan route** (`test_unrestricted_key_loop_halts`): a subaccount-0 non-shared account with an unrestricted key. After 3 park cycles the runner raises `halt:all` and no fill is delivered. The old loop can no longer run forever.

### Could a System 2 fill ever be delivered as an orphan? No.
The orphan route requires all four of these:
- a REST fill, read with `GET /portfolio/fills?subaccount=<ours>`;
- that passes `row_in_subaccount`, so a row naming another subaccount was already dropped;
- `key_restricted` (the start refuses unless the key is proven by a 403 on subaccount 0);
- `subaccount != 0`.

System 2 lives on subaccount 0, and a restricted key cannot read it (server-side scoping). WS copies never take the orphan route; they are re-parked and then time out.

A manual UI order on subaccount 1 is looked up first. Its client id lacks our prefix, so it is proven foreign and dropped before any timeout. It never becomes an orphan, and the mismatch path halts as before.

### Can a fill be booked twice when the ack arrives late? Not in inventory. It is double-counted in the P&L ledger tool (LOW L1 below).
- The fill ids go into `fills_seen` (`_pre_event`). Late WS and REST copies are dropped at the door or in `_flush_release`.
- The OrderManager moves position, cash and fees only in `_on_fill`. Attaching the orphan to the order on the late ack (`_apply_fill`) adjusts only the order's own counters.
- Probe: position stays 100 after the late ack and both late copies.

### Replay exactness: exact
- The orphan fill is recorded on `events.live` with the `dhm1-` prefix, at the time of the fills read that carried it.
- In replay the raw copies are dropped as unknown, exactly as live dropped them.
- The live side adds the order id to `known_oids` on release, and replay adds it on the recorded event, so later events of that order resolve identically.
- The loop halt goes through an injected `RiskStateSeed`, so replay halts at the same point.

---

## F2: the watchdog's write probe. **Closed.**

### What it sends
`rest_write_probe` sends `DELETE /portfolio/events/orders/<uuid4>?subaccount=<n>&exchange_index=<first configured shard>` through the watchdog's scoped write client.

Probe `test_write_probe_ids_fresh_and_scope`:
- 50 calls produce 50 distinct ids.
- The path always has an id segment, so it is never the bulk path.
- `exchange_index = -1` is refused (a ValueError).
- A probe built for subaccount 0 is refused by the client guard before sending (UnscopedWriteError).

### Can it hit a real order? Practically no.
- The id is a fresh random uuid4 each call (122 random bits).
- Even on a collision the request is limited to subaccount 1 (the watchdog's configured subaccount, never 0 on the shared account: config plus guard).

### Can it touch subaccount 0? No.
- `venue_scope_problems` refuses subaccount 0 on a shared account.
- The client's `write_subaccount` guard refuses any other subaccount.

### Does anything other than 404 count as success? No.
Probe `test_write_probe_outcomes`:

| Response | Result |
|---|---|
| 404 | OK |
| 403, 401, 400 | `WriteProbeError` |
| 500 (unknown outcome) | `WriteProbeError` |
| 200 with a body | `WriteProbeError` plus a CRITICAL log |
| 204 | `WriteProbeError` plus a CRITICAL log |

### Other checks, all correct
- The runner requires `api_write_ok is True` (`watchdog_beat_problem`), and the old beat without it is now rejected.
- `api_ok` means read AND write.
- A successful cancel-all no longer sets `api_ok` unless the write probe has succeeded.
- A failed write probe is retried every `api_probe_interval_s`.
- Live mode validates `api_write_probe_interval_s` to be in [read interval, 3600].

### To confirm on the first live start (fails closed if wrong)
- **V1: what Kalshi answers for a non-existent order.** The spec says a cancel of a non-existent id is 404. If it answers 400 instead, `api_write_ok` stays false and the runner refuses to start. That is safe, but it blocks going live.
- **V2: whether a key without write scope gets 403 rather than 404.** The probe's evidence assumes Kalshi checks scope before it checks existence. If it checked existence first, a read-only key would get 404 and pass. Check once with a read-only key restricted to subaccount 1: expect 403. The keys `create-key` makes always have `read` + `write`, so this matters only for a hand-made key.

---

## F3: backward wall-clock step. **Closed.**

A NEW heartbeat (`t != prev_raw`) of the SAME watched runner (same pid and session), fresh on the new clock, resets the stored heartbeat time. With no such heartbeat within `stale_s` of first noticing the future-stamped time, the watchdog fires.

Probes:
- **Runner alive, 3 s backward step** (`test_backward_step_alive_no_fire`): no cancel-all, state ARMED, `back_steps == 1`.
- **Runner dead at a 3 s or 60 s step** (`test_backward_step_dead_runner_fires`): fires 2.25 s after the step, which is `stale_s` plus one poll. A dead runner writes no new heartbeat, so the reset can never hide it: re-reading an unchanged file gives `t == prev_raw`, which is not a reset.
- **Steps smaller than `max_future_s`:** the stored maximum lags by at most the step, so detection of a death is delayed by at most about 2 s and nothing fires spuriously.
- **1.5 s skew probe** (from the previous check): still no fire.

---

## Findings

### LOW L1: an orphan fill attached by a late ack is counted twice in the session ledger
**Inventory is not affected.**

The OrderManager emits `orphan_fill` for the orphan and then `fill` when the late ack attaches it (repo test `test_orphan_fill_without_client_id_attached_on_ack`: `["accepted", "fill"]`). `MarketMaker` turns both kinds into `Log("fill")` (`mm.py`, `k in ("fill", "orphan_fill")`). `dh.live.replay.ledger_from_log` books every `log.fill` line.

So `python -m dh.live.tools ledger` (P&L attribution, markouts) counts that fill twice, and `stats.fills` counts it twice. Position, cash and fees in the OrderManager are counted once. This existed before for WS fills racing the ack; the F1 orphan path makes it somewhat more reachable.

**Fix:** skip the "fill" log on attach when an orphan log was already emitted, or have the ledger de-duplicate by trade id.

### LOW L2: the lookup's list fallback scans the ticker's whole order history
`KalshiVenue.find_order` runs `iter_orders(subaccount, ticker)` with no `min_ts` and no status filter, and stops at the first match. It is called on every lookup retry (every `unknown_order_lookup_retry_s`, default 2 s) while an order is parked, and once by the session check.

For a market this runner has quoted all day, that can be several 1000-row pages per retry. Cost is bounded by the park duration (at most 10 s per order) and by the process's rate limiter, but it spends the shared read budget, which can delay the runner's own fills and positions reads during a park.

**Fix:** pass `min_ts` (for example the session start or the parked event's time minus a margin). Walking newest-first already makes the first page likely.

---

## Checked and correct
- Orphan delivery is limited to REST fills of our subaccount under the proven restricted key, never on subaccount 0, and never for an order already proven foreign.
- The loop halt fires at exactly the configured number of cycles, is sticky and persisted, and later events of that order are dropped without re-parking.
- The `park_s < 3 × confirm_s` rule is enforced in live mode, so the gate cannot reopen while an event is parked for the first time.
- The one-time GET-by-id check (`verify_live get_order_by_id_finds_shard_orders`) is read-only and informational.
- The write probe cannot reach subaccount 0 or the bulk endpoint, only 404 counts as success, ids are fresh each call, and the result is required by the runner.
- The backward-step reset needs a new trusted heartbeat from the same runner; a dead runner still fires within `stale_s`.
- The config, venue-scope and guard protections from earlier rounds are unchanged; the full suite is green.
