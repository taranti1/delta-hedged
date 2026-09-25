# Operations runbook: recorder, paper trading, live trading (M1)

Practical steps for running the system on a real host. Everything here was built and tested
offline; the steps marked **verify live** have never touched the real API and must be checked
the first time. Commands run from the repository root with the venv active
(`. .venv/bin/activate`).

| Process | Command | Restart policy |
|---|---|---|
| Recorder (market data capture) | `scripts/record.py` | always |
| Strategy runner, paper or live | `scripts/run_live.py` | **never automatic**: a human restarts it |
| Watchdog (dead-man cancel-all, live only) | `scripts/watchdog.py` | always |
| Prometheus / Grafana (optional) | `deploy/docker-compose.yml` | always |

Exit codes of `run_live.py`: `0` normal stop or kill file, `2` refused to start (message says
why), `3` shutdown could not confirm that all orders are cancelled (check the Kalshi UI now; the
watchdog keeps trying), `4` strategy/consumer error (fail-safe stop).

One runner per `paths.data_root` and per heartbeat file: the runner holds a `flock` on
`<data_root>/runner.lock` and `<heartbeat>.lock` and refuses to start while another process
holds them, or while the heartbeat file is fresh from another pid. A restart keeps the day's
risk state (section 8): a halt stays a halt, the daily-loss budget is not refilled.

---------------------------------------------------------------------------------------------
## 1. One-time setup

### 1.1 Host
* One VM in AWS us-east-1, Python 3.11, `chrony` synced to Amazon Time Sync
  (`chronyc tracking`: offset well under 1 ms).
* `uv venv .venv && . .venv/bin/activate && uv pip install -e '.[dev]'`
* `python -m pytest -q` (all offline; must pass).
* Runtime directory for the kill file and heartbeat (tmpfs, recreate after every reboot):
  `sudo mkdir -p /run/dh && sudo chown $USER /run/dh`
  (permanent: a systemd-tmpfiles line `d /run/dh 0755 <user> <group> -`).

### 1.2 Kalshi account and API keys
1. Use a **dedicated account or subaccount** for the bot. Cancel-all, the ghost-order sweep and
   the position reconciliation act on every order/position of that (sub)account: manual
   trading there will be cancelled and will halt the bot.
2. In the Kalshi web app: account settings, **API keys**, create a new key. Kalshi generates
   an RSA key pair: download the private key file (shown **once**) and copy the **Key ID**.
3. Create a **second key for the watchdog**, so a revoked/rate-limited runner key does not
   disable the dead-man switch.
4. Store keys outside the repository, readable only by you:
   `mkdir -p ~/.kalshi && chmod 700 ~/.kalshi && mv <download>.pem ~/.kalshi/runner.pem && chmod 600 ~/.kalshi/*.pem`
   (`*.pem`, `*.key`, `secrets/` and `.env` are git-ignored; never commit a key).

### 1.3 Environment variables
Put these in an env file (e.g. `/etc/dh/env`, `chmod 600`) loaded by your shell or service:
```sh
export KALSHI_KEY_ID=<runner key id>
export KALSHI_PRIVATE_KEY_PATH=$HOME/.kalshi/runner.pem
export KALSHI_WATCHDOG_KEY_ID=<watchdog key id>
export KALSHI_WATCHDOG_PRIVATE_KEY_PATH=$HOME/.kalshi/watchdog.pem
```

### 1.4 Configuration files
```sh
cp config/kalshi.example.yaml config/kalshi.yaml   # env: prod | demo, REST/WS settings
cp config/live.example.yaml   config/live.yaml     # runner: mode, paths, venue, universe...
```
* `config/m1.yaml` holds the strategy (sizes, limits, timers). Do not edit it casually: its
  digest is written on every log line and every session record.
* `config/live.yaml`: keep `mode: paper` until section 5. Set `venue.subaccount` if you use a
  subaccount (`null`/`0` = the primary account). The subaccount is sent **explicitly on every
  request**, `0` included: Kalshi reads an omitted subaccount as "all subaccounts" on
  `GET /portfolio/orders`, `GET /portfolio/fills` and cancel-all. Fills, order updates and
  positions of other subaccounts arriving on the (account-wide) private WebSocket channels
  are dropped (`dh_foreign_subaccount_events_total`). `paths.data_root` (default `data/live`)
  must differ from the recorder's root: two processes writing `kalshi.ws` into one store would
  interleave two connections' sequence numbers.
* `paths.heartbeat_file` is the LIVE runner's heartbeat (the watchdog reads it); a paper runner
  writes `paths.paper_heartbeat_file` (default: the same name with `.paper` inserted,
  `/run/dh/heartbeat.paper.json`), so it can never be mistaken for the live runner.
* `paths.risk_state_file` (default `<data_root>/state/risk_state.<mode>.json`): the day's P&L,
  a carried halt and pause, written every 2 s, on every halt and at shutdown (section 8).
* **External venues stay off in M1** (`feeds.only: []`). This is deliberate: M1 prices and
  detects jumps on BRTI alone, so external books add load on the single strategy consumer
  without protecting against the failure that matters (a lagging or frozen BRTI relay). With
  no external feeds the risk engine's ">= 2 fresh external venues" rule is **off**; staleness
  is judged on BRTI itself, by receive age AND CF source age (quoting stops at 10 s, and at
  3 s for markets within 10 minutes of expiry). If you enable feeds later, use trade/ticker
  channels, never full order books, and remember that `dh/feeds` readers do not yet yield to
  the event loop per frame.

---------------------------------------------------------------------------------------------
## 2. Pre-flight checks (new host, new key, after upgrades)

| Check | Command | Pass |
|---|---|---|
| Kalshi REST + WS + BRTI | `python scripts/smoke_kalshi.py --seconds 60` (`--demo` for demo) | all 8 checks PASS (status, limits, KXBTCD specs and fee type, orderbooks, WS subscriptions, WS books equal REST snapshots, BRTI rates and latency, no gaps) |
| External venues | `python scripts/smoke_feeds.py --seconds 60` | every enabled venue PASS |
| Benchmark back-fill (**verify live**) | `python -m dh.live.tools backfill` | `"ok": true`, coverage >= 0.9 |
| Resting orders | `python -m dh.live.tools orders` | `0 resting orders` (exit 0) |
| Fee schedule | `python scripts/verify_fee_schedule.py --days 14` | series fee types supported; once the account has fills: every fill matches (sets `fees.balance_precision_dollars`) |
| Clock | `chronyc tracking` | offset < 1 ms |

**Back-fill**: the fair-value model needs one half-life of every volatility EWMA, the longest
being 1 day, before it quotes. At start-up the runner fetches 2 days of BRTI through Kalshi's
CF Benchmarks passthrough, in hourly chunks, newest first:
`GET /trade-api/v2/cfbenchmarks/history/values?id=BRTI&timespan=3600s&timestamp=<chunk end ms>`
and down-samples it to one print per minute. The passthrough's parameter formats are not in
Kalshi's openapi spec: if `tools backfill` returns no ticks, adjust `backfill.timespan` /
`backfill.timestamp` in `config/live.yaml` (placeholders `{start_ms} {end_ms} {start_s} {end_s}
{span_s} {span_ms}`, extra query parameters in `backfill.extra_params`) until it does. Without
the history the runner still starts, logs `fair-value model NOT ready`, and **does not quote
until it has seen >= 1 day of live BRTI ticks** (metric `dh_fv_ready` = 0).

---------------------------------------------------------------------------------------------
## 3. Recorder (M1.0)

```sh
python scripts/record.py --config config/feeds.yaml
```
Run it as a service for at least 7 days before trusting any analysis (BUILD_PLAN M1.0: fewer
than 0.1% sequence gaps). It writes `data/raw/<stream>/<date>/<hour>.jsonl.zst` and never
trades. The runner records its own session store separately (`paths.data_root`).

---------------------------------------------------------------------------------------------
## 4. Paper trading (M1.4): the strategy against the live book, no orders

```sh
python scripts/run_live.py --config config/m1.yaml --live-config config/live.yaml --mode paper
```
What happens at start-up (each step is logged; `exit 2` means a step refused):
1. configs loaded, mode resolved; single-instance locks taken, kill-file directory checked,
   heartbeat `starting` written (paper: the paper heartbeat file);
2. REST client with the account's rate limits; exchange status;
3. the day's risk state: the paper state file (paper never reads the account's fills) ->
   `RiskStateSeed`, the strategy's first event (section 8);
4. market discovery: open KXBTCD markets expiring within `universe.horizon_s` (2 h: the
   current and next hour). Markets whose rules text fails the sanity check, or that are not
   open, are skipped (logged); unresolved/unsupported fee types stay untradable;
5. benchmark back-fill and fair-value warm-up (section 2);
6. the MarketMaker is built with this session's client_order_id prefix
   `<run_prefix>-<8-char token>` (ids never repeat across restarts; recorded in `meta`); the
   paper simulator (`policy: conservative`, latency from `paper.*`) replaces the order venue.
   **No order endpoint is ever called; the account's own fills/orders channels are not
   subscribed**;
7. WebSocket: order books + trades for the universe, market lifecycle, BRTI 1 Hz and 5 Hz;
8. the loop starts. Every 2 minutes new markets are discovered and added
   (`MarketMaker.add_markets`, recorded in the session's `meta` stream); settled markets
   are pruned.

Run paper for **at least 7 days** (restarts are fine: every session is a separate record).
Stop with Ctrl-C / `kill -TERM <pid>`.

### 4.1 Evaluate paper results (daily)
```sh
ls data/live_logs/                                                 # one JSON log per session
python -m dh.live.tools ledger --log data/live_logs/<session>.jsonl # P&L attribution
python -m dh.live.tools replay --log data/live_logs/<session>.jsonl # determinism check
```
`ledger` prints net cents per filled contract with an event-clustered 95% CI, markouts at
0.1 s to 60 s, fees, contracts/day and $/day (simulated fills, policy C). `replay` re-runs the
recorded session through the backtest runner and must print `"identical": true`; anything
else is a bug to fix before going live (it prints the first differing decision). Long
sessions take a while to replay: check one session per day.

Paper exit criterion (BUILD_PLAN K.1): after 7 days under policy C, the net c/contract CI
upper bound must be >= 0 in the segments you intend to trade; otherwise stop.

---------------------------------------------------------------------------------------------
## 5. Promotion to live at M1 size (M1.5)

### 5.1 Checklist (every item true, written down with the date)
- [ ] Pre-flight checks of section 2 pass on this host, with this key.
- [ ] 7+ days of paper: CI criterion met; `replay` identical on sampled sessions.
- [ ] Settlement convention check (BUILD_PLAN M1.2) passed.
- [ ] Fee check: `verify_fee_schedule.py` shows the KXBTCD fee type is supported and, if the
      account already has fills, every fill matches. (The runner also checks every live fill
      exactly, including Kalshi's per-order rounding, and blocks new orders on a mismatch.)
- [ ] Watchdog running with its own key; kill drill done (section 6) on the demo env or with
      the runner in paper mode + watchdog `--cancel-now`.
- [ ] `config/m1.yaml` unchanged since paper (same digest in the logs).
- [ ] The account/subaccount holds only the capital you accept to risk (M1 limits: daily loss
      halt $25, worst case $20 per event / $50 total, clips and per-market limits as in
      `config/m1.yaml`).

### 5.2 Configure and start
1. `config/live.yaml`: `mode: live` (and `venue.subaccount` if used). Live mode refuses
   `loop.strategy_error: continue`, `venue.exclude_events_with_positions: false` and
   `venue.startup_cancel_all: false` (paper-only debugging settings).
2. Start the watchdog first, in its own terminal/service:
   ```sh
   python scripts/watchdog.py --live-config config/live.yaml
   ```
3. Start the runner (both the config flag and the command-line flag are required):
   ```sh
   python scripts/run_live.py --config config/m1.yaml --live-config config/live.yaml \
       --mode live --i-understand-this-sends-real-orders --duration 3600
   ```
   Use `--duration` for the first sessions and stay at the screen.

Live start-up adds, in this order, before discovery:
1. **clean slate**: `DELETE /portfolio/events/orders?subaccount=<n>`, then the resting-order
   list must come back empty (leftovers are cancelled one by one, 3 rounds; still resting ->
   exit 2);
2. **positions** are read AFTER that (an order resting while positions are read could fill
   unseen): **events that already hold a position are excluded** for this session (they
   settle within the hour);
3. **the day's P&L** from Kalshi: `GET /portfolio/fills` and `GET /portfolio/settlements`
   since UTC midnight plus the positions, INCLUDING the excluded events (a lower bound: open
   positions at their worst case), combined with the persisted state -> the risk seed
   (section 8);
4. **new orders are held for `venue.cancel_all_hold_s` (60 s) after that cancel-all**: Kalshi
   documents that a cancel-all may also cancel orders placed during the following minute.
   The strategy is told (`kalshi.reconcile` stale) and does not quote until the hold ends.

After discovery the exchange **order group** is created (rolling 15 s fill cap from
`risk.order_group_limit_contracts`, auto-cancel). The runner refuses to trade if any of these
fail; a failure after the cancel-all leaves nothing behind (the order group is deleted, the
heartbeat says `stopped`, the locks are released).

Running paper and live side by side (e.g. to A/B a config change): give the paper runner its
own live-config copy with different `paths.data_root`, `paths.log_dir` and `metrics.port`
(the paper heartbeat file is separate automatically; the same data_root is refused by the
lock). Both may share the kill file (a kill stops both). Under Docker
(`deploy/docker-compose.yml`, `config/live.docker.yaml`) the paths point at the mounted
`/data` volume; `/run/dh` is shared with the watchdog container.

### 5.3 What the runner does while live
* Every order: post-only, `cancel_order_on_pause`, inside the order group, expiring after
  `order_expiry_s` (120 s, `config/m1.yaml`: the exchange-side backstop if host and watchdog
  both lose Kalshi), client_order_id `<run_prefix>-<session token>-<n>`. Several quotes
  decided together go out as one batched request; a quote that would wait more than
  `venue.max_place_wait_s` for rate-limit tokens is dropped (reason `rate_budget`) instead of
  arriving stale (requests already through the rate limiter are not counted twice).
* A create with unknown outcome (timeout, 5xx) is **never resent**: the runner looks the
  order up by client_order_id with the explicit subaccount (backoff 0.5 s .. 30 s), and only
  an order created at or after the request (minus `venue.create_match_skew_s`) counts, never
  an older order with the same id; still absent after 30 s -> declared rejected, and looked
  for again after 10 s and 30 s (`missing_recheck_s`): if it turns up resting it is fed back
  and cancelled (`venue.revived`). HTTP 409 on a create (id already used) is a definite reject.
* Cancels with unknown outcome are looked up and **re-sent for as long as the order still
  rests** (backoff capped at `venue.recancel_backoff_max_s`); after `venue.max_cancel_retries`
  the order is flagged STUCK (`venue.cancel_stuck` log, `dh_venue_stuck_cancels`) and the
  re-cancelling continues. A 404 on the order lookup counts only when the resting-order list
  confirms the order is gone. The strategy re-sends its own cancel every change timeout
  (`cancel_retry`), and the 30 s sweep re-cancels any order still resting more than the change
  timeout after its cancel (`cancel_resend` log).
* Every 30 s: positions from `GET /portfolio/positions` vs the strategy's fill-derived
  positions (the DISCREPANCY must persist 5 s unchanged, i.e. survive a fill in flight, before
  it halts; new fills moving both sides do not reset it), and resting orders vs the strategy
  (unknown resting orders are cancelled; orders the strategy believes live but that no longer
  rest are looked up). WebSocket position messages go through the same persistence check.
* **WebSocket reconnect**: fills and order updates sent during an outage are lost and the own
  channels have no sequence numbers, so no gap is ever reported. On every disconnect the
  strategy is told `kalshi.reconcile` stale (it cancels and stops quoting); after the
  reconnect (`venue.reconnect_settle_s`) the runner back-fills `GET /portfolio/fills` since
  the disconnect minus `fills_backfill_margin_s`, checks positions and resting orders, and
  only then sends `resynced` (a position still differing is confirmed first: halt, or clear).
  A fill whose `post_position` disagrees (a lost fill) pauses the strategy and triggers the
  same immediate reconciliation instead of halting on the spot; only a confirmed snapshot
  mismatch halts. Every `fills_backfill_interval_s` (60 s) the fills of the last few minutes
  are fetched anyway and those older than `fills_backfill_min_age_s` (10 s) that the
  WebSocket never delivered are fed; a fill already delivered (by trade/fill id) is never
  delivered twice, whichever copy (REST or WebSocket) comes first. Back-filled fills get the
  same exact fee check.
* **Data lag**: the lag is the larger of the runner-queue lag and the exchange-time lag of
  Kalshi market data (BRTI source time, trade and book-delta `ts_ms`, against a trailing
  latency baseline), so frames piling up in the WebSocket receive buffer are seen. Above
  `loop.max_lag_s` new orders are blocked AND the strategy is told (`runner.lag` stale: it
  cancels its quotes); it resumes once fresh data kept the lag below half of that for
  `loop.lag_resume_s`. A loop stall (timers far behind) is handled the same way, whether a
  wake-up or an event comes first. The consumer yields to the event loop after every item
  that sent orders and every `loop.yield_items` items / `loop.yield_ms`: cancels and the
  heartbeat never wait for a backlog.
* **CancelAll**: with a Halt in the same cycle -> `DELETE /portfolio/events/orders` (and new
  orders held 60 s, moot while halted); without one (lag, disconnect, reconciling, pauses) ->
  the strategy's working orders are cancelled in batches and every other order still resting
  (REST list) is swept, so quoting can resume without the one-minute cancel-all tail.
* Every 2 s: `GET /portfolio/orders/queue_positions` -> calibration samples of the queue
  estimator (`dh_queue_error_contracts`, `queue_positions` log lines).
* Every fill's `fee_cost` is checked exactly; a mismatch blocks new orders, cancels all and is
  persisted as a halt.
* New orders (and amends) are blocked (cancels never) while the kill file exists, after a
  strategy Halt (before the orders decided in the same cycle go out), on a fee mismatch, on
  data lag or a loop stall, while reconciling, during a cancel-all hold, while the clock offset
  (chrony offset plus the drift of the session clock from the wall clock) exceeds
  `loop.clock_block_ms` on `loop.clock_block_samples` samples in a row, and per market after a
  close-time / tick-grid change of that market or a spec change found by re-discovery. Event
  fee overrides (`event_fee_update`) are re-priced by the strategy itself; the runner's fee
  check follows them (a cleared override restores the market's base fee), and re-discovery
  compares the base fee, so an override never blocks a market.
* Receive times come from one monotonic, strictly increasing clock anchored to the wall
  clock at start: a wall-clock step never moves them (a large step shows up as clock offset
  and blocks new orders; a restart re-anchors).

---------------------------------------------------------------------------------------------
## 6. Kill procedures (fastest first)

1. **Kill file** (preferred; <= 0.2 s): `echo "reason" > /run/dh/KILL`
   New orders blocked, cancel-all via REST, graceful stop (in-flight requests awaited,
   resting orders verified, order group deleted). The runner refuses to start while the file
   exists: remove it after the investigation (`rm /run/dh/KILL`).
2. **SIGTERM / Ctrl-C**: same graceful stop without the kill-file flag.
3. **Watchdog, manual**: `python scripts/watchdog.py --live-config config/live.yaml --cancel-now`
   (its own key and session: works when the runner is hung).
4. **Watchdog, automatic**: locks onto the live runner it armed on (pid + session; any other
   writer of the file is ignored, so a paper runner or a second process can neither disarm
   it nor keep it quiet) and fires `DELETE /portfolio/events/orders?subaccount=<n>` when that
   runner's heartbeat is older than 2 s (`watchdog.stale_s`), when the file vanished, or when
   a shutdown hangs longer than the runner's `shutdown_timeout_s` + `watchdog.stopping_grace_s`
   (the runner also stops writing `stopping` after its timeout). It retries every second until
   it succeeds and repeats every 30 s while stale. After each attempt it writes
   `<heartbeat>.cancel_all`: a runner that is still alive (it was only hung) then holds new
   orders for 60 s and reconciles. A clean shutdown writes heartbeat state `stopped` only after
   the cancel-all was confirmed, which disarms it; a new live runner re-arms it.
5. **Kalshi web/mobile app**: portfolio, open orders, cancel them (works when our host is
   down).
6. **Revoke the API key(s)** in the Kalshi settings (stops new orders; does NOT cancel resting
   ones, so do 5 as well).

Exchange-side protections always in force: order-group auto-cancel on fill bursts,
`cancel_order_on_pause`, order expiry (`order_expiry_s`, 120 s), and Kalshi's own
cancel-on-disconnect is NOT assumed.

**The cancel-all tail**: Kalshi documents that `DELETE /portfolio/events/orders` may also
cancel orders placed during the minute after the request. After any global cancel-all (the
start-up clean slate, a kill, a halt, the watchdog) new orders are therefore held for
`venue.cancel_all_hold_s` (60 s); a restart right after a kill or a watchdog trigger quotes a
minute later.

After any kill: `python -m dh.live.tools orders` must print `0 resting orders`; check
positions in the Kalshi UI; write down what happened.

---------------------------------------------------------------------------------------------
## 7. Monitoring

* Metrics: `curl -s 127.0.0.1:9108/metrics` (Prometheus text; scrape config in
  `deploy/prometheus.yml`), health: `curl -sf 127.0.0.1:9108/health` (HTTP 503 when unhealthy).
* Logs: `data/live_logs/<session>.jsonl`, one JSON object per line with `k` (kind), `t`
  (event time, ns), `cfg` (config digests), `sha` (git commit).
* Raw session store: `data/live/raw/<stream>/...`: `kalshi.ws` (every frame), `kalshi.rest.*`
  (every REST response), `events.live` (order results fed to the strategy), `events.paper`
  (simulator messages), `meta` (session start/end, universe changes, warm-up), `clock`.

| Metric | Meaning | Alert when |
|---|---|---|
| `dh_heartbeat_ts` | last heartbeat (unix s) | older than 5 s |
| `dh_consumer_lag_seconds` | data lag: max(queue lag, exchange-time lag) | > 0.5 s sustained |
| `dh_lag_episodes_total{why}`, `dh_loop_stalls_total` | lag / stall gate closures | growing |
| `dh_queue_depth` | events waiting | > 1000 |
| `dh_gate_closed`, `dh_gate_reason{reason}` | new orders blocked (lag, reconciling, cancel_all_hold, clock) | 1 for long (read `/health` `gate`) |
| `dh_reconciling` | own-activity reconciliation in progress | 1 for > 60 s |
| `dh_venue_stuck_cancels` | orders still resting after every cancel failed | > 0: Kalshi UI now |
| `dh_duplicate_fills_dropped_total`, `dh_foreign_subaccount_events_total`, `dh_cancel_resends_total` | reconciliation details | investigate if growing |
| `dh_day_pnl_dollars` | the UTC day's P&L incl. earlier sessions (persisted) | near -$25 |
| `dh_watchdog_cancel_alls_seen_total` | the watchdog cancelled everything while this runner lived | > 0 |
| `dh_halted{scope}` | strategy Halt seen | 1: see section 8 |
| `dh_fv_ready` | fair-value model warm | 0 after start-up |
| `dh_brti_age_seconds` | benchmark tick age | > 3 s |
| `dh_kalshi_ws_ok` | Kalshi WS connected | 0 for > 10 s |
| `dh_mm_quotes_placed`, `dh_mm_fills`, `dh_mm_halted_cycles`, `dh_mm_reason{reason}` | strategy activity and why it does not quote | halted cycles growing |
| `dh_position_contracts{ticker}`, `dh_equity_dollars`, `dh_fees_paid_dollars` | book | equity drawdown near the daily halt |
| `dh_fee_mismatch_total` | fee reconciliation | > 0 |
| `dh_position_suspects_total` / `dh_position_mismatches_total` | reconciliation | mismatches > 0 |
| `dh_ghost_orders_total`, `dh_fills_backfilled_total` | order / fill reconciliation | > 0 (investigate) |
| `dh_venue_unknown_outcomes`, `dh_venue_pending_reconciliations` | REST writes with unknown outcome | pending > 0 for > 60 s |
| `dh_rest_rtt_seconds_{count,sum,max}{op,outcome}` | REST latency | p99 > 250 ms |
| `dh_gate_rejects_total{reason}` | orders blocked by the gate | growing unexpectedly |
| `dh_clock_offset_seconds`, `dh_clock_alarms_total` | clock offset (chrony + session-clock drift) | alarms > 0 (> 5 ms); orders blocked above 250 ms |
| `dh_recorder_write_errors`, `dh_record_errors_total` | capture | > 0 |
| `dh_strategy_errors_total`, `dh_source_restarts_total{source}` | crashes / reconnect loops | > 0 |

Useful log queries (`jq`):
```sh
L=data/live_logs/<session>.jsonl
jq -c 'select(.k=="halt" or .k=="kill" or .k=="gate" or .k=="block" or .k=="reconcile")' $L
jq -c 'select(.k=="fee_mismatch" or .k=="position_mismatch" or .k=="ghost_orders" or .k=="cancel_resend")' $L
jq -c 'select(.k=="venue.cancel_stuck" or .k=="venue.revived" or .k=="venue.create_conflict")' $L
jq -c 'select(.k|startswith("venue."))' $L          # unknown outcomes, reconciliations, cancel-alls
jq -c 'select(.k=="log.fill")' $L                     # our fills (with fair value at fill)
jq -c 'select(.k=="action" and .type=="PlaceOrder") | [.t,.ticker,.book_side,.px_dollars,.qty]' $L
jq -c 'select(.k=="feed_status")' $L                 # gaps / disconnects
```

---------------------------------------------------------------------------------------------
## 8. Halts, blocks and restarts

| Log / metric | Cause | What to do |
|---|---|---|
| `halt` scope `all`, reason `daily_loss` | the UTC day's P&L (all sessions of the day) <= -$25 | stop for the day; review fills/markouts |
| `halt` scope `all`, reason `reconciliation:...` | position differs from the exchange for > 5 s | `tools reconcile`; compare `log.fill` with the Kalshi fill history; find the lost/extra fill |
| `halt` reason `carried_over:...` | a halt of an earlier session (risk state) | as for the original reason; then `--reset-daily-halt` |
| `gate` `fee_mismatch` | a fill's fee differs from the model | `verify_fee_schedule.py`; fix `config/fees.yaml` / precision |
| `block` `close_date_updated` / `tick_grid_changed` / `spec_changed` | a traded market changed | nothing: that market is out for the session; new markets use new specs |
| `fee_update` (log) | an event fee override | nothing: the strategy re-prices (unsupported types become untradable) |
| `log.risk` `abnormal_move`, `settlement_loss_pause` | timed pauses inside the strategy | nothing (they expire; a pause survives a restart) |
| `gate` `lag` | data lag or a loop stall (quotes cancelled) | check CPU, `dh_queue_depth`, `dh_consumer_lag_seconds`; reduce load |
| `gate` `reconciling` / `reconcile` log | WS reconnect, cancel-all hold | nothing; > 60 s: check REST (`reconcile_error` lines) |
| `gate` `cancel_all_hold` | the minute after a global cancel-all | nothing (expires) |
| `gate` `clock` | clock offset > 250 ms persists | fix chrony; restart the runner (re-anchors its clock) |
| `venue.cancel_stuck` | an order keeps resting although every cancel fails | Kalshi UI: cancel it by hand; watchdog `--cancel-now` |

**Halts survive restarts.** The runner persists the day's P&L, the halt flag/reason and any
pause (`paths.risk_state_file`, atomically, every 2 s and immediately on a halt) and, live,
re-derives the day's P&L from Kalshi's fills and settlements at start-up. The next session
therefore starts **halted** after a halt, starts with the day's loss already counted (e.g.
-$24.99, then a 1-cent loss halts at -$25 in total), and keeps a settlement-loss pause. A
daily-loss halt ends with the UTC day; a reconciliation or fee-mismatch halt persists across
days. After investigating, override explicitly:
```sh
python scripts/run_live.py ... --reset-daily-halt   # forgives the halt, the pause AND the day's loss so far
```
The override is logged (`risk_reset_by_operator`, with the values it forgave) and recorded in
the session's `meta`. Do not delete the state file instead: the start-up re-derivation from
Kalshi would still count the day's loss.

A restart after a crash is safe: start-up cancels leftovers (verified), excludes events with
positions, counts the day's P&L, holds new orders for the cancel-all minute and creates a
fresh order group.

---------------------------------------------------------------------------------------------
## 9. Daily reconciliation (about 10 minutes)

1. `python -m dh.live.tools orders` after the session stopped: 0 resting orders.
2. `python -m dh.live.tools reconcile --log data/live_logs/<session>.jsonl`: exchange fills
   since the session start vs the log, per ticker (count, contracts, fees): 0 mismatched.
3. Kalshi UI: positions and settlements of the day vs `log.settle` lines; balance change vs
   `ledger` net.
4. Metrics of the day: `dh_fee_mismatch_total`, `dh_position_mismatches_total`,
   `dh_ghost_orders_total` all 0; `dh_venue_pending_reconciliations` 0; unknown outcomes all
   followed by `venue.reconciled`; halts explained; gaps and reconnects counted.
5. `python -m dh.live.tools replay --log ...` on one session: identical.
6. `python -m dh.live.tools ledger --log ...`: append the summary to the daily journal
   (net c/contract, CI, markouts, contracts/day).

---------------------------------------------------------------------------------------------
## 10. Before increasing size

All must hold (BUILD_PLAN K.3 / section G):
* >= 4 weeks and >= 5,000 live fills at M1 size;
* realized net c/contract CI lower bound > 0 in the segments you scale, and live markouts no
  worse than paper by more than 0.5c;
* zero unexplained reconciliation mismatches, fee model exact on every fill;
* REST p99 latency stable; write-bucket usage leaves at least 50% headroom at the current
  quote rate (watch `dh_gate_rejects_total{reason="rate_budget"}` and HTTP 429 counts);
* order-group triggers rare and explained; queue-estimator error acceptable
  (`dh_queue_error_contracts`);
* no clock alarms.

Then change **one** knob at a time in `config/m1.yaml` (e.g. `quoting.clip_contracts`,
`risk.max_pos_per_market`, the loss limits, `risk.order_group_limit_contracts`) through a
reviewed commit, run it in paper next to the live config for a few days, and promote it only
if the paper comparison holds. Any result better than 0.75c/contract triggers an audit of fill
simulation and attribution before it is believed.

---------------------------------------------------------------------------------------------
## 11. Troubleshooting

| Symptom | Fix |
|---|---|
| `refusing to start: kill-file directory /run/dh does not exist` | section 1.1 |
| `kill file ... is present` | investigate, then `rm /run/dh/KILL` |
| `another runner holds .../runner.lock` / `heartbeat ... is fresh from pid N` | a runner is still alive (or died < 5 s ago): stop it, or give the second runner its own paths |
| `risk state file ... is unreadable` | inspect it; restore or remove it deliberately (it carries halts) |
| `N orders still resting after the start-up cancel-all` | Kalshi UI; `tools orders`; retry |
| `loop.strategy_error must be 'stop' in live mode` (and the other live refusals) | fix `config/live.yaml` |
| `no Kalshi API credentials` | section 1.3 |
| `exchange not trading` (live) | wait for the exchange; paper mode keeps recording |
| `start-up cancel-all failed` / `could not create the exchange order group` | REST/auth problem: run `smoke_kalshi.py` |
| `no tradable markets` | series/horizon wrong, or all markets fail the rules check (see the `market ...` log lines) |
| `fair-value model NOT ready` | back-fill failed (section 2); the runner waits for 1 day of live ticks |
| many `dh_gate_rejects_total{reason="rate_budget"}` / HTTP 429 | account tier too low for the quote rate: raise `quoting.requote_min_interval_ms` or reduce markets |
| `dh_consumer_lag_seconds` high / `gate lag` | CPU starved or a slow cycle: fewer markets/feeds, faster host |
| exit code 3 | the cancel-all could not be confirmed: Kalshi UI now; the watchdog keeps trying |
