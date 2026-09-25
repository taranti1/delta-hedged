# Operations runbook: recorder, paper trading, live trading (M1)

Practical steps for running the system on a real host. Everything here was built and tested
offline; the steps marked **verify live** have never touched the real API and must be checked
the first time. Commands run from the repository root with the venv active
(`. .venv/bin/activate`).

| Process | Command | Restart policy |
|---|---|---|
| Recorder (market data capture) | `scripts/record.py` | always |
| Strategy runner, paper or live | `scripts/run_live.py` | **never automatic**: a human restarts it |
| Watchdog (dead-man cancel-all) | `scripts/watchdog.py` | always |
| Prometheus / Grafana (optional) | `deploy/docker-compose.yml` | always |

Exit codes of `run_live.py`: `0` normal stop or kill file, `2` refused to start (message says
why), `3` shutdown could not confirm that all orders are cancelled (check the Kalshi UI now; the
watchdog keeps trying), `4` strategy/consumer error (fail-safe stop).

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
  subaccount. `paths.data_root` (default `data/live`) must differ from the recorder's root:
  two processes writing `kalshi.ws` into one store would interleave two connections' sequence
  numbers.

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
1. configs loaded, mode resolved, kill-file directory and heartbeat checked;
2. REST client with the account's rate limits; exchange status;
3. market discovery: open KXBTCD markets expiring within `universe.horizon_s` (2 h: the
   current and next hour). Markets whose rules text fails the sanity check, or that are not
   open, are skipped (logged); unresolved/unsupported fee types stay untradable;
4. benchmark back-fill and fair-value warm-up (section 2);
5. the MarketMaker is built; the paper simulator (`policy: conservative`, latency from
   `paper.*`) replaces the order venue. **No order endpoint is ever called; the account's own
   fills/orders channels are not subscribed**;
6. WebSocket: order books + trades for the universe, market lifecycle, BRTI 1 Hz and 5 Hz;
7. the loop starts. Every 2 minutes new markets are discovered and added
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
      halt $25, worst case $20 per event / $50 total, 2-contract clips, 10 per market).

### 5.2 Configure and start
1. `config/live.yaml`: `mode: live` (and `venue.subaccount` if used).
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

Live start-up adds, before discovery: the account's positions are read and **events that
already hold a position are excluded** for this session (they settle within the hour); every
leftover resting order is cancelled (`venue.startup_cancel_all`); after discovery the exchange
**order group** is created (rolling 15 s fill cap from `risk.order_group_limit_contracts`,
auto-cancel). The runner refuses to trade if any of these fail.

Running paper and live side by side (e.g. to A/B a config change): give the paper runner
its own live-config copy with different `paths.data_root`, `paths.log_dir`,
`paths.heartbeat_file` and `metrics.port`. Both may share the kill file (a kill stops both).
Under Docker (`deploy/docker-compose.yml`) pass `--live-config` explicitly and point
`paths.data_root` / `paths.log_dir` at the mounted `/data` volume; `/run/dh` is already mounted
for the kill file and heartbeat.

### 5.3 What the runner does while live
* Every order: post-only, `cancel_order_on_pause`, inside the order group. Several quotes
  decided together go out as one batched request; a quote that would wait more than
  `venue.max_place_wait_s` for rate-limit tokens is dropped (reason `rate_budget`) instead of
  arriving stale.
* A create with unknown outcome (timeout, 5xx) is **never resent**: the runner looks the
  order up by client_order_id (backoff 0.5 s .. 30 s); if it is still absent after 30 s it is
  declared rejected. Cancels with unknown outcome are looked up and re-sent while the order
  still rests.
* Every 30 s: positions from `GET /portfolio/positions` vs the strategy's fill-derived
  positions (a mismatch must persist 5 s, i.e. survive a fill in flight, before it halts), and
  resting orders vs the strategy (unknown resting orders are cancelled; orders the strategy
  believes live but that no longer rest are looked up). WebSocket position messages go
  through the same persistence check. After a sequence gap on our own channels the strategy
  pauses 30 s and the runner reconciles immediately, including fills the WebSocket missed.
* Every 2 s: `GET /portfolio/orders/queue_positions` -> calibration samples of the queue
  estimator (`dh_queue_error_contracts`, `queue_positions` log lines).
* Every fill's `fee_cost` is checked exactly; a mismatch blocks new orders and cancels all.
* New orders are blocked (cancels never) while the kill file exists, after a strategy Halt,
  on a fee mismatch, while the consumer lags more than `loop.max_lag_s`, and per market after
  a fee change or a close-time / tick-grid change of that market.

---------------------------------------------------------------------------------------------
## 6. Kill procedures (fastest first)

1. **Kill file** (preferred; <= 0.2 s): `echo "reason" > /run/dh/KILL`
   New orders blocked, cancel-all via REST, graceful stop (in-flight requests awaited,
   resting orders verified, order group deleted). The runner refuses to start while the file
   exists: remove it after the investigation (`rm /run/dh/KILL`).
2. **SIGTERM / Ctrl-C**: same graceful stop without the kill-file flag.
3. **Watchdog, manual**: `python scripts/watchdog.py --live-config config/live.yaml --cancel-now`
   (its own key and session: works when the runner is hung).
4. **Watchdog, automatic**: fires `DELETE /portfolio/events/orders` when the live runner's
   heartbeat is older than 2 s (`watchdog.stale_s`), retries every second until it succeeds,
   repeats every 30 s while the heartbeat stays stale. A clean shutdown writes heartbeat state
   `stopped` only after the cancel-all was confirmed, which disarms it.
5. **Kalshi web/mobile app**: portfolio, open orders, cancel them (works when our host is
   down).
6. **Revoke the API key(s)** in the Kalshi settings (stops new orders; does NOT cancel resting
   ones, so do 5 as well).

Exchange-side protections always in force: order-group auto-cancel on fill bursts,
`cancel_order_on_pause`, and Kalshi's own cancel-on-disconnect is NOT assumed.

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
| `dh_consumer_lag_seconds` | event receive -> strategy | > 0.5 s sustained |
| `dh_queue_depth` | events waiting | > 1000 |
| `dh_gate_closed` | new orders blocked | 1 (read `/health` `gate`) |
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
| `dh_clock_offset_seconds`, `dh_clock_alarms_total` | clock health | alarms > 0 (> 5 ms) |
| `dh_recorder_write_errors`, `dh_record_errors_total` | capture | > 0 |
| `dh_strategy_errors_total`, `dh_source_restarts_total{source}` | crashes / reconnect loops | > 0 |

Useful log queries (`jq`):
```sh
L=data/live_logs/<session>.jsonl
jq -c 'select(.k=="halt" or .k=="kill" or .k=="gate" or .k=="block")' $L
jq -c 'select(.k=="fee_mismatch" or .k=="position_mismatch" or .k=="ghost_orders")' $L
jq -c 'select(.k|startswith("venue."))' $L          # unknown outcomes, reconciliations, cancel-alls
jq -c 'select(.k=="log.fill")' $L                     # our fills (with fair value at fill)
jq -c 'select(.k=="action" and .type=="PlaceOrder") | [.t,.ticker,.book_side,.px_dollars,.qty]' $L
jq -c 'select(.k=="feed_status")' $L                 # gaps / disconnects
```

---------------------------------------------------------------------------------------------
## 8. Halts, blocks and restarts

| Log / metric | Cause | What to do |
|---|---|---|
| `halt` scope `all`, reason `daily_loss` | daily P&L <= -$25 | stop for the day; review fills/markouts |
| `halt` scope `all`, reason `reconciliation:...` | position differs from the exchange for > 5 s | `tools reconcile`; compare `log.fill` with the Kalshi fill history; find the lost/extra fill |
| `gate` `fee_mismatch` | a fill's fee differs from the model | `verify_fee_schedule.py`; fix `config/fees.yaml` / precision |
| `block` `fee_update` / `close_date_updated` / `tick_grid_changed` / `spec_changed` | a traded market changed | nothing: that market is out for the session; new markets use new specs |
| `log.risk` `abnormal_move`, `settlement_loss_pause` | timed pauses inside the strategy | nothing (they expire) |
| `gate` `lag` | the event loop is overloaded | check CPU, `dh_queue_depth`; reduce feeds |

Every M1 halt is a manual reset: investigate, then restart the runner (the halt state is not
carried over). A restart after a crash is safe: start-up cancels leftovers, excludes events
with positions, and creates a fresh order group.

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
| `no Kalshi API credentials` | section 1.3 |
| `exchange not trading` (live) | wait for the exchange; paper mode keeps recording |
| `start-up cancel-all failed` / `could not create the exchange order group` | REST/auth problem: run `smoke_kalshi.py` |
| `no tradable markets` | series/horizon wrong, or all markets fail the rules check (see the `market ...` log lines) |
| `fair-value model NOT ready` | back-fill failed (section 2); the runner waits for 1 day of live ticks |
| many `dh_gate_rejects_total{reason="rate_budget"}` / HTTP 429 | account tier too low for the quote rate: raise `quoting.requote_min_interval_ms` or reduce markets |
| `dh_consumer_lag_seconds` high | CPU starved: fewer external feeds, faster host |
| exit code 3 | the cancel-all could not be confirmed: Kalshi UI now; the watchdog keeps trying |
