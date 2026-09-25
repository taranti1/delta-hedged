# Operations runbook: recorder, paper trading, live trading (M1)

Practical steps for running the system on a real host. Everything here was built and tested
offline; the steps marked **verify live** have never touched the real API and must be checked
the first time. Commands run from the repository root with the venv active
(`. .venv/bin/activate`).

| Process | Command | Restart policy |
|---|---|---|
| Recorder (market data capture) | `scripts/record.py` | always |
| Strategy runner, paper or live | `scripts/run_live.py` | **never automatic**: a human restarts it |
| Watchdog (dead-man group trigger + cancel-all, live only) | `scripts/watchdog.py` | always (macOS: launchd template `deploy/launchd/com.dh.watchdog.plist`) |
| Prometheus / Grafana (optional) | `deploy/docker-compose.yml` | always |

**Deployment (decided 2026-09-25):** System 1 (this repository) trades **subaccount 1** of the
user's Kalshi account, with API keys **restricted to subaccount 1**, on **exchange shard 2**
(where every KXBTC* market lives), from **this Mac**. Subaccount 0 belongs to the other live
system (System 2, `trading-strategy/Kalshi`) and must never be touched: `venue.shared_account:
true` makes the runner and the watchdog refuse subaccount 0, and the REST client refuses any
write that does not name subaccount 1 explicitly (`docs/research/SHARED_ACCOUNT_AUDIT.md`,
`docs/research/KALSHI_DOCS_RECONCILIATION.md`).

Exit codes of `run_live.py`: `0` normal stop or kill file, `2` refused to start (message says
why), `3` shutdown could not confirm that all orders are cancelled (check the Kalshi UI now; the
watchdog keeps trying), `4` strategy/consumer error or a dead background loop (heartbeat, risk
state, fills, positions, clock, reconciler...: fail-safe stop with the normal cancel-all).

One runner per `paths.data_root` and per heartbeat file: the runner holds a `flock` on
`<data_root>/runner.lock` and `<heartbeat>.lock` and refuses to start while another process
holds them, or while the heartbeat file is fresh from another pid. **One Kalshi (sub)account
per runner**: those locks are per path, so two runners with different `data_root` /
heartbeat paths on the same subaccount are NOT prevented, and they would cancel each other's
orders, sweep them as ghosts and see each other's positions as mismatches. A restart keeps
the day's risk state (section 8): a halt stays a halt, the daily-loss budget is not refilled.
Runtime directory of the kill file / heartbeat / locks: `/run/dh` on Linux, `data/run` (under
the repository, created on demand) on macOS, when `paths.kill_file` / `paths.heartbeat_file`
are left empty.

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

#### 1.1a The macOS host (recorder since 2026-09-25; the live deployment host)
The recorder runs on a Mac (Apple silicon, macOS 26, uv-managed CPython 3.12 venv:
`.venv/bin/python`, installed with `-e '.[dev]'`), and live trading is deployed on the same
Mac (subaccount 1, section 1.2). What differs from the Linux VM above:
* **No `timeout`, `chronyc`, `timedatectl` or `adjtimex`.** `scripts/record.py`'s clock
  sampler (`dh.store.recorder.sample_clock`) falls back to a query-only SNTP exchange
  (`sntp -t 2 <server of /etc/ntp.conf, default time.apple.com>`, which never sets the clock)
  and records `src: "sntp"`, `offset_s` (positive = local clock BEHIND), `est_error_s` (the
  +/- bound) and `synced: null` every 60 s in stream `clock`. Measured on 2026-09-25: the Mac
  ran **~33-37 ms behind** NTP (+/- ~20 ms) at 15:15 ET, drifting to ~47-51 ms by 16:15 ET
  (macOS `timed` disciplines loosely). All `recv - exchange` latencies measured here
  carry that bias (negative values for Kraken/Gemini/Coinbase are this offset, not time
  travel); correct research latencies with the `clock` stream. If `sntp` is unavailable the
  sample degrades to `src: "unknown"` (the recorder never crashes on it).
* **Live clock gate on macOS**: there is no chronyd, so on darwin the runner's clock gate
  also trusts the query-only `sntp` sample of the same sampler
  (`dh.live.runner.DARWIN_CLOCK_SOURCES`): "synchronised" means `sntp` answered with an error
  bound (`+/-`) within `loop.clock_max_est_error_ms` (default `clock_block_ms` = 250 ms); the
  offset gate (`clock_block_ms`) applies as on Linux; no answer, or a larger bound, blocks new
  orders (`gate clock`). This Mac measured **35-50 ms behind NTP**: below the 250 ms block, so
  it trades, but set `loop.clock_alarm_ms: 100` in `config/live.yaml` here (the 5 ms alarm
  would fire on every sample; the warning is rate-limited to one a minute), and remember that
  every latency number the runner reports (`dh_consumer_lag_seconds`, `dh_lag_baseline_seconds`,
  `recv - exchange` in the logs) carries that 35-50 ms bias. `sntp` is one network round trip
  per sample (every `loop.clock_sample_s`, 60 s).
* **Sleep**: run long jobs under `caffeinate -i` (prevents idle sleep; a closed laptop lid
  still sleeps the machine: never close it while live). Check with `pmset -g assertions`.
  A sleeping Mac stops runner AND watchdog: the 120 s order expiry is then the only protection.
* **Runtime directory**: `/run/dh` does not exist on macOS. With `paths.kill_file` /
  `paths.heartbeat_file` left empty (the example config) they default to `data/run/KILL` and
  `data/run/heartbeat.json` under the repository (`dh.live.config.default_run_dir`; created on
  demand, git-ignored). The kill file is then `echo why > data/run/KILL`.
* **Watchdog as a launchd agent**: `deploy/launchd/com.dh.watchdog.plist` (KeepAlive,
  `--arm-on-start`, caffeinate; see `deploy/launchd/README.md`; not installed automatically).
  The runner stays manual (never a launchd job): start it in a terminal under
  `caffeinate -i` (section 5.2).
* Binance (HTTP 451) and Bybit (HTTP 403) are geo-blocked from this US host: both are
  `enabled: false` in `config/feeds.yaml`.

### 1.2 Kalshi account: subaccount 1, restricted keys, shard-2 funding
**Do these steps with `scripts/account_setup.py` (dry run by default; procedure and exact
commands in [docs/ACCOUNT_SETUP.md](ACCOUNT_SETUP.md)).**

System 1 trades a **dedicated subaccount (1)** of an account shared with System 2 (which owns
subaccount 0). Cancel-all, the ghost-order sweep and the position reconciliation act on every
order/position of subaccount 1: never trade it by hand. Kalshi facts this rests on (openapi
3.31.0, docs.kalshi.com, `docs/research/KALSHI_DOCS_RECONCILIATION.md` findings 1, 4, 5, 11):
subaccounts are API-only and need the Advanced API tier or above; every KXBTC* market is on
**exchange shard 2**; collateral is checked inside each shard's matching engine and a
subaccount's balance is **local to a shard**; order groups work on one shard and one
subaccount only; rate limits are per ACCOUNT (shared with System 2).

One-time steps (the user, by hand; read-only checks with `python -m dh.live.tools ...`):
1. **API tier**: Advanced or above (`GET /account/limits` `usage_tier`; Advanced is self-serve:
   `POST /account/api_usage_level/upgrade`).
2. **Create subaccount 1 on shard 2**: `POST /portfolio/subaccounts` with body
   `{"exchange_index": 2}` (the body's shard defaults to 0). Leave its netting OFF.
3. **Fund subaccount 1 on shard 2**: `POST /portfolio/subaccounts/transfer` with
   `from_subaccount: 0`, `to_subaccount: 1`, `amount_cents`, a fresh `client_transfer_id` and
   **`exchange_index: 2`** (default 0 would fund the wrong shard). If the cash sits on shard 0,
   move it first (`POST /portfolio/intra_exchange_instance_transfer`, asynchronous, up to three
   non-atomic steps; or the UI at kalshi.com/account/exchange-indexes). Do the transfer while no
   System 2 session runs, bracket it with balance reads, and **declare it to System 2**
   (`two_leg_launcher declare-transfer`, D-059): an undeclared transfer breaks System 2's cash
   parity. Fund at least the worst-case loss + margin: `max(risk.max_total_worst_loss,
   risk.daily_loss_halt)` of `config/m1.yaml` ($50) + `venue.min_balance_margin_dollars` ($10) =
   **$60**, plus what the resting quotes' collateral needs (Kalshi rejects an order the shard
   balance cannot collateralize). Check: `python -m dh.live.tools balance` (per shard, vs $60).
4. **Two API keys restricted to subaccount 1** (`scripts/account_setup.py create-key`, i.e.
   `POST /api_keys/generate` with `subaccount: 1`; the web UI only creates unrestricted keys): one for the runner, one for the watchdog. A restricted key "may only
   read and trade on that sub-account": Kalshi itself then refuses any System 1 action on
   subaccount 0, and scopes the private WebSocket channels (`fill`, `user_orders`,
   `market_positions`, `order_group_updates`) to subaccount 1 server-side. Keep System 2's
   unrestricted key untouched and **never give System 1 that key** (the live start-up verifies
   the restriction: `GET /api_keys`, else the balance response's missing `balance_breakdown`;
   an unrestricted key refuses the start with `venue.key_restricted_to_subaccount: true`). The
   second key protects the dead-man switch against a revoked runner key, NOT against rate
   limits (budgets are per account; cancel-all costs 2 tokens and a 429 carries no penalty, so
   the watchdog's 1 s retry suffices).
5. Store keys outside the repository, readable only by you:
   `mkdir -p ~/.kalshi && chmod 700 ~/.kalshi && mv <download>.pem ~/.kalshi/runner.pem && chmod 600 ~/.kalshi/*.pem`
   (`*.pem`, `*.key`, `secrets/` and `.env` are git-ignored; never commit a key).
6. `config/live.yaml`: `venue.subaccount: 1`, `venue.shared_account: true`,
   `venue.key_restricted_to_subaccount: true`, `venue.exchange_indexes: [2]` (the example
   config's values); `config/kalshi.yaml`: `rate_limits.account_share` <= 0.5 (0.2 here; live
   refuses more on a shared account).

What this settles and what stays open (**verify live**, section 5.1):
* `GET /historical/fills` now takes `subaccount` (openapi 3.31.0; omitted = all subaccounts):
  the day's P&L passes it explicitly, so historical rows are attributed like live ones (no more
  "names no subaccount" refusal).
* WebSocket `fill` / `market_positions` / `user_orders` messages mark `subaccount` optional.
  With a restricted key a message WITHOUT it is subaccount 1's (server-side scoping) and the
  runner accepts it; one with another number is dropped. The first own fill of every session
  logs `verify_live ws_fill_subaccount_field` (present or not; metric `dh_verify_live`).
* Unscoped `GET /portfolio/balance` = System 2's primary balance (spec): System 2 reads it as
  "the account total"; check once after funding that it equals the sub-0 row of
  `GET /portfolio/subaccounts/balances` (SHARED_ACCOUNT_AUDIT C(i)).

### 1.3 Environment variables
Put these in an env file (e.g. `/etc/dh/env`, `chmod 600`) loaded by your shell or service:
```sh
export KALSHI_KEY_ID=<runner key id>
export KALSHI_PRIVATE_KEY_PATH=$HOME/.kalshi/runner.pem
export KALSHI_WATCHDOG_KEY_ID=<watchdog key id>
export KALSHI_WATCHDOG_PRIVATE_KEY_PATH=$HOME/.kalshi/watchdog.pem
```

**For live trading on subaccount 1 these are System 1's OWN two restricted keys (1.2), never
System 2's.** On this Mac put the four lines in a System 1 env file (e.g. `~/.kalshi/dh.env`,
`chmod 600`) and point `config/kalshi.yaml` `auth.env_file` at it: the runner, the watchdog
(also as a launchd agent, which inherits no shell environment) and the tools then all read the
restricted keys. The setup below (System 2's `prod.env`, an unrestricted key) is fine for the
read-only recorder and tools, but a live runner with `venue.key_restricted_to_subaccount: true`
refuses to start with it.

**Or reuse an existing credentials file without copying it** (how this Mac's recorder is set
up): in `config/kalshi.yaml`
```yaml
auth:
  env_file: /path/to/trading-strategy/Kalshi/.secrets/prod.env # read in place, never copied
  key_id_env: KALSHI_PROD_API_KEY_ID
  private_key_path_env: KALSHI_PROD_PRIVATE_KEY_PATH
```
`dh.kalshi.config.load_config` reads that KEY=VALUE file (`dh/kalshi/envfile.py`: `export`,
quotes and `#` comments accepted, no shell expansion) into the process environment before
the lookups: variables already set in the environment win, names starting with `ALLOW_` are
**never** loaded (the other system's safety switches must not leak into this one), and only
variable NAMES are ever logged (`kalshi: env file ...: loaded ['KALSHI_PROD_API_KEY_ID', ...]`).
Every entry point that builds a Kalshi client goes through `load_config` (smoke_kalshi,
record.py, verify_fee_schedule, download_kalshi_history, `dh.live.tools`, the runner, the
watchdog), so they all see the same credentials. The key id is redacted from
`KalshiSigner.__repr__`.

**Shared account.** This key belongs to the same Kalshi account as the user's other, live
trading system. REST budgets are per ACCOUNT (docs.kalshi.com "Rate Limits and Tiers": REST
and FIX drain the same read/write buckets), so `config/kalshi.yaml` sets
`rate_limits.account_share: 0.2`: the limiter reads `GET /account/limits` and keeps this
process to 20% of the refill rate and bucket capacity (capacity floored at the largest single
request cost). Observed 2026-09-25: tier **basic**, read 200 tokens/s (capacity 600), write
100 tokens/s (capacity 100), default cost 10, CF passthrough 50, `GET /portfolio/orders/{id}`
and cancels 2 -> this system: **read 40 tokens/s (cap 120), write 20/s (cap 30)**. Kalshi
documents no WebSocket connection limit; every process here opens at most ONE Kalshi
WebSocket (all subscriptions multiplexed on it). The recorder, smoke checks and `dh.live.tools`
build their REST client with `read_only=True`: any non-GET raises before it is signed or sent.

### 1.4 Configuration files
```sh
cp config/kalshi.example.yaml config/kalshi.yaml   # env: prod | demo, REST/WS settings
cp config/live.example.yaml   config/live.yaml     # runner: mode, paths, venue, universe...
```
Both copies are host-local and git-ignored (they may point at host secrets files). On this
Mac `config/kalshi.yaml` exists (env file, `account_share: 0.2`,
`fees.balance_precision_dollars: "0.0001"` from the fee check of section 2); `config/live.yaml`
does not yet (`dh.live.tools` then falls back to `config/live.example.yaml`).
* `config/m1.yaml` holds the strategy (sizes, limits, timers). Do not edit it casually: its
  digest is written on every log line and every session record.
* `config/live.yaml`: keep `mode: paper` until section 5. `venue.subaccount: 1` (the
  deployment; `null`/`0` = the primary account, refused live with `venue.shared_account:
  true`). The subaccount is sent **explicitly on every request**, `0` included: Kalshi reads
  an omitted subaccount as "all subaccounts" on `GET /portfolio/orders`, `GET /portfolio/fills`
  and cancel-all; in live mode the REST client refuses, before signing, any write that does not
  name `venue.subaccount` and an exchange shard (`UnscopedWriteError`). Fills, order updates
  and positions of other subaccounts arriving on the private WebSocket channels are dropped
  (`dh_foreign_subaccount_events_total`), as are those of markets outside the configured series
  (`dh_foreign_series_events_total`). `venue.exchange_indexes: [2]`: the shards traded and
  funded (a market on an unknown or other shard is skipped at discovery). `paths.data_root`
  (default `data/live`) must differ from the recorder's root: two processes writing `kalshi.ws`
  into one store would interleave two connections' sequence numbers.
* `paths.heartbeat_file` is the LIVE runner's heartbeat (the watchdog reads it; it names the
  runner's subaccount and order groups, which the watchdog triggers); a paper runner writes
  `paths.paper_heartbeat_file` (default: the same name with `.paper` inserted,
  `<run dir>/heartbeat.paper.json`), so it can never be mistaken for the live runner.
* `paths.risk_state_file` (default `<data_root>/state/risk_state.<mode>.json`): the day's P&L
  (realized and mark), a carried halt (reason, scope, the UTC day it was decided) and pause,
  an operator's loss-budget base; written every 2 s, on every halt (fsync of the file and its
  directory) and at shutdown (section 8).
* Clock (live): the runner samples `chronyc` (else `timedatectl`; macOS: a query-only `sntp`,
  1.1a) every `loop.clock_sample_s` (must be > 0 live). A sample from anything else (no
  chronyd reachable; on Linux an sntp answer does not count), not synchronised, or with an
  estimated error above `loop.clock_max_est_error_ms` (default = `clock_block_ms`) blocks new
  orders like a large offset (section 5.3). Under Docker the strategy container mounts
  `/run/chrony` for `chronyc`.
* **External venues stay off in M1** (`feeds.only: []`). This is deliberate: M1 prices and
  detects jumps on BRTI alone, so external books add load on the single strategy consumer
  without protecting against the failure that matters (a lagging or frozen BRTI relay). With
  no external feeds the risk engine's ">= 2 fresh external venues" rule is **off**; staleness
  is judged on BRTI itself, by receive age AND CF source age (quoting stops at 10 s, and at
  3 s for markets within 10 minutes of expiry). If you enable feeds later, use trade/ticker
  channels, never full order books: the `dh/feeds` readers yield to the event loop after
  every frame, but every event is still work for the single strategy consumer.

---------------------------------------------------------------------------------------------
## 2. Pre-flight checks (new host, new key, after upgrades)

| Check | Command | Pass |
|---|---|---|
| Kalshi REST + WS + BRTI | `python scripts/smoke_kalshi.py --seconds 60` (`--demo` for demo) | all 11 checks PASS (status, limits, KXBTCD discovery + specs, rules sanity, fee type, REST orderbooks, WS subscriptions, WS books equal REST snapshots, BRTI 1 Hz and 5 Hz rates and latency, no gaps) |
| External venues | `python scripts/smoke_feeds.py --seconds 60` | every enabled venue PASS |
| Benchmark back-fill | `python -m dh.live.tools backfill` | `"ok": true`, coverage >= 0.9 |
| Resting orders | `python -m dh.live.tools orders` | `0 resting orders (subaccount 1)` (exit 0) |
| Collateral | `python -m dh.live.tools balance` | every `venue.exchange_indexes` shard of subaccount 1 `OK` (>= worst-case loss + margin, $60) |
| Fee schedule | `python scripts/verify_fee_schedule.py --days 14` | series fee types supported; once the account has fills: every fill matches (sets `fees.balance_precision_dollars`) |
| Clock | `chronyc tracking` (Linux) / `sntp time.apple.com` (macOS; query only, never sets the clock) | offset < 1 ms (Linux); macOS: answers, offset well below 250 ms and `+/-` below 250 ms (measured ~35-50 ms, +/- ~20 ms) |

**First live run, 2026-09-25, this Mac, prod, shared account (all read-only):**

| Check | Result | Notes |
|---|---|---|
| `smoke_kalshi.py --seconds 60` | **11/11 PASS** first try | 6 KXBTCD markets; 14,900 WS frames, 0 gaps; WS books == REST; BRTI 1 Hz: 61 ticks (1.00/s), recv-source p50 60 ms, Kalshi hop 71 ms; 5 Hz: 305 ticks (4.99/s), p50 23 ms, hop 34 ms (both biased ~-35 ms by this Mac's clock) |
| `smoke_feeds.py` | **all 9 enabled feeds PASS** after fixes | coinbase, kraken, bitstamp, gemini, cryptocom, deribit, deribit_options, okx, hyperliquid (see docs/ENVIRONMENT.md for the fixes) |
| `dh.live.tools backfill` | **ok, coverage 1.0** | 49 hourly requests, 861,949 BRTI ticks (5 Hz), 2,880/2,880 minute points, 122 s at the 20% budget (50 tokens per request), peak RSS ~350 MB |
| `dh.live.tools orders` | 0 resting orders (primary subaccount) | pure `GET /portfolio/orders` |
| `verify_fee_schedule.py --days 14` | **OK** | KXBTCD, KXBTC, KXBTC15M: `quadratic` x1, no scheduled changes; the account's 40 fills (the other system's, non-BTC markets) all match exactly at balance precision **$0.0001** (22 exact / 16 trade-only / 2 within rounding at $0.01) -> `fees.balance_precision_dollars: "0.0001"` in `config/kalshi.yaml` |

**Back-fill**: the fair-value model needs one half-life of every volatility EWMA, the longest
being 1 day, before it quotes. At start-up the runner fetches 2 days of BRTI through Kalshi's
CF Benchmarks passthrough, one clock hour per request, newest first (verified live):
`GET /trade-api/v2/cfbenchmarks/history/values?id=BRTI&timespan=HOUR&timestamp=2026-09-25T17:00:00.000Z`
(the timestamp is the hour's START, truncated to the timespan as CF requires; `backfill.align:
true`) -> `{"data": {"serverTime": ..., "payload": [{"time": <ms>, "value": "83737.50"}, ...]}}`,
the hour's ticks at 5 Hz (18,000 rows, ~750 kB). The newest (current) hour may be partial or,
within `backfill.recent_delay_s`, empty (CF: history can lag up to 15 min): an empty recent
chunk is skipped, an empty older chunk stops the back-fill. Ticks are down-sampled to one
print per minute. Placeholders for `backfill.timespan` / `backfill.timestamp`: `{start_ms}
{end_ms} {start_s} {end_s} {span_s} {span_ms} {start_iso} {end_iso}`; extra query parameters in
`backfill.extra_params`. Without the history the runner still starts, logs `fair-value model
NOT ready`, and **does not quote until it has seen >= 1 day of live BRTI ticks** (metric
`dh_fv_ready` = 0).

---------------------------------------------------------------------------------------------
## 3. Recorder (M1.0)

```sh
python scripts/record.py --config config/feeds.yaml
```
Run it as a service for at least 7 days before trusting any analysis (BUILD_PLAN M1.0: fewer
than 0.1% sequence gaps). It writes `data/raw/<stream>/<date>/<hour>.jsonl.zst` and never
trades (its Kalshi REST client is `read_only`). The runner records its own session store
separately (`paths.data_root`). One recorder per store: it holds an exclusive `flock` on
`<root>/recorder.lock` and a second instance exits with status 2.

**What it records** (default `config/feeds.yaml`, verified live 2026-09-25):
* Kalshi, ONE WebSocket: `orderbook_delta` + `trade` + `ticker` for **every open KXBTCD, KXBTC
  and KXBTC15M market** (`market_horizon_h: 0`; ~360-640 markets depending on the hour: the
  current and next hourly events, the next day's 17:00 event and a weekly event),
  `market_lifecycle_v2` for all markets, BRTI on `cfbenchmarks_value` (1 Hz) and
  `cfbenchmarks_value_5hz`. REST: order-book snapshots of all subscribed markets every 60 s,
  series/event/fee metadata on every discovery. Discovery runs every 300 s; from 2 s after
  the open time of each market closing within `pending_lookahead_h` (the next KXBTC15M, the
  next hourly event: they open 15 min / 1 h before closing), retried every 10 s until listed
  (observed: the 16:30 KXBTC15M market was subscribed 13 s after its open); and on lifecycle
  `created`/`activated` events of already-open markets (at most every `min_refresh_gap_s`;
  `created` events of markets that open later, e.g. a day of KXBTC15M created at once, are
  parked until their open time instead of re-running discovery). Kalshi REST use measured:
  105 GETs in 14 min = ~1.2 tokens/s, 3% of this system's 40 tokens/s share.
* External venues (no credentials): Coinbase `level2` (full book) + trades, Kraken book 100 +
  trades, Bitstamp diff book + trades, Gemini `l2`, Crypto.com book 50 + trades, Deribit
  BTC-PERPETUAL book/ticker/trades/index/DVOL and option tickers (2 nearest expiries,
  |K/S - 1| <= 5%), OKX swap books5/trades/funding/mark/OI/index/liquidations, Hyperliquid
  l2Book (fast) + trades + asset context. Binance and Bybit are disabled (geo-blocked).
* `clock` every 60 s (macOS: `sntp`, section 1.1a) and `meta` (session start/end, config).

**Measured data rates** (this Mac, 2026-09-25 afternoon ET; zstd level 3, on-disk bytes):

| Stream | msgs/s | raw MB/h | disk MB/h |
|---|---|---|---|
| kalshi.ws | 600-1,500 | 600-1,450 | 60-145 |
| deribit.options (+/-5%, ~60 instruments) | ~52 | ~145 | ~18 (34 at +/-10%) |
| coinbase.ws (full level2) | ~20 | ~70-245 | ~6-21 |
| cryptocom.ws | ~46 | ~65-77 | ~10-13 |
| kraken.ws | ~60-140 | ~40-100 | ~6-13 |
| gemini.ws | ~45-190 | ~13-56 | ~2-10 |
| deribit.ws | ~8 | ~16-27 | ~4-6 |
| okx.ws | ~21-28 | ~22-27 | ~3-4 |
| bitstamp.ws, hyperliquid.ws | ~3.5 each | ~5-12 | ~2-3 each |
| kalshi.rest.* | <1 | ~15-45 | ~1-2 |
| **total** | | **~1.0-2.4 GB/h** | **~120-250 MB/h = 3-6 GB/day** |

Ranges are quiet vs busy periods (the minutes around an hourly expiry are the busiest). With
~200 GB free that is roughly 5-8 weeks; plan to move `data/raw` off the machine or prune old
hours before then (the store is append-only; nothing prunes it automatically). Reduced on
2026-09-25: Deribit options narrowed from +/-10% to +/-5% moneyness (near-ATM IV is what M1
needs; `agg2` is not sparser than `100ms`). Kept on purpose: Coinbase full-depth `level2`
(largest BRTI constituent; the BRTI methodology uses order-book depth) and all Kalshi books.

**Running it on this Mac** (until the launchd agent is installed):
```sh
cd /Users/thomast/Desktop/delta-hedged && mkdir -p data/logs
nohup caffeinate -i .venv/bin/python scripts/record.py --config config/feeds.yaml >> data/logs/record.out 2>&1 &
```
`caffeinate` forks: the started PID becomes the recorder (python) and a child `caffeinate`
holds the no-idle-sleep assertion until that PID exits. PIDs are kept in
`data/logs/record.pid` (recorder) and `data/logs/record.caffeinate.pid`. Stop cleanly with
`kill -TERM "$(cat data/logs/record.pid)"` (flush + fsync + segment indexes, ~5 s). The
permanent setup is the launchd agent in `deploy/launchd/` (KeepAlive, caffeinate, logs in
`data/logs/`; see its README; not installed yet).

**Health**: a status line every 60 s in the log: per stream msgs/s, age of the last message,
gaps / resyncs / reconnects / stale / errors (Kalshi: gaps, dups, connects), recorder MB and
write errors, and the clock sample. `python scripts/replay_inspect.py --root data list | gaps | clock` inspects the store.

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
- [ ] Subaccount 1 exists (created with `exchange_index: 2`), its netting is OFF, and it is
      funded **on shard 2**: `python -m dh.live.tools balance` says OK (section 1.2 step 3);
      the funding transfer was declared to System 2.
- [ ] Runner and watchdog keys are System 1's own, **restricted to subaccount 1**, in System 1's
      env file (1.2 step 4, 1.3); System 2's key is nowhere in System 1's configuration.
- [ ] `config/live.yaml`: `venue.subaccount: 1`, `venue.shared_account: true`,
      `venue.key_restricted_to_subaccount: true`, `venue.exchange_indexes: [2]`;
      `config/kalshi.yaml`: `rate_limits.account_share` <= 0.5 (0.2).
- [ ] Watchdog running with its own key and `--arm-on-start` (macOS: the launchd agent,
      `deploy/launchd/README.md`), logging `subaccount 1`; kill drill done (section 6) with
      the runner in paper mode + watchdog `--cancel-now` (triggers the heartbeat's groups, then
      cancels subaccount 1's orders).
- [ ] One runner per Kalshi subaccount; System 1 only ever on subaccount 1 (section 1.2).
- [ ] Clock: `chronyc tracking` works for the runner's user/container (Linux) or `sntp
      time.apple.com` answers (macOS; `loop.clock_alarm_ms: 100` there), and the runner's first
      minutes show no `gate clock` (section 5.3).
- [ ] `config/m1.yaml` unchanged since paper (same digest in the logs).
- [ ] Subaccount 1 holds only the capital you accept to risk (M1 limits: daily loss halt $25,
      worst case $20 per event / $50 total, clips and per-market limits as in `config/m1.yaml`).
- [ ] After the first live session, the **verify live** items are settled and written down:
      `jq -c 'select(.k=="verify_live")' <session log>` shows `ws_fill_subaccount_field` (does
      the WS fill carry `subaccount`?) and `queue_positions_covers_shard` (does
      `GET /portfolio/orders/queue_positions` return shard-2 orders?), `venue.order_group`
      shows the group created on shard 2 and the first order bodies carry `exchange_index: 2`
      (raw `kalshi.rest.orders` stream), and no `position_confirm_deferred` storm.

### 5.2 Configure and start
1. `config/live.yaml`: `mode: live`, plus the subaccount deployment keys of 5.1. Live mode
   refuses `loop.strategy_error: continue`, `venue.exclude_events_with_positions: false`,
   `venue.startup_cancel_all: false` (paper-only debugging settings), `loop.clock_sample_s: 0`,
   `venue.exchange_status_interval_s: 0` / `venue.balance_interval_s: 0`, an empty
   `venue.exchange_indexes`, and, with `venue.shared_account: true`, subaccount null/0 or
   `rate_limits.account_share` > 0.5.
2. Start the watchdog first (macOS: `launchctl bootstrap gui/$(id -u)
   ~/Library/LaunchAgents/com.dh.watchdog.plist`, `deploy/launchd/README.md`; or by hand in its
   own terminal):
   ```sh
   python scripts/watchdog.py --live-config config/live.yaml --arm-on-start
   ```
   (`--arm-on-start`: a watchdog restarted while the runner may have died meanwhile still
   triggers its order groups and cancels its orders, section 6.) Its log says `watching
   .../heartbeat.json (stale after 2.0s; subaccount 1)`.
3. Start the runner by hand (never a launchd job; both the config flag and the command-line
   flag are required; on macOS under `caffeinate -i`, lid open):
   ```sh
   caffeinate -i python scripts/run_live.py --config config/m1.yaml --live-config config/live.yaml \
       --mode live --i-understand-this-sends-real-orders --duration 3600
   ```
   Use `--duration` for the first sessions and stay at the screen.

Live start-up adds, in this order, before discovery:
0. a watchdog marker `<heartbeat>.cancel_all` left from before this start is renamed to
   `<heartbeat>.cancel_all.stale-<unix s>` (logged): it is about an earlier runner; then the
   REST client is built (live: it refuses any write not naming subaccount 1 and a shard);
1. **exchange status of shard 2**: `GET /exchange/status`, the shard's own entry in
   `exchange_index_statuses` (the top level describes shard 0); not trading -> exit 2. The
   schedule (`GET /exchange/schedule`: maintenance windows, the weekly Thursday 03:00-05:00 ET
   trading pause as a gap in `standard_hours`) is read and its next closure logged;
2. **collateral**: `GET /portfolio/balance?subaccount=1&exchange_index=2` plus the cost of the
   shard's open positions and resting orders of subaccount 1 (read-only, before any write:
   proves the subaccount exists and is funded where its collateral is checked) must be >=
   worst-case loss + margin ($60) on every `venue.exchange_indexes` shard, else exit 2;
   `dh_balance_dollars{exchange_index}` / `dh_shard_funds_dollars` from the first second;
3. **key restriction** (`venue.key_restricted_to_subaccount: true`): `GET /api_keys` must show
   the runner key restricted to subaccount 1 (else, if the key is not listed or the call is
   refused, the balance response must lack `balance_breakdown`, which Kalshi omits only for
   restricted keys); an unrestricted key -> exit 2;
4. **clean slate**: `DELETE /portfolio/events/orders?subaccount=1` (subaccount 1 only, every
   shard; scoped server-side too by the restricted key), then the resting-order list of
   subaccount 1 must come back empty (leftovers are cancelled one by one with their own shard,
   3 rounds; still resting -> exit 2);
5. **positions** of subaccount 1 are read AFTER that (an order resting while positions are read
   could fill unseen): **events that already hold a position are excluded** for this session
   (they settle within the hour); positions outside the configured series are logged and
   ignored; a malformed position row refuses the start;
6. **the day's P&L** from Kalshi (section 8): `GET /historical/cutoff`, today's fills
   (`GET /portfolio/fills?subaccount=1`, plus `GET /historical/fills?subaccount=1` for the part
   before the cutoff) and settlements, the open positions at exchange prices (`GET /markets`:
   long at the YES bid, short at the YES ask, a determined market at its payout) and the
   positions held at 00:00 UTC at the last trade before midnight (`GET /markets/trades`),
   INCLUDING the excluded events; combined with the persisted state -> the risk seed. Fill /
   settlement rows of markets outside the series are skipped and logged; a malformed or
   timeless fill / settlement row of the series, or a failed read, refuses the start (exit 2);
   a missing price falls back to the worst case and is logged (`risk state: ... no exchange
   price`). If midnight UTC passes meanwhile, it is derived again for the new day;
7. **new orders are held for `venue.cancel_all_hold_s` (60 s) after that cancel-all**: Kalshi
   documents that a cancel-all may also cancel orders placed during the following minute.
   The strategy is told (`kalshi.reconcile` stale) and does not quote until the hold ends.

Discovery keeps only markets on a known shard listed in `venue.exchange_indexes` (skipped
otherwise: `market ...: exchange shard ...` log lines). Then the exchange **order group** is
created **on each shard in use** (shard 2: `POST /portfolio/order_groups/create` with
`subaccount: 1, exchange_index: 2`; rolling 15 s fill cap from
`risk.order_group_limit_contracts`, auto-cancel; groups do not work across shards). The runner
refuses to trade if any of these fail; a failure after the cancel-all leaves nothing behind
(the order group is deleted, the heartbeat says `stopped`, the locks are released).

Running paper and live side by side (e.g. to A/B a config change): give the paper runner its
own live-config copy with different `paths.data_root`, `paths.log_dir` and `metrics.port`
(the paper heartbeat file is separate automatically; the same data_root is refused by the
lock). Both may share the kill file (a kill stops both). Under Docker
(`deploy/docker-compose.yml`, `config/live.docker.yaml`) the paths point at the mounted
`/data` volume; `/run/dh` is shared with the watchdog container.

### 5.3 What the runner does while live
* Every order: post-only, `cancel_order_on_pause`, inside the order group **of its shard**,
  expiring after `order_expiry_s` (120 s, `config/m1.yaml`: the exchange-side backstop if host
  and watchdog both lose Kalshi), client_order_id `<run_prefix>-<session token>-<n>`, and
  **`subaccount: 1` and `exchange_index: 2` explicitly** (never auto-routed: auto-routing costs
  latency and bills the unscoped write bucket plus every shard's; an explicit shard bills only
  shard 2's). Cancels carry the order's own shard (from the resting-order row, else its
  market's; `-1` = Kalshi's documented "auto-route by ticker" only for an order whose shard is
  unknown, with its ticker: a cancel is never withheld), batch-cancel items too; amends and
  decreases carry it in the body, order-group writes as a query parameter. A market whose
  shard is unknown or not in `venue.exchange_indexes` is never placed (local reject). Several
  quotes decided together go out as one batched request; a quote that would wait more than
  `venue.max_place_wait_s` for rate-limit tokens is dropped (reason `rate_budget`) instead of
  arriving stale (requests already through the rate limiter are not counted twice).
* **Exchange pauses**: `GET /exchange/status` every `venue.exchange_status_interval_s` (10 s;
  shard 2's own entry), the schedule's closures (re-read hourly; quotes pulled
  `venue.pause_lead_s` = 60 s before one, e.g. the weekly Thursday 03:00-05:00 ET trading pause)
  and any place rejected with a pause-like reason (held `venue.pause_reject_hold_s` = 30 s,
  status polled at once): new orders blocked (`gate exchange_pause`) and the strategy told
  `kalshi.reconcile` stale (it cancels its quotes while cancels still work). When trading is
  active again, fills / positions / resting orders are re-read before it quotes (`resynced`).
  A **trading pause** (`trading_active` false) still accepts cancels; an **exchange pause**
  (`exchange_active` false, logged CRITICAL) rejects cancels too: then only
  `cancel_order_on_pause` (set on every order) protects the resting quotes.
* **Collateral**: every `venue.balance_interval_s` (60 s) the shard's funds are re-read:
  `GET /portfolio/balance?subaccount=1&exchange_index=2` (the AVAILABLE cash) + the cost of
  subaccount 1's open positions there (`market_exposure_dollars`) + the collateral of its
  resting orders there (so our own quotes and fills never make a funded shard look empty; a
  transfer out or a real loss does); below the requirement ($60) or unreadable -> `gate
  balance` (new orders blocked, resting ones stay) until a read shows it covered again.
  `dh_balance_dollars` = available cash, `dh_shard_funds_dollars` = the funds compared.
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
  rest are looked up). A difference is confirmed only by a positions read that follows a
  `GET /portfolio/fills` read (no minimum age) started after the difference was first seen:
  a fill the WebSocket lost silently is back-filled by it instead of halting the bot. Each
  positions read is preceded by `GET /exchange/user_data_timestamp` (the time up to which
  Kalshi's portfolio reads are validated; REST lags the exchange): a read whose timestamp is
  older than the last WebSocket fill never confirms a difference (`position_confirm_deferred`,
  `dh_position_reads_stale_total`; read again at once). WebSocket position messages can raise
  or clear a suspicion (and trigger that check at once) but never confirm one.
* Positions of excluded events (held at start-up) are carried at their start-up marks; when
  such a market is determined, the runner realizes it at once (an updated `RiskStateSeed`
  right after the settlement event, recorded for replay; `excluded_settlement` log line), so
  the daily-loss limit and the persisted state see the settlement.
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
  latency baseline), so frames piling up in the WebSocket receive buffer are seen. The
  baseline (each source's smallest age over `loop.lag_window_s`) is capped at
  `loop.lag_baseline_cap_ms` (default `clock_block_ms` + 100 = 350 ms): a backlog present
  since start-up, or lasting longer than the window, stays lag instead of becoming "normal
  latency". A source whose smallest age exceeds the cap logs `lag_baseline_over_cap` and
  counts `dh_lag_baseline_over_cap_total` (`dh_lag_baseline_seconds{source}` shows each
  uncapped baseline: if a source's NORMAL latency is above the cap, measure it in paper mode
  and raise the cap deliberately). Above `loop.max_lag_s` new orders are blocked (before the
  strategy sees the event that revealed the lag) AND the strategy is told right after it
  (`runner.lag` stale: it cancels its quotes); it resumes once fresh data kept the lag below
  half of that for `loop.lag_resume_s`. A loop stall (timers far behind) is handled the same
  way, whether a wake-up or an event comes first. The consumer yields to the event loop after
  every item that sent orders and every `loop.yield_items` items / `loop.yield_ms`: cancels
  and the heartbeat never wait for a backlog.
* **CancelAll**: with a (manual) Halt in the same cycle -> first the **order-group trigger**
  (`PUT /portfolio/order_groups/{id}/trigger?subaccount=1&exchange_index=2`: cancels the
  group's quotes, rejects new ones, no documented trailing tail; the group is never reset or
  re-created in this session), then `DELETE /portfolio/events/orders?subaccount=1` (and new
  orders held 60 s, moot while halted); without one (lag, disconnect, reconciling, pauses) ->
  the strategy's working orders are cancelled in batches and every other order still resting
  (REST list) is swept, so quoting can resume without the one-minute cancel-all tail.
* Every 2 s: `GET /portfolio/orders/queue_positions?subaccount=1` -> calibration samples of the
  queue estimator (`dh_queue_error_contracts`, `queue_positions` log lines). The endpoint has
  no `exchange_index` parameter and the docs do not say whether it covers shard 2 (**verify
  live**): `dh_queue_positions_coverage` is the share of our resting orders it returned, and
  the first poll logs `verify_live queue_positions_covers_shard` (ok = any returned).
* Every fill's `fee_cost` is checked exactly; a mismatch blocks new orders, cancels all and is
  persisted as a halt.
* New orders (and amends) are blocked (cancels never) while the kill file exists, after a
  strategy Halt (before the orders decided in the same cycle go out), on a fee mismatch, on
  data lag or a loop stall, while reconciling, during a cancel-all hold, during an exchange /
  trading pause, while the shard balance is below the requirement, while the clock offset
  (chrony offset plus the drift of the session clock from the wall clock) exceeds
  `loop.clock_block_ms` on `loop.clock_block_samples` checks in a row **or the clock cannot be
  trusted** (live: no `chronyc`/`timedatectl` answer (macOS: no `sntp` answer), not
  synchronised, estimated error above `loop.clock_max_est_error_ms`, or every market-data
  source stamped in our future, which
  proves the local clock behind; `dh_clock_untrusted` = 1, re-sampled every
  `loop.clock_resample_s` until it recovers; the strategy is told `runner.clock` stale and
  pulls its quotes), and per market after a
  close-time / tick-grid change of that market or a spec change found by re-discovery. Event
  fee overrides (`event_fee_update`) are re-priced by the strategy itself; the runner's fee
  check follows them (a cleared override restores the market's base fee), and re-discovery
  compares the base fee, so an override never blocks a market.
* Receive times come from one monotonic, strictly increasing clock anchored to the wall
  clock at start: a wall-clock step never moves them (a large step shows up as clock offset
  and blocks new orders; a restart re-anchors).

---------------------------------------------------------------------------------------------
## 6. Kill procedures (fastest first)

Every kill path acts on **subaccount 1 only**: `DELETE /portfolio/events/orders?subaccount=1`
is scoped by its parameter AND, with the restricted keys, by Kalshi itself; System 2's orders on
subaccount 0 are never touched. Each kill path first **triggers the order group(s)**
(`PUT /portfolio/order_groups/{id}/trigger?subaccount=1&exchange_index=2`): the fastest scoped
kill, it cancels every quote of the group on its shard and rejects new ones until a reset (none
follows), with no documented trailing tail (unlike cancel-all's "orders placed during the next
minute may also be cancelled"); the subaccount's cancel-all follows for everything outside a
group. Runtime directory below: `/run/dh` (Linux) or `data/run` (macOS).

1. **Kill file** (preferred; <= 0.2 s): `echo "reason" > /run/dh/KILL` (macOS:
   `echo "reason" > data/run/KILL`)
   New orders blocked, group trigger, cancel-all via REST, graceful stop (in-flight requests
   awaited, resting orders verified, order group deleted). The runner refuses to start while
   the file exists: remove it after the investigation (`rm <run dir>/KILL`).
2. **SIGTERM / Ctrl-C**: same graceful stop (group trigger, cancel-all, verification) without
   the kill-file flag.
3. **Watchdog, manual**: `python scripts/watchdog.py --live-config config/live.yaml --cancel-now`
   (its own key and session: works when the runner is hung): triggers the groups named in the
   heartbeat file, then cancels all of subaccount 1.
4. **Watchdog, automatic**: locks onto the live runner it armed on (pid + session; any other
   writer of the file is ignored, so a paper runner or a second process can neither disarm
   it nor keep it quiet) and, when that runner's heartbeat is older than 2 s
   (`watchdog.stale_s`), when the file vanished, or when a shutdown hangs longer than the
   runner's `shutdown_timeout_s` + `watchdog.stopping_grace_s` (the runner also stops writing
   `stopping` after its timeout), first triggers the order groups of the runner's last
   heartbeat (only groups of `venue.subaccount`, each on its shard), then fires
   `DELETE /portfolio/events/orders?subaccount=1`. It retries every second until the
   cancel-all succeeds and repeats every 30 s while stale. After each attempt it writes
   `<heartbeat>.cancel_all` (`{"t", "ok", "watched": [pid, session], "groups_triggered"}`). A
   live runner that
   finds a marker written after its own start **about itself** was alive but unresponsive:
   it **halts** (Halt(all), reason `watchdog_cancel_all`, persisted and carried across
   restarts: investigate why the heartbeat went stale, then `--reset-daily-halt`). A marker
   about another runner (a restart racing a trigger) only holds new orders for 60 s and
   reconciles. A clean shutdown writes heartbeat state `stopped` only after the cancel-all was
   confirmed, which disarms it; a new live runner re-arms it.
   **`--arm-on-start`** (used by `deploy/docker-compose.yml`): on its first poll a restarted
   watchdog acts on an EXISTING LIVE heartbeat only (mode live, state running/stopping): it
   locks onto that runner if the heartbeat is fresh, and cancels all at once if it is stale
   (the runner died while the watchdog was down). A missing file, an unreadable one, or any
   other heartbeat (paper, `starting`, `stopped`) leaves it DISARMED until a fresh live
   heartbeat appears; it never locks onto a `starting` runner (its heartbeat is not refreshed
   during the start-up sequence).
5. **Kalshi web/mobile app**: portfolio, open orders, cancel them (works when our host is
   down). Subaccounts are API-only: the app may not show subaccount 1's orders; then use 3
   from any host with the watchdog key (`--cancel-now`).
6. **Revoke the API key(s)** in the Kalshi settings (stops new orders; does NOT cancel resting
   ones, so do 3 or 5 as well).

Exchange-side protections always in force: order-group auto-cancel on fill bursts (on shard
2, where the quotes are), `cancel_order_on_pause`, order expiry (`order_expiry_s`, 120 s), and
Kalshi's own cancel-on-disconnect is NOT assumed.

**During an exchange pause** (`GET /exchange/status` `exchange_active` false, CRITICAL log
`EXCHANGE PAUSE`) Kalshi rejects cancels as well: the kill file, the watchdog, the group
trigger and the UI all fail until it ends (the watchdog keeps retrying every second). Resting
quotes are then protected only by `cancel_order_on_pause` (set on every order: Kalshi cancels
them at the pause) and the 120 s expiry. A trading pause (the weekly Thursday 03:00-05:00 ET
window, `trading_active` false) still accepts cancels; sessions may be disconnected in it.

**The cancel-all tail**: Kalshi documents that `DELETE /portfolio/events/orders` may also
cancel orders placed during the minute after the request. After any global cancel-all (the
start-up clean slate, a kill, a halt, the watchdog) new orders are therefore held for
`venue.cancel_all_hold_s` (60 s); a restart right after a kill or a watchdog trigger quotes a
minute later.

After any kill: `python -m dh.live.tools orders` must print `0 resting orders (subaccount 1)`;
check positions (`GET /portfolio/positions?subaccount=1`, or the UI); write down what happened.

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
| `dh_gate_closed`, `dh_gate_reason{reason}` | new orders blocked (lag, reconciling, cancel_all_hold, clock, exchange_pause, balance) | 1 for long (read `/health` `gate`) |
| `dh_exchange_paused`, `dh_exchange_active`, `dh_trading_active`, `dh_exchange_pauses_total`, `dh_pause_rejects_total`, `dh_next_closure_ts` | trading / exchange pauses of shard 2 (status poll, schedule, rejects) | paused outside the Thursday window |
| `dh_balance_dollars{exchange_index}`, `dh_shard_funds_dollars{exchange_index}`, `dh_balance_required_dollars` | subaccount 1's available cash per shard; its funds (cash + positions at cost + resting collateral) vs the requirement | funds below the requirement |
| `dh_verify_live{check}` | open questions settled by the first live session (`ws_fill_subaccount_field`, `queue_positions_covers_shard`): 1 as expected, 0 not | 0 |
| `dh_queue_positions_coverage` | share of our resting orders `queue_positions` returned (shard-2 coverage) | < 1 persistently |
| `dh_position_reads_stale_total` | positions reads older than the last WS fill (user_data_timestamp): never confirmed | growing fast |
| `dh_foreign_series_events_total` | own-channel events of markets outside the series (dropped) | > 0 (who trades subaccount 1?) |
| `dh_reconciling` | own-activity reconciliation in progress | 1 for > 60 s |
| `dh_venue_stuck_cancels` | orders still resting after every cancel failed | > 0: Kalshi UI now |
| `dh_duplicate_fills_dropped_total`, `dh_foreign_subaccount_events_total`, `dh_cancel_resends_total` | reconciliation details | investigate if growing |
| `dh_day_pnl_dollars` | the UTC day's real P&L incl. earlier sessions (persisted) | near -$25 (minus a reset's base) |
| `dh_day_realized_dollars`, `dh_day_mark_dollars`, `dh_day_budget_base_dollars` | its realized part, the open positions' mark, an operator reset's base | |
| `dh_excluded_settlements_total` | markets of excluded events settled during the session | |
| `dh_watchdog_cancel_alls_seen_total` | the watchdog cancelled everything after this runner started | > 0 (about this runner: it halted) |
| `dh_lag_baseline_seconds{source}`, `dh_lag_baseline_over_cap_total{source}` | uncapped exchange-time latency baseline per source; times it exceeded the cap | over-cap growing |
| `dh_clock_untrusted` | the last clock sample cannot vouch for the clock (live) | 1 |
| `dh_loop_deaths_total{loop}` | a background loop died (the runner stopped with exit 4) | > 0 |
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
| `dh_clock_offset_seconds`, `dh_clock_alarms_total` | clock offset (chrony / macOS sntp + session-clock drift) | alarms > 0 (> `clock_alarm_ms`: 5 ms Linux, 100 ms on this Mac); orders blocked above 250 ms |
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
jq -c 'select(.k=="verify_live" or .k=="exchange_pause" or .k=="exchange_status" or .k=="pause_reject")' $L
jq -c 'select(.k=="kill_switch" or .k=="venue.groups_triggered" or .k=="venue.order_group")' $L
```

---------------------------------------------------------------------------------------------
## 8. Halts, blocks and restarts

| Log / metric | Cause | What to do |
|---|---|---|
| `halt` scope `all`, reason `daily_loss` | the UTC day's P&L (all sessions of the day) <= -$25 | stop for the day; review fills/markouts |
| `halt` scope `all`, reason `reconciliation:...` | position differs from the exchange for > 5 s | `tools reconcile`; compare `log.fill` with the Kalshi fill history; find the lost/extra fill |
| `halt` reason `carried_over:...` | a halt of an earlier session (risk state) | as for the original reason; then `--reset-daily-halt` |
| `halt` reason `carried_over:watchdog_cancel_all` | the watchdog cancelled everything while this runner was alive (its heartbeat went stale: blocked loop, disk, CPU) | find why the heartbeat stopped (`dh_heartbeat_ts`, logs); `tools orders`; then `--reset-daily-halt` |
| `loop_died` log, exit 4 | a background loop (heartbeat, risk state, fills, positions, clock, reconciler...) raised or returned | read the traceback; fix before restarting |
| `gate` `fee_mismatch` | a fill's fee differs from the model | `verify_fee_schedule.py`; fix `config/fees.yaml` / precision |
| `block` `close_date_updated` / `tick_grid_changed` / `spec_changed` | a traded market changed | nothing: that market is out for the session; new markets use new specs |
| `fee_update` (log) | an event fee override | nothing: the strategy re-prices (unsupported types become untradable) |
| `log.risk` `abnormal_move`, `settlement_loss_pause` | timed pauses inside the strategy | nothing (they expire; a pause survives a restart) |
| `gate` `lag` | data lag or a loop stall (quotes cancelled) | check CPU, `dh_queue_depth`, `dh_consumer_lag_seconds`; reduce load |
| `gate` `reconciling` / `reconcile` log | WS reconnect, cancel-all hold | nothing; > 60 s: check REST (`reconcile_error` lines) |
| `gate` `cancel_all_hold` | the minute after a global cancel-all | nothing (expires) |
| `gate` `clock` | clock offset > 250 ms persists, or the clock cannot be trusted (`gate` log `why`: unmeasurable / not synchronised / estimated error / behind exchange time) | fix chrony (`chronyc tracking`; Docker: `/run/chrony` mounted; macOS: `sntp time.apple.com` must answer, check the network / time server); restart the runner (re-anchors its clock) |
| `gate` `exchange_pause`, `exchange_pause` / `exchange_status` log | shard 2 not trading (status poll), a scheduled closure within `pause_lead_s`, or a place rejected for a pause | nothing: quoting resumes after trading is active again and fills / positions / orders were re-read; an `EXCHANGE PAUSE` (cancels rejected too): watch `dh_venue_stuck_cancels`, resting quotes rely on `cancel_order_on_pause` |
| `gate` `balance` | subaccount 1's balance on a shard below worst-case loss + margin (or unreadable) | fund subaccount 1 on that shard (1.2 step 3, declared to System 2) or stop; reopens on the next read (60 s) |
| `kill_switch` log, `venue.groups_triggered` | a manual halt / kill / fee mismatch / watchdog marker / shutdown triggered the order group(s) | nothing: the group stays triggered for the session (never reset); the next start creates a fresh one |
| `verify_live` log, `dh_verify_live{check}` 0 | the first live session answered an open question differently than expected | `ws_fill_subaccount_field` 0 with an unrestricted key: switch to restricted keys (1.2); `queue_positions_covers_shard` 0: queue calibration has no shard-2 samples (report, not a trading problem) |

**Halts survive restarts.** The runner persists the day's P&L, the halt (reason, scope, the
UTC day it was decided) and any pause (`paths.risk_state_file`, atomically, every 2 s and
immediately on a halt, fsynced) and, live, re-derives the day's P&L from Kalshi at start-up.
The next session therefore starts **halted** after a halt, starts with the day's loss already
counted (e.g. -$24.99, then a 1-cent loss halts at -$25 in total), and keeps a
settlement-loss pause. A daily-loss halt ends with the UTC day **on which it was decided**
(a halted runner still up after midnight does not carry it into the next day's restart); a
reconciliation, fee-mismatch or watchdog halt persists across days.

How the day's P&L is counted at a restart (so an ordinary restart with inventory does not
halt):
* only subaccount 1 is read (`subaccount=1` on fills, historical fills, settlements and
  positions) and only markets of the configured series count: rows of other markets on the
  subaccount are skipped and logged (never a refusal), and positions there are reported, not
  valued;
* real P&L = **realized** (today's fills cash minus fees, plus settlements, minus the value of
  the positions held at 00:00 UTC at the last trade before midnight) + the **open positions
  at exchange prices** (long at the YES bid, short at the YES ask, a determined market at its
  payout, a closed one awaiting determination at its last trade);
* the realized part is the lower of the persisted one and Kalshi's; the open positions are
  always valued afresh (a pessimistic mark persisted earlier is never locked in);
* a price that does not exist falls back to the worst case (open long $0, short $1; long
  held at midnight $1) and is logged: restarting in the minutes between a held market's close
  and its determination can therefore count it pessimistically (it is corrected when the
  market settles during the session, but a halt it caused stays): prefer restarting after the
  determination;
* settlement rows' `fee_cost` is not added (the spec calls it the total fees paid; the fills
  already counted them).

After investigating a halt, override explicitly:
```sh
python scripts/run_live.py ... --reset-daily-halt   # clears the halt and the pause; fresh loss budget
```
The halt and pause are cleared and the daily-loss limit counts **from the current real day
P&L** (e.g. real -$26 at the reset: the runner halts again at -$51); the real P&L stays
recorded (`dh_day_pnl_dollars`, the state file's `budget_base_usd`, both numbers logged at
start-up), and a later restart the same UTC day keeps that base. The override is logged
(`risk_reset_by_operator`, with the values it cleared) and recorded in the session's `meta`.
Do not delete the state file instead: the start-up re-derivation from Kalshi would still count
the day's loss (and a sticky halt would be lost).

A restart after a crash is safe: start-up checks the shard balance, cancels subaccount 1's
leftovers (verified), excludes events with positions, counts the day's P&L, holds new orders
for the cancel-all minute and creates a fresh order group on shard 2 (the old one may still be
triggered: it is never reused). The weekly Thursday 03:00-05:00 ET trading pause needs no
restart: the runner pulls its quotes a minute before, waits, re-reads and resumes.

---------------------------------------------------------------------------------------------
## 9. Daily reconciliation (about 10 minutes)

1. `python -m dh.live.tools orders` after the session stopped: 0 resting orders (subaccount 1).
2. `python -m dh.live.tools reconcile --log data/live_logs/<session>.jsonl`: exchange fills
   of subaccount 1 since the session start vs the log, per ticker (count, contracts, fees): 0
   mismatched.
3. Positions and settlements of the day (subaccount 1: API, the app may not show subaccounts)
   vs `log.settle` lines; `python -m dh.live.tools balance` change vs `ledger` net.
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
| `refusing to start: kill-file directory /run/dh does not exist` | section 1.1 (Linux); on macOS leave `paths.kill_file` empty (`data/run`, created on demand) |
| `kill file ... is present` | investigate, then `rm <run dir>/KILL` |
| `venue.shared_account is true: venue.subaccount must be a dedicated subaccount` / `rate_limits.account_share ... must be <= 0.5` | `config/live.yaml` / `config/kalshi.yaml` (section 1.2 step 6) |
| `balance of subaccount 1 on shard(s) {...} does not cover ...` / `GET /portfolio/balance?subaccount=1 failed` | fund subaccount 1 on shard 2 (1.2 step 3); a failed read: the subaccount may not exist, or the key is restricted to another one |
| `venue.key_restricted_to_subaccount is true but GET /api_keys: the runner key is NOT restricted ...` | the runner uses an unrestricted key (e.g. System 2's): create System 1's restricted keys (1.2 step 4, 1.3) |
| `exchange not trading on shard(s) [2]` | a trading / exchange pause (e.g. Thursday 03:00-05:00 ET): wait |
| `UnscopedWriteError ... refused before sending` in the log | a code path tried to write without subaccount 1 / a shard: a bug, nothing was sent; report it |
| `market ...: exchange shard unknown` / `not in venue.exchange_indexes` | Kalshi moved or did not label the market's shard: nothing is traded there; if KXBTC* moved shard, fund the new shard and update `venue.exchange_indexes` |
| `another runner holds .../runner.lock` / `heartbeat ... is fresh from pid N` | a runner is still alive (or died < 5 s ago): stop it, or give the second runner its own paths |
| `risk state file ... is unreadable` | inspect it; restore or remove it deliberately (it carries halts) |
| `risk state: malformed fill row ...` / `... without created_time` / `settlement row ... payout unknown` | a REST row the day's P&L cannot use: check it in the Kalshi UI / raw `kalshi.rest.portfolio` capture; retry later; never start on a partial view |
| `risk state: GET /historical/cutoff failed` / `today's P&L could not be derived` | REST/network problem at start-up: `smoke_kalshi.py`; retry |
| `risk state: fill/settlement rows outside [...] skipped` | subaccount 1 has activity in other markets (a manual trade?): not counted in the day's P&L; find out who trades subaccount 1 |
| `malformed position row` | inspect `GET /portfolio/positions`; the runner will not trade with an unknown inventory |
| `the UTC day kept changing while today's P&L was derived` | start-up took more than a day's rollover twice: retry |
| `N orders still resting after the start-up cancel-all` | Kalshi UI; `tools orders`; retry |
| `loop.strategy_error must be 'stop' in live mode` (and the other live refusals) | fix `config/live.yaml` |
| `no Kalshi API credentials` | section 1.3 |
| `exchange not trading` (live) | wait for the exchange; paper mode keeps recording |
| `start-up cancel-all failed` / `could not create the exchange order group ... on shard(s) [2]` | REST/auth problem (`smoke_kalshi.py`), or subaccount 1 does not exist on shard 2 |
| `no tradable markets` | series/horizon wrong, or all markets fail the rules check (see the `market ...` log lines) |
| `fair-value model NOT ready` | back-fill failed (section 2); the runner waits for 1 day of live ticks |
| many `dh_gate_rejects_total{reason="rate_budget"}` / HTTP 429 | account tier too low for the quote rate: raise `quoting.requote_min_interval_ms` or reduce markets |
| `dh_consumer_lag_seconds` high / `gate lag` | CPU starved or a slow cycle: fewer markets/feeds, faster host |
| exit code 3 | the cancel-all could not be confirmed: Kalshi UI now; the watchdog keeps trying |
