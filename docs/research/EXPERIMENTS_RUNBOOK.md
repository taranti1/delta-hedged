# Experiments runbook: E1–E10 on recorded data

Status: every remaining experiment of `docs/TEST_MATRIX.md` runs with **one command** on a
recording made by `scripts/record.py`, through the production strategy code (the same
`MarketMaker` the live runner uses), the queue-aware exchange simulator and the ledger. No real
Kalshi data exists yet (the build environment cannot reach Kalshi). The pipelines are proven end
to end on a **synthetic** recording written in the collector's own on-disk formats
(`docs/research/synthetic_demo/`, banner *SYNTHETIC DATA — pipeline validation only*); those
numbers validate code paths and injected known answers, never edge.

```
python scripts/run_experiment.py <name> --root data --t0 2026-10-01 --t1 2026-10-08 [--out DIR] [--jobs 4]
    names: universe | replay | flow | e1 | e2 | e3 | e4 | e67 | e8 | e9 | e10 | all | synth | demo
    fitted inputs (checked for look-ahead against t0, section 2a): [--fv-config FV.json] [--flow-segments FLOW.json]
```

Defaults: strategy `config/m1.yaml`, fill policies `B,C` (`--policies A,B,C` adds A as a
reference; a result that holds only under A is flagged `holds_only_under_A` and rejected), output
`<root>/results/<name>/` (one CSV per table + one short markdown with the decision rule and a
verdict line). `--t0/--t1 auto` = coverage of the `kalshi.ws` stream. Start every analysis with
`universe` (market specs, fee table, rejected markets, own fills found in the window).

## 1. Recording prerequisites

`python scripts/record.py` with `config/feeds.yaml`:

| need | setting | used by |
|---|---|---|
| Kalshi WS with credentials (`KALSHI_KEY_ID`, `KALSHI_PRIVATE_KEY_PATH`) | `kalshi.enabled: true`, series `[KXBTCD, KXBTC, KXBTC15M]`, `market_channels: [orderbook_delta, trade, ticker]`, `lifecycle_channels: [market_lifecycle_v2]` | all |
| BRTI 1 Hz + 5 Hz | `index_channels: [cfbenchmarks_value, cfbenchmarks_value_5hz]` | all (fair value, settlement window), E2 target |
| BRTI history for warm-up | `cf_history_interval_s: 3600` (recommended; default 0) | fair-value warm-up at each window start |
| constituent venues | coinbase, kraken, bitstamp, gemini, cryptocom (+ paxos) | E2 features, E3 features, strategy health |
| perps (optional) | bybit / okx / hyperliquid / binance_futures (where legal) | E2 `basis` feature |
| market metadata | **automatic** (this change): at start-up and every `market_refresh_s` the collector records `GET /series/{s}`, `GET /series/fee_changes`, `GET /events/fee_changes` and `GET /events/{e}` for every newly discovered event, in addition to the `/markets` pages it already recorded | spec/fee rebuild |
| clock | chrony-synced host; the collector samples the offset to stream `clock` | latency-sensitive E1/E2/E8 |

Sessions of the live runner (`scripts/run_live.py`) record the same raw `kalshi.ws` frames
(including our private `fill` / `user_orders` frames) plus `events.live` (adapter results) and
`events.paper` (paper-simulator messages). The replay reads `kalshi.ws`, the venue streams and
`events.md.*` (normalized market-data caches) only; `events.live` / `events.paper` never reach
the replayed strategy.

## 2. How a replay is built (dh/research/replay_env.py)

* **Universe.** `build_universe(root, t0, t1)` merges every recorded Market object (`/markets`
  pages, `/markets/{t}`, `/events/{e}` nested markets) with the WS lifecycle messages
  (`created` + `additional_metadata`, `metadata_updated` — KXBTC15M strikes are set after open —
  `close_date_updated`, `price_level_structure_updated`, `determined`/`settled`) in receive
  order. A market becomes **available** at the receive time of the first record from which a
  complete `MarketSpec` can be built (`dh.kalshi.normalize.rest_market_to_spec`: strike and tick
  grid known), never before t0: markets available at t0 are passed to `MarketMaker(...)`, later
  ones are added with `MarketMaker.add_markets` at that time (at the earliest
  `expiration - (max_tau_s + 300 s)`) together with a snapshot of their replayed book, and settled
  markets are pruned hourly (`prune_settled`). Fees: `dh.kalshi.fees.resolve_fee_fields` as of
  max(availability, t0) with event overrides (EventData, scheduled `/events/fee_changes`, WS
  `event_fee_update`) over the series (+ scheduled `/series/fee_changes`) over the market; a fee
  change inside the window is reported in the notes (the replay keeps the fee in force at
  availability). Markets without a resolvable fee are never quoted (the strategy refuses).
* **Events.** `ReplayStream` = `dh.store.replay` normalization (the live sequencer for
  `kalshi.ws`) over `[t0 - 15 min, t1 + 5 min)`: records before t0 prime books, connection state
  and the settlement window (synthesized snapshots and a `kalshi.ws connected` status at t0);
  only settlement messages pass after t1. REST order-book snapshots (`kalshi.rest.orderbook`) are
  not replayed (applying them to a sequenced WS book would double-count deltas) and CF-history
  REST records only warm the fair value (they are history, not live ticks).
* **Own-order footprint** (recordings made while we traded): `OwnFootprintFilter` drops
  `orderbook_delta` messages carrying our `client_order_id` (Kalshi sets it only on deltas we
  caused) and tracks our resting qty per level from them; it subtracts that qty from later
  snapshots; public trades whose `trade_id` equals one of our private `fill` messages (pre-scanned
  for the window) are reduced by our qty (dropped when fully ours); the taker-caused level decrease
  that consumed our resting qty (which does *not* carry our id) is absorbed against our tracked qty
  (pending within 2 s; a decrease seen before its print is corrected retroactively, leaving a
  transient of a few ms); liquidity we took as a taker is kept in the counterfactual book until its
  level empties or the next snapshot; private channels and their gap statuses are dropped. The
  counters are in `summary['own_filter']`.
* **Fair-value warm-up** (`--warm`): the production model needs >= 1 day of benchmark history
  (1-day EWMA). `recorded` (default) = BRTI ticks received in `[t0 - 1.5 d, t0)` (WS and
  CF-history REST records); `csv:<path>` = a price file (`ts_ms|ts_ns|timestamp` + `price|close`,
  e.g. Bitstamp 1-minute closes); `recorded+gbm` = if still not ready, a seeded GBM history
  (flagged SYNTHETIC in the summary and warnings). If the model is not warm at t0 the strategy
  does not quote until it is; the CLI prints a WARNING.
* **Simulation.** `KalshiExchangeSim` (per-ticker fee schedules and per-order fee rounding via a
  thin subclass), fill
  policies A/B/C, latency `LatencyModel` placeholders (30 ms submit/response, 10 ms WS; policy C
  x1.5) or `--latency-ms submit,response,ws`. Settlements missing from the stream are filled from
  recorded REST results after the run. Deterministic: same inputs and seeds, same numbers
  (`tests/research/test_replay_env.py`); `--jobs N` runs variants in parallel processes with
  identical results.
* **Inference.** Event-clustered bootstrap CIs everywhere (`dh.research.exp_common`); paired
  comparisons resample settlement events jointly across the two arms of the same recording.

## 2a. Look-ahead guards (fitted inputs, causal joins)

* **Fair-value parameters (in-sample FV).** The committed `dh/models/data/fv_recommended.json` was
  fitted on Bitstamp 1-minute history through `data_end_utc` = 1790301480 (2026-09-25T00:38Z).
  A replay whose t0 is before the end of the FV fitting data uses look-ahead parameters: every
  replay summary carries `fv_params` (`... IN-SAMPLE (fitted on data through ...)` /
  `out-of-sample` / `n/a (synthetic recording)`) and `fv_params_in_sample`, the CLI and every report
  print a `WARNING: in-sample FV ...` line, and each report's run table has an `FV parameters` row.
  Recordings made from 2026-09-25 on are out-of-sample for the committed config. If the config is
  later refitted on data that covers a recording (e.g. on captured BRTI), replays of that recording
  become in-sample: refit walk-forward on data strictly before t0 (`dh/research/fv_study`; a config
  with `data_end_utc` <= t0) and pass it with `--fv-config FV.json`, or report the result as
  in-sample FV. A config without `data_end_utc` is treated as in-sample. Affected: every replay
  (E2 P&L hook, E3, E4, E6/E7, E9, E10) and E8/E3-live (fair-value probe). E1 does not use these
  parameters (Gaussian research fair value with the benchmark vol realized over the 6 h BEFORE t0).
* **Taker-flow segments.** By default the strategy's fill model uses the config's flow parameters
  (not fitted on the recording). `run_experiment.py flow --t0 A --t1 B` calibrates
  `dh.research.calibrate_flow` on the recorded public tape (our own taker prints removed; BRTI ticks
  as the point-in-time reference, available at receive time) with a **time split**: a chronological
  70/30 split by market expiration (`--flow-split`) or walk-forward by day (`--walk-forward-days N`),
  both market-disjoint and purged (test markets' data before the split are dropped). It writes
  `flow_metrics.csv` / `flow_calibration.md` with **in-sample and out-of-sample** rows (predicted /
  realized taker contracts, WAPE over segments, Poisson deviance of order counts vs a pooled
  per-side null), `flow_segments_train.json` (the graded training fit) and `flow_segments.json`
  (fit on the whole window; `meta.fit_end_ms` = last expiration). Use the latter only for replays
  that start later: `e4 --t0 B --t1 C --flow-segments <out>/flow_segments.json`. Replays check
  `fit_end_ms` against t0 (`flow_in_sample` + `WARNING: in-sample flow ...`). The same split runs
  on downloaded history: `python -m dh.research.calibrate_flow --trades T --markets M --btc B --out D
  [--split 0.7 | --walk-forward-days 1]`.
* **BTC reference joins (E0, calibrate_flow).** Bitstamp/Kraken OHLC exports stamp a bar at its
  OPEN and its price is the bar CLOSE; `dh.research.kalshi_data.btc_price_asof` uses the last bar
  whose close time (`close_ts_ms`, else `ts_ms + --btc-bar-ms`, default 60 000) is <= the trade
  time, so a trade 1 s after a bar opens sees the previous bar (`tests/research/test_leakage_guards.py`).
  Point-in-time prices (index ticks) use `--btc-bar-ms 0`.
* **Walk-forward models.** E2: time folds, never shuffled, embargo max(h) + 5 s; the hook's beta is
  fitted on the first part and replayed on the rest. E3: folds of consecutive settlement events;
  each fold is predicted by models trained on earlier events' fills whose 10 s label ended before
  the fold's first fill; the cancel rule is fitted on replays of `[t0, split)` and scored on
  `[split, t1)`.
* **Settlement prints.** Only `dh.settlement` maps benchmark ticks to window prints
  (`SettlementTracker`: a 1 Hz tick with source time u is the print for second ceil(u), the later
  of two ticks in one second is kept; window (close - 60 s, close]). The replay, E2's window-average
  table and the synthetic recording (whole-second source stamps) do not re-derive it.

## 3. Runtime and memory

Measured on the synthetic demo (4 cores, one replay per core): one replay of one recorded hour
with ~9–18 markets inside `max_tau_s` and a 500 ms quote cycle takes ~2 min of one core; ~95% is
the strategy's quote cycle (per market and cycle ~1 ms: fair-value band = 9 digital prices,
scenario-grid risk charges, candidate evaluation). Estimate for real data:

    CPU s per recorded hour ~= 3600 x (1000 / quote_period_ms) x N_markets_within_max_tau x 1 ms
                               + 10 us x recorded Kalshi frames

KXBTCD alone with ~60 listed strikes and the production 200 ms cycle is ~16 min per recorded hour
(~6.5 h per day) per replay. Speed knobs for research runs (confirm any decision with the
production settings on a shorter window): `--quote-period-ms 1000` (x5), `--max-strikes N`
(only the N strikes per event nearest the benchmark exist for the strategy; e.g. 15 -> ~1/4),
`--jobs` (parallel replays), shorter windows. Memory: the ledger keeps the logged fair-value
series of every market (~100 bytes per market-second); run windows of <= 1 day and pool daily
ledgers (`e67 --ledger day1.csv --ledger day2.csv`) for multi-week segment studies. Each
replay's per-fill ledger is written by `replay` (`ledger_<policy>.csv`).

## 4. Experiments

Data needs are minimums for CIs that can decide (>= 200 settlement events per reported segment
is the E0 convention); every command also takes `--root/--t0/--t1/--out/--jobs/--warm/--seed`.

| # | module | data needed | command | replays (B,C) | outputs | decision rule (docs/TEST_MATRIX.md) |
|---|---|---|---|---|---|---|
| E1 | `exp1_staleness.run` | >= 7 days of Kalshi L2 + BRTI 5 Hz + venue books (stability: several months) | `run_experiment.py e1 [--step-ms 100]` | 0 (panel) | `e1_staleness_lead_lag.csv` (gap-closure b_h and move response c_h with block-bootstrap CI per horizon), half-life and gap-after-move economics in the markdown | accept if the lag coefficient > 0 with CI, >= 0.5 tick within 1 s and stable across months; reject if no response beyond receive latency or < 0.2 tick |
| E2 | `exp2_nowcast` | >= 3 days (folds = UTC days; 7 recommended) of BRTI 5 Hz + >= 4 constituent books (+ perps for basis) | `run_experiment.py e2 [--step-ms 200] [--split 0.5] [--no-replica] [--no-pnl]` | 4 (P&L hook on the second part) | `e2_nowcast_forecast.csv` (OOS RMSE/MAE by horizon: last print, median mid, replica, ridge, LightGBM; gain CI), `_window_average.csv`, `_pnl_hook.csv` | accept if RMSE improves >= 10% at 0.2–1 s AND replayed P&L improves (paired CI > 0 under B and C); reject if < 5% or no P&L gain |
| E3 | `exp3_toxicity` | >= 7 days (>= 5 000 shadow fills, >= 200 events); live fills when M1 trades (`--live-fills`) | `run_experiment.py e3 [--split 0.5] [--live-fills]` | 2 + 4 | `e3_toxicity_markouts.csv` (net markout vs fill price 0.1–60 s + settlement), `_models.csv` (walk-forward AUC/log-loss/Brier, OOS R^2), `_univariate.csv`, `_cancel_rule.csv`, per-fill `e3_toxicity_fills_<p>.csv` | accept if the toxicity-aware cancel rule raises net c/contract >= 0.1c (CI > 0) at <= 20% fill loss; reject if no OOS lift or it vanishes under C |
| E4 | `exp4_queue` | >= 7 days | `run_experiment.py e4 [--grid variants.yaml]` | 16 (8 variants) | `e4_queue_variants.csv` (net c/ct CI, $/day, fills/day, quote-hours, $ per quote-hour, paired diff vs `recenter_always`), `_fill_position_mix.csv` | accept keep-priority if it beats always-re-centering by > 0.05c/contract (CI > 0) and in $/day under B and C |
| E6/E7 | `exp67_segments` | >= 14 days (tau x \|z\| cells) | `run_experiment.py e67 [--ledger L.csv ...]` | 3 (A,B,C) | `e67_segments_tau.csv`, `_abs_z.csv`, `_yes_price.csv`, `_tau_x_z.csv` (net c/ct CI, fills/day, markouts, toxic share, recommendation) | quote buckets with CI lower bound > 0 (B and C); disable buckets with CI upper bound < 0 (B or C) |
| E8 | `exp8_taker` | >= 7 days | `run_experiment.py e8 [--step-ms 250] [--latency-ms 40,40,15]` | 2 scans | `e8_taker_summary.csv` (opportunities/day by edge threshold, fill rate after latency, net c/ct at 5 s / 60 s / settlement with CI, $/day), `_by_touch_staleness.csv`, `e8_taker_takes_<p>.csv` | accept if net > 0.5c/contract after fees with CI > 0 and >= 20 opportunities/day; otherwise taking stays disabled |
| E9 | `exp9_multistrike` | >= 7 days | `run_experiment.py e9 [--strikes 1,3,0]` | 6 | `e9_multistrike_variants.csv` ($/day, net c/ct, peak/mean collateral, mean \|D\|, netting ratio, delta turnover per contract) | accept if $/day is up and delta (hedge) turnover per contract is down vs the single best strike |
| E10 | `exp10_capacity` | >= 7 days | `run_experiment.py e10 [--multipliers 1,2,5,10,20,50] [--no-scale-limits]` | 12 | `e10_capacity_by_size.csv` (net c/ct CI, fills/day, contracts/day, flow share, inventory sd), `_capacity.csv` (largest clip multiple above 1.0 / 0.75 / 0.5 / 0.05 c and breakeven, point and CI-lower-bound) | measurement: report capacity (no market impact modeled: upper bound) |

E0 is `dh.research.exp0_maker_pnl` (public trades vs settlement: `scripts/download_kalshi_history.py`
tables, no recording needed; the BTC reference is joined causally, `--btc-bar-ms`, section 2a).
E1 runs the existing lead-lag estimator of `dh.research.exp1_staleness` on the
own-footprint-filtered `ReplayStream` with the universe's specs and the benchmark's vol realized
over the 6 h before t0. E5 is `dh.research.hedge_study` (real BTC paths); its fill-based
rerun uses the E4/E10 replay machinery with `hedge.enabled` in the config once hedge-venue data are
recorded.

Per-experiment method notes:
* **E2.** Features use events received <= t; targets are the benchmark as received at t + h;
  walk-forward folds are never shuffled and have an embargo of max(h) + 5 s. The window-average
  table scores `(sum_fixed + m * S_hat) / n` in the last 120 s before each expiry against the
  realized 60-print average. The P&L hook fits `beta` (share of the venue-vs-BRTI gap closed
  within 0.5 s) on the first part and replays `NowcastMarketMaker` (nowcast =
  brti_last + beta x (median venue mid - brti_last)) against the baseline on the rest.
* **E3.** Label: toxic = the fair value moved against the fill within 10 s (the strategy's logged F
  for replayed fills; the probe's fair value for live fills); the P&L target is the net 10 s markout
  vs the fill price after fees.
  Features are computed by one function at fill time and at decision time (queue at placement,
  quote age, adverse venue move 0.1–5 s sign-adjusted for the market's delta, touch imbalance,
  venue imbalance, taker volume 10/60 s, tau, \|z\|, price, spread, sigma, fair-value edge). The
  cancel rule (logistic P(toxic) >= threshold chosen on training fills, <= 20% removed, scored on
  the 60 s net markout) is replayed by `ToxicityGuardMM` on the held-out part.
* **E4.** Variants: `recenter_always` (benchmark: replace_rel = 0, min age 0, kappa 0), `config`,
  `hysteresis_strong`, `age_only_5s`, `join_only` (improve multiplier 0), `improve_x2`,
  `touch_only` (no quotes behind the touch), `wide_ladder`; custom grids: JSON/YAML
  `{name: {section: {field: value}}}`.
* **E8.** Opportunities use the strategy's own band (buy if F_lo - ask - taker fee >= threshold);
  one marketable order per episode, arriving after a sampled submit latency against the book as
  recorded then (queue-empty check; our consumed size is remembered per level). The exchange
  simulator is not used here because it does not implement IOC (see section 6).
* **E9.** `NearestStrikes(n)` picks, per event, the n strikes nearest the last BRTI tick received
  before the event's first quotable time (causal); the strategy never sees the other strikes.
* **E10.** Clip size x k with every size-denominated risk limit x k (`--no-scale-limits` keeps them).

## 5. Synthetic demo (pipeline proof, not evidence)

```
python scripts/run_experiment.py demo --jobs 4              # ~35 min on 4 cores -> docs/research/synthetic_demo/
python scripts/run_experiment.py synth --root /tmp/synth --events 2 --spacing 300 --strike-delay 20
python scripts/run_experiment.py all --root /tmp/synth --config synthetic --jobs 4
```

`dh.research.synth_recording` writes, for chained `dh.sim.synthetic` events: raw Kalshi WS frames
(`kalshi.ws`, contiguous per-sid sequence numbers, validated by the live sequencer), raw
KalshiRest records (`kalshi.rest.*`: listing pages, events, series, fee changes, settled markets,
a 2-day CF-history record for warm-up), a `meta` record with `"synthetic": true`, and the
external venues as a **normalized-event cache** (`events.md.ext`, codec events; raw venue
protocols add nothing the experiments use). Every report on such a root carries the synthetic
banner and every CSV a leading `synthetic` column. Known-answer tests
(`tests/research/test_experiments_synthetic.py`): E2 recovers the injected benchmark
publication delay (gain shrinks when the delay is removed); E8 finds many more +EV takes with a 3 s
than a 0.2 s maker lag, with positive 1 s markouts after fees; E3 shadow fills mark out worse
with informed flow on the same price path. E1 on the recording path recovers the injected maker lag
(gap-closure half-life ~0.16 s for a 0.2 s lag vs ~1.5 s for a 3 s lag).

## 6. Limitations (read before trusting a number)

* **No market impact**: other participants never react to our simulated orders; E10 capacity is
  an upper bound. Queue position is modeled (policies A/B/C), not observed; calibrate against
  live `queue_positions` (dh.execution.queue.QueueCalibrator) before believing B.
* **Latency** placeholders until measured live; E8 is latency-sensitive (`--latency-ms`).
* **Fees**: one fee schedule per market for the whole window (changes inside the window are
  listed in the report notes). Simulated fills pay Kalshi's per-order balance rounding with
  carried rebates (`OrderFeeAccumulator`, balance precision from `config/fees.yaml`, $0.01 for
  non-direct members; `summary['fee_model'] == 'per_order_rounding'`); E8 takes pay the per-fill
  trade fee. Both follow `config/fees.yaml`, which is not yet verified against live fills.
* **Own-footprint filter**: approximate at the millisecond scale (a level decrease that precedes
  its print is corrected when the print arrives); the counterfactual assumes the takers who hit
  our orders would still have traded the same total (their other prints are kept).
* **Fair-value markouts** use the strategy's logged fair value (logged every <= 1 s or on a
  0.2c change): markouts at 0.1–0.5 s partly reflect logging granularity. They are only as
  out-of-sample as the FV parameters (section 2a).
* **E6/E7 bucket recommendations** are selected and reported on the same window (a segment table,
  not a fitted model): confirm a quote/disable decision by rerunning `e67` on a LATER window before
  changing `quoting` limits.
* **Flow calibration from recordings** counts recorder downtime inside the window as exposure
  without trades (rates biased low); fit across outages only after checking the session records.
  |z| segments use a fixed 40 % vol (calibrate_flow convention), not the strategy's live sigma.
* **E2 perp basis** needs recorded perps; the synthetic demo has none. The BRTI replica is costly
  on deep books (`--no-replica`).
* **Chunked windows** (separate runs per day) reset strategy state (positions, limits) at chunk
  boundaries; positions open at a boundary are settled from recorded results but the next chunk
  starts flat.

## 7. Proposed changes outside dh/research (not made here)

* `dh.strategy.mm`: implement `fair_value.nowcast = brti_plus_composite` / `nowcast_beta` (the
  config keys exist, `_nowcast` ignores them) — E2's `NowcastMarketMaker` is the reference; expose
  a public pre-admission quote-filter hook (E3's `ToxicityGuardMM` overrides the private
  `_quote_market`); performance for replay throughput (vectorize the 9 band evaluations per
  market with `digital_vec`, cache `MarketSpec.tick_grid()`, skip the scenario-grid work for
  markets with no candidate inside the price floor/cap); an optional strike filter in
  `QuotingCfg` (e.g. `max_strikes_per_event`) so E9-style universes need no spec filtering.
* `dh.execution.exchange_sim`: pass the ticker to `fee_fn` / `order_fee_fn` (per-series fee
  schedules; replay_env maps the order id back to its ticker, and subclasses `_fill` on the
  fallback path) and honour `time_in_force` IOC/FOK for non-post-only orders (the remainder
  currently rests), so E8 could run through the simulator.
* `dh.backtest.runner.run`: an option not to retain every `Log` in `RunResult.logs` (replay_env
  uses `drive`, an exact copy of the protocol without retention, checked against `run`).
* `dh.kalshi.normalize`: carry the strike fields of lifecycle `created`/`metadata_updated` into
  `KalshiMarketLifecycle` (the core event has the fields; replay_env parses the raw frames).
* `.gitignore`: add `data/results/` (default output directory of `run_experiment.py`).
