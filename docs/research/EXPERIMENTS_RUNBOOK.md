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
reference; every P&L table flags `holds_only_under_A`, and no verdict can accept without B and C),
output `<root>/results/<name>/` (one CSV per table, one short markdown with the decision rule and
the verdict, and `<name>_verdict.json`). `--t0/--t1 auto` = coverage of the `kalshi.ws` stream.
Start every analysis with `universe` (market specs, fee table, rejected markets, own fills found
in the window). A flag the chosen command does not use is an error (nothing runs), so a setting
can never be silently ignored.

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
  `event_fee_update`) over the series (+ scheduled `/series/fee_changes`) over the market, using
  ONLY records received by then (never a later series/event snapshot or a later market update);
  a market whose fee becomes known only after its spec becomes available at that later time
  (`resolve_fee_availability`). A fee change inside the window is reported in the notes (the
  replay keeps the fee in force at availability). Markets without a resolvable fee are never
  quoted (the strategy refuses).
* **Events.** `ReplayStream` = `dh.store.replay` normalization (the live sequencer for
  `kalshi.ws`) over `[t0 - 15 min, t1 + 5 min)`, extended back to the LAST `orderbook_snapshot` of
  every quotable market before t0 (searched back `prime_search_s`, default 6 h): records before
  t0 prime books, connection state and the settlement window (synthesized snapshots and a
  `kalshi.ws connected` status at t0); only settlement messages pass after t1. Markets that never
  get a valid book are counted (`summary['books_never_valid']`) and warned about (they are never
  quoted). REST order-book snapshots (`kalshi.rest.orderbook`) are
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
  CF-history REST records); `csv:<path>` = a price file (`ts_ms|ts_ns|timestamp` + `price|value`
  for point-in-time prices, or `close` for OHLC bars stamped at their OPEN, which are used only
  from their close = stamp + bar length); `recorded+gbm` = if still not ready, a seeded GBM
  history anchored on the first price known BEFORE t0 (flagged SYNTHETIC in the summary and
  warnings; no price before t0 = no fallback, never a later tick). If the model is not warm at t0
  the strategy does not quote until it is; the CLI prints a WARNING.
* **Simulation.** `KalshiExchangeSim` (per-ticker fee schedules and per-order fee rounding via a
  thin subclass), fill policies A/B/C, latency (`replay_env.research_latency`): the
  `LatencyModel` placeholders for submit/response/ws (30/30/10 ms lognormal) plus the Kalshi
  MARKET-DATA latency measured on the recording (median receive - exchange time of trade/delta
  messages received in the 2 h before t0, else the first 10 min after t0; documented fallback
  25 ms), or `--latency-ms submit,response,ws[,md]`; policy C scales everything x1.5. The library
  default md = 0 would let our orders win races they lose. Every report prints the full latency
  model. Settlements missing from the stream are filled from recorded REST results after the run.
  Deterministic: same inputs and seeds, same numbers (`tests/research/test_replay_env.py`);
  truncating a window leaves every earlier action unchanged (`test_replay_fixes.py`); `--jobs N`
  runs variants in parallel processes with identical results.
* **Ledger columns.** `event` = the settlement cluster (expiration time: KXBTCD, KXBTC and
  KXBTC15M markets expiring together settle on one BRTI average, so they are ONE event for every
  CI and count), `event_ticker`, `expiration_ns`, regime columns (`day`, `weekend`, `rv_1h` =
  benchmark realized vol over the prior hour, causal) and status columns (`synthetic`,
  `fv_status`, `flow_status`) that travel with pooled ledgers (`e67 --ledger`).

## 2b. Inference and evidence guards

* **Intervals.** Every CI is the hull of a studentized cluster bootstrap-t (cluster-robust
  linearized SE) and a jackknife-t interval, over settlement events (expirations). The plain
  percentile bootstrap was anti-conservative: simulated one-sided error P(lo > 0 | mean 0) with
  settlement-driven P&L was 4.6 % (20 events) / 3.9 % (200) against 2.5 % nominal; the hull gives
  2.5 % / 2.8 % (4 000 / 3 000 simulated datasets; `tests/research/test_inference.py`). A day-block
  CI (UTC days as clusters) is a second check wherever a decision is made, applied from 5 days on.
* **Multiplicity.** One-sided jackknife-t p-values with Holm: E0 across every segment examined,
  E6/E7 within each table family, E4 across its pre-registered keep-priority variants, E9 across
  its variants.
* **Verdict guards** (`exp_common.Report`; `<name>_verdict.json` keeps both fields): a decision
  becomes INCONCLUSIVE when fewer than 20 settlement events stand behind it, when a policy-based
  experiment lacks results under BOTH B and C, and (ACCEPT only) on a synthetic recording or with
  fitted inputs in sample or of unknown status. The rule's own outcome is a separate field and
  never appears inside the INCONCLUSIVE text.
* **Economic acceptance.** E2 needs contracts/day, profitable contracts/day and $/day not lower
  under B and C on top of the net c/contract CI; E3 needs $/day and PROFITABLE contracts/day (the
  TEST_MATRIX wording) not lower, since its own <= 20 % fill-loss rule allows fewer total
  contracts; E4/E9 need $/day up. The day-block second check applies to E2, E3, E4, E8, E9 and the
  E6/E7 confirmation sample.
* **Regime splits** (TEST_MATRIX): tau bucket, realized-vol tercile and weekday/weekend tables for
  E2 (forecast gain by vol/weekday; P&L hook by all three), E3, E4, E8, E9 and E10; E1 by
  calendar month (stability).

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
is the E0 convention); every replay command also takes `--root/--t0/--t1/--out/--jobs/--warm/
--seed/--latency-ms`. A verdict backed by fewer than 20 settlement events (expirations) is
printed as INCONCLUSIVE (section 2b; the rule's own outcome is a separate field), and E6/E7
buckets need >= 20 events under both B and C for any recommendation.

| # | module | data needed | command | replays (B,C) | outputs | decision rule (docs/TEST_MATRIX.md) |
|---|---|---|---|---|---|---|
| E1 | `exp1_staleness.run` | >= 7 days of Kalshi L2 + BRTI 5 Hz + venue books (stability: several months) | `run_experiment.py e1 [--step-ms 100] [--latency-ms s,r,w,md]` | 0 (panel) | `e1_staleness_lead_lag.csv` (lag coefficient c_h on the PAST external move, measured beyond the receive latency, with CIs over time blocks shared by all markets; response in ticks to a > 2 sd move; gap-closure b_h for reference only), `_by_month.csv` | accept if c CI > 0 at some h <= 1 s beyond the receive latency, response >= 0.5 tick, in every calendar month; reject if the response CI upper bound < 0.2 tick at every h <= 1 s |
| E2 | `exp2_nowcast` | >= 3 days (folds = UTC days; 7 recommended) of BRTI 5 Hz + >= 4 constituent books (+ perps for basis) | `run_experiment.py e2 [--step-ms 200] [--split 0.5] [--no-replica] [--no-pnl]` | 4 (P&L hook on the second part) | `e2_nowcast_forecast.csv` (OOS RMSE/MAE by horizon: last print, median mid, replica, ridge, LightGBM; gain CI), `_window_average.csv`, `_pnl_hook.csv`, `_forecast_regimes.csv`, `_pnl_hook_regimes.csv` | accept if RMSE improves >= 10% at 0.2–1 s AND replayed net c/contract improves (paired CI > 0) with contracts/day and $/day not lower, under B and C; reject if < 5% or no P&L gain |
| E3 | `exp3_toxicity` | >= 7 days (>= 5 000 shadow fills, >= 200 events); live fills when M1 trades (`--live-fills`) | `run_experiment.py e3 [--split 0.5] [--live-fills]` | 2 + 4 | `e3_toxicity_markouts.csv` (net markout vs fill price 0.1–60 s + settlement), `_models.csv` (walk-forward AUC/log-loss/Brier, OOS R^2), `_oos_lift.csv`, `_univariate.csv`, `_cancel_rule.csv`, `_regimes.csv`, `_cancel_rule_regimes.csv`, per-fill `e3_toxicity_fills_<p>.csv` | accept if the cancel rule raises net c/contract >= 0.1c (CI > 0) at <= 20% fill loss with $/day not lower, under B and C; reject if no OOS lift (Brier-lift CI upper bound <= 0) or the lift vanishes under C |
| E4 | `exp4_queue` | >= 7 days | `run_experiment.py e4 [--grid variants.yaml]` | 16 (8 variants) | `e4_queue_variants.csv` (net c/ct CI, $/day, fills/day, quote-hours, $ per quote-hour, paired diff vs `recenter_always` with p-value and day-block CI), `_fill_position_mix.csv`, `_regimes.csv` | accept a pre-registered keep-priority variant (config, hysteresis_strong, age_only_5s; Holm across them) if it beats always-re-centering by > 0.05c/contract (CI > 0) and in $/day under B and C; else reject (differences within CI) |
| E6/E7 | `exp67_segments` | >= 14 days (tau x \|z\| cells) + a later confirmation window | `run_experiment.py e67 [--split 0.5] [--confirm-t0 T --confirm-t1 T'] [--ledger L.csv ...]` | 3 (A,B,C) (+3 on the confirmation window) | `e67_segments_tau.csv`, `_abs_z.csv`, `_yes_price.csv`, `_tau_x_z.csv` (net c/ct CI, p-values, Holm flags, confirmation lower bound, fills/day, markouts, toxic share, recommendation) | quote only buckets Holm-significant > 0 in the selection sample AND with CI lower bound > 0 on the disjoint later sample, under B and C; disable buckets Holm-significant < 0 (B or C) |
| E8 | `exp8_taker` | >= 7 days | `run_experiment.py e8 [--step-ms 250] [--latency-ms 40,40,15,25]` | 8 scans (one per threshold and policy) | `e8_taker_summary.csv` (per threshold scan: opportunities/day, fill rate after latency, net c/ct at 5 s / 60 s / settlement with CI and day-block check, $/day), `_by_touch_staleness.csv`, `_regimes.csv`, `e8_taker_takes_<p>.csv` | accept if the 0.5c scan nets > 0.5c/contract after the exact per-order fee with CI > 0 and >= 20 opportunities/day, under B and C; otherwise taking stays disabled |
| E9 | `exp9_multistrike` | >= 7 days | `run_experiment.py e9 [--strikes 1,3,0]` | 6 | `e9_multistrike_variants.csv` ($/day, net c/ct, peak/mean collateral, mean \|D\|, netting ratio, delta turnover per contract), `_paired.csv` (decision CIs), `_regimes.csv` | accept if $/day is up (paired CI over settlement events) and delta turnover per contract is down (paired CI over 1 h blocks) vs the single best strike, under B and C, Holm across variants |
| E10 | `exp10_capacity` | >= 7 days | `run_experiment.py e10 [--multipliers 1,2,5,10,20,50] [--no-scale-limits]` | 12 | `e10_capacity_by_size.csv` (net c/ct CI, fills/day, contracts/day, share of the QUOTABLE taker flow, inventory sd), `_capacity.csv` (largest clip multiple above 1.0 / 0.75 / 0.5 / 0.05 c and breakeven, point and CI-lower-bound), `_regimes.csv` | measurement: report capacity (no market impact modeled: upper bound) |

E0 is `dh.research.exp0_maker_pnl` (public trades vs settlement: `scripts/download_kalshi_history.py`
tables, no recording needed; `python -m dh.research.exp0_maker_pnl --trades T --markets M [--btc B]
[--fee-type KXBTCD=quadratic_with_maker_fees ...]`): clusters = expirations, the exact per-order
maker fee (rounded up to the cent; each print priced as one order, an upper bound) by series fee
type, only segments with >= 200 settlement events, Holm across every segment examined, coded
verdict (`exp0.md`, `exp0_verdict.json`). E1 runs on the own-footprint-filtered `ReplayStream`
with the universe's specs and the benchmark's vol realized over the 6 h before t0. E5 is
`dh.research.hedge_study` (real BTC paths); it does NOT rerun through the replay: `drive()` has no
hedge-venue simulator, so `hedge.enabled` in a replay config produces hedge orders that never
fill. A fill-based E5 needs a hedge simulator wired into `drive` (proposed, section 7).

Per-experiment method notes:
* **E1.** x = research fair value from the external composite, y = Kalshi mid on a 100 ms grid.
  The decision statistic is the lag coefficient c_h of y(t+a+h) - y(t+a) on x(t) - x(t-1 s), with
  a = the measured Kalshi receive latency rounded up to the grid (a pure delivery delay therefore
  shows no response); CIs over 60 s time blocks shared by every market (the BTC path is common);
  response in ticks = c_h x the mean |external move| over moves > 2 sd. The gap-closure slope b_h
  and the gap-after-move economics are reported for reference only: quote noise and fair-value
  model error make them positive in a zero-lag market (the null test in `test_e1_statistic.py`).
* **E2.** Features use events received <= t; targets are the benchmark as received at t + h;
  walk-forward folds are never shuffled and have an embargo of max(h) + 5 s. The window-average
  table scores `(sum_fixed + m * S_hat) / n` in the last 120 s before each expiry against the
  realized 60-print average. The P&L hook fits `beta` (share of the venue-vs-BRTI gap closed
  within 0.5 s) on rows of the first part whose target ended 5 s before the split, and replays
  `NowcastMarketMaker` (nowcast = brti_last + beta x (median venue mid - brti_last)) against the
  baseline on the rest.
* **E3.** Label: toxic = the fair value moved against the fill within 10 s (the strategy's logged F
  for replayed fills; the probe's fair value for live fills); the P&L target is the net 10 s markout
  vs the fill price after fees.
  Features are computed by one function at fill time and at decision time (queue at placement,
  quote age, adverse venue move 0.1–5 s sign-adjusted for the market's delta, touch imbalance,
  venue imbalance, taker volume 10/60 s, tau, \|z\|, price, spread, sigma, fair-value edge). The
  cancel rule (logistic P(toxic) >= threshold chosen on training fills, <= 20% removed, scored on
  the 60 s net markout) is trained on POLICY-B fills whose 60 s label ended before the split and
  replayed by `ToxicityGuardMM` on the held-out part. Replayed-fill features are captured at MATCH
  time through the simulator's fill hook (the strategy has seen only earlier events), and markouts
  use the logged fair value only when it is at most 5 s old.
* **E4.** Variants: `recenter_always` (benchmark: replace_rel = 0, min age 0, kappa 0), `config`,
  `hysteresis_strong`, `age_only_5s`, `join_only` (improve multiplier 0), `improve_x2`,
  `touch_only` (no quotes behind the touch), `wide_ladder`; custom grids: JSON/YAML
  `{name: {section: {field: value}}}`. Only the keep-priority variants (config, hysteresis_strong,
  age_only_5s; every variant of a custom grid) are tested, with Holm across them; the placement
  variants are descriptive.
* **E6/E7.** Selection on the earlier sample (Holm within each table family), confirmation on a
  disjoint LATER sample: `--confirm-t0/--confirm-t1` (replayed separately) or the built-in split
  of settlement events by expiration at `t0 + --split x (t1 - t0)`. The verdict states where the
  buckets were selected and confirmed. Pooled ledgers keep their status columns; a ledger without
  them has an unknown status and cannot ACCEPT.
* **E8.** Opportunities use the strategy's own band (buy if F_lo - ask - exact per-order taker fee
  >= threshold); one marketable order per episode, arriving after a sampled submit latency plus
  the market-data latency against the book as recorded then (queue-empty check; our consumed size
  is remembered per level) and charged through an OrderFeeAccumulator. Each threshold is its own
  scan (an episode starts when the edge first reaches THAT threshold); the verdict uses the 0.5c
  scans. The exchange simulator is not used here because it does not implement IOC (section 6).
* **E9.** `NearestStrikes(n)` keeps, per event, the n strikes nearest the last BRTI tick RECEIVED at
  the event's first quotable time max(earliest availability, earliest expiry - horizon, t0); a
  strike listed later is judged at its own availability; no earlier tick = event skipped. The
  strategy never sees the other strikes.
* **E10.** Clip size x k with every size-denominated risk limit x k (`--no-scale-limits` keeps them).
  Flow share = our contracts / public contracts traded in markets the strategy could quote at the
  time of the print (known, 0 < tau <= max_tau_s, far strikes only inside min_tau_s).

## 5. Synthetic demo (pipeline proof, not evidence)

```
python scripts/run_experiment.py demo --jobs 4              # ~39 min on 4 cores -> docs/research/synthetic_demo/
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
with informed flow on the same price path. E1's lag coefficient accepts an injected 1.5 s lag and
rejects a zero-lag market with quote noise and model error, and a pure receive delay
(`tests/research/test_e1_statistic.py`). The demo window has 4 settlement events, so every
event-based verdict in it is INCONCLUSIVE by the 20-event guard, and every ACCEPT would be capped
anyway (synthetic recording); each report keeps the rule's own outcome in a separate field, and the
README lists both. The flow stage shows the in-sample / out-of-sample table (`flow/`). Leakage
guards are tested in `tests/research/test_leakage_guards.py` (causal BTC bar join, time-split
flow calibration, in-sample FV / flow labels, replay wiring of fitted flow segments); the audit
fixes in `test_verdict_guards.py`, `test_inference.py`, `test_multiplicity.py`,
`test_replay_fixes.py` and `test_e8_e3_cli_fixes.py`, each with null (zero-effect) cases that must
not ACCEPT.

## 6. Limitations (read before trusting a number)

* **No market impact**: other participants never react to our simulated orders; E10 capacity is
  an upper bound. Queue position is modeled (policies A/B/C), not observed; calibrate against
  live `queue_positions` (dh.execution.queue.QueueCalibrator) before believing B.
* **Latency**: submit/response/ws are placeholders until measured live; market-data latency is
  measured on the recording (section 2). E8 is latency-sensitive (`--latency-ms`).
* **Fees**: one fee schedule per market for the whole window (changes inside the window are
  listed in the report notes). Simulated fills and E8 takes pay Kalshi's per-order balance
  rounding with carried rebates (`OrderFeeAccumulator`, balance precision from
  `config/fees.yaml`, $0.01 for non-direct members; `summary['fee_model'] == 'per_order_rounding'`);
  E0 prices each public print as one maker order (an upper bound on the fee). All follow
  `config/fees.yaml`, which is not yet verified against live fills.
* **Own-footprint filter**: approximate at the millisecond scale (a level decrease that precedes
  its print is corrected when the print arrives); the counterfactual assumes the takers who hit
  our orders would still have traded the same total (their other prints are kept).
* **Fair-value markouts** use the strategy's logged fair value (logged every <= 1 s or on a
  0.2c change): markouts at 0.1–0.5 s partly reflect logging granularity. They are only as
  out-of-sample as the FV parameters (section 2a).
* **E6/E7**: 'quote' requires confirmation on a later sample (built in); 'disable' is decided on
  the selection sample with Holm (the safe direction). With a short window the built-in split
  leaves few settlement events on each side: prefer `--confirm-t0/--confirm-t1` on a later week.
* **Strategy fill/adverse parameters** in the config carry no fitting window; the look-ahead
  check covers the FV parameters and bound flow segments only (reports say so).
* **Flow calibration from recordings** counts recorder downtime inside the window as exposure
  without trades (rates biased low); fit across outages only after checking the session records.
  |z| segments use a fixed 40 % vol (calibrate_flow convention), not the strategy's live sigma, and
  exposure |z| is evaluated at each 60 s cell start, so in the final minute (where |z| moves fast)
  some orders land in segments with no exposure. The gamma-Poisson prior (`--flow-prior-s`, 1800 s)
  dominates segments with less exposure than that: on short windows the fit under-predicts busy
  segments even in sample (the synthetic demo's 1-hour window: predicted/realized 0.63 in sample).
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
* `dh.backtest` / replay: a hedge-venue simulator in `drive()` so E5 can rerun on replayed fills.
* `dh.strategy.config`: a `fit_window` / `data_end_utc` for the fill and adverse-selection
  parameters so the look-ahead check can cover them.
