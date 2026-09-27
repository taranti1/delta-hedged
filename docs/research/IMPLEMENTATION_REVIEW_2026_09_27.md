# Review fixes and validation — 2026-09-27

This change addresses the September 26 deep review's implementation defects and adds
the measurement needed for further research. It does **not** establish a profitable
strategy or authorize live trading. No account setup, subaccount/shard transfer, order,
watchdog change, service restart, or operation in the other trading project was performed.
The host-only `config/kalshi.yaml`, `config/live.yaml`, `config/paper.yaml`, and all of
`data/` remain local and Git-ignored.

## Findings and changes

| Finding | Implementation / remaining evidence |
|---|---|
| F01 restart accounting | Paper sessions explicitly start independent portfolios. Previous cash and marks are both excluded; existing halts and pauses persist. The lifetime audit reconstructs every fill and joins later settlements. This is **not** inventory restoration or a continuous simulated account. Live REST-based restart accounting is unchanged. |
| F02 feed latency | Paper exposes `md_ms`, default 25 ms, in the same simulator latency primitive used in research. This is a documented prior, not a new measurement. Old replay metadata preserves its original zero delay. Future runs need calibrated latency distributions and stress tests. |
| F03 replacement | A full-capacity quote can be evaluated for cancellation and replacement. The old exposure remains reserved until acknowledgment; a new quote must fit the current exposure. |
| F04 touch-only | Explicit placement restriction, applied to every candidate and existing quote; E4 uses it. |
| F05 flow features | Versioned feature definitions, nearest range boundary, and production binding validation. Approximate historical fits cannot silently enter real production/replay. Production-model-state flow fitting is still required. |
| F06 recording gaps | Recorded exposure and numerator filtering require healthy books, connection coverage, and fresh BRTI. Delayed messages require receive-time coverage, independently of exchange timestamps. |
| F07 timing | Fill logs preserve match/receive timestamps. Quote logs include order ID, decision time, actual forecast, feature version, and model identity. Short markouts require sufficiently recent observations and report actual sample age. Legacy missing match times remain explicitly counted. |
| F08 operator ledger | Full observed session durations include quiet time; unresolved fills stay visible. Canonical expiration clustering uses the research robust confidence interval rather than a one-event percentile interval. |
| F09 download checkpoints | Dataset-specific checkpoints; output structure validation; missing/corrupt outputs and empty BRTI files retried. No downloader was started in this task. |
| F10 fees and metadata | Subset downloads merge metadata and retain dated series snapshots. Historical fee reconstruction identifies intervals without dated evidence instead of treating a current schedule as historical proof. |
| F11 effective model | Unsupported legacy overrides fail visibly, supported caps/freshness controls are enforced, and effective/artifact model identities are logged. |
| F12 fee leakage | Per-order fee carry is modeled and residual discrepancies are checked at precise monetary units. |
| F13 allocation | Better opportunities can trigger safe cancellation of weaker retained quotes; rejected opportunities explain their binding constraints. Rate priors still need finite-horizon execution calibration. No profit improvement is asserted from code alone. |
| F14 conditional edge | An offline quote/fill outcome diagnostic grades causally linked pre-trade predictions in fixed bins, with day-clustered uncertainty. It excludes unknown/unsupported hedge costs. Legacy logs cannot retrospectively supply missing quote links or match times. No new fitted trading rule is promoted from this small sample. |
| F15 portfolio risk | Canonical settlement groups and incremental scenario risk preserve adverse fill subsets. Separate conservative collateral reservations enforce the configured capital bound. Unresolved fills, cancel races, and pending amendment increases remain reserved. |
| F16 evidence gates | Hashed input inventories, changed-input checks, chronological candidate confirmation, explicit unknown fee coverage, and `tradable=false` for public-tape exploration. An independently locked executable-policy holdout is still required. |
| F17 storage/analysis | Streaming compressed readers and two-pass selective mark retention. Lossless archival of completed paper logs with SHA-256 round-trip verification. General event replay still retains the marks supplied to it; this change specifically bounds log attribution memory by fills rather than all fair-value messages. |

## Lossless compression completed

All ten paper logs had terminal `session_end` records. A targeted open-file check found
no open handles; the paper heartbeat already reported stopped. No process was stopped
by this task. Each archive was published only after full decompression reproduced the
original SHA-256 and byte count, with repeated source identity checks.

* Before: **6,230,505,376 bytes**.
* After: **520,993,484 bytes**.
* Saved: **5,709,511,892 bytes (91.64%)**.

Archives remain in `data/paper_logs/*.jsonl.zst`, with per-file `.archive.json` verification
receipts. Raw recording streams and almost all historical Parquet files were already
compressed with Zstandard and were left in their native formats. No observations were
sampled, rounded, aggregated, or discarded to obtain the saving.

To restore an original log (leaves the archive intact and refuses to overwrite a file):

```sh
.venv/bin/python -m dh.store.archive_logs --restore --execute data/paper_logs/NAME.jsonl.zst
```

Do not include both restored and compressed copies of the same session in an audit.
The ledger CLI rejects duplicates. For additional logs, the archiver only accepts
completed `paper_logs/paper-*.jsonl` sessions; it rejects an absent terminal marker,
symlinks, source changes, and existing destination files.

## Rebuilt paper audit

The new streaming audit read all ten archived sessions, including later outcomes for
earlier sessions. Results are stored locally in
`data/results/review_fixes_20260927/lifetime_paper_fills.csv` and
`lifetime_paper_summary.json`.

* 314 fills; 302 settled, 12 unresolved (50 contracts).
* 1,102.43 settled contracts across 38 expirations and roughly 1.717 observed session-days.
* Settled simulated net P&L: **+$22.6691**, with logged zero fees and no hedging.
* Net edge: **+2.0563 cents/contract**, expiration-cluster 95% interval
  **[-2.5901, +8.7731] cents/contract**.
* The strongest expiration earned $18.95; excluding it leaves $3.7191.
* All 314 legacy fills lack exchange execution timestamps. These are old simulations,
  including the old latency assumptions, not a test of the fixed strategy.
* Only 1,593 fair-value observations needed to be retained for attribution, rather than
  every fair-value record in 6.23 GB of logs.

The confidence interval includes losses. These observations still do not establish a
positive executable edge, a scalable capacity, or a real-money budget. Missing outcomes
remain unresolved; the audit never silently converts them to zero profit.

Rebuild the offline lifetime audit:

```sh
.venv/bin/python -m dh.live.tools ledger --logs-dir data/paper_logs --data data/paper --outcomes-root data/external/kalshi
```

Grade future logs with the new causal quote IDs (legacy logs will be reported as missing
pre-trade prediction evidence):

```sh
.venv/bin/python -m dh.research.execution_audit --logs-dir data/paper_logs --data data/paper --outcomes-root data/external/kalshi --out data/results/execution_audit
```

## Validation and next decision

Final complete offline suite: **1,372 passed, 9 network-marked tests deselected**
(237.85 seconds). Temporary loopback servers were permitted for local integration tests;
no authenticated exchange experiment was performed. Test output is retained locally at
`data/results/review_fixes_20260927/tests_final.txt`. The run reported 16 warnings,
principally the Python multiprocessing fork warning in multithreaded tests; no failures.

A fresh independent agent reviewed the code and prompted additional fixes for unresolved
order exposure, cancel-during-amend adverse prices, receive/exchange clock consistency,
reconnect backlog, hedge-cost eligibility, and implementation/config identity. It then
rechecked the fixes, ran focused regression batches, and reported no remaining actionable
correctness issues in the reviewed changes. Its assessment is bounded to source and
offline behavior, not live integration certification or profitability.

All three host-only configuration files were checked unchanged. No files from `data/`
or host-only configuration are tracked; the complete data directory is now explicitly
ignored as well. Validation was completed before committing; no deployment was performed.

Next research work is a fresh paper evaluation with these assumptions recorded, followed
by later-period confirmation of a locked policy. Do not reuse old exploratory “GO” labels,
proxy flow tables, or in-sample forecast improvements as permission to trade. Preserve
the existing E0/paper no-live decision and the existing disabled hedge default.

## Independent sign-off review (2026-09-27, after b53216a)

Three further independent reviews (execution/risk, accounting/runtime, research/data) re-read
b53216a against its surrounding code. Signed off with the following corrections, each with a
regression test:

| Area | Defect | Correction |
|---|---|---|
| Strategy (regression) | `event_over_limit` indexed `self.specs[w.ticker]` for every order with fillable qty, including terminal orders of settled markets that `prune_settled` had already dropped: `KeyError` on every cycle | `prune_settled` keeps any market with an order that can still fill; the risk loops skip unknown specs/groups (`tests/strategy/test_signoff_fixes.py`) |
| Strategy (F15, found by the first full-day replay) | `_risk_groups` now includes expired, unsettled markets with exposure; when their settlement window cannot be reconstructed (every observation skipped: prints gone or a benchmark outage), `window_state` raised and stopped the strategy (live: exit 4) | Undefined window = unknown outcome, scored over the full price range (worst case), counted in `risk_window_undefined` |
| Strategy (F13) | A still-blocked candidate evicted one more retained quote every cycle before the first `opportunity_cost` cancel was acknowledged (lost quotes, nothing placed) | At most one eviction outstanding until its cancel resolves |
| Strategy (F12) | Fee check: 1-micro tolerance plus a whole-session cumulative residual, halting (sticky across restarts) on benign rounding convention differences never validated against real fills | Per-fill tolerance = the fill's rounding/rebate part (same bound as the live runner's `reconcile_fill_fee`), residual bounded per order, reconciliation state bounded |
| Ledger (F07) | Subsecond markouts required an observation within h/2; the strategy logs F every 1 s or on > 0.2c moves, so 0.1-1 s markouts survived only when F jumped (biased subset, no count shown) | A logged value bracketed by the next log within the 1.25 s cadence is current; the streaming audit retains that next observation; summaries print `markout_{h}s_n` |
| Audit (F17) | Duplicate plain/compressed copies rejected only by the CLI; a `.zst` truncated at a line boundary ended silently | `ledger_from_logs` rejects duplicate sessions; the `.zst` reader raises on an unfinished frame |
| Flow fit (F05/F06) | Numerator required exchange time <= receipt; with this host's clock skew 60-90 % of real taker orders were dropped while exposure stayed in the denominator | Receipt decides coverage; the exchange time may lead it by <= 1 s |
| Downloader (F09) | Deliberately written old empty BRTI hours were refetched on every run | Valid (schema) hour files are done |
| Evidence (F10/F16) | `manifest_matches` could raise after a full run; explicit null fee multipliers raised | Returns False; null means the default |

Also added for the next evaluation: `run_experiment.py replay --queue-stress FRAC,CONTRACTS`
(every order arrival also waits behind undisplayed priority; can only remove fills, property
test `tests/execution/test_queue_stress.py`) and `--prime-search-h`.

Known and left as is (documented, not defects for the M1 configuration): `touch_only` classifies
existing orders from a book that includes our own orders in live but not in paper/replay (it is
off in `config/m1.yaml`; E4 touch-only results would not transfer as-is); new-order loss is
scored over the ±`stress_move_frac` scenario grid while the collateral cap still bounds far
strikes; the collateral bound also blocks position-reducing quotes once capital binds; fitted
flow tables cannot be bound to real replays until a production-state fitter exists.

The rebuilt lifetime paper audit is unchanged in P&L (+$22.6691, +2.06c/contract, CI
[-2.59, +8.77]c over 38 expirations); with the markout fix, markouts now cover 296-302 of 302
settled fills and are negative at every horizon (-0.05c at 0.1 s to -1.50c at 60 s): adverse
selection is present and grows with the horizon.
