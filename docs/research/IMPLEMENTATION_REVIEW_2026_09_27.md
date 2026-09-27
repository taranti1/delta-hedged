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
