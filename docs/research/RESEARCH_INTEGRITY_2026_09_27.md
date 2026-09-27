# Research integrity corrections — 2026-09-27

These changes improve measurement and fail closed on unsupported evidence. They do not
establish positive edge or authorize live trading. The research changes did not rewrite
historical datasets or host-only configuration. Separate lossless paper-log archival is
documented in `IMPLEMENTATION_REVIEW_2026_09_27.md`.

## Flow fitting

Flow tables now carry `feature_version`. The current historical fitter explicitly uses
`spot_fixed_vol_nearest_boundary_v2`: both boundaries of a range are considered, but its
volatility and settlement-window approximation remain different from production.
Non-synthetic replay rejects this proxy, missing versions, and bare segment mappings.
The production contract is `model_remaining_average_nearest_boundary_v1`. Loading a JSON
for inspection is allowed; binding it to execution must validate that contract. A future
fitter must replay the causal production model state before writing that version; merely
renaming the metadata is invalid. Synthetic fixtures may exercise wiring when both the
recording and artifact are explicitly marked synthetic; their evidence gates remain.

Recording fits now require healthy market exposure: valid snapshots, no unresolved
sequence/disconnection status, public-frame freshness at most 5 seconds, and BRTI source
and receive freshness at most 3 seconds. Silent gaps invalidate books until a fresh
snapshot. Numerator observations and denominator time use the same half-open intervals;
observed quiet time remains in the denominator. These thresholds are conservative data
quality assumptions, not latency calibration. Coverage identity is included in provenance.
The exposure grid is one second. Input trades retain exchange timestamps for sweep identity and receive timestamps for
causal features and health filtering. A sweep is available at its latest receipt, and all
of its receipts and its exchange match time must lie inside one healthy interval.
Reconnect backlog from a preceding gap is excluded, rather than assigned fresh flow on
recovery. Source times later than receipt are conservatively excluded in covered fits. Archive inputs without receipt
timestamps remain exchange-clock proxies. This does not recover unrecorded messages.

## History downloads

Markets, trades, and candles have independent checkpoints. Completed files must still
exist with a readable Parquet footer and matching schema. Legacy checkpoints migrate only
when the requested output exists and validates. Empty/unreadable BRTI outputs are retried.
This is a structural check, not proof of complete exchange tape or absence of corrupted
individual data pages. Reconciliation of trade IDs and exchange volume remains necessary.

Fee and incentive refreshes upsert identities while preserving other series and past
records. New series snapshots are archived with fetch times. Historical E0 fee evaluation
uses effective-dated series/event changes and snapshots; current metadata never proves
fees before its fetch time. Undated event overrides remain unknown. Unknown rows retain a
clearly identified descriptive fee estimate and block promotion. Earlier snapshots lost
before this fix cannot be reconstructed from today's metadata alone.

## E0 evidence

Each new run records SHA-256 identities of inputs, source, fitted model artifacts and fee
configuration, including uncommitted changes; exact parameters; and whether identities
changed while it ran. Host-only configs and secrets are excluded. Full-sample winners are exploratory candidates. A candidate
in the later half must also have passed selection in the earlier half with the same
minimum-event requirement. This split does not itself certify an untouched holdout: the
historical dataset has already been inspected. All public-tape reports remain
`tradable=false` and list promotion blockers, including incomplete fee history and absent
locked-policy/untouched-holdout attestation. Newly selected rules need a registered policy
and genuinely later data plus executable net-profit evaluation before promotion.

Large historical studies were not rerun during implementation. Use an immutable local
snapshot for that work and do not compete with active trading for network or CPU capacity.
