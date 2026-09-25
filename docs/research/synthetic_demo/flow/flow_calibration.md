# Taker-flow calibration from a recording (time split)

**SYNTHETIC DATA — pipeline validation only**

Window 2026-09-24T12:00:00.000Z .. 2026-09-24T13:00:00.000Z; 5977 public prints, 36 fully observed markets; BRTI ticks as the point-in-time reference.

Method: chronological; training data end (UTC ms): 1790253900000; whole-sample fit (flow_segments.json) data end: 1790254800000 -- use it only for replays that start later. Rows `in_sample` grade the fit on its own training data; `out_of_sample` rows grade it on later data only. ratio_ct = predicted / realized taker contracts; wape_ct = sum |pred - realized| / realized over segments; dev_explained = 1 - Poisson deviance(model) / deviance(pooled per-side rate) of order counts.

Provenance of `flow_segments.json` (look-ahead guard): fitted on data 2026-09-24T12:00:02.100000Z .. 2026-09-24T13:00:00Z (every datum before the end); dataset sha256:c7aee4e525803ba2 (recording /private/tmp/claude-501/-Users-thomast-Desktop-delta-hedged/d71a108d-06fa-4bfc-b8c3-afada2d04599/scratchpad/demo_data [2026-09-24T12:00:00.000Z .. 2026-09-24T13:00:00.000Z]: kalshi.ws public prints + BRTI ticks); method: calibrate_flow.fit_segments: gamma-Poisson shrunk segment rates (prior_s=1800, min_orders=30, vol_ann=0.4); every market of the sample (graded by a chronological split).
Provenance of `flow_segments_train.json` (look-ahead guard): fitted on data 2026-09-24T12:00:02.100000Z .. 2026-09-24T12:45:00Z (every datum before the end); dataset sha256:56779d7a76bd83c4 (recording /private/tmp/claude-501/-Users-thomast-Desktop-delta-hedged/d71a108d-06fa-4bfc-b8c3-afada2d04599/scratchpad/demo_data [2026-09-24T12:00:00.000Z .. 2026-09-24T13:00:00.000Z]: kalshi.ws public prints + BRTI ticks); method: calibrate_flow.fit_segments: gamma-Poisson shrunk segment rates (prior_s=1800, min_orders=30, vol_ann=0.4); training markets of a chronological split by expiration (train_frac=0.7).

| sample | fit_on | eval_on | markets | segments | exposure_h | orders | realized_ct | pred_ct | ratio_ct | wape_ct | dev_model | dev_pooled | dev_explained |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| in_sample | train | train | 27 | 40 | 13.28 | 4211 | 78855.0 | 49616.2 | 0.6292 | 0.6818 | 6473.4 | 12727.5 | 0.4914 |
| out_of_sample | train | test | 9 | 31 | 4.425 | 1365 | 24932.0 | 16644.1 | 0.6676 | 0.7376 | 1657.4 | 3569.6 | 0.5357 |

