# delta-hedged

A deterministic research and trading system for **passive market making on Kalshi BTC threshold
markets** (`KXBTCD`, with `KXBTC` / `KXBTC15M` support). It adds portfolio-level BTC hedging
only where it pays. The same Strategy object runs live, in paper (shadow) mode, and in replay
of recorded data.

**Start with [`docs/BUILD_PLAN.md`](docs/BUILD_PLAN.md)** (deliverables A-K). Every claim there
is tagged [VERIFIED] (real data, reproducible), [BUILT] (implemented and tested offline, live
verification pending) or [ESTIMATE]. [`docs/CORE_QUESTIONS.md`](docs/CORE_QUESTIONS.md) answers
the 12 core research questions one by one, each with its status and the test that settles it.

## What is established so far

| Finding | Evidence |
|---|---|
| Per-fill delta hedging of Kalshi binaries is uneconomic: a one-way at-the-money hedge costs 0.6-13c vs a 0.15-0.5c edge target | `docs/00_PREMISE_CHALLENGE.md` |
| Continuous delta hedging raises P&L variance at 5-12 bp perp fees. At M1 size the optimal hedge is none; a mean-variance band pays only at scale and <= ~1 bp costs | Experiment 5 on 14,967 real BTC hours: `docs/research/05_hedge_policy.md` |
| Best fair value: seasonal horizon-weighted EWMA vol + Student-t tails, -12.1 mn log loss [-12.8, -11.4] vs a Gaussian digital with 2h EWMA vol; calibration error 0.2-0.5c | 15,005 out-of-sample hours: `docs/research/01_fair_value_calibration.md` |
| Gaussian tails understate 3-sd outcomes ~4-5x, so the naive "sell longshots" edge largely disappears | same |
| Ignoring the 60-print settlement average misprices a 1-sd strike by 4.8c at 2 min and 12c at window open | same |
| Kalshi streams the settlement benchmark (BRTI, 1 Hz and 5 Hz) on its own WebSocket, with REST history | `docs/kalshi_specs/asyncapi.yaml`, `docs/DATA_SOURCES.md` |
| [VERIFIED] Settlement convention: `expiration_value` = average of the 60 BRTI prints stamped T-60 s .. T-1 s with T = `close_time` (not `expected_expiration_time` = close + 5 min), rounded half up to cents; identical across KXBTCD/KXBTC/KXBTC15M. Matches 13/13 recorded expirations and 10,015/10,075 in the CF history (the 60 misses are half-cent ties before 2026-08-21, when Kalshi rounded the binary double; explained exactly). Kalshi's streamed `last_60s_windowed_average_15min` is one second off and is NOT the settlement value | `docs/research/M1_2_SETTLEMENT_CHECK.md` |
| [VERIFIED] KXBTCD, KXBTC and KXBTC15M charge no maker fee today (`quadratic` x1) and the account's balance precision is $0.0001 | `scripts/verify_fee_schedule.py`, `docs/RUNBOOK.md` |
| [VERIFIED, in-sample, 30/14/7 days] Experiment 0 on 36 M public prints: the average maker nets +0.18c to +0.26c per contract (CIs include 0.15c), last-in-queue fills (policy C) lose 0.6-0.8c; **no segment clears 0.15c under both B and C**, so M1.1 keeps nothing yet (K.1's hard falsification is not met either) | `docs/research/E0_RESULTS.md` |
| With maker fees, each order pays its exact fee rounded up to the cent, so the cost per contract depends on order size: 0.50-0.60c near 50c, and at least 0.50c for any 2-lot. The quoter prices the exact per-order fee and picks fee-efficient sizes (formula from the fee rules; to be confirmed on live fills in M1.3) | `docs/MODELS.md` s.3, `dh/kalshi/fees.py` |

**Not yet established: whether net edge exists after fills.** Public-tape E0 on one month finds no
segment that clears the bar for a back-of-queue maker; fill-conditioned evidence (paper mode, M1.4)
and a longer history (download running) decide it. `docs/BUILD_PLAN.md` sections F and K give the
fastest safe path to the answer and the exact stop/scale criteria.

## Layout

```
dh/core        units, events, actions, market/settlement specs, books, Strategy contract
dh/kalshi      auth, REST, WebSocket, normalization, exact fees, market metadata
dh/feeds       Coinbase, Kraken, Bitstamp, Gemini, Crypto.com, Deribit, perps, BRTI replica
dh/store       append-only raw recorder and deterministic replay
dh/settlement  BRTI settlement-window tracker
dh/models      settlement-aware fair value, greeks, tails, vol forecaster, calibration
dh/execution   order manager, queue estimator, Kalshi exchange simulator (fill policies A/B/C), hedge simulator
dh/strategy    MarketMaker: quote EV, scenario-grid risk, hedge band, kill switches
dh/backtest    replay runner, ledger/attribution, known-answer harness
dh/live        live / paper runner, watchdog
dh/research    experiments E0-E10, fair-value study, hedge study
docs/          plan, models, architecture, lifecycle, test matrix, runbooks, results
```

## Quick start

```sh
uv venv .venv && . .venv/bin/activate && uv pip install -e '.[dev]'
pytest -q                       # offline test suite
python -m dh.research.hedge_study --csv <bitstamp_1m.csv>   # reproduce Experiment 5
python -m dh.research.fv_study.run                           # reproduce the fair-value study
```

Live operation: `docs/RUNBOOK.md` (smoke tests -> recorder -> paper mode -> tiny live).

September 27 review fixes, lossless log compression, rebuilt paper accounting, and remaining
research requirements: [implementation review](docs/research/IMPLEMENTATION_REVIEW_2026_09_27.md).
Paper restarts now represent independent portfolios; use the lifetime audit to join their
fills to later outcomes. These fixes do not establish positive edge or change the no-live
decision. See also [research integrity](docs/research/RESEARCH_INTEGRITY_2026_09_27.md) for
flow-feature compatibility, recording coverage, historical fees, and evidence gates.
