# delta-hedged

A deterministic research and trading system for **passive market making on Kalshi BTC threshold
markets** (`KXBTCD`, with `KXBTC` / `KXBTC15M` support). It adds portfolio-level BTC hedging
only where it pays. The same Strategy object runs live, in paper (shadow) mode, and in replay
of recorded data.

**Start with [`docs/BUILD_PLAN.md`](docs/BUILD_PLAN.md)** (deliverables A-K). Every claim there
is tagged [VERIFIED] (real data, reproducible), [BUILT] (implemented and tested offline, live
verification pending) or [ESTIMATE].

## What is established so far

| Finding | Evidence |
|---|---|
| Per-fill delta hedging of Kalshi binaries is uneconomic: a one-way at-the-money hedge costs 0.6-13c vs a 0.15-0.5c edge target | `docs/00_PREMISE_CHALLENGE.md` |
| Continuous delta hedging raises P&L variance at 5-12 bp perp fees. At M1 size the optimal hedge is none; a mean-variance band pays only at scale and <= ~1 bp costs | Experiment 5 on 14,967 real BTC hours: `docs/research/05_hedge_policy.md` |
| Best fair value: seasonal horizon-weighted EWMA vol + Student-t tails, -12.1 mn log loss [-12.8, -11.4] vs a Gaussian digital with 2h EWMA vol; calibration error 0.2-0.5c | 15,005 out-of-sample hours: `docs/research/01_fair_value_calibration.md` |
| Gaussian tails understate 3-sd outcomes ~4-5x, so the naive "sell longshots" edge largely disappears | same |
| Ignoring the 60-print settlement average misprices a 1-sd strike by 4.8c at 2 min and 12c at window open | same |
| Kalshi streams the settlement benchmark (BRTI, 1 Hz and 5 Hz) on its own WebSocket, with REST history | `docs/kalshi_specs/asyncapi.yaml`, `docs/DATA_SOURCES.md` |
| With maker fees, each order pays its exact fee rounded up to the cent, so the cost per contract depends on order size: 0.50-0.60c near 50c, and at least 0.50c for any 2-lot. The quoter prices the exact per-order fee and picks fee-efficient sizes (formula from the fee rules; to be confirmed on live fills in M1.3) | `docs/MODELS.md` s.3, `dh/kalshi/fees.py` |

**Not yet established: whether net edge exists after fills.** That requires Kalshi data, which
this build environment could not reach. `docs/BUILD_PLAN.md` sections F and K give the
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
