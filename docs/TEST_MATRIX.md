# E. Research test matrix

Conventions for every test:
* **Time-ordered splits only.** Walk-forward: fit on data before month M, evaluate on month M,
  roll. Never shuffle. Features at time t use only events with receive time <= t.
* **Inference clusters by settlement event** (all markets of one expiration share one outcome):
  event-level block bootstrap for CIs. Report effect sizes in cents per contract, not just p-values.
* **Regime splits reported for every test:** tau bucket (>30m, 10-30m, 5-10m, 1-5m, 30-60s,
  <30s), |z| bucket, realized-vol tercile, trend/chop (|6h return|/vol), weekday/weekend,
  high/low taker flow, scheduled-event hours (CPI/FOMC/NFP) vs normal.
* **Economic acceptance beats statistical acceptance.** A feature or model is adopted only if the
  replayed strategy's realized net cents/contract improves with a 95% CI excluding zero *and*
  profitable contracts/day does not fall. Better Brier score alone is never sufficient.
* **Fill-model robustness.** Every P&L claim is reported under fill policies A (optimistic),
  B (realistic) and C (conservative). A claim that holds only under A is rejected.

| # | Hypothesis | Dataset | Dependent variable | Features | Benchmark | Method | Accept if | Reject if |
|---|---|---|---|---|---|---|---|---|
| E0 | Makers as a group earn more than the maker fee in some KXBTCD/KXBTC/KXBTC15M segments | Kalshi `/historical/trades` + settled markets (+BRTI history) | maker gross/net P&L to settlement, c/contract | price, maker side, tau, z, hour, weekday, size | 0 and the maker fee | Event-bootstrap segment means (`dh/research/exp0_maker_pnl.py`) | Some segment has net CI > 0.15c with >= 200 events | Every segment's net CI upper bound < 0.15c: stop, or pivot to incentive-driven quoting only |
| E1 | Kalshi prices are stale vs external BTC at 100ms-5s | Recorded Kalshi L2 + BRTI 5Hz + venue books | future change of Kalshi mid/microprice over h in {0.1,0.25,0.5,1,2,5}s | past change of model fair value from external composite over the same grid | Zero-lag (efficient) market | Lead-lag regression, cross-correlation, impulse response; synthetic KAT with known lag | Lag coefficient > 0 with CI, economically >= 0.5 tick within 1s, stable across months | No response beyond receive latency, or < 0.2 tick |
| E2 | Constituent books/perps predict the next BRTI print | Venue L2 + perps + BRTI 1Hz/5Hz | next BRTI print minus last print (and 60s window average) | venue mids, microprices, depth-weighted consolidated mid, BRTI replica, perp basis, OFI | last BRTI print (random walk) | OOS RMSE and MAE by horizon; walk-forward ridge/GBM vs replica | RMSE improves >= 10% at 0.2-1s *and* E1/E3 P&L improves in replay | < 5% RMSE gain or no P&L gain |
| E3 | Fill toxicity is predictable | Our fills (live M1) + shadow fills of hypothetical quotes in replay | markout at 0.1/0.5/1/5/10/30/60s and to settlement | queue ahead, quote age, ext return 0.1-5s, book imbalance (Kalshi, venues), taker intensity, tau, z, spread, vol | Unconditional markout | Walk-forward logistic/GBM on sign plus regression on size; evaluate by cancel-policy replay | Toxicity-aware cancel policy raises net c/contract by >= 0.1c (CI > 0) at <= 20% fill loss | No OOS lift, or lift vanishes under fill policy C |
| E4 | Queue priority is worth more than continuous repricing | Replay with queue estimator (calibrated to live `queue_positions`) | net P&L per quote-hour | queue position, quote age, join vs improve vs behind, cancel/re-entry | Always re-center at fair | Policy replay A/B/C, and live A/B at tiny size | Keep-priority rule beats re-centering by > 0.05c/contract and in $/day | Differences within CI |
| E5 | Banded portfolio hedging beats per-fill and none at target scale | Real BTC paths (done: `docs/research/05_hedge_policy.md`); then recorded fills | utility = mean - (lambda/2) var of hourly P&L; net c/contract | band width, fee tier, scale, informed share | No hedge | Paired replay on identical fills | Band beats no-hedge utility at the configured lambda and scale | Otherwise hedge stays disabled (current M1 conclusion) |
| E6 | Net edge differs by time to expiry | Replay + live fills | realized net c/contract, fills/day, toxicity per tau bucket | tau buckets | Pooled | Segment means with event bootstrap | Quote only buckets with net CI lower bound > 0 | Buckets with CI upper bound < 0 are disabled |
| E7 | Net edge differs by normalized strike distance | same | same, per \|z\| bucket (and YES price bucket) | \|z\|, price | Pooled | same | same | same |
| E8 | Selective taking of stale Kalshi quotes is profitable | Recorded L2 + model FV | taker P&L to 60s mark and to settlement after taker fee | FV - ask (or bid - FV), staleness age, ext move | Never take | Replay with realistic taker latency and queue-empty checks | Net > 0.5c/contract after fees with CI > 0, >= 20 opportunities/day | Otherwise disabled |
| E9 | Quoting many strikes improves netting and capital efficiency | Replay | net $/day, hedge turnover, peak collateral, fills | number of strikes quoted, netting on/off | Single best strike | Paired replays | $/day up and hedge turnover down per contract | No improvement |
| E10 | Capacity | Replay with size scaling (x1 .. x50) and flow-share caps | net c/contract, fills/day, share of taker flow, inventory s.d. | clip size, number of levels | x1 | Replay under policies B and C; queue dilution modeled | Report capacity at > 1.0, 0.75, 0.5, 0.05 c and breakeven | n/a (measurement) |
| FV | Settlement-aware analytical fair value is calibrated | Real BTC 1m (done in `docs/research/01_*`), then BRTI 1Hz/5Hz | YES outcome | spot, sigma forecast, tau, window state, tail model | Gaussian point-in-time digital | Walk-forward Brier/log loss, reliability by tau and \|z\| | Calibration within CI in every tau and \|z\| bucket | Systematic miscalibration in any bucket -> fix before trading it |
| ST | Settlement convention reproduces `expiration_value` | BRTI REST history + settled markets | published expiration value | 60 prints in (T-60, T] vs [T-60, T) vs other conventions | — | Exact comparison per market | Max abs error below $0.01 (rounding) on >= 99% of events | Any systematic mismatch: stop trading the final minute |
| INC | Liquidity incentives are earnable at our size | `/incentive_programs` + our resting-order logs | incentive $ earned per quote-hour | target size, discount factor, rank vs others | 0 | Live measurement | Adds to net P&L after meeting program rules | Unearnable or negligible |

## Feature-family ablation protocol (applies to FV, E2, E3)

1. Baseline: analytical digital with EWMA vol and Gaussian tails, benchmark = last BRTI print.
2. Add one family at a time: momentum (0.1-60s returns), cross-venue divergence, microstructure
   (microprice, imbalance, OFI, trade intensity), volatility (realized vs implied, vol-of-vol),
   Kalshi-specific (mid, spread, depth, imbalance, trade flow, FV-mid gap, quote age, queue),
   settlement-specific (fixed prints, required average).
3. For each: OOS forecast metric, *and* replayed net P&L under fill policies B and C.
4. Keep only families that pass both. Re-test the retained set jointly for redundancy.

## Leakage checklist (audited in Phase 3)

* Features use receive time, never exchange time from the future of receipt.
* Settlement outcomes, `expiration_value` and final BRTI prints never enter features before T.
* Vol, seasonality, tail parameters and fill/toxicity model fits use only past data.
* Shadow-fill markouts use fair value computed causally at each horizon (no smoothing across t).
* Replay determinism: truncating the stream at t leaves every action before t unchanged.
