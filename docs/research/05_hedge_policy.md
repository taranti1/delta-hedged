# Experiment 5 (real BTC paths): hedge frequency and policy

**Evidence type:** real BTC price paths (Bitstamp 1-minute, 2025-01-08 to 2026-09-24,
14,967 clean hours); **synthetic** Kalshi fill flow (labeled below). Reproduce with
`python -m dh.research.hedge_study --csv <bitstamp_1m.csv> --out docs/research`
(full table: `docs/research/tables/hedge_policy_summary.csv`).

## Setup

* One KXBTCD-like event per hour, strikes every $250. Fills are Poisson (20 per hour) in minutes
  0-57, at strikes about 1.5 steps s.d. around the money, random side. Clip 10 contracts
  (200/hour), with scale-ups x5 and x25.
* Fills are priced at model fair value, so the Kalshi leg has zero expected P&L and policy
  differences isolate hedge cost against risk reduction. "Informed" variant: 30% of fills are
  on the side adverse to the next ~5-minute BTC move (a crude toxic-flow model).
* Delta uses the Gaussian digital with the 60-print averaging window and trailing EWMA vol
  (no look-ahead). The hedge trades at minute closes, costing fee + 0.5 bp half-spread (no
  market impact modeled). **Every policy unwinds its hedge when the event settles.** An earlier
  version that carried residual hedges past settlement left naked BTC exposure and *raised*
  variance: binary deltas vanish at settlement, so the hedge must too.
* Utility per hour = mean - (lambda/2) var, for lambda in {1e-4, 1e-3, 1e-2} per $.

## Results (200 contracts/hour unless stated; SE of means <= 0.14c/contract)

Net P&L in cents per Kalshi contract (hedge cost plus hedge P&L; Kalshi leg at fair value):

| policy | uninformed 0.6bp | 5bp | 12bp | informed 0.6bp | 5bp | 12bp |
|---|---|---|---|---|---|---|
| none | -0.09 | -0.09 | -0.09 | **-1.90** | -1.90 | -1.90 |
| mean-variance band (lambda 1e-4 .. 1e-3) | -0.09 | -0.09 | -0.09 | -1.90 | -1.90 | -1.90 |
| mean-variance band (lambda 1e-2) | -0.21 | -0.10 | -0.10 | -1.66 | -1.90 | -1.90 |
| hedge each fill once, unwind at settlement | -1.06 | -5.07 | -11.45 | **-1.09** | -5.15 | -11.60 |
| rebalance fully every 15 min | -0.61 | -2.83 | -6.35 | -2.13 | -4.37 | -7.93 |
| rebalance fully every 5 min | -1.12 | -5.57 | -12.66 | -1.88 | -6.39 | -13.55 |
| continuous (every minute) | -2.34 | -11.75 | -26.74 | -2.39 | -11.88 | -26.99 |

Hourly P&L s.d. versus no hedge ($14.4/h at 200 contracts/h): continuous -49% at 0.6 bp but
**+11% at 5 bp and +135% at 12 bp**, where gamma-driven churn near expiry makes costs large and
random. Each-fill hedging: -24% / -18% / +2%.

Scale effects (best policy by utility):
* 200 contracts/h: no hedge is optimal at every fee tier for lambda <= 1e-3. Only lambda 1e-2 at
  0.6 bp justifies a very wide band (0.14c/contract, s.d. -19%).
* 1,000 contracts/h: at 0.6 bp and lambda 1e-2, the band costs 0.84c/contract and cuts s.d. 44%.
  At 5 bp only lambda 1e-2 hedges (0.72c/contract, -18%).
* 5,000 contracts/h: at 0.6 bp and lambda 1e-3, the band costs 0.46c/contract and cuts s.d. 35%.
  At 12 bp essentially never hedge.

## Answers

1. **How often should the hedge be adjusted?** Only when the portfolio delta leaves the
   mean-variance band `B = 2 c S / (lambda sigma^2 h_eff)`, and then only back to the band edge.
   Time-based and continuous rebalancing are dominated at every fee tier. Hedges are unwound when
   their event settles.
2. **When is not hedging superior?** At M1 scale always. At any scale when the hedge venue costs
   5 bp or more, unless risk aversion is extreme. The value of hedging grows linearly with size
   (variance ~ q^2, cost ~ q), so hedging is a *scaling* tool, not an edge source.
3. **Does immediate hedging recoup adverse selection?** Partly, and only when execution is nearly
   free. With 30% informed fills, hedging each fill at 0.6 bp recovered 0.81c of 1.90c of adverse
   selection. At 5 bp it lost 3.25c more than doing nothing. The better lever is **not getting
   the toxic fill** (toxicity-aware cancel/skew), which is where research effort should go
   (Experiments 1, 3, 8).
4. **Implication for fee tier:** the hedge becomes economically relevant only at the Kalshi perp's
   top maker tiers (about 1 bp or less). Entering at 5/12 bp means the hedge engine stays off,
   and risk is controlled by limits, skew and cross-strike netting.

## Limitations

1-minute resolution understates within-minute gamma near expiry. No market impact at large
hedge sizes. The synthetic flow's strike and side distribution and its informed fraction are
assumptions: rerun this study on recorded Kalshi fills (same code path, real fills) once
available. Bitstamp prices stand in for both the BRTI and the perp (basis ignored).
