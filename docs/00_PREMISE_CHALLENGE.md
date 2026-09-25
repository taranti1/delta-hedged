# 00 — Premise challenge (read this before anything else)

Status: **analysis + real-data numbers (BTC only)**. No Kalshi order-book or trade data could be
collected from the build environment (egress policy blocks Kalshi, exchange and data-vendor hosts;
see `docs/ENVIRONMENT.md`). Every Kalshi-specific claim below is a **hypothesis to be tested**,
not a result.

## 1. What we are building (precise)

A deterministic passive market maker on Kalshi BTC threshold markets (series `KXBTCD`, hourly,
"Will the BRTI 60-second average at T be above K?"), optionally extended to `KXBTC` ranges and
`KXBTC15M`, which:

1. estimates `P(settlement average > K)` from the live settlement benchmark (BRTI, streamed by
   Kalshi's own `cfbenchmarks_value[_5hz]` WebSocket channels) and external BTC venues,
2. rests post-only YES bids and YES asks (== NO bids) where
   `P(fill) x E[net P&L | fill] > 0` after fees, adverse selection, hedge cost and risk charges,
3. manages the resulting binary inventory primarily by **quote skew and limits**, and hedges the
   *netted portfolio* BTC delta with a perp only when the risk reduction is worth more than the
   hedge's all-in cost,
4. logs every event and every considered quote so the same code replays deterministically.

Objective: maximize long-run realized net dollars per unit of scarce resource
(capital, risk budget, queue position, taker flow), subject to hard drawdown/tail limits.
Primary unit metrics: **realized net cents per filled contract** and **profitable contracts
filled per day** — both must be attractive at the same time.

## 2. The premise, attacked

### 2.1 "Hedge the delta of each Kalshi fill" is uneconomic by one to two orders of magnitude

Real inputs: BTC = $84,541 (Bitstamp close 2026-09-25 01:57Z), 30-day realized vol 35%
annualized (1-minute and hourly returns agree, so microstructure noise is small at 1m).
Settlement is the average of 60 one-second BRTI prints, so the effective variance time is
`(time to first print) + 19.5 s` (derivation in `docs/MODELS.md`).

Delta of one $1 binary, the BTC notional needed to hedge it, and the **one-way** hedge cost at
Kalshi BTC perp fee tiers (maker 5.0 -> 0.6 bps, taker 12.0 -> 2.6 bps by 30-day volume,
charged on notional; spread/slippage excluded):

| time to T | strike distance | P(YES) | hedge notional | cost @0.6bp | @2.6bp | @5bp | @12bp | Kalshi maker fee* | Kalshi taker fee |
|---|---|---|---|---|---|---|---|---|---|
| 60 min | ATM | 0.50 | $107 | 0.64c | 2.79c | 5.36c | 12.9c | 0.44c | 1.75c |
| 60 min | 1.0 sd | 0.16 | $65 | 0.39c | 1.69c | 3.25c | 7.8c | 0.23c | 0.93c |
| 60 min | 2.0 sd | 0.023 | $15 | 0.09c | 0.38c | 0.73c | 1.7c | 0.04c | 0.16c |
| 10 min | ATM | 0.50 | $270 | 1.62c | 7.0c | 13.5c | 32.4c | 0.44c | 1.75c |
| 2 min | ATM | 0.50 | $713 | 4.3c | 18.6c | 35.7c | 85.6c | 0.44c | 1.75c |

\*Only if the series fee type is `quadratic_with_maker_fees`; `quadratic` charges makers nothing.
Fee type is read from the API at runtime, never assumed.

Target net edge is 0.15-0.5 c/contract. A single one-way ATM hedge costs more than that at every
fee tier; a delta-neutral book near expiry would also have to be re-hedged continuously as gamma
explodes. **Conclusion: per-fill hedging is rejected analytically.** Hedging can only pay on the
*residual* delta that survives (a) two-sided flow netting within a strike, (b) netting across
strikes of the same event, (c) quote skew that pulls inventory back toward flat. That residual
must be hedged with resting (maker) perp orders where possible, inside wide, gamma-aware bands.

### 2.2 What hedging can and cannot do for a binary book

At settlement the book pays `sum_i q_i (1{A > K_i} - p_i)`: a step function of the settlement
average A. A linear perp hedge removes the *slope* of that function around the current price,
not the steps. The pin/jump risk at strikes near A is unhedgeable with a linear instrument and is
controlled only by position limits per strike/event and by not accumulating inventory near the
money close to expiry.

Mean-variance hedge rule (used by the hedge engine; derivation in `docs/MODELS.md`): hedging an
exposure of `D` BTC over horizon `h` removes variance `D^2 sigma_S^2 h` at cost `c |D| S`, so
hedging is worth it only when

    |D| > c * S / (lambda * sigma_S^2 * h)

where `lambda` is the risk-aversion implied by our daily risk budget. Small books sit inside the
band and should **not** hedge; the band tightens as the book grows (variance ~ D^2, cost ~ |D|).
That is the quantitative answer to "when is not hedging superior?".

### 2.3 Diversification does most of the risk work

`KXBTCD` settles 24 times a day, and hourly BTC returns are nearly uncorrelated hour to hour. An
unhedged residual of N contracts near the money carries about `0.5 N` dollars of settlement
standard deviation per event. Example: edge 0.3 c on 2,000 contracts/hour ($6/h) against a
residual of 200 contracts ($100/h s.d.) gives an annualized Sharpe of about
0.06 x sqrt(24 x 365) = 5.6. Variance is not the binding problem at small scale; **adverse
selection is**. Hedging reduces variance but does nothing for adverse selection: a filled quote
that was stale is a loss whether or not it is hedged.

## 3. Candidate edges and why competition might not have removed them

Each is stated as a falsifiable hypothesis. "Our model is better" is not an accepted answer.

| # | Hypothesis | Concrete reason it could survive competition | How it dies |
|---|---|---|---|
| H1 | Makers earn spread from retail taker flow | 1c tick on a $1 contract = 1% of notional; takers also pay up to 1.75c in fees; retail flow on hourly BTC; binary settlement and capital lock-up deter generic MMs | At 1-tick spreads the rent goes to queue-priority holders; back-of-queue fills are adversely selected. Dies if fill-conditioned markouts eat the half-spread |
| H2 | Longshot premium: YES at 1-10c is overpriced | Documented favorite-longshot bias on Kalshi (makers earn, takers lose, worst at low prices); selling tails ties up ~95c of collateral per contract (capital segmentation); low delta so little hedge cost; tiny fees since p(1-p) is small | BTC hourly returns have kurtosis about 11: fat tails may *justify* the prices. Dies if tail-calibrated fair value is at or above market prices |
| H3 | Faster fair-value updates (BRTI 5Hz plus constituent venues) cut adverse selection vs slower makers | Kalshi write rate limits and REST round-trips cap everyone's update speed; many participants key off Kalshi prices or the 1Hz index | Dies if the markout of our fills within about 1s of external moves is no better than the average maker's |
| H4 | Settlement-window arithmetic in the final minute (fixed prints, required remaining average) | Naive "spot vs strike" pricing is wrong once prints are fixed; requires modeling the exact benchmark | Fast informed takers know it too; the final minute has the most toxicity and gamma. Dies on net P&L even if prediction is better |
| H5 | Liquidity-incentive rewards on BTC markets | Fixed reward pools paid for resting size near the touch | Pool shared by competitors, driving the marginal maker's profit toward zero; must be earned, not assumed |
| H6 | Cross-series consistency (KXBTCD thresholds vs KXBTC ranges vs the 15-minute series on the same timestamp) | Different participant sets per series | Fees on two legs; probably rare and small. Monitor only |

## 4. The cheapest decisive test comes first (Experiment 0)

Kalshi's public `/historical/trades` (price, size, taker side, timestamp) joined with
`/historical/markets` (result, expiration value) gives, for every trade, the **maker's gross P&L
to settlement**:

    maker_pnl = (settle - yes_px) if taker sold YES  else  (yes_px - settle)

Bucketed by price, time-to-expiry, normalized strike distance, time of day and trade size, this
measures directly where makers as a group earn and lose on `KXBTCD`, before fees and before any
queue selection effects. It needs no order-book capture and no account. If makers in aggregate
do not earn more than the maker fee in a segment, no queue-position cleverness will rescue that
segment. Experiment 0 therefore gates everything else.

Caveat: the aggregate maker average mixes front-of-queue professionals with slow back-of-queue
makers. Our fills are a *selected* subset, so Experiments 3 and 4 (fill-conditioned markouts,
queue economics) remain necessary.

## 5. Ranked starting hypothesis for where net edge lives (to be overturned by data)

1. **Mid/far strikes (YES 3-25c / 75-97c), 10-60 minutes to expiry.** Wider spreads, lower fees,
   low gamma, cheap residual hedge, longshot premium.
2. **Near-the-money quoting only when the spread exceeds 1 tick** (early in the hour, overnight or
   weekends, after vol spikes once the book re-stabilizes), with fast cancel on external moves.
3. **Final minute:** quote only far-from-strike contracts whose required remaining average is many
   standard deviations away (fair value near 0 or 1). No near-strike quoting (Experiment 6 decides).
4. ATM with a 1-tick spread and maker fees: presumed **uneconomic** for a new, back-of-queue entrant
   unless liquidity incentives pay for it.

## 6. What would stop the project early

* Experiment 0 shows maker gross P&L to settlement at or below the maker fee in every
  price/time/distance segment of `KXBTCD`, `KXBTC` and `KXBTC15M`.
* The settlement-benchmark replica (1Hz/5Hz BRTI window average) cannot reproduce published
  `expiration_value`s to within a few dollars (the settlement model would be wrong).
* M1 live fills show negative realized net cents per contract with a 95% CI entirely below zero
  after 5,000 fills, in the segments predicted to be best.

## 7. What was established in this environment vs what needs data

| Established now (real data) | Needs Kalshi data (collector provided) |
|---|---|
| Fair-value model calibration on real BTC 1-minute paths (2025-01 to 2026-09) | Experiment 0 (public trades vs settlement) |
| Tail model choice (Gaussian vs vol-mixture vs Student-t) | Experiments 1-4, 6-10 (L2 + trades + BRTI capture) |
| Vol seasonality (hour of day, weekday) | Fee type per series, incentive programs, tick structure |
| Hedge-policy comparison on real BTC paths with simulated inventory | Actual fill rates, queue dynamics, markouts |
| Exact settlement-window math, delta and gamma | Settlement-convention check vs `expiration_value` |
