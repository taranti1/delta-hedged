# The 12 core questions: current answers

Each answer says what is known now and how sure we are, then which test settles it. Tags:

- **[VERIFIED]**: measured on real data in this repo, reproducible.
- **[BUILT]**: implemented and tested offline (unit tests plus known-answer tests on a synthetic
  market), with parameters not yet fitted to Kalshi data.
- **[OPEN]**: needs Kalshi data that could not be collected from the build environment
  (`docs/ENVIRONMENT.md`).

Experiment ids refer to `docs/TEST_MATRIX.md`. The order of work is `docs/BUILD_PLAN.md`
section F.

---------------------------------------------------------------------------------------------
### 1. What is the best deterministic estimate of the fair probability that each threshold resolves YES?

**Answer.** The probability that the average of the *remaining* settlement prints clears the
required level.

- **The settlement rule is modelled exactly.** Kalshi settles on the average of the 60
  once-per-second BRTI prints in (T-60 s, T], so the model tracks:
  - the prints already fixed;
  - the required remaining average `R* = (K n - sum_fixed) / m`;
  - the exact variance time of the remaining average,
    `tau_first + step((m+1)(2m+1)/(6m) - 1)`, which is about `tau_first + 19.5 s` before the
    window opens.
- **Volatility** is a seasonal (hour-of-week), horizon-weighted blend of EWMAs.
- **Tails** are Student-t, with degrees of freedom that depend on the horizon.
- **Quotes use a band, not a point.** The band `[F_lo, F_hi]` spans vol x0.85/x1.2, Gaussian
  vs Student-t tails, and 1x/2x nowcast error.

**Status.**
- [VERIFIED] on 15,005 out-of-sample hours of real BTC (Bitstamp 1 min as a stand-in for
  BRTI). The model beats a Gaussian digital with 2 h EWMA vol by 12.1 mn log loss
  [-12.8, -11.4], with calibration error of 0.2-0.5c (`docs/research/01_fair_value_calibration.md`).
- Ignoring the averaging window misprices a 1-sd strike by 4.8c at 2 min and by 12c at
  window open.
- [OPEN] Re-verify on BRTI itself and against Kalshi prices.

**Settled by.**
- FV (walk-forward calibration by tau and |z| on BRTI 1 Hz/5 Hz).
- ST (the settlement convention reproduces every published `expiration_value` to $0.01).

### 2. How much incremental forecasting value comes from each data source?

**Answer: unknown until measured, and measured the same way for every source.** Each feature
family is added alone to the analytical baseline and kept only if it improves both:
- the out-of-sample forecast;
- the replayed net cents per contract, under fill policies B and C.

This is the ablation protocol in `docs/TEST_MATRIX.md`. The candidate families are:
- constituent venues' books (E2);
- perp basis and order-flow imbalance;
- Deribit implied vol (FV);
- Kalshi's own book and flow (E3).

Kalshi's own `cfbenchmarks_value_5hz` stream carries the settlement index itself, so the
baseline already starts from the right source.

**Status.** [BUILT] Adapters for Coinbase, Kraken, Bitstamp, Gemini, Crypto.com, Deribit and
the perps; a BRTI replica; and the replay harness for E2 (`dh/research/exp2_nowcast.py`).
[OPEN] The measurements themselves.

**Settled by.** E2: adopt a composite nowcast only if RMSE improves by at least 10% at
0.2-1 s *and* replayed P&L improves. Then FV and E3 ablations.

### 3. Can we predict when a Kalshi quote is stale relative to the BTC market?

**Answer.** Measure it as a lead-lag between model fair value and Kalshi mid/microprice at
horizons of 0.1-5 s.

**Status.** [BUILT] `dh/research/exp1_staleness.py` uses the production pricer (vectorized),
full-grid pairing and block bootstrap.
- The known-answer test recovers injected maker lags. Injected lags of 0.3 s, 1.5 s and 4 s
  measure as half-lives of 0.21 s, 0.75 s and 1.98 s.
- After 2-sd external moves, the injected staleness shows up as 1.4-3.7 ticks.

[OPEN] Real Kalshi data.

**Settled by.** E1: accept if the lag is at least 0.5 tick within 1 s, with a CI, and stable
across months. E8 then decides whether *taking* stale quotes pays: at least 0.5c net after
fees, with at least 20 opportunities a day.

### 4. Can we predict whether an incoming fill is likely to be toxic?

**Answer.** M1 uses an interpretable parametric model of the expected adverse move given a
fill: a base cost, a momentum term against us, a final-minute term, and a multiplier for
quotes that sit behind the touch (`docs/MODELS.md` s.3.2).

**Status.** [BUILT], with [ESTIMATE] parameters. E3 fits it on real fills:
- M1 live fills;
- shadow fills of hypothetical quotes in replay (`dh/research/exp3_toxicity.py`).

The time split is guarded against leakage (`tests/research/test_leakage_guards.py`).

**Settled by.** E3: adopt a toxicity-aware cancel/skew only if it raises net c/contract by
at least 0.1c (CI > 0) at no more than 20% fill loss, *under fill policy C*.
Experiment 5 found that avoiding the toxic fill beats hedging it: hedging recovered at most
0.81c of 1.90c adverse selection, and only at 0.6 bp.

### 5. How should quote price, size and willingness to trade depend on the state?

**Answer.** Through one objective per candidate order. For each price p and size z:
- `v = edge vs band - E[adverse selection | fill] - exact fee(p, z) - hedge cost - marginal
  inventory risk` per filled contract;
- `EVrate = fill intensity(queue ahead) x v`.

Quote the argmax only if v > v_min; otherwise do not quote. Across markets, rank by EV rate
per $ of collateral under hard limits (`docs/MODELS.md` s.3). Each factor in the question
enters as follows:

| factor | enters through |
|---|---|
| fair value, volatility | edge vs band (the band widens with vol and model uncertainty) |
| time to settlement | variance time; adverse-selection final-minute term; near-expiry limits; final-window near-strike rule |
| distance to strike | segment (tau, \|z\|) of the flow and adverse-selection models; `z_near` guard in the final window |
| queue position | fill intensity h(Q) (compound-Poisson taker flow vs queue ahead); keep-vs-replace hysteresis |
| external BTC movement | nowcast; adverse-selection momentum term; requote on a 2-sd move; abnormal-move pause |
| Kalshi order flow | segment taker rates and sizes (fitted with shrinkage) |
| current inventory | marginal risk charge from the exact scenario grid of the settlement average |
| expected hedge cost | hedge term (zero in M1: hedge off) |
| current gamma | the scenario grid prices the full step payoff, so gamma is inside the inventory term |
| size | clip, capacity under limits, and the fee-efficient size (Kalshi's per-order fee rounding) |

**Status.** [BUILT], tested with known-answer tests. Parameters stay [ESTIMATE] until E3/E4
and the flow calibration run on real data.

### 6. What is the correct delta of each Kalshi binary position?

**Answer.** `dP/dS` of the settlement-window digital, which differs from the point-in-time
digital:
- inside the window only the remaining prints depend on future spot;
- the hedge leg maps to the remaining average R.

At portfolio level, `dollar_delta = Cov(PnL, R) / Var(R)` on the exact scenario grid across
all strikes and strike types.

**Status.** [VERIFIED] analytic delta and gamma against finite differences and Monte Carlo
(`dh/models/fairvalue.py`, `dh/strategy/scenario.py`).

### 7. How often should the hedge actually be adjusted?

**Answer.** Only when portfolio delta leaves the mean-variance band
`B = 2 c S / (lambda sigma^2 h)`, and then only back to the band edge. Hedges are unwound when
their event settles.

**Status.** [VERIFIED] on 14,967 real BTC hours with synthetic fill flow
(`docs/research/05_hedge_policy.md`):
- Time-based and continuous rebalancing are dominated at every fee tier.
- Continuous hedging *raises* P&L s.d. by 11% at 5 bp and by 135% at 12 bp.

**Settled by.** E5 re-run on recorded Kalshi fills.

### 8. When is NOT hedging immediately superior to immediate hedging?

**Answer: at M1 scale, always; at any scale when the hedge costs 5 bp or more.**
- A one-way at-the-money hedge costs 0.64-12.9c per contract 60 minutes out, and more near
  expiry, against a 0.15-0.5c edge target (`docs/00_PREMISE_CHALLENGE.md` s.2.1).
- Per-fill hedging loses 1.06-11.45c per contract.
- Hedging is a *scaling* tool, since variance grows as q^2 and cost as q. It starts to pay at
  about 1,000+ contracts/hour and hedge costs of about 1 bp or less.

**Status.** [VERIFIED] (Experiment 5). The M1 config has the hedge disabled.

### 9. When should the system stop quoting entirely?

**Answer.** Deterministic rules, each tested (`dh/strategy/risk.py`, `docs/MODELS.md` s.6):

| stop | condition |
|---|---|
| **Feeds** | Kalshi WebSocket disconnected or any sequence gap: cancel all, resnapshot, 5 s settle |
| | Book gap in one market: cancel that market |
| | BRTI older than 3 s (by receive *and* source time) near expiry: stop near-expiry quoting |
| | BRTI older than 10 s: cancel all; 30 s of fresh ticks needed to resume |
| | Fewer than 2 fresh external venues (when configured) |
| **Market state** | Abnormal move (6 sigma over 60 s): pause 120 s |
| | Final window, near strike: no quotes |
| | Paused or deactivated markets |
| | Unresolved or unsupported fee type |
| **Account integrity** | Fee mismatch on any fill: halt quoting |
| | Position mismatch: halt everything |
| **Loss limits** | Daily loss of $25: halt, manual reset |
| | Settlement loss of $15: pause |
| | Order-group trigger: cooldown |
| **Economics** | No candidate with v > v_min |
| **Manual** | Kill file; watchdog (cancel all when the heartbeat stops) |

The final-window near-strike rule is `|z| < 2.5` of the nearest strike inside 90 s.

**Status.** [BUILT], with regression tests for every rule.

### 10. Which strike / time-to-expiry combinations contain the most extractable edge?

**Answer.** A ranked hypothesis, to be overturned by data (`docs/00_PREMISE_CHALLENGE.md` s.5):
1. Mid and far strikes (YES 3-25c / 75-97c), 10-60 minutes out.
2. Near the money only when the spread exceeds one tick.
3. Far-from-strike quoting in the final minute.

The real data already rules out a naive longshot premium: Gaussian tails understate 3-sd
moves about 4-5x.

**Status.** [OPEN].

**Settled by.**
- E0 on historical trades: keep only segments whose net CI lower bound exceeds 0.15c.
- E6/E7 on replay and live fills, by tau x |z|.

### 11. How much daily volume can realistically be captured before marginal fills destroy the edge?

**Answer.** Unknown. The placeholders in `docs/BUILD_PLAN.md` section J are 2,000 / 45,000 /
300,000 contracts a day (bear/base/bull).

**Status.** [BUILT] E10 capacity replay (`dh/research/exp10_capacity.py`): size scaling x1-x50
with flow-share caps and queue dilution, under policies B and C. [OPEN] Real data.

**Settled by.** E10 reports capacity at > 1.0c, 0.75c, 0.5c and 0.05c per contract and at
breakeven. The "does not scale" criteria are in section K.

### 12. Does the strategy remain profitable after all costs and tail events?

**Answer. Not yet known; no claim of edge is made.** The cost terms are handled as follows:

| cost | handling |
|---|---|
| Kalshi fees | priced exactly per order, including Kalshi's rounding of each order's fee up to the cent (up to 1c per order) |
| Hedge fees, perp spread, funding, failed hedge execution | zero by design in M1 (hedge off; E5) |
| Slippage | maker-only, post-only quoting |
| Adverse selection and stale-quote losses | measured as fill markouts (policies B/C) |
| Tail events | bounded by exact worst-case loss limits per event and in total |

Economics are in `docs/BUILD_PLAN.md` section J [ESTIMATE]: net per contract of -0.7c bear,
0.3c base, 1.45c bull.

**Settled by.** In order:
1. E0.
2. Paper mode for 7 days (policy C).
3. Live M1 for at least 4 weeks and at least 5,000 fills.

Decided by the falsification criteria in `docs/BUILD_PLAN.md` section K. Any result above
0.75c per contract triggers an audit of fills and attribution before it is believed.
