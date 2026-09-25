# Decision models: quote value, inventory risk, hedging, limits

Companion to `docs/MODELS_fairvalue.md` (settlement-window fair value, greeks, tail models),
which is written by the models workstream. Notation: YES price p in dollars; fair value
F = P(YES); A = settlement average; S = current benchmark nowcast; q_i = signed YES position in
market i (contracts); H = perp position (BTC).

## 1. Fair value used for trading: a band, not a point

The quoting engine never trades against a point estimate. For every market it computes

    F_lo <= F <= F_hi

where F uses the production configuration from the calibration study, and the band spans
(i) volatility uncertainty (sigma at the 20th and 80th percentiles of its forecast-error
distribution), (ii) tail-model uncertainty (Gaussian vs fat-tailed alternative), and
(iii) nowcast uncertainty in S (benchmark staleness plus cross-venue dispersion). Bids are
evaluated against F_lo and asks against F_hi. This makes the engine ambiguity-averse exactly
where models disagree most: far tails (longshots) and the final minute. It is also the
principled tail-risk penalty.

## 2. Scenario-grid risk engine (handles every strike type and tail model)

For each event (one expiration T) discretize A on a grid of N points (default 801) spanning
quantiles 1e-6 to 1-1e-6 of the model distribution of A. That distribution is the known fixed
sum plus m times the remaining-average distribution, with the configured tail model. The
grid carries weights w_n. Then for the event's book:

    PnL(A_n) = sum_i q_i (payoff_i(A_n) - c_i) + H_event (A_n - S)     c_i = cost basis
    E, Var, CVaR_95, WorstCase = moments / tail statistics over (PnL(A_n), w_n)

The marginal risk charge of adding delta contracts in market k is computed exactly by
re-evaluating the objective with q_k + delta. All operations are numpy vector ops on N points,
microseconds per candidate. This captures cross-strike netting automatically: a long YES at K1
and a long NO at K2 > K1 in the same event is partly offsetting.

Risk objective per event (mean-variance plus CVaR):

    R = (lambda / 2) Var[PnL] + lambda_tail * max(0, CVaR_95_loss - L_tail)

with lambda = 1 / (f_kelly * W_risk): W_risk = risk capital, f_kelly = Kelly fraction
(default 0.25). The second term only binds when an event's tail loss exceeds its budget.

## 3. Quote value

For market k and side s in {bid, ask}, candidate price p on the tick grid (up to W ticks from
the touch, never crossing), and size z:

    edge(p)    = F_lo - p                          (bid)      p - F_hi   (ask)
    fee(p)     = expected maker fee per contract (dh.kalshi.fees, exact schedule incl. type)
    AS(p, x)   = E[F_{t+h} - F_t | our fill, features x]  signed against us (adverse selection)
    hedge(p)   = c_h * S * rho * (|D + delta_k| - |D|)     marginal expected hedge cost
    inv(p, z)  = [R(q_k + z*sgn) - R(q_k)] / z             marginal risk charge per contract
    v(p)       = edge - AS - fee - hedge - inv             net value per filled contract
    h(p, Q)    = fill intensity (contracts/s) given queue ahead Q at p and flow state
    EVrate(p)  = h(p, Q) * v(p)                            dollars per second of quoting

Decision rule per market-side: choose argmax_p EVrate(p) subject to v(p) > v_min (default
0.1c); if none qualifies, **do not quote**. Queue priority is valued through h: a resting order
keeps its actual (smaller) queue-ahead Q_own, so it is replaced only if

    EVrate(new) > EVrate(existing, Q_own) + kappa_replace     or   v(existing) < 0

The same rule covers the explicit decision between joining the best price (long queue, full
edge), improving by one tick (first in queue, one tick less edge) and stepping behind the best
(fills only on sweeps).

Across markets, candidates are ranked by EVrate per unit of capital:

    score = EVrate / (collateral_per_contract * z)
    collateral: p for a YES bid, 1 - p for a YES ask (buying NO)

The best-scoring candidates are admitted greedily while every hard limit (section 6) and the
write rate-limit budget hold.

### 3.1 M1 fill-intensity model (replaced by the fitted model once data exists)

Taker volume arriving at the touch on side s follows a compound Poisson process with segment
rate Lambda_s (contracts/s by time-to-expiry bucket, distance bucket and hour of day), measured
from recorded public trades. A resting order with queue-ahead Q and size z gets filled volume

    h(p, Q) ~= Lambda_s(p) * P(taker size > Q) * E[min(size - Q, z) | size > Q] / E[size]

for p at the touch; for p one tick better than the touch, Q = 0 but Lambda is that of the
improved level (initially the touch rate). For p behind the touch, only sweeps fill:
Lambda_sweep(p). The size distribution is empirical per segment.

### 3.2 M1 adverse-selection model

AS for a maker fill in segment g at horizon h (default 60 s, also to settlement):

    AS_g(x) = a_g + b_g * max(0, r_ext * sgn_s) / sigma_1s + c_g * 1{tau < 120 s}

where r_ext is the external composite return over the last 2 s signed toward our side and
sigma_1s is 1-second vol. Coefficients start from maker markouts computed on *all* public
trades (Experiment 0/3). They are later refit on our own fills with a gradient-boosted model
only if that improves out-of-sample realized net P&L (not only markout R^2).

## 4. Hedge engine: mean-variance band, trade to the boundary

Portfolio delta D = sum_i q_i Delta_i + H. Holding D for an effective horizon h_eff costs
(lambda/2) D^2 sigma_S^2 h_eff (sigma_S in $ per sqrt(s)). Hedging costs c_h |D| S, with
c_h = fee + half-spread + expected slippage as a fraction. The no-trade band is

    B = max(B_min, 2 c_h S / (lambda sigma_S^2 h_eff))

and when |D| > B the engine trades D back to +/- B (not to zero). h_eff = min(time to the
exposure-weighted settlement, inventory half-life). Hedges are post-only limit orders at the
hedge venue's touch when time permits (|D| < B_urgent), and IOC orders otherwise.

Real-data arithmetic (BTC $84.5k, vol 35%): holding a static delta hedge to settlement cuts an
ATM contract's P&L s.d. from 50c to 30c (-64% variance), a 1-sd strike from 36.5c to 27.4c
(-44%) and a 2-sd strike from 14.9c to 13.9c (-13%). Round-trip hedge cost per ATM contract at
60 minutes is 1.3c at 0.6 bp and 10.7c at 5 bp. With lambda = 1e-4/$ (full Kelly on $10k), the
hedge pays only beyond about 800 (0.6 bp) to 6,700 (5 bp) net same-direction ATM contracts in
one event. **At M1 sizes the optimal hedge is none.** The hedge engine is live-tested at tiny
size for plumbing, and becomes economically active only as size grows (Experiment 5 finds the
empirical band scale).

## 5. Final-minute regime

Inside (T-60 s, T] the fair value uses the fixed prints (`SettlementTracker`), so
R* = (K n - sum_fixed) / m and sd_R = sigma_S sqrt(tau_first + step((m+1)(2m+1)/(6m) - 1)).
Default M1 policy: no new quotes on markets with |z| < z_min_final (default 2.5) after
T - 90 s; existing orders on those markets are canceled at T - 90 s. Far-from-strike markets
may be quoted, since their fair value is pinned near 0 or 1. Experiment 6 revises this.

## 6. Hard limits and kill switches (deterministic; config/risk.yaml)

| Limit | M1 default | Action on breach |
|---|---|---|
| position per market | 25 contracts | block orders that increase it |
| worst-case event loss (grid) | $50 | block increasing orders |
| aggregate worst-case loss, all open events | $150 | block increasing orders |
| portfolio \|D\| unhedged | 0.05 BTC | skew hard; hedge if the venue is enabled |
| perp hedge notional | $10,000 | block hedge increases |
| daily realized+marked P&L | -$75 | Halt(all) until manual reset |
| loss on any single settlement | -$40 | Halt(quoting) for 1 hour |
| BRTI tick age | > 3 s: cancel quotes with tau < 10 min; > 10 s: cancel all | resume after 30 s of fresh ticks |
| external composite age | > 2 s (any 2 of 3 major venues) | widen to model-only mode or cancel |
| Kalshi WS disconnect / seq gap | immediate | cancel all via REST; resnapshot; resume after books valid 5 s |
| hedge venue disconnect | immediate | stop quoting sides that increase \|D\| |
| abnormal volatility | 1-min \|return\| > 6 sigma or realized 5-min vol > 3x 1-day | cancel all, pause 120 s |
| fee reconciliation mismatch | any fill off by > rounding tolerance | Halt(quoting) |
| position reconciliation mismatch | fills vs `GET /portfolio/positions` | Halt(all) |
| order-group limit (exchange-side) | 50 contracts per 15 s | exchange auto-cancels the group |
| near-settlement tightening | tau < 5 min: position limits x0.5; tau < 90 s: near-strike quoting off | automatic |

"Delta-hedged" never means riskless. Residual risks tracked in the risk report: gap/jump risk
(PnL(A) steps), basis (hedge venue vs BRTI), settlement-index risk (BRTI outages, methodology
changes, `expiration_value` convention), liquidity (unable to exit or hedge), model risk
(F band width), operational risk (disconnects, stuck orders, rejected cancels).
