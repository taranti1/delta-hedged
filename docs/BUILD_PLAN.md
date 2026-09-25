# Kalshi BTC market-making: build plan (deliverables A-K)

**Read this first.** Everything below is labeled **[VERIFIED]** (measured on real data in this
repo, reproducible with the named command), **[BUILT]** (implemented and tested offline, but
not yet run against live endpoints) or **[ESTIMATE]** (an assumption to be replaced by
measurement). No Kalshi order-book, trade or benchmark data could be collected from the build
environment (egress blocked; see `docs/ENVIRONMENT.md`). **No claim of edge is made yet.** This
plan is a system that can find out, safely and fast, whether edge exists and how much scales.

---------------------------------------------------------------------------------------------
## A. Strategy specification

**What it trades.** Kalshi `KXBTCD` hourly BTC threshold contracts: "the simple average of the
60 once-per-second CF Benchmarks BRTI prints in (T-60 s, T] is above K". `KXBTC` ranges and
`KXBTC15M` are supported by the same code but are enabled only if Experiment 0 shows edge there.

**What it does.** A single-threaded, deterministic Strategy (`dh/strategy/mm.py`) repeats the
following every 200 ms of event time, and immediately after a benchmark move larger than
2 one-second standard deviations:

1. Validate feeds. Kalshi WS connected with valid books; BRTI tick age <= 3 s near expiry and
   <= 10 s otherwise, with 30 s of fresh ticks required after an outage; at least 2 fresh
   external venues. Otherwise cancel everything.
2. Nowcast the benchmark. S = latest BRTI tick (Kalshi `cfbenchmarks_value_5hz`). The nowcast
   error sd grows with tick age and enters the price in quadrature.
3. Settlement state per expiration. Prints already fixed in the window, the required average of
   the remaining prints for each strike, and the exact variance time of the remaining average.
4. Fair value **band** [F_lo, F, F_hi] per market [VERIFIED model choice]:
   - volatility: a seasonal, horizon-weighted blend of EWMAs;
   - tails: horizon-dependent Student-t;
   - the band spans vol x0.85/x1.2, Gaussian vs Student-t tails, and 1x/2x nowcast error.
   Delta and gamma are analytic.
5. For every tick price near the touch and near fair value, compute the net value per filled
   contract: `v = edge vs band - E[adverse selection | fill] - exact maker fee - hedge cost -
   marginal inventory risk`. The inventory term comes from a scenario grid of the settlement
   average, exact across strikes and strike types. Then compute `EVrate = fill intensity(queue
   ahead) x v`.
6. Keep, replace (with hysteresis), place or pull each market-side quote. Rank all proposed new
   quotes across strikes by EVrate per $ of collateral. Admit them greedily under hard limits:
   position per market, exact worst-case loss per event and in total (adversarial fills), and
   portfolio delta.
7. Only post-only maker orders, carrying `cancel_order_on_pause` and belonging to an exchange
   order group (a rolling-15-second fill cap acts as an exchange-side burst breaker).
8. **No BTC hedge at M1 size** [VERIFIED, Experiment 5 on real paths]. The mean-variance
   hedge band activates only at scale and with hedge costs of about 1 bp or less; hedges are
   unwound as the event settles.
9. Settle, reconcile fees (every fill) and positions (every 30 s). Kill switches per section I.

**Objective.** Maximize long-run realized net dollars per unit of scarce resource (collateral,
risk budget, queue position, taker flow). Primary metrics: realized net cents per filled
contract (event-clustered CI) and profitable contracts per day, both reported under fill
policies B (realistic) and C (conservative).

**Where edge is hypothesized (ranked, to be falsified; `docs/00_PREMISE_CHALLENGE.md` s.3).**
1. Mid and far strikes (YES 3-25c / 75-97c), 10-60 minutes out: wider spreads, lower fees, low
   gamma.
2. Near-the-money quoting only when the spread is wider than one tick.
3. Final-minute quoting of far-from-strike markets, where exact settlement arithmetic matters
   [VERIFIED: ignoring the 60-print average misprices a 1-sd strike by 4.8c at 2 min and 12c at
   window open].
4. At-the-money with a one-tick spread and maker fees: presumed uneconomic for a new entrant.

What the real data already rules out:
- A naive **longshot premium**. Gaussian tails understate 3-sd outcomes about 4-5x, so selling
  1c YES at 3 sd is worth only 0.3-0.5c before fees, and there is no edge at 2.5 sd.
- **Per-fill delta hedging**, which costs 1-27c per contract [VERIFIED].

---------------------------------------------------------------------------------------------
## B. Data-source map

`docs/DATA_SOURCES.md`. All free sources:
- **Kalshi WS:** book deltas, trades, lifecycle, fills and orders, plus **BRTI at 1 Hz and 5 Hz
  via `cfbenchmarks_value[_5hz]`**, i.e. the settlement benchmark itself.
- **Kalshi REST:** markets, series fees, fee changes, historical trades and markets, **queue
  positions**, incentives, and the CF history passthrough.
- **BRTI constituent venues:** Coinbase, Kraken, Bitstamp, Gemini, Crypto.com.
- **Perps and options:** Deribit, Binance futures (data only), Bybit, OKX, Hyperliquid.
- Every adapter is **[BUILT]**; run `scripts/smoke_kalshi.py` and `scripts/smoke_feeds.py`
  before trusting data.

## C. System architecture

`docs/ARCHITECTURE.md`. The live and replay paths share one Strategy object:

    Kalshi WS + venue WSs + REST snapshots -> raw append-only recorder (zstd, receive ns)
      -> pure normalizers -> single ordered event queue -> Strategy.on_event (deterministic)
      -> actions -> async venue adapters (Kalshi V2 REST / simulator) -> acks and fills as events

Components by package:
- `dh.kalshi` / `dh.feeds`: ingestion and normalization.
- `dh.store`: raw capture and deterministic replay.
- `dh.settlement` + `dh.models`: benchmark state and fair value.
- `dh.execution`: order manager, queue estimator, exchange and hedge simulators.
- `dh.strategy`: quote EV, scenario risk, hedge, kill switches, the MarketMaker.
- `dh.backtest`: runner, ledger/attribution, known-answer harness.
- `dh.live`: live/paper runner and watchdog.
- `dh.research`: experiments.

Python is sufficient until Experiments 1/3 show that more than 0.1c/contract of adverse
selection comes from fills landing within 10 ms of our cancel (the migration rule in
`docs/ARCHITECTURE.md`).

## D. Mathematical models

| Model | Where | Status |
|---|---|---|
| Settlement-window fair value: `P(avg of remaining m prints > R*)`, `R* = (Kn - sum_fixed)/m`, variance time `tau_first + step((m+1)(2m+1)/(6m) - 1)` | `docs/MODELS_fairvalue.md`, `dh/models/fairvalue.py` | [VERIFIED] vs brute force and Monte Carlo |
| Vol: seasonal (hour-of-week, NY time) horizon-weighted EWMA blend; tails: Student-t by horizon | `docs/research/01_fair_value_calibration.md` | [VERIFIED] -12.1 mn log loss [-12.8, -11.4] vs Gaussian+2h EWMA on 15,005 out-of-sample hours; calibration error 0.2-0.5c |
| Delta = dP/dS (BTC per YES contract); gamma analytic; portfolio delta sum q_i Delta_i + H | `dh/models/fairvalue.py`, `dh/strategy/scenario.py` | [VERIFIED] vs finite differences |
| Inventory risk: exact scenario grid of the settlement average; mean-variance + CVaR charge; exact adversarial worst case | `docs/MODELS.md` s.2, `dh/strategy/scenario.py` | [BUILT], tested vs closed forms |
| Fill intensity: compound Poisson taker flow vs queue ahead, per segment, gamma-Poisson shrinkage | `dh/strategy/fill_model.py`, `dh/research/calibrate_flow.py` | [BUILT]; parameters [ESTIMATE] until calibrated |
| Toxicity: E[markout given fill, features]; parametric in M1, fitted in M2 only if it lifts net P&L | `docs/MODELS.md` s.3.2, Experiment 3 | [BUILT]; parameters [ESTIMATE] |
| Quote objective: EVrate = intensity x (edge - AS - fee - hedge - inventory); ranking by EVrate per $ collateral | `dh/strategy/quoting.py`, `mm.py` | [BUILT] |
| Hedge: trade to the band edge when \|D\| > 2cS/(lambda sigma^2 h); unwind at settlement | `docs/research/05_hedge_policy.md` | [VERIFIED] on real paths with synthetic flow |
| Fees: exact Kalshi quadratic schedule by series type and multiplier, with micro rounding and per-order carry | `dh/kalshi/fees.py`, `config/fees.yaml` | [BUILT]; must be verified vs live `fee_cost` |

## E. Research test matrix

`docs/TEST_MATRIX.md` gives, for every hypothesis: hypothesis, dataset, dependent variable,
features, benchmark, method, and accept and reject rules. It covers E0-E10 plus fair-value
calibration, the settlement-convention check and incentives.

Results so far:
- **Fair value:** [VERIFIED], `docs/research/01_*`.
- **E5 hedging:** [VERIFIED on real paths], `docs/research/05_*`.
- **E0 and E1:** [BUILT], and the known-answer tests recover injected effects:
  - Maker P&L sign follows flow toxicity.
  - Injected maker lags of 0.3, 1.5 and 4 s give measured half-lives of 0.21, 0.75 and
    1.98 s. After 2-sd external moves the injected staleness shows as 1.4-3.7 ticks.
- **E2-E4 and E6-E10:** replay harness in `dh/research/` (runbook:
  `docs/research/EXPERIMENTS_RUNBOOK.md`).

---------------------------------------------------------------------------------------------
## F. M1: the simplest version that can trade tiny size safely and produce useful data

| Step | What | Exit criterion |
|---|---|---|
| M1.0 | Deploy on AWS us-east-1: `scripts/record.py` (Kalshi WS for all `KXBTC*` + BRTI 1/5 Hz + venues + Deribit) and `scripts/download_kalshi_history.py` (6-12 months of `KXBTCD`/`KXBTC`/`KXBTC15M` trades and settled markets) | Smoke tests pass; 7 days recorded with fewer than 0.1% sequence gaps |
| M1.1 | **Experiment 0** on historical trades | Keep only segments whose maker net P&L CI lower bound exceeds 0.15c/contract; stop if none |
| M1.2 | **Settlement check:** BRTI prints vs every settled `expiration_value` | 99% of events match to $0.01, else fix the convention before any final-minute trading |
| M1.3 | Fee verification (`scripts/verify_fee_schedule.py`) and resolution of each series' `fee_type` | Model matches `fee_cost` on every test fill |
| M1.4 | **Paper mode** (`scripts/run_live.py --mode paper`): the live strategy against the live book with a conservative (C) simulator | 7 days; shadow net c/contract and markouts by segment |
| M1.5 | **Live tiny size** (`config/m1.yaml`), for >= 4 weeks and >= 5,000 fills | Section K decision |

M1 live settings (`config/m1.yaml`):
- **Size:** clip 5 contracts; max 10 contracts per market. (Clip 2 would pay ~0.25c per
  contract in per-order fee rounding alone; clip 5 pays ~0.10c.)
- **Loss limits:** worst case $20 per event and $50 in total; daily loss halt $25; settlement
  loss pause at $15.
- **Market scope:** YES price 3-97c; no near-strike quotes after T-90 s; only segments that
  pass E0.
- **Exchange protection:** hedge disabled; order-group limit 20 contracts per 15 s.

M1 produces what everything later needs:
- live fills and markouts;
- queue-position calibration (`/portfolio/orders/queue_positions` vs the estimator);
- latency distributions, fee reconciliation and the real taker-flow rates per segment.

## G. M2: improvements justified only by M1 evidence

Each item ships only if its experiment passes on M1 data under fill policies B **and** C.
- Fitted fill-intensity per segment and a **toxicity-aware cancel/skew** (E3).
- Composite BRTI nowcast from constituent venues (E2, when it improves both RMSE and P&L).
- Queue-priority rules (E4).
- Segment enablement by tau and \|z\| (E6/E7).
- Quoting the current and next events.
- `KXBTC` ranges and `KXBTC15M` (E0 per series); incentive capture where programs exist.
- Size x5 (clip 10, limits x4) if capacity replay (E10) holds the edge.
- Final-minute far-strike quoting if E6 shows net > 0 in the <60 s buckets.
- Selective taking of stale quotes as a separate strategy (E8, only if > 0.5c net after fees
  with >= 20 opportunities/day).

## H. M3: scaling and sophistication

Each item follows its own evidence gate.
- **Hedge engine on:** only if E5 re-run on real fills shows positive utility at the achieved
  perp fee tier (about 1 bp or less).
- **Portfolio optimization across series and expirations.**
- **Automated monthly refits** of vol, tails and flow models.
- **Rust feed-to-cancel hot path:** only if the latency rule in C triggers.
- **Multi-account / FCM routing:** only if capacity is exhausted.
- **Direct membership** (if available to us): the balance precision drops from $0.01 to
  $0.0001, which removes nearly all per-order fee rounding. That is worth 0.05-0.3c per
  contract on maker-fee series at 5-10 lots.
- **Richer ensembles:** only with out-of-sample net P&L improvement (never on forecast metrics
  alone).

---------------------------------------------------------------------------------------------
## I. Risk controls (exact limits and kill switches)

`docs/MODELS.md` s.6 and `dh/strategy/risk.py`.

**Limits:**
- position per market, halved inside 5 minutes;
- exact worst-case loss per event and in total, with adversarial fills of working orders;
- portfolio \|delta\|;
- hedge notional;
- daily loss halt (manual reset);
- per-settlement loss pause of 1 hour.

**Kill switches, routed by exact stream name:**
- Kalshi WS disconnect or gap: cancel all, resnapshot, 5 s settle before resuming.
- Per-market book gap: cancel that market.
- BRTI stale: > 3 s stops near-expiry quoting; > 10 s cancels all; 30 s of fresh ticks
  required to resume.
- External venues stale: stop quoting.
- Abnormal move: 6-sigma over 60 s pauses for 120 s.
- Order-group trigger: cooldown plus reset.
- Fee mismatch halts quoting; position mismatch halts everything.
- Manual kill file; independent watchdog process (cancel-all when the heartbeat stops).

**Exchange-side protections:** `cancel_order_on_pause`, order groups, and optional order
expiry.

**Residual risks tracked** (a "hedged" book is never riskless):
- gap/jump risk: the step payoff at strikes near the settlement average;
- basis risk: hedge venue vs BRTI;
- settlement-index risk: BRTI outages, methodology changes, `expiration_value` convention;
- liquidity, model (width of the fair-value band) and operational risk.

## J. Economics [ESTIMATE unless marked]

Unit economics per filled contract (cents):

| Term | Bear | Base | Bull | Basis |
|---|---|---|---|---|
| Gross edge vs fair at fill | 0.6 | 1.2 | 2.0 | [ESTIMATE] 1c tick near the money, 2-5c spreads on wings |
| Adverse selection (60 s markout) | -0.8 | -0.6 | -0.5 | [ESTIMATE]; E3 measures it |
| Kalshi maker fee, incl. per-order rounding | -0.50 | -0.30 | 0.00 | [VERIFIED formula] 0.0175 x P(1-P) per contract if the series charges makers, then each order's cash is rounded to $0.01, so an order pays its exact fee rounded up to the cent: 0.50-0.60c per contract near 50c, 0.10-0.30c on the wings at 5-10 lots, >= 0.50c for any 2-lot (`docs/MODELS.md` s.3). 0 for `quadratic` series. Some KXBTC series are `quadratic` (other repo's 2026-09-16 fee map); KXBTCD unknown |
| Hedge cost | 0 | 0 | -0.05 | [VERIFIED] no hedge at M1/M2 size |
| **Net per contract** | **-0.7** | **0.3** | **1.45** | Target >= 0.15c; > 0.75c triggers a fill-model audit before belief |

Scale (taker flow is the binding resource, not capital):

| | Bear | Base | Bull |
|---|---|---|---|
| KXBTCD taker volume, contracts/day | 1M | 5M | 15M |
| Share of volume in segments with edge | 20% | 30% | 40% |
| Our capture share of that flow | 1% | 3% | 5% |
| Our contracts/day | 2,000 | 45,000 | 300,000 |
| Net $/day | -14 | 135 | 4,350 |
| Annualized net $ | < 0 (stop) | ~50k | ~1.6M |
| Peak collateral (about 10x average deployed) | $2k | $5k | $30k |
| Risk capital for drawdowns (about 20x daily P&L s.d.) | $5k | $15k | $60k |

Volume figures are placeholders. The first week of recording plus the historical downloader
replaces them with measured `KXBTCD` volume by segment, which E10 then converts into capacity
at > 1.0c, > 0.75c, > 0.5c, > 0.05c and breakeven. Liquidity-incentive income is excluded
until actually earned.

## K. Falsification criteria

**1. The strategy does not work** (stop, or pivot to incentives-only quoting) if any holds:
- E0: maker net P&L CI upper bound < 0.15c in every segment of every BTC series.
- Paper mode (policy C), 7 days: net c/contract CI upper bound < 0.
- Live M1 after >= 5,000 fills: realized net c/contract CI entirely < 0 in the segments
  predicted best, or live markouts worse than paper by more than 0.5c.
- Settlement convention cannot be reproduced (no final-minute trading at all).

**2. It works but does not scale** if:
- Live net CI is > 0 at M1 size, but capacity replay (E10, policies B and C, calibrated with live
  queue data) shows fewer than 20,000 contracts/day at > 0.15c (under about $30/day).
- Or live net c/contract falls by more than 50% when size doubles.
- Or our share of profitable taker flow saturates below 3%.

**3. It has real scalable alpha** when all hold:
- Realized net >= 0.3c/contract with CI lower bound > 0.15c over >= 20,000 fills.
- Stable across >= 3 consecutive monthly walk-forward periods and across vol regimes and
  weekday/weekend.
- Degrades by less than 30% at 5x size.
- E10 capacity >= 50,000 contracts/day above 0.15c.
- Profitable contracts per day and net cents per contract are both attractive at the same time.

Any result > 0.75c/contract triggers an audit of fill simulation and attribution before it is
believed (`docs/TEST_MATRIX.md`).
