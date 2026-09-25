# Fair value, delta and gamma of Kalshi BTC threshold contracts

Code: `dh/settlement/window.py`, `dh/models/fairvalue.py`, `dh/models/tails.py`,
`dh/models/vol.py`, `dh/models/fvmodel.py`. Calibration evidence:
`docs/research/01_fair_value_calibration.md`. Decision models (quote value, risk, hedging)
are in `docs/MODELS.md`, which defers the settlement-window math, greeks and tail models to
this document; the premise document's "derivation in docs/MODELS.md" also refers to this
material (section 3 below).

## 1. What is priced

A KXBTCD market expiring at `T` pays $1 if the expiration value `A` beats the strike, where
`A` is the simple average of `n = 60` once-per-second BRTI prints stamped `T-59s, ..., T`
(window `(T-60s, T]`, the convention Kalshi documents for `last_60s_windowed_average_15min`;
it still has to be reconciled with published `expiration_value`s, see section 7).

| strike_type | YES iff |
|---|---|
| `greater` | `A > floor` |
| `greater_or_equal` | `A >= floor` |
| `less` | `A < cap` |
| `less_or_equal` | `A <= cap` |
| `between` | `floor <= A <= cap` |

## 2. Window state

At time `now` the tracker (`SettlementTracker.window_state`) reduces the index history to

    WindowState(n_obs, k_fixed, sum_fixed, m_remaining, tau_first_s, step_s)

with `n_obs = k_fixed + m_remaining`, `tau_first_s` the seconds until the first unfixed print
and `step_s = 1`. Before the window opens `k = 0`, `m = 60`, `tau_first = T - 59s - now`.

Settlement arithmetic. With `R` the average of the `m` remaining prints,

    A = (sum_fixed + m R) / n,      A > K  <=>  R > K_req = (K n - sum_fixed) / m.

`K_req` is the **required remaining average**. When `m = 0` the outcome is known: the code
returns the exact payoff of `sum_fixed / n` (strict vs non-strict inequality honoured) with
zero delta and gamma.

Print selection (details in the module docstring): `IndexTick.ts_exch` (the upstream source
timestamp) defines the second. 1 Hz print for second `s` = the tick stamped in `[s, s+1)`;
with only the 5 Hz feed, the last 5 Hz tick at or before `s` (final once a later tick
arrives). Duplicates are idempotent; conflicting values for the same source timestamp keep the
first. Missing seconds: `gap_policy='carry_forward'` (default, previous print) or `'skip'`
(average of the prints that exist). Observations whose time has passed but whose print has not
arrived are *pending*: unfixed with `tau_first = 0`.

## 3. Distribution of the remaining average

Model the benchmark as arithmetic Brownian motion around the current nowcast `spot`:

    S(now + u) = spot + drift(u) + sigma_abs W(u),      sigma_abs in $/sqrt(s).

The remaining prints are at `tau_j = tau_1 + (j-1) h`, `j = 1..m` (`h = step_s`), so

    R - spot - drift = sigma_abs (1/m) sum_j W(tau_j),
    Var(R) = sigma_abs^2 V,   V = (1/m^2) sum_i sum_j min(tau_i, tau_j).

**Derivation of the closed form.** `min(tau_i, tau_j) = tau_1 + (min(i,j) - 1) h`. The number
of ordered pairs `(i, j)` with `min(i, j) = k` is `2(m - k) + 1`, so

    sum_i sum_j min(i, j) = sum_{k=1..m} k (2(m-k) + 1) = m (m+1)(2m+1) / 6

and

    V = tau_1 + h [ (m+1)(2m+1) / (6m) - 1 ]        (seconds).

For the full window (`m = 60`, `h = 1 s`): `V = tau_1 + 19.50 s`. The average of 60 prints
has the variance of a *single* print taken 20.5 s after the window opens (at `T - 39.5 s`), not
of the print at `T`: at 2 minutes before expiry the effective horizon is 80.5 s, not 120 s
(sd 18% lower), at 60 minutes it is 3560.5 s (0.6% lower). Inside the window `V` shrinks both
because `tau_1 -> 0` and because `m` falls: with `m` prints left and the next one due now,
`V = m/3 - 1/2 + 1/(6m)` seconds (19.5 s at `m = 60`, 4.5 s at `m = 15`).

Irregular or pending observation times use the exact general form
`V = (1/m^2) sum_k tau_(k) (2(m - k) + 1)` over the sorted times (`avg_variance_time_general`).
Tests check the closed form against the brute-force double sum and a seeded Monte Carlo of
Brownian paths (`tests/models/test_fairvalue.py`).

Time-varying volatility. With a deterministic intraday profile `sigma(u)`,
`Var(R) = int sigma(u)^2 g(u)^2 du` with `g(u) = #{j : tau_j >= u} / m` (fraction of the
remaining prints still ahead of `u`). The implementation uses the time-average of the seasonal
factor over `[now, T]` for the whole of `V`; the only error is the weighting of the final minute
by `g^2 < 1`, negligible except within a minute or two of expiry.

Nowcast error. If `spot` differs from the true current index by an independent error with sd
`nowcast_sd` (venue basis, feed latency), `Var(R - spot) = sigma_abs^2 V + nowcast_sd^2`
(keyword `nowcast_sd` of `digital`). This keeps prices and greeks finite in the last seconds,
where `V -> 0`; without it the model would claim certainty that the nowcast cannot deliver.

## 4. Price and greeks

Write `sd` for the standard deviation of `R`, `mu = spot + drift_abs`, and let `eps` have the
standardized tail distribution `F` (density `f`, `f'` its derivative). With
`z = (K_req - mu) / sd`:

| type | `P(YES)` | `delta = dP/dspot` | `gamma = d2P/dspot2` |
|---|---|---|---|
| greater(_or_equal) | `1 - F(z)` (computed as `sf(z)`) | `f(z) / sd` | `-f'(z) / sd^2` |
| less(_or_equal) | `F(z)` | `-f(z) / sd` | `f'(z) / sd^2` |
| between | `F(z_cap) - F(z_floor)` | `(f(z_floor) - f(z_cap)) / sd` | `(f'(z_cap) - f'(z_floor)) / sd^2` |

since `dz/dspot = -1/sd`. For the Gaussian `f'(z) = -z f(z)`, so an out-of-the-money
`greater` contract (`z > 0`) has positive gamma. Units: `spot` in $, `delta` per $ of index,
which **equals the BTC quantity that offsets one YES contract** on a $1 payout
(hedge notional = `delta * spot`); `gamma` per $^2.

Why the fixed prints do not appear in delta: a $1 move of spot moves every remaining print by
$1 and the average by `m/n` dollars, while the sd of the average is `(m/n) sd`; both factors
cancel, so `delta = f(z)/sd` with `sd` the sd of `R`.

Numerics: upper tails use `sf` directly (no `1 - cdf` cancellation); `between` uses
`sf(z_f) - sf(z_c)` when both thresholds are above the mean and `cdf(z_c) - cdf(z_f)` when both
are below. Parity (`P(greater K) + P(less_or_equal K) = 1`), finite-difference greeks, limiting
cases (`m = 0`, `tau -> 0`, far strikes, `sigma = 0`) and vectorized-vs-scalar agreement are
all tested.

Scale of the greeks at BTC $84,541, 35% annualized vol, ATM: 60 min before `T`, `sd = $314`,
`delta = 0.3989 / 314 = 0.00127 BTC` ($107 notional); 2 min before, `V = 80.5 s`,
`sd = $47`, $713 notional. Gamma is `z f(z) / sd^2`: it grows like `1/sd^2` as expiry
approaches, which is why near-the-money inventory cannot be hedged smoothly in the last minutes
(`docs/research/tables/fv_q5_greeks.csv` has the full grid, including in-window states).

## 5. Tail models (`dh/models/tails.py`)

All are symmetric with unit variance, so `sd` keeps its meaning and only the shape changes.

* **Gauss.** Exact under the Brownian model with known volatility.
* **StudentT(nu)**, `nu > 2`: `eps = T_nu * sqrt((nu - 2)/nu)`. `F(z) = stdtr(nu, z/s)`,
  `f(z) = t_nu(z/s)/s`, `f'(z) = -f(z) (nu+1)(z/s) / (s (nu + (z/s)^2))`, `s = sqrt((nu-2)/nu)`.
  Kurtosis `3 + 6/(nu - 4)`.
* **VolMixture(cv)**: `eps = s * N(0,1)` with `log s ~ N(-omega^2, omega^2)`,
  `omega^2 = log(1 + cv^2)` (so `E[s^2] = 1`, `sd(s)/E[s] = cv`). Integrated with 48-point
  Gauss-Hermite quadrature, node scales renormalized to exact unit variance:
  `F(z) = sum_i w_i Phi(z/s_i)`, `f(z) = sum_i w_i phi(z/s_i)/s_i`,
  `f'(z) = -sum_i w_i z phi(z/s_i)/s_i^3`. Kurtosis `3 (1 + cv^2)^4`. It represents
  uncertainty about the volatility that will be realized over the horizon.
* **EmpiricalTail(u)**: Gaussian-kernel smoothed empirical distribution of standardized
  residuals (research reference only; tabulated on a grid).

In production the tail enters with a scale: `sd_used = c * sd_model`, with `(nu, c)`
depending on the horizon (`TailSchedule` in `dh/models/fvmodel.py`).

**Fair-value band.** `digital_band(spec, ws, spot, sigma_abs_values, tails, drift_abs,
nowcast_sd_values)` evaluates `P(YES)` over every combination of volatility, tail-model and
nowcast-error scenarios and returns `(p_lo, p_hi, center)`, the band that `docs/MODELS.md`
section 1 quotes against (bids vs `p_lo`, asks vs `p_hi`). At the money a symmetric model's
band collapses to 0.5 whatever the volatility; the band is widest on the shoulders and in the
tails, which is where the calibration study finds the Gaussian and fat-tailed models disagree.

## 6. Volatility (`dh/models/vol.py`)

* **EwmaVol** (streaming, irregular steps): returns are folded in once `min_dt_s` has elapsed
  since the previous anchor; `x = r^2 / dt` is a per-second variance sample and, with
  `a = 2^(-dt/H)`, `S <- a S + (1-a) x`, `W <- a W + (1-a)`, `var = S / W`. `W` makes the
  estimate unbiased during warm-up and independent of the sampling frequency for Brownian
  prices. Returns spanning more than `max_dt_s` (outages) are dropped while history decays.
* **SeasonalVol**: multiplicative sigma profile `f(t)` (flat / time-of-day / weekday-weekend x
  time-of-day / hour-of-week; bucket width and time zone configurable, DST-aware). The weekly
  time-average of `f^2` is 1. Fitted on training returns normalized by a slow causal vol level.
  EWMAs run on deseasonalized returns (`x / mean f^2` over the return's interval) and the
  forecast for `[now, T]` is multiplied back by the mean of `f^2` over `[now, T]`
  (exact piecewise integration).
* **VolForecaster**: `sigma^2(now, T) = c^2 * mean_f2(now, T) * sum_i w_i(h) ewma_i(now)` with
  weights interpolated in the horizon `h = T - now` (HAR-style: short horizons lean on short
  half-lives). Weights are fitted walk-forward by QLIKE, which targets the conditional mean of
  realized variance and is robust to its heavy right tail.
* Realized-measure helpers: realized variance, bipower variation, jump share, Parkinson and
  Garman-Klass range variances.

## 7. Exact vs approximate, and known limitations

Exact (given the model inputs): the window accounting and required remaining average; the
variance time for equally spaced (or arbitrary) print times under constant volatility; the
greeks of the stated distribution; determined outcomes including strict/non-strict inequalities.

Approximations and limitations:

1. **Arithmetic vs log dynamics.** BTC is closer to geometric Brownian motion. For a martingale
   price the lognormal model shifts `P` by about `phi(z) (sigma sqrt(t) / 2)(z^2 - 1)`: at a
   one-hour horizon and 50% annualized vol (`sigma sqrt(t) = 0.53%`) that is at most about
   0.1 cent at the money and 0.04 cent at 2 sd. Below the 1-cent tick and below what the
   calibration study can resolve; ignored.
2. **Jumps and news.** Not modelled structurally. Fat-tailed shapes (Student-t, vol mixture)
   absorb their unconditional effect; scheduled events (US macro releases, FOMC) raise
   volatility far above the average seasonal profile on the days they happen. An event
   calendar is the obvious next input.
3. **Symmetric tails.** No skew; the study checks up and down tails separately.
4. **Constant volatility within the horizon** apart from the deterministic seasonal factor;
   vol-of-vol is represented only through the tail shape.
5. **Benchmark vs venue basis.** `spot` must be a nowcast of BRTI. If it comes from a single
   venue or from a stale BRTI print, pass the basis/latency error as `nowcast_sd`. BRTI is a
   composite of constituent order books, so its 1-second returns can have microstructure
   properties (smoothing, autocorrelation) that 1-minute Bitstamp closes do not show; the study
   validated 60-second return sampling (`min_dt_s = 60`). Faster sampling must first be
   checked for variance-ratio bias on captured BRTI.
6. **Parameters come from a proxy.** Vol blend weights, seasonal profile and tail parameters
   were fitted on Bitstamp 1-minute candles with OHLC4 of the final minute as the settlement
   proxy. Re-fit on captured BRTI 1 Hz data and published `expiration_value`s before relying
   on the tails.
7. **Settlement convention.** The `(T-60s, T]` window, the gap policy and any rounding of
   `expiration_value` are inferred from the API documentation, not verified against settled
   markets.

## 8. Using it in the strategy

```python
from dh.settlement import SettlementTracker
from dh.models.fvmodel import FairValueModel, load_recommended_config

tracker = SettlementTracker()                      # gap_policy='carry_forward' until verified
fv = FairValueModel.from_config(load_recommended_config())

def on_index(tick):                                # every IndexTick (1 Hz and/or 5 Hz)
    tracker.on_index(tick)
    fv.update(tick.ts_exch, tick.value)            # source time; EWMAs sample >= 60 s returns

def quote_inputs(spec, now_ns, nowcast, nowcast_sd):
    ws = tracker.window_state(spec.settlement, spec.expiration_ts, now_ns)
    d = fv.price(spec, ws, spot=nowcast, now_ns=now_ns, nowcast_sd=nowcast_sd)
    return d.p_yes, d.delta, d.gamma             # $1 payout; delta = BTC per YES contract
```

`dh/models/data/fv_recommended.json` is regenerated by
`python -m dh.research.fv_study.run --out docs/research`; `FairValueModel.from_config` also
accepts a hand-edited dict with the same keys (`vol.half_lives_s`,
`vol.weights_by_horizon_s`, `seasonal`, `tail.by_horizon_s`). For portfolio hedging, the book
delta is `sum_i q_i * delta_i` (YES-equivalent quantities; a NO position is `-q` YES plus
cash), and `sum_i q_i * gamma_i` says how fast it drifts as the index moves.
