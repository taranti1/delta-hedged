# Execution model: order state, queue position, simulated fills

Code: `dh/execution/` (`order_manager.py`, `queue.py`, `latency.py`, `exchange_sim.py`,
`hedge_sim.py`, `markout.py`, `driver.py`). Tests: `tests/execution/`.

**Principle.** A backtest never assumes "the price touched my level, so I was filled". A maker
fill needs marketable volume that has cleared the queue ahead of our simulated order, or a
taker that went *through* our price. Every result is produced under three fill policies:
**A optimistic, B realistic, C conservative**. A strategy that is profitable only under A is
rejected. The ordering A >= B >= C of cumulative fills is a theorem of the model (section 5)
and a property test (`test_policy_ordering.py`).

Status: the model follows the vendored Kalshi specs (openapi 3.30.0, asyncapi 2.0.0). No
Kalshi data could be captured in the build environment, so every default parameter below
(latencies, match window) is a **placeholder until it is calibrated live** (section 11).

## 1. Conventions

* Units follow `dh.core.units`: px ints of $0.0001 on the YES scale, qty ints of 0.01
  contracts, money ints of micro-dollars. Hedge venues use float USD and BTC.
* Kalshi has two **bid** books per market: YES bids and NO bids. A NO bid at q is a YES ask at
  1 - q. Our V2 order `book_side='bid'` (buy YES) rests in the YES book at px. Our
  `book_side='ask'` (sell YES == buy NO) rests in the NO book at NO price 1 - px. Both books are
  bid books, so "at or better" always means "px >= p" on that book's own scale.
* A public trade with `taker_outcome_side='no'` (`taker_book_side='ask'`) sells YES into the YES
  book at `yes_px`. A trade with `'yes'` (`'bid'`) buys YES from the NO book at NO price
  1 - `yes_px`. `KalshiTrade.taker_side` carries `taker_outcome_side`.
* Our fills are always at **our** limit price when we are the maker, including sweep-through
  fills. The taker pays the better price, which is what price priority implies.

## 2. Clocks and latency (`latency.py`, `exchange_sim.py`)

The simulator runs on the **recorded timeline**, where each market event carries its local
receive `ts`. A recorded event at `ts` happened at the exchange at `ts - md_offset`. `md_offset`
is the median of the `md` latency distribution. It is a constant, so the recorded event order is
preserved, and it defaults to 0.

| delay | meaning | default (placeholder) |
|---|---|---|
| `submit` | decision -> create request at the matching engine | lognormal, median 30 ms, sigma 0.4 |
| `cancel` | decision -> cancel/amend/decrease at the matching engine | = submit |
| `response` | engine -> REST response (OrderAck / OrderReject / CancelAck) | lognormal, median 30 ms |
| `ws` | engine -> private WS message (KalshiFill / KalshiOrderUpdate / KalshiOrderGroupUpdate) | lognormal, median 10 ms, sigma 0.5 |
| `md` | exchange -> our receipt of public market data (constant offset) | 0 |

Distributions: `Fixed(ms)`, `LogNormal(median_ms, sigma, floor_ms, cap_ms)` and
`Empirical(samples_ms)`, which resamples measured round-trips. Each delay kind has its **own
seeded stream**. For example, extra fill messages under policy A never shift the submit
latencies of later orders, which makes cross-policy comparisons pathwise. Policy C multiplies
every delay by 1.5 by default (`latency_multiplier`).

Timing rules:

* An action decided at `d` reaches the engine at exchange time `d + submit`, which is sim time
  `d + submit + md_offset`. Requests for the same order (following amend ids) arrive in the
  order they were sent.
* At a tie with a recorded event, the **recorded event is processed first**.
* REST responses arrive at `exchange time + response`. WS messages arrive at
  `exchange time + ws` and are FIFO like one WebSocket connection. No message is delivered
  earlier than the sim time that produced it. The resulting bias is that a response cannot beat
  the market data of its own exchange instant. That bias is conservative and zero when
  `md_offset = 0`.
* Races emerge from these delays rather than being scripted:
  * fill before ack: `ws < response`;
  * fill between a cancel decision and its arrival: the cancel is acked for the rest, or
    rejected with `already_filled`;
  * a user_orders update before the REST ack.

Runner protocol, implemented by `driver.run_interleaved`:

```
for ev in events (non-decreasing ts):
    deliver sim.pop_due(ev.ts - 1)          # our messages strictly before ev
    due = sim.on_market_event(ev)           # exchange processes items < ev.ts, then ev
    strategy.on_event(ev); then each of `due` (messages at exactly ev.ts)
    every action decided at t -> sim.submit(action, t)
```

`next_due_ns()` returns the earliest pending item. That is either an exchange-side arrival,
expiry or close, or a delivery.

## 3. Queue model (`queue.py`, `QueueEstimator`)

State per resting order k: `q_k` = others' qty ahead of us at our level (int), and `R_k` = our
remaining qty. Order k joins its level with sequence number `seq_k`, which gives time priority
among our own orders.

1. **Arrival.** `q = displayed qty at our price - trade prints at that level whose book delta
   has not arrived yet`. We join behind everything visible. In the backtest the recorded book
   never contains our simulated orders.
2. **Trade at our price** (qty v): `q <- max(0, q - v)`. The front of the queue trades first.
3. **Trade/delta matching.** Kalshi publishes a `trade` message and an `orderbook_delta` for
   the same match, in no guaranteed order. Per (ticker, book, px) we keep FIFO lists:
   * *trade-first*: unmatched print volume is consumed by later negative deltas;
   * *delta-first*: unexplained negative-delta volume waits up to `match_window_ns`
     (default 250 ms) for a print, which consumes it.
   A match therefore never moves `q` twice (`test_queue.py` covers both arrival orders). Stale
   print volume is dropped after the window.
4. **Cancellations.** A negative delta that no print explains within the window is a cancel.
   `L` is the level qty excluding us just before the cancel, `v` is the canceled qty, and
   `L' = L - v`:

   | policy | rule | meaning |
   |---|---|---|
   | A optimistic | `q' = min(q - v, L')` | every cancel was ahead of us |
   | B realistic | `q' = min(q - floor(v*q/L), L')` | cancels are pro rata to position |
   | C conservative | `q' = min(q, L')` | cancels were behind us, unless the level is now smaller than our queue |

   All results are floored at 0. At classification time `q` is also clamped to
   `displayed others + still-unclassified depletion`. That clamp is policy-independent, and it
   is needed because FIFO print/delta pairing can make a stored `L` stale. A cancel only
   affects orders that were resting when its delta happened.
5. **Positive deltas** join behind us: `q` is unchanged.
6. **Snapshot** (resync): pending matches are dropped and `q <- min(q, displayed level)`.

Consequence: with the whole level ahead of us (`q = L`, typical right after arrival), B equals
A. B differs from A only after others have joined behind us.

## 4. Fill mechanisms (`KalshiExchangeSim`)

For a print of qty v on book b at price p, our orders on b at px >= p are visited in price-time
priority. Order k can receive `min(R_k, max(0, v - blocking_k))`:

| where the order is | blocking_k |
|---|---|
| at the print price (i) | own orders at better prices (their R) + `q_k` + own earlier orders at the same level |
| better than the print price, the taker **swept through** (ii), A/B | own better-priced R + own earlier same-level R. Price priority means the displayed queue at our level was consumed before the taker reached p; afterwards `q <- 0` |
| better than the print price (ii), C | also counts `q_k` and the queue of our better levels. C only fills through once prints have shown its queue exhausted; its `q` is untouched |

(iii) **Crossing inference, A/B only.** A positive delta on the *opposite* book at a price that
crosses our resting price (a new YES ask at or below our bid, or a new YES bid at or above our
ask) is inferred to be an aggressor that would have matched us. We can receive up to the delta
size, blocked by `q` and by our own better orders. C never fills from crossing inference.
Snapshots never create fills.

**Marketable orders** (`post_only=False`) walk the displayed opposite levels up to their limit
at the makers' prices, paying taker fees. Displayed size we take is recorded in a per-level
*phantom consumption* ledger, so later orders cannot take it again; the ledger shrinks when the
recorded level shrinks. Self-trade prevention follows Kalshi `taker_at_cross`: the walk stops
before our own resting opposite order and the remainder is canceled. The rest of the order rests
normally.

**Post-only orders** that would cross the available displayed opposite best, or our own resting
opposite order, are rejected with `OrderReject(reason='post_only_cross')`.

**No market impact.** Our resting orders never enter the recorded book, and nobody reacts to
them. A fill does not remove recorded liquidity, so the "excess" of a print over `q` is volume
that in reality went to makers behind us.

## 5. Policy ordering guarantee

Claim: on the same recorded stream, with the same order schedule and the same arrival times,
every order's cumulative filled qty satisfies F_A >= F_B >= F_C at every instant.

Sketch. By induction over events, assume `q_A <= q_B <= q_C` and `R_A <= R_B <= R_C` for every
order.

* Each step is monotone:
  * the trade update `max(0, q - v)`;
  * the policy cancel maps (`q - v <= q - floor(vq/L) <= q`, all followed by the same clamps);
  * the snapshot clamp;
  * the sweep reset (A/B set 0, C keeps `q`).
* The blocking terms are sums of q's and R's, with q omitted for A/B only where C includes it.
  So blocking_A <= blocking_B <= blocking_C, and `F' = min(Q, F + max(0, v - blocking))`
  preserves the order.
* The print/delta bookkeeping, arrival queues and post-only checks depend only on market events.

Conditions and caveats:

* The guarantee assumes equal arrival times, so tests disable C's latency multiplier. With the
  multiplier C also acts later, which is realistic stress but not monotone: a late arrival can
  be lucky.
* Order-group caps and strategies that react to their own fills can also break pathwise
  comparability.
* Fills are ordered per order, not merely in total. `test_policies_actually_differ` checks that
  the policies really separate. On streams with multi-level sweeps every order eventually fills
  under every policy; they then differ in *when* they fill.
