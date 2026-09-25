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

## 6. Exchange behaviour simulated

| feature | simulation | Kalshi source |
|---|---|---|
| Create validation | Rejects, checked in this order: `duplicate_client_order_id` (409), `invalid_side`, `invalid_count`, `invalid_price` (tick grid from `register_market(MarketSpec)`), `market_closed`, `market_paused`, `order_group_not_found`, `order_group_triggered`, `invalid_expiration`, `post_only_cross` | CreateOrderV2 |
| Cancel | `CancelAck(canceled_qty = remaining)`. If the order was already filled: `OrderReject(request='cancel', reason='already_filled')`. Also `already_canceled` and `not_found` | CancelOrderV2 (`reduced_by`) |
| Amend | `count` = filled + desired remaining. A size decrease keeps queue priority; a price change or size increase goes to the back of the new level. The response carries the new client_order_id and the same order_id. A price change that would cross is rejected | AmendOrderV2 note on priority |
| Decrease | `reduce_to` keeps priority; 0 cancels the order | DecreaseOrderV2 |
| CancelAll | Every resting order (optionally filtered by tickers) is canceled via user_orders updates. There is no per-order REST ack | DELETE /portfolio/events/orders |
| GTT expiry | `expiration_ts` is floored to whole seconds, as the API takes seconds. The order is canceled at expiry | `expiration_time` |
| Market close | The close time comes from `MarketSpec.close_ts` or lifecycle `created`/`close_date_updated`. At close, resting orders are auto-canceled and new orders rejected. No fills after close, and `determined`/`settled`/`Settlement` also close the market | market lifecycle |
| Pause | Lifecycle `deactivated` / `is_deactivated`: orders with `cancel_on_pause` are canceled, the others stay but cannot fill, and new orders are rejected until `activated` | `cancel_order_on_pause` |
| Order groups | `create_order_group(id, limit)`: matched qty over a rolling 15 s window is capped at the limit. Reaching the limit triggers the group: every order in it is canceled, new orders are rejected, and WS `KalshiOrderGroupUpdate('triggered')` is sent. `reset_order_group`, `update_order_group_limit` and `delete_order_group` are also supported. **Assumption to verify live:** the fill that reaches the limit is truncated at the limit | order groups (rolling 15 s `contracts_limit`) |
| Messages | Each fill produces `KalshiFill` (trade_id unique, `post_position`, `fee_fn` fee) and `KalshiOrderUpdate` (user_orders). REST results are `OrderAck`, `OrderReject` and `CancelAck` | fill, user_orders |

Order-group management has no `dh.core.actions` action yet, so these calls act immediately with
no latency (see section 12).

## 7. OrderManager (runs inside the Strategy, live and backtest)

States: `PENDING_NEW -> RESTING -> {PENDING_CANCEL, PENDING_AMEND} -> {FILLED, CANCELED}`, plus
`REJECTED`. `request_place`, `request_cancel`, `request_cancel_all`, `request_amend` and
`request_decrease` only record intent; confirmed states change on events.
`request_cancel` returns False when the CancelOrder must not be sent now. A cancel for an order
without an order_id is **deferred**, and `on_event` later emits `cancel_ready` carrying the
order_id.

Quantity model per order:

* `cap` = max fillable at the exchange;
* `fill_sum` = sum of deduplicated fill messages;
* `filled_rep` = the maximum filled qty reported by acks, updates or cancel acks;
* `filled = max(fill_sum, filled_rep)`;
* `remaining = cap - filled` while live;
* `inflight = filled_rep - fill_sum`, i.e. fills that happened but whose message has not
  arrived yet.

`position(ticker)` comes **only** from fill messages. `worst_case_exposure(ticker, side)` is the
position plus everything that could still fill or is in flight:

* resting, pending-new and pending-cancel remaining;
* the amend-up target;
* in-flight fills;
* for a terminal-but-unresolved order, its whole unfilled cap.

Cash and fees are accumulated in micros.

| race | handling |
|---|---|
| Fill before the create ack | A fill with our client_order_id attaches to the PENDING_NEW order, which becomes RESTING, or FILLED if complete. A fill without client_order_id is buffered by order_id (`orphan_fill`) while position updates immediately. The buffered fill is attached when an ack or update reveals the order_id |
| Ack after a full fill | The ack reflects placement time (`fill_qty=0, remaining=qty`). Terminal states are absorbing and `cap = min(cap, fill + remaining)`, so the order stays FILLED |
| Fills after a cancel request | They are counted normally. A fill that completes the order moves it to FILLED |
| Cancel ack while fills are in flight | The final filled qty is `cap - canceled_qty`. This is exact, and the difference is tracked as in-flight until the fills arrive |
| Cancel rejected because already filled | The order becomes FILLED with `filled_rep = cap`. Exposure keeps the in-flight fills |
| Cancel rejected as not_found, already_canceled or gone | Without an order_id, the cancel overtook the create: the order stays PENDING_CANCEL and the cancel is re-armed. With an order_id, the order is CANCELED but *unresolved*: exposure keeps its unfilled cap and `reconcile_needed` is emitted until a terminal update arrives |
| Transient cancel reject (rate limit, 5xx) | The order reverts to its working state |
| Duplicate fills | Deduplicated by `trade_id` from WS, REST backfill and replays. They are counted in `stats` |
| Amend ack | Re-keyed to the new client_order_id with the old one kept as an alias. `cap` becomes the amended count and the price becomes the new price. Fills that quote either id or the order_id resolve. An amend reject reverts, or finalizes on `already_filled` |
| Out-of-order order updates | Terminal states are absorbing. `filled_rep` is a running maximum. A `resting` update is ignored when its `ts_exch` is older than one already applied, and `cap` only shrinks through updates |
| Unknown-outcome create (timeout or 5xx reject, or no response after `ack_timeout_ns` on a `Timer`) | The order stays PENDING_NEW with `unknown_outcome`, its full qty counts in exposure, and `reconcile_needed` is emitted. It is resolved by an update or fill (the order exists), or by `reconcile_missing` (it never reached the book, so REJECTED) |
| Position cross-check | A fill `post_position` that differs from ours, or a `KalshiPositionSnapshot` / `reconcile_position` mismatch, emits `position_mismatch`. The position is adopted only with `adopt=True` |
| Order group triggered | `group_triggered` is emitted and `group_blocked(id)` holds until the group is reset. The member orders' cancel updates follow |

`test_order_manager.py::test_any_delivery_order_converges` feeds every permutation hypothesis
finds of an order's full message set, with duplicates, missing client ids and cancel requests
at random points. It checks four things:

* the position never exceeds the truth;
* worst-case exposure never under-states the final position;
* terminal states never resurrect;
* the final state, position and filled qty converge.

## 8. Hedge venue (`hedge_sim.py`)

`HedgeVenueSim(venue, maker_bps, taker_bps, latency, seed, symbol=..., fill_policy=...)` consumes
`ExtBookSnapshot`, `ExtBookDelta`, `ExtBBO`, `ExtTrade` and `PerpState`.

* Market orders walk the book at arrival. Depth that is short leaves a partial fill, and the
  rest is canceled with reason `insufficient_depth`.
* Marketable limits take up to their limit and rest the remainder. Post-only limits that would
  cross are rejected.
* Resting orders queue behind the displayed size and fill on prints at or through their price,
  with the same blocking rules as Kalshi. Unexplained size decreases move the queue by policy.
  Prints are matched only to *later* decreases (trade-first).
* Fees are bps of notional. `slippage_log` records the VWAP against the mid at decision and at
  arrival.
* Funding is paid at each `next_funding_ts` as `-position_btc * mark * funding_rate`, using the
  latest announced rate, into `funding_usd` and the `on_funding` hook.
* It emits `HedgeFill` and `HedgeOrderUpdate`.

## 9. Markouts (`markout.py`)

The signed markout in cents per contract (positive = good for us) is:

* `100 * (fv(t+h) - px)` for bids;
* `100 * (px - fv(t+h))` for asks.

It is computed at horizons `[0.1, 0.5, 1, 5, 10, 30, 60]` s and to settlement (payout 1/0).
`fv` can be any of:

* a callable;
* `(ts, values)` arrays, looked up as-of and NaN outside the series;
* a per-ticker dict;
* a `(ticker, ts)` callable.

Fees are reported separately (`fee_c`). `summarize` gives the contract-weighted mean and the
standard error per horizon.

## 10. What is NOT modeled

* **Market impact of our quotes.** Other participants do not see or react to our simulated
  orders: they don't step ahead of or away from them, and flow does not change. Our fills do not
  remove recorded liquidity, so "excess" print volume went to makers behind us in reality.
* **Hidden or iceberg liquidity and off-book blocks.** Block trades (`is_block`) are ignored.
* **Queue position after an amend or decrease at the exchange.** The model follows the
  openapi note: decreases keep priority, everything else loses it. **This must be measured
  live** with the queue_positions endpoint before relying on it.
* **Deltas and trades caused by our real orders in recordings made while we were live.** They
  are treated as other participants' activity. Before backtesting such periods, strip them using
  `own_client_order_id` and our fill trade_ids.
* **Cancel classification.** It is a policy assumption, not an observation: Kalshi does not
  publish order-level data.
* **Market-data jitter.** It is a constant offset; per-event md jitter should be folded into
  the submit distribution.
* **Exchange behaviour not modeled:**
  * the fee-rounding carry per order, unless `KalshiExchangeSim(order_fee_fn=...)` is given
    (the KAT harness passes an `OrderFeeAccumulator` per order; audit M8);
  * rate limits;
  * the "cancel-all also cancels orders placed within the next minute" behaviour;
  * self-trade prevention mode `maker`;
  * IOC/FOK time-in-force: `PlaceOrder.time_in_force` reaches the live adapter, but the
    simulator treats every order as good-till-canceled.
* **Crossing inference.** Rule (iii) assumes the aggressor behind a crossing level would have
  traded with us. It may have been a post-only order that would have been rejected, which is why
  C excludes it.

## 11. Calibration from live data

1. **Queue estimator.** In live trading, run `QueueCalibrator` (all three policies,
   `book_includes_own=True`) on the real book. Our own positive `orderbook_delta` carrying our
   client_order_id fixes each arrival queue exactly. Poll `GET /portfolio/orders/queue_positions`
   and feed `ingest_exchange_queue_position`. `summary()` gives the bias, MAE and RMSE per
   policy. Use the policy with the smallest bias as the "realistic" default, and keep C as the
   acceptance bar.
2. **Shadow orders.** Replay the live session's recording through `KalshiExchangeSim` with the
   *same* orders we actually sent, at their real decision times. Compare simulated and real
   fills per order: fill probability, time to fill and partial sizes. A persistent
   `sim_fills > real_fills` means the policy is too optimistic.
3. **Latency.** Log `decision -> REST response` and `engine ts_ms -> WS receipt` for every
   request, and feed those samples to `Empirical`. Measure `md` from `ts_ms` against receive
   time.
4. **Match window.** Measure the distribution of |trade ts - matching delta ts| per match and
   set `match_window_ns` to its 99th percentile.
5. **Amend priority.** Place, then amend down, then query the queue position. Separately, amend
   the price or size up and query again. Verify the decrease-keeps-priority rule.
6. **Order-group limit semantics.** In the demo environment, check whether the limit-reaching
   fill is truncated or completed.

## 12. Requested `dh.core` changes (not made: core is frozen)

* An action to manage Kalshi order groups (`CreateOrderGroup`, `ResetOrderGroup`,
  `UpdateOrderGroupLimit`, `DeleteOrderGroup`), so the Strategy owns groups with latency like
  any request. Today the simulator exposes direct methods.
* A `PlaceOrder.time_in_force: 'gtc' | 'ioc' | 'fok'` field. IOC is the natural way to take
  liquidity; without it, a non-post-only order rests its remainder.
* An optional `KalshiFill.fill_id` separate from `trade_id` if the adapter ever sees
  REST fills whose `trade_id` differs from the WS `trade_id` (dedupe key).
