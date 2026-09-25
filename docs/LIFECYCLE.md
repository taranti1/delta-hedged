# XVIII. Deterministic trade lifecycle and race conditions

## The loop (single-threaded strategy; I/O in adapters)

| # | Step | Where | Deterministic rule |
|---|---|---|---|
| 1 | Ingest Kalshi + external data | adapters -> events | every frame stamped with receive ns and recorded before parsing |
| 2 | Validate feeds | `RiskEngine.health` | Kalshi books valid (no gap, 5 s since resync); BRTI age (receive *and* source time) <= 3 s near expiry / 10 s overall; runner not lagging, not reconciling, clock within limits; >= 2 fresh external venues only when venues are configured (off in M1: `feeds.only: []`) |
| 3 | Settlement state | `SettlementTracker` | fixed prints in (T-60, T], required remaining average per strike |
| 4 | Fair probability | `digital` + band | F, F_lo, F_hi from nowcast S, sigma forecast, window state, tail model |
| 5 | Delta / gamma | same call | analytic greeks of the averaging-window digital |
| 6 | Fill probability | `FillIntensityModel` | intensity from segment flow and queue-ahead |
| 7 | Toxicity | `AdverseSelectionModel` | expected markout given recent FV move, tau, queue position |
| 8 | Candidate EV | `quoting.evaluate` | v = edge - AS - fee - hedge - inventory; EVrate = intensity x v |
| 9 | Risk penalties | `scenario` grid | marginal variance/CVaR charge; hard limits via `RiskEngine` |
| 10 | Rank | `mm` | score = EVrate / collateral, greedy admission under limits and write budget |
| 11 | Submit passive orders | adapter | post-only V2 orders, `cancel_order_on_pause`, the order group of the market's shard, 120 s expiry in M1, explicit `subaccount` (1) and `exchange_index` (2) on every write; exact per-order fee (incl. rounding) priced into v, fee-efficient size |
| 12 | Monitor queue / external BTC | events + `queue_positions` poll | queue estimator (live: an order joins the queue at its own book delta); requote timer 200 ms; immediate requote on a benchmark move > 2 sigma(1 s) |
| 13 | Cancel/reprice when EV < 0 | `decide_side` | cancel if v < 0; replace only if EVrate gain > kappa |
| 14 | Process fills | `OrderManager` | dedupe by trade_id; position, cost basis, fee reconciliation |
| 15 | Portfolio delta | `mm` | D = sum q_i Delta_i + H |
| 16 | Hedge trigger | `decide_hedge` | \|D\| > band -> trade to band edge |
| 17 | Execute hedge | adapter | post-only at touch, IOC if urgent |
| 18 | Residual exposure | risk report | CVaR/worst-case per event; limits |
| 19 | Settle | lifecycle/settlement events | payout, unwind event's hedge, settlement-loss check |
| 20 | Reconcile | ledger | exchange positions/fills/fees vs internal ledger; mismatch halts (only on a positions read at least as recent as the last WS fill: `GET /exchange/user_data_timestamp`) |

## Race conditions and their deterministic handling

Strategy-level handling is in `dh/strategy` and `dh/execution/order_manager.py`; live-runner
handling is in `dh/live` (details in `docs/RUNBOOK.md`). Every runner-generated input to the
strategy is recorded, so a session replays bit for bit.

| Race | What happens | Handling |
|---|---|---|
| Fill arrives before the create ack | WS `fill` / `user_order` beats the REST response | Order manager buffers by client_order_id; the ack later attaches the order_id; position updated at fill time |
| Own book delta vs create ack | the live book shows our order before or after the ack | queue position is set only once our own delta shows the order (remembered if it came first); after 2 s without it, the order joins behind the whole displayed level |
| Fill during pending cancel | cancel in flight, order matched first | fill counts; worst-case exposure includes pending-cancel qty until CancelAck |
| Cancel rejected (already filled / not found) | REST 404 / error after full fill | canonical reject reasons: already filled -> FILLED, not found / already canceled -> CANCELED; a 404 is final only once the resting list confirms the order is gone |
| Cancel gets no definite answer | timeout / 5xx during a partial outage | strategy re-sends the cancel every change timeout (10 s); the venue keeps re-cancelling (capped backoff) while GET says resting and raises a stuck alarm; the sweep re-cancels stale PENDING_CANCEL orders |
| Create timeout (unknown outcome) | no response | counted as live for risk; never resent; looked up by client_order_id (subaccount explicit, only orders created after the request) with backoff; declared rejected after 30 s, re-checked at +10 s / +30 s, and pulled if it turns up |
| Duplicate client_order_id | ids reused across restarts | ids are `<run_prefix>-<session token>-n`; a 409 on a single create is a definite reject |
| Duplicate / out-of-order WS messages | reconnect replays, seq gaps | Kalshi seq per sid on market channels; fills de-duplicated by trade_id; order updates applied only if fill/remaining are monotone |
| Lost own fills / order updates | WS disconnect (own channels carry no seq) | on reconnect the runner holds quoting (`kalshi.reconcile` stale), back-fills fills since the disconnect, re-checks positions and resting orders, then resumes; a periodic back-fill runs every 60 s |
| Fill's post_position disagrees | a missed fill not yet back-filled | pause quoting, cancel all, request reconciliation; only a mismatch confirmed by REST (keyed on the discrepancy, persisting 5 s) halts |
| Position message vs fills / settlement | WS `market_positions` can beat its fill or follow settlement | never applied raw: a discrepancy must persist before it halts; closed/settled markets are skipped |
| Book gap during quoting | missed delta | FeedStatus(gap) -> CancelAll for affected markets -> resnapshot -> 5 s settle -> resume |
| Consumer lag / data backlog | frames pile up faster than the strategy consumes them | lag = max(queue lag, exchange-time lag); above 1 s the runner blocks new orders and sends `runner.lag` stale (the strategy cancels its quotes); the consumer yields after every order dispatch and feed readers per frame, so cancels and heartbeats are never starved |
| Stale quote vs external jump | BTC moves while our order rests | immediate requote trigger; cancels are sent before new quotes; a quote that would wait > 0.5 s for write tokens is dropped; order-group limit caps burst fills; abnormal-move pause |
| Rate limit | write bucket empty or 429 | 429 drains the bucket; batches capped by bucket capacity; dropped quotes are returned to the strategy as rejects (its order view stays exact) |
| Amend priority | amend may reset queue position | M1 uses cancel/replace; Experiment 4 measures amend semantics via `queue_positions` |
| Hedge venue rejects or partially fills | margin / connectivity | hedge engine sees the real position only from HedgeFill; a rejected/canceled hedge stops counting as pending; if the venue is down, quoting stops on sides that increase \|D\| (hedging is off in M1) |
| Settlement while orders rest | market closes at T | orders auto-canceled at close; strategy stops quoting near-strike markets inside T-90 s |
| Exchange pause | trading paused (weekly Thursday 03:00-05:00 ET, maintenance, ad hoc) | the runner polls `GET /exchange/status` (shard 2's entry) every 10 s, reads the schedule hourly and treats pause-like place rejects the same way: new orders blocked, strategy told `kalshi.reconcile` stale (quotes pulled; 60 s ahead of a scheduled closure), re-read of fills / positions / orders before resuming; `cancel_order_on_pause` on every order is the only protection during an EXCHANGE pause (cancels rejected too); lifecycle `deactivated` -> cancel and no quoting until reactivated |
| Global cancel-all tail | Kalshi may also cancel orders placed in the minute after a cancel-all | every terminal kill first TRIGGERS the order group(s) (scoped, no documented tail), then `DELETE /portfolio/events/orders?subaccount=1`; new orders held 60 s after any global cancel-all (start-up, kill, watchdog); strategy CancelAll uses batch cancels of our own orders, never the global endpoint |
| Process crash | strategy dies with resting orders | watchdog (separate process and restricted key, locked to the runner it armed on) triggers the runner's order groups then cancels subaccount 1's orders when the heartbeat is > 2 s stale; 120 s order expiry; order-group limits; restart cancels leftovers first |
| Another system on the same account | System 2 trades subaccount 0 with its own key | System 1 is on subaccount 1 with keys restricted to it; every write names subaccount 1 (the REST client refuses anything else before sending), own-channel events of other subaccounts / series are dropped, collateral is checked per shard |
| Restart during the day | a new process would forget today's losses and halts | persisted risk state (realized part and mark kept apart) plus the day P&L re-derived from REST fills and settlements, with open positions at exchange prices, seed the strategy (`RiskStateSeed`, first event; re-sent in-session when an excluded event settles or a watchdog cancel-all names this runner). A daily-loss halt holds for its own UTC day; other halts keep their scope across restarts; `--reset-daily-halt` starts a fresh loss budget and keeps the real P&L on record |
| Clock drift | local clock off or stepped | monotonic receive clock anchored at start; chrony offset recorded (macOS: query-only `sntp`, ~35-50 ms behind NTP measured); alarm > `clock_alarm_ms`; a persistent offset > 250 ms (or an untrusted sample) blocks new orders and sends `runner.clock` stale |
