# XVIII. Deterministic trade lifecycle and race conditions

## The loop (single-threaded strategy; I/O in adapters)

| # | Step | Where | Deterministic rule |
|---|---|---|---|
| 1 | Ingest Kalshi + external data | adapters -> events | every frame stamped with receive ns and recorded before parsing |
| 2 | Validate feeds | `RiskEngine.health` | Kalshi books valid (no gap, 5 s since resync), BRTI age <= 3 s (near expiry) / 10 s (all), >= 2 fresh external venues |
| 3 | Settlement state | `SettlementTracker` | fixed prints in (T-60, T], required remaining average per strike |
| 4 | Fair probability | `digital` + band | F, F_lo, F_hi from nowcast S, sigma forecast, window state, tail model |
| 5 | Delta / gamma | same call | analytic greeks of the averaging-window digital |
| 6 | Fill probability | `FillIntensityModel` | intensity from segment flow and queue-ahead |
| 7 | Toxicity | `AdverseSelectionModel` | expected markout given recent FV move, tau, queue position |
| 8 | Candidate EV | `quoting.evaluate` | v = edge - AS - fee - hedge - inventory; EVrate = intensity x v |
| 9 | Risk penalties | `scenario` grid | marginal variance/CVaR charge; hard limits via `RiskEngine` |
| 10 | Rank | `mm` | score = EVrate / collateral, greedy admission under limits and write budget |
| 11 | Submit passive orders | adapter | post-only V2 orders, `cancel_order_on_pause`, order group, optional expiry |
| 12 | Monitor queue / external BTC | events + `queue_positions` poll | queue estimator; requote timer 200 ms; immediate requote on BTC move > 0.25 sigma(1s) |
| 13 | Cancel/reprice when EV < 0 | `decide_side` | cancel if v < 0; replace only if EVrate gain > kappa |
| 14 | Process fills | `OrderManager` | dedupe by trade_id; position, cost basis, fee reconciliation |
| 15 | Portfolio delta | `mm` | D = sum q_i Delta_i + H |
| 16 | Hedge trigger | `decide_hedge` | \|D\| > band -> trade to band edge |
| 17 | Execute hedge | adapter | post-only at touch, IOC if urgent |
| 18 | Residual exposure | risk report | CVaR/worst-case per event; limits |
| 19 | Settle | lifecycle/settlement events | payout, unwind event's hedge, settlement-loss check |
| 20 | Reconcile | ledger | exchange positions/fills/fees vs internal ledger; mismatch halts |

## Race conditions and their deterministic handling

| Race | What happens | Handling |
|---|---|---|
| Fill arrives before the create ack | WS `fill` / `user_order` beats the REST response | Order manager buffers by client_order_id; the ack later attaches the order_id; position updated at fill time |
| Fill during pending cancel | cancel in flight, order matched first | fill counts; worst-case exposure includes pending-cancel qty until CancelAck |
| Cancel rejected (already filled / not found) | REST 404 / error after full fill | state -> FILLED from fills; if fills are missing, reconcile via `GET /portfolio/orders/{id}` |
| Create timeout (unknown outcome) | no response | mark UNKNOWN; count as live for risk; query `GET /portfolio/orders?...` by client_order_id; never resubmit blindly |
| Duplicate / out-of-order WS messages | reconnect replays, seq gaps | Kalshi seq per sid; dedupe fills by trade_id; order updates applied only if fill/remaining are monotone |
| Book gap during quoting | missed delta | FeedStatus(gap) -> CancelAll for affected markets -> resnapshot -> 5 s settle -> resume |
| Stale quote vs external jump | BTC moves while our order rests | immediate requote trigger; order-group limit caps burst fills; toxicity model widens or pulls |
| Amend priority | amend may reset queue position | M1 uses cancel/replace; Experiment 4 measures amend semantics via `queue_positions` |
| Hedge venue rejects or partially fills | margin / connectivity | hedge engine sees the real position only from HedgeFill; retries with backoff; if the venue is down, quoting stops on sides that increase \|D\| |
| Settlement while orders rest | market closes at T | orders auto-canceled at close; strategy stops quoting near-strike markets at T-90 s |
| Exchange pause | trading paused | `cancel_order_on_pause`; lifecycle `deactivated` -> no quoting until reactivated plus a resync |
| Process crash | strategy dies with resting orders | watchdog cancel-all; order expiries; order-group limits |
| Clock drift | local clock off | chrony offset recorded; alarm > 5 ms; features use receive time consistently |
