# C. System architecture (and XVII. implementation stack)

## Data flow

```
                       ┌────────────────────────── one host, one process per role ───────────────────────────┐
 Kalshi WS  ──frames──▶│ ingest (asyncio)  ──raw bytes + recv_ns──▶ Recorder (append-only zstd segments)       │
  book/trade/BRTI/fills│      │                                                                               │
 Venue WSs  ──frames──▶│      └─normalize (pure)─▶ Event bus ──▶ Strategy.on_event (single thread, deterministic)│
 Deribit/perps ───────▶│                                          │  ├ books / settlement tracker / vol / nowcast │
 REST snapshots ──────▶│                                          │  ├ fair value band + greeks                   │
                       │                                          │  ├ fill & toxicity models                     │
                       │                                          │  ├ scenario-grid risk + limits + kill switches│
                       │                                          │  ├ quote EV optimizer + order manager         │
                       │                                          │  └ hedge engine                               │
                       │                                          ▼                                               │
                       │                                 Actions ──▶ Venue adapters (async REST: Kalshi V2 orders, │
                       │                                            hedge venue) ──acks/rejects──▶ Event bus       │
                       │  Monitor: Prometheus metrics + JSON logs + kill-switch file watch + alerting             │
                       └──────────────────────────────────────────────────────────────────────────────────────────┘
 Research (separate process/machine): replay reader ──▶ same Strategy + simulated venues ──▶ ledger/attribution
```

Component boundaries map to packages: ingestion `dh.kalshi.ws` / `dh.feeds.*`; normalization
`dh.kalshi.normalize` / feed `normalize()`; storage `dh.store.recorder`; replay
`dh.store.replay`; features and fair value `dh.settlement` + `dh.models`; execution and fill
model `dh.execution` + `dh.strategy.fill_model`; quoting `dh.strategy.quoting`; inventory and
risk `dh.strategy.scenario` + `dh.strategy.risk`; hedge `dh.strategy.hedging`; order manager
`dh.execution.order_manager`; monitoring `dh.live.monitor`; backtesting `dh.backtest`.

## Determinism contract

* The Strategy is a pure function of (config, ordered events). Time = event receive time.
  Timers are synthesized every N ms of event time by the runner (live and replay).
* Live sessions record **every** inbound event, including our acks, fills and FeedStatus, so a
  live session replays bit-for-bit and any divergence between live decisions and replayed
  decisions is a bug (checked nightly: `scripts/replay_live_session.py`).
* Config is hashed into every log line; code version (git SHA) is logged at startup.

## Recommended minimal production stack

| Layer | Choice | Why |
|---|---|---|
| Live process | Python 3.11, asyncio, `websockets`, `aiohttp`, `orjson`, numpy | Hot path cost is 0.1-5 ms per decision vs 20-60 ms network round trip to Kalshi |
| Historical store | Append-only zstd JSONL raw segments per stream per hour, compacted nightly to Parquet | Lossless raw capture; parsers can be fixed and re-run over history |
| Feature store / research DB | Parquet + DuckDB (or Polars) | Zero-ops, columnar, fast enough for 100s of GB |
| Backtester | `dh.backtest.runner` replaying the store through the production Strategy + `KalshiExchangeSim` | Same code path as live |
| Monitoring | Prometheus client + Grafana; JSON logs to local disk + Loki (optional) | Standard, cheap |
| Alerting | Grafana alerts -> phone push; the kill switch is local and never depends on alerting | Safety must not depend on network |
| Deployment | Docker Compose on one VM in AWS us-east-1 (Kalshi and most venues' US endpoints), chrony with Amazon Time Sync | Simple, reproducible |
| Secrets | API key id + RSA private key file mounted read-only (never in git, never logged) | |
| Config | YAML in git, hashed into logs; changes only via reviewed commits | Deterministic |

## When is Python not enough? (quantified rule, not "HFT" folklore)

Measured budget per reaction (external move -> cancel request on the wire):
* network to Kalshi REST: typically 20-60 ms (measure with `scripts/smoke_kalshi.py`);
* benchmark relay latency (CF -> Kalshi -> us): measured from `received_at` vs `source_ts_ms`;
* Python decision path: about 0.1-1 ms for FV (vectorized, 20 strikes), about 1-10 ms for a
  full requote cycle with scenario-grid risk (optimizable by evaluating risk only for the top
  candidates).

A Rust/C++ hot path saves about 1-10 ms. It pays only if Experiments 1 and 3 show that a
material share of adverse selection comes from fills that arrive **within 10 ms of the moment
our cancel would have landed**. Formally: migrate the feed-to-cancel path when the estimated
avoidable adverse selection `sum(markout of toxic fills with t_fill - t_signal in
[L_py - 10ms, L_py]) / contracts > 0.1 c/contract`. Otherwise effort goes into better signals
(earlier external venues) and toxicity-aware quoting, which dominate a few ms of compute.

## Failure-mode architecture

* **Kalshi-side protections that work even if our process dies:** `cancel_order_on_pause`;
  order groups with a rolling 15-second contracts limit and auto-cancel (fill-burst breaker);
  optional `expiration_time` on quotes as a dead-man switch. A watchdog process holding its
  own API session calls `DELETE /portfolio/events/orders` (cancel all) if the strategy
  heartbeat stops for > 2 s.
* **Our-side protections:** kill switches in `dh.strategy.risk` (docs/MODELS.md section 6),
  position reconciliation against `GET /portfolio/positions` every 30 s, fee reconciliation on
  every fill, and a manual kill file (`touch /run/dh/KILL`) checked by the runner every loop.
