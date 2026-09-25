# Module contracts (build-time source of truth for all contributors)

`dh/core/*` is frozen shared contract: units, events, actions, market specs, books, the
Strategy protocol. Change it only through the orchestrator. Every other package owns its
directory and exposes the functions below. Imports flow downward only (see `dh/__init__.py`).
`dh.research` may import anything; nothing imports `dh.research`.

Hard rules for all code:
* Deterministic: no wall clock inside strategy, models or simulators (take `now_ns`
  arguments); seeded RNGs only (`numpy.random.default_rng(seed)`).
* Exchange/ledger quantities are ints (`dh.core.units`). Models use floats.
* Every public function has a docstring stating units. Every module has tests in `tests/<pkg>/`.
* No network in tests. Network-dependent code is exercised with spec-shaped fixtures, plus
  `scripts/smoke_*.py` for manual live checks (marked `@pytest.mark.network` if under pytest).

---------------------------------------------------------------------------------------------
## dh.kalshi — exchange adapter (owner: Kalshi builder)

```python
# auth.py
class KalshiSigner:
    def __init__(self, key_id: str, private_key_pem: bytes | str | Path): ...
    def headers(self, method: str, path: str, now_ms: int | None = None) -> dict[str, str]
        # path = full API path e.g. '/trade-api/v2/portfolio/events/orders' (no query string)

# normalize.py  — pure: raw WS/REST JSON -> normalized events (dh.core.events)
def ws_message_to_events(msg: dict, recv_ns: int) -> list[Event]
def rest_orderbook_to_snapshot(ticker: str, body: dict, recv_ns: int) -> KalshiBookSnapshot
def rest_trade_to_event(row: dict, recv_ns: int) -> KalshiTrade
def rest_market_to_spec(market: dict, series: dict | None, event: dict | None) -> MarketSpec

# fees.py — exact, config-driven (config/fees.yaml), dynamic fee type resolution
class FeeEngine:
    def __init__(self, rates: FeeRates): ...
    def schedule_for(self, series: dict, event: dict | None, market: dict | None) -> FeeSchedule
class FeeSchedule:
    fee_type: str; multiplier: Decimal; source: str
    def trade_fee_micros(self, px: int, qty: int, is_taker: bool) -> int   # unrounded fee ceil'd to micro$
    def expected_fee_per_contract(self, px: int, is_taker: bool) -> float  # dollars, for models
class OrderFeeAccumulator:  # per-order balance rounding + rebate carry, per Kalshi fee-rounding rules
    def apply_fill(self, px: int, qty: int, is_taker: bool) -> FeeBreakdown  # micros: trade, rounding, rebate, net
def reconcile_fill_fee(expected_micros: int, reported_fee_cost: str) -> FeeCheck

# rest.py (async, aiohttp), ws.py (async, websockets), rate_limit.py (token buckets)
class KalshiRest: ...   # all endpoints used by collectors, live runner, research downloaders
class KalshiWS:
    async def run(self, emit: Callable[[Event], None]) -> None   # reconnects; emits FeedStatus
    # seq gap per sid -> FeedStatus('gap') + update_subscription(get_snapshot) resync
```

## dh.feeds + dh.store — external data and capture (owner: feeds builder)

```python
# feeds/base.py
class FeedClient(ABC):
    name: str                                  # stream id, e.g. 'coinbase.ws'
    async def run(self, emit_raw: Callable[[str, int, bytes], None]) -> None
        # emit_raw(stream, recv_ns, raw_frame_bytes); reconnect w/ backoff; staleness watchdog
    @staticmethod
    def normalize(raw: bytes, recv_ns: int, state: NormalizerState) -> list[Event]  # pure
# store/recorder.py  — append-only raw capture, zstd segments per stream per hour
class Recorder:
    def write(self, stream: str, recv_ns: int, raw: bytes) -> None
# store/replay.py
def iter_raw(root: Path, streams: list[str], t0_ns: int, t1_ns: int) -> Iterator[RawRecord]
def iter_events(root: Path, streams: list[str], t0_ns: int, t1_ns: int) -> Iterator[Event]
    # k-way merge ordered by (recv_ns, stream rank, record seq); normalizes on the fly
```

## dh.models + dh.settlement — pricing (owner: models builder)

```python
# settlement/window.py
@dataclass
class WindowState:
    n_obs: int; k_fixed: int; sum_fixed: float; m_remaining: int
    tau_first_s: float          # seconds from now to the first *unfixed* observation (>= 0)
    step_s: float
class SettlementTracker:            # fed IndexTick events; knows each expiration's window
    def on_index(self, tick: IndexTick) -> None
    def window_state(self, spec: SettlementSpec, expiration_ns: int, now_ns: int) -> WindowState
    def required_remaining_avg(self, K: float, ws: WindowState) -> float  # (K*n - sum_fixed)/m

# models/fairvalue.py
def remaining_avg_variance_time(ws: WindowState) -> float
    # Var[(1/m) sum_j W(tau_j)] in seconds for tau_j = tau_first + (j-1)*step
    # = tau_first + step * ((m+1)(2m+1)/(6m) - 1)
def digital(spec: MarketSpec, ws: WindowState, spot: float, sigma_abs: float,
            tail: TailModel, drift_abs: float = 0.0) -> Digital
    # spot: current nowcast of the benchmark ($); sigma_abs: $ per sqrt(second)
    # returns Digital(p_yes, delta, gamma, sd_remaining, z)
    #   delta = dP/dspot (per $; == BTC hedge qty per YES contract), gamma = d2P/dspot2
class TailModel: 'gauss' | StudentT(nu) | VolMixture(cv)   # calibrated in research

# models/vol.py
class EwmaVol:            # streaming; returns sigma in log-return units per sqrt(second)
    def __init__(self, half_life_s: float, min_dt_s: float = 1.0): ...
    def update(self, ts_ns: int, price: float) -> None
    def sigma(self, now_ns: int) -> float
class SeasonalVol:        # multiplicative hour-of-week profile fitted offline
    def factor(self, ts_ns: int) -> float
def blended_sigma(...)    # combination chosen by research (docs/research/*)

# models/calibration.py
def brier(p, y) -> float; def log_loss(p, y, eps=1e-6) -> float
def reliability(p, y, bins) -> DataFrame; def ece(p, y, bins) -> float
```

## dh.execution — orders, queues, simulated exchange (owner: execution builder)

```python
# order_manager.py — shared by live runner and backtest (inside the Strategy)
class OrderManager:
    def request_place(self, a: PlaceOrder, now_ns: int) -> None
    def request_cancel(self, a: CancelOrder, now_ns: int) -> None
    def on_event(self, ev: Event) -> list[OrderEvent]   # acks/rejects/fills/updates -> state
    def working(self, ticker: str | None = None) -> list[WorkingOrder]
    def position(self, ticker: str) -> int              # signed YES qty from fills
# queue.py
class QueueEstimator:     # per our resting order: contracts ahead at our price
    policy: 'optimistic' | 'realistic' | 'conservative'
    def on_book_delta / on_trade / on_own_order ...
# exchange_sim.py
class KalshiExchangeSim:
    def __init__(self, latency: LatencyModel, fill_policy: str, fee_fn, seed: int): ...
    def on_market_event(self, ev: Event) -> list[Event]    # may generate our fills
    def submit(self, action: Action, decision_ns: int) -> None
    def pop_due(self, until_ns: int) -> list[Event]        # acks/fills scheduled by latency
    def next_due_ns(self) -> int | None
# hedge_sim.py
class HedgeVenueSim: ...  # walks external L2 for marketable orders, maker fills on trade-through
```

## dh.strategy / dh.backtest / dh.live (Phase 2, built on the above)
* `dh.strategy.mm.MarketMaker(config, markets, ...)` implements `Strategy`.
* `dh.backtest.runner.run(events, strategy, exchange_sim, hedge_sim, timers) -> Ledger`.
* `dh.live.runner` wires real adapters to the identical `MarketMaker`.

## Runner-generated strategy inputs (live mode)

All are recorded on `events.live`, so a session replays bit for bit.

| Event | When | Strategy effect |
|---|---|---|
| `RiskStateSeed(day_start_ns, day_pnl_usd, halted, halt_reason, pause_until_ns, halt_scope)` | start-up (first event); again when an excluded event settles, or a watchdog cancel-all names this runner | the day's earlier P&L counts toward the daily-loss limit; a carried halt (with its scope) or pause is restored; repeats are safe |
| `FeedStatus("runner.lag", stale/resumed)` | consumer or exchange-time lag above / back under the limit | cancel all quotes and block quoting until resumed |
| `FeedStatus("kalshi.reconcile", stale/resynced)` | reconnect reconciliation, and the 60 s after a global cancel-all | same |
| `FeedStatus("runner.clock", stale/resumed)` | receive-clock offset cannot be measured or exceeds the limit | same |
