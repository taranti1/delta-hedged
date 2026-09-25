"""End-to-end known-answer harness: MarketMaker + KalshiExchangeSim on a synthetic market.

Used by tests/backtest/test_kat_end_to_end.py and for quick what-if runs. The synthetic
market is NOT evidence of edge; it verifies that the full pipeline (strategy, simulator,
ledger) is internally consistent and reacts to injected effects in the right direction.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, replace

import numpy as np

from dh.backtest.ledger import Ledger
from dh.backtest.runner import RunResult, run
from dh.core.units import NS_PER_S
from dh.execution.exchange_sim import KalshiExchangeSim
from dh.execution.latency import LatencyModel
from dh.kalshi.fees import FeeEngine, OrderFeeAccumulator
from dh.models.fvmodel import FairValueModel, load_recommended_config
from dh.sim.synthetic import SynthConfig, SyntheticMarket
from dh.strategy.config import StrategyConfig
from dh.strategy.mm import MarketMaker

SEC_YR = 365.0 * 24 * 3600


def warm_fv_model(fv: FairValueModel, end_ns: int, S_end: float, vol_ann: float, days: float = 2.0,
                  step_s: int = 60, seed: int = 0) -> None:
    """Feed a seeded GBM history ending at (end_ns, S_end) so every EWMA is warm."""
    rng = np.random.default_rng(seed)
    n = int(days * 86400 / step_s)
    sig = vol_ann * math.sqrt(step_s / SEC_YR)
    steps = rng.standard_normal(n) * sig
    logp = math.log(S_end) - np.concatenate([[0.0], np.cumsum(steps[::-1])])[::-1][:-1]
    for i, lp in enumerate(logp):
        fv.update(end_ns - (n - i) * step_s * NS_PER_S, float(math.exp(lp)))


@dataclass
class KatResult:
    run: RunResult
    summary: dict
    mm: MarketMaker
    ledger: Ledger
    sim: KalshiExchangeSim
    settlement_value: float


def run_synthetic(synth: SynthConfig, cfg: StrategyConfig, *, policy: str = "realistic",
                  latency: LatencyModel | None = None, seed: int = 1, warm_seed: int = 0) -> KatResult:
    sm = SyntheticMarket(synth)
    events = sm.generate()
    specs = sm.specs()
    fv = FairValueModel.from_config(load_recommended_config())
    warm_fv_model(fv, events[0].ts, synth.S0, synth.vol_ann, seed=warm_seed)
    fee_engine = FeeEngine.from_config()
    mm = MarketMaker(cfg, specs, fv_model=fv, fee_engine=fee_engine)
    sched = {s.ticker: fee_engine.schedule_for_spec(s.fee_type, s.fee_multiplier) for s in specs}
    one = next(iter(sched.values()))

    def fee_fn(px: int, qty: int, is_taker: bool) -> int:
        return one.trade_fee_micros(px, qty, is_taker)

    accs: dict[str, OrderFeeAccumulator] = {}

    def order_fee_fn(order_key: str, book_side: str, px: int, qty: int, is_taker: bool) -> int:
        acc = accs.get(order_key)
        if acc is None:
            acc = accs[order_key] = OrderFeeAccumulator(one, book_side)
        return acc.apply_fill(px, qty, is_taker).net_micros

    sim = KalshiExchangeSim(latency or LatencyModel.fixed(submit_ms=40, response_ms=40, ws_ms=25), policy, fee_fn,
                            seed=seed, order_fee_fn=order_fee_fn)
    for s in specs:
        sim.register_market(s)
    ledger = Ledger({s.ticker: s.event_ticker for s in specs}, {s.ticker: s.expiration_ts for s in specs})

    def on_ev(ev) -> None:
        ledger.on_event(ev)

    def on_action(ts, a) -> None:
        from dh.core.actions import Log

        if isinstance(a, Log):
            ledger.on_log(ts, a)

    res = run(events, mm, sim, timer_period_ns=cfg.timers.quote_period_ms * 1_000_000, on_event=on_ev,
              on_action=on_action)
    summary = ledger.summary()
    return KatResult(res, summary, mm, ledger, sim, sm.settlement_value)


def default_kat_config(**over) -> StrategyConfig:
    cfg = StrategyConfig()
    q = replace(cfg.quoting, enabled_series=("KXBTCD",), clip_contracts=5.0, price_floor_px=100, price_cap_px=9900,
                requote_min_interval_ms=500)
    r = replace(cfg.risk, stale_ext_s=5.0)
    t = replace(cfg.timers, quote_period_ms=500)
    return replace(cfg, quoting=q, risk=r, timers=t, **over)
