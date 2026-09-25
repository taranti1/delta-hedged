"""Small MarketMaker driver for scenario tests: no simulator, the test plays the exchange."""
from __future__ import annotations

from dh.backtest.kat import default_kat_config, warm_fv_model
from dh.core.actions import Log
from dh.core.events import ExtBBO, IndexTick, Timer
from dh.core.market import MarketSpec, PriceRange
from dh.core.units import NS_PER_MS, NS_PER_S
from dh.kalshi.fees import FeeEngine
from dh.models.fvmodel import FairValueModel, load_recommended_config
from dh.strategy.mm import MarketMaker

T0 = 1_790_300_000 * NS_PER_S
EXP = T0 + 3000 * NS_PER_S
TICK = "KXBTCD-TEST-T84000"


def spec(ticker=TICK, K=84000.0, strike_type="greater", exp=EXP, cap=None, event="KXBTCD-TEST",
         series="KXBTCD") -> MarketSpec:
    return MarketSpec(ticker=ticker, event_ticker=event, series_ticker=series, strike_type=strike_type,
                      floor_strike=K if strike_type != "less" else None, cap_strike=cap,
                      open_ts=T0 - 3600 * NS_PER_S, close_ts=exp, expiration_ts=exp,
                      price_ranges=(PriceRange(100, 9900, 100),), fee_type="quadratic_with_maker_fees",
                      fee_multiplier=1.0)


class Driver:
    def __init__(self, specs, cfg=None, S0: float = 84000.0) -> None:
        self.cfg = cfg or default_kat_config()
        fv = FairValueModel.from_config(load_recommended_config())
        warm_fv_model(fv, T0, S0, 0.35)
        self.mm = MarketMaker(self.cfg, specs, fv_model=fv, fee_engine=FeeEngine.from_config())
        self.now = T0
        self.S = S0

    def feed(self, ev) -> list:
        return [a for a in self.mm.on_event(ev) if not isinstance(a, Log)]

    def inputs(self, ts: int, brti_src_lag_ns: int = 0) -> list:
        out = []
        for v in ("coinbase", "kraken", "bitstamp"):
            out += self.feed(ExtBBO(ts, ts, v, "BTC-USD", self.S - 0.5, 1.0, self.S + 0.5, 1.0))
        out += self.feed(IndexTick(ts, ts - brti_src_lag_ns, "BRTI", self.S, "5hz"))
        return out

    def advance(self, until: int, step_ms: int = 200, brti_src_lag_ns: int = 0) -> list:
        out = []
        while self.now + step_ms * NS_PER_MS <= until:
            self.now += step_ms * NS_PER_MS
            out += self.inputs(self.now, brti_src_lag_ns)
            out += self.feed(Timer(self.now))
        return out
