"""Synthetic market generator for known-answer tests (KATs) of the full pipeline.

NOT evidence of edge. Its purpose is to verify that the backtester, fill simulator, P&L
attribution and experiment scripts recover properties we inject on purpose:
  * a known Kalshi staleness (background makers price off BTC lagged by `mm_lag_s`),
  * a known favorite-longshot bias (retail buys cheap YES above fair),
  * known informed flow (latency takers pick off quotes staler than a threshold),
  * a known settlement rule (60 once-per-second benchmark prints in (T-60s, T]).

Everything is seeded and produced in receive-time order as dh.core.events.
"""

from __future__ import annotations

import heapq
import math
from dataclasses import dataclass, field

import numpy as np
from scipy.special import ndtr

from dh.core.events import (
    ExtBBO,
    ExtTrade,
    FeedStatus,
    IndexTick,
    KalshiBookDelta,
    KalshiBookSnapshot,
    KalshiMarketLifecycle,
    KalshiTrade,
    Settlement,
)
from dh.core.market import MarketSpec, PriceRange
from dh.core.units import NS_PER_MS, NS_PER_S, PX_SCALE, QTY_SCALE

SEC_YR = 365.0 * 24 * 3600


@dataclass(frozen=True)
class SynthConfig:
    seed: int = 1
    expiration_ns: int = 1_790_300_000 * NS_PER_S  # T (aligned to a whole second)
    duration_s: float = 3600.0  # simulate [T - duration, T]
    step_ms: int = 100
    S0: float = 84_000.0
    vol_ann: float = 0.35
    jump_rate_per_hour: float = 0.0
    jump_sd_frac: float = 0.003
    venues: tuple[str, ...] = ("coinbase", "kraken", "bitstamp")
    venue_half_spread: float = 0.5
    venue_noise_sd: float = 1.0
    venue_update_prob: float = 0.6
    venue_delay_ms: int = 30
    brti_noise_sd: float = 0.3
    brti_delay_ms: int = 250
    strike_step: float = 250.0
    n_strikes_each_side: int = 6
    tick_px: int = 100
    mm_lag_s: float = 1.5  # background makers' information lag (injected staleness)
    mm_half_spread_ticks: int = 1
    mm_update_prob: float = 0.15  # per market per step
    mm_levels: int = 4
    mm_size_mean: float = 40.0
    noise_taker_rate_per_s: float = 0.04  # taker orders per second per market
    taker_size_mean: float = 12.0
    taker_size_cv: float = 1.5
    informed: bool = True
    informed_edge_ticks: float = 2.0  # pick off quotes staler than this many ticks beyond fee
    informed_max_size: float = 30.0
    longshot_bias: float = 0.0  # prob. a noise taker on a cheap (<15c) YES buys regardless of side draw
    kalshi_md_delay_ms: int = 20
    fee_taker_rate: float = 0.07


@dataclass
class _MktState:
    spec: MarketSpec
    yes_bids: dict[int, int] = field(default_factory=dict)  # px -> qty (YES scale)
    no_bids: dict[int, int] = field(default_factory=dict)  # px -> qty (NO scale)


def _digital(S: float, K: float, sig_abs_s: float, secs_left: float) -> float:
    var_t = max(secs_left - 59.0, 0.0) + 19.5025 if secs_left > 60 else _window_var(secs_left)
    sd = sig_abs_s * math.sqrt(max(var_t, 1e-9))
    return float(ndtr((S - K) / sd))


def _window_var(secs_left: float) -> float:
    # inside the window: remaining m prints, first unfixed print in (secs_left mod 1) seconds
    m = max(int(math.ceil(secs_left)), 1)
    tau_first = secs_left - (m - 1)
    return tau_first + ((m + 1) * (2 * m + 1) / (6 * m) - 1)


class SyntheticMarket:
    def __init__(self, cfg: SynthConfig) -> None:
        self.cfg = cfg
        self.rng = np.random.default_rng(cfg.seed)
        self.sid = 1  # orderbook_delta subscription
        self.seq = 0
        self.trade_sid = 2  # trade channel: its own sid and sequence counter (as on Kalshi)
        self.trade_seq = 0
        self.markets: dict[str, _MktState] = {}
        self.true_price: list[tuple[int, float]] = []  # (ns, price) exchange time
        self.brti_prints: dict[int, float] = {}  # second (ns) -> value
        self.fv_true: dict[str, list[tuple[int, float]]] = {}

    # ------------------------------------------------------------------ helpers
    def specs(self) -> list[MarketSpec]:
        return [m.spec for m in self.markets.values()]

    def _make_markets(self, S: float) -> None:
        c = self.cfg
        atm = round(S / c.strike_step) * c.strike_step
        T = c.expiration_ns
        for j in range(-c.n_strikes_each_side, c.n_strikes_each_side + 1):
            K = atm + j * c.strike_step
            t = f"SYN-{int(K)}"
            spec = MarketSpec(
                ticker=t, event_ticker="SYN-E", series_ticker="KXBTCD", strike_type="greater",
                floor_strike=K, cap_strike=None, open_ts=T - int(c.duration_s * NS_PER_S) - NS_PER_S,
                close_ts=T, expiration_ts=T, price_ranges=(PriceRange(100, 9900, c.tick_px),),
                fee_type="quadratic_with_maker_fees", fee_multiplier=1.0,
            )
            self.markets[t] = _MktState(spec)
            self.fv_true[t] = []

    def _next_seq(self) -> int:
        self.seq += 1
        return self.seq

    def _mm_book(self, fv: float) -> tuple[dict[int, int], dict[int, int]]:
        c = self.cfg
        tick = c.tick_px
        fv_px = fv * PX_SCALE
        bid = int(math.floor((fv_px - c.mm_half_spread_ticks * tick) / tick) * tick)
        ask = int(math.ceil((fv_px + c.mm_half_spread_ticks * tick) / tick) * tick)
        if ask - bid < tick:
            ask = bid + tick
        yes_bids: dict[int, int] = {}
        no_bids: dict[int, int] = {}
        for lvl in range(c.mm_levels):
            b = bid - lvl * tick
            a = ask + lvl * tick
            if 100 <= b <= 9900:
                yes_bids[b] = int(max(1, self.rng.exponential(c.mm_size_mean))) * QTY_SCALE
            if 100 <= a <= 9900:
                no_bids[PX_SCALE - a] = int(max(1, self.rng.exponential(c.mm_size_mean))) * QTY_SCALE
        return yes_bids, no_bids

    def _diff(self, ts: int, t: str, side: str, old: dict[int, int], new: dict[int, int]) -> list:
        out = []
        for px in sorted(set(old) | set(new)):
            d = new.get(px, 0) - old.get(px, 0)
            if d:
                out.append(KalshiBookDelta(ts=ts, ts_exch=ts - self.cfg.kalshi_md_delay_ms * NS_PER_MS,
                                           ticker=t, sid=self.sid, seq=self._next_seq(), side=side,  # type: ignore[arg-type]
                                           px=px, delta=d))
        return out

    def _take(self, ts: int, st: _MktState, taker_yes: bool, size_ct: float) -> list:
        """Taker buys YES (lifts YES asks = NO bids) or sells YES (hits YES bids)."""
        out = []
        left = int(round(size_ct)) * QTY_SCALE
        book = st.no_bids if taker_yes else st.yes_bids
        for px in sorted(book, reverse=True):  # best NO bid = best YES ask; best YES bid
            if left <= 0:
                break
            q = book[px]
            take = min(q, left)
            left -= take
            yes_px = PX_SCALE - px if taker_yes else px
            ex = ts - self.cfg.kalshi_md_delay_ms * NS_PER_MS
            self.trade_seq += 1
            out.append(KalshiTrade(ts=ts, ts_exch=ex, ticker=st.spec.ticker,
                                   trade_id=f"T{self.trade_seq}", yes_px=yes_px, qty=take,
                                   taker_side="yes" if taker_yes else "no", sid=self.trade_sid,
                                   seq=self.trade_seq))
            out.append(KalshiBookDelta(ts=ts, ts_exch=ex, ticker=st.spec.ticker, sid=self.sid,
                                       seq=self._next_seq(), side="no" if taker_yes else "yes",
                                       px=px, delta=-take))
            if take == q:
                del book[px]
            else:
                book[px] = q - take
        return out

    # ------------------------------------------------------------------ main
    def generate(self) -> list:
        c = self.cfg
        rng = self.rng
        T = c.expiration_ns
        t0 = T - int(c.duration_s * NS_PER_S)
        step = c.step_ms * NS_PER_MS
        n = int(c.duration_s * 1000 / c.step_ms) + 1
        dt = c.step_ms / 1000.0
        sig_step = c.vol_ann * math.sqrt(dt / SEC_YR)
        # true log-price path (exchange time) with optional jumps
        z = rng.standard_normal(n)
        logp = np.empty(n)
        logp[0] = math.log(c.S0)
        jump_p = c.jump_rate_per_hour * dt / 3600.0
        for i in range(1, n):
            j = rng.normal(0, c.jump_sd_frac) if rng.random() < jump_p else 0.0
            logp[i] = logp[i - 1] + sig_step * z[i] - 0.5 * sig_step**2 + j
        price = np.exp(logp)
        times = t0 + np.arange(n) * step
        self.true_price = list(zip(times.tolist(), price.tolist()))
        self._make_markets(price[0])
        sig_abs = lambda S: S * c.vol_ann / math.sqrt(SEC_YR)  # noqa: E731
        lag_steps = int(round(c.mm_lag_s / dt))
        heap: list[tuple[int, int, object]] = []
        k = 0

        def push(ev) -> None:
            nonlocal k
            heapq.heappush(heap, (ev.ts, k, ev))
            k += 1

        push(FeedStatus(ts=t0, ts_exch=0, stream="kalshi.ws", status="connected"))
        # initial snapshots from makers' view
        for t, st in self.markets.items():
            fv = _digital(price[0], st.spec.floor_strike, sig_abs(price[0]), (T - t0) / NS_PER_S)
            st.yes_bids, st.no_bids = self._mm_book(fv)
            push(KalshiBookSnapshot(ts=t0 + NS_PER_MS, ts_exch=t0, ticker=t, sid=self.sid, seq=self._next_seq(),
                                    yes_bids=tuple(sorted(st.yes_bids.items())),
                                    no_bids=tuple(sorted(st.no_bids.items()))))
        tk = 0
        for i in range(n):
            ts_ex = int(times[i])
            S = float(price[i])
            secs_left = (T - ts_ex) / NS_PER_S
            # external venues
            for v in c.venues:
                if rng.random() < c.venue_update_prob:
                    mid = S + rng.normal(0, c.venue_noise_sd)
                    push(ExtBBO(ts=ts_ex + c.venue_delay_ms * NS_PER_MS, ts_exch=ts_ex, venue=v, symbol="BTC-USD",
                                bid=mid - c.venue_half_spread, bid_size=1.0, ask=mid + c.venue_half_spread,
                                ask_size=1.0))
                if rng.random() < 0.05:
                    tk += 1
                    push(ExtTrade(ts=ts_ex + c.venue_delay_ms * NS_PER_MS, ts_exch=ts_ex, venue=v, symbol="BTC-USD",
                                  price=S, size=float(rng.exponential(0.05)),
                                  aggressor="buy" if rng.random() < 0.5 else "sell", trade_id=f"x{tk}"))
            # benchmark: 5 Hz and 1 Hz prints (value = true + small noise), published with delay
            if (ts_ex - t0) % (200 * NS_PER_MS) == 0:
                val = S + rng.normal(0, c.brti_noise_sd)
                push(IndexTick(ts=ts_ex + c.brti_delay_ms * NS_PER_MS, ts_exch=ts_ex, index_id="BRTI", value=val,
                               feed="5hz"))
                if ts_ex % NS_PER_S == 0:
                    self.brti_prints[ts_ex] = val
                    push(IndexTick(ts=ts_ex + c.brti_delay_ms * NS_PER_MS + NS_PER_MS, ts_exch=ts_ex,
                                   index_id="BRTI", value=val, feed="1hz"))
            if secs_left <= 0:
                break
            recv = ts_ex + c.kalshi_md_delay_ms * NS_PER_MS
            S_lag = float(price[max(0, i - lag_steps)])
            for t, st in self.markets.items():
                K = st.spec.floor_strike
                fv_true = self._fv_given_prints(S, K, sig_abs(S), ts_ex)
                if i % 10 == 0:
                    self.fv_true[t].append((ts_ex, fv_true))
                # background makers refresh from lagged information
                if rng.random() < c.mm_update_prob:
                    fv_mm = self._fv_given_prints(S_lag, K, sig_abs(S_lag), ts_ex)
                    nb, na = self._mm_book(fv_mm)
                    evs = self._diff(recv, t, "yes", st.yes_bids, nb) + self._diff(recv, t, "no", st.no_bids, na)
                    # cancels before new orders, so the book is never transiently crossed
                    base_seq = min((e.seq for e in evs), default=0)
                    evs.sort(key=lambda e: (e.delta > 0, e.seq))
                    for j, ev in enumerate(evs):
                        push(KalshiBookDelta(ts=ev.ts, ts_exch=ev.ts_exch, ticker=ev.ticker, sid=ev.sid,
                                             seq=base_seq + j, side=ev.side, px=ev.px, delta=ev.delta))
                    st.yes_bids, st.no_bids = nb, na
                # informed takers: pick off quotes stale beyond threshold (incl. taker fee)
                if c.informed and st.no_bids and st.yes_bids:
                    best_ask = PX_SCALE - max(st.no_bids)
                    best_bid = max(st.yes_bids)
                    thr = c.informed_edge_ticks * c.tick_px / PX_SCALE
                    fee = c.fee_taker_rate * fv_true * (1 - fv_true)
                    if fv_true - best_ask / PX_SCALE > thr + fee:
                        for ev in self._take(recv, st, True, min(c.informed_max_size, st.no_bids[max(st.no_bids)] / QTY_SCALE)):
                            push(ev)
                    elif best_bid / PX_SCALE - fv_true > thr + fee:
                        for ev in self._take(recv, st, False, min(c.informed_max_size, st.yes_bids[best_bid] / QTY_SCALE)):
                            push(ev)
                # noise takers
                if rng.random() < c.noise_taker_rate_per_s * dt:
                    mu = math.log(c.taker_size_mean) - 0.5 * math.log(1 + c.taker_size_cv**2)
                    size = max(1.0, round(rng.lognormal(mu, math.sqrt(math.log(1 + c.taker_size_cv**2)))))
                    buy_yes = rng.random() < 0.5
                    if c.longshot_bias > 0 and st.no_bids and (PX_SCALE - max(st.no_bids)) < 1500:
                        if rng.random() < c.longshot_bias:
                            buy_yes = True
                    for ev in self._take(recv, st, buy_yes, size):
                        push(ev)
        # settlement
        window = [T - (60 - kk) * NS_PER_S for kk in range(1, 61)]
        vals = [self.brti_prints[w] for w in window if w in self.brti_prints]
        A = float(np.mean(vals))
        self.settlement_value = A
        for t, st in self.markets.items():
            yes = st.spec.yes_wins(A)
            push(KalshiMarketLifecycle(ts=T + 2 * NS_PER_S, ts_exch=T, ticker=t, event_type="determined",
                                       result="yes" if yes else "no", settlement_value="1.0000" if yes else "0.0000"))
            push(Settlement(ts=T + 2 * NS_PER_S, ts_exch=T, ticker=t, result="yes" if yes else "no",
                            expiration_value=A, settlement_px=PX_SCALE if yes else 0))
        return [e for _, _, e in sorted(heap, key=lambda x: (x[0], x[1]))]

    def _fv_given_prints(self, S: float, K: float, sig_abs_s: float, ts_ex: int) -> float:
        """True fair value given prints already fixed in the window (exact rule)."""
        T = self.cfg.expiration_ns
        secs_left = (T - ts_ex) / NS_PER_S
        if secs_left >= 60:
            return _digital(S, K, sig_abs_s, secs_left)
        window = [T - (60 - kk) * NS_PER_S for kk in range(1, 61)]
        fixed = [self.brti_prints[w] for w in window if w <= ts_ex and w in self.brti_prints]
        kf = len(fixed)
        m = 60 - kf
        if m <= 0:
            return 1.0 if sum(fixed) / 60 > K else 0.0
        req = (K * 60 - sum(fixed)) / m
        next_obs = min(w for w in window if w > ts_ex)
        tau_first = (next_obs - ts_ex) / NS_PER_S
        var_t = tau_first + ((m + 1) * (2 * m + 1) / (6 * m) - 1)
        return float(ndtr((S - req) / (sig_abs_s * math.sqrt(var_t))))
