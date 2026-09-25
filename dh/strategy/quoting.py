"""Quote evaluation: expected net value rate of every candidate price on one market side.

    v(p)      = edge(p) - AS - fee(p) - hedge_cost - inv_charge        $ per filled contract
    EVrate(p) = intensity(p, Q_eff) * v(p)                              $ per second
    score     = EVrate / (collateral per contract * size)               ranking across markets

See docs/MODELS.md section 3. Pure functions of the provided context; no I/O, no clock.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable

import numpy as np

from dh.core.book import KalshiBook
from dh.core.market import MarketSpec
from dh.core.units import PX_SCALE, QTY_SCALE
from dh.strategy.fill_model import AdverseSelectionModel, FillIntensityModel, segment_key
from dh.strategy.scenario import EventGrid, marginal_risk_charge


@dataclass(frozen=True)
class ExistingOrder:
    client_order_id: str
    px: int
    remaining_qty: int  # 0.01-contract units
    queue_ahead: float  # contracts ahead of us at px (estimator)


@dataclass
class MarketQuoteContext:
    spec: MarketSpec
    book: KalshiBook
    F: float
    F_lo: float
    F_hi: float
    delta_btc: float  # dF/dS: BTC per YES contract
    tau_s: float
    z: float  # normalized distance of strike (for segmenting)
    maker_fee: Callable[[int], float]  # px -> expected maker fee $ per contract
    dF_recent: float  # fair-value change over the AS lookback ($ per contract, signed)
    grid: EventGrid
    base_pnl: np.ndarray  # event book P&L vector (positions only)
    payoff_k: np.ndarray  # this market's YES payoff on the grid
    lam: float
    lambda_tail: float
    tail_budget: float
    D_btc: float  # current portfolio delta (BTC)
    hedge_cost_frac: float  # all-in hedge cost as fraction of notional
    spot: float
    rho_hedged: float
    clip_contracts: float
    capacity_contracts: dict[str, float]  # side -> max additional contracts allowed by limits
    existing: dict[str, list[ExistingOrder]] = field(default_factory=dict)
    max_ticks_from_touch: int = 3
    price_floor_px: int = 100
    price_cap_px: int = 9900


@dataclass
class QuoteCandidate:
    ticker: str
    side: str  # 'bid' | 'ask'
    px: int
    size: float  # contracts
    position: str  # touch | improve | behind | empty
    q_eff: float
    edge: float
    adverse: float
    fee: float
    hedge_cost: float
    inv_charge: float
    value: float  # net $ per filled contract
    intensity: float  # contracts per second
    ev_rate: float  # $ per second
    collateral: float  # $ per contract
    score: float
    existing_id: str = ""

    def as_log(self) -> dict:
        return {k: (round(v, 8) if isinstance(v, float) else v) for k, v in self.__dict__.items()}


def _candidate_prices(ctx: MarketQuoteContext, side: str) -> list[tuple[int, str]]:
    """Candidate prices: join / improve-by-one / behind-by-k around the touch, plus the ticks
    nearest the conservative fair value (so wide or empty books are quoted near fair rather than
    one tick inside a far-away touch). Never crosses the opposite best (post-only)."""
    spec, book = ctx.spec, ctx.book
    grid = [p for p in spec.tick_grid() if ctx.price_floor_px <= p <= ctx.price_cap_px]
    bb, ba = book.best_bid(), book.best_ask()
    w = ctx.max_ticks_from_touch
    cands: set[int] = set()
    if side == "bid":
        allowed = [p for p in grid if ba is None or p < ba]
        if not allowed:
            return []
        if bb is not None:
            up = [p for p in allowed if p > bb]
            if up:
                cands.add(min(up))  # improve by one tick
            if bb in allowed:
                cands.add(bb)  # join
            cands.update(sorted((p for p in allowed if p < bb), reverse=True)[:w])  # behind
        fair_px = ctx.F_lo * PX_SCALE
        cands.update(sorted((p for p in allowed if p <= fair_px), reverse=True)[: w + 1])
    else:
        allowed = [p for p in grid if bb is None or p > bb]
        if not allowed:
            return []
        if ba is not None:
            down = [p for p in allowed if p < ba]
            if down:
                cands.add(max(down))
            if ba in allowed:
                cands.add(ba)
            cands.update(sorted(p for p in allowed if p > ba)[:w])
        fair_px = ctx.F_hi * PX_SCALE
        cands.update(sorted(p for p in allowed if p >= fair_px)[: w + 1])
    return [(p, _position_of(ctx, side, p)) for p in sorted(cands)]


def _better_qty(book: KalshiBook, side: str, px: int) -> float:
    """Displayed contracts at prices better than px on our side (must trade before us)."""
    if side == "bid":
        return sum(q for p, q in book.yes_bids.items() if p > px) / QTY_SCALE
    return sum(q for p, q in book.no_bids.items() if PX_SCALE - p < px) / QTY_SCALE


def evaluate(
    ctx: MarketQuoteContext,
    side: str,
    flow: FillIntensityModel,
    adverse: AdverseSelectionModel,
    px: int,
    position: str,
    queue_at_px: float,
    size: float,
    existing_id: str = "",
) -> QuoteCandidate:
    p = px / PX_SCALE
    sgn = 1.0 if side == "bid" else -1.0
    edge = (ctx.F_lo - p) if side == "bid" else (p - ctx.F_hi)
    key = segment_key(ctx.tau_s, abs(ctx.z), side)
    adverse_move = -ctx.dF_recent if side == "bid" else ctx.dF_recent
    as_cost = adverse.expected(key=key, adverse_recent_move=adverse_move, tau_s=ctx.tau_s, position=position)
    fee = ctx.maker_fee(px)
    dq = sgn * size
    d_new = ctx.D_btc + dq * ctx.delta_btc
    hedge_cost = ctx.hedge_cost_frac * ctx.spot * ctx.rho_hedged * (abs(d_new) - abs(ctx.D_btc)) / size
    inv = marginal_risk_charge(
        ctx.grid, ctx.base_pnl, ctx.payoff_k, p, dq, ctx.lam, ctx.lambda_tail, ctx.tail_budget
    )
    value = edge - as_cost - fee - hedge_cost - inv
    q_eff = _better_qty(ctx.book, side, px) + queue_at_px
    inten = flow.intensity(key=key, q_eff=q_eff, size=size, position=position)
    ev = inten * value
    collateral = p if side == "bid" else 1.0 - p
    score = ev / max(collateral * size, 1e-9)
    return QuoteCandidate(
        ticker=ctx.spec.ticker, side=side, px=px, size=size, position=position, q_eff=q_eff,
        edge=edge, adverse=as_cost, fee=fee, hedge_cost=hedge_cost, inv_charge=inv, value=value,
        intensity=inten, ev_rate=ev, collateral=collateral, score=score, existing_id=existing_id,
    )


@dataclass
class SideDecision:
    side: str
    keep: list[str]  # client_order_ids to keep
    cancel: list[str]  # client_order_ids to cancel
    place: QuoteCandidate | None
    best: QuoteCandidate | None  # best new candidate considered (for logs)
    existing_eval: list[QuoteCandidate]


def decide_side(
    ctx: MarketQuoteContext,
    side: str,
    flow: FillIntensityModel,
    adverse: AdverseSelectionModel,
    v_min: float,
    kappa_replace: float,
) -> SideDecision:
    """Choose to keep, replace, place or pull our quote on one side of one market."""
    cap = ctx.capacity_contracts.get(side, 0.0)
    size = min(ctx.clip_contracts, cap)
    existing = ctx.existing.get(side, [])
    ex_evals = [
        evaluate(ctx, side, flow, adverse, o.px, _position_of(ctx, side, o.px), o.queue_ahead,
                 o.remaining_qty / QTY_SCALE, existing_id=o.client_order_id)
        for o in existing
    ]
    best: QuoteCandidate | None = None
    if size > 0:
        for px, pos in _candidate_prices(ctx, side):
            queue = ctx.book.bid_qty(px) / QTY_SCALE if side == "bid" else ctx.book.ask_qty(px) / QTY_SCALE
            c = evaluate(ctx, side, flow, adverse, px, pos, queue, size)
            if c.value >= v_min and (best is None or c.ev_rate > best.ev_rate):
                best = c
    keep: list[str] = []
    cancel: list[str] = []
    place: QuoteCandidate | None = None
    good_existing = [e for e in ex_evals if e.value >= 0.0]
    bad_existing = [e for e in ex_evals if e.value < 0.0]
    cancel += [e.existing_id for e in bad_existing]
    if good_existing:
        top = max(good_existing, key=lambda e: e.ev_rate)
        others = [e for e in good_existing if e is not top]
        cancel += [e.existing_id for e in others]  # one working order per side
        if best is not None and best.px != top.px and best.ev_rate > top.ev_rate + kappa_replace:
            cancel.append(top.existing_id)
            place = best
        else:
            keep.append(top.existing_id)
    elif best is not None:
        place = best
    return SideDecision(side, keep, cancel, place, best, ex_evals)


def _position_of(ctx: MarketQuoteContext, side: str, px: int) -> str:
    bb, ba = ctx.book.best_bid(), ctx.book.best_ask()
    if side == "bid":
        if bb is None or px > bb:
            return "improve"
        return "touch" if px == bb else "behind"
    if ba is None or px < ba:
        return "improve"
    return "touch" if px == ba else "behind"
