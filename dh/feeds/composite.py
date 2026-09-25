"""BRTI nowcast inputs from the latest books of constituent venues (pure functions).

Given ``{venue: ExtBook}`` (dh.core.book) for the BRTI constituent USD spot books, compute:

  * per-venue mid, microprice, spread and near-touch depth (``venue_quote``),
  * the median of venue mids (``median_mid``),
  * a depth-weighted consolidated mid (``depth_weighted_mid``: venue mids weighted by the
    smaller side's USD depth within ``depth_bps`` of the touch),
  * a methodology-inspired BRTI replica (``brti_replica``).

These are *candidate* nowcasts. Research (Experiment 2) compares them with the real BRTI ticks
from Kalshi's ``cfbenchmarks_value[_5hz]`` channels and calibrates the parameters below.

Reading of the CME CF Real Time Indices methodology (from memory of the public methodology
document; every parameter is marked CALIBRATE and must be fitted against recorded BRTI):

  1. Inputs are the order books of the constituent exchanges (USD pairs). Books that are
     stale, crossed/erroneous, or whose mid deviates too far from the other exchanges are
     excluded ("potentially erroneous data" rules).
  2. All bids and all asks are merged into one consolidated order book. Very large orders are
     capped at a dynamic order-size cap (mean + k standard deviations of order sizes) so a
     single outsized order cannot dominate.
  3. Price-volume curves: askPV(v) is the marginal ask price at cumulative volume v when
     walking up the consolidated asks; bidPV(v) likewise walking down the bids. The mid
     price-volume curve is midPV(v) = (askPV(v) + bidPV(v)) / 2 and the mid spread-volume
     curve is midSV(v) = askPV(v) / midPV(v) - 1.
  4. Utilized depth v_T = max{v : midSV(v) <= D}, with D the spread-deviation threshold.
  5. The index is the weighted average of midPV over [0, v_T] with an exponential density
     of rate lambda = 1 / (C_lambda * v_T), normalized over [0, v_T]:
        RTI = integral_0^vT midPV(v) * lambda * exp(-lambda v) dv / (1 - exp(-lambda v_T)).
     midPV is a step function, so the integral is evaluated exactly segment by segment.

Units: prices USD, sizes BTC, times ns.
"""

from __future__ import annotations

import math
import statistics
from collections.abc import Mapping
from dataclasses import dataclass

from dh.core.book import ExtBook

NS_PER_MS = 1_000_000


# ============================================================================ per-venue inputs
@dataclass(frozen=True, slots=True)
class VenueQuote:
    venue: str
    symbol: str
    bid: float
    ask: float
    bid_size: float
    ask_size: float
    mid: float
    microprice: float
    spread_bps: float
    depth_bid_usd: float  # USD resting within depth_bps of the best bid
    depth_ask_usd: float
    age_ms: float  # now - last book update
    included: bool = True
    reason: str = ""


def venue_quote(venue: str, book: ExtBook, now_ns: int, depth_bps: float = 10.0) -> VenueQuote | None:
    """Top-of-book summary of one venue book (None if the book is empty on a side)."""
    t = book.top()
    if t is None:
        return None
    mid = t.mid
    return VenueQuote(
        venue=venue,
        symbol=book.symbol,
        bid=t.bid,
        ask=t.ask,
        bid_size=t.bid_size,
        ask_size=t.ask_size,
        mid=mid,
        microprice=t.microprice,
        spread_bps=(t.ask - t.bid) / mid * 1e4 if mid > 0 else float("nan"),
        depth_bid_usd=book.depth_usd("b", depth_bps),
        depth_ask_usd=book.depth_usd("a", depth_bps),
        age_ms=(now_ns - book.ts) / NS_PER_MS,
    )


def screen_quotes(
    quotes: list[VenueQuote], stale_after_ms: float, deviation_limit: float, max_spread_bps: float = 100.0
) -> list[VenueQuote]:
    """Apply the erroneous-data screens; returns quotes with ``included``/``reason`` set.

    Excludes: stale books (age > stale_after_ms), crossed/locked books, spreads wider than
    max_spread_bps, and mids deviating from the median of the remaining mids by more than
    ``deviation_limit`` (fraction)."""
    out: list[VenueQuote] = []
    for q in quotes:
        reason = ""
        if q.age_ms > stale_after_ms:
            reason = f"stale {q.age_ms:.0f}ms"
        elif q.bid >= q.ask:
            reason = "crossed"
        elif q.spread_bps > max_spread_bps:
            reason = f"spread {q.spread_bps:.1f}bps"
        out.append(_with(q, not reason, reason))
    live = [q.mid for q in out if q.included]
    if live:
        med = statistics.median(live)
        out = [
            _with(q, False, f"deviation {abs(q.mid / med - 1):.4f}")
            if q.included and med > 0 and abs(q.mid / med - 1) > deviation_limit
            else q
            for q in out
        ]
    return out


def _with(q: VenueQuote, included: bool, reason: str) -> VenueQuote:
    return VenueQuote(
        q.venue, q.symbol, q.bid, q.ask, q.bid_size, q.ask_size, q.mid, q.microprice, q.spread_bps,
        q.depth_bid_usd, q.depth_ask_usd, q.age_ms, included, reason,
    )


def median_mid(quotes: list[VenueQuote]) -> float | None:
    """Median of included venue mids."""
    mids = [q.mid for q in quotes if q.included]
    return statistics.median(mids) if mids else None


def depth_weighted_mid(quotes: list[VenueQuote]) -> float | None:
    """Venue mids weighted by min(bid depth, ask depth) near the touch (USD)."""
    num = den = 0.0
    for q in quotes:
        if not q.included:
            continue
        w = min(q.depth_bid_usd, q.depth_ask_usd)
        num += w * q.mid
        den += w
    if den <= 0:
        return median_mid(quotes)
    return num / den


def microprice_median(quotes: list[VenueQuote]) -> float | None:
    mps = [q.microprice for q in quotes if q.included]
    return statistics.median(mps) if mps else None


# ============================================================================ BRTI replica
@dataclass(frozen=True, slots=True)
class BrtiParams:
    """All CALIBRATE: fit against recorded BRTI (Experiment 2)."""

    spread_threshold: float = 0.005  # D: max midSV defining the utilized depth
    lambda_factor: float = 0.3  # lambda = 1 / (lambda_factor * v_T)
    size_cap_k: float = 5.0  # order-size cap = mean + k * std (k <= 0 disables the cap)
    size_cap_band: float = 0.005  # levels within this fraction of the consolidated mid form the cap sample
    size_cap_min_sample: int = 10
    deviation_limit: float = 0.05  # exclude a venue whose mid deviates from the median by more
    stale_after_ms: float = 30_000.0
    max_spread_bps: float = 100.0
    max_levels_per_venue: int = 0  # 0 = use all published levels
    depth_bps: float = 10.0  # for the per-venue depth numbers


@dataclass(frozen=True, slots=True)
class BrtiResult:
    value: float  # replica index value (USD)
    utilized_depth: float  # v_T (BTC)
    lam: float  # lambda (1/BTC)
    size_cap: float  # applied order-size cap (BTC); inf if disabled
    best_bid: float
    best_ask: float
    venues: tuple[str, ...]  # venues contributing to the consolidated book
    n_segments: int


def _levels(book: ExtBook, side: str, max_levels: int) -> list[tuple[float, float]]:
    if side == "b":
        items = list(reversed(book.bids.items()))  # best (highest) first
    else:
        items = list(book.asks.items())  # best (lowest) first
    if max_levels > 0:
        items = items[:max_levels]
    return [(p, s) for p, s in items if s > 0]


def size_cap(bids: list[tuple[float, float]], asks: list[tuple[float, float]], params: BrtiParams) -> float:
    """Dynamic order-size cap: mean + k*std of level sizes within ``size_cap_band`` of the
    consolidated mid (falls back to all levels if the sample is too small)."""
    if params.size_cap_k <= 0 or not bids or not asks:
        return math.inf
    mid = 0.5 * (bids[0][0] + asks[0][0])
    lo, hi = mid * (1 - params.size_cap_band), mid * (1 + params.size_cap_band)
    sample = [s for p, s in bids if p >= lo] + [s for p, s in asks if p <= hi]
    if len(sample) < params.size_cap_min_sample:
        sample = [s for _, s in bids] + [s for _, s in asks]
    if len(sample) < 2:
        return math.inf
    m = statistics.fmean(sample)
    sd = statistics.pstdev(sample)
    return m + params.size_cap_k * sd


def price_volume_segments(
    bids: list[tuple[float, float]], asks: list[tuple[float, float]]
) -> list[tuple[float, float, float, float]]:
    """Consolidated (bids best-first, asks best-first) -> segments (v0, v1, bidPV, askPV) on
    which both marginal prices are constant, covering [0, min(total bid, total ask)]."""
    segs: list[tuple[float, float, float, float]] = []
    i = j = 0
    v = 0.0
    b_end = bids[0][1] if bids else 0.0
    a_end = asks[0][1] if asks else 0.0
    while i < len(bids) and j < len(asks):
        v1 = min(b_end, a_end)
        if v1 > v:
            segs.append((v, v1, bids[i][0], asks[j][0]))
            v = v1
        if b_end <= v1:
            i += 1
            if i < len(bids):
                b_end += bids[i][1]
        if a_end <= v1:
            j += 1
            if j < len(asks):
                a_end += asks[j][1]
    return segs


def brti_from_levels(
    bids: list[tuple[float, float]], asks: list[tuple[float, float]], params: BrtiParams = BrtiParams()
) -> tuple[float, float, float, int] | None:
    """(value, v_T, lambda, n_segments) from consolidated, capped, sorted levels."""
    segs = price_volume_segments(bids, asks)
    if not segs:
        return None
    D = params.spread_threshold
    vT = 0.0
    used: list[tuple[float, float, float]] = []
    for v0, v1, bp, ap in segs:
        mid = 0.5 * (bp + ap)
        if mid <= 0 or ap / mid - 1.0 > D:
            break
        used.append((v0, v1, mid))
        vT = v1
    if vT <= 0 or not used:
        _, _, bp, ap = segs[0]
        return 0.5 * (bp + ap), 0.0, math.inf, len(segs)
    lam = 1.0 / (params.lambda_factor * vT)
    z = -math.expm1(-lam * vT)  # 1 - exp(-lambda vT)
    acc = 0.0
    for v0, v1, mid in used:
        w = math.exp(-lam * v0) - math.exp(-lam * v1)
        acc += mid * w
    return acc / z, vT, lam, len(used)


def consolidated_levels(
    books: Mapping[str, ExtBook], params: BrtiParams = BrtiParams(), venues: list[str] | None = None
) -> tuple[list[tuple[float, float]], list[tuple[float, float]], float]:
    """Merge the given venues' books into (bids desc, asks asc) with sizes capped; returns the
    cap too. Venue order is irrelevant (ties are merged by price; deterministic)."""
    names = sorted(books) if venues is None else sorted(venues)
    raw_b: list[tuple[float, float]] = []
    raw_a: list[tuple[float, float]] = []
    for v in names:
        raw_b += _levels(books[v], "b", params.max_levels_per_venue)
        raw_a += _levels(books[v], "a", params.max_levels_per_venue)
    raw_b.sort(key=lambda x: -x[0])
    raw_a.sort(key=lambda x: x[0])
    cap = size_cap(raw_b, raw_a, params)
    bids = [(p, min(s, cap)) for p, s in raw_b]
    asks = [(p, min(s, cap)) for p, s in raw_a]
    return bids, asks, cap


def brti_replica(
    books: Mapping[str, ExtBook], now_ns: int, params: BrtiParams = BrtiParams()
) -> BrtiResult | None:
    """Methodology-inspired BRTI replica from constituent books (see module docstring)."""
    quotes = [q for v in sorted(books) if (q := venue_quote(v, books[v], now_ns, params.depth_bps)) is not None]
    quotes = screen_quotes(quotes, params.stale_after_ms, params.deviation_limit, params.max_spread_bps)
    venues = [q.venue for q in quotes if q.included]
    if not venues:
        return None
    bids, asks, cap = consolidated_levels(books, params, venues)
    if not bids or not asks:
        return None
    res = brti_from_levels(bids, asks, params)
    if res is None:
        return None
    value, vT, lam, n = res
    return BrtiResult(value, vT, lam, cap, bids[0][0], asks[0][0], tuple(venues), n)


# ============================================================================ all inputs
@dataclass(frozen=True, slots=True)
class Nowcast:
    ts: int
    quotes: tuple[VenueQuote, ...]
    median_mid: float | None
    depth_weighted_mid: float | None
    microprice_median: float | None
    brti: BrtiResult | None


def nowcast(books: Mapping[str, ExtBook], now_ns: int, params: BrtiParams = BrtiParams()) -> Nowcast:
    """Every candidate nowcast input at ``now_ns`` from ``{venue: ExtBook}``."""
    quotes = [q for v in sorted(books) if (q := venue_quote(v, books[v], now_ns, params.depth_bps)) is not None]
    quotes = screen_quotes(quotes, params.stale_after_ms, params.deviation_limit, params.max_spread_bps)
    return Nowcast(
        ts=now_ns,
        quotes=tuple(quotes),
        median_mid=median_mid(quotes),
        depth_weighted_mid=depth_weighted_mid(quotes),
        microprice_median=microprice_median(quotes),
        brti=brti_replica(books, now_ns, params),
    )
