"""Exact, config-driven Kalshi trading-fee model (config/fees.yaml).

Formula (Kalshi fee schedule, openapi Series.fee_type description):

    unrounded fee ($) = M * rate * C * P * (1 - P)

  M    fee multiplier (series ``fee_multiplier``, or event ``fee_multiplier_override``)
  C    contracts (Qty / 100), P = YES price in dollars (Px / 10_000). P(1-P) is symmetric,
       so the fee is the same for the YES and the NO leg of a fill.
  rate by role and ``fee_type``:
       taker                              0.07   (every quadratic type)
       maker, quadratic                   0      (no maker fee)
       maker, quadratic_with_maker_fees   0.0175 (= 0.25 x taker)
       maker, quadratic_with_combo_maker_fees  0.035 (= 0.5 x taker)
       flat (or any unknown type)         UNSUPPORTED -> UnsupportedFeeType, never priced as 0

In integer units: fee_micros_exact = M * rate * qty * px * (10_000 - px) / 10_000.

Rounding (Kalshi "fee rounding" rules; mechanics as implemented and reconciled in
taranti1/trading-strategy kalshi_m0/fees/engine.py — the docs page itself could not be
fetched from the build environment):

  1. trade fee    = ceil to $0.000001 (1 micro) of the unrounded fee, per fill.
  2. rounding fee = the fill's cash change (trade cash - trade fee) is floored to the
                    account's balance precision (``balance_precision_dollars``: $0.01 for
                    non-direct members, $0.0001 for direct members); the difference
                    (always in [0, precision)) is charged as a rounding fee. The floor is
                    taken on the signed cash flow, so it always favours the exchange.
  3. rebate       = rounding fees accumulate PER ORDER (``OrderFeeAccumulator``). Whenever
                    the carried total reaches one precision unit, one unit is rebated on
                    the current fill, repeatedly, but never so much that the fill's net fee
                    (trade + rounding - rebate) would become negative; any excess stays in
                    the carry for later fills of the same order.
  net fee = trade + rounding - rebate  (>= 0 for every fill).

Worked example (docs, reproduced in tests): buy 1 YES @ $0.055, taker, M = 1:
unrounded 0.07 * 1 * 0.055 * 0.945 = $0.00363825 -> trade $0.003639; cash -0.055 - 0.003639
= -0.058639 -> floored to -$0.06 -> rounding $0.001361; net fee $0.005000.

Cash sign per YES-book side: a 'bid' fill pays P*C; an 'ask' fill pays (1-P)*C when it
opens NO (or receives P*C when it closes YES). Both ask cash flows are congruent modulo
$0.01 because C*$1 is a multiple of $0.01, so the rounding is identical either way.

``fee_waiver_expiration_time`` (Market): the spec only says "time when this market's fee
waiver expires" and does not state which fees are waived, so fees are NOT zeroed by default.
The schedule records the waiver and ``waiver_active(now_ns)`` reports it; zeroing is opt-in
(``apply_fee_waiver=True``) and must first be confirmed against reported ``fee_cost``.

Every fill's reported ``fee_cost`` should be reconciled with ``reconcile_fill_fee``; a
mismatch beyond the rounding component should halt quoting.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from decimal import ROUND_CEILING, Decimal, localcontext
from pathlib import Path
from typing import Any

import yaml

from dh.core.units import MICROS, PX_SCALE, micros_from_dollars, notional_micros
from dh.kalshi.wire import iso_to_ns, opt_iso_to_ns, to_decimal

DEFAULT_FEES_YAML = Path(__file__).resolve().parents[2] / "config" / "fees.yaml"
SUPPORTED_FEE_TYPES = ("quadratic", "quadratic_with_maker_fees", "quadratic_with_combo_maker_fees")


class UnsupportedFeeType(ValueError):
    """The fee type/multiplier cannot be priced (flat, unknown, unresolved, invalid)."""


# ============================================================================ rates
@dataclass(frozen=True, slots=True)
class FeeRates:
    """Fee rates (Decimal, dimensionless) + account balance precision (micros)."""

    taker: Decimal
    maker: Mapping[str, Decimal]
    balance_precision_micros: int = 10_000
    effective_from: str = ""
    verified_against_live_fills: bool = False
    source: str = ""

    @classmethod
    def from_dict(cls, d: Mapping[str, Any], source: str = "") -> FeeRates:
        """Parse the fees.yaml mapping. Decimal values must be quoted strings (no floats)."""
        rates = d["rates"]
        maker_raw = rates.get("maker") or {}
        maker = {str(k): _dec_cfg(v, f"rates.maker.{k}") for k, v in maker_raw.items() if v is not None}
        bp = micros_from_dollars(str(_dec_cfg(d.get("balance_precision_dollars", "0.01"), "balance_precision")))
        if bp <= 0:
            raise ValueError("balance_precision_dollars must be > 0")
        return cls(
            taker=_dec_cfg(rates["taker"], "rates.taker"),
            maker=maker,
            balance_precision_micros=bp,
            effective_from=str(d.get("effective_from", "")),
            verified_against_live_fills=bool(d.get("verified_against_live_fills", False)),
            source=source,
        )

    @classmethod
    def from_yaml(cls, path: str | Path = DEFAULT_FEES_YAML) -> FeeRates:
        """Load config/fees.yaml (default: the repository's copy)."""
        p = Path(path)
        with p.open("r", encoding="utf-8") as f:
            return cls.from_dict(yaml.safe_load(f), source=str(p))

    def rate(self, fee_type: str, is_taker: bool) -> Decimal:
        """Fee rate for a role under fee_type; raises UnsupportedFeeType (flat/unknown)."""
        if fee_type not in SUPPORTED_FEE_TYPES:
            raise UnsupportedFeeType(f"fee_type {fee_type!r} is not supported (refuse to price as zero)")
        if is_taker:
            return self.taker
        if fee_type not in self.maker:
            raise UnsupportedFeeType(f"no maker rate configured for fee_type {fee_type!r}")
        return self.maker[fee_type]


def _dec_cfg(v: Any, name: str) -> Decimal:
    if isinstance(v, float):
        raise ValueError(f"{name}: quote decimal values in fees.yaml (got float {v!r})")
    if isinstance(v, bool) or v is None:
        raise ValueError(f"{name}: expected a decimal string, got {v!r}")
    d = Decimal(str(v))
    if not d.is_finite() or d < 0:
        raise ValueError(f"{name}: invalid value {v!r}")
    return d


# ============================================================================ resolution
def resolve_fee_fields(
    series: Mapping[str, Any] | None,
    event: Mapping[str, Any] | None = None,
    market: Mapping[str, Any] | None = None,
) -> tuple[str, Decimal | None, str]:
    """(fee_type, multiplier, source) with precedence event override > series > market.

    Event overrides (EventData.fee_type_override / fee_multiplier_override, or the WS
    event_fee_update) apply field by field; null means "fall back to the series". A resolved
    type with no multiplier anywhere gets multiplier 1. Nothing resolved -> ('', None,
    'unresolved'). source: 'event_override' | 'series' | 'market' | 'unresolved'.
    """
    ev = event or {}
    se = series or {}
    mk = market or {}
    if se.get("fee_type"):
        base, base_src = se, "series"
    elif mk.get("fee_type"):
        base, base_src = mk, "market"
    else:
        base, base_src = {}, "unresolved"
    t_over = ev.get("fee_type_override")
    m_over = ev.get("fee_multiplier_override")
    fee_type = str(t_over or base.get("fee_type") or "")
    mult_raw = m_over if m_over is not None else base.get("fee_multiplier")
    if not fee_type:
        return "", None, "unresolved"
    mult = Decimal(1) if mult_raw is None else to_decimal(mult_raw)
    if not mult.is_finite() or mult < 0:
        raise UnsupportedFeeType(f"invalid fee multiplier {mult_raw!r}")
    src = "event_override" if (t_over or m_over is not None) else base_src
    return fee_type, mult, src


def apply_scheduled_changes(
    obj: Mapping[str, Any],
    changes: Iterable[Mapping[str, Any]],
    fetched_ns: int,
    now_ns: int,
    *,
    type_key: str = "fee_type",
    mult_key: str = "fee_multiplier",
) -> dict[str, Any]:
    """Apply fee changes scheduled in (fetched_ns, now_ns] to a series/event dict snapshot.

    For GET /series/fee_changes (SeriesFeeChange: fee_type, fee_multiplier, scheduled_ts) use
    the defaults; for GET /events/fee_changes (EventFeeChange) pass
    type_key='fee_type_override', mult_key='fee_multiplier_override'. The latest change wins.
    """
    out = dict(obj)
    due = []
    for ch in changes:
        ts = opt_iso_to_ns(ch.get("scheduled_ts"))
        if fetched_ns < ts <= now_ns:
            due.append((ts, ch))
    for _ts, ch in sorted(due, key=lambda x: x[0]):
        out[type_key] = ch.get(type_key)
        out[mult_key] = ch.get(mult_key)
    return out


# ============================================================================ schedule
@dataclass(frozen=True, slots=True)
class FeeBreakdown:
    """Per-fill fee components, all int micro-dollars. net = trade + rounding - rebate >= 0.

    cash_micros: signed cash change of the fill including fees (negative = paid), aligned to
    the balance precision (plus any rebate).
    """

    trade_micros: int
    rounding_micros: int
    rebate_micros: int
    net_micros: int
    cash_micros: int


@dataclass(frozen=True, slots=True)
class FeeSchedule:
    """Resolved fee schedule for one market (fee_type + multiplier + provenance)."""

    fee_type: str
    multiplier: Decimal
    source: str
    rates: FeeRates
    fee_waiver_expiration_ns: int = 0  # 0 = no waiver
    apply_fee_waiver: bool = False

    @property
    def supported(self) -> bool:
        """True if fees can be priced (quadratic family with a finite multiplier)."""
        return self.fee_type in SUPPORTED_FEE_TYPES and self.source != "unresolved"

    def rate(self, is_taker: bool) -> Decimal:
        """Effective rate incl. multiplier (M * rate); raises UnsupportedFeeType."""
        if self.source == "unresolved":
            raise UnsupportedFeeType("fee schedule unresolved (no event/series/market fee_type)")
        return self.multiplier * self.rates.rate(self.fee_type, is_taker)

    def waiver_active(self, now_ns: int) -> bool:
        """True if the market reports a fee waiver that has not expired at now_ns."""
        return bool(self.fee_waiver_expiration_ns) and now_ns < self.fee_waiver_expiration_ns

    def unrounded_fee(self, px: int, qty: int, is_taker: bool) -> Decimal:
        """Exact unrounded fee in dollars for qty (0.01 contracts) at YES price px (1e-4 $)."""
        return self._exact_micros(px, qty, is_taker) / MICROS

    def _exact_micros(self, px: int, qty: int, is_taker: bool) -> Decimal:
        if not 0 <= px <= PX_SCALE:
            raise ValueError(f"px {px} outside [0, {PX_SCALE}]")
        if qty < 0:
            raise ValueError(f"qty {qty} < 0")
        r = self.rate(is_taker)
        with localcontext() as ctx:
            ctx.prec = 60
            return r * qty * px * (PX_SCALE - px) / PX_SCALE

    def trade_fee_micros(self, px: int, qty: int, is_taker: bool, now_ns: int | None = None) -> int:
        """Unrounded fee ceiled to whole micro-dollars (int micros).

        now_ns is only used when apply_fee_waiver=True (then 0 while the waiver is active).
        """
        exact = self._exact_micros(px, qty, is_taker)
        if self.apply_fee_waiver and now_ns is not None and self.waiver_active(now_ns):
            return 0
        return int(exact.to_integral_value(rounding=ROUND_CEILING))

    def expected_fee_per_contract(self, px: int, is_taker: bool) -> float:
        """Model-facing expected fee per contract in dollars: M * rate * P * (1-P), unrounded.

        Rounding adds < 1 balance-precision unit per order and is mostly rebated on
        multi-fill orders; use ``order_fee_micros`` for an exact single-fill figure.
        """
        return float(self._exact_micros(px, 100, is_taker)) / MICROS

    def order_accumulator(
        self, book_side: str = "bid", balance_precision_micros: int | None = None
    ) -> OrderFeeAccumulator:
        """Fresh per-order rounding accumulator."""
        return OrderFeeAccumulator(self, book_side, balance_precision_micros)

    def order_fee_micros(self, px: int, qty: int, is_taker: bool, book_side: str = "bid") -> FeeBreakdown:
        """Exact fees of an order filled in one fill (fresh accumulator)."""
        return self.order_accumulator(book_side).apply_fill(px, qty, is_taker)


class OrderFeeAccumulator:
    """Per-order balance-precision rounding with carried rebates (see module docstring).

    book_side: the order's side on the YES book ('bid' buys YES, 'ask' sells YES / buys NO).
    balance_precision_micros: 10_000 ($0.01, non-direct) or 100 ($0.0001, direct members);
    defaults to the schedule's configured value.
    """

    __slots__ = ("schedule", "book_side", "bp", "carry_micros", "fills", "total")

    def __init__(self, schedule: FeeSchedule, book_side: str = "bid", balance_precision_micros: int | None = None):
        if book_side not in ("bid", "ask"):
            raise ValueError(f"book_side must be 'bid' or 'ask', got {book_side!r}")
        self.schedule = schedule
        self.book_side = book_side
        self.bp = int(balance_precision_micros or schedule.rates.balance_precision_micros)
        if self.bp <= 0:
            raise ValueError("balance precision must be positive")
        self.carry_micros = 0  # accumulated, not yet rebated rounding fees (>= 0)
        self.fills = 0
        self.total = FeeBreakdown(0, 0, 0, 0, 0)

    def apply_fill(self, px: int, qty: int, is_taker: bool, now_ns: int | None = None) -> FeeBreakdown:
        """Fees for one fill of this order: px YES price (1e-4 $), qty (0.01 contracts)."""
        trade = self.schedule.trade_fee_micros(px, qty, is_taker, now_ns)
        leg_px = px if self.book_side == "bid" else PX_SCALE - px
        cash = -notional_micros(leg_px, qty)
        provisional = cash - trade
        aligned = (provisional // self.bp) * self.bp  # floor (toward -inf) favours the exchange
        rounding = provisional - aligned
        self.carry_micros += rounding
        rebate = 0
        while self.carry_micros >= self.bp and trade + rounding - (rebate + self.bp) >= 0:
            rebate += self.bp
            self.carry_micros -= self.bp
        net = trade + rounding - rebate
        out = FeeBreakdown(trade, rounding, rebate, net, cash - net)
        t = self.total
        self.total = FeeBreakdown(
            t.trade_micros + trade,
            t.rounding_micros + rounding,
            t.rebate_micros + rebate,
            t.net_micros + net,
            t.cash_micros + out.cash_micros,
        )
        self.fills += 1
        return out


# ============================================================================ engine
class FeeEngine:
    """Builds FeeSchedules from exchange metadata using configured rates."""

    def __init__(self, rates: FeeRates | None = None, *, apply_fee_waiver: bool = False) -> None:
        self.rates = rates if rates is not None else FeeRates.from_yaml()
        self.apply_fee_waiver = apply_fee_waiver

    @classmethod
    def from_config(cls, path: str | Path = DEFAULT_FEES_YAML, **kw: Any) -> FeeEngine:
        return cls(FeeRates.from_yaml(path), **kw)

    def schedule(
        self, fee_type: str, multiplier: Decimal | str | int = 1, source: str = "explicit", fee_waiver_expiration_ns: int = 0
    ) -> FeeSchedule:
        """Schedule from explicit values (multiplier as Decimal/str/int, never float)."""
        mult = to_decimal(multiplier)
        if not mult.is_finite() or mult < 0:
            raise UnsupportedFeeType(f"invalid fee multiplier {multiplier!r}")
        return FeeSchedule(fee_type, mult, source, self.rates, fee_waiver_expiration_ns, self.apply_fee_waiver)

    def schedule_for(
        self,
        series: Mapping[str, Any] | None,
        event: Mapping[str, Any] | None = None,
        market: Mapping[str, Any] | None = None,
    ) -> FeeSchedule:
        """Resolve event override > series > market. Never raises for flat/unknown types:
        the returned schedule has ``supported == False`` and raises when used for pricing."""
        fee_type, mult, src = resolve_fee_fields(series, event, market)
        waiver_raw = (market or {}).get("fee_waiver_expiration_time")
        waiver_ns = iso_to_ns(str(waiver_raw)) if waiver_raw else 0
        return FeeSchedule(
            fee_type,
            mult if mult is not None else Decimal(1),
            src,
            self.rates,
            waiver_ns,
            self.apply_fee_waiver,
        )

    def schedule_for_spec(self, fee_type: str, fee_multiplier: float) -> FeeSchedule:
        """From MarketSpec.fee_type / fee_multiplier (float converted via its shortest repr)."""
        if not fee_type:
            return FeeSchedule("", Decimal(1), "unresolved", self.rates, 0, self.apply_fee_waiver)
        return self.schedule(fee_type, Decimal(repr(float(fee_multiplier))), source="spec")


# ============================================================================ reconciliation
@dataclass(frozen=True, slots=True)
class FeeCheck:
    """Comparison of a modeled fee with the exchange-reported fee_cost (micros).

    diff_micros = reported - expected. ok: |diff| <= tolerance. exact: diff == 0.
    matched: which modeled component equals the report exactly ('net' | 'trade' | '')
    when a FeeBreakdown was supplied — reveals whether fee_cost includes rounding.
    """

    expected_micros: int
    reported_micros: int | None
    diff_micros: int
    tolerance_micros: int
    ok: bool
    exact: bool
    matched: str = ""
    detail: str = ""
    extra: dict[str, Any] = field(default_factory=dict)


def reconcile_fill_fee(
    expected_micros: int,
    reported_fee_cost: str,
    *,
    breakdown: FeeBreakdown | None = None,
    tolerance_micros: int | None = None,
    balance_precision_micros: int = 10_000,
) -> FeeCheck:
    """Check a fill's reported fee_cost (fixed-point dollar string) against the model.

    Tolerance = the rounding component: |rounding - rebate| of ``breakdown`` when given
    (the part of the fee that depends on the rounding convention), else one balance
    precision unit; ``tolerance_micros`` overrides both.
    """
    if tolerance_micros is None:
        if breakdown is not None:
            tolerance_micros = abs(breakdown.rounding_micros - breakdown.rebate_micros)
        else:
            tolerance_micros = balance_precision_micros
    try:
        reported = micros_from_dollars(str(reported_fee_cost).strip())
    except (ValueError, ArithmeticError) as exc:
        return FeeCheck(expected_micros, None, 0, tolerance_micros, False, False, "", f"unparseable fee_cost: {exc}")
    diff = reported - expected_micros
    matched = ""
    if breakdown is not None:
        if reported == breakdown.net_micros:
            matched = "net"
        elif reported == breakdown.trade_micros:
            matched = "trade"
    ok = abs(diff) <= tolerance_micros
    detail = "exact" if diff == 0 else ("within rounding tolerance" if ok else "MISMATCH")
    return FeeCheck(expected_micros, reported, diff, tolerance_micros, ok, diff == 0, matched, detail)
