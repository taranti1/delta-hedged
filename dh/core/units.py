"""Exact integer units for Kalshi prices, contract counts and money.

Kalshi wire formats (openapi 3.30.0 / asyncapi 2.0.0):
  * prices are fixed-point dollar strings; requests accept 2-4 decimals, valid ticks are
    constrained by the market's ``price_ranges`` (1c, 0.1c or 0.01c steps);
  * contract counts are fixed-point strings with exactly 2 decimals ("10.00");
  * fees / costs are fixed-point dollar strings with up to 6 decimals.

Internally we never use binary floats for anything that touches the exchange or the ledger:

  Px    int, 1 unit = $0.0001  (PX_SCALE = 10_000;  $1.00 == 10_000)
  Qty   int, 1 unit = 0.01 contract (QTY_SCALE = 100; 1 contract == 100)
  Micros int, 1 unit = $0.000001 (MICROS = 1_000_000)

Models (fair value, volatility, ...) work in floats and convert at the boundary.
"""

from __future__ import annotations

from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal, InvalidOperation

PX_SCALE = 10_000
QTY_SCALE = 100
MICROS = 1_000_000

PX_ONE = PX_SCALE  # price of a certain payout ($1)
NS_PER_S = 1_000_000_000
NS_PER_MS = 1_000_000


class UnitError(ValueError):
    """Raised when a wire value cannot be represented exactly in internal units."""


def _dec(value: str | int | Decimal) -> Decimal:
    if isinstance(value, float):
        raise TypeError("binary float not accepted at the exchange boundary; pass str")
    try:
        return Decimal(str(value))
    except InvalidOperation as exc:  # pragma: no cover - defensive
        raise UnitError(f"not a number: {value!r}") from exc


def px_from_dollars(value: str | int | Decimal) -> int:
    """'0.5600' -> 5600. Raises if the value is not a multiple of $0.0001."""
    d = _dec(value) * PX_SCALE
    if d != d.to_integral_value():
        raise UnitError(f"price {value!r} finer than $0.0001")
    px = int(d)
    if not 0 <= px <= PX_SCALE:
        raise UnitError(f"price {value!r} outside [0, 1]")
    return px


def px_to_dollars(px: int) -> str:
    """5600 -> '0.5600' (4 decimals, accepted by all order endpoints)."""
    if not 0 <= px <= PX_SCALE:
        raise UnitError(f"px {px} outside [0, {PX_SCALE}]")
    return f"{px // PX_SCALE}.{px % PX_SCALE:04d}"


def px_to_float(px: int) -> float:
    return px / PX_SCALE


def qty_from_fp(value: str | int | Decimal) -> int:
    """'10.00' -> 1000. Raises if finer than 0.01 contracts."""
    d = _dec(value) * QTY_SCALE
    if d != d.to_integral_value():
        raise UnitError(f"count {value!r} finer than 0.01")
    return int(d)


def qty_to_fp(qty: int) -> str:
    """1000 -> '10.00'."""
    sign = "-" if qty < 0 else ""
    q = abs(qty)
    return f"{sign}{q // QTY_SCALE}.{q % QTY_SCALE:02d}"


def qty_to_float(qty: int) -> float:
    return qty / QTY_SCALE


def contracts(n: int | float) -> int:
    """Whole/fractional contract count -> Qty units (exact for multiples of 0.01)."""
    q = round(n * QTY_SCALE)
    if abs(q - n * QTY_SCALE) > 1e-6:
        raise UnitError(f"{n} contracts not a multiple of 0.01")
    return int(q)


def micros_from_dollars(value: str | int | Decimal) -> int:
    """'0.004375' -> 4375. Values finer than $0.000001 raise."""
    d = _dec(value) * MICROS
    if d != d.to_integral_value():
        raise UnitError(f"money {value!r} finer than $0.000001")
    return int(d)


def micros_to_dollars(m: int) -> str:
    sign = "-" if m < 0 else ""
    a = abs(m)
    return f"{sign}{a // MICROS}.{a % MICROS:06d}"


def ceil_micros(value: Decimal) -> int:
    """Round a Decimal dollar amount UP to whole micro-dollars."""
    return int((value * MICROS).to_integral_value(rounding=ROUND_CEILING))


def floor_micros(value: Decimal) -> int:
    return int((value * MICROS).to_integral_value(rounding=ROUND_FLOOR))


def no_px(yes_px: int) -> int:
    """Price of the complementary NO contract (a NO bid at q == a YES ask at 1 - q)."""
    return PX_SCALE - yes_px


def notional_micros(px: int, qty: int) -> int:
    """Exact cost in micro-dollars of qty (0.01 units) contracts at px (1e-4 units)."""
    # px/1e4 dollars * qty/1e2 contracts = px*qty/1e6 dollars = px*qty micros.
    return px * qty
