"""Small, pure helpers for Kalshi wire formats (timestamps, fixed-point strings, JSON).

Units (see dh.core.units): prices -> Px ints (1e-4 $), counts -> Qty ints (0.01 contract),
money -> Micros ints (1e-6 $), times -> int ns since the Unix epoch (UTC).
"""

from __future__ import annotations

import calendar
import re
from decimal import Decimal, InvalidOperation
from typing import Any

from dh.core.units import (
    NS_PER_MS,
    NS_PER_S,
    micros_from_dollars,
    px_from_dollars,
    qty_from_fp,
)

_ISO_RE = re.compile(
    r"^(\d{4})-(\d{2})-(\d{2})[T ](\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,9})\d*)?"
    r"(Z|z|[+-]\d{2}:?\d{2})?$"
)


def iso_to_ns(value: str) -> int:
    """RFC3339 / ISO-8601 timestamp -> int ns since epoch (UTC), exact to the nanosecond.

    Accepts 'Z' or '+HH:MM' offsets and 0-9 fractional digits (extra digits truncated).
    A timestamp without an offset is interpreted as UTC.
    """
    m = _ISO_RE.match(value.strip())
    if not m:
        raise ValueError(f"not an RFC3339 timestamp: {value!r}")
    y, mo, d, h, mi, s, frac, tz = m.groups()
    secs = calendar.timegm((int(y), int(mo), int(d), int(h), int(mi), int(s), 0, 0, 0))
    if tz and tz not in ("Z", "z"):
        sign = 1 if tz[0] == "+" else -1
        digits = tz[1:].replace(":", "")
        secs -= sign * (int(digits[:2]) * 3600 + int(digits[2:]) * 60)
    frac_ns = int((frac or "").ljust(9, "0")) if frac else 0
    return secs * NS_PER_S + frac_ns


def opt_iso_to_ns(value: Any) -> int:
    """Nullable RFC3339 field -> ns, 0 when absent/empty."""
    if value is None or value == "":
        return 0
    return iso_to_ns(str(value))


def ms_to_ns(value: Any) -> int:
    """Unix milliseconds (int or numeric str) -> ns; 0 when absent."""
    if value is None or value == "":
        return 0
    return int(value) * NS_PER_MS


def s_to_ns(value: Any) -> int:
    """Unix seconds (int or numeric str) -> ns; 0 when absent."""
    if value is None or value == "":
        return 0
    return int(value) * NS_PER_S


def epoch_to_ns(value: Any) -> int:
    """Unix epoch of unknown unit (s, ms, us or ns, inferred by magnitude) or ISO string -> ns.

    Used only where a source's unit is undocumented (CF Benchmarks passthrough). Thresholds
    are valid for dates between 1973 and 2286.
    """
    if value is None or value == "":
        return 0
    if isinstance(value, str) and not _looks_numeric(value):
        return iso_to_ns(value)
    v = Decimal(str(value))
    a = abs(v)
    if a >= Decimal(10) ** 17:
        return int(v)  # ns
    if a >= Decimal(10) ** 14:
        return int(v * 1_000)  # us
    if a >= Decimal(10) ** 11:
        return int(v * 1_000_000)  # ms
    return int(v * NS_PER_S)  # s


def _looks_numeric(s: str) -> bool:
    try:
        Decimal(s)
        return True
    except (InvalidOperation, ValueError):
        return False


def opt_px(value: Any) -> int:
    """Nullable fixed-point dollar price -> Px (1e-4 $); 0 when absent."""
    if value is None or value == "":
        return 0
    return px_from_dollars(str(value))


def opt_qty(value: Any) -> int:
    """Nullable fixed-point count ('10.00') -> Qty (0.01 contract, signed); 0 when absent."""
    if value is None or value == "":
        return 0
    return qty_from_fp(str(value))


def opt_micros(value: Any) -> int:
    """Nullable fixed-point dollar amount (up to 6 dp) -> Micros; 0 when absent."""
    if value is None or value == "":
        return 0
    return micros_from_dollars(str(value))


def number_to_str(value: Any) -> str | None:
    """JSON number (int/float) or numeric str -> canonical decimal string, None stays None.

    Floats are rendered with Python's shortest round-trip repr, so 1.5 -> '1.5', 1 -> '1'.
    """
    if value is None:
        return None
    if isinstance(value, bool):
        raise TypeError("boolean is not a number")
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        d = Decimal(repr(value))
    else:
        d = Decimal(str(value))
    if d == d.to_integral_value():
        return str(d.quantize(Decimal(1)))
    return format(d.normalize(), "f")


def as_dict(value: Any) -> dict[str, Any]:
    """value if it is a JSON object, else {} (defensive access to optional sub-objects)."""
    return value if isinstance(value, dict) else {}


def to_decimal(value: Any) -> Decimal:
    """Exact Decimal from a JSON number/str (floats via shortest repr, never binary expansion)."""
    if isinstance(value, bool):
        raise TypeError("boolean is not a number")
    if isinstance(value, float):
        return Decimal(repr(value))
    return Decimal(str(value))


def to_float(value: Any) -> float | None:
    """Model-side float from str/number; None when absent/unparseable."""
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


API_PATH_PREFIXES = ("/trade-api/v2",)


def normalize_route(path: str) -> str:
    """'/trade-api/v2/markets/X?y=1' -> '/markets/X' (API-relative, no query, no trailing '/')."""
    p = path.split("?", 1)[0]
    for pre in API_PATH_PREFIXES:
        if p.startswith(pre + "/") or p == pre:
            p = p[len(pre) :]
    if len(p) > 1:
        p = p.rstrip("/")
    return p or "/"
