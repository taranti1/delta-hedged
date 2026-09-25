"""Kalshi BTC settlement convention: settlement time T, window, rounding, 15-minute strikes.

VERIFIED 2026-09-25 (docs/research/M1_2_SETTLEMENT_CHECK.md, docs/kalshi_specs/samples_2026-09-25):

* **T = close_time**, not ``expected_expiration_time``. On every KXBTCD / KXBTC / KXBTC15M market
  ``expected_expiration_time`` (and ``occurrence_datetime``) is close_time + 5 min and
  ``latest_expiration_time`` / ``expiration_time`` close + 7 days; those are exchange processing
  deadlines, not the reference time. ``rules_primary``: "the simple average of the sixty seconds
  of ... BRTI before 3 PM EDT" where close_time is 3 PM EDT. KXBTCD-26SEP2515 and
  KXBTC15M-26SEP251500 (both close 19:00Z) settled on the same ``expiration_value`` 83950.62, and
  the 15-minute one was settled at 19:00:08Z (before close + 5 min).
* **Window** [T - 60 s, T): the 60 once-per-second BRTI prints whose CF source times are
  T-60 s, ..., T-1 s ("the sixty seconds of BRTI before" T; ``dh.core.market.SettlementSpec``
  include_close_tick=False). VERIFIED to the cent on every recorded expiration, whereas the
  (T-60 s, T] window -- which Kalshi's streamed ``last_60s_windowed_average_15min`` uses --
  matched 1 of 11 (docs/research/M1_2_SETTLEMENT_CHECK.md).
* **Rounding**: the published ``expiration_value`` has 2 decimals on all three series (KXBTC15M
  ``rules_secondary``: "rounded to the nearest 2 decimal places"); the outcome compares that
  rounded value with the strike (``SettlementSpec.round_decimals``).
* **Strikes**: KXBTCD ``greater`` ("above", strict; strikes at X.99); KXBTC ranges ``between``
  (inclusive) plus ``greater`` / ``less`` tails; KXBTC15M ``greater_or_equal`` ("at least")
  against the PREVIOUS quarter's expiration value (its ``floor_strike`` once set).
* **Missing / incomplete data resolves No** (contract terms BTC.pdf, CRYPTO.pdf): a benchmark gap
  inside the window is a No-risk condition (``WindowState.n_missing``), not something to fill.

This module derives T independently from the rules text and from the event ticker (Kalshi event
tickers encode the NEW YORK local time: ``KXBTCD-26SEP2515`` = 2026-09-25 15:00 ET,
``KXBTC15M-26SEP251500`` = 15:00 ET) so that ``dh.kalshi.normalize.rest_market_to_spec`` can
require both to agree with ``close_time`` and refuse a market whose settlement time is
ambiguous. All functions are pure.
"""

from __future__ import annotations

import datetime as _dt
import re
from zoneinfo import ZoneInfo

from dh.core.market import round_half_up  # noqa: F401  (re-export; defined in core to avoid an import cycle)
from dh.core.units import NS_PER_S

NY = ZoneInfo("America/New_York")
_MONTHS = {m: i for i, m in enumerate(("JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"), 1)}
_TZ_OFFSET_H = {"EDT": -4, "EST": -5}
# "before 3 PM EDT", "before 3:00 PM EDT", "before 11:45 AM EST", "before 5pm ET"
_RX_BEFORE = re.compile(r"\bbefore\s+(\d{1,2})(?::(\d{2}))?\s*([AaPp])\.?\s*[Mm]\.?\s+(EDT|EST|ET)\b")
# "on Sep 25, 2026" / "on September 25, 2026"
_RX_DATE = re.compile(r"\bon\s+([A-Za-z]{3,9})\.?\s+(\d{1,2}),?\s+(\d{4})\b")
# event-ticker time codes: YYMONDDHH (hourly series) or YYMONDDHHMM (15-minute series)
_RX_TICKER = re.compile(r"^(\d{2})([A-Z]{3})(\d{2})(\d{2})(\d{2})?$")
# series whose event ticker carries HH (hourly) or HHMM (15 min) in New York time
TICKER_TIME_DIGITS = {"KXBTCD": 2, "KXBTC": 2, "KXBTC15M": 4}


def _to_ns(d: _dt.datetime) -> int:
    return int(d.timestamp()) * NS_PER_S


def _local_to_ns(year: int, month: int, day: int, hour: int, minute: int, tz: str | None) -> int:
    """Wall-clock time in New York (or a fixed EDT/EST offset) -> UTC ns."""
    naive = _dt.datetime(year, month, day, hour, minute)
    if tz in _TZ_OFFSET_H:
        return _to_ns(naive.replace(tzinfo=_dt.timezone(_dt.timedelta(hours=_TZ_OFFSET_H[tz]))))
    return _to_ns(naive.replace(tzinfo=NY))


def _hour24(h: int, ampm: str) -> int:
    h = h % 12
    return h + 12 if ampm.lower() == "p" else h


def rules_times_ns(rules_primary: str | None) -> list[int]:
    """Every "before <time> <tz> ... on <Mon D, YYYY>" reference in the rules text, in order (UTC ns).

    Each time takes the first date that follows it (KXBTCD: "... before 3 PM EDT is above X at 3 PM
    EDT on Sep 25, 2026"; KXBTC15M: "... before 3:00 PM EDT on Sep 25, 2026 is at least the simple
    average of ... before 2:45 PM EDT on September 25, 2026"). [] when nothing parses.
    """
    text = str(rules_primary or "")
    out: list[int] = []
    for m in _RX_BEFORE.finditer(text):
        d = _RX_DATE.search(text, m.end())
        if d is None:
            continue
        mon = _MONTHS.get(d.group(1)[:3].upper())
        if mon is None:
            continue
        try:
            out.append(_local_to_ns(int(d.group(3)), mon, int(d.group(2)), _hour24(int(m.group(1)), m.group(3)),
                                    int(m.group(2) or 0), m.group(4).upper()))
        except ValueError:
            continue
    return out


def rules_settlement_time_ns(rules_primary: str | None) -> int | None:
    """Settlement reference time T stated by ``rules_primary`` (the first "before <time>"), or None."""
    ts = rules_times_ns(rules_primary)
    return ts[0] if ts else None


def rules_strike_reference_time_ns(rules_primary: str | None) -> int | None:
    """KXBTC15M: the time of the average the market is compared with (the second "before <time>";
    the previous quarter's close), or None."""
    ts = rules_times_ns(rules_primary)
    return ts[1] if len(ts) >= 2 else None


def ticker_settlement_time_ns(event_ticker: str | None, series: str | None = None) -> int | None:
    """Settlement time encoded in a Kalshi BTC event ticker (New York local time), or None.

    Only the known formats are parsed: KXBTCD / KXBTC ``-YYMONDDHH``, KXBTC15M ``-YYMONDDHHMM``.
    Anything else (other series, synthetic tickers) returns None.
    """
    et = str(event_ticker or "")
    parts = et.split("-")
    if len(parts) < 2:
        return None
    ser = series or parts[0]
    digits = TICKER_TIME_DIGITS.get(ser)
    if digits is None or parts[0] != ser:
        return None
    m = _RX_TICKER.match(parts[1])
    if m is None:
        return None
    has_min = m.group(5) is not None
    if (digits == 4) != has_min:
        return None
    mon = _MONTHS.get(m.group(2))
    if mon is None:
        return None
    try:
        return _local_to_ns(2000 + int(m.group(1)), mon, int(m.group(3)), int(m.group(4)),
                            int(m.group(5) or 0), None)
    except ValueError:
        return None


def check_settlement_time(market: dict, close_ns: int, series: str | None = None) -> tuple[str, list[str]]:
    """Cross-check ``close_time`` (= T) against the rules text and the event ticker.

    The rules text is the contract's definition, so it is binding: a parseable time that differs
    from close_time is a 'mismatch' (the market must not be traded). The event-ticker time is a
    naming convention: it is binding only when the rules text states no parseable time; when the
    rules verify T, a disagreeing ticker is reported as 'verified' with a detail line (callers may
    log it; ``dh.kalshi.metadata.rules_flags`` flags it informationally).
    Returns (status, details) with status 'verified' | 'unverified' (nothing parsed) | 'mismatch'.
    ``expected_expiration_time`` is deliberately NOT a source: it is close + 5 min.
    """
    details: list[str] = []
    r = rules_settlement_time_ns(market.get("rules_primary"))
    et = market.get("event_ticker") or "-".join(str(market.get("ticker", "")).split("-")[:-1])
    t = ticker_settlement_time_ns(str(et), series)
    if t is not None and t != close_ns:
        details.append(f"event ticker time {t // NS_PER_S} != close_time {close_ns // NS_PER_S}")
    if r is not None:
        if r != close_ns:
            return "mismatch", [f"rules_primary time {r // NS_PER_S} != close_time {close_ns // NS_PER_S}", *details]
        return "verified", details
    if t is None:
        return "unverified", details
    return ("mismatch" if details else "verified"), details


__all__ = [
    "rules_times_ns",
    "rules_settlement_time_ns",
    "rules_strike_reference_time_ns",
    "ticker_settlement_time_ns",
    "check_settlement_time",
    "round_half_up",
    "TICKER_TIME_DIGITS",
]
