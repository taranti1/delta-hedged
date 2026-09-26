"""Risk state carried across restarts (audit live C1; live review N1, N3, N7, N8).

A restart must never hand the strategy a fresh daily-loss budget, forget a Halt or cut a
pause short. It must not invent losses either: a pessimistic seed halts ordinary restarts for
the rest of the day.

The day's P&L (UTC day D, YES terms, dollars):

    real       = realized + open value
    realized   = today's fills cash (buy YES -px*q, sell YES +px*q) - their fees
               + today's settlements ((yes_count - no_count) * YES payout)
               - value of the positions held at 00:00 UTC, at the last trade at or before
                 midnight (fixed for the day)
    open value = the positions held now: a market with a result at its payout; an open market
                 at exchange prices (long YES at the YES bid, short YES at the YES ask); a
                 market past its close_time without a result (closed, awaiting
                 determination) at the payout our own BRTI prints of its settlement window
                 imply (``mark_source`` own_benchmark; the window's prints are back-filled with
                 one CF passthrough call per hour, ``backfill_window_prints``), at the worst case
                 when a print is missing or the value is within $0.01 of a strike, at the last
                 trade when the window cannot be evaluated (dh.settlement.closemark). REST bid /
                 ask are never used after close_time (stale pre-close quotes, then 0 / 1.00).

A price that does not exist falls back to the worst case (open: long $0, short $1; midnight:
long $1, short $0) and is logged. Every open position's price source is in
``DayPnl.price_src`` ("open:<ticker>": "<mark_source>: why") and ``DayPnl.mark_source``. A
settlement row's ``fee_cost`` is not added: the spec describes it as the total fees paid (next
to the position's cost basis), not as a charge at settlement; trading fees are counted once,
from today's fills.

Sources at start-up
  * the PERSISTED state of earlier sessions (``RiskStateStore``; written every
    ``loop.risk_state_interval_s``, on every Halt (fsync) and at shutdown): the real day P&L,
    its realized part and the open positions' mark at that moment, the halt (reason, scope,
    the UTC day it was decided), the pause and an operator's loss-budget base;
  * live only, Kalshi REST (``derive_day_pnl``): today's fills (GET /portfolio/fills, plus
    GET /historical/fills for the part before GET /historical/cutoff's trades_created_ts, both
    with the explicit subaccount), settlements, positions and the prices above. Every fill /
    settlement row of the configured series must parse and carry its time, or the start-up is
    refused (RiskStateError -> exit 2); rows of other series are skipped and reported.

The seed (dh.core.events.RiskStateSeed, the strategy's first event, recorded for replay):
    realized = min(persisted realized, REST realized)       (live; paper: persisted)
    real     = realized + the FRESH open value                (a persisted mark is never reused)
    seed     = real - budget base                             (what counts toward the loss limit)
A halt is carried when its reason is sticky (anything but the daily loss: position
reconciliation, fee mismatch, the watchdog's cancel-all) or when it was decided on the same
UTC day: a daily-loss halt ends at midnight even if the halted runner kept running past it.
``--reset-daily-halt`` clears the halt and the pause and starts a fresh loss budget from the
current real P&L (the limit applies to losses after the reset); the real P&L stays recorded.

During a session the runner keeps a ``RiskBook`` of what the strategy's own equity does not
cover: the seed's realized P&L, the positions of events excluded from the session (valued at
their start-up marks) and the budget base. When an excluded market settles, the book realizes
it and the runner pushes an updated RiskStateSeed, so the loss limit and the persisted state
see the settlement at once (and the stale mark is gone).
"""

from __future__ import annotations

import json
import logging
import os
import time
from collections.abc import Iterable
from dataclasses import asdict, dataclass, field, fields
from pathlib import Path
from typing import Any

from dh.core.units import NS_PER_S, PX_SCALE, px_from_dollars, qty_from_fp
from dh.kalshi.wire import opt_iso_to_ns
from dh.settlement.closemark import (
    EXCHANGE_QUOTE,
    LAST_TRADE,
    RESULT_REST,
    WORST_CASE,
    WindowOutcome,
    close_mark,
    evaluate_window,
)

log = logging.getLogger("dh.live.riskstate")

DAY_NS = 86_400 * NS_PER_S
CARRIED = "carried_over:"
MARKETS_PER_CALL = 50  # GET /markets?tickers=... batch size
MIDNIGHT_LOOKBACK_S = (900, 6 * 3600, 3 * 86_400)  # windows searched for the last trade before 00:00


def day_start(ts_ns: int) -> int:
    return ts_ns - ts_ns % DAY_NS


def day_str(ts_ns: int) -> str:
    return time.strftime("%Y-%m-%d", time.gmtime(ts_ns // NS_PER_S))


def base_reason(reason: str) -> str:
    """Strip 'carried_over:' prefixes (a carried halt keeps its original reason)."""
    r = reason or ""
    while r.startswith(CARRIED):
        r = r[len(CARRIED):]
    return r


def sticky(reason: str) -> bool:
    """A halt that survives the UTC day boundary (everything but the daily-loss halt)."""
    return bool(reason) and not base_reason(reason).startswith("daily_loss")


class RiskStateError(RuntimeError):
    """The risk state cannot be established (unreadable file, malformed REST rows, REST
    failure): the live runner refuses to start (exit 2)."""


# ============================================================================ persistence
@dataclass
class RiskState:
    day_start_ns: int = 0
    day_pnl_usd: float = 0.0  # the day's REAL P&L (realized + mark), reporting
    halted: bool = False
    halt_reason: str = ""
    pause_until_ns: int = 0
    session: str = ""
    mode: str = ""
    updated_ns: int = 0
    realized_usd: float | None = None  # day_pnl_usd without the open positions (None: written before the split)
    mark_usd: float = 0.0  # value of the open positions at updated_ns (a restart re-values them)
    budget_base_usd: float = 0.0  # real P&L at an operator reset: the daily-loss limit counts from here
    halt_scope: str = ""  # all | quoting
    halt_day_ns: int = 0  # the UTC day (start ns) on which the halt was decided


def fsync_dir(path: str | Path) -> None:
    """fsync a directory so a rename inside it survives a power loss."""
    try:
        fd = os.open(str(path), os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:  # some filesystems do not support it
        pass
    finally:
        os.close(fd)


class RiskStateStore:
    """One JSON file, replaced atomically (tmp + fsync + rename + directory fsync)."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.last: RiskState | None = None

    def load(self) -> RiskState | None:
        try:
            d = json.loads(self.path.read_text())
        except FileNotFoundError:
            return None
        except (OSError, ValueError) as exc:
            raise RiskStateError(f"risk state file {self.path} is unreadable ({exc}): fix or remove it "
                                 "deliberately (it carries halts across restarts)") from exc
        if not isinstance(d, dict):
            raise RiskStateError(f"risk state file {self.path}: expected an object")
        known = {k: d[k] for k in RiskState.__dataclass_fields__ if k in d}
        try:
            return RiskState(**known)
        except TypeError as exc:
            raise RiskStateError(f"risk state file {self.path}: {exc}") from exc

    def save(self, st: RiskState, *, fsync: bool = False) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_name(self.path.name + f".tmp{os.getpid()}")
        data = json.dumps(asdict(st), sort_keys=True)
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(data)
            if fsync:
                f.flush()
                os.fsync(f.fileno())
        os.replace(tmp, self.path)
        if fsync:
            fsync_dir(self.path.parent)  # the rename itself must survive a power loss
        self.last = st


# ============================================================================ REST rows
def row_in_subaccount(row: dict[str, Any], sub: int) -> bool:
    """A REST row (Fill / Order / Settlement) fetched with ``subaccount=<sub>`` belongs to
    ``sub``: its subaccount field matches, or it is absent (omitted for the primary account;
    the explicit query parameter scoped the request). An explicit other value is dropped."""
    from dh.kalshi.normalize import subaccount_of

    sa = subaccount_of(row)
    if sa == sub:
        return True
    explicit = row.get("subaccount") is not None or row.get("subaccount_number") is not None
    return not explicit and sa == 0 and sub != 0


def _rid(row: Any) -> str:
    if not isinstance(row, dict):
        return repr(row)[:60]
    return str(row.get("fill_id") or row.get("trade_id") or row.get("ticker") or "?")[:60]


def row_ticker(row: Any) -> str:
    """Market ticker of a REST Fill / Settlement row ('' if absent)."""
    if not isinstance(row, dict):
        return ""
    return str(row.get("ticker") or row.get("market_ticker") or "")


def in_series(ticker: str, series: Iterable[str] | None) -> bool:
    """``ticker`` belongs to one of ``series`` (None / empty = no filter)."""
    if not series:
        return True
    return ticker.split("-", 1)[0] in set(series)


def parse_fill_row(row: Any, subaccount: int, *, historical: bool = False) -> Any:
    """REST Fill row -> KalshiFill, or None when it is another subaccount's. A malformed row, or
    one without its time, raises RiskStateError (the day's P&L would be wrong).

    Live and historical rows are attributed alike: both reads pass ``subaccount`` explicitly
    (GET /historical/fills takes it since openapi 3.31.0; omitted = all subaccounts), so a row
    without a subaccount field is the requested subaccount's and a row naming another one is
    dropped (``row_in_subaccount``). ``historical`` is kept for the caller's statistics."""
    from dh.kalshi.normalize import rest_fill_to_event

    if not isinstance(row, dict):
        raise RiskStateError(f"malformed fill row {_rid(row)}")
    if not row_in_subaccount(row, subaccount):
        return None
    try:
        f = rest_fill_to_event(row, 0)
    except (KeyError, ValueError, TypeError, ArithmeticError) as exc:
        raise RiskStateError(f"malformed fill row {_rid(row)}: {type(exc).__name__}: {exc}") from exc
    if not (row.get("fill_id") or row.get("trade_id")):
        raise RiskStateError(f"fill row without fill_id / trade_id ({f.ticker})")
    if not (row.get("ticker") or row.get("market_ticker")):
        raise RiskStateError(f"fill row {_rid(row)} without a ticker")
    if f.ts_exch <= 0:
        raise RiskStateError(f"fill row {_rid(row)} without created_time: cannot tell whether it is today's")
    if f.qty <= 0 or not 0 <= f.yes_px <= PX_SCALE:
        raise RiskStateError(f"fill row {_rid(row)} with count {row.get('count_fp')!r} / price {f.yes_px}")
    return f


def _settle_px(row: dict[str, Any]) -> int | None:
    """YES payout of a settlement in px units (10_000 = $1), None if unknown."""
    v = row.get("value")
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        px = int(round(float(v) * PX_SCALE / 100))  # cents -> px
        return px if 0 <= px <= PX_SCALE else None
    res = str(row.get("market_result") or "").lower()
    if res == "yes":
        return PX_SCALE
    if res == "no":
        return 0
    return None


def parse_settlement_row(row: Any, subaccount: int) -> tuple[str, int, int, int] | None:
    """REST Settlement row -> (ticker, yes_count - no_count, YES payout px, settled ns), or None
    when it is another subaccount's. Malformed / timeless / unknown payout -> RiskStateError."""
    if not isinstance(row, dict):
        raise RiskStateError(f"malformed settlement row {_rid(row)}")
    if not row_in_subaccount(row, subaccount):
        return None
    tk = str(row.get("ticker") or "")
    if not tk:
        raise RiskStateError("settlement row without a ticker")
    try:
        t = opt_iso_to_ns(row.get("settled_time"))
        q = qty_from_fp(str(row["yes_count_fp"])) - qty_from_fp(str(row["no_count_fp"]))
    except (KeyError, ValueError, TypeError, ArithmeticError) as exc:
        raise RiskStateError(f"malformed settlement row {tk}: {type(exc).__name__}: {exc}") from exc
    if t <= 0:
        raise RiskStateError(f"settlement row {tk} without settled_time: cannot tell whether it is today's")
    px = _settle_px(row)
    if px is None:
        raise RiskStateError(f"settlement row {tk}: payout unknown (market_result {row.get('market_result')!r}, "
                             f"value {row.get('value')!r})")
    return tk, q, px, t


@dataclass
class DayFlows:
    """Today's own activity from REST rows (see ``day_flows``)."""

    day_start_ns: int
    flow: dict[str, int] = field(default_factory=dict)  # ticker -> signed YES qty traded today
    settled: dict[str, int] = field(default_factory=dict)  # ticker -> yes - no qty settled today
    cash_micros: int = 0
    fee_micros: int = 0
    settle_micros: int = 0
    fills: int = 0
    historical_fills: int = 0
    settlements: int = 0
    foreign: dict[str, int] = field(default_factory=dict)  # ticker outside the series -> rows skipped

    def midnight_positions(self, positions: dict[str, int]) -> dict[str, int]:
        """Positions held at 00:00 UTC = now - today's fills + today's settled quantities."""
        out = {}
        for tk in sorted(set(positions) | set(self.flow) | set(self.settled)):
            q0 = int(positions.get(tk, 0)) - self.flow.get(tk, 0) + self.settled.get(tk, 0)
            if q0:
                out[tk] = q0
        return out


def day_flows(day_start_ns: int, fills: Iterable[Any], settlements: Iterable[Any], *, subaccount: int = 0,
              historical_fills: Iterable[Any] = (), series: Iterable[str] | None = None) -> DayFlows:
    """Parse and validate today's fills (live + historical, de-duplicated by fill / trade id)
    and settlements. Rows before ``day_start_ns`` are skipped; bad rows raise RiskStateError.
    Rows of a market outside ``series`` (e.g. a manual trade on the subaccount) are skipped and
    counted in ``foreign`` BEFORE they are parsed: they never refuse the start."""
    out = DayFlows(day_start_ns)
    seen: set[str] = set()
    series = tuple(series or ())
    for hist, rows in ((False, fills), (True, historical_fills)):
        for row in rows:
            tk = row_ticker(row)
            if tk and not in_series(tk, series):
                out.foreign[tk] = out.foreign.get(tk, 0) + 1
                continue
            f = parse_fill_row(row, subaccount, historical=hist)
            if f is None:
                continue
            ids = {str(row.get("fill_id") or ""), str(row.get("trade_id") or "")} - {""}
            if ids & seen:
                continue
            seen |= ids
            if f.ts_exch < day_start_ns:
                continue
            sgn = 1 if f.book_side == "bid" else -1
            out.flow[f.ticker] = out.flow.get(f.ticker, 0) + sgn * f.qty
            out.cash_micros -= sgn * f.yes_px * f.qty  # px (1e-4 $) * qty (1e-2) = 1e-6 $
            out.fee_micros += f.fee_micros
            out.fills += 1
            out.historical_fills += int(hist)
    for row in settlements:
        tk = row_ticker(row)
        if tk and not in_series(tk, series):
            out.foreign[tk] = out.foreign.get(tk, 0) + 1
            continue
        s = parse_settlement_row(row, subaccount)
        if s is None:
            continue
        tk, q, px, t = s
        if t < day_start_ns:
            continue
        out.settled[tk] = out.settled.get(tk, 0) + q
        out.settle_micros += px * q
        out.settlements += 1
    return out


@dataclass
class DayPnl:
    """The day's P&L from Kalshi (module docstring). Positions in 0.01 contracts, px in 1e-4 $."""

    day_start_ns: int
    pnl_usd: float = 0.0  # real = realized + open
    realized_usd: float = 0.0
    open_usd: float = 0.0  # positions held now, at exchange prices
    midnight_usd: float = 0.0  # positions held at 00:00 UTC, at the last trade at or before it
    cash_usd: float = 0.0
    fees_usd: float = 0.0
    settlements_usd: float = 0.0
    fills: int = 0
    historical_fills: int = 0
    settlements: int = 0
    positions_now: dict[str, int] = field(default_factory=dict)
    positions_midnight: dict[str, int] = field(default_factory=dict)
    open_px: dict[str, int] = field(default_factory=dict)  # ticker -> px the open position is valued at
    midnight_px: dict[str, int] = field(default_factory=dict)
    price_src: dict[str, str] = field(default_factory=dict)  # "open:<t>" / "midnight:<t>" -> where the price came from
    mark_source: dict[str, str] = field(default_factory=dict)  # ticker -> source of its open mark (closemark names)
    window_backfill: dict[str, Any] = field(default_factory=dict)  # the CF back-fill of closed markets' windows
    fallbacks: list[str] = field(default_factory=list)  # prices that did not exist: worst case used
    foreign: dict[str, int] = field(default_factory=dict)  # fill/settlement rows outside the series (skipped)

    # the open positions' specs (from their Market rows): the runner re-marks an excluded
    # position when its market closes during the session. Not part of the summary.
    specs: dict[str, Any] = field(default_factory=dict, repr=False)

    def summary(self) -> dict[str, Any]:
        d = asdict(self)
        d.pop("specs", None)
        for k in ("pnl_usd", "realized_usd", "open_usd", "midnight_usd", "cash_usd", "fees_usd", "settlements_usd"):
            d[k] = round(d[k], 6)
        return d


def day_pnl_from_flows(fl: DayFlows, positions: dict[str, int], *, open_px: dict[str, int] | None = None,
                       midnight_px: dict[str, int] | None = None, price_src: dict[str, str] | None = None) -> DayPnl:
    """Value today's flows and positions (module docstring). ``open_px`` / ``midnight_px``:
    {ticker: px}; a missing price falls back to the worst case and is listed in ``fallbacks``
    (with the reason ``price_src`` gives)."""
    out = DayPnl(fl.day_start_ns, fills=fl.fills, historical_fills=fl.historical_fills, settlements=fl.settlements,
                 price_src=dict(price_src or {}), foreign=dict(fl.foreign))
    open_px = open_px or {}
    midnight_px = midnight_px or {}
    open_micros = mid_micros = 0
    for tk, q in sorted(positions.items()):
        if not q:
            continue
        out.positions_now[tk] = int(q)
        px = open_px.get(tk)
        why = out.price_src.get(f"open:{tk}", "")
        if px is None:
            px = 0 if q > 0 else PX_SCALE
            reason = why.split(": ", 1)[1] if why.startswith(WORST_CASE + ": ") else (why or "no exchange price")
            out.fallbacks.append(f"{tk}: open {q / 100:+.2f} valued at ${px / PX_SCALE:.2f} (worst case: {reason})")
            out.mark_source[tk] = WORST_CASE
        else:
            out.mark_source[tk] = why.split(":", 1)[0] if why else ""
        out.open_px[tk] = px
        open_micros += q * px
    for tk, q0 in fl.midnight_positions(positions).items():
        out.positions_midnight[tk] = q0
        px = midnight_px.get(tk)
        if px is None:
            px = PX_SCALE if q0 > 0 else 0
            out.fallbacks.append(f"{tk}: {q0 / 100:+.2f} held at 00:00 UTC valued at ${px / PX_SCALE:.2f} "
                                 "(no trade at or before midnight)")
        out.midnight_px[tk] = px
        mid_micros += q0 * px
    out.cash_usd = fl.cash_micros / 1e6
    out.fees_usd = fl.fee_micros / 1e6
    out.settlements_usd = fl.settle_micros / 1e6
    out.open_usd = open_micros / 1e6
    out.midnight_usd = mid_micros / 1e6
    out.realized_usd = out.cash_usd - out.fees_usd + out.settlements_usd - out.midnight_usd
    out.pnl_usd = out.realized_usd + out.open_usd
    return out


def day_pnl_from_rows(day_start_ns: int, fills: Iterable[Any], settlements: Iterable[Any], positions: dict[str, int],
                      *, subaccount: int = 0, historical_fills: Iterable[Any] = (), open_px: dict[str, int] | None = None,
                      midnight_px: dict[str, int] | None = None, series: Iterable[str] | None = None) -> DayPnl:
    """Pure form of ``derive_day_pnl`` (tests feed rows and prices directly)."""
    fl = day_flows(day_start_ns, fills, settlements, subaccount=subaccount, historical_fills=historical_fills,
                   series=series)
    return day_pnl_from_flows(fl, positions, open_px=open_px, midnight_px=midnight_px)


# ============================================================================ prices
def _px(v: Any) -> int | None:
    if v in (None, ""):
        return None
    try:
        return px_from_dollars(str(v))
    except (ValueError, TypeError, ArithmeticError):
        return None


def payout_px(result: str, settlement_value: Any = None) -> int | None:
    """YES payout (px) of a determined market: its settlement value (dollars) when given, else
    from the result; None when neither says."""
    p = _px(settlement_value)
    if p is not None:
        return p
    r = str(result or "").lower()
    return PX_SCALE if r == "yes" else 0 if r == "no" else None


CLOSED_STATUSES = ("closed", "determined", "disputed", "amended", "finalized", "settled")


def row_close_ns(m: dict[str, Any]) -> int:
    """close_time of a Market row (ns), 0 if absent / unparseable."""
    try:
        return opt_iso_to_ns(m.get("close_time"))
    except (TypeError, ValueError):
        return 0


def row_result(m: dict[str, Any]) -> str:
    """The Market row's result ('yes' / 'no' / 'scalar'), '' when there is none yet."""
    r = str(m.get("result") or "").lower()
    return r if r in ("yes", "no", "scalar") else ""


def row_closed(m: dict[str, Any], now_ns: int | None) -> bool:
    """The market no longer trades: its close_time has passed (``now_ns``), or its status says
    so (closed / determined / finalized ...; the only test without ``now_ns``)."""
    close = row_close_ns(m)
    if now_ns is not None and close and now_ns >= close:
        return True
    return str(m.get("status") or "").lower() in CLOSED_STATUSES


def row_spec(m: dict[str, Any]) -> Any:
    """MarketSpec of a Market row for the settlement evaluation (strike, close_time, the
    series' settlement convention; the tick grid does not matter here), None when the row
    does not define one (another series, unsupported strike, rules_primary naming another
    settlement time than close_time)."""
    from dh.core.market import MarketSpec
    from dh.kalshi.normalize import default_settlement, rest_market_to_spec, series_of
    from dh.settlement.convention import rules_settlement_time_ns

    try:
        return rest_market_to_spec(m)
    except (KeyError, ValueError, TypeError):  # UnsupportedMarket is a ValueError (e.g. no tick grid in the row)
        pass
    try:
        series = series_of(m, None, None)
        settle = default_settlement(series)
        close = row_close_ns(m)
        if settle is None or not close:
            return None
        rules_t = rules_settlement_time_ns(m.get("rules_primary"))
        if rules_t is not None and rules_t != close:
            return None
        floor, cap = m.get("floor_strike"), m.get("cap_strike")
        return MarketSpec(ticker=str(m["ticker"]), event_ticker=str(m.get("event_ticker") or ""), series_ticker=series,
                          strike_type=str(m.get("strike_type") or ""), floor_strike=None if floor is None else float(floor),
                          cap_strike=None if cap is None else float(cap), open_ts=0, close_ts=close, expiration_ts=close,
                          settlement=settle)
    except (KeyError, ValueError, TypeError):
        return None


def mark_px(m: dict[str, Any], q: int, *, now_ns: int | None = None,
            outcome: WindowOutcome | None = None) -> tuple[int | None, str]:
    """(px, "<mark_source>: why") an open position of ``q`` is worth according to Market object
    ``m`` (module docstring; dh.settlement.closemark):

      a result             -> its payout (result_rest)
      past close_time (``now_ns``) or a closed status, no result
                           -> the payout our own prints of the window imply (``outcome``:
                              own_benchmark); a missing print / a value within $0.01 of a strike
                              -> (None, worst_case: why); the window not evaluable -> the last
                              trade (last_trade), else (None, worst_case: why). REST bid / ask
                              are NEVER used after the close
      open                 -> long at the YES bid, short at the YES ask (exchange_quote); a
                              market that is not active (paused) without one at its last trade

    (None, why) = no price: the caller values the position at the worst case and logs why."""
    res = row_result(m)
    if res:
        p = payout_px(res, m.get("settlement_value_dollars"))
        if p is not None:
            return p, f"{RESULT_REST}: payout ({res})"
    status = str(m.get("status") or "").lower()
    last = _px(m.get("last_price_dollars"))
    if row_closed(m, now_ns):
        cm = close_mark(q, outcome, last_trade_px=last)
        if cm.source == WORST_CASE:
            return None, f"{WORST_CASE}: {cm.detail} (status {status or '?'})"
        return cm.px, f"{cm.source}: {cm.detail}"
    p = _px(m.get("yes_bid_dollars" if q > 0 else "yes_ask_dollars"))
    if q > 0 and p is not None and p > 0:
        return p, f"{EXCHANGE_QUOTE}: YES bid"
    if q < 0 and p is not None and 0 < p < PX_SCALE:
        return p, f"{EXCHANGE_QUOTE}: YES ask"
    if status not in ("", "active") and last is not None and 0 < last < PX_SCALE:
        return last, f"{LAST_TRADE}: last trade (status {status})"
    return None, f"{WORST_CASE}: no YES {'bid' if q > 0 else 'ask'} (status {status or '?'})"


WINDOW_CHUNK_NS = 3600 * NS_PER_S  # the CF passthrough's timespan=HOUR


async def backfill_window_prints(rest: Any, specs: Iterable[Any]) -> tuple[Any, dict[str, Any]]:
    """The BRTI prints of the settlement windows of ``specs`` (closed markets without a result)
    from the CF Benchmarks passthrough: ONE call per hour the windows touch (normally one),
    GET /cfbenchmarks/history/values?id=BRTI&timespan=HOUR&timestamp=<hour START, ISO ms>
    (the format VERIFIED LIVE on 2026-09-25: the hour's 5 Hz ticks), fed as the 5 Hz stream into
    a fresh SettlementTracker: a print is final once a later tick exists. CF publishes with a
    delay (up to 15 min), so a window at the end of the published data stays not evaluable (its
    mark falls back). Failures are reported in the info, never raised. Returns (tracker, info)."""
    from dataclasses import replace as _replace

    from dh.kalshi.normalize import cf_history_to_ticks
    from dh.live.startup import _iso_ms
    from dh.settlement.window import SettlementTracker

    specs = [s for s in specs if s is not None]
    index_id = specs[0].settlement.index_id if specs else "BRTI"
    tracker = SettlementTracker(index_id=index_id, retain_s=4 * 3600.0)
    chunk = WINDOW_CHUNK_NS
    hours: set[int] = set()
    for s in specs:
        obs = s.settlement.obs_times(s.expiration_ts)
        hours.update({obs[0] - obs[0] % chunk, obs[-1] - obs[-1] % chunk})
    info: dict[str, Any] = {"hours": sorted(h // NS_PER_S for h in hours), "calls": 0, "ticks": 0, "errors": []}
    for h in sorted(hours):
        info["calls"] += 1
        try:
            body = await rest.get_cfbenchmarks_history(index_id, timespan="HOUR", timestamp=_iso_ms(h))
        except Exception as exc:  # noqa: BLE001 - unavailable history: the marks fall back
            info["errors"].append(f"hour {h // NS_PER_S}: {type(exc).__name__}: {exc}"[:300])
            continue
        for t in cf_history_to_ticks(body, h, index_id):
            if h <= t.ts_exch < h + chunk:
                tracker.on_index(_replace(t, feed="5hz"))
                info["ticks"] += 1
    return tracker, info


@dataclass
class OpenMarks:
    """The open positions' prices (``open_marks``)."""

    px: dict[str, int] = field(default_factory=dict)
    src: dict[str, str] = field(default_factory=dict)  # "open:<t>" -> "<mark_source>: why"
    specs: dict[str, Any] = field(default_factory=dict)  # ticker -> MarketSpec (from its row)
    outcomes: dict[str, WindowOutcome] = field(default_factory=dict)  # closed, no result: our window evaluation
    backfill: dict[str, Any] = field(default_factory=dict)


async def open_marks(rest: Any, positions: dict[str, int], *, now_ns: int | None = None,
                     window_backfill: bool = False) -> OpenMarks:
    """Prices of the open positions (GET /markets?tickers=..., archived markets via
    GET /historical/markets/{ticker}; ``mark_px``). The markets past their close without a
    result are evaluated from their window's prints (``backfill_window_prints``, one CF call per
    hour) when ``window_backfill`` is true."""
    from dh.kalshi.rest import KalshiHTTPError

    tickers = sorted(t for t, q in positions.items() if q)
    rows: dict[str, dict[str, Any]] = {}
    for i in range(0, len(tickers), MARKETS_PER_CALL):
        body = await rest.get_markets(tickers=tickers[i:i + MARKETS_PER_CALL])
        for m in (body or {}).get("markets") or []:
            if isinstance(m, dict) and m.get("ticker"):
                rows[str(m["ticker"])] = m
    for t in tickers:
        if t in rows:
            continue
        try:
            body = await rest.get_historical_market(t)
        except KalshiHTTPError as exc:
            if exc.status == 404:
                continue
            raise
        m = (body or {}).get("market")
        if isinstance(m, dict):
            rows[t] = m
    out = OpenMarks()
    for t in tickers:
        sp = row_spec(rows[t]) if t in rows else None
        if sp is not None:
            out.specs[t] = sp
    waiting = [t for t in tickers if t in out.specs and not row_result(rows[t]) and row_closed(rows[t], now_ns)]
    if waiting and window_backfill:
        tracker, out.backfill = await backfill_window_prints(rest, [out.specs[t] for t in waiting])
        out.backfill["markets"] = waiting
        for t in waiting:
            out.outcomes[t] = evaluate_window(out.specs[t], tracker)
    for t in tickers:
        m = rows.get(t)
        if m is None:
            p, why = None, f"{WORST_CASE}: market not found"
        else:
            p, why = mark_px(m, positions[t], now_ns=now_ns, outcome=out.outcomes.get(t))
        out.src[f"open:{t}"] = why
        if p is not None:
            out.px[t] = p
    return out


async def last_trade_px(rest: Any, ticker: str, at_ns: int, trades_cutoff_ns: int) -> tuple[int | None, str]:
    """Price of the last trade of ``ticker`` at or before ``at_ns`` (GET /markets/trades, or
    /historical/trades for the part before the historical cutoff), searched in widening windows."""
    at_s = at_ns // NS_PER_S
    cut_s = trades_cutoff_ns // NS_PER_S
    for back in MIDNIGHT_LOOKBACK_S:
        lo = at_s - back
        spans: list[tuple[int, int, bool]] = []
        if at_s >= cut_s:
            spans.append((max(lo, cut_s), at_s, False))
        if lo < cut_s:
            spans.append((lo, min(at_s, cut_s), True))
        best: tuple[tuple[int, str], int] | None = None
        for a, b, hist in spans:
            async for tr in rest.iter_trades(ticker=ticker, min_ts=a, max_ts=b, historical=hist):
                if not isinstance(tr, dict):
                    continue
                try:
                    t = opt_iso_to_ns(tr.get("created_time"))
                    p = px_from_dollars(str(tr["yes_price_dollars"]))
                except (KeyError, ValueError, TypeError, ArithmeticError):
                    continue
                if not t or t > at_ns:
                    continue
                k = (t, str(tr.get("trade_id") or ""))
                if best is None or k > best[0]:
                    best = (k, p)
        if best is not None:
            return best[1], f"last trade {time.strftime('%H:%M:%S', time.gmtime(best[0][0] // NS_PER_S))}Z"
    return None, "no trade at or before 00:00 UTC"


async def historical_cutoff(rest: Any) -> dict[str, int]:
    """GET /historical/cutoff -> {field: ns}; trades_created_ts is required."""
    try:
        body = await rest.get_historical_cutoff()
    except Exception as exc:  # noqa: BLE001
        raise RiskStateError(f"GET /historical/cutoff failed ({type(exc).__name__}: {exc}): cannot tell whether "
                             "today's fills are complete") from exc
    out: dict[str, int] = {}
    for k in ("trades_created_ts", "market_settled_ts", "orders_updated_ts", "market_positions_last_updated_ts"):
        v = body.get(k) if isinstance(body, dict) else None
        if v in (None, ""):
            continue
        try:
            out[k] = opt_iso_to_ns(v)
        except (ValueError, TypeError) as exc:
            raise RiskStateError(f"GET /historical/cutoff: unparseable {k} {v!r}") from exc
    if "trades_created_ts" not in out:
        raise RiskStateError(f"GET /historical/cutoff without trades_created_ts ({body!r:.200})")
    return out


async def derive_day_pnl(rest: Any, day_start_ns: int, positions: dict[str, int], *, subaccount: int = 0,
                         series: Iterable[str] | None = None, now_ns: int | None = None,
                         window_backfill: bool = False) -> DayPnl:
    """Today's P&L from Kalshi (module docstring): the historical cutoff, today's fills (live
    and, before the cutoff, historical: both with the explicit ``subaccount``), settlements,
    then the prices of the open positions (``open_marks``: ``now_ns`` tells which markets are
    past their close; ``window_backfill`` back-fills their windows' prints) and of the
    positions held at midnight. Fill and settlement rows outside ``series`` are skipped
    (``DayPnl.foreign``); pass the positions of ``series`` only. Any failure raises
    RiskStateError (a failed window back-fill does not: those marks fall back)."""
    try:
        cut = await historical_cutoff(rest)
        trades_cut = cut["trades_created_ts"]
        min_s = day_start_ns // NS_PER_S
        fills = [f async for f in rest.iter_fills(min_ts=min_s, subaccount=subaccount)]
        hist: list[Any] = []
        if trades_cut > day_start_ns:  # part of today is only in the historical set
            hist = [f async for f in rest.iter_historical_fills(min_ts=min_s, max_ts=-(-trades_cut // NS_PER_S),
                                                                subaccount=subaccount)]
        settles = [s async for s in rest.iter_settlements(min_ts=min_s, subaccount=subaccount)]
        fl = day_flows(day_start_ns, fills, settles, subaccount=subaccount, historical_fills=hist, series=series)
        om = await open_marks(rest, positions, now_ns=now_ns, window_backfill=window_backfill)
        open_px, src = om.px, dict(om.src)
        mid_px: dict[str, int] = {}
        for tk in fl.midnight_positions(positions):
            p, why = await last_trade_px(rest, tk, day_start_ns, trades_cut)
            src[f"midnight:{tk}"] = why
            if p is not None:
                mid_px[tk] = p
    except RiskStateError:
        raise
    except Exception as exc:  # noqa: BLE001 - network / auth / shape: never seed from a partial view
        raise RiskStateError(f"today's P&L could not be derived from Kalshi ({type(exc).__name__}: {exc})") from exc
    out = day_pnl_from_flows(fl, positions, open_px=open_px, midnight_px=mid_px, price_src=src)
    out.specs = dict(om.specs)
    out.window_backfill = dict(om.backfill)
    if om.outcomes:
        out.window_backfill["outcomes"] = {t: {"status": o.status, "value": o.value, "prints": o.n_prints,
                                               "missing": o.n_missing, "detail": o.detail}
                                           for t, o in sorted(om.outcomes.items())}
    return out


# ============================================================================ the seed
@dataclass
class SeedDecision:
    day_start_ns: int
    day_pnl_usd: float  # what the strategy counts toward the daily-loss limit (real - budget base)
    halted: bool
    halt_reason: str
    pause_until_ns: int
    notes: list[str] = field(default_factory=list)
    overridden: dict[str, Any] = field(default_factory=dict)  # what --reset-daily-halt cleared
    real_pnl_usd: float = 0.0  # the day's real P&L (reporting)
    realized_usd: float = 0.0
    mark_usd: float = 0.0
    budget_base_usd: float = 0.0
    halt_scope: str = ""
    halt_day_ns: int = 0


def decide_seed(now_ns: int, prev: RiskState | None, rest_pnl: Any, *, reset: bool = False) -> SeedDecision:
    """Combine the persisted state and the REST derivation (module docstring). ``rest_pnl`` is a
    DayPnl (anything with ``pnl_usd`` and ``open_usd``) or None (paper)."""
    ds = day_start(now_ns)
    same_day = prev is not None and prev.day_start_ns == ds
    notes: list[str] = []
    realized: list[float] = []
    real_caps: list[float] = []
    mark = 0.0
    if same_day and prev is not None:
        if prev.realized_usd is None:  # written before the realized / mark split: its total caps the result
            real_caps.append(float(prev.day_pnl_usd))
            notes.append(f"persisted day P&L {prev.day_pnl_usd:+.2f} (session {prev.session or '?'}, no realized/mark split)")
        else:
            realized.append(float(prev.realized_usd))
            notes.append(f"persisted realized {prev.realized_usd:+.2f} (session {prev.session or '?'}; its mark "
                         f"{prev.mark_usd:+.2f} is not reused)")
    if rest_pnl is not None:
        r_open = float(getattr(rest_pnl, "open_usd", 0.0))
        realized.append(float(rest_pnl.pnl_usd) - r_open)
        mark = r_open
        notes.append(f"REST realized {float(rest_pnl.pnl_usd) - r_open:+.2f}, open positions at exchange prices {r_open:+.2f}")
    elif same_day and prev is not None and prev.realized_usd is not None:
        mark = float(prev.mark_usd)  # paper: the simulated positions' last valuation
    if realized:
        real = min(realized) + mark
        if real_caps:
            real = min([real, *real_caps])
    else:
        real = min(real_caps) if real_caps else 0.0
    base = float(prev.budget_base_usd) if same_day and prev is not None else 0.0
    halted, reason, scope, hday, pause = False, "", "", 0, 0
    if prev is not None and prev.halted:
        r = base_reason(prev.halt_reason) or "halt"
        d = int(prev.halt_day_ns or prev.day_start_ns)
        if sticky(r) or d == ds:
            halted, reason, scope, hday = True, r, prev.halt_scope or "all", d
            notes.append(f"halt carried over: {r} (decided {day_str(d)}, scope {scope})")
        else:
            notes.append(f"{r} halt of {day_str(d)} not carried into {day_str(ds)} (a daily-loss halt ends at midnight UTC)")
    if prev is not None and prev.pause_until_ns > now_ns:
        pause = int(prev.pause_until_ns)
        notes.append(f"pause carried over until {pause}")
    if base:
        notes.append(f"daily-loss budget counts from {base:+.2f} (operator reset earlier today)")
    dec = SeedDecision(ds, real - base, halted, reason, pause, notes, {}, real, real - mark, mark, base, scope, hday)
    if reset:
        dec.overridden = {"halted": halted, "halt_reason": reason, "halt_scope": scope, "pause_until_ns": pause,
                          "day_pnl_usd": real - base, "budget_base_usd": base}
        dec.halted, dec.halt_reason, dec.halt_scope, dec.halt_day_ns, dec.pause_until_ns = False, "", "", 0, 0
        dec.budget_base_usd = real
        dec.day_pnl_usd = 0.0
        dec.notes.append(f"OPERATOR RESET (--reset-daily-halt): halt and pause cleared; the day's real P&L {real:+.2f} "
                         f"stays recorded; the daily-loss limit now counts from {real:+.2f}")
    return dec


def make_seed(ts: int, dec: SeedDecision) -> Any:
    """The RiskStateSeed event for ``dec`` (with its halt scope once the event carries one)."""
    from dh.core.events import RiskStateSeed

    kw: dict[str, Any] = {}
    if dec.halted and dec.halt_scope and "halt_scope" in {f.name for f in fields(RiskStateSeed)}:
        kw["halt_scope"] = dec.halt_scope
    return RiskStateSeed(ts, 0, dec.day_start_ns, float(dec.day_pnl_usd), bool(dec.halted), dec.halt_reason,
                         int(dec.pause_until_ns), **kw)


def state_from_decision(dec: SeedDecision, *, session: str, mode: str, now_ns: int) -> RiskState:
    return RiskState(day_start_ns=dec.day_start_ns, day_pnl_usd=dec.real_pnl_usd, halted=dec.halted,
                     halt_reason=dec.halt_reason, pause_until_ns=dec.pause_until_ns, session=session, mode=mode,
                     updated_ns=now_ns, realized_usd=dec.realized_usd, mark_usd=dec.mark_usd,
                     budget_base_usd=dec.budget_base_usd, halt_scope=dec.halt_scope if dec.halted else "",
                     halt_day_ns=dec.halt_day_ns if dec.halted else 0)


# ============================================================================ the runner's book
@dataclass
class RiskBook:
    """The live runner's per-day risk bookkeeping for what the strategy's equity does not cover:

      realized_usd  realized P&L of the day before this session (the seed's), plus settlements of
                    excluded markets during it (after 00:00 UTC: minus the excluded positions'
                    value at midnight)
      excluded      {ticker: (signed YES qty, px it is valued at)}: positions of events excluded
                    from the session, at their start-up marks until they settle
      base_usd      the operator's budget base of the day (--reset-daily-halt)
      mark_src      {ticker: mark source of the excluded position} (dh.settlement.closemark names)
      specs         {ticker: MarketSpec} of the excluded markets (from their REST rows)
      watch         excluded markets still OPEN at start-up: when one closes during the session,
                    the runner re-marks it from the BRTI prints of its window (``remark``)

    The real day P&L = the strategy's P&L of the day + realized_usd + value of ``excluded``; the
    strategy carries (via RiskStateSeed) that carry minus the base."""

    day_start_ns: int = 0
    realized_usd: float = 0.0
    base_usd: float = 0.0
    excluded: dict[str, tuple[int, int]] = field(default_factory=dict)
    halt_day_ns: int = 0  # the carried halt's original UTC day (0 = none)
    halt_scope: str = ""
    seed_day_ns: int = -1  # the last RiskStateSeed the strategy was fed (its day and value)
    seed_usd: float = 0.0
    mark_src: dict[str, str] = field(default_factory=dict)
    specs: dict[str, Any] = field(default_factory=dict)
    watch: set[str] = field(default_factory=set)

    @classmethod
    def from_decision(cls, dec: SeedDecision, excluded: dict[str, tuple[int, int]] | None = None, *,
                      specs: dict[str, Any] | None = None, sources: dict[str, str] | None = None,
                      now_ns: int | None = None) -> RiskBook:
        """``specs`` / ``sources``: the excluded markets' specs and start-up mark sources
        (``DayPnl.specs`` / ``DayPnl.mark_source``); with ``now_ns``, the excluded markets that
        close after it are watched (re-marked at their close)."""
        ex = {t: (int(q), int(px)) for t, (q, px) in (excluded or {}).items() if q}
        mark = sum(q * px for q, px in ex.values()) / 1e6
        sp = {t: s for t, s in (specs or {}).items() if t in ex and s is not None}
        watch = {t for t, s in sp.items() if now_ns is not None and s.close_ts > now_ns}
        return cls(dec.day_start_ns, dec.real_pnl_usd - mark, dec.budget_base_usd, ex,
                   dec.halt_day_ns if dec.halted else 0, dec.halt_scope if dec.halted else "",
                   mark_src={t: str(v) for t, v in (sources or {}).items() if t in ex}, specs=sp, watch=watch)

    def remark(self, ticker: str, px: int, source: str, ts: int) -> tuple[int, int] | None:
        """Re-value an excluded position at ``px`` (its market closed: dh.settlement.closemark).
        Returns the (qty, previous px), None if ``ticker`` is not an excluded position."""
        self.roll(ts)  # a new day's baseline is the value at midnight, before this re-mark
        pos = self.excluded.get(ticker)
        if pos is None:
            return None
        self.excluded[ticker] = (pos[0], int(px))
        self.mark_src[ticker] = source
        return pos

    def mark_usd(self) -> float:
        return sum(q * px for q, px in self.excluded.values()) / 1e6

    def carry_usd(self) -> float:
        return self.realized_usd + self.mark_usd()

    def roll(self, ts: int) -> bool:
        """A new UTC day: the excluded positions' value at midnight is the new day's baseline and
        the operator's budget base lapses."""
        ds = day_start(ts)
        if ds <= self.day_start_ns:
            return False
        self.realized_usd = -self.mark_usd()
        self.base_usd = 0.0
        self.day_start_ns = ds
        return True

    def seed_value(self, ts: int) -> float:
        """What the strategy should carry for ``ts``'s day (real carry minus the budget base)."""
        self.roll(ts)
        return self.carry_usd() - self.base_usd

    def settle(self, ticker: str, payout: int, ts: int) -> tuple[int, int] | None:
        """An excluded market was determined: realize its payout (its mark goes away). Returns the
        (qty, mark px) it had, None if ``ticker`` is not an excluded position."""
        self.roll(ts)
        pos = self.excluded.pop(ticker, None)
        self.watch.discard(ticker)
        self.specs.pop(ticker, None)
        self.mark_src.pop(ticker, None)
        if pos is not None:
            self.realized_usd += pos[0] * payout / 1e6
        return pos

    def note_seed(self, ev: Any) -> None:
        """The strategy was fed ``ev`` (a RiskStateSeed); it ignores seeds of another UTC day."""
        if day_start(int(ev.ts)) == int(ev.day_start_ns):
            self.seed_day_ns, self.seed_usd = int(ev.day_start_ns), float(ev.day_pnl_usd)

    def carried_seed(self, ts: int) -> float:
        """The carried P&L the strategy counts on ``ts``'s day (its last seed of that day, else 0)."""
        return self.seed_usd if self.seed_day_ns == day_start(ts) else 0.0


__all__ = ["DAY_NS", "DayFlows", "DayPnl", "RiskBook", "RiskState", "RiskStateError", "RiskStateStore", "SeedDecision",
           "OpenMarks", "backfill_window_prints", "base_reason", "day_flows", "day_pnl_from_flows", "day_pnl_from_rows", "day_start", "decide_seed",
           "derive_day_pnl", "fsync_dir", "historical_cutoff", "in_series", "last_trade_px", "make_seed", "mark_px",
           "open_marks", "parse_fill_row", "parse_settlement_row", "payout_px", "row_closed", "row_in_subaccount", "row_result", "row_spec", "row_ticker",
           "state_from_decision", "sticky"]
