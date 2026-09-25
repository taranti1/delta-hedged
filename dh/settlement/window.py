"""Settlement-window state for Kalshi crypto contracts (BRTI 60-print average).

Settlement convention (see ``dh.core.market.SettlementSpec``): the expiration value of a
KXBTCD market expiring at T is the simple average of ``n_obs`` (=60) once-per-second BRTI
prints stamped at T-59s, ..., T-1s, T (window (T-60s, T]).  This module turns a stream of
``IndexTick`` events into, for any expiration and any "now":

    WindowState(n_obs, k_fixed, sum_fixed, m_remaining, tau_first_s, step_s)

i.e. how many observations are already fixed, their sum, how many are still random and when
the first random one will be printed.  The pricing model (``dh.models.fairvalue``) needs
nothing else.

Which print is "the" print for observation second s
---------------------------------------------------
``IndexTick.ts_exch`` is the upstream CF Benchmarks source timestamp (ns); it -- never the local
receive time -- defines which second a print belongs to.

* 1 Hz feed (``feed in {'1hz', 'rest'}``): the print for second s is the tick whose source
  timestamp lies in [s, s + 1s) ("the tick stamped at s").  BRTI 1 Hz values are published on
  whole seconds, so in practice ts_exch == s exactly.  If two different 1 Hz ticks map to the
  same second, the one closest to s (earliest) is kept and a conflict is counted.
* 5 Hz feed only (``feed == '5hz'``): the print for second s is the LAST 5 Hz tick with
  ts_exch <= s ("last tick at or before s").  That value is final only once a tick with
  ts_exch > s has been seen (in-order delivery assumed; out-of-order ticks are inserted in
  source-time order).  If the 5 Hz stream publishes on the 200 ms grid, the tick at exactly
  s.000 is used, which is the same sample the 1 Hz stream would carry.
* When both exist for a second, the 1 Hz tick wins (``prefer_1hz=True``).

Duplicates: an identical (feed class, ts_exch) arriving twice is ignored (idempotent replay);
a different value for the same source timestamp is ignored too (first one wins, mirroring
Kalshi's "duplicate or out-of-order upstream source timestamps are ignored") and counted in
``stats['conflicts']``.

Missing seconds (gap policy) -- MUST be verified against Kalshi ``expiration_value``
-------------------------------------------------------------------------------------
If no print exists for an observation second but a later print does (so none will arrive),
the second is "missing":

* ``gap_policy='carry_forward'`` (default): use the last print at or before s (from either
  feed).  Many index averaging methodologies treat a missing publication this way, and it is
  what "last tick at or before s" on the 5 Hz feed does automatically.
* ``gap_policy='skip'``: drop the observation; the settlement is then the average of the
  prints that exist (n_obs shrinks accordingly; WindowState.n_obs == k_fixed + m_remaining).

Which of the two Kalshi/CF Benchmarks actually apply is NOT documented in the vendored specs;
``dh.research`` must reconcile reconstructed averages with published ``expiration_value`` (and
with the ``last_60s_windowed_average_15min`` field, which Kalshi streams for quarter-hour
closes -- every hourly expiry is a quarter-hour close) before trading size.

Pending observations: an observation whose time has passed (obs_time <= now) but whose print
has not arrived yet (latency) and cannot yet be declared missing is *pending*: it is treated
as unfixed with zero time to print (tau_first_s = 0).  The equal-spacing assumption of
``WindowState`` then places the next observation one full step later, which overstates the
remaining variance by at most one step (conservative, negligible outside the final seconds).
Pending observations older than ``pending_grace_s`` with no later print are also left pending
(the feed may simply be stalled); callers should watch feed health (FeedStatus) and
``WindowState.n_pending``.

Everything is deterministic: no wall clock, only event source timestamps and the ``now_ns``
argument.
"""

from __future__ import annotations

import bisect
import math
from dataclasses import dataclass, field
from typing import Iterable, Literal

from dh.core.events import IndexTick
from dh.core.market import SettlementSpec
from dh.core.units import NS_PER_S

GapPolicy = Literal["carry_forward", "skip"]
_ONE_HZ_FEEDS = ("1hz", "rest")


@dataclass(frozen=True, slots=True)
class WindowState:
    """State of one settlement window at a given time.

    n_obs        observations that will make up the average (== k_fixed + m_remaining)
    k_fixed      observations whose value is already known
    sum_fixed    sum of those k_fixed values ($)
    m_remaining  observations still to be printed (random)
    tau_first_s  seconds from now to the first unfixed observation (>= 0)
    step_s       spacing of the remaining observations (s)
    n_filled     fixed observations whose value came from the gap policy (diagnostic)
    n_pending    unfixed observations whose time already passed (print not yet received)
    """

    n_obs: int
    k_fixed: int
    sum_fixed: float
    m_remaining: int
    tau_first_s: float
    step_s: float
    n_filled: int = 0
    n_pending: int = 0

    def __post_init__(self) -> None:
        if self.k_fixed < 0 or self.m_remaining < 0:
            raise ValueError("k_fixed and m_remaining must be >= 0")
        if self.n_obs != self.k_fixed + self.m_remaining:
            raise ValueError(
                f"n_obs ({self.n_obs}) != k_fixed ({self.k_fixed}) + m_remaining ({self.m_remaining})"
            )
        if self.n_obs <= 0:
            raise ValueError("n_obs must be positive")
        if self.tau_first_s < 0 or self.step_s < 0:
            raise ValueError("tau_first_s and step_s must be >= 0")

    @property
    def fixed_avg(self) -> float:
        """Average of the fixed prints ($); nan when nothing is fixed."""
        return self.sum_fixed / self.k_fixed if self.k_fixed else math.nan

    @property
    def is_final(self) -> bool:
        """True when every observation is fixed (the settlement value is known)."""
        return self.m_remaining == 0

    @property
    def settlement_value(self) -> float:
        """Settlement average if final, else nan."""
        return self.sum_fixed / self.n_obs if self.m_remaining == 0 else math.nan


def pre_window_state(spec: SettlementSpec, expiration_ns: int, now_ns: int) -> WindowState:
    """WindowState when no observation is fixed yet (all n_obs remaining).

    Pure function of the spec and times; used by research/backtests without an index stream.
    tau_first_s is clamped at 0 if now is already inside the window.
    """
    first = spec.window_start_ns(expiration_ns)
    return WindowState(
        n_obs=spec.n_obs,
        k_fixed=0,
        sum_fixed=0.0,
        m_remaining=spec.n_obs,
        tau_first_s=max(0.0, (first - now_ns) / NS_PER_S),
        step_s=spec.step_ns / NS_PER_S,
    )


def required_remaining_avg(K: float, ws: WindowState) -> float:
    """Average the remaining m prints must have for the full average to equal K ($).

    (K * n_obs - sum_fixed) / m_remaining.  The settlement average exceeds K iff the remaining
    average exceeds this value.  Edge case m_remaining == 0 (outcome already determined):
    returns -inf if the fixed average is above K, +inf if below, nan if exactly equal (the
    outcome then depends on the strike type's inclusivity -- use ``MarketSpec.yes_wins``).
    """
    if ws.m_remaining == 0:
        avg = ws.sum_fixed / ws.n_obs
        if avg > K:
            return -math.inf
        if avg < K:
            return math.inf
        return math.nan
    return (K * ws.n_obs - ws.sum_fixed) / ws.m_remaining


def window_state_from_prints(
    spec: SettlementSpec,
    expiration_ns: int,
    now_ns: int,
    prints: dict[int, float],
    gap_policy: GapPolicy = "carry_forward",
    last_before: float | None = None,
) -> WindowState:
    """WindowState from a complete map {obs_time_ns: value} of known prints (offline helper).

    An observation with obs_time <= now_ns and no entry in ``prints`` is treated as missing
    (the caller asserts the map is complete up to now): filled by carry-forward of the previous
    print (or ``last_before`` for the first observation) or skipped, per ``gap_policy``.
    Observations after now are remaining.
    """
    obs = spec.obs_times(expiration_ns)
    step_s = spec.step_ns / NS_PER_S
    k = 0
    s = 0.0
    filled = 0
    skipped = 0
    last = last_before
    idx_first_unfixed = len(obs)
    for i, t in enumerate(obs):
        if t > now_ns:
            idx_first_unfixed = i
            break
        v = prints.get(t)
        if v is None:
            if gap_policy == "carry_forward" and last is not None:
                v = last
                filled += 1
            else:
                skipped += 1
                continue
        k += 1
        s += v
        last = v
    m = len(obs) - idx_first_unfixed
    tau_first = max(0.0, (obs[idx_first_unfixed] - now_ns) / NS_PER_S) if m > 0 else 0.0
    return WindowState(
        n_obs=k + m, k_fixed=k, sum_fixed=s, m_remaining=m, tau_first_s=tau_first, step_s=step_s, n_filled=filled
    )


@dataclass(slots=True)
class _QH:
    """Latest Kalshi-reported running average of a quarter-hour settlement window."""

    src_ns: int
    avg: float
    n: int


@dataclass
class SettlementTracker:
    """Accumulates BRTI prints and answers WindowState queries for any expiration.

    Feed it every ``IndexTick`` (any order of feeds; each feed in source order).  Ticks for
    other index ids are ignored.  Memory is bounded by ``retain_s`` of source time.

    Parameters
    ----------
    index_id        index to track ('BRTI')
    gap_policy      'carry_forward' | 'skip' for missing seconds (see module docstring)
    prefer_1hz      when both feeds have a value for a second, use the 1 Hz one
    use_5hz         derive per-second prints from the 5 Hz feed when no 1 Hz print exists
    retain_s        keep this many seconds of history (source time) behind the newest tick
    """

    index_id: str = "BRTI"
    gap_policy: GapPolicy = "carry_forward"
    prefer_1hz: bool = True
    use_5hz: bool = True
    retain_s: float = 7200.0
    # state
    _p1: dict[int, tuple[int, float]] = field(default_factory=dict, repr=False)  # sec_ns -> (ts_exch, v)
    _t5: list[int] = field(default_factory=list, repr=False)  # 5 Hz source times, ascending
    _v5: list[float] = field(default_factory=list, repr=False)
    _max_src: int = field(default=-1, repr=False)
    _qh: dict[int, _QH] = field(default_factory=dict, repr=False)  # close_ns -> latest qh avg
    stats: dict[str, int] = field(
        default_factory=lambda: {"ticks": 0, "duplicates": 0, "conflicts": 0, "out_of_order": 0, "ignored": 0}
    )

    # ------------------------------------------------------------------ ingestion
    def on_index(self, tick: IndexTick) -> None:
        """Ingest one index tick (uses tick.ts_exch as the source timestamp, ns)."""
        if tick.index_id != self.index_id or tick.ts_exch <= 0 or not math.isfinite(tick.value):
            self.stats["ignored"] += 1
            return
        self.stats["ticks"] += 1
        src = tick.ts_exch
        if tick.feed in _ONE_HZ_FEEDS:
            sec = src - (src % NS_PER_S)
            old = self._p1.get(sec)
            if old is None:
                self._p1[sec] = (src, float(tick.value))
            elif old[0] == src:
                self.stats["duplicates" if old[1] == tick.value else "conflicts"] += 1
            else:
                # two different 1 Hz ticks in the same second: keep the one closest to s
                self.stats["conflicts"] += 1
                if src < old[0]:
                    self._p1[sec] = (src, float(tick.value))
            if tick.qh_avg is not None and tick.qh_n > 0:
                close = self._qh_close_ns(src)
                cur = self._qh.get(close)
                if cur is None or src >= cur.src_ns:
                    self._qh[close] = _QH(src, float(tick.qh_avg), int(tick.qh_n))
        elif tick.feed == "5hz":
            n = len(self._t5)
            if n == 0 or src > self._t5[-1]:
                self._t5.append(src)
                self._v5.append(float(tick.value))
            else:
                i = bisect.bisect_left(self._t5, src)
                if i < n and self._t5[i] == src:
                    self.stats["duplicates" if self._v5[i] == tick.value else "conflicts"] += 1
                    return
                self.stats["out_of_order"] += 1
                self._t5.insert(i, src)
                self._v5.insert(i, float(tick.value))
        else:
            self.stats["ignored"] += 1
            return
        if src > self._max_src:
            self._max_src = src
            self._maybe_prune()

    def on_ticks(self, ticks: Iterable[IndexTick]) -> None:
        """Convenience: ingest many ticks in order."""
        for t in ticks:
            self.on_index(t)

    @staticmethod
    def _qh_close_ns(src_ns: int) -> int:
        """Quarter-hour close that a final-minute print belongs to: ceil to the next :00/:15/:30/:45."""
        q = 900 * NS_PER_S
        return -((-src_ns) // q) * q

    def _maybe_prune(self) -> None:
        horizon = self._max_src - int(self.retain_s * NS_PER_S)
        if self._t5 and self._t5[0] < horizon - 60 * NS_PER_S and len(self._t5) > 4096:
            i = bisect.bisect_left(self._t5, horizon)
            # keep one tick before the horizon so "last at or before" still works at the edge
            i = max(0, i - 1)
            del self._t5[:i]
            del self._v5[:i]
        if len(self._p1) > 2 * self.retain_s + 1024:
            for k in [k for k in self._p1 if k < horizon]:
                del self._p1[k]
            for k in [k for k in self._qh if k < horizon]:
                del self._qh[k]

    # ------------------------------------------------------------------ queries
    @property
    def latest_src_ns(self) -> int:
        """Newest source timestamp seen (ns), -1 if none."""
        return self._max_src

    def latest_value(self) -> float | None:
        """Most recent index value by source time (either feed), or None."""
        best_t, best_v = -1, None
        if self._t5:
            best_t, best_v = self._t5[-1], self._v5[-1]
        if self._p1:
            sec = max(self._p1)
            t, v = self._p1[sec]
            if t >= best_t:
                best_t, best_v = t, v
        return best_v

    def _last_5hz_at_or_before(self, t: int) -> tuple[int, float] | None:
        i = bisect.bisect_right(self._t5, t)
        if i == 0:
            return None
        return self._t5[i - 1], self._v5[i - 1]

    def _last_any_at_or_before(self, t: int) -> float | None:
        """Carry-forward value: newest print (either feed) with source time <= t."""
        best_t, best_v = -1, None
        r = self._last_5hz_at_or_before(t)
        if r is not None:
            best_t, best_v = r
        sec = t - (t % NS_PER_S)
        for j in range(0, int(self.retain_s) + 1):  # bounded scan back over 1 Hz seconds
            s = sec - j * NS_PER_S
            if s + NS_PER_S <= best_t:  # every 1 Hz tick at or before second s is older
                break
            e = self._p1.get(s)
            if e is not None and e[0] <= t:
                if e[0] >= best_t:
                    best_t, best_v = e
                break
        return best_v

    def _from_5hz(self, obs_ns: int) -> tuple[str, float | None] | None:
        """5 Hz resolution: last tick at or before obs_ns, final once a later tick exists."""
        if not (self.use_5hz and self._t5) or self._t5[-1] <= obs_ns:
            return None  # no 5 Hz data, or not final yet
        r = self._last_5hz_at_or_before(obs_ns)
        if r is not None and obs_ns - r[0] < NS_PER_S:
            return "exact5", r[1]
        return self._missing(obs_ns)  # the 5 Hz stream itself skipped this second

    def print_for(self, obs_ns: int) -> tuple[str, float | None]:
        """Resolve the settlement print for observation time ``obs_ns`` (source time, ns).

        Returns (status, value) with status one of:
          'exact'   1 Hz print stamped in that second
          'exact5'  derived from the 5 Hz feed (last tick at or before obs_ns, which is final)
          'filled'  missing, value from carry-forward (gap_policy='carry_forward')
          'skipped' missing and gap_policy='skip' (value None)
          'pending' not yet determinable (value None)
        """
        e = self._p1.get(obs_ns - (obs_ns % NS_PER_S))
        if e is not None:
            if self.prefer_1hz or not self.use_5hz:
                return "exact", e[1]
            r5 = self._from_5hz(obs_ns)
            return r5 if (r5 is not None and r5[0] == "exact5") else ("exact", e[1])
        r5 = self._from_5hz(obs_ns)
        if r5 is not None:
            return r5
        if self._max_src > obs_ns:
            return self._missing(obs_ns)
        return "pending", None

    def _missing(self, obs_ns: int) -> tuple[str, float | None]:
        if self.gap_policy == "carry_forward":
            v = self._last_any_at_or_before(obs_ns)
            if v is not None:
                return "filled", v
        return "skipped", None

    def window_state(self, spec: SettlementSpec, expiration_ns: int, now_ns: int) -> WindowState:
        """WindowState of the window expiring at ``expiration_ns`` as known at ``now_ns``.

        Observations with obs_time > now_ns are remaining.  Observations with obs_time <= now_ns
        are fixed if their print is known (or declared missing and filled), skipped under
        gap_policy='skip', and otherwise pending (then they and all later observations count
        as remaining, with tau_first_s = 0).
        """
        if spec.index_id != self.index_id:
            raise ValueError(f"tracker follows {self.index_id}, spec wants {spec.index_id}")
        obs = spec.obs_times(expiration_ns)
        step_s = spec.step_ns / NS_PER_S
        k = 0
        s = 0.0
        filled = 0
        first_unfixed = len(obs)
        pending = 0
        for i, t in enumerate(obs):
            if t > now_ns:
                first_unfixed = i
                break
            status, v = self.print_for(t)
            if status == "pending":
                first_unfixed = i
                pending = sum(1 for tt in obs[i:] if tt <= now_ns)
                break
            if status == "skipped":
                continue
            k += 1
            s += v  # type: ignore[operator]
            if status == "filled":
                filled += 1
        m = len(obs) - first_unfixed
        if m > 0:
            tau_first = max(0.0, (obs[first_unfixed] - now_ns) / NS_PER_S)
        else:
            tau_first = 0.0
        if k + m == 0:
            raise ValueError("every observation was skipped; settlement undefined")
        return WindowState(
            n_obs=k + m,
            k_fixed=k,
            sum_fixed=s,
            m_remaining=m,
            tau_first_s=tau_first,
            step_s=step_s,
            n_filled=filled,
            n_pending=pending,
        )

    def required_remaining_avg(self, K: float, ws: WindowState) -> float:
        """(K * n_obs - sum_fixed) / m_remaining; see module-level ``required_remaining_avg``."""
        return required_remaining_avg(K, ws)

    def settlement_value(self, spec: SettlementSpec, expiration_ns: int) -> float | None:
        """Reconstructed expiration value once every observation is determined, else None."""
        ws = self.window_state(spec, expiration_ns, expiration_ns)
        if ws.m_remaining:
            return None
        return ws.sum_fixed / ws.n_obs

    def kalshi_window_avg(self, expiration_ns: int) -> tuple[float, int] | None:
        """Kalshi's own running (avg, count) of the final-minute window closing at expiration.

        From ``last_60s_windowed_average_15min`` (quarter-hour closes only; every hourly KXBTCD
        expiry is one).  Use it to cross-check ``window_state`` (sum_fixed ~= avg * count).
        """
        q = self._qh.get(expiration_ns)
        return None if q is None else (q.avg, q.n)


__all__ = [
    "WindowState",
    "SettlementTracker",
    "required_remaining_avg",
    "pre_window_state",
    "window_state_from_prints",
    "GapPolicy",
]
