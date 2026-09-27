"""Replay a recorded live / paper session and compare its decisions (determinism check).

    info = load_session("data/live")                      # last session in that store
    res = replay_session("data/live", load_config("config/m1.yaml"))
    diff = compare_with_log(res, "data/live_logs/<session>.jsonl", info)

What a session leaves on disk (dh.live.runner / dh.live.app):
  kalshi.ws, <venue>.ws     raw frames (normalized again by dh.store.replay.iter_events)
  events.live               events the runner fed that are not raw frames, in processing
                            order: the start-up RiskStateSeed, adapter results (acks, rejects,
                            reconciliation updates, gate rejects), runner-derived events (lag /
                            reconciliation / clock FeedStatus, checked position snapshots,
                            back-filled fills, order-group updates, the updated RiskStateSeed
                            after an excluded market settled or was re-marked at its close, or
                            the watchdog's halt)
  events.paper              simulator messages (audit only: the replay regenerates them)
  meta                      session_start (configs, universe, paper simulator config, id
                            prefix, subaccount), fv_warmup (the exact benchmark points fed to
                            the model), universe_add / universe_prune / queue_resync at their
                            times, session_end (last delivered ts)
The replay rebuilds the same MarketMaker (same specs, same warm-up, same client_order_id
prefix), applies the recorded universe changes at their recorded times and the runner's
inbound rules, and runs dh.backtest.runner.run over the recorded events (paper: with a
simulator built from the recorded config, which regenerates the fills). events.live is merged
LAST: a runner-derived event shares the timestamp of the item that caused it and was fed
after it. Any difference in actions or Log records up to the last delivered ts is a bug; that
includes the strategy's ``close_mark`` logs (positions in closed markets awaiting their result,
marked from the recorded BRTI prints and lifecycle results: dh.settlement.closemark).
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import orjson

from dh.core.market import MarketSpec
from dh.core.units import NS_PER_MS
from dh.live.config import PaperCfg
from dh.live.startup import build_paper_sim, spec_from_dict, warm_fv
from dh.store.replay import iter_events, iter_raw
from dh.store.textio import open_text

REPLAY_STREAMS = ("kalshi.ws", "events.live")  # events.live is always merged last (see module docstring)


@dataclass
class SessionInfo:
    session: str
    mode: str
    t0: int
    t1: int
    last_ts: int
    specs: list[MarketSpec]
    fv_points: list[tuple[int, float]]
    changes: list[tuple[int, str, Any]] = field(default_factory=list)  # (t, kind, payload)
    paper: dict[str, Any] | None = None
    strategy_digest: str = ""
    start: dict[str, Any] = field(default_factory=dict)
    group_map: dict[str, str] = field(default_factory=dict)  # exchange order-group id -> logical id
    id_prefix: str | None = None  # the session's client_order_id prefix (None: config run_prefix)
    subaccount: int = 0
    key_restricted: bool = False  # venue.key_restricted_to_subaccount of the session
    series: tuple[str, ...] = ()  # the session's series (own-activity events of others were dropped)
    own_id_prefix: str = ""  # live: fills / order updates of other client_order_ids were dropped ('' = no filter)


def load_session(root: str | Path, session: str | None = None) -> SessionInfo:
    """Collect one session's meta records (default: the last session in the store)."""
    recs = [orjson.loads(r.data) for r in iter_raw(root, ["meta"], 0, 2**62)]
    starts = [m for m in recs if m.get("kind") == "session_start" and (session is None or m.get("session") == session)]
    if not starts:
        raise ValueError(f"no session_start record{'' if session is None else ' for ' + session} in {root}")
    st = starts[-1]
    sid = st.get("session", "")
    mine = [m for m in recs if m.get("session") == sid]
    warm = next((m for m in mine if m.get("kind") == "fv_warmup"), {"points": []})
    end = next((m for m in reversed(mine) if m.get("kind") == "session_end"), None)
    changes: list[tuple[int, str, Any]] = []
    for m in mine:
        k = m.get("kind")
        if k == "universe_add":
            changes.append((int(m["t"]), k, [spec_from_dict(d) for d in m["specs"]]))
        elif k == "universe_prune":
            changes.append((int(m["t"]), k, int(m.get("before_ns", 0))))
        elif k == "queue_resync":
            changes.append((int(m["t"]), k, m["rows"]))
    group_map = {str(m["id"]): str(m["logical"]) for m in mine if m.get("kind") == "order_group_map"}
    return SessionInfo(
        session=sid, mode=str(st.get("mode", "live")), t0=int(st["t"]), t1=int(end["t"]) + 1 if end else 2**62,
        last_ts=int(end.get("last_ts", 0)) if end else 2**62,
        specs=[spec_from_dict(d) for d in st.get("specs", [])],
        fv_points=[(int(t), float(v)) for t, v in warm.get("points", [])],
        changes=sorted(changes, key=lambda c: c[0]), paper=st.get("paper"),
        strategy_digest=str(st.get("strategy_digest", "")), start=st, group_map=group_map,
        id_prefix=st.get("id_prefix") or None, subaccount=int(st.get("subaccount") or 0),
        key_restricted=bool(((st.get("live_config") or {}).get("venue") or {}).get("key_restricted_to_subaccount", False)),
        series=tuple(st.get("series") or ()), own_id_prefix=str(st.get("own_id_prefix") or ""))


class UniverseReplay:
    """Strategy wrapper reproducing what the live runner did around the strategy:
      * recorded universe changes applied at their recorded times (before the first
        merged-stream item at or after the change, as the live consumer did);
      * live inbound rules (LiveRunner.push / _on_event): events of another subaccount
        (runner.own_subaccount_ok, incl. the restricted-key rule) or of a market outside the
        session's series (runner.own_series_ok) dropped; fills / order updates whose
        client_order_id lacks the session's own prefix and whose order id is not known
        (runner.own_order_ok, the known ids built from the same events in the same order)
        dropped (live PARKED those without a client id; a parked event released later was
        recorded on events.live with the order's client_order_id at its release time, so it is
        delivered here from there: review NEW-1); order-group ids translated to the logical id (other groups dropped); WS
        market_position snapshots dropped (the runner fed only checked ones, recorded on
        events.live with source 'ws_checked' / 'rest'); a fill whose trade/fill id was already
        delivered dropped (a REST back-fill that beat the WS message)."""

    def __init__(self, strategy: Any, changes: list[tuple[int, str, Any]], sim: Any = None, paper_fees: Any = None,
                 *, live: bool = False, group_map: dict[str, str] | None = None, subaccount: int = 0,
                 key_restricted: bool = False, series: tuple[str, ...] = (), own_id_prefix: str = "") -> None:
        from dh.live.runner import SeenIds

        self.strategy = strategy
        self.changes = list(changes)
        self.sim = sim
        self.paper_fees = paper_fees
        self.live = live
        self.group_map = dict(group_map or {})
        self.logical = set(self.group_map.values())
        self.subaccount = int(subaccount)
        self.key_restricted = bool(key_restricted)
        self.series = tuple(series)
        self.fills_seen = SeenIds()
        self.own_id_prefix = str(own_id_prefix or "")
        self.known_oids = SeenIds()

    def _apply(self, kind: str, payload: Any, t: int) -> None:
        s = self.strategy
        if kind == "universe_add":
            added = set(s.add_markets(payload))
            new = [x for x in payload if x.ticker in added]
            if self.sim is not None:
                for x in new:
                    self.sim.register_market(x)
            if self.paper_fees is not None:
                self.paper_fees.add_specs(new)
        elif kind == "universe_prune":
            s.prune_settled(int(payload))
        elif kind == "queue_resync":
            for coid, _ticker, qty, _est in payload:
                s.queue.ingest_exchange_queue_position(coid, int(qty), t, resync=True)

    def on_event(self, ev: Any) -> list[Any]:
        while self.changes and self.changes[0][0] <= ev.ts:
            t, kind, payload = self.changes.pop(0)
            self._apply(kind, payload, t)
        if self.paper_fees is not None:
            from dh.core.events import KalshiFeeUpdate
            from dh.live.runner import effective_fee

            if isinstance(ev, KalshiFeeUpdate):  # as LiveRunner._on_fee_update does in paper mode
                for t, spec in list(getattr(self.strategy, "specs", {}).items()):
                    if spec.event_ticker != ev.event_ticker:
                        continue
                    eff = effective_fee(spec, ev)
                    if eff is not None and eff[0]:
                        self.paper_fees.set_fee(t, eff[0], eff[1])
        if self.live:
            from dataclasses import replace

            from dh.core.events import KalshiFill, KalshiOrderGroupUpdate, KalshiPositionSnapshot
            from dh.live.runner import OWN_TYPES, note_own_order, own_order_ok, own_series_ok, own_subaccount_ok

            if isinstance(ev, OWN_TYPES) and not (own_subaccount_ok(ev, self.subaccount, key_restricted=self.key_restricted)
                                                  and own_series_ok(ev, self.series)):
                return []
            if not own_order_ok(ev, self.own_id_prefix, self.known_oids):
                return []
            note_own_order(ev, self.known_oids)
            if isinstance(ev, KalshiPositionSnapshot) and ev.source == "ws":
                return []
            if isinstance(ev, KalshiOrderGroupUpdate) and ev.order_group_id not in self.logical:
                logical = self.group_map.get(ev.order_group_id)
                if not logical:
                    return []
                ev = replace(ev, order_group_id=logical)
            if isinstance(ev, KalshiFill):
                if self.fills_seen.seen(ev):
                    return []
                self.fills_seen.add(ev.trade_id, ev.fill_id)
        return self.strategy.on_event(ev)


@dataclass
class ReplayResult:
    actions: list[tuple[int, Any]]
    logs: list[tuple[int, Any]]
    strategy: Any
    info: SessionInfo
    sim: Any = None


def replay_session(root: str | Path, scfg: Any, *, session: str | None = None, extra_streams: tuple[str, ...] = (),
                   fee_engine: Any = None) -> ReplayResult:
    """Re-run a recorded session through dh.backtest.runner.run (see module docstring).

    ``scfg`` must be the session's StrategyConfig (its digest is checked). External-venue
    streams found in the store (e.g. 'coinbase.ws') are included automatically;
    ``extra_streams`` adds other recorded event streams."""
    from dh.backtest.runner import run
    from dh.kalshi.fees import FeeEngine
    from dh.models.fvmodel import FairValueModel, load_recommended_config
    from dh.strategy.mm import MarketMaker

    info = load_session(root, session)
    if info.strategy_digest and scfg.digest() != info.strategy_digest:
        raise ValueError(f"strategy config digest {scfg.digest()} != session's {info.strategy_digest}")
    fee_engine = fee_engine or FeeEngine.from_config()
    fv = FairValueModel.from_config(load_recommended_config())
    warm_fv(fv, info.fv_points)
    mm = MarketMaker(scfg, info.specs, fv_model=fv, fee_engine=fee_engine, book_includes_own=(info.mode == "live"),
                     id_prefix=info.id_prefix)
    sim = fees = None
    if info.mode == "paper":
        paper = dict(info.paper or {})
        paper.setdefault("md_ms", 0.0)  # legacy recordings really used zero public-data delay
        sim, fees = build_paper_sim(PaperCfg(**paper), info.specs, fee_engine)
    wrapper = UniverseReplay(mm, info.changes, sim, fees, live=(info.mode == "live"), group_map=info.group_map,
                             subaccount=info.subaccount, key_restricted=info.key_restricted, series=info.series,
                             own_id_prefix=info.own_id_prefix)
    from dh.feeds.registry import has_normalizer
    from dh.store.replay import list_streams

    feeds = [x for x in list_streams(root) if has_normalizer(x)]  # external venues this session recorded
    raw = [x for x in REPLAY_STREAMS if x != "events.live"]
    streams = list(dict.fromkeys(raw + feeds + [x for x in extra_streams if x != "events.live"] + ["events.live"]))
    events = iter_events(root, streams, info.t0, info.t1)
    res = run(events, wrapper, sim, timer_period_ns=int(scfg.timers.quote_period_ms) * NS_PER_MS,
              end_ns=None if info.last_ts >= 2**62 else info.last_ts)
    return ReplayResult(res.actions, res.logs, mm, info, sim)


def _norm(obj: Any) -> Any:
    return json.loads(orjson.dumps(obj, default=str, option=orjson.OPT_NON_STR_KEYS))


def logged_decisions(log_path: str | Path, until_ts: int) -> tuple[list[Any], list[Any]]:
    """(actions, logs) the live strategy emitted, from the runner's JSON log."""
    acts, logs = [], []
    for r in iter_log_records(log_path):
        if r.get("t", 0) > until_ts:
            continue
        if r["k"] == "action":
            acts.append((r["t"], r["type"], {k: v for k, v in r.items() if k not in ("k", "t", "cfg", "sha", "type", "origin")}))
        elif r["k"].startswith("log."):
            logs.append((r["t"], r["k"][4:], {k: v for k, v in r.items() if k not in ("k", "t", "cfg", "sha")}))
    return acts, logs


def replayed_decisions(res: ReplayResult, until_ts: int) -> tuple[list[Any], list[Any]]:
    """The replay's (actions, logs) in the same normalized form as ``logged_decisions``."""
    from dh.live.runner import _action_fields

    acts = [(ts, type(a).__name__, _norm(_action_fields(a))) for ts, a in res.actions if ts <= until_ts]
    logs = [(ts, a.kind, _norm(a.payload)) for ts, a in res.logs if ts <= until_ts]
    return acts, logs


def session_specs(info: SessionInfo) -> list[MarketSpec]:
    """Every market the session traded: the start-up universe plus roll-over additions."""
    out = {s.ticker: s for s in info.specs}
    for _t, kind, payload in info.changes:
        if kind == "universe_add":
            for s in payload:
                out.setdefault(s.ticker, s)
    return list(out.values())


def log_fill_is_new(rec: dict[str, Any], seen: set[str]) -> bool:
    """A ``log.fill`` record not booked yet: its trade id (logged since review L1) is new, or it
    has none (older logs). Adds the id to ``seen``. The strategy logs each fill once; this also
    guards the P&L tools against a fill logged twice (e.g. an orphan fill and its late attach
    in logs written before the fix)."""
    tid = str(rec.get("trade_id") or "")
    if not tid:
        return True
    if tid in seen:
        return False
    seen.add(tid)
    return True


def iter_log_records(path: str | Path):
    """Stream JSON records; corrupt/truncated lines fail visibly instead of hiding fills."""
    with open_text(path) as stream:
        for line in stream:
            if line.strip():
                yield orjson.loads(line)


def ledger_from_log(log_path: str | Path, specs: list[MarketSpec]) -> Any:
    """Bounded-memory two-pass attribution of plain/gzip/zstd session logs."""
    return ledger_from_logs([log_path], specs)


def ledger_from_logs(log_paths: list[str | Path], specs: list[MarketSpec]) -> Any:
    """Audit independent sessions together, joining later outcomes to earlier fills.

    Session cash/marks in risk seeds are never treated as profit. All fills remain visible,
    including unresolved inventory. Read each log twice and retain only fair values needed
    at fills and markout horizons (memory grows with fills, not millions of FV records).
    Exposure is summed full session duration, including quiet periods. This is an audit of
    independent paper portfolios, not a claim that their positions were restored on restart.
    """
    import bisect
    from collections import defaultdict

    from dh.backtest.ledger import Ledger, MARKOUT_H_S
    from dh.core.actions import Log
    from dh.core.events import KalshiFill, Settlement
    from dh.core.units import NS_PER_S

    paths = [Path(p).resolve() for p in log_paths]
    if len(paths) != len(set(paths)):
        raise ValueError("a session log was supplied more than once")
    sessions = [p.name.removesuffix(".gz").removesuffix(".zst").removesuffix(".jsonl") for p in paths]
    if len(sessions) != len(set(sessions)):
        raise ValueError("multiple compressed/uncompressed copies of the same session")
    led = Ledger({s.ticker: s.event_ticker for s in specs}, {s.ticker: s.expiration_ts for s in specs})
    duration = 0
    bounds = []
    missing_match_times = incomplete_sessions = 0
    for path in paths:
        seen: set[str] = set()  # paper simulator identifiers may repeat in a later session
        first = last = None
        complete = False
        for r in iter_log_records(path):
            k, t = r.get("k", ""), int(r.get("t", 0))
            first = t if first is None else min(first, t)
            last = t if last is None else max(last, t)
            complete |= k == "session_end"
            if k == "log.fill" and log_fill_is_new(r, seen):
                missing_match_times += not bool(r.get("ts_exch"))
                led.on_event(KalshiFill(t, int(r.get("ts_exch") or 0), r["ticker"],
                                        str(r.get("trade_id") or ""), "", str(r.get("coid", "")),
                                        r["side"], int(r["px"]), int(r["qty"]), bool(r.get("taker", False)),
                                        int(r.get("fee", 0)), 0, False))
                led.fills[-1].fv_key = f"{path}\0{r['ticker']}"
            elif k == "log.settle":
                px = int(r["px"])
                previous = led.settle.get(r["ticker"])
                if previous is not None and previous != px / 10_000:
                    raise ValueError(f"conflicting settlement outcomes for {r['ticker']}")
                led.on_event(Settlement(t, 0, r["ticker"], "yes" if px > 0 else "no", None, px))
        if first is not None:
            duration += max(0, last - first)
            bounds.append((first, last))
            incomplete_sessions += not complete
    led.observation_duration_ns = duration
    led.audit_notes.update({"sessions": len(paths), "incomplete_sessions": incomplete_sessions,
                            "fills_without_exchange_timestamp": missing_match_times,
                            "duration_basis": "sum of observed full-log session spans"})
    if bounds:
        led.first_ts = min(x[0] for x in bounds)
        led.last_ts = max(x[1] for x in bounds)

    targets = defaultdict(set)
    for f in led.fills:
        targets[f.fv_key].add(f.ts)
        targets[f.fv_key].update(f.ts + int(h * NS_PER_S) for h in MARKOUT_H_S)
    targets = {tk: sorted(ts) for tk, ts in targets.items()}
    if not targets:
        return led
    selected = {}  # (session/ticker, target) -> latest observation at/before target
    following = {}  # (session/ticker, target) -> first observation after it (Ledger bracketing)

    def retain(tk, target, previous):
        if previous is None:
            return
        key = (tk, target)
        if key not in selected or previous[0] > selected[key][0]:
            selected[key] = previous

    for path in paths:
        prev, cursors = {}, defaultdict(int)
        for r in iter_log_records(path):
            tk = f"{path}\0{r.get('ticker')}"
            if r.get("k") != "log.fv" or tk not in targets:
                continue
            t = int(r["t"])
            old = prev.get(tk)
            if old is not None and t < old[0]:
                raise ValueError(f"out-of-order fair values for {tk} in {path}")
            ts, i = targets[tk], cursors[tk]
            j = bisect.bisect_left(ts, t, lo=i)
            cur = (t, float(r["F"]), float(r.get("delta", 0.0)))
            for target in ts[i:j]:
                retain(tk, target, old)
                following.setdefault((tk, target), cur)
            cursors[tk] = j
            prev[tk] = cur
        for tk, old in prev.items():
            for target in targets[tk][cursors[tk]:]:
                retain(tk, target, old)
    kept = defaultdict(dict)
    for (tk, _), value in (*selected.items(), *following.items()):
        kept[tk][value[0]] = value
    for tk, observations in kept.items():
        for t, F, delta in sorted(observations.values()):
            led.on_log(t, Log("fv", {"ticker": tk, "F": F, "delta": delta}))
    return led


def add_downloaded_outcomes(ledger: Any, root: str | Path) -> int:
    """Join actual downloaded settlements, read-only, to a lifetime audit.

    ``root`` is the history downloader's output directory (containing ``markets``).
    Only explicit yes/no results with consistent payouts are accepted; conflicts fail
    visibly. Missing files/results remain unresolved, never valued as zero profit.
    """
    import pyarrow.parquet as pq

    from dh.core.units import PX_SCALE

    wanted = {f.ticker for f in ledger.fills}
    events = {ledger.event_of.get(t) or t.rsplit('-', 1)[0] for t in wanted}
    found = {}
    for event in sorted(events):
        path = Path(root) / 'markets' / f"series={event.split('-', 1)[0]}" / f'{event}.parquet'
        if not path.is_file():
            continue
        rows = pq.ParquetFile(path).read(columns=['ticker', 'result', 'settlement_px']).to_pylist()
        for row in rows:
            tk, result = row['ticker'], row['result']
            if tk not in wanted or result not in ('yes', 'no'):
                continue
            px = PX_SCALE if result == 'yes' else 0
            if row['settlement_px'] is not None and row['settlement_px'] != px:
                raise ValueError(f'inconsistent downloaded settlement for {tk}')
            payout = px / PX_SCALE
            previous = found.get(tk, ledger.settle.get(tk))
            if previous is not None and previous != payout:
                raise ValueError(f'conflicting settlement outcomes for {tk}')
            found[tk] = payout
    added = sum(t not in ledger.settle for t in found)
    ledger.settle.update(found)
    ledger.audit_notes['downloaded_outcomes_added'] = added
    return added
