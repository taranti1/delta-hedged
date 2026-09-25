"""Replay a recorded live / paper session and compare its decisions (determinism check).

    info = load_session("data/live")                      # last session in that store
    res = replay_session("data/live", load_config("config/m1.yaml"))
    diff = compare_with_log(res, "data/live_logs/<session>.jsonl", info)

What a session leaves on disk (dh.live.runner / dh.live.app):
  kalshi.ws, <venue>.ws     raw frames (normalized again by dh.store.replay.iter_events)
  events.live               adapter results fed to the strategy (acks, rejects, reconciliation
                            updates, position snapshots, order-group updates, gate rejects)
  events.paper              simulator messages (audit only: the replay regenerates them)
  meta                      session_start (configs, universe, paper simulator config),
                            fv_warmup (the exact benchmark points fed to the model),
                            universe_add / universe_prune / queue_resync at their times,
                            session_end (last delivered ts)
The replay rebuilds the same MarketMaker (same specs, same warm-up), applies the recorded
universe changes at their recorded times, and runs dh.backtest.runner.run over the recorded
events (paper: with a simulator built from the recorded config, which regenerates the fills).
Any difference in actions or Log records up to the last delivered ts is a bug.
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

REPLAY_STREAMS = ("kalshi.ws", "events.live")


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
        strategy_digest=str(st.get("strategy_digest", "")), start=st, group_map=group_map)


class UniverseReplay:
    """Strategy wrapper reproducing what the live runner did around the strategy:
      * recorded universe changes applied at their recorded times (before the first
        merged-stream item at or after the change, as the live consumer did);
      * live inbound rules (LiveRunner.push): order-group ids translated to the logical id
        (other groups dropped); WS market_position snapshots dropped (the runner fed only
        checked ones, recorded on events.live with source 'ws_checked' / 'rest')."""

    def __init__(self, strategy: Any, changes: list[tuple[int, str, Any]], sim: Any = None, paper_fees: Any = None,
                 *, live: bool = False, group_map: dict[str, str] | None = None) -> None:
        self.strategy = strategy
        self.changes = list(changes)
        self.sim = sim
        self.paper_fees = paper_fees
        self.live = live
        self.group_map = dict(group_map or {})
        self.logical = set(self.group_map.values())

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
        if self.live:
            from dataclasses import replace

            from dh.core.events import KalshiOrderGroupUpdate, KalshiPositionSnapshot

            if isinstance(ev, KalshiPositionSnapshot) and ev.source == "ws":
                return []
            if isinstance(ev, KalshiOrderGroupUpdate) and ev.order_group_id not in self.logical:
                logical = self.group_map.get(ev.order_group_id)
                if not logical:
                    return []
                ev = replace(ev, order_group_id=logical)
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
    mm = MarketMaker(scfg, info.specs, fv_model=fv, fee_engine=fee_engine, book_includes_own=(info.mode == "live"))
    sim = fees = None
    if info.mode == "paper":
        sim, fees = build_paper_sim(PaperCfg(**(info.paper or {})), info.specs, fee_engine)
    wrapper = UniverseReplay(mm, info.changes, sim, fees, live=(info.mode == "live"), group_map=info.group_map)
    from dh.feeds.registry import has_normalizer
    from dh.store.replay import list_streams

    feeds = [x for x in list_streams(root) if has_normalizer(x)]  # external venues this session recorded
    streams = list(dict.fromkeys(list(REPLAY_STREAMS) + feeds + list(extra_streams)))
    events = iter_events(root, streams, info.t0, info.t1)
    res = run(events, wrapper, sim, timer_period_ns=int(scfg.timers.quote_period_ms) * NS_PER_MS,
              end_ns=None if info.last_ts >= 2**62 else info.last_ts)
    return ReplayResult(res.actions, res.logs, mm, info, sim)


def _norm(obj: Any) -> Any:
    return json.loads(orjson.dumps(obj, default=str, option=orjson.OPT_NON_STR_KEYS))


def logged_decisions(log_path: str | Path, until_ts: int) -> tuple[list[Any], list[Any]]:
    """(actions, logs) the live strategy emitted, from the runner's JSON log."""
    acts, logs = [], []
    for line in Path(log_path).read_text().splitlines():
        r = json.loads(line)
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


def ledger_from_log(log_path: str | Path, specs: list[MarketSpec]) -> Any:
    """P&L attribution (dh.backtest.ledger.Ledger) straight from a session's JSON log, without
    re-running the strategy: ``log.fill`` (our fills: paper = simulator, live = exchange),
    ``log.fv`` (fair values for markouts) and ``log.settle`` (settlement prices).

        info = load_session("data/live")
        led = ledger_from_log("data/live_logs/<session>.jsonl", session_specs(info))
        print(led.summary())   # net c/contract (event-clustered CI), markouts, $/day, ...
    """
    from dh.backtest.ledger import Ledger
    from dh.core.actions import Log
    from dh.core.events import KalshiFill, Settlement

    led = Ledger({s.ticker: s.event_ticker for s in specs}, {s.ticker: s.expiration_ts for s in specs})
    for line in Path(log_path).read_text().splitlines():
        r = json.loads(line)
        k, t = r.get("k", ""), int(r.get("t", 0))
        if k == "log.fv":
            led.on_log(t, Log("fv", {"ticker": r["ticker"], "F": r["F"], "delta": r.get("delta", 0.0)}))
        elif k == "log.fill":
            led.on_event(KalshiFill(t, 0, r["ticker"], "", "", str(r.get("coid", "")), r["side"], int(r["px"]),
                                    int(r["qty"]), bool(r.get("taker", False)), int(r.get("fee", 0)), 0, False))
        elif k == "log.settle":
            px = int(r["px"])
            led.on_event(Settlement(t, 0, r["ticker"], "yes" if px > 0 else "no", None, px))
    return led
