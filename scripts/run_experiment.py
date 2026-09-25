#!/usr/bin/env python
"""Run a research experiment on a recording (one command per experiment).

    python scripts/run_experiment.py <name> --root data --t0 2026-10-01 --t1 2026-10-08 [--out DIR]

names
  universe   rebuild and print the market universe of the window (specs, fees, rejects, own fills)
  replay     one replay of the strategy (MarketMaker + KalshiExchangeSim + Ledger): summary + ledger
  flow       calibrate taker flow on the window with a time split (in-sample AND out-of-sample
             table) -> flow_segments.json for replays that start after the window  dh.research.flow_recording
  e1         Kalshi staleness vs external BTC (lead-lag)                        dh.research.exp1_staleness
  e2         nowcast of the next BRTI print / settlement average (+ P&L hook)      dh.research.exp2_nowcast
  e3         fill toxicity: markouts, walk-forward models, cancel-rule replay     dh.research.exp3_toxicity
  e4         queue priority vs repricing: policy grid                              dh.research.exp4_queue
  e67        net edge by tau / |z| / YES price                                     dh.research.exp67_segments
  e8         selective taking of stale quotes                                     dh.research.exp8_taker
  e9         quoting 1 vs 3 vs all strikes                                        dh.research.exp9_multistrike
  e10        capacity: clip size x1..x50                                          dh.research.exp10_capacity
  all        e1 e2 e3 e4 e67 e8 e9 e10 in sequence
  synth      write a SYNTHETIC recording to --root (pipeline validation only)
  demo       synthetic recording + every experiment -> docs/research/synthetic_demo/

Times: ISO-8601 UTC ('2026-10-01', '2026-10-01T13:00') or epoch s/ms/us/ns; 'auto' = the
coverage of the kalshi.ws stream. Every P&L table is reported under fill policies B and C
(--policies, A for reference only). Fitted inputs are checked for look-ahead against t0:
--fv-config (fair-value parameters; default dh/models/data/fv_recommended.json) and
--flow-segments (taker flow; default the strategy config's) -- reports say "IN-SAMPLE" and warn
when they were fitted on data that does not precede the window. See
docs/research/EXPERIMENTS_RUNBOOK.md.
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from dh.research.exp_common import fmt_ns, parse_policies, parse_time  # noqa: E402

EXPERIMENTS = ("e1", "e2", "e3", "e4", "e67", "e8", "e9", "e10")


def coverage(root: str, stream: str = "kalshi.ws") -> tuple[int, int]:
    """(first, last + 1) receive time of a stream (segment indexes, else hour bounds)."""
    from dh.store.recorder import HOUR_NS
    from dh.store.replay import read_index, segment_files

    segs = segment_files(root, stream)
    if not segs:
        raise SystemExit(f"no '{stream}' data under {root}/raw")
    first = read_index(segs[0][2])
    last = read_index(segs[-1][2])
    t0 = first["first_t"] if first else segs[0][0]
    t1 = (last["last_t"] + 1) if last else segs[-1][0] + HOUR_NS
    return int(t0), int(t1)


def load_cfg(path: str | None, synthetic_series: str | None = None):
    from dh.strategy.config import load_config

    if path == "synthetic":
        from dh.research.synth_recording import synth_strategy_config

        return synth_strategy_config(synthetic_series or "KXBTCD")
    return load_config(path) if path else load_config(REPO / "config" / "m1.yaml")


def latency_from(args, uni=None):
    """--latency-ms submit,response,ws[,md] (ms, fixed) -> LatencyModel; without the flag the research
    placeholders; md defaults to the recording's measured Kalshi market-data latency (audit M4)."""
    from dh.execution.latency import LatencyModel
    from dh.research.replay_env import research_latency

    if not args.latency_ms:
        return research_latency(uni, None, seed=args.seed) if uni is not None else None
    parts = [float(x) for x in args.latency_ms.split(",")]
    if len(parts) not in (3, 4):
        raise SystemExit("--latency-ms needs submit,response,ws[,md] in ms")
    md = parts[3] if len(parts) == 4 else None
    lat = LatencyModel.fixed(parts[0], parts[1], parts[2], md_ms=md or 0.0, seed=args.seed)
    return research_latency(uni, lat, md_ms=md, seed=args.seed) if uni is not None else lat


# options each command consumes (anything else given explicitly is an error: audit C3)
_REPLAY = {"policies", "warm", "seed", "latency_ms", "quote_period_ms", "max_strikes", "fv_config", "flow_segments", "config"}
CONSUMES: dict[str, set[str]] = {
    "universe": {"max_strikes", "config"},
    "replay": _REPLAY,
    "flow": {"flow_split", "walk_forward_days", "flow_prior_s", "max_strikes", "config"},
    "e1": {"step_ms", "latency_ms", "config", "max_strikes"},
    "e2": _REPLAY | {"step_ms", "split", "no_pnl", "no_replica", "jobs"},
    "e3": _REPLAY | {"split", "live_fills", "jobs"},
    "e4": _REPLAY | {"grid", "jobs"},
    "e67": _REPLAY | {"ledger", "confirm_t0", "confirm_t1", "split", "jobs"},
    "e8": _REPLAY | {"step_ms", "jobs"},
    "e9": _REPLAY | {"strikes", "jobs"},
    "e10": _REPLAY | {"multipliers", "no_scale_limits", "jobs"},
    "synth": {"events", "spacing", "strike_delay", "overwrite"},
    "demo": {"jobs", "events", "spacing"},
}
CONSUMES["all"] = set().union(*(CONSUMES[e] for e in ("e1", "e2", "e3", "e4", "e67", "e8", "e9", "e10")))
_ALWAYS = {"name", "root", "t0", "t1", "out", "log_level"}


def unconsumed(ap, a) -> list[str]:
    """Options given with a non-default value that the chosen command does not use."""
    used = CONSUMES.get(a.name, set()) | _ALWAYS
    bad = []
    for act in ap._actions:  # noqa: SLF001 - argparse has no public accessor
        d = act.dest
        if d in ("help",) or d in used or not act.option_strings:
            continue
        if getattr(a, d, None) != act.default:
            bad.append(act.option_strings[-1])
    return bad


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("name", choices=("universe", "replay", "flow", *EXPERIMENTS, "all", "synth", "demo"))
    ap.add_argument("--root", default=str(REPO / "data"), help="recording root (holds raw/)")
    ap.add_argument("--t0", default="auto")
    ap.add_argument("--t1", default="auto")
    ap.add_argument("--out", default=None, help="output directory (default <root>/results/<name>)")
    ap.add_argument("--config", default=None, help="strategy YAML (default config/m1.yaml); 'synthetic' = fast "
                                                   "research config for synthetic recordings")
    ap.add_argument("--policies", default="B,C", help="fill policies, e.g. A,B,C (A is reference only)")
    ap.add_argument("--jobs", type=int, default=1, help="parallel replays (fork)")
    ap.add_argument("--warm", default="recorded", help="fair-value warm-up: recorded | recorded+gbm | csv:<path> | gbm")
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--latency-ms", default="",
                    help="fixed latency 'submit,response,ws[,md]' in ms (default: placeholders; md = the recording's "
                         "measured Kalshi market-data latency, fallback 25 ms)")
    ap.add_argument("--split", type=float, default=0.5, help="E2/E3: fraction of the window used for fitting")
    ap.add_argument("--grid", default=None, help="E4: JSON string or YAML/JSON file {variant: {section: {field: value}}}")
    ap.add_argument("--multipliers", default="1,2,5,10,20,50", help="E10: clip multiples")
    ap.add_argument("--no-scale-limits", action="store_true", help="E10: keep risk limits fixed as size grows")
    ap.add_argument("--strikes", default="1,3,0", help="E9: strikes per event (0 = all)")
    ap.add_argument("--ledger", action="append", default=[], help="E6/E7: analyze ledger CSV(s) instead of replaying")
    ap.add_argument("--confirm-t0", default=None, help="E6/E7: start of a LATER, disjoint window confirming the buckets "
                                                      "selected on [t0, t1) (default: chronological split, --split)")
    ap.add_argument("--confirm-t1", default=None, help="E6/E7: end of the confirmation window")
    ap.add_argument("--live-fills", action="store_true", help="E3: also analyze our live fills in the recording")
    ap.add_argument("--step-ms", type=int, default=0, help="E2 panel / E8 decision grid step (ms)")
    ap.add_argument("--no-pnl", action="store_true", help="E2: skip the P&L hook replays")
    ap.add_argument("--no-replica", action="store_true", help="E2: skip the BRTI replica feature (deep L2 books: slow)")
    ap.add_argument("--events", type=int, default=4, help="synth/demo: number of synthetic events")
    ap.add_argument("--spacing", type=int, default=900, help="synth/demo: seconds between synthetic expiries")
    ap.add_argument("--strike-delay", type=float, default=0.0, help="synth: strike published this long after open")
    ap.add_argument("--overwrite", action="store_true", help="synth: replace an existing recording")
    ap.add_argument("--quote-period-ms", type=int, default=0,
                    help="research speed knob: strategy quote-cycle period (production config: 200 ms)")
    ap.add_argument("--max-strikes", type=int, default=0,
                    help="research speed knob: keep only the N strikes per event nearest the benchmark at the "
                         "event's first quotable time (0 = all; E9 then compares within those N)")
    ap.add_argument("--fv-config", default=None,
                    help="fair-value parameter JSON fitted strictly before t0 (walk-forward); default: the committed "
                         "config (in-sample for windows before its data_end_utc: flagged in every report)")
    ap.add_argument("--flow-segments", default=None,
                    help="taker-flow segments JSON (from the 'flow' command on an EARLIER window) for the strategy's "
                         "fill model; default: the strategy config's flow parameters")
    ap.add_argument("--flow-split", type=float, default=0.7, help="flow: chronological train fraction")
    ap.add_argument("--walk-forward-days", type=int, default=0, help="flow: walk-forward by day (> 0) instead")
    ap.add_argument("--flow-prior-s", type=float, default=1800.0,
                    help="flow: gamma-Poisson prior exposure (s) shrinking thin segments toward the pooled rate")
    ap.add_argument("--log-level", default="WARNING")
    a = ap.parse_args(argv)
    bad = unconsumed(ap, a)
    if bad:
        ap.error(f"{', '.join(bad)} not used by '{a.name}' (see the command list; nothing was run)")
    logging.basicConfig(level=a.log_level.upper(), format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    wall = time.perf_counter()

    if a.name == "demo":
        from dh.research.synthetic_demo import run_demo

        run_demo(out_dir=a.out or str(REPO / "docs" / "research" / "synthetic_demo"),
                 data_root=None if a.root == str(REPO / "data") else a.root, n_jobs=a.jobs, n_events=a.events,
                 spacing_s=a.spacing)
        print(f"demo done in {time.perf_counter() - wall:.0f}s")
        return 0
    if a.name == "synth":
        from dh.research.synth_recording import SynthRecordingConfig, write_synthetic_recording

        info = write_synthetic_recording(a.root, SynthRecordingConfig(n_events=a.events, event_spacing_s=a.spacing,
                                                                      strike_delay_s=a.strike_delay),
                                         overwrite=a.overwrite)
        print(json.dumps({"root": info.root, "t0": fmt_ns(info.t0), "t1": fmt_ns(info.t1), "records": info.n_records,
                          "events": list(info.tickers)}, indent=2))
        print("SYNTHETIC DATA — pipeline validation only. Use --config synthetic for fast replays.")
        return 0

    root = a.root
    t0 = parse_time(a.t0) if a.t0 != "auto" else coverage(root)[0]
    t1 = parse_time(a.t1) if a.t1 != "auto" else coverage(root)[1]
    if t1 <= t0:
        raise SystemExit("t1 must be after t0")
    out = Path(a.out) if a.out else Path(root) / "results" / a.name
    cfg = load_cfg(a.config)
    if a.quote_period_ms:
        from dataclasses import replace

        cfg = replace(cfg, timers=replace(cfg.timers, quote_period_ms=a.quote_period_ms))
    policies = parse_policies(a.policies)
    from dh.research.replay_env import NearestStrikes, bind_replay_inputs, build_universe, inputs_meta, restrict_universe

    uni = build_universe(root, t0, t1)
    if a.max_strikes > 0:
        uni = restrict_universe(uni, NearestStrikes(a.max_strikes, cfg.quoting.max_tau_s), cfg.quoting.enabled_series,
                                reason=f"--max-strikes {a.max_strikes}")
    bind_replay_inputs(uni, fv_config=a.fv_config, flow_segments=a.flow_segments)
    print(f"window {fmt_ns(t0)} .. {fmt_ns(t1)}  markets={len(uni.markets)} specs={len(uni.specs())} "
          f"rejected={len(uni.rejected())} own_fills={len(uni.own_fills)}"
          + ("  [SYNTHETIC DATA — pipeline validation only]" if uni.synthetic else ""))
    for n in uni.notes:
        print("note:", n)
    imeta, iwarn = inputs_meta(uni, t0)
    lat = None
    if a.name not in ("flow", "universe"):
        print("; ".join(f"{k}: {v}" for k, v in imeta.items()))
        for w in iwarn:
            print("WARNING:", w)
        lat = latency_from(a, uni)
        from dh.research.replay_env import describe_latency

        print("latency:", describe_latency(lat, uni))
    if set(policies) & {"B", "C"} != {"B", "C"} and a.name not in ("flow", "universe", "e1"):
        print("WARNING: verdicts need results under BOTH fill policies B and C; they will be INCONCLUSIVE")

    def progress(msg: str) -> None:
        print("  ", msg, flush=True)

    names = EXPERIMENTS if a.name == "all" else (a.name,)
    for name in names:
        t_start = time.perf_counter()
        o = out / name if a.name == "all" else out
        if name == "universe":
            print(uni.fee_table().to_string(index=False))
            rej = uni.rejected()
            if rej:
                print(f"{len(rej)} markets without a usable spec, e.g.:")
                for k, v in list(sorted(rej.items()))[:10]:
                    print(f"  {k}: {v}")
            continue
        if name == "flow":
            from dh.research.flow_recording import fit_flow

            res = fit_flow(root, t0, t1, o, universe=uni, train_frac=a.flow_split, walk_forward_days=a.walk_forward_days,
                           prior_s=a.flow_prior_s)
            print(res.metrics.to_string(index=False))
            print(f"flow_segments.json: fitted on data through {fmt_ns((res.all_end_ms or 0) * 1_000_000)} "
                  "(use for replays that start later)")
        elif name == "replay":
            from dh.research.replay_env import run_replay

            o.mkdir(parents=True, exist_ok=True)
            for p in policies:
                res = run_replay(root, t0, t1, cfg, p, latency=lat, warm=a.warm, seed=a.seed, universe=uni)
                res.df.to_csv(o / f"ledger_{p}.csv", index=False, float_format="%.6g")
                (o / f"summary_{p}.json").write_text(json.dumps(res.summary, indent=2, default=str))
                s = res.summary
                print(f"policy {p}: fills={s.get('fills')} net={s.get('net_c_per_contract', math.nan):.3f}c/ct "
                      f"CI={s.get('net_c_ci95')} $/day={s.get('net_usd_per_day', 0):.2f} fv_warm={s.get('fv_warm')}")
                for w in s.get("warnings", []):
                    print("WARNING:", w)
        elif name == "e1":
            from dh.research import exp1_staleness as m

            m.run(root, t0, t1, o, cfg=cfg, step_ms=a.step_ms or 100, universe=uni,
                  md_latency_ms=lat.dists["md"].median() if lat is not None else None)
        elif name == "e2":
            from dh.research import exp2_nowcast as m

            m.run(root, t0, t1, o, cfg=cfg, step_ms=a.step_ms or 200, pnl=not a.no_pnl, policies=policies,
                  split_frac=a.split, warm=a.warm, universe=uni, n_jobs=a.jobs, progress=progress,
                  replica=not a.no_replica, seed=a.seed, latency=lat)
        elif name == "e3":
            from dh.research import exp3_toxicity as m

            m.run(root, t0, t1, o, cfg=cfg, policies=policies, split_frac=a.split, warm=a.warm, universe=uni,
                  live=a.live_fills, seed=a.seed, n_jobs=a.jobs, progress=progress, latency=lat)
        elif name == "e4":
            from dh.research import exp4_queue as m

            m.run(root, t0, t1, o, cfg=cfg, policies=policies, grid=a.grid, warm=a.warm, n_jobs=a.jobs, universe=uni,
                  seed=a.seed, progress=progress, latency=lat)
        elif name == "e67":
            from dh.research import exp67_segments as m

            m.run(root, t0, t1, o, cfg=cfg, policies=parse_policies(a.policies if "A" in a.policies else "A," + a.policies),
                  warm=a.warm, n_jobs=a.jobs, universe=uni, ledgers=a.ledger, seed=a.seed, progress=progress, latency=lat,
                  confirm_t0=parse_time(a.confirm_t0) if a.confirm_t0 else None,
                  confirm_t1=parse_time(a.confirm_t1) if a.confirm_t1 else None, select_frac=a.split)
        elif name == "e8":
            from dh.research import exp8_taker as m

            m.run(root, t0, t1, o, cfg=cfg, policies=policies, latency=lat, step_ms=a.step_ms or 250,
                  warm=a.warm, universe=uni, seed=a.seed, n_jobs=a.jobs)
        elif name == "e9":
            from dh.research import exp9_multistrike as m

            m.run(root, t0, t1, o, cfg=cfg, policies=policies, counts=[int(x) for x in a.strikes.split(",")],
                  warm=a.warm, n_jobs=a.jobs, universe=uni, seed=a.seed, progress=progress, latency=lat)
        elif name == "e10":
            from dh.research import exp10_capacity as m

            m.run(root, t0, t1, o, cfg=cfg, policies=policies, multipliers=[float(x) for x in a.multipliers.split(",")],
                  scale_limits=not a.no_scale_limits, warm=a.warm, n_jobs=a.jobs, universe=uni, seed=a.seed,
                  progress=progress, latency=lat)
        print(f"{name}: wrote {o} ({time.perf_counter() - t_start:.0f}s)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
