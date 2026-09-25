"""End-to-end pipeline validation on a SYNTHETIC recording (not evidence of edge).

    python scripts/run_experiment.py demo --jobs 4          # -> docs/research/synthetic_demo/

1. writes a synthetic recording in the collector's formats (dh.research.synth_recording: raw
   Kalshi WS frames and REST records, normalized-event cache for the external venues) with
   injected effects: background-maker lag, informed (latency) takers, a delayed benchmark;
2. rebuilds its market universe from the recorded REST/lifecycle records;
3. runs every experiment (E1, E2, E3, E4, E6/E7, E8, E9, E10) through the same CLI code paths as for
   real recordings, under fill policies B and C (A where the experiment reports it);
4. writes the small CSV/markdown outputs plus README.md (index, known-answer checks, runtimes).
The recording itself (tens of MB) goes to a scratch data root and is not committed; it is
regenerated bit-for-bit from the seed.
"""

from __future__ import annotations

import json
import math
import tempfile
import time
from pathlib import Path
from typing import Any

import pandas as pd

from dh.research import (
    exp1_staleness,
    exp2_nowcast,
    exp3_toxicity,
    exp4_queue,
    exp8_taker,
    exp9_multistrike,
    exp10_capacity,
    exp67_segments,
)
from dh.research.exp_common import SYNTHETIC_BANNER, SYNTHETIC_NOTE, fmt_ns, git_commit
from dh.research.replay_env import build_universe, run_replay
from dh.research.synth_recording import SynthRecordingConfig, synth_strategy_config, write_synthetic_recording
from dh.sim.synthetic import SynthConfig

DEMO_SYNTH = SynthConfig(n_strikes_each_side=4, mm_lag_s=1.5, informed=True, informed_edge_ticks=1.0, vol_ann=0.6,
                         mm_update_prob=0.3, noise_taker_rate_per_s=0.06, longshot_bias=0.2)


def run_demo(out_dir: str | Path = "docs/research/synthetic_demo", data_root: str | Path | None = None, *,
             n_jobs: int = 4, n_events: int = 4, spacing_s: int = 900, seed: int = 7) -> dict[str, Any]:
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    root = Path(data_root) if data_root else Path(tempfile.mkdtemp(prefix="dh_synth_demo_"))
    timings: dict[str, float] = {}
    t = time.perf_counter()
    rc = SynthRecordingConfig(n_events=n_events, event_spacing_s=spacing_s, synth=DEMO_SYNTH, strike_delay_s=15.0,
                              seed=seed)
    info = write_synthetic_recording(root, rc, overwrite=True)
    timings["write_recording"] = time.perf_counter() - t
    t0, t1 = info.t0, info.t1
    cfg = synth_strategy_config(info.series)
    uni = build_universe(root, t0, t1)
    results: dict[str, Any] = {}

    def stage(name: str, fn):
        s = time.perf_counter()
        results[name] = fn()
        timings[name] = time.perf_counter() - s

    # single replay summary per policy (the base of every P&L experiment)
    def base_replays():
        rows = []
        for p in ("A", "B", "C"):
            r = run_replay(root, t0, t1, cfg, p, universe=uni)
            s = r.summary
            rows.append({"policy": p, "fills": s.get("fills"), "contracts": s.get("contracts"),
                         "net_c_per_contract": s.get("net_c_per_contract"), "net_c_ci95": s.get("net_c_ci95"),
                         "net_usd": s.get("net_usd"), "markout_10s_c": s.get("markout_10s_c"),
                         "quotes_placed": s.get("mm_quotes_placed"), "cancels": s.get("mm_cancels"),
                         "markets": s.get("n_specs"), "added_late": s.get("n_added_late"), "fv_warm": s.get("fv_warm"),
                         "wall_s": round(s.get("wall_s", 0.0), 1)})
        df = pd.DataFrame(rows)
        df.insert(0, "synthetic", True)
        df.to_csv(out / "replay_summary.csv", index=False, float_format="%.6g")
        return df

    stage("replay", base_replays)
    stage("e1", lambda: exp1_staleness.run(root, t0, t1, out / "e1", cfg=cfg, universe=uni))
    stage("e2", lambda: exp2_nowcast.run(root, t0, t1, out / "e2", cfg=cfg, universe=uni, n_jobs=n_jobs))
    stage("e3", lambda: exp3_toxicity.run(root, t0, t1, out / "e3", cfg=cfg, universe=uni, n_jobs=n_jobs))
    stage("e4", lambda: exp4_queue.run(root, t0, t1, out / "e4", cfg=cfg, universe=uni, n_jobs=n_jobs))
    stage("e67", lambda: exp67_segments.run(root, t0, t1, out / "e67", cfg=cfg, universe=uni, n_jobs=n_jobs))
    stage("e8", lambda: exp8_taker.run(root, t0, t1, out / "e8", cfg=cfg, universe=uni, n_jobs=n_jobs))
    stage("e9", lambda: exp9_multistrike.run(root, t0, t1, out / "e9", cfg=cfg, universe=uni, n_jobs=n_jobs))
    stage("e10", lambda: exp10_capacity.run(root, t0, t1, out / "e10", cfg=cfg, universe=uni, n_jobs=n_jobs))
    for p in out.rglob("e67_segments_ledger_*.csv"):
        p.unlink()  # per-fill ledgers are regenerated on demand; keep the committed outputs small
    for p in out.rglob("e3_toxicity_fills_*.csv"):
        p.unlink()
    for p in out.rglob("e8_taker_takes_*.csv"):
        p.unlink()
    _write_readme(out, info, uni, results, timings, cfg, n_jobs)
    return {"info": info, "results": results, "timings": timings}


def _fmt(x: Any, nd: int = 2) -> str:
    try:
        f = float(x)
    except (TypeError, ValueError):
        return str(x)
    return "nan" if not math.isfinite(f) else f"{f:.{nd}f}"


def _write_readme(out: Path, info, uni, results: dict[str, Any], timings: dict[str, float], cfg, n_jobs: int) -> None:
    r = results
    e2 = r["e2"]["forecast"]
    e2s = e2[(e2.model == "ridge")].set_index("horizon")["rmse_gain_pct"].to_dict() if len(e2) else {}
    e8 = r["e8"]["summary"]
    e8b = e8[(e8.policy == "B") & (e8.threshold_c == 0.0)].iloc[0] if len(e8) else None
    e3m = r["e3"]["markouts"]
    e10c = r["e10"]["capacity"]
    bk = e10c[(e10c.level_c == 0.0) & (e10c.basis == "point")] if len(e10c) else e10c
    rep = r["replay"]
    lines = [
        f"# Synthetic demo — {SYNTHETIC_BANNER}", "",
        SYNTHETIC_NOTE, "",
        "Every file in this directory was produced by `python scripts/run_experiment.py demo --jobs "
        f"{n_jobs}` (module `dh.research.synthetic_demo`), from a synthetic recording written in the "
        "collector's on-disk formats and replayed through exactly the code paths used for real recordings. "
        "It proves the pipeline runs end to end; it says nothing about Kalshi.", "",
        "## Recording", "",
        f"* generator `dh.research.synth_recording` (seed {info.config.get('seed')}), git `{git_commit()}`",
        f"* window {fmt_ns(info.t0)} .. {fmt_ns(info.t1)}: {len(info.expirations)} events of {info.config.get('event_spacing_s')} s, "
        f"{sum(len(v) for v in info.tickers.values())} markets ({info.series}, fee type {info.config.get('fee_type')}), "
        f"strikes published {info.config.get('strike_delay_s')} s after open (KXBTC15M-style metadata_updated path)",
        f"* formats: raw Kalshi WS frames (`kalshi.ws`: {info.n_records.get('kalshi.ws', 0)} frames) and REST records "
        "(`kalshi.rest.*`: markets, events, series, fee changes, settled markets, CF-history warm-up); external venues "
        f"as a normalized-event cache (`events.md.ext`: {info.n_records.get('events.md.ext', 0)} events)",
        f"* injected: {json.dumps(info.config.get('injected'), default=str)}",
        f"* universe rebuilt from the recording: {len(uni.specs())} specs, {len(uni.rejected())} rejected, fee table "
        + "; ".join(f"{x.series} {x.fee_type} x{x.fee_multiplier:g}" for x in uni.fee_table().itertuples()),
        f"* strategy config: `dh.research.synth_recording.synth_strategy_config` (quote cycle "
        f"{cfg.timers.quote_period_ms} ms, clip {cfg.quoting.clip_contracts:g}, research-only)", "",
        "## Known-answer checks (asserted in tests/research/)", "",
        f"* **E2** benchmark published {info.config['injected']['brti_delay_ms']} ms after the venues move -> the "
        "walk-forward nowcast must beat the last print: ridge RMSE gain "
        + ", ".join(f"{h} {_fmt(v, 1)}%" for h, v in e2s.items()) + " (`e2/`).",
        f"* **E8** background makers lag BTC by {info.config['injected']['mm_lag_s']} s -> stale quotes exist: "
        + (f"{_fmt(e8b.opportunities_per_day, 0)} opportunities/day at 0c edge (B), 5 s markout of taker fills "
           f"{_fmt(e8b.net_5s_c)} c/contract after fees" if e8b is not None else "n/a") + " (`e8/`). "
        "tests/research/test_experiments_synthetic.py compares a 0.2 s and a 3 s maker lag.",
        "* **E3** informed takers pick off stale quotes -> maker fills are adversely selected; the test compares "
        "shadow-fill markouts with and without informed flow (`e3/`).", "",
        "## Results index (synthetic; policies B and C unless noted)", "",
        "| experiment | output | headline (SYNTHETIC) | runtime |", "|---|---|---|---|",
        f"| replay | `replay_summary.csv` | fills A/B/C = {', '.join(str(x) for x in rep['fills'])}; net c/ct B "
        f"{_fmt(rep.loc[rep.policy == 'B', 'net_c_per_contract'].iloc[0])} | {timings['replay']:.0f} s |",
        f"| E1 staleness | `e1/e1_staleness.md` | gap-closure half-life {_fmt(r['e1']['half_life_s'])} s "
        f"(injected maker lag {info.config['injected']['mm_lag_s']} s) | {timings['e1']:.0f} s |",
        f"| E2 nowcast | `e2/e2_nowcast.md` | ridge RMSE gain {', '.join(f'{h}: {_fmt(v, 1)}%' for h, v in e2s.items())} | {timings['e2']:.0f} s |",
        f"| E3 toxicity | `e3/e3_toxicity.md` | {len(r['e3']['fills'].get('B', []))} B fills; net 10 s markout B "
        f"{_fmt(e3m[(e3m.policy == 'B') & (e3m.horizon == '10s')].net_markout_c.iloc[0]) if len(e3m) else 'n/a'} c | {timings['e3']:.0f} s |",
        f"| E4 queue | `e4/e4_queue.md` | {len(r['e4']['table'])} variant x policy rows | {timings['e4']:.0f} s |",
        f"| E6/E7 segments | `e67/e67_segments.md` | tau/|z|/price buckets under A, B, C | {timings['e67']:.0f} s |",
        f"| E8 taker | `e8/e8_taker.md` | see known-answer check above | {timings['e8']:.0f} s |",
        f"| E9 multi-strike | `e9/e9_multistrike.md` | nearest 1 / 3 / all strikes | {timings['e9']:.0f} s |",
        "| E10 capacity | `e10/e10_capacity.md` | breakeven clip multiple "
        + ", ".join(f"{x.policy}: {_fmt(x.max_clip_multiple, 1)}" for x in bk.itertuples()) + f" | {timings['e10']:.0f} s |",
        "", f"Recording written in {timings['write_recording']:.0f} s; {n_jobs} worker processes.", "",
        "Per-fill tables (ledgers, E3 fills, E8 takes) are not committed; rerun the demo to regenerate them.",
    ]
    (out / "README.md").write_text("\n".join(lines) + "\n")
