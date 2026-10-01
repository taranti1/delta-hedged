"""Quick screen (2026-09-30): the neighbouring-strike rule (risk.block_opposite_neighbor_usd).

Fixed-opportunity, like dh/research/rule_screen_20260930.py: every recorded fill is kept or
removed; freed risk budget and the quotes placed instead are NOT modeled (a replay does that).
A fill is flagged when, in fill order within its run and settlement hour, it opens or adds a
YES position opposite in sign to one held on a 'greater' strike within G dollars (a fill that
reduces its own market's position is never flagged). Primary G = $100 (adjacent KXBTCD
strikes: the first live hour, short 83,400 + long 83,500, settled inside the band for -$4.60).

    python -m dh.research.neighbor_screen_20260930 data/results/screen_20260930/fills.parquet OUT_DIR
"""
from __future__ import annotations

import sys
from collections import defaultdict
from pathlib import Path

import numpy as np
import pandas as pd

GAPS = (100.0, 200.0, 300.0)
PRIMARY = 100.0

# the first live session (live-20261001T022326Z-tm7in2t7), from its log
LIVE = [
    dict(src="live", run="live-20261001T022326Z-tm7in2t7", cfg="m1_live", ticker="KXBTCD-26SEP3023-T83399.99",
         t_fill=1790821704681083758, side=-1, px=0.55, ct=5.0, fee=0.0, settle=1.0),
    dict(src="live", run="live-20261001T022326Z-tm7in2t7", cfg="m1_live", ticker="KXBTCD-26SEP3023-T83499.99",
         t_fill=1790821917479879299, side=1, px=0.47, ct=5.0, fee=0.0, settle=0.0),
]


def strike(ticker: str) -> float:
    return float(ticker.rsplit("-T", 1)[1])


def flag(fills: pd.DataFrame, gap: float) -> np.ndarray:
    out = np.zeros(len(fills), bool)
    for _, g in fills.groupby(["run", "expiry"], sort=False):
        pos: dict[str, float] = defaultdict(float)
        for i in g.sort_values("t_fill").index:
            r = fills.loc[i]
            d = float(r.side)
            if pos[r.ticker] * d >= 0:  # opens or adds (a reduction is never flagged)
                k = strike(r.ticker)
                out[fills.index.get_loc(i)] = any(
                    t2 != r.ticker and abs(strike(t2) - k) <= gap + 0.01 and q * d < 0 for t2, q in pos.items())
            pos[r.ticker] += d * float(r.ct)
    return out


def boot_ci(per_exp: pd.Series, n: int = 4000, seed: int = 20260930) -> tuple[float, float]:
    rng = np.random.default_rng(seed)
    v = per_exp.values
    s = [v[rng.integers(0, len(v), len(v))].sum() for _ in range(n)]
    return tuple(np.percentile(s, [2.5, 97.5]))  # type: ignore[return-value]


def main(fills_path: str, out_dir: str) -> None:
    f = pd.read_parquet(fills_path)
    live = pd.DataFrame(LIVE)
    live["expiry"] = 1790823600 * 10**9
    live["net"] = live.side * live.ct * (live.settle - live.px) - live.fee
    f = pd.concat([f, live], ignore_index=True)
    f = f[f.settle.notna()].reset_index(drop=True)
    sets = {
        "paper_all": f.src.eq("paper"),
        "paper_m1_corrected": f.src.eq("paper") & f.cfg.eq("ece7ceef2c224a55"),
        "paper_m1_small": f.src.eq("paper") & f.cfg.eq("860cfc8faa25933c"),
        "replay_B_m1": f.run.isin(["s1_day0926_base_B", "s3_later_B"]),
        "replay_C_m1": f.run.isin(["s1_day0926_base_C", "s3_later_C"]),
        "replay_B_small": f.run.eq("s4_small_B"),
        "replay_C_small": f.run.eq("s4_small_C"),
        "live": f.src.eq("live"),
    }
    rows = []
    for gap in GAPS:
        fl = flag(f, gap)
        for name, m in sets.items():
            d = f[m.values]
            b = fl[m.values]
            per_exp = d.assign(x=np.where(b, -d.net, 0.0)).groupby("expiry").x.sum()  # change from blocking
            lo, hi = boot_ci(per_exp) if len(per_exp) > 1 else (np.nan, np.nan)
            base = d.groupby("expiry").net.sum()
            kept = base + per_exp
            rows.append(dict(gap=gap, dataset=name, fills=len(d), flagged=int(b.sum()),
                             flagged_ct=d.ct[b].sum(), flagged_net=d.net[b].sum(),
                             flagged_c_ct=100 * d.net[b].sum() / max(d.ct[b].sum(), 1e-9),
                             other_c_ct=100 * d.net[~b].sum() / max(d.ct[~b].sum(), 1e-9),
                             base_net=base.sum(), change=per_exp.sum(), ci_lo=lo, ci_hi=hi,
                             base_worst_hour=base.min(), kept_worst_hour=kept.min()))
    res = pd.DataFrame(rows)
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    res.to_csv(Path(out_dir) / "neighbor_rule.csv", index=False)
    pd.set_option("display.width", 250)
    print(res.round(2).to_string(index=False))


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
