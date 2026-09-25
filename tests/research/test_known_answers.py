"""Known-answer tests: research methods must recover effects injected into synthetic markets."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from dh.core.events import KalshiTrade, Settlement
from dh.research.exp0_maker_pnl import event_bootstrap, prepare
from dh.research.exp1_staleness import build_panel, half_life, lead_lag
from dh.sim.synthetic import SynthConfig, SyntheticMarket


@pytest.mark.slow
def test_exp1_recovers_injected_staleness_ordering():
    hl = []
    for lag in (0.3, 3.0):
        sm = SyntheticMarket(SynthConfig(duration_s=600, seed=21, mm_lag_s=lag, mm_update_prob=0.5,
                                         informed=False, vol_ann=0.6))
        evs = sm.generate()
        panel = build_panel(evs, {s.ticker: s for s in sm.specs()}, step_ms=100, vol_ann=0.6)
        hl.append(half_life(lead_lag(panel)))
    assert hl[0] < 0.5 < hl[1] < 3.0


def _trades(cfg_kw, n=6):
    tr, mk = [], []
    for e in range(n):
        cfg = SynthConfig(seed=500 + e, duration_s=600, expiration_ns=(1_790_300_000 + e * 3600) * 10**9, **cfg_kw)
        for x in SyntheticMarket(cfg).generate():
            if isinstance(x, KalshiTrade):
                tr.append(dict(ticker=f"{x.ticker}-{e}", ts_ms=x.ts_exch // 10**6, yes_px=x.yes_px, qty=x.qty,
                               taker_side=x.taker_side))
            elif isinstance(x, Settlement):
                mk.append(dict(ticker=f"{x.ticker}-{e}", event_ticker=f"E{e}", expiration_ts_ms=cfg.expiration_ns // 10**6,
                               result=x.result, strike_type="greater", floor_strike=float(x.ticker.split("-")[1]),
                               cap_strike=np.nan))
    return pd.DataFrame(tr), pd.DataFrame(mk)


@pytest.mark.slow
def test_exp0_sign_of_maker_pnl_follows_flow_toxicity():
    tr, mk = _trades(dict(informed=False))
    m_uninf, lo, _ = event_bootstrap(prepare(tr, mk, maker_rate=0.0), "maker_gross", 200)
    assert m_uninf > 0
    tr, mk = _trades(dict(informed=True, informed_edge_ticks=0.5))
    m_inf, _, _ = event_bootstrap(prepare(tr, mk, maker_rate=0.0), "maker_gross", 200)
    assert m_inf < m_uninf
