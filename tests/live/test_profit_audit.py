"""Regression scenarios for restart accounting and streaming performance attribution."""
import gzip
import json
import math

import pytest
import zstandard

from dh.backtest.ledger import Ledger
from dh.core.events import KalshiFill, Settlement
from dh.core.units import NS_PER_S as S
from dh.kalshi.fees import FeeEngine
from dh.live.config import PaperCfg
from dh.live.replay import ledger_from_log, ledger_from_logs, logged_decisions
from dh.live.riskstate import RiskState, day_start, decide_seed
from dh.live.startup import build_paper_sim
from dh.live.tools import _session_of
from .fakes import kxbtcd_spec


def test_paper_restart_new_portfolio_does_not_inherit_phantom_cash_or_mark():
    now = 1_790_400_000 * S
    prev = RiskState(day_start(now), 2.17, True, 'fee_mismatch', now + S, 'old', 'paper', now,
                     realized_usd=-10.0, mark_usd=12.17, budget_base_usd=-2.0)
    seed = decide_seed(now, prev, None, independent_paper=True)
    assert seed.day_pnl_usd == seed.realized_usd == seed.mark_usd == seed.budget_base_usd == 0
    assert seed.halted and seed.halt_reason == 'fee_mismatch' and seed.pause_until_ns == now + S
    assert prev.mark_usd == 12.17  # immutable input audit trail
    with pytest.raises(ValueError):
        decide_seed(now, prev, object(), independent_paper=True)


@pytest.mark.parametrize('sigma', [0.0, 0.4])
def test_paper_market_data_offset_is_configurable_and_conservative(sigma):
    sim, _ = build_paper_sim(PaperCfg(sigma=sigma, md_ms=20.0), [], FeeEngine.from_config())
    assert sim.md_offset == 30_000_000  # default C multiplier 1.5
    sim0, _ = build_paper_sim(PaperCfg(sigma=sigma, md_ms=0.0), [], FeeEngine.from_config())
    assert sim0.md_offset == 0  # exact legacy replay remains possible


def write_log(path, records):
    data = ''.join(json.dumps(r) + '\n' for r in records).encode()
    if path.suffix == '.gz':
        data = gzip.compress(data)
    elif path.suffix == '.zst':
        data = zstandard.ZstdCompressor().compress(data)
    path.write_bytes(data)


@pytest.mark.parametrize('suffix', ['.jsonl', '.jsonl.gz', '.jsonl.zst'])
def test_streaming_ledger_preserves_match_times_full_duration_and_sparse_marks(tmp_path, suffix):
    spec = kxbtcd_spec()
    records = [{'k': 'session_start', 't': S}]
    # Thousands of irrelevant FV rows cannot inflate the retained ledger history.
    records += [{'k': 'log.fv', 't': S + i * 10_000_000, 'ticker': spec.ticker,
                 'F': 0.50, 'delta': 0.0} for i in range(1000)]
    records += [
        {'k': 'log.fv', 't': 12 * S, 'ticker': spec.ticker, 'F': 0.40},
        {'k': 'log.fill', 't': 13 * S, 'ts_exch': 11 * S, 'ticker': spec.ticker,
         'trade_id': 'fill', 'side': 'bid', 'px': 4500, 'qty': 100, 'fee': 0},
        {'k': 'log.settle', 't': 20 * S, 'ticker': spec.ticker, 'px': 10000},
        {'k': 'session_end', 't': 3601 * S}]
    path = tmp_path / ('paper-session' + suffix)
    write_log(path, records)
    led = ledger_from_log(path, [spec])
    df = led.attribute()
    assert df.ts.iloc[0] == 11 * S and df.F.iloc[0] == .50
    assert 'mo_0.1s_c' not in df  # previous sample cannot demonstrate zero adverse selection
    assert df['mo_1s_c'].iloc[0] == pytest.approx(-10.)
    assert df['mo_1s_observed_ts'].iloc[0] == 12 * S
    assert sum(len(v.ts) for v in led.fv.values()) <= 8
    summary = led.summary()
    assert summary['days'] == pytest.approx(1 / 24)
    assert summary['net_usd_per_day'] == pytest.approx(.55 * 24)
    assert all(math.isnan(x) for x in summary['net_c_ci95'])
    assert not summary['inference_sufficient']
    assert _session_of(str(path)) == 'paper-session'
    assert len(logged_decisions(path, 10_000 * S)[1]) == 1003


def test_lifetime_audit_keeps_restart_fills_and_joins_later_outcomes(tmp_path):
    spec = kxbtcd_spec()
    paths = [tmp_path / 'paper-one.jsonl', tmp_path / 'paper-two.jsonl.zst']
    fill = {'k': 'log.fill', 'ticker': spec.ticker, 'trade_id': 'paper-f1',
            'side': 'bid', 'px': 9000, 'qty': 100, 'fee': 0}
    write_log(paths[0], [{'k': 'session_start', 't': S}, dict(fill, t=2*S),
                         {'k': 'risk_seed', 't': 3*S, 'day_pnl_usd': 2000},
                         {'k': 'session_end', 't': 4*S}])
    write_log(paths[1], [{'k': 'session_start', 't': 5*S}, dict(fill, t=6*S),
                         {'k': 'log.settle', 't': 7*S, 'ticker': spec.ticker, 'px': 0},
                         {'k': 'session_end', 't': 8*S}])
    prior = ledger_from_log(paths[0], [spec]).summary()
    assert prior['unresolved_fills'] == 1
    summary = ledger_from_logs(paths, [spec]).summary()
    assert summary['fills'] == summary['settled_fills'] == 2
    assert summary['unresolved_fills'] == 0
    assert summary['net_usd'] == -1.8
    assert summary['days'] == pytest.approx(6 / 86400)
    with pytest.raises(ValueError, match='more than once'):
        ledger_from_logs([paths[0], paths[0]], [spec])


def test_series_sharing_expiration_are_one_inference_cluster():
    led = Ledger({'A': 'eventA', 'B': 'eventB'}, {'A': 20*S, 'B': 20*S})
    for ticker in ('A', 'B'):
        led.on_event(KalshiFill(2*S, 0, ticker, ticker, '', '', 'bid', 5000, 100, False, 0, 0, False))
        led.on_event(Settlement(20*S, 0, ticker, 'yes', None, 10000))
    summary = led.summary()
    assert summary['events'] == 1
    assert all(math.isnan(x) for x in summary['net_c_ci95'])


def test_overlapping_sessions_keep_separate_fair_values(tmp_path):
    spec = kxbtcd_spec()
    paths = [tmp_path / 'a.jsonl', tmp_path / 'b.jsonl']
    for i, path in enumerate(paths):
        write_log(path, [
            {'k': 'log.fv', 't': S, 'ticker': spec.ticker, 'F': .2 + .6*i},
            {'k': 'log.fill', 't': S + 1, 'ticker': spec.ticker, 'side': 'bid', 'px': 5000,
             'qty': 100, 'trade_id': 'same-id'},
            {'k': 'log.settle', 't': 3*S, 'ticker': spec.ticker, 'px': 10000}])
    df = ledger_from_logs(paths, [spec]).attribute()
    assert df.F.tolist() == pytest.approx([.2, .8])


def test_downloaded_outcomes_join_only_explicit_results_and_reject_conflicts(tmp_path):
    import pyarrow as pa
    import pyarrow.parquet as pq
    from dh.live.replay import add_downloaded_outcomes

    spec = kxbtcd_spec()
    log = tmp_path / 'one.jsonl'
    write_log(log, [{'k': 'log.fill', 't': S, 'ticker': spec.ticker, 'side': 'bid', 'px': 9000, 'qty': 100}])
    led = ledger_from_log(log, [spec])
    path = tmp_path / 'markets' / f"series={spec.event_ticker.split('-', 1)[0]}" / (spec.event_ticker + '.parquet')
    path.parent.mkdir(parents=True)
    def outcome(result, px):
        pq.write_table(pa.Table.from_pylist([{'ticker': spec.ticker, 'result': result, 'settlement_px': px}],
                                           schema=pa.schema([('ticker', pa.string()), ('result', pa.string()),
                                                             ('settlement_px', pa.int64())])), path)
    outcome('', None)
    assert add_downloaded_outcomes(led, tmp_path) == 0 and led.summary()['unresolved_fills'] == 1
    outcome('no', 0)
    assert add_downloaded_outcomes(led, tmp_path) == 1 and led.summary()['net_usd'] == -.9
    outcome('yes', 10000)
    with pytest.raises(ValueError, match='conflicting'):
        add_downloaded_outcomes(led, tmp_path)
    outcome('yes', 0)
    with pytest.raises(ValueError, match='inconsistent'):
        add_downloaded_outcomes(led, tmp_path)
