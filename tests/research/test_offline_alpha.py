"""Causality and time-unit guards for offline feature analysis."""
import numpy as np
import pandas as pd
from dh.research.offline_alpha_models import asof_values,attach_features
from dh.research.offline_alpha import NS


def test_asof_does_not_use_equal_or_future_received_tick():
    actual=asof_values(np.array([10,11,20,21]),np.array([10,20]),np.array([100.,999.]))
    np.testing.assert_allclose(actual,[np.nan,100.,100.,999.],equal_nan=True)


def test_gap_cannot_be_backfilled_from_next_observation():
    actual=asof_values(np.array([1,10,25]),np.array([0,20]),np.array([100.,200.]),max_age_ns=3)
    np.testing.assert_allclose(actual,[100.,np.nan,np.nan],equal_nan=True)


def test_future_price_change_leaves_all_earlier_features_unchanged():
    times=np.array([10,20,30]);query=np.array([15,20,25,30])
    first=asof_values(query,times,np.array([100.,101.,102.]))
    second=asof_values(query,times,np.array([100.,101.,1000000.]))
    np.testing.assert_array_equal(first,second)


def test_threshold_ticker_expiration_is_nanoseconds():
    x=pd.Series(['26SEP2717'])
    ns=pd.to_datetime(x,format='%y%b%d%H').dt.tz_localize('America/New_York').dt.tz_convert('UTC').dt.as_unit('ns').astype('int64').iloc[0]
    assert ns==pd.Timestamp('2026-09-27T21:00Z').value
    assert (ns-pd.Timestamp('2026-09-27T20:30Z').value)/NS==1800


def test_quote_log_after_action_only_joins_same_decision(tmp_path):
    import json
    from dh.research.offline_alpha_models import repair_quote_cache
    t=pd.Timestamp('2026-09-27T20:30Z').value
    order=dict(session='paper-test',t=t,ticker='KXBTCD-26SEP2717-T84099.99',side=1,px=.5,coid='id1',F=.6,delta=.001,z=0.,fv_t=t,q_eff=np.nan,position='unknown',intensity=np.nan,score=np.nan,value=np.nan,edge=np.nan)
    pd.DataFrame([order]).to_parquet(tmp_path/'orders.parquet')
    log=tmp_path/'paper-test.jsonl'
    records=[dict(k='log.quote',t=t,ticker=order['ticker'],side='bid',px=5000,coid='id1',existing_id='',q_eff=20.,F=.6,fv_ts=t,ts_decision=t),
             dict(k='log.quote',t=t+100,ticker=order['ticker'],side='bid',px=5000,coid='id1',existing_id='',q_eff=999.,F=.99,fv_ts=t+100,ts_decision=t+100)]
    log.write_text('\n'.join(json.dumps(x) for x in records))
    (tmp_path/'paper_meta.json').write_text(json.dumps({'inputs':[{'path':str(log)}]}))
    repair_quote_cache(tmp_path)
    d=pd.read_parquet(tmp_path/'orders.parquet')
    assert d.q_eff.iloc[0]==20.
    assert d.F.iloc[0]==.6
    assert d.tau.iloc[0]==1800.


def test_delayed_benchmark_message_cannot_rewind_available_price():
    from dh.research.offline_alpha_models import canonical_benchmark
    b=pd.DataFrame({'t':[10,20,30,40,50],'exchange_t':[10,20,15,20,20],'value':[100.,101.,999.,102.,999.],'feed':['5hz','5hz','1hz','1hz','5hz']})
    c=canonical_benchmark(b)
    np.testing.assert_array_equal(c.value,[100.,101.,101.,102.,102.])
    np.testing.assert_array_equal(c.source_latest,[10,20,20,20,20])


def test_external_delay_keeps_current_benchmark_in_price_gaps():
    from dh.research.offline_alpha_diagnostics import delayed_features
    p=pd.DataFrame({'brti':[100.,102.,104.],'brti_age':[0.,0.,0.], 'coinbase_gap':[1.,1.,1.], 'coinbase_micro_gap':[.1,.2,.3], 'coinbase_age':[.01,.02,.03]})
    x=delayed_features(p,['brti_age','coinbase_gap','coinbase_micro_gap','coinbase_age'],200)
    # Old venue price 101 minus CURRENT benchmark 102 = -1; old gap +1 is invalid.
    assert x.coinbase_gap.iloc[1]==-1.
    assert x.coinbase_micro_gap.iloc[1]==.1
    assert abs(x.coinbase_age.iloc[1]-.21)<1e-12
    assert x.brti_age.iloc[1]==0.
