"""Timing, selection and concentration diagnostics; never changes the selected policy."""
from pathlib import Path
import json,pickle
import numpy as np
import pandas as pd
from dh.research.offline_alpha import NS,STEP,TRAIN_END,VAL_END
from dh.research.offline_alpha_models import estimator,predict_batched,families,canonical_benchmark,asof_values,economics


def delayed_features(panel,features,delay_ms):
    """Delay external observations; recompute their gaps against the CURRENT benchmark."""
    own=families(panel)['own_history'];external=[c for c in features if c not in own]
    X=panel[features].astype(float).copy();steps=round(delay_ms*1e6/STEP)
    shifted=panel[external].shift(steps).astype(float)
    # Gaps already contain the benchmark; shifting the old gap alone is incorrect.
    correction=panel.brti.shift(steps)-panel.brti
    for c in external:
        if c=='spot_gap' or (c.endswith('_gap') and not c.endswith('_micro_gap')):shifted[c]+=correction
        if c.endswith('_age'):shifted[c]+=delay_ms/1000
    X[external]=shifted
    return X


def run(out=Path('data/results/alpha_20260929')):
    panel=pd.read_parquet(out/'panel.parquet');preds=pd.read_parquet(out/'nowcast_predictions.parquet')
    models=pickle.load((out/'nowcast_models.pkl').open('rb'));selected=json.loads((out/'nowcast_selection.json').read_text())['selected']
    common=preds.eligible.astype(bool);metrics=[];fixed=[]
    for h in (.5,1):
        key=f'{selected}_{h:g}';m,feat=models[key];target=f'y_{h:g}';valid=common&panel[target].notna()
        tr=valid&(panel.t<TRAIN_END-6*NS)&(panel.t%NS==0);te=valid&(panel.split=='test')
        for delay in (0,200,1000):
            X=delayed_features(panel,feat,delay);y=panel.loc[te,target].to_numpy()
            p=predict_batched(m,X.loc[te]);rmse=np.sqrt(np.mean((y-p)**2));gain=1-rmse/np.sqrt(np.mean(y*y))
            if h==.5:fixed.append(dict(delay_ms=delay,n=len(y),rmse=rmse,gain_vs_last=gain,method='frozen model; delayed price gaps recomputed against current benchmark'))
            refit=estimator();refit.fit(X.loc[tr],panel.loc[tr,target]);pp=predict_batched(refit,X.loc[te])
            metrics.append(dict(horizon_s=h,delay_ms=delay,n=len(y),frozen_rmse=rmse,frozen_gain=gain,refit_rmse=np.sqrt(np.mean((y-pp)**2)),refit_gain=1-np.sqrt(np.mean((y-pp)**2)/np.mean(y*y))))
    pd.DataFrame(fixed).to_csv(out/'nowcast_delay_stress.csv',index=False)
    pd.DataFrame(metrics).to_csv(out/'delay_aware_refit.csv',index=False)
    # How much predictive information remains AFTER a potential order-response delay?
    b=canonical_benchmark(pd.read_parquet(out/'cache/brti.parquet').sort_values('t',kind='stable').drop_duplicates('t',keep='last'))
    bt=b.t.to_numpy();bv=b.value.to_numpy();src=b.source_latest.to_numpy();t=panel.t.to_numpy();rows=[]
    for h in (.5,1):
        feat=models[f'{selected}_{h:g}'][1]
        for latency in (0,50,100,200):
            arrival=t+latency*1000000;j=np.maximum(np.searchsorted(bt,arrival,side='left')-1,0)
            arrived=asof_values(arrival,bt,bv,500000000)
            arrived=np.where(arrival-src[j]<=500000000,arrived,np.nan)
            # Raw double precision current level avoids quantizing small returns at $84k.
            current=asof_values(t,bt,bv,500000000)
            y=panel[f'y_{h:g}'].to_numpy(dtype=float)-(arrived-current)
            valid=common&np.isfinite(y);tr=valid&(t<TRAIN_END-6*NS)&(t%NS==0);te=valid&(panel.split=='test')
            mod=estimator();mod.fit(panel.loc[tr,feat],y[tr]);p=predict_batched(mod,panel.loc[te,feat]);z=y[te]
            rows.append(dict(horizon_s=h,action_delay_ms=latency,n=len(z),baseline_rmse=np.sqrt(np.mean(z*z)),model_rmse=np.sqrt(np.mean((z-p)**2)),gain_vs_zero=1-np.sqrt(np.mean((z-p)**2)/np.mean(z*z))))
    pd.DataFrame(rows).to_csv(out/'post_latency_predictability.csv',index=False)
    q=pd.read_parquet(out/'quote_predictions.parquet');curve=[]
    for name in ('price_only','quote_state','quote_and_external'):
        tr=q.train_used;va=q.validation_used;score=q[f'{name}_score']
        for quantile in (0,.05,.10,.15,.20,.30,.50):
            threshold=-np.inf if quantile==0 else np.quantile(score[tr],quantile)
            z=economics(q[va],score[va]>=threshold);z.update(model=name,training_quantile=quantile,threshold=threshold,eligible=z['contract_retention']>=.8)
            curve.append(z)
    pd.DataFrame(curve).to_csv(out/'validation_thresholds.csv',index=False)
    # Event concentration is descriptive; does not revise selection.
    q[q.test_used].groupby('expiration').agg(net=('net','sum'),contracts=('filled_ct','sum'),orders=('t','size'),filled_orders=('filled','sum')).to_csv(out/'test_event_concentration.csv')
    print('TIMING\n',pd.DataFrame(metrics).to_string(index=False),flush=True)
    print('POST LATENCY\n',pd.DataFrame(rows).to_string(index=False),flush=True)
    print('FILTER VALIDATION\n',pd.DataFrame(curve)[['model','training_quantile','contract_retention','delta_net','eligible']].to_string(index=False),flush=True)
if __name__=='__main__':run()
