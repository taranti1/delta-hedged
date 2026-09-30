"""Train/validation/test analysis of immutable offline_alpha caches."""
from __future__ import annotations
import os
for k in ('OMP_NUM_THREADS','OPENBLAS_NUM_THREADS','MKL_NUM_THREADS','VECLIB_MAXIMUM_THREADS','NUMEXPR_NUM_THREADS'):os.environ[k]='1'
import argparse,json,math,pickle,warnings
from pathlib import Path
import numpy as np
import pandas as pd
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import Ridge,LogisticRegression
from sklearn.pipeline import make_pipeline
from sklearn.metrics import mean_squared_error,mean_absolute_error,log_loss,brier_score_loss,roc_auc_score
from dh.research.offline_alpha import START,END,TRAIN_END,VAL_END,STEP,NS,STREAMS,lines
from dh.research.exp_common import cluster_mean_ci
SPOTS=('coinbase','kraken','bitstamp','gemini','cryptocom');PERPS=('deribit','okx','hyperliquid')

def estimator(kind='ridge'):
    return make_pipeline(SimpleImputer(strategy='median',add_indicator=True,keep_empty_features=True),StandardScaler(),
                         Ridge(alpha=100.) if kind=='ridge' else LogisticRegression(C=.1,max_iter=2000,random_state=17))

def predict_batched(model,X,proba=False,batch=20000):
    """Bound temporary matrices so fitting does not crowd out the live collector."""
    chunks=[]
    for i in range(0,len(X),batch):
        a=X.iloc[i:i+batch]
        chunks.append(model.predict_proba(a)[:,1] if proba else model.predict(a))
    return np.concatenate(chunks) if chunks else np.array([])


def asof_values(t,source_t,values,max_age_ns=None):
    """Last strictly earlier received value; never backfill an unavailable past."""
    j=np.searchsorted(source_t,t,side='left')-1
    good=j>=0;out=np.full(len(t),np.nan);jj=np.maximum(j,0)
    if max_age_ns is not None:good&=(t-source_t[jj]<=max_age_ns)
    out[good]=values[jj[good]]
    return out

def canonical_benchmark(b):
    """As-received state with production source-time priority; delayed old ticks cannot rewind price."""
    last_src=-1;value=np.nan;one_hz=False;values=[];sources=[]
    for r in b.itertuples():
        src=int(r.exchange_t);preferred=r.feed=='1hz'
        if src>last_src or (src==last_src and preferred and not one_hz):
            last_src=src;value=float(r.value);one_hz=preferred
        values.append(value);sources.append(last_src)
    b=b.copy();b['value']=values;b['source_latest']=sources
    return b


def build_panel(cache,out):
    dest=out/'panel.parquet'
    if dest.exists() and (out/'panel_verified.json').exists():return pd.read_parquet(dest)
    t=np.arange(START,END,STEP,dtype=np.int64);b=pd.read_parquet(cache/'brti.parquet').sort_values('t',kind='stable').drop_duplicates('t',keep='last')
    b=canonical_benchmark(b)
    bt=b.t.to_numpy();bv=b.value.to_numpy();bst=b.source_latest.to_numpy();j=np.searchsorted(bt,t,side='left')-1;ix=np.maximum(j,0)
    panel=pd.DataFrame({'t':t,'brti':np.where(j>=0,bv[ix],np.nan),'brti_age':np.where(j>=0,np.maximum((t-bt[ix])/NS,(t-bst[ix])/NS),np.nan)})
    for h in (.2,1,5):panel[f'brti_ret_{h:g}']=panel.brti-asof_values(t-int(h*NS),bt,bv,3*NS)
    for h in (.2,.5,1):
        future_t=t+int(h*NS);future_i=np.maximum(np.searchsorted(bt,future_t,side='left')-1,0)
        y=asof_values(future_t,bt,bv,.5*NS)-panel.brti
        y=np.where(future_t-bst[future_i]<=.5*NS,y,np.nan)
        panel[f'y_{h:g}']=np.where((panel.brti_age<=.5)&(t+int(h*NS)<END),y,np.nan)
    coverage=[]
    for stream in STREAMS:
        v=stream.split('.')[0];f=pd.read_parquet(cache/f'{v}.parquet');assert np.array_equal(f.t,t)
        for col in ('mid','micro','imb','spread','age','flow1','flow5'):
            panel[f'{v}_{col}']=f[col].to_numpy()
        panel[f'{v}_gap']=f.mid-panel.brti;panel[f'{v}_micro_gap']=f.micro-f.mid
        for h in (.2,1,5):panel[f'{v}_ret_{h:g}']=f.mid-f.mid.shift(round(h*NS/STEP))
        coverage.append(dict(venue=v,fresh_fraction=f.mid.notna().mean(),rows=len(f),max_valid_book_age_s=f.age.max()))
    with warnings.catch_warnings():
        warnings.simplefilter('ignore',RuntimeWarning)
        panel['spot_mid']=np.nanmedian(panel[[f'{v}_mid' for v in SPOTS]],axis=1)
        panel['spot_gap']=panel.spot_mid-panel.brti
        panel['spot_dispersion']=np.nanmax(panel[[f'{v}_mid' for v in SPOTS]],axis=1)-np.nanmin(panel[[f'{v}_mid' for v in SPOTS]],axis=1)
    panel['n_spot']=panel[[f'{v}_mid' for v in SPOTS]].notna().sum(axis=1)
    for v in PERPS:panel[f'{v}_basis']=panel[f'{v}_mid']-panel.spot_mid
    panel['split']=np.select([t<TRAIN_END,t<VAL_END],['train','validation'],default='test')
    panel['hour']=t//(3600*NS)
    
    for col in panel.select_dtypes(include='float').columns:panel[col]=panel[col].astype('float32')
    panel.to_parquet(dest,index=False);pd.DataFrame(coverage).to_csv(out/'venue_coverage.csv',index=False)
    (out/'panel_verified.json').write_text(json.dumps(dict(version='source_priority_v1',rows=len(panel),description='Receive-causal, latest source timestamp with 1Hz tie priority; receive and source freshness enforced')))
    return panel

def families(panel):
    own=['brti_age','brti_ret_0.2','brti_ret_1','brti_ret_5']
    spot=[f'{v}_gap' for v in SPOTS]+['spot_gap','spot_dispersion','n_spot']
    micro=[f'{v}_{c}' for v in SPOTS for c in ('micro_gap','imb','spread','age','flow1','flow5','ret_0.2','ret_1','ret_5')]
    perp=[f'{v}_{c}' for v in PERPS for c in ('basis','micro_gap','imb','age','flow1','flow5','ret_0.2','ret_1','ret_5')]
    return dict(own_history=own,spot_prices=own+spot,spot_microstructure=own+spot+micro,spot_and_perpetuals=own+spot+micro+perp)

def error_gain_ci(y,p,base,groups):
    d=pd.DataFrame({'sse':(y-p)**2,'base':(y-base)**2,'g':groups}).groupby('g').sum()
    rng=np.random.default_rng(1701);draw=rng.integers(len(d),size=(2000,len(d)))
    vals=1-np.sqrt(d.sse.to_numpy()[draw].sum(axis=1)/d.base.to_numpy()[draw].sum(axis=1))
    return np.quantile(vals,[.025,.975]).tolist()

def nowcast(panel,out):
    fam=families(panel);metrics=[];preds=pd.DataFrame({'t':panel.t,'split':panel.split});models={}
    # Fair comparison on shared healthy spot/benchmark coverage, all candidate families.
    common=(panel.n_spot>=2)&(panel.brti_age<=.5)&(panel.t>=pd.Timestamp('2026-09-26',tz='UTC').value)
    for h in (.2,.5,1):
        target=f'y_{h:g}';valid=common&panel[target].notna()
        train=valid&(panel.t<TRAIN_END-6*NS)&(panel.t%(NS)==0)
        for name,feat in fam.items():
            m=estimator();m.fit(panel.loc[train,feat],panel.loc[train,target]);models[f'{name}_{h:g}']=(m,feat)
            preds[f'{name}_{h:g}']=predict_batched(m,panel[feat]);
        for split in ('validation','test'):
            sel=valid&(panel.split==split);y=panel.loc[sel,target].to_numpy();base=np.zeros(len(y));own=preds.loc[sel,f'own_history_{h:g}'].to_numpy()
            for name in ('last_print',*fam):
                p=base if name=='last_print' else preds.loc[sel,f'{name}_{h:g}'].to_numpy()
                ci=error_gain_ci(y,p,base,panel.loc[sel,'hour'].to_numpy())
                metrics.append(dict(split=split,horizon_s=h,model=name,n=len(y),hours=panel.loc[sel,'hour'].nunique(),rmse=math.sqrt(mean_squared_error(y,p)),mae=mean_absolute_error(y,p),rmse_gain_vs_last=1-math.sqrt(mean_squared_error(y,p)/mean_squared_error(y,base)),gain_lo=ci[0],gain_hi=ci[1],rmse_gain_vs_own=1-math.sqrt(mean_squared_error(y,p)/mean_squared_error(y,own))))
    met=pd.DataFrame(metrics);met.to_csv(out/'nowcast_metrics.csv',index=False)
    best=met[(met.split=='validation')&(met.horizon_s==.5)&(met.model!='last_print')].sort_values('rmse').iloc[0].model
    (out/'nowcast_selection.json').write_text(json.dumps(dict(selected=best,selection='validation 0.5s RMSE, no final-test refit'),indent=2))
    with (out/'nowcast_models.pkl').open('wb') as f:pickle.dump(models,f)
    preds['eligible']=common;preds.to_parquet(out/'nowcast_predictions.parquet',index=False)
    # Freeze model and shift only external inputs: received information must survive reaction time.
    timing=[];m,feat=models[f'{best}_0.5'];external=[c for c in feat if c not in fam['own_history']]
    for delay in (0,200,1000):
        from dh.research.offline_alpha_diagnostics import delayed_features
        X=delayed_features(panel,feat,delay)
        sel=common&panel.y_0_5.notna() if 'y_0_5' in panel else common&panel['y_0.5'].notna()
        sel&=panel.split=='test';p=predict_batched(m,X.loc[sel]);y=panel.loc[sel,'y_0.5'].to_numpy()
        timing.append(dict(delay_ms=delay,n=len(y),rmse=math.sqrt(mean_squared_error(y,p)),gain_vs_last=1-math.sqrt(mean_squared_error(y,p)/np.mean(y*y))))
    pd.DataFrame(timing).to_csv(out/'nowcast_delay_stress.csv',index=False)
    ablation=[]
    for venue in (*SPOTS,*PERPS):
        feat=fam['own_history']+[f'{venue}_{c}' for c in ('gap','micro_gap','imb','age','flow1','flow5','ret_0.2','ret_1','ret_5')]
        train=common&panel['y_0.5'].notna()&(panel.t<TRAIN_END-6*NS)&(panel.t%NS==0)
        model=estimator();model.fit(panel.loc[train,feat],panel.loc[train,'y_0.5'])
        for split in ('validation','test'):
            sel=common&panel['y_0.5'].notna()&(panel.split==split);y=panel.loc[sel,'y_0.5'].to_numpy();p=predict_batched(model,panel.loc[sel,feat])
            ablation.append(dict(venue=venue,split=split,n=len(y),rmse=np.sqrt(np.mean((y-p)**2)),gain_vs_last=1-np.sqrt(np.mean((y-p)**2)/np.mean(y*y))))
    pd.DataFrame(ablation).to_csv(out/'nowcast_single_venue.csv',index=False)
    print('NOWCAST',best,met[(met.split=='test')&(met.model.isin(['last_print','own_history',best]))].to_string(index=False),flush=True)
    return models,preds

def attach_features(orders,panel):
    d=orders.sort_values('t').copy();cols=[c for c in panel if c not in ('split','hour') and not c.startswith('y_')]
    # Grid emitted before processing messages at its timestamp; exact match remains causal.
    d=pd.merge_asof(d,panel[cols].rename(columns={'t':'feature_t'}),left_on='t',right_on='feature_t',direction='backward',tolerance=STEP)
    d['price_paid']=np.where(d.side==1,d.px,1-d.px);d['log_tau']=np.log1p(d.tau.clip(lower=0));d['edge_c']=100*d.side*(d.F-d.px);d['quoted_edge_c']=100*d.edge
    d['log_queue']=np.log1p(d.q_eff.clip(lower=0));d['log_intensity']=np.log1p(d.intensity.clip(lower=0));d['abs_z']=d.z.abs();d['abs_delta']=d.delta.abs()
    d['fv_age']=(d.t-d.fv_t)/NS
    for pos in ('touch','improve','behind'):d[f'quote_{pos}']=(d.position==pos).astype(float)
    # Direction adjustment uses quote's sign; no fill- or settlement-time inputs.
    for c in families(panel)['spot_and_perpetuals']:
        if c.endswith(('gap','ret_0.2','ret_1','ret_5','flow1','flow5','imb','basis')):d[f'signed_{c}']=d.side*d[c]
        else:d[f'signed_{c}']=d[c]
    return d

def repair_quote_cache(cache):
    """Quote logs follow actions; join only an identical decision timestamp and quote key.

    Earlier logs have no coid in log.quote, so match the exact session/time/ticker/side/price.
    Never attach a later quote update to the initial decision.
    """
    stamp=cache/'quote_join_verified.json'
    if stamp.exists():return
    d=pd.read_parquet(cache/'orders.parquet');meta=json.loads((cache/'paper_meta.json').read_text())
    keys={(r.session,int(r.t),r.ticker,int(r.side),round(r.px*1e4)):i for i,r in enumerate(d.itertuples())}
    assert len(keys)==len(d),'Ambiguous same-decision order key'
    times={};matched=set();counts={}
    for inp in meta['inputs']:
        p=Path(inp['path']);ses=p.name.split('.jsonl')[0]
        for line in lines(p):
            if b'log.quote' not in line and b'log.settle' not in line:continue
            r=json.loads(line);t=int(r['t'])
            if t>=END:break
            if r['k']=='log.settle':
                tk=r['ticker'];times[tk]=min(times.get(tk,t),t);continue
            if r.get('existing_id'):continue
            key=(ses,t,r['ticker'],1 if r['side']=='bid' else -1,int(r['px']))
            if key not in keys:continue
            i=keys[key]
            if r.get('coid') and r['coid']!=d.at[i,'coid']:raise ValueError('Quote/order coid mismatch')
            if int(r.get('ts_decision',t))!=t or int(r.get('fv_ts',t))>t:raise ValueError('Noncausal logged quote')
            for c in ('edge','value','q_eff','position','intensity','score','F','delta'):
                if c in r:d.at[i,c]=r[c]
            for src,dst in (('fv_ts','fv_t'),('z_near','z')):
                if src in r:d.at[i,dst]=r[src]
            matched.add(i)
    z=d.ticker.str.split('-').str[1]
    d['expiration']=pd.to_datetime(z,format='%y%b%d%H').dt.tz_localize('America/New_York').dt.tz_convert('UTC').dt.as_unit('ns').astype('int64')
    d['tau']=(d.expiration-d.t)/NS
    d.to_parquet(cache/'orders.parquet',index=False)
    (cache/'settlement_known_times.json').write_text(json.dumps(times))
    stamp.write_text(json.dumps(dict(orders=len(d),matched=len(matched),min_tau=float(d.tau.min()),max_tau=float(d.tau.max()),method='Exact decision timestamp/session/ticker/side/price; coid checked when available'),indent=2))


def known_settlements(cache):
    dest=cache/'settlement_known_times.json'
    if dest.exists():return json.loads(dest.read_text())
    meta=json.loads((cache/'paper_meta.json').read_text());times={}
    for inp in meta['inputs']:
        for line in lines(Path(inp['path'])):
            if b'log.settle' not in line:continue
            r=json.loads(line);t=int(r['t'])
            if t>=END:break
            tk=r['ticker'];times[tk]=min(times.get(tk,t),t)
    dest.write_text(json.dumps(times));return times

def economics(d,keep):
    b=d.net.sum();k=d.loc[keep];ct=k.filled_ct.sum();base_ct=d.filled_ct.sum()
    return dict(orders=len(d),kept_orders=int(keep.sum()),filled_orders=int(d.filled.sum()),kept_filled_orders=int(k.filled.sum()),contracts=base_ct,kept_contracts=ct,contract_retention=ct/base_ct if base_ct else 1.,base_net=b,kept_net=k.net.sum(),delta_net=k.net.sum()-b,base_c=100*b/base_ct if base_ct else np.nan,kept_c=100*k.net.sum()/ct if ct else np.nan)

def fill_models(cache,panel,out):
    d=attach_features(pd.read_parquet(cache/'orders.parquet'),panel)
    d['label_known_t']=d.ticker.map(known_settlements(cache));d['split']=np.select([d.t<TRAIN_END,d.t<VAL_END],['train','validation'],default='test')
    d['eligible']=d.complete & d.net.notna() & (d.tau>=90) & (d.tau<=3900) & d.px.between(.03,.97) & (d.expiration<END) & (d.fv_t<=d.t) & (d.fv_age<=3) & d.F.notna()
    # A quote not filled has a zero outcome after its finite life; filled quotes need a known settlement.
    d['label_known_t']=np.where(d.filled,d.label_known_t,d.t+130*NS)
    train=d.eligible&(d.t<TRAIN_END)&(d.label_known_t<TRAIN_END)
    val=d.eligible&(d.t>=TRAIN_END)&(d.t<VAL_END)&(d.label_known_t<VAL_END)
    test=d.eligible&(d.t>=VAL_END)&(d.label_known_t<END)
    price=['side','price_paid','F','edge_c','quoted_edge_c','log_tau','abs_z','abs_delta']
    quote=price+['log_queue','value','log_intensity','score','fv_age','quote_touch','quote_improve','quote_behind']
    external=[f'signed_{c}' for c in families(panel)['spot_and_perpetuals']]
    fam={'price_only':price,'quote_state':quote,'quote_and_external':quote+external}
    metrics=[];econ=[];models={};selected={};pred=d.copy()
    for name,feat in fam.items():
        clf=estimator('logistic');clf.fit(d.loc[train,feat],d.loc[train,'filled'].astype(int))
        reg=estimator();trf=train&d.filled
        reg.fit(d.loc[trf,feat],d.loc[trf,'net_c'],ridge__sample_weight=d.loc[trf,'filled_ct']/d.loc[trf,'filled_ct'].mean())
        p=predict_batched(clf,d[feat],proba=True);mu=predict_batched(reg,d[feat])
        fee_c=100*d.loc[trf,'fee'].sum()/d.loc[trf,'filled_ct'].sum()
        mu=np.clip(mu,-100*d.price_paid-fee_c,100*(1-d.price_paid)-fee_c)
        mean_frac=(d.loc[trf,'filled_ct']/d.loc[trf,'requested_ct']).clip(upper=1).mean()
        score=p*mu/100*d.requested_ct*mean_frac;pred[f'{name}_fill_prob']=p;pred[f'{name}_net_c']=mu;pred[f'{name}_score']=score
        # Fixed 6 threshold candidates set on training scores; validation picks dollar profit subject to retention.
        thresholds=[-np.inf,*np.quantile(score[train],[.05,.10,.15,.20,.30,.50])]
        choices=[]
        for threshold in thresholds:
            res=economics(d[val],score[val]>=threshold)
            if res['contract_retention']>=.8:choices.append((res['kept_net'],threshold,res))
        _,threshold,vr=max(choices,key=lambda x:(x[0],-x[1]))
        selected[name]=dict(threshold=float(threshold) if np.isfinite(threshold) else None,policy='score_at_least' if np.isfinite(threshold) else 'keep_all',validation=vr,features=feat,train_orders=int(train.sum()),train_fills=int(trf.sum()),training_fee_c=float(fee_c))
        models[name]=(clf,reg,feat,mean_frac,threshold,fee_c)
        for split,mask in (('validation',val),('test',test)):
            y=d.loc[mask,'filled'].astype(int);pv=p[mask];base=np.repeat(d.loc[train,'filled'].mean(),sum(mask))
            fm=mask&d.filled;yr=d.loc[fm,'net_c'].to_numpy();pr=mu[fm];base_mu=np.average(d.loc[trf,'net_c'],weights=d.loc[trf,'filled_ct'])
            metrics.append(dict(model=name,split=split,n=int(mask.sum()),fills=int(fm.sum()),brier=brier_score_loss(y,pv),base_brier=brier_score_loss(y,base),log_loss=log_loss(y,pv,labels=[0,1]),base_log_loss=log_loss(y,base,labels=[0,1]),auc=roc_auc_score(y,pv) if y.nunique()>1 else np.nan,conditional_rmse=np.sqrt(np.mean((yr-pr)**2)),base_conditional_rmse=np.sqrt(np.mean((yr-base_mu)**2))))
            keep=score[mask]>=threshold;z=economics(d[mask],keep)
            x=d[mask].copy();x['delta']=np.where(keep,0,-x.net);ev=x.groupby('expiration').delta.sum()
            ci=cluster_mean_ci(ev.to_numpy(),None,ev.index.to_numpy(),n_boot=5000)
            z.update(model=name,split=split,threshold=threshold,event_count=len(ev),delta_lo=ci.lo*len(ev),delta_hi=ci.hi*len(ev),gate_pass=bool(z['delta_net']>0 and z['contract_retention']>=.8 and ci.lo>0))
            econ.append(z)
            if split=='test':
                x['keep']=keep;x.groupby('expiration').agg(base_net=('net','sum'),delta_net=('delta','sum'),contracts=('filled_ct','sum')).to_csv(out/f'event_effects_{name}.csv')
                pred.loc[mask,f'{name}_keep']=keep
    sensitivity=[]
    for name in fam:
        keep=pred.loc[test,f'{name}_keep'].astype(bool)
        for cents in (0,.25,1):
            x=d[test].copy();x['net']=x.net-cents*x.filled_ct/100
            res=economics(x,keep);res.update(model=name,extra_cost_c=cents);sensitivity.append(res)
    pd.DataFrame(sensitivity).to_csv(out/'filter_cost_sensitivity.csv',index=False)
    by_version=[]
    for name in fam:
        for digest,ix in d[test].groupby('strategy_digest').groups.items():
            keep=pred.loc[ix,f'{name}_keep'].astype(bool)
            res=economics(d.loc[ix],keep);res.update(model=name,strategy_digest=digest);by_version.append(res)
    pd.DataFrame(by_version).to_csv(out/'filter_by_strategy_version.csv',index=False)
    # Selection is validation-only. No final-test best-of-family headline.
    best=max(selected,key=lambda k:selected[k]['validation']['kept_net'])
    selected['selected_family']=best
    pd.DataFrame(metrics).to_csv(out/'fill_prediction_metrics.csv',index=False);pd.DataFrame(econ).to_csv(out/'filter_economics.csv',index=False)
    (out/'fill_selection.json').write_text(json.dumps(selected,indent=2));pred['train_used']=train;pred['validation_used']=val;pred['test_used']=test;pred.to_parquet(out/'quote_predictions.parquet',index=False)
    with (out/'fill_models.pkl').open('wb') as f:pickle.dump(models,f)
    quality=d.groupby('split').agg(orders=('t','size'),filled_orders=('filled','sum'),filled_contracts=('filled_ct','sum'),known_net=('net','sum'),eligible=('eligible','sum'),fresh_external=('n_spot',lambda x:(x>=2).mean()),first=('t','min'),last=('t','max'))
    quality.to_csv(out/'quote_coverage.csv')
    print('FILLS',best,pd.DataFrame(econ).to_string(index=False),flush=True)
    return models,pred

def main():
    a=argparse.ArgumentParser();a.add_argument('--out',type=Path,default=Path('data/results/alpha_20260929'));args=a.parse_args();out=args.out;cache=out/'cache'
    repair_quote_cache(cache);panel=build_panel(cache,out);nowcast(panel,out);fill_models(cache,panel,out)
if __name__=='__main__':main()
