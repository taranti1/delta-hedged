"""Offline causal feature cache and quote-level alpha experiments. No live endpoints."""
from __future__ import annotations
import os
for k in ('OMP_NUM_THREADS','OPENBLAS_NUM_THREADS','MKL_NUM_THREADS','VECLIB_MAXIMUM_THREADS','NUMEXPR_NUM_THREADS'):
    os.environ[k]='1'
import argparse,hashlib,io,json,math,time
from collections import deque,Counter
from pathlib import Path
import numpy as np
import pandas as pd
import orjson,zstandard
from dh.feeds.registry import normalizer_for
from dh.core.book import ExtBook
from dh.core.events import ExtBookSnapshot,ExtBookDelta,ExtBBO,ExtTrade,FeedStatus,IndexTick
from dh.kalshi.normalize import ws_message_to_events
NS=10**9;STEP=200_000_000
START=pd.Timestamp('2026-09-25T22:00Z').value
END=pd.Timestamp('2026-09-28T10:00Z').value
TRAIN_END=pd.Timestamp('2026-09-27T00:00Z').value
VAL_END=pd.Timestamp('2026-09-27T12:00Z').value
STREAMS=('coinbase.ws','kraken.ws','bitstamp.ws','gemini.ws','cryptocom.ws','deribit.ws','okx.ws','hyperliquid.ws')

def utc(t):return pd.Timestamp(t,tz='UTC').isoformat()
def sha(p):
    h=hashlib.sha256()
    with p.open('rb') as f:
        while b:=f.read(1<<20):h.update(b)
    return h.hexdigest()
def files(root,stream):
    from dh.store.replay import segment_files,read_index
    out=[]
    for _,_,p in segment_files(root,stream,START-3600*NS,END):
        ix=read_index(p)
        if ix is None or not ix.get('closed_ns'):continue
        if p.stat().st_size!=ix['file_bytes']:raise ValueError(f'Closed segment changed: {p}')
        out.append((p,ix))
    return out

def lines(p):
    with p.open('rb') as f:
        if p.suffix=='.zst':
            with zstandard.ZstdDecompressor().stream_reader(f) as zz:
                yield from io.BufferedReader(zz)
        else:yield from f

def cache_venue(root,out,stream):
    name=stream.split('.')[0];dest=out/f'{name}.parquet'
    if dest.exists():return
    norm,state=normalizer_for(stream);book=None;bbo=None;tradeq=deque();cum=0.;total=0.;rows=[]
    g=START;counts=Counter();inputs=[];max_t=0
    def emit(t):
        while tradeq and tradeq[0][0]<t-5*NS:tradeq.popleft()
        candidates=[]
        if book is not None and book.valid and book.top() and 0<=t-book.ts<=2*NS and not book.crossed():
            v=book.top();candidates.append((book.ts,v.bid,v.ask,v.bid_size,v.ask_size))
        if bbo is not None and 0<=t-bbo.ts<=2*NS and 0<bbo.bid<bbo.ask:
            candidates.append((bbo.ts,bbo.bid,bbo.ask,bbo.bid_size,bbo.ask_size))
        if candidates:
            bt,bid,ask,bs,az=max(candidates);mid=(bid+ask)/2;den=bs+az
            imb=(bs-az)/den if den>0 else 0.;micro=(ask*bs+bid*az)/den if den>0 else mid
            rows.append((t,mid,micro,imb,ask-bid,(t-bt)/NS,sum(sz for ts,sz in tradeq if ts>=t-NS),sum(sz for _,sz in tradeq),cum,total,bt))
        else:rows.append((t,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,np.nan,cum,total,0))
    for p,ix in files(root,stream):
        before=p.stat();inputs.append(dict(path=str(p),bytes=before.st_size,sha256=sha(p)))
        for line in lines(p):
            r=orjson.loads(line);t=int(r['t'])
            if t>=END:break
            if t<max_t:
                counts['backward_receive_steps']+=1
                counts['max_backward_ns']=max(counts['max_backward_ns'],max_t-t)
                t=max_t  # conservative availability: preserve arrival order, never move a later frame earlier
            max_t=t
            while g<=t and g<END:emit(g);g+=STEP
            raw=r['d'].encode()
            for ev in norm(raw,t,state):
                counts[type(ev).__name__]+=1
                if isinstance(ev,ExtBookSnapshot):
                    if book is None:book=ExtBook(ev.venue,ev.symbol)
                    if book.symbol==ev.symbol:book.apply_snapshot(ev)
                elif isinstance(ev,ExtBookDelta):
                    if book is not None and book.valid and book.symbol==ev.symbol:book.apply(ev)
                elif isinstance(ev,ExtBBO):bbo=ev
                elif isinstance(ev,ExtTrade) and ev.aggressor in ('buy','sell'):
                    signed=ev.size*(1 if ev.aggressor=='buy' else -1);tradeq.append((t,signed));cum+=signed;total+=ev.size
                elif isinstance(ev,FeedStatus) and ev.status in ('gap','disconnected','connected','stale'):
                    if book is not None:book.valid=False
                    bbo=None;tradeq.clear()
        if p.stat().st_size!=before.st_size:raise ValueError(f'Input mutated {p}')
        print(name,p.name,utc(ix['last_t']),flush=True)
    while g<END:emit(g);g+=STEP
    pd.DataFrame(rows,columns=['t','mid','micro','imb','spread','age','flow1','flow5','cumflow','cumvolume','book_t']).to_parquet(dest,index=False)
    (out/f'{name}_meta.json').write_text(json.dumps(dict(inputs=inputs,counts=counts,normalizer_stats=state.stats),indent=2))

def cache_brti(root,out):
    dest=out/'brti.parquet'
    if dest.exists():return
    rows=[];inputs=[]
    for p,ix in files(root,'kalshi.ws'):
        before=p.stat();inputs.append(dict(path=str(p),bytes=before.st_size,sha256=sha(p)))
        for line in lines(p):
            if b'cfbenchmarks_value' not in line:continue
            r=orjson.loads(line);t=int(r['t'])
            if t>=END:break
            msg=orjson.loads(r['d'])
            for ev in ws_message_to_events(msg,t):
                if isinstance(ev,IndexTick) and ev.index_id=='BRTI':rows.append((t,ev.ts_exch,ev.value,ev.feed))
        if p.stat().st_size!=before.st_size:raise ValueError(f'Input mutated {p}')
        print('brti',p.name,len(rows),flush=True)
    b=pd.DataFrame(rows,columns=['t','exchange_t','value','feed']).sort_values('t',kind='stable').drop_duplicates(['t','exchange_t','feed'],keep='last')
    b.to_parquet(dest,index=False);(out/'brti_meta.json').write_text(json.dumps(inputs,indent=2))

def cache_quotes(root,out):
    dest=out/'orders.parquet'
    if dest.exists():return
    orders=[];fills=[];settles={};inputs=[];fv={};count=Counter();session_bounds=[]
    logs=sorted((root/'paper_logs').glob('paper-*'))
    logs=[p for p in logs if p.name.endswith(('.jsonl','.jsonl.zst')) and p.name<'paper-20260928']
    for p in logs:
        before=p.stat();inputs.append(dict(path=str(p),bytes=before.st_size,sha256=sha(p)))
        ses=p.name.split('.jsonl')[0];o={};quotes={};seen=set();first=last=0;ended=False;sf=[]
        for line in lines(p):
            # Fair-value records dominate logs; parse them only because early logs omit causal quote fields.
            if not any(k in line for k in (b'log.fv',b'log.quote',b'log.fill',b'log.settle',b'"action"',b'session_start',b'session_end')):continue
            r=orjson.loads(line);k=r.get('k');t=int(r.get('t',0))
            if t>=END:break
            if not first:first=t
            last=max(last,t);count[k]+=1
            if k=='session_end':ended=True
            if k=='log.fv':fv[r['ticker']]=r
            elif k=='log.quote':
                if r.get('coid') and not r.get('existing_id'):quotes[r['coid']]=r
            elif k=='action' and r.get('type')=='PlaceOrder':
                coid=r['client_order_id'];q=quotes.get(coid,{});v=fv.get(r['ticker'],{})
                if int(v.get('t',0))>t:v={}
                if int(q.get('t',0))>t:q={}
                row=dict(session=ses,coid=coid,t=t,ticker=r['ticker'],side=1 if r['book_side']=='bid' else -1,px=r['px']/1e4,requested_ct=r['qty']/100,
                    F=q.get('F',v.get('F',np.nan)),delta=q.get('delta',v.get('delta',np.nan)),z=q.get('z_near',abs(v.get('z',np.nan))),
                    fv_t=q.get('fv_ts',v.get('t',0)),edge=q.get('edge',np.nan),value=q.get('value',np.nan),q_eff=q.get('q_eff',np.nan),
                    position=q.get('position','unknown'),intensity=q.get('intensity',np.nan),score=q.get('score',np.nan),strategy_digest=r.get('cfg',''),
                    cancel_t=0,expiry_order=int(r.get('expiration_ts',0)),filled_ct=0.,fee=0.,fill_cash=0.,fill_n=0,first_fill_t=0)
                o[coid]=row
            elif k=='action' and r.get('type')=='CancelOrder':
                if r['client_order_id'] in o:o[r['client_order_id']]['cancel_t']=t
            elif k=='log.fill':
                key=r.get('trade_id') or (r.get('coid'),t,r.get('qty'),r.get('px'))
                if key in seen:continue
                seen.add(key);coid=r.get('coid');qty=r['qty']/100;fee=r.get('fee',0)/1e6
                # Fee logs carry microdollars, matching Ledger.
                if coid in o:
                    a=o[coid];a['filled_ct']+=qty;a['fee']+=fee;a['fill_cash']+=(-1 if r['side']=='bid' else 1)*qty*r['px']/1e4;a['fill_n']+=1
                    if not a['first_fill_t']:a['first_fill_t']=int(r.get('ts_exch') or t)
                sf.append(dict(session=ses,coid=coid,t=t,match_t=int(r.get('ts_exch') or t),ticker=r['ticker'],side=1 if r['side']=='bid' else -1,px=r['px']/1e4,contracts=qty,fee=fee,F=r.get('F',np.nan)))
            elif k=='log.settle':
                tk=r['ticker'];value=r['px']/1e4
                if tk in settles and settles[tk]!=value:raise ValueError(f'Conflicting outcome {tk}')
                settles[tk]=value
        for a in o.values():a['session_last_t']=last;a['session_ended']=ended
        orders.extend(o.values());fills.extend(sf);session_bounds.append(dict(session=ses,first=first,last=last,ended=ended,orders=len(o),fills=len(sf)))
        if p.stat().st_size!=before.st_size:raise ValueError(f'Historical log mutated {p}')
        print('paper',ses,len(o),len(sf),flush=True)
    d=pd.DataFrame(orders)
    # Threshold tickers encode New York local expiration (September daylight saving).
    z=d.ticker.str.split('-').str[1]
    d['expiration']=pd.to_datetime(z,format='%y%b%d%H').dt.tz_localize('America/New_York').dt.tz_convert('UTC').dt.as_unit('ns').astype('int64')
    d['tau']=(d.expiration-d.t)/NS;d['strike']=d.ticker.str.split('-T').str[1].astype(float)
    d['settle']=d.ticker.map(settles);d['net']=d.fill_cash+d.side*d.filled_ct*d.settle-d.fee
    d.loc[d.filled_ct==0,'net']=0.
    # Conservative censoring: order has had its natural 120s life or observed session completion.
    d['complete']=(d.t+130*NS<d.session_last_t)|d.session_ended
    d['filled']=d.filled_ct>0;d['net_c']=np.where(d.filled,100*d.net/d.filled_ct,np.nan)
    d.to_parquet(dest,index=False);pd.DataFrame(fills).to_parquet(out/'paper_fills.parquet',index=False)
    (out/'paper_meta.json').write_text(json.dumps(dict(inputs=inputs,counts=count,sessions=session_bounds,settlements=settles),indent=2))

def main():
    p=argparse.ArgumentParser();p.add_argument('stage',choices=['cache','quotes','brti','venues']);p.add_argument('--root',type=Path,default=Path('data'));p.add_argument('--out',type=Path,default=Path('data/results/alpha_20260929/cache'));a=p.parse_args();a.out.mkdir(parents=True,exist_ok=True)
    if a.stage in ('cache','quotes'):cache_quotes(a.root,a.out)
    if a.stage in ('cache','brti'):cache_brti(a.root,a.out)
    if a.stage in ('cache','venues'):
        for stream in STREAMS:cache_venue(a.root,a.out,stream)
if __name__=='__main__':main()
