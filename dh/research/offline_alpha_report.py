"""Write a self-contained report and static research figure from completed experiments."""
from pathlib import Path
import hashlib,json,subprocess,os
os.environ.setdefault("MPLCONFIGDIR", "/private/tmp/dh-alpha-matplotlib")
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

OUT=Path('data/results/alpha_20260929')

def money(x):return f'−${-x:.2f}' if x<0 else f'${x:.2f}'
def pct(x):return f'{100*x:.1f}%'
def main():
    p=OUT.resolve();m=pd.read_csv(p/'nowcast_metrics.csv');f=pd.read_csv(p/'fill_prediction_metrics.csv');e=pd.read_csv(p/'filter_economics.csv');delay=pd.read_csv(p/'post_latency_predictability.csv');refit=pd.read_csv(p/'delay_aware_refit.csv');venue=pd.read_csv(p/'nowcast_single_venue.csv');cv=pd.read_csv(p/'validation_thresholds.csv');conc=pd.read_csv(p/'test_event_concentration.csv');q=pd.read_parquet(p/'quote_predictions.parquet')
    chosen=json.loads((p/'nowcast_selection.json').read_text())['selected'];test=m[(m.split=='test')&(m.model==chosen)]
    labels={'price_only':'Prices / model edge','quote_state':'Prices + quote state','quote_and_external':'Quote state + external feeds'}
    plt.rcParams.update({'font.family':'DejaVu Sans','font.size':10,'axes.spines.top':False,'axes.spines.right':False,'axes.titleweight':'bold','figure.facecolor':'#f6f8fb','axes.facecolor':'white'})
    fig,ax=plt.subplots(2,2,figsize=(13,8.5));fig.subplots_adjust(top=.88,bottom=.15,left=.15,right=.97,hspace=.48,wspace=.36)
    colors=['#bcc7d5','#42a6a1','#2453a4'];names=['spot_prices','spot_microstructure','spot_and_perpetuals'];x=np.arange(3)
    for j,(name,c) in enumerate(zip(names,colors)):
        z=m[(m.split=='test')&(m.model==name)].sort_values('horizon_s');ax[0,0].bar(x+(j-1)*.24,z.rmse_gain_vs_last*100,.22,label=name.replace('_',' '),color=c)
    ax[0,0].set(xticks=x,xticklabels=['0.2 sec','0.5 sec','1 sec'],ylabel='Reduction in prediction error (%)',title='External order-book and flow data help');ax[0,0].legend(frameon=False,fontsize=8)
    z=delay[delay.horizon_s==.5];ax[0,1].plot(z.action_delay_ms,z.gain_vs_zero*100,'o-',color='#2453a4',lw=2)
    for r in z.itertuples():ax[0,1].annotate(pct(r.gain_vs_zero),(r.action_delay_ms,r.gain_vs_zero*100),xytext=(0,8),textcoords='offset points',ha='center',fontsize=9)
    ax[0,1].axhline(0,color='#8994a6',lw=1);ax[0,1].set(xlabel='Potential action delay (milliseconds)',ylabel='Remaining-move forecast gain (%)',title='The advantage fades after the decision',ylim=(-3,16),xticks=[0,50,100,200])
    z=f[f.split=='test'].set_index('model');order=list(labels);vals=[z.loc[k,'conditional_rmse'] for k in order]
    ax[1,0].barh(np.arange(3),vals,color=['#bcc7d5','#42a6a1','#2453a4']);base=z.base_conditional_rmse.iloc[0];ax[1,0].axvline(base,color='#cc6b4d',ls='--',label=f'Training-mean benchmark: {base:.2f}¢')
    ax[1,0].set(yticks=np.arange(3),yticklabels=['Price model','Quote-state model','External-feed model'],xlabel='Conditional profit prediction error (¢ / contract)',title='Predicting profit remains unsolved',xlim=(0,max(vals)*1.18));ax[1,0].invert_yaxis();ax[1,0].legend(frameon=True,facecolor='white',framealpha=.95,fontsize=8,loc='lower left')
    z=cv[cv.training_quantile==.05].set_index('model');vals=[z.loc[k,'delta_net'] for k in order]
    ax[1,1].bar(np.arange(3),vals,color=['#bcc7d5','#42a6a1','#2453a4']);ax[1,1].axhline(0,color='#8994a6',lw=1)
    for i,k in enumerate(order):ax[1,1].text(i,vals[i]-.7,f'{money(vals[i])}\n{pct(z.loc[k,"contract_retention"])} volume kept',ha='center',va='top',fontsize=9)
    ax[1,1].set(xticks=np.arange(3),xticklabels=['Price model','Quote state','External feeds'],ylabel='Change in validation profit ($)',title='Even the smallest tested filters hurt',ylim=(min(vals)-5,1))
    fig.suptitle('A short-lived forecasting signal — no validated profitable-fill filter',x=.08,ha='left',fontsize=18,fontweight='bold',color='#17243c')
    fig.text(.08,.918,'Recorded data only • chronological validation and test • no production changes',color='#53647c',fontsize=11)
    fig.text(.08,.025,'Forecast test: 18 observed hourly blocks. Profit tests: simulated paper orders; historical fills held fixed.\nTiming diagnostics do not model Kalshi queue priority or executable liquidation.',fontsize=9,color='#53647c')
    fig.savefig(p/'experiment_summary.png',dpi=180);fig.savefig(p/'experiment_summary.svg');plt.close(fig)
    ft=[]
    for h in (.2,.5,1):
        a=m[(m.split=='test')&(m.horizon_s==h)&(m.model=='last_print')].iloc[0];b=test[test.horizon_s==h].iloc[0]
        ft.append(f'| {h:g} s | ${a.rmse:.3f} | ${b.rmse:.3f} | {pct(b.rmse_gain_vs_last)} | {pct(b.gain_lo)} to {pct(b.gain_hi)} |')
    fills=[]
    for key,label in labels.items():
        z=f[(f.split=='test')&(f.model==key)].iloc[0]
        fills.append(f'| {label} | {z.auc:.3f} | {z.log_loss:.4f} | {z.conditional_rmse:.2f}¢ |')
    lat=[]
    for r in delay[delay.horizon_s==.5].itertuples():lat.append(f'| {r.action_delay_ms} ms | ${r.baseline_rmse:.3f} | ${r.model_rmse:.3f} | {pct(r.gain_vs_zero)} |')
    splitrows=[]
    for name,col in [('Training','train_used'),('Validation','validation_used'),('Final test','test_used')]:
        a=q[q[col]];splitrows.append(f'| {name} | {len(a):,} | {int(a.filled.sum())} | {int(a.fill_n.sum())} | {a.expiration.nunique()} |')
    grid=[]
    for key,label in labels.items():
        z=cv[(cv.model==key)&(cv.training_quantile==.05)].iloc[0]
        grid.append(f'| {label} | {pct(z.contract_retention)} | {money(z.delta_net)} |')
    supported=bool(e[(e.split=='test')].gate_pass.any())
    status=dict(tradable=False,production_changed=False,live_processes_touched=False,recording_started=False,
                forecast_model=chosen,forecast_signal_observed=True,profitable_fill_filter_supported=supported,
                responsive_replay_run=False,responsive_replay_reason='No candidate improved validation dollars while retaining >=80% of filled contracts; every family selected no filtering.',
                clean_test_caveat='Legacy/corrected paper implementations overlap the test; reported by strategy digest. Historic dataset is not an independently attested prospective holdout.',
                analysis_window_end_utc='2026-09-28T10:00:00Z')
    (p/'decision.json').write_text(json.dumps(status,indent=2))
    report=f'''# Offline alpha experiments — results

**Conclusion: the existing external feeds contain useful, very short-lived information about upcoming BRTI messages. The tested models do not yet identify a profitable subset of fills. No new trading policy passed the research gate.**

This work read historical data only. It did not start data collection, submit orders, modify live configurations, stop processes, or operate existing terminal sessions. Outputs, fitted research models and an offline rerun script are saved. A full responsive B/C strategy replay was **not run**: every tested quote-filter family chose no filtering on validation, so none passed the predeclared prerequisite for that expensive stage. Forecast accuracy alone is not evidence of executable alpha.

![Experiment summary]({p}/experiment_summary.png)

## Data and experimental design

The frozen cutoff is September 28, 2026 at 10:00 UTC. The receive-time panel spans September 25 22:00 through that cutoff, at 200 ms intervals: **1,080,000 grid points**, before freshness exclusions. It combines Coinbase, Kraken, Bitstamp, Gemini, Crypto.com, Deribit, OKX and Hyperliquid with the recorded BRTI feed. A few days of data are not a few million independent observations.

Paper logs supply **17,652 order submissions and 478 simulated partial/full fills**. Every submission was joined to its own quote diagnostics at the exact decision timestamp, market, side and price; order IDs were checked where available. Unfilled orders are retained. Partial fills are aggregated by order. This is a stronger test than selecting attractive-looking realized fills after the fact.

| Quote sample | Orders | Orders that filled | Fill records | Expiration clusters |
|---|---:|---:|---:|---:|
{chr(10).join(splitrows)}

Training uses Sep 25–26 paper quotes, with the forecast model trained on Sep 26. Validation is Sep 27 00:00–12:00 UTC. The final test is Sep 27 12:00–Sep 28 10:00 UTC. Some hours have no recording or no active paper session. The forecast test has about 322,500 usable observations across 18 hourly blocks; the quote test has 17 expiration clusters.

Models never train on future labels: a filled order's result must have been logged before the training boundary. Unfilled orders need a fully observed lifetime. Eligibility also requires known outcome/accounting, fresh causal fair value, the 3–97-cent YES price range, and 90–3900 seconds to expiration. The strategy's last-90-second far-tail exception is intentionally outside this study. Quotes with unavailable outcomes remain in the audit cache and are excluded explicitly; their profit is not silently set to zero.

The paper history spans a code change. The final evaluated sample includes **1,526 legacy submissions / 100 filled contracts**, and **6,673 corrected-code submissions / 570.99 filled contracts**. The first cohort made $23.65 and the second $30.85 in the saved simulation. Pooling them is a research screen, not a certification of the corrected strategy's execution model. The historical data had been available previously; this is a chronology-respecting held-out test for these fitted models, not an independently attested prospective live holdout.

## Experiment 1: do external feeds add predictive information?

Yes, in this recording. A regularized model using spot order-book, microprice, imbalance, recent flow/returns and perpetual features was selected using **validation 0.5-second error only**, then evaluated without refitting on the final test.

RMSE below is dollar error in the BTC benchmark, not trading profit or binary-contract return.

| Horizon | Last-print benchmark RMSE | Selected model RMSE | Error reduction | Hour-block 95% interval |
|---|---:|---:|---:|---:|
{chr(10).join(ft)}

The model also beats the stronger comparator using past BRTI returns alone; that comparator is slightly worse than the last-print forecast on the final sample. Thus the result is not merely recovery of benchmark momentum.

The feature-family comparison matters:

- At 0.5 seconds, **spot price differences alone improve error by only 1.0%**.
- Adding spot microstructure and recent flow/returns raises the gain to **11.7%**.
- Adding the recorded perpetual features raises it to **12.6%**—about 0.9 percentage point more, or a 1.0% reduction in error relative to the spot-microstructure model.
- One-venue diagnostics, with the same own-history controls, show Crypto.com contributing the strongest standalone gain at 0.5 seconds (**11.1%**), followed by OKX (**8.1%**) and Gemini (**4.5%**). Coinbase and Deribit each contribute under 1% in this particular held-out period. These diagnostics are not a new test-selected trading rule; feeds are correlated and these numbers are not causal attribution of the combined model.

This favors extracting more value from the already recorded book/flow information before purchasing another feed. BRTI itself aggregates exchange order data, so a short lead over delivered benchmark messages is plausible. The [official BRTI description](https://www.cfbenchmarks.com/data/indices/BRTI) describes its order-data basis and 200 ms publication frequency. The research panel is **not** a reconstruction or certification of the official benchmark methodology.

## Timing substantially weakens the opportunity

Two distinct diagnostics were run after the main models were frozen. Neither changes the selected trading policy.

**Delayed information.** With external observations delayed by 200 ms, the frozen model is about 3.1% worse than the last-print baseline. Re-training on the same earlier training period with that delay explicitly represented recovers only **0.3% improvement**. At a one-second external delay, even the delay-aware refit is slightly worse than the last-print baseline. The corrected calculation compares delayed venue prices against the *current* benchmark; shifting an already-computed old price gap would be inconsistent.

**Movement left after an order can react.** At decision time t, predict BRTI(t+0.5s) − BRTI(t+action delay), using only information available at t. Each latency-specific regression is fitted on the original training period, with the feature family and regularization held fixed. This asks whether there is information about the move remaining after a potential response, rather than crediting a trade for a move that already occurred before it could arrive.

| Potential action delay | Zero-move forecast RMSE | Model RMSE | Remaining-move error reduction |
|---|---:|---:|---:|
{chr(10).join(lat)}

The same pattern appears at a one-second horizon: about 6.9% gain with zero action delay, 1.4% after 50 ms, and no gain after 100 ms. These are exploratory timing diagnostics, not a complete order/queue simulation. They do not show that Kalshi executable prices lag BRTI by the same amount, or that a 50 ms trade would make money.

**Interpretation:** much of the measured information appears to anticipate the next delivered benchmark update, rather than forecast a durable BTC price move. Its most plausible application is a fast stale-quote cancel/valuation adjustment. A 200 ms quote cycle plus network and exchange latency may consume it. Measured end-to-end response time matters more here than adding a larger model.

## Experiment 2: predict fills and their profitability separately

The three model families use (a) price/model edge, (b) those inputs plus queue/quote state, and (c) quote state plus external signals. The price comparator includes both central fair-value edge and the conservative edge actually logged by the strategy.

A logistic model estimates whether an order fills under the existing cancellation policy. A regularized regression estimates net cents per filled contract. Their product, adjusted for requested size and the training average filled fraction, ranks expected dollars per quote. Fitted parameters, imputation, standardization and fee assumptions use the training sample only. Conditional-profit predictions are bounded to possible binary payouts.

| Model | Held-out fill-ranking AUC | Held-out fill log loss | Conditional profit RMSE |
|---|---:|---:|---:|
{chr(10).join(fills)}

Higher AUC is better; lower losses are better. **AUC 0.942 is not 94.2% classification accuracy.** It indicates strong ranking of filled versus unfilled orders. The quote-state model also improves probability loss, so the fill ranking is not just an attractive-looking AUC.

However, the training-mean conditional-profit benchmark has **37.05¢ RMSE**. Every fitted profit model is worse: price-only 39.83¢, quote-state 39.58¢, and external 43.12¢. Adding external inputs worsens both the quote-state model's fill calibration and its conditional-profit prediction on this test. With only 124 filled training orders, the rich external model is especially vulnerable to estimation error. This rejects these fitted models as promotion candidates; it does not prove profitable fills are intrinsically unpredictable.

Predicting an execution is therefore much easier here than predicting a good execution. Large/deep queue differences help identify whether an order will be reached, but that alone does not identify which filled contracts will pay off favorably.

## Economic policy test: every family selected no filtering

The policy threshold was selected on validation, not on final-test profit. It could remove at most 20% of filled contracts and had to increase total dollars. Candidate thresholds were fixed percentiles of training expected-dollar scores, plus a no-filter fallback.

Even the smallest tested rejection cutoff—the lowest 5% of **training** quote scores, applied unchanged to validation—hurt:

| Model | Validation filled contracts retained | Change in validation dollars |
|---|---:|---:|
{chr(10).join(grid)}

The 5% training-score cutoff is not a promise to reject exactly 5% of later quotes or contracts. Distribution changes explain the different retention rates. All larger tested cutoffs also failed to improve validation dollars. Thus the outcome is not solely an artifact of the 80% retention requirement.

Every family chose **keep all quotes**. Validation remains **−$8.11**; final-test profit remains **+$54.50**, with **$0 incremental profit attributed to the learned filters**. The $54.50 is a separate, later paper cohort, not an improvement on the original $21.69 queue run. Two expirations contribute $34.60 of that profit (63.5%), and the sample includes the code-version split described above.

No learned rule qualified for the full responsive B/C replay. Running that expensive stage with the selected no-op rule would reproduce its baseline behavior rather than test a new strategy. The quote-level filtering test itself holds subsequent historical opportunities fixed; it does not model the inventory/risk-headroom changes caused by removing orders. Even a positive fixed-opportunity result would still need responsive replay before being called a trading improvement.

## Decision and next useful work

**Do not promote the fitted profitable-fill filters.** They failed their validation economics and held-out conditional-profit tests. Do not convert the +50% queue assumption into intentional delay; the new timing tests give another reason to avoid that interpretation.

**Retain the external forecasting model as a research candidate.** The raw forecasting improvement is real within this recorded sample, but its very short life makes execution the central obstacle. The next experiment should freeze a simple event-driven stale-quote guard, fit it on corrected-code quote histories, and test its full submission/cancellation latency against actual executable Kalshi books under B and C. It should be assessed separately from a general settlement-profit predictor. Using future recorded days from the already running collector is sufficient; this task did not require a new collection session.

The most useful modeling improvement is to add more correctly timed filled-order examples and shorter-horizon adverse-move labels, while retaining unfilled exposure and actual cancellation behavior. A rich settlement-profit model trained on 124 filled orders is not well supported. The saved cache and joins make those additional studies much cheaper than re-reading all raw data. A forecast gain of 12.6% is not a 12.6% profit gain, and none of these results validates scalable positive trading economics.

## Integrity, scope and reproducibility

- Original recordings, simulation outputs, live configurations, services and terminal sessions were untouched. Historical input sizes were checked before/after reading and SHA-256 hashes saved. The analysis uses one worker and one native math thread; the app sandbox disallowed lowering scheduling priority. Prediction matrices are processed in batches to limit temporary memory.
- External books use the repository's own sequence/checksum normalizers. Invalid/disconnected books are excluded; books older than two seconds are unavailable. The current and future benchmark labels require fresh source and receive timestamps. Exact receipt ties use information available before that receipt.
- Kraken had six backward receipt timestamps (maximum 41.853 ms) and OKX three (maximum 42.804 ms). Arrival order is preserved; availability is conservatively advanced to the maximum receipt timestamp already observed. Nine old-source BRTI arrivals are prevented from rewinding the benchmark; 1 Hz takes priority on equal-source ties, matching the production tracker.
- Fresh external-book coverage on the full 60-hour grid is roughly 78–89% per venue. Gaps are not carried forward indefinitely. Almost every quoted opportunity has at least two fresh spot venues because the paper strategy was largely inactive during the main outage.
- Quotes and outcomes from old paper sessions retain the old simulator/latency limitations. The code-version split is explicit. Simulations and paper records are not live fills.
- Price forecast uncertainty uses hourly blocks. Economic uncertainty groups by expiration. Only a few calendar days are represented, so these intervals do not capture uncertainty across many market regimes.
- Latency diagnostics were added after the main results and are descriptive. No filter threshold or feature family was reselected on their final-test outcomes.
- Superseded preliminary panel outputs are clearly marked in `preverification/`; the final panel uses source-time reconciliation and freshness. No decisions were based on preliminary test scores.
- Focused tests check strict as-of joins, stale gaps, future-data invariance, nanosecond units, exact quote/decision joins, delayed benchmark handling and delayed-price-gap arithmetic. Results are in `tests.txt`.

See [RUNBOOK.md]({p}/RUNBOOK.md) for the offline rerun command, artifact map and explicit limitations. The frozen design and quality corrections are in [PROTOCOL.md]({p}/PROTOCOL.md). The machine-readable decision is [decision.json]({p}/decision.json).
'''
    (p/'REPORT.md').write_text(report)
    source=[Path('dh/research/offline_alpha.py'),Path('dh/research/offline_alpha_models.py'),Path('dh/research/offline_alpha_diagnostics.py'),Path('dh/research/offline_alpha_report.py'),Path('tests/research/test_offline_alpha.py'),Path('scripts/run_offline_alpha.sh'),Path('config/m1.yaml')]
    manifest={'git_head':subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip(),'source_hashes':{str(x):hashlib.sha256(x.read_bytes()).hexdigest() for x in source},'input_manifests':[str(x.relative_to(p)) for x in (p/'cache').glob('*_meta.json')], 'research_only':True}
    (p/'manifest.json').write_text(json.dumps(manifest,indent=2))
    print(p/'REPORT.md')
if __name__=='__main__':main()
