#!/usr/bin/env python3
"""Reproduce publication locally from checked evidence; performs no remote actions."""
from pathlib import Path
import collections,csv,hashlib,json,math,shutil
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.ticker import MaxNLocator
P=Path(__file__).resolve().parent;ROOT=P.parents[1];FIG=ROOT/'figures';FIG.mkdir(exist_ok=True)
KEY=['workload','skew','workers','mode'];MODES=['pg','bcdb_det','bcdb_merkle'];NAMES={'pg':'PG (SSI + jitter)','bcdb_det':'det','bcdb_merkle':'det + Merkle'}
COMBOS=[('a',0.),('a',.99),('a',1.2),('b',.99),('c',.99),('d',.99),('f',.99)];COLORS=['#0072B2','#E69F00','#009E73'];MARKERS=['o','s','^']
D=pd.read_csv(P/'evidence/campaign/summary.csv');S=pd.read_csv(P/'evidence/smoke/summary.csv');O=pd.read_csv(P/'previous_v2_trials.csv')
assert len(D)==252 and len(S)==3 and len(O)==84 and not D.duplicated(KEY+['trial']).any()
assert (D.groupby(KEY).trial.agg(set)=={1,2,3}).all()
assert set(map(tuple,D[KEY].drop_duplicates().to_numpy()))==set(map(tuple,O[KEY].to_numpy()))
for d in (D,S):
 assert (d[['divergence_count','permanent_failures','retry_exhausted']]==0).all().all()
 assert (d.validated_completed_queries==20000).all() and (d.loc[d['mode']=='bcdb_merkle','merkle_verify']=='PASS').all()
 assert d.noise_excluded.eq(False).all()
# Root publication normalizes line endings; remote evidence bytes remain untouched.
(ROOT/'summary.csv').write_bytes((P/'evidence/campaign/summary.csv').read_bytes().replace(b'\r\n',b'\n'))
for group in ('campaign','smoke'):
 assert list(csv.DictReader((P/'evidence'/group/'summary.csv').open()))==list(csv.DictReader((P/'evidence'/group/'all_attempts.csv').open()))
small=json.load(open(P/'evidence/remote_small_checks.json'))
assert all(x['all_case_directories']==x['accepted_directories'] for x in small['run_directories'].values())
for group in ('campaign','smoke'):
 for case in (P/'evidence/raw'/group).rglob('setup.json'):
  setup=json.load(open(case)); mode='bcdb_merkle' if 'bcdb_merkle' in case.parent.name else 'plain'
  manifest=json.load(open(P/'evidence/baseline_evidence'/('golden_manifest.json' if mode=='bcdb_merkle' else 'plain_manifest.json')))
  assert setup['provenance']['baseline_manifest']==manifest
  assert setup['indexes']==manifest['indexes']
D.to_csv(P/'trials.csv',index=False);S.to_csv(P/'smoke_trials.csv',index=False)
METRICS=['device_read_mib','device_write_mib','device_read_await_ms','blk_read_time_ms','blk_write_time_ms','retry_attempts','retry_backoff_requested_ms','reset_time_ms','restore_write_mib','settle_wait_s','wall_time_ms','device_window_ms','checkpoint_ms','checkpoint_write_mib']
rows=[]
for key,g in D.groupby(KEY,sort=True):
 values=g.tps.to_numpy();v=dict(zip(KEY,key));v.update(n=len(values),median_tps=float(np.median(values)),min_tps=float(np.min(values)),max_tps=float(np.max(values)),best_tps=float(np.max(values)),mean_tps=float(np.mean(values)),sample_std_tps=float(np.std(values,ddof=1)),cv_pct=float(np.std(values,ddof=1)/np.mean(values)*100),range_pct_of_median=float((max(values)-min(values))/np.median(values)*100),max_min_ratio=float(max(values)/min(values)))
 for t in [1,2,3]:v[f'trial_{t}_tps']=float(g.loc[g.trial==t,'tps'].iloc[0])
 for m in METRICS:v[m+'_median']=float(g[m].median())
 v['high_cv_gt_10_pct']=v['cv_pct']>10;rows.append(v)
G=pd.DataFrame(rows);G.to_csv(P/'configuration_summary.csv',index=False);G.sort_values('cv_pct',ascending=False).to_csv(P/'variability.csv',index=False)
R=[]
for key,g in G.groupby(KEY[:3]):
 x=g.set_index('mode');v=dict(zip(KEY[:3],key))
 for mode in MODES:v[mode+'_median_tps']=x.loc[mode,'median_tps']
 for label,a,b in [('det_over_pg','bcdb_det','pg'),('merkle_over_det','bcdb_merkle','bcdb_det'),('merkle_over_pg','bcdb_merkle','pg')]:v[label]=x.loc[a,'median_tps']/x.loc[b,'median_tps']
 for metric in ['device_read_mib','device_write_mib']:
  a=x.loc['bcdb_merkle',metric+'_median'];b=x.loc['bcdb_det',metric+'_median'];v[metric+'_merkle_over_det']=a/b if b>0 else np.nan
 R.append(v)
R=pd.DataFrame(R);R.to_csv(P/'matched_ratios.csv',index=False)
C=G.merge(O[KEY+['tps']+METRICS+['workload_sha256']],on=KEY,validate='one_to_one').rename(columns={'tps':'previous_v2_tps'})
C['new_over_previous']=C.median_tps/C.previous_v2_tps;C['change_pct']=(C.new_over_previous-1)*100;C['previous_n']=1
for m in METRICS:C.rename(columns={m:'previous_'+m},inplace=True)
assert (C.workload_sha256==C[['workload','skew']].apply(lambda r:D.loc[(D.workload==r.workload)&(D['skew']==r['skew']),'workload_sha256'].iloc[0],axis=1)).all()
C.to_csv(P/'previous_v2_comparison.csv',index=False)
H=[]
for label in ['det_over_pg','merkle_over_det','merkle_over_pg']:
 for stat,value in [('min',R[label].min()),('median',R[label].median()),('max',R[label].max())]:H.append(dict(metric=label,statistic=stat,value=value,n=28))
for mode,g in C.groupby('mode'):
 for stat,value in [('min',g.new_over_previous.min()),('median',g.new_over_previous.median()),('max',g.new_over_previous.max())]:H.append(dict(metric=mode+'_new_over_v2',statistic=stat,value=value,n=28))
pd.DataFrame(H).to_csv(P/'headline_numbers.csv',index=False)
IT=json.load(open(P/'independent_terminal_check.json'))
assert len(IT['records'])==255 and all(not x['issues'] and x['terminal_ids']==x['unique_ids']==20000 for x in IT['records'])
A=json.load(open(P/'audit_raw.json'));A2=json.load(open(P/'audit_supplement.json'));F=json.load(open(P/'source_and_fetch_checks.json'))
assert not F['errors'] and not A['provenance_issues'] and all(x['match'] for x in A['harness_hash_checks'])
for path in ['247_binary_recheck.json','247_dependencies_recheck.json']:
 assert all(x['actual']==x['expected'] for x in json.load(open(P/'evidence'/path)))
assert all(x['actual']==x['expected'] for x in A2['local_dependencies'])
auditrows=[];telem=[];err=[];counts=[]
for group,d in A['datasets'].items():
 assert not d['issues'] and not d['rejection_markers'] and len(d['records'])==d['rows']
 extra={r['case']:r for r in A2[group]};ec=collections.Counter();wc=collections.Counter()
 for r in d['records']:
  assert not r['issues'] and not extra[r['case']]['issues']
  ec.update(extra[r['case']]['postgres_errors']);wc.update(extra[r['case']]['postgres_warnings'])
  v=dict(dataset=group,case=r['case'],**dict(zip(KEY+['trial'],r['configuration'])),audit_pass=True,validated_row_results=r['terminal_results']['validated_row_results'],empty_results=r['terminal_results']['empty_results'],merkle_verify=r['merkle_verify'],settle_wait_s=r['settle_wait_s'],content_checked=r['reset_content_checked'],accepted_unix_ns=r['accepted_unix_ns'])
  for k,x in r['det_counters'].items():v[k]=x['max_observed'];v[k+'_emitted']=bool(x['samples'])
  auditrows.append(v)
  for host,t in r['telemetry'].items():telem.append(dict(dataset=group,case=r['case'],host='db_247' if host=='telemetry.jsonl' else 'gateway_111',**{k:v for k,v in t.items() if k not in ('findings','boot_ids','clocksource','kernels')},clocksource=';'.join(t['clocksource']),boot_id=';'.join(t['boot_ids']),kernel=';'.join(t['kernels']),excluded=bool(t['findings'])))
 for msg,n in ec.items():err.append(dict(dataset=group,severity='error_or_fatal',message=msg,count=n))
 for msg,n in wc.items():err.append(dict(dataset=group,severity='warning',message=msg,count=n))
 counts.append(dict(dataset=group,accepted=d['rows'],rejected=0,noise_excluded=0,terminal_results=sum(r['terminal_results']['validated_row_results'] for r in d['records']),empty_results=sum(r['terminal_results']['empty_results'] for r in d['records']),merkle_pass=sum(r['merkle_verify']=='PASS' for r in d['records']),divergence=0,permanent_failures=0,exhausted_retries=0,content_checks=sum(r['reset_content_checked'] for r in d['records']),det_counter_cases_emitted=sum(any(c['samples'] for c in r['det_counters'].values()) for r in d['records'])))
pd.DataFrame(auditrows).to_csv(P/'audit_cases.csv',index=False);pd.DataFrame(telem).to_csv(P/'telemetry_summary.csv',index=False);pd.DataFrame(err).to_csv(P/'log_messages.csv',index=False);pd.DataFrame(counts).to_csv(P/'audit_counts.csv',index=False)
with (P/'REMOTE_RAW_SHA256SUMS').open('w') as f:
 for path,v in sorted(A['raw_hashes'].items()):f.write(v['sha256']+'  '+path+'\n')
pd.DataFrame([dict(path=k,**v) for k,v in sorted(A['raw_hashes'].items())]).to_csv(P/'remote_raw_inventory.csv',index=False)
plt.rcParams.update({'font.family':'DejaVu Sans','font.size':8,'axes.labelsize':9,'axes.titlesize':9,'legend.fontsize':8,'xtick.labelsize':8,'ytick.labelsize':8,'pdf.fonttype':42,'ps.fonttype':42,'axes.spines.top':False,'axes.spines.right':False,'savefig.dpi':300})
def save(fig,name):
 for ext in ('pdf','png'):fig.savefig(FIG/(name+'.'+ext))
 plt.close(fig)
def lines(ax,w,s):
 for mode,color,marker in zip(MODES,COLORS,MARKERS):
  d=G[(G.workload==w)&(G['skew']==s)&(G['mode']==mode)].sort_values('workers');x=d.workers.to_numpy();y=d.median_tps.to_numpy()/1000;lo=d.min_tps.to_numpy()/1000;hi=d.max_tps.to_numpy()/1000
  ax.fill_between(x,lo,hi,color=color,alpha=.18,linewidth=0);ax.plot(x,y,color=color,marker=marker,markersize=3.5,linewidth=1.3,label=NAMES[mode])
 ax.set_xticks([1,4,8,16]);ax.set_xlim(.5,16.5);ax.set_ylim(bottom=0);ax.set_ylabel('Throughput (kstmt/s)');ax.set_xlabel('Workers');ax.set_title(f'{w.upper()}, θ = {s:g}');ax.grid(axis='y',alpha=.25,linewidth=.5);ax.yaxis.set_major_locator(MaxNLocator(4))
for w,s in COMBOS:
 fig,ax=plt.subplots(figsize=(3.35,2.4));lines(ax,w,s);fig.legend(*ax.get_legend_handles_labels(),loc='upper center',bbox_to_anchor=(.5,.998),ncol=3,frameon=False,columnspacing=.65,handlelength=1.4);fig.subplots_adjust(left=.19,right=.97,bottom=.20,top=.76);save(fig,f'scaling_{w}_{s}')
# Tall single-column panel: all points remain readable at 3.35-inch print width.
fig,axes=plt.subplots(7,1,figsize=(3.35,9.0));
for ax,(w,s) in zip(axes,COMBOS):
 lines(ax,w,s);ax.set_xlabel('');ax.set_ylabel('kstmt/s',fontsize=8);ax.set_title('');ax.set_title(f'{w.upper()}, θ = {s:g}',loc='left',fontsize=8);ax.tick_params(labelsize=8)
[ax.tick_params(labelbottom=False) for ax in axes[:-1]]
axes[-1].set_xlabel('Workers');fig.legend(*axes[0].get_legend_handles_labels(),loc='upper center',ncol=3,frameon=False,columnspacing=.65,handlelength=1.3);fig.subplots_adjust(left=.17,right=.97,bottom=.045,top=.95,hspace=.90);save(fig,'oom_scaling_all')
def heat(data,value,name,title,vmin,vmax):
 z=np.array([[data.loc[(data.workload==w)&(data['skew']==s)&(data.workers==t),value].iloc[0] for t in [1,4,8,16]] for w,s in COMBOS]);fig,ax=plt.subplots(figsize=(3.35,2.8));im=ax.imshow(z,cmap='cividis',vmin=vmin,vmax=vmax,aspect='auto');ax.set_xticks(range(4),[1,4,8,16]);ax.set_yticks(range(7),[f'{w.upper()} θ{s:g}' for w,s in COMBOS]);ax.set_xlabel('Workers');ax.set_title(title.replace(': ', ':\n') if len(title)>38 else title,fontsize=8.4)
 for i in range(7):
  for j in range(4):ax.text(j,i,f'{z[i,j]:.2f}',ha='center',va='center',fontsize=8,color='white' if (z[i,j]-vmin)/(vmax-vmin)<.5 else 'black')
 cb=fig.colorbar(im,ax=ax,fraction=.045,pad=.04);cb.ax.tick_params(labelsize=8);cb.set_label('');fig.subplots_adjust(left=.23,right=.90,bottom=.18,top=.87);save(fig,name)
for value,name,title in [('det_over_pg','det_over_pg','det / PG: ratio of medians'),('merkle_over_det','merkle_over_det','Merkle / det: ratio of medians'),('merkle_over_pg','merkle_over_pg','Merkle / PG: ratio of medians')]:heat(R,value,name,title,.55,1.25)
for mode in MODES:heat(C[C['mode']==mode],'new_over_previous','previous_v2_'+mode,f'{NAMES[mode]}: new median / Oct 1 trial',.55,1.05)
# Save every numeric fact used in the narrative as a traceable metric.
V=json.load(open(P/'evidence/baseline_evidence/verification.json'));BM=json.load(open(P/'evidence/baseline_evidence/golden_manifest.json'));BP=json.load(open(P/'evidence/baseline_evidence/plain_manifest.json'));args=json.load(open(P/'evidence/campaign/campaign.json'))['arguments']
facts={k:v for k,v in args.items() if isinstance(v,(str,int,float,bool))};facts.update(frozen_commit=A['head'],configurations=len(G),campaign_cases=len(D),smoke_cases=len(S),all_terminal_results=int((D.validated_completed_queries.sum()+S.validated_completed_queries.sum())),campaign_terminal_results=int(D.validated_completed_queries.sum()),total_merkle_pass=85,campaign_merkle_pass=84,smoke_merkle_pass=1,cv_max_pct=G.cv_pct.max(),cv_gt10_count=int((G.cv_pct>10).sum()),max_range_pct_of_median=G.range_pct_of_median.max(),retry_attempts_campaign=int(D.retry_attempts.sum()),workload_wall_min_ms=int(D.wall_time_ms.min()),workload_wall_max_ms=int(D.wall_time_ms.max()),heap_bytes=BM['heap_bytes'],heap_gib=BM['heap_bytes']/2**30,heap_pages=BM['relpages'],merkle_baseline_bytes=BM['total_bytes'],plain_baseline_bytes=BP['total_bytes'],source_archive_sha256=F['archive_sha256'],source_files_checked=F['source_files_checked'],manifest_files_checked=F['manifest_files'],curated_case_files=F['curated_files'],curated_case_bytes=F['curated_bytes'],raw_case_files=len(A['raw_hashes']),raw_case_bytes=sum(v['bytes'] for v in A['raw_hashes'].values()),campaign_start_utc='2026-10-10T20:42:31Z',campaign_end_utc='2026-10-11T11:13:44Z',campaign_elapsed_hours=(pd.Timestamp('2026-10-11T11:13:44Z')-pd.Timestamp('2026-10-10T20:42:31Z')).total_seconds()/3600,previous_v2_cases=len(O),previous_v2_n=1,diagnostic_cv_threshold_pct=10,qualification_min_trials=5)
runtime=json.load(open(P/'evidence/provenance/runtime_environment.json'))
facts.update(gateway_terminals=96,det_client_workers=96,direct_terminal_protocol=2,settle_consecutive_probes=3,settle_idle_write_mib_limit=2,settle_read_slack_us=30,settle_write_slack_us=50,noise_gap_threshold_s=10,noise_window_min_s=10,noise_io_full_threshold_pct=95,noise_memory_full_threshold_pct=50,figure_width_inches=3.35,previous_v2_git_head='19562ae046786ccd43a5b710a49c4d475714cc33',db_cpu_model=runtime['cpu_model'],db_mem_total_kib=int(runtime['meminfo'].splitlines()[0].split()[1]),db_kernel=runtime['kernel'])
for k,v in V['merkle_stats'].items():
 if isinstance(v,(int,float,str,bool)):facts['baseline_merkle_'+k]=v
for k,v in V['node_storage'].items():facts['node_storage_'+k]=v
T=pd.DataFrame(telem)
for host,g in T[T.dataset=='campaign'].groupby('host'):
 for k,v in {'shorter_than_10s_cases':int((g.duration_s<10).sum()),'max_sampler_gap_s':g.max_gap_s.max(),'min_mem_available_kib':g.min_mem_available_kib.min(),'max_load1':g.max_load1.max(),'max_io_full_fraction':g.max_io_full_fraction.max(),'max_memory_full_fraction':g.max_memory_full_fraction.max()}.items():facts[host+'_'+k]=v
pd.DataFrame([{'metric':k,'value':v} for k,v in facts.items()]).to_csv(P/'setup_and_audit_facts.csv',index=False)
print(json.dumps({'configurations':len(G),'ratios':len(R),'cv_max_pct':G.cv_pct.max(),'audit_cases':len(auditrows),'figures_pdf_png':len(list(FIG.glob('*.pdf'))),'source_errors':len(F['errors'])},indent=2))
