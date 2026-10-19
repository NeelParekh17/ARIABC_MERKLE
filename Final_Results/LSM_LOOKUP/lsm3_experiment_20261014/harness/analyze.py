#!/usr/bin/env python3
"""Analyze preserved accepted cases; never promote missing points or noise attempts."""
import argparse,csv,json,re,statistics as st
from collections import Counter,defaultdict
from pathlib import Path

def writecsv(path,rows):
 if not rows:path.write_text('');return
 fields=list(dict.fromkeys(k for r in rows for k in r))
 with path.open('w',newline='') as f:
  w=csv.DictWriter(f,fieldnames=fields);w.writeheader();w.writerows(rows)
def diffrows(before,after,field,key):
 a={r[key]:r for r in before or []};b={r[key]:r for r in after or []}
 return {k:int(v[field])-int(a.get(k,{}).get(field,0)) for k,v in b.items()}
def psi_summary(path):
 records=[]
 for line in path.read_text().splitlines():
  item=json.loads(line)
  if 'monotonic_ns' in item:records.append(item)
 if len(records)<2:return dict(samples=len(records),io_full_pct=None,memory_full_pct=None,max_gap_s=None)
 elapsed=(records[-1]['monotonic_ns']-records[0]['monotonic_ns'])/1e9
 def fraction(key):
  def total(item):
   for line in (item.get(key) or '').splitlines():
    if line.startswith('full '):return int(dict(x.split('=') for x in line.split()[1:])['total'])
   return None
  before,after=total(records[0]),total(records[-1])
  return (after-before)/(elapsed*1e6)*100 if elapsed>0 and before is not None and after is not None else None
 return dict(samples=len(records),io_full_pct=fraction('pressure/io'),memory_full_pct=fraction('pressure/memory'),max_gap_s=max((b['monotonic_ns']-a['monotonic_ns'])/1e9 for a,b in zip(records,records[1:])))
def analyze(root,out):
 root=root.resolve();out=out.resolve()
 out.mkdir(parents=True,exist_ok=True);rows=[];attempts=[]
 for summary in sorted(root.glob('*/run_*/summary.csv')):
  stage=summary.parent.parent.name
  if stage not in ['smoke','ff90']:continue
  for r in csv.DictReader(summary.open()):
   case=Path(r['artifact_dir'])
   if not case.exists():case=summary.parent/case.name
   saved=json.loads((case/'result.json').read_text())
   assert saved['gateway']['gateway_returncode']==0 and r['noise_excluded'].lower()=='false'
   assert int(r['divergence_count'])==int(r['permanent_failures'])==0
   assert int(r['validated_completed_queries'])==int(r['total_queries'])
   if r['mode'].startswith('merkle'):assert (case/'merkle_verify.txt').read_text().strip()=='t'
   x=json.loads((case/'detailed_counters.json').read_text());a=x['before'];b=x['after_shutdown_restart'];n=int(r['total_queries'])
   idxread=diffrows(a['indexes'],b['indexes'],'idx_blks_read','indexrelname');idxhit=diffrows(a['indexes'],b['indexes'],'idx_blks_hit','indexrelname')
   up=diffrows(a['updates'],b['updates'],'n_tup_upd','relname');hot=diffrows(a['updates'],b['updates'],'n_tup_hot_upd','relname');ins=diffrows(a['updates'],b['updates'],'n_tup_ins','relname')
   tre=diffrows(a['tables'],b['tables'],'heap_blks_read','relname');thi=diffrows(a['tables'],b['tables'],'heap_blks_hit','relname')
   fpi=json.loads((case/'wal_fpi_count.json').read_text())['fpi_count'] if (case/'wal_fpi_count.json').exists() else None
   merges=x['after_compaction_drain'].get('merges_requested',0)-a.get('merges_requested',0)
   completed=len(re.findall(r'LOG:  Lsm3: truncate index usertable_merkle_lookup_idx_top', (case/'postgres.log').read_text()))
   tops={z['relname']:z['bytes'] for z in x['after_compaction_drain']['sizes'] if z['relname'].startswith('usertable_merkle_lookup_idx_top')}
   dbpsi=psi_summary(case/'telemetry.jsonl');gwpsi=psi_summary(case/'gateway_telemetry.jsonl')
   noise=json.loads((case/'noise_qualification.json').read_text())
   row=dict(stage=stage,noise_excluded=noise['excluded'],noise_findings=json.dumps(noise['findings']),db_io_full_psi_pct=dbpsi['io_full_pct'],db_memory_full_psi_pct=dbpsi['memory_full_pct'],gateway_io_full_psi_pct=gwpsi['io_full_pct'],gateway_memory_full_psi_pct=gwpsi['memory_full_pct'],max_sampler_gap_s=max(dbpsi['max_gap_s'] or 0,gwpsi['max_gap_s'] or 0),fillfactor=int(r['fillfactor']),skew=float(r['skew']),workers=int(r['workers']),mode=r['mode'],trial=int(r['trial']),tps=float(r['tps']),statements=n,
    gateway_wall_ms=float(r['wall_time_ms']),device_window_ms=float(r['device_window_ms']),reset_time_ms=float(r['reset_time_ms']),settle_wait_s=float(r['settle_wait_s']),checkpoint_ms=float(r['checkpoint_ms']),device_read_kb_per_stmt=float(r['device_read_mib'])*1024/n,
    device_write_kb_per_stmt=float(r['device_write_mib'])*1024/n,wal_bytes_per_stmt=x['wal_bytes']/n,fpis_per_stmt=fpi/n if fpi is not None else '',
    lookup_reads=sum(v for k,v in idxread.items() if k.startswith('usertable_merkle_lookup_idx')),
    lookup_hits=sum(v for k,v in idxhit.items() if k.startswith('usertable_merkle_lookup_idx')),
    lookup_base_reads=idxread.get('usertable_merkle_lookup_idx',0),lookup_top_reads=sum(v for k,v in idxread.items() if k.startswith('usertable_merkle_lookup_idx_top')),
    lookup_base_hits=idxhit.get('usertable_merkle_lookup_idx',0),lookup_top_hits=sum(v for k,v in idxhit.items() if k.startswith('usertable_merkle_lookup_idx_top')),
    primary_reads=idxread.get('usertable_pkey1',0),primary_hits=idxhit.get('usertable_pkey1',0),
    node_heap_reads=tre.get('merkle_node_usertable',0),node_heap_hits=thi.get('merkle_node_usertable',0),
    node_index_reads=sum(v for k,v in idxread.items() if k.startswith('merkle_node_usertable')),
    node_index_hits=sum(v for k,v in idxhit.items() if k.startswith('merkle_node_usertable')),
    updates=up.get('usertable',0),hot_updates=hot.get('usertable',0),non_hot_updates=up.get('usertable',0)-hot.get('usertable',0),
    inferred_lookup_inserts=(ins.get('usertable',0)+up.get('usertable',0)-hot.get('usertable',0)) if r['mode'].startswith('merkle') else 0,
    merges_requested=merges,merges_completed=completed,top0_end_bytes=tops.get('usertable_merkle_lookup_idx_top0',0),top1_end_bytes=tops.get('usertable_merkle_lookup_idx_top1',0),
    compaction_drain_ms=x['compaction_drain_ms'],drain_inclusive_tps=n/((float(r['wall_time_ms'])+x['compaction_drain_ms'])/1000),
    merkle_verify=r['merkle_verify'],artifact_dir=str(case))
   rows.append(row)
  if (summary.parent/'all_attempts.csv').exists():
   attempts.extend(dict(stage=stage,**r) for r in csv.DictReader((summary.parent/'all_attempts.csv').open()))
 counts=dict(Counter(r['stage'] for r in rows));complete=counts=={'smoke':3,'ff90':72}
 unique={(r['stage'],r['skew'],r['workers'],r['mode'],r['trial']) for r in rows}
 assert len(unique)==len(rows),'Duplicate accepted configuration/trial'
 assert all(r['fillfactor']==90 and r['statements']==20000 for r in rows)
 metadata=dict(expected={'smoke':3,'ff90':72},stage_counts=counts,complete=complete,scope='ff90 only; ff100 and sustained cancelled by user',compaction_studied=False)
 (out/'analysis_status.json').write_text(json.dumps(metadata,indent=2)+'\n')
 writecsv(out/'results.csv',rows);writecsv(out/'attempts.csv',attempts)
 groups=defaultdict(list)
 for r in rows:groups[tuple(r[k] for k in ['stage','fillfactor','skew','workers','mode'])].append(r)
 aggregated=[];by_point=defaultdict(dict)
 for (stage,ff,theta,w,mode),rr in sorted(groups.items()):
  vals=[r['tps'] for r in rr];item=dict(stage=stage,fillfactor=ff,skew=theta,workers=w,mode=mode,n=len(rr),median_tps=st.median(vals),min_tps=min(vals),max_tps=max(vals),cv_pct=st.stdev(vals)/st.mean(vals)*100 if len(vals)>1 else '')
  for k in ['device_read_kb_per_stmt','device_write_kb_per_stmt','wal_bytes_per_stmt','fpis_per_stmt','lookup_reads','lookup_hits','lookup_base_reads','lookup_top_reads','lookup_base_hits','lookup_top_hits','primary_reads','primary_hits','node_heap_reads','node_heap_hits','node_index_reads','node_index_hits','inferred_lookup_inserts','updates','hot_updates','non_hot_updates','merges_requested','merges_completed','top0_end_bytes','top1_end_bytes','compaction_drain_ms','drain_inclusive_tps']:
   v=[r[k] for r in rr if isinstance(r[k],(float,int))];item[k]=st.median(v) if v else ''
  aggregated.append(item);by_point[stage,ff,theta,w][mode]=item
 writecsv(out/'medians.csv',aggregated)
 ratios=[]
 for (stage,ff,theta,w),mm in sorted(by_point.items()):
  if set(mm)!={'det','merkle_btree','merkle_lsm'}:continue
  d,b,l=[mm[m]['median_tps'] for m in ['det','merkle_btree','merkle_lsm']]
  ratios.append(dict(stage=stage,fillfactor=ff,skew=theta,workers=w,det_tps=d,btree_tps=b,lsm_tps=l,btree_det_ratio=b/d,lsm_det_ratio=l/d,gap_closed=(l-b)/(d-b) if d!=b else '',baseline_gap_tps=d-b,no_det_advantage=d<=b,lsm_btree_gain=l/b-1,min_trials=min(mm[m]['n'] for m in mm)))
 writecsv(out/'ratios.csv',ratios)
 activity=[]
 for theta,w in sorted({(r['skew'],r['workers']) for r in rows if r['stage']=='ff90'}):
  point=dict(skew=theta,workers=w)
  for mode in ['det','merkle_btree','merkle_lsm']:
   rr=[r for r in rows if r['stage']=='ff90' and r['skew']==theta and r['workers']==w and r['mode']==mode]
   point[mode+'_trials']=len(rr)
   for field in ['updates','hot_updates','non_hot_updates','inferred_lookup_inserts','merges_requested','merges_completed']:
    point[mode+'_'+field+'_sum']=sum(r[field] for r in rr)
   if mode=='merkle_lsm' and rr:
    for field in ['top0_end_bytes','top1_end_bytes']:
     point[field+'_min']=min(r[field] for r in rr);point[field+'_max']=max(r[field] for r in rr)
  activity.append(point)
 writecsv(out/'hot_lookup_by_point.csv',activity)

 lines=['# LSM lookup results','',f'Accepted cases: {len(rows)}/75 (smoke 3 + ff90 72). Complete: {complete}. Preserved current-stage attempts: {len(attempts)}. Missing configurations are incomplete, not zeros. Smoke is separate from the grid.','', '| Stage | FF | Theta | W | Mode | n | Median TPS | Min | Max |','|---|---:|---:|---:|---|---:|---:|---:|---:|']
 for r in aggregated:lines.append(f"| {r['stage']} | {r['fillfactor']} | {r['skew']} | {r['workers']} | {r['mode']} | {r['n']} | {r['median_tps']:.2f} | {r['min_tps']:.2f} | {r['max_tps']:.2f} |")
 lines+=['','| Stage | FF | Theta | W | Btree/det | LSM/det | Gap closed |','|---|---:|---:|---:|---:|---:|---:|']
 for r in ratios:
  gap=f"{r['gap_closed']:.1%}" if r['gap_closed']!='' else 'undefined (zero baseline gap)'
  lines.append(f"| {r['stage']} | {r['fillfactor']} | {r['skew']} | {r['workers']} | {r['btree_det_ratio']:.4f} | {r['lsm_det_ratio']:.4f} | {gap} |")
 lines+=['','Per-point counters below are medians of the accepted ff90 trials; block counters are 8 KiB PostgreSQL buffer accesses. Device KB uses 1024-byte units. HOT is the fraction of updates; lookup inserts are inferred, not AM callback instrumentation.','',
 '| Theta | W | Mode | HOT % | Non-HOT | Lookup reads/hits | Node heap/index reads | Node heap/index hits | WAL KiB/stmt | FPI/stmt | Device R/W KiB/stmt | Merges completed (sum) |',
 '|---|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|']
 for r in aggregated:
  if r['stage']!='ff90':continue
  hot=100*r['hot_updates']/r['updates'] if r['updates'] else 0
  merge_sum=sum(x['merges_completed'] for x in groups[('ff90',r['fillfactor'],r['skew'],r['workers'],r['mode'])])
  lines.append(f"| {r['skew']} | {r['workers']} | {r['mode']} | {hot:.2f} | {r['non_hot_updates']} | {r['lookup_reads']}/{r['lookup_hits']} | {r['node_heap_reads']}/{r['node_index_reads']} | {r['node_heap_hits']}/{r['node_index_hits']} | {r['wal_bytes_per_stmt']/1024:.3f} | {r['fpis_per_stmt']:.4f} | {r['device_read_kb_per_stmt']:.3f}/{r['device_write_kb_per_stmt']:.3f} | {merge_sum} |")
 lines+=['','Exact activity counts summed across the three trials at each point (the per-trial CSV retains every value). Lookup inserts = usertable inserts + non-HOT updates; there are no usertable INSERT statements in workload A.','',
 '| Theta | W | det HOT/updates | Btree HOT/updates | Btree lookup inserts | LSM HOT/updates | LSM lookup inserts | LSM requested/completed merges | LSM top0 end bytes min-max | LSM top1 end bytes min-max |',
 '|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|']
 for r in activity:
  def hot_count(mode):return f"{r[mode+'_hot_updates_sum']}/{r[mode+'_updates_sum']}"
  lines.append(f"| {r['skew']} | {r['workers']} | {hot_count('det')} | {hot_count('merkle_btree')} | {r['merkle_btree_inferred_lookup_inserts_sum']} | {hot_count('merkle_lsm')} | {r['merkle_lsm_inferred_lookup_inserts_sum']} | {r['merkle_lsm_merges_requested_sum']}/{r['merkle_lsm_merges_completed_sum']} | {r.get('top0_end_bytes_min','')}-{r.get('top0_end_bytes_max','')} | {r.get('top1_end_bytes_min','')}-{r.get('top1_end_bytes_max','')} |")
 lines+=['',f"Observed ff90 LSM merges: {sum(r['merges_requested'] for r in rows if r['stage']=='ff90' and r['mode']=='merkle_lsm')} requested, {sum(r['merges_completed'] for r in rows if r['stage']=='ff90' and r['mode']=='merkle_lsm')} completed.",'', 'Gap closed = (LSM - Btree)/(det - Btree), using matched medians. It is unstable when the original gap is small; negative values and values over 100% are retained. When Btree is already as fast as or faster than det, the formula is reported but there is no positive det advantage to close. No causal claim is based on smoke.','',
 'Counter attribution: lookup reads/hits sum base and both tops. Lookup inserts are inferred from usertable inserts + non-HOT updates, not measured AM calls. WAL includes compaction completed before the explicit drain; drain time and drain-inclusive TPS are separate from gateway TPS. FPI counts come from decoding the measured WAL interval. Device I/O includes unrelated traffic on that device and sampler/launch overhead around the gateway window; both durations are saved. Checkpoint and restore times are saved separately. PSI averages and maximum sampler gaps are recorded per case; accepted cases pass the same predeclared severe-stall rule in every mode.','',
 'Compaction behaviour was not studied by user choice: ff100 baselines/grid and sustained runs were cancelled. The complete authorized dataset is smoke:3 plus ff90:72. Merge requests/completions and end top sizes observed in these short ff90 runs are reported; they do not establish sustained compaction performance.']
 (out/'RESULTS.md').write_text('\n'.join(lines)+'\n')
 if aggregated:
  import matplotlib;matplotlib.use('Agg');import matplotlib.pyplot as plt
  panels=sorted({(r['fillfactor'],r['skew']) for r in aggregated if r['stage'] in ['ff90']})
  if panels:
   fig,ax=plt.subplots(len(panels),2,figsize=(7,2.7*len(panels)),squeeze=False)
   styles={'det':('#0072B2','o'),'merkle_btree':('#D55E00','s'),'merkle_lsm':('#009E73','^')}
   for i,(ff,theta) in enumerate(panels):
    for mode,(color,marker) in styles.items():
     rr=sorted([r for r in aggregated if r['stage'] in ['ff90'] and r['fillfactor']==ff and r['skew']==theta and r['mode']==mode],key=lambda x:x['workers'])
     ax[i,0].plot([r['workers'] for r in rr],[r['median_tps'] for r in rr],color=color,marker=marker,label=mode)
     ax[i,0].fill_between([r['workers'] for r in rr],[r['min_tps'] for r in rr],[r['max_tps'] for r in rr],color=color,alpha=.15)
    pp=sorted([r for r in ratios if r['stage'] in ['ff90'] and r['fillfactor']==ff and r['skew']==theta],key=lambda x:x['workers'])
    for mode,key in [('merkle_btree','btree_det_ratio'),('merkle_lsm','lsm_det_ratio')]:
     color,marker=styles[mode];ax[i,1].plot([r['workers'] for r in pp],[r[key] for r in pp],color=color,marker=marker,label=mode)
    for j in range(2):ax[i,j].set(xlabel='Workers',title=f'FF {ff}, theta {theta}');ax[i,j].grid(alpha=.25);ax[i,j].legend(fontsize=8);ax[i,j].set_xticks([1,4,8,16])
    ax[i,0].set(ylabel='Statements / second',ylim=(0,None));ax[i,1].set(ylabel='Merkle / det TPS');ax[i,1].axhline(1,color='gray',linestyle=':')
   fig.suptitle('100M rows, workload A, fillfactor 90: median TPS',fontsize=11)
   fig.text(.5,.012,'Shading: trial min-max (n=3). Ratios use matched medians. Sustained compaction not studied.',ha='center',fontsize=7)
   fig.tight_layout(rect=(0,.045,1,.95));fig.savefig(out/'tps_and_ratios.png',dpi=200);fig.savefig(out/'tps_and_ratios.pdf');plt.close(fig)
 print(json.dumps(dict(accepted=len(rows),complete=complete,matched_points=len(ratios),out=str(out))))
if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('--root',type=Path,required=True);p.add_argument('--out',type=Path,required=True);a=p.parse_args();analyze(a.root,a.out)
