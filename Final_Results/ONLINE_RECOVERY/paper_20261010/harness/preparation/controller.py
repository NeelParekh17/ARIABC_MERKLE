#!/usr/bin/env python3
"""Detached, resumable rotated recovery campaign; retain every raw attempt."""
import argparse,csv,datetime,fcntl,json,os,statistics,subprocess,time
from pathlib import Path
B=Path('/home/neel/ariabc_data/paper_20261010_recov')
results=B/'results';results.mkdir(exist_ok=True)
p=argparse.ArgumentParser();p.add_argument('--phase',choices=['smoke','matrix'],required=True);p.add_argument('--resume',action='store_true');a=p.parse_args()
lock=(results/'controller.lock').open('a');fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
plan=[(1,'A'),(1,'C')] if a.phase=='smoke' else []
modes=['A','C','B','M_passive','M_active','M_mixed','L','L_mix','C_deep']
if a.phase=='matrix':
    for rep in range(1,4):
        rotated=modes[(rep-1)*3:]+modes[:(rep-1)*3]
        plan.extend((rep,mode) for mode in rotated)
(results/(a.phase+'_plan.json')).write_text(json.dumps(plan,indent=2)+'\n')
attempts_path=results/(a.phase+'_attempts.jsonl')
records=[json.loads(x) for x in attempts_path.read_text().splitlines()] if attempts_path.exists() else []
if records and not a.resume:raise RuntimeError('existing results require --resume')
def now():return datetime.datetime.now(datetime.timezone.utc).isoformat()
def status(state,**extra):
    obj={'time':now(),'phase':a.phase,'state':state,'pid':os.getpid(),'planned':len(plan),
         'accepted':sum(r.get('status')=='ACCEPTED' for r in records),
         'rejected':sum(r.get('status')=='REJECTED' for r in records),
         'noise_excluded':sum(r.get('status')=='NOISE_EXCLUDED' for r in records),**extra}
    (results/'status.json').write_text(json.dumps(obj,indent=2)+'\n')
    with (results/'status.txt').open('a') as f:f.write(json.dumps(obj)+'\n')
    print(json.dumps(obj),flush=True)
def tables():
    fields=['run_id','stage','scenario','rep','attempt','status','tps_majority_visible','overhead_vs_baseline_pct',
            'cut_ms','repair_ms','catchup_ms','total_recovery_ms','mismatched_partitions','differing_leaves',
            'candidate_rows','rows_deleted','rows_upserted','full_copies','empty_100ms_buckets','max_completion_gap_ms',
            'quorum_completions','phase8_merkle_pass','phase8_root','raw_divergence_count','gateway_exit_code',
            'dt_opf','dt_opf_stale_error','dt_rel_read_tags','failures','noise']
    good=[r for r in records if r.get('status')=='ACCEPTED']
    baselines=[float(r['tps_majority_visible']) for r in good if r['scenario']=='A']
    base=statistics.median(baselines) if baselines else None
    for name,rows in [('all_attempts.csv',records),('summary.csv',good)]:
        with (results/(a.phase+'_'+name)).open('w',newline='') as f:
            w=csv.DictWriter(f,fieldnames=fields);w.writeheader()
            for r in rows:
                row={k:r.get(k) for k in fields}
                row['overhead_vs_baseline_pct']=100*(float(r['tps_majority_visible'])/base-1) if base and r.get('tps_majority_visible') not in (None,'UNKNOWN') else None
                for k in ('dt_opf','dt_opf_stale_error','dt_rel_read_tags'):row[k]=r.get('det_counters',{}).get(k,{}).get('sum')
                w.writerow(row)
    summary={}
    for mode in modes:
        vals=[float(r['tps_majority_visible']) for r in good if r['scenario']==mode]
        if vals:summary[mode]={'n':len(vals),'median_tps':statistics.median(vals),'min_tps':min(vals),'best_tps':max(vals),
                              'delta_vs_baseline_pct':100*(statistics.median(vals)/base-1) if base else None}
    (results/(a.phase+'_aggregate.json')).write_text(json.dumps(summary,indent=2)+'\n')
status('STARTED')
for rep,mode in plan:
    prior=[r for r in records if r.get('rep')==rep and r.get('scenario')==mode]
    if any(r.get('status')=='ACCEPTED' for r in prior):continue
    trial=1+max((r['attempt'] for r in prior),default=0)
    # Incomplete directories from a disconnected controller remain raw.
    while (B/'repo/scripts/bench_full_results'/f'cluster4_paper_20261010_recov_{a.phase}_{mode}_r{rep}_a{trial}').exists():trial+=1
    for _ in range(3):
        run_id=f'cluster4_paper_20261010_recov_{a.phase}_{mode}_r{rep}_a{trial}'
        out=B/'repo/scripts/bench_full_results'/run_id
        status('RUNNING',run_id=run_id,scenario=mode,rep=rep,attempt=trial)
        start=time.time()
        with (results/(run_id+'.log')).open('x') as f:
            rc=subprocess.run(['bash',str(B/'run_case.sh'),mode,str(rep),str(trial),a.phase],stdout=f,stderr=subprocess.STDOUT).returncode
        if not (out/'recovery_s1024.env').exists():
            out.mkdir(parents=True,exist_ok=True)
            (out/'recovery_s1024.env').write_text(f'scenario={mode}\nstage={a.phase}\nrep={rep}\nattempt={trial}\nexit_code={rc}\ncollection_exit_code=1\n')
        with (results/(run_id+'_acceptance.log')).open('x') as f:
            check=subprocess.run(['python3',str(B/'collect_run.py'),str(out)],stdout=f,stderr=subprocess.STDOUT).returncode
        if (out/'acceptance.json').exists():r=json.loads((out/'acceptance.json').read_text())
        else:r={'run_id':run_id,'stage':a.phase,'scenario':mode,'rep':rep,'attempt':trial,'status':'REJECTED','failures':['acceptance collector failed'],'noise':[]}
        r['case_seconds']=time.time()-start;r['runner_process_rc']=rc;r['collector_rc']=check
        with attempts_path.open('a') as f:f.write(json.dumps(r)+'\n')
        records.append(r);tables();status(r['status'],run_id=run_id,scenario=mode,rep=rep,attempt=trial)
        if r['status']=='ACCEPTED':break
        # Stop if owned cleanup failed rather than interfering with other processes.
        if any('collection' in s for s in r.get('failures',[])) and rc!=0:
            status('STOPPED_REVIEW_REQUIRED',run_id=run_id);raise SystemExit(2)
        trial+=1
    else:
        status('STOPPED_REVIEW_REQUIRED',run_id=run_id);raise SystemExit(2)
tables();status('DONE')
