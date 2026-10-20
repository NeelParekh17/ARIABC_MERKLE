#!/usr/bin/env python3
"""TPC-C-derived direct-count campaign: matched pilots, parity, fixed windows."""
import argparse
import csv
import gzip
import hashlib
import json
import math
import os
from pathlib import Path
import shutil
import statistics
import subprocess
import time
import fcntl
from paper_campaign import read, write, status, recover, retire_database, process_start, digest

HERE=Path(__file__).resolve().parent
MODES=['pg','det','merkle']
POINTS=[(w,32) for w in (5,10,20,30,50,75,100)]+[(100,k) for k in (8,16,24,48,64)]
ENV_FAILURES={'window_300s','time_driven_checkpoint','configured_window','window_available',
              'sampler_quality','mix_minimum_payment','mix_minimum_order_status','mix_minimum_delivery','mix_minimum_stock_level','direct_window_fully_covered','direct_clock_alignment','steady_state','pilot_matched_ramp'}

def effective(r,kind):
    failed=set(r['acceptance']['failed_checks'])
    if kind in ('pilot','parity','smoke'):
        failed-={'window_300s','time_driven_checkpoint'}
    return not failed

def coverage_retry_n(n,metrics,ramp_s,window_s):
    """Sizing only: a short attempt must never cause a shorter metric window."""
    minimum=math.ceil(n*1.5)
    submit_s=metrics.get('max_submit_ms',0)/1000
    if submit_s<=0 or metrics.get('completion_records')!=n or not metrics.get('clock_alignment_valid'):
        return minimum
    capacity=metrics.get('capacity_profile',{}).get('max_business_tps') or 0
    rate=max(n/submit_s,capacity)
    return max(minimum,math.ceil(rate*(ramp_s+window_s)*1.20+65536))

def compress_bulk(run):
    """Reversible lossless archival after measurement and acceptance; no traces removed."""
    records=read(run/'LOSSLESS_ARCHIVES.json',[])
    known={r['original'] for r in records};original_hashes=read(run/'EVIDENCE.sha256.json',{})
    for compressed in sorted(run.glob('*.gz')):
        original=compressed.name[:-3]
        if original in known or (run/original).exists() or original not in original_hashes:continue
        verify=hashlib.sha256();size=0
        with gzip.open(compressed,'rb') as src:
            for chunk in iter(lambda:src.read(1024*1024),b''):verify.update(chunk);size+=len(chunk)
        assert verify.hexdigest()==original_hashes[original]
        records.append(dict(original=original,archive=compressed.name,original_sha256=verify.hexdigest(),
                            original_bytes=size,compressed_bytes=compressed.stat().st_size))
        known.add(original)
    for p in sorted(run.iterdir()):
        if p.name not in ('workload.sql','tx_timeline.csv','server.log','server.err.log') and not p.name.startswith('ptrace.'):
            continue
        if not p.is_file() or p.name.endswith('.gz') or p.stat().st_size<1024*1024:continue
        compressed=p.with_name(p.name+'.gz'); h=hashlib.sha256()
        with p.open('rb') as src,gzip.open(compressed,'wb',compresslevel=1) as dst:
            for chunk in iter(lambda:src.read(1024*1024),b''):h.update(chunk);dst.write(chunk)
        verify=hashlib.sha256()
        with gzip.open(compressed,'rb') as src:
            for chunk in iter(lambda:src.read(1024*1024),b''):verify.update(chunk)
        assert verify.digest()==h.digest()
        records.append(dict(original=p.name,archive=compressed.name,original_sha256=h.hexdigest(),
                            original_bytes=p.stat().st_size,compressed_bytes=compressed.stat().st_size))
        p.unlink()
    write(run/'LOSSLESS_ARCHIVES.json',records)
    manifest={p.name:digest(p) for p in sorted(run.iterdir())
              if p.is_file() and p.name not in ('ARCHIVED_EVIDENCE.sha256.json',)}
    write(run/'ARCHIVED_EVIDENCE.sha256.json',manifest)

def report(root,state):
    rows=[]
    for task in state['tasks']:
        for attempt in task['attempts']:
            r=read(Path(attempt['path'])/'acceptance.json')
            if not r:continue
            m=r.get('direct_metrics',{});steady=m.get('steady_state',{})
            row=dict(kind=task['kind'],mode=task['mode'],warehouses=task['w'],workers=task['workers'],
                trial=task['trial'],attempt=attempt['number'],count=attempt['n'],path=attempt['path'],
                accepted=r['acceptance']['accepted'],qualified=attempt.get('qualified',False),
                tpmc_equivalent=m.get('tpmc_equivalent'),tpmc_equivalent_raw=m.get('tpmc_equivalent_raw'),
                total_business_tps=m.get('total_business_tps'),committed_only_nopm=m.get('committed_only_nopm'),
                window_s=m.get('duration_s'),ramp_s=m.get('ramp_s'),cv=steady.get('cv'),
                slope_tpmc_per_minute=steady.get('slope_tpmc_per_minute'),
                fitted_change_fraction=steady.get('fitted_end_to_start_change_fraction'),
                slope_ci95_lower=steady.get('slope_ci95_lower'),slope_ci95_upper=steady.get('slope_ci95_upper'),
                no_significant_linear_trend=steady.get('no_significant_linear_trend'),steady_state_passed=steady.get('passed'),
                noise_flags=';'.join(r['noise_flags']),failed_checks=';'.join(sorted(set(r['acceptance']['failed_checks'])|set(attempt.get('driver_failed_checks',[])))),
                selection_note=attempt.get('retry_reason',''),preliminary_calibration=attempt.get('preliminary_calibration',False),
                **r.get('det_counters',{}))
            row.update({f'{t}_mix_pct':v*100 if v is not None else None for t,v in m.get('realised_mix',{}).items()})
            row.update({f'{t}_mix_minimum_pass':v for t,v in m.get('mix_minimum_checks',{}).items()})
            for t,rt in m.get('response_times',{}).items():
                row.update({f'{t}_{k}':v for k,v in rt.items() if k!='count'})
            rows.append(row)
    columns=sorted({k for r in rows for k in r})
    for name,group in [('all_runs.csv',rows),('qualified_runs.csv',[r for r in rows if r['qualified']])]:
        with (root/name).open('w',newline='') as f:
            w=csv.DictWriter(f,fieldnames=columns);w.writeheader();w.writerows(group)
    summaries=[]
    for key in sorted({(r['kind'],r['mode'],r['warehouses'],r['workers']) for r in rows if r['qualified'] and r.get('tpmc_equivalent') is not None}):
        group=[r for r in rows if r['qualified'] and (r['kind'],r['mode'],r['warehouses'],r['workers'])==key]
        vals=[r['tpmc_equivalent'] for r in group]
        summaries.append(dict(kind=key[0],mode=key[1],warehouses=key[2],workers=key[3],n=len(vals),
            median_tpmc_equivalent=statistics.median(vals),best_tpmc_equivalent=max(vals),min_tpmc_equivalent=min(vals),
            median_business_tps=statistics.median(r['total_business_tps'] for r in group),
            median_committed_nopm=statistics.median(r['committed_only_nopm'] for r in group)))
    with (root/'summary.csv').open('w',newline='') as f:
        columns=['kind','mode','warehouses','workers','n','median_tpmc_equivalent','best_tpmc_equivalent','min_tpmc_equivalent','median_business_tps','median_committed_nopm']
        w=csv.DictWriter(f,fieldnames=columns);w.writeheader();w.writerows(summaries)
    all_summaries=[]
    for key in sorted({(r['kind'],r['mode'],r['warehouses'],r['workers']) for r in rows if r.get('tpmc_equivalent') is not None}):
        group=[r for r in rows if (r['kind'],r['mode'],r['warehouses'],r['workers'])==key and r.get('tpmc_equivalent') is not None]
        vals=[r['tpmc_equivalent'] for r in group]
        all_summaries.append(dict(kind=key[0],mode=key[1],warehouses=key[2],workers=key[3],n=len(vals),
              strict_accepted_n=sum(r['accepted'] for r in group),qualified_n=sum(r['qualified'] for r in group),
              median_tpmc_equivalent=statistics.median(vals),min_tpmc_equivalent=min(vals),best_tpmc_equivalent=max(vals)))
    with (root/'all_summary.csv').open('w',newline='') as f:
        writer=csv.DictWriter(f,fieldnames=['kind','mode','warehouses','workers','n','strict_accepted_n','qualified_n','median_tpmc_equivalent','min_tpmc_equivalent','best_tpmc_equivalent'])
        writer.writeheader();writer.writerows(all_summaries)
    write(root/'campaign_acceptance.json',dict(accepted=state['phase']=='campaign' and all(t.get('done') for t in state['tasks']),
          requested_runs=len(state['tasks']),completed_logical_runs=sum(bool(t.get('done')) for t in state['tasks']),
          raw_attempts=len(rows),strict_accepted=sum(r['accepted'] for r in rows)))

def preparation_tasks(smoke_rates):
    tasks=[]
    for index,(w,k) in enumerate(POINTS):
        rotation=index%3
        for mode in MODES[rotation:]+MODES[:rotation]:
            scale=(.65 if mode=='pg' else .75) if w==100 else 1
            if w==100 and k!=32:scale*=max(.35,min(1.5,k/32))
            guess=smoke_rates[mode]*scale
            tasks.append(dict(kind='pilot',mode=mode,w=w,workers=k,trial=1,
                              n=math.ceil(guess*510+65536),attempts=[]))
    for w in (5,10,20,30,50,75,100):
        for mode in ('det','merkle'):
            tasks.append(dict(kind='parity',mode=mode,w=w,workers=32,trial=1,n=20000,attempts=[]))
    return tasks

def construct_plan(home,state):
    rates={};setup={};parity=[]
    for t in state['tasks']:
        a=next(a for a in t['attempts'] if a.get('qualified'))
        run=Path(a['path']);r=read(run/'acceptance.json')
        if t['kind']=='pilot':
            m=r['direct_metrics'];assert m['ramp_s']>=300;series=m['throughput_series']
            capacity=m.get('capacity_profile',{})
            estimate=capacity.get('max_business_tps',max(p['total_business_tps'] for p in series))
            mean=capacity.get('mean_business_tps',m['total_business_tps'])
            rates[(t['w'],t['workers'],t['mode'])]=dict(mean=mean,upper=estimate*1.20,path=str(run),
                  fixed_window_business_tps=m['total_business_tps'],basis='complete post-ramp gateway minute bins; sizing/ETA only')
            # Restored images avoid repeated loader/index construction. Pilot
            # observed lifecycle minus gateway duration bounds copy/check work.
            setup[(t['w'],t['workers'],t['mode'])]=max(120,a['elapsed_s']-m['max_finish_ms']/1000)
    for w in (5,10,20,30,50,75,100):
        pair=[t for t in state['tasks'] if t['kind']=='parity' and t['w']==w]
        dirs=[Path(next(a for a in t['attempts'] if a.get('qualified'))['path']) for t in pair]
        ok=len(dirs)==2 and all((dirs[0]/f).read_bytes()==(dirs[1]/f).read_bytes() for f in ('initial_state.hash','state.hash'))
        sql_hashes=[(d/'workload.sha256').read_text().split()[0] for d in dirs]
        ok=ok and len(sql_hashes)==2 and sql_hashes[0]==sql_hashes[1]
        parity.append(dict(warehouses=w,count=20000,same_workload_and_state=ok,paths=list(map(str,dirs))))
    write(home/'standard_parity.json',parity)
    assert all(p['same_workload_and_state'] for p in parity),'fixed-N state parity failed'
    tasks=[]
    for trial in range(1,4):
        for index,(w,k) in enumerate(POINTS):
            rotation=(index+trial)%3
            for mode in MODES[rotation:]+MODES[:rotation]:
                rate=rates[w,k,mode]
                n=math.ceil(rate['upper']*(300+600)*1.15+65536)
                tasks.append(dict(kind='sweep',mode=mode,w=w,workers=k,trial=trial,n=n,
                                  pilot=rate,setup_s=setup[w,k,mode],attempts=[]))
    # One accepted headline per mode; Cl. 5.5.1.4 does not require a separate
    # reproducibility measurement. Failed attempts remain and are retried.
    for mode in MODES:
        rate=rates[100,32,mode];n=math.ceil(rate['upper']*(300+7200)*1.10+65536)
        tasks.append(dict(kind='headline',mode=mode,w=100,workers=32,trial=1,n=n,
                          pilot=rate,setup_s=setup[100,32,mode],attempts=[]))
    hours=sum(t['n']/t['pilot']['mean']+t['setup_s'] for t in tasks)/3600
    preparation_hours=(time.time()-state.get('started_epoch',time.time()))/3600
    plan=dict(preparation_hours=preparation_hours,total_estimated_hours=preparation_hours+hours*1.20,protocol='direct_fixed_v1',tasks=tasks,base_hours=hours,with_noise_allowance_hours=hours*1.20,
              minimum_sweep_runs=108,headline_runs=3,parity=parity,mix_method='shared_23_card_deck_v1',deck_sets=96,
              steady_state=dict(cv_limit=.40,abs_fitted_change_limit=.20,slope_ci_confidence=.95,hac_lag_minutes=5,requires_zero_in_slope_ci=True),
              gateway_timeout_s=dict(sweep=14400,headline=43200),
              label='TPC-C-derived, unaudited',origin='first gateway submit',initial_sweep_order=['det','merkle','pg'],ramp_s=300,sweep_window_s=600,headline_window_s=7200)
    write(home/'standard_plan.json',plan)
    with (home/'standard_plan.csv').open('w',newline='') as f:
        writer=csv.DictWriter(f,fieldnames=['kind','mode','w','workers','trial','n','pilot_mean_business_tps','pilot_upper_business_tps','setup_s'])
        writer.writeheader()
        writer.writerows({k:t[k] for k in ('kind','mode','w','workers','trial','n','setup_s')} |
                        dict(pilot_mean_business_tps=t['pilot']['mean'],pilot_upper_business_tps=t['pilot']['upper']) for t in tasks)
    with (home/'standard_eta.csv').open('w',newline='') as f:
        writer=csv.DictWriter(f,fieldnames=['preparation_hours','base_campaign_hours','noise_allowance_factor','remaining_campaign_hours','total_estimated_hours'])
        writer.writeheader();writer.writerow(dict(preparation_hours=preparation_hours,base_campaign_hours=hours,noise_allowance_factor=1.20,remaining_campaign_hours=hours*1.20,total_estimated_hours=preparation_hours+hours*1.20))
    if preparation_hours+hours*1.20>120:
        (home/'BLOCKED.md').write_text(f'Total ETA including preparation {preparation_hours+hours*1.20:.2f}h exceeds five days. Options: improve pilot-derived sizing margin only after coverage validation; run the two-hour headline first and split sweeps into a separately approved phase; or approve a longer campaign. No publication run launched.\n')
        return False
    return True

def main():
    ap=argparse.ArgumentParser();ap.add_argument('phase',choices=['smoke','smoke_wal','prepare','campaign']);args=ap.parse_args()
    home=Path(os.environ['CAMPAIGN_HOME'])
    # The frozen deck_20261012 campaign (engine 16577072) resumes only from its own checkout.
    tag=os.environ.get('TPCC_CAMPAIGN_TAG','main_20261020')
    assert tag=='main_20261020', 'old campaign may never be resumed'
    control=home/tag;control.mkdir(exist_ok=True)
    root=control/('B_standard_'+args.phase)
    assert home==Path.home()/'claude_checks/tpcc_20261020'
    assert subprocess.check_output(['id','-un'],text=True).strip()=='protectdr'
    root.mkdir(exist_ok=True)
    lock=(home/'paper_campaign.lock').open('w');fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    os.environ['CAMPAIGN_LOCK_HELD']='1'
    os.environ['PREPARED_CACHE']=str(home/'prepared')
    state=read(root/'state.json')
    if state is None:
        if args.phase in ('smoke','smoke_wal'):
            tasks=[dict(kind='smoke',mode=m,w=5,workers=32,trial=1,n=131072 if args.phase=='smoke_wal' else 262144,attempts=[]) for m in MODES]
        elif args.phase=='prepare':
            smoke=read(control/'B_standard_smoke_wal/state.json')
            assert read(control/'B_standard_smoke_wal/status.json')['phase']=='complete'
            smoke_rates={t['mode']:read(Path(next(a for a in t['attempts'] if a.get('qualified'))['path'])/'direct_metrics.json')['total_business_tps'] for t in smoke['tasks']}
            tasks=preparation_tasks(smoke_rates)
            # The former deck pilots (engine 16577072) provide conservative initial
            # counts, not new rates or results. Every task is measured again.
            old=read(Path.home()/'claude_checks/paper_20261010_tpcc/deck_20261012/B_standard_prepare/state.json',{})
            guesses={(t['w'],t['workers'],t['mode']):t['n'] for t in old.get('tasks',[]) if t['kind']=='pilot'}
            for t in tasks:
                if t['kind']=='pilot':t['n']=max(t['n'],guesses.get((t['w'],t['workers'],t['mode']),0))
        else:
            plan=read(control/'standard_plan.json');assert plan['protocol']=='direct_fixed_v1' and plan['total_estimated_hours']<=120
            assert plan['mix_method']=='shared_23_card_deck_v1'
            assert read(control/'B_standard_prepare/status.json')['phase']=='complete'
            tasks=plan['tasks']
        started=time.time()
        if args.phase=='prepare':
            started=read(control/'PREPARATION_STARTED.json')['epoch']
        state=dict(phase=args.phase,tasks=tasks,started_epoch=started,pilot_ramp_s=300);write(root/'state.json',state)
    if args.phase=='prepare' and state.get('pilot_ramp_s')!=300:
        # Keep early short calibration attempts, but do not select them for
        # matched sizing. Upgrade unused counts once, preserving per-mode guesses.
        for t in state['tasks']:
            if t['kind']!='pilot':continue
            t['n']=math.ceil((t['n']-65536)*510/210+65536)
            for a in t['attempts']:
                m=read(Path(a['path'])/'direct_metrics.json',{})
                if m and m.get('ramp_s',0)<300:
                    a.update(qualified=False,preliminary_calibration=True,
                             retry_reason='short preliminary calibration retained; matched pilot requires five-minute ramp')
                    t.pop('done',None)
        state['pilot_ramp_s']=300;write(root/'state.json',state)
    recover(root)
    for index,t in enumerate(state['tasks']):
        if t.get('done'):continue
        while not t.get('done'):
            matched=[a for a in t['attempts'] if not a.get('preliminary_calibration') and not a.get('retained_setup_failure') and not a.get('interrupted_before_start')]
            if len(matched)>=3 and t['kind']!='headline' and matched[-1].get('processed'):
                status(root,'blocked',reason='three attempts exhausted',task=t);return 2
            if shutil.disk_usage(home).free/2**30<160:
                status(root,'blocked',reason='less than 160GiB free for live database/WAL');return 2
            pending=t['attempts'][-1] if t['attempts'] and not t['attempts'][-1].get('processed') else None
            number=pending['number'] if pending else len(t['attempts'])+1;kind=t['kind']
            run=root/f'{t["mode"]}_w{t["w"]}_n{t["n"]}_workers{t["workers"]}_trial{t["trial"]}_attempt{number}'
            # Headline and sweep W100/32 need separate namespaces.
            runroot=root/('B_'+kind)
            runroot.mkdir(exist_ok=True)
            run=runroot/run.name
            if pending:
                a=pending;run=Path(a['path'])
                a['elapsed_s']=time.time()-a.get('started_epoch',time.time())
                if not run.exists():
                    a.update(processed=True,interrupted_before_start=True)
                    write(root/'state.json',state)
                    continue
            else:
                a=dict(number=number,n=t['n'],path=str(run),started_epoch=time.time())
                t['attempts'].append(a);write(root/'state.json',state)
                cfg=dict(smoke=(2,30,1),pilot=(300,60,1),parity=(2,5,1),sweep=(300,600,0),headline=(300,7200,0))[kind]
                env=dict(os.environ,RUNROOT=str(runroot),ATTEMPT_SUFFIX=f'_attempt{number}',RUN_KIND=kind,
                         WARMUP_S=str(cfg[0]),MIN_WINDOW_S=str(cfg[1]),SMOKE=str(cfg[2]),
                         MEASUREMENT_METHOD='legacy_parity' if kind=='parity' else 'direct_fixed',
                         SHARED_BUFFERS='32GB',CHECKPOINT_TIMEOUT='5min',DET_WINDOW='1024',DET_PIPELINE_DEPTH='1024',
                         GATEWAY_TIMEOUT_S='43200' if kind=='headline' else os.environ.get('GATEWAY_TIMEOUT_S','14400'))
                status(root,'running',task_index=index+1,total_tasks=len(state['tasks']),kind=kind,mode=t['mode'],warehouses=t['w'],workers=t['workers'],trial=t['trial'],attempt=number,run=str(run))
                start=time.monotonic()
                with (runroot/(run.name+'.driver.log')).open('w') as log:
                    child=subprocess.Popen(['bash',str(HERE/'run.sh'),t['mode'],str(t['w']),str(t['n']),str(t['workers']),str(t['trial'])],env=env,stdout=log,stderr=subprocess.STDOUT)
                    write(root/'active.json',dict(pid=child.pid,process_start=process_start(child.pid),run=str(run)))
                    a['rc']=child.wait()
                a['elapsed_s']=time.monotonic()-start;(root/'active.json').unlink(missing_ok=True)
            r=read(run/'acceptance.json')
            if not r:status(root,'blocked',reason='missing acceptance evidence',run=str(run));return 2
            retire_database(run);compress_bulk(run)
            if kind=='headline':
                subprocess.run([os.environ.get('PLOT_PYTHON','python3'),str(HERE/'plot_headline.py'),str(run)],check=True)
            a['processed']=True
            failed=set(r['acceptance']['failed_checks'])
            if (run/'interrupted.json').exists():
                a['retry_reason']='interrupted attempt retained; resume with fresh database'
                write(root/'state.json',state);report(root,state);continue
            if kind in ('pilot','parity','smoke'):failed-={'window_300s','time_driven_checkpoint'}
            # WAL range/stat checks cannot pass when the measurement window itself was not reached
            # (wal_collection.json covered=false); treat them as part of the coverage retry, not correctness.
            if 'direct_window_fully_covered' in failed and read(run/'wal_collection.json',{}).get('covered') is False:
                a['wal_checks_consequential_to_coverage']=sorted(failed&{'wal_stats','wal_incremental_ranges'})
                failed-={'wal_stats','wal_incremental_ranges'}
            if kind=='pilot' and read(run/'config.json',{}).get('warmup_s',0)<300:
                failed.add('pilot_matched_ramp')
                a['preliminary_calibration']=True
            a['driver_failed_checks']=sorted(failed)
            if failed-ENV_FAILURES:
                write(root/'state.json',state)
                report(root,state);status(root,'blocked',reason='correctness/settings/lifecycle gate',failed_checks=sorted(failed),run=str(run));
                if kind=='headline':
                    a['retry_reason']='headline acceptance failure retained; automatic extra attempt'
                    write(root/'state.json',state);continue
                return 2
            stalled=r.get('window',{}).get('longest_stall_s',0)>=10 or 'sampler_quality' in failed
            if not failed and (kind=='parity' or not stalled):
                a['qualified']=True;t['done']=True
            elif 'pilot_matched_ramp' in failed:
                a['retry_reason']='short preliminary calibration retained; matched pilot requires five-minute ramp'
            elif {'direct_window_fully_covered','window_available','configured_window'}&failed:
                a['retry_reason']='window coverage insufficient; retain and resize this mode N from observed span, at least 50 percent larger'
                metrics=read(run/'direct_metrics.json',{})
                cfg=read(run/'config.json')
                t['n']=max(t['n'],coverage_retry_n(a['n'],metrics,cfg['warmup_s'],cfg['min_window_s']))
            else:a['retry_reason']='predeclared stability/stall/environment rule; retain raw extra attempt'
            write(root/'state.json',state);report(root,state)
            if args.phase=='campaign' and a.get('qualified') and not (root/'FIRST_REAL_ACCEPTED.json').exists():
                write(root/'FIRST_REAL_ACCEPTED.json',dict(run=str(run),metrics=r.get('direct_metrics'),acceptance=r['acceptance']))
                status(root,'first_run_handoff',run=str(run),seconds=90);time.sleep(90)
    if args.phase=='prepare':
        try:planned=construct_plan(control,state)
        except AssertionError as error:
            (control/'BLOCKED.md').write_text('Preparation plan correctness check failed: '+str(error)+'. See standard_parity.json and retained raw evidence. Frozen engine source must remain unchanged; no publication run launched.\n')
            status(root,'blocked',reason='preparation plan correctness check',detail=str(error));return 2
        if not planned:
            status(root,'blocked',reason='ETA exceeds five days; see BLOCKED.md');return 2
    status(root,'complete',phase_completed=args.phase);return 0

if __name__=='__main__':raise SystemExit(main())
