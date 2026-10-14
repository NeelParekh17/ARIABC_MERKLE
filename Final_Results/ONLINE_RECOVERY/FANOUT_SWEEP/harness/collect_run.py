#!/usr/bin/env python3
"""Strict raw-evidence acceptance, including injected recovery and all nodes."""
import csv,importlib.util,json,re,statistics,sys
from pathlib import Path
B=Path('/home/neel/ariabc_data/fanout_sweep_20261014')
spec=importlib.util.spec_from_file_location('comparison',B/'repo/scripts/distributed/recovery_s1024/compare_recovery.py')
cmp=importlib.util.module_from_spec(spec);spec.loader.exec_module(cmp)
out=Path(sys.argv[1])
r=cmp.extract(out)
summary=cmp.env(out/'run_summary.env');meta=cmp.env(out/'run_meta.env');case=cmp.env(out/'recovery_s1024.env')
fails=[]
rows=int(case['rows'])
fanout=int(case['fanout'])
for k,v in {'workload_transactions':'160000','client_quorum_complete_count':'160000','all3_audit_valid':'yes',
            'divergence_count':'0','permanent_failures':'0','recovery_failure_count':'0',
            'async_all3_failure_count':'0','async_all3_timeout_count':'0','async_all3_missing_count':'0'}.items():
    if summary.get(k)!=v:fails.append(k+'='+str(summary.get(k)))
if sum(int(summary.get(k,'-1000000')) for k in ('async_all3_verified_count','async_all3_recovery_attributed_count'))!=160000:
    fails.append('all-three verified plus recovery-attributed count')
if cmp.env(out/'gateway_exit.env').get('gateway_exit_code')!='0':fails.append('gateway exit missing/nonzero')
if case.get('exit_code')!='0' or case.get('collection_exit_code')!='0':fails.append('runner/collection exit')
if meta.get('BINARY_PROVENANCE_PASS')!='1':fails.append('binary provenance')
if meta.get('git_head')!='9cf6c70a39e9bf848c93c2c24bdb55181f4b3356':fails.append('git HEAD')
if r['phase8_merkle_pass']!='PASS':fails.append('final all-three audit')
final={}
for node in ('1_admin123','2_user4','4_utkarsh'):
    parts=cmp.read_text(out/('post_verify_readback_node'+node+'.out')).strip().split('|')
    final[node]={'raw':parts}
    if len(parts)!=4 or parts[0]!=str(rows+1) or parts[2]!='t':fails.append(node+' final count/verify')
events=[cmp.kv(line) for line in cmp.read_text(out/'gateway_test.log').splitlines() if line.startswith('RECOVERY_EVENT ')]
fault=case['scenario'] not in ('A','B')
if fault:
    timeline=cmp.env(out/'fault_timeline.env')
    if timeline.get('fault_inject_rc')!='0':fails.append('fault injection unsuccessful/missing')
    try:
        import datetime as _dt
        _ph=cmp.env(out/'phase_markers.env')
        _wf=_dt.datetime.fromisoformat(_ph['PHASE_6_WORKLOAD_FINISHED']).timestamp()*1000
        if int(timeline.get('fault_inject_done_epoch_ms','0'))>_wf:fails.append('fault landed after workload finished')
    except (KeyError,ValueError):fails.append('fault timing unavailable')
    target='1' if case['scenario'] in ('L','L_mix') else '4'
    if not events or any(e.get('result')!='PASS' or e.get('live')!='1' or e.get('node')!=target for e in events):
        fails.append('successful live recovery event missing/wrong target')
    if int(summary.get('recovery_success_count','-1'))!=len(events):fails.append('recovery success/event count')
    if case['scenario']=='M_passive' and (not events or any(e.get('reason')!='merkle_compare' for e in events)):
        fails.append('passive detection was not merkle_compare')
    for key in cmp.EVENT_FIELDS:
        if r[key]=='UNKNOWN':fails.append('missing metric '+key)
    if r['candidate_rows']=='UNKNOWN':fails.append('missing fetched rows')
else:
    if events or summary.get('recovery_triggered_count')!='0':fails.append('unexpected no-fault recovery')
for key in ('empty_100ms_buckets','max_completion_gap_ms','tps_majority_visible'):
    if r[key]=='UNKNOWN':fails.append('missing metric '+key)
for host in ('10.129.148.247','10.129.148.246','10.129.148.248'):
    geom=cmp.read_text(out/('geometry_'+host+'.txt'))
    if not re.search(r'5448\s*\|\s*serializable\s*\|\s*on',geom):fails.append(host+' isolation/direct Merkle')
    for opt,v in [('partitions','200'),('fanout',str(fanout)),('split_threshold','1024'),('merge_threshold','256')]:
        if opt+"='"+v+"'" not in geom:fails.append(host+' geometry '+opt)
    for setting in ('fsync','synchronous_commit','full_page_writes'):
        if not re.search(r'\n\s*'+setting+r'\s*\n[- ]+\n\s*on\s*\n',geom):fails.append(host+' '+setting)
for node in ('1_admin123','2_user4','4_utkarsh'):
    profile=cmp.read_text(out/('server_node'+node+'.log'))
    if 'PROFILE_RAFT_STORAGE ' not in profile:fails.append(node+' missing Raft storage profile')
if fault and len(events)!=1:fails.append('expected exactly one controlled recovery event')
if str(r['full_copies'])!='0':fails.append('full-table copy fallback')
if rows > 12000:
    for host in ('10.129.148.247','10.129.148.246','10.129.148.248'):
        g=cmp.read_text(out/('geometry_'+host+'.txt'))
        if not re.search(r'\n\s*0\s*\|\s*f\s*\|',g):fails.append(host+' no internal tree level')
samples=[]
for line in cmp.read_text(out/'health/samples.jsonl').splitlines():
    try:samples.append(json.loads(line))
    except ValueError:fails.append('invalid health sample')
timeline=cmp.env(out/'fault_timeline.env')
start=int(timeline.get('gateway_start_epoch_ms','0'))
end=start+int(summary.get('all3_audit_drained_ms','0'))
noise=[];health={}
for host in ('10.129.148.247','10.129.148.246','10.129.148.248'):
    vals=[s for s in samples if s.get('ip')==host and start<=s['epoch_ms']<=end and 'error' not in s]
    if len(vals)<2:fails.append(host+' insufficient measured health samples')
    if not vals:continue
    swapped=sum(max(0,vals[-1][k]-vals[0][k]) for k in ('pswpin','pswpout'))*vals[0]['page_bytes']
    health[host]={'samples':len(vals),'min_mem_available_kb':min(v['mem_available_kb'] for v in vals),
                  'min_mem_free_kb':min(v['mem_free_kb'] for v in vals),'swap_io_bytes':swapped,
                  'max_lag_txids':max((v['lag_txids'] for v in vals if v.get('lag_txids') is not None),default=None),
                  'max_load1':max(v['load'][0] for v in vals)}
    if swapped>64*1024*1024:noise.append(host+' more than 64 MiB swap IO during gateway interval')
    if len({v['boot_id'] for v in vals})!=1:fails.append(host+' reboot during run')
    if host.endswith('.247') and any(v['clocksource']!='tsc' for v in vals):fails.append('gateway clocksource')
det={k:{'samples':0,'sum':None,'max':None} for k in ('dt_opf','dt_opf_stale_error','dt_rel_read_tags')}
for p in out.glob('bcdb_phase_trace_node*'):
    if not p.is_file():continue
    try:
        with p.open() as f:
            for row in csv.DictReader(f):
                for k in det:
                    if row.get(k) and row[k].isdigit():
                        v=int(row[k]);d=det[k];d['samples']+=1;d['sum']=(d['sum'] or 0)+v;d['max']=max(d['max'] or 0,v)
    except (ValueError,csv.Error):pass
if case['scenario']=='C_deep':
    for k,d in det.items():
        if not d['samples']:fails.append('deep telemetry missing '+k)
r.update({'rows':rows,'fanout':fanout,'stage':case['stage'],'rep':int(case['rep']),'attempt':int(case['attempt']),
          'quorum_completions':summary.get('client_quorum_complete_count'),
          'raw_divergence_count':summary.get('raw_divergence_count'),
          'gateway_exit_code':cmp.env(out/'gateway_exit.env').get('gateway_exit_code'),
          'final_all_three':final,'events':events,'health':health,'det_counters':det,
          'failures':fails,'noise':noise,'status':'REJECTED' if fails else 'NOISE_EXCLUDED' if noise else 'ACCEPTED'})
(out/'acceptance.json').write_text(json.dumps(r,indent=2)+'\n')
print(json.dumps({k:r[k] for k in ('run_id','scenario','status','failures','noise','tps_majority_visible')},indent=2))
sys.exit(2 if fails else 3 if noise else 0)
