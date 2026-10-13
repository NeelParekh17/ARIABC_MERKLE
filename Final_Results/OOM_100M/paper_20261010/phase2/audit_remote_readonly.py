#!/usr/bin/env python3
"""Execute through SSH stdin, read only, emit one JSON artifact to stdout."""
import collections,csv,datetime,hashlib,io,json,math,re,statistics,sys
from pathlib import Path
root=Path('/home/neel/claude_checks/paper_20261010_oom/results');repo=root.parent/'repo'
sys.dont_write_bytecode=True;sys.path.insert(0,str(repo/'scripts/distributed'))
from benchmark_validation import parse_gateway_result
from benchmark_contract import validate_ycsb_results,validate_settings

def digest(p):
 h=hashlib.sha256()
 with p.open('rb') as f:
  for b in iter(lambda:f.read(1048576),b''): h.update(b)
 return h.hexdigest()
def check(ok,message,issues):
 if not ok: issues.append(message)
def pressure(o,key):
 for l in (o.get(key) or '').splitlines():
  if l.startswith('full '): return int(dict(p.split('=') for p in l.split()[1:])['total'])
def telemetry(p):
 samples=[];findings=[]
 for l in p.read_text().splitlines():
  try: o=json.loads(l)
  except ValueError: findings.append('invalid telemetry');continue
  if 'error' in o: findings.append('telemetry error')
  elif 'monotonic_ns' in o:samples.append(o)
 gaps=[(b['monotonic_ns']-a['monotonic_ns'])/1e9 for a,b in zip(samples,samples[1:])]
 if any(x>=10 for x in gaps):findings.append('sampler gap >=10s')
 psi={k:[] for k in ('pressure/io','pressure/memory')}
 for i,a in enumerate(samples):
  b=next((x for x in samples[i+1:] if (x['monotonic_ns']-a['monotonic_ns'])/1e9>=10),None)
  if b is None:continue
  elapsed=(b['monotonic_ns']-a['monotonic_ns'])/1e9
  for k,bound in [('pressure/io',.95),('pressure/memory',.5)]:
   lo,hi=pressure(a,k),pressure(b,k)
   if lo is not None and hi is not None:
    frac=(hi-lo)/(elapsed*1e6);psi[k].append(frac)
    if frac>=bound:findings.append(k+' full pressure')
 def mem(k):return [int(m[1]) for o in samples if (m:=re.search(r'^'+k+r':\s+(\d+)',o.get('meminfo',''),re.M))]
 loads=[float(o['loadavg'].split()[0]) for o in samples if o.get('loadavg')]
 return dict(samples=len(samples),duration_s=(samples[-1]['monotonic_ns']-samples[0]['monotonic_ns'])/1e9 if len(samples)>1 else 0,max_gap_s=max(gaps,default=0),findings=findings,clocksource=sorted({o.get('clocksource','missing') for o in samples}),boot_ids=sorted({o.get('boot_id','missing') for o in samples}),kernels=sorted({o.get('kernel','missing') for o in samples}),max_io_full_fraction=max(psi['pressure/io'],default=None),max_memory_full_fraction=max(psi['pressure/memory'],default=None),min_mem_available_kib=min(mem('MemAvailable'),default=None),min_swap_free_kib=min(mem('SwapFree'),default=None),max_load1=max(loads,default=None))
expected=json.loads((root/'provenance/published_workloads.json').read_text());planned=list(csv.DictReader((root/'provenance/planned_case_order.csv').open()))
key=lambda r:(r['workload'],float(r['skew']),r['mode'],int(r['workers']),int(r['trial']))
known={}
for name in ('cpp_binaries.sha256','postgres.sha256'):
 for l in (root/'provenance'/name).read_text().splitlines():
  a,b=l.split(maxsplit=1);known[Path(b).name]=a
report=dict(time_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),head=(root/'provenance/git_head.txt').read_text().strip(),datasets={},raw_hashes={},provenance_issues=[])
check(report['head']=='16577072ecf888fbbbb5d80d0e90fc340c8ca342','HEAD mismatch',report['provenance_issues'])
for group,count in [('smoke',3),('campaign',252)]:
 run=next((root/group).glob('run_*')); rows=list(csv.DictReader((run/'summary.csv').open()));args=json.loads((run/'campaign.json').read_text())['arguments'];issues=[];records=[]
 check(len(rows)==count,'summary count mismatch',issues);check(len(set(map(key,rows)))==count,'duplicate configurations',issues)
 if group=='campaign':check(list(map(key,rows))==list(map(key,planned)),'planned order mismatch',issues)
 check((args['db_port'],args['server_port'],args['raft_port'])==(5468,8068,9068),'port mismatch',issues)
 if group=='campaign':check(args['trials']==3 and args['auto_resume'],'trials/auto_resume mismatch',issues)
 cal=json.loads(next(run.glob('device_calibration_*.json')).read_text()); thresholds=cal['thresholds']
 check(cal['probe']['settled'] and cal['probe']['wait_s']>=300,'calibration too short',issues)
 for k,metric,add in [('qd1_us','idle_qd1_median_us',30),('qd8_us','idle_qd8_median_us',30),('wr_us','idle_wr_median_us',50)]:check(math.isclose(thresholds[k],max(cal[metric]*1.25,cal[metric]+add)),'calibration threshold mismatch',issues)
 for row in rows:
  case=Path(row['artifact_dir']); ci=[];rec=dict(case=case.name,configuration=key(row),issues=ci)
  try:
   saved=json.loads((case/'result.json').read_text());setup=json.loads((case/'setup.json').read_text());mode=row['mode'];logs={n:(case/n).read_text(errors='replace') for n in ('gateway.log','server.log','server.err.log','postgres.log')}
   check(not any((case/n).exists() for n in ('FAILED.txt','cleanup_errors.txt','NOISE_EXCLUDED.txt')),'rejection marker present',ci)
   g=parse_gateway_result(logs['gateway.log'],20000,saved['gateway']['gateway_returncode'],mode=mode);terminal=validate_ycsb_results(logs['server.log'],20000,mode=mode)
   validate_settings(setup['settings'],'32MB',int(row['workers']),mode)
   for k,v in dict(default_transaction_isolation='serializable',merkle_apply_synchronous_direct='on',bcdb_serial_gate_source='0',bcdb_ledger_trace='off').items():check(setup['settings'].get(k)==v,'setting '+k,ci)
   check(row['isolation']=='serializable' and row['shared_buffers']=='32MB' and int(row['db_rows'])==100000000,'row setup mismatch',ci)
   check(setup['keyspace']=='100000000|1|100000000','keyspace mismatch',ci)
   for k in ('total_queries','validated_completed_queries','wall_time_ms','wall_including_drains_ms','divergence_count','permanent_failures'):check(int(row[k])==g[k],'gateway vs CSV '+k,ci)
   check(math.isclose(float(row['tps']),g['tps'],rel_tol=1e-12),'TPS vs raw interval',ci)
   for k,v in saved['row'].items():
    if k in row:
     if v is None:check(row[k]=='','CSV null mismatch '+k,ci)
     elif isinstance(v,(float,int)) and not isinstance(v,bool):check(math.isclose(float(row[k]),v,rel_tol=1e-12,abs_tol=1e-9),'JSON vs CSV '+k,ci)
     else:check(str(v)==row[k],'JSON vs CSV '+k,ci)
   check(saved['gateway']['validated_row_results']==terminal['validated_row_results'] and saved['gateway']['empty_results']==terminal['empty_results'],'saved terminal counts mismatch',ci)
   name=f"ycsb_{row['workload']}_skew_{float(row['skew'])}_20000.sql";check(row['workload_sha256']==expected[name]['sha256']==digest(run/'workloads'/name),'workload hash mismatch',ci)
   wp=Path(saved['gateway']['command'][saved['gateway']['command'].index('--queryFrom')+1]);check(digest(wp)==expected[name]['sha256'],'executed gateway workload hash mismatch',ci)
   for b in setup['provenance']['executables']:check(b['sha256']==known[Path(b['path']).name],'binary provenance mismatch',ci)
   check(len(setup['provenance']['executables'])==3,'missing executable provenance',ci)
   check(saved['gateway']['command'][0].endswith('/ariabc_pg_gateway') and setup['server']['command'][0].endswith('/ariabc_pg_server'),'wrong commands',ci)
   if mode=='bcdb_merkle':
    check(row['merkle_verify']=='PASS' and (case/'merkle_verify.txt').read_text().strip()=='t','Merkle verify not PASS',ci)
    for k,v in dict(partitions=200,fanout=32,split_threshold=1024,merge_threshold=256).items():check(setup['merkle_stats'][k]==v,'Merkle geometry '+k,ci)
   retry=dict(re.findall(r'\b(retry_attempts_total|retry_backoff_requested_ms|retry_exhausted_total|retry_jitter)=(\d+)',logs['server.log']))
   if mode=='pg':check(retry.get('retry_jitter')=='1' and row['pg_retry_jitter']=='on','PG jitter missing',ci)
   for k,l in [('retry_attempts','retry_attempts_total'),('retry_backoff_requested_ms','retry_backoff_requested_ms'),('retry_exhausted','retry_exhausted_total')]:check(int(row[k])==int(retry.get(l,0)),'retry count mismatch '+k,ci)
   check(int(row['retry_exhausted'])==0,'exhausted retries',ci)
   all_logs='\n'.join(logs[n] for n in ('server.log','server.err.log','postgres.log'));dt={};stored_dt=json.loads((case/'det_counters.json').read_text())
   for k in ('dt_opf','dt_opf_stale_error','dt_rel_read_tags'):
    vals=list(map(int,re.findall(r'\b'+k+r'=(\d+)',all_logs)));dt[k]=dict(samples=vals,max_observed=max(vals) if vals else None);check(dt[k]==stored_dt[k],'det counter mismatch '+k,ci)
   nq=json.loads((case/'noise_qualification.json').read_text());tel={n:telemetry(case/n) for n in ('telemetry.jsonl','gateway_telemetry.jsonl')};excluded=any(t['findings'] for t in tel.values())
   check(nq['excluded']==excluded,'noise qualification mismatch',ci);check(not excluded and not nq['findings'] and row['noise_excluded']=='False','accepted noise',ci)
   check(tel['gateway_telemetry.jsonl']['clocksource']==['tsc'],'gateway clocksource not tsc',ci)
   check(all(t['samples']>=1 and len(t['boot_ids'])==1 for t in tel.values()),'missing telemetry/reboot',ci)
   settle=setup['settle'];history=settle['history'];check(settle['settled'] and 20<=settle['wait_s']<=1800,'settle wait invalid',ci)
   for e in history[-3:]:
    check(e['write_mib']<=2,'settle idle writes above limit',ci)
    for m,k in [('qd1','qd1_us'),('qd8','qd8_us'),('wr','wr_us')]:check(e[m]['median_us']<=round(thresholds[k],1),'settle '+m+' over threshold',ci)
   check('CACHES_DROPPED' in setup['cache_drop_output'],'cache drop evidence missing',ci)
   rec.update(tps=g['tps'],terminal_results=terminal,retries=retry,det_counters=dt,telemetry=tel,settle_wait_s=settle['wait_s'],settle_last3=history[-3:],reset_content_checked='CONTENT_CHECK' in setup['reset_output'],merkle_verify=row['merkle_verify'],accepted_unix_ns=(case/'result.json').stat().st_mtime_ns,reset_time_ms=float(row['reset_time_ms']),postgres_error_lines=sum(bool(re.search(r'\b(?:ERROR|FATAL|PANIC):',l)) for l in logs['postgres.log'].splitlines()))
   for p in case.iterdir():
    if p.is_file():report['raw_hashes'][str(p)]=dict(sha256=digest(p),bytes=p.stat().st_size)
  except Exception as e:ci.append(type(e).__name__+': '+str(e))
  records.append(rec)
 markers=[str(p) for n in ('FAILED.txt','NOISE_EXCLUDED.txt','cleanup_errors.txt') for p in run.glob('*/'+n)]
 check(not markers,'rejection markers exist',issues)
 report['datasets'][group]=dict(run_dir=str(run),summary_sha256=digest(run/'summary.csv'),rows=len(rows),issues=issues,rejection_markers=markers,records=records)
report['harness_hash_checks']=[]
for l in (root/'provenance/harness.sha256').read_text().splitlines():
 sha,name=l.split(maxsplit=1);p=repo/name
 report['harness_hash_checks'].append(dict(path=str(p),expected=sha,actual=digest(p),match=sha==digest(p)))
print(json.dumps(report))
