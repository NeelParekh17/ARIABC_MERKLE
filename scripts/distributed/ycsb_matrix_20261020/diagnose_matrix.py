"""Read-only Phase 1b extension of check_results.py. Never imports/runs the harness.
Run `sampler` on DB host to summarize existing JSONL, `artifacts` on controller.
Only artifacts mode writes, exclusively beneath evidence/phase1b.
"""
import argparse, collections, datetime, json, re, statistics, time
from pathlib import Path
ROOT=Path('/home/neel/claude_ctl/ycsb_matrix_20261020')
def lsn(s):
 a,b=s.split('/'); return (int(a,16)<<32)+int(b,16)
def kv(s):
 return {p[0].rstrip(':'):float(p[1]) for x in s.splitlines() if len(p:=x.split())>1 and re.fullmatch(r'-?\d+(\.\d+)?',p[1])}
def sampler():
 groups={}; bad=0
 with Path('/home/neel/claude_checks/ycsb_matrix_20261020/evidence/wal_sampler.jsonl').open() as f:
  for line in f:
   try:r=json.loads(line)
   except json.JSONDecodeError:bad+=1;continue
   c=r.get('case_dir',''); phase=r.get('phase','unknown');t=r['t']
   g=groups.setdefault(c,dict(first=t,last=t,phases={},measurement_errors=0,valid=0,stats_samples=0,merges=[0,0],bins={},first_sample=None,last_sample=None))
   g['last']=t;p=g['phases'].setdefault(phase,dict(first=t,last=t,count=0,errors=0));p['last']=t;p['count']+=1;p['errors']+=int('error'in r)
   if r.get('stats'):
    g['stats_samples']+=1
    for i,v in enumerate(r['stats'].get('merges',[])[:2]):g['merges'][i]=max(g['merges'][i],v)
    g['last_stats']=r['stats']
   if phase!='measurement':continue
   if 'error'in r:g['measurement_errors']+=1;continue
   g['valid']+=1
   point={k:r[k] for k in ('t','commits','lsn','postmaster_start') if k in r}
   if g['first_sample'] is None:g['first_sample']=point
   g['last_sample']=point
   b=str(int((t-g['first_sample']['t'])//60));q=g['bins'].setdefault(b,dict(first=point,last=point,n=0));q['last']=point;q['n']+=1
 for g in groups.values():
  for b in g['bins'].values():
   a,z=b['first'],b['last'];dt=z['t']-a['t'];same=a.get('postmaster_start')==z.get('postmaster_start')
   b['commit_s']=(z['commits']-a['commits'])/dt if dt>0 and same else None
   b['wal_mib_s']=(lsn(z['lsn'])-lsn(a['lsn']))/dt/1048576 if dt>0 and same else None
 print(json.dumps(dict(snapshot=time.time(),malformed_lines=bad,cases=groups)))
def artifacts():
 out=ROOT/'evidence/phase1b';out.mkdir(exist_ok=True)
 sample=json.loads((out/'sampler_summary.json').read_text());rows=[];errors=[];hashes=collections.defaultdict(set); manifests=collections.defaultdict(set);exes=collections.defaultdict(set);campaigns=[]
 for cf in sorted((ROOT/'results/matrix').glob('*_*/run_*/campaign.json')):
  cd=json.loads(cf.read_text());camp=cf.parent; kind=camp.parent.name.split('_',1)[1]
  campaigns.append(dict(name=camp.parent.name,path=str(camp),timings=[json.loads(t.read_text()) for t in camp.glob('campaign_setup_timing_*.json')],planned=cd['workloads']))
  for rf in camp.glob('*/result.json'):
   case=rf.parent;res=json.loads(rf.read_text());r=res['row'];s=json.loads((case/'setup.json').read_text());io=json.loads((case/'io.json').read_text());w=json.loads((case/'wal.json').read_text());pc=json.loads((case/'wal_post_checkpoint.json').read_text());g=sample['cases'].get(str(case),{})
   r.update(kind=kind,case=str(case),result_mtime=rf.stat().st_mtime,sampler=g)
   r['start']=g.get('first',rf.stat().st_mtime-r['reset_time_ms']/1000-r['wall_time_ms']/1000)
   r['elapsed_to_result_s']=rf.stat().st_mtime-r['start'];r['residual_s']=r['elapsed_to_result_s']-r['reset_time_ms']/1000-r['wall_time_ms']/1000-r['checkpoint_ms']/1000-r['lsm_drain_ms']/1000
   r['reset_ex_settle_s']=r['reset_time_ms']/1000-r['settle_wait_s']; r['wal_full_b_stmt']=(lsn(pc['lsn'])-lsn(w['before']['lsn']))/2400000
   bef,aft=io['device_before'],io['device_after'];r['write_await_ms']=(aft['write_ms']-bef['write_ms'])/max(1,aft['write_ios']-bef['write_ios']);r['read_iops']=r['device_read_ios']/(r['device_window_ms']/1000);r['write_iops']=r['device_write_ios']/(r['device_window_ms']/1000)
   r['continuous_write_mib']=io['continuous_write_mib'];r['checkpoints_run']=json.loads((case/'checkpoint.json').read_text())['pg_before']['checkpoints_req']-io['pg_before']['checkpoints_req']
   r['gateway_start']=None; r['gateway_end']=None
   # Telemetry is already on controller. Stream once; keep compact per-minute sums.
   ts=[];bins={};mins={};maxs={};firstvm=None;lastvm=None;firstcpu=None;lastcpu=None;prev=None;psum={}
   for line in (case/'telemetry.jsonl').open():
    try:t=json.loads(line)
    except json.JSONDecodeError:errors.append(str(case)+': malformed telemetry');continue
    if 'unix_ns'not in t:continue
    epoch=t['unix_ns']/1e9;ts.append(epoch);mem=kv(t.get('meminfo',''));vm=kv(t.get('vmstat',''))
    if firstvm is None:firstvm=vm
    lastvm=vm
    for k in ('MemAvailable','Dirty','Writeback','SwapFree','Cached','Shmem'):
     v=mem.get(k,0);mins[k]=min(mins.get(k,v),v);maxs[k]=max(maxs.get(k,v),v)
    for k in ('pressure/io','pressure/memory','pressure/cpu'):
     m=re.search(r'full .*total=(\d+)',t.get(k,''))
     if m:
      a=psum.setdefault(k,[int(m[1]),int(m[1])]);a[1]=int(m[1])
    cpu=[int(x) for x in t.get('stat','cpu 0 0 0 0 0').splitlines()[0].split()[1:9]]
    if firstcpu is None:firstcpu=cpu
    lastcpu=cpu
    ds=next((x.split()[3:] for x in t.get('diskstats','').splitlines() if x.split()[2:3]==['nvme0n1p2']),None)
    if ds:
     ds=list(map(int,ds));b=bins.setdefault(str(int((epoch-ts[0])//60)),dict(first_t=epoch,last_t=epoch,first=ds,last=ds,dirty_max=0,available_min=mem.get('MemAvailable',0)))
     b['last_t']=epoch;b['last']=ds;b['dirty_max']=max(b['dirty_max'],mem.get('Dirty',0));b['available_min']=min(b['available_min'],mem.get('MemAvailable',0))
   duration=ts[-1]-ts[0] if len(ts)>1 else 1
   for b in bins.values():
    dt=b['last_t']-b['first_t'];a,z=b['first'],b['last'];b['read_await_ms']=(z[3]-a[3])/max(1,z[0]-a[0]);b['write_await_ms']=(z[7]-a[7])/max(1,z[4]-a[4]);b['read_mib_s']=(z[2]-a[2])/2048/max(1,dt);b['write_mib_s']=(z[6]-a[6])/2048/max(1,dt)
   r['telemetry']=dict(n=len(ts),first=ts[0]if ts else None,last=ts[-1]if ts else None,mins_kib=mins,maxs_kib=maxs,bins=bins,vm_delta={k:lastvm.get(k,0)-firstvm.get(k,0) for k in ('pswpin','pswpout','oom_kill','pgscan_direct','allocstall_normal','pgmajfault','nr_throttled_written')}if firstvm else {},pressure_full_pct={k:(v[1]-v[0])/1e6/duration*100 for k,v in psum.items()})
   if firstcpu and lastcpu:
    delta=[z-a for a,z in zip(firstcpu,lastcpu)];r['telemetry']['cpu_iowait_pct']=delta[4]/max(1,sum(delta))*100;r['telemetry']['cpu_busy_pct']=(sum(delta)-delta[3]-delta[4])/max(1,sum(delta))*100
   gw=(case/'gateway.log').read_text();r['signatures']={k:int(v) for k,v in re.findall(r'\b(tx_signatures_signed|tx_signatures_verified|tx_signature_mismatches)=(\d+)',gw)}
   direct=re.search(r'direct_terminal_success_count=(\d+)',gw);r['direct_success']=int(direct[1]) if direct else None
   with (case/'server.log').open('rb') as f:
    f.seek(max(0,(case/'server.log').stat().st_size-65536));tail=f.read().decode(errors='replace')
   prof=next((l for l in tail.splitlines() if l.startswith('PROFILE_SERVER ')),'')
   r['profile']={k:float(v) for k,v in re.findall(r'\b(exec_calls|exec_ms|pg_query_ms|retryable_sqlstate_40001|retryable_sqlstate_40P01|retryable_sqlstate_57014|retry_attempts_total|retry_backoff_requested_ms|retry_exhausted_total)=([0-9.e+]+)',prof)}
   def err(msg):errors.append(str(case)+': '+msg)
   for name in ('FAILED.txt','cleanup_errors.txt'):
    if(case/name).exists():err(name)
   if (r['isolation'],r['shared_buffers'],r['workers'],r['total_queries'],r['validated_completed_queries'])!=('serializable','32MB',16,2400000,2400000):err('wrong config/count')
   if s['settings'].get('transaction_isolation')!='serializable' or s['settings'].get('default_transaction_isolation')!='serializable' or s['settings'].get('shared_buffers')!='4096':err('effective settings wrong')
   for k in ('retry_exhausted','permanent_failures','divergence_count'):
    if r[k]:err(k+'='+str(r[k]))
   for k in ('deterministic_error_count','nonterminal_failure_count','gateway_returncode'):
    if res['gateway'].get(k,0):err('gateway '+k+'='+str(res['gateway'][k]))
   if kind!='plain' and (case/'merkle_verify.txt').read_text().strip()!='t':err('Merkle verify')
   if kind=='lsm' and (r['lsm_drain_ms']<=0 or not g.get('stats_samples')):err('LSM drain/stats absent')
   if not g.get('valid') or g.get('measurement_errors'):err('sampler missing/errors')
   if r['wal_bytes']!=w['wal_bytes'] or w['before']['postmaster_start']!=w['after']['postmaster_start'] or r['wal_full_b_stmt']<r['wal_bytes_per_statement']:err('WAL inconsistency')
   if not s.get('settle',{}).get('settled'):err('unsettled')
   if 'CACHES_DROPPED' not in s.get('cache_drop_output',''):err('cold cache drop evidence absent')
   expected_base={'plain':'base_ff90_plain','lsm':'base_ff90_mlsm','btree':'base_ff90'}[kind]
   if Path(res['provenance']['baseline_dir']).name!=expected_base or s.get('keyspace')!='100000000|1|100000000':err('baseline/keyspace mismatch')
   if r['signatures'].get('tx_signature_mismatches',0):err('signature mismatch')
   if r['direct_success']!=2400000 or any(r['signatures'].get(k)!=2400000 for k in ('tx_signatures_signed','tx_signatures_verified')):err('gateway terminal/signature count mismatch')
   expected=cd['workloads']['ycsb_'+r['workload']+'_skew_'+str(float(r['skew']))+'_2400000.sql']['sha256']
   if expected!=r['workload_sha256']:err('planned workload SHA mismatch')
   if kind=='lsm' and (g['last_stats'].get('phase')!='idle' or g['last_stats'].get('flush')):err('LSM final sampled state not idle')
   if kind!='plain' and (res['merkle_stats'].get('lag_items')!=0 or res['merkle_stats'].get('recovery_state')!='READY'):err('Merkle lag/recovery state')
   hashes[(r['workload'],str(r['skew']))].add(r['workload_sha256']);manifest=res['provenance']['baseline_manifest'];manifests[res['provenance']['baseline_dir']].add(json.dumps(manifest,sort_keys=True));r['baseline_manifest']=manifest;r['baseline_dir']=res['provenance']['baseline_dir'];r['reset_content_check']=s['reset_output'][-1000:]
   for e in res['provenance']['executables']:exes[e['path']].add(e['sha256'])
   rows.append(r)
 for k,v in hashes.items():
  if len(v)!=1:errors.append(str(k)+': SHA mismatch')
 keys=[(r['workload'],r['skew'],r['kind'],r['mode']) for r in rows]
 if len(keys)!=len(set(keys)):errors.append('duplicate accepted keys')
 for wl in 'dcb':
  if sum(r['workload']==wl for r in rows)!=16:errors.append(wl+': missing completed cases')
 for k,v in manifests.items():
  if len(v)!=1:errors.append(k+': baseline manifest drift')
 for k,v in exes.items():
  if len(v)!=1:errors.append(k+': executable drift')
 ends={}
 for line in (ROOT/'results/matrix/status.txt').read_text().splitlines():
  x=line.split()
  if len(x)>2 and x[1]=='CAMPAIGN_END':ends[x[2]]=datetime.datetime.fromisoformat(x[0].replace('Z','+00:00')).timestamp()
 for r in rows:
  nxt=[g['first'] for k,g in sample['cases'].items() if k.rsplit('/',1)[0]==r['case'].rsplit('/',1)[0] and g['first']>r['start']]
  end=min(nxt) if nxt else ends.get(r['workload']+'_'+r['kind'])
  if end:r['cycle_s']=end-r['start'];r['cleanup_s']=end-r['result_mtime']
  phases=r['sampler']['phases'];ks=['setup','measurement','drain_checkpoint','verify']
  r['phase_s']={k:phases[ks[i+1]]['first']-phases[k]['first'] for i,k in enumerate(ks[:-1]) if k in phases and ks[i+1] in phases}
  if end and 'verify' in phases:r['phase_s']['verify_and_cleanup']=end-phases['verify']['first']
 result=dict(snapshot=time.time(),sampler_snapshot=sample['snapshot'],sampler_malformed=sample['malformed_lines'],errors=errors,cases=sorted(rows,key=lambda r:r['start']),campaigns=campaigns,hashes={str(k):list(v)for k,v in hashes.items()},baselines={k:[json.loads(x)for x in v]for k,v in manifests.items()},executables={k:list(v)for k,v in exes.items()},failure_files=[str(x)for x in(ROOT/'results/matrix').glob('*_*/run_*/*/FAILED.txt')],cleanup_error_files=[str(x)for x in(ROOT/'results/matrix').glob('*_*/run_*/*/cleanup_errors.txt')])
 (out/'audit.json').write_text(json.dumps(result,indent=2)+'\n'); print(json.dumps(dict(cases=len(rows),errors=errors,snapshot=result['snapshot'])))
p=argparse.ArgumentParser();p.add_argument('mode',choices=['sampler','artifacts']);a=p.parse_args();sampler()if a.mode=='sampler'else artifacts()
