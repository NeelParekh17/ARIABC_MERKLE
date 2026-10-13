#!/usr/bin/env python3
"""Audit accepted phase-1 evidence on .111; retain large raw logs remotely."""
import csv, datetime, hashlib, io, json, sys
from pathlib import Path
root=Path('/home/neel/claude_checks/paper_20261010_oom')
sys.path.insert(0,str(root/'repo/scripts/distributed'))
from benchmark_validation import parse_gateway_result
from benchmark_contract import validate_ycsb_results
out=root/'results/phase1_audit';out.mkdir(exist_ok=True)
expected=json.loads((root/'results/provenance/published_workloads.json').read_text())
report={'time_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'HEAD':(root/'results/provenance/git_head.txt').read_text().strip(),'datasets':{}}
assert report['HEAD']=='16577072ecf888fbbbb5d80d0e90fc340c8ca342'
for group in ('smoke','campaign'):
 paths=list((root/'results'/group).glob('run_*/summary.csv'));assert len(paths)==1,(group,paths)
 snapshot=paths[0].read_bytes();rows=list(csv.DictReader(io.StringIO(snapshot.decode())));assert len(rows)>=3
 arguments=json.loads((paths[0].parent/'campaign.json').read_text())['arguments']
 assert (arguments['db_port'],arguments['server_port'],arguments['raft_port'])==(5468,8068,9068)
 if group=='smoke': assert len(rows)==3 and {r['mode'] for r in rows}=={'pg','bcdb_det','bcdb_merkle'}
 if group=='campaign':
  assert arguments['auto_resume'] and arguments['trials']==3
  planned=list(csv.DictReader((root/'results/provenance/planned_case_order.csv').open()))
  keys=lambda rs:[(r['workload'],float(r['skew']),r['mode'],int(r['workers']),int(r['trial'])) for r in rs]
  assert keys(rows)==keys(planned[:len(rows)])
 keyset=set();records=[];digest_lines=[]
 for row in rows:
  key=(row['workload'],float(row['skew']),row['mode'],int(row['workers']),int(row['trial']))
  assert key not in keyset;keyset.add(key)
  assert row['isolation']=='serializable' and row['shared_buffers']=='32MB'
  assert int(row['total_queries'])==int(row['validated_completed_queries'])==20000
  assert all(int(row[k])==0 for k in ('divergence_count','permanent_failures','retry_exhausted'))
  if row['mode']=='pg': assert row['pg_retry_jitter']=='on'
  assert row.get('noise_excluded') in ('False','false','0'),row
  case=Path(row['artifact_dir']);saved=json.loads((case/'result.json').read_text());setup=json.loads((case/'setup.json').read_text())
  assert not any((case/f).exists() for f in ('FAILED.txt','cleanup_errors.txt','NOISE_EXCLUDED.txt'))
  assert not json.loads((case/'noise_qualification.json').read_text())['excluded']
  assert saved['gateway']['gateway_returncode']==0
  mode='pg' if row['mode']=='pg' else row['mode']
  metrics=parse_gateway_result((case/'gateway.log').read_text(),20000,0,mode=mode)
  terminal=validate_ycsb_results((case/'server.log').read_text(),20000,mode=mode)
  assert terminal['validated_row_results']==20000 and metrics['validated_completed_queries']==20000
  assert float(saved['row']['tps'])==float(row['tps'])
  settings=setup['settings'];assert int(settings['shared_buffers'])*int(settings['block_size'])==32*2**20
  for k,v in dict(fsync='on',full_page_writes='on',synchronous_commit='on',default_transaction_isolation='serializable',merkle_apply_synchronous_direct='on',bcdb_serial_gate_source='0').items():assert settings[k]==v,(k,settings[k])
  name=f"ycsb_{row['workload']}_skew_{float(row['skew'])}_20000.sql"
  assert row['workload_sha256']==expected[name]['sha256']
  assert hashlib.sha256((paths[0].parent/'workloads'/name).read_bytes()).hexdigest()==row['workload_sha256']
  if row['mode']=='bcdb_merkle':
   assert row['merkle_verify']=='PASS' and (case/'merkle_verify.txt').read_text().strip()=='t'
   for k,v in dict(partitions=200,fanout=32,split_threshold=1024,merge_threshold=256).items(): assert setup['merkle_stats'][k]==v
  for p in sorted(case.iterdir()):
   if p.is_file():
    h=hashlib.sha256()
    with p.open('rb') as f:
     for b in iter(lambda:f.read(1024*1024),b''):h.update(b)
    digest_lines.append(h.hexdigest()+'  '+str(p))
  elapsed_estimate=float(row['reset_time_ms'])/1000 + ((case/'result.json').stat().st_mtime_ns-(case/'setup.json').stat().st_mtime_ns)/1e9
  records.append(dict(configuration=key,artifact_dir=str(case),tps=float(row['tps']),terminal_results=terminal,gateway_returncode=0,merkle_verify=row['merkle_verify'],case_elapsed_estimate_s=elapsed_estimate,accepted_unix_ns=(case/'result.json').stat().st_mtime_ns,det_counters=json.loads((case/'det_counters.json').read_text())))
 (out/(group+'_summary_snapshot.csv')).write_bytes(snapshot)
 (out/(group+'_RAW_SHA256SUMS')).write_text('\n'.join(digest_lines)+'\n')
 failed=list(paths[0].parent.glob('*/FAILED.txt'));noise=list(paths[0].parent.glob('*/NOISE_EXCLUDED.txt'))
 report['datasets'][group]=dict(run_dir=str(paths[0].parent),accepted=len(rows),failed_attempts=len(failed),noise_excluded_attempts=len(noise),merkle_pass=sum(r['mode']=='bcdb_merkle' for r in rows),terminal_results=len(rows)*20000,records=records)
(out/'acceptance.json').write_text(json.dumps(report,indent=2)+'\n')
print(json.dumps({k:{x:v[x] for x in ('accepted','failed_attempts','noise_excluded_attempts','merkle_pass','terminal_results')} for k,v in report['datasets'].items()},indent=2))
