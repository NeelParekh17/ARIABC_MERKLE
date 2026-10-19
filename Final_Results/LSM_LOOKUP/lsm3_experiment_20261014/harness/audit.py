#!/usr/bin/env python3
"""Independent retained-evidence audit; no database or benchmark execution."""
import argparse,csv,hashlib,json,re
from collections import Counter
from pathlib import Path

def audit(root,out):
 root=root.resolve();out=out.resolve()
 cases=[];probes={};all_keys=set();binary_contract=None;module_contract=None;source_contract=None
 for summary in sorted(root.glob('*/run_*/summary.csv')):
  stage=summary.parent.parent.name
  if stage not in ['smoke','ff90']:continue
  campaign=json.loads((summary.parent/'campaign.json').read_text())
  if source_contract is None:source_contract=campaign['source_sha256']
  assert campaign['source_sha256']==source_contract,(stage,'mixed harness sources')
  assert campaign['arguments']['reset_mode']=='delta'
  assert (campaign['arguments']['db_port'],campaign['arguments']['server_port'],campaign['arguments']['raft_port'])==(5488,8088,9088)
  planned=json.loads((summary.parent/'planned_cases.json').read_text());seen=set()
  assert len(planned)=={'smoke':3,'ff90':72}[stage]
  for row in csv.DictReader(summary.open()):
   case=Path(row['artifact_dir'])
   if not case.exists():case=summary.parent/case.name
   saved=json.loads((case/'result.json').read_text());setup=json.loads((case/'setup.json').read_text())
   provenance=setup['provenance'];executables=provenance['executables'];module=provenance['lsm3_module']
   if binary_contract is None:binary_contract=executables;module_contract=module
   assert executables==binary_contract and module==module_contract,(case,'mixed binaries')
   assert module['sha256']=='3d5d9913d45a54e164c4fd42540885ab0e95a062ba608c76bfcca0a71b55e8ad'
   assert setup['settings']['shared_preload_libraries']=='lsm3' and setup['settings']['lsm3.top_index_size']=='1024'
   assert setup['settings']['fsync']==setup['settings']['full_page_writes']==setup['settings']['synchronous_commit']=='on'
   n=int(row['total_queries']);key=(row['workload'],float(row['skew']),row['mode'],int(row['workers']),int(row['trial']));assert key not in seen;seen.add(key);assert list(key) in planned
   assert (stage,*key) not in all_keys;all_keys.add((stage,*key))
   assert int(row['fillfactor'])==90 and n==20000
   assert row['mode'] in ['det','merkle_btree','merkle_lsm']
   assert int(row['trial']) in ([1] if stage=='smoke' else [1,2,3])
   assert (float(row['skew']),int(row['workers'])) in ([(0.0,8)] if stage=='smoke' else [(t,w) for t in [0.0,0.99] for w in [1,4,8,16]])
   assert saved['gateway']['gateway_returncode']==0
   cmd=saved['gateway']['command']
   assert cmd[cmd.index('--numTerminals')+1]==cmd[cmd.index('--detClientWorkers')+1]=='96'
   assert not any((case/f).exists() for f in ['FAILED.txt','cleanup_errors.txt','NOISE_EXCLUDED.txt'])
   assert json.loads((case/'noise_qualification.json').read_text())['excluded'] is False
   assert row['noise_excluded'].lower()=='false'
   work=summary.parent/'workloads'/f"ycsb_{row['workload']}_skew_{float(row['skew'])}_{n}.sql"
   assert hashlib.sha256(work.read_bytes()).hexdigest()==row['workload_sha256'];sql=work.read_text().splitlines();assert len(sql)==n
   if n==20000:
    published={0.0:'7a031904b743e46ac36c81f8ee675141eec90f2831dd31f8788bed7a36bd1b83',0.99:'a3f07805d1b15666b9ef9ce62daa7f441b622c08d151bc1c8d5ab67cfc97559b'}
    assert row['workload']=='a' and row['workload_sha256']==published[float(row['skew'])]
   log=(case/'server.log').read_text();header=r'single-gateway-direct-\d+[ \t]+\d+[ \t]+(?:SELECT|UPDATE|INSERT|DELETE)[ \t]+'
   log=re.sub(r'(?='+header+')','\n',log)
   records=re.findall(r'^single-gateway-direct-(\d+)[ \t]+1[ \t]+(SELECT|UPDATE|INSERT|DELETE)[ \t]+([^\n]*)$',log,re.M)
   ids=[int(x[0]) for x in records];assert len(ids)==n and set(ids)==set(range(1,n+1)),(case,len(ids),n)
   tags=Counter()
   for ident,kind,tail in records:
    stmt=sql[int(ident)-1].lstrip().upper();expected='UPDATE' if stmt.startswith('WITH ') else stmt.split()[0];assert kind==expected
    count=tail.split();assert count[:2]==['0','1'] if kind=='INSERT' else count[0]=='1',(case,ident,kind,tail)
    tags[kind]+=1
   gw=(case/'gateway.log').read_text();success=re.findall(r'direct_terminal_success_count=(\d+)',gw);assert success and int(success[-1])==n
   for k in ['divergence_count','permanent_failures','nonterminal_failure_count','deterministic_error_count']:
    vals=re.findall(k+r'=(\d+)',gw);assert vals and all(int(v)==0 for v in vals),(case,k)
   assert setup['settings']['default_transaction_isolation']=='serializable';assert setup['settings']['shared_buffers']=='4096'
   assert setup['settings']['merkle_apply_synchronous_direct']=='on';assert setup['settings']['bcdb_serial_gate_source']=='0'
   assert int(row['db_rows'])==100000000 and setup['keyspace']=='100000000|1|100000000'
   assert 'CACHES_DROPPED' in setup['cache_drop_output']
   reset=setup['reset_output']
   assert 'RESTORE_METHOD=delta' in reset or (stage=='smoke' and 'RESTORE_METHOD=seed_cp' in reset and 'CONTENT_CHECK=identical' in reset)
   if row['mode'].startswith('merkle'):
    lookup=next(x['indexdef'] for x in setup['indexes'] if x['indexname']=='usertable_merkle_lookup_idx')
    assert ('USING lsm3 (' if row['mode']=='merkle_lsm' else 'USING btree (') in lookup
    assert (case/'merkle_verify.txt').read_text().strip()=='t';assert setup['merkle_stats']['version']==11
    probe=setup['lookup_probe'];assert 'usertable_merkle_lookup_idx' in probe['split_plan']
    assert 'usertable_merkle_lookup_idx' in probe['repair_plan']
    pk=(int(row['fillfactor']),row['mode']);prior=probes.get(pk);assert prior is None or prior==probe;probes[pk]=probe
    for k,v in dict(partitions=200,fanout=32,split_threshold=1024,merge_threshold=256).items():assert setup['merkle_stats'][k]==v
   detail=json.loads((case/'detailed_counters.json').read_text())
   assert (case/'wal_fpi_count.json').exists();assert detail['wal_bytes']>0
   if row['mode']=='merkle_lsm':
    merges=detail['after_compaction_drain']['merges_requested']-detail['before']['merges_requested'];completed=len(re.findall(r'LOG:  Lsm3: truncate index usertable_merkle_lookup_idx_top',(case/'postgres.log').read_text()))
    assert merges==completed,(case,merges,completed)
   cases.append(dict(stage=stage,case=str(case),pass_check=True,terminals=n,operations=dict(tags),mode=row['mode']))
 for ff in {k[0] for k in probes}:
  b=probes.get((ff,'merkle_btree'));l=probes.get((ff,'merkle_lsm'))
  if b and l:
   assert b['bounds']==l['bounds'] and b['range_fingerprint']==l['range_fingerprint'],(ff,'lookup range mismatch')
 counts=dict(Counter(x['stage'] for x in cases));complete=counts=={'smoke':3,'ff90':72}
 out.write_text(json.dumps(dict(cases=cases,accepted=len(cases),stage_counts=counts,complete=complete,terminals=sum(x['terminals'] for x in cases),expected={'smoke':3,'ff90':72},executables=binary_contract,lsm3_module=module_contract,source_sha256=source_contract),indent=2)+'\n')
 print(json.dumps(dict(accepted=len(cases),terminals=sum(x['terminals'] for x in cases),pass_check=True,complete=complete)))
if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('--root',type=Path,required=True);p.add_argument('--out',type=Path,required=True);a=p.parse_args();audit(a.root,a.out)
