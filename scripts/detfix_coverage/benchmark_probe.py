#!/usr/bin/env python3
"""Short det executions proving read coverage counters under benchmark plans."""
import csv,json,os,subprocess as S,time
from pathlib import Path
from audit import ROOT,INST,PG,native
for case in ('tpcc','ycsb'):
 run=ROOT/'probes'/(case+os.environ.get('PROBE_LABEL',''));run.mkdir(parents=True,exist_ok=False)
 data=run/'pgdata';S.run(['cp','-a','--reflink=never',str(ROOT/'planner_pgdata'),str(data)],check=True)
 if case=='tpcc':
  sql=(ROOT/'planner_workload.sql').read_text().splitlines()[:200]
 else:
  sql=(ROOT/'src/scripts/ycsb_suite/ycsb_workload_f_skew_0_99_20k.txt').read_text().splitlines()[:256]
 (run/'workload.sql').write_text('\n'.join(sql)+'\n')
 with (data/'postgresql.conf').open('a') as f:f.write("\ndefault_transaction_isolation='serializable'\nbcdb_serial_gate_mode=1\nbcdb_serial_gate_source=0\nbcdb_dt_conflict_tracking=on\nbcdb_worker_count=2\nenable_seqscan=off\nbcdb_dt_completion_only_skip_reads=off\n")
 env=dict(os.environ,BCDB_PHASE_TRACE=str(run/'ptrace'))
 def cmd(args,name):
  with (run/name).open('w') as f:S.run([str(x) for x in args],check=True,stdout=f,stderr=S.STDOUT,env=env)
 started=False
 try:
  cmd([INST/'bin/pg_ctl','-D',data,'-l',run/'postgres.log','-w','start'],'start.log');started=True
  p=PG(55491)
  (run/'effective_settings.json').write_text(json.dumps(p.query("SELECT name,setting FROM pg_settings WHERE name LIKE 'bcdb_%' OR name IN ('enable_seqscan','enable_indexonlyscan') ORDER BY name"),indent=2));p.close()
  with (run/'client.log').open('w') as f:S.run(['python3',str(ROOT/'audit.py'),'--native',str(run),'--workers','2','--port','55491'],stdout=f,stderr=S.STDOUT,check=True,timeout=90)
 finally:
  if started:cmd([INST/'bin/pg_ctl','-D',data,'-m','fast','-w','stop'],'stop.log')
 rows=[json.loads(x) for x in (run/'det_results.jsonl').read_text().splitlines()]
 counters={}
 for trace in run.glob('ptrace*'):
  with trace.open() as f:
   for row in csv.DictReader(f):
    for k,v in row.items():
     if k and k.startswith('dt_'):counters[k]=counters.get(k,0)+int(v)
 summary={'case':case,'requested':len(sql),'completed':len(rows),'errors':[r for r in rows if r['status'] not in (1,2)],'counters':counters,'purpose':'Counter check only; Task B own-write semantics not merged'}
 (run/'summary.json').write_text(json.dumps(summary,indent=2)+'\n');print(json.dumps(summary),flush=True)
