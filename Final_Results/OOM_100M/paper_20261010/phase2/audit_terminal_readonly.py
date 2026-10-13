"""Second terminal parser independent of harness validators; read only on .111."""
import collections,csv,json,re
from pathlib import Path
root=Path('/home/neel/claude_checks/paper_20261010_oom/results');records=[]
# Deliberately parse unanchored headers: concurrent stdout can concatenate records.
rx=re.compile(r'single-gateway-direct-(\d+)[ \t]+\d+[ \t]+(SELECT|UPDATE|INSERT|DELETE)[ \t]+(\d+)(?:[ \t]+(\d+))?')
for group in ('smoke','campaign'):
 run=next((root/group).glob('run_*'))
 for row in csv.DictReader((run/'summary.csv').open()):
  case=Path(row['artifact_dir']);issues=[];ids=[];counts=collections.Counter()
  for match in rx.finditer((case/'server.log').read_text(errors='replace')):
   request,cmd,a,b=match.groups();ids.append(int(request));counts[cmd]+=1
   if cmd=='INSERT':
    if a!='0' or b!='1':issues.append('invalid INSERT tag')
   elif a!='1':issues.append('invalid affected/returned rows')
  if len(ids)!=20000 or set(ids)!=set(range(1,20001)):issues.append('terminal ID coverage/duplicate')
  gw=(case/'gateway.log').read_text();profiles=[dict(re.findall(r'(\w+)=(\S+)',line)) for line in gw.splitlines() if line.startswith('PROFILE_GATEWAY ')]
  if not profiles:issues.append('missing profile')
  else:
   final=profiles[-1]
   for key,val in {'completion_path':'direct','submit_mode':'event','direct_completion_protocol':'2','direct_terminal_success_count':'20000','not_accepted':'0'}.items():
    if final.get(key)!=val:issues.append('gateway '+key)
  for field in ['divergence_count','permanent_failures','deterministic_error_count','nonterminal_failure_count','duplicate_key_errors']:
   vals=re.findall(r'\b'+field+r'=(\d+)',gw)
   if not vals or any(x!='0' for x in vals):issues.append('gateway '+field)
  wl=run/'workloads'/f"ycsb_{row['workload']}_skew_{float(row['skew'])}_20000.sql";expected=collections.Counter()
  for line in wl.read_text().splitlines():
   line=line.strip()
   if not line or line.startswith(('--','/*','\\')):continue
   op=line.split()[0].upper();# Canonical F is WITH ... UPDATE ... RETURNING, and emits UPDATE 1.
   if op=='WITH':
    if not re.match(r'WITH ycsb_read AS MATERIALIZED .*\) UPDATE usertable ',line):raise ValueError('unrecognized canonical CTE')
    op='UPDATE'
   expected[op]+=1
  if counts!=expected:issues.append('operation mix differs from SQL input')
  records.append(dict(dataset=group,case=case.name,terminal_ids=len(ids),unique_ids=len(set(ids)),operation_counts=dict(counts),expected_operation_counts=dict(expected),issues=issues))
print(json.dumps(dict(parser='independent unanchored request headers + terminal tags + complete ID set + SQL operation mix + every gateway error counter',records=records)))
