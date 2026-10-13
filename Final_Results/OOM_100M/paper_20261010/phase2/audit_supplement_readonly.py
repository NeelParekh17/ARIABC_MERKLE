import collections,csv,hashlib,json,re
from pathlib import Path
root=Path('/home/neel/claude_checks/paper_20261010_oom/results');out={};known={}
for name in ('postgres.sha256','cpp_dynamic_dependencies.sha256','server_dynamic_dependencies.sha256'):
 values=[]
 for line in (root/'provenance'/name).read_text().splitlines():
  if re.match(r'^[a-f0-9]{64}\s+',line):
   sha,name=line.split(maxsplit=1);p=Path(name)
   if p.is_absolute() and p.exists():values.append(dict(path=name,expected=sha,actual=hashlib.sha256(p.read_bytes()).hexdigest()))
 out.setdefault('local_dependencies',[]).extend(values)
for group in ('campaign','smoke'):
 run=next((root/group).glob('run_*'));case_errors=[]
 for row in csv.DictReader((run/'summary.csv').open()):
  p=Path(row['artifact_dir']);s=json.load(open(p/'setup.json'));issues=[]
  canonical={'bcdb_serial_gate_mode':'1','bcdb_serial_gate_source':'0','bcdb_dt_conflict_tracking':'on','bcdb_result_ring_slots':'2048','bcdb_dt_completion_only_skip_reads':'off','bcdb_dt_hashtab_switch_threshold':'1500','bcdb_gate_telemetry':'off','bcdb_gate_snapshot_each_block':'off','merkle_apply_synchronous_direct':'on','wal_compression':'off'}
  for k,v in canonical.items():
   if s['settings'].get(k)!=v:issues.append('setting '+k)
  cmd=s['server']['command']; gw=json.load(open(p/'result.json'))['gateway']['command'];dt=row['mode']!='pg';want='1' if dt else '0'
  for command,checks in [(cmd,{'--dbPort':'5468','--clientPort':'8068','--bypassRaft':'1','--dbType':want,'--dbConnPoolSize':row['workers'],'--pgExecMode':'event'}),(gw,{'--dbType':want,'--numTerminals':'96','--detClientWorkers':'96','--completionPath':'direct','--waitMajority':'0','--totalNodes':'1','--submitMode':'event'})]:
   for k,v in checks.items():
    if k not in command or command[command.index(k)+1]!=v:issues.append('command '+k)
  errors=collections.Counter();warnings=collections.Counter()
  for line in (p/'postgres.log').read_text(errors='replace').splitlines():
   m=re.search(r'\b(ERROR|FATAL|PANIC|WARNING):\s+(.*)',line)
   if m:
    msg=m[2];msg=re.sub(r'process \d+','process PID',msg);msg=re.sub(r'PID \d+','PID N',msg)
    (warnings if m[1]=='WARNING' else errors)[m[1]+': '+msg]+=1
  raw=(p/'server.log').read_text(errors='replace')+'\n'+(p/'server.err.log').read_text(errors='replace')
  retry=collections.Counter(re.findall(r'SQLSTATE[^\n]{0,90}',raw))
  case_errors.append(dict(case=p.name,issues=issues,postgres_errors=dict(errors),postgres_warnings=dict(warnings),server_sqlstates=dict(retry),reset_output=s['reset_output']))
 out[group]=case_errors
print(json.dumps(out))
