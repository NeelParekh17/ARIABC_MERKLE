import json,os,subprocess as S
from pathlib import Path
r=Path('/home/neel/claude_ctl/lsm_lookup_20261014');o=r/'small_final'
assert (o/'exit.txt').read_text().strip()=='0'
os.environ['TMPDIR']=str(r/'scratch');os.environ['LD_LIBRARY_PATH']=str(r/'pg_install/lib')+':/home/neel/Desktop/rdkafka_local/lib'
p=o/'mutations.sql'
with p.open('w') as f:
 for i in range(1,501):
  for s in ['db','dl']:f.write(f"UPDATE {s}.usertable SET v='changed' WHERE ycsb_key={i};\n")
 for i in range(501,1001):
  for s in ['db','dl']:f.write(f"DELETE FROM {s}.usertable WHERE ycsb_key={i};\n")
 for i in range(1001,1501):
  for s in ['db','dl']:f.write(f"UPDATE {s}.usertable SET ycsb_key=ycsb_key+100000 WHERE ycsb_key={i};\n")
a=json.loads((o/'command.json').read_text())
for opt,value in [('--queryFrom',str(p)),('--detStartSeq','140000'),('--reqIdOffset','1'),('--clientId','single-gateway-direct-update')]:a[a.index(opt)+1]=value
(o/'mutations_command.json').write_text(json.dumps(a,indent=2)+'\n')
with (o/'gateway_mutations.log').open('w') as f:rc=S.run(a,stdout=f,stderr=S.STDOUT,timeout=180).returncode
(o/'mutations_exit.txt').write_text(str(rc)+'\n');assert rc==0
