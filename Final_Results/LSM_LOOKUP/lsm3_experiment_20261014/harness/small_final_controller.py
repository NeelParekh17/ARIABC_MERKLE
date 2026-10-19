import json,os,subprocess as S
from pathlib import Path
r=Path('/home/neel/claude_ctl/lsm_lookup_20261014');out=r/'small_final';out.mkdir(exist_ok=True)
os.environ['TMPDIR']=str(r/'scratch');os.environ['LD_LIBRARY_PATH']=str(r/'pg_install/lib')+':/home/neel/Desktop/rdkafka_local/lib'
p=out/'insert.sql'
with p.open('w') as f:
 for i in range(1,70001):
  for s in ['db','dl']:f.write(f"INSERT INTO {s}.usertable VALUES({i},'det-compaction');\n")
args=[r/'build/bin/ariabc_pg_gateway','--nodes','10.129.148.247:8088','--queryFrom',p,'--dbType','1','--detStartSeq','0','--reqIdOffset','1','--detWindow','1024','--detBatchSize','256','--dbConnPoolSize','16','--submitMode','event','--detSubmitPipeline','1','--detPipelineDepth','1024','--detClientMode','event','--detClientWorkers','96','--detClientInflight','16','--clientId','single-gateway-direct','--numTerminals','96','--connFanout','1','--waitMajority','0','--completionPath','direct','--totalNodes','1']
(out/'command.json').write_text(json.dumps([str(x) for x in args],indent=2)+'\n')
with (out/'gateway.log').open('w') as f:
 result=S.run([str(x) for x in args],stdout=f,stderr=S.STDOUT,timeout=600)
(out/'exit.txt').write_text(str(result.returncode)+'\n');assert result.returncode==0
