#!/usr/bin/env python3
""".247 only. Never opens a published source with PostgreSQL."""
import hashlib,json,os,shutil,subprocess as S,time
from pathlib import Path
R=Path('/home/neel/claude_checks/lsm_lookup_20261014');D=Path('/home/neel/ariabc_data/lsm_lookup_20261014');P=Path('/home/neel/ariabc_data/paper_20261010_oom');O=R/'baselines_ff100';O.mkdir(exist_ok=True)
os.environ['TMPDIR']=str(R/'scratch');os.environ['LD_LIBRARY_PATH']=str(R/'install/lib')+':/home/neel/Desktop/rdkafka_local/lib'
I=R/'install/bin';RESERVE=40*1024**3;active=None

def status(s):
 line=time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime())+' '+s
 (O/'status.txt').write_text(line+'\n');print(line,flush=True)
def run(cmd,name,check=True):
 with (O/name).open('a') as f:
  f.write(json.dumps([str(x) for x in cmd])+'\n');f.flush();p=S.Popen([str(x) for x in cmd],stdout=f,stderr=S.STDOUT)
  while p.poll() is None:
   free=shutil.disk_usage(D).free
   with (O/'resources.jsonl').open('a') as m:m.write(json.dumps(dict(time=time.time(),free_bytes=free,meminfo=Path('/proc/meminfo').read_text()))+'\n')
   if free<RESERVE:
    p.terminate();p.wait();raise RuntimeError('40GiB reserve reached during '+name)
   time.sleep(5)
 if check and p.returncode:raise RuntimeError(name+' exit '+str(p.returncode))
 return p.returncode
def sql(s):
 return S.check_output([str(I/'psql'),'-X','-h','127.0.0.1','-p','5488','-U','postgres','-d','postgres','-v','ON_ERROR_STOP=1','-At','-c',s],text=True).strip()
def identity(d):
 txt=S.check_output([str(I/'pg_controldata'),str(d)],text=True);a={k.strip():v.strip() for k,v in (l.split(':',1) for l in txt.splitlines() if ':' in l)}
 assert a['Database cluster state']=='shut down'
 return dict(db_system_id=a['Database system identifier'],cluster_state=a['Database cluster state'],checkpoint_lsn=a['Latest checkpoint location'],pg_control_sha256=hashlib.sha256((d/'global/pg_control').read_bytes()).hexdigest(),pg_version_sha256=hashlib.sha256((d/'PG_VERSION').read_bytes()).hexdigest(),file_count=sum(1 for p in d.rglob('*') if p.is_file()),total_bytes=int(S.check_output(['du','-sb',str(d)],text=True).split()[0]))
def start(d):
 global active
 assert d.parent==D and not d.is_symlink()
 assert not any(p.stat().st_nlink>1 for p in (d/'base').rglob('*') if p.is_file()), 'PG start on linked immutable baseline refused'
 assert shutil.disk_usage(D).free>=RESERVE
 with (O/(d.name+'_free_before_start.log')).open('a') as f:S.run(['free','-g'],stdout=f,check=True)
 (d/'postgresql.auto.conf').write_text(f"port=5488\nlisten_addresses='127.0.0.1'\nunix_socket_directories='{R}/scratch'\nshared_buffers='32MB'\nmaintenance_work_mem='256MB'\nwork_mem='4MB'\nmax_parallel_maintenance_workers=0\nmax_parallel_workers_per_gather=0\nautovacuum=off\ndefault_transaction_isolation='serializable'\nenable_merkle_index=on\nmerkle_apply_synchronous_direct=on\nbcdb_worker_count=1\nbcdb_ledger_trace=off\nshared_preload_libraries='lsm3'\nlsm3.top_index_size=1024\nmax_worker_processes=32\n")
 active=d;run([I/'pg_ctl','-D',d,'-l',O/(d.name+'_postgres.log'),'-w','start'],d.name+'_control.log')
def stop():
 global active
 if active is not None:
  run([I/'pg_ctl','-D',active,'-m','fast','-w','stop'],active.name+'_control.log');active=None
def copy(src,dst):
 assert dst.parent==D and not dst.exists()
 # Reserve full apparent source size plus 3GiB for WAL and index build.
 size=int(S.check_output(['du','-sb',str(src)],text=True).split()[0]);free=shutil.disk_usage(D).free
 assert free-size-3*1024**3>=RESERVE,('insufficient copy headroom',src,dst,size,free)
 dst.mkdir();run(['rsync','-a','--inplace','--no-whole-file','--stats',str(src)+'/',str(dst)+'/'],dst.name+'_copy.log')
 run(['diff','-rq','--no-dereference',src,dst],dst.name+'_byte_identity.log')
def verify(d,merkle,ff):
 status('VERIFY '+d.name)
 assert sql('SELECT count(*),min(ycsb_key),max(ycsb_key) FROM usertable')=='100000000|1|100000000'
 cat=json.loads(sql("SELECT json_build_object('heap_bytes',pg_relation_size('usertable'),'index_bytes',pg_relation_size('usertable_pkey1'),'total_relation_bytes',pg_total_relation_size('usertable'),'relpages',(SELECT relpages FROM pg_class WHERE oid='usertable'::regclass),'fillfactor',(SELECT reloptions FROM pg_class WHERE oid='usertable'::regclass),'indexes',(SELECT json_agg(i) FROM (SELECT indexname,indexdef FROM pg_indexes WHERE tablename='usertable' ORDER BY indexname)i),'sizes',(SELECT json_agg(i) FROM (SELECT n.nspname,c.relname,pg_relation_size(c.oid) bytes FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.relname LIKE '%usertable%' ORDER BY n.nspname,c.relname)i))"))
 assert cat['fillfactor']==['fillfactor='+str(ff)]
 geom=None;root=None
 if merkle:
  assert sql("SELECT merkle_verify('usertable')")=='t'
  geom=json.loads(sql("SELECT merkle_tree_stats('usertable')"));root=sql("SELECT merkle_root_hash('usertable')")
  for k,v in dict(version=11,partitions=200,fanout=32,split_threshold=1024,merge_threshold=256).items():assert geom[k]==v,(k,geom)
 sql('CHECKPOINT');stop()
 manifest=dict(version=2,db_rows=100000000,keyspace='100000000|1|100000000',**cat,**identity(d),merkle_stats=geom,root=root,baseline_dir=str(d),fillfactor_value=ff,build_commit='a7fc519dfdd0713247922abff3bc3c80b752e27e')
 (D/('.ariabc_golden_manifest_'+d.name+'.json')).write_text(json.dumps(manifest,indent=2)+'\n');(O/(d.name+'.json')).write_text(json.dumps(manifest,indent=2)+'\n')
 return manifest
