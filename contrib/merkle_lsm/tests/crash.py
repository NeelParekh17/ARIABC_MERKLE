# Run on the isolated lab host only; see tests/README.md.
import collections,json,os,signal,subprocess,time
from pathlib import Path
import psycopg
from psycopg import sql
R=Path(os.environ.get('MERKLE_LSM_TEST_ROOT',str(Path.home()/'claude_checks/lsm_integration_20261019')));O=R/'evidence';B=R/'install/bin'
os.environ['TMPDIR']=str(R/'scratch');os.environ['LD_LIBRARY_PATH']=str(R/'install/lib')
def connect():return psycopg.connect(f'host={R}/socket port=5494 user=postgres dbname=postgres client_encoding=UTF8',autocommit=True)
c=connect()
def q(s,p=None):return c.execute(s,p)
def record(label,**v):
 print(label,json.dumps(v,default=str),flush=True)
 with (O/'crash_validation.jsonl').open('a') as f:f.write(json.dumps(dict(label=label,**v),default=str)+'\n')
def configure(point,level,delay=60000,error=False):
 for name,value in [('debug_point',point),('debug_level',level),('debug_delay_ms',delay),('debug_error',error)]:
  q(sql.SQL('ALTER SYSTEM SET {} = {}').format(sql.Identifier('merkle_lsm.'+name),sql.Literal(str(value))))
 q('SELECT pg_reload_conf()');time.sleep(.3)
def wait_marker(offset,point,level,timeout=20):
 marker=f'merkle_lsm injection point {point} level {level}'
 start=time.monotonic()
 while time.monotonic()-start<timeout:
  if marker in (O/'postgres.log').read_bytes()[offset:].decode(errors='replace'):return
  time.sleep(.05)
 raise AssertionError('injection did not fire: '+marker)
def audit(table):
 q('SET enable_seqscan=on; SET enable_indexscan=off; SET enable_bitmapscan=off')
 truth=collections.Counter(q(f'SELECT k,ctid::text,v FROM {table} WHERE k>0').fetchall())
 assert sum(truth.values())==6000
 for method in ['index','bitmap','only']:
  q('SET enable_seqscan=off; SET enable_indexscan='+('off' if method=='bitmap' else 'on')+'; SET enable_bitmapscan='+('on' if method=='bitmap' else 'off')+'; SET enable_indexonlyscan='+('on' if method=='only' else 'off'))
  stmt=f'SELECT k,v FROM {table} WHERE k>0' if method=='only' else f'SELECT k,ctid::text,v FROM {table} WHERE k>0'
  expected=collections.Counter((k,v) for k,t,v in truth.elements()) if method=='only' else truth
  got=collections.Counter(q(stmt).fetchall());assert got==expected,(table,method,len(got),len(expected))
  assert all(n==1 for n in got.values()),(table,'duplicates')
 q('RESET enable_seqscan; RESET enable_indexscan; RESET enable_bitmapscan; RESET enable_indexonlyscan')
for levels in [2,3,4]:
 for level in range(levels-1):
  for point in [1,2,3]:
   table=f'crash_l{levels}_d{level}_p{point}';idx=table+'_idx'
   q(f'DROP TABLE IF EXISTS {table}');q(f'CREATE TABLE {table}(k int,v text)')
   q(f'CREATE INDEX {idx} ON {table} USING merkle_lsm(k) INCLUDE(v) WITH(levels={levels},top_index_size=16,size_ratio=2)')
   configure(point,level)
   offset=(O/'postgres.log').stat().st_size
   q(f"INSERT INTO {table} SELECT i,'row-'||i FROM generate_series(1,6000)i")
   wait_marker(offset,point,level)
   mode='immediate' if (levels+level+point)%2 else 'worker_sigkill'
   c.close()
   # Disable the hook in the disposable auto.conf before recovery workers start.
   with (R/'data/postgresql.auto.conf').open('a') as f:f.write("\nmerkle_lsm.debug_point=0\nmerkle_lsm.debug_error=off\n")
   if mode=='immediate':
    with (O/'crash_ctl.log').open('a') as f:subprocess.run([B/'pg_ctl','-D',R/'data','-m','immediate','-w','stop'],stdout=f,stderr=f,check=True)
    with (O/'crash_ctl.log').open('a') as f:subprocess.run([B/'pg_ctl','-D',R/'data','-l',O/'postgres.log','-w','start'],stdout=f,stderr=f,check=True)
   else:
    d=connect();pid=d.execute("SELECT pid FROM pg_stat_activity WHERE backend_type='merkle_lsm merger' AND datname=current_database()").fetchone()[0];d.close();os.kill(pid,signal.SIGKILL)
   start=time.monotonic()
   while True:
    try:c=connect();q('SELECT 1');break
    except psycopg.OperationalError:
     if time.monotonic()-start>45:raise
     time.sleep(.1)
   configure(0,0,0,False)
   q("SET statement_timeout='60s'")
   audit(table)
   q('SELECT merkle_lsm_flush(%s::regclass)',(idx,));audit(table)
   components=q('SELECT * FROM merkle_lsm_components(%s::regclass)',(idx,)).fetchall()
   assert all(x[5]==0 for x in components if x[0]!=levels-1)
   assert sum(x[5] for x in components)==6000,components
   record('crash_PASS',levels=levels,level=level,point=point,mode=mode,components=components)
   q(f'DROP TABLE {table}')
# One ERROR at each level must be retried successfully by the same worker.
for level in range(3):
 table=f'error_d{level}';idx=table+'_idx'
 q(f'DROP TABLE IF EXISTS {table}');q(f'CREATE TABLE {table}(k int,v text)')
 q(f'CREATE INDEX {idx} ON {table} USING merkle_lsm(k) INCLUDE(v) WITH(levels=4,top_index_size=16,size_ratio=2)')
 configure(1,level,0,True);offset=(O/'postgres.log').stat().st_size
 q(f"INSERT INTO {table} SELECT i,'row-'||i FROM generate_series(1,6000)i")
 wait_marker(offset,1,level);configure(0,0,0,False)
 q('SELECT merkle_lsm_flush(%s::regclass)',(idx,));audit(table)
 record('error_PASS',level=level)
 q(f'DROP TABLE {table}')
record('PASS')
