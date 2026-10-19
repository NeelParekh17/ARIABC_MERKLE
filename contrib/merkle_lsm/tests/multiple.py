# Run on the isolated lab host only; see tests/README.md.
import collections,concurrent.futures,json,os,subprocess,time
from pathlib import Path
import psycopg
R=Path(os.environ.get('MERKLE_LSM_TEST_ROOT',str(Path.home()/'claude_checks/lsm_integration_20261019')));O=R/'evidence';B=R/'install/bin'
env=dict(os.environ,TMPDIR=str(R/'scratch'),LD_LIBRARY_PATH=str(R/'install/lib'),PGHOST=str(R/'socket'),PGPORT='5494',PGUSER='postgres')
def conn(db='postgres'):return psycopg.connect(f'host={R}/socket port=5494 user=postgres dbname={db} client_encoding=UTF8',autocommit=True)
c=conn()
def rec(label,**v):
 print(label,json.dumps(v,default=str),flush=True)
 with (O/'multi_validation.jsonl').open('a') as f:f.write(json.dumps(dict(label=label,**v),default=str)+'\n')
for db in ['multi_lsm_a','multi_lsm_b']:
 if not c.execute('SELECT 1 FROM pg_database WHERE datname=%s',(db,)).fetchone():c.execute('CREATE DATABASE '+db)
 d=conn(db);d.execute('CREATE EXTENSION IF NOT EXISTS merkle_lsm');d.close()
def populate(db):
 d=conn(db)
 for n in range(9):
  table=f't{n}';name=('long_index_'+('x'*48)+str(n)) if n==0 else f'lookup_{n}'
  d.execute(f'DROP TABLE IF EXISTS {table} CASCADE');d.execute(f'CREATE TABLE {table}(k int,v text)')
  d.execute(f'CREATE INDEX {name} ON {table} USING merkle_lsm(k) WITH(levels=4,top_index_size=16,size_ratio=2)')
  if n==0:d.execute(f'ALTER INDEX {name} RENAME TO renamed_long_base')
 for turn in range(8):
  for n in range(9):d.execute(f"INSERT INTO t{n} SELECT i,'v-'||i FROM generate_series({turn*1000+1},{turn*1000+1000}) i")
 names=['renamed_long_base']+[f'lookup_{n}' for n in range(1,9)]
 for n,name in enumerate(names):
  d.execute('SELECT merkle_lsm_flush(%s::regclass)',(name,))
  d.execute('SET enable_seqscan=off; SET enable_bitmapscan=off')
  rows=d.execute(f'SELECT k,ctid::text,v FROM t{n} WHERE k>0').fetchall()
  assert len(rows)==8000 and len(set(rows))==8000
  d.execute('SET enable_indexscan=off; SET enable_bitmapscan=on')
  assert collections.Counter(d.execute(f'SELECT k,ctid::text,v FROM t{n} WHERE k>0').fetchall())==collections.Counter(rows)
  d.execute('RESET enable_seqscan; RESET enable_bitmapscan; RESET enable_indexscan')
 rec('multi_populated',database=db,indexes=len(names),components=d.execute('SELECT index,level,slot,role,merge_count FROM merkle_lsm_status ORDER BY index,level,slot').fetchall())
 d.close()
with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:list(pool.map(populate,['multi_lsm_a','multi_lsm_b']))
# pg_dump must omit internal components: only user indexes are recreated.
with (O/'multi_dump.sql').open('w') as f:subprocess.run([B/'pg_dump','-d','multi_lsm_a'],stdout=f,stderr=subprocess.PIPE,env=env,check=True)
if c.execute("SELECT 1 FROM pg_database WHERE datname='multi_lsm_restore'").fetchone():
 c.execute('ALTER DATABASE multi_lsm_restore ALLOW_CONNECTIONS false')
 c.execute("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname='multi_lsm_restore'");time.sleep(.4)
 c.execute('DROP DATABASE multi_lsm_restore')
c.execute('CREATE DATABASE multi_lsm_restore')
with (O/'multi_restore.log').open('w') as f:subprocess.run([B/'psql','-X','-v','ON_ERROR_STOP=1','-d','multi_lsm_restore','-f',O/'multi_dump.sql'],stdout=f,stderr=f,env=env,check=True)
d=conn('multi_lsm_restore')
assert d.execute("SELECT count(*) FROM pg_class c JOIN pg_am a ON a.oid=c.relam WHERE a.amname='merkle_lsm'").fetchone()[0]==9
for n in range(9):assert d.execute(f'SELECT count(*),sum(k) FROM t{n}').fetchone()==(8000,32004000)
rec('dump_restore_PASS');d.close()
# DROP INDEX, DROP TABLE, and DROP EXTENSION CASCADE follow pg_depend edges.
d=conn('multi_lsm_b')
components=[x[0] for x in d.execute("SELECT relation::oid FROM merkle_lsm_components('lookup_1') WHERE role<>'base'").fetchall()]
d.execute('DROP INDEX lookup_1')
assert d.execute('SELECT count(*) FROM pg_class WHERE oid=ANY(%s)',(components,)).fetchone()[0]==0
d.execute('DROP TABLE t2')
d.execute('DROP EXTENSION merkle_lsm CASCADE')
assert d.execute("SELECT count(*) FROM pg_class WHERE relname LIKE 'ml_%'").fetchone()[0]==0
rec('drop_PASS');d.close()
rec('PASS')
