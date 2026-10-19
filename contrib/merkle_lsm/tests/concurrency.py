# Run on the isolated lab host only; see tests/README.md.
import collections,concurrent.futures,json,os,subprocess,time,random
from pathlib import Path
import psycopg
R=Path(os.environ.get('MERKLE_LSM_TEST_ROOT',str(Path.home()/'claude_checks/lsm_integration_20261019')));O=R/'evidence'
DSN=f'host={R}/socket port=5494 user=postgres dbname=postgres client_encoding=UTF8'
def conn():return psycopg.connect(DSN,autocommit=True)
c=conn()
def q(s,p=None):return c.execute(s,p)
def rec(label,**v):
 print(label,json.dumps(v,default=str),flush=True)
 with (O/'concurrency_validation.jsonl').open('a') as f:f.write(json.dumps(dict(label=label,**v),default=str)+'\n')
# Deliberate write skew across a real source relfilenode replacement.
q('DROP TABLE IF EXISTS skew');q('CREATE TABLE skew(k int,oncall bool)')
q('CREATE INDEX skew_idx ON skew USING merkle_lsm(k) WITH(top_index_size=1024)')
q('INSERT INTO skew VALUES(1,true),(2,true)')
a,b=conn(),conn()
for d in [a,b]:
 d.execute('BEGIN ISOLATION LEVEL SERIALIZABLE');d.execute('SET LOCAL enable_seqscan=off; SET LOCAL enable_bitmapscan=off')
 assert d.execute('SELECT count(*) FROM skew WHERE k BETWEEN 1 AND 2 AND oncall').fetchone()[0]==2
plan=a.execute('EXPLAIN (COSTS OFF) SELECT * FROM skew WHERE k BETWEEN 1 AND 2 AND oncall').fetchall();assert 'skew_idx' in str(plan)
b.execute('UPDATE skew SET oncall=false WHERE k=2');b.execute('COMMIT');b.close()
before=q("SELECT sum(merge_count) FROM merkle_lsm_components('skew_idx')").fetchone()[0]
q("SELECT merkle_lsm_flush('skew_idx')")
after=q("SELECT sum(merge_count) FROM merkle_lsm_components('skew_idx')").fetchone()[0];assert after>before
try:
 a.execute('UPDATE skew SET oncall=false WHERE k=1');a.execute('COMMIT')
 raise AssertionError('write skew was allowed')
except psycopg.errors.SerializationFailure:
 a.execute('ROLLBACK');rec('write_skew_PASS',sqlstate='40001',merge_count_before=before,merge_count_after=after,plan=plan)
a.close()
# Index-range phantoms: each transaction inserts into the range the other read,
# with and without a real L0 rotation/copy/replacement between read and insert.
for drain in (False,True):
 q('DROP TABLE IF EXISTS phantom');q('CREATE TABLE phantom(k int)')
 q('CREATE INDEX phantom_idx ON phantom USING merkle_lsm(k) WITH(top_index_size=1024)')
 q('INSERT INTO phantom SELECT generate_series(1,2000)');q("SELECT merkle_lsm_flush('phantom_idx')")
 q('INSERT INTO phantom SELECT generate_series(5001,5100)');q('VACUUM phantom')
 a,b=conn(),conn()
 for d,lo in ((a,1),(b,1001)):
  d.execute('BEGIN ISOLATION LEVEL SERIALIZABLE');d.execute('SET LOCAL enable_seqscan=off; SET LOCAL enable_bitmapscan=off')
  assert d.execute('SELECT count(*) FROM phantom WHERE k BETWEEN %s AND %s',(lo,lo+999)).fetchone()[0]==1000
  d.execute('SELECT 1')  # drop the finished portal; it pins the components
 before=q("SELECT sum(merge_count) FROM merkle_lsm_components('phantom_idx')").fetchone()[0]
 if drain:
  q("SELECT merkle_lsm_flush('phantom_idx')")
  assert q("SELECT sum(merge_count) FROM merkle_lsm_components('phantom_idx')").fetchone()[0]>before
 failed=0
 for d,k in ((a,1500),(b,500)):
  try:d.execute('INSERT INTO phantom VALUES(%s)',(k,));d.execute('COMMIT')
  except psycopg.errors.SerializationFailure:d.execute('ROLLBACK');failed+=1
 a.close();b.close()
 assert failed==1,('phantom write skew was allowed',drain)
 rec('phantom_PASS',drain=drain,sqlstate='40001')
# Mix updates, key changes, inserts, deletes, readers and VACUUM with retries.
q('DROP TABLE IF EXISTS concurrent_lsm');q('CREATE TABLE concurrent_lsm(k int,v text,hot int) WITH(fillfactor=50)')
q('CREATE INDEX concurrent_lsm_idx ON concurrent_lsm USING merkle_lsm(k) INCLUDE(v) WITH(levels=4,top_index_size=16,size_ratio=2)')
q("INSERT INTO concurrent_lsm SELECT i,'seed-'||i,0 FROM generate_series(1,1000)i")
def client(n):
 d=conn();retries=0
 for turn in range(250):
  for attempt in range(1000):
   try:
    d.execute('BEGIN ISOLATION LEVEL SERIALIZABLE')
    d.execute('SET LOCAL enable_seqscan=off')
    count=d.execute('SELECT count(*) FROM concurrent_lsm WHERE k BETWEEN 1 AND 100000').fetchone()[0]
    if n<4:
     key=10000+n*1000+turn
     d.execute('INSERT INTO concurrent_lsm VALUES(%s,%s,0)',(key,f'row-{n}-{turn}'))
     d.execute('UPDATE concurrent_lsm SET hot=hot+1 WHERE k=%s',((turn%100)+1,))
     if turn%3==0:d.execute("UPDATE concurrent_lsm SET v=v||'u' WHERE k=%s",((turn%100)+1,))
     if turn%4==0:d.execute('DELETE FROM concurrent_lsm WHERE k=%s',(101+turn%100,))
    d.execute('COMMIT');break
   except psycopg.errors.SerializationFailure:
    d.execute('ROLLBACK');retries+=1;time.sleep(random.uniform(.001,min(.1,.002*(attempt+2))))
  else:raise AssertionError('retry exhausted')
 d.close();return dict(client=n,retries=retries,transactions=250)
def vacuum():
 d=conn()
 for _ in range(5):d.execute('VACUUM concurrent_lsm');time.sleep(.05)
 d.close()
with concurrent.futures.ThreadPoolExecutor(max_workers=7) as pool:
 fs=[pool.submit(client,n) for n in range(6)]+[pool.submit(vacuum)]
 results=[f.result() for f in fs]
q("SELECT merkle_lsm_flush('concurrent_lsm_idx')")
q('SET enable_indexscan=off; SET enable_bitmapscan=off')
truth=collections.Counter(q('SELECT k,ctid::text,v FROM concurrent_lsm WHERE k>0').fetchall())
q('SET enable_seqscan=off; SET enable_indexscan=on')
assert collections.Counter(q('SELECT k,ctid::text,v FROM concurrent_lsm WHERE k>0').fetchall())==truth
q('SET enable_indexscan=off; SET enable_bitmapscan=on')
assert collections.Counter(q('SELECT k,ctid::text,v FROM concurrent_lsm WHERE k>0').fetchall())==truth
q('SET enable_indexscan=on; SET enable_bitmapscan=off; SET enable_indexonlyscan=on')
assert collections.Counter(q('SELECT k,v FROM concurrent_lsm WHERE k>0').fetchall())==collections.Counter((k,v) for k,t,v in truth.elements())
q('RESET enable_seqscan; RESET enable_indexscan; RESET enable_bitmapscan; RESET enable_indexonlyscan')
rec('concurrency_PASS',clients=results,rows=sum(truth.values()),components=q("SELECT * FROM merkle_lsm_components('concurrent_lsm_idx')").fetchall())
# This fork's pgbench predates --max-tries. Use disjoint writer ranges and
# read-only clients for a pgbench smoke; the Python run above retries 40001.
p=R/'scratch/concurrent.pgbench'
p.write_text('BEGIN ISOLATION LEVEL SERIALIZABLE;\nSET LOCAL enable_seqscan=off;\nSELECT count(*) FROM concurrent_lsm WHERE k BETWEEN 1 AND 100000;\nCOMMIT;\n')
env=dict(os.environ,PGHOST=str(R/'socket'),PGPORT='5494',PGUSER='postgres',PGDATABASE='postgres',TMPDIR=str(R/'scratch'),LD_LIBRARY_PATH=str(R/'install/lib'))
with (O/'pgbench.log').open('w') as f:
 result=subprocess.run([R/'install/bin/pgbench','-n','-c','4','-j','4','-t','100','-f',p],stdout=f,stderr=f,env=env)
assert result.returncode==0
rec('PASS')
