"""Run only on .247; every assertion has retained SQL/plan evidence."""
import concurrent.futures, collections, json, os, subprocess, time
from pathlib import Path
import psycopg
R=Path(os.environ.get('MERKLE_LSM_TEST_ROOT',str(Path.home()/'claude_checks/lsm_integration_20261019')))
DSN=f'host={R}/socket port=5494 user=postgres client_encoding=UTF8 dbname=postgres'
OUT=R/'evidence'
def conn(db='postgres'):
 return psycopg.connect(DSN.replace('dbname=postgres',f'dbname={db}'),autocommit=True)
c=conn()
c.execute("SET statement_timeout='90s'")
def record(label,**values):
 print(label,json.dumps(values,default=str),flush=True)
 with (OUT/'validation.jsonl').open('a') as f:f.write(json.dumps(dict(label=label,**values),default=str)+'\n')
def query(s,p=None):return c.execute(s,p)
def audit(table,label):
 query('RESET enable_seqscan; RESET enable_indexscan; RESET enable_indexonlyscan; RESET enable_bitmapscan')
 query('SET enable_indexscan=off; SET enable_bitmapscan=off')
 truth=collections.Counter(query(f'SELECT k,ctid::text,v FROM {table} WHERE k IS NOT NULL').fetchall())
 only_truth=collections.Counter(query(f'SELECT k,v FROM {table} WHERE k IS NOT NULL').fetchall())
 plans={}
 for kind in ['index','bitmap','only']:
  query('SET enable_seqscan=off')
  query('SET enable_indexscan='+('off' if kind=='bitmap' else 'on'))
  query('SET enable_bitmapscan='+('on' if kind=='bitmap' else 'off'))
  query('SET enable_indexonlyscan='+('on' if kind=='only' else 'off'))
  sql=f'SELECT k,v FROM {table} WHERE k IS NOT NULL' if kind=='only' else f'SELECT k,ctid::text,v FROM {table} WHERE k IS NOT NULL'
  plan='\n'.join(x[0] for x in query('EXPLAIN (COSTS OFF) '+sql).fetchall());plans[kind]=plan
  expected_node={'index':'Index Scan','bitmap':'Bitmap Heap Scan','only':'Index Only Scan'}[kind]
  assert expected_node in plan,(kind,plan)
  got=collections.Counter(query(sql).fetchall());assert got==(only_truth if kind=='only' else truth),(table,kind,len(got),len(truth))
  if kind!='only': assert all(n==1 for n in got.values()),(table,kind,'duplicate key/TID')
 query('RESET enable_seqscan; RESET enable_indexscan; RESET enable_indexonlyscan; RESET enable_bitmapscan')
 record(label,table=table,rows=sum(truth.values()),plans=plans)
def flush(index):query('SELECT merkle_lsm_flush(%s::regclass)',(index,))
query('CREATE EXTENSION IF NOT EXISTS merkle_lsm')
query('DROP TABLE IF EXISTS smoke')
for levels in [2,3,4]:
 table=f'level{levels}';idx=table+'_idx'
 query(f'DROP TABLE IF EXISTS {table}');query(f'CREATE TABLE {table}(k int,v text) WITH (fillfactor=50)')
 query(f'CREATE INDEX {idx} ON {table} USING merkle_lsm(k) INCLUDE(v) WITH (levels={levels},top_index_size=16,size_ratio=2)')
 for batch in range(120):
  query(f"INSERT INTO {table} SELECT i,'row-'||i FROM generate_series({batch*1000+1},{(batch+1)*1000}) i")
  time.sleep(.08)
  if batch in (7,15):audit(table,f'levels{levels}_interleaved{batch}')
 audit(table,f'levels{levels}_insert')
 query(f"UPDATE {table} SET v='hot-'||k WHERE k%7=0")
 # INCLUDE(v) forces these updates to be non-HOT; create a second nonindexed field to exercise HOT.
 query(f'ALTER TABLE {table} ADD COLUMN hot int DEFAULT 0')
 query(f'UPDATE {table} SET hot=hot+1 WHERE k%3=0')
 query(f'UPDATE {table} SET k=k+100000 WHERE k%11=0')
 audit(table,f'levels{levels}_updates')
 query(f'DELETE FROM {table} WHERE k%5=0')
 query(f'VACUUM (ANALYZE) {table}')
 audit(table,f'levels{levels}_vacuum')
 flush(idx);audit(table,f'levels{levels}_drained')
 comps=query('SELECT * FROM merkle_lsm_components(%s::regclass)',(idx,)).fetchall()
 assert all(x[5]==0 for x in comps if x[0]!=levels-1),comps
 assert all(x[7]>=3 for x in comps if x[0]!=levels-1),comps
 record(f'levels{levels}_components',components=comps)
 query(f'REINDEX INDEX {idx}');audit(table,f'levels{levels}_reindex')
 with c.cursor().copy(f'COPY {table}(k,v) FROM STDIN') as cp:
  for j in range(20000):
   cp.write_row((200000+j,'copy-'+str(j)))
   if j%1000==0:time.sleep(.03)
 audit(table,f'levels{levels}_copy')
 flush(idx);audit(table,f'levels{levels}_copy_drained')
record('levels_PASS')
