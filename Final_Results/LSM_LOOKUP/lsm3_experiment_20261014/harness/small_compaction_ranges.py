import json,psycopg
from pathlib import Path
O=Path('/home/neel/claude_checks/lsm_lookup_20261014/small_compaction')
c=psycopg.connect('host=127.0.0.1 port=5488 user=postgres dbname=postgres',autocommit=True)
c.execute('SET enable_seqscan=off');c.execute('SET enable_bitmapscan=off');c.execute('SET enable_indexonlyscan=off')
rows=[];total=0
for part in range(4):
 results={};plans={}
 for schema in ['b','l']:
  where="WHERE merkle_partition_for_hash(merkle_key_hash(ycsb_key),4)="+str(part)+" AND merkle_key_hash(ycsb_key) BETWEEN decode('0000000000000000','hex') AND decode('ffffffffffffffff','hex')"
  stmt=f'SELECT count(*),min(ycsb_key),max(ycsb_key),sum(ycsb_key),min(v),max(v) FROM {schema}.usertable '+where
  results[schema]=c.execute(stmt).fetchone();plans[schema]=c.execute('EXPLAIN(COSTS OFF) '+stmt).fetchall();assert 'usertable_merkle_lookup_idx' in str(plans[schema])
 assert results['b']==results['l'];total+=results['l'][0];rows.append(dict(partition=part,result=results['l'],plans=plans))
assert total==70000,total
(O/'post_restart_range_check.json').write_text(json.dumps(dict(pass_check=True,total_rows=total,ranges=rows),indent=2,default=str)+'\n')
