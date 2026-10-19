import json,psycopg
from pathlib import Path
r=Path('/home/neel/claude_checks/lsm_lookup_20261014/small')
c=psycopg.connect('host=127.0.0.1 port=5488 user=postgres dbname=postgres',autocommit=True)
c.execute('SET enable_hashjoin=off');c.execute('SET enable_mergejoin=off');c.execute('SET enable_seqscan=off')
cte="WITH b(p,lo,hi) AS (SELECT u.p,u.lo,merkle_node_upper_bound(u.lo,u.l) FROM unnest(ARRAY[0,1]::int4[],ARRAY[decode('0000000000000000','hex'),decode('8000000000000000','hex')]::bytea[],ARRAY[1,1]::int4[]) u(p,lo,l)) "
query="SELECT DISTINCT ON (t.ycsb_key) t.* FROM b JOIN {s}.usertable t ON merkle_partition_for_hash(merkle_key_hash(t.ycsb_key),4)=b.p AND merkle_key_hash(t.ycsb_key) BETWEEN b.lo AND b.hi ORDER BY t.ycsb_key"
results={};plans={}
for schema in ['b','l']:
 stmt=cte+query.format(s=schema)
 plans[schema]=c.execute('EXPLAIN (COSTS OFF) '+stmt).fetchall()
 assert 'usertable_merkle_lookup_idx' in str(plans[schema])
 with c.cursor().copy('COPY ('+stmt+') TO STDOUT') as cp:results[schema]=b''.join(bytes(row) for row in cp)
assert results['b']==results['l']
(r/'repair_copy.json').write_text(json.dumps(dict(pass_check=True,plans=plans,copy_bytes=len(results['l']),rows=len(results['l'].splitlines())),indent=2)+'\n')
