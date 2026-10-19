from pathlib import Path
import json,psycopg,os,signal,subprocess
r=Path('/home/neel/claude_checks/lsm_lookup_20261014');out=r/'small_compaction'
c=psycopg.connect('host=127.0.0.1 port=5488 user=postgres dbname=postgres',autocommit=True)
res={}
for s in ['b','l']:
 root,verify=c.execute(f"SELECT merkle_root_hash('{s}.usertable'),merkle_verify('{s}.usertable')").fetchone()
 hashes=c.execute(f'SELECT merkle_tuple_hash(u.*) FROM {s}.usertable u').fetchall()
 oracle=(sum(int.from_bytes(h[0],'little') for h in hashes)%(1<<256)).to_bytes(32,'little').hex()
 assert root==oracle and verify
 res[s]=dict(root=root,oracle=oracle,verify=verify,rows=len(hashes))
assert res['b']==res['l']
(out/'det_audit.json').write_text(json.dumps(res,indent=2)+'\n')
pid=int((out/'det_server.pid').read_text());assert str(r/'cluster/ariabc_pg/build/bin/ariabc_pg_server') in Path(f'/proc/{pid}/cmdline').read_text();os.kill(pid,signal.SIGTERM)
merges=c.execute("SELECT lsm3_get_merge_count('l.usertable_merkle_lookup_idx'),lsm3_top_index_size('l.usertable_merkle_lookup_idx')").fetchone();assert merges[0]>=1; (out/'merges.json').write_text(json.dumps(dict(merge_count=merges[0],active_top_bytes=merges[1]))+'\n');c.close()
subprocess.run([str(r/'install/bin/pg_ctl'),'-D','/home/neel/ariabc_data/lsm_lookup_20261014/small_compaction','-m','fast','-w','stop'],check=True)
(out/'status.txt').write_text('SQL_AND_DET_PASS\n')
