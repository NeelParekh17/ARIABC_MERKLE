#!/usr/bin/env python3
""".247 only, after the ff90 grid ends. Reuse only our stopped working copies."""
from baseline_tools import *
import baseline_tools as T

def adopt(src,working,dst):
 assert src.parent==D and working.parent==D and dst.parent==D
 assert not working.is_symlink() and not src.is_symlink() and not dst.exists()
 assert not (working/'postmaster.pid').exists()
 if working.exists():
  run(['rsync','-a','--inplace','--no-whole-file','--delete','--stats',str(src)+'/',str(working)+'/'],dst.name+'_adopt_restore.log')
  run(['diff','-rq','--no-dereference',src,working],dst.name+'_adopt_identity.log')
  working.rename(dst)
 else:copy(src,dst)
try:
 status('FF100_INITIAL_INVENTORY')
 (O/'disk_before.json').write_text(json.dumps(dict(free=shutil.disk_usage(D).free,dirs={p.name:int(S.check_output(['du','-sb',str(p)],text=True).split()[0]) for p in D.iterdir() if p.is_dir()}),indent=2)+'\n')
 for mode,suffix in [('plain','_plain'),('merkle','')]:
  d=D/('base_ff100'+suffix)
  if (O/(d.name+'.json')).exists():continue
  src=D/('base_ff90'+suffix);adopt(src,D/('pgdata_'+mode),d);start(d)
  status('FF100_REWRITE '+mode)
  sql('ALTER TABLE usertable SET (fillfactor=100)');sql('VACUUM FULL ANALYZE usertable')
  if mode=='merkle':sql('REINDEX INDEX usertable_merkle_idx')
  sql('VACUUM ANALYZE usertable')
  verify(d,mode=='merkle',100)
 d=D/'base_ff100_lsm'
 if not (O/(d.name+'.json')).exists():
  adopt(D/'base_ff100',D/'pgdata_lsm',d);start(d);status('FF100_LSM_CREATE')
  sql('CREATE EXTENSION lsm3');sql('DROP INDEX usertable_merkle_lookup_idx')
  sql('CREATE INDEX usertable_merkle_lookup_idx ON usertable USING lsm3(merkle_partition_for_hash(merkle_key_hash(ycsb_key),200),merkle_key_hash(ycsb_key),ycsb_key) WITH (top_index_size=1024)')
  sql('VACUUM ANALYZE usertable');verify(d,True,100)
 roots=[json.loads((R/'baselines/base_ff90.json').read_text())['root'],json.loads((O/'base_ff100.json').read_text())['root'],json.loads((O/'base_ff100_lsm.json').read_text())['root']];assert len(set(roots))==1,roots
 final={p.name:identity(p) for p in D.iterdir() if p.is_dir() and p.name.startswith('base_')}
 (O/'final_identities.json').write_text(json.dumps(final,indent=2)+'\n')
 # Dedup only immutable copies; all working files remain independent.
 run(['python3',R/'dedup_baselines.py'],'immutable_dedup.log')
 # Reserve three working copies, WAL headroom and the mandatory 40GiB.
 needed=sum(v['total_bytes'] for k,v in final.items() if k.startswith('base_ff100'))+20*1024**3+RESERVE
 free=shutil.disk_usage(D).free
 if free<needed:
  (O/'BLOCKED.md').write_text(f'Need {needed} bytes for three working copies + 20GiB WAL + 40GiB reserve; free {free}. Baselines: {json.dumps(final,indent=2)}\n');raise RuntimeError('Insufficient disk for ff100 working copy')
 status('FF100_BASELINES_PASS_STOPPED')
except BaseException as e:
 (O/'FAILED.txt').write_text(repr(e)+'\n')
 try:stop()
 except Exception as x:(O/'cleanup_failure.txt').write_text(repr(x)+'\n')
 status('FAILED '+repr(e));raise
