#!/usr/bin/env python3
""".247 only: equalize visibility-map/hint state before any timed run."""
import json,os,shutil,time
from pathlib import Path
import baseline_tools as T
R=T.R;D=T.D;O=R/'baselines_normalized';O.mkdir(exist_ok=True);T.O=O
oldroot=json.loads((R/'baselines/base_ff90.json').read_text())['root']
try:
 for name,merkle in [('base_ff90_plain',False),('base_ff90',True),('base_ff90_lsm',True)]:
  d=D/name;manifest=D/('.ariabc_golden_manifest_'+name+'.json');before=json.loads(manifest.read_text())
  if (O/(name+'.json')).exists():continue
  T.status('NORMALIZE '+name)
  (O/(name+'_before.json')).write_text(json.dumps(before,indent=2)+'\n')
  T.start(d)
  if name!='base_ff90_lsm':T.sql('VACUUM ANALYZE usertable')
  cat=json.loads(T.sql("SELECT json_build_object('relpages',relpages,'relallvisible',relallvisible,'relfrozenxid',relfrozenxid::text,'checksums',current_setting('data_checksums'),'wal_log_hints',current_setting('wal_log_hints')) FROM pg_class WHERE oid='usertable'::regclass"))
  assert cat['relallvisible']==cat['relpages'],cat
  (O/(name+'_visibility.json')).write_text(json.dumps(cat,indent=2)+'\n')
  if name!='base_ff90_lsm':new=T.verify(d,merkle,90)
  else:
   assert T.sql("SELECT merkle_root_hash('usertable')")==oldroot
   T.sql('CHECKPOINT');T.stop();new=dict(before,**T.identity(d));manifest.write_text(json.dumps(new,indent=2)+'\n');(O/(name+'.json')).write_text(json.dumps(new,indent=2)+'\n')
  if merkle:assert new['root']==oldroot
  assert new['heap_bytes']==before['heap_bytes'] and new['index_bytes']==before['index_bytes'] and new['relpages']==before['relpages']
 T.status('NORMALIZED_FF90_BASELINES_PASS_STOPPED')
except BaseException as e:
 (O/'FAILED.txt').write_text(repr(e)+'\n')
 try:T.stop()
 except Exception as x:(O/'cleanup_failure.txt').write_text(repr(x)+'\n')
 T.status('FAILED '+repr(e));raise
