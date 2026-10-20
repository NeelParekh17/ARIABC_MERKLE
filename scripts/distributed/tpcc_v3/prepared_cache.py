#!/usr/bin/env python3
"""Campaign-owned, stopped initial PG images; all restored state is verified.

No active database copy. No hard links. Keep source/binary/schema provenance and
full initial checksum. Images contain only WAL >= clean-shutdown REDO segment.
"""
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tarfile

def sha(path):
    h=hashlib.sha256()
    with path.open('rb') as f:
        for chunk in iter(lambda:f.read(1024*1024),b''):h.update(chunk)
    return h.hexdigest()

def main():
    operation,run,archive,w,kind=sys.argv[1:]
    run,archive=Path(run),Path(archive)
    root=Path(os.environ['CAMPAIGN_HOME'])
    assert run.is_relative_to(root) and archive.parent in (root/'prepared_20261011',root/'prepared')
    manifest=archive.with_suffix('.manifest.json')
    src=Path(os.environ['SRC']); inst=Path(os.environ['INST'])
    identity=dict(warehouses=int(w),kind=kind,seed=int(os.environ['SEED']),
                  geometry={k:os.environ[k] for k in ('FILLFACTOR','SPLIT','MERGE','P','PKC','SUB')},
                  postgres_sha256=sha(inst/'bin/postgres'),
                  loader_sha256=sha(src/'scripts/tpcc_v3/tpcc_load.py'),
                  schema_sha256=sha(src/'scripts/tpcc_v3/tpcc_schema.sql'),
                  procs_sha256=sha(src/'scripts/tpcc_v3/tpcc_procs.sql'))
    pgdata=run/'pgdata'
    if operation=='save':
        assert not archive.exists() and not (pgdata/'postmaster.pid').exists()
        control=subprocess.check_output([str(inst/'bin/pg_controldata'),str(pgdata)],text=True)
        assert re.search(r'Database cluster state:\s+shut down\s*$',control,re.M)
        redo=re.search(r'Latest checkpoint.s REDO WAL file:\s+(\w+)',control)[1]
        tmp=archive.with_suffix('.new')
        with tarfile.open(tmp,'w:gz',compresslevel=1) as tf:
            tf.add(pgdata,arcname='pgdata',recursive=False)
            for p in sorted(pgdata.rglob('*')):
                # Omit obsolete WAL from a clean shutdown, retain REDO and all
                # later files. The measured clusters retain full configured WAL.
                if p.parent.name=='pg_wal' and re.fullmatch(r'[0-9A-F]{24}',p.name) and p.name<redo:continue
                tf.add(p,arcname=str(p.relative_to(run)),recursive=False)
        tmp.replace(archive)
        data=dict(identity=identity,initial_state=(run/'initial_state.hash').read_text(),
                  archive_sha256=sha(archive),clean_shutdown_redo_file=redo,source_run=str(run),control=control)
        manifest.write_text(json.dumps(data,indent=2)+'\n')
    elif operation in ('restore','verify-state'):
        data=json.loads(manifest.read_text())
        assert data['identity']==identity,'prepared initial image provenance changed'
        if operation=='restore':
            assert not pgdata.exists() and sha(archive)==data['archive_sha256']
            with tarfile.open(archive) as tf:
                # All members originate in a campaign-owned PG directory.
                assert all(not m.issym() and not m.islnk() and Path(m.name).parts[0]=='pgdata' and '..' not in Path(m.name).parts for m in tf.getmembers())
                tf.extractall(run,filter='data')
            # Legacy initial archives omitted the root directory member; its
            # implicit extraction mode follows umask and is too permissive for
            # PostgreSQL. Enforce initdb's private root mode for every restore.
            pgdata.chmod(0o700)
            (run/'prepared_image.json').write_text(json.dumps(data,indent=2)+'\n')
        else:
            assert (run/'initial_state.hash').read_text()==data['initial_state'],'restored initial state mismatch'
    else:raise ValueError(operation)

if __name__=='__main__':main()
