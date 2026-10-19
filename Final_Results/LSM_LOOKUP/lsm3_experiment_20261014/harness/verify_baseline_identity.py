#!/usr/bin/env python3
"""Read-only .247 cluster-control/size inventory; never starts PostgreSQL."""
import hashlib,json,subprocess
from pathlib import Path
R=Path('/home/neel/claude_checks/lsm_lookup_20261014');D=Path('/home/neel/ariabc_data/lsm_lookup_20261014');P=Path('/home/neel/ariabc_data/paper_20261010_oom')
subprocess.run(['free','-g'],check=True,stdout=(R/'evidence/final_identity_free_g.log').open('w'))
def identity(d):
 text=subprocess.check_output([str(R/'install/bin/pg_controldata'),str(d)],text=True)
 a={k.strip():v.strip() for k,v in (line.split(':',1) for line in text.splitlines() if ':' in line)}
 assert a['Database cluster state']=='shut down'
 return dict(db_system_id=a['Database system identifier'],cluster_state=a['Database cluster state'],checkpoint_lsn=a['Latest checkpoint location'],pg_control_sha256=hashlib.sha256((d/'global/pg_control').read_bytes()).hexdigest(),pg_version_sha256=hashlib.sha256((d/'PG_VERSION').read_bytes()).hexdigest(),file_count=sum(1 for p in d.rglob('*') if p.is_file()),total_bytes=int(subprocess.check_output(['du','-sb',str(d)],text=True).split()[0]))
published={name:identity(P/name) for name in ['pgdata_base_v2_ff90','pgdata_base_v2_ff90_plain']}
assert published==json.loads((R/'baselines/source_identity_before.json').read_text())
own={name:identity(D/name) for name in ['base_ff90','base_ff90_plain','base_ff90_lsm']}
for name,value in own.items():
 manifest=json.loads((R/'baselines_normalized'/f'{name}.json').read_text())
 assert value=={k:manifest[k] for k in value},name
result=dict(published=published,own=own,pass_check=True,scope='metadata/control/size identities, not a full content rehash')
(R/'evidence/final_baseline_identities.json').write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps(dict(pass_check=True,published=len(published),own=len(own))))
