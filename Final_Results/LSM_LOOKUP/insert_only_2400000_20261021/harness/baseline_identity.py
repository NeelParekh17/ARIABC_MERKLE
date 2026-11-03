#!/usr/bin/env python3
"""Read-only baseline heap/PK identity check. Run on .247 only after gate."""
from collections import defaultdict
import hashlib
import json
from pathlib import Path
import re

POOL=Path('/home/neel/ariabc_data/lsm_lookup_20261014')
OUT=Path('/home/neel/claude_checks/lsm_relation_audit_20261021/evidence/baseline_data_identity.json')
results={}
for kind in ['base_ff90_plain','base_ff90','base_ff90_mlsm']:
    manifest=json.loads((POOL/('.ariabc_golden_manifest_'+kind+'.json')).read_text())
    groups=defaultdict(list)
    for d in (POOL/kind/'base').iterdir():
        if d.is_dir():
            for f in d.iterdir():
                if re.fullmatch(r'\d+(?:\.\d+)?',f.name):
                    groups[(d.name,f.name.split('.')[0])].append(f)
    results[kind]={}
    for role,sizekey in [('heap','heap_bytes'),('primary_key','index_bytes')]:
        matching=[files for files in groups.values() if sum(f.stat().st_size for f in files)==manifest[sizekey]]
        if len(matching)!=1:raise RuntimeError((kind,role,'ambiguous physical size identity',len(matching)))
        entries=[]
        for f in sorted(matching[0],key=lambda p:int(p.name.split('.')[1]) if '.' in p.name else 0):
            h=hashlib.sha256()
            with f.open('rb') as source:
                for chunk in iter(lambda:source.read(8<<20),b''):h.update(chunk)
            entries.append(dict(path=str(f.relative_to(POOL/kind)),bytes=f.stat().st_size,sha256=h.hexdigest()))
        results[kind][role]=entries
        print(kind,role,'hashed_bytes',sum(e['bytes'] for e in entries),flush=True)
reference=results['base_ff90']
identical=all(results[kind]==reference for kind in results)
OUT.write_text(json.dumps(dict(byte_identical=identical,baselines=results),indent=2)+'\n')
if not identical:raise RuntimeError('Baseline heap or PK bytes differ; inspect retained hashes before benchmarking')
print('BASELINE_DATA_BYTE_IDENTICAL')
