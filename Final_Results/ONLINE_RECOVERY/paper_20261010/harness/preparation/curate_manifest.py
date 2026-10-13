#!/usr/bin/env python3
"""Write checksums for completed run artifacts without bulk transaction traces."""
import hashlib
from pathlib import Path
B=Path('/home/neel/ariabc_data/paper_20261010_recov')
root=B/'repo/scripts/bench_full_results'
for d in sorted(root.glob('cluster4_paper_20261010_recov_*')):
    if not (d/'acceptance.json').exists():continue
    lines=[]
    for p in sorted(d.rglob('*')):
        if not p.is_file() or p.name=='CURATED.sha256' or p.name.startswith(('bcdb_phase_trace_node','tx_latency')):continue
        lines.append(hashlib.sha256(p.read_bytes()).hexdigest()+'  '+p.relative_to(d).as_posix())
    (d/'CURATED.sha256').write_text('\n'.join(lines)+'\n')
print('completed curated manifests')
