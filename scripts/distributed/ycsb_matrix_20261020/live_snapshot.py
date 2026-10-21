"""Bounded read-only DB-host snapshot: /proc, device counters, existing log tails."""
import json,time,re
from pathlib import Path
r=Path('/home/neel/claude_checks/ycsb_matrix_20261020');f=r/'evidence/wal_sampler.jsonl'
with f.open('rb')as h:
 h.seek(max(0,f.stat().st_size-8192));ls=h.read().decode(errors='replace').splitlines()
last=json.loads(ls[-1]);p=Path('/home/neel/ariabc_data/lsm_lookup_20261014/server.log')
with p.open('rb')as h:
 h.seek(max(0,p.stat().st_size-8192));tail=h.read().decode(errors='replace')
ids=[int(x)for x in re.findall('single-gateway-direct-(\\d+)',tail)]
print(json.dumps(dict(t=time.time(),sample=last,device_stat=[int(x)for x in Path('/sys/dev/block/259:2/stat').read_text().split()],meminfo=Path('/proc/meminfo').read_text(),vmstat=Path('/proc/vmstat').read_text(),io_pressure=Path('/proc/pressure/io').read_text(),memory_pressure=Path('/proc/pressure/memory').read_text(),server_tail_max_request_id=max(ids)if ids else None,server_log_mtime=p.stat().st_mtime)))
