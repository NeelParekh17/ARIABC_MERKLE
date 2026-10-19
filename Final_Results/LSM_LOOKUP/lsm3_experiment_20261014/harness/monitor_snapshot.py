#!/usr/bin/env python3
"""Read-only benchmark observer; appends its own supervision evidence."""
import csv,datetime,json
from pathlib import Path
R=Path('/home/neel/claude_ctl/lsm_lookup_20261014/results')
paths=sorted((R/'ff90').glob('run_*/summary.csv'))
rows=list(csv.DictReader(paths[-1].open())) if paths else []
log=(R/'ff90.log').read_text().splitlines()
active=next((x for x in reversed(log) if x.startswith('Running ')), '')
status=(R/'status.txt').read_text().splitlines()[-1]
attempts=list(csv.DictReader((paths[-1].parent/'all_attempts.csv').open())) if paths and (paths[-1].parent/'all_attempts.csv').exists() else []
a=dict(utc=datetime.datetime.now(datetime.timezone.utc).isoformat(timespec='seconds'),accepted=len(rows),active=active,latest=log[-1] if log else '',status=status,done=(R/'stages/ff90.OK').exists(),noise_exclusions=sum(x.get('noise_excluded','').lower()=='true' for x in attempts))
with (R/'supervision.jsonl').open('a') as f:f.write(json.dumps(a)+'\n')
print(json.dumps(a))
