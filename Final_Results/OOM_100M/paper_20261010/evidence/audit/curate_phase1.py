#!/usr/bin/env python3
"""Snapshot small phase-1 evidence; keep pgdata and bulk server logs remote."""
import hashlib
import json
import shutil
from pathlib import Path

root = Path('/home/neel/claude_checks/paper_20261010_oom/results')
audit = root / 'phase1_audit'
report = json.loads((audit / 'acceptance.json').read_text())
out = audit / 'curated'
out.mkdir(exist_ok=False)

def copy(source, relative):
    destination = out / relative
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, destination)

for source in audit.iterdir():
    if source.is_file():
        copy(source, Path('audit') / source.name)
for name in ('generation.log', 'verification.log', 'space_recheck.json',
             'preflight.log', 'workload_hashes.log', 'status.txt', 'commands.log',
             'smoke.log', 'campaign.log'):
    copy(root / name, Path('controller') / name)
for dirname in ('baseline_evidence', 'provenance'):
    for source in (root / dirname).rglob('*'):
        if source.is_file() and source.name != 'source.tar.gz':
            copy(source, source.relative_to(root))
for group, dataset in report['datasets'].items():
    run = Path(dataset['run_dir'])
    for name in ('campaign.json', 'preflight.txt', 'device_calibration.json', 'all_attempts.csv'):
        if (run / name).is_file():
            copy(run / name, Path(group) / name)
    for source in run.glob('device_calibration_*.json'):
        copy(source, Path(group) / source.name)
    for record in dataset['records']:
        case = Path(record['artifact_dir'])
        for name in ('result.json', 'setup.json', 'gateway.log', 'merkle_verify.txt',
                     'noise_qualification.json', 'det_counters.json', 'io.json',
                     'checkpoint.json', 'snapshots.json', 'telemetry.jsonl', 'gateway_telemetry.jsonl',
                     'pg_stats_before.json', 'pg_stats_after.json'):
            if (case / name).is_file():
                copy(case / name, Path(group) / case.name / name)
lines = []
for source in sorted(out.rglob('*')):
    if source.is_file():
        lines.append(hashlib.sha256(source.read_bytes()).hexdigest() + '  ' + str(source.relative_to(out)))
(out / 'SHA256SUMS').write_text('\n'.join(lines) + '\n')
print(out)
