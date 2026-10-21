"""Audit accepted smoke or D-group artifacts, including per-case sampled WAL."""
import argparse
import json
from pathlib import Path
import subprocess

ROOT = Path('/home/neel/claude_ctl/ycsb_matrix_20261020')
DBROOT = '/home/neel/claude_checks/ycsb_matrix_20261020'
parser = argparse.ArgumentParser()
parser.add_argument('stage', choices=['smoke', 'matrix'])
args = parser.parse_args()
out = ROOT / 'results' / args.stage
sample = subprocess.run(['ssh', '-o', 'ControlMaster=no', 'neel@10.129.148.247',
                         f'cat {DBROOT}/evidence/wal_sampler.jsonl'],
                        text=True, capture_output=True, check=True).stdout
(ROOT / 'evidence' / f'{args.stage}_wal_sampler.jsonl').write_text(sample)
samples = [json.loads(line) for line in sample.splitlines()]
accepted = []
workloads = {}
timings = []
errors = []
for kind in ('plain', 'lsm', 'btree'):
    campaign_root = out / ('d_' + kind)
    campaigns = sorted(campaign_root.glob('run_*/campaign.json'))
    if len(campaigns) != 1:
        errors.append(f'{kind}: expected one campaign, found {len(campaigns)}')
        continue
    campaign = campaigns[0].parent
    for timing in campaign.glob('campaign_setup_timing_*.json'):
        timings.append(dict(kind=kind, **json.loads(timing.read_text())))
    for result_file in campaign.glob('*/result.json'):
        case = result_file.parent
        result = json.loads(result_file.read_text())
        row = result['row']
        if row['workload'] != 'd':
            continue
        if (case / 'FAILED.txt').exists() or (case / 'cleanup_errors.txt').exists():
            errors.append(f'{case}: failure evidence present')
        key = (row['workload'], row['skew'])
        digest = row['workload_sha256']
        workloads.setdefault(key, set()).add(digest)
        if row['isolation'] != 'serializable' or row['shared_buffers'] != '32MB':
            errors.append(f'{case}: incorrect isolation/buffers')
        for metric in ('retry_exhausted', 'permanent_failures', 'divergence_count'):
            if row[metric] != 0:
                errors.append(f'{case}: {metric}={row[metric]}')
        expected = 20000 if args.stage == 'smoke' else 2400000
        if row['total_queries'] != expected or row['validated_completed_queries'] != expected:
            errors.append(f'{case}: incomplete workload')
        if row['mode'] == 'bcdb_merkle' and (case / 'merkle_verify.txt').read_text().strip() != 't':
            errors.append(f'{case}: Merkle verification missing')
        if kind == 'lsm' and row['lsm_drain_ms'] <= 0:
            errors.append(f'{case}: LSM drain missing')
        wal = json.loads((case / 'wal.json').read_text())
        post_checkpoint = json.loads((case / 'wal_post_checkpoint.json').read_text())
        def lsn(value):
            high, low = value.split('/')
            return (int(high, 16) << 32) + int(low, 16)
        inclusive_wal = lsn(post_checkpoint['lsn']) - lsn(wal['before']['lsn'])
        per_case = [r for r in samples if r.get('case_dir') == str(case)
                    and r.get('phase') == 'measurement' and 'lsn' in r]
        sample_errors = [r for r in samples if r.get('case_dir') == str(case)
                         and r.get('phase') == 'measurement' and 'error' in r]
        if sample_errors:
            errors.append(f'{case}: sampler errors during measurement')
        if not per_case:
            errors.append(f'{case}: no 1 Hz measurement WAL samples')
        if kind == 'lsm' and not any(r.get('stats') is not None for r in per_case):
            errors.append(f'{case}: no LSM merge stats')
        if kind != 'lsm' and any(r.get('stats') is not None for r in per_case):
            errors.append(f'{case}: unexpected LSM stats')
        if wal['wal_bytes'] != row['wal_bytes'] or row['wal_bytes'] < 0:
            errors.append(f'{case}: inconsistent WAL bytes')
        accepted.append(dict(kind=kind, **row, wal_samples=len(per_case),
                             wal_through_checkpoint_bytes_per_statement=inclusive_wal / expected,
                             sampler_errors=len(sample_errors),
                             sampler_example=per_case[0] if per_case else None))
for combo, hashes in workloads.items():
    if len(hashes) != 1:
        errors.append(f'{combo}: workload hash mismatch')
expected_cases = 4 if args.stage == 'smoke' else 16
if len(accepted) != expected_cases:
    errors.append(f'Expected {expected_cases} accepted cases, found {len(accepted)}')
skews = [.99] if args.stage == 'smoke' else [0., .5, .99, 1.2]
expected_keys = {(skew, kind, mode) for skew in skews for kind, mode in
                 [('plain', 'pg'), ('plain', 'bcdb_det'),
                  ('lsm', 'bcdb_merkle'), ('btree', 'bcdb_merkle')]}
actual_keys = [(float(row['skew']), row['kind'], row['mode']) for row in accepted]
if set(actual_keys) != expected_keys or len(actual_keys) != len(set(actual_keys)):
    errors.append('Missing or duplicate configuration keys')
anchors = dict(pg=30676, bcdb_det=18436, lsm=17576, btree=13091)
for row in accepted:
    if args.stage == 'matrix' and float(row['skew']) == .99:
        mode = row['mode'] if row['kind'] == 'plain' else row['kind']
        row['anchor_tps'] = anchors[mode]
        row['anchor_difference_pct'] = (row['tps'] / anchors[mode] - 1) * 100
report = dict(status='PASS' if not errors else 'FAIL', stage=args.stage,
              errors=errors, setup_timings=timings, cases=accepted)
(ROOT / 'evidence' / f'{args.stage}_audit.json').write_text(json.dumps(report, indent=2) + '\n')
print(json.dumps(report, indent=2))
raise SystemExit(bool(errors))
