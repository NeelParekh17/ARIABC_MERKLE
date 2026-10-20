#!/usr/bin/env python3
"""Run a balanced, resumable fanout experiment on the lab controller."""
import argparse
import csv
import datetime
import fcntl
import json
import os
from pathlib import Path
import statistics
import subprocess
import time


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--base', type=Path, required=True)
    p.add_argument('--phase', choices=('smoke', 'matrix', 'recheck'), required=True)
    p.add_argument('--resume', action='store_true')
    a = p.parse_args()
    base = a.base.resolve()
    results = base / 'results'
    results.mkdir(exist_ok=True)
    lock = (results / 'controller.lock').open('a')
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    config = json.loads((base / 'campaign.json').read_text())
    modes = config['scenarios']
    plan = []
    for rep in range(1, config['repetitions'] + 1):
        fanouts = config['fanouts']
        fanouts = fanouts[(rep - 1) % len(fanouts):] + fanouts[:(rep - 1) % len(fanouts)]
        for size in config['sizes'][::1 if rep % 2 else -1]:
            for fanout in fanouts:
                rotated = modes[(rep - 1) % len(modes):] + modes[:(rep - 1) % len(modes)]
                plan.extend(dict(rows=size, fanout=fanout, scenario=m, rep=rep) for m in rotated)
    if a.phase == 'smoke':
        # Qualify the widest new fanouts with a full online repair before the matrix.
        plan = [dict(rows=n, fanout=f, scenario='M_mixed', rep=1)
                for n in config['sizes'] for f in (1024, 512)]
    if a.phase == 'recheck':
        # Re-measure selected fault cases (same reps = same fault seeds), interleaved with a control fanout.
        plan = [dict(rows=n, fanout=f, scenario='M_mixed', rep=rep)
                for rep in (1, 2, 3) for n in config['sizes'] for f in config['recheck_fanouts']]
    (results / (a.phase + '_plan.json')).write_text(json.dumps(plan, indent=2) + '\n')
    record_file = results / (a.phase + '_attempts.jsonl')
    records = [json.loads(s) for s in record_file.read_text().splitlines()] if record_file.exists() else []
    if records and not a.resume:
        raise RuntimeError('existing attempts require --resume')

    def status(state, **extra):
        obj = dict(time=datetime.datetime.now(datetime.timezone.utc).isoformat(),
                   state=state, phase=a.phase, pid=os.getpid(), planned=len(plan),
                   accepted=sum(r.get('status') == 'ACCEPTED' for r in records),
                   attempts=len(records), **extra)
        temporary = results / 'status.json.tmp'
        temporary.write_text(json.dumps(obj, indent=2) + '\n')
        temporary.replace(results / 'status.json')
        print(json.dumps(obj), flush=True)

    def tables():
        fields = ['run_id', 'rows', 'fanout', 'scenario', 'rep', 'attempt', 'status',
                  'tps_majority_visible', 'cut_ms', 'repair_ms', 'catchup_ms', 'total_recovery_ms',
                  'event_count', 'mismatched_partitions', 'differing_leaves', 'candidate_rows',
                  'rows_upserted', 'rows_deleted', 'full_copies', 'empty_100ms_buckets',
                  'max_completion_gap_ms', 'phase8_merkle_pass', 'phase8_root', 'case_seconds',
                  'failures', 'noise']
        with (results / (a.phase + '_all_attempts.csv')).open('w', newline='') as f:
            w = csv.DictWriter(f, fields, extrasaction='ignore')
            w.writeheader()
            w.writerows(records)
        aggregate = []
        for n in config['sizes']:
            for fanout in config['fanouts']:
                baseline = [float(r['tps_majority_visible']) for r in records
                            if r.get('status') == 'ACCEPTED' and r.get('rows') == n
                            and r.get('fanout') == fanout and r.get('scenario') == 'A']
                for mode in modes:
                    good = [r for r in records if r.get('status') == 'ACCEPTED'
                            and r.get('rows') == n and r.get('fanout') == fanout
                            and r.get('scenario') == mode]
                    if not good:
                        continue
                    vals = [float(r['tps_majority_visible']) for r in good]
                    row = dict(rows=n, fanout=fanout, scenario=mode, n=len(good),
                               median_tps=statistics.median(vals), min_tps=min(vals), max_tps=max(vals),
                               cv_pct=100 * statistics.stdev(vals) / statistics.mean(vals) if len(vals) > 1 else None,
                               delta_vs_same_fanout_baseline_pct=100 * (statistics.median(vals) / statistics.median(baseline) - 1) if baseline else None)
                    for key in ('cut_ms', 'repair_ms', 'catchup_ms', 'total_recovery_ms', 'candidate_rows'):
                        values = [float(r[key]) for r in good]
                        row['median_' + key] = statistics.median(values)
                        row['min_' + key] = min(values)
                        row['max_' + key] = max(values)
                    aggregate.append(row)
        (results / (a.phase + '_aggregate.json')).write_text(json.dumps(aggregate, indent=2) + '\n')
        if aggregate:
            with (results / (a.phase + '_summary.csv')).open('w', newline='') as f:
                w = csv.DictWriter(f, list(aggregate[0]))
                w.writeheader()
                w.writerows(aggregate)

    status('STARTED')
    for case in plan:
        prior = [r for r in records if all(r.get(k) == v for k, v in case.items())]
        if any(r.get('status') == 'ACCEPTED' for r in prior):
            continue
        attempt = max((r['attempt'] for r in prior), default=0) + 1
        run_id = f"cluster4_{config['tag']}_{a.phase}_n{case['rows']}_f{case['fanout']}_{case['scenario']}_r{case['rep']}_a{attempt}"
        out = base / 'repo/scripts/bench_full_results' / run_id
        while out.exists():
            attempt += 1
            run_id = run_id.rsplit('_a', 1)[0] + '_a' + str(attempt)
            out = out.parent / run_id
        status('RUNNING', run_id=run_id, **case)
        start = time.monotonic()
        with (results / (run_id + '.log')).open('x') as log:
            rc = subprocess.run(['bash', str(base / 'run_case.sh'), case['scenario'],
                                 str(case['rep']), str(attempt), a.phase, str(case['rows']),
                                 str(case['fanout'])], stdout=log, stderr=subprocess.STDOUT).returncode
        if not (out / 'recovery_s1024.env').exists():
            out.mkdir(parents=True, exist_ok=True)
            (out / 'recovery_s1024.env').write_text('\n'.join(f'{k}={v}' for k, v in {
                **case, 'attempt': attempt, 'stage': a.phase, 'exit_code': rc,
                'collection_exit_code': 1}.items()) + '\n')
        with (results / (run_id + '_acceptance.log')).open('x') as log:
            check = subprocess.run(['python3', str(base / 'collect_run.py'), str(out)],
                                   stdout=log, stderr=subprocess.STDOUT).returncode
        r = json.loads((out / 'acceptance.json').read_text()) if (out / 'acceptance.json').exists() else dict(
            status='REJECTED', failures=['collector failed'], noise=[])
        r.update(case, attempt=attempt, run_id=run_id, runner_rc=rc, collector_rc=check,
                 case_seconds=time.monotonic() - start)
        if rc != 0 or check not in (0, 3):
            r['status'] = 'REJECTED'
        with record_file.open('a') as f:
            f.write(json.dumps(r) + '\n')
        records.append(r)
        tables()
        status(r['status'], run_id=run_id, **case)
        # No silent replacement of failed/noisy attempts. Resume follows diagnosis.
        if r['status'] != 'ACCEPTED':
            status('STOPPED_REVIEW_REQUIRED', run_id=run_id, failures=r.get('failures'), noise=r.get('noise'))
            return 2
    tables()
    status('DONE')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
