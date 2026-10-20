#!/usr/bin/env python3
"""Ranking-only resumable paper driver; all metrics/gates come from v3 run.sh.

Retries are whole matched blocks, selected in chronological order using gates,
never throughput. Original attempts remain in all_runs.csv and the run tree.
"""
import argparse
import csv
import fcntl
import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import statistics
import subprocess
import time

HERE = Path(__file__).resolve().parent
MODES = ['pg', 'det', 'merkle']
ENVIRONMENT_CHECKS = {'sampler_quality', 'window_300s', 'configured_window',
                      'time_driven_checkpoint', 'window_available'}


def read(path, default=None):
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError):
        return default


def write(path, obj):
    tmp = path.with_suffix(path.suffix + '.new')
    tmp.write_text(json.dumps(obj, indent=2) + '\n')
    tmp.replace(path)


def digest(path):
    h = hashlib.sha256()
    with path.open('rb') as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()


def process_start(pid):
    try:
        fields = Path(f'/proc/{pid}/stat').read_text().rsplit(')', 1)[1].split()
        return None if fields[0] == 'Z' else fields[19]
    except OSError:
        return None


def status(root, phase, **kwargs):
    obj = dict(phase=phase, updated_utc=time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()), **kwargs)
    write(root / 'status.json', obj)
    print(json.dumps(obj), flush=True)


def retire_database(run):
    """Only stopped, campaign-owned physical DBs; retain all benchmark evidence."""
    pgdata = run / 'pgdata'
    rt = read(run / 'runtime.json', {})
    if not pgdata.exists() or not rt.get('postgres_stopped') or (pgdata / 'postmaster.pid').exists():
        return
    assert run.is_relative_to(Path(os.environ['CAMPAIGN_HOME']))
    for name in ('postgresql.conf', 'pg_hba.conf', 'pg_ident.conf'):
        shutil.copy2(pgdata / name, run / name)
    control = subprocess.run([str(Path(os.environ['INST']) / 'bin/pg_controldata'), str(pgdata)],
                             capture_output=True, text=True)
    (run / 'pg_controldata.txt').write_text(control.stdout + control.stderr)
    if not (run / 'acceptance.json').exists():
        return
    # Every evidence file is hashed before retiring disposable database pages.
    manifest = {str(p.relative_to(run)): digest(p) for p in sorted(run.rglob('*'))
                if p.is_file() and not p.is_symlink() and not p.is_relative_to(pgdata)
                and p.name != 'EVIDENCE.sha256.json'}
    write(run / 'EVIDENCE.sha256.json', manifest)
    shutil.rmtree(pgdata)
    if (run / 'wal_view').exists():
        shutil.rmtree(run / 'wal_view')  # links into disposable pgdata, not raw logs
    write(run / 'database_retired.json', dict(reason='stopped disposable database; all raw logs/configs/results retained',
          time_utc=time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()), evidence_files=len(manifest)))


def recover(root):
    active = read(root / 'active.json')
    if not active:
        return
    pid, start = active['pid'], active['process_start']
    # A driver crash may leave its owned shell running. Rejoin it before resume.
    while start is not None and process_start(pid) == start:
        status(root, 'reattaching', active=active)
        time.sleep(30)
    run = Path(active['run'])
    if not read(run / 'runtime.json', {}).get('finished'):
        for name in ('gateway', 'sampler', 'wal_collector', 'server', 'iostat'):
            p = run / (name + '.pid')
            if not p.exists():
                continue
            child = int(p.read_text())
            try:
                cmd = Path(f'/proc/{child}/cmdline').read_bytes().decode().replace('\x00', ' ')
                environ = Path(f'/proc/{child}/environ').read_bytes().decode().split('\x00')
            except OSError:
                continue
            # Only binaries/scripts in our isolated tree. Verify PID ownership.
            if str(run) in cmd or 'RUN=' + str(run) in environ:
                os.kill(child, signal.SIGTERM)
        pgdata = run / 'pgdata'
        if (pgdata / 'postmaster.pid').exists():
            subprocess.run([str(Path(os.environ['INST']) / 'bin/pg_ctl'), '-D', str(pgdata),
                            '-m', 'fast', '-w', '-t', '600', 'stop'], check=True)
        write(run / 'interrupted.json', dict(active=active, retained=True))
        runtime = read(run / 'runtime.json', {})
        runtime.update(finished=False, postgres_stopped=not (pgdata / 'postmaster.pid').exists(), stop_rc=0)
        write(run / 'runtime.json', runtime)
        subprocess.run(['python3', str(HERE / 'summary.py'), str(run)], check=True)
        retire_database(run)
        # A partial attempt remains raw evidence and is never resumed in-place.
    elif (run / 'acceptance.json').exists():
        retire_database(run)
    (root / 'active.json').unlink()


def effective(result, smoke):
    failed = set(result['acceptance']['failed_checks'])
    if smoke:
        failed -= {'window_300s', 'time_driven_checkpoint'}
    return not failed


def publish(root, state, smoke):
    all_rows, selected = [], []
    parity_rows = []
    for block in state['blocks']:
        for btry in block['tries']:
            if 'state_parity' in btry:
                parity_rows.append(dict(warehouses=block['w'], workers=block['workers'], trial=block['trial'],
                                        block_try=btry['try'], same_workload_and_state=btry['state_parity']))
            for entry in btry['runs']:
                run = Path(entry['path'])
                r = read(run / 'acceptance.json')
                if not r:
                    all_rows.append(dict(path=str(run), mode=entry['mode'], warehouses=block['w'],
                       workers=block['workers'], trial=block['trial'], block_try=btry['try'],
                       accepted=False, qualified=False, noise_flags='interrupted_or_missing_evidence'))
                    continue
                win = r.get('window', {})
                row = dict(mode=r['mode'], warehouses=r['warehouses'], workers=r['workers'], trial=r['trial'],
                    block_try=btry['try'], count=r['count'], accepted=r['acceptance']['accepted'],
                    smoke=smoke, qualified=bool(btry.get('selected')), tps=win.get('tps'),
                    tps_gateway=win.get('tps_gateway'), window_s=win.get('duration_s'), nopm=win.get('nopm'),
                    longest_stall_s=win.get('longest_stall_s'), permanent_failures_zero=r['acceptance']['checks'].get('permanent_failures_zero'),
                    divergence_zero=r['acceptance']['checks'].get('divergence_zero'),
                    user_aborts=r.get('user_aborts_total'), serialization_failures=r.get('serialization_failures'),
                    total_restarts=r.get('total_restarts'), noise_flags=';'.join(r['noise_flags']),
                    failed_checks=';'.join(r['acceptance']['failed_checks']), path=str(run), **r.get('det_counters', {}))
                all_rows.append(row)
                if btry.get('selected'):
                    selected.append(row)
    columns = sorted({k for row in all_rows for k in row})
    write(root / 'state_parity.json', parity_rows)
    for name, rows in [('all_runs.csv', all_rows), ('qualified_runs.csv', selected)]:
        with (root / name).open('w', newline='') as f:
            wr = csv.DictWriter(f, fieldnames=columns); wr.writeheader(); wr.writerows(rows)
    for name, rows in [('summary.csv', selected), ('all_summary.csv', all_rows)]:
        summary = []
        for key in sorted({(r['mode'], r['warehouses'], r['workers']) for r in rows}):
            group = [r for r in rows if (r['mode'], r['warehouses'], r['workers']) == key]
            rates = [r['tps'] for r in group if r.get('tps') is not None]
            if rates:
                summary.append(dict(mode=key[0], warehouses=key[1], workers=key[2], n=len(rates),
                    tps_median=statistics.median(rates), tps_best=max(rates), tps_min=min(rates)))
        with (root / name).open('w', newline='') as f:
            wr = csv.DictWriter(f, fieldnames=['mode', 'warehouses', 'workers', 'n', 'tps_median', 'tps_best', 'tps_min'])
            wr.writeheader(); wr.writerows(summary)
    write(root / 'campaign_acceptance.json', dict(accepted=not smoke and all(b.get('selected') for b in state['blocks']),
        smoke=smoke, qualified_blocks=sum(bool(b.get('selected')) for b in state['blocks']),
        requested_blocks=len(state['blocks']), raw_attempts=len(all_rows), qualified_attempts=len(selected),
        strict_accepted_attempts=sum(bool(r.get('accepted')) for r in all_rows)))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--root', type=Path, required=True)
    ap.add_argument('--plan', type=Path, required=True)
    ap.add_argument('--smoke', action='store_true')
    args = ap.parse_args()
    if not args.smoke:
        raise SystemExit('Old common-N full plan disabled; use standard_campaign.py campaign')
    home = Path(os.environ['CAMPAIGN_HOME']).resolve()
    root = args.root.resolve()
    assert home == Path.home() / 'claude_checks/paper_20261010_tpcc'
    assert root.parent == home and root.name.startswith('B_')
    assert subprocess.check_output(['id', '-un'], text=True).strip() == 'protectdr'
    assert '10.129.7.57' in subprocess.check_output(['hostname', '-I'], text=True).split()
    root.mkdir(exist_ok=True)
    lock = (home / 'paper_campaign.lock').open('w')
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    os.environ['CAMPAIGN_LOCK_HELD'] = '1'
    plan = read(args.plan)
    state = read(root / 'state.json')
    if state is None:
        trials = 1 if args.smoke else 3
        state = dict(plan=plan, smoke=args.smoke, blocks=[dict(w=p['w'], workers=p['workers'],
                     n=p['count'], trial=t, tries=[]) for t in range(1, trials+1) for p in plan['points']])
        write(root / 'state.json', state)
        shutil.copy2(args.plan, root / 'plan.json')
    assert state['plan'] == plan and state['smoke'] == args.smoke
    recover(root)
    for index, block in enumerate(state['blocks']):
        if block.get('selected'):
            continue
        # Skip complete earlier tries; an interrupted try is preserved and retried.
        while not block.get('selected'):
            if len(block['tries']) >= 3:
                status(root, 'blocked', reason='three matched block attempts exhausted; inspect retained evidence', block=block)
                return 2
            trial, w, workers = block['trial'], block['w'], block['workers']
            attempt = len(block['tries']) + 1
            rotation = (index + trial - 1 + attempt - 1) % 3
            order = MODES[rotation:] + MODES[:rotation]
            btry = {'try': attempt, 'count': block['n'], 'order': order, 'runs': []}
            block['tries'].append(btry)
            write(root / 'state.json', state)
            results = {}
            for mode in order:
                free = shutil.disk_usage(home).free / 2**30
                if free < (160 if w >= 50 else 140):
                    status(root, 'blocked', reason='insufficient free disk for isolated live database/WAL', free_gib=free)
                    return 2
                run = root / f'{mode}_w{w}_n{block["n"]}_workers{workers}_trial{trial}_attempt{attempt}'
                entry = dict(mode=mode, path=str(run))
                btry['runs'].append(entry)
                write(root / 'state.json', state)
                status(root, 'running', block_index=index+1, total_blocks=len(state['blocks']), mode=mode,
                       warehouses=w, workers=workers, trial=trial, block_try=attempt, run=str(run), free_gib=free)
                env = dict(os.environ, RUNROOT=str(root), ATTEMPT_SUFFIX=f'_attempt{attempt}')
                with (root / (run.name + '.driver.log')).open('w') as log:
                    child = subprocess.Popen(['bash', str(HERE / 'run.sh'), mode, str(w), str(block['n']), str(workers), str(trial)],
                                             env=env, stdout=log, stderr=subprocess.STDOUT)
                    write(root / 'active.json', dict(pid=child.pid, process_start=process_start(child.pid), run=str(run)))
                    rc = child.wait()
                entry['rc'] = rc
                (root / 'active.json').unlink(missing_ok=True)
                result = read(run / 'acceptance.json', {})
                if not result:
                    status(root, 'blocked', reason='run did not produce acceptance evidence', run=str(run), rc=rc)
                    return 2
                results[mode] = result
                retire_database(run)
                write(root / 'state.json', state)
                publish(root, state, args.smoke)
                failed = set(result['acceptance']['failed_checks'])
                if args.smoke:
                    failed -= {'window_300s', 'time_driven_checkpoint'}
                if failed - ENVIRONMENT_CHECKS:
                    status(root, 'blocked', reason='correctness/settings/lifecycle failure', failed_checks=sorted(failed), run=str(run))
                    return 2
                marker = root / 'FIRST_REAL_ACCEPTED.json'
                if not args.smoke and result['acceptance']['accepted'] and not marker.exists():
                    write(marker, dict(run=str(run), acceptance=result['acceptance'],
                          window=result.get('window'), retained_evidence=True))
                    # One bounded handoff interval permits curated evidence retrieval
                    # before the next measured run. Detached campaign resumes itself.
                    status(root, 'first_run_handoff', run=str(run), seconds=90)
                    time.sleep(90)
            d, m = results['det'], results['merkle']
            paths = {mode: Path(r['path']) for mode, r in results.items()}
            parity = all((paths['det'] / name).read_text().split()[0] == (paths['merkle'] / name).read_text().split()[0]
                         for name in ('workload.sha256',))
            parity = parity and (paths['det'] / 'initial_state.hash').read_bytes() == (paths['merkle'] / 'initial_state.hash').read_bytes()
            parity = parity and bool(d.get('state_hash')) and d['state_hash'] == m['state_hash']
            btry['state_parity'] = parity
            write(root / 'state.json', state)
            if not parity:
                publish(root, state, args.smoke)
                status(root, 'blocked', reason='matched det/Merkle workload or state parity failed', block=block)
                return 2
            stalled = any(r.get('window', {}).get('longest_stall_s', 0) >= 10 or
                          'sampler_quality' in r['acceptance']['failed_checks'] for r in results.values())
            btry['stalled'] = stalled
            if all(effective(r, args.smoke) for r in results.values()) and not stalled:
                btry['selected'] = True
                block['selected'] = attempt
            elif any({'window_300s', 'configured_window', 'window_available'} & set(r['acceptance']['failed_checks'])
                     for r in results.values()) and not args.smoke:
                block['n'] = int(block['n'] * 1.5)
                btry['retry_reason'] = 'observed window too short; next complete matched block increases N by 50 percent'
            else:
                btry['retry_reason'] = 'predeclared stall/sampler/checkpoint rule; preserve complete block and add matched extra attempt'
            write(root / 'state.json', state)
            publish(root, state, args.smoke)
    status(root, 'complete', smoke=args.smoke, blocks=len(state['blocks']))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
