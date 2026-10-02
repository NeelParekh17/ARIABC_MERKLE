#!/usr/bin/env python3
"""Ranking campaign driver, fail-closed acceptance and published-shape CSVs."""
import csv
import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import socket
import statistics
import subprocess
import sys

from summary import kv, rows, trace_stats

MODES = ('pg', 'det', 'merkle')
WAREHOUSES = (5, 10, 20, 30, 50, 75, 100)
WORKERS = (8, 16, 24, 32, 48, 64)
# Alternate sweeps, run the shared point once, keep pg/det/Merkle adjacent.
POINTS = ((5,32),(100,8),(10,32),(100,16),(20,32),(100,24),
          (30,32),(100,32),(50,32),(100,48),(75,32),(100,64))
TABLES = {'warehouse','district','customer','stock','oorder','order_line','new_order','history','item'}


class HardFailure(Exception):
    pass


def need(ok, message):
    if not ok:
        raise HardFailure(message)


def status(root, text):
    line = datetime.datetime.now().astimezone().isoformat() + ' ' + text + '\n'
    temp = root / 'status.txt.tmp'
    temp.write_text(line)
    temp.replace(root / 'status.txt')
    with (root / 'status_history.txt').open('a') as stream:
        stream.write(line)
    print(line, end='', flush=True)


def accepted(root):
    return [json.loads(p.read_text()) for p in sorted(root.glob('*_w*/accepted.json'))]


def validate(root, run, group, attempt, reason):
    cfg = kv((run / 'config.txt').read_text())
    result = kv((run / 'result.txt').read_text())
    mode, w, workers, count = cfg['mode'], int(cfg['warehouses']), int(cfg['workers']), int(cfg['tx'])
    sys.path.insert(0, str(Path(os.environ['SRCDIR']) / 'scripts/distributed'))
    from benchmark_validation import parse_gateway_result, count_workload_queries
    for name in ('postgres_workload.log','server.log','server.err.log'):
        need(not re.search(r'\b(?:FATAL|PANIC)\b', (run / name).read_text(errors='replace')),
             'FATAL/PANIC in ' + name)
    settings = {r['name']: r['setting'] for r in rows(run / 'settings.csv')}
    for name, value in {'default_transaction_isolation':'serializable','fsync':'on',
                        'synchronous_commit':'on','full_page_writes':'on',
                        'merkle_apply_synchronous_direct':'on','shared_buffers':'4194304',
                        'bcdb_worker_count':str(1 if mode == 'pg' else workers)}.items():
        need(settings.get(name) == value, f'{name}={settings.get(name)}, expected {value}')
    restored = {r['relname']: r for r in rows(run / 'restore_relations.csv')}
    need(set(restored) == TABLES, 'missing restored relations')
    for name, row in restored.items():
        need(row['relpersistence'] == 'p' and int(row['heap_bytes']) > 0 and
             row['reloptions'] == 'fillfactor=90', name + ': incorrect heap configuration')
    opts = rows(run / 'merkle_options.csv')
    if mode == 'merkle':
        need(result.get('merkle_verify') == '9:true', 'Merkle verification did not pass 9:true')
        need(len(opts) == 9, 'missing Merkle indexes')
        want = dict(fanout='32', partitions='16384', partition_key_columns='1',
                    subpartitions='16', split_threshold='1024', merge_threshold='256')
        for idx in opts:
            opt = dict(v.split('=',1) for v in idx['reloptions'].split(';'))
            need(all(opt.get(k) == v for k,v in want.items()), 'incorrect Merkle geometry')
    else:
        need(not opts, 'unexpected Merkle indexes')
    workload = root / 'workloads' / f'tpcc-{count}-w{w}-seed42.txt'
    need(count_workload_queries(workload) == count, 'wrong workload count')
    gateway = (run / 'gateway.log').read_text()
    for counter in ('divergence_count','permanent_failures','deterministic_error_count'):
        vals = re.findall(rf'\b{counter}=(\d+)\b',gateway)
        need(not vals or int(vals[-1]) == 0, 'nonzero '+counter)
    parsed = parse_gateway_result(gateway, count,
                                  int(result['gateway_rc']), 'pg' if mode == 'pg' else 'bcdb_'+mode)
    need(parsed['completed_tps'] > 0 and abs(parsed['completed_tps']/parsed['tps'] - 1) < .005,
         'completed TPS differs from count / wall time')
    state = (run / 'state.hash').read_bytes()
    need(len(state.split()) == 8, 'missing eight-table published state projection')
    sha = hashlib.sha256(state).hexdigest()
    if mode != 'pg':
        for other in accepted(root):
            if other['group'] == group and other['mode'] != 'pg' and other['W'] == w:
                need(other['state_sha256'] == sha, f'det/Merkle state mismatch at W={w}: {other["run"]}')
    wal = int(result['wal_bytes'])
    need(wal > 0 and 'Total' in (run / 'walstats.txt').read_text(), 'missing WAL evidence')
    hot = {}
    with (run / 'tabstats.csv').open() as stream:
        for schema, name, ins, upd, hotupd, deleted in csv.reader(stream):
            if schema == 'public':
                upd, hotupd = int(upd), int(hotupd)
                need(0 <= hotupd <= upd, 'invalid HOT counters')
                hot[name] = hotupd / upd if upd else None
    for name in ('warehouse','district','customer','stock'):
        need(hot.get(name) is not None, f'missing final stats for {name}')
    serial = (run / 'postgres_workload.log').read_text().count('could not serialize')
    restarts = serial
    if mode != 'pg':
        trace = trace_stats(run)
        need(trace['rows'] == count, 'wrong final phase trace count')
        restarts = trace['totals'].get('restarts',0)
    data = dict(mode=mode, layout='wh16' if mode == 'merkle' else 'cur', W=w,
                workers=workers, trial=attempt, group=group, tps=parsed['completed_tps'],
                restarts=restarts, merkle_verify=result.get('merkle_verify',''), perm_fail=0, div=0,
                pg_serialization_failures=serial if mode == 'pg' else '',
                rerun=attempt > 1, rerun_reason=reason, wal_per_tx=wal/count,
                hot_fractions=hot, state_sha256=sha, run=str(run), accepted=True,
                completed=count, workload_sha256=hashlib.sha256(workload.read_bytes()).hexdigest())
    (run / 'accepted.json').write_text(json.dumps(data,indent=2)+'\n')
    return data


def write_csv(path, fields, data):
    with path.open('w', newline='') as stream:
        out = csv.DictWriter(stream,fieldnames=fields,extrasaction='ignore')
        out.writeheader()
        out.writerows(data)


def summarize(root):
    data = [r for r in accepted(root) if r['group'] == 'matrix']
    extras = ['rerun','rerun_reason','wal_per_tx','hot_warehouse','hot_district','hot_customer','hot_stock',
              'state_sha256','run']
    for r in data:
        for t in ('warehouse','district','customer','stock'):
            r['hot_'+t] = r['hot_fractions'].get(t)
    write_csv(root/'all_runs.csv', ['mode','layout','W','workers','trial','tps','restarts',
              'merkle_verify','perm_fail','div','pg_serialization_failures']+extras, data)
    combined = []
    for sweep, dimension in (('warehouses_w32','W'), ('workers_w100','workers')):
        target = root / sweep
        target.mkdir(exist_ok=True)
        selected = [r for r in data if (r['workers'] == 32 if dimension == 'W' else r['W'] == 100)]
        write_csv(target/'all_runs.csv',['mode','layout',dimension,'trial','tps','restarts',
                  'merkle_verify','perm_fail','div','pg_serialization_failures']+extras,selected)
        summary = []
        for mode in MODES:
            for point in sorted({r[dimension] for r in selected}):
                rs = [r for r in selected if r['mode']==mode and r[dimension]==point]
                if not rs:
                    continue
                best = max(rs,key=lambda r:r['tps'])
                rates = [r['tps'] for r in rs]
                row = dict(config=mode+'_'+best['layout'], **{('warehouses' if dimension=='W' else 'workers'):point},
                           trials=len(rs),best_tps=max(rates),median_tps=statistics.median(rates),min_tps=min(rates),
                           tps=best['tps'],rerun=any(r['rerun'] for r in rs),wal_per_tx=best['wal_per_tx'],
                           restarts=best['restarts'],**{k:best[k] for k in extras if k.startswith('hot_')})
                summary.append(row)
                combined.append(dict(sweep=sweep,**row))
        write_csv(target/'summary.csv',['config','warehouses' if dimension=='W' else 'workers','trials',
                  'best_tps','median_tps','min_tps','tps','rerun','wal_per_tx','restarts']+
                  [k for k in extras if k.startswith('hot_')],summary)
    write_csv(root/'summary.csv',['sweep','config','warehouses','workers','trials','best_tps','median_tps',
              'min_tps','tps','rerun','wal_per_tx','restarts']+[k for k in extras if k.startswith('hot_')],combined)
    (root/'summary.md').write_text(f'TPC-C FF90, SERIALIZABLE, synchronous Merkle 1024/256.\n'
        f'{len(data)} accepted attempts; target 36 distinct matrix runs.\n'
        'One initial trial; best of initial plus one flagged rerun. Both values retained.\n'
        'Shared W=100/32 is represented in both sweep CSVs. Failed attempts: failures.jsonl.\n'
        'State comparison uses the published eight-table count/hash sum projection, excluding timestamps.\n')


def run_case(root, w, workers, mode, group, attempt=1, reason=''):
    label = f'{group}_k{workers}_t{attempt}'
    run = root / f'{mode}_{label}_w{w}'
    need(not run.exists(), 'Refusing existing run '+str(run))
    need(shutil.disk_usage(root).free > 80_000_000_000, 'less than 80GB free')
    need(subprocess.call(['sha256sum','-c',str(root/'provenance/binaries.sha256')],
                         stdout=subprocess.DEVNULL) == 0, 'binary provenance changed')
    for port in (55439,18100,19100):
        with socket.socket() as sock:
            sock.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
            sock.bind(('127.0.0.1',port))
    status(root,f'RUNNING {group} W={w} workers={workers} mode={mode} attempt={attempt} reason={reason}')
    tx = 2000 if group == 'smoke' else 20000
    with (root / f'{mode}_{label}_w{w}.log').open('w') as log:
        rc = subprocess.call(['bash',str(Path(__file__).with_name('sweep_run.sh')),label,
                              str(w),str(tx),str(workers),'16384','1','16',mode],stdout=log,stderr=log)
    # Hard corruption and isolation/configuration failures must never be retried.
    for name in ('postgres_workload.log','server.log','server.err.log'):
        p = run/name
        if p.exists():
            need(not re.search(r'\b(?:FATAL|PANIC)\b',p.read_text(errors='replace')), 'FATAL/PANIC '+str(p))
    need(not (run/'pgdata/postmaster.pid').exists(), 'owned PostgreSQL still running after child exit')
    try:
        if rc:
            raise ValueError(f'run exit={rc}')
        data = validate(root,run,group,attempt,reason)
    except (ValueError, OSError, KeyError) as exc:
        with (root/'failures.jsonl').open('a') as stream:
            stream.write(json.dumps(dict(W=w,workers=workers,mode=mode,group=group,
                                         attempt=attempt,run=str(run),error=str(exc)))+'\n')
        status(root,f'FAILED_RUN {run.name} {exc}; retain artifacts')
        return False
    status(root,f'PASS {run.name} TPS={data["tps"]} completed={tx} divergence=0 failures=0')
    # Only stopped, accepted, campaign-owned PGDATA; retain every evidence file.
    pgdata = run/'pgdata'
    need(pgdata.is_dir() and not pgdata.is_symlink() and (pgdata/'PG_VERSION').is_file(), 'unsafe cleanup')
    shutil.rmtree(pgdata)
    (run/'pgdata_removed.txt').write_text('Accepted and stopped; logs/statistics/hashes preserved.\n')
    summarize(root)
    return True


def stalls(root):
    # Include recovered initial failures; use the best accepted attempt at a point.
    primary = []
    for row in accepted(root):
        if row['group'] == 'matrix':
            primary.append(row)
    flags = {}
    for dimension, points in (('W',WAREHOUSES),('workers',WORKERS)):
        for mode in MODES:
            lookup = {}
            for row in primary:
                if row['mode']==mode and (row['workers']==32 if dimension=='W' else row['W']==100):
                    if row[dimension] not in lookup or row['tps'] > lookup[row[dimension]]['tps']:
                        lookup[row[dimension]] = row
            for i, point in enumerate(points):
                ns = points[max(0,i-1):i] + points[i+1:i+2]
                if point in lookup and all(n in lookup for n in ns):
                    row = lookup[point]
                    threshold = .65*min(lookup[n]['tps'] for n in ns)
                    if row['tps'] < threshold:
                        key = (row['W'],row['workers'],mode)
                        flags.setdefault(key,[]).append(f'{dimension}={point} tps={row["tps"]} threshold={threshold}')
    (root/'stall_flags.json').write_text(json.dumps([dict(W=w,workers=k,mode=m,reasons=r)
        for (w,k,m),r in flags.items()],indent=2)+'\n')
    return {key:'; '.join(value) for key,value in flags.items()}


def main():
    action, root = sys.argv[1], Path(sys.argv[2]).resolve()
    if action == 'summarize':
        summarize(root)
        return
    try:
        if action == 'smoke':
            need(not (root/'smoke.ok').exists(), 'smoke already ran')
            for mode in ('pg','merkle'):
                need(run_case(root,5,32,mode,'smoke'), 'smoke failed')
            (root/'smoke.ok').write_text('pg and Merkle: 2000/2000 FF90 W=5 workers=32 PASS\n')
            status(root,'SMOKE_COMPLETE pg and merkle 2000/2000')
            return
        if action == 'pass2':
            # Second full pass (user decision 2026-10-02): one more attempt per point/mode,
            # reverse point order to decorrelate host drift; then one extra attempt for any
            # point whose best value is still < 0.65 x its neighbours. Plots use best of attempts.
            need((root/'matrix.started').exists() and not (root/'pass2.started').exists(), 'pass2 needs a finished matrix and runs once')
            (root/'pass2.started').write_text(datetime.datetime.now().isoformat()+'\n')
            def next_attempt(w, workers, mode):
                used = [r['trial'] for r in accepted(root) if r['group']=='matrix' and r['W']==w and r['workers']==workers and r['mode']==mode]
                runs = list(root.glob(f'{mode}_matrix_k{workers}_t*_w{w}'))
                return max([0]+used+[int(x.name.split('_t')[1].split('_')[0]) for x in runs]) + 1
            failed = []
            for w, workers in reversed(POINTS):
                for mode in MODES:
                    if not run_case(root,w,workers,mode,'matrix',next_attempt(w,workers,mode),'pass2'):
                        failed.append((w,workers,mode))
            for (w,workers,mode), reason in stalls(root).items():
                run_case(root,w,workers,mode,'matrix',next_attempt(w,workers,mode),'pass2 stall: '+reason)
            summarize(root)
            status(root,f'PASS2_COMPLETE failed_pass2_runs={failed}')
            return
        if action == 'extra':
            # Orchestrator-selected suspect points (2026-10-02): det W20/32, Merkle W100/16,
            # det W100/64 still looked stall-hit after pass2; two more attempts each.
            for w, workers, mode in ((20,32,'det'),(100,16,'merkle'),(100,64,'det')):
                for _ in range(2):
                    used = [int(x.name.split('_t')[1].split('_')[0]) for x in root.glob(f'{mode}_matrix_k{workers}_t*_w{w}')]
                    run_case(root,w,workers,mode,'matrix',max(used)+1,'extra: suspect stall after pass2')
            summarize(root)
            status(root,'EXTRA_COMPLETE')
            return
        need(action == 'matrix' and (root/'smoke.ok').exists(), 'validated smoke required')
        need(not (root/'matrix.started').exists(), 'matrix already started')
        (root/'matrix.started').write_text(datetime.datetime.now().isoformat()+'\n')
        retry = {}
        for w, workers in POINTS:
            for mode in MODES:
                if not run_case(root,w,workers,mode,'matrix'):
                    retry[w,workers,mode] = 'failed initial attempt'
        # Recover isolated failures before assessing neighbouring throughput.
        # Failure retries and noise retries share one retry budget per point/mode.
        unresolved = []
        for (w,workers,mode), reason in retry.items():
            if not run_case(root,w,workers,mode,'matrix',2,reason):
                unresolved.append((w,workers,mode))
        for (w,workers,mode), reason in stalls(root).items():
            if (w,workers,mode) not in retry:
                retry[w,workers,mode] = reason
                if not run_case(root,w,workers,mode,'matrix',2,reason):
                    unresolved.append((w,workers,mode))
        summarize(root)
        data = [r for r in accepted(root) if r['group']=='matrix']
        for w,workers in POINTS:
            for mode in MODES:
                need(any(r['W']==w and r['workers']==workers and r['mode']==mode for r in data),
                     f'no accepted run W={w} workers={workers} {mode}')
        status(root,f'COMPLETE distinct_runs=36 accepted_attempts={len(data)} reruns={len(retry)} failed_reruns={unresolved}')
    except Exception as exc:
        summarize(root)
        status(root,f'HARD_FAILURE {type(exc).__name__}: {exc}')
        raise


if __name__ == '__main__':
    main()
