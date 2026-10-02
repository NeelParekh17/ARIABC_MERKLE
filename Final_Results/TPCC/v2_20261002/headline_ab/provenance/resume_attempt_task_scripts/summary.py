#!/usr/bin/env python3
"""Validate ranking-only v2 artifacts and summarize accepted runs (stdlib only)."""
import argparse
import csv
import hashlib
import json
from pathlib import Path
import re
import statistics
import sys

TABLES = {'warehouse', 'district', 'customer', 'stock', 'oorder', 'order_line', 'new_order', 'history'}
PHASES = ('parse_plan_us', 'portal_run_us', 'gate_us', 'conflict_us', 'apply_us', 'finish_us')


def require(ok, message):
    if not ok:
        raise ValueError(message)


def kv(text):
    return dict(re.findall(r'(\w+)=([^\s]+)', text))


def rows(path):
    with path.open() as stream:
        return list(csv.DictReader(stream))


def trace_stats(run):
    counts, totals, ids = 0, {}, set()
    for path in sorted((run / 'ptrace_keep').glob('ptrace.[0-9]*')):
        for row in rows(path):
            tx = int(row['tx_id'])
            # worker.c emits tx->tx_id after delete_tx(tx); labels can be reused.
            # Gateway protocol v2, rather than trace labels, proves completion.
            ids.add(tx)
            counts += 1
            for name, value in row.items():
                if name != 'tx_id':
                    totals[name] = totals.get(name, 0) + int(value)
    require(counts > 0, 'missing phase traces')
    residual = totals['total_us'] - sum(totals.get(key, 0) for key in PHASES)
    return {'rows': counts, 'unique_trace_ids': len(ids),
            'reused_trace_ids': counts-len(ids), 'totals': totals, 'residual_us': residual,
            'residual_us_per_tx': residual / counts,
            'residual_fraction': residual / totals['total_us'] if totals['total_us'] else 0}


def accept(args):
    run = args.run.resolve()
    cfg = kv((run / 'config.txt').read_text())
    result = kv((run / 'result.txt').read_text())
    sys.path.insert(0, str(args.src / 'scripts/distributed'))
    from benchmark_validation import parse_gateway_result
    count = int(cfg['tx'])
    require(cfg['mode'] in ('det', 'merkle'), 'only det/Merkle modes supported')
    parsed = parse_gateway_result((run / 'gateway.log').read_text(), count,
                                  returncode=int(result['gateway_rc']),
                                  mode='bcdb_' + cfg['mode'])
    require(parsed['completed_tps'] > 0, 'missing TPS')
    require(abs(parsed['completed_tps'] - parsed['tps']) / parsed['tps'] < .005,
            'reported TPS disagrees with completed count / gateway wall time')
    settings = {r['name']: r['setting'] for r in rows(run / 'settings.csv')}
    for name, value in {'default_transaction_isolation': 'serializable', 'synchronous_commit': 'on',
                        'fsync': 'on', 'full_page_writes': 'on',
                        'merkle_apply_synchronous_direct': 'on', 'bcdb_worker_count': cfg['workers']}.items():
        require(settings.get(name) == value, f'incorrect setting {name}: {settings.get(name)}')
    restored = {r['relname']: r for r in rows(run / 'restore_relations.csv')}
    require(set(restored) == TABLES | {'item'}, 'missing restored table')
    for name, rel in restored.items():
        require(rel['relpersistence'] == 'p' and int(rel['heap_bytes']) > 0,
                f'{name}: must be LOGGED and nonempty')
        wanted = '' if cfg['fillfactor'] == 'default' or name == 'item' else 'fillfactor=' + cfg['fillfactor']
        require(rel['reloptions'] == wanted, f'{name}: incorrect fillfactor {rel["reloptions"]}')
    options = rows(run / 'merkle_options.csv')
    if cfg['mode'] == 'merkle':
        require(result.get('merkle_verify') == '9:true', 'Merkle verification must be 9:true')
        require(len(options) == 9, 'missing Merkle index options')
        wanted = {'fanout': '32', 'partitions': '16384', 'partition_key_columns': '1',
                  'subpartitions': '16', 'split_threshold': cfg['split'], 'merge_threshold': cfg['merge']}
        for idx in options:
            actual = dict(item.split('=', 1) for item in idx['reloptions'].split(';'))
            require(all(actual.get(k) == v for k, v in wanted.items()), f'incorrect Merkle geometry {idx}')
    else:
        require(not options, 'det run has Merkle indexes')
    state = (run / 'state.hash').read_bytes()
    require(state.strip() and len(state.split()) == 8, 'missing complete reference state.hash')
    if args.reference:
        require(state == args.reference.read_bytes(), f'logical state differs from {args.reference}')
    for logfile in ('postgres_workload.log', 'server.log', 'server.err.log'):
        require(not re.search(r'\b(?:PANIC|FATAL)\b', (run / logfile).read_text(errors='replace')),
                f'FATAL/PANIC in {logfile}')
    wal = int(result['wal_bytes'])
    require(wal > 0 and 'Total' in (run / 'walstats.txt').read_text(), 'missing WAL stats')
    trace = trace_stats(run)
    require(trace['rows'] == count, f'phase trace count {trace["rows"]} != {count}')
    hot = {}
    with (run / 'tabstats.csv').open() as stream:
        for schema, name, ins, upd, hotupd, deleted in csv.reader(stream):
            if schema == 'public':
                updates, hotupdates = int(upd), int(hotupd)
                require(0 <= hotupdates <= updates, f'{name}: invalid HOT counters')
                hot[name] = {'updates': updates, 'hot_updates': hotupdates,
                             'non_hot_fraction': (updates - hotupdates) / updates if updates else None}
    for name in ('stock', 'customer', 'district', 'warehouse'):
        require(hot.get(name, {}).get('updates', 0) > 0, f'{name}: missing final update stats')
    data = {'config': args.config, 'trial': args.trial, 'group': args.group,
            'run': str(run), **cfg, **parsed, 'wal_bytes': wal, 'wal_per_tx': wal / count,
            'hot': hot, 'trace': trace, 'state_sha256': hashlib.sha256(state).hexdigest(),
            'state_comparison': 'PASS' if args.reference else 'reference', 'accepted': True}
    (run / 'accepted.json').write_text(json.dumps(data, indent=2) + '\n')
    print(json.dumps({k: data[k] for k in ('run', 'config', 'tps', 'wal_per_tx', 'state_comparison')}))


def summarize(root):
    data = [json.loads(p.read_text()) for p in sorted(root.glob('*_w100/accepted.json'))]
    require(data, 'no accepted runs')
    with (root / 'all_runs.csv').open('w') as stream:
        names = ('group', 'config', 'trial', 'workers', 'fillfactor', 'tps', 'wal_per_tx', 'state_sha256', 'run')
        out = csv.DictWriter(stream, fieldnames=names, extrasaction='ignore')
        out.writeheader()
        out.writerows(data)
    lines = ['# TPC-C v2', '', 'Only accepted runs are included. SERIALIZABLE; synchronous Merkle; durability on.',
             'Headline: W=100, 32 workers, 20,000 transactions; trial-outer C1..C5.',
             'Shared-host noise: inspect each host_before.txt; compare det controls from the same trial.',
             'TPS uses gateway workload wall time. Phase sums overlap across workers and are not elapsed time.',
             'state.hash uses the published eight-table logical projection, excluding wall-clock timestamp fields.',
             'Accepted PGDATA is removed to bound disk use; logs, hashes, traces, settings and WAL stats remain.', '',
             '| Group | Workers | Config | Trials | Best TPS | Median TPS | Min TPS | Median WAL bytes/tx |',
             '|---|---:|---|---:|---:|---:|---:|---:|']
    groups = {}
    for row in data:
        groups.setdefault((row['group'], int(row['workers']), row['config']), []).append(row)
    for (group, workers, config), rs in sorted(groups.items()):
        rates = [r['tps'] for r in rs]
        lines.append(f'| {group} | {workers} | {config} | {len(rs)} | {max(rates):.2f} | {statistics.median(rates):.2f} | {min(rates):.2f} | {statistics.median(r["wal_per_tx"] for r in rs):.1f} |')
    lines += ['', 'C1=det default; C2=Merkle 32/8 default; C3=Merkle 1024/256 default; C4=det FF90; C5=Merkle 1024/256 FF90.', '',
              '| Group | Workers | Ratio | Median paired ratio | Best TPS ratio |', '|---|---:|---|---:|---:|']
    for group, workers in sorted({(k[0], k[1]) for k in groups if k[0] != 'smoke'}):
        for merkle, det in (('C2', 'C1'), ('C3', 'C1'), ('C5', 'C4'), ('C5', 'C1')):
            ms, ds = groups.get((group, workers, merkle), []), groups.get((group, workers, det), [])
            dby = {r['trial']: r for r in ds}
            pairs = [r['tps'] / dby[r['trial']]['tps'] for r in ms if r['trial'] in dby]
            if pairs:
                lines.append(f'| {group} | {workers} | {merkle}/{det} | {statistics.median(pairs):.4f} | {max(r["tps"] for r in ms)/max(r["tps"] for r in ds):.4f} |')
    lines += ['', '| Group | Workers | Config | Table | Non-HOT / updates (pooled) |', '|---|---:|---|---|---:|']
    for (group, workers, config), rs in sorted(groups.items()):
        for table in sorted(TABLES):
            updates = sum(r['hot'].get(table, {}).get('updates', 0) for r in rs)
            hot = sum(r['hot'].get(table, {}).get('hot_updates', 0) for r in rs)
            if updates:
                lines.append(f'| {group} | {workers} | {config} | {table} | {(updates-hot)/updates:.4%} ({updates-hot}/{updates}) |')
    lines += ['', '| Group | Workers | Config | Trial | Trace rows | Restarts | Residual us/tx | Residual / total |', '|---|---:|---|---:|---:|---:|---:|---:|']
    for row in data:
        trace = row['trace']
        lines.append(f'| {row["group"]} | {row["workers"]} | {row["config"]} | {row["trial"]} | {trace["rows"]} | {trace["totals"].get("restarts",0)} | {trace["residual_us_per_tx"]:.2f} | {trace["residual_fraction"]:.4%} |')
    lines += ['', 'Residual = sum(total_us) minus sum(parse_plan_us + portal_run_us + gate_us + conflict_us + apply_us + finish_us).',
              'Nested wait/apply counters remain in accepted.json and ptrace_keep; do not add them to the top-level phases.',
              'Trace tx_id labels can be reused because worker.c emits them after delete_tx(tx); gateway verified terminal results prove completion. Reused-label counts are recorded in accepted.json.', '',
              f'Accepted runs: {len(data)}. Unique headline state hashes: {len({r["state_sha256"] for r in data if r["group"] == "headline"})}.',
              'The master stops on any failed completion, settings, Merkle, state, trace or WAL evidence check.', '']
    (root / 'summary.md').write_text('\n'.join(lines))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='action', required=True)
    p = sub.add_parser('accept')
    p.add_argument('--run', type=Path, required=True)
    p.add_argument('--src', type=Path, required=True)
    p.add_argument('--reference', type=Path)
    p.add_argument('--config', required=True)
    p.add_argument('--trial', type=int, required=True)
    p.add_argument('--group', choices=('smoke', 'headline', 'sweep'), required=True)
    p = sub.add_parser('summarize')
    p.add_argument('root', type=Path)
    args = parser.parse_args()
    if args.action == 'accept':
        accept(args)
    else:
        summarize(args.root)


if __name__ == '__main__':
    main()
