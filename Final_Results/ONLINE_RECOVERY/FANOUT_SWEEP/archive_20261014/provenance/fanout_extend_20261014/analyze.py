#!/usr/bin/env python3
"""Summarize preserved fanout attempts; smoke never enters measured statistics."""
import argparse
import csv
import hashlib
import json
import math
from pathlib import Path
import re
import statistics


def geometry(path):
    lines = path.read_text().splitlines() if path.exists() else []
    for i, line in enumerate(lines):
        if all(k in line for k in ('nodes', 'leaves', 'internal_nodes', 'max_prefix_bits')):
            keys = [s.strip() for s in line.split('|')]
            values = [s.strip() for s in lines[i + 2].split('|')]
            return {k: float(v) for k, v in zip(keys, values)}
    return {}


def write_csv(path, rows):
    if not rows:
        return
    with path.open('w', newline='') as f:
        w = csv.DictWriter(f, list(dict.fromkeys(k for row in rows for k in row)))
        w.writeheader()
        w.writerows(rows)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--base', type=Path, required=True)
    p.add_argument('--plot', action='store_true', help='also render PDF/PNG figures with matplotlib')
    a = p.parse_args()
    base = a.base.resolve()
    config = json.loads((base / 'campaign.json').read_text())
    results = base / 'results'
    attempts = results / 'matrix_attempts.jsonl'
    records = [json.loads(s) for s in attempts.read_text().splitlines()] if attempts.exists() else []
    expected = {(n, f, s, r) for n in config['sizes'] for f in config['fanouts']
                for s in config['scenarios'] for r in range(1, config['repetitions'] + 1)}
    accepted = [r for r in records if r['status'] == 'ACCEPTED']
    actual = [(r['rows'], r['fanout'], r['scenario'], r['rep']) for r in accepted]
    if len(actual) != len(set(actual)):
        raise RuntimeError('duplicate accepted target; investigate before summarizing')
    if set(actual) - expected:
        raise RuntimeError('accepted target outside the frozen plan')
    enriched = []
    identities = set()
    for r in records:
        out = base / 'repo/scripts/bench_full_results' / r['run_id']
        if r['status'] == 'ACCEPTED':
            meta = dict(line.split('=', 1) for line in (out / 'run_meta.env').read_text().splitlines() if '=' in line)
            identity = tuple(meta.get(k) for k in ('source_fingerprint', 'postgres_sha256',
                                                   'ariabc_pg_server_sha256', 'ariabc_pg_gateway_sha256'))
            if any(v is None or not re.fullmatch('[0-9a-f]{64}', v) for v in identity):
                raise RuntimeError('missing source/binary identity: ' + r['run_id'])
            harness = tuple(hashlib.sha256(next(out.glob(pattern)).read_bytes()).hexdigest()
                            for pattern in ('cluster_runner_*.sh', 'run_case.sh', 'serializable_fault_injector.py', 'serializable_remote_db.py'))
            identities.add(identity + harness)
        ev = r.get('events', [{}])
        ev = ev[0] if ev else {}
        row = {k: v for k, v in r.items() if not isinstance(v, (dict, list))}
        row.update(detection=ev.get('reason'), recovery_boundary=ev.get('B'),
                   reference_node=ev.get('ref'), target_node=ev.get('node'))
        for h, node in (('10.129.148.247', 'node1_admin123'), ('10.129.148.246', 'node2_user4'),
                        ('10.129.148.248', 'node4_utkarsh')):
            for when, prefix in (('before_', 'geometry_before_'), ('after_', 'geometry_')):
                g = geometry(out / (prefix + h + '.txt'))
                for k, v in g.items():
                    row[when + node + '_' + k] = v
                if 'max_prefix_bits' in g:
                    row[when + node + '_max_split_levels'] = g['max_prefix_bits'] / math.log2(r['fanout'])
            text = (out / ('server_' + node + '.log')).read_text(errors='replace') if (out / ('server_' + node + '.log')).exists() else ''
            profiles = re.findall(r'PROFILE_RAFT_STORAGE[^\n]*', text)
            metrics = dict(re.findall(r'(fdatasync_\w+)=([0-9.]+)', profiles[-1])) if profiles else {}
            row[node + '_fsync_total_ms'] = metrics.get('fdatasync_total_ms')
            row[node + '_fsync_p99_ms'] = metrics.get('fdatasync_p99_ms')
        enriched.append(row)
    write_csv(results / 'matrix_enriched_attempts.csv', enriched)
    if len(identities) > 1:
        raise RuntimeError('source, binary, or prepared harness changed among accepted matrix runs')
    groups = []
    for n in config['sizes']:
        for f in config['fanouts']:
            baseline = [float(r['tps_majority_visible']) for r in accepted
                        if (r['rows'], r['fanout'], r['scenario']) == (n, f, 'A')]
            for mode in config['scenarios']:
                good = [r for r in accepted if (r['rows'], r['fanout'], r['scenario']) == (n, f, mode)]
                if not good:
                    continue
                vals = [float(r['tps_majority_visible']) for r in good]
                row = dict(rows=n, fanout=f, scenario=mode, repetitions=len(vals),
                           complete=len(vals) == config['repetitions'], median_tps=statistics.median(vals),
                           min_tps=min(vals), max_tps=max(vals),
                           cv_pct=100 * statistics.stdev(vals) / statistics.mean(vals) if len(vals) > 1 else None,
                           delta_vs_same_fanout_baseline_pct=100 * (statistics.median(vals) / statistics.median(baseline) - 1) if baseline else None)
                for k in ('cut_ms', 'repair_ms', 'catchup_ms', 'total_recovery_ms', 'candidate_rows',
                          'rows_upserted', 'rows_deleted', 'differing_leaves', 'mismatched_partitions', 'max_completion_gap_ms'):
                    numbers = [float(r[k]) for r in good]
                    row['median_' + k] = statistics.median(numbers)
                    row['min_' + k] = min(numbers)
                    row['max_' + k] = max(numbers)
                groups.append(row)
    write_csv(results / 'fanout_summary.csv', groups)
    complete = set(actual) == expected
    changes_path = base / 'provenance/host_environment_changes.jsonl'
    host_changes = [json.loads(s) for s in changes_path.read_text().splitlines()] if changes_path.exists() else []
    audit = dict(complete=complete, planned=len(expected), accepted=len(actual), attempts=len(records),
                 identical_source_binaries_and_harness=len(identities) == 1,
                 host_environment_changes=host_changes,
                 rejected=sum(r['status'] == 'REJECTED' for r in records),
                 noisy=sum(r['status'] == 'NOISE_EXCLUDED' for r in records),
                 missing=[list(t) for t in sorted(expected - set(actual))])
    (results / 'matrix_completion_audit.json').write_text(json.dumps(audit, indent=2) + '\n')
    lines = ['# Fanout, recovery, and cluster TPS', '',
             f"Status: {'COMPLETE' if complete else 'INCOMPLETE'} — {len(actual)}/{len(expected)} accepted targets; {len(records)} attempts.",
             '', 'Smoke trials are excluded. All failed/noisy matrix attempts remain in the attempt tables.',
             '', f"Engine source HEAD: `{config['source_head']}`; build and binary hashes are in `../provenance/`.",
             'Gateway/controller: .247; replicas: .247/.246/.248. Historical .111 gateway results are not matched baselines.',
             'S1024/M256/P200 and synchronous durability remain fixed. Faults are injected replica corruption, not crashes.',
             '', '| Rows | Fanout | Scenario | n | Median TPS | TPS min–max | CV % | Median repair ms | Median cut ms | Median catch-up ms | Median total ms |',
             '|---:|---:|---|---:|---:|---|---:|---:|---:|---:|---:|']
    for r in groups:
        cv = f"{r['cv_pct']:.2f}" if r['cv_pct'] is not None else 'unavailable'
        lines.append(f"| {r['rows']} | {r['fanout']} | {r['scenario']} | {r['repetitions']} | {r['median_tps']:.1f} | {r['min_tps']:.1f}–{r['max_tps']:.1f} | {cv} | {r['median_repair_ms']:.1f} | {r['median_cut_ms']:.1f} | {r['median_catchup_ms']:.1f} | {r['median_total_recovery_ms']:.1f} |")
    lines.extend(['', 'Use actual geometry in `matrix_enriched_attempts.csv` to interpret fanout effects.',
                  'Recovery boundary and reference watermark are retained: different replay backlogs can change total recovery time independently of Merkle repair.',
                  'Concurrent workload updates can overwrite injected UPDATE faults before detection. Compare actual repaired rows and candidate volume along with repair time; 100 injected rows does not imply 100 surviving damaged rows.',
                  'Min/max show observed repetitions, not confidence intervals. Missing counters remain unavailable.',
                  'Storage profiles are diagnostic evidence; no post-hoc storage exclusion is applied.', ''])
    if host_changes:
        lines.extend(['Host environment changes are recorded in `../provenance/host_environment_changes.jsonl`.',
                      'Pooled throughput comparisons are descriptive; the host environment was not verified constant across all accepted targets.'])
        lines.extend('- ' + change['note'] for change in host_changes)
        lines.append('')
    (results / 'REPORT.md').write_text('\n'.join(lines))
    if a.plot and groups:
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt
        figures = results / 'figures'
        figures.mkdir(exist_ok=True)
        for metric, ylabel in [('tps', 'Cluster majority-visible TPS'), ('repair_ms', 'Merkle repair (ms)'),
                               ('total_recovery_ms', 'Total recovery (ms)')]:
            fig, axes = plt.subplots(1, len(config['sizes']), figsize=(11, 4), squeeze=False)
            for ax, n in zip(axes[0], config['sizes']):
                for mode in config['scenarios']:
                    if metric != 'tps' and mode in ('A', 'B'):
                        continue
                    points = sorted((r for r in groups if r['rows'] == n and r['scenario'] == mode), key=lambda r: r['fanout'])
                    ys = [r['median_' + metric] for r in points]
                    lo = [y - r['min_' + metric] for y, r in zip(ys, points)]
                    hi = [r['max_' + metric] - y for y, r in zip(ys, points)]
                    ax.errorbar([r['fanout'] for r in points], ys, yerr=[lo, hi], marker='o', capsize=3, label=mode)
                ax.set_xscale('log', base=2)
                ax.set_xticks(config['fanouts'], labels=config['fanouts'])
                ax.set_title(f'{n:,} initial rows')
                ax.set_xlabel('Merkle fanout (S1024/M256/P200)')
                ax.set_ylabel(ylabel)
                ax.grid(alpha=.25)
                ax.legend(fontsize=8)
            fig.suptitle(('Complete matrix' if complete else 'INCOMPLETE matrix') + ' — medians with observed min/max')
            fig.tight_layout()
            for extension in ('pdf', 'png'):
                fig.savefig(figures / (metric + '_by_fanout.' + extension), dpi=180)
            plt.close(fig)
    print(json.dumps(audit))


if __name__ == '__main__':
    main()
