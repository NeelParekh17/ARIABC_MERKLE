#!/usr/bin/env python3
"""Summarize preserved fanout attempts; smoke never enters measured statistics."""
import argparse
import csv
import datetime
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


FANOUT_ALLOWLIST = re.compile(rb'\[\[ "\$FANOUT" =~ \^\([0-9|]+\)\$ \]\]')


def harness_bytes(path):
    # Widening run_case.sh's fanout allowlist (e.g. adding 2/4/8 to a finished
    # matrix) does not change how any already-allowed case runs.
    data = path.read_bytes()
    return FANOUT_ALLOWLIST.sub(b'[[ FANOUT_ALLOWLIST ]]', data) if path.name == 'run_case.sh' else data


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
            harness = tuple(hashlib.sha256(harness_bytes(next(out.glob(pattern)))).hexdigest()
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
            recovered = re.findall(r'verb=RECOVER result=OK[^\n]*', text)
            if recovered:
                # The repaired node's own split of repair_ms (server clock).
                rec = dict(re.findall(r'(\w+)=([0-9.]+)', recovered[0]))
                for k in ('localise', 'transfer', 'apply', 'verify'):
                    row[k + '_ms'] = float(rec.get(k + '_us', 0)) / 1000
                row['drain_ms_server'] = float(rec.get('drain_ms', 0))
                row['rebase_ms'] = float(rec.get('rebase_ms', 0))
                row['repair_other_ms'] = max(0.0, float(rec.get('repair_ms', 0)) - sum(
                    row[k + '_ms'] for k in ('localise', 'transfer', 'apply', 'verify')))
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
    # The provenance log is inherited from the workspace this campaign was cloned
    # from; a change observed before this campaign's first run says nothing about it.
    metas = [base / 'repo/scripts/bench_full_results' / r['run_id'] / 'run_meta.env' for r in records]
    first_run = min((m.stat().st_mtime for m in metas if m.exists()), default=None)
    def observed(change):
        return datetime.datetime.strptime(change['observed_utc'], '%Y%m%dT%H%M%SZ').replace(
            tzinfo=datetime.timezone.utc).timestamp()
    predating = [c for c in host_changes if first_run is not None and observed(c) < first_run]
    host_changes = [c for c in host_changes if c not in predating]
    audit = dict(complete=complete, planned=len(expected), accepted=len(actual), attempts=len(records),
                 identical_source_binaries_and_harness=len(identities) == 1,
                 host_environment_changes=host_changes,
                 host_environment_changes_predating_campaign=len(predating),
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
    if predating:
        lines.extend([f'{len(predating)} host-change note(s) in the inherited provenance log predate this campaign\'s first run and are not applied here.', ''])
    lines.extend(['Harness identity ignores only the fanout allowlist line of `run_case.sh`, so widening the fanout set does not split the identity.',
                  'Figures: `figures/fanout_overview.png` plus one figure each: TPS, total recovery split into every phase, Merkle repair split into localise/transfer/apply/verify, percent share per phase, each phase as a line, repair work/tree depth.', ''])
    (results / 'REPORT.md').write_text('\n'.join(lines))
    if a.plot and groups:
        plot(results / 'figures', config, [r for r in enriched if r['status'] == 'ACCEPTED'], complete)
    print(json.dumps(audit))


PHASES = [('cut_ms', 'Cut (wait for replica backlog + snapshot)', '#4c72b0'),
          ('drain_ms_server', 'Drain', '#64b5cd'),
          ('localise_ms', 'Repair: localise (roots + leaves)', '#ccb974'),
          ('transfer_ms', 'Repair: transfer (fetch ref rows)', '#dd8452'),
          ('apply_ms', 'Repair: apply (delete + upsert)', '#c44e52'),
          ('verify_ms', 'Repair: verify (roots + merkle_verify)', '#8172b3'),
          ('rebase_ms', 'Rebase', '#937860'),
          ('catchup_ms', 'Catch-up (replay missed blocks)', '#55a868')]
REPAIR_PHASES = PHASES[2:6]
SCENARIO_STYLE = {'A': ('No fault (A)', '#4c72b0', 'o'), 'M_mixed': ('Fault + online recovery (M_mixed)', '#c44e52', 's')}


def plot(figures, config, accepted, complete):
    """Every figure: x = fanout (log2 spaced), line = median, dots = each repetition."""
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt

    figures.mkdir(exist_ok=True)
    for old in list(figures.glob('*.png')) + list(figures.glob('*.pdf')):
        old.unlink()
    fault = [s for s in config['scenarios'] if s not in ('A', 'B')]

    def runs(n, f, s):
        return [r for r in accepted if (r['rows'], r['fanout'], r['scenario']) == (n, f, s)]

    def values(rs, key):
        return [float(r[key]) for r in rs if r.get(key) not in (None, '')]

    def series(ax, n, s, key, scale=1.0, label=None, color=None, marker='o'):
        xs, med = [], []
        for i, f in enumerate(config['fanouts']):
            v = [x * scale for x in values(runs(n, f, s), key)]
            if not v:
                continue
            xs.append(i)
            med.append(statistics.median(v))
            ax.scatter([i + d for d in jitter(len(v))], v, s=14, color=color, alpha=.45, zorder=3)
        ax.plot(xs, med, marker=marker, color=color, label=label, lw=2, zorder=4)

    def jitter(k):
        return [(-0.08 + 0.16 * j / (k - 1)) if k > 1 else 0 for j in range(k)]

    def axis(ax, ylabel, title):
        ax.set_xticks(range(len(config['fanouts'])), labels=config['fanouts'])
        ax.set_xlim(-0.5, len(config['fanouts']) - 0.5)
        ax.set_xlabel('Merkle fanout')
        ax.set_ylabel(ylabel)
        ax.set_title(title, fontsize=11)
        ax.grid(alpha=.25)
        ax.set_ylim(bottom=0)

    def tps(ax, n):
        for s in config['scenarios']:
            name, color, marker = SCENARIO_STYLE.get(s, (s, None, 'o'))
            series(ax, n, s, 'tps_majority_visible', label=name, color=color, marker=marker)
        axis(ax, 'Throughput (TPS)', 'Cluster throughput vs fanout')
        ax.legend(fontsize=8, loc='lower right')

    def medians(n, s, key):
        return [statistics.median(values(runs(n, f, s), key) or [0]) / 1000 for f in config['fanouts']]

    def stack(ax, n, s, phases, total_key, percent=False):
        """Stacked median phases; the remainder of the median total is 'Other'."""
        parts = [(name, color, medians(n, s, key)) for key, name, color in phases]
        totals = medians(n, s, total_key)
        other = [max(0.0, t - sum(p[2][i] for p in parts)) for i, t in enumerate(totals)]
        parts.append(('Other (gateway / unattributed)', '#bbbbbb', other))
        scale = [100 / (sum(p[2][i] for p in parts) or 1) if percent else 1 for i in range(len(totals))]
        bottom = [0.0] * len(totals)
        for name, color, med in parts:
            med = [m * k for m, k in zip(med, scale)]
            ax.bar(range(len(med)), med, .6, bottom=bottom, color=color, label=name, alpha=.9,
                   edgecolor='white', linewidth=.5)
            if percent:
                for i, (b, m) in enumerate(zip(bottom, med)):
                    if m >= 6:
                        ax.text(i, b + m / 2, f'{m:.0f}%', ha='center', va='center', fontsize=6.5, color='white')
            bottom = [b + m for b, m in zip(bottom, med)]

    def total(ax, n):
        for s in fault:
            stack(ax, n, s, PHASES, 'total_recovery_ms')
            series(ax, n, s, 'total_recovery_ms', scale=1e-3, label='Total (median; dots = runs)', color='black', marker='D')
        axis(ax, 'Recovery time (s)', 'Recovery time vs fanout — every phase (medians)')
        ax.legend(fontsize=7, loc='upper left', ncol=2)

    def share(ax, n):
        for s in fault:
            stack(ax, n, s, PHASES, 'total_recovery_ms', percent=True)
        axis(ax, 'Share of total recovery (%)', 'Where recovery time goes (% of median total)')
        ax.set_ylim(0, 100)
        ax.legend(fontsize=6.5, loc='upper center', bbox_to_anchor=(0.5, -0.14), ncol=3, frameon=False)

    def repair(ax, n):
        for s in fault:
            stack(ax, n, s, REPAIR_PHASES, 'repair_ms')
            series(ax, n, s, 'repair_ms', scale=1e-3, label='Repair total (median; dots = runs)', color='black', marker='D')
        axis(ax, 'Repair time (s)', 'Merkle repair breakdown vs fanout (medians)')
        ax.legend(fontsize=7, loc='upper left', ncol=2)

    def phases(ax, n):
        for s in fault:
            for key, name, color in PHASES:
                series(ax, n, s, key, scale=1e-3, label=name, color=color)
        axis(ax, 'Time (s)', 'Each recovery phase vs fanout (median; dots = runs)')
        ax.legend(fontsize=7, loc='upper left', ncol=2)

    def work(ax, n):
        for s in fault:
            series(ax, n, s, 'candidate_rows', label='Rows read by repair', color='#8172b3')
        axis(ax, 'Rows read by repair', 'Repair work and tree depth vs fanout')
        twin = ax.twinx()
        series(twin, n, 'A', 'after_node1_admin123_max_split_levels', label='Tree levels below partition root (node1)',
               color='#937860', marker='^')
        twin.set_ylabel('Tree levels')
        twin.set_ylim(bottom=0)
        lines = ax.get_legend_handles_labels(), twin.get_legend_handles_labels()
        ax.legend(lines[0][0] + lines[1][0], lines[0][1] + lines[1][1], fontsize=8, loc='upper right')

    status = 'complete matrix' if complete else 'INCOMPLETE matrix'
    for n in config['sizes']:
        suffix = f'_n{n}' if len(config['sizes']) > 1 else ''
        caption = (f'{n:,} rows · P200/S1024/M256 · median of {config["repetitions"]} runs (dots = individual runs) · {status}')
        fig, axes = plt.subplots(3, 2, figsize=(15, 15))
        for draw, ax in zip((tps, total, repair, share, phases, work), axes.flat):
            draw(ax, n)
        fig.suptitle('Merkle fanout: throughput and online recovery\n' + caption, fontsize=12)
        fig.tight_layout()
        for extension in ('pdf', 'png'):
            fig.savefig(figures / f'fanout_overview{suffix}.{extension}', dpi=160)
        plt.close(fig)
        for name, draw in (('tps_vs_fanout', tps), ('recovery_time_vs_fanout', total),
                           ('repair_breakdown_vs_fanout', repair), ('recovery_share_vs_fanout', share),
                           ('recovery_phases_vs_fanout', phases), ('repair_work_vs_fanout', work)):
            fig, ax = plt.subplots(figsize=(9, 5.5))
            draw(ax, n)
            fig.suptitle(caption, fontsize=8, y=0.995)
            fig.tight_layout()
            for extension in ('pdf', 'png'):
                fig.savefig(figures / f'{name}{suffix}.{extension}', dpi=160)
            plt.close(fig)


if __name__ == '__main__':
    main()
