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
STAGE_ALLOWLIST = re.compile(rb'\$STAGE =~ \^\([a-z|]+\)\$')


def harness_bytes(path):
    # Widening run_case.sh's fanout or stage allowlist (e.g. adding 2/4/8 to a
    # finished matrix, or a recheck stage) does not change how any case runs.
    data = path.read_bytes()
    if path.name != 'run_case.sh':
        return data
    return STAGE_ALLOWLIST.sub(b'$STAGE =~ STAGE_ALLOWLIST', FANOUT_ALLOWLIST.sub(b'[[ FANOUT_ALLOWLIST ]]', data))


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
    p.add_argument('--recheck', choices=('off', 'pool', 'replace'), default='off',
                   help='use accepted runs from results/recheck_attempts.jsonl (same build, same seeds): '
                        'pool = add them to the matrix runs; replace = for every rechecked (fanout, scenario) '
                        'use only the recheck runs')
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
    pooled = list(accepted)
    if a.recheck != 'off' and (results / 'recheck_attempts.jsonl').exists():
        recheck = [dict(json.loads(s), stage='recheck')
                   for s in (results / 'recheck_attempts.jsonl').read_text().splitlines()]
        if any((r['rows'], r['fanout'], r['scenario']) not in {(n, f, m) for n, f, m, _ in expected} for r in recheck):
            raise RuntimeError('recheck target outside the frozen plan')
        rechecked = [r for r in recheck if r['status'] == 'ACCEPTED']
        if a.recheck == 'replace':
            groups = {(r['rows'], r['fanout'], r['scenario']) for r in rechecked}
            for r in records:
                if r['status'] == 'ACCEPTED' and (r['rows'], r['fanout'], r['scenario']) in groups:
                    r['replaced_by_recheck'] = True
            pooled = [r for r in pooled if not r.get('replaced_by_recheck')]
        records = records + recheck
        pooled += rechecked
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
                # A missing field stays None (unavailable), never a measured zero.
                num = lambda k, scale=1.0: float(rec[k]) * scale if k in rec else None
                for k in ('localise', 'transfer', 'apply', 'verify'):
                    row[k + '_ms'] = num(k + '_us', 1e-3)
                row['drain_ms_server'] = num('drain_ms')
                row['rebase_ms'] = num('rebase_ms')
                parts = [row[k + '_ms'] for k in ('localise', 'transfer', 'apply', 'verify')]
                if None not in parts and 'repair_ms' in rec:
                    row['repair_other_ms'] = max(0.0, float(rec['repair_ms']) - sum(parts))
                if None not in (row['drain_ms_server'], row['rebase_ms']) and 'repair_ms' in rec:
                    # Everything the RECOVER call does before replay starts.
                    row['repair_total_ms'] = row['drain_ms_server'] + float(rec['repair_ms']) + row['rebase_ms']
                if 'replay_target' in rec and 'replay_from' in rec:
                    row['replay_backlog_blocks'] = float(rec['replay_target']) - float(rec['replay_from']) + 1
            replays = re.findall(r'RECOVERY_REPLAY_LIVE[^\n]*', text)
            if replays:
                rep = dict(re.findall(r'(\w+)=([0-9.]+)', replays[0]))
                row['replay_entries'] = float(rep['entries']) if 'entries' in rep else None
                row['replay_ms_server'] = float(rep['ms']) if 'ms' in rep else None
            if ev.get('L') is not None:
                # The held cut the repair used is the reference node's CUT at the event's L.
                held = re.findall(r'verb=CUT result=OK L=%s B=[^\n]*held=1' % ev.get('L'), text)
                if held:
                    cut = dict(re.findall(r'(\w+_us)=([0-9]+)', held[0]))
                    for k in ('prepare', 'export', 'digest'):
                        row['cut_' + k + '_ms'] = float(cut[k + '_us']) / 1000 if k + '_us' in cut else None
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
                good = [r for r in pooled if (r['rows'], r['fanout'], r['scenario']) == (n, f, mode)]
                if not good:
                    continue
                vals = [float(r['tps_majority_visible']) for r in good]
                row = dict(rows=n, fanout=f, scenario=mode, repetitions=len(vals),
                           complete=len(vals) >= config['repetitions'], median_tps=statistics.median(vals),
                           min_tps=min(vals), max_tps=max(vals),
                           cv_pct=100 * statistics.stdev(vals) / statistics.mean(vals) if len(vals) > 1 else None,
                           delta_vs_same_fanout_baseline_pct=100 * (statistics.median(vals) / statistics.median(baseline) - 1) if baseline else None)
                for k in ('cut_ms', 'repair_ms', 'catchup_ms', 'total_recovery_ms', 'candidate_rows',
                          'rows_upserted', 'rows_deleted', 'differing_leaves', 'mismatched_partitions', 'max_completion_gap_ms'):
                    numbers = [float(r[k]) for r in good]
                    row['median_' + k] = statistics.median(numbers)
                    row['min_' + k] = min(numbers)
                    row['max_' + k] = max(numbers)
                # Sub-phase timings parsed from the server logs (enriched rows).
                rich = [e for e in enriched if e['status'] == 'ACCEPTED' and not e.get('replaced_by_recheck') and
                        (e['rows'], e['fanout'], e['scenario']) == (n, f, mode)]
                for k in SUBPHASE_KEYS:
                    numbers = [float(e[k]) for e in rich if e.get(k) not in (None, '')]
                    row['median_' + k] = statistics.median(numbers) if numbers else None
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
                  'Figures: `figures/fanout_overview.png` plus one figure each: TPS; total recovery split into every phase; cut (prepare/export/digest); repair (drain/localise/transfer/apply/verify/rebase); replay (catch-up time, blocks replayed, backlog); percent share per phase; each phase as a line; repair work/tree depth.', ''])
    if a.recheck == 'pool':
        lines.extend(['Recheck runs (`recheck_attempts.jsonl`, same build and fault seeds) are pooled into the statistics and figures;',
                      'pooled fanouts have more repetitions (see the n column).', ''])
    elif a.recheck == 'replace':
        lines.extend(['For every rechecked (fanout, scenario), the statistics and figures use only the recheck runs',
                      '(`recheck_attempts.jsonl`, same build and fault seeds); the matrix runs they replace stay in the attempt tables.', ''])
    (results / 'REPORT.md').write_text('\n'.join(lines))
    if a.plot and groups:
        plot(results / 'figures', config,
             [r for r in enriched if r['status'] == 'ACCEPTED' and not r.get('replaced_by_recheck')], complete)
    print(json.dumps(audit))


SUBPHASE_KEYS = ('cut_prepare_ms', 'cut_export_ms', 'cut_digest_ms', 'drain_ms_server', 'localise_ms',
                 'transfer_ms', 'apply_ms', 'verify_ms', 'rebase_ms', 'repair_total_ms',
                 'replay_entries', 'replay_backlog_blocks', 'replay_ms_server')
CUT_PHASES = [('cut_prepare_ms', 'Prepare (reach target entry)', '#9db4d8'),
              ('cut_export_ms', 'Export (wait for replica backlog + snapshot export)', '#4c72b0'),
              ('cut_digest_ms', 'Digest (Merkle roots at the cut)', '#2a4d7f')]
PHASES = [('cut_ms', 'Cut (wait for replica backlog + snapshot)', '#4c72b0'),
          ('drain_ms_server', 'Drain', '#64b5cd'),
          ('localise_ms', 'Repair: localise (roots + leaves)', '#ccb974'),
          ('transfer_ms', 'Repair: transfer (fetch ref rows)', '#dd8452'),
          ('apply_ms', 'Repair: apply (delete + upsert)', '#c44e52'),
          ('verify_ms', 'Repair: verify (partition roots; optional heap audit)', '#8172b3'),
          ('rebase_ms', 'Rebase', '#937860'),
          ('catchup_ms', 'Catch-up (replay missed blocks)', '#55a868')]
REPAIR_PHASES = PHASES[1:7]  # drain, localise, transfer, apply, verify, rebase
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

    def representative(n, f, s, total_key):
        """The repetition whose total is the median (lower middle for even counts)."""
        rs = sorted((r for r in runs(n, f, s) if r.get(total_key) not in (None, '')),
                    key=lambda r: float(r[total_key]))
        return rs[(len(rs) - 1) // 2] if rs else None

    def stack(ax, n, s, phases, total_key, percent=False, other_label='Other (gateway / unattributed)'):
        """Stack the phases of each fanout's median-total run, so the parts add up to that
        run's total exactly ('Other' = total - measured phases). Unavailable phases are
        not drawn as zero: the bar is marked 'n/a'."""
        for i, f in enumerate(config['fanouts']):
            r = representative(n, f, s, total_key)
            if r is None:
                if runs(n, f, s):
                    ax.text(i, 0, 'n/a', ha='center', va='bottom', fontsize=7, color='#c44e52')
                continue
            total_s = float(r[total_key]) / 1000
            parts = [(name, color, float(r[key]) / 1000 if r.get(key) not in (None, '') else None)
                     for key, name, color in phases]
            missing = [name for name, _, v in parts if v is None]
            measured = sum(v for _, _, v in parts if v is not None)
            parts.append((other_label, '#bbbbbb', max(0.0, total_s - measured)))
            if percent and measured > total_s * 1.001:
                # Phases exceed the total (clock skew); a % of the total is undefined.
                ax.text(i, 0, 'phases > total', ha='center', va='bottom', fontsize=6, color='#c44e52', rotation=90)
                continue
            denom = total_s or 1.0
            bottom = 0.0
            for name, color, v in parts:
                if v is None:
                    continue
                h = 100 * v / denom if percent else v
                labelled = ax.__dict__.setdefault('_stack_labels', set())
                ax.bar(i, h, .6, bottom=bottom, color=color, alpha=.9, edgecolor='white', linewidth=.5,
                       label=None if name in labelled else name)
                labelled.add(name)
                if percent and h >= 6:
                    ax.text(i, bottom + h / 2, f'{h:.0f}%', ha='center', va='center', fontsize=6.5, color='white')
                bottom += h
            if missing:
                ax.text(i, bottom, 'n/a: ' + ', '.join(m.split(' (')[0] for m in missing),
                        ha='center', va='bottom', fontsize=6, color='#c44e52', rotation=90)
            if measured > total_s * 1.001:
                ax.text(i, bottom, 'phases > total', ha='center', va='bottom', fontsize=6, color='#c44e52')

    def total(ax, n):
        for s in fault:
            stack(ax, n, s, PHASES, 'total_recovery_ms')
            series(ax, n, s, 'total_recovery_ms', scale=1e-3, label='Total (median; dots = runs)', color='black', marker='D')
        axis(ax, 'Recovery time (s)', 'Recovery time vs fanout — every phase (bars: median-total run)')
        ax.legend(fontsize=7, loc='upper left', ncol=2)

    def share(ax, n):
        for s in fault:
            stack(ax, n, s, PHASES, 'total_recovery_ms', percent=True)
        axis(ax, 'Share of total recovery (%)', "Where recovery time goes (% of the median-total run's total)")
        ax.set_ylim(0, 100)
        ax.legend(fontsize=6.5, loc='upper center', bbox_to_anchor=(0.5, -0.14), ncol=3, frameon=False)

    def cut(ax, n):
        for s in fault:
            stack(ax, n, s, CUT_PHASES, 'cut_ms', other_label='Other (control RPCs, gateway)')
            series(ax, n, s, 'cut_ms', scale=1e-3, label='Cut total (median; dots = runs)', color='black', marker='D')
        axis(ax, 'Cut time (s)', 'Cut breakdown vs fanout (bars: median-total run)')
        ax.legend(fontsize=7, loc='upper left', ncol=2)

    def repair(ax, n):
        for s in fault:
            stack(ax, n, s, REPAIR_PHASES, 'repair_total_ms', other_label='Other (unattributed)')
            series(ax, n, s, 'repair_total_ms', scale=1e-3, label='Repair total: drain..rebase (median; dots = runs)',
                   color='black', marker='D')
        axis(ax, 'Repair time (s)', 'Repair breakdown vs fanout (bars: median-total run)')
        ax.legend(fontsize=7, loc='upper left', ncol=2)

    def replay(ax, n):
        for s in fault:
            xs, med = [], []
            for i, f in enumerate(config['fanouts']):
                v = values(runs(n, f, s), 'catchup_ms')
                if v:
                    xs.append(i)
                    med.append(statistics.median(v) / 1000)
            ax.bar(xs, med, .6, color='#55a868', alpha=.9, label='Catch-up time (median)')
            series(ax, n, s, 'catchup_ms', scale=1e-3, label='Catch-up per run', color='black', marker='D')
        axis(ax, 'Catch-up time (s)', 'Replay (catch-up) vs fanout')
        twin = ax.twinx()
        for s in fault:
            series(twin, n, s, 'replay_entries', label='Blocks replayed', color='#8172b3', marker='^')
            series(twin, n, s, 'replay_backlog_blocks', label='Blocks behind when replay started', color='#dd8452', marker='v')
        twin.set_ylabel('Raft log blocks')
        twin.set_ylim(bottom=0)
        lines = ax.get_legend_handles_labels(), twin.get_legend_handles_labels()
        ax.legend(lines[0][0] + lines[1][0], lines[0][1] + lines[1][1], fontsize=7, loc='upper left')

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
        extra = sorted({r['fanout'] for r in accepted if r.get('stage') == 'recheck' and r['rows'] == n})
        runs_note = (f'median of {config["repetitions"]} runs' if not extra else
                     f'median of {config["repetitions"]} runs, {2 * config["repetitions"]} at fanout '
                     + '/'.join(map(str, extra)) + ' (fault runs, recheck pooled)')
        if extra and not any(r.get('stage') != 'recheck' and r['fanout'] in extra and r['scenario'] != 'A'
                             for r in accepted if r['rows'] == n):
            runs_note = (f'median of {config["repetitions"]} runs; fault runs at fanout '
                         + '/'.join(map(str, extra)) + ' are the latest recheck runs')
        caption = f'{n:,} rows · P200/S1024/M256 · {runs_note} (dots = individual runs) · {status}'
        fig, axes = plt.subplots(4, 2, figsize=(15, 20))
        for draw, ax in zip((tps, total, cut, repair, replay, share, phases, work), axes.flat):
            draw(ax, n)
        fig.suptitle('Merkle fanout: throughput and online recovery\n' + caption, fontsize=12)
        fig.tight_layout()
        for extension in ('pdf', 'png'):
            fig.savefig(figures / f'fanout_overview{suffix}.{extension}', dpi=160)
        plt.close(fig)
        for name, draw in (('tps_vs_fanout', tps), ('recovery_time_vs_fanout', total),
                           ('cut_breakdown_vs_fanout', cut), ('repair_breakdown_vs_fanout', repair),
                           ('replay_vs_fanout', replay), ('recovery_share_vs_fanout', share),
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
