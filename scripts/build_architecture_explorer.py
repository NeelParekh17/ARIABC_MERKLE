#!/usr/bin/env python3
"""Assemble ARCHITECTURE_EXPLORER.html from its editable assets, reviewed source hashes and result CSVs.

Assets (docs/explorers/): suite-boot.js, suite.css, suite.js, architecture/{page.html, architecture.css,
app.js, evidence.js, main.js}. The output is one self-contained page with no external dependencies.

Result inputs are read, never modified:
  Final_Results/OOM_100M/adhash_20261012/*.csv                (primary OOM table: paper PG/det + AdHash Merkle)
  Final_Results/OOM_100M/paper_20261010/phase2/*.csv          (setup facts, smoke, XOR Merkle rows kept as history)
  Final_Results/ONLINE_RECOVERY/paper_20261010/*.csv, runs/    (9 scenarios x 3 + 1 extra)
Outputs:
  ARCHITECTURE_EXPLORER.html, docs/explorers/results_data.json, docs/explorers/architecture_sources.json

  python3 scripts/build_architecture_explorer.py                    # Git checkout at REVIEWED_HEAD
  python3 scripts/build_architecture_explorer.py --snapshot S.json  # copied tree without .git
  python3 scripts/build_architecture_explorer.py --check            # recompute and compare, write nothing

The source manifest is guarded by the review gate in scripts/explorer_common.py; result numbers carry
their own frozen campaign commits and are never relabelled as reviewed-head measurements.
"""
import argparse
import csv
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import statistics
import sys

from explorer_common import (ROOT, REVIEWED_DATE, PREVIOUS_REVIEW, add_snapshot_argument, embed_json,
                             replace_block, review_identity, sha256_bytes, sha256_file)

PAGE = ROOT / 'ARCHITECTURE_EXPLORER.html'
SUITE = ROOT / 'docs/explorers'
ASSETS = SUITE / 'architecture'
OOM = ROOT / 'Final_Results/OOM_100M/paper_20261010/phase2'
OOMA = ROOT / 'Final_Results/OOM_100M/adhash_20261012'
REC = ROOT / 'Final_Results/ONLINE_RECOVERY/paper_20261010'
RESULTS_MARKERS = ('<script id="explorer-results" type="application/json">', '</script><!-- explorer-results-end -->')
SOURCES_MARKERS = ('<script id="explorer-sources" type="application/json">', '</script><!-- explorer-sources-end -->')
# placeholder -> asset files, inlined in this order
INLINE = {
    '/* SUITE_BOOT */': [SUITE / 'suite-boot.js'],
    '/* SUITE_CSS */': [SUITE / 'suite.css'],
    '/* ARCH_CSS */': [ASSETS / 'architecture.css'],
    '/* SUITE_JS */': [SUITE / 'suite.js'],
    '/* APP_JS */': [ASSETS / 'app.js', ASSETS / 'evidence.js', ASSETS / 'main.js'],
}

# Every file the architecture page cites or animates.  The page re-hashes these
# when served over HTTP and reports any whose bytes no longer match.
SOURCES = {
    'ariabc_pg/src/ariabc_pg_gateway.cxx': 'gateway pipeline, vote store, all-3 audit',
    'ariabc_pg/src/async_cluster_submitter.cxx': 'submit socket I/O thread',
    'ariabc_pg/src/ariabc_pg_server.cxx': 'server, raft orderer, direct path, raft params',
    'ariabc_pg/src/pg_state_machine.cxx': 'commit → deterministic sequence',
    'ariabc_pg/src/pg_state_machine_recovery.cxx': 'QUARANTINE / CUT / RECOVER / replay',
    'ariabc_pg/src/gateway_recovery_manager.hxx': 'detection, donor ranking, attempts',
    'ariabc_pg/src/replica_repair.cxx': 'localise, COPY ranges, REINDEX-then-full-copy, AdHash digest',
    'ariabc_pg/src/pg_executor.cxx': 'executor lanes, Kafka receipts',
    'ariabc_pg/src/kafka_console.cxx': 'Kafka producer / consumer config',
    'ariabc_pg/src/durable_log_store.cxx': 'Raft log fsync',
    'ariabc_pg/src/blake3_tx_signer.hxx': 'gateway-side BLAKE3 tag',
    'NuRaft/src/raft_server.cxx': 'NuRaft background threads',
    'NuRaft/src/asio_service.cxx': 'NuRaft network workers',
    'src/backend/tcop/postgres.c': 's-prefix dispatch',
    'src/backend/bcdb/worker.c': 'admission, gate, OPF, settle, stale-error, commit',
    'src/backend/bcdb/shm_transaction.c': 'tags, validation, OPF request, deferred apply',
    'src/include/bcdb/shm_transaction.h': 'the eight fallback reasons',
    'src/backend/bcdb/shm_block.c': 'finalized prefix',
    'src/backend/bcdb/middleware.c': 'result payloads',
    'src/backend/bcdb/globals.c': 'source defaults',
    'src/include/bcdb/globals.h': 'default ring and hashtab sizes',
    'src/backend/bcdb/worker_controller.c': 'idle worker backends',
    'src/backend/bcdb/recovery.c': 'CUT snapshot export, rebase',
    'src/backend/executor/nodeModifyTable.c': 'DML fallback guards, Merkle delete/insert',
    'src/backend/executor/execIndexing.c': 'exclusion-constraint flag',
    'src/pl/plpgsql/src/pl_exec.c': 'exception-block guard',
    'src/backend/access/heap/heapam.c': 'deferred index phase, partial predicates',
    'src/include/access/merkle.h': 'Merkle defaults, format 11',
    'src/backend/access/merkle/merkle.c': 'reloptions: fanout 2..256 power of two',
    'src/backend/access/merkle/merklebuild.c': 'bulk build, shape and backing-name guards',
    'src/backend/access/merkle/merkledelta.c': 'signed AdHash staging',
    'src/backend/access/merkle/merkleapply.c': 'synchronous apply, split/merge, legacy rebuild',
    'src/backend/access/merkle/merkleinsert.c': 'index AM insert path',
    'src/backend/access/merkle/merkleutil.c': 'row hash, route, AdHash add / negate, backing name',
    'src/backend/access/merkle/merkleverify.c': 'format, hash, count and internal-node checks',
    'src/backend/commands/tablecmds.c': 'rename / SET SCHEMA guards',
    'src/backend/commands/schemacmds.c': 'schema rename guard',
    'scripts/distributed/run_4node_raft_cluster.sh': 'cluster runner',
    'scripts/distributed/cluster_sweep_support.py': 'cluster flags',
    'scripts/distributed/run_all_modes_gateway_sweep.py': 'single-node / ranking runner',
    'scripts/distributed/recovery/fault_injector.py': 'recovery fault injection',
    'scripts/distributed/recovery/remote_db.py': 'Python recovery digest (AdHash)',
}

OOM_SETTINGS = [
    ('Engine snapshot (frozen)', 'frozen_commit'),
    ('Rows', 'db_rows'),
    ('Heap', 'heap_gib'),
    ('Fillfactor', 'usertable_fillfactor'),
    ('shared_buffers (timed)', 'shared_buffers'),
    ('Statements per case', 'txs'),
    ('Trials per point', 'trials'),
    ('Seed', 'seed'),
    ('Reset mode', 'reset_mode'),
    ('PG retry jitter', 'pg_retry_jitter'),
    ('Executor mode', 'pg_exec_mode'),
    ('Gateway terminals', 'gateway_terminals'),
    ('Det client workers', 'det_client_workers'),
    ('Merkle partitions', 'baseline_merkle_partitions'),
    ('Merkle fanout', 'baseline_merkle_fanout'),
    ('Split / merge', None),
    ('Merkle nodes / leaves', None),
    ('DB host', 'db_cpu_model'),
    ('Campaign start (UTC)', 'campaign_start_utc'),
    ('Campaign end (UTC)', 'campaign_end_utc'),
    ('Previous v2 git head', 'previous_v2_git_head'),
]


def rel(path):
    return str(Path(path).relative_to(ROOT))


def read_csv(path):
    with open(path, newline='') as f:
        return list(csv.DictReader(f))


def num(value, digits=None):
    """CSV text -> number; empty stays None (null), never 0."""
    if value is None or value == '':
        return None
    try:
        x = float(value)
    except ValueError:
        return value
    if digits is not None:
        x = round(x, digits)
    return int(x) if x.is_integer() and digits in (None, 0) else x


def oom_data():
    """Primary OOM table as published: PG/det rows from paper_20261010 (16577072), Merkle rows from the
    AdHash re-measurement adhash_20261012 (6a1c66d6). The XOR Merkle rows stay available as history."""
    files = {OOMA: ['configuration_summary.csv', 'matched_ratios.csv', 'headline_numbers.csv', 'trials.csv',
                    'previous_v2_comparison.csv', 'provenance/binary_sha256.json', 'provenance/engine_commit_step2.txt'],
             OOM: ['configuration_summary.csv', 'audit_counts.csv', 'setup_and_audit_facts.csv', 'binary_hashes.csv',
                   'smoke_trials.csv']}
    inputs = {rel(d / f): sha256_file(d / f) for d, names in files.items() for f in names}
    facts = {r['metric']: r['value'] for r in read_csv(OOM / 'setup_and_audit_facts.csv')}
    merkle_commit = (OOMA / 'provenance/engine_commit_step2.txt').read_text().strip()
    previous = {(r['workload'], r['skew'], r['workers'], r['mode']): num(r['previous_v2_tps'], 2)
                for r in read_csv(OOMA / 'previous_v2_comparison.csv')}
    xor = {(r['workload'], r['skew'], r['workers']): r for r in read_csv(OOM / 'configuration_summary.csv')
           if r['mode'] == 'bcdb_merkle'}
    trials = read_csv(OOMA / 'trials.csv')
    retries = {}
    for r in trials:
        retries.setdefault((r['workload'], r['skew'], r['workers'], r['mode']), []).append(num(r['retry_attempts']))
    points = []
    for r in read_csv(OOMA / 'configuration_summary.csv'):
        key = (r['workload'], r['skew'], r['workers'], r['mode'])
        vals = [v for v in retries.get(key, []) if v is not None]
        pt = {'workload': r['workload'], 'skew': r['skew'], 'workers': int(r['workers']),
              'mode': r['mode'], 'n': int(r['n']), 'median': num(r['median_tps'], 2),
              'min': num(r['min_tps'], 2), 'max': num(r['max_tps'], 2), 'cv_pct': num(r['cv_pct'], 3),
              'trials': [num(r[f'trial_{i}_tps'], 2) for i in (1, 2, 3)],
              'retry_median': statistics.median(vals) if vals else None,
              'source': r['source'], 'previous_v2_tps': previous.get(key)}
        if r['mode'] == 'bcdb_merkle':
            x = xor.get(key[:3])
            pt['xor'] = {'median': num(x['median_tps'], 2), 'min': num(x['min_tps'], 2), 'max': num(x['max_tps'], 2)} if x else None
        points.append(pt)
    ratios = [{'workload': r['workload'], 'skew': r['skew'], 'workers': int(r['workers']),
               'det_over_pg': num(r['det_over_pg'], 5), 'merkle_over_det': num(r['merkle_over_det'], 5),
               'merkle_over_pg': num(r['merkle_over_pg'], 5)} for r in read_csv(OOMA / 'matched_ratios.csv')]
    headline = {}
    for r in read_csv(OOMA / 'headline_numbers.csv'):
        headline.setdefault(r['metric'], {'n': int(r['n'])})[r['statistic']] = num(r['value'], 5)
    smoke = read_csv(OOM / 'smoke_trials.csv')
    labels = ('dt_opf', 'dt_opf_stale_error', 'dt_rel_read_tags')
    emitted = sum(1 for r in trials + smoke if any(r.get(lbl, '') != '' for lbl in labels))
    total = lambda k: sum(num(r[k]) or 0 for r in trials)
    audit = {'cases': len(trials), 'smoke_cases': len(smoke),
             'by_mode': {m: sum(1 for r in trials if r['mode'] == m) for m in ('pg', 'bcdb_det', 'bcdb_merkle')},
             'terminal_results': total('validated_completed_queries'), 'divergence': total('divergence_count'),
             'permanent_failures': total('permanent_failures'),
             'merkle_pass': sum(1 for r in trials if r['merkle_verify'] == 'PASS'),
             'merkle_cases': sum(1 for r in trials if r['mode'] == 'bcdb_merkle'),
             'xor_campaign': next((dict((k, num(v) if k != 'dataset' else v) for k, v in a.items())
                                   for a in read_csv(OOM / 'audit_counts.csv') if a['dataset'] == 'campaign'), None)}
    settings = [['PG / det rows: engine', facts.get('frozen_commit', ''), 'paper_20261010 setup_and_audit_facts.csv:frozen_commit'],
                ['Merkle rows: engine', merkle_commit, 'adhash_20261012/provenance/engine_commit_step2.txt'],
                ['Merkle aggregate', 'AdHash, index format 11; baseline REINDEXed from the paper copy', 'adhash_20261012/README.md']]
    for label, key in OOM_SETTINGS:
        if key:
            value = facts.get(key, '')
            if key == 'heap_gib' and value:
                value = f'{float(value):.2f} GiB ({int(facts["heap_bytes"]):,} bytes)'
            if key == 'db_cpu_model':
                value = value.split(':', 1)[-1].strip()
            settings.append([label, value, 'setup_and_audit_facts.csv:' + key])
        elif label.startswith('Split'):
            settings.append([label, f'{facts.get("baseline_merkle_split_threshold")} / {facts.get("baseline_merkle_merge_threshold")}',
                             'setup_and_audit_facts.csv:baseline_merkle_split_threshold,merge_threshold'])
        else:
            settings.append([label, f'{int(facts.get("baseline_merkle_total_nodes", 0)):,} / {int(facts.get("baseline_merkle_leaf_nodes", 0)):,}',
                             'setup_and_audit_facts.csv:baseline_merkle_total_nodes,leaf_nodes'])
    binaries = [[r['binary'] + ' (PG/det)', r['sha256']] for r in read_csv(OOM / 'binary_hashes.csv')]
    binaries += [[Path(k).name + ' (Merkle)', v] for k, v in json.loads((OOMA / 'provenance/binary_sha256.json').read_text()).items()]
    return {
        'source_dir': rel(OOMA), 'history_dir': rel(OOM), 'inputs': inputs,
        'frozen_commit': facts.get('frozen_commit'), 'merkle_commit': merkle_commit,
        'previous_v2_git_head': facts.get('previous_v2_git_head'),
        'campaign_start_utc': facts.get('campaign_start_utc'), 'campaign_end_utc': facts.get('campaign_end_utc'),
        'elapsed_hours': num(facts.get('campaign_elapsed_hours'), 2),
        'configurations': num(facts.get('configurations')),
        'qualification_min_trials': num(facts.get('qualification_min_trials')),
        'cv_max_pct': max(p['cv_pct'] for p in points if p['cv_pct'] is not None),
        'cv_gt10_count': sum(1 for p in points if (p['cv_pct'] or 0) > 10),
        'audit': audit, 'headline': headline, 'settings': settings, 'binaries': binaries,
        'points': points, 'ratios': ratios,
        'det_counters': {'labels': list(labels), 'trials_with_values': emitted, 'trials_checked': len(trials) + len(smoke)},
    }


EVENT_KEYS = ('L', 'B', 'cut_ms', 'repair_ms', 'drain_ms', 'catchup_ms', 'recover_call_ms', 'replay_from',
              'replay_target', 'mismatched_partitions', 'differing_leaves', 'rows_deleted', 'rows_upserted',
              'full_copies', 'total_ms', 'attempt', 'ref')


def recovery_event(run_id):
    log = REC / 'runs' / run_id / 'gateway_test.log'
    if not log.is_file():
        return None
    for line in log.read_text(errors='replace').splitlines():
        if 'RECOVERY_EVENT ' in line and 'result=PASS' in line:
            fields = dict(re.findall(r'(\w+)=(\S+)', line))
            return {k: num(fields[k]) for k in EVENT_KEYS if k in fields}
    return None


def recovery_data():
    files = ['scenario_summary.csv', 'all_attempts.csv', 'storage_flags.csv', 'provenance/git_head.txt']
    inputs = {f: sha256_file(REC / f) for f in files}
    attempts = []
    for r in read_csv(REC / 'all_attempts.csv'):
        a = {'run_id': r['run_id'], 'scenario': r['scenario'], 'rep': int(r['rep']), 'attempt': int(r['attempt']),
             'status': r['status'], 'primary': r['primary'] == 'True', 'tps': num(r['tps'], 2)}
        for k in ('cut_ms', 'repair_ms', 'catchup_ms', 'total_recovery_ms', 'mismatched_partitions',
                  'differing_leaves', 'candidate_rows', 'rows_deleted', 'rows_upserted', 'full_copies',
                  'empty_100ms_buckets', 'quorum_completions', 'recovery_at_det_seq'):
            a[k] = num(r[k])
        a['max_completion_gap_ms'] = num(r['max_completion_gap_ms'], 2)
        for k in ('phase8_merkle_pass', 'phase8_root', 'detected_by', 'target_node', 'reference_node', 'storage_flag'):
            a[k] = r[k] or None
        a['event'] = recovery_event(r['run_id']) if r['detected_by'] else None
        attempts.append(a)
    scenarios = []
    for r in read_csv(REC / 'scenario_summary.csv'):
        s = {k: (num(v, 3) if k not in ('scenario', 'roots', 'merkle_pass_all') else v) for k, v in r.items()}
        s['merkle_pass_all'] = r['merkle_pass_all'] == 'True'
        prim = [a for a in attempts if a['scenario'] == r['scenario'] and a['primary']]
        for k in ('cut_ms', 'repair_ms', 'catchup_ms', 'total_recovery_ms', 'tps'):
            vals = [a[k] for a in prim if a[k] is not None]
            s['spread_' + k] = [min(vals), max(vals)] if vals else None
        s['runs'] = [a['run_id'] for a in prim]
        scenarios.append(s)
    accepted = [a for a in attempts if a['status'] == 'ACCEPTED']
    return {
        'source_dir': rel(REC),
        'inputs': inputs,
        'frozen_commit': (REC / 'provenance/git_head.txt').read_text().strip(),
        'scenarios': scenarios,
        'attempts': attempts,
        'storage_flags': read_csv(REC / 'storage_flags.csv'),
        'validation': {
            'attempts': len(attempts), 'accepted': len(accepted),
            'primary': sum(1 for a in attempts if a['primary']),
            'scenarios': len(scenarios),
            'all_quorum_160000': all(a['quorum_completions'] == 160000 for a in accepted),
            'all_phase8_pass': all(a['phase8_merkle_pass'] == 'PASS' for a in accepted),
            'roots': sorted({a['phase8_root'] for a in accepted if a['phase8_root']}),
            'full_copies_total': sum(a['full_copies'] or 0 for a in accepted),
            'events_parsed': sum(1 for a in attempts if a['event']),
        },
    }


def adhash_data():
    files = ['comparison/comparison.csv', 'comparison/rejection.csv', 'campaign/summary.csv']
    inputs = {rel(OOMA / f): sha256_file(OOMA / f) for f in files}
    points = []
    for r in read_csv(OOMA / 'comparison/comparison.csv'):
        points.append({'workload': r['workload'], 'skew': r['skew'], 'workers': int(r['workers']),
                       'new_n': num(r['new_merkle_n']), 'new_median': num(r['new_merkle_median_tps'], 2),
                       'new_min': num(r['new_merkle_min_tps'], 2), 'new_max': num(r['new_merkle_max_tps'], 2),
                       'published_median': num(r['published_merkle_median_tps'], 2),
                       'merkle_ratio': num(r['merkle_ratio'], 5), 'det_ratio': num(r['det_control_ratio'], 5),
                       'adjusted': num(r['drift_adjusted_merkle_ratio'], 5), 'complete': r['point_complete'] == 'True'})
    cases = read_csv(OOMA / 'campaign/summary.csv')
    rejected = read_csv(OOMA / 'comparison/rejection.csv')
    ratios = [p['merkle_ratio'] for p in points if p['merkle_ratio'] is not None]
    adjusted = [p['adjusted'] for p in points if p['adjusted'] is not None]
    return {'source_dir': rel(OOMA / 'comparison'), 'inputs': inputs,
            'engine_commit': (OOMA / 'provenance/engine_commit_step2.txt').read_text().strip(),
            'points': points,
            'summary': {'cases': len(cases) + len(rejected), 'accepted': len(cases),
                        'merkle_trials': sum(1 for r in cases if r['mode'] == 'bcdb_merkle'),
                        'det_trials': sum(1 for r in cases if r['mode'] == 'bcdb_det'),
                        'complete': sum(1 for p in points if p['complete']),
                        'median_ratio': round(statistics.median(ratios), 5), 'min_ratio': min(ratios),
                        'max_ratio': max(ratios), 'median_adjusted': round(statistics.median(adjusted), 5)}}


def build_results():
    return {'schema': 2, 'oom': oom_data(), 'recovery': recovery_data(), 'adhash': adhash_data(),
            'note': 'Numbers are read from the curated CSVs; display rounding only. Each campaign keeps its own frozen engine commit.'}


def asset_files():
    return [p for paths in INLINE.values() for p in paths] + [ASSETS / 'page.html']


def build_sources(head, provenance):
    files = {}
    for path, purpose in SOURCES.items():
        raw = (ROOT / path).read_bytes()
        files[path] = {'sha256': sha256_bytes(raw), 'bytes': len(raw), 'purpose': purpose}
    return {'head': head, 'reviewed_date': REVIEWED_DATE, 'previous_review': PREVIOUS_REVIEW,
            'provenance': provenance,
            'scope': 'Reviewed source semantics for the animations and settings; result campaigns keep their own frozen commits.',
            'assets': {rel(p): sha256_file(p) for p in asset_files()},
            'files': files}


def render(results, sources):
    html = (ASSETS / 'page.html').read_text()
    for marker, paths in INLINE.items():
        if html.count(marker) != 1:
            raise SystemExit(f'Template marker must be unique: {marker}')
        body = '\n'.join(p.read_text() for p in paths)
        closing = '</style' if marker.endswith('CSS */') else '</script'
        if closing in body.lower():
            raise SystemExit(f'Asset for {marker} contains {closing}, which would end the inline block early')
        html = html.replace(marker, body)
    html = replace_block(html, *RESULTS_MARKERS, embed_json(results))
    return replace_block(html, *SOURCES_MARKERS, embed_json(sources))


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--check', action='store_true', help='compare without writing')
    add_snapshot_argument(parser)
    args = parser.parse_args()
    head, provenance = review_identity(SOURCES, args.snapshot)
    results = build_results()
    sources = build_sources(head, provenance)
    if args.check:
        problems = []
        page = PAGE.read_text() if PAGE.is_file() else ''
        a = page.find(RESULTS_MARKERS[0])
        stamp = None
        if a >= 0:
            embedded = json.loads(page[a + len(RESULTS_MARKERS[0]):page.index(RESULTS_MARKERS[1])])
            stamp = embedded.get('generated_utc')
        want = render(dict(results, generated_utc=stamp) if stamp else results, sources)
        if page != want:
            problems.append('ARCHITECTURE_EXPLORER.html differs from assets + CSVs + reviewed sources')
        for name, data in (('results_data.json', results), ('architecture_sources.json', sources)):
            path = SUITE / name
            disk = json.loads(path.read_text()) if path.is_file() else None
            if disk is not None:
                disk.pop('generated_utc', None)
            if disk != json.loads(embed_json(data)):
                problems.append(f'docs/explorers/{name} is stale')
        if problems:
            print('STALE: ' + '; '.join(problems))
            sys.exit(1)
        print(f'OK: page, data and {len(sources["files"])} source hashes match {head[:8]}')
        return
    stamped = dict(results, generated_utc=datetime.now(timezone.utc).replace(microsecond=0).isoformat())
    PAGE.write_text(render(stamped, sources))
    (SUITE / 'results_data.json').write_text(json.dumps(stamped, indent=1, sort_keys=True) + '\n')
    (SUITE / 'architecture_sources.json').write_text(json.dumps(sources, indent=1, sort_keys=True) + '\n')
    v = results['recovery']['validation']
    print(f'Built {PAGE.name}: OOM {len(results["oom"]["points"])} points, recovery {v["scenarios"]} scenarios / '
          f'{v["attempts"]} attempts, AdHash {len(results["adhash"]["points"])} points; '
          f'{len(sources["files"])} source hashes at {head[:8]}')


if __name__ == '__main__':
    main()
