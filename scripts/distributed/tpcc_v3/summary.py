#!/usr/bin/env python3
"""TPC-C-derived window metrics and evidence-based acceptance (stdlib only)."""
import argparse
import csv
import datetime as dt
import json
import re
import statistics
from pathlib import Path

LABEL = 'tpmC-equivalent, unaudited (no keying/think time)'
TABLES = {'warehouse', 'district', 'customer', 'history', 'item', 'stock',
          'oorder', 'new_order', 'order_line'}


def fields(line):
    return dict(re.findall(r'\b([A-Za-z_][A-Za-z_0-9]*)=([^\s]+)', line))


def progress(line):
    if 'PROGRESS_GATEWAY_DET' not in line:
        return None
    f = fields(line)
    try:
        out = {k: float(f[k]) for k in ('elapsed_s', 'completed', 'sent', 'total')}
        for k in ('user_aborts', 'permanent_failures', 'divergence_count',
                  'wall_time_unix_ms', 'final'):
            out[k] = float(f[k]) if k in f else None
        return out
    except (KeyError, ValueError):
        return None


def read_json(path, default=None):
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError):
        return {} if default is None else default


def text(path):
    return path.read_text(errors='replace') if path.exists() else ''


def samples(path):
    rows = []
    with path.open(newline='') as f:
        for r in csv.DictReader(f):
            for k in ('wall_epoch', 'elapsed_s', 'completed', 'sent', 'total',
                      'user_aborts', 'district_next', 'progress_age_s', 'query_ms', 'load1'):
                r[k] = float(r[k]) if r.get(k) else None
            rows.append(r)
    return rows


def select_window(rows, warmup, completed_includes_aborts=True, tx_per_committed_new_order=None):
    """Window = first sample at/after warmup .. last sample before submission completes.

    Stalls (checkpoints, retry storms, gate waits) are part of sustained throughput and
    stay INSIDE the window; they are reported as stall_seconds, never cut out.
    Primary TPS is DB-side: committed NewOrders in the window (sum(d_next_o_id) delta)
    scaled by the workload's total/committed-NewOrder ratio. The gateway completion
    counter is batch-granular in pg mode (it advances per detWindow chunk), so it is
    kept only as the tps_gateway cross-check.
    """
    valid = [r for r in rows if all(r.get(k) is not None for k in
             ('elapsed_s', 'wall_epoch', 'completed', 'sent', 'total', 'district_next'))
             and r.get('wal_lsn') and not r.get('error')]
    if len(valid) < 2:
        raise ValueError('fewer than two valid matched samples')
    start_i = next((i for i, r in enumerate(valid) if r['elapsed_s'] >= warmup), None)
    if start_i is None:
        raise ValueError('no sample after warmup')
    cutoff = next((i for i, r in enumerate(valid) if r['sent'] >= r['total']), len(valid))
    reason = 'before_submission_complete' if cutoff < len(valid) else 'before_last_observation'
    rates = []
    for i in range(1, len(valid)):
        seconds = valid[i]['wall_epoch'] - valid[i-1]['wall_epoch']
        rates.append((valid[i]['completed'] - valid[i-1]['completed']) / seconds
                     if seconds > 0 else 0)
    end_i = cutoff - 1
    if end_i <= start_i:
        raise ValueError('submission/drain boundary precedes a usable window')
    a, b = valid[start_i], valid[end_i]
    duration = b['wall_epoch'] - a['wall_epoch']
    if duration <= 0:
        raise ValueError('nonpositive window')
    for k in ('completed', 'district_next'):
        if b[k] < a[k]:
            raise ValueError('nonmonotonic ' + k)
    aborts = (b['user_aborts'] - a['user_aborts']) if all(
        r.get('user_aborts') is not None for r in (a, b)) else None
    delta = b['completed'] - a['completed']
    outcomes = delta if completed_includes_aborts else delta + aborts if aborts is not None else None
    if outcomes is None:
        raise ValueError('exclusive completion counter requires an abort counter')
    new_orders = b['district_next'] - a['district_next']
    inside = valid[start_i:end_i + 1]
    stall_s, longest, run_s = 0.0, 0.0, 0.0
    for x, y in zip(inside, inside[1:]):
        dt = y['wall_epoch'] - x['wall_epoch']
        if y['district_next'] == x['district_next']:
            stall_s += dt; run_s += dt; longest = max(longest, run_s)
        else:
            run_s = 0.0
    return dict(start=a, end=b, duration_s=duration, completed_delta=delta,
                outcomes_delta=outcomes,
                completed_includes_aborts=completed_includes_aborts,
                committed_delta=(delta - aborts if completed_includes_aborts else delta) if aborts is not None else None,
                user_aborts_delta=aborts,
                new_orders_delta=b['district_next'] - a['district_next'],
                tps=(new_orders * tx_per_committed_new_order / duration
                     if tx_per_committed_new_order else outcomes / duration),
                tps_source='db_new_orders_x_workload_ratio' if tx_per_committed_new_order else 'gateway_completed',
                tps_gateway=outcomes / duration,
                nopm=new_orders * 60 / duration,
                stall_seconds=stall_s, longest_stall_s=longest,
                end_reason=reason, sample_count=end_i-start_i+1)


def wal_stats(raw):
    # PostgreSQL pg_waldump: Total N (%) record_bytes (%) fpi_bytes (%) combined (%).
    for line in raw.splitlines():
        if line.lstrip().startswith('Total'):
            nums = re.findall(r'\d+(?:\.\d+)?', line)
            # Total has blank percentage for N in PG13 (upstream pg_waldump.c).
            if len(nums) == 7:
                return dict(records=int(nums[0]), record_bytes=int(nums[1]),
                            fpi_bytes=int(nums[3]), combined_bytes=int(nums[5]))
            if len(nums) == 8:
                return dict(records=int(nums[0]), record_bytes=int(nums[2]),
                            fpi_bytes=int(nums[4]), combined_bytes=int(nums[6]))
    return None


def checkpoint_events(raw, window):
    events = []
    for line in raw.splitlines():
        if 'checkpoint starting:' not in line and 'checkpoint complete:' not in line:
            continue
        m = re.match(r'(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d(?:\.\d+)?)', line)
        if not m:
            continue
        stamp = dt.datetime.fromisoformat(m[1]).replace(tzinfo=dt.timezone.utc).timestamp()
        if window['start']['wall_epoch'] <= stamp <= window['end']['wall_epoch']:
            events.append(dict(wall_epoch=stamp, line=line,
                               time_driven='checkpoint starting: time' in line))
    return events


def verify_settings(run, cfg):
    actual = {}
    try:
        with (run / 'settings.csv').open() as f:
            actual = {r['name']: r['setting'] for r in csv.DictReader(f)}
    except (OSError, KeyError):
        pass
    expected = cfg.get('expected_settings', {})
    checks = {k: actual.get(k) == str(v) for k, v in expected.items()}
    return dict(ok=bool(expected) and all(checks.values()), checks=checks, actual=actual)


def tx_ratio(run):
    """Total transactions per committed NewOrder in this attempt's workload file."""
    meta = read_json(run / 'workload.sql.meta.json')
    try:
        committed = meta['counts']['new_order'] - meta['expected_rollbacks']
        return meta['count'] / committed if committed > 0 else None
    except (KeyError, TypeError):
        return None


def attempt(run):
    cfg = read_json(run / 'config.json')
    rt = read_json(run / 'runtime.json')
    raw = text(run / 'gateway.log')
    last = {}
    prog = []
    for line in raw.splitlines():
        last.update(fields(line))
        p = progress(line)
        if p:
            prog.append(p)
    checks = {}
    def check(name, ok):
        checks[name] = bool(ok)
    n = cfg.get('count')
    final = next((p for p in reversed(prog) if p['final'] == 1), {})
    def integer(key):
        try:
            return int(last[key])
        except (KeyError, ValueError):
            return None
    completed = int(final['completed']) if final else integer('client_quorum_complete_count')
    aborts = integer('user_aborts')
    meta = read_json(run / 'workload.sql.meta.json')
    expected_aborts = meta.get('expected_rollbacks', meta.get('expected_user_aborts'))
    if isinstance(expected_aborts, dict):
        expected_aborts = sum(expected_aborts.values())
    check('gateway_rc_zero', rt.get('gateway_rc') == 0)
    # Tolerate either Agent A counter convention. Record the inferred convention explicitly.
    inclusive = completed == n
    check('all_transactions_accounted', n is not None and completed is not None and
          (inclusive or (aborts is not None and completed + aborts == n)))
    check('expected_business_aborts', aborts is not None and expected_aborts is not None
          and aborts == expected_aborts)
    if 'user_abort_counter_supported' in last:
        check('abort_counter_supported', integer('user_abort_counter_supported') == 1)
        check('documented_completion_counter', inclusive)
    if 'user_abort_poll_failures' in last:
        check('abort_counter_polling', integer('user_abort_poll_failures') == 0)
    try:
        initial_next = int(text(run / 'initial_district_next.txt').strip())
        final_next = int(text(run / 'final_district_next.txt').strip())
        expected_new_orders = meta['counts']['new_order'] - expected_aborts
        check('committed_new_order_total', final_next - initial_next == expected_new_orders)
    except (ValueError, KeyError, TypeError):
        check('committed_new_order_total', False)
    check('permanent_failures_zero', integer('permanent_failures') == 0)
    check('divergence_zero', integer('divergence_count') == 0)
    check('final_progress_present', bool(final))
    check('consistency', re.search(r'^consistency_ok=t\s*$', text(run / 'consistency.txt'), re.M) is not None
          and rt.get('consistency_rc') == 0)
    consistency = text(run / 'consistency.txt')
    check('consistency_conditions_1_to_4', all(re.search(
        r'^condition_' + str(i) + r'_[^=\n]+=(?:t|true)\s*$', consistency, re.M) for i in range(1, 5)))
    merkle = text(run / 'merkle_verify.txt').strip().splitlines()
    check('merkle_verify', cfg.get('mode') != 'merkle' or
          (len(merkle) == 9 and all(re.fullmatch(r'\w+\|t', s) for s in merkle)
           and {s.split('|')[0] for s in merkle} == TABLES))
    settings = verify_settings(run, cfg)
    check('settings', settings['ok'])
    try:
        with (run / 'relations.csv').open() as f:
            rels = list(csv.DictReader(f))
        check('logged_fillfactor', len(rels) == 9 and {r['relname'] for r in rels} == TABLES
              and all(r['relpersistence'] == 'p' and
                      'fillfactor=' + str(cfg.get('fillfactor', 90)) in r['reloptions'].split(';') for r in rels))
        with (run / 'indexes.csv').open() as f:
            indexes = list(csv.DictReader(f))
        lookup_tables = {r['tablename'] for r in indexes if r['indexname'].endswith('_merkle_lookup_idx')}
        check('merkle_lookup_indexes', cfg.get('mode') != 'merkle' or lookup_tables == TABLES)
    except (OSError, KeyError):
        check('relation_index_evidence', False)
    state = text(run / 'state.hash').strip()
    check('state_hash_all_tables', rt.get('state_hash_rc') == 0 and
          all(re.search(r'\b' + t + r'[=:]', state) for t in TABLES))
    check('provenance_present', (run / 'provenance.json').exists())
    result = dict(path=str(run), mode=cfg.get('mode'), warehouses=cfg.get('warehouses'),
                  workers=cfg.get('workers'), trial=cfg.get('trial'), count=n,
                  completed_total=completed, user_aborts_total=aborts, expected_aborts=expected_aborts,
                  nopm_label=LABEL, settings=settings, state_hash=state, noise_flags=[])
    try:
        rows = samples(run / 'samples.csv')
        win = select_window(rows, cfg.get('warmup_s', 60), inclusive, tx_ratio(run))
        (run / 'window.json').write_text(json.dumps(win, indent=2) + '\n')
        result['window'] = win
        check('window_300s', win['duration_s'] >= 300)
        check('configured_window', win['duration_s'] >= cfg.get('min_window_s', 300))
        check('window_abort_counter', win['user_aborts_delta'] is not None and win['user_aborts_delta'] >= 0)
        in_window = [r for r in rows if win['start']['wall_epoch'] <= r['wall_epoch'] <= win['end']['wall_epoch']]
        # 1 Hz target; under load the /proc scan can delay a sample by a few seconds.
        # Window endpoints use exact timestamps, so gaps up to 5 s do not bias rates.
        check('sampler_quality', all(not r.get('error') and r.get('progress_age_s') is not None
              and 0 <= r['progress_age_s'] <= 5.0 and r['query_ms'] <= 1000 for r in in_window)
              and all(0 < b['wall_epoch'] - a['wall_epoch'] <= 5.0
                      for a, b in zip(in_window, in_window[1:])))
        check('affinity', bool(in_window) and all(r.get('affinity_ok') == '1' for r in in_window))
        cp = checkpoint_events(text(run / 'postgres_workload.log'), win)
        result['checkpoints_in_window'] = cp
        check('time_driven_checkpoint', any(e['time_driven'] for e in cp))
        check('no_requested_or_wal_checkpoint_in_window', all('checkpoint starting:' not in e['line']
              or e['time_driven'] for e in cp))
        if win['longest_stall_s'] >= 10:
            result['noise_flags'].append('stall_ge_10s_in_window')
        if any(r['load1'] is not None and r['load1'] > cfg.get('load_noise_threshold', 192) for r in in_window):
            result['noise_flags'].append('host_load_high')
        for r in in_window:
            consumers = json.loads(r.get('other_cpu_json') or '[]')
            if any(p['cpu_pct'] >= cfg.get('other_cpu_noise_threshold', 100) for p in consumers):
                result['noise_flags'].append('other_process_cpu_high')
                break
        if not checks['sampler_quality']:
            result['noise_flags'].append('sampler_gap_or_stale_progress')
        ws = wal_stats(text(run / 'walstats.txt'))
        result['wal'] = ws
        check('wal_stats', rt.get('waldump_rc') == 0 and ws is not None and
              ws['record_bytes'] + ws['fpi_bytes'] == ws['combined_bytes'])
        if ws and win['committed_delta'] and win['committed_delta'] > 0:
            for k in ('record_bytes', 'fpi_bytes', 'combined_bytes'):
                ws[k + '_per_committed_tx'] = ws[k] / win['committed_delta']
        else:
            check('wal_denominator', False)
    except (OSError, ValueError, KeyError, TypeError) as e:
        result['window_error'] = str(e)
        check('window_available', False)
    # Restarts are whole-attempt counters, never pretend aggregate concurrent times are wall time.
    server = text(run / 'server.log') + text(run / 'server.err.log')
    sf = fields(server.replace('\n', ' '))
    result['serialization_failures'] = sf.get('retryable_sqlstate_40001', sf.get('serialization_failures'))
    result['executor_retry_attempts'] = sf.get('retry_attempts_total')
    result['executor_retry_exhausted'] = sf.get('retry_exhausted_total')
    restarts = 0
    trace_count = 0
    for path in run.glob('ptrace*.*'):
        if not path.is_file():
            continue
        for row in csv.reader(path.open()):
            try:
                restarts += int(row[1]); trace_count += 1
            except (ValueError, IndexError):
                pass
    result['total_restarts'] = restarts if trace_count else None
    result['traced_transactions'] = trace_count
    check('attempt_finished', rt.get('finished') is True)
    check('sampler_rc_zero', rt.get('sampler_rc') == 0)
    check('postgres_stopped', rt.get('stop_rc') == 0 and rt.get('postgres_stopped') is True)
    result['noise_flags'] = sorted(set(result['noise_flags']))
    result['acceptance'] = dict(accepted=all(checks.values()), checks=checks,
                                failed_checks=[k for k, v in checks.items() if not v],
                                smoke=cfg.get('smoke', False))
    (run / 'acceptance.json').write_text(json.dumps(result, indent=2) + '\n')
    return result


def summarize(root):
    runs = sorted(p.parent for p in root.glob('*/config.json'))
    if (root / 'config.json').exists():
        runs = [root]
    results = [attempt(p) for p in runs]
    columns = ['mode', 'warehouses', 'workers', 'trial', 'count', 'accepted', 'window_s',
               'tps', 'nopm', 'window_aborts', 'total_aborts', 'record_bytes_per_committed_tx',
               'fpi_bytes_per_committed_tx', 'total_restarts', 'serialization_failures',
               'noise_flags', 'failed_checks', 'path']
    with (root / 'results.csv').open('w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=columns); writer.writeheader()
        for r in results:
            w, wal = r.get('window', {}), r.get('wal') or {}
            writer.writerow(dict(mode=r['mode'], warehouses=r['warehouses'], workers=r['workers'],
                trial=r['trial'], count=r['count'], accepted=r['acceptance']['accepted'],
                window_s=w.get('duration_s'), tps=w.get('tps'), nopm=w.get('nopm'),
                window_aborts=w.get('user_aborts_delta'), total_aborts=r['user_aborts_total'],
                record_bytes_per_committed_tx=wal.get('record_bytes_per_committed_tx'),
                fpi_bytes_per_committed_tx=wal.get('fpi_bytes_per_committed_tx'),
                total_restarts=r['total_restarts'], serialization_failures=r['serialization_failures'],
                noise_flags=';'.join(r['noise_flags']), failed_checks=';'.join(r['acceptance']['failed_checks']),
                path=r['path']))
    lines = ['# TPC-C-derived v3 results', '', LABEL + '.', '',
             'All attempts are retained, including noisy and rejected attempts. Medians include every '
             'attempt with a computable window; missing windows and failed acceptance make a point incomplete.', '',
             'Merkle includes nine extra expression lookup b-tree indexes. Autovacuum is enabled. '
             'The ranking host has one NUMA node; host contention must be assessed from saved evidence.', '',
             '| Mode | W | Workers | N | Attempts / requested | TPS median [min, max] | NOPM median [min, max] | Accepted |',
             '|---|---:|---:|---:|---:|---|---|---:|']
    groups = {}
    for r in results:
        groups.setdefault((r['mode'], r['warehouses'], r['workers'], r['count']), []).append(r)
    campaign = read_json(root / 'campaign.json')
    for (mode, warehouses, workers, count), group in sorted(groups.items()):
        def stats(key):
            values = [r['window'][key] for r in group if 'window' in r]
            return f'{statistics.median(values):.2f} [{min(values):.2f}, {max(values):.2f}]' if values else 'missing'
        lines.append(f'| {mode} | {warehouses} | {workers} | {count} | {len(group)} / {campaign.get("trials", "?")} '
                     f'| {stats("tps")} | {stats("nopm")} | {sum(r["acceptance"]["accepted"] for r in group)} |')
    lines += ['', '## Every attempt', '', '| Attempt | Window s | TPS | NOPM | Abort outcomes (window / total) | Status / noise |',
              '|---|---:|---:|---:|---|---|']
    for r in results:
        w = r.get('window', {})
        status = 'PASS' if r['acceptance']['accepted'] else 'REJECT: ' + ', '.join(r['acceptance']['failed_checks'])
        lines.append(f'| {Path(r["path"]).name} | {w.get("duration_s", "missing")} | {w.get("tps", "missing")} '
                     f'| {w.get("nopm", "missing")} | {w.get("user_aborts_delta", "unknown")} / {r["user_aborts_total"]} '
                     f'| {status}; {", ".join(r["noise_flags"])} |')
    # C3 cross-mode contract for identical generated SQL and initial seed.
    parity = []
    for key in {(r['warehouses'], r['workers'], r['trial'], r['count']) for r in results}:
        pair = [r for r in results if (r['warehouses'], r['workers'], r['trial'], r['count']) == key
                and r['mode'] in ('det', 'merkle')]
        if len(pair) == 2:
            hashes = [text(Path(r['path']) / 'workload.sha256').split()[0] for r in pair
                      if text(Path(r['path']) / 'workload.sha256').strip()]
            initials = [text(Path(r['path']) / 'initial_state.hash').strip() for r in pair]
            ok = len(hashes) == 2 and hashes[0] == hashes[1] and bool(initials[0]) and initials[0] == initials[1] and bool(pair[0]['state_hash']) and pair[0]['state_hash'] == pair[1]['state_hash']
            parity.append(dict(point=key, same_workload_and_state=ok))
            lines += ['', f'Det/Merkle state parity {key}: {"PASS" if ok else "FAIL"}.']
    (root / 'state_parity.json').write_text(json.dumps(parity, indent=2) + '\n')
    (root / 'summary.md').write_text('\n'.join(lines) + '\n')
    expected = campaign.get('trials', 0) * len(campaign.get('points', [])) * len(campaign.get('modes', []))
    campaign_ok = bool(results) and all(r['acceptance']['accepted'] for r in results) and all(p['same_workload_and_state'] for p in parity)
    if campaign:
        campaign_ok = campaign_ok and len(results) == expected
    (root / 'campaign_acceptance.json').write_text(json.dumps(dict(accepted=campaign_ok,
         attempts=len(results), expected_attempts=expected or None, state_parity=parity), indent=2) + '\n')
    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('root', type=Path)
    parser.add_argument('--window-only', action='store_true')
    args = parser.parse_args()
    if args.window_only:
        cfg = read_json(args.root / 'config.json')
        prog = [progress(s) for s in text(args.root / 'gateway.log').splitlines()]
        final = next((p for p in reversed(prog) if p and p['final'] == 1), None)
        inclusive = final is None or final['completed'] == cfg.get('count')
        w = select_window(samples(args.root / 'samples.csv'), cfg.get('warmup_s', 60), inclusive, tx_ratio(args.root))
        (args.root / 'window.json').write_text(json.dumps(w, indent=2) + '\n')
    else:
        summarize(args.root)


if __name__ == '__main__':
    main()
