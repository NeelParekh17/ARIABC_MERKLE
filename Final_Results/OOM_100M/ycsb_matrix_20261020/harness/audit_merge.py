# Audit all 80 matrix cases and merge them into one summary.csv.
import csv, json, collections, datetime
from pathlib import Path
ROOT = Path.home() / 'claude_ctl/ycsb_matrix_20261020'
M = ROOT / 'results/matrix'
samp = json.loads((ROOT / 'evidence/sampler_per_case.json').read_text())
def lsn(s): a, b = s.split('/'); return (int(a, 16) << 32) + int(b, 16)
def ts(s): return datetime.datetime.fromisoformat(s)
rows, errors = [], []
for res in sorted(M.glob('*_*/run_*/*/result.json')):
    case = res.parent; camp = case.parent.parent.name
    r = json.loads(res.read_text())['row']
    def err(m): errors.append(f'{camp}/{case.name}: {m}')
    if (case / 'FAILED.txt').exists() or (case / 'cleanup_errors.txt').exists(): err('failure artifact')
    st = json.loads((case / 'setup.json').read_text())['settings']
    if st.get('default_transaction_isolation') != 'serializable': err('isolation ' + str(st.get('default_transaction_isolation')))
    if st.get('shared_buffers') not in ('4096', '32MB'): err('shared_buffers ' + str(st.get('shared_buffers')))
    if r['isolation'] != 'serializable' or r['shared_buffers'] != '32MB': err('row config')
    for k in ('retry_exhausted', 'divergence_count', 'permanent_failures'):
        if float(r[k] or 0) != 0: err(f'{k}={r[k]}')
    if int(r['validated_completed_queries']) != 2400000 or int(r['total_queries']) != 2400000: err('count')
    merkle = r['mode'] == 'bcdb_merkle'
    mv = (case / 'merkle_verify.txt').read_text().strip() if (case / 'merkle_verify.txt').exists() else None
    if merkle and (mv != 't' or r['merkle_verify'] != 'PASS'): err(f'verify {mv} {r["merkle_verify"]}')
    w = json.loads((case / 'wal.json').read_text()); wp = json.loads((case / 'wal_post_checkpoint.json').read_text())
    if w['before']['postmaster_start'] != wp['postmaster_start']: err('postmaster restarted before checkpoint snapshot')
    wal_full = (lsn(wp['lsn']) - lsn(w['before']['lsn'])) / 2400000
    bracket_s = (ts(wp['time']) - ts(w['before']['time'])).total_seconds()
    sk = f'{camp}/{case.parent.name}/{case.name}'; s = samp.get(sk)
    if s is None or s['meas'] < 50: err('sampler rows missing')
    elif s['meas_err']: err(f'sampler measurement errors {s["meas_err"]}')
    am = r['merkle_lookup_am'] if merkle else ''
    if merkle and am == 'merkle_lsm' and (s is None or s['final'] is None): err('lsm state missing')
    io = json.loads((case / 'io.json').read_text())
    rows.append(dict(
        workload=r['workload'], skew=float(r['skew']), mode=r['mode'], lookup_am=am,
        label={'pg': 'pg', 'bcdb_det': 'det'}.get(r['mode']) or ('merkle_lsm' if am == 'merkle_lsm' else 'merkle_btree'),
        tps=round(float(r['tps']), 2), gateway_s=round(int(r['wall_time_ms']) / 1000, 3),
        lsm_drain_s=round(float(r['lsm_drain_ms']) / 1000, 3) if am == 'merkle_lsm' else '',
        run_to_checkpoint_s=round(bracket_s, 3), run_to_checkpoint_stmt_s=round(2400000 / bracket_s, 1),
        wal_window_b_per_stmt=round(float(r['wal_bytes_per_statement']), 2), wal_full_b_per_stmt=round(wal_full, 2),
        device_read_mib=round(float(r['device_read_mib']), 1), device_write_mib=round(float(r['device_write_mib']), 1),
        read_await_ms=round(float(r['device_read_await_ms']), 3),
        write_await_ms=round((io['device_after']['write_ms'] - io['device_before']['write_ms']) /
                             max(1, io['device_after']['write_ios'] - io['device_before']['write_ios']), 3),
        retry_attempts=int(float(r['retry_attempts'])), retry_exhausted=int(float(r['retry_exhausted'])),
        merkle_verify=r['merkle_verify'], lsm_l0_max_bytes=s['l0'] if am == 'merkle_lsm' else '',
        lsm_l1_max_bytes=s['l1'] if am == 'merkle_lsm' else '', lsm_merges=json.dumps(s['merges']) if am == 'merkle_lsm' else '',
        reset_s=round(float(r['reset_time_ms']) / 1000, 1), restore_write_mib=round(float(r['restore_write_mib']), 1),
        settle_qd1_us=round(float(r['settle_qd1_median_us']), 1), workload_sha256=r['workload_sha256'],
        campaign=camp, case_dir=sk))
# Coverage and workload parity.
keys = collections.Counter((x['workload'], x['skew'], x['label']) for x in rows)
want = {(w, s, l) for w in 'abcdf' for s in (0.0, 0.5, 0.99, 1.2) for l in ('pg', 'det', 'merkle_lsm', 'merkle_btree')}
if set(keys) != want or any(v != 1 for v in keys.values()): errors.append(f'coverage: {len(rows)} rows, missing {want - set(keys)}')
sha = collections.defaultdict(set)
for x in rows: sha[(x['workload'], x['skew'])].add(x['workload_sha256'])
errors += [f'sha parity {k}' for k, v in sha.items() if len(v) != 1]
rows.sort(key=lambda x: ('dcbaf'.index(x['workload']), x['skew'], ['pg', 'det', 'merkle_lsm', 'merkle_btree'].index(x['label'])))
with open(ROOT / 'evidence/matrix_summary_80.csv', 'w', newline='') as f:
    wr = csv.DictWriter(f, fieldnames=list(rows[0])); wr.writeheader(); wr.writerows(rows)
print(json.dumps(dict(rows=len(rows), errors=errors, retries={f"{x['workload']}/{x['skew']}/{x['label']}": x['retry_attempts'] for x in rows if x['retry_attempts']}), indent=1))
