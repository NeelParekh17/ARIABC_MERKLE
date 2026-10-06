#!/usr/bin/env python3
"""Database-free fixture tests for window, NOPM, WAL, counters and rejection gates."""
import csv
import json
import math
import tempfile
from pathlib import Path
from summary import TABLES, LABEL, attempt, progress, samples, select_window, summarize, wal_stats
from sampler import COLUMNS
from recommend_n import recommend


def fixture(root, mode='merkle', exclusive=False):
    root.mkdir()
    cfg = dict(mode=mode, warehouses=5, workers=32, count=50000, trial=1,
               warmup_s=60, min_window_s=300, expected_settings={'autovacuum':'on'})
    (root/'config.json').write_text(json.dumps(cfg))
    (root/'runtime.json').write_text(json.dumps(dict(gateway_rc=0, consistency_rc=0,
        state_hash_rc=0, waldump_rc=0, finished=True, sampler_rc=0, stop_rc=0, postgres_stopped=True)))
    (root/'provenance.json').write_text('{}')
    (root/'workload.sql.meta.json').write_text(json.dumps({'expected_rollbacks':500, 'counts':{'new_order':22500}}))
    (root/'initial_district_next.txt').write_text('150050\n')
    (root/'final_district_next.txt').write_text('172050\n')
    (root/'workload.sha256').write_text('aabbcc  workload.sql\n')
    state = ' '.join(t+'=100:1234' for t in sorted(TABLES))
    (root/'state.hash').write_text(state+'\n')
    (root/'initial_state.hash').write_text(state+'\n')
    (root/'settings.csv').write_text('name,setting\nautovacuum,on\n')
    with (root/'relations.csv').open('w') as f:
        f.write('relname,relpersistence,reloptions\n')
        f.writelines(t+',p,fillfactor=90\n' for t in sorted(TABLES))
    with (root/'indexes.csv').open('w') as f:
        f.write('tablename,indexname\n')
        f.writelines(t+','+t+'_merkle_lookup_idx\n' for t in sorted(TABLES))
    (root/'merkle_verify.txt').write_text(''.join(t+'|t\n' for t in sorted(TABLES)))
    (root/'consistency.txt').write_text(''.join(f'condition_{i}_test=true\n' for i in range(1,5))+'consistency_ok=t\n')
    (root/'postgres_workload.log').write_text('2026-01-01 00:03:00.000 UTC [123] LOG:  checkpoint starting: time\n'
        '2026-01-01 00:03:10.000 UTC [123] LOG:  checkpoint complete: wrote 50 buffers\n')
    (root/'server.log').write_text('PROFILE_SERVER retryable_sqlstate_40001=7 retry_attempts_total=9 retry_exhausted_total=0\n')
    epoch = 1767225600
    logs = []
    with (root/'samples.csv').open('w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS); writer.writeheader()
        for i in range(1, 501):
            completed = i*100 - (i if exclusive else 0)
            sent = min(50000, i*125)
            row = dict(wall_epoch=epoch+i, elapsed_s=i, completed=completed, sent=sent,
                       total=50000, user_aborts=i, district_next=150050+i*44,
                       wal_lsn='0/'+format(0x100000+i*2000,'X'), progress_age_s=.1,
                       query_ms=5, load1=10, affinity_ok=1, other_cpu_json='[]', error='')
            writer.writerow(row)
            logs.append(f'PROGRESS_GATEWAY_DET wall_time_unix_ms={(epoch+i)*1000} elapsed_s={i}.0 '
                f'total=50000 sent={sent} completed={completed} user_aborts={i} permanent_failures=0 divergence_count=0')
        logs[-1] += ' final=1'
    (root/'gateway.log').write_text('\n'.join(logs)+'\nuser_aborts=500\npermanent_failures=0\ndivergence_count=0\n')
    (root/'walstats.txt').write_text('Type N (%) Record size (%) FPI size (%) Combined size (%)\n'
        'Total 1000 335610 [83.33%] 67122 [16.67%] 402732 [100%]\n')
    return root


def main():
    with tempfile.TemporaryDirectory(prefix='tpcc-v3-summary-test-') as tmp:
        base = Path(tmp)
        run = fixture(base/'merkle')
        w = select_window(samples(run/'samples.csv'), 60)
        assert w['start']['elapsed_s'] == 60 and w['end']['elapsed_s'] == 399
        assert w['duration_s'] == 339 and w['completed_delta'] == 33900
        assert w['committed_delta'] == 33561 and w['user_aborts_delta'] == 339
        assert w['tps'] == 100 and w['nopm'] == 2640
        assert w['new_orders_delta'] == 14916
        assert wal_stats((run/'walstats.txt').read_text())['record_bytes'] == 335610
        r = attempt(run)
        assert r['acceptance']['accepted'], r['acceptance']
        assert r['wal']['record_bytes_per_committed_tx'] == 10
        assert r['wal']['fpi_bytes_per_committed_tx'] == 2
        assert len(r['checkpoints_in_window']) == 2
        assert r['serialization_failures'] == '7'
        assert r['nopm_label'] == LABEL
        other = fixture(base/'det', mode='det', exclusive=True)
        exclusive = attempt(other)
        assert exclusive['acceptance']['accepted'], exclusive['acceptance']
        assert exclusive['window']['tps'] == 100 and exclusive['window']['committed_delta'] == 33561
        rows = samples(run/'samples.csv')
        for i, row in enumerate(rows):
            t = i+1
            row['sent'] = t*100
            row['total'] = 100000
            row['completed'] = min(t,250)*100+max(0,t-250)*10
        falling = select_window(rows,60)
        # Stalls/slowdowns stay inside the window (no truncation); only submission
        # completion or the last observation ends it.
        assert falling['end']['elapsed_s'] == 500, falling
        assert falling['end_reason'] == 'before_last_observation'
        ratio = select_window(rows, 60, True, 2.0)
        assert ratio['tps_source'] == 'db_new_orders_x_workload_ratio'
        assert abs(ratio['tps'] - ratio['new_orders_delta'] * 2.0 / ratio['duration_s']) < 1e-9
        short = rows[:10]
        try:
            select_window(short,60)
            raise AssertionError('short fixture incorrectly yielded window')
        except ValueError:
            pass
        (run/'consistency.txt').write_text('consistency_ok=f\n')
        (run/'settings.csv').write_text('name,setting\nautovacuum,off\n')
        rejected = attempt(run)
        assert not rejected['acceptance']['accepted']
        assert 'consistency' in rejected['acceptance']['failed_checks']
        assert 'settings' in rejected['acceptance']['failed_checks']
        assert progress('PROGRESS_GATEWAY_DET elapsed_s=1 completed=2 sent=3 total=4')['user_aborts'] is None
        # Missing counters cannot silently turn a failed run into a successful one.
        (other/'gateway.log').write_text((other/'gateway.log').read_text().replace('user_aborts=', 'missing_aborts='))
        assert not attempt(other)['acceptance']['accepted']
        summarized = summarize(base)
        assert len(summarized) == 2 and (base/'summary.md').exists()
        assert len(list(csv.DictReader((base/'results.csv').open()))) == 2
        assert not json.loads((base/'campaign_acceptance.json').read_text())['accepted']
        rec = recommend({'pg':1000,'det':500,'merkle':250})
        assert rec['count'] == 590536 and rec['count'] > 1000*(60+300+60)+65536
        assert math.isclose(w['nopm'], 60*w['new_orders_delta']/w['duration_s'])
    print('PASS: warmup/drain window, sustained-drop cutoff, NOPM, WAL record/FPI math, inclusive/exclusive abort counters, checkpoints, rejection gates, all-attempt reporting, N recommendation')


if __name__ == '__main__':
    main()
