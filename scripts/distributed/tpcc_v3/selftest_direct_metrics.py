#!/usr/bin/env python3
"""Boundary, abort inclusion, per-type latency and drift rejection fixtures."""
import csv
import hashlib
import json
from pathlib import Path
import tempfile
from direct_metrics import analyze, stability, window
from selftest_summary import fixture
from summary import attempt, wal_stats

def main():
    with tempfile.TemporaryDirectory(prefix='direct-metrics-') as d:
        p=Path(d)
        # First completion is exactly at window start; last at window end must
        # be excluded. Submit times extend past the interval independently.
        sql=['SELECT public.new_order_proc_exec(1,1,1,ARRAY[1,2,3,4,100001],ARRAY[1,1,1,1,1],ARRAY[1,1,1,1,1],NULL);\n',
             'SELECT public.new_order_proc_exec(1,1,1,ARRAY[1,2,3,4,5],ARRAY[1,1,1,1,1],ARRAY[1,1,1,1,1],NULL);\n',
             'SELECT public.payment_proc_exec(1,1,1,1,1,1,NULL);\n',
             'SELECT public.order_status_proc_exec(1,1,1);\n',
             'SELECT public.delivery_proc_exec(1,1,NULL);\n',
             'SELECT public.stock_level_exec(1,1,10);\n',
             'SELECT public.payment_proc_exec(1,1,1,1,1,1,NULL);\n',
             'SELECT public.delivery_proc_exec(1,1,NULL);\n']
        (p/'workload.sql').write_text(''.join(sql))
        rows=[(0,1000,2000),(1,3000,4000),(2,4000,5000),(3,5000,6000),(4,6000,7000),(5,7000,8000),(6,31000,32000),(7,33000,34000)]
        with (p/'tx_timeline.csv').open('w',newline='') as f:
            w=csv.DictWriter(f,fieldnames=['tx_idx','latency_ms','finish_ms','submit_ms']);w.writeheader()
            w.writerows(dict(tx_idx=i,latency_ms=fin-sub,finish_ms=fin,submit_ms=sub) for i,sub,fin in rows)
        (p/'gateway.log').write_text('PROGRESS_GATEWAY_DET elapsed_s=34.0 final=1\n')
        r=analyze(p,dict(count=8,warmup_s=2,min_window_s=30))
        assert r['window_business_count']==6
        assert r['window_new_order_including_rollbacks']==2
        assert r['window_new_order_rollbacks']==1
        assert r['tpmc_equivalent']==4 and r['committed_only_nopm']==2
        assert r['total_business_tps']==.2
        assert r['response_times']['payment']['p90_ms']==1000
        assert r['window_fully_covered_before_submission_ends']
        samples=[dict(elapsed_s=e,progress_age_s=age,wall_epoch=100+e+age,district_next=1000,wal_lsn='0/100',error='') for e,age in [(2,.1),(3,.2),(30,.2),(31,2)]]
        w=window(p,dict(count=8,warmup_s=2,min_window_s=30),samples)
        assert w['start']['elapsed_s']==3 and w['end']['elapsed_s']==30
        assert stability([100]*10,60)['passed']
        assert stability([100,102,98,102,98,98,102,98,102,100],60)['passed']
        assert not stability(list(range(100,110)),60)['passed']
        assert not stability([10,20,30,40,50,60,70,80,90,100],60)['passed']
        assert not stability([100,100,0,0,100,100,100,100,100,100],60)['passed']
        # Start with a complete direct-window fixture whose only failure is
        # Delivery mix, then repair only the mix and require full acceptance.
        gate=fixture(p/'hard_mix_gate',mode='pg')
        config=json.loads((gate/'config.json').read_text())
        config.update(measurement_method='direct_fixed',smoke=True)
        (gate/'config.json').write_text(json.dumps(config))
        (gate/'initial_consistency.txt').write_bytes((gate/'consistency.txt').read_bytes())
        pattern=[0]*45+[1]*44+[2]*4+[3]*3+[4]*4
        def gate_workload(pattern):
            body=[]
            for i in range(50000):
                t=pattern[i%100]
                body.append(sql[0] if t==0 and i%100==0 else sql[1] if t==0 else sql[t+1])
            (gate/'workload.sql').write_text(''.join(body))
        gate_workload(pattern)
        with (gate/'tx_timeline.csv').open('w',newline='') as f:
            writer=csv.DictWriter(f,fieldnames=['tx_idx','latency_ms','finish_ms','submit_ms']);writer.writeheader()
            for i in range(50000):
                fin=(i+1)*10;sub=max(0,fin-100)
                writer.writerow(dict(tx_idx=i,finish_ms=fin,submit_ms=sub,latency_ms=fin-sub))
        result=attempt(gate)
        assert result['acceptance']['failed_checks']==['mix_minimum_delivery'],result['acceptance']
        # Change one Payment per100 to Delivery: both meet their minima.
        pattern[88]=3;gate_workload(pattern)
        (gate/'direct_metrics.json').unlink()
        result=attempt(gate)
        assert result['acceptance']['accepted'],result['acceptance']
        # Exact WAL chain is independently a hard gate; altered raw bytes
        # cannot be hidden behind the aggregate Total line.
        config['wal_accounting']='incremental_record_ranges_v1'
        (gate/'config.json').write_text(json.dumps(config))
        selected=json.loads((gate/'window.json').read_text())
        raw=(gate/'walstats.txt').read_bytes();stats=wal_stats(raw.decode())
        (gate/'wal_chunks').mkdir();(gate/'wal_chunks/part0000.txt').write_bytes(raw)
        parts=[dict(start_lsn=selected['start']['wal_lsn'],end_lsn=selected['end']['wal_lsn'],
                    stats=stats,raw_sha256=hashlib.sha256(raw).hexdigest())]
        (gate/'wal_chunks.json').write_text(json.dumps(parts))
        (gate/'wal_collection.json').write_text(json.dumps(dict(covered=True,start=selected['start'],end=selected['end'])))
        assert attempt(gate)['acceptance']['accepted']
        (gate/'wal_chunks/part0000.txt').write_bytes(raw+b'altered\n')
        assert attempt(gate)['acceptance']['failed_checks']==['wal_incremental_ranges']
        # Duplicate completion records cannot create fabricated throughput.
        with (p/'tx_timeline.csv').open('a') as f:f.write('0,1000,2000,1000\n')
        try: analyze(p,dict(count=8,warmup_s=2,min_window_s=30))
        except ValueError: pass
        else: raise AssertionError('duplicate transaction was accepted')
    print('PASS direct counts, rollback inclusion, boundary handling, type response times, coverage, stable/drifting/stalled series and duplicate rejection')

if __name__=='__main__':main()
