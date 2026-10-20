#!/usr/bin/env python3
"""Check the submitted matrix, mode-specific sizing and fixed-N parity contract."""
import json
from pathlib import Path
import tempfile
import time
from standard_campaign import preparation_tasks,construct_plan,write,compress_bulk,coverage_retry_n
import hashlib

short=dict(max_submit_ms=200000,completion_records=900000,clock_alignment_valid=True,
           capacity_profile=dict(max_business_tps=5000))
assert coverage_retry_n(900000,short,300,60)>5000*360
assert coverage_retry_n(900000,{},300,60)==1350000
assert coverage_retry_n(900000,dict(short,clock_alignment_valid=False),300,60)==1350000

with tempfile.TemporaryDirectory(prefix='standard-plan-') as directory:
    home=Path(directory);tasks=preparation_tasks(dict(pg=1500,det=700,merkle=350))
    assert len(tasks)==50
    assert len({(t['w'],t['workers'],t['mode']) for t in tasks if t['kind']=='pilot'})==36
    for i,t in enumerate(tasks):
        run=home/f'run{i}';run.mkdir()
        (run/'initial_state.hash').write_text(f'initial_w{t["w"]}\n')
        (run/'state.hash').write_text(f'final_w{t["w"]}\n')
        (run/'workload.sha256').write_text(f'work_w{t["w"]} workload.sql\n')
        rate=dict(pg=1500,det=700,merkle=350)[t['mode']]
        write(run/'acceptance.json',dict(direct_metrics=dict(total_business_tps=rate,ramp_s=300,
              throughput_series=[dict(total_business_tps=rate)],max_finish_ms=180000)))
        t['attempts']=[dict(path=str(run),qualified=True,elapsed_s=240)]
    assert construct_plan(home,dict(tasks=tasks,started_epoch=time.time()))
    plan=json.loads((home/'standard_plan.json').read_text());real=plan['tasks']
    assert len(real)==111 and sum(t['kind']=='headline' for t in real)==3
    for trial in (1,2,3):
        group=[t for t in real if t['kind']=='sweep' and t['trial']==trial]
        assert len(group)==36
    sizes=[next(t['n'] for t in real if t['kind']=='sweep' and t['w']==100 and t['workers']==32 and t['trial']==1 and t['mode']==mode) for mode in ('pg','det','merkle')]
    assert sizes[0]>sizes[1]>sizes[2]
    assert all(p['same_workload_and_state'] for p in plan['parity'])
    # Duration runs must not silently pass a broken correctness pair.
    (home/'run49'/'state.hash').write_text('diverged\n')
    try:construct_plan(home,dict(tasks=tasks))
    except AssertionError:pass
    else:raise AssertionError('parity mismatch admitted')
with tempfile.TemporaryDirectory(prefix='archive-resume-') as directory:
    run=Path(directory);body=b'raw evidence row\n'*100000
    (run/'workload.sql').write_bytes(body)
    write(run/'EVIDENCE.sha256.json',{'workload.sql':hashlib.sha256(body).hexdigest()})
    compress_bulk(run)
    (run/'LOSSLESS_ARCHIVES.json').unlink()  # simulate controller crash after unlink
    compress_bulk(run)
    records=json.loads((run/'LOSSLESS_ARCHIVES.json').read_text())
    assert records[0]['original_sha256']==hashlib.sha256(body).hexdigest()
    assert records[0]['original_bytes']==len(body)
print('PASS 108 rotating sweeps + 3 headlines, per-mode N, seven fixed-N pairs, parity rejection')
