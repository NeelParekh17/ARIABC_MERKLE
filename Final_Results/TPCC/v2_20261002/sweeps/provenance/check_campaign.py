import json,sys,tempfile
from pathlib import Path
root=Path(sys.argv[1]); sys.path.insert(0,str(root/'scripts'))
import sweep_summary as s
assert len(s.POINTS)==len(set(s.POINTS))==12
assert set(s.POINTS)=={(w,32) for w in s.WAREHOUSES}|{(100,k) for k in s.WORKERS}
assert len(s.POINTS)*len(s.MODES)==36
with tempfile.TemporaryDirectory(dir=root/'provenance') as temp:
    test=Path(temp)
    for w,k in s.POINTS:
        for mode in s.MODES:
            tps=100.0
            if (w,k,mode) in ((5,32,'pg'),(100,32,'merkle')): tps=64.0
            if (w,k,mode)==(100,64,'det'): tps=65.0
            run=test/f'{mode}_test_k{k}_w{w}'; run.mkdir()
            (run/'accepted.json').write_text(json.dumps(dict(group='matrix',trial=1,W=w,workers=k,mode=mode,tps=tps)))
    flags=s.stalls(test)
    assert set(flags)=={(5,32,'pg'),(100,32,'merkle')},flags
    assert 'W=' in flags[(100,32,'merkle')] and 'workers=' in flags[(100,32,'merkle')]
print('PASS: 36 distinct matrix runs, full requested point coverage, endpoint/interior threshold, strict <0.65, shared-point retry deduplication')
for p in root.glob('*_smoke*_w5/accepted.json'):
    r=json.loads(p.read_text())
    print(json.dumps({k:r[k] for k in ('mode','tps','completed','perm_fail','div','merkle_verify','wal_per_tx','restarts')}))
