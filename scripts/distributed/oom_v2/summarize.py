#!/usr/bin/env python3
"""Copy the accepted 84-case CSV and verify its evidence before marking DONE."""
import csv
import json
import shutil
import sys
from pathlib import Path

out = Path(sys.argv[1])
runs = sorted((out / 'campaign').glob('run_*/summary.csv'))
assert len(runs) == 1, runs
source = runs[0]
rows = list(csv.DictReader(source.open()))
expected = {(w, str(float(s)), m, n, 1) for w, s in [('a', 0), ('a', .99), ('a', 1.2), ('b', .99), ('c', .99), ('d', .99), ('f', .99)] for m in ['pg', 'bcdb_det', 'bcdb_merkle'] for n in [1, 4, 8, 16]}
actual = {(r['workload'], str(float(r['skew'])), r['mode'], int(r['workers']), int(r['trial'])) for r in rows}
assert len(rows) == 84 and actual == expected, (len(rows), expected - actual)
hashes = json.loads((out / 'provenance/published_workloads.json').read_text())
for row in rows:
    assert row['isolation'] == 'serializable', row
    for counter in ['divergence_count', 'permanent_failures', 'retry_exhausted']:
        assert int(row[counter]) == 0, row
    assert int(row['total_queries']) == int(row['validated_completed_queries']) == 20000
    assert row['workload_sha256'] == hashes[f"ycsb_{row['workload']}_skew_{float(row['skew'])}_20000.sql"]['sha256']
    case = Path(row['artifact_dir'])
    assert (case / 'result.json').is_file()
    assert not (case / 'FAILED.txt').exists() and not (case / 'cleanup_errors.txt').exists()
    setup = json.loads((case / 'setup.json').read_text())
    assert setup['settings']['merkle_apply_synchronous_direct'] == 'on'
    if row['mode'] == 'bcdb_merkle':
        assert row['merkle_verify'] == 'PASS' and (case / 'merkle_verify.txt').read_text().strip() == 't'
        for key, value in dict(partitions=200, fanout=32, split_threshold=1024, merge_threshold=256).items():
            assert setup['merkle_stats'][key] == value
shutil.copy2(source, out / 'summary.csv')
text = ['Fresh OOM v2 campaign: 84/84 accepted cases.', '',
        '100M freshly loaded rows; heap fillfactor 90; synchronous Merkle 200 partitions / fanout 32 / split 1024 / merge 256.',
        'SERIALIZABLE only; seed 42; 20K statements; workers 1,4,8,16; jitter on; delta/cold reset; 32MB timed shared_buffers.',
        'All seven SQL workload hashes match the published manifests. Zero divergence, permanent failures, and exhausted retries.',
        'All 28 Merkle cases have post-workload verification PASS. One trial per point; throughput differences are observations.', '',
        '| Workload | Skew | Mode | Workers | TPS | Read MiB | Write MiB |',
        '| --- | --- | --- | ---: | ---: | ---: | ---: |']
for r in sorted(rows, key=lambda r: (r['workload'], float(r['skew']), r['mode'], int(r['workers']))):
    text.append(f"| {r['workload']} | {r['skew']} | {r['mode']} | {r['workers']} | {float(r['tps']):.2f} | {float(r['device_read_mib']):.2f} | {float(r['device_write_mib']):.2f} |")
(out / 'summary.md').write_text('\n'.join(text) + '\n')
print('SUMMARY_PASS: 84 accepted cases')
