"""Read-only comparison of the B-tree anchor deviation with reference trials."""
import csv
import json
from pathlib import Path

ROOT = Path('/home/neel/claude_ctl/ycsb_matrix_20261020')
case = next((ROOT / 'results/matrix/d_btree').glob('run_*/d_s0.99_bcdb_merkle_w16_t1'))
new = json.loads((case / 'result.json').read_text())['row']
settings = json.loads((case / 'setup.json').read_text())['settings']
columns = ['tps', 'wall_time_ms', 'device_read_mib', 'device_write_mib',
           'device_read_await_ms', 'restore_write_mib', 'settle_wait_s',
           'settle_qd1_median_us', 'retry_attempts', 'retry_exhausted', 'workload_sha256']
old = []
reference = ROOT / 'src/Final_Results/LSM_LOOKUP/merkle_lsm_ycsb_d_20261019/comparison'
for path in reference.rglob('summary.csv'):
    for row in csv.DictReader(path.open()):
        if row['mode'] != 'bcdb_merkle':
            continue
        entry = {key: row.get(key) for key in columns}
        setup = path.parent / Path(row['artifact_dir']).name / 'setup.json'
        if not setup.exists():
            setup = Path(row['artifact_dir']) / 'setup.json'
        if setup.exists():
            original = json.loads(setup.read_text())['settings']
            entry['setting_differences'] = {key: [original.get(key), value]
                                          for key, value in settings.items()
                                          if original.get(key) != value}
        old.append(entry)
samples = [json.loads(line) for line in (case / 'telemetry.jsonl').read_text().splitlines()]
def vm(row):
    return {key: int(value) for key, value in (line.split() for line in row['vmstat'].splitlines())}
first, last = vm(samples[0]), vm(samples[-1])
memory = [int(next(line.split()[1] for line in row['meminfo'].splitlines()
                   if line.startswith('MemAvailable:'))) / 1024 for row in samples]
intervals = []
for before, after in zip(samples, samples[1:]):
    def disk(row):
        values = next(line.split()[3:] for line in row['diskstats'].splitlines()
                      if line.split()[2] == 'nvme0n1p2')
        return list(map(int, values))
    a, b = disk(before), disk(after)
    reads = b[0] - a[0]
    if reads > 0:
        intervals.append(dict(t=after['unix_ns'] / 1e9, reads=reads,
                              read_await_ms=(b[3] - a[3]) / reads))
report = dict(new={key: new.get(key) for key in columns}, reference=old,
              anchor_difference_pct=(new['tps'] / 13091 - 1) * 100,
              vm_deltas={key: last.get(key, 0) - first.get(key, 0)
                         for key in ['pswpin', 'pswpout', 'oom_kill', 'pgmajfault']},
              minimum_mem_available_mib=min(memory),
              worst_read_intervals=sorted(intervals, key=lambda row: row['read_await_ms'], reverse=True)[:5],
              conclusion='Observed higher run-time device read latency; identical workload hash. '
                         'This comparison alone does not establish the cause of the latency change.')
(ROOT / 'evidence/anchor_investigation.json').write_text(json.dumps(report, indent=2) + '\n')
print(json.dumps(report, indent=2))
