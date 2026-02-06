import glob
import os
import re
import csv
import sys

# Pattern for filename: benchmark_results_{scale}_{algo}_run{run}.txt
# Example: benchmark_results_10k_md5_run1.txt
filename_pattern = re.compile(r'benchmark_results_(\d+k)_(\w+)_run(\d+)\.txt')

# Patterns for file content
# Note: The logs might have float values or slightly different formatting, but based on the example:
# Insert Time: 5s, Throughput: 2000 ops/sec
insert_pattern = re.compile(r'Insert Time: ([\d\.]+)s, Throughput: ([\d\.]+) ops/sec')
update_pattern = re.compile(r'Update Time: ([\d\.]+)s, Throughput: ([\d\.]+) ops/sec')
delete_pattern = re.compile(r'Delete Time: ([\d\.]+)s, Throughput: ([\d\.]+) ops/sec')

results = []

path = '/work/ARIABC/AriaBC/scripts/benchmark_results_*.txt'
files = glob.glob(path)

for fpath in files:
    fname = os.path.basename(fpath)
    match = filename_pattern.search(fname)
    if not match:
        continue
    
    scale, algo, run = match.groups()
    
    with open(fpath, 'r') as f:
        content = f.read()
        
    insert_match = insert_pattern.search(content)
    update_match = update_pattern.search(content)
    delete_match = delete_pattern.search(content)
    
    row = {
        'Scale': scale,
        'Algorithm': algo,
        'Run': run,
        'Insert Time (s)': insert_match.group(1) if insert_match else 'N/A',
        'Insert TPS': insert_match.group(2) if insert_match else 'N/A',
        'Update Time (s)': update_match.group(1) if update_match else 'N/A',
        'Update TPS': update_match.group(2) if update_match else 'N/A',
        'Delete Time (s)': delete_match.group(1) if delete_match else 'N/A',
        'Delete TPS': delete_match.group(2) if delete_match else 'N/A',
    }
    results.append(row)

# Sort results
def parse_scale(s):
    return int(s.replace('k', '000'))

results.sort(key=lambda x: (parse_scale(x['Scale']), x['Algorithm'], int(x['Run'])))

writer = csv.DictWriter(sys.stdout, fieldnames=['Scale', 'Algorithm', 'Run', 'Insert Time (s)', 'Insert TPS', 'Update Time (s)', 'Update TPS', 'Delete Time (s)', 'Delete TPS'])
writer.writeheader()
writer.writerows(results)
