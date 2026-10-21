"""One diagnostic repeat with unchanged case settings, while D review is paused.

The official matrix row is retained. This writes a separate campaign and never
approves or advances the matrix controller.
"""
import json
import os
from pathlib import Path
import sys

ROOT = Path('/home/neel/claude_ctl/ycsb_matrix_20261020')
status = (ROOT / 'results/matrix/status.txt').read_text()
if 'D_GROUP_REVIEW_WAIT' not in status or (ROOT / 'evidence/d_group.approved').exists():
    raise SystemExit('The matrix must be paused for D review before this diagnostic')
manifest = next((ROOT / 'results/matrix/d_btree').glob('run_*/campaign.json'))
arguments = json.loads(manifest.read_text())['arguments']
arguments.update(combos=[['d', .99]], out_dir=str(ROOT / 'results/anchor_check'),
                 resume_dir=None, auto_resume=True)
argv = []
for key, value in arguments.items():
    if key.startswith('_') or value is None:
        continue
    flag = '--' + key.replace('_', '-')
    if isinstance(value, bool):
        if value:
            argv.append(flag)
    elif key == 'combos':
        argv.extend([flag, ','.join(f'{workload}:{skew}' for workload, skew in value)])
    elif isinstance(value, list):
        argv.extend([flag, *map(str, value)])
    else:
        argv.extend([flag, str(value)])
os.chdir(ROOT / 'src')
os.environ['TMPDIR'] = str(ROOT / 'scratch')
os.environ['PYTHONDONTWRITEBYTECODE'] = '1'
sys.path.insert(0, str(ROOT / 'src/scripts/distributed'))
from run_oom_100m_benchmark import main
print('Diagnostic only; original matrix row retained. Arguments:', argv, flush=True)
main(argv)
