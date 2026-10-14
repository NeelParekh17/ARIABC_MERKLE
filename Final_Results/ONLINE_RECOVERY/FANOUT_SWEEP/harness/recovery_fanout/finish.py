#!/usr/bin/env python3
"""Wait for the complete remote matrix, then produce reports and PDF/PNG plots."""
import argparse
import datetime
import json
import os
from pathlib import Path
import subprocess
import time


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--base', type=Path, required=True)
    a = p.parse_args()
    b = a.base.resolve()
    if '10.129.148.247' not in subprocess.check_output(['hostname', '-I'], text=True).split():
        p.error('postprocess on .247')
    while True:
        status = json.loads((b / 'results/status.json').read_text())
        if status['state'] == 'STOPPED_REVIEW_REQUIRED':
            print('Campaign stopped for review; publication artifacts are not complete.', flush=True)
            return 2
        if status['state'] == 'DONE' and status['phase'] == 'matrix':
            break
        time.sleep(15)
    # The supervisor writes its exit marker after its own stdlib analysis.
    while not (b / 'results/supervisor_exit.env').exists() or 'supervisor_exit_code=0' not in (b / 'results/supervisor_exit.env').read_text():
        time.sleep(2)
    scripts = b / 'repo/scripts/distributed/recovery_fanout'
    subprocess.run(['python3', str(scripts / 'analyze.py'), '--base', str(b)], check=True)
    subprocess.run(['python3', str(scripts / 'curate.py'), '--base', str(b)], check=True)
    result = dict(matrix_complete=True, figures_complete=False,
                  time=datetime.datetime.now(datetime.timezone.utc).isoformat())
    # Dependencies are installed only after measurement, in the campaign's venv.
    # Lab hosts without python3-venv (no sudo) get a pip --target directory instead.
    try:
        env = b / 'analysis_venv'
        lib = b / 'analysis_pylib'
        with (b / 'results/plot_dependencies.log').open('a') as log:
            made_venv = subprocess.run(['python3', '-m', 'venv', '--system-site-packages', str(env)],
                                       stdout=log, stderr=subprocess.STDOUT).returncode == 0
            if made_venv:
                python, extra_env, freeze = str(env / 'bin/python'), {}, []
                install = ['matplotlib==3.10.7']
            else:
                subprocess.run(['rm', '-rf', str(env)], check=True)
                python, extra_env, freeze = 'python3', {'PYTHONPATH': str(lib)}, ['--path', str(lib)]
                install = ['--target', str(lib), 'matplotlib==3.10.7']
                print('venv unavailable; using pip --target ' + str(lib), file=log, flush=True)
            env_vars = {**os.environ, **extra_env}
            subprocess.run([python, '-m', 'pip', 'install', '-q', *install],
                           stdout=log, stderr=subprocess.STDOUT, check=True, timeout=600, env=env_vars)
        subprocess.run([python, str(scripts / 'analyze.py'), '--base', str(b), '--plot'], check=True, env=env_vars)
        with (b / 'provenance/plot_dependencies.txt').open('w') as f:
            subprocess.run([python, '-m', 'pip', 'freeze', *freeze], stdout=f, check=True, env=env_vars)
        result['figures_complete'] = True
    except (OSError, subprocess.SubprocessError) as error:
        result['figure_error'] = str(error)
    (b / 'results/artifact_status.json').write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(result), flush=True)
    return 0 if result['figures_complete'] else 3


if __name__ == '__main__':
    raise SystemExit(main())
