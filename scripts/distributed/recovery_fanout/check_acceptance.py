#!/usr/bin/env python3
"""Exercise acceptance failure cases using explicitly synthetic fixture copies.

Run on .247. These checks validate the collector, never the engine or TPS.
"""
import argparse
import json
from pathlib import Path
import re
import shutil
import subprocess
import tempfile


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--base', type=Path, required=True)
    p.add_argument('--fixtures', type=Path, required=True)
    a = p.parse_args()
    if '10.129.148.247' not in subprocess.check_output(['hostname', '-I'], text=True).split():
        p.error('checks only on .247')
    config = json.loads((a.base / 'campaign.json').read_text())
    checks = []
    with tempfile.TemporaryDirectory(prefix='contract_', dir=a.base / 'scratch') as temporary:
        for mode in ('A', 'M_passive'):
            source = a.fixtures / mode
            changes = ['valid', 'missing_vote', 'wrong_fanout', 'wrong_heap', 'bad_merkle',
                       'missing_health', 'failed_gateway', 'wrong_head', 'missing_profile']
            if mode == 'M_passive':
                changes.extend(['interleaved_recovery', 'missing_fetched_rows',
                                'interleaved_without_fetched_rows'])
            for change in changes:
                out = Path(temporary) / (mode + '_' + change)
                shutil.copytree(source, out)
                case = (out / 'recovery_s1024.env').read_text()
                case += '\nrows=12000\nfanout=32\nsynthetic_contract_fixture=true\n'
                (out / 'recovery_s1024.env').write_text(case)
                meta = (out / 'run_meta.env').read_text()
                meta = re.sub(r'(?m)^git_head=.*$', 'git_head=' + config['source_head'], meta)
                (out / 'run_meta.env').write_text(meta)
                if change == 'missing_vote':
                    path = out / 'run_summary.env'
                    path.write_text(path.read_text().replace('async_all3_missing_count=0', 'async_all3_missing_count=1'))
                elif change == 'wrong_fanout':
                    path = out / 'geometry_10.129.148.246.txt'
                    path.write_text(path.read_text().replace("fanout='32'", "fanout='64'"))
                elif change in ('wrong_heap', 'bad_merkle'):
                    path = out / 'post_verify_readback_node4_utkarsh.out'
                    parts = path.read_text().strip().split('|')
                    parts[0 if change == 'wrong_heap' else 2] = '12000' if change == 'wrong_heap' else 'f'
                    path.write_text('|'.join(parts) + '\n')
                elif change == 'missing_health':
                    (out / 'health/samples.jsonl').write_text('')
                elif change == 'failed_gateway':
                    (out / 'gateway_exit.env').write_text('gateway_exit_code=1\n')
                elif change == 'wrong_head':
                    (out / 'run_meta.env').write_text(meta.replace(config['source_head'], '0' * 40))
                elif change == 'missing_profile':
                    (out / 'server_node4_utkarsh.log').write_text('')
                elif change in ('interleaved_recovery', 'missing_fetched_rows',
                                'interleaved_without_fetched_rows'):
                    for path in out.glob('server_node*.log'):
                        text = path.read_text()
                        if change != 'missing_fetched_rows':
                            text = text.replace('RECOVERY_CTRL ', 'RECOVERY_REPLAY_LIVE node=4 from=RECOVERY_CTRL ')
                        if change != 'interleaved_recovery':
                            text = re.sub(r'candidate_rows=\d+', '', text)
                        path.write_text(text)
                result = subprocess.run(['python3', str(a.base / 'collect_run.py'), str(out)], capture_output=True, text=True)
                record = json.loads((out / 'acceptance.json').read_text())
                expected = 'ACCEPTED' if change in ('valid', 'interleaved_recovery') else 'REJECTED'
                if record['status'] != expected or (expected == 'ACCEPTED' and result.returncode != 0):
                    raise AssertionError((mode, change, record['status'], record['failures'], record['noise']))
                checks.append(dict(scenario=mode, mutation=change, expected=expected, actual=record['status']))
    (a.base / 'provenance/collector_contract_checks.json').write_text(json.dumps(checks, indent=2) + '\n')
    print(f'{len(checks)} acceptance contract checks PASS (synthetic fixtures, no engine claim)')


if __name__ == '__main__':
    main()
