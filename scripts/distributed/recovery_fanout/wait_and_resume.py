#!/usr/bin/env python3
"""Resume the unchanged campaign after shared-node memory pressure subsides.

This resource watcher is outside the measured harness. It never kills other
processes, changes kernel settings, or changes a benchmark configuration.
"""
import argparse
import collections
import datetime
import fcntl
import json
import os
from pathlib import Path
import subprocess
import time


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--base', type=Path, required=True)
    p.add_argument('--max-preflight-swap-mib', type=int, default=1,
                   help='maximum cumulative swap I/O per host over five minutes (1-64 MiB)')
    p.add_argument('--min-available-246-gib', type=int, default=4,
                   help='minimum available RAM on the contended .246 node (4-12 GiB)')
    a = p.parse_args()
    if not 1 <= a.max_preflight_swap_mib <= 64:
        p.error('--max-preflight-swap-mib must be between 1 and 64')
    if not 4 <= a.min_available_246_gib <= 12:
        p.error('--min-available-246-gib must be between 4 and 12')
    maximum_swap_bytes = a.max_preflight_swap_mib * 1024**2
    b = a.base.resolve()
    if '10.129.148.247' not in subprocess.check_output(['hostname', '-I'], text=True).split():
        p.error('watch only on .247')
    lock = (b / 'results/resource_wait.lock').open('a')
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    os.environ['PATH'] = str(b / 'tools') + ':' + os.environ['PATH']
    minimum_kb = {'10.129.148.247': 8 * 1024**2, '10.129.148.246': a.min_available_246_gib * 1024**2,
                  '10.129.148.248': 6 * 1024**2}
    history = collections.deque()

    def state(name, **extra):
        record = dict(state=name, utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),
                      pid=os.getpid(), minimum_mem_available_kb=minimum_kb, stable_seconds_required=300,
                      maximum_preflight_swap_bytes=maximum_swap_bytes, **extra)
        temporary = b / 'results/resource_wait_status.json.tmp'
        temporary.write_text(json.dumps(record, indent=2) + '\n')
        temporary.replace(b / 'results/resource_wait_status.json')
        print(json.dumps(record), flush=True)

    while True:
        status = json.loads((b / 'results/status.json').read_text())
        if status['state'] == 'DONE' and status['phase'] == 'matrix':
            state('DONE')
            return 0
        if status['state'] != 'STOPPED_REVIEW_REQUIRED':
            state('OTHER_CONTROLLER_ACTIVE_OR_REVIEW_NEEDED', campaign_status=status)
            return 2
        if status.get('failures') or not status.get('noise'):
            state('CORRECTNESS_OR_COLLECTION_REVIEW_REQUIRED', campaign_status=status)
            return 2
        records = [json.loads(s) for s in (b / 'results/matrix_attempts.jsonl').read_text().splitlines()]
        last = records[-1]
        point = tuple(last[k] for k in ('rows', 'fanout', 'scenario', 'rep'))
        noisy = sum(r.get('status') == 'NOISE_EXCLUDED' and
                    tuple(r[k] for k in ('rows', 'fanout', 'scenario', 'rep')) == point for r in records)
        if noisy >= 3:
            state('REPEATED_NOISE_REVIEW_REQUIRED', point=point, excluded_attempts=noisy)
            return 3
        samples = {}
        for host in minimum_kb:
            try:
                result = subprocess.run(['ssh', '-o', 'ConnectTimeout=5', 'neel@' + host,
                                         'python3 ' + str(b / 'host_probe.py')],
                                        capture_output=True, text=True, check=True, timeout=10)
                samples[host] = json.loads(result.stdout)
            except (subprocess.SubprocessError, ValueError) as error:
                samples[host] = {'error': str(error)}
        stamp = time.monotonic()
        good = all('error' not in samples[h] and samples[h]['mem_available_kb'] >= need
                   for h, need in minimum_kb.items())
        if good:
            history.append((stamp, samples))
            # The entire preceding five-minute window must meet memory bounds
            # and stay within the configured cumulative swap I/O bound per host.
            while len(history) > 1 and history[1][0] <= stamp - 300:
                history.popleft()
            first = history[0]
            stable_for = stamp - first[0]
            swap_bytes = {h: sum(max(0, samples[h][k] - first[1][h][k])
                                for k in ('pswpin', 'pswpout')) * samples[h]['page_bytes']
                          for h in minimum_kb}
            if any(v > maximum_swap_bytes for v in swap_bytes.values()):
                history.clear()
                stable_for = 0
        else:
            history.clear()
            stable_for = 0
            swap_bytes = {}
        with (b / 'results/resource_wait_samples.jsonl').open('a') as f:
            f.write(json.dumps(dict(utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),
                                   hosts=samples, stable_seconds=stable_for, swap_bytes=swap_bytes)) + '\n')
        state('WAITING_MEMORY_STABILITY', point=point,
              current_mem_available_kb={h: samples[h].get('mem_available_kb') for h in minimum_kb},
              stable_seconds=stable_for, recent_swap_io_bytes=swap_bytes)
        if good and stable_for >= 300 and swap_bytes and max(swap_bytes.values()) <= maximum_swap_bytes:
            tag = datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ')
            state('RESUMING', point=point, resume_tag=tag)
            with (b / 'results' / ('supervisor_resource_resume_' + tag + '.log')).open('x') as log:
                rc = subprocess.run(['bash', str(b / 'supervise.sh'), str(b), '--resume'],
                                    stdout=log, stderr=subprocess.STDOUT).returncode
            state('CONTROLLER_RETURNED', exit_code=rc)
            if rc == 0:
                with (b / 'results' / ('postprocess_resource_resume_' + tag + '.log')).open('x') as log:
                    post_rc = subprocess.run(['python3', str(b / 'repo/scripts/distributed/recovery_fanout/finish.py'),
                                              '--base', str(b)], stdout=log, stderr=subprocess.STDOUT).returncode
                state('DONE' if post_rc == 0 else 'FIGURES_OR_ANALYSIS_REVIEW_REQUIRED', postprocess_exit_code=post_rc)
                return post_rc
            history.clear()
        time.sleep(30)


if __name__ == '__main__':
    raise SystemExit(main())
