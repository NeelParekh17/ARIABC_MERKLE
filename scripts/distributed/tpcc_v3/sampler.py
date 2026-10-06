#!/usr/bin/env python3
"""One-Hz SQL/gateway samples; interval /proc CPU evidence every ten seconds."""
import argparse
import csv
import json
import os
import pwd
import subprocess
import time
from pathlib import Path
from summary import progress

COLUMNS = ['wall_epoch', 'wall_utc', 'monotonic_s', 'elapsed_s', 'completed', 'sent', 'total',
           'user_aborts', 'progress_wall_epoch', 'progress_age_s', 'district_next', 'wal_lsn',
           'query_ms', 'load1', 'load5', 'load15', 'affinity_ok', 'owned_affinity_json',
           'other_cpu_json', 'error']


def process_snapshot():
    out = {}
    for p in Path('/proc').iterdir():
        if not p.name.isdigit():
            continue
        try:
            raw = (p / 'stat').read_text()
            stat = raw[raw.rfind(')')+2:].split()
            pid = int(p.name)
            out[pid] = dict(pid=pid, ppid=int(stat[1]), ticks=int(stat[11])+int(stat[12]),
                start=stat[19], user=pwd.getpwuid(p.stat().st_uid).pw_name,
                args=(p / 'cmdline').read_bytes().replace(b'\0', b' ').decode(errors='replace')[:300],
                cpus=sorted(os.sched_getaffinity(pid)))
        except (OSError, ValueError, KeyError, ProcessLookupError):
            continue
    return out


def cpu_evidence(previous, current, seconds, roots, expected):
    owned = {pid for pid in roots if pid > 0} | {os.getpid()}
    changed = True
    while changed:
        changed = False
        for pid, p in current.items():
            if p['ppid'] in owned and pid not in owned:
                owned.add(pid); changed = True
    affinity = [{'pid': pid, 'cpus': p['cpus']} for pid, p in current.items() if pid in owned and pid != os.getpid()]
    others = []
    for pid, p in current.items():
        before = previous.get(pid)
        if pid in owned or not before or before['start'] != p['start'] or seconds <= 0:
            continue
        cpu = 100 * (p['ticks'] - before['ticks']) / os.sysconf('SC_CLK_TCK') / seconds
        if cpu > 0:
            others.append(dict(pid=pid, user=p['user'], cpu_pct=round(cpu, 2), args=p['args']))
    return sorted(others, key=lambda x: -x['cpu_pct'])[:10], affinity, bool(affinity) and all(set(p['cpus']) == expected for p in affinity)


def cpuset(value):
    out = set()
    for part in value.split(','):
        if '-' in part:
            a, b = map(int, part.split('-')); out.update(range(a, b+1))
        else:
            out.add(int(part))
    return out


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--run', required=True, type=Path)
    p.add_argument('--psql', required=True)
    p.add_argument('--port', required=True)
    p.add_argument('--pg-pid', required=True, type=int)
    p.add_argument('--server-pid', required=True, type=int)
    p.add_argument('--cpuset', required=True)
    a = p.parse_args()
    expected = cpuset(a.cpuset)
    # psql -c with several statements prints only the LAST result, so use separate -c
    # commands. READ COMMITTED READ ONLY: the sampler must not take SERIALIZABLE SIREAD
    # locks on district (that would add rw-conflicts to the pg workload).
    command = [a.psql, '-X', '-qAt', '-v', 'ON_ERROR_STOP=1', '-h', '127.0.0.1', '-p', a.port,
               '-U', 'postgres', '-d', 'postgres',
               '-c', "SET default_transaction_isolation = 'read committed'",
               '-c', 'SET default_transaction_read_only = on',
               '-c', 'SELECT extract(epoch FROM clock_timestamp()), sum(d_next_o_id), pg_current_wal_lsn() '
                     'FROM public.district']
    logfile = (a.run / 'gateway.log').open('r')
    latest, partial = None, ''
    last_proc, proc_time = process_snapshot(), time.monotonic()
    others, affinity, affinity_ok = [], [], False
    start = deadline = time.monotonic()
    with (a.run / 'samples.csv').open('w', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=COLUMNS); writer.writeheader(); stream.flush()
        (a.run / 'sampler.ready').touch()
        while not (a.run / 'sampler.stop').exists():
            mono = time.monotonic()
            row = dict(monotonic_s=mono-start, error='')
            # Capture progress before SQL so counter timestamps never follow the DB sample.
            partial += logfile.read()
            pieces = partial.split('\n'); partial = pieces.pop()
            for line in pieces:
                point = progress(line)
                if point:
                    latest = point
            begin = time.monotonic()
            try:
                result = subprocess.run(command, capture_output=True, text=True, timeout=3, check=True)
                lines = [s for s in result.stdout.splitlines() if s.count('|') == 2]
                stamp, district, lsn = lines[-1].split('|')
                row.update(wall_epoch=float(stamp), district_next=int(district), wal_lsn=lsn)
            except (subprocess.SubprocessError, ValueError, IndexError) as e:
                detail = getattr(e, 'stderr', None) or str(e)
                row.update(wall_epoch=time.time(), error=type(e).__name__ + ': ' + detail[:300])
            row['query_ms'] = round((time.monotonic()-begin)*1000, 3)
            row['wall_utc'] = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(row['wall_epoch']))
            if latest:
                for k in ('elapsed_s', 'completed', 'sent', 'total', 'user_aborts'):
                    row[k] = latest[k]
                if latest['wall_time_unix_ms'] is not None:
                    row['progress_wall_epoch'] = latest['wall_time_unix_ms']/1000
                    row['progress_age_s'] = row['wall_epoch'] - row['progress_wall_epoch']
            row['load1'], row['load5'], row['load15'] = Path('/proc/loadavg').read_text().split()[:3]
            gateway_pid = 0
            try:
                gateway_pid = int((a.run / 'gateway.pid').read_text())
            except (OSError, ValueError):
                pass
            if mono - proc_time >= 10 or not affinity or (gateway_pid and gateway_pid not in {p['pid'] for p in affinity}):
                current = process_snapshot()
                others, affinity, affinity_ok = cpu_evidence(last_proc, current, mono-proc_time,
                    [a.pg_pid, a.server_pid, gateway_pid], expected)
                last_proc, proc_time = current, mono
            row.update(other_cpu_json=json.dumps(others), owned_affinity_json=json.dumps(affinity),
                       affinity_ok=int(affinity_ok))
            writer.writerow(row); stream.flush()
            deadline += 1
            time.sleep(max(0, deadline-time.monotonic()))
    logfile.close()


if __name__ == '__main__':
    main()
