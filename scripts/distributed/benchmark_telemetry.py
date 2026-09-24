"""Read-only 1 Hz host and PostgreSQL wait telemetry; JSONL, monotonic timestamps."""
import argparse
import json
from pathlib import Path
import signal
import subprocess
import time


def snapshot(psql=None, port=5432):
    result = dict(monotonic_ns=time.monotonic_ns(), unix_ns=time.time_ns())
    for name in ('meminfo', 'vmstat', 'diskstats', 'stat', 'loadavg', 'pressure/io', 'pressure/memory', 'pressure/cpu'):
        p = Path('/proc') / name
        result[name] = p.read_text() if p.exists() else None
    result['cpu_frequency'] = {str(p): p.read_text().strip() for p in Path('/sys/devices/system/cpu').glob('cpu[0-9]*/cpufreq/scaling_cur_freq')}
    result['processes'] = subprocess.run(['ps', '-eo', 'pid,ppid,stat,pcpu,pmem,comm', '--sort=-pcpu'], capture_output=True, text=True, timeout=3).stdout
    if psql:
        sql = "SELECT coalesce(json_agg(t),'[]') FROM (SELECT backend_type,state,wait_event_type,wait_event,count(*) FROM pg_stat_activity WHERE pid<>pg_backend_pid() GROUP BY 1,2,3,4) t;"
        r = subprocess.run([psql, '-X', '-At', '-v', 'ON_ERROR_STOP=1', '-p', str(port), '-U', 'postgres', '-d', 'postgres', '-c', sql], capture_output=True, text=True, timeout=5)
        result['pg_waits'] = dict(returncode=r.returncode, stdout=r.stdout, stderr=r.stderr)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--max-seconds', type=int, default=3600)
    parser.add_argument('--psql')
    parser.add_argument('--port', type=int, default=5432)
    args = parser.parse_args()
    running = True
    def stop(*_):
        nonlocal running
        running = False
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    deadline = time.monotonic() + args.max_seconds
    while running and time.monotonic() < deadline:
        start = time.monotonic()
        try:
            print(json.dumps(snapshot(args.psql, args.port)), flush=True)
        except BrokenPipeError:
            return
        except Exception as e:
            print(json.dumps(dict(error=str(e), unix_ns=time.time_ns())), flush=True)
        time.sleep(max(0, 1 - (time.monotonic()-start)))


if __name__ == '__main__':
    main()
