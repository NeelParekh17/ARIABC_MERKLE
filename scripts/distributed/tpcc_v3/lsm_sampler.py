#!/usr/bin/env python3
"""One-Hz merkle_lsm merge evidence for Merkle runs; summary per ramp/window/drain.

Sampling uses merkle_lsm_stats (metapage only, no component scans) in its own
process, so the main sampler's timing and quality gates are unchanged. The
summary counts merges per index and level in each phase and compares gateway
completion rates for window seconds with and without a merge in progress.
"""
import argparse
import datetime as dt
import json
import subprocess
import time
from pathlib import Path
from summary import samples

QUERY = ("SELECT json_build_object('t', extract(epoch FROM clock_timestamp()), 'lsn', pg_current_wal_lsn(), "
         "'stats', (SELECT coalesce(json_object_agg(c.relname, merkle_lsm_stats(c.oid)), '{}') "
         "FROM pg_class c JOIN pg_am a ON a.oid = c.relam WHERE a.amname = 'merkle_lsm'), "
         "'merger', (SELECT string_agg(coalesce(state, '') || ':' || coalesce(wait_event, ''), '|') "
         "FROM pg_stat_activity WHERE backend_type = 'merkle_lsm merger'))")


def sample(a):
    command = [a.psql, '-X', '-qAt', '-v', 'ON_ERROR_STOP=1', '-h', '127.0.0.1', '-p', a.port,
               '-U', 'postgres', '-d', 'postgres',
               '-c', "SET default_transaction_isolation = 'read committed'", '-c', QUERY]
    with (a.run / 'lsm_samples.jsonl').open('w') as stream:
        (a.run / 'lsm_sampler.ready').touch()
        deadline = time.monotonic()
        while not (a.run / 'lsm_sampler.stop').exists():
            try:
                result = subprocess.run(command, capture_output=True, text=True, timeout=3, check=True)
                row = json.loads(result.stdout.splitlines()[-1])
            except (subprocess.SubprocessError, ValueError, IndexError) as e:
                detail = getattr(e, 'stderr', None) or str(e)
                row = dict(t=time.time(), error=type(e).__name__ + ': ' + detail[:300])
            stream.write(json.dumps(row) + '\n'); stream.flush()
            deadline += 1
            time.sleep(max(0, deadline-time.monotonic()))


def drain_bounds(run):
    try:
        stamps = [dt.datetime.fromisoformat(s.strip()).timestamp()
                  for s in (run / 'lsm_drain.log').read_text().splitlines() if s[:2] == '20' and 'T' in s]
    except (OSError, ValueError):
        return None
    return (stamps[0], stamps[-1]) if len(stamps) >= 2 else None


def summarize(run):
    rows = [r for r in map(json.loads, (run / 'lsm_samples.jsonl').read_text().splitlines()) if 'stats' in r]
    window = json.loads((run / 'window.json').read_text())
    w0, w1 = window['start']['wall_epoch'], window['end']['wall_epoch']
    drain = drain_bounds(run)

    def phase(t):
        if t < w0:
            return 'ramp'
        if t <= w1:
            return 'window'
        if drain and drain[0] <= t <= drain[1] + 1:
            return 'drain'
        return 'tail'

    merges = {}
    events = []
    for prev, cur in zip(rows, rows[1:]):
        for index, s in cur['stats'].items():
            before = prev['stats'].get(index)
            if not before:
                continue
            for level, (a, b) in enumerate(zip(before['merges'], s['merges'])):
                if b > a:
                    key = f'{index}/L{level}'
                    merges.setdefault(key, dict(ramp=0, window=0, tail=0, drain=0))[phase(cur['t'])] += b - a
                    events.append(dict(t=cur['t'], phase=phase(cur['t']), index=index, level=level,
                                       count=b-a, last_merge_us=s['last_merge_us']))
    in_window = [r for r in rows if w0 <= r['t'] <= w1]
    active = [r['t'] for r in in_window if any(s['phase'] != 'idle' for s in r['stats'].values())]

    def level_bytes(r):
        return {i: dict(levels=[sum(x) for x in s['bytes']], base=s['base_bytes']) for i, s in r['stats'].items()}

    # Gateway completion rate per sampler interval, split by merge activity.
    busy = set(int(t) for t in active)
    rates = dict(merging=[], idle=[])
    points = [r for r in samples(run / 'samples.csv') if w0 <= r['wall_epoch'] <= w1 and r.get('completed') is not None]
    for a, b in zip(points, points[1:]):
        seconds = b['wall_epoch'] - a['wall_epoch']
        if seconds <= 0:
            continue
        hit = any(int(a['wall_epoch']) <= t <= int(b['wall_epoch']) for t in busy)
        rates['merging' if hit else 'idle'].append((b['completed'] - a['completed']) / seconds)
    mean = lambda v: sum(v)/len(v) if v else None
    out = dict(samples=len(rows), window_samples=len(in_window), window_s=w1-w0, drain=drain,
               merges=merges, merge_events=events,
               window_merge_active_s=len(active),
               tps_while_merging=mean(rates['merging']), tps_while_idle=mean(rates['idle']),
               intervals_merging=len(rates['merging']), intervals_idle=len(rates['idle']),
               bytes_window_start=level_bytes(in_window[0]) if in_window else None,
               bytes_window_end=level_bytes(in_window[-1]) if in_window else None,
               bytes_final=level_bytes(rows[-1]) if rows else None,
               sampler_errors=sum(1 for r in map(json.loads, (run / 'lsm_samples.jsonl').read_text().splitlines()) if 'error' in r))
    (run / 'lsm_summary.json').write_text(json.dumps(out, indent=2) + '\n')
    lines = ['# merkle_lsm merges', '', f'Window {w1-w0:.0f} s; merge in progress {len(active)} s of it.', '',
             '| Index / level | Ramp | Window | Tail | Drain |', '|---|---:|---:|---:|---:|']
    lines += [f'| {k} | {v["ramp"]} | {v["window"]} | {v["tail"]} | {v["drain"]} |' for k, v in sorted(merges.items())]
    fmt = lambda x: 'n/a' if x is None else f'{x:.1f}'
    lines += ['', f'Window TPS while merging {fmt(out["tps_while_merging"])} ({out["intervals_merging"]} intervals), '
              f'idle {fmt(out["tps_while_idle"])} ({out["intervals_idle"]} intervals).', '']
    (run / 'lsm_summary.md').write_text('\n'.join(lines))


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--run', required=True, type=Path)
    p.add_argument('--psql')
    p.add_argument('--port')
    p.add_argument('--summarize', action='store_true')
    a = p.parse_args()
    if a.summarize:
        summarize(a.run)
    else:
        sample(a)


if __name__ == '__main__':
    main()
