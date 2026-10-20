#!/usr/bin/env python3
"""Append a one-block report for every finished fanout-campaign attempt (smoke and matrix).

Reads results/{smoke,matrix}_attempts.jsonl as the controller appends to them and parses the
run's server logs for the cut / repair / replay breakdown (same fields as analyze.py).
Writes results/run_reports.md; with --follow it keeps watching until the campaign finishes.
"""
import argparse
import json
from pathlib import Path
import re
import time


def num(d, k, scale=1.0):
    return float(d[k]) * scale if k in d else None


def fmt_s(ms):
    return 'n/a' if ms is None else f'{ms / 1000:.2f}s'


def breakdown(out, ev):
    """Cut/repair/replay timings for the recovery event of one run directory."""
    b = {}
    for log in sorted(out.glob('server_node*.log')):
        text = log.read_text(errors='replace')
        rec = re.findall(r'verb=RECOVER result=OK[^\n]*', text)
        if rec:
            d = dict(re.findall(r'(\w+)=([0-9.]+)', rec[0]))
            b.update(drain=num(d, 'drain_ms'), localise=num(d, 'localise_us', 1e-3),
                     transfer=num(d, 'transfer_us', 1e-3), apply=num(d, 'apply_us', 1e-3),
                     verify=num(d, 'verify_us', 1e-3), rebase=num(d, 'rebase_ms'),
                     candidates=num(d, 'candidate_rows'))
        rep = re.findall(r'RECOVERY_REPLAY_LIVE[^\n]*', text)
        if rep:
            d = dict(re.findall(r'(\w+)=([0-9.]+)', rep[0]))
            b.update(replay_blocks=num(d, 'entries'))
        if ev.get('L'):
            held = re.findall(r'verb=CUT result=OK L=%s B=[^\n]*held=1' % ev['L'], text)
            if held:
                d = dict(re.findall(r'(\w+_us)=([0-9]+)', held[0]))
                b.update(cut_prepare=num(d, 'prepare_us', 1e-3), cut_export=num(d, 'export_us', 1e-3),
                         cut_digest=num(d, 'digest_us', 1e-3))
    return b


def final_check(nodes):
    """PASS when every node reports the same rows, Merkle root and data hash and merkle_verify=t."""
    if not isinstance(nodes, dict) or not nodes:
        return 'n/a'
    raws = [tuple(v.get('raw', [])) for v in nodes.values()]
    if len(set(raws)) == 1 and len(raws[0]) >= 3 and raws[0][2] == 't':
        return f"PASS ({len(raws)} nodes, rows {raws[0][0]}, root {raws[0][1][:12]}…)"
    return 'FAIL (' + ', '.join(f"{k}: rows {v.get('raw', ['?'])[0]} root {str(v.get('raw', ['', '?'])[1])[:12]}…"
                                for k, v in nodes.items()) + ')'


def report(base, stage, r):
    out = base / 'repo/scripts/bench_full_results' / r['run_id']
    ev = (r.get('events') or [{}])[0]
    tps = r.get('tps_majority_visible')
    lines = [f"### {stage} f{r['fanout']} {r['scenario']} rep{r['rep']} attempt{r['attempt']} — **{r['status']}**",
             f"- run: `{r['run_id']}` ({r.get('case_seconds', 0) / 60:.1f} min)",
             f"- TPS (majority visible): {float(tps):.0f}" if tps not in (None, '') else '- TPS: n/a']
    if r.get('failures'):
        lines.append(f"- failures: {r['failures']}")
    if r.get('noise'):
        lines.append(f"- noise: {r['noise']}")
    if ev:
        b = breakdown(out, ev)
        g = lambda k: b.get(k)
        lines += [
            f"- recovery total {fmt_s(num(ev, 'total_ms'))}: detect→quarantine {fmt_s(num(ev, 'detect_to_quarantine_ms'))}, "
            f"cut {fmt_s(num(ev, 'cut_ms'))}, repair call {fmt_s(num(ev, 'recover_call_ms'))}",
            f"- cut: prepare {fmt_s(g('cut_prepare'))}, export (backlog wait + snapshot) {fmt_s(g('cut_export'))}, "
            f"digest {fmt_s(g('cut_digest'))}",
            f"- repair: drain {fmt_s(g('drain'))}, localise {fmt_s(g('localise'))}, transfer {fmt_s(g('transfer'))}, "
            f"apply {fmt_s(g('apply'))}, verify {fmt_s(g('verify'))}, rebase {fmt_s(g('rebase'))} "
            f"(differing leaves {ev.get('differing_leaves', '?')}, rows read {int(g('candidates')) if g('candidates') is not None else '?'}, "
            f"upserted {ev.get('rows_upserted', '?')}, deleted {ev.get('rows_deleted', '?')}, full copies {ev.get('full_copies', '?')})",
            f"- replay: catch-up {fmt_s(num(ev, 'catchup_ms'))}, blocks replayed "
            f"{int(g('replay_blocks')) if g('replay_blocks') is not None else 'n/a'}, live={ev.get('live', '?')}"]
    elif r['scenario'] != 'A':
        lines.append('- no recovery event recorded')
    lines.append(f"- final check, all 3 nodes: {final_check(r.get('final_all_three'))}; "
                 f"divergences: {r.get('divergence_count', 'n/a')}")
    return '\n'.join(lines) + '\n\n'


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--base', type=Path, required=True)
    p.add_argument('--follow', action='store_true')
    a = p.parse_args()
    base = a.base.resolve()
    results = base / 'results'
    out = results / 'run_reports.md'
    seen = {}
    if out.exists():
        seen = {m: True for m in re.findall(r'run: `([^`]+)`', out.read_text())}
    while True:
        for stage in ('smoke', 'matrix', 'recheck'):
            f = results / f'{stage}_attempts.jsonl'
            if not f.exists():
                continue
            for line in f.read_text().splitlines():
                r = json.loads(line)
                if r['run_id'] in seen:
                    continue
                seen[r['run_id']] = True
                with out.open('a') as w:
                    w.write(report(base, stage, r))
                print(f"REPORTED {stage} {r['run_id']} {r['status']}", flush=True)
        if not a.follow:
            return 0
        # Keep following through review stops (the supervisor may resume); stop after
        # the final report/plot step has written artifact_status.json.
        if (results / 'artifact_status.json').exists():
            a.follow = False
            continue
        time.sleep(20)


if __name__ == '__main__':
    raise SystemExit(main())
