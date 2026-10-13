#!/usr/bin/env python3
""".111 read-only progress/analysis observer; leaves measured harness frozen."""
import csv, datetime as D, json, statistics, subprocess as S, time
from pathlib import Path

ROOT=Path('/home/neel/claude_ctl/f15_adhash_20261012')
OUT=ROOT/'results'
PUB=OUT/'provenance/published_configuration_summary.csv'
refs={(r['workload'],float(r['skew']),int(r['workers']),r['mode']):r for r in csv.DictReader(PUB.open())}

def latest(stage):
    p=sorted((OUT/stage).glob('run_*/campaign.json'),key=lambda x:x.stat().st_mtime)
    return p[-1].parent if p else None

def rows(stage):
    p=latest(stage)
    return list(csv.DictReader((p/'summary.csv').open())) if p and (p/'summary.csv').exists() else []

def observe():
    smoke=[]
    for r in rows('smoke'):
        ref=refs[(r['workload'],float(r['skew']),int(r['workers']),r['mode'])]
        smoke.append(dict(mode=r['mode'],workload=r['workload'],skew=float(r['skew']),workers=int(r['workers']),
            new_tps=float(r['tps']),published_median_tps=float(ref['median_tps']),published_min_tps=float(ref['min_tps']),
            published_max_tps=float(ref['max_tps']),ratio=float(r['tps'])/float(ref['median_tps']),artifact_dir=r['artifact_dir']))
    (OUT/'smoke_comparison.json').write_text(json.dumps(smoke,indent=2)+'\n')
    lines=['# F15 OOM smoke comparison','',
        'Single fresh 20K-statement trials, A theta .99 / worker 1. Smoke is separate from the 112-case grid.','',
        '| Mode | New TPS | Published median | Published min | Published max | New / published |','|---|---:|---:|---:|---:|---:|']
    for x in smoke:lines.append(f"| {x['mode']} | {x['new_tps']:.2f} | {x['published_median_tps']:.2f} | {x['published_min_tps']:.2f} | {x['published_max_tps']:.2f} | {x['ratio']:.4f} |")
    lines+=['','One smoke trial does not establish grid medians. Det control and adjusted ratios are diagnostic; engine/host provenance differs from the published campaign.']
    (OUT/'SMOKE_COMPARISON.md').write_text('\n'.join(lines)+'\n')
    run=latest('campaign');accepted=rows('campaign')
    attempts=list(csv.DictReader((run/'all_attempts.csv').open())) if run and (run/'all_attempts.csv').exists() else []
    bymode={m:sum(r['mode']==m for r in accepted) for m in ['bcdb_merkle','bcdb_det']}
    # Use saved setup evidence for case spacing and anchor ETA at the last
    # accepted result, so the prediction does not drift solely with poll time.
    starts=[]
    for r in accepted:
        case=Path(r['artifact_dir'])
        starts.append(min(p.stat().st_mtime for p in case.iterdir() if p.is_file()))
    spacings=[b-a for a,b in zip(starts,starts[1:]) if b>a]
    mean_case=statistics.mean(spacings) if spacings else None
    remaining=112-len(accepted)
    last_finish=max((Path(r['artifact_dir'])/'result.json').stat().st_mtime for r in accepted) if accepted else None
    eta=D.datetime.fromtimestamp(last_finish+remaining*mean_case,D.timezone.utc).isoformat() if mean_case else None
    value=dict(updated_utc=D.datetime.now(D.timezone.utc).isoformat(),smoke_accepted=len(smoke),
        campaign_dir=str(run) if run else None,accepted=len(accepted),planned=112,accepted_by_mode=bymode,
        attempts=len(attempts),noise_excluded=sum(str(r.get('noise_excluded')).lower()=='true' for r in attempts),
        mean_observed_case_spacing_s=mean_case,eta_utc=eta,
        eta_limit='Provisional from completed case spacing; early mode mix, restores and verification can change it.')
    (OUT/'progress.json').write_text(json.dumps(value,indent=2)+'\n')
    S.run(['python3',str(ROOT/'compare.py'),'--results',str(OUT/'campaign'),'--published',str(PUB),
           '--published-trials',str(OUT/'provenance/published_trials.csv'),'--out',str(OUT/'comparison')],check=True)

while True:
    try:observe()
    except Exception as e:(OUT/'progress_observer_error.txt').write_text(repr(e)+'\n')
    state=(OUT/'status.txt').read_text() if (OUT/'status.txt').exists() else ''
    if 'GRID_DONE_112_CASES' in state:
        # Stop only observers whose stdout still points at our exact log file.
        pidfile=OUT/'controller_iostat.pid'
        if pidfile.exists():
            pid=int(pidfile.read_text())
            fd=Path(f'/proc/{pid}/fd/1')
            if fd.exists() and str(fd.resolve())==str(OUT/'controller_iostat.txt'):
                S.run(['kill','-TERM',str(pid)])
        S.run(['ssh','neel@10.129.148.247',
               'touch ~/claude_checks/f15_adhash_20261012/oom_baseline/GRID_DONE.flag; '
               'p=$(cat ~/claude_checks/f15_adhash_20261012/oom_baseline/host_iostat.pid); '
               'if test "$(readlink /proc/$p/fd/1)" = "$HOME/claude_checks/f15_adhash_20261012/oom_baseline/host_iostat.txt"; then kill -TERM "$p"; fi'])
        break
    time.sleep(45)
