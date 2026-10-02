#!/usr/bin/env python3
"""Fetch a bounded read-only ranking evidence snapshot; verify remote SHA-256."""
import hashlib
import json
import pathlib
import subprocess

ROOT = pathlib.Path(__file__).resolve().parents[3]
DEST = ROOT / 'Final_Results/TPCC/v2_20261002'
HOST = 'protectdr@10.129.7.57'
SOURCES = {'sweeps': 'tpcc_sweep_v2_20261002_130000',
           'headline_ab': 'tpcc_v2_20261002_051210'}
REMOTE = r'''
import hashlib, json, os, pathlib, sys
root = pathlib.Path.home() / 'claude_checks' / sys.argv[1]
top = {'summary.csv','summary.md','all_runs.csv','status_history.txt','status.txt',
       'stall_flags.json','failures.jsonl','commands.log','campaign.log','pass2.log',
       'extra.log','report_O.md'}
keep = {'accepted.json','result.txt','config.txt','server_config.txt','gateway.log',
        'state.hash','merkle_options.csv','restore_relations.csv','host_before.txt',
        'settings.csv','prewarm_blocks.txt','workload.sha256','acceptance.log'}
files = []
skipped = []
for base, dirs, names in os.walk(root):
    dirs[:] = sorted(d for d in dirs if d not in {'pgdata','ptrace_keep','pycache','__pycache__'}
                     and not d.startswith('pgdata'))
    for name in sorted(names):
        p = pathlib.Path(base) / name
        rel = p.relative_to(root)
        if p.is_symlink():
            continue
        ok = (len(rel.parts) == 1 and name in top) or rel.parts[0] in {
            'scripts','provenance','continuation','warehouses_w32','workers_w100'}
        if len(rel.parts) == 2 and rel.parts[0].startswith(('pg_','det_','merkle_')):
            ok = name in keep or name.startswith(('tabstats','idxstats','walstats','postgres'))
        if not ok:
            continue
        size = p.stat().st_size
        if size > 5 * 1024 * 1024:
            skipped.append({'path':str(rel),'bytes':size,'reason':'5 MiB evidence cap'})
            continue
        files.append({'path':str(rel),'bytes':size,'sha256':hashlib.sha256(p.read_bytes()).hexdigest()})
print(json.dumps({'source':str(root),'files':files,'skipped':skipped}))
'''

def main():
    for key, source in SOURCES.items():
        dst = DEST / key
        dst.mkdir(parents=True, exist_ok=True)
        # Immutable snapshots must never silently replace previously fetched evidence.
        if (dst / 'FETCH_MANIFEST.json').exists():
            raise SystemExit(f'Existing snapshot: {dst}; refusing to overwrite')
        result = subprocess.run(['ssh','-o','BatchMode=yes',HOST,'python3','-',source],
                                input=REMOTE, text=True, capture_output=True, check=True)
        manifest = json.loads(result.stdout)
        selected = dst / 'FETCH_FILES.txt'
        selected.write_text(''.join(f['path']+'\n' for f in manifest['files']))
        subprocess.run(['rsync','-rt','--files-from='+str(selected),
                        '-e','ssh -o BatchMode=yes',HOST+':'+manifest['source']+'/',str(dst)+'/'],check=True)
        for f in manifest['files']:
            if hashlib.sha256((dst/f['path']).read_bytes()).hexdigest() != f['sha256']:
                raise SystemExit(f"Remote changed during fetch: {key}/{f['path']}")
        (dst/'FETCH_MANIFEST.json').write_text(json.dumps(manifest,indent=2)+'\n')
        (dst/'SHA256SUMS').write_text(''.join(f"{f['sha256']}  {f['path']}\n" for f in manifest['files']))
        print(key, len(manifest['files']), 'files,', sum(f['bytes'] for f in manifest['files']), 'bytes; verified')

if __name__ == '__main__':
    main()
