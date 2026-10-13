#!/usr/bin/env python3
"""Partial OOM comparison. Execute analysis on .247 or .111, never locally.

Reads paper-harness result.json + noise_qualification.json artifacts. Invalid,
incomplete and excluded attempts remain in rejection.csv and do not enter TPS
statistics. An absent trial stays absent. No smoke values enter the full grid.
"""
import argparse, csv, json, math, statistics
from collections import defaultdict
from pathlib import Path

COMBOS=[('a','0'),('a','0.99'),('a','1.2'),('b','0.99'),('c','0.99'),('d','0.99'),('f','0.99')]
EXPECTED_BINARIES={
    'postgres':'f5af4b193a309f19015ed7fc2aa247abe7a5e53aeedc189647563d549aa6adea',
    'ariabc_pg_server':'dddda624c2bd96ab08212f8e0913bf4fb4abe3f895ca557e44ba9b2b4a573271',
    'ariabc_pg_gateway':'6b589940c483a0a1a5075c4b22e8fdcf3764c3a0e7f49d46649f8aa33105bb98',
}

def key(row):
    return (str(row['workload']).lower(),format(float(row['skew']),'.8g'),int(row['workers']),row['mode'])

def ratio(a,b):
    return a/b if a is not None and b is not None and b>0 else None

def stats(values):
    return (statistics.median(values),min(values),max(values)) if values else (None,None,None)

def accept(path, published_hashes):
    payload=json.loads(path.read_text());r=payload['row'];g=payload.get('gateway',{})
    failures=[]
    executables=payload.get('provenance',{}).get('executables',[])
    observed={Path(x['path']).name:x['sha256'] for x in executables}
    if observed!=EXPECTED_BINARIES:failures.append('accepted-build binary provenance')
    if r.get('mode') not in ('bcdb_merkle','bcdb_det'):failures.append('unplanned mode')
    if str(r.get('isolation','')).lower()!='serializable':failures.append('isolation')
    for field,expected in [('total_queries',20000),('validated_completed_queries',20000),('db_rows',100000000),('divergence_count',0),('permanent_failures',0)]:
        if r.get(field)!=expected:failures.append(field)
    if r.get('shared_buffers')!='32MB' or r.get('reset_mode')!='delta':failures.append('buffer/reset method')
    if g.get('gateway_returncode')!=0:failures.append('gateway exit')
    for field in ['divergence_count','permanent_failures','deterministic_error_count','nonterminal_failure_count','empty_results']:
        if g.get(field)!=0:failures.append('gateway '+field)
    if g.get('validated_completed_queries')!=20000 or g.get('validated_row_results')!=20000:failures.append('terminal success coverage')
    if r.get('mode')=='bcdb_merkle':
        if r.get('merkle_verify')!='PASS':failures.append('merkle_verify')
        geometry=payload.get('merkle_stats',{})
        for field,expected in [('version',11),('partitions',200),('fanout',32),('split_threshold',1024),('merge_threshold',256)]:
            if geometry.get(field)!=expected:failures.append('geometry '+field)
    if r.get('noise_excluded') is not False or (path.parent/'NOISE_EXCLUDED.txt').exists():failures.append('noise exclusion')
    noise=path.parent/'noise_qualification.json'
    if not noise.exists():failures.append('missing noise qualification')
    else:
        q=json.loads(noise.read_text())
        # The paper harness writes excluded; accept only an explicit clean result.
        clean=q.get('noise_excluded',q.get('excluded'))
        if clean is not False:failures.append('unclean noise qualification')
    expected=published_hashes.get((str(r.get('workload','')).lower(),format(float(r.get('skew',0)),'.8g')))
    if not expected or r.get('workload_sha256')!=expected:failures.append('workload hash')
    command=g.get('command',[])
    def option(name):
        return command[command.index(name)+1] if name in command else None
    if option('--numTerminals')!='96' or option('--detWindow')!='1024':failures.append('gateway settings')
    if not isinstance(r.get('tps'),(int,float)) or not math.isfinite(r['tps']) or r['tps']<=0:failures.append('TPS')
    return r,failures

def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--results',type=Path,required=True,help='full-grid campaign directory, never smoke directory')
    p.add_argument('--published',type=Path,required=True,help='paper phase2/configuration_summary.csv')
    p.add_argument('--published-trials',type=Path,required=True,help='published summary.csv for workload hashes')
    p.add_argument('--out',type=Path,required=True)
    a=p.parse_args();a.out.mkdir(parents=True,exist_ok=True)
    pub={key(r):r for r in csv.DictReader(a.published.open())}
    hashes={}
    for r in csv.DictReader(a.published_trials.open()):
        k=(r['workload'].lower(),format(float(r['skew']),'.8g'));h=r['workload_sha256']
        if k in hashes and hashes[k]!=h:raise ValueError('published workload hash disagreement')
        hashes[k]=h
    values=defaultdict(dict);rejected=[];accepted_paths={};ambiguous=set()
    canonical=set()
    for summary in a.results.rglob('summary.csv') if a.results.exists() else []:
        for row in csv.DictReader(summary.open()):
            canonical.add((Path(row['artifact_dir'])/'result.json').resolve())
    for path in sorted(a.results.rglob('result.json')) if a.results.exists() else []:
        try:
            r,reasons=accept(path,hashes)
            if canonical and path.resolve() not in canonical:reasons.append('not canonical accepted summary attempt')
            if reasons:rejected.append(dict(path=str(path),reasons='; '.join(reasons)));continue
            k=key(r);trial=int(r['trial'])
            if k not in pub or trial not in (range(1,4) if k[3]=='bcdb_merkle' else [1]):
                rejected.append(dict(path=str(path),reasons='unplanned point or trial'));continue
            token=(k,trial)
            if token in ambiguous:
                rejected.append(dict(path=str(path),reasons='ambiguous accepted trial'));continue
            if trial in values[k]:
                prior=accepted_paths.pop(token)
                del values[k][trial];ambiguous.add(token)
                rejected.extend([dict(path=prior,reasons='ambiguous accepted trial'),dict(path=str(path),reasons='ambiguous accepted trial')])
                continue
            values[k][trial]=float(r['tps'])
            accepted_paths[token]=str(path)
        except Exception as e:rejected.append(dict(path=str(path),reasons=str(e)))
    output=[]
    for workload,skew in COMBOS:
        for workers in [1,4,8,16]:
            m=(workload,skew,workers,'bcdb_merkle');d=(workload,skew,workers,'bcdb_det')
            pm=pub[m];pd=pub[d]
            nm,nmin,nmax=stats(list(values[m].values()));nd,_,_=stats(list(values[d].values()))
            mr=ratio(nm,float(pm['median_tps']));dr=ratio(nd,float(pd['median_tps']))
            output.append(dict(workload=workload,skew=skew,workers=workers,new_merkle_n=len(values[m]),
                new_merkle_median_tps=nm,new_merkle_min_tps=nmin,new_merkle_max_tps=nmax,
                published_merkle_n=int(pm['n']),published_merkle_median_tps=float(pm['median_tps']),
                published_merkle_min_tps=float(pm['min_tps']),published_merkle_max_tps=float(pm['max_tps']),
                merkle_ratio=mr,new_det_n=len(values[d]),new_det_tps=nd,published_det_median_tps=float(pd['median_tps']),
                det_control_ratio=dr,drift_adjusted_merkle_ratio=ratio(mr,dr),
                point_complete=len(values[m])==3 and len(values[d])==1))
    with (a.out/'comparison.csv').open('w',newline='') as f:
        w=csv.DictWriter(f,fieldnames=list(output[0]));w.writeheader();w.writerows(output)
    with (a.out/'rejection.csv').open('w',newline='') as f:
        w=csv.DictWriter(f,fieldnames=['path','reasons']);w.writeheader();w.writerows(rejected)
    observed=[x['merkle_ratio'] for x in output if x['merkle_ratio'] is not None]
    complete=[x['merkle_ratio'] for x in output if x['point_complete']]
    adjusted=[x['drift_adjusted_merkle_ratio'] for x in output if x['drift_adjusted_merkle_ratio'] is not None]
    def display(v):return 'unavailable' if not v else f'{statistics.median(v):.6f}'
    lines=['# AdHash OOM comparison','',f'Accepted: {sum(len(v) for v in values.values())}/112 cases. Rejected/incomplete/excluded artifacts: {len(rejected)}.',
           f'Complete points: {sum(x["point_complete"] for x in output)}/28.',
           f'Overall median new/published Merkle ratio (observed points): {display(observed)}.',
           f'Overall median ratio (complete points only): {display(complete)}.',
           f'Overall median drift-adjusted Merkle ratio (matched observed points): {display(adjusted)}.','',
           'Partial points use available qualified trials; absent trials are blank, never zero. Ratios use published three-trial medians. Drift adjustment = Merkle ratio / det-control ratio. Det has one new trial per point; this is a diagnostic adjustment, not a causal attribution. Published/current engine commits and host provenance differ. Canonical accepted summary attempts take precedence over retained reruns.','',
           '| Workload | theta | workers | new Merkle n | new median / min / max | published median / min / max | Merkle ratio | det ratio | adjusted ratio |',
           '|---|---:|---:|---:|---|---|---:|---:|---:|']
    def n(v):return '' if v is None else f'{v:.6f}'
    for x in output:
        lines.append('| '+ ' | '.join([x['workload'],x['skew'],str(x['workers']),str(x['new_merkle_n']),
            ' / '.join(n(x[k]) for k in ['new_merkle_median_tps','new_merkle_min_tps','new_merkle_max_tps']),
            ' / '.join(n(x[k]) for k in ['published_merkle_median_tps','published_merkle_min_tps','published_merkle_max_tps']),
            n(x['merkle_ratio']),n(x['det_control_ratio']),n(x['drift_adjusted_merkle_ratio'])])+' |')
    (a.out/'COMPARISON.md').write_text('\n'.join(lines)+'\n')

if __name__=='__main__':main()
