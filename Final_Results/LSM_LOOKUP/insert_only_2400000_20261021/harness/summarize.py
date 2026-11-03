#!/usr/bin/env python3
"""Summarize audit evidence on .111. Raw measurements remain unchanged."""
from collections import Counter,defaultdict
import argparse
import csv
import json
from pathlib import Path
import re
import statistics

ROOT=Path('/home/neel/claude_ctl/lsm_relation_audit_20261021')
PHASE={0:'setup',1:'foreground',2:'drain',3:'checkpoint',4:'shutdown'}
MAP=re.compile(r'^@(\w+)\[([^\]]+)\]:\s*(.*?)\s*$')


def read(path):return json.loads(path.read_text())


def parse_trace(path):
    maps=defaultdict(dict)
    for line in path.read_text().splitlines():
        m=MAP.match(line)
        if m:
            try:key=tuple(int(s.strip()) for s in m[2].split(','))
            except ValueError:continue
            if m[1]=='names':value=m[3]
            else:
                try:value=int(m[3])
                except ValueError:value=m[3]
            maps[m[1]][key]=value
    return maps


def name(r):return r.get('nspname','public')+'.'+r['relname']


def summarize(directory):
    before=read(directory/'before.json');after=read(directory/'after_restart.json')
    trace=parse_trace(directory/'io.trace')
    for key,ops in trace.get('ops',{}).items():
        steps=sum(trace.get('distance',{}).get(key+(b,),0) for b in range(7))
        if steps!=ops-1 or key not in trace.get('run',{}):
            raise RuntimeError(f'{directory.name}: incomplete offset stream {key}: ops={ops}, transitions={steps}')
        if trace.get('contiguous',{}).get(key,0)>steps:
            raise RuntimeError(directory.name+': impossible contiguous-pair count')
    objects={r['oid']:r for r in before['all_filenodes']}
    objects.update({r['oid']:r for r in before['relations']})
    filenodes={};rfn_names=defaultdict(set);inode_paths={};actors={}

    def addrels(rows):
        for r in rows:
            oid=r['oid'];objects[oid]={**objects.get(oid,{}),**r}
            p=r.get('filepath')
            if not p:continue
            parts=p.split('/')
            db=int(parts[-2]) if parts[-2].isdigit() else 0
            rel=int(r['relfilenode'])
            filenodes[(db,rel)]=name(objects[oid]);rfn_names[rel].add(name(objects[oid]))

    def activity(rows):
        for r in rows:
            b=r.get('backend_type') or ''
            actors[r['pid']]='merger' if 'merkle_lsm merger' in b else (
                'checkpointer' if b=='checkpointer' else 'bgwriter' if b=='background writer' else
                'walwriter' if b=='walwriter' else 'foreground_or_monitor')

    addrels(before['all_filenodes']);addrels(before['relations']);activity(before['activity'])
    for line in (directory/'sampler.jsonl').read_text().splitlines():
        s=json.loads(line)
        addrels(s.get('relations',[]));activity(s.get('activity',[]))
        for f in s.get('files',[]):inode_paths[(f['device'],f['inode'])]=f['path']
    addrels(after['all_filenodes']);activity(after['activity'])
    for (oid,rel),count in trace.get('replacements',{}).items():
        if oid in objects:
            r=objects[oid];p=r.get('filepath','base/0/0');db=int(p.split('/')[-2])
            filenodes[(db,rel)]=name(r);rfn_names[rel].add(name(r))

    def inode_name(device,inode,generation):
        path=inode_paths.get((device,inode),'')
        filename=str(trace.get('names',{}).get((device,inode,generation),'?'))
        if path and Path(path).name!=filename:path=''
        if re.fullmatch(r'[0-9A-F]{24}',filename):return 'WAL','main'
        m=re.fullmatch(r'(\d+)(?:_(fsm|vm|init))?(?:\.(\d+))?',filename)
        if not m:return 'OTHER:'+filename,'other'
        rel=int(m[1]);fork=m[2] or 'main'
        db=int(Path(path).parent.name) if path and Path(path).parent.name.isdigit() else None
        if (db,rel) in filenodes:return filenodes[(db,rel)],fork
        choices=rfn_names[rel]
        if len(choices)==1:return next(iter(choices)),fork
        return 'UNMAPPED:'+filename,fork

    io=defaultdict(Counter)
    for metric in ['ops','bytes','contiguous','distance','runs','run']:
        for k,value in trace.get(metric,{}).items():
            pid,dev,inode,generation,phase,operation=k[:6]
            rel,fork=inode_name(dev,inode,generation)
            g=(PHASE.get(phase,str(phase)),actors.get(pid,'unknown_pid'),rel,fork,'write' if operation else 'read')
            if metric=='run':
                bucket=0 if value<=1 else 1 if value<=2 else 2 if value<=4 else 3 if value<=8 else 4 if value<=16 else 5 if value<=32 else 6 if value<=64 else 7
                io[g]['runs_'+str(bucket)]+=1
            elif len(k)==7:io[g][metric+'_'+str(k[6])]+=value
            else:io[g][metric]+=value
    dirty=defaultdict(Counter)
    for metric in ['dirty_calls','dirtied']:
        for (pid,db,rfn,phase,fork,hint),value in trace.get(metric,{}).items():
            rel=filenodes.get((db,rfn),'UNMAPPED:'+str(rfn))
            key=(PHASE.get(phase,str(phase)),actors.get(pid,'unknown_pid'),rel,str(fork),bool(hint))
            dirty[key][metric]+=value
    io_rows=[dict(phase=k[0],actor=k[1],relation=k[2],fork=k[3],operation=k[4],**v) for k,v in sorted(io.items())]
    dirty_rows=[dict(phase=k[0],actor=k[1],relation=k[2],fork=k[3],hint=k[4],**v) for k,v in sorted(dirty.items())]
    write(directory/'offset_histograms.json',dict(distance_bins=['same block','+1 block','-1 block','2..7 blocks','8..63 blocks','64..1023 blocks','>=1024 blocks'],
        run_bins=['1','2','3..4','5..8','9..16','17..32','33..64','>=65'],
        note='Per-process/per-inode/per-file-generation streams; run length counts 8KB-equivalent blocks. Actual successful VFS bytes, not media-cache misses.',
        rows=io_rows,dirtied=dirty_rows))

    def relname(walrel):
        try:sp,db,rfn=(int(s) for s in walrel.split('/'))
        except ValueError:return walrel
        return filenodes.get((db,rfn),'UNMAPPED:'+walrel)

    wal=read(directory/'wal_summary.json')
    walrows=defaultdict(Counter)
    for r in wal['relations']:
        k=(r['phase'],relname(r['rel']))
        walrows[k].update(wal_bytes=r.get('record_bytes',0)+r.get('fpi_bytes',0),fpis=r.get('fpis',0),
            fpi_bytes=r.get('fpi_bytes',0),wal_block_references=r.get('block_refs',0))
    for r in wal['blocks']:
        walrows[(r['phase'],relname(r['rel']))]['unique_wal_referenced_pages']+=r['unique_wal_referenced_main_blocks']
    write(directory/'wal_by_relation.json',[dict(phase=k[0],relation=k[1],**v) for k,v in sorted(walrows.items())])
    rels=[]
    after_oid={r['oid']:r for r in after['relations']}
    for b in before['relations']:
        a=after_oid.get(b['oid'],{})
        delta={}
        for section in ['table_stats','table_io','index_stats','index_io']:
            for key,value in (b.get(section) or {}).items():
                new=(a.get(section) or {}).get(key)
                if key.endswith('id') or key in ['relid','indexrelid']:continue
                if isinstance(value,(int,float)) and isinstance(new,(int,float)):
                    delta[key]=new-value
        rels.append(dict(relation=name(b),oid=b['oid'],before_bytes=b['bytes'],after_bytes=a.get('bytes'),delta=delta))
    write(directory/'relation_counters.json',rels)
    write(directory/'inventory.json',before['relations'])
    if (directory/'calls.trace').exists():
        calls=parse_trace(directory/'calls.trace')
        callrows=[]
        for function,entries in calls.items():
            if not function.startswith('calls_'):continue
            for (pid,oid),count in entries.items():
                r=objects.get(oid)
                if r and name(r) in {x['relation'] for x in rels}:
                    callrows.append(dict(function=function[6:],pid=pid,oid=oid,relation=name(r),calls=count))
        write(directory/'call_counts.json',callrows)
        write_tsv(directory/'call_counts.tsv',callrows)
    if (directory/'plans.json').exists():
        plans=[]
        for p in read(directory/'plans.json'):
            b={r['oid']:r for r in p['before']['relations']};changes=[]
            for a in p['after']['relations']:
                old=b.get(a['oid'],{});delta={}
                for section in ['table_stats','table_io','index_stats','index_io']:
                    for key,value in (old.get(section) or {}).items():
                        new=(a.get(section) or {}).get(key)
                        if key.endswith('id'):continue
                        if isinstance(value,(int,float)) and isinstance(new,(int,float)) and new!=value:delta[key]=new-value
                if delta:changes.append(dict(relation=name(a),delta=delta))
            w=read(directory/('plan_'+p['operation'])/'wal_summary.json')
            plans.append(dict(operation=p['operation'],sql=p['sql'],pid=p['pid'],plan=p['plan'],
                lsn_before=p['before']['lsn'],lsn_after=p['after']['lsn'],counter_changes=changes,
                wal=[dict(**r,relation=relname(r['rel'])) for r in w['relations']]))
        write(directory/'plan_summary.json',plans)
    data=dict(case=directory.name,relations=rels,lsm_before=before.get('lsm'),
        lsm_after_run=read(directory/'after_run.json').get('lsm'),
        lsm_after_drain=read(directory/'after_drain.json').get('lsm'))
    write(directory/'summary.json',data)
    # Plain TSV is easier to re-check than a wide nested JSON object.
    table=[]
    for r in rels:
        for phase in ['foreground','drain','checkpoint','shutdown']:
            rows=[x for x in io_rows if x['relation']==r['relation'] and x['phase']==phase]
            d=[x for x in dirty_rows if x['relation']==r['relation'] and x['phase']==phase]
            w=walrows.get((phase,r['relation']),{})
            table.append(dict(case=directory.name,phase=phase,relation=r['relation'],
                pg_buffer_reads_total=r['delta'].get('heap_blks_read',r['delta'].get('idx_blks_read',0)),
                file_read_blocks=sum(x.get('bytes',0) for x in rows if x['operation']=='read')/8192,
                file_write_blocks=sum(x.get('bytes',0) for x in rows if x['operation']=='write')/8192,
                file_read_main_blocks=sum(x.get('bytes',0) for x in rows if x['operation']=='read' and x['fork']=='main')/8192,
                file_write_main_blocks=sum(x.get('bytes',0) for x in rows if x['operation']=='write' and x['fork']=='main')/8192,
                dirtied=sum(x.get('dirtied',0) for x in d),
                dirty_calls=sum(x.get('dirty_calls',0) for x in d),**w))
    write_tsv(directory/'relation_io.tsv',table)
    if not trace.get('dirty_calls') or not trace.get('dirtied'):
        raise RuntimeError(directory.name+': dirty instrumentation produced no observations')
    workload=read(ROOT/'evidence/workloads.json')
    expected=next(iter(workload.values()))['statements']
    observed=sum(x.get('dirty_calls',0) for x in dirty_rows if x['relation']=='public.usertable'
        and x['phase']=='foreground' and not x['hint'])
    if observed<expected:
        raise RuntimeError(f'{directory.name}: incomplete heap dirty-call coverage: {observed} < {expected}')
    if not any(len(k)>=5 and k[4]==1 for k in trace.get('ops',{})):
        raise RuntimeError(directory.name+': no foreground phase file observations')
    # A relation inode seen under a second name would merge two files' offset
    # histories. Fail if this is more than an isolated event; otherwise record it
    # (insert_btree_t1 trial 1 hit one event on the never-replaced B-tree lookup).
    renamed_relations=[]
    for key,count in trace.get('renamed',{}).items():
        filename=str(trace.get('names',{}).get(key,'?'))
        if re.fullmatch(r'\d+(?:_(?:fsm|vm|init))?(?:\.\d+)?',filename) and not re.fullmatch(r'[0-9A-F]{24}',filename):
            renamed_relations.append(dict(key=str(key),last_name=filename,events=count))
    if sum(r['events'] for r in renamed_relations)>10:
        raise RuntimeError(directory.name+': relation inodes acquired multiple names: '+str(renamed_relations))
    if any(len(v)>=60000 for v in trace.values()):
        raise RuntimeError(directory.name+': BPF map is close to its configured capacity')
    unknown=sum(x.get('bytes',0) for x in io_rows if x['relation'].startswith('UNMAPPED:'))
    generations=defaultdict(set)
    for dev,inode,generation in trace.get('names',{}):generations[(dev,inode)].add(generation)
    write(directory/'coverage.json',dict(unmapped_file_io_bytes=unknown,renamed_relation_inodes=renamed_relations,
        map_entries={k:len(v) for k,v in trace.items()},
        validated_offset_streams=len(trace.get('ops',{})),
        expected_statements=expected,foreground_heap_dirty_calls=observed,
        reused_inodes=[dict(device=k[0],inode=k[1],generations=sorted(v)) for k,v in generations.items() if len(v)>1],
        unmapped_dirty_calls=sum(x.get('dirty_calls',0) for x in dirty_rows if x['relation'].startswith('UNMAPPED:')),
        unknown_actor_bytes=sum(x.get('bytes',0) for x in io_rows if x['actor']=='unknown_pid'),
        observed_processes=actors,filenode_mapping=[dict(db=k[0],filenode=k[1],relation=v) for k,v in sorted(filenodes.items())]))
    return data


def write(path,value):path.write_text(json.dumps(value,indent=2,default=str)+'\n')


def write_tsv(path,rows):
    fields=list(dict.fromkeys(k for r in rows for k in r))
    with path.open('w') as f:
        w=csv.DictWriter(f,fields,delimiter='\t');w.writeheader();w.writerows(rows)


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--case');args=parser.parse_args()
    if args.case:
        summarize(ROOT/'results'/args.case)
        print('SUMMARY_OK',args.case)
        return
    results=[]
    for d in sorted((ROOT/'results').iterdir()):
        if d.is_dir() and (d/'PASS.json').exists():
            summarize(d)
            result=read(d/'PASS.json');timing=read(d/'timing.json')
            results.append(dict(case=d.name,workload=result['workload'],kind=result['kind'],trial=result['trial'],
                tps=result['metrics']['tps'],**{k:v for k,v in timing.items() if k!='markers'}))
    write(ROOT/'evidence/results.json',results);write_tsv(ROOT/'evidence/results.tsv',results)
    variability=[]
    for wl in ['insert','update']:
        for kind in ['plain','btree','lsm']:
            rows=[r for r in results if r['workload']==wl and r['kind']==kind]
            if not rows:continue
            t=[r['tps'] for r in rows]
            variability.append(dict(workload=wl,kind=kind,trials=len(t),min_tps=min(t),max_tps=max(t),
                median_tps=statistics.median(t),cv_pct=statistics.stdev(t)/statistics.mean(t)*100 if len(t)>1 else None,
                median_full_cost_tps=statistics.median(r['total_tps'] for r in rows)))
    write(ROOT/'evidence/variability.json',variability)
    print(json.dumps(dict(completed=len(results),variability=variability),indent=2))


if __name__=='__main__':main()
