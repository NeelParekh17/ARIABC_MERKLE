#!/usr/bin/env python3
"""Incremental exact WAL statistics for a conservative inner sample interval.

Dump disjoint ranges during measurement, before recycling. Intermediate range
ends are real WAL-record starts (pg_waldump -n1), so spanning records are neither
lost nor counted twice. Retain every command, LSN and raw statistics output.
The primary gateway completion window is unchanged.

A physical replication slot, reserved before the window and advanced after each
dump, keeps every undumped segment from being recycled. A sampled LSN is the WAL
write frontier, so the record after it may not be on disk yet: boundary lookups
retry briefly instead of failing (pg W5 2026-10-12: "could not find a valid
record after 1/1598FFA8").
"""
import argparse,csv,hashlib,json,os,re,subprocess,time
from pathlib import Path
from summary import wal_stats

SLOT='tpcc_wal_collector'
BOUNDARY_RETRY_S=10

def lsn(value):
    hi,lo=value.split('/');return (int(hi,16)<<32)+int(lo,16)

def sql(query):
    cmd=[str(Path(os.environ['INST'])/'bin/psql'),'-X','-qAt','-v','ON_ERROR_STOP=1','-h','127.0.0.1',
         '-p',os.environ['PORT'],'-U','postgres','-d','postgres','-c',query]
    result=subprocess.run(cmd,capture_output=True,text=True,timeout=30)
    if result.returncode:raise RuntimeError(f'psql failed: {query}: {result.stderr.strip()}')
    return result.stdout.strip()

def tail(path,stop):
    while not path.exists():
        if stop.exists():return
        time.sleep(.2)
    with path.open() as f:
        pending=''
        while True:
            piece=f.readline()
            if piece:
                pending+=piece
                if pending.endswith('\n'):
                    yield pending;pending=''
            elif stop.exists():
                if pending:raise ValueError('truncated sampler CSV')
                return
            else:time.sleep(.2)

class Collector:
    def __init__(self,run,ramp,duration,interval=60):
        self.run=run;self.lo=ramp+1;self.hi=ramp+duration;self.interval=interval
        self.first=self.last=None;self.cursor=None;self.last_dump_time=None;self.parts=[];self.finished=False
        self.folder=run/'wal_chunks';self.folder.mkdir(exist_ok=False)
        self.binary=str(Path(os.environ['INST'])/'bin/pg_waldump');self.waldir=str(run/'pgdata/pg_wal')
        self.slot_events=[]
    def reserve(self):
        # Reserved now (ramp), so its restart LSN precedes every window sample.
        restart=sql(f"SELECT lsn FROM pg_create_physical_replication_slot('{SLOT}',true)")
        self.slot_events.append(dict(event='create',restart_lsn=restart,wall_epoch=time.time()))
    def release(self):
        if not self.slot_events or self.slot_events[-1]['event']=='drop':return
        sql(f"SELECT pg_drop_replication_slot('{SLOT}')")
        self.slot_events.append(dict(event='drop',wall_epoch=time.time()))
    def command(self,args,name,retry_s=0):
        cmd=[self.binary,'-p',self.waldir]+args
        deadline=time.monotonic()+retry_s;tries=0
        while True:
            result=subprocess.run(cmd,capture_output=True,text=True);tries+=1
            if not result.returncode or time.monotonic()>=deadline:break
            time.sleep(.2)
        (self.folder/(name+'.txt')).write_text(result.stdout+result.stderr)
        if result.returncode:raise RuntimeError(f'pg_waldump failed: {cmd} rc={result.returncode} tries={tries}')
        if tries>1:self.slot_events.append(dict(event='boundary_retry',name=name,tries=tries,wall_epoch=time.time()))
        return result.stdout,cmd
    def dump(self,end):
        if lsn(end)==lsn(self.cursor):return
        if lsn(end)<lsn(self.cursor):raise ValueError('nonmonotonic WAL interval')
        name=f'part{len(self.parts):04d}'
        raw,cmd=self.command(['-s',self.cursor,'-e',end,'--stats=record'],name)
        stats=wal_stats(raw)
        if not stats or stats['record_bytes']+stats['fpi_bytes']!=stats['combined_bytes']:raise ValueError('invalid WAL stats')
        self.parts.append(dict(start_lsn=self.cursor,end_lsn=end,command=cmd,stats=stats,
                               raw_sha256=hashlib.sha256((self.folder/(name+'.txt')).read_bytes()).hexdigest()))
        self.cursor=end
        sql(f"SELECT pg_replication_slot_advance('{SLOT}','{end}')")
        (self.run/'wal_chunks.json').write_text(json.dumps(self.parts,indent=2)+'\n')
    def row(self,row):
        if row.get('error') or not all(row.get(k) for k in ('elapsed_s','progress_age_s','wall_epoch','district_next','wal_lsn')):return False
        elapsed=float(row['elapsed_s'])+float(row['progress_age_s'])
        if self.lo<=elapsed-.051 and elapsed+.051<self.hi:
            if self.first is None:
                self.first=row.copy();self.cursor=row['wal_lsn'];self.last_dump_time=elapsed
            self.last=row.copy()
            if elapsed-self.last_dump_time>=self.interval:
                # Align internal boundaries to a real record start, retaining
                # the cross-boundary record in exactly the following chunk.
                raw,_=self.command(['-s',row['wal_lsn'],'-n','1'],f'boundary{len(self.parts):04d}',BOUNDARY_RETRY_S)
                match=re.search(r'\blsn:\s+([0-9A-F]+/[0-9A-F]+)',raw)
                if not match:raise ValueError('missing WAL boundary record')
                self.dump(match[1]);self.last_dump_time=elapsed
        elif self.first is not None and elapsed+.051>=self.hi:
            # Use the last fully inside sample exactly as the analysis selector.
            self.finish(True);return True
        return False
    def finish(self,covered):
        if self.finished:return
        self.finished=True
        if self.first is None or self.last is None:raise ValueError('no WAL window samples')
        self.dump(self.last['wal_lsn'])
        sums={k:sum(p['stats'][k] for p in self.parts) for k in ('records','record_bytes','fpi_bytes','combined_bytes')}
        if not self.parts:raise ValueError('empty WAL interval')
        # Preserve the existing parser/gate while retaining per-part raw output.
        (self.run/'walstats.txt').write_text('Incremental disjoint WAL ranges; see wal_chunks.json and wal_chunks/*.txt\n'
            f"Total {sums['records']} {sums['record_bytes']} [0.00%] {sums['fpi_bytes']} [0.00%] {sums['combined_bytes']} [100.00%]\n")
        self.release()
        (self.run/'wal_collection.json').write_text(json.dumps(dict(covered=covered,start=self.first,end=self.last,
            stats=sums,parts=len(self.parts),interval_s=self.interval,source_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            slot=dict(name=SLOT,events=self.slot_events),
            method='disjoint exact record-aligned intermediate ranges, native final sample endpoint'),indent=2)+'\n')

def main():
    p=argparse.ArgumentParser();p.add_argument('run',type=Path);p.add_argument('--ramp',type=float,required=True)
    p.add_argument('--duration',type=float,required=True);p.add_argument('--interval',type=float,default=60);a=p.parse_args()
    collector=Collector(a.run,a.ramp,a.duration,a.interval)
    csv.field_size_limit(4*1024*1024)
    collector.reserve()
    try:
        for row in csv.DictReader(tail(a.run/'samples.csv',a.run/'wal_collector.stop')):
            if collector.row(row):return 0
        collector.finish(False);return 2
    finally:
        collector.release()

if __name__=='__main__':raise SystemExit(main())
