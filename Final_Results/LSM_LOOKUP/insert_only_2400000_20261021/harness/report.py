#!/usr/bin/env python3
"""Generate a reviewable report draft from retained, validated measurements."""
from collections import Counter,defaultdict
import csv
import hashlib
import json
from pathlib import Path
import statistics

ROOT=Path('/home/neel/claude_ctl/lsm_relation_audit_20261021')
DBROOT='/home/neel/claude_checks/lsm_relation_audit_20261021'


def read(p):return json.loads(p.read_text())
def num(x):return f'{x:,.0f}' if isinstance(x,(float,int)) else str(x)
def table(head,rows):
    def cell(s):return str(s).replace('|','\\|').replace('\n','<br>')
    return '\n'.join(['| '+' | '.join(head)+' |','|'+'|'.join('---' for _ in head)+'|']+
        ['| '+' | '.join(cell(v) for v in r)+' |' for r in rows])+'\n'


def role(r):
    name=r['relname']
    if r['relkind']=='t':return 'TOAST chunk_id, chunk_seq, chunk_data','heap/TOAST maintenance','External payload chunks; not a second full row'
    if name.startswith('pg_toast_'):return 'TOAST (chunk_id,chunk_seq) -> chunk TID','TOAST index maintenance','No'
    if name=='usertable':return 'Key + 10 text values + MVCC header','heap insert/update through table AM','Yes: primary row storage'
    if name=='usertable_pkey1':return 'ycsb_key -> heap TID','ordinary executor btinsert on INSERT/non-HOT UPDATE','Key only'
    if name=='usertable_merkle_idx':return 'Merkle format/routing/build metapage','merkle_init_tree at build/rebuild','No'
    if name=='merkle_node_usertable':return 'count,partition,prefix,is_leaf,node_id,aggregate hash','merkle_direct_update_node; structural SPI/build paths','No row payload'
    if name=='merkle_node_usertable_pkey':return '(partition,node_id,prefix) -> node TID','node heap INSERT/non-HOT UPDATE','Node key only'
    if name.endswith('_prefix_idx'):return '(partition,prefix,node_id) -> node TID','node heap INSERT/non-HOT UPDATE','Node key only'
    if name.endswith('_root_idx'):return 'partition -> root TID, WHERE prefix=0','node heap INSERT/non-HOT UPDATE','Node key only'
    if name=='usertable_merkle_lookup_idx':
        return '(partition,route hash,ycsb_key) -> heap TID',('ml_build; ml_copy to base' if r['amname']=='merkle_lsm' else 'ordinary executor btinsert'),'Key only; no 10 text fields'
    if name.endswith('_meta'):return 'MlMeta state, OIDs, active slots, epochs, counters','ml_write_meta / flush / build, generic WAL','No'
    if '_l0_s' in name:return '(partition,route hash,ycsb_key) -> heap TID','ml_insert -> btinsert on active slot; source replacement','Key only'
    if '_l1_s' in name:return '(partition,route hash,ycsb_key) -> heap TID','ml_copy from L0; source replacement','Key only'
    return str(r.get('indexdef') or r.get('columns')),'See catalog definition','Inspect definition'


def io_summary(d,relation,phase=None,actor=None,operation=None):
    out=Counter()
    for r in read(d/'offset_histograms.json')['rows']:
        if r['relation']!=relation or r['fork']!='main':continue
        if phase and r['phase']!=phase:continue
        if actor and r['actor']!=actor:continue
        if operation and r['operation']!=operation:continue
        out.update({k:v for k,v in r.items() if isinstance(v,(float,int))})
    transitions=sum(out['distance_'+str(b)] for b in range(7))
    return dict(ops=out['ops'],blocks=out['bytes']/8192,transitions=transitions,
        nonadjacent=sum(out['distance_'+str(b)] for b in range(3,7)),contiguous=out['contiguous'])


def main():
    results=read(ROOT/'evidence/results.json')
    if len(results)!=12 or 'AUDIT_COMPLETE' not in (ROOT/'status.txt').read_text():
        raise RuntimeError('A complete 12-case campaign is required before a final report draft')
    v=read(ROOT/'evidence/variability.json')
    dirs={r['case']:ROOT/'results'/r['case'] for r in results}
    first={kind:dirs['insert_'+kind+'_t1'] for kind in ['plain','btree','lsm']}
    inventory={k:read(d/'inventory.json') for k,d in first.items()}
    def size(kind,rel):return next(r['bytes'] for r in inventory[kind] if r['relname']==rel)
    med={(r['workload'],r['kind']):r for r in v}
    cases={name:read(d/'PASS.json') for name,d in dirs.items()}
    lines=['# Merkle / merkle_lsm relation and write-I/O audit','',
        '## Conclusions for the advisor','',
        f'**1. The data still lives in a heap.** The baseline application heap is {num(size("lsm","usertable"))} bytes. '
        f'The {num(size("lsm","usertable_merkle_lookup_idx"))}-byte public lookup is a secondary index of '
        '`(partition, route hash, user key) -> heap TID`, with four hidden upper B-trees and one metadata page. '
        'The ten text values are absent from these lookup entries. `lsm3` has the same secondary-index role. '
        'Merkle nodes are separate heap rows; the core Merkle AM relation contains its metadata, not a second row store.','',
        '**2. Adding Merkle changes both aggregate-node maintenance and lookup maintenance.** The table below compares '
        'the same 2.4M SQL statements on byte-checked 100M-row baselines, with two trials per case. '
        'All TPS values are instrumented measurements; they do not establish an uninstrumented production speedup.','',
        table(['Workload','Plain median TPS','Merkle+B-tree median TPS','Merkle+LSM median TPS'],
            [[wl]+[num(med[(wl,k)]['median_tps']) for k in ['plain','btree','lsm']] for wl in ['insert','update']]),
        '**3. “Only sequential I/O” is not an implementation guarantee.** The implementation WAL-logs ordinary B-tree '
        'updates at every LSM level and inserts sorted merge entries into an existing destination tree. '
        'The file-offset evidence below distinguishes adjacency, repeated pages and long jumps, and separates actual '
        'file writes from WAL modification order. Heap and Merkle-node writes remain outside this lookup optimization.','']
    for operation in ['read','write']:
        s=io_summary(first['lsm'],'public.usertable_merkle_lookup_idx',operation=operation)
        lines.append(f'- Insert/LSM trial 1 base {operation}: {num(s["ops"])} file calls; '
            f'{num(s["nonadjacent"])} nonadjacent transitions out of {num(s["transitions"])}; '
            f'{num(s["contiguous"])} forward-contiguous pairs. These are real file offsets, not guaranteed device-cache misses.')
    lines+=['','## Discrepancies and bounded findings','',
        '- This is a secondary LSM lookup, not primary LSM row storage. Keys are duplicated in indexes; payload rows are not duplicated there.',
        '- The core Merkle integrity structure and its node heap have not become LSM-based.',
        '- Active L0 is itself a PostgreSQL B-tree, not an append-only on-disk SSTable or a dedicated memory table.',
        '- `ml_copy` inserts into an existing B-tree. The architecture documents already say this; the sequential-only expectation is stronger than the docs.',
        '- C `index_beginscan` in `merkleapply.c` searches the node-table PK. Auxiliary lookup use for splits is planned SQL through SPI. Outer EXPLAIN omits both the commit-time direct node path and nested SPI plans.',
        '- Payload updates are HOT-eligible, not guaranteed universally HOT. Exact fractions and any lookup WAL are reported below.',
        '- `merkleinsert.c:38-40` has an outdated comment saying UPDATE uses ambulkdelete; actual UPDATE capture is in the executor and ambulkdelete is a VACUUM statistics stub.',
        '- L0/L1/base copies are intentional entry placements, not by themselves evidence of duplicate executor maintenance.',
        '- `pg_relation_size` / parent-only table-index totals omit hidden upper components. The explicit inventory includes them.',
        '- pgstats buffer misses and successful VFS reads are not synonymous with physical device reads. No per-relation NAND/device cache-miss count is claimed.',
        '', '## Part 1 — relations and contents','',
        'Each inventory is taken from a restored 100M-row working copy before the benchmark. Bytes are exact. '
        'The main/FSM/VM fork sizes are separate; entries without payload remain metadata/index storage even when relkind is `i`.','']
    for kind,rows in inventory.items():
        lines+=['### '+kind,'',table(['Relation','Kind / AM','Main / FSM / VM bytes','Contents','Writer','Row data?'],
            [[r['nspname']+'.'+r['relname'],str(r['relkind'])+' / '+str(r['amname']),
              ' / '.join(num(r.get(k) or 0) for k in ['bytes','fsm_bytes','vm_bytes']),*role(r)] for r in rows]),
            f'Catalog definitions, column types, OIDs, relfilenodes and all index flags: `evidence/{first[kind].name}/inventory.json`.','']
    lines+=['To store Merkle nodes only inside the Merkle index relation, replace the backing heap/three-index persistence with '
        'custom index pages, addressing, allocation, locking and WAL/recovery, including commit/abort visibility and rollback '
        'currently supplied by the backing heap; the application heap would still exist. '
        'To put application rows into an LSM and remove that heap, implement primary/table storage with full row versions, '
        'MVCC visibility, conflict/predicate handling, durability/recovery, deletion/version reclamation, large-value storage '
        'and secondary-index row identity. Changing this secondary lookup AM cannot accomplish either change by itself. '
        'No engine change was made.','',
        '## Part 2 — actual SQL, plans, hidden paths and correctness','',
        'Diagnostic clients explicitly begin SERIALIZABLE transactions, run EXPLAIN ANALYZE, commit, disconnect, and wait for '
        'PG13 statistics publication. The captured WAL range includes commit-time work which EXPLAIN execution totals omit. '
        'Plans run after the timed benchmark; they do not warm or mutate its starting state.','']
    for kind in ['plain','btree','lsm']:
        d=dirs['update_'+kind+'_t2'];plans=read(d/'plan_summary.json')
        lines+=['### '+kind+' plans','']
        for p in plans:
            lines+=['**'+p['operation']+'**','', '```sql',p['sql'],'```','',
                '```json',json.dumps(p['plan'],indent=2),'```','',
                'Changed relation counters: '+json.dumps(p['counter_changes'])+'.','']
        lines += [f'Per-statement WAL and counter evidence: `evidence/{d.name}/plan_summary.json`; '
            f'AM call counts: `evidence/{d.name}/call_counts.tsv`.','']
    lines+=['Lookup-positive controls insert a 50K batch, wait for an L0->L1 merge, then append 1K tails outside the timing window. '
        'They compare old/base, L1 and L0 point results against the primary-key oracle, and a mixed old/new multi-row range against '
        'an independent forced heap scan. Default point plans and any forced-plan fallback are retained. '
        'The short call trace counts parent/child B-tree scans and insert callbacks. These controls do not prove all SQL '
        'semantics or every concurrent/crash schedule.','',
        '## Part 3 — write-intensive measurements','',
        'Engine/source SHA: `5f2a078eadb8817246f5d500dee2822f826d9c1e`. The pre-existing baseline records a7fc519d; '
        'it was not rebuilt. Matching install/server/gateway digests and source manifests were checked. '
        'Heap and primary-index segment hashes were compared read-only across all three baseline directories. '
        'Every trial uses a pristine physical delta restore, cold OS caches, reference SSD calibration/settling, 32MB '
        'shared buffers, 16 workers, SERIALIZABLE, fsync/full-page-writes/synchronous-commit on. The OS page cache is unbounded.','',
        'Pure INSERT uses monotonic keys 100000001..102400000, avoiding the stock all_insert bucket allocator overflow. '
        'UPDATE uses the standard ten-field SQL constructor, finite Zipf .99 and seed 42. Each file is identical across '
        'all setups/trials. Trial order is plain/btree/lsm then lsm/btree/plain.','',
        '### Every trial and drain cost','',table(['Case','Gateway TPS','Gateway s','Drain s','Checkpoint s','Sum-phase TPS','Continuous bracket TPS'],
            [[r['case'],num(r['tps']),f'{r["gateway_s"]:.3f}',f'{r["drain_s"]:.3f}',f'{r["checkpoint_s"]:.3f}',
              num(r['total_tps']),num(r['continuous_bracket_tps'])] for r in results]),'',
        'Gateway TPS uses the canonical all-terminal-results interval. Sum-phase TPS includes gateway time plus actual remote '
        'flush and checkpoint command time. The continuous bracket includes intervening observation, server stop and SSH/job-poll '
        'overheads through completed checkpoint. Neither includes physical restore, validation, WAL analysis or final verification. '
        'Shutdown I/O is collected separately to expose any remaining write debt.','',
        table(['Workload','Setup','Trials','TPS min / max','CV %','Median sum-phase TPS'],
            [[r['workload'],r['kind'],r['trials'],num(r['min_tps'])+' / '+num(r['max_tps']),
              f'{r["cv_pct"]:.2f}',num(r['median_full_cost_tps'])] for r in v]),'',
        'Two trials describe observed spread; they are not a statistical performance qualification. No failed/noisy observation '
        'is silently promoted to an accepted result.','',
        '### Whole-device and WAL totals','']
    device_rows=[]
    for case,d in dirs.items():
        data=read(d/'io.json');a=data['device_before'];b=data['device_after'];z=data['device_checkpoint']
        wal=read(d/'wal_summary.json')
        record_bytes=sum(r['total_bytes'] for r in wal['rmgr'])
        fpi_bytes=sum(r['fpi_bytes'] for r in wal['rmgr'])
        wal_file_bytes=sum(r.get('bytes',0) for r in read(d/'offset_histograms.json')['rows']
            if r['relation']=='WAL' and r['operation']=='write' and r['phase']!='setup')
        device_rows.append([case]+[f'{(v[k]-a[k])/2048:.2f}' for v in [b,z] for k in ['read_sectors','write_sectors']]+
            [f'{record_bytes/2**20:.2f}',f'{fpi_bytes/2**20:.2f}',f'{wal_file_bytes/2**20:.2f}'])
    lines += [table(['Case','Run device read MiB','Run device write MiB','Through checkpoint read MiB','Through checkpoint write MiB',
        'Decoded WAL record MiB','FPI MiB','WAL file VFS write MiB'],device_rows),'',
        'Device counters cover the whole filesystem device, including monitoring/log traffic. Decoded WAL record bytes exclude '
        'WAL page headers and alignment; WAL-file writes can rewrite part-filled pages and are a separate quantity. '
        'The raw LSN endpoints and independent waldump stats are retained.','',
        '### HOT and merge accounting','']
    hot=[];merge=[]
    for case,d in dirs.items():
        s=read(d/'summary.json')
        for r in s['relations']:
            if r['relation']=='public.usertable':
                u=r['delta'].get('n_tup_upd',0);h=r['delta'].get('n_tup_hot_upd',0)
                hot.append([case,num(u),num(h),f'{100*h/u:.4f}%' if u else 'N/A'])
        if s['lsm_before']:
            a=s['lsm_before']['merges'];b=s['lsm_after_run']['merges'];c=s['lsm_after_drain']['merges']
            merge.append([case,str([b[i]-a[i] for i in range(2)]),str([c[i]-b[i] for i in range(2)])])
    lines+=[table(['Case','Application updates','HOT updates','HOT fraction'],hot),'',
        table(['Case','Run L0 / L1 completed merges','Drain L0 / L1 completed merges'],merge),'',
        '### Per-run, per-relation totals','',
        'PG reads are shared-buffer misses over the full stats bracket, including clean shutdown/restart publication. '
        'File reads/writes are successful VFS bytes / 8192, include repeat transfers and all forks, and may hit Linux page cache. '
        'Dirtied is the actual per-process shared_blks_dirtied increment observed at MarkBufferDirty/Hint return, not an estimate '
        'from WAL. WAL image bytes use the actual image length after holes/compression.','']
    for case,d in dirs.items():
        with (d/'relation_io.tsv').open() as f:rows=list(csv.DictReader(f,delimiter='\t'))
        totals=defaultdict(Counter)
        for r in rows:
            t=totals[r['relation']]
            t['pg_buffer_reads']=float(r['pg_buffer_reads_total'])
            for k in ['file_read_blocks','file_write_blocks','dirtied','wal_bytes','fpis']:
                t[k]+=float(r.get(k) or 0)
        lines+=['#### '+case,'',table(['Relation','PG buffer reads','File read blocks','Dirtied','File write blocks','WAL bytes','FPIs'],
            [[rel]+[num(v[k]) for k in ['pg_buffer_reads','file_read_blocks','dirtied','file_write_blocks','wal_bytes','fpis']]
             for rel,v in totals.items()]),
            f'All phase/actor/direction distance and run histograms: `evidence/{case}/offset_histograms.json`. '
            f'Phase totals: `evidence/{case}/relation_io.tsv`. Rmgr + relation WAL: `evidence/{case}/wal_summary.json` '
            f'and matching-version `waldump_stats_*.txt`.','']
    lines+=['### Attribution and limits','',
        '- Offset histograms use each process/inode/file-generation/direction stream and reset at phase boundaries; generation '
        'tracking prevents reused component inodes from mixing histories. Distance bins are same, '
        '+1, -1, 2..7, 8..63, 64..1023 and >=1024 blocks. Runs count contiguous 8KB-equivalent transfers in bins '
        '1, 2, 3..4, 5..8, 9..16, 17..32, 33..64, >=65. The merger actor remains separate during the foreground interval.',
        '- These are file offsets and PostgreSQL buffer dirtying events, not per-relation device-sector or NAND traces. '
        'Aggregate device read/write sectors are in each io.json. Cache hits, read-ahead, filesystem writeback and device scheduling '
        'prevent equating VFS bytes with physical media bytes.',
        '- WAL block-reference order shows modified pages, not writeback order. Distinct WAL-referenced pages are recorded separately '
        'and are not mislabeled as buffer dirty transitions.',
        '- A foreground backend can flush a base page previously dirtied by the merger when evicting a shared buffer. '
        'A foreground pwrite PID alone therefore does not prove a foreground base-index insertion; dirtying origins and AM calls '
        'must be compared. Per-process distance gaps can also reflect interleaved sequential writers rather than random device order.',
        '- Record bytes for records referencing multiple relations are shared equally; actual FPI bytes belong to their block relation. '
        'Storage CREATE/TRUNCATE records are attributed by their exact storage path. Transaction/control records without a relation '
        'remain NO_BLOCK_REFERENCE. Global rmgr totals retain all WAL and provide a reconciliation boundary.',
        '- BPF uprobes and monitoring add overhead. The independent .111 mock control verified exact known dirty increments, offsets, '
        'phase control, PIE symbol relocation and relfilenode identity. It is not an engine correctness or throughput result.',
        '- All-request terminal success, one affected row per request, exact final count/bounds and full Merkle verification are required. '
        'Compare stored merkle_root.txt across identical deterministic runs; any mismatch requires explicit reporting, not engine fixes.',
        '', '## Scripts, commands and evidence','',
        f'Controller root (.111): `{ROOT}`. DB root (.247): `{DBROOT}`. '
        'Raw compressed pg_waldump files remain under the DB root results/<case>/waldump.txt.gz; no raw WAL or big server log '
        'needs to be downloaded to review the tables. `commands.jsonl` retains executed remote command bodies with credentials redacted; '
        'detached command scripts/stdout/stderr/status/timing remain in each host root jobs/. The local agents/codex.log retains '
        'staging, source-navigation and tool commands.','',
        table(['Script','Purpose','SHA256'],[[p.name,{
            'audit.py':'gate, reset, benchmark, phase boundaries, acceptance, cleanup',
            'observer.py':'catalog/stats/inode sampling, plans, scan controls',
            'io.bt':'kernel file-offset collector; generated additions observe dirtying and relfilenodes',
            'wal.py':'stream raw WAL and derive per-relation/rmgr/FPI summaries',
            'baseline_identity.py':'read-only baseline heap/PK segment hash comparison',
            'summarize.py':'derive tables/histograms/coverage from preserved observations',
            'instrumentation_smoke.py':'isolated .111 instrumentation positive control',
            'report.py':'generate this reviewable draft'} .get(p.name,'supporting script'),
            hashlib.sha256(p.read_bytes()).hexdigest()] for p in sorted((ROOT/'scripts').iterdir()) if p.suffix=='.py' or p.name=='io.bt']),
        '', 'Launch and reproduction entry points (never use /tmp on the lab hosts):','',
        '```bash',
        'ssh -o ControlMaster=no neel@10.129.27.111',
        'export TMPDIR=$HOME/claude_ctl/lsm_relation_audit_20261021/scratch',
        'python3 -u $HOME/claude_ctl/lsm_relation_audit_20261021/scripts/audit.py',
        'python3 $HOME/claude_ctl/lsm_relation_audit_20261021/scripts/summarize.py',
        'python3 $HOME/claude_ctl/lsm_relation_audit_20261021/scripts/report.py','```','',
        'The controller is normally launched detached in its own tmux session. It never builds/rebuilds the baselines, '
        'never starts before the matrix end/exit/no-postgres gate, enforces >=30GB free before restore, and limits '
        'cleanup to its own port/socket markers in the authorized work copies. Failed/incomplete case evidence is archived '
        'before any retry. Immutable baseline pg_control/PG_VERSION hashes and timestamps must match before/after. '
        'No engine source, git commit/push, other campaign session, base_ff90_lsm, pgdata_lsm or .codex was changed.','']
    (ROOT/'REPORT_DRAFT.md').write_text('\n'.join(lines)+'\n')
    print(ROOT/'REPORT_DRAFT.md')


if __name__=='__main__':main()
