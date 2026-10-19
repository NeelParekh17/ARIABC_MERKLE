#!/usr/bin/env python3
"""Render LSM architecture additions on the lab host; no database access.

Uses the existing Merkle guide's Pillow figure helpers and visual conventions.
Run: python3 render_visual.py --out <isolated-output-directory>
"""
import argparse
import importlib.util
import json
from pathlib import Path

HELPER = Path(__file__).resolve().parents[1] / 'merkle_implementation_diagrams/render.py'
spec = importlib.util.spec_from_file_location('merkle_figures', HELPER)
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)


def figures():
    out = []
    add = out.append
    add(m.panels('09_baselines', 'Two baselines, two independent changes',
        ['LEGACY STORAGE REPORT', 'OCTOBER LSM EXPERIMENT'], [
            ('User heap: fillfactor 100\n32 rows/page; 25.60 GB',
             'User heap: fillfactor 90\n29 rows/page; 28.25 GB'),
            ('Merkle: fanout 4, split 32\n8,958,860 reported node rows',
             'Merkle: fanout 32, split 1,024\n211,400 recorded node rows'),
            ('Global node table + 2 indexes\n1,385,996,288 bytes',
             'Dedicated node table + 3 indexes\n38,436,864 bytes'),
            ('Lookup B-tree: 3,154,264,064 bytes\nPrimary key: 2,246,197,248 bytes',
             'Same initial lookup base / primary size\nLSM adds two empty 8 KiB tops')],
        'Heap growth comes from fillfactor; node-storage reduction comes mainly from geometry.',
        'Reported legacy bytes vs recorded experiment catalogue. LSM did not cause the node-count reduction.'))

    add(m.lanes('10_heap_math', 'Why the heap is 28.25 GB', [
        ('Fixed-width benchmark row\n248-byte aligned heap tuple',
         '+ 4-byte line pointer\n= 252 bytes of page space per row'),
        ('8,192-byte page; 24-byte header\nfillfactor 90 reserves 819 bytes',
         'floor((8,192 - 24 - 819) / 252)\n= 29 rows per page'),
        ('100,000,000 benchmark rows\nceil(100,000,000 / 29)',
         '3,448,276 pages x 8,192 bytes\n= 28,248,276,992 bytes')],
        'PostgreSQL uses an integer free-space target: 8,192 x 10 / 100 = 819 bytes.',
        'Matches this baseline main fork. Different row widths, load history, FSM/VM and WAL change other sizes.'))

    node_indexes = m.tree('11_node_indexes', 'Three B-trees over the Merkle node heap',
        'ariabc_internal.merkle_node_usertable\n211,400 rows; heap fillfactor 80', [
            ('PRIMARY KEY\n(partition_id, node_id, prefix_len)\nExact node + uniqueness', 'blue'),
            ('PREFIX INDEX\n(partition_id, prefix_len, node_id)\nPartition / depth / prefix scans', 'blue'),
            ('PARTIAL ROOT INDEX\n(partition_id)\nWHERE prefix_len = 0\n200 baseline roots', 'amber')],
        'These index the node heap. The LSM lookup indexes application rows instead.',
        'Bytes: heap 24,739,840 + PK 6,676,480 + prefix 7,004,160 + root 16,384 = 38,436,864.')
    for key in ['c0', 'c1', 'c2']:
        x, y, width, height, text, color = node_indexes.nodes[key]
        node_indexes.nodes[key] = (x, y, width, 195, text, color)
    add(node_indexes)

    f = m.Figure('12_entry_bytes', 'One lookup leaf entry consumes 28 bytes',
        'Non-null (smallint partition, bytea route hash, integer user key) in this benchmark.', 900,
        'bytea uses a 1-byte short header + 8-byte digest. The route hash is not a SQL int8 value.')
    x = 70
    fields = [('header', 'IndexTupleData\n8 bytes\noffsets 0-7', 300, 'blue'),
              ('part', 'partition\n2 bytes\noffsets 8-9', 220, 'green'),
              ('hash', 'short bytea + digest\n9 bytes\noffsets 10-18', 420, 'amber'),
              ('pad', 'pad\n1 byte\noffset 19', 160, 'gray'),
              ('key', 'user key\n4 bytes\noffsets 20-23', 280, 'green')]
    for key, text, width, color in fields:
        f.box(key, text, x, 230, width, 170, color)
        x += width + 15
    f.box('tuple', 'Aligned index tuple\n8 + 2 + 9 + 1 + 4 = 24 bytes', 70, 520, 650, 160, 'blue')
    f.box('space', 'Page space per leaf entry\n24-byte tuple + 4-byte line pointer = 28', 870, 520, 650, 160, 'amber')
    f.edge('tuple', 'space')
    add(f)

    add(m.lanes('13_capacity', 'Top capacity: estimate versus observation', [
        ('Configured trigger: 1 MiB\n1,048,576 / 8,192',
         '128 total pages\nIncludes metapage and internal pages'),
        ('28 bytes per non-null leaf entry\nAssume densely packed leaf pages',
         'Roughly 30,000-33,000 entries\nPacking estimate, not a measured limit'),
        ('Actual short YCSB D cases\n1,024 new lookup entries',
         'top0: 56 KiB; top1: 8 KiB\nZero requested merges')],
        'Random inserts and page splits change occupancy; B-tree fillfactor is not a constant density.',
        'A top is a PostgreSQL relation, not a reserved 1 MiB RAM buffer.'))

    add(m.pipeline('14_size_checks', 'Why a 1 MiB setting can overshoot', [
        ('Reserve zero-based insertion number\nunder the shared spinlock', 'blue'),
        ('Check positions 0, 65,536, ...\nFirst check is insertion 1', 'blue'),
        ('Between checks\nkeep inserting into the active top', 'amber'),
        ('At a check: top > 1 MiB?\nNo merge already running?', 'amber'),
        ('Rotate tops and request merge\nnext insertion uses the other top', 'green'),
        ('During a merge\nactive top can keep growing', 'amber')],
        'The next periodic check after position 0 is the 65,537th callback since counter initialization.',
        'No hard 1 MiB or 2 MiB bound. Index callbacks count; SELECTs and HOT updates do not.'))

    f = m.Figure('15_sparse_merge', 'Sorting does not remove sparse base updates',
        'Schematic page destinations. The measured base has 385,042 total 8 KiB pages, not a measured leaf count.', 980,
        'Merge reads/modifies destination pages + generates WAL. Cache reuse can help; sustained cost is unmeasured.')
    f.box('top', 'Frozen top: sorted recent entries\nFour illustrated keys target far-apart base leaves',
          430, 205, 740, 150, 'amber')
    for i in range(10):
        f.box(f'p{i}', f'leaf\n{i}', 70 + i*147, 560, 130, 110,
              'amber' if i in (0, 3, 6, 9) else 'gray')
    f.branch('top', [f'p{i}' for i in (0, 3, 6, 9)], 440)
    f.box('cost', 'Foreground: small top writes\nBackground: deferred base-page work\nBoth consume CPU, cache, I/O and WAL bandwidth',
          230, 710, 1140, 150, 'blue')
    add(f)

    add(m.pipeline('16_wal', 'The merge uses PostgreSQL WAL machinery', [
        ('Foreground top insertion\nbtinsert() -> B-tree WAL', 'blue'),
        ('Merge into existing base\n_bt_doinsert() -> B-tree WAL', 'blue'),
        ('Call RelationTruncate(index, 0)\nto clear the inactive top', 'amber'),
        ('WAL-log before physical truncation\nXLOG_SMGR_TRUNCATE', 'amber'),
        ('Recreate empty main-fork metapage\nlog_newpage(MAIN_FORKNUM)', 'green'),
        ('Sync new index storage\nsmgrimmedsync()', 'green')],
        'Shown for the experiment\'s ordinary logged indexes; source-level mechanism, not a crash-test result.',
        'This is main-fork metapage initialization, not an unlogged-relation INIT_FORKNUM rebuild.'))

    add(m.lanes('17_crash_boundaries', 'Crash recovery: mechanism is not qualification', [
        ('Crash after some base copies\nbut before the top is cleared',
         'Recovered base/top copies may coexist\nRead path suppresses identical key/TID copies', 'amber', 'blue'),
        ('Crash across truncate / metapage rebuild\nor a repeated consolidation',
         'Replay ordering and completeness\nneed explicit crash injection tests', 'amber', 'amber'),
        ('Control entry reconstructed at startup\nlsm3_get_entry()',
         'Find both tops by name; larger is active\nEqual sizes -> top0; counters reset', 'gray', 'blue')],
        'Copy persistence after a crash is a possible state inferred from WAL; it was not fault-injected.',
        'Startup code shows no dedicated orphan-entry scrub or durable merge-resume protocol.'))

    add(m.pipeline('18_copy_locks', 'COPY has a special merger-lock workaround', [
        ('COPY holds index relation locks\nthrough its command lifecycle', 'gray'),
        ('Merger needs AccessExclusiveLock\nto clear the inactive top', 'amber'),
        ('Insertion path notices merge state\nchecks retained lock on other top', 'blue'),
        ('Inside COPY: periodically release lock\nrecord it in Lsm3ReleasedLocks', 'amber'),
        ('Brief yield, then reacquire\nlsm3_reacquire_locks()', 'blue'),
        ('Finish hook also reacquires locks\nclears Lsm3InsideCopy', 'green')],
        'Lsm3InsideCopy changes lock release timing; it does not make COPY a separate storage format.',
        'This explains the source workaround. Broad COPY / crash concurrency has not been qualified.'))

    add(m.pipeline('19_creation', 'How we built the experimental lookup index', [
        ('Pin Postgres Pro lsm3\napply fork API + expression fixes', 'gray'),
        ('Build / install on the lab host\npreload final module in every mode', 'blue'),
        ('Copy the 100M Merkle baseline\nreplace only the lookup AM', 'amber'),
        ('btbuild(): existing rows -> base\nwrapper creates two empty tops', 'blue'),
        ('Tops are hidden from separate plans\nparent AM searches all three', 'blue'),
        ('Equal VACUUM preparation\nverify roots, range rows and plans', 'green')],
        'AriaBC a7fc519d; upstream lsm3 3bac1042. Isolated and uncommitted, not a default rollout.',
        'Lookup key expressions and synchronous Merkle maintenance remain unchanged.'))

    add(m.panels('20_extension_fixes', 'Four lsm3 defects fixed in the experiment',
        ['DEFECT', 'CORRECTION'], [
            ('Top-size helper called twice\nblock count misused as relation OID',
             'Call relation-size helper once\nconvert its block count to bytes'),
            ('Worker registration loop used OR\nretry limit could fail to bound waiting',
             'Use AND\nbounded retries while worker is absent'),
            ('Concurrent insertion counter check\ncould miss a periodic boundary',
             'Reserve insertion position\nunder the existing spinlock'),
            ('DROP INDEX hook ignored IF EXISTS\nmissing index caused a failure',
             'Use missing_ok lookup\nskip an absent relation')],
        'Separate from compatibility adaptation; all remain in the isolated extension patch.'))

    add(m.panels('21_harness_fixes', 'Two setup failures, two harness fixes',
        ['BEFORE', 'AFTER'], [
            ('Rebuilt node table had stale relpages\npreflight reported missing / empty tree',
             'Check pg_relation_size\nphysical relation size, not stale statistics'),
            ('PostgreSQL socket moved to scratch\ntelemetry still looked in default /tmp',
             'psql -h 127.0.0.1 -p <port>\nTCP loopback on the database host')],
        'Measurement / preflight fixes; neither changes the LSM algorithm or row integrity.'))

    add(m.panels('22_d_results', 'YCSB D: the short-window insertion benefit',
        ['MERKLE + B-TREE', 'MERKLE + LSM'], [
            ('TPS: 10,293 / 10,793\nmidpoint 10,543',
             'TPS: 11,474 / 11,455\nmidpoint 11,465; +8.74%'),
            ('1,024 inserts in each case\nlookup buffer reads: 2,002 / 2,003',
             'Same 1,024 inserts\ntop reads: 7 / 7; base reads: 0 / 0'),
            ('Average WAL: 16.115 MB\ndevice writes: 30.57 MiB',
             'Average WAL: 8.544 MB\ndevice writes: 20.16 MiB'),
            ('Two trials, 16 workers, skew 0.99\n100M rows; cold-start preparation',
             'top0 56 KiB; top1 8 KiB\nNo consolidation in either timed run')],
        'Measured, not projected. Buffer reads are not physical device reads; MB is decimal, MiB is binary.',
        'Only 1.743-1.943 s per case; insufficient repeats; det was not rerun at this D point.'))

    add(m.lanes('23_a_results', 'Workload A barely writes the lookup index', [
        ('239,892 updates per Merkle mode\nacross its 24 grid cases',
         'Inferred lookup insertions\nB-tree: 70; LSM: 71'),
        ('Most value updates remain HOT\nuser heap fillfactor 90',
         'Little foreground lookup work to save\nMerkle node hashing / updates still run'),
        ('Three trials at each matched point\nLSM versus B-tree median TPS',
         '-2.61% to +2.23%; ranges overlap\nNo consistent gain; zero LSM merges')],
        'Lookup insertion totals are inferred from user-table insert and non-HOT update statistics.',
        '75 accepted cases incl. smoke; 1.5M request/row results; 50/50 Merkle verifications PASS.'))

    add(m.lanes('24_evidence_limits', 'What was tested, what remains open', [
        ('Small SQL / deterministic streams\nactual automatic LSM merges',
         '150K scalar rows: two merges\n143K det commands: one composite merge', 'green', 'green'),
        ('100M-row A and D performance\nshort measurement windows',
         'A: no consistent improvement\nD: +8.74%; neither includes consolidation', 'blue', 'blue'),
        ('Sustained consolidation / crash safety\nfull multi-replica recovery with LSM',
         'Not established by this experiment\nRequires separate targeted qualification', 'amber', 'amber')],
        'Working optional experiment; the evidence does not justify a universal speedup or default rollout.',
        'No new database, benchmark or crash experiment was run while preparing this guide.'))

    add(m.panels('25_review_corrections', 'Corrections to the proposed additions',
        ['PROPOSED DESCRIPTION', 'SOURCE-GROUNDED DESCRIPTION'], [
            ('Route hash is int8\n14-byte payload -> 24-byte tuple',
             'Route hash is bytea\n2 + 9 + 1 + 4 payload bytes -> 24-byte tuple'),
            ('A 1 MiB top holds exactly 33,500 rows\novershoot is bounded near 2 MiB',
             'Capacity depends on packing / overhead\nsampled checking imposes no such hard bound'),
            ('Truncation logs an init-fork rebuild\nnode-table change arrived in a3312766',
             'Empty metapage goes to MAIN_FORKNUM\nnode-table / root index added in a1bb5bda')],
        'The legacy report is a historical footprint; it is not a specification of the LSM baseline.',
        'Root-index usefulness is source-supported; no near-instant epoch-verification latency was measured.'))
    return out


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--out', type=Path, required=True)
    args = parser.parse_args()
    args.out.mkdir(parents=True, exist_ok=True)
    for figure in figures():
        figure.render(args.out)
    if m.ERRORS:
        raise RuntimeError('\n'.join(m.ERRORS))
    (args.out / 'figures.json').write_text(json.dumps(m.MANIFEST, indent=2) + '\n')
    print(f'PASS: {len(m.MANIFEST)} new figures; text fits; connectors and bounds checked.')


if __name__ == '__main__':
    main()
