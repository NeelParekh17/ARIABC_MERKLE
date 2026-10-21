# Why merkle_lsm helps D, but not A/B: Phase 1c

**D issues ~120,000 INSERTs: every one inserts a new tuple/TID into the large, hash-ordered Merkle lookup index. A/B/F update only unindexed payload fields; their lookup key is unchanged, and the saved A/B LSM footprint shows almost no lookup-index insertion (consistent with predominantly HOT updates).**  
**The LSM replaces that lookup index, not the Merkle node store. Payload updates still hash the full row and maintain leaf/ancestor nodes, regardless of HOT; that work is common to LSM and B-tree.**  
**A is not cost-free: at skew 0 Merkle adds ~235 WAL B/statement and ~2.6 GiB device writes over DET; its similar TPS is one I/O-sensitive trial, not evidence that maintenance is disabled.**  
**D’s +35–66% timed TPS gain includes deferred merge work: after drain/checkpoint, LSM and B-tree write volumes nearly match and LSM total WAL is slightly higher.**  
**The operation mixes are correct YCSB-style mixes; key-preserving UPDATE is expected. Exact HOT fractions and D split counts were not saved, so a small post-matrix confirmation is proposed below.**

Evidence cutoff: saved artifacts collected at **2026-10-21 00:32:43 UTC**; sampler summarized at **2026-10-21 00:30:07 UTC**. D/C/B/A: 64 accepted cases, each 2,400,000 statements, workers=16, SERIALIZABLE, shared_buffers=32MB, fillfactor=90, one trial. F is running; only its already-generated SQL is analyzed here. No server SQL, restart, disk test, baseline read/copy, controller edit, or harness edit was performed. Source is the isolated checkout at HEAD `5f2a078e`. Workload/telemetry analysis ran at nice 19 on .111; the existing sampler JSONL was read once on .247 and reduced to compact statistics without any new query.

## 1. What each workload actually sends

The harness imports generate_workload_statements and passes num_tx, 100M keys, seed+100×skew and LargeZipfGenerator: [scripts/distributed/run_oom_100m_benchmark.py:425](../../../../../scripts/distributed/run_oom_100m_benchmark.py:425). SQL forms and mixes: [scripts/generate_ycsb_workloads.py:80](../../../../../scripts/generate_ycsb_workloads.py:80); [scripts/generate_ycsb_workloads.py:115](../../../../../scripts/generate_ycsb_workloads.py:115); selection/dispatch: [scripts/generate_ycsb_workloads.py:269](../../../../../scripts/generate_ycsb_workloads.py:269). Counts below come from a complete, sequential scan of **all 20 real files** in the plain campaigns, not from regenerating SQL. Each SHA256 matches its campaign manifest; every accepted mode’s SHA matches this same file. Every file has 2,400,000 single-line SQL statements. No DELETE, scan, other statement, malformed update, key assignment, or out-of-range A/B/F update was found.

| Workload/skew | SELECT | UPDATE | INSERT | RMW CTE | Actual write line / count evidence |
| --- | --- | --- | --- | --- | --- |
| A/0.0 | 1,199,976 | 1,200,024 | 0 | 0 | 1 / [data](phase1c_evidence/SQL_COUNTS.tsv:2) |
| A/0.5 | 1,199,772 | 1,200,228 | 0 | 0 | 2 / [data](phase1c_evidence/SQL_COUNTS.tsv:3) |
| A/0.99 | 1,199,462 | 1,200,538 | 0 | 0 | 1 / [data](phase1c_evidence/SQL_COUNTS.tsv:4) |
| A/1.2 | 1,199,985 | 1,200,015 | 0 | 0 | 4 / [data](phase1c_evidence/SQL_COUNTS.tsv:5) |
| B/0.0 | 2,279,919 | 120,081 | 0 | 0 | 17 / [data](phase1c_evidence/SQL_COUNTS.tsv:6) |
| B/0.5 | 2,280,369 | 119,631 | 0 | 0 | 28 / [data](phase1c_evidence/SQL_COUNTS.tsv:7) |
| B/0.99 | 2,280,481 | 119,519 | 0 | 0 | 38 / [data](phase1c_evidence/SQL_COUNTS.tsv:8) |
| B/1.2 | 2,279,809 | 120,191 | 0 | 0 | 4 / [data](phase1c_evidence/SQL_COUNTS.tsv:9) |
| C/0.0 | 2,400,000 | 0 | 0 | 0 | — / [data](phase1c_evidence/SQL_COUNTS.tsv:10) |
| C/0.5 | 2,400,000 | 0 | 0 | 0 | — / [data](phase1c_evidence/SQL_COUNTS.tsv:11) |
| C/0.99 | 2,400,000 | 0 | 0 | 0 | — / [data](phase1c_evidence/SQL_COUNTS.tsv:12) |
| C/1.2 | 2,400,000 | 0 | 0 | 0 | — / [data](phase1c_evidence/SQL_COUNTS.tsv:13) |
| D/0.0 | 2,279,469 | 0 | 120,531 | 0 | 17 / [data](phase1c_evidence/SQL_COUNTS.tsv:14) |
| D/0.5 | 2,279,746 | 0 | 120,254 | 0 | 28 / [data](phase1c_evidence/SQL_COUNTS.tsv:15) |
| D/0.99 | 2,280,350 | 0 | 119,650 | 0 | 38 / [data](phase1c_evidence/SQL_COUNTS.tsv:16) |
| D/1.2 | 2,279,885 | 0 | 120,115 | 0 | 4 / [data](phase1c_evidence/SQL_COUNTS.tsv:17) |
| F/0.0 | 1,199,976 | 0 | 0 | 1,200,024 | 1 / [data](phase1c_evidence/SQL_COUNTS.tsv:18) |
| F/0.5 | 1,199,772 | 0 | 0 | 1,200,228 | 2 / [data](phase1c_evidence/SQL_COUNTS.tsv:19) |
| F/0.99 | 1,199,462 | 0 | 0 | 1,200,538 | 1 / [data](phase1c_evidence/SQL_COUNTS.tsv:20) |
| F/1.2 | 1,199,985 | 0 | 0 | 1,200,015 | 4 / [data](phase1c_evidence/SQL_COUNTS.tsv:21) |

Original SQL locations are recorded in SQL_COUNTS.tsv and sql_counts.json: `.111:/home/neel/claude_ctl/ycsb_matrix_20261020/results/matrix/<workload>_plain/<run>/workloads/ycsb_<workload>_skew_<skew>_2400000.sql:<write line above>`. Full SHA256s, exact first examples/line numbers, all column-set counts, and key bounds are retained in sql_counts.json. A and F have identical operation choices/key/value RNG consumption at each skew; F replaces the UPDATE form with an RMW CTE, so their write counts match exactly.

Every A/B UPDATE and every F RMW sets **FIELD1 through FIELD10**, each to a new 20-character string; **YCSB_KEY is never in SET**. All update keys stay in 1..100,000,000. The lookup index expression and third key depend only on ycsb_key, so none of its values changes. This is distinct from the Merkle contribution hash, which covers the full row. Source: [scripts/generate_ycsb_workloads.py:84](../../../../../scripts/generate_ycsb_workloads.py:84); full-row hash: [src/backend/access/merkle/merkleutil.c:605](../../../../../src/backend/access/merkle/merkleutil.c:605).

The exact F SQL shape (one request/transaction) is:
```sql
WITH ycsb_read AS MATERIALIZED
  (SELECT * FROM usertable WHERE YCSB_KEY=<k>)
UPDATE usertable SET FIELD1=<20 chars>, ..., FIELD10=<20 chars>
FROM ycsb_read
WHERE usertable.YCSB_KEY=ycsb_read.YCSB_KEY RETURNING usertable.*;
```
The SET values are generated constants, not a key change or arithmetic increment. Source: [scripts/generate_ycsb_workloads.py:89](../../../../../scripts/generate_ycsb_workloads.py:89). F is a logical read+update but counts as one submitted statement; it does not insert a new user key.

D really inserts **new, unique, monotonically increasing integer keys**, not UPDATEs disguised as inserts: every file’s first inserted key is 100,000,001 and all subsequent inserted keys increase by exactly one. No reuse or collision was found. Its read-latest generator uses max acknowledged key minus a Zipf offset, with a **1,500-operation static horizon**, not runtime acknowledgements. This is an approximation designed to keep reads behind pipelined inserts; not a proof that any future concurrency schedule must complete them within 1,500 operations. Source: [scripts/generate_ycsb_workloads.py:280](../../../../../scripts/generate_ycsb_workloads.py:280) and [scripts/generate_ycsb_workloads.py:317](../../../../../scripts/generate_ycsb_workloads.py:317).

| D skew | New inserts | New key interval | Nonsequential inserts | SELECTs of new keys >100M |
| --- | --- | --- | --- | --- |
| 0.0 | 120,531 | 100,000,001–100,120,531 | 0 | 1,317 |
| 0.5 | 120,254 | 100,000,001–100,120,254 | 0 | 52,531 |
| 0.99 | 119,650 | 100,000,001–100,119,650 | 0 | 1,299,927 |
| 1.2 | 120,115 | 100,000,001–100,120,115 | 0 | 2,079,729 |

These sequential integers are append-friendly for the primary-key B-tree, while the lookup index is ordered by `(partition(hash(k)), hash(k), k)`: hashing distributes newly inserted keys across the existing ~3.15-GB lookup structure. The LSM’s aminsert goes into the active L0 B-tree, not directly into the large base: [contrib/merkle_lsm/merkle_lsm.c:395](../../../../../contrib/merkle_lsm/merkle_lsm.c:395). The expression index definitions in every saved Merkle setup confirm the same three keys and differ only by AM/options.

## 2. Separate the three physical structures

| Structure | Stored key/data | What A/B/F changes | Does swapping lookup AM change it? | Source |
| --- | --- | --- | --- | --- |
| usertable_pkey1 | B-tree(ycsb_key) | HOT: no new entry; non-HOT: new TID entry even if key unchanged | No | [src/backend/access/heap/heapam_handler.c:334](../../../../../src/backend/access/heap/heapam_handler.c:334) |
| usertable_merkle_lookup_idx | (merkle_partition_for_hash(merkle_key_hash(ycsb_key),200), merkle_key_hash(ycsb_key), ycsb_key) | HOT: no insertion; INSERT/non-HOT UPDATE: new index tuple/TID | Yes: B-tree base versus merkle_lsm active top | Saved setup.indexes; [contrib/merkle_lsm/merkle_lsm.c:395](../../../../../contrib/merkle_lsm/merkle_lsm.c:395) |
| usertable_merkle_idx / internal node heap | Full-row aggregate, keyed by node identity | Full-row old/new hash delta and leaf/ancestor node updates, also on HOT | No; internal node indexes remain ordinary B-trees | [src/backend/bcdb/shm_transaction.c:3314](../../../../../src/backend/bcdb/shm_transaction.c:3314); [src/backend/access/merkle/merkleutil.c:1642](../../../../../src/backend/access/merkle/merkleutil.c:1642) |

The Merkle integrity AM has no tuple/bitmap scan callback ([src/backend/access/merkle/merkle.c:390](../../../../../src/backend/access/merkle/merkle.c:390)). The application’s `WHERE YCSB_KEY=k` queries have a direct primary-key access path; they do not ask the hash lookup index for a partition/hash interval. The source has no A/B payload-update path that writes the usertable lookup index as part of node maintenance. A non-HOT user heap update still inserts ordinary index entries with a new TID: HEAP_INDEX_NO_MERKLE skips only the integrity AM, **not** merkle_lsm ([src/backend/access/heap/heapam.c:1866](../../../../../src/backend/access/heap/heapam.c:1866)). No saved per-query EXPLAIN was collected, so this describes available predicates and source paths, not a newly observed execution plan.

PostgreSQL 13’s HOT decision requires the new tuple on the same heap page and no changed indexed attributes: [src/backend/access/heap/heapam.c:3744](../../../../../src/backend/access/heap/heapam.c:3744). Indexed expression dependencies are included via pull_varattnos: [src/backend/utils/cache/relcache.c:4900](../../../../../src/backend/utils/cache/relcache.c:4900). Heap-only tuples set update_indexes=false ([src/backend/access/heap/heapam_handler.c:334](../../../../../src/backend/access/heap/heapam_handler.c:334)). BCDB applies non-Merkle index entries only when this flag is true ([src/backend/bcdb/shm_transaction.c:3311](../../../../../src/backend/bcdb/shm_transaction.c:3311)). Thus an unchanged lookup key is **HOT-eligible**, not automatically HOT: page space/pruning and visibility horizons still matter. An ordinary non-HOT update creates fresh entries in all applicable indexes even when their key values are unchanged; old index versions are reclaimed later, not immediately deleted by UPDATE.

## 3. Saved LSM footprint: the decisive workload difference

The baseline has two L0 and two L1 component B-trees, each initially 8,192 bytes (empty metadata page); the base is 3,154,264,064 bytes. merkle_lsm_stats reports **physical relation lengths**, not entry counts ([contrib/merkle_lsm/merkle_lsm.c:692](../../../../../contrib/merkle_lsm/merkle_lsm.c:692)). This table gives maximum bytes for each component during the measurement marker, in KiB; maxima for separate components need not occur in the same sample. Merges are per-case maximum lifetime counters through drain/verify, never sums across restarts.

| Case | Max L0 slots KiB | Max L1 slots KiB | Measurement stats samples | Merges in measurement [L0,L1] | Merges after drain [L0,L1] | Final component KiB | Drain s |
| --- | --- | --- | --- | --- | --- | --- | --- |
| D/0 [data](phase1c_evidence/LSM_SAMPLER.tsv:2) | 1016/1024 | 4920/8 | 276 | [5, 0] | [6, 2] | 8/8/8/8 | 31.044 |
| D/0.5 [data](phase1c_evidence/LSM_SAMPLER.tsv:3) | 1024/1024 | 4912/8 | 271 | [5, 0] | [6, 2] | 8/8/8/8 | 9.455 |
| D/0.99 [data](phase1c_evidence/LSM_SAMPLER.tsv:4) | 1032/1024 | 4808/8 | 204 | [5, 0] | [6, 2] | 8/8/8/8 | 8.554 |
| D/1.2 [data](phase1c_evidence/LSM_SAMPLER.tsv:5) | 1024/1016 | 4920/8 | 174 | [5, 0] | [6, 2] | 8/8/8/8 | 8.651 |
| C/0 [data](phase1c_evidence/LSM_SAMPLER.tsv:6) | 8/8 | 8/8 | 213 | [0, 0] | [0, 0] | 8/8/8/8 | 0.337 |
| C/0.5 [data](phase1c_evidence/LSM_SAMPLER.tsv:7) | 8/8 | 8/8 | 207 | [0, 0] | [0, 0] | 8/8/8/8 | 0.335 |
| C/0.99 [data](phase1c_evidence/LSM_SAMPLER.tsv:8) | 8/8 | 8/8 | 156 | [0, 0] | [0, 0] | 8/8/8/8 | 0.335 |
| C/1.2 [data](phase1c_evidence/LSM_SAMPLER.tsv:9) | 8/8 | 8/8 | 134 | [0, 0] | [0, 0] | 8/8/8/8 | 0.337 |
| B/0 [data](phase1c_evidence/LSM_SAMPLER.tsv:10) | 8/8 | 8/8 | 383 | [0, 0] | [0, 0] | 8/8/8/8 | 0.330 |
| B/0.5 [data](phase1c_evidence/LSM_SAMPLER.tsv:11) | 8/8 | 8/8 | 376 | [0, 0] | [0, 0] | 8/8/8/8 | 0.441 |
| B/0.99 [data](phase1c_evidence/LSM_SAMPLER.tsv:12) | 16/8 | 8/8 | 216 | [0, 0] | [1, 1] | 8/8/8/8 | 1.047 |
| B/1.2 [data](phase1c_evidence/LSM_SAMPLER.tsv:13) | 16/8 | 8/8 | 190 | [0, 0] | [1, 1] | 8/8/8/8 | 1.036 |
| A/0 [data](phase1c_evidence/LSM_SAMPLER.tsv:14) | 8/8 | 8/8 | 970 | [0, 0] | [0, 0] | 8/8/8/8 | 0.355 |
| A/0.5 [data](phase1c_evidence/LSM_SAMPLER.tsv:15) | 8/8 | 8/8 | 952 | [0, 0] | [0, 0] | 8/8/8/8 | 0.346 |
| A/0.99 [data](phase1c_evidence/LSM_SAMPLER.tsv:16) | 16/8 | 8/8 | 587 | [0, 0] | [1, 1] | 8/8/8/8 | 3.372 |
| A/1.2 [data](phase1c_evidence/LSM_SAMPLER.tsv:17) | 16/8 | 8/8 | 478 | [0, 0] | [1, 1] | 8/8/8/8 | 1.037 |

A/B at skew 0 or .5 have **zero nonempty component samples and zero merges even after explicit flush**. At .99/1.2, only the first L0 slot reaches 16 KiB (one leaf page plus metadata), while L1 remains empty throughout measurement; [1,1] is a tiny forced post-run drain, not sustained background compaction. In D, each case reaches ~1,024 KiB L0 and ~4,800–4,920 KiB L1, with five L0 merges during the measurement marker and six L0/two L1 after drain. C is empty throughout. These are observations of the targeted index, not an inference from equal TPS.

This strongly supports **predominantly HOT payload updates / negligible lookup-index insertion** in these A/B runs. Exact n_tup_hot_upd/n_tup_upd fractions were not saved, and 1-Hz physical-length samples are not tuple counts; one small page cannot establish how many versions were inserted/reclaimed. Therefore this report does not claim a measured 100% HOT rate. There are no saved per-table update/HOT counters, raw per-case WAL segments or pg_waldump record histograms, nor post-run heap/primary/lookup size snapshots in these case directories. Current .247 WAL belongs to later/restored cases and was not read. The finite confirmation below is required to turn the HOT interpretation into a counter-based measurement. F has the same eligibility by source, but no F LSM case has run at the evidence cutoff, so its L0/HOT behavior remains unmeasured.

## 4. Merkle node work and split retrieval

For DET/Merkle updates the real route is nodeModifyTable’s deferred write → apply_optim_update. The latter hashes the old row, performs the heap update, conditionally maintains ordinary indexes, hashes the actual new heap row, and compares old/new routing digests. With the same key it stages MERKLE_DELTA_UPDATE_SAME_LEAF(new−old): [src/backend/executor/nodeModifyTable.c:1925](../../../../../src/backend/executor/nodeModifyTable.c:1925); [src/backend/bcdb/shm_transaction.c:3195](../../../../../src/backend/bcdb/shm_transaction.c:3195); [src/backend/bcdb/shm_transaction.c:3311](../../../../../src/backend/bcdb/shm_transaction.c:3311); [src/backend/bcdb/shm_transaction.c:3382](../../../../../src/backend/bcdb/shm_transaction.c:3382). This explicitly executes even when HOT skips ordinary index inserts. It is not disabled maintenance.

That event changes the aggregate hash but has **count_delta=0**; only INSERT/DELETE increments/decrements counts ([src/backend/access/merkle/merkledelta.c:210](../../../../../src/backend/access/merkle/merkledelta.c:210)). Pre-commit/coalesced apply updates leaf and ancestor rows in `ariabc_internal.merkle_node_usertable`, using cached/direct TIDs or that node table’s primary-key B-tree, not usertable_merkle_lookup_idx ([src/backend/access/merkle/merkleapply.c:1528](../../../../../src/backend/access/merkle/merkleapply.c:1528); [src/backend/access/merkle/merkleapply.c:1952](../../../../../src/backend/access/merkle/merkleapply.c:1952); [src/backend/access/merkle/merkleapply.c:2351](../../../../../src/backend/access/merkle/merkleapply.c:2351)). Node updates may themselves be HOT at their fillfactor 80; otherwise their own B-trees are updated ([src/backend/access/merkle/merkleutil.c:1642](../../../../../src/backend/access/merkle/merkleutil.c:1642); [src/backend/access/merkle/merkleapply.c:2167](../../../../../src/backend/access/merkle/merkleapply.c:2167)). Switching the usertable lookup AM changes none of this.

Coalesced same-leaf updates queue a split/merge only for nonzero count_delta; splitting additionally requires count>1024 and a valid depth ([src/backend/access/merkle/merkleapply.c:2470](../../../../../src/backend/access/merkle/merkleapply.c:2470); [src/backend/access/merkle/merkleapply.c:2285](../../../../../src/backend/access/merkle/merkleapply.c:2285)). Only a real do_split issues the SPI partition/hash-range query over usertable; it hashes full rows, so the source query is not purely index-only despite a “covering” comment ([src/backend/access/merkle/merkleapply.c:655](../../../../../src/backend/access/merkle/merkleapply.c:655)). Routine A/B/F payload updates need no such row retrieval. For a key change, BCDB stages delete+insert into potentially different routes, which can change leaf counts and trigger structural work.

| Workload | Logical leaf count delta | Lookup index insertion | Split row retrieval during this mix |
| --- | --- | --- | --- |
| C | 0 | None | None from user DML |
| A/B/F | 0 per unchanged-key update | Only non-HOT physical updates; A/B footprint negligible | No count-triggered retrieval from these same-key events |
| D | +1 per inserted key | Every INSERT, ~120K/case | Only when a count-changing leaf crosses threshold; actual calls not collected |
| Key-changing/non-HOT diagnostic | Key change: −1/+1 across routes; unchanged-key non-HOT: 0 | Every successful non-HOT update | Only route/count threshold crossings, not every update |

**Important correction to evidence interpretation:** all saved `result.merkle_stats` are exactly `setup.merkle_stats`; the harness copies them at [scripts/distributed/run_oom_100m_benchmark.py:1372](../../../../../scripts/distributed/run_oom_100m_benchmark.py:1372), and obtains them before the run at [scripts/distributed/run_oom_100m_benchmark.py:1043](../../../../../scripts/distributed/run_oom_100m_benchmark.py:1043). The repeated 211,400 total / 204,800 leaf nodes, READY state and lag=0 are **baseline** observations, not post-run evidence. This also corrects the broader READY/lag wording in Phase 1b. Post-run merkle_verify.txt=`t` is real and independently saved. We cannot use unchanged result node counts to claim zero D splits. Baseline mean occupancy is 100M/204,800=488.281 rows/leaf versus threshold 1,024; ~120K new rows adds only ~0.59 rows/leaf on average, but without leaf occupancy/post-run split counters that is not proof of an exact split count. D’s proven distinction is the direct INSERT stream into the lookup index, not demonstrated faster split scans.

## 5. Exact performance, WAL and device-write comparisons

TPS is the gateway interval. Device writes are partition-wide (heap, indexes, WAL, logs and other host traffic), not an index-only counter. WAL window includes the run and collector/log-validation settling; WAL full additionally includes explicit LSM drain and checkpoint. Reset/settle/verification are excluded. Every table entry links to a line-addressable extract preserving full precision and its original `.111` case path; original keys are row.tps, row.device_write_mib, row.wal_bytes_per_statement, io.continuous_write_mib and wal_post_checkpoint.lsn−wal.before.lsn. All 64 saved cases have terminal count 2.4M, zero permanent/divergence/exhausted failures, and all 32 Merkle cases verify `t`. Trials are unreplicated, so “within noise” means no consistent signal here, not a statistically measured noise interval.

| Workload/skew | DET TPS | LSM TPS | B-tree TPS | LSM vs B-tree | B-tree vs DET | Case evidence |
| --- | --- | --- | --- | --- | --- | --- |
| D/0 | 11720.4 | 11432.8 | 8464.6 | +35.07% | -27.78% | [DET](phase1c_evidence/FACTS.tsv:3) / [LSM](phase1c_evidence/FACTS.tsv:4) / [BTREE](phase1c_evidence/FACTS.tsv:2) |
| D/0.5 | 12316.0 | 11897.6 | 8728.9 | +36.30% | -29.13% | [DET](phase1c_evidence/FACTS.tsv:7) / [LSM](phase1c_evidence/FACTS.tsv:8) / [BTREE](phase1c_evidence/FACTS.tsv:6) |
| D/0.99 | 18530.1 | 17751.5 | 11731.3 | +51.32% | -36.69% | [DET](phase1c_evidence/FACTS.tsv:11) / [LSM](phase1c_evidence/FACTS.tsv:12) / [BTREE](phase1c_evidence/FACTS.tsv:10) |
| D/1.2 | 23640.9 | 22299.9 | 13423.1 | +66.13% | -43.22% | [DET](phase1c_evidence/FACTS.tsv:15) / [LSM](phase1c_evidence/FACTS.tsv:16) / [BTREE](phase1c_evidence/FACTS.tsv:14) |
| C/0 | 16370.9 | 16537.0 | 16428.9 | +0.66% | +0.35% | [DET](phase1c_evidence/FACTS.tsv:19) / [LSM](phase1c_evidence/FACTS.tsv:20) / [BTREE](phase1c_evidence/FACTS.tsv:18) |
| C/0.5 | 17402.8 | 17326.5 | 17127.4 | +1.16% | -1.58% | [DET](phase1c_evidence/FACTS.tsv:23) / [LSM](phase1c_evidence/FACTS.tsv:24) / [BTREE](phase1c_evidence/FACTS.tsv:22) |
| C/0.99 | 27457.4 | 27217.4 | 27210.9 | +0.02% | -0.90% | [DET](phase1c_evidence/FACTS.tsv:27) / [LSM](phase1c_evidence/FACTS.tsv:28) / [BTREE](phase1c_evidence/FACTS.tsv:26) |
| C/1.2 | 35305.5 | 35560.8 | 35558.2 | +0.01% | +0.72% | [DET](phase1c_evidence/FACTS.tsv:31) / [LSM](phase1c_evidence/FACTS.tsv:32) / [BTREE](phase1c_evidence/FACTS.tsv:30) |
| B/0 | 8670.1 | 7519.6 | 8519.0 | -11.73% | -1.74% | [DET](phase1c_evidence/FACTS.tsv:35) / [LSM](phase1c_evidence/FACTS.tsv:36) / [BTREE](phase1c_evidence/FACTS.tsv:34) |
| B/0.5 | 9980.0 | 7786.5 | 7782.1 | +0.06% | -22.02% | [DET](phase1c_evidence/FACTS.tsv:39) / [LSM](phase1c_evidence/FACTS.tsv:40) / [BTREE](phase1c_evidence/FACTS.tsv:38) |
| B/0.99 | 14919.2 | 15987.0 | 14789.7 | +8.10% | -0.87% | [DET](phase1c_evidence/FACTS.tsv:43) / [LSM](phase1c_evidence/FACTS.tsv:44) / [BTREE](phase1c_evidence/FACTS.tsv:42) |
| B/1.2 | 21907.8 | 19431.3 | 20774.4 | -6.46% | -5.17% | [DET](phase1c_evidence/FACTS.tsv:47) / [LSM](phase1c_evidence/FACTS.tsv:48) / [BTREE](phase1c_evidence/FACTS.tsv:46) |
| A/0 | 2829.1 | 2579.5 | 2801.6 | -7.93% | -0.97% | [DET](phase1c_evidence/FACTS.tsv:51) / [LSM](phase1c_evidence/FACTS.tsv:52) / [BTREE](phase1c_evidence/FACTS.tsv:50) |
| A/0.5 | 2772.1 | 2639.0 | 2612.8 | +1.00% | -5.75% | [DET](phase1c_evidence/FACTS.tsv:55) / [LSM](phase1c_evidence/FACTS.tsv:56) / [BTREE](phase1c_evidence/FACTS.tsv:54) |
| A/0.99 | 4882.6 | 4434.4 | 4765.1 | -6.94% | -2.41% | [DET](phase1c_evidence/FACTS.tsv:59) / [LSM](phase1c_evidence/FACTS.tsv:60) / [BTREE](phase1c_evidence/FACTS.tsv:58) |
| A/1.2 | 5916.6 | 5546.5 | 5520.7 | +0.47% | -6.69% | [DET](phase1c_evidence/FACTS.tsv:63) / [LSM](phase1c_evidence/FACTS.tsv:64) / [BTREE](phase1c_evidence/FACTS.tsv:62) |

A’s B-tree Merkle cost is not uniformly zero: it is −0.97%, −5.75%, −2.41%, −6.69% versus DET as skew rises. B/.5 shows −22.02% versus DET even though its lookup index is idle; this is evidence that replacing the lookup AM does not remove other Merkle overhead. No per-Merkle-stage timing was enabled in the saved profiler, so exact CPU/lock/I/O attribution percentages cannot be recovered.

D/.99/BTREE is the original 11,731-TPS anchor outlier retained in the matrix. The separate Phase-1 diagnostic confirmation reached 13,176 TPS (`.111:evidence/anchor_confirmation.json`); it is not a replacement trial here. D’s consistent direction across the other skews and its directly observed lookup-index insertion/merge activity remain the mechanism evidence, rather than treating these four skew settings as repeated trials.

### A: same-combo time and write costs

| Skew/mode | Gateway s | Device write MiB | WAL window/full B per stmt | Read/write await ms | I/O full-pressure % | Peak dirty MiB | Retries |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 0/DET | 848.320 | 27186.4 | 3297.568/3297.568 | 1.596/4.491 | 39.19 | 506.9 | 0 |
| 0/LSM | 930.414 | 30034.3 | 3532.616/3532.617 | 1.678/4.498 | 39.31 | 642.6 | 0 |
| 0/BTREE | 856.657 | 29866.8 | 3532.619/3532.620 | 1.519/4.046 | 37.47 | 575.2 | 0 |
| 0.5/DET | 865.770 | 25834.8 | 3000.704/3000.704 | 1.654/4.221 | 37.43 | 519.6 | 0 |
| 0.5/LSM | 909.451 | 28401.4 | 3235.761/3235.761 | 1.684/4.211 | 37.13 | 577.8 | 0 |
| 0.5/BTREE | 918.550 | 28736.1 | 3235.760/3235.760 | 1.665/4.369 | 37.01 | 590.1 | 0 |
| 0.99/DET | 491.543 | 10846.6 | 1101.027/1101.027 | 1.077/2.449 | 19.46 | 362.5 | 0 |
| 0.99/LSM | 541.229 | 12705.6 | 1333.474/1333.739 | 1.118/2.486 | 19.85 | 410.0 | 0 |
| 0.99/BTREE | 503.658 | 12883.0 | 1333.698/1333.698 | 1.051/2.550 | 18.87 | 395.9 | 0 |
| 1.2/DET | 405.637 | 6426.5 | 367.985/367.985 | 0.665/0.569 | 7.34 | 39.7 | 0 |
| 1.2/LSM | 432.702 | 7846.3 | 588.257/588.536 | 0.640/0.986 | 7.94 | 79.0 | 0 |
| 1.2/BTREE | 434.729 | 7909.6 | 591.381/591.381 | 0.660/0.844 | 7.67 | 92.2 | 0 |

### B: same-combo time and write costs

| Skew/mode | Gateway s | Device write MiB | WAL window/full B per stmt | Read/write await ms | I/O full-pressure % | Peak dirty MiB | Retries |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 0/DET | 276.813 | 3629.5 | 381.172/381.172 | 0.814/1.577 | 40.80 | 159.2 | 0 |
| 0/LSM | 319.166 | 4344.7 | 412.058/412.058 | 0.912/2.445 | 42.95 | 183.1 | 0 |
| 0/BTREE | 281.722 | 4216.8 | 412.058/412.058 | 0.790/1.691 | 41.53 | 180.8 | 0 |
| 0.5/DET | 240.482 | 3629.4 | 370.422/370.422 | 0.710/1.204 | 37.93 | 164.3 | 0 |
| 0.5/LSM | 308.225 | 4182.4 | 401.215/401.215 | 0.886/2.070 | 40.15 | 198.7 | 0 |
| 0.5/BTREE | 308.402 | 4225.1 | 401.215/401.216 | 0.879/2.168 | 40.31 | 191.8 | 0 |
| 0.99/DET | 160.867 | 2544.8 | 156.472/156.472 | 0.499/1.080 | 15.14 | 128.9 | 0 |
| 0.99/LSM | 150.122 | 2801.6 | 187.402/187.429 | 0.430/0.617 | 13.52 | 164.6 | 0 |
| 0.99/BTREE | 162.275 | 2760.5 | 187.379/187.379 | 0.473/0.774 | 14.65 | 164.8 | 0 |
| 1.2/DET | 109.550 | 1736.6 | 50.616/50.616 | 0.309/0.330 | 5.19 | 83.9 | 0 |
| 1.2/LSM | 123.512 | 2068.0 | 81.234/81.288 | 0.437/0.356 | 6.05 | 87.3 | 0 |
| 1.2/BTREE | 115.527 | 2060.9 | 81.270/81.270 | 0.306/0.325 | 4.96 | 87.2 | 0 |

At A/0, DET already spends 848.320 s, writes 27,186.4 MiB and logs 3,297.568 B/statement. B-tree Merkle spends 856.657 s, writes 29,866.8 MiB (+9.86%), and logs 3,532.619 B/statement (+7.13%). Its read await is lower (1.519 versus 1.596 ms), so the wall-time near-tie does not establish zero extra work; differing I/O service and overlapping worker work can hide a modest increment. LSM logs the same 3,532.616 B/statement with empty lookup components, but its higher read await (1.678 ms) coincides with the slower 930.414-s result. This is consistent with trial-to-trial service variability; a causal decomposition or an intrinsic LSM penalty is not established by one trial.

The common A cost is updating ~1.2M heap tuples on a 28.25-GB heap with only 32MB PostgreSQL buffers and a larger unbounded OS cache: random heap fetches/dirty-page eviction, heap WAL plus possible full-page images, and common Merkle node/hash maintenance. Merkle adds about 220–235 WAL B/statement over DET in A, versus about 31 in B; those bytes do not disappear just because the lookup index is idle. full_page_writes is on in saved settings, and the heap WAL code permits full-page backups even for HOT ([src/backend/access/heap/heapam.c:7478](../../../../../src/backend/access/heap/heapam.c:7478)). The exact full-page-image share was **not decoded**, so aggregate WAL cannot prove its percentage. Saved telemetry shows I/O pressure and dirty pages as tabulated, not swap-out: all A/B cases have zero measured swap-out. Low-skew A host CPU busy is ~7–8%, but this aggregate cannot rule out a single-core bottleneck.

SSI retries cannot explain LSM-versus-B-tree here: **all DET/LSM/B-tree cases have zero reported retry attempts**. Plain PG does pay hotspot retry costs (B/.99=1,312; B/1.2=23,079; A/.99=64,308; A/1.2=938,038). All exhausted counters remain zero. These PG differences must not be mixed into a comparison between the two deterministic Merkle AMs. Internal Merkle-node apply temporarily suppresses SSI checks for its commutative aggregate while restoring the outer isolation contract; this is shared by both AMs ([src/backend/access/merkle/merkleapply.c:2706](../../../../../src/backend/access/merkle/merkleapply.c:2706)).

### D: timed gain and deferred costs

| Skew | Timed writes MiB LSM/BT | Through-CP writes MiB LSM/BT | Window WAL B/stmt LSM/BT | Full WAL B/stmt LSM/BT | LSM drain s | Run+drain+CP effective stmt/s LSM/BT | Inclusive gain |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 0 | 2281.4/3990.4 | 3945.6/4086.8 | 61.733/375.340 | 384.392/375.340 | 31.044 | 9576.9/8455.5 | +13.26% |
| 0.5 | 2408.8/4057.8 | 4051.5/4159.4 | 61.607/374.604 | 383.635/374.604 | 9.455 | 11325.0/8719.1 | +29.89% |
| 0.99 | 2148.8/3669.3 | 3763.3/3791.4 | 61.308/373.039 | 382.028/373.039 | 8.554 | 16632.2/11713.9 | +41.99% |
| 1.2 | 2023.2/3569.4 | 3637.5/3675.3 | 61.538/374.223 | 383.246/374.223 | 8.651 | 20556.2/13400.0 | +53.40% |

The last two columns are only the stated sum of recorded timers: the omitted copied-log/collector gap is long enough for a background merger to continue. For a stricter elapsed bracket, the saved same-host timestamps from wal.before.time through wal_post_checkpoint.time include that gap, telemetry start/stop, collector settling, drain and checkpoint:

| D skew | Before-run→after-CP elapsed s LSM/BT | Bracket stmt/s LSM/BT | Bracket gain |
| --- | --- | --- | --- |
| 0 | 318.917808/353.887591 | 7525.4/6781.8 | +10.97% |
| 0.5 | 282.727410/342.903290 | 8488.7/6999.1 | +21.28% |
| 0.99 | 214.333163/285.643092 | 11197.5/8402.1 | +33.27% |
| 1.2 | 184.422031/247.669725 | 13013.6/9690.3 | +34.30% |

This stricter bracket still favors LSM by 11–34%, rather than the gateway-only 35–66%. It still excludes reset/settle and post-checkpoint restart/Merkle verification, so it is not full campaign throughput. No extra timing run was made.

The measurement marker sees five L0 merges but no completed L1 merge; L1 grows only to ~4.9 MiB, below its 16-MiB target. The explicit flush then forces L1→base. This is exactly the size/flush logic in [contrib/merkle_lsm/merge.c:290](../../../../../contrib/merkle_lsm/merge.c:290) and [contrib/merkle_lsm/merge.c:344](../../../../../contrib/merkle_lsm/merge.c:344). Hence D avoids foreground random inserts into the large base, but much of that base/WAL work happens later. Window write ratios of ~1.68–1.77× become only ~1.01–1.04× after drain/checkpoint. LSM full WAL is ~382–384 B/statement, versus B-tree ~373–375, so **the timed WAL reduction is not an overall WAL-amplification reduction**. Run+drain+checkpoint still favors LSM (last column), but is a derived interval, not whole campaign time: it excludes copied-log validation, reset, settle and Merkle verification. No raw WAL histogram was saved to apportion the gap to B-tree full-page images versus other record classes.

## 6. Workload correctness versus upstream YCSB

The requested operation mixes agree with the upstream definitions: [A](https://github.com/brianfrankcooper/YCSB/blob/master/workloads/workloada), [B](https://github.com/brianfrankcooper/YCSB/blob/master/workloads/workloadb), [C](https://github.com/brianfrankcooper/YCSB/blob/master/workloads/workloadc), [D](https://github.com/brianfrankcooper/YCSB/blob/master/workloads/workloadd), [F](https://github.com/brianfrankcooper/YCSB/blob/master/workloads/workloadf). Key-preserving value updates are expected; D is the only insert-producing mix among these five. That is a genuine workload distinction, not a missing-index-maintenance bug.

| Property | Current harness | Upstream comparison / implication |
| --- | --- | --- |
| A/B/C/D/F mixes | 50/50 R/U; 95/5 R/U; read-only; 95/5 R/I; 50/50 R/RMW | Matches the five definitions above; sampled operation counts fluctuate around probabilities. |
| Updated payload | All 10 text fields, 20 characters each | Upstream defaults update one field, with 100-byte fields; this is a configured/custom value-update variant, not an identical default YCSB implementation. |
| Key preservation | A/B/F never SET ycsb_key | Upstream update uses a separate record key and new field values; payload-only updates are normal. |
| Key order / popularity | Integer keys; direct rank Zipf; D new keys monotonic, read-latest lagged by 1500 operations | Upstream defaults hash physical key names, use scrambled Zipf / acknowledged latest. Local physical locality and static horizon differ. |
| F request | One MATERIALIZED read CTE + UPDATE RETURNING | Upstream calls read and update; local single-statement serializable form changes transaction/binding costs. |

These default field, key-order and RMW behaviors are from [CoreWorkload.java:107–171, 356–366, 477–493, 707–804](https://github.com/brianfrankcooper/YCSB/blob/master/core/src/main/java/site/ycsb/workloads/CoreWorkload.java#L707); D explicitly documents hashed key order in [workloadd:23–26](https://github.com/brianfrankcooper/YCSB/blob/master/workloads/workloadd#L23). Locally see [scripts/distributed/large_zipf.py:37](../../../../../scripts/distributed/large_zipf.py:37) and [scripts/generate_ycsb_workloads.py:280](../../../../../scripts/generate_ycsb_workloads.py:280). These differences affect portability/cache/retry behavior and should be disclosed, but cannot explain away the observed absence of lookup writes in A/B or the mandatory lookup insert stream in D. The files contain real mutations, not no-op UPDATEs or missing D INSERTs; no malformed mix or cross-mode hash mismatch was found.

## 7. Post-matrix confirmation and extra workload proposal — not run

**First confirm HOT with counters.** Only after the matrix finishes and the host is available, use a new disposable cluster under `.247:~/claude_checks/ycsb_matrix_20261020/phase1c_hot_confirm/` on a separately checked free port (e.g. 5496), using the same built binaries. Never use the four base directories or the matrix working copies. Use shared_buffers=32MB, WAL/fsync/synchronous_commit on, SERIALIZABLE, autovacuum off, same table schema (integer ycsb_key + ten text fields), same primary/Merkle/lookup indexes, 200 partitions, fanout 32, split 1024/merge 256; LSM levels 3, top 1024 KiB, ratio 16. No disk benchmark is needed.

| Small experiment arm | Fresh data / operations | What to record and predicted discriminant |
| --- | --- | --- |
| HOT-eligible baseline | Load 30,000 20-character rows at fillfactor=90; build primary + Merkle + LSM lookup; 5,000 seeded A-style single-row payload UPDATEs, one transaction/request | n_tup_upd and n_tup_hot_upd deltas for the application table; L0/L1 physical bytes + merge counters before/during/after, and a final flush/verify. Expect high HOT fraction and little lookup growth. |
| Packed-page contrast | Fresh identical fillfactor=100 table; same seeded operations | Expect more non-HOT early updates, but measure it; pruning/reuse can restore HOT later. An ALTER fillfactor without physically repacking is not this experiment. |
| Guaranteed non-HOT control | Fresh ff90 table plus an ordinary B-tree on field1; same UPDATE changes field1 | Changing an indexed payload field disqualifies HOT even though ycsb_key and lookup expressions stay unchanged. n_tup_hot_upd should be zero and lookup entries must grow; keep this extra index identical in LSM/B-tree arms. |
| Insertion positive control | Fresh copy; 5,000 new sequential keys | Every INSERT must reach L0. Distinguishes absent insertion from a broken sampler. |

Collect `SELECT n_tup_upd,n_tup_hot_upd FROM pg_stat_user_tables WHERE relid='hot_probe'::regclass` before/after and `merkle_lsm_stats` before any forced flush. In this PG13 build close the workload connection and allow the stats collector to publish, then clear the observer snapshot/read counters; do not rely on newer-version force-flush functions. Filter by application relid so internal-node HOT updates are not counted as user HOT updates. Run one worker first to establish mechanism, then 16 if desired. Record index/heap sizes after work (before restore), table relfilenode/OID, LSN bracket and a short archived WAL range. Offline pg_waldump can then count Heap HOT_UPDATE versus UPDATE, B-tree insertion records, and full-page images per relation. Verify the Merkle root after every arm. No exact HOT percentage is predeclared as a passing requirement; the non-HOT and insert controls must visibly grow the lookup index.

**Then measure the AM benefit where lookup inserts are guaranteed.** The cleanest immediate existing option is `all_insert`, identical SQL across DET/LSM/B-tree, on restored 100M baselines, with 16 workers and the same durable settings. Use e.g. **160,000 statements at skew 0**, at least three repeats, retain all results, and require real L0/L1 merges; report both gateway and the full before-run→after-checkpoint throughput, full WAL, full device writes, HOT counters and post-run sizes. Validate 160,000 successful inserts, final row count 100,160,000, correct key bounds and Merkle verification, not merely gateway return code. Inserts necessarily feed the index, so a difference is interpretable; the magnitude remains an experimental outcome, not guaranteed.

Do **not** blindly request 2.4M all_insert statements with this generator: its bucket allocator spaces keys by 2×num_tx ([scripts/generate_ycsb_workloads.py:233](../../../../../scripts/generate_ycsb_workloads.py:233)), and the harness integer-overflow guard rejects that at 100M rows ([scripts/distributed/run_oom_100m_benchmark.py:1548](../../../../../scripts/distributed/run_oom_100m_benchmark.py:1548)). At 160K statements, 5,000 buckets give a conservative maximum 1,700,160,000, safely below INT_MAX. For a matched 2.4M-statement sustained test, propose a separately named monotonic-insert generator or D-derived 80%-INSERT/20%-read variant after this matrix, with IDs 100,000,001 onward; never change the current workload files or controller to implement it mid-run. Fresh snapshots must have identical data/index layout and restored WAL state for both AMs.

A secondary diagnostic is A with guaranteed non-HOT updates (indexed field1, or fresh packed ff100/controlled inline row growth), always measure n_tup_hot_upd rather than assuming it. **Lower** fillfactor gives more free page space and generally promotes HOT; use **higher** fillfactor or indexed-column changes to suppress it. Added indexes/row growth also add other costs, so this is a mechanism contrast, not a replacement “standard A” result. If lookup insertion becomes substantial, LSM can reduce foreground large-index insert cost; if heap/WAL/node work still dominates, TPS gain can remain small. LSM is not expected to accelerate read-only C or largely HOT payload updates through a lookup index they scarcely mutate.

## Evidence locations and reproducibility

Authoritative `.111` analysis root: `/home/neel/claude_ctl/ycsb_matrix_20261020/evidence/phase1c/`. `sql_counts.json` preserves original workload paths/SHA256s/counts/examples; `sampler.json` preserves phase-specific byte extrema, sample counts, merge transitions and final states from `.247:/home/neel/claude_checks/ycsb_matrix_20261020/evidence/wal_sampler.jsonl`; `artifacts.json` preserves each original result path, metrics, setup-versus-result stats, log-tail profiles and telemetry aggregates. `FACTS.tsv`/`SQL_COUNTS.tsv`/`LSM_SAMPLER.tsv` provide line references used here. Small copies are alongside this report in `agents/phase1c_evidence/`. No SQL workload or large server log was copied from .247.

Read-only helpers are `agents/phase1c_sql.py`, `phase1c_sampler.py`, `phase1c_artifacts.py`, `phase1c_report.py`; remote helpers use new filenames in the authorized campaign roots. Existing `check_results.py`, run_matrix.sh, workload generator, sampler daemon and harness were not edited. The SQL scan reads each of the 20 unique files once; it does not execute statements. The sampler helper reads only the existing sampler file. No new build, regression, benchmark, DB session or experiment was run.
