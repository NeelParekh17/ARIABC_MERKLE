# Merkle + merkle_lsm: what is stored, and what I/O a write does

Audit date: 2026-10-21/22. Build 5f2a078e (13devel). The OOM baseline has 100M rows: a 28.2 GB heap and 32 MB `shared_buffers`. Every case used 16 workers at SERIALIZABLE, 2.4M statements, and 2 trials.

Codex wrote the brief, the static findings (`STATIC_FINDINGS.md`) and the harness. Claude fixed the harness and ran it to completion (see Harness changes). All 12 cases passed. Merkle `verify` returned `t` in all 8 Merkle cases. Scan correctness and range correctness held, and the post-diagnostic verify returned `t`.

## Conclusion (for the advisor)

**Q1. Is the Merkle index a separate relation? Does the LSM hold the data?**

Yes, it is separate, and no, the LSM holds no row data.

- The row data lives only in the heap: `usertable`, 28.2 GB. No other relation holds field values.
- The Merkle integrity index `usertable_merkle_idx` (AM `merkle`) is an empty 8 KB stub.
- The Merkle tree itself is stored in a **separate ordinary heap table**, `ariabc_internal.merkle_node_usertable`. It is 24.7 MB, holds node hashes, and has three B-tree indexes (pkey, prefix, root).
- `usertable_merkle_lookup_idx` maps `(partition, route hash, key) → heap TID`. With merkle_lsm it is five B-trees (L0 pair, L1 pair, 3.15 GB base) plus a meta relation. It is a **secondary index**, exactly like the upstream lsm3 it derives from.
- So we do *not* have "data in the LSM". An update still writes the heap tuple, and a new key also writes the PK, the lookup index and the Merkle node rows.
- Getting the advisor's design would need a different storage model: a table AM whose primary storage is the LSM, with Merkle node hashes kept inside that storage. That is analysis only; no implementation was attempted.

**Q2. Write-intensive load, original vs with the index.**

Insert-heavy load, 2.4M new keys:

| setup | TPS (run / incl. drain + checkpoint), t1 · t2 | blocks read | blocks written | WAL |
|---|---|---|---|---|
| original (no Merkle) | 8641/8639 · 8223/8172 | ≈0 | 0.18M | 0.80 GB |
| Merkle + B-tree lookup | 3405/3404 · 3027/2978 | 5.51M | 3.86M | 4.97 GB |
| Merkle + LSM lookup | 3963/3874 · 3524/3302 | 3.8–3.9M | 3.5M | 5.23 GB |

- Merkle costs about 2.5× in TPS.
- The LSM gets back +16% TPS over the B-tree lookup in both trials, or +11–14% once the drain is counted.
- It cuts lookup-base reads by 62% (3.94M → 1.51M) and writes by 38% (2.39M → 1.49M).
- **It does not reduce WAL**: about 5% more, because every entry is logged in L0, then L1, then the base.

Update-heavy load (2.4M non-key updates):

| setup | TPS t1 · t2 | blocks read | blocks written | WAL |
|---|---|---|---|---|
| original | 3541 · 3086 | 2.28M | 1.19M | 4.45 GB |
| Merkle + B-tree | 3085 · 2736 | 3.86M | 2.34M | 5.49 GB |
| Merkle + LSM | 2817 · 2773 | 3.86M | 2.34M | 5.49 GB |

- Merkle costs 11–13% TPS here.
- B-tree and LSM are **identical block for block**. YCSB updates are HOT, so the lookup index is never written.
- The TPS gap between them is not consistent (−9% in t1, +1% in t2) and matches the run-to-run drift visible in the plain case (3541 → 3086). It is noise, not an LSM effect.

**Q3. Does the plan do the right thing? Does the LSM give only sequential I/O?**

The plans are right.

- Every YCSB statement uses `usertable_pkey1`.
- The Merkle work runs from C and never shows in EXPLAIN. Per-relation counters show 3 node-row HOT updates per write through `node_pkey`.
- A foreground insert writes the active L0 tree only. The base is never written by an insert.
- Lookups probe all components. Scan and range results are equal to a sequential scan.

**But "LSM ⇒ only sequential I/O" is not true for our implementation.** These are measured file offsets; "far" means at least 64 blocks from the previous I/O of the same process on the same file.

| relation, insert load | B-tree lookup | LSM lookup |
|---|---|---|
| lookup base, reads | 3.94M, **87% far** (backends) | 1.51M. The merger's sweep (1.27M) is 100% near and 65% at the next block. |
| lookup base, writes | 2.39M, **100% far** | 1.49M, **still 48–70% far** (merger 52%, backend evictions 70%) |
| Merkle node heap, read / write | 1.29M / 1.29M, 87% / 96% far | 1.53M / 1.51M, 96% / 97% far |
| node pkey, reads | 221K, 85% far | 553K, 91% far |
| all relations | 86% of reads far, 78% of writes far | 56% of reads far, 60% of writes far |

- The LSM turns the lookup-index **reads** into a mostly sequential merge sweep.
- **Writes stay random.** The L1 → base merge (`ml_copy`) inserts entry by entry into the existing 3 GB B-tree: page splits plus out-of-order eviction and checkpoint writeback. The base still gets 382K full-page images, the same as the B-tree lookup.
- The **Merkle node table is random in every Merkle case** and the LSM does not touch it: 2.6–3.0M page reads and writes per 2.4M writes.
  - These are PostgreSQL-level (VFS) page I/Os. They happen because 32 MB of shared_buffers cannot keep the node table's 3,020 pages resident.
  - The table is only 24.7 MB, so most of those reads are probably served from the OS page cache, not the SSD. On real disk, the heap and lookup index (28 GB and 3 GB) are the bigger random-I/O sources.
  - Geometry, from `setup.json` and the index reloptions: 200 partitions, fanout 32, split_threshold 1024, 204,800 leaves. That gives root → level-1 → leaf, so exactly 3 node updates per write; the counters show 7.2M updates for 2.4M writes.
- In the update load, the heap and PK are about 100% random **even without Merkle**. That is inherent to uniform updates on a 28 GB table with 32 MB of cache.

### Discrepancies found (docs / expectations vs code)

1. **The data is not in the LSM.** merkle_lsm and lsm3 are key → TID secondary indexes, and the heap is still the only data store (Q1).
2. **The Merkle tree is a separate heap relation.** `merkle_node_usertable` has 3 indexes and is maintained synchronously, about 3 HOT updates per write. It is not LSM-based.
3. **The L1 → base merge is not sequential on write.**
   - Merger base writes are 52% far; foreground 881K and drain 228K.
   - The FPI count is unchanged (382K vs 382K).
   - WAL for the base is unchanged (3.00 GB), and L0 + L1 add about 0.43 GB more.
4. **Backends write merger-dirtied base pages.**
   - Backends evict and write 343K base pages that the merger dirtied. These are 70% far, during the foreground.
   - So user backends still pay random base-page writes, just not logical inserts.
   - The merger in turn evicts 217K node-heap pages. Buffer eviction is shared under 32 MB `shared_buffers`.
5. **LSM merges cost the node table its HOT updates.** In LSM insert runs:
   - only 7,071,766 and 7,084,708 of the 7.2M node updates were HOT, against 7,200,000 of 7.2M with the B-tree lookup;
   - the node heap grew from 24.7 MB to 47–50 MB, and the node pkey from 6.7 to 13.3 MB;
   - `node_pkey` and `prefix_idx` gained about 90K page writes each;
   - node-heap reads rose by 19%.

   Likely cause: the merger's long copy transactions hold back the pruning horizon. This is not proven.
6. **LSM lookup plans use a bitmap scan.** merkle_lsm's cost estimate gives 2501 rows where the B-tree gives 1, so the planner picks a Bitmap Heap Scan instead of an Index Scan. Results are correct and the cost is small: 13–15 buffer hits vs 5.
7. **The plain baseline still contains the Merkle node table and its indexes.** These are `merkle_node_usertable` and its indexes, about 38 MB with no Merkle index. They saw 0 I/O in the plain runs, so the measurements are unaffected. Static finding: `merkle_drop_node_table` is never called.
8. **Not defects:**
   - No relation is maintained twice. The LSM case has no leftover B-tree lookup; the lookup relation *is* the merkle_lsm base.
   - Hidden components are `indislive/indisready/indisvalid = false` and never entered the executor index list. Inserts hit only `ml_*_l0_s0`.
   - The 1-block lookup reads during a plain PK SELECT are the planner reading the index metapage.

## Part 1: Relation inventory

All three baselines share a byte-identical heap and PK (`evidence/controller/evidence/baseline_data_identity.json`, `"byte_identical": true`).

| relation | relkind / AM | size | one entry holds | written by | row data? |
|---|---|---|---|---|---|
| `usertable` | r / heap | 28.2 GB | full row: key + 10 fields | executor | **yes (only copy)** |
| `usertable_pkey1` | i / btree | 2.25 GB | key → TID | executor index insert (skipped on HOT) | key only |
| `usertable_merkle_idx` | i / merkle | 8 KB | nothing (stub; the AM hooks drive node updates) | merkle AM `aminsert` | no |
| `ariabc_internal.merkle_node_usertable` (+ pkey 6.7 MB, prefix_idx 7.0 MB, root_idx 16 KB) | r / heap | 24.7 MB | node id/prefix → 32-byte hash, child metadata | `merkleapply.c` via `index_beginscan` on the node pkey plus heap update | no (hashes) |
| `usertable_merkle_lookup_idx`, B-tree mode | i / btree | 3.15 GB | (partition, route hash, key) → TID | executor index insert | key only |
| `usertable_merkle_lookup_idx`, LSM mode | i / merkle_lsm | 3.15 GB base | same as above | merger only (`ml_copy`) | key only |
| `ml_16856_l0_s0/s1`, `ml_16856_l1_s0/s1` | i / btree, hidden (live/ready/valid = f) | 8 KB at rest | same as above | L0: foreground `aminsert`; L1: merger | key only |
| `ml_16856_meta` | i / merkle_lsm_meta, hidden | 8 KB | epoch, active slots, merge counters | merger | no |
| TOAST tables of both heaps | t / heap | 0 | — | — | — |

**Advisor's design vs ours**

- In a "data in the LSM" design, the LSM is the table. A write appends one record to a memtable or log, and compaction rewrites sorted runs sequentially.
- Ours keeps a heap and adds four write targets per new key: the PK, the lookup (L0), and 3 node-row updates.
- Getting there needs three changes:
  - a table AM whose primary storage is the LSM, so that TIDs disappear and the PK and lookup become the LSM key order;
  - Merkle node hashes kept in, or derived from, that structure;
  - merges that build new sorted runs (a bulk build) instead of inserting into an existing B-tree.

## Part 2: Plans and code path (live, update_*_t2 diagnostics)

| statement | plan, all 3 setups | relations touched in B-tree mode (counters) | relations touched in LSM mode | WAL block refs, LSM mode |
|---|---|---|---|---|
| read | Index Scan `usertable_pkey1` | heap, PK (+ lookup metapage by the planner) | heap, PK, meta (planner) | none |
| update | ModifyTable → Index Scan PK, HOT | heap, PK, merkle_idx, node heap ×3 HOT, node pkey ×3 | same; lookup untouched | heap, node heap |
| insert | ModifyTable → Result | heap, PK, **lookup**, node heap ×3, node pkey | heap, PK, **`ml_16856_l0_s0`** + meta, node heap ×3 | heap, PK, node heap, **l0_s0** |
| read-modify-write | CTE + Nested Loop, PK ×2 | like update | like update | heap, node heap |

- The plain setup touches only heap and PK.
- Merkle split lookups (SPI split SQL) use `usertable_merkle_lookup_idx`:
  - B-tree mode: Index Scan, 5 buffer hits.
  - LSM mode: Bitmap Index Scan → Bitmap Heap Scan, 12–15 hits. It opens all 5 B-trees; the uprobe count is `btbeginscan` ×30 per component during the diagnostics.
  - 7 point keys and one range gave the same rows as a sequential scan (`scan_correctness.json`, `range_correctness.json`, `"equal": true`).
- `unmerged_components.json` / `range_correctness.json`: key 110051000 was inserted while it sat unmerged in L0/L1. The range scan returned it (`new_rows` 1, `equal: true`), and a merge followed (merge counters went from [1,1] to [2,1]). The base size was unchanged at 3,154,264,064 bytes.
- In LSM insert runs, the foreground base I/O came from the **merger** pid (1.27M reads, 881K writes) and from backend evictions (343K). Backends never issued logical base inserts: the MarkBufferDirty uprobe on the base shows only the merger pid.

## Part 3: Write-intensive I/O

| case | run TPS | total TPS | drain s | checkpoint s | merges L0→L1 / L1→base (run → after drain) |
|---|---|---|---|---|---|
| insert_plain t1 / t2 | 8641 / 8223 | 8639 / 8172 | 0 | 0.1 / 1.8 | — |
| insert_btree t1 / t2 | 3405 / 3027 | 3404 / 2978 | 0 | 0.2 / 12.8 | — |
| insert_lsm t1 / t2 | 3963 / 3524 | 3874 / 3302 | 11.7 / 45.8 | 2.2 / 0.1 | 97/5 → 98/7 (both trials) |
| update_plain t1 / t2 | 3541 / 3086 | 3483 / 2984 | 0 | 11.3 / 26.7 | — |
| update_btree t1 / t2 | 3085 / 2736 | 3081 / 2736 | 0 | 1.0 / 0.0 | — |
| update_lsm t1 / t2 | 2817 / 2773 | 2808 / 2746 | 0.9 / 1.1 | 1.8 / 7.4 | 0/0 → 1/1 |

**Per-relation I/O, insert t1, all phases.** Each cell is blocks read / blocks written / WAL / FPI; t2 is within 3%.

| relation | plain | B-tree | LSM |
|---|---|---|---|
| heap | 679 / 166K / 0.65 GB / 1 | 56K / 166K / 0.65 GB / 1 | 42K / 166K / 0.65 GB / 1 |
| PK | 5 / 13K / 0.16 GB / 3 | same | same |
| lookup base | — | 3.94M / 2.39M / 3.00 GB / 382K | 1.51M / 1.49M / 3.00 GB / 382K |
| LSM L0 | — | — | 30K / 50K / 0.22 GB / 98 |
| LSM L1 | — | — | 106K / 118K / 0.21 GB / 7 |
| node heap | — | 1.29M / 1.29M / 1.17 GB / 3020 | 1.53M / 1.51M / 0.95 GB / 3020 |
| node pkey | — | 221K / 0 / 0 / 0 | 553K / 94K / 18 MB / 814 |
| node prefix_idx | — | 0 | 89K / 91K / 19 MB / 854 |
| node root_idx | — | 0 | 17K / 17K / 3 MB / 1 |

**Update t1/t2.**

- heap: 1.19M/1.19M (plain) vs 1.32M/1.32M (Merkle), 4.45 GB WAL, 504K FPI in every setup.
- PK reads: 1.09M (plain) vs 1.35M (Merkle).
- node heap: 1.02M/1.02M, 1.04 GB WAL, the same in B-tree and LSM mode.
- lookup and LSM components: under 1K blocks (diagnostics only).

**Access pattern, t1 foreground unless stated.** Columns are I/Os, % at the next block (+1), % near (under 64 blocks) and % far (64 blocks or more).

| case | actor | relation | op | n | +1 | near | far |
|---|---|---|---|---|---|---|---|
| insert_plain | backend | heap | write | 88K | 85 | 86 | 14 |
| insert_plain | bgwriter | heap | write | 74K | 100 | 100 | 0 |
| insert_btree | backend | lookup | read | 3.94M | 0 | 13 | **87** |
| insert_btree | backend | lookup | write | 2.20M | 0 | 0 | **100** |
| insert_btree | backend | node heap | read / write | 1.29M / 1.18M | 0 | 13 / 4 | **87 / 96** |
| insert_lsm | merger | lookup base | read | 1.27M | 65 | 100 | 0 |
| insert_lsm | merger | lookup base | write | 881K | 17 | 48 | **52** |
| insert_lsm | backend (eviction) | lookup base | write | 343K | 1 | 30 | **70** |
| insert_lsm (drain) | merger | lookup base | read / write | 239K / 228K | 54 / 19 | 99 / 50 | 1 / **50** |
| insert_lsm | merger | L1 | read | 104K | 1 | 1 | **99** |
| insert_lsm | backend | L0 | read / write | 28K / 28K | 1 / 4 | 56 / 74 | 44 / 26 |
| insert_lsm | backend | node heap | read / write | 1.53M / 1.13M | 0 | 4 / 3 | **96 / 97** |
| insert_lsm | merger (eviction) | node heap | write | 217K | 0 | 4 | 96 |
| update_* (all 3) | backend | heap | read / write | 1.2–1.3M / 0.9–1.1M | 0 | 0 | **100** |
| update_* (all 3) | backend | PK | read | 1.1–1.35M | 0 | 5–7 | **93–95** |
| update_btree/lsm | backend | node heap | read / write | 1.02M / 0.88M | 2 / 0 | 14 / 5 | **86 / 95** |

The full table for all actors and phases is in `evidence/tables.md` (generated by `analysis/make_tables.py` from `evidence/facts.json`).

**Totals and cost per 2.4M inserts, all relations:**

- B-tree lookup: 9.37M block I/Os, 4.97 GB WAL.
- LSM lookup: 7.2–7.4M block I/Os (−22%) and 5.23 GB WAL (+5%), plus a 12–46 s drain after the run (its TPS cost is already in total TPS).

## Harness changes (in `repo/scripts/lsm_relation_audit/`; engine untouched)

1. **Struct offsets.** `audit.py` replaced gdb struct-offset discovery with an `offsetof()` C probe compiled against `pg_config --includedir-server`, with addresses from `nm`. The install has no DWARF, so gdb failed with "No symbol table is loaded".
2. **WAL retention.** It sets `ALTER SYSTEM SET wal_keep_segments=2560` after setup, so `pg_waldump` can read the whole run. 13devel has no `wal_keep_size`.
3. **Reused calibration.** `AUDIT_CALIBRATION` reuses the earlier device calibration (`device_calibration_40a481a9.json`: qd1 156 µs, qd8 308 µs, write 490 µs). Fresh calibration failed twice: a foreign writer on .247 puts about 13 MiB every 30–50 s.
4. **Smoke mode and restartability.** Added `AUDIT_ONLY` (smoke subset), `AUDIT_CASE_SUFFIX`, and skipping `baseline_identity` when its evidence already exists.
5. **Poll backoff.** The detached-job poll is now 3 s for the first 30 s, then 30 s. Polling every 3 s created logind/journald writes every few seconds, so the SSD settle probe never saw idle and timed out after 1800 s.
6. **Rename tolerance.** `summarize.py` records renamed relation inodes in `coverage.json` and fails only above 10 events. B-tree lookup segment files 16495 and 16495.2 showed 1–2 rename or reuse events; their bytes are attributed to their final names.

## Limitations

- **Unmapped I/O.**
  - Update runs: about 3.1 GB in all three setups, mostly backend reads of `pg_xact` segments 0000–0004 (CLOG lookups for unhinted baseline tuples). It is equal across setups.
  - LSM insert runs: 0.69–0.75 GB, also mostly `pg_xact` 0000–0004 backend reads. B-tree inserts: 0.15 GB, mostly catalog.
  - Why LSM inserts consult CLOG more (bitmap heap rechecks or held-back hint setting) was not attributed.
- **Writes are charged to whoever does the eviction.** "Backend" base writes in LSM mode are evictions of merger-dirtied pages, and "merger" node-heap writes are the reverse. The MarkBufferDirty uprobes identify who modified a page.
- **Shared calibration.** All cases used the same calibration and a cold reset per case. Trial 2 ran 5–13% slower than trial 1 in every setup: host drift on the QLC SSD. Compare within a trial.
- **One configuration only.** Insert results apply to 32 MB `shared_buffers` with a uniform key stream. A larger cache would hide more of the B-tree lookup's random reads.
- **Not measured:**
  - an unthrottled drain (the drain merge is serial);
  - blktrace at the block layer. VFS offsets were used instead, so a VFS read may be an OS page-cache hit (likely for the 24.7 MB node table, unlikely for the 28 GB heap and the 3 GB lookup on a 15 GB host), and a "far" VFS write can still be reordered or merged by kernel writeback and the SSD.

## Evidence

**Local** (`.bench_tmp/lsm_relation_audit_20261021/agents/`):

- `evidence/cases/<case>/`: per case, everything except raw traces and logs:
  - `PASS.json`, `timing.json`;
  - `relation_io.tsv`, `offset_histograms.json`;
  - `wal_by_relation.json`, `waldump_stats_*.txt`;
  - `relation_counters.json`, `inventory.json`;
  - `before.json` and `after_*.json` (merkle_lsm state);
  - `coverage.json`.
  - The `update_*_t2` cases also hold `plans.json`, `plan_summary.json`, `plan_*`, `scan_correctness.json`, `range_correctness.json`, `unmerged_components.json` and `call_counts.*`.
- `evidence/facts.json` and `evidence/tables.md`: compact facts and tables. They are produced by `analysis/extract.py`, which runs on .111, and `analysis/make_tables.py`.
- `evidence/controller/status.txt` (the run log) and `evidence/controller/evidence/`: provenance, sha256s, offsets, calibration, smoke runs.
- `STATIC_FINDINGS.md`, `PROGRESS.md`: Codex's source analysis.

**.111** `~/claude_ctl/lsm_relation_audit_20261021/`:

- `results/<case>/`: full results, including `io.trace`, `calls.trace`, `sampler.jsonl` and server logs, 1.6 GB in total.
- `driver_full.log`, `status.txt`, `scripts/`.

**.247** `~/claude_checks/lsm_relation_audit_20261021/`:

- `jobs/<case>/waldump.txt.gz` (raw `pg_waldump`).
- The PG install is reused from `~/claude_checks/ycsb_matrix_20261020/install`.

**Data directories.** Working copies are `~/ariabc_data/lsm_lookup_20261014/pgdata_{plain,merkle}`, restored from the read-only baselines `base_ff90_plain`, `base_ff90` and `base_ff90_mlsm`. No baseline was modified.

## Addendum, 2026-10-22: merkle_lsm with L0 = 4 MB, size_ratio = 64 (1 trial)

**Setup**
- Case `insert_lsm_t1_t4r64`: the same 2.4M-insert workload and harness as the runs above.
- On the restored working copy, `ALTER INDEX usertable_merkle_lookup_idx SET (top_index_size=4096, size_ratio=64)`, then `REINDEX` and `CHECKPOINT`, which took 217 s before the run.
- The merger reads level sizes from the meta page that is written at build time, so a REINDEX is required; ALTER alone has no effect. `merkle_lsm_config` confirms `{"levels":3,"top_index_size":4096,"size_ratio":64}`.
- Harness hook: `AUDIT_LSM_RELOPTS` in `audit.py`.

**Transaction TPS** (drain excluded): **5209**, against 3963 for LSM at the default sizes (+31%), 3405 for the B-tree lookup, and 8641 for det. That is 60% of det, up from 46%.

**Merges**
- During the run: 24 L0→L1 and **0 L1→base**. L1 ended at 93 MB.
- During the drain: 1 L0→L1 and 2 L1→base, taking 123 s.

**Foreground I/O vs LSM at the default sizes**

| | default sizes | 4 MB / ratio 64 | change |
|---|---|---|---|
| block reads | 3.63M | 0.71M | −80% |
| block writes | 3.31M | 0.88M | −73% |
| WAL | 5.19 GB | 2.29 GB | — |
| FPIs | 386K | 3.7K | — |
| lookup-base I/O | 1.27M reads, 1.26M writes | 0 | — |
| node-heap reads | 1.53M | 0.49M | −68%; less buffer-pool pressure from base pages |
| node HOT updates | 7,071,766 of 7.2M | 7,185,043 of 7.2M | node table grew to 33.5 MB instead of 50 MB |

**Drain**
- The base merge is now a dense sequential sweep: base reads 464K (100% near, 85% at +1), base writes 429K (87% near).
- It writes 3.0 GB of WAL with 382K FPIs, once.

**Steady state:** L1 reaches 256 MB after about 6.6M inserts, and from then on one such base sweep happens while transactions run.

## Addendum, 2026-10-22: ablation with Merkle and no lookup index (1 trial)

**Setup**
- Case `insert_btree_t1_nolookup`: base_ff90, then `DROP INDEX usertable_merkle_lookup_idx` and `CHECKPOINT` on the working copy.
- Harness hook: `AUDIT_DROP_LOOKUP=1`.
- There were 0 splits, so nothing needed the index. `merkle_verify` returned `t`.

**Transaction TPS**

| setup | TPS | ms per insert per worker |
|---|---|---|
| det | 8641 | 1.85 |
| Merkle, no lookup index | 6380 | 2.51 (74% of det) |
| Merkle + LSM 4 MB / 64 | 5209 | 3.07 |
| Merkle + LSM default | 3963 | 4.04 |

**Overhead of LSM 4 MB / 64 over det**

| | ms per insert | share of overhead |
|---|---|---|
| Merkle node path, hashing and locks | 0.66 | 54% |
| The lookup index / LSM | 0.56 | 46% |
| **total** | **1.22** | |

The LSM share includes its pressure on the buffer pool. Node-heap I/O was 143K reads and 161K writes with no lookup index, against 493K and 509K with the LSM, whose L1 grew to 93 MB.

**Foreground I/O and WAL**

| | no lookup index | det |
|---|---|---|
| block reads | 154K | 0.7K |
| block writes | 339K | 175K |
| WAL | 1.98 GB | 0.80 GB |

Without a lookup index, the node path costs little disk I/O. It is mainly CPU, WAL (node heap 1.17 GB) and possibly root-lock waits; the time split was not measured.

## Addendum, 2026-10-22: bulk-rebuild merge (code change, 1 trial)

**The change**
- Made in `repo/contrib/merkle_lsm/merge.c` (uncommitted), behind GUC `merkle_lsm.bulk_merge`.
- An L0→L1 merge now:
  1. reads the old L1 and the frozen L0 physically through a BAS_BULKREAD ring;
  2. sorts and de-duplicates the entries;
  3. calls `RelationSetNewRelfilenode` and `btbuild`, through a private table AM whose build scan replays the sort.
- Merges into the base still use `ml_copy`.
- Validation on an isolated cluster passed: installcheck, plus levels, capabilities, merkle, concurrency, pgbench, multiple and crash.

**Benchmark** (case `insert_lsm_t1_t4r64_bulk`, L0 4 MB / ratio 64)
- **TPS: 5013**, against 5209 with the old merge. That is within trial noise, so there is no gain.
- Merkle verify `t`. Lookup entries 102,400,000 = heap rows 102,400,000.
- Merges: 25 L0→L1 in the run, 0 into the base.

**The diagnostic step failed, and it is a harness problem**
- The probe inserts 50K rows (about 2 MB) and waits for an L0→L1 merge. With L0 = 4 MB that merge never triggers.

**What the rebuild changed**
- L1 I/O is now sequential: merger reads 100% at +1, writes 99% at +1, and **0 WAL**, because `wal_level=minimal` lets the build skip WAL.
- Node-heap I/O is unchanged: 553K reads and 569K writes, against 493K and 509K with the old merge. So L1 merges were **not** what pushed node pages out of the buffer pool.
- One candidate left: the two 4 MB L0 trees (1,024 of the 4,096 buffers) competing with the 3,020-page node table. Not yet tested.

**The new latency tail**
- `nbtsort.c` always runs `smgrimmedsync` on the new file at the end of the build: up to 75 MB per merge on the QLC SSD.
- p90 improved from 129 to 85 ms, but p99 went from 852 to 1078 ms and max from 2.6 to 8.3 s.
- The stalls appear as backends waiting on WALWriteLock / WALSync during merges.

## Addendum, 2026-10-22: levels=4 (L0 1 MB, L1 16 MB, L2 256 MB) with bulk merge (1 trial)

**Setup and correctness**
- Case `insert_lsm_t1_l4_bulk`.
- The lookup index was recreated with `merkle_create_lookup_index('usertable','merkle_lsm',4,1024,16)`, which took 247 s.
- All checks passed: Merkle verify `t`, post-diagnostic verify `t`, lookup entries = heap rows = 102,400,000.

**TPS: 4919.** Comparison: bulk merge at 4 MB / ratio 64 gave 5013, the old merge 5209, no lookup index 6380.

**Merges**
- During the run: 101 L0→L1, 4 L1→L2, 0 into the base.
- After the drain: 102, 6 and 3.

**The L0-pressure hypothesis is half confirmed: I/O fell, TPS did not rise**
- Node-heap I/O fell from 553K reads / 569K writes to **288K / 308K**.
- Total foreground I/O fell from 720K reads / 911K writes to 435K / 638K.
- I/O does not track TPS at these levels:
  - The old merge at 4 MB / 64 had the most I/O and the highest TPS.
  - The remaining 0.6 ms per insert is therefore not disk reads.

**Remaining suspects**
- **CPU:** the host has 16 hardware threads (8 cores) for 16 workers plus the merger, gateway server and background processes. Bulk merges add sorts.
- **WAL and commit syncs:** p99 is about 1.07 s and max 7.3 s in both bulk runs.
- **Root-row locks.**

**Device state:** the SSD settled slower at QD1 than before (137 µs vs 101–103 µs).

**Next step:** profile with perf and wait events (experiment B).

## Addendum, 2026-10-22: profile with perf and wait events (experiment B, 1 trial each)

**Setup**
- Cases `insert_btree_t1_nolookup_prof` (**6208 TPS**) and `insert_lsm_t1_t4r64_prof` (**4869 TPS**, old merge, L0 4 MB / ratio 64).
- `pg_stat_activity` sampled every 100 ms across the whole transaction window, plus 60 s of system-wide perf at 499 Hz.
- Evidence: `evidence/profile/`.

**Client backends are mostly waiting on the commit WAL flush**

Average number of client backends in each state (out of about 13 active):

| state | no lookup | LSM |
|---|---|---|
| LWLock:WALWriteLock | 8.79 | 9.18 |
| IPC:BlockCommit (det ordered commit) | 1.59 | 1.50 |
| CPU | 1.48 | 1.20 |
| IO:WALSync | 0.81 | 0.82 |
| Lock:transactionid (Merkle root rows) | 0.34 | 0.35 |
| IO:DataFileRead | about 0 | about 0 |

**Per insert (wait state × run time / 2.4M)**
- WALWriteLock: 1.42 → **1.89 ms (+0.47)**.
- BlockCommit: +0.05 ms. WALSync: +0.04 ms. Root locks: +0.02 ms. CPU: +0.01 ms.
- Total difference: about +0.58 ms, of which about 80% is waiting for the WAL flush.

**CPU**
- About 37% of samples (178K of a possible 479K, idle loop excluded), so CPU is not saturated.
- The profiles are nearly identical. The top entries are `bcdb_wait_for_serial_slot` (7%) and vdso clock reads. There is no LSM-specific hot spot.

**Conclusion**
- At this scale the system is bound by WAL commit flush latency. The LSM costs throughput because it adds WAL bytes per commit (L0 + L1, about 180 B per insert; 2.29 vs 1.98 GB in total) and keeps the shared QLC SSD busier (merger writes), which makes each group-commit flush slower.
- Read I/O and CPU are not the limit. That explains why halving node-heap and total I/O did not change TPS, and why the bulk merge's fsync bursts hurt the tail.
