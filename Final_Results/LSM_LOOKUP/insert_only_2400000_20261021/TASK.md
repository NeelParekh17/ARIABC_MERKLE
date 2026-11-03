# Task: audit what the Merkle + merkle_lsm lookup design really stores and what I/O a write does

You are the worker. An orchestrator (Claude) reviews everything you report and
re-checks your evidence on the hosts, so report facts exactly, including failures
and anything that contradicts the claims below. Finding a discrepancy is a valid
and useful result; do not smooth it over.

## Background: the advisor's questions (verbatim intent)

The advisor asked us to verify the following about the Merkle design with the LSM
lookup index (`contrib/merkle_lsm`, default lookup AM since 56b23af8):

1. "The data is also in a normal heap table. An update of a record causes a heap
   update, and the index itself is a relation. The Merkle index is basically a
   relation; it should not exist as a separate relation, it should only exist as an
   index. In a basic LSM tree there is no separate data, the data is in the LSM index
   itself. It should be the same here." -> Establish exactly which relations exist
   for a Merkle-enabled table, what each stores, and whether any of them duplicates
   the row data. State plainly whether merkle_lsm (and the upstream lsm3 it derives
   from, `Final_Results/LSM_LOOKUP/lsm3_experiment_20261014/lsm3_source`) is a
   secondary index of (key -> heap TID) or a data-holding LSM store.
2. "For a write-intensive load, take the original and then take after adding the
   index", and confirm what changes.
3. "Check the plan also ... check that it is actually doing the right thing." With a
   separate B-tree index, an update touches the tuple, the index, and several nodes
   along the path: the inner nodes are cached but the lower levels cause random I/O.
   "With an LSM tree there would only be sequential I/O, no random I/O." -> Verify
   whether that is true for our implementation, per relation, with measurements.

Things the orchestrator already knows (verify, do not assume):
- merkle_lsm only replaces the auxiliary lookup index `<table>_merkle_lookup_idx`
  ((partition, route hash, key...) -> heap TID). It has an L0 pair of B-trees, an L1
  pair and the original ~3 GB base B-tree, plus a metadata relation (see
  `doc/LSM_LOOKUP_ARCHITECTURE.md`, `doc/LSM_LOOKUP_ARCHITECTURE_DETAILED.md`,
  `contrib/merkle_lsm/merkle_lsm.c`, `contrib/merkle_lsm/merge.c`).
- The Merkle integrity index (AM `merkle`) and its node storage (synchronous
  leaf-to-root hash maintenance, `src/backend/access/merkle/`) are NOT LSM-based.
- Merges into lower levels insert entry by entry into an existing destination B-tree
  (`ml_copy`), so an L1 -> base merge may still do random I/O on the base, only
  batched and deferred.
- YCSB A/B/F updates are HOT (non-key columns only), so they skip index inserts
  entirely; only INSERTs (workload D) write the lookup index
  (`.bench_tmp/ycsb_matrix_20261020/agents/LSM_WORKLOAD_ANALYSIS.md`).
- Merkle-internal lookups are issued from C (`index_beginscan` in
  `src/backend/access/merkle/merkleapply.c`), not by the planner, so `EXPLAIN` alone
  cannot show them.

## What to produce

### Part 1: relation inventory (static + live catalog)
For a 100M-row OOM baseline of each kind (plain, Merkle+btree, Merkle+LSM), list every
relation belonging to `usertable` (heap, TOAST, PK, Merkle index, Merkle node
storage, lookup index, every hidden merkle_lsm component and its meta relation):
relkind, AM, size, what one entry/tuple contains, which code path writes it, and
whether it holds any copy of row data (field values) or only keys/hashes/TIDs.
Explain in plain words how this differs from a "data lives in the LSM" design, and
what would have to change to get there (analysis only, no implementation).

### Part 2: plans and code-path verification
- `EXPLAIN (ANALYZE, BUFFERS)` of the exact SQL statements the YCSB harness sends
  (read, update, insert, read-modify-write) in each of the three setups.
- Prove which relations each statement actually reads and writes, including the
  Merkle-internal path the planner does not show: use per-relation counter deltas
  (`pg_statio_all_tables/indexes`, `pg_stat_all_indexes`, including the hidden
  components), `pg_waldump` block references per relfilenode, and/or tracing.
- Confirm: foreground INSERTs go to the active L0 tree only and never touch the base
  during the run; the base is written only by merges; no relation is maintained twice
  (for example a leftover B-tree lookup next to the LSM, or a hidden component
  entering the executor's ordinary index list); reads probe all components and return
  correct results.

### Part 3: write-intensive I/O measurement
Compare three setups on the same data and the same workload file: original
(`bcdb_det`, no Merkle, base `base_ff90_plain`), Merkle+B-tree lookup (`base_ff90`),
Merkle+LSM lookup (`base_ff90_mlsm`). Workloads:
- insert-heavy (every statement an INSERT of a new key; this is the case that writes
  the lookup index). The existing `all_insert` generator tops out at ~160K
  statements; write a small generator if you need more.
- update-heavy (YCSB A-style 100% updates of non-key fields), to show what the Merkle
  node path costs per update when the lookup index is not involved.
Run length: long enough that merkle_lsm performs several L0->L1 merges during the
run and the L1->base merge is observed (in the run or in the drain); a few minutes
per run, 2 trials per case is enough. Same methodology as the 2026-10-19 D runs
(`Final_Results/LSM_LOOKUP/merkle_lsm_ycsb_d_20261019/README.md`, `run_d.sh`):
cold reset, shared_buffers 32MB, 16 workers, SERIALIZABLE.

For every run, per relation (heap, PK, Merkle node storage, lookup base, each LSM
component), report: blocks read from disk, blocks dirtied/written, WAL bytes and FPIs
(by rmgr and relation, from `pg_waldump`), and, as the key evidence for the advisor,
the ACCESS PATTERN: how random vs sequential the physical reads/writes are. Measure
the pattern from real offsets, not inference: e.g. block numbers from `pg_waldump`
per relfilenode, `strace -e pread64,pwrite64` on backends/checkpointer/bgwriter
mapped to relfilenodes, or blktrace/bpftrace if sudo is available (sudo password for
.111 is in `~/.env` there; check what works on .247 without asking for more). Give a
distance/run-length histogram per relation, separately for the foreground run, the
merges and the drain/checkpoint. Also report TPS and total cost including the drain.

## Conclusion section (most important)
A short plain-language answer for the advisor to each of the three questions, each
backed by a number from your evidence, and a list of every discrepancy between the
advisor's expectations / the docs and what the code actually does. If something
is a real defect (for example double maintenance, base written by foreground inserts,
or wrong scan results), say so and give the evidence; do NOT fix engine code.

## Hosts and hard rules

- DB host 10.129.148.247 (user neel), controller + gateway 10.129.27.111 (user neel).
  Always `ssh -o ControlMaster=no`. Detach long jobs with tmux (on .111) or
  `(setsid nohup ... &)`; never block an ssh session on a long job.
- **A different campaign is running right now**: the YCSB matrix (tmux `ycsb_matrix`
  on .111, postgres on .247 from `~/claude_checks/ycsb_matrix_20261020/install` on
  `~/ariabc_data/lsm_lookup_20261014/pgdata_*`). Its progress is in
  `.111:~/claude_ctl/ycsb_matrix_20261020/results/matrix/status.txt`; it ends after
  `CAMPAIGN_END f_btree` and the `run_matrix.sh` process exits. Until then you must
  NOT start any postgres, benchmark, build, large copy or trace on .247, and must not
  touch the matrix's files, ports, tmux session or processes. Until then do Parts 1
  and 2 from source and docs, and write your scripts in your clone. Poll the status
  file cheaply (every ~10 min, sleep between checks). When the matrix has ended and
  there is no postgres running on .247, start your live work.
- After the matrix ends you may reuse its PG install on .247
  (`~/claude_checks/ycsb_matrix_20261020/install`, built from 5f2a078e = your clone's
  HEAD; verify the sha256s it recorded) and the harness working copies in
  `~/ariabc_data/lsm_lookup_20261014` the same way the matrix used them. The
  `base_*` directories there are read-only inputs: never modify, delete or rebuild
  them. Do not delete `base_ff90_lsm` or `pgdata_lsm`. Do not touch other campaigns'
  dirs, tmux sessions (`cluster_full`, `stall_catcher`, `ycsb_matrix` on .111) or
  processes.
- Never use /tmp on the lab hosts. New roots:
  .247 `~/claude_checks/lsm_relation_audit_20261021/`,
  .111 `~/claude_ctl/lsm_relation_audit_20261021/` (TMPDIR = a scratch dir there).
- .247 has 15 GB RAM: shared_buffers 32MB as the reference runs, no extra large
  processes; require >= 30 GB free on .247 before copying/restoring anything.
- Builds, tests and DB work only on .247 / .111, never on this local machine.
- Benchmarked clients are SERIALIZABLE only (monitoring/sampler sessions may use
  read committed).
- Do not git commit or push. Do not modify engine code (`src/`, `contrib/`).
  Harness/script changes only if needed, minimal, in your clone, each listed with the
  reason. Do not delete the `.codex` file in the repo.

## Report

Write `agents/FINAL_REPORT.md` (in this workspace,
`/work/ARIABC/AriaBC/.bench_tmp/lsm_relation_audit_20261021/agents/`): conclusion
section first, then Parts 1-3 with tables, every script and command used, exact paths
of all evidence on both hosts, and anything you could not measure and why. Copy the
small evidence files (summaries, histograms, EXPLAIN outputs, waldump stats) into
`agents/evidence/`. Then end.
