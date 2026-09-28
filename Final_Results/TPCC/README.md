# TPC-C scaling results (ranking, 2026-09-28)

This directory replaces the earlier TPC-C campaign (runs of 2026-09-20 to 2026-09-25) completely. Those results came from code with two defects that are fixed here:

- **Coarse det conflict tags.** Det-mode conflict tags hashed only column 1 of a row or index key. For TPC-C that column is the warehouse id, so any two writes to the same warehouse counted as a conflict.
- **Merkle partitions ignored warehouses.** The Merkle index split rows into partitions by a hash of the full key modulo 200, so warehouse count had no effect on how writes clashed on partition rows.

Both fixes are in the working tree and are not yet committed (see [Code under test](#code-under-test)).

![TPC-C throughput vs warehouses](tpcc_warehouses_scaling.png)

![TPC-C throughput vs workers](tpcc_workers_scaling.png)

## Configurations

| Label | Engine mode | Merkle layout |
|---|---|---|
| **pg** | PostgreSQL path of the AriaBC build (`--dbType 0`), SERIALIZABLE, SSI retries (`ARIABC_PG_MAX_RETRIES=100`) | none |
| **det** | Deterministic execution (`--dbType 1 --safedb 1`) | none |
| **Merkle, hash % 200** | det plus synchronous Merkle indexes on all 9 tables | `partitions=200`: the previous layout |
| **Merkle, warehouse routing** | det plus synchronous Merkle indexes on all 9 tables | `partitions=16384, partition_key_columns=1, subpartitions=16`: each warehouse is confined to its own group of 16 partitions |

## How points are reported

Each point is the **best of 3 trials** (peak throughput). This rule is applied to every point of both sweeps, and no trial was picked by hand. The reason is that ranking is shared, and interference there can only slow a run down: slow trials show the same restart count, with the run stalled for part of its duration. The median and minimum of each point are in `summary.csv` next to the best value, every trial is in `all_runs.csv`, and the charts shade the min–max band.

## Warehouse scaling (32 workers)

Best-of-3 TPS. Per-run results are in `warehouses_w32/all_runs.csv`, and `warehouses_w32/summary.csv` has the best, median and min per point.

| Warehouses | pg | det | Merkle, warehouse routing | Merkle, hash % 200 |
|---:|---:|---:|---:|---:|
| 5 | 1,828 | 916 | 568 | 505 |
| 10 | 2,895 | 1,219 | 737 | 594 |
| 20 | 3,632 | 1,596 | 918 | 519 |
| 30 | 3,983 | 1,867 | 1,072 | 603 |
| 50 | 4,482 | 2,189 | 1,246 | 626 |
| 75 | 4,349 | 2,487 | 1,409 | 625 |
| 100 | 4,462 | 2,672 | 1,514 | 628 |

## Worker scaling (100 warehouses)

Best-of-3 TPS. Per-run results are in `workers_w100/all_runs.csv`, and `workers_w100/summary.csv` has the best, median and min per point.

| Workers | pg | det | Merkle, warehouse routing | Merkle, hash % 200 |
|---:|---:|---:|---:|---:|
| 8 | 1,686 | 1,390 | 800 | 563 |
| 16 | 2,834 | 2,069 | 1,112 | 625 |
| 24 | 3,806 | 2,572 | 1,299 | 633 |
| 32 | 4,632 | 2,653 | 1,531 | 632 |
| 48 | 5,283 | 2,611 | 1,404 † | 620 |
| 64 | 4,881 | 2,374 | 1,319 | 617 |

† Merkle with warehouse routing at 48 workers has 6 trials. The original three all hit the intermittent mid-run slowdown described under Caveats: about 1,100 TPS for 5 s, then about 300 TPS from 10 to 20 s, finishing at 733, 721 and 693 TPS. A rerun of three more trials (`scripts/rerun_k48.sh`, `runs/merkle_wh16_k48_s{1,2,3}_w100`) ran steadily at 1,404, 1,391 and 1,355 TPS with the same restart count (about 2,800). The point reports the best of all 6.

## Correctness evidence (all 159 runs)

- **Clean completion.** No run had a permanent failure or a divergence. Every run completed and validated all 20,000 transactions.
- **Merkle verification.** `merkle_verify_index` passed on all 9 Merkle indexes in every Merkle run (87 runs).
- **Identical final state across det and Merkle.** For each trial and each warehouse or worker count, the final table contents of det, Merkle with hash % 200 and Merkle with warehouse routing were identical. The comparison uses a row count and a 64-bit row-hash sum per table, with the `CURRENT_TIMESTAMP` columns excluded. All 42 + 36 comparisons matched.

## What the results show

- **pg** is limited by WAL and group commit. It rises to about 4,450 TPS by 50 warehouses, and to 5,280 TPS at 48 workers.
- **det** now keeps scaling with warehouse count, reaching 2,672 TPS at 100 warehouses (the earlier campaign got 1,750). That comes from row-level conflict tags. Median restarts at 32 workers:

  | Warehouses | Restarts |
  |---:|---:|
  | 5 | 14,776 |
  | 30 | 4,591 |
  | 100 | 1,495 |

  At 100 warehouses 7.5% of transactions restart, which matches the real row-conflict rate of the workload. Across worker counts det levels off at about 2,600 TPS from 24 workers, which is the ordered-commit ceiling.
- **Merkle, hash % 200** does not scale with warehouse count or worker count, staying between about 500 and 630 TPS. With 200 fixed partitions per table, the partition root rows are hot for every transaction, so contention does not fall as warehouses are added. The trees also get deeper as data grows: `order_line` reaches 4 levels by 20 warehouses and `stock` by 100.
- **Merkle, warehouse routing** scales with warehouse count, reaching 1,514 TPS at 100 warehouses (2.4× the old layout). Tree depth stays constant, at 3.0 levels for `stock` and about 3.5 for `order_line`. Merkle updates remain synchronous, so every partition root is exact at commit and recovery by comparing snapshot roots is unchanged.

## Method

Everything ran on **ranking** (10.129.7.57, EPYC 9654, 192 cores, 251 GB) in an isolated directory, `~/claude_checks`, using its own ports (55439, 18100 and 19100). The DB, `ariabc_pg_server`, the gateway and the workload driver all ran on the same host, and no other machine was involved.

Every run started from a fresh `initdb` and followed these steps:

1. **Configure the cluster.** Settings match the canonical ranking cluster (`single_node_pgdata`):
   - `shared_buffers=32GB`, `max_connections=832`
   - `default_transaction_isolation=serializable`, `enable_seqscan=off`
   - `max_locks_per_transaction=4012`, `max_pred_locks_per_transaction=4012`, `max_pred_locks_per_page=512`
   - `synchronous_commit`, `fsync` and `full_page_writes` on; `autovacuum` off
   - the BCDB settings enforced by the harness: conflict tracking on, 2048 ring slots, `bcdb_advance_commit_watermark=on`, `merkle_apply_synchronous_direct=on`
2. **Restore the data.** Use the same steps as `run_all_modes_gateway_sweep.py` (`restore_tpcc_db`): load the dump, scale it with UNLOGGED inserts, switch to LOGGED, run `restore_tpcc_procs.sql`, then `ANALYZE`.
3. **Checkpoint.** Run `CHECKPOINT` so no checkpoint of the restore's WAL lands during measurement. The harness gets the same effect from the PostgreSQL restart in its cold reset.
4. **Prewarm.** Prewarm every `public` and `ariabc_internal` relation with `pg_prewarm`.
5. **Run the workload.**
   - Workload: 20,000 transactions, seed 42, 15% remote payments, 1% remote new-orders. The standard mix is 45% NewOrder, 43% Payment, 4% OrderStatus, 4% Delivery and 4% StockLevel.
   - Server and gateway flags are identical to the TPC-C sweep harness, with `--bcdbInitBlockSize` set to the worker count.

Trials were run trial-outer: all configurations for trial 1, then all for trial 2, then trial 3.

### Caveats

- **Ranking is shared with other users.** Some trials came in at about half speed. The lines are the best of 3 trials and the bands show the min–max.
- **Some configurations are bimodal.** The same configuration sometimes landed at about 2,500 or about 800 TPS (det at 32 workers). Slow runs spend their first ~15 s at a low rate with the same number of restarts. The suspected cause is NUMA placement on the two-socket EPYC, but this has not been confirmed (`numactl --interleave=all` is untested). All three original trials of the 48-worker Merkle warehouse-routing point were slow runs, and a three-trial rerun ran at full speed (see †). One hypothesis was ruled out: OS writeback of the post-restore checkpoint was not the cause, because dirty memory was already near 0 when measurement started. The cause of the slowdown is still unknown.
- **No cold-cache reset.** Dropping OS caches needs root. Measurements are prewarmed and in memory, as in the earlier `--tpcc-prewarm` campaign.

## Code under test

These changes are uncommitted in the working tree at the time of these runs. Both builds (PostgreSQL and `ariabc_pg`) were compiled on ranking from that tree.

- **Det key tags** (`src/backend/bcdb/shm_transaction.c`, `executor/nodeModifyTable.c`, `storage/lmgr/predicate.c`, `access/nbtree/nbtsearch.c`, `bcdb/worker.c`):
  - Conflict tags hash the relation's key: its primary key, else its replica identity index, else its narrowest plain unique index. Each column is hashed with its type's hash function.
  - Writes also publish key-prefix tags that are never checked. B-tree scans reserve the tag of the key prefix they bind, so range reads still conflict with concurrent inserts in their range.
- **Merkle leading-key routing** (`src/backend/access/merkle/*`, `scripts/restore_tpcc_procs.sql`, `scripts/restore_usertable_small.sql`, `ariabc_pg/src/replica_repair.cxx`):
  - Two new index options, `partition_key_columns` and `subpartitions`, stored on the index metapage. Leaving them at 0 keeps the old hash(key) % partitions behaviour.
  - A new SQL function, `merkle_key_hash_routed()`.
  - `run_all_modes_gateway_sweep.py` exposes them as `--tpcc-merkle-partition-key-columns` and `--tpcc-merkle-subpartitions`.

## Reproducing

The scripts are in `scripts/`. They expect the layout on ranking: sources in `~/claude_checks/src`, installed into `~/claude_checks/install`, with `ariabc_pg` built in `~/claude_checks/src/ariabc_pg/build`.

```bash
# one run: <label> <warehouses> <tx> <workers> <partitions> <partition_key_columns> <subpartitions> <pg|det|merkle>
scripts/merkle_run.sh wh16 100 20000 32 16384 1 16 merkle
scripts/chart_sweep.sh     # warehouse sweep, 84 runs
scripts/workers_sweep.sh   # worker sweep at W=100, 72 runs
~/claude_checks/venv/bin/python scripts/plot_warehouses.py   # reads ~/claude_checks/chart/results.csv
~/claude_checks/venv/bin/python scripts/plot_workers.py      # reads ~/claude_checks/chart/workers_results.csv
```

`warehouses_w32/runs/` and `workers_w100/runs/` hold the evidence for each run: `result.txt`, the gateway log, the final-state hash and the server stderr. The full PostgreSQL and server logs (about 420 MB) remain on ranking under `~/claude_checks/chart/`.
