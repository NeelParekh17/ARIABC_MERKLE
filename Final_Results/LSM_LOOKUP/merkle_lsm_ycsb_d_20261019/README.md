# merkle_lsm — OOM 100M YCSB workload D (2026-10-19)

Merkle mode (`bcdb_merkle`) with the lookup index `usertable_merkle_lookup_idx` as
`merkle_lsm` (3 levels, L0 1 MiB, size ratio 16). Source: main at 3a382713 plus the
uncommitted working tree of that day. The same-length B-tree Merkle, det and pg runs are
in `comparison/` (section below).

Setup, as in the 2026-10-15 D runs: 100M rows, fillfactor 90, workload D, Zipf 0.99,
16 workers, SERIALIZABLE, shared_buffers 32 MB, delta restore + cold cache per trial,
DB on .247, gateway on .111. Baseline `base_ff90_mlsm` is a copy of the B-tree
`base_ff90` with only the lookup index rebuilt by `merkle_create_lookup_index()`
(same 3,154,264,064-byte base, same 211,400 Merkle nodes). Difference: each run is
2.4M statements (~2.3 min) instead of 20K, so merges actually happen.

## Results

| Trial | TPS | Run (s) | LSM drain after run (s) | Device read / write (MiB) | Retries | Merkle verify |
|---|---:|---:|---:|---:|---:|---|
| 1 | 17,404 | 137.9 | 7.8 | 5,747 / 2,136 | 0 | PASS |
| 2 | 17,605 | 136.3 | 9.4 | 5,728 / 2,128 | 0 | PASS |
| 3 | 17,576 | 136.6 | 9.4 | 5,723 / 2,115 | 0 | PASS |

Median 17,576 TPS. WAL (trial 3, from sampled `pg_current_wal_lsn`): ~917 MB for
2.40M commits ≈ 381 B/statement.

## Merges (1 Hz sampler, `merkle_lsm_stats`; trial 1 not sampled)

During each run: **5 L0→L1 merges**, one every ~25 s, each 233–251 ms; commits/s
stayed 17–19K through every merge (no visible dip). L1 grew to ~5.0 MB of its
16 MiB target, so no L1→base merge happened during the runs.

The drain (`merkle_lsm_flush`, outside the TPS window) then did one more L0→L1
merge (~210 ms) and two L1→base merges (880–965 ms; in trial 3 the first one,
~5 MB into the cold 3 GB base, took 7.6 s).

## Same-length comparison: B-tree Merkle, det, pg (2026-10-19 evening)

Same harness, same 2.4M-statement workload file (identical `workload_sha256`), D
Zipf 0.99, 16 workers, SERIALIZABLE, delta restore + cold cache. B-tree Merkle
uses the original `base_ff90`; det and pg use `base_ff90_plain` (no Merkle
index). Run as `MODES="bcdb_merkle bcdb_det pg" AM=btree BASE=base_ff90
OUT=ycsb_d_cmp run_d.sh`; output in `comparison/`.

| Mode | Trial TPS | Median TPS | Run (s, median) | Device read / write (MiB, median) | Retries | Merkle verify |
|---|---|---:|---:|---:|---:|---|
| pg | 30,676 / 30,671 / 30,720 | 30,676 | 78.2 | 5,669 / 1,124 | 0 | n/a |
| det (`bcdb_det`) | 18,422 / 18,463 / 18,436 | 18,436 | 130.2 | 5,684 / 1,498 | 0 | n/a |
| Merkle, `merkle_lsm` lookup | 17,404 / 17,605 / 17,576 | 17,576 | 136.6 (+9.4 drain) | 5,728 / 2,128 | 0 | PASS |
| Merkle, B-tree lookup | 13,161 / 13,091 / 12,356 | 13,091 | 183.3 | 6,730 / 3,617 | 0 | PASS |

- `merkle_lsm` vs B-tree lookup: **+34% TPS** (17,576 vs 13,091). Counting the
  post-run drain as run time (2.4M / 145.9 s = 16.4K TPS) it is still +26%.
- Merkle overhead over det drops from 29% (B-tree) to 5% (LSM).
- Device writes drop 41% (3,617 → 2,128 MiB) and reads 15% (6,730 → 5,728 MiB):
  the B-tree lookup index takes random hash-ordered leaf writes, the LSM takes
  sequential L0 appends plus a few merges.
- Backend buffer writes are about the same (~200K for both Merkle variants vs
  ~6–7K for det/pg), so most of the remaining Merkle cost is the node table and
  split work, not the lookup index.
- WAL was not measured for this campaign: the sampler's non-LSM path failed to
  parse (`merkle_lsm_stats` is not installed in these baselines), so only the
  LSM WAL figure above (~381 B/statement) exists. The sampler is fixed on .247
  for future runs.
- The old 20K-statement D numbers (B-tree 10,293 / 10,793, lsm3 11,474 / 11,455 TPS,
  in `../lsm3_experiment_20261014/ycsb_d_run_20261015/`) are ramp-up dominated
  and not comparable with either table.

## Files

`campaign/` LSM harness output; `comparison/` B-tree Merkle/det/pg harness output (logs and
the 151 MB workload file omitted); (summary.csv, per-trial result/setup/io/checkpoint JSON);
`lsm_sampler_t2_t3.jsonl` per-second merge state, sizes, commits, WAL LSN;
`run_d.sh`, `lsm_sampler.py` the exact controller and sampler; `baseline_convert.txt`.
