# merkle_lsm — OOM 100M YCSB workload D (2026-10-19)

Merkle mode (`bcdb_merkle`) with the lookup index `usertable_merkle_lookup_idx` as
`merkle_lsm` (3 levels, L0 1 MiB, size ratio 16). Source: main at 3a382713 plus the
uncommitted working tree of that day. Only LSM runs; the B-tree comparison is the
2026-10-15 D runs in `../lsm3_experiment_20261014/ycsb_d_run_20261015/`.

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

## Comparison with the old B-tree D runs — read with care

Old B-tree D (2026-10-15): 10,293 / 10,793 TPS; old lsm3: 11,474 / 11,455 TPS. Those
runs were only 20K statements (~1.8 s), dominated by cold-start ramp-up. The
~17.5K TPS here comes from 2.4M-statement runs, so the gap is mostly run length,
not the index. A like-for-like LSM vs B-tree number needs a B-tree run of the same
length (`run_d.sh` with `--merkle-lookup-am btree` on `base_ff90`).

## Files

`campaign/` harness output (summary.csv, per-trial result/setup/io/checkpoint JSON);
`lsm_sampler_t2_t3.jsonl` per-second merge state, sizes, commits, WAL LSN;
`run_d.sh`, `lsm_sampler.py` the exact controller and sampler; `baseline_convert.txt`.
