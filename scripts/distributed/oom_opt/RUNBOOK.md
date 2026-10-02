# Task H: measure WAL compression first, then a fair HOT baseline

These are instructions for the orchestrator, not evidence of executed tests.
Keep PostgreSQL/server on `neel@10.129.148.247`, controller/gateway on `.111`.
Freeze the runner, its dependencies, this directory and the selected remote
PostgreSQL installation before each campaign. No build is needed for either
treatment. Never edit or start a golden baseline or the canonical
`~/Desktop/ariabc_cluster/.bench_tmp/single_node_pgdata`.

Before running, the orchestrator should perform these syntax checks on `.247`
in the synced checkout; Task H did not run them on the workstation:

```bash
python3 -m py_compile scripts/distributed/oom_opt/analyze.py \
  scripts/distributed/oom_opt/run_wal_compression.py
bash -n scripts/distributed/oom_opt/prepare_fillfactor90.sh
```

## 1. No-rebuild WAL compression experiment

Published and new S1024 setup JSON both have `wal_compression=off`.
`wal_compression=on` preserves `fsync`, `full_page_writes`, and synchronous
commit; PG13 compresses only WAL full-page images with PGLZ and reconstructs
them during redo. It changes the benchmark configuration, so report a new
paired experiment and apply it to det and to pg if pg is compared.

`run_wal_compression.py` imports **the existing**
`scripts/distributed/run_oom_100m_benchmark.py`. Its only treatment is an
explicit `wal_compression` line in the restored working copy's
`postgresql.auto.conf`, before both reset-phase startups. It validates the
effective setting, rejects `pg_rc` and dataset generation, adds wrapper/config
identity to the stock campaign/resume source contract, and adds treatment proof
to each setup JSON. Baseline validation/derivation run unchanged. The parent
runner still restores physically, settles the SSD, drops caches, runs the same
workload, retries SERIALIZABLE failures, checkpoints separately, and verifies
Merkle. Later checkpoint/verify restarts reuse the treated working config.

On the controller, after syncing the new files and runner dependencies:

```bash
cd /home/neel/ARIABC/AriaBC
set -euo pipefail
OOM_H_TAG=$(date -u +%Y%m%dT%H%M%SZ)
OOM_H_OUT=scripts/bench_full_results/oom_opt/$OOM_H_TAG
OOM_H_COMMON=(
  --remote-host 10.129.148.247 --remote-user neel
  --remote-dir /tmp/ariabc_oom_100m --db-port 5438 --server-port 8000
  --cluster-dir /home/neel/Desktop/ariabc_cluster
  --gateway-host 10.129.27.111 --gateway-user neel
  --gateway-repo /home/neel/ARIABC/AriaBC
  --install-dir /home/neel/claude_opt/install_opt
  --db-rows 100000000 --shared-buffers 32MB --txs 20000 --seed 42
  --workers 1 16 --combos a:0.0 f:0.99 --trials 1 --skip-gen
  --reset-mode delta --delta-content-check sampled --verify-mode full
  --gateway-timeout 1800 --reset-timeout 3600 --verify-timeout 1800
  --settle-factor 1.25 --settle-min-s 20 --settle-max-s 1800
  --calibrate-min-wait-s 300)
OOM_H_WRAPPER=(python3 -u scripts/distributed/oom_opt/run_wal_compression.py)
OOM_H_MERKLE=(--base-dir-name pgdata_base_f32s1024 --modes bcdb_merkle)
OOM_H_DET=(--base-dir-name pgdata_base_fanout32_tblnamed --modes bcdb_det)
mkdir -p "$OOM_H_OUT"
for OOM_H_WAL in off on; do
  "${OOM_H_WRAPPER[@]}" --wal-compression "$OOM_H_WAL" "${OOM_H_COMMON[@]}" \
    "${OOM_H_MERKLE[@]}" --dry-run --out-dir "$OOM_H_OUT/dry_merkle_$OOM_H_WAL"
  "${OOM_H_WRAPPER[@]}" --wal-compression "$OOM_H_WAL" "${OOM_H_COMMON[@]}" \
    "${OOM_H_DET[@]}" --dry-run --out-dir "$OOM_H_OUT/dry_det_$OOM_H_WAL"
done
"${OOM_H_WRAPPER[@]}" --wal-compression off "${OOM_H_COMMON[@]}" \
  "${OOM_H_MERKLE[@]}" --preflight-only --out-dir "$OOM_H_OUT/preflight_merkle"
"${OOM_H_WRAPPER[@]}" --wal-compression off "${OOM_H_COMMON[@]}" \
  "${OOM_H_DET[@]}" --preflight-only --out-dir "$OOM_H_OUT/preflight_det"
# Counterbalance whole-campaign order: det off->on, Merkle on->off.
"${OOM_H_WRAPPER[@]}" --wal-compression off "${OOM_H_COMMON[@]}" \
  "${OOM_H_DET[@]}" --out-dir "$OOM_H_OUT/det_off" 2>&1 | tee "$OOM_H_OUT/det_off.log"
"${OOM_H_WRAPPER[@]}" --wal-compression on "${OOM_H_COMMON[@]}" \
  "${OOM_H_DET[@]}" --out-dir "$OOM_H_OUT/det_on" 2>&1 | tee "$OOM_H_OUT/det_on.log"
"${OOM_H_WRAPPER[@]}" --wal-compression on "${OOM_H_COMMON[@]}" \
  "${OOM_H_MERKLE[@]}" --out-dir "$OOM_H_OUT/merkle_on" 2>&1 | tee "$OOM_H_OUT/merkle_on.log"
"${OOM_H_WRAPPER[@]}" --wal-compression off "${OOM_H_COMMON[@]}" \
  "${OOM_H_MERKLE[@]}" --out-dir "$OOM_H_OUT/merkle_off" 2>&1 | tee "$OOM_H_OUT/merkle_off.log"
```

This is **16 measured cases**: four points × two modes × two WAL settings.
The same frozen optimized install is used for det and Merkle to isolate the
configuration; the campaign's older plain heap baseline is retained. If CPU
changes relative to canonical det are also of interest, add an explicit
canonical-install det control, not a silent binary substitution. To compare pg,
repeat both det commands with `--modes pg` and distinct output directories.
Never use `pg_rc`.

For the smallest gate, use just A θ0 w1 off/on first (four mode/treatment
cases), then proceed to the other three points. On the common arrays above,
append `--combos a:0.0 --workers 1` so argparse uses those last values.
Repeat the apparent winner and its off control at least three cold trials
before a throughput claim, using a fresh output root and reversed treatment
order. One trial is exploratory. Expect lower WAL/device writes; SSD reads and
checkpoint data-page bytes should remain similar. A/F TPS could improve by
roughly 0–15%, but PGLZ CPU can lose performance; compression ratio and fsync
cost are not measured in the old artifacts. Adopt only after paired results.

Resume only with the same wrapper and treatment:

```bash
# Replace this path with the actual run directory printed by that invocation.
"${OOM_H_WRAPPER[@]}" --wal-compression on "${OOM_H_COMMON[@]}" \
  "${OOM_H_MERKLE[@]}" --resume-dir "$OOM_H_OUT/merkle_on/run_<timestamp>_<uuid>"
```

Runbook `oom_s1024/RUNBOOK.md` section B has the exact remote loader checks.
Preserve its ELF/loader, binary hash, configure, baseline and source evidence.
Inspect each `setup.json` for the WAL treatment, SERIALIZABLE, synchronous
Merkle apply, full-page writes, fsync and synchronous commit. Accept results
only with completed-query count, `divergence_count=0`, `permanent_failures=0`,
no failed/cleanup-error files, and Merkle `merkle_verify.txt=t` / PASS.
This single-node benchmark has no distributed post-marker/all-three check;
do not claim distributed acceptance from it.

## 2. Fillfactor 90 requires a physically rewritten common baseline

`ALTER TABLE ... SET (fillfactor=90)` alone does not create free space in existing
full pages. Use the provided `prepare_fillfactor90.sh` on `.247`, under a fresh
`/tmp/ariabc_oom_ff90_<tag>` root. It keeps the original baseline, verifies roots
before/after, drops Merkle/lookup in the copy, rewrites the heap/PK once,
recreates the lookup and S1024 tree once, verifies keyspace/geometry/full roots,
and shuts down cleanly. Its explicit geometry options avoid reliance on global
defaults. It uses 32MB shared buffers, 512MB maintenance memory, no parallel
maintenance and requires 12GiB available memory for the custom build arrays.

It requires **200GiB free on the same filesystem** as a conservative allowance
for all retained golden/plain/working/check copies, temporary rewrite/sort files
and WAL. The reported 62GB free cannot support this procedure while preserving
the existing artifacts. Provision additional same-device capacity first; a
different device invalidates the comparison to the QLC campaign. The estimate
is deliberately conservative; an orchestrator may separately design and
document a smaller sequential storage plan, but must not delete old baselines
or artifacts to make this script pass.

On `.247`, after provisioning sufficient capacity and syncing the script:

```bash
cd /home/neel/ARIABC/AriaBC
OOM_H_FF_ROOT=/tmp/ariabc_oom_ff90_$(date -u +%Y%m%dT%H%M%SZ)
bash scripts/distributed/oom_opt/prepare_fillfactor90.sh "$OOM_H_FF_ROOT"
```

Copy its `preparation/` evidence back into the controller campaign archive.
Expect heap growth from 25.60GB (23.84GiB) to approximately 28.4–29.3GB
(26.5–27.3GiB), accounting for integral tuples/page; PK ~2.1GiB and lookup ~3GiB
stay similar. Allow roughly **1–4 hours** for copy, heap rewrite, PK/lookup
sort/build, Merkle build and full verification; this is unmeasured planning
time, and an exhausted QLC SLC cache can take longer. Merkle build custom
arrays peak around 8.94GiB; `maintenance_work_mem` does not limit them.

On `.111`, using the WAL common arrays from section 1, set the exact new root
printed by `.247` and freeze it. The stock runner derives a matching
`pgdata_base_f32s1024_ff90_plain` and benchmarks all modes against the rewritten
heap. Keep WAL off to isolate HOT, then optionally cross WAL on separately.

```bash
# Set to the exact .247 root from preparation, not a new timestamp here.
OOM_H_FF_ROOT=/tmp/ariabc_oom_ff90_<tag_from_247>
python3 -u scripts/distributed/run_oom_100m_benchmark.py "${OOM_H_COMMON[@]}" \
  --remote-dir "$OOM_H_FF_ROOT" --base-dir-name pgdata_base_f32s1024_ff90 \
  --modes bcdb_det bcdb_merkle pg --dry-run --out-dir "$OOM_H_OUT/ff90_dry"
python3 -u scripts/distributed/run_oom_100m_benchmark.py "${OOM_H_COMMON[@]}" \
  --remote-dir "$OOM_H_FF_ROOT" --base-dir-name pgdata_base_f32s1024_ff90 \
  --modes bcdb_det bcdb_merkle pg --preflight-only --out-dir "$OOM_H_OUT/ff90_preflight"
python3 -u scripts/distributed/oom_opt/run_wal_compression.py --wal-compression off \
  "${OOM_H_COMMON[@]}" --remote-dir "$OOM_H_FF_ROOT" \
  --base-dir-name pgdata_base_f32s1024_ff90 --modes bcdb_det bcdb_merkle pg \
  --out-dir "$OOM_H_OUT/ff90_off" 2>&1 | tee "$OOM_H_OUT/ff90_off.log"
```

This adds 12 one-trial cases if pg is included, eight otherwise. Compare to
fresh fillfactor100 off controls for **each mode**, not only historical det.
Measure usertable HOT fraction rather than assuming 90% fillfactor guarantees
HOT with high concurrency/long snapshot horizons. Merkle must still update
every leaf and ancestor when the user update is HOT. Node fillfactor and tree
geometry must be the same in both controls; if a rebuild changes node layout,
prepare a fillfactor100 copy with the same rebuild/layout treatment first, or
label the experiment as combined heap+node preparation rather than a HOT-only
claim. The pending compact node rebuild code must not enter only one side.

## 3. Attribution follow-up on dedicated remote copies

The old campaigns do not contain per-relation CPU/WAL counters. For one
A θ0 w1 and one F θ0.99 w16 **separate profiling run**, record
`relation_snapshot.sql` immediately before/after the workload, before the
checkpoint/full verification. Poll collector counters until the known
successful update/insert total is published; use `pg_stat_clear_snapshot()` in
fresh autocommit queries. Subtract the snapshots for node heap/index/lookup/PK
buffer reads and HOT fractions. Buffer misses include OS-cache hits; use device
stats for actual SSD bytes. Both DET and plain SQL should be distinguished.

Record start/end `pg_current_wal_insert_lsn()` around the workload. After the
end, flush WAL with a checkpoint **outside the measured window**, retain the
segment files covering that interval before they recycle, and run on `.247`:

```bash
# Substitute actual recorded LSNs and stopped profiling-copy WAL directory.
/home/neel/claude_opt/install_opt/bin/pg_waldump \
  --path=/tmp/<dedicated_profile_root>/<working_copy>/pg_wal \
  --start='<start_lsn>' --end='<end_lsn>' --stats > wal.stats.txt
/home/neel/claude_opt/install_opt/bin/pg_waldump \
  --path=/tmp/<dedicated_profile_root>/<working_copy>/pg_wal \
  --start='<start_lsn>' --end='<end_lsn>' --bkp-details > wal.records.txt
```

Map `rel tablespace/database/relfilenode` to the snapshot's relfilenodes, count
FPI **stored** lengths separately from resource-manager record bytes, and do
not count an FPI once per record and again as heap/index bytes. PostgreSQL 13
has no `pg_stat_wal` view; use the LSN byte difference and pg_waldump. Record
build CPU profile counters already in `shm_transaction.c`, or an external
remote sampling profile, for row-hash/route/prep/node-apply timings. Sampling
and telemetry are overlapping observations, not sums of workload wall time.
Repeat cold and warmed relation probes separately; neither substitutes for the
four matched accepted gateway points.

## 4. Reproduce this task's local analysis (data only)

```bash
python3 scripts/distributed/oom_opt/analyze.py \
  --comparison .bench_tmp/oom_s1024_20261001/comparison/comparison.csv \
  --published Final_Results/OOM_100M/summary.csv \
  --out-dir .bench_tmp/codex_tasks/oom_h_analysis_<fresh_tag>
```

It reads raw result/setup/io/checkpoint evidence and SQL hashes for all 24
pairs, writes detailed CSV and controls/settings/read-only JSON, and refuses
to overwrite an analysis directory. Mutation-normalized differences are
**equivalents for mixed workloads**, not individually timed update latencies.
