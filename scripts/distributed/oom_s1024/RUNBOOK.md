# OOM 100M: fanout 32, split 1024, merge 256, optimized PostgreSQL

Prepared from the current runner and the published `Final_Results/OOM_100M`
artifacts. These commands have **not been executed** by Task E. Run sequentially;
keep the canonical install and both published baselines unchanged. Sync this
runner, `oom_s1024/`, and its existing Python/SQL dependencies to the controller
checkout before starting. Freeze those sources for the campaign: resume checks
hashes of the benchmark sources, including distributed scripts.

## Path and binary contract

`--install-dir /home/neel/claude_opt/install_opt` selects `.247`'s `postgres`,
`pg_ctl`, `psql`, `pg_controldata`, `pg_isready`, and the lib directory used by
SQL, telemetry, and the C++ server. It does **not** build, sync, or select the
C++ executables. The server is
`--cluster-dir/ariabc_pg/build/bin/ariabc_pg_server` on `.247`; the gateway is
`--gateway-repo/ariabc_pg/build/bin/ariabc_pg_gateway` on `.111`.
Defaults below preserve both canonical C++ paths. The server speaks libpq to
PostgreSQL, rather than loading a backend ABI; CMake links the server to libpq
and the gateway does not link to libpq. Published COMMANDS.md says that server
was built on `.111` against `.247`'s install because `.247` lacks a C++ compiler.
The runner cannot prove that historical build provenance from the executable.

With the stipulated same PG13 configure flags and complete optimized install,
both det and Merkle can use `install_opt`; there is no backend ABI link from
the C++ server. The run here deliberately uses canonical PostgreSQL for det
drift controls. `LD_LIBRARY_PATH` on `.247` prepends `install_opt/lib`, then
`/home/neel/Desktop/rdkafka_local/lib`; loader compatibility still needs the
checks below, particularly any absolute ELF RPATH. On `.111` the gateway
inherits its existing environment; the runner never applies `--install-dir`
there. `$libdir` and installed share/extension paths follow the selected
PostgreSQL install. Hardcoded absolute paths in copied configuration or catalog
entries would need separate review; the runner does not rewrite those paths.
Merkle functions are backend builtins, not an extension installed by this runner.

`--base-dir-name` is a basename under `/tmp/ariabc_oom_100m`. In delta mode,
Merkle uses that name and pg/det use `<name>_plain`; missing/stale plain copies
are rebuilt from that golden baseline with both usertable Merkle indexes dropped.
No new plain-name option is needed for these **separate** campaigns: the controls
use the old base name, hence the existing old `_plain` copy and its original
derivation manifest. Combining controls with the new Merkle base name would
derive a new plain baseline instead.

## A. Rebuild on .247 only

If the orchestrator has already completed this baseline, skip this rebuild and
retain its rebuild/verification evidence. The following block requires a fresh
destination and refuses to overwrite a prepared baseline. It operates on its
own copy and port **5449**, never the canonical single_node_pgdata. An existing
unrebuilt copy made by the orchestrator can instead be used by omitting only
the `test ! -e "$OOM_BASE"` and `cp` lines after confirming it is that pristine,
stopped copy. Do not rerun the index rebuild against a campaign baseline.

Merkle build retains about 48 B per row (~4.8 GB / 4.47 GiB for 100M), and
`merkle_prepare_sorted_entries` temporarily retains both the old and sorted
arrays (~9.6 GB / 8.94 GiB). `maintenance_work_mem` does **not** cap those custom
allocations. Use 32MB buffers, 512MB maintenance memory, 4MB work memory, no
parallel maintenance, and at least 12 GiB MemAvailable before starting. Avoid
other workloads during the build. Do not copy the old bulk-loader's 4GB buffer
and 6GB maintenance settings onto this 15GB host.

```bash
ssh neel@10.129.148.247
set -euo pipefail
export LC_ALL=C
OOM_ROOT=/tmp/ariabc_oom_100m
OOM_OLD=$OOM_ROOT/pgdata_base_fanout32_tblnamed
OOM_BASE=$OOM_ROOT/pgdata_base_f32s1024
OOM_INSTALL=/home/neel/claude_opt/install_opt
OOM_PORT=5449
OOM_EVIDENCE=$OOM_ROOT/rebuild_f32s1024_$(date -u +%Y%m%dT%H%M%SZ)
export LD_LIBRARY_PATH="$OOM_INSTALL/lib:/home/neel/Desktop/rdkafka_local/lib:${LD_LIBRARY_PATH:-}"
test ! -d "$OOM_ROOT/benchmark.lock"
test -f "$OOM_OLD/PG_VERSION"
test ! -f "$OOM_OLD/postmaster.pid"
"$OOM_INSTALL/bin/pg_controldata" "$OOM_OLD" | grep -Eq 'Database cluster state:[[:space:]]+shut down$'
if fuser "$OOM_PORT"/tcp >/dev/null 2>&1; then exit 1; fi
awk '/MemAvailable:/ {if ($2 < 12582912) exit 1; ok=1} END {if (!ok) exit 1}' /proc/meminfo
free -h
df -h "$OOM_ROOT"
test ! -e "$OOM_BASE"
mkdir "$OOM_EVIDENCE"
cp -a --reflink=never "$OOM_OLD" "$OOM_BASE"
cp -a "$OOM_BASE/postgresql.auto.conf" "$OOM_EVIDENCE/original.auto.conf"
cat > "$OOM_BASE/postgresql.auto.conf" <<'CONF'
port = 5449
listen_addresses = '127.0.0.1'
shared_buffers = '32MB'
maintenance_work_mem = '512MB'
work_mem = '4MB'
max_parallel_maintenance_workers = 0
max_parallel_workers_per_gather = 0
bcdb_worker_count = 1
bcdb_ledger_trace = off
enable_merkle_index = on
merkle_apply_synchronous_direct = on
autovacuum = off
fsync = on
full_page_writes = on
synchronous_commit = on
checkpoint_timeout = '30min'
max_wal_size = '20GB'
CONF
oom_stop() {
    "$OOM_INSTALL/bin/pg_ctl" -D "$OOM_BASE" -w -t 120 stop -m fast
}
trap 'oom_stop || true' EXIT
"$OOM_INSTALL/bin/pg_ctl" -D "$OOM_BASE" -l "$OOM_EVIDENCE/postgres.log" -w -t 120 start
OOM_PSQL=("$OOM_INSTALL/bin/psql" -X -v ON_ERROR_STOP=1 -h 127.0.0.1 -p "$OOM_PORT" -U postgres -d postgres)
"${OOM_PSQL[@]}" -Atc "SELECT pg_get_indexdef('usertable_merkle_lookup_idx'::regclass);" > "$OOM_EVIDENCE/lookup.before.txt"
"${OOM_PSQL[@]}" <<'SQL' | tee "$OOM_EVIDENCE/rebuild.txt"
\timing on
BEGIN;
DROP INDEX usertable_merkle_idx;
CREATE INDEX usertable_merkle_idx ON usertable USING merkle (ycsb_key)
    WITH (partitions=200, fanout=32);
COMMIT;
ANALYZE ariabc_internal.merkle_node_usertable;
SELECT pg_relation_size('ariabc_internal.merkle_node_usertable') AS node_heap_bytes,
       pg_total_relation_size('ariabc_internal.merkle_node_usertable') AS node_total_bytes;
SELECT count(*) AS nodes, count(*) FILTER (WHERE is_leaf) AS leaves
FROM ariabc_internal.merkle_node_usertable;
SELECT prefix_len, is_leaf, count(*) AS nodes, sum(tuple_count) AS tuple_counts
FROM ariabc_internal.merkle_node_usertable GROUP BY prefix_len, is_leaf ORDER BY prefix_len, is_leaf;
SQL
"${OOM_PSQL[@]}" -Atc "SELECT merkle_tree_stats('usertable');" > "$OOM_EVIDENCE/tree_stats.json"
python3 - "$OOM_EVIDENCE/tree_stats.json" <<'PY'
import json, sys
s = json.load(open(sys.argv[1]))
assert all(s[k] == v for k, v in dict(partitions=200, fanout=32, split_threshold=1024, merge_threshold=256).items()), s
assert s['total_nodes'] > 0, s
print(s)
PY
"${OOM_PSQL[@]}" -Atc "SELECT merkle_verify('usertable');" > "$OOM_EVIDENCE/merkle_verify.txt"
test "$(cat "$OOM_EVIDENCE/merkle_verify.txt")" = t
"${OOM_PSQL[@]}" -Atc "SELECT pg_get_indexdef('usertable_merkle_lookup_idx'::regclass);" > "$OOM_EVIDENCE/lookup.after.txt"
cmp "$OOM_EVIDENCE/lookup.before.txt" "$OOM_EVIDENCE/lookup.after.txt"
"${OOM_PSQL[@]}" -c 'CHECKPOINT;'
oom_stop
trap - EXIT
cp -a "$OOM_EVIDENCE/original.auto.conf" "$OOM_BASE/postgresql.auto.conf"
rm -f "$OOM_BASE/postmaster.opts"
sync
"$OOM_INSTALL/bin/pg_controldata" "$OOM_BASE" > "$OOM_EVIDENCE/pg_controldata.txt"
grep -Eq 'Database cluster state:[[:space:]]+shut down$' "$OOM_EVIDENCE/pg_controldata.txt"
sha256sum "$OOM_INSTALL/bin/postgres" "$OOM_BASE/global/pg_control" "$OOM_BASE/PG_VERSION" > "$OOM_EVIDENCE/sha256.txt"
du -sb "$OOM_BASE" > "$OOM_EVIDENCE/baseline_bytes.txt"
printf '%s\n' "$OOM_EVIDENCE"
exit
```

Expected analytical geometry is approximately 211,400 visible nodes and 204,800
leaves, versus ~6.78M old nodes. Record the actual values; do not hardcode those
estimates as validation limits. `merkleBuild` clears old node rows using DELETE,
so a lower visible count alone does **not** prove a smaller physical heap/index
footprint. The block records both sizes. This runbook adds no CLUSTER/VACUUM FULL
layout treatment to the requested index rebuild. If physical bloat remains,
decide and document a separate baseline preparation treatment before measuring;
do not silently change it during the campaign. `ANALYZE` is necessary for the
runner's positive-relpages check, but does not remove bloat.

## B. Loader checks, dry run, and preflight

Read-only loader checks on `.247` (do not build or replace any binaries):

```bash
ssh neel@10.129.148.247 'bash -s' <<'SH'
set -euo pipefail
OOM_INSTALL=/home/neel/claude_opt/install_opt
export LD_LIBRARY_PATH="$OOM_INSTALL/lib:/home/neel/Desktop/rdkafka_local/lib:${LD_LIBRARY_PATH:-}"
"$OOM_INSTALL/bin/postgres" --version
"$OOM_INSTALL/bin/pg_config" --configure
"$OOM_INSTALL/bin/pg_config" --pkglibdir
"$OOM_INSTALL/bin/pg_config" --sharedir
test -d "$("$OOM_INSTALL/bin/pg_config" --pkglibdir)"
test -d "$("$OOM_INSTALL/bin/pg_config" --sharedir)/extension"
for OOM_BIN in "$OOM_INSTALL/bin/postgres" /home/neel/Desktop/ariabc_cluster/ariabc_pg/build/bin/ariabc_pg_server; do
    OOM_LDD=$(ldd "$OOM_BIN")
    printf '%s\n' "$OOM_LDD"
    if printf '%s\n' "$OOM_LDD" | grep -F 'not found'; then exit 1; fi
done
sha256sum "$OOM_INSTALL/bin/postgres" "$OOM_INSTALL/lib/libpq.so.5" /home/neel/Desktop/ariabc_cluster/ariabc_pg/build/bin/ariabc_pg_server
SH
```

Run the controller on `.111`, with the updated checkout. This host runs only the
runner/driver and gateway; PostgreSQL and the C++ server remain on `.247`.
Define these arrays in one Bash session and keep that session for section C:

```bash
ssh neel@10.129.27.111
cd /home/neel/ARIABC/AriaBC
set -euo pipefail
OOM_TAG=$(date -u +%Y%m%dT%H%M%SZ)
OOM_OUT=scripts/bench_full_results/oom_s1024/$OOM_TAG
OOM_COMMON=(python3 -u scripts/distributed/run_oom_100m_benchmark.py
  --remote-host 10.129.148.247 --remote-user neel
  --remote-dir /tmp/ariabc_oom_100m --db-port 5438 --server-port 8000
  --cluster-dir /home/neel/Desktop/ariabc_cluster
  --gateway-host 10.129.27.111 --gateway-user neel
  --gateway-repo /home/neel/ARIABC/AriaBC
  --db-rows 100000000 --shared-buffers 32MB --txs 20000 --seed 42
  --trials 1 --skip-gen --reset-mode delta --delta-content-check sampled
  --verify-mode full --gateway-timeout 1800 --reset-timeout 3600 --verify-timeout 1800
  --settle-factor 1.25 --settle-min-s 20 --settle-max-s 1800 --calibrate-min-wait-s 300)
OOM_MERKLE=(--install-dir /home/neel/claude_opt/install_opt
  --base-dir-name pgdata_base_f32s1024 --modes bcdb_merkle
  --combos a:0.0 a:0.99 a:1.2 b:0.99 d:0.99 f:0.99 --workers 1 4 8 16)
OOM_DET=(--install-dir /home/neel/Desktop/ariabc_install
  --base-dir-name pgdata_base_fanout32_tblnamed --modes bcdb_det
  --combos a:0.99 f:0.99 --workers 1 16)
"${OOM_COMMON[@]}" "${OOM_MERKLE[@]}" --dry-run --out-dir "$OOM_OUT/dry_merkle"
"${OOM_COMMON[@]}" "${OOM_DET[@]}" --dry-run --out-dir "$OOM_OUT/dry_det"
"${OOM_COMMON[@]}" "${OOM_MERKLE[@]}" --preflight-only --out-dir "$OOM_OUT/preflight_merkle"
"${OOM_COMMON[@]}" "${OOM_DET[@]}" --preflight-only --out-dir "$OOM_OUT/preflight_det"
```

Dry run only generates SQL and campaign metadata. Preflight contacts both hosts,
checks executable hashes, binaries, DB/server/Raft ports (5438/8000/9000), sudo
cache-drop access, storage/device identity, and memory; it starts no database
and does not validate the baseline geometry. The campaign validates the baseline
later. Preserve loader-check output alongside campaign evidence.

## C. Campaign and det drift controls

Execute in the same controller Bash session, sequentially. Each command creates
a unique `run_<timestamp>_<uuid>` subdirectory; even an existing output parent
does not overwrite old cases. There are 24 Merkle cases and four det controls.

```bash
mkdir -p "$OOM_OUT"
"${OOM_COMMON[@]}" "${OOM_DET[@]}" --out-dir "$OOM_OUT/det_control" 2>&1 | tee "$OOM_OUT/det_control.log"
"${OOM_COMMON[@]}" "${OOM_MERKLE[@]}" --out-dir "$OOM_OUT/merkle" 2>&1 | tee "$OOM_OUT/merkle.log"
python3 scripts/distributed/oom_s1024/compare.py \
  --new "$OOM_OUT/merkle" --control "$OOM_OUT/det_control" \
  --old-summary Final_Results/OOM_100M/summary.csv --out-dir "$OOM_OUT/comparison"
```

Published det/Merkle campaign used the same base seed, 20k statements, workers,
durability, 96 gateway terminals/client workers, event/direct path and cold/delta
policy. Its `verify_mode` was **fast**; this requested campaign explicitly uses
**full** pre-run keyspace verification. Both modes drop caches after validation,
and every Merkle case verifies the full table afterward. Current generator and
LargeZipf hashes match the archived campaign's hashes; `compare.py` additionally
requires exact per-case workload hashes. If they change, do not treat different
workload bytes as a geometry speedup. Controls are historical det versus fresh
canonical det, not optimized-det measurements. Single trials cannot isolate
geometry from optimized-code effects or establish stable performance rankings.

For an interrupted campaign, inspect its `FAILED.txt`, logs, and the `.247`
`benchmark.lock/owner.txt`. The runner deliberately leaves the lock after an
unclean controller death; remove it only after confirming the owner is dead and
its server/DB instances are stopped. Resume the **specific** run directory with
the same frozen source and binaries:

```bash
# Replace this with the actual run path printed in merkle.log; do not use the dry/preflight run.
OOM_RESUME=$(find "$OOM_OUT/merkle" -mindepth 1 -maxdepth 1 -type d -name 'run_*' | sort | tail -n 1)
test -f "$OOM_RESUME/campaign.json"
"${OOM_COMMON[@]}" "${OOM_MERKLE[@]}" --resume-dir "$OOM_RESUME"
```

## Validation, evidence, and runtime

The runner accepts the new geometry without relaxing any old guard. There is
no split-32, node-count-6.78M, or fixed index-definition check. It checks:

- Clean `pg_controldata` shutdown; version-2 golden manifest matching row count,
  keyspace, system ID, checkpoint LSN, pg_control/PG_VERSION hashes, file count,
  and total bytes. A new named baseline gets its own manifest. Missing/stale
  manifests trigger a full keyspace scan of a temporary physical copy, relation
  sizes/pages, and recorded index definitions. This scan alone is not Merkle
  verification and cached manifests do not hash every relation file.
- Plain derivation provenance and fresh shutdown/control/size metadata; physical
  reset checks (file count, control/version hashes, cp byte size or rsync
  size/mtime/mode), sampled `diff -rq`, keyspace, heap/PK sizes and relpages.
  The golden copy itself is never started or modified by these validations.
- User index mode (presence of USING merkle and lookup index), positive node
  relpages and now a positive visible node count. Observed persisted geometry
  from `merkle_tree_stats`, rather than changed defaults, is saved in setup and
  result JSON. No fixed geometry acceptance restriction is imposed by the runner.
- Effective 32MB buffers, workers, isolation, Merkle enablement, durability,
  canonical BCDB/direct-apply settings, counts/I/O/checkpoint flags; validated
  completions, no divergence or permanent failures, and full Merkle PASS.
- Resume requires version-3 source contract and matching three executable
  hashes/paths, valid saved gateway/server results, no failed/cleanup-error
  evidence, and saved Merkle `t`. Binary hashes are recorded for fresh runs but
  there is no comparison to a predetermined build hash. Source hashes are of
  controller inputs, not proof of the remote optimized PostgreSQL build.

Working directories are `/tmp/ariabc_oom_100m/pgdata_merkle` and `pgdata_plain`;
they are disposable and restored from the stopped baselines. Golden verification
uses `pgdata_golden_check`. A fresh campaign can seed a full physical copy before
delta restores; allow baseline + check/working-copy space and at least 20GiB WAL
headroom. Do not run competing runners in this remote root.

Outputs on `.111` are `$OOM_OUT/{merkle,det_control}/run_*/`: campaign.json,
preflight.txt (three executable hashes), device calibration, workloads,
summary.csv, REPORT.md and per-case setup/result/io/checkpoint JSON, telemetry,
server/gateway/PostgreSQL logs, and Merkle verification. The new setup/result
provenance includes install/cluster/gateway paths, selected baseline manifest,
three executable hashes/hosts/paths and observed Merkle geometry. `result.json`
retains the runner's existing row/summary schema; provenance is separate.
`comparison/{comparison.csv,comparison.md}` contains all historical det/S32/new
S1024 metrics, ratios, drift controls, trials and artifact paths. Comparison reads
accepted summaries and cross-checks result/io/verify evidence, rejecting mismatched
workloads/settings/geometry and failed attempts. Missing new points stay blank.
Keep rebuild evidence on `.247` and copy it to the campaign archive afterward.

TPS uses gateway workload time only. `reset_time_ms` includes restore, validation,
SSD settle, cache drop and startup; the separate checkpoint and post-run Merkle
verify are outside TPS. Full Merkle verify streams/hash-scans 100M rows with
32MB shared buffers, not a 100M-entry build array. It checks the XOR of stored
roots against visible row hashes, not every internal/leaf relationship.

Budget approximately **4–20 minutes per Merkle case** (reset + full keyspace scan
+ workload + checkpoint/restart + full row-hash verification), **1–5 minutes per
det control**, plus at least five-minute idle calibration for each campaign and
one-time check-copy/full validation. A planning allowance is **2–9 hours for the
28 cases**, plus roughly **0.5–2 hours for copy/rebuild/verification**. These are
unmeasured estimates; use the first case/rebuild timing to revise them. Each SSD
settle can take up to 1800s and cold QLC copies/byte comparisons can dominate.
Do not infer total runtime from the 1–22s archived workload intervals. A verify
timeout fails closed; if the first full scan exceeds 1800s, retain the failed
evidence and choose a larger `--verify-timeout` for a new campaign.
