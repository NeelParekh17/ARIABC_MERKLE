# Online recovery rerun: synchronous Merkle F32 / S1024 / M256

Prepared 2026-10-01. All commands below are for the remote orchestrator. No SSH,
build, PostgreSQL, regression test or benchmark was run by Task I locally.
Use a frozen snapshot of the **entire current working tree**, including other
agents' uncommitted Merkle changes. Do not use HEAD alone or overwrite published
`Final_Results/ONLINE_RECOVERY/{summary.csv,runs,graphs,Report.md}`.

## Why the published replication script must not be run directly

`run_recovery_cluster_test.sh:39` forwards fixed 96 lanes, 8 workers, 160k YCSB,
Raft/Kafka majority_async_all3, 32MB buffers and synchronous Merkle flags to
`run_4node_raft_cluster.sh`. That runner:

| Phase | Build/deployment behavior in the original runner |
|---|---|
| delegation, lines 125–290 | rsync --delete to `.111:/home/neel/ARIABC/AriaBC`; LOCAL_INSTALL_DIR becomes `/home/neel/ARIABC/install` |
| 0.8, 2240–2320 | builds PG and C++ **on the gateway**, using ensure_custom_install_from_repo.sh; changes its source ring constant if needed; writes build manifests |
| 1, 2370–2430 | rsync --delete source to all nodes' `/home/neel/Desktop/ariabc_cluster`; U24 PG install to `/home/neel/Desktop/ariabc_install`; C++ to `ariabc_cluster/ariabc_pg/build/bin` |
| 1.5, 2435–2590 | user4 builds PG on user4; C++ in `/tmp/ariabc_pg_build_u22`, copied to `/home/neel/Desktop/ariabc_pg_build_u22/bin`; uses user4's OpenSSL and glibc |
| 3, 3045 onward | reconfigures/restarts `ariabc_cluster/.bench_tmp/single_node_pgdata`; port 5438; restore drops/recreates usertable_small |
| cleanup / final profiling | broad pkill plus hardcoded fuser port 9000; truncates canonical server logs; fresh Raft namespace and Kafka topic reset |

`--skip-build` skips both gateway and U22 compilation, **not deployment**.
`--skip-sync` skips deployment, **not database restart/restore**. Setting
REMOTE_REPO_ROOT or REMOTE_INSTALL_DIR in the environment does not redirect
the original runner: those variables are assigned unconditionally.

The isolated procedure below avoids those paths, uses port **5448**, Raft
**9018**, client **8018/8018/8019**, its own Raft directory and Kafka topics.
`.247` is also the OOM host: the orchestrator must serialize recovery with OOM
and other resource-heavy work even though the ports differ. Never stop, copy,
restore, or otherwise touch canonical `single_node_pgdata`.

## Preflight and immutable backup (orchestrator)

Choose a new tag once; keep it unchanged on every host. Examples assume neel
on all four machines, as cluster_topology.sh specifies. Use configured SSH
credentials, never put the password in logs. Check password-mode SSH too if
the orchestrator supplies ARIABC_CLUSTER_PASSWORD: node_ssh prefers sshpass.

```bash
export RTAG="$(date -u +%Y%m%dT%H%M%SZ)_I"
export RBASE="/home/neel/Desktop/recovery_s1024_$RTAG"
for host in 10.129.148.247 10.129.148.246 10.129.148.248 10.129.27.111; do
  ssh -o BatchMode=yes -o ConnectTimeout=10 neel@"$host" \
    'hostname; uname -a; free -m; df -h /home/neel /tmp; vmstat 1 5; command -v rsync; command -v python3; ss -ltn'
done
ssh neel@10.129.27.111 \
  'cat /sys/devices/system/clocksource/clocksource0/current_clocksource; timedatectl status'
```

Require `tsc` on .111 and record synchronized clocks on all hosts. If it is
not tsc, let the host administrator/orchestrator correct it before this suite.
Require 5448/9018/8018 (8019 on .248) free, sufficient disk for two builds and
backups, and no swapping on user4 during the actual runs (`vmstat si/so` near
zero). Record user4's MemAvailable, swap, Kafka JVM RSS/heap and resident competing
processes. Do not turn off Kafka or reduce durability to fit memory. Defer the
run if resources are inadequate; report memory-pressure trials separately.

Back up canonical binaries/libraries and manifests **before any staging**,
including .111's install and C++ binaries. This procedure does not modify them;
the archive is an additional recovery option. Archive only the existing paths,
never the canonical PGDATA. On **each host**, with RTAG exported:

```bash
RBK="/home/neel/Desktop/recovery_backup_$RTAG"
test ! -e "$RBK"
mkdir -p "$RBK"
items=()
for p in Desktop/ariabc_install Desktop/ariabc_cluster/ariabc_pg/build/bin \
         Desktop/ariabc_pg_build_u22/bin ARIABC/install ARIABC/AriaBC/ariabc_pg/build/bin; do
  if test -e "/home/neel/$p"; then items+=("$p"); fi
done
test "${#items[@]}" -gt 0
tar -C /home/neel -cpf "$RBK/binaries.tar" "${items[@]}"
sha256sum "$RBK/binaries.tar" > "$RBK/binaries.tar.sha256"
tar -tf "$RBK/binaries.tar" > "$RBK/inventory.txt"
```

Also preserve the published git_status.txt, uncommitted_diff.patch,
source_fingerprint, per-platform SHA256 and manifests, workload/restore scripts
and archived run logs. The existing binary backup represents the host **now**;
it is not evidence that the published 2026-09-27 binaries are still installed.
Published user4 hashes differ from U24 hashes by design, despite matching
source identity. Merely restoring today's binaries cannot recreate an older
source/binary combination if it has already been replaced.

## Stage and build only on .247

The orchestrator transfers the frozen current tree to the new
`.247:$RBASE/repo` (omit .git, result directories, .bench_tmp and build outputs;
retain all source and workload files). Save the originating diff/status and
source identity alongside the experiment. All following build commands run on
**.247**. The original runner's gateway/user4 build phases will be skipped.

user4 requires an Ubuntu 22.04 ABI build. Build it in an Ubuntu 22.04 container
on .247, with host networking; do not compile on user4 or the workstation.
If .247 lacks a compiler, container runtime or required build dependencies,
the orchestrator must provision them before proceeding. This is a real
prerequisite, not an assumption that the published runner resolves it on .247.

On .247, create a build image (or use an equivalent existing pinned image;
record its digest):

```bash
mkdir -p "$RBASE/u22_image"
cat > "$RBASE/u22_image/Dockerfile" <<'EOF'
FROM ubuntu:22.04
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y \
    build-essential python3 flex bison pkg-config libssl-dev zlib1g-dev \
    libreadline-dev libkrb5-dev liblz4-dev libzstd-dev ca-certificates curl \
    binutils rsync && rm -rf /var/lib/apt/lists/*
EOF
docker build -t "recovery-u22:$RTAG" "$RBASE/u22_image"
docker image inspect "recovery-u22:$RTAG" > "$RBASE/u22_image/identity.json"
```

Stage the runner's portable CMake 3.28.3 at
`/tmp/cmake-3.28.3-linux-x86_64` and cached
`/tmp/librdkafka-v2.3.0.tar.gz` (verify archive/version before use). Build
librdkafka **2.3.0 with the older ABI** into the new RBASE/rdkafka, then share
that install on all hosts. Do not replace Desktop/rdkafka_local. On .247:

```bash
mkdir -p "$RBASE/rdkafka_src"
tar -xzf /tmp/librdkafka-v2.3.0.tar.gz -C "$RBASE/rdkafka_src" --strip-components=1
docker run --rm --network host --user "$(id -u):$(id -g)" \
  -v "$RBASE:$RBASE" -w "$RBASE/rdkafka_src" "recovery-u22:$RTAG" \
  bash -c './configure --prefix="$1/rdkafka" && make -j2 && make install' bash "$RBASE"
docker run --rm --network host --user "$(id -u):$(id -g)" \
  -v "$RBASE:$RBASE" -v /tmp/cmake-3.28.3-linux-x86_64:/opt/cmake:ro \
  -e PATH=/opt/cmake/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin \
  -w "$RBASE/repo" "recovery-u22:$RTAG" \
  bash scripts/distributed/recovery_s1024/build_artifacts.sh "$RBASE" u22 \
  > "$RBASE/build_u22.log" 2>&1
export PATH="/tmp/cmake-3.28.3-linux-x86_64/bin:$PATH"
bash "$RBASE/repo/scripts/distributed/recovery_s1024/build_artifacts.sh" "$RBASE" u24 \
  > "$RBASE/build_u24.log" 2>&1
```

The helper builds Release C++, optimized PG and writes SHA/source manifests.
Both artifact build.env files must have the **same source_fingerprint and
ring_capacity**. Do not set an artificial source fingerprint override to make
provenance pass. Missing dependencies or build failure must stop deployment.
The helper and container workflow have not been executed here; the orchestrator
must review and validate them on .247.

Deploy from .247 to all three nodes and .111, to the exact same RBASE paths:
source to RBASE/repo, older-ABI librdkafka to RBASE/rdkafka, appropriate PG install
to RBASE/install. U24 nodes and .111 consume artifacts/u24/bin at
RBASE/repo/ariabc_pg/build/bin; user4 consumes artifacts/u22/bin at
RBASE/u22_build/bin. A remote orchestration loop on .247:

```bash
for host in 10.129.148.247 10.129.148.246 10.129.148.248 10.129.27.111; do
  abi=u24
  if test "$host" = 10.129.148.246; then abi=u22; fi
  dest="$RBASE/repo/ariabc_pg/build/bin"
  if test "$abi" = u22; then dest="$RBASE/u22_build/bin"; fi
  ssh neel@"$host" "mkdir -p '$RBASE/repo' '$RBASE/install' '$RBASE/rdkafka' '$dest'"
  if test "$host" != 10.129.148.247; then
    rsync -a --exclude=.git --exclude=.bench_tmp --exclude=bench_full_results \
      --exclude='build*' --exclude='*.o' --exclude='*.a' \
      "$RBASE/repo/" "neel@$host:$RBASE/repo/"
    rsync -a "$RBASE/rdkafka/" "neel@$host:$RBASE/rdkafka/"
  fi
  rsync -a "$RBASE/install_$abi/" "neel@$host:$RBASE/install/"
  rsync -a "$RBASE/artifacts/$abi/bin/" "neel@$host:$dest/"
  ssh neel@"$host" "mkdir -p '$RBASE/artifacts/u24'"
  rsync -a "$RBASE/artifacts/u24/build.env" "neel@$host:$RBASE/artifacts/u24/build.env"
done
```

Use only fresh directories, no rsync --delete on a canonical path. Record
`ldd` and `postgres --version` / `pg_config --configure` on every host; user4
must resolve libpq, OpenSSL 3 and librdkafka without GLIBC version errors.
For source checks, use source_fingerprint.py with the recorded ring capacity.

## Prepare isolated runner and PostgreSQL

On .111 with RTAG/RBASE exported:

```bash
python3 "$RBASE/repo/scripts/distributed/recovery_s1024/prepare_runner.py" \
  --repo "$RBASE/repo" --tag "$RTAG"
export RUNNER="$RBASE/repo/scripts/distributed/recovery_s1024/cluster_runner_$RTAG.sh"
cat "${RUNNER%.sh}.diff"
bash -n "$RUNNER"
sha256sum "$RUNNER" > "$RBASE/prepared_runner.sha256"
python3 -m venv "$RBASE/repo/.venv"
"$RBASE/repo/.venv/bin/pip" install 'psycopg[binary]'
export PATH="$RBASE/repo/.venv/bin:$PATH"
"$RBASE/repo/.venv/bin/pip" freeze > "$RBASE/python_dependencies.txt"
```

The generated copy redirects **every** canonical PGDATA, install, U22 binary,
remote repo/log and NuRaft log literal; replaces hardcoded cleanup port 9000
with RAFT_PORT; removes three broad pkill lines. Its source runner is untouched,
so the compiler-source fingerprint remains portable. Save the emitted diff
and runner SHA with each run. This is an orchestration-only adaptation, not a
change to recovery or canonical row hashing. The generator fails on recognized
runner drift and never overwrites output. Review any newer cleanup paths before
running; do not blindly reuse it after runner changes.

The generator also creates `serializable_remote_db.py` and
`serializable_fault_injector.py` in the new task directory and redirects the
runner to them. The original injector explicitly assigns READ_COMMITTED
(fault_injector.py:91); its NodeConnection also sets a read-committed default
(remote_db.py:58). The copies replace both settings with SERIALIZABLE and keep
the existing ten-attempt, whole-transaction client retry loop. Their diff is
included in the emitted runner .diff; archive both generated files and hashes.
This makes new fault injection compliant with the strict isolation constraint,
but differs from the published injector's isolation. The same-copy geometry
controls separate that effect from geometry. Provision python3-venv on .111
if absent; the virtual environment install above requires only binary psycopg.

On each DB host, **only against the new RBASE**, before the first run:

```bash
export LD_LIBRARY_PATH="$RBASE/rdkafka/lib:$RBASE/install/lib:${LD_LIBRARY_PATH:-}"
RDATA="$RBASE/repo/.bench_tmp/recovery_pgdata"
test ! -e "$RDATA"
mkdir -p "$RBASE/repo/.bench_tmp"
"$RBASE/install/bin/initdb" -D "$RDATA" -U postgres -A trust
cat >> "$RDATA/postgresql.conf" <<'EOF'
port = 5448
listen_addresses = '*'
max_connections = 256
shared_buffers = '32MB'
default_transaction_isolation = 'serializable'
bcdb_worker_count = 8
merkle_apply_synchronous_direct = on
synchronous_commit = on
fsync = on
full_page_writes = on
EOF
printf 'host all all 10.129.0.0/16 trust\n' >> "$RDATA/pg_hba.conf"
```

Do not copy canonical PGDATA as initialization. The runner starts the isolated
instance, bootstraps Merkle schema, and restores 12,000 rows for each case. Its
BCDB submit path explicitly uses SERIALIZABLE (middleware.c:1219). Keep conflict
tracking=1 and light snapshot=0, no transaction-level READ COMMITTED tuning.
Ordinary repair DML uses BEGIN and inherits the SERIALIZABLE DB default. Snapshot
keepers explicitly use REPEATABLE READ READ ONLY to import the recovery cut;
this is the existing recovery mechanism, not workload isolation. The current
Merkle maintenance internally suppresses SSI temporarily (merkleapply.c:2734);
it restores the outer isolation and snapshot. This pre-existing code needs
the orchestrator's correctness validation; it is not a benchmark isolation knob.

Serialization failures must be retried by the SQL client/executor for the whole
transaction. pg_executor.cxx:3761 and :5984 implement retryable SQLSTATE handling.
The actual det/event path also has worker error handling: retain final retry
and exhaustion counters; any terminal exhaustion/permanent failure invalidates
the run. Do not silently change the workload to avoid 40001.

## Exact scenario commands (on .111, orchestrator only)

Use Kafka brokers already running on .247/.246/.248. Preflight Kafka APIs and
rdkafka 2.3.0 on each host. Do not let the runner reformat/restart shared brokers:
`--skip-kafka --skip-rdkafka-setup` below. Create a **fresh topic per run** using
the same three one-replica partitions as the published colocated setup.
Run A/B/C/M_mixed/L_mix_prio sequentially, at least three repetitions; interleave
baseline and geometry order across repetitions to expose drift.

```bash
export BYPASS_DELEGATION=1 SKIP_BUILD=1 SKIP_SYNC=1
export LOCAL_INSTALL_DIR="$RBASE/install"
export RESULT_RING_CAPACITY="$(sed -n 's/^ring_capacity=//p' "$RBASE/artifacts/u24/build.env")"
export ARIABC_KAFKA_RESULT_BATCH_TARGET_RECORDS=128 ARIABC_KAFKA_ASYNC_RESULT_BATCH_RECORDS=256
export ARIABC_FULL_RESULT_REPLICA_LIMIT=-1 ARIABC_KAFKA_PAYLOAD_FORMAT=text
export BCDB_DET_QUEUE_HIGH_WM=65536 BCDB_DET_QUEUE_LOW_WM=32768
export RECOVERY_INTERVAL_MS=1000 BENCH_COLD_CACHE=0 TX_SIGN=blake3
export CLUSTER_STOP_POSTGRES_ON_EXIT=0
export GATEWAY_STALL_WATCHDOG=1 GATEWAY_STALL_POLL_SECONDS=5 GATEWAY_STALL_MAX_CYCLES=12
export COLLECT_FINAL_SERVER_PROFILE=1
export S=1024 M=256 F=32 REP=1
common=(--threads 96 --det-client-workers 96 --det-client-inflight 16
 --per-thread-window 256 --det-batch-size 256 --pool-size 8
 --server-exec-workers 8 --server-pg-connections 8 --bcdb-workers 8 --bcdb-init-block-size 8
 --bcdb-decouple-workers 1 --conn-fanout 1 --raft-ordered-fanout 1
 --raft-ordering-policy leader-assigned --raft-ordered-batch-append 1
 --raft-ordered-batch-target-entries 64 --raft-ordered-batch-linger-us 1000
 --raft-ordered-coalesce-log 1 --kafka-completion-mode majority_async_all3 --det-window 65536
 --enable-merkle-index 1 --tx-sign blake3 --raft-apply-ledger-mode off
 --db-shared-buffers 32MB --det-pipeline-depth 0 --det-block-parallel 64
 --det-event-block-fastpath 0 --submit-mode event --parallelism-mode pipeline
 --ordering-mode raft-kafka --bcdb-dt-conflict-tracking 1 --bcdb-dt-light-snapshot 0
 --workload "$RBASE/repo/scripts/ycsb_recovery/ycsb_workload_a_skew_0_00_160k.txt"
 --merkle-partitions 200 --merkle-fanout "$F" --merkle-split-threshold "$S" --merkle-merge-threshold "$M"
 --db-port 5448 --recovery-db-port 5448 --raft-port 9018 --node-client-ports 8018,8018,8019
 --raft-storage-dir "$RBASE/raft" --skip-sync --skip-build --skip-cleanup --skip-kafka --skip-rdkafka-setup)
run_case() {
  local scenario="$1"
  shift
  export CLUSTER_RUN_ID="cluster4_s1024_${RTAG}_f${F}s${S}_${scenario}_r${REP}"
  export KAFKA_RESULT_TOPIC="recovery_${RTAG}_f${F}s${S}_${scenario}_r${REP}"
  local out="$RBASE/repo/scripts/bench_full_results/$CLUSTER_RUN_ID"
  test ! -e "$out" || return 2
  ssh neel@10.129.148.247 \
    "/home/neel/Desktop/kafka_2.13-3.7.0/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic '$KAFKA_RESULT_TOPIC' --replica-assignment 1,2,3"
  local rc=0
  # The runner creates out itself with exclusive mkdir and tees runner.log.
  bash "$RUNNER" "${common[@]}" "$@" > "$RBASE/console_${CLUSTER_RUN_ID}.log" 2>&1 || rc=$?
  mkdir -p "$out"
  printf 'scenario=%s\ngroup=f%ss%s\nfanout=%s\nsplit=%s\nmerge=%s\nrep=%s\n' \
    "$scenario" "$F" "$S" "$F" "$S" "$M" "$REP" > "$out/recovery_s1024.env"
  cp "$RUNNER" "${RUNNER%.sh}.diff" "$RBASE/prepared_runner.sha256" "$out/"
  cp "$RBASE/repo/scripts/distributed/recovery_s1024/serializable_remote_db.py" \
     "$RBASE/repo/scripts/distributed/recovery_s1024/serializable_fault_injector.py" "$out/"
  cp "$RBASE/python_dependencies.txt" "$out/"
  cp "$RBASE/console_${CLUSTER_RUN_ID}.log" "$out/console.log"
  printf 'exit_code=%s\n' "$rc" >> "$out/recovery_s1024.env"
  # PG remains running solely to collect geometry and settings after Phase 8.
  for host in 10.129.148.247 10.129.148.246 10.129.148.248; do
    ssh neel@"$host" "LD_LIBRARY_PATH='$RBASE/rdkafka/lib:$RBASE/install/lib' '$RBASE/install/bin/psql' -X -h 127.0.0.1 -p 5448 -U postgres postgres -f '$RBASE/repo/scripts/distributed/recovery_s1024/geometry.sql'" \
      > "$out/geometry_${host}.txt" 2>&1 || rc=1
    ssh neel@"$host" "LD_LIBRARY_PATH='$RBASE/install/lib' '$RBASE/install/bin/pg_ctl' -D '$RBASE/repo/.bench_tmp/recovery_pgdata' -m fast -w stop" \
      > "$out/stop_${host}.log" 2>&1 || rc=1
  done
  printf 'collection_exit_code=%s\n' "$rc" >> "$out/recovery_s1024.env"
  test "$rc" -eq 0
}
run_case A --recovery-mode off
run_case B --recovery-mode both
run_case C --recovery-mode both --inject-fault-node utkarsh \
  --inject-fault-count 100 --inject-fault-delay-sec 5 --inject-fault-type update
run_case M_mixed --recovery-mode both --inject-fault-node utkarsh \
  --inject-fault-count 100 --inject-fault-delay-sec 5 --inject-fault-type mixed
run_case L_mix_prio --recovery-mode both --inject-fault-node admin123 \
  --inject-fault-count 100 --inject-fault-delay-sec 5 --inject-fault-type mixed
```

Deployment above includes `.111:$RBASE/artifacts/u24/build.env`. Ensure `set -euo pipefail` in the
orchestrator shell so a failure stops the suite. Preserve failure artifacts.
Do not precreate runner outputs for unrelated runs or reuse CLUSTER_RUN_ID.

For the same-new-binary geometry control set `S=32 M=8 F=32` **and rebuild the
common array with those values**, then repeat the five exact run_case commands.
For a direct historical-geometry bridge, additionally use `F=4 S=32 M=8` with
the same binaries. The third geometry is particularly useful here: F32/S32
creates about 6,400 tiny leaves, whereas published online geometry is consistent
with F4/S32, not the separate single-node F32/S32 size-scaling experiment.
Use REP=2,3 with fresh IDs/topics, keeping workers, lanes, dataset, isolation,
fault count/delay, Kafka and durability fixed. No rebuild between geometries.

## Collection, acceptance and comparison

Collect entire run directories, exit codes, all three node logs, phase_markers,
fault_timeline/injection logs, final executor/retry/workload counters, provenance
and manifest files, geometry files, node memory profiles, tx_latency.csv,
tps_timeline.csv and post-marker readbacks. Archive .247 build logs and U22 image
identity once. `L_mix_prio` is accepted as that scenario only if RECOVERY_EVENT
actually selects `ref=4`; otherwise retain it and label the actual donor.

Require 160,000 client completions, divergence_count=0, permanent_failures=0,
all3_audit_valid=yes, zero recovery failures, successful nonzero recovery for
each fault scenario, full_copies=0, LIVE replay evidence, and Phase-8 **all three**
matching roots/row counts/heap digests with merkle_verify=t. Geometry/settings
must show SERIALIZABLE default and synchronous Merkle=on. Normal A/B runs with
zero triggers do not validate repair. Require a full-hash/root audit of initial
restore and the compact rebuild path remotely before accepting throughput.

The published summary was generated by
`scripts/distributed/generate_online_recovery_final_results.py:184`. Do not run
that generator: it overwrites published outputs. The new stdlib comparison
reuses its metric names and signed overhead convention, avoids counting
runner/gateway duplicate events, keeps missing evidence UNKNOWN, and adds
candidate rows/full copies. Overhead uses the median valid A run **within the
same geometry group**, not historical 8850 TPS. Multiple recovery events are
summed and explicitly counted; do not mistake a multi-event trial for one fault.

On the orchestrator with copied new runs and the published directory present:

```bash
python3 scripts/distributed/recovery_s1024/compare_recovery.py /path/to/fetched/new_runs \
  --csv /path/to/fresh/comparison.csv --markdown /path/to/fresh/comparison.md --strict
```

The comparison's evidence_status covers log/summary/post-marker acceptance,
not geometry, platform ABI, durability or live-session isolation verification;
those remain orchestrator checks. Missing selected baselines produce UNKNOWN
overhead. CSV retains every published row and every new trial rather than
replacing them with an average. Report per-scenario median/range across repeats
and separate published-to-new code+geometry changes from same-binary geometry
changes. Leave analysis_s1024.md measured cells pending until this evidence exists.

## Restore / stop procedure

Stop only RBASE/repo/.bench_tmp/recovery_pgdata using RBASE/install/bin/pg_ctl
and remaining server listeners at 9018 and 8018/8019. Archive logs first; never
run broad pkill or original --stop-only. Leave new source/install/artifacts and
Kafka topics intact for provenance until the orchestrator explicitly retires
the experiment. The normal isolated procedure needs **no canonical binary
restore** and never changes canonical PGDATA. Canonical Kafka brokers remain
running; only dedicated recovery_* topics were created.

If a canonical binary path was changed outside this procedure, first stop its
processes through their owning orchestrator, verify backup SHA and inventory,
then on that specific host extract `binaries.tar` with `tar -C /home/neel -xpf`.
Validate binary/library hashes against the backup, then let the owning
orchestrator restart its service. Never restore binaries while a process is
using them, never extract anything into canonical PGDATA, and do not claim that
today's backup matches published 2026-09-27 SHA values without checking them.
