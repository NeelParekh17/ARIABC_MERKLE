#!/usr/bin/env bash
set -euo pipefail
B=/home/neel/Desktop/recovery_s1024_20261001T060514ZJ
RTAG=20261001T060514ZJ
F=${1:?}; S=${2:?}; M=${3:?}; CASE=${4:?}; ATTEMPT=${5:-1}
[[ $ATTEMPT =~ ^[0-9]+$ ]]
[[ $(hostname -I) == *10.129.27.111* ]]
cd "$B"
mkdir -p "$B/tmp"
export TMPDIR="$B/tmp"
[[ $F/$S/$M == 32/1024/256 || $F/$S/$M == 4/32/8 ]]
export PATH="$B/tools:$B/venv/bin:$PATH"
export LD_LIBRARY_PATH="$B/rdkafka/lib:$B/install/lib"
export BYPASS_DELEGATION=1 SKIP_BUILD=1 SKIP_SYNC=1 SKIP_CLEANUP=0
export LOCAL_INSTALL_DIR="$B/install"
export RESULT_RING_CAPACITY=2048
export ARIABC_KAFKA_RESULT_BATCH_TARGET_RECORDS=128 ARIABC_KAFKA_ASYNC_RESULT_BATCH_RECORDS=256
export ARIABC_FULL_RESULT_REPLICA_LIMIT=-1 ARIABC_KAFKA_PAYLOAD_FORMAT=text
export BCDB_DET_QUEUE_HIGH_WM=65536 BCDB_DET_QUEUE_LOW_WM=32768
export RECOVERY_INTERVAL_MS=1000 BENCH_COLD_CACHE=0 TX_SIGN=blake3
export CLUSTER_STOP_POSTGRES_ON_EXIT=0 COLLECT_FINAL_SERVER_PROFILE=1
export GATEWAY_STALL_WATCHDOG=1 GATEWAY_STALL_POLL_SECONDS=5 GATEWAY_STALL_MAX_CYCLES=12
export CLUSTER_RUN_ID="cluster4_s1024_${RTAG}_f${F}s${S}_${CASE}_r${ATTEMPT}"
export KAFKA_RESULT_TOPIC="recovery_f${F}s${S}_${CASE}_r${ATTEMPT}_${RTAG}"
RUNNER="$B/repo/scripts/distributed/recovery_s1024/cluster_runner_${RTAG}.sh"
OUT="$B/repo/scripts/bench_full_results/$CLUSTER_RUN_ID"
mkdir -p "$B/repo/scripts/bench_full_results"
test ! -e "$OUT"
test -f "$B/oom_gate_open.txt"
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
 --workload "$B/repo/scripts/ycsb_recovery/ycsb_workload_a_skew_0_00_160k.txt"
 --merkle-partitions 200 --merkle-fanout "$F" --merkle-split-threshold "$S" --merkle-merge-threshold "$M"
 --db-port 5448 --recovery-db-port 5448 --raft-port 9018 --node-client-ports 8018,8018,8019
 --raft-storage-dir "$B/raft" --raft-cluster-id "$CLUSTER_RUN_ID" --skip-sync --skip-build --skip-kafka --skip-rdkafka-setup)
case "$CASE" in
 A) extra=(--recovery-mode off);;
 B) extra=(--recovery-mode both);;
 C) extra=(--recovery-mode both --inject-fault-node utkarsh --inject-fault-count 100 --inject-fault-delay-sec 5 --inject-fault-type update);;
 M_mixed) extra=(--recovery-mode both --inject-fault-node utkarsh --inject-fault-count 100 --inject-fault-delay-sec 5 --inject-fault-type mixed);;
 L_mix_prio) extra=(--recovery-mode both --inject-fault-node admin123 --inject-fault-count 100 --inject-fault-delay-sec 5 --inject-fault-type mixed);;
 *) exit 2;;
esac
mkdir -p "$B/preflight/$CLUSTER_RUN_ID"
for H in 10.129.148.247 10.129.148.246 10.129.148.248; do
 ssh neel@"$H" "set -e; test ! -e '$B/raft/$CLUSTER_RUN_ID'; free -m; cat /proc/loadavg; cat /proc/vmstat; test -z \"\$(ss -ltnH | awk '\$4 ~ /:(5448|9018|8018|8019)$/ {print}')\"" > "$B/preflight/$CLUSTER_RUN_ID/$H.log"
done
ssh neel@10.129.148.247 "set -e; if ! command -v java >/dev/null; then export JAVA_HOME=/home/neel/Desktop/usr/lib/jvm/java-21-openjdk-amd64; export PATH=\$JAVA_HOME/bin:\$PATH; fi; '/home/neel/Desktop/kafka_2.13-3.7.0/bin/kafka-topics.sh' --bootstrap-server localhost:9092 --create --topic '$KAFKA_RESULT_TOPIC' --replica-assignment 1,2,3; '/home/neel/Desktop/kafka_2.13-3.7.0/bin/kafka-topics.sh' --bootstrap-server localhost:9092 --describe --topic '$KAFKA_RESULT_TOPIC'"
rc=0
bash "$RUNNER" "${common[@]}" "${extra[@]}" > "$B/console_$CLUSTER_RUN_ID.log" 2>&1 || rc=$?
mkdir -p "$OUT"
printf 'scenario=%s\ngroup=f%ss%s\nfanout=%s\nsplit=%s\nmerge=%s\nrep=1\nexit_code=%s\n' "$CASE" "$F" "$S" "$F" "$S" "$M" "$rc" > "$OUT/recovery_s1024.env"
printf 'attempt=%s\n' "$ATTEMPT" >> "$OUT/recovery_s1024.env"
cp "$RUNNER" "${RUNNER%.sh}.diff" "$B/guardrail_adaptation.diff" "$B/pid_recording.diff" "$B/python_dependencies.txt" "$B/geometry.sql" "$B/run_case.sh" "$OUT/"
cp "$B/repo/scripts/distributed/recovery_s1024/serializable_"*.py "$OUT/"
cp "$B/console_$CLUSTER_RUN_ID.log" "$OUT/console.log"
cp -a "$B/preflight/$CLUSTER_RUN_ID" "$OUT/health_before"
sha256sum "$RUNNER" "$B/repo/scripts/distributed/recovery_s1024/serializable_"*.py > "$OUT/prepared_scripts.sha256"
for H in 10.129.148.247 10.129.148.246 10.129.148.248; do
 ssh neel@"$H" "LD_LIBRARY_PATH='$B/rdkafka/lib:$B/install/lib' '$B/install/bin/psql' -X -h 127.0.0.1 -p 5448 -U postgres postgres -f '$B/geometry.sql'" > "$OUT/geometry_$H.txt" 2>&1 || rc=1
 ssh neel@"$H" "bash '$B/stop_server.sh'; LD_LIBRARY_PATH='$B/install/lib' '$B/install/bin/pg_ctl' -D '$B/repo/.bench_tmp/recovery_pgdata' -m fast -w stop; free -m; cat /proc/vmstat; ss -ltnH | awk '\$4 ~ /:(5448|9018|8018|8019)$/ {print}'" > "$OUT/cleanup_$H.log" 2>&1 || rc=1
done
printf 'collection_exit_code=%s\n' "$rc" >> "$OUT/recovery_s1024.env"
echo "CASE_DONE $CLUSTER_RUN_ID rc=$rc"
exit "$rc"
