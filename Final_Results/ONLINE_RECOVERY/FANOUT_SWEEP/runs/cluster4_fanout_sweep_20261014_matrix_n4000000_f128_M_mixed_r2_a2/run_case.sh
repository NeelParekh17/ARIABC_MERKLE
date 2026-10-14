#!/usr/bin/env bash
set -euo pipefail
B=/home/neel/ariabc_data/fanout_sweep_20261014
CASE=${1:?}; REP=${2:?}; ATTEMPT=${3:?}; STAGE=${4:?}; ROWS=${5:?}; FANOUT=${6:?}
export FANOUT_ROWS="$ROWS" FANOUT_FAULT_SEED="$((20261013+REP))"
[[ "$ROWS" == 12000 || "$ROWS" == 4000000 ]]
[[ "$FANOUT" =~ ^(16|32|64|128|256)$ ]]
export PATH="$B/tools:$PATH"
python3 "$B/select_restore.py" "$ROWS"
[[ $(hostname -I) == *10.129.148.247* ]]
[[ $REP =~ ^[0-9]+$ && $ATTEMPT =~ ^[0-9]+$ && $STAGE =~ ^(smoke|matrix)$ ]]
export TMPDIR="$B/scratch" PATH="$B/tools:$B/venv/bin:$PATH" LD_LIBRARY_PATH="$B/rdkafka/lib:$B/install/lib"
export BYPASS_DELEGATION=1 SKIP_BUILD=1 SKIP_SYNC=1 SKIP_CLEANUP=0 LOCAL_INSTALL_DIR="$B/install"
export RESULT_RING_CAPACITY=2048 CALLER_GIT_HEAD=9cf6c70a39e9bf848c93c2c24bdb55181f4b3356
export ARIABC_KAFKA_RESULT_BATCH_TARGET_RECORDS=128 ARIABC_KAFKA_ASYNC_RESULT_BATCH_RECORDS=256
export ARIABC_FULL_RESULT_REPLICA_LIMIT=-1 ARIABC_KAFKA_PAYLOAD_FORMAT=text
export BCDB_DET_QUEUE_HIGH_WM=65536 BCDB_DET_QUEUE_LOW_WM=32768
export RECOVERY_INTERVAL_MS=1000 BENCH_COLD_CACHE=0 TX_SIGN=blake3
export CLUSTER_STOP_POSTGRES_ON_EXIT=0 COLLECT_FINAL_SERVER_PROFILE=1
export GATEWAY_STALL_WATCHDOG=1 GATEWAY_STALL_POLL_SECONDS=5 GATEWAY_STALL_MAX_CYCLES=12
export MERKLE_DRAIN_SSH_TIMEOUT=300
export POLL_COUNT=360000 POLL_INTERVAL_US=500 NODE_SSH_COMMAND_TIMEOUT=300
export KAFKA_HOST=10.129.148.247 KAFKA_PORT=9192 KAFKA_FAST_RESET=1
export KAFKA_BOOTSTRAP=10.129.148.247:9192,10.129.148.246:9192,10.129.148.248:9192 KAFKA_ROUTING_MODE=colocated
export BCDB_GATE_TELEMETRY=0 BCDB_PHASE_TRACE_ON=0 BCDB_BLOCK_PROFILE=0 ARIABC_OS_PROFILE=0
export CLUSTER_RUN_ID="cluster4_fanout_sweep_20261014_${STAGE}_n${ROWS}_f${FANOUT}_${CASE}_r${REP}_a${ATTEMPT}"
export KAFKA_RESULT_TOPIC="fanout_sweep_20261014_${STAGE}_n${ROWS}_f${FANOUT}_${CASE}_r${REP}_a${ATTEMPT}"
RUNNER="$B/repo/scripts/distributed/recovery_s1024/cluster_runner_fanout_sweep_20261014.sh"
OUT="$B/repo/scripts/bench_full_results/$CLUSTER_RUN_ID"
test ! -e "$OUT"
test "$(cat /sys/devices/system/clocksource/clocksource0/current_clocksource)" = tsc
(cd "$B/repo" && sha256sum -c "$B/provenance/experiment_inputs.sha256")
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
 --workload "$B/repo/scripts/ycsb_recovery/ycsb_a_n${ROWS}_160k.txt"
 --merkle-partitions 200 --merkle-fanout "$FANOUT" --merkle-split-threshold 1024 --merkle-merge-threshold 256
 --db-port 5448 --recovery-db-port 5448 --raft-port 9018 --node-client-ports 8018,8018,8019
 --raft-storage-dir "$B/raft" --raft-cluster-id "$CLUSTER_RUN_ID" --skip-sync --skip-build --skip-kafka --skip-rdkafka-setup)
case "$CASE" in
 A) extra=(--recovery-mode off);;
 B) extra=(--recovery-mode both);;
 C|C_deep) extra=(--recovery-mode both --inject-fault-node utkarsh --inject-fault-count 100 --inject-fault-delay-sec 5 --inject-fault-type update);;
 M_passive) extra=(--recovery-mode passive --inject-fault-node utkarsh --inject-fault-count 100 --inject-fault-delay-sec 5 --inject-fault-type update);;
 M_active) extra=(--recovery-mode active --inject-fault-node utkarsh --inject-fault-count 100 --inject-fault-delay-sec 5 --inject-fault-type update);;
 M_mixed) extra=(--recovery-mode both --inject-fault-node utkarsh --inject-fault-count 100 --inject-fault-delay-sec 5 --inject-fault-type mixed);;
 L) extra=(--recovery-mode both --inject-fault-node admin123 --inject-fault-count 100 --inject-fault-delay-sec 5 --inject-fault-type update);;
 L_mix) extra=(--recovery-mode both --inject-fault-node admin123 --inject-fault-count 100 --inject-fault-delay-sec 5 --inject-fault-type mixed);;
 *) exit 2;;
esac
if [[ $CASE == C_deep ]]; then
 export BCDB_GATE_TELEMETRY=1 BCDB_PHASE_TRACE_ON=1 BCDB_BLOCK_PROFILE=1 ARIABC_OS_PROFILE=1
fi
HEALTH="$B/health/$CLUSTER_RUN_ID"
mkdir -p "$HEALTH" "$B/repo/scripts/bench_full_results"
for H in 10.129.148.247 10.129.148.246 10.129.148.248; do
 timeout 60 ssh -o BatchMode=yes -o ConnectTimeout=5 -o ServerAliveInterval=5 -o ServerAliveCountMax=3 neel@"$H" "set -e; test ! -e '$B/raft/$CLUSTER_RUN_ID'; bash '$B/host_sample.sh'; test -z \"\$(ss -ltnH | awk '\$4 ~ /:(5448|9018|8018|8019)$/ {print}')\"" > "$HEALTH/before_$H.log"
done
ssh neel@10.129.148.247 "set -e; if ! command -v java >/dev/null; then export JAVA_HOME=/home/neel/Desktop/usr/lib/jvm/java-21-openjdk-amd64; export PATH=\$JAVA_HOME/bin:\$PATH; fi; '/home/neel/Desktop/kafka_2.13-3.7.0/bin/kafka-topics.sh' --bootstrap-server localhost:9192 --create --topic '$KAFKA_RESULT_TOPIC' --replica-assignment 1,2,3; '/home/neel/Desktop/kafka_2.13-3.7.0/bin/kafka-topics.sh' --bootstrap-server localhost:9192 --describe --topic '$KAFKA_RESULT_TOPIC'" > "$HEALTH/topic.log" 2>&1
python3 "$B/health_watch.py" "$HEALTH" > "$HEALTH/watch.log" 2>&1 & HEALTH_PID=$!
iostat -x -t 1 > "$HEALTH/iostat_gateway.log" 2>&1 & IOSTAT_PID=$!
rc=0
bash "$RUNNER" "${common[@]}" "${extra[@]}" > "$B/console_$CLUSTER_RUN_ID.log" 2>&1 || rc=$?
kill -TERM "$HEALTH_PID" "$IOSTAT_PID" 2>/dev/null || true
wait "$HEALTH_PID" 2>/dev/null || true
wait "$IOSTAT_PID" 2>/dev/null || true
mkdir -p "$OUT"
printf 'scenario=%s\ngroup=n%s_f%s\nfanout=%s\nrows=%s\nsplit=1024\nmerge=256\nrep=%s\nattempt=%s\nstage=%s\nexit_code=%s\n' "$CASE" "$ROWS" "$FANOUT" "$FANOUT" "$ROWS" "$REP" "$ATTEMPT" "$STAGE" "$rc" > "$OUT/recovery_s1024.env"
cp "$RUNNER" "${RUNNER%.sh}.diff" "$B/guardrail_adaptation.diff" "$B/generator_adaptation.diff" "$B/injector_jitter.diff" "$B/python_dependencies.txt" "$B/controlled_injector.diff" "$B/campaign.json" "$B/run_case.sh" "$OUT/"
cp "$B/repo/scripts/distributed/recovery_s1024/serializable_"*.py "$OUT/"
cp "$B/console_$CLUSTER_RUN_ID.log" "$OUT/console.log"
cp -a "$HEALTH" "$OUT/health"
sha256sum "$RUNNER" "$B/repo/scripts/distributed/recovery_s1024/serializable_"*.py > "$OUT/prepared_scripts.sha256"
cp "$B/provenance/experiment_inputs.sha256" "$OUT/experiment_inputs.sha256"
collection_rc=0
for H in 10.129.148.247 10.129.148.246 10.129.148.248; do
 timeout 60 ssh -o BatchMode=yes -o ConnectTimeout=5 -o ServerAliveInterval=5 -o ServerAliveCountMax=3 neel@"$H" "LD_LIBRARY_PATH='$B/rdkafka/lib:$B/install/lib' '$B/install/bin/psql' -X -h 127.0.0.1 -p 5448 -U postgres postgres -f '$B/geometry.sql'" > "$OUT/geometry_$H.txt" 2>&1 || collection_rc=1
 timeout 60 ssh -o BatchMode=yes -o ConnectTimeout=5 -o ServerAliveInterval=5 -o ServerAliveCountMax=3 neel@"$H" "bash '$B/host_sample.sh'; bash '$B/stop_server.sh'; if LD_LIBRARY_PATH='$B/install/lib' '$B/install/bin/pg_ctl' -D '$B/repo/.bench_tmp/recovery_pgdata' status >/dev/null 2>&1; then LD_LIBRARY_PATH='$B/install/lib' '$B/install/bin/pg_ctl' -D '$B/repo/.bench_tmp/recovery_pgdata' -m fast -w stop; fi" > "$OUT/cleanup_$H.log" 2>&1 || collection_rc=1
done
printf 'collection_exit_code=%s\n' "$collection_rc" >> "$OUT/recovery_s1024.env"
printf '%s\n' "$CLUSTER_RUN_ID" > "$B/last_run_id.txt"
echo "CASE_DONE $CLUSTER_RUN_ID rc=$rc"
if [[ $collection_rc != 0 ]]; then exit "$collection_rc"; fi
exit "$rc"
