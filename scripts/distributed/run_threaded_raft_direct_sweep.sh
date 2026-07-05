#!/usr/bin/env bash
# Run a matched 1/2/4/8/16 threaded Raft-direct benchmark sweep.
#
# Intended repository location:
#   scripts/distributed/run_threaded_raft_direct_sweep.sh
#
# It keeps the three real execution layers matched for every point:
#   gateway workers = server executor workers = owned PG connections = BCDB workers = T
#
# It emits:
#   scripts/bench_full_results/thread_sweep_<UTC timestamp>/
#     raw_runs.csv
#     run_dirs.txt
#     thread_sweep_tps.png
#     thread_sweep_summary.csv
#
# Usage:
#   chmod +x scripts/distributed/run_threaded_raft_direct_sweep.sh
#   scripts/distributed/run_threaded_raft_direct_sweep.sh
#
# Optional:
#   REPS=1 scripts/distributed/run_threaded_raft_direct_sweep.sh
#   SKIP_LOCAL_BUILD=1 scripts/distributed/run_threaded_raft_direct_sweep.sh

set -euo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel 2>/dev/null)" ||
  { echo "ERROR: run this from inside the AriaBC Git repository." >&2; exit 1; }

RUNNER="$REPO_ROOT/scripts/distributed/run_4node_raft_cluster.sh"
SUMMARIZER="$REPO_ROOT/scripts/distributed/summarize_raft_profile.py"
PLOTTER="$REPO_ROOT/scripts/distributed/plot_threaded_raft_direct_sweep.py"
RESULTS_ROOT="$REPO_ROOT/scripts/bench_full_results"
WORKLOAD="${WORKLOAD:-$REPO_ROOT/scripts/ycsb-skew0-99-tx-20k-point-safedb-intkey-insert12k-uniq.txt}"

REPS="${REPS:-3}"
SKIP_LOCAL_BUILD="${SKIP_LOCAL_BUILD:-0}"
SKIP_RDKAFKA_SETUP="${SKIP_RDKAFKA_SETUP:-1}"

THREAD_COUNTS=(1 2 4 8 16)
RAFT_ORDERING_POLICY="${RAFT_ORDERING_POLICY:-leader-assigned}"

for required in "$RUNNER" "$SUMMARIZER" "$PLOTTER" "$WORKLOAD"; do
  [[ -e "$required" ]] ||
    { echo "ERROR: required path is missing: $required" >&2; exit 1; }
done
[[ "$REPS" =~ ^[1-9][0-9]*$ ]] ||
  { echo "ERROR: REPS must be a positive integer, got: $REPS" >&2; exit 1; }

mkdir -p "$RESULTS_ROOT"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUT_DIR="$RESULTS_ROOT/thread_sweep_${STAMP}"
mkdir -p "$OUT_DIR"

RAW_CSV="$OUT_DIR/raw_runs.csv"
RUN_DIRS="$OUT_DIR/run_dirs.txt"
COMMAND_LOG="$OUT_DIR/commands.log"

printf 'threads,rep,target_entries,linger_us,det_window,artifact_dir,' > "$RAW_CSV"
python3 "$SUMMARIZER" "$RESULTS_ROOT"/cluster4_* 2>/dev/null | head -n1 | \
  tr -d '\r' >> "$RAW_CSV" || {
    # The summarizer needs a real artifact to print a header. Use the known schema.
    printf 'run_id,orderer_policy,assigned_seq_mode,tps,p50,p95,p99,client_workers,server_workers,bcdb_workers,bcdb_init_arg_size,leader_id,target_entries,linger_us,entries_per_fsync,fsync_p50,fsync_p95,append_entries_avg,orderer_gap_wait_ms,executor_queue_delay_ms,pqexec_avg,max_pqexec,kafka_pending_max,gateway_submit_to_accept_ms,gateway_accept_to_terminal_ms,merkle_pass,divergence_count,permanent_failures\n' >> "$RAW_CSV"
  }
: > "$RUN_DIRS"
: > "$COMMAND_LOG"

echo "Output directory: $OUT_DIR"
echo "Repository:       $REPO_ROOT"
echo "Workload:         $WORKLOAD"
echo "Repetitions:      $REPS"

bash -n "$RUNNER"
python3 -m py_compile "$SUMMARIZER" "$PLOTTER"
git -C "$REPO_ROOT" diff --check

if [[ "$SKIP_LOCAL_BUILD" != "1" ]]; then
  cmake --build "$REPO_ROOT/ariabc_pg/build" \
    --target ariabc_pg_gateway ariabc_pg_server \
    -j"$(nproc)"
fi

# These are best-known conservative settings for a matched closed-loop curve.
# They avoid injecting unnecessary linger at 1 worker.
batch_settings() {
  local t="$1"
  case "$t" in
    1)  echo "1 0" ;;
    2)  echo "2 100" ;;
    4)  echo "4 250" ;;
    8)  echo "8 500" ;;
    16) echo "16 500" ;;
    *)  echo "ERROR: unsupported thread count: $t" >&2; return 1 ;;
  esac
}

find_new_artifact() {
  local before_list="$1"
  local new_name

  new_name="$(
    comm -13 "$before_list" \
      <(find "$RESULTS_ROOT" -mindepth 1 -maxdepth 1 -type d -name 'cluster4_*' \
          -printf '%f\n' 2>/dev/null | sort) |
      tail -n1
  )"

  [[ -n "$new_name" && -d "$RESULTS_ROOT/$new_name" ]] || return 1
  printf '%s\n' "$RESULTS_ROOT/$new_name"
}

validate_artifact() {
  local artifact="$1"
  local expected_t="$2"

  python3 - "$artifact" "$expected_t" <<'PY'
import csv
import pathlib
import re
import sys

artifact = pathlib.Path(sys.argv[1])
expected_t = int(sys.argv[2])

meta = {}
meta_file = artifact / "run_meta.env"
if meta_file.exists():
    for line in meta_file.read_text(errors="replace").splitlines():
        if "=" in line:
            k, v = line.split("=", 1)
            meta[k] = v

checks = {
    "execution_profile": "threaded-raft-direct",
    "det_client_mode": "threadpool",
    "det_client_workers": str(expected_t),
    "server_exec_workers": str(expected_t),
    "server_pg_connections": str(expected_t),
}
for key, want in checks.items():
    got = meta.get(key)
    if got != want:
        raise SystemExit(
            f"INVALID artifact {artifact.name}: {key}={got!r}, expected {want!r}"
        )

policy = meta.get("raft_ordering_policy")
if policy not in {"preassigned", "leader-assigned"}:
    raise SystemExit(f"INVALID artifact {artifact.name}: raft_ordering_policy={policy!r}")

summary = (artifact / "run_summary.env").read_text(errors="replace")
def get_env(key):
    m = re.search(rf"(?m)^{re.escape(key)}=(.*)$", summary)
    return m.group(1).strip() if m else ""

if get_env("divergence_count") != "0":
    raise SystemExit(f"INVALID artifact {artifact.name}: divergence_count is not zero")
if get_env("permanent_failures") != "0":
    raise SystemExit(f"INVALID artifact {artifact.name}: permanent_failures is not zero")

runner = (artifact / "runner.log").read_text(errors="replace")
if not re.search(r"post-marker .*PASS|consistency: PASS", runner):
    raise SystemExit(f"INVALID artifact {artifact.name}: Merkle/post-marker PASS missing")
PY
}

append_row() {
  local artifact="$1"
  local threads="$2"
  local rep="$3"
  local target="$4"
  local linger="$5"
  local window="$6"

  local header row
  header="$(python3 "$SUMMARIZER" "$artifact" | head -n1 | tr -d '\r')"
  row="$(python3 "$SUMMARIZER" --no-header "$artifact" | tail -n1 | tr -d '\r')"
  [[ -n "$row" ]] ||
    { echo "ERROR: summarizer produced no CSV row for $artifact" >&2; exit 1; }

  printf '%s,%s,%s,%s,%s,"%s",%s\n' \
    "$threads" "$rep" "$target" "$linger" "$window" "$artifact" "$row" >> "$RAW_CSV"
}

first_run=1
for T in "${THREAD_COUNTS[@]}"; do
  read -r TARGET LINGER < <(batch_settings "$T")
  WINDOW="$((T * 1024))"

  for REP in $(seq 1 "$REPS"); do
    echo
    echo "=================================================================="
    echo "threads=$T rep=$REP target=$TARGET linger=${LINGER}us window=$WINDOW"
    echo "=================================================================="

    before_runs="$(mktemp)"
    find "$RESULTS_ROOT" -mindepth 1 -maxdepth 1 -type d -name 'cluster4_*' \
      -printf '%f\n' | sort > "$before_runs"

    cmd=(
      "$RUNNER"
      --ordering-mode raft-kafka
      --execution-profile threaded-raft-direct
      --workload "$WORKLOAD"
      --threads "$T"
      --det-client-workers "$T"
      --det-client-inflight 1
      --server-exec-workers "$T"
      --server-pg-connections "$T"
      --bcdb-workers "$T"
      --bcdb-decouple-workers 1
      --raft-ordered-fanout 1
      --raft-ordering-policy "$RAFT_ORDERING_POLICY"
      --raft-ordered-batch-append 1
      --raft-ordered-batch-target-entries "$TARGET"
      --raft-ordered-batch-linger-us "$LINGER"
      --raft-ordered-coalesce-log 0
      --kafka-completion-mode async
      --det-window "$WINDOW"
    )

    printf 'SKIP_RDKAFKA_SETUP=%q ' "$SKIP_RDKAFKA_SETUP" >> "$COMMAND_LOG"
    if [[ "$first_run" -eq 0 ]]; then
      printf 'SKIP_BUILD=1 ' >> "$COMMAND_LOG"
    fi
    printf '%q ' "${cmd[@]}" >> "$COMMAND_LOG"
    printf '\n' >> "$COMMAND_LOG"

    if [[ "$first_run" -eq 0 ]]; then
      env SKIP_BUILD=1 SKIP_RDKAFKA_SETUP="$SKIP_RDKAFKA_SETUP" "${cmd[@]}"
    else
      env SKIP_RDKAFKA_SETUP="$SKIP_RDKAFKA_SETUP" "${cmd[@]}"
      first_run=0
    fi

    artifact="$(find_new_artifact "$before_runs")" || {
      rm -f "$before_runs"
      echo "ERROR: could not find the new cluster artifact after this run." >&2
      exit 1
    }
    rm -f "$before_runs"

    validate_artifact "$artifact" "$T"
    append_row "$artifact" "$T" "$REP" "$TARGET" "$LINGER" "$WINDOW"
    printf '%s\n' "$artifact" | tee -a "$RUN_DIRS"

    python3 "$SUMMARIZER" --pretty "$artifact"
  done
done

python3 "$PLOTTER" \
  --input "$RAW_CSV" \
  --out-dir "$OUT_DIR" \
  --title "Threaded Raft-Direct: matched worker sweep"

echo
echo "Sweep complete."
echo "Raw CSV:        $RAW_CSV"
echo "Summary CSV:    $OUT_DIR/thread_sweep_summary.csv"
echo "TPS graph:      $OUT_DIR/thread_sweep_tps.png"
echo "Run directories:$RUN_DIRS"
