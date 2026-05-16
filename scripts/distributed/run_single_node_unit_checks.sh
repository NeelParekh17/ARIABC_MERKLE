#!/usr/bin/env bash
set -euo pipefail

# One-node-per-host NuRaft+PG unit checks with robust staging and timing.
# Runs each host independently, captures per-node timing, and writes a summary CSV/MD.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

NODES=""
SSH_KEY=""
SSH_PORT=22
REMOTE_REPO_ROOT="/home/neel/Desktop/ariabc_cluster"
REMOTE_INSTALL_DIR="/home/neel/Desktop/ariabc_install"
MODES="det"
THREADS="1"
RUNS=1
WORKLOADS="ycsbtx-skew-01-24k-pt-intkey-sid-clean-20k.txt"
DB_NAME="postgres"
DB_USER="postgres"
DB_PORT=5438
RETRIES=1

TEMPLATE_CONF_LOCAL="/work/ARIABC/pgdata/postgresql.conf"
ENSURE_SCRIPT_LOCAL="$REPO_ROOT/scripts/distributed/ensure_single_node_postgres.sh"
BENCH_MATRIX_LOCAL="$REPO_ROOT/scripts/bench_threads_matrix.py"
START_SERVER_LOCAL="$REPO_ROOT/scripts/start_server.sh"
RESTORE_SQL_LOCAL="$REPO_ROOT/scripts/restore_usertable_small.sql"

usage() {
  cat <<'EOF'
Usage:
  run_single_node_unit_checks.sh \
    --nodes <user1@host1,user2@host2,...> \
    [--ssh-key <path>] [--ssh-port <22>] \
    [--remote-repo-root </home/neel/Desktop/ariabc_cluster>] \
    [--remote-install-dir </home/neel/Desktop/ariabc_install>] \
    [--modes <det>] [--threads <1>] [--runs <1>] \
    [--workloads <csv>] [--db-name <postgres>] [--db-user <postgres>] [--db-port <5438>] \
    [--retries <1>]

Output:
  scripts/bench_full_results/single_node_unit_checks_<timestamp>/
    - summary.csv
    - summary.md
    - per-node/<safe_node>/
EOF
}

trim() {
  local s="$1"
  s="${s#${s%%[![:space:]]*}}"
  s="${s%${s##*[![:space:]]}}"
  printf '%s' "$s"
}

split_csv() {
  local csv="$1"
  local -n out_ref="$2"
  out_ref=()
  IFS=',' read -r -a raw <<< "$csv"
  for x in "${raw[@]}"; do
    x="$(trim "$x")"
    [[ -n "$x" ]] && out_ref+=("$x")
  done
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --nodes) NODES="${2:-}"; shift 2 ;;
    --ssh-key) SSH_KEY="${2:-}"; shift 2 ;;
    --ssh-port) SSH_PORT="${2:-22}"; shift 2 ;;
    --remote-repo-root) REMOTE_REPO_ROOT="${2:-}"; shift 2 ;;
    --remote-install-dir) REMOTE_INSTALL_DIR="${2:-}"; shift 2 ;;
    --modes) MODES="${2:-det}"; shift 2 ;;
    --threads) THREADS="${2:-1}"; shift 2 ;;
    --runs) RUNS="${2:-1}"; shift 2 ;;
    --workloads) WORKLOADS="${2:-}"; shift 2 ;;
    --db-name) DB_NAME="${2:-postgres}"; shift 2 ;;
    --db-user) DB_USER="${2:-postgres}"; shift 2 ;;
    --db-port) DB_PORT="${2:-5438}"; shift 2 ;;
    --retries) RETRIES="${2:-1}"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown arg: $1" >&2; usage; exit 2 ;;
  esac
done

if [[ -z "$NODES" ]]; then
  echo "ERROR: --nodes is required" >&2
  usage
  exit 2
fi

for req in "$TEMPLATE_CONF_LOCAL" "$ENSURE_SCRIPT_LOCAL" "$BENCH_MATRIX_LOCAL" "$START_SERVER_LOCAL" "$RESTORE_SQL_LOCAL"; do
  if [[ ! -f "$req" ]]; then
    echo "ERROR: required local file missing: $req" >&2
    exit 2
  fi
done

if [[ ! -x "$ENSURE_SCRIPT_LOCAL" ]]; then
  echo "ERROR: ensure script not executable: $ENSURE_SCRIPT_LOCAL" >&2
  exit 2
fi

ssh_base=(ssh -o BatchMode=yes -o StrictHostKeyChecking=no -p "$SSH_PORT")
rsync_ssh="ssh -o BatchMode=yes -o StrictHostKeyChecking=no -p $SSH_PORT"
scp_base=(scp -o BatchMode=yes -o StrictHostKeyChecking=no -P "$SSH_PORT")
if [[ -n "$SSH_KEY" ]]; then
  ssh_base+=(-i "$SSH_KEY")
  rsync_ssh+=" -i $SSH_KEY"
  scp_base+=(-i "$SSH_KEY")
fi

declare -a NODE_ARR=()
split_csv "$NODES" NODE_ARR
if [[ "${#NODE_ARR[@]}" -eq 0 ]]; then
  echo "ERROR: no nodes parsed from --nodes" >&2
  exit 2
fi

ts="$(date +%Y%m%d_%H%M%S)"
out_root="$REPO_ROOT/scripts/bench_full_results/single_node_unit_checks_${ts}"
node_root="$out_root/per-node"
mkdir -p "$node_root"
summary_csv="$out_root/summary.csv"
summary_md="$out_root/summary.md"

echo "node,safe_node,status,attempts,start_time,end_time,elapsed_s,results_csv,summary_csv_path,error" > "$summary_csv"

echo "== Single-Node Unit Checks =="
echo "Nodes: ${NODE_ARR[*]}"
echo "Output: $out_root"
echo

pass_count=0
fail_count=0

for node in "${NODE_ARR[@]}"; do
  safe_node="${node//@/_at_}"
  safe_node="${safe_node//./_}"
  local_node_dir="$node_root/$safe_node"
  mkdir -p "$local_node_dir"

  start_h="$(date '+%F %T %z')"
  start_e="$(date +%s)"
  status="FAIL"
  err_msg="unknown"
  attempts_done=0
  pulled_results=""
  pulled_summary=""

  echo "[START] node=$node at $start_h"

  for attempt in $(seq 1 "$RETRIES"); do
    attempts_done="$attempt"
    remote_out_dir="$REMOTE_REPO_ROOT/scripts/bench_results/nodecheck_${safe_node}_${ts}_a${attempt}"
    remote_template_conf="$REMOTE_REPO_ROOT/.bench_tmp/shared_postgresql.conf"

    set +e
    "${ssh_base[@]}" "$node" "mkdir -p '$REMOTE_REPO_ROOT/.bench_tmp' '$REMOTE_REPO_ROOT/scripts/distributed' '$REMOTE_REPO_ROOT/scripts'"
    rc_setup=$?
    if [[ "$rc_setup" -ne 0 ]]; then
      err_msg="ssh_setup_failed"
      set -e
      continue
    fi

    "${scp_base[@]}" "$TEMPLATE_CONF_LOCAL" "$node:$remote_template_conf" >/dev/null 2>&1
    "${scp_base[@]}" "$ENSURE_SCRIPT_LOCAL" "$node:$REMOTE_REPO_ROOT/scripts/distributed/ensure_single_node_postgres.sh" >/dev/null 2>&1
    "${scp_base[@]}" "$BENCH_MATRIX_LOCAL" "$node:$REMOTE_REPO_ROOT/scripts/bench_threads_matrix.py" >/dev/null 2>&1
    "${scp_base[@]}" "$START_SERVER_LOCAL" "$node:$REMOTE_REPO_ROOT/scripts/start_server.sh" >/dev/null 2>&1
    "${scp_base[@]}" "$RESTORE_SQL_LOCAL" "$node:$REMOTE_REPO_ROOT/scripts/restore_usertable_small.sql" >/dev/null 2>&1
    rc_stage=$?
    if [[ "$rc_stage" -ne 0 ]]; then
      err_msg="scp_stage_failed"
      set -e
      continue
    fi

    "${ssh_base[@]}" "$node" "chmod +x '$REMOTE_REPO_ROOT/scripts/distributed/ensure_single_node_postgres.sh' '$REMOTE_REPO_ROOT/scripts/start_server.sh'" >/dev/null 2>&1

    remote_cmd="set -euo pipefail
cd '$REMOTE_REPO_ROOT/scripts'
export ARIABC_PSQL='$REMOTE_INSTALL_DIR/bin/psql'
export ARIABC_INSTALL_DIR='$REMOTE_INSTALL_DIR'
export ARIABC_DIR='$REMOTE_REPO_ROOT'
export ARIABC_PGPORT='$DB_PORT'
pgdata_line=\$(bash '$REMOTE_REPO_ROOT/scripts/distributed/ensure_single_node_postgres.sh' \
  --repo-root '$REMOTE_REPO_ROOT' \
  --install-dir '$REMOTE_INSTALL_DIR' \
  --db-port '$DB_PORT' \
  --db-user '$DB_USER' \
  --db-name '$DB_NAME' \
  --template-config '$remote_template_conf' | tail -n 1)
if [[ \$pgdata_line == PGDATA=* ]]; then
  export ARIABC_PGDATA=\${pgdata_line#PGDATA=}
fi
PYTHON_BIN=python3
if [[ -x '$REMOTE_REPO_ROOT/.venv/bin/python' ]]; then
  PYTHON_BIN='$REMOTE_REPO_ROOT/.venv/bin/python'
fi
\$PYTHON_BIN -u bench_threads_matrix.py \
  --modes '$MODES' \
  --threads '$THREADS' \
  --runs '$RUNS' \
  --workloads '$WORKLOADS' \
  --db '$DB_NAME' \
  --user '$DB_USER' \
  --port '$DB_PORT' \
  --out-dir '$remote_out_dir'"

    "${ssh_base[@]}" "$node" "bash -lc $(printf '%q' "$remote_cmd")" >"$local_node_dir/remote_attempt_${attempt}.out" 2>"$local_node_dir/remote_attempt_${attempt}.err"
    rc_run=$?

    if [[ "$rc_run" -eq 0 ]]; then
      rsync -a -e "$rsync_ssh" "$node:$remote_out_dir/" "$local_node_dir/" >/dev/null 2>&1
      if [[ -f "$local_node_dir/results.csv" && -f "$local_node_dir/summary.csv" ]]; then
        status="PASS"
        err_msg=""
        pulled_results="$local_node_dir/results.csv"
        pulled_summary="$local_node_dir/summary.csv"
        set -e
        break
      fi
      err_msg="missing_results_after_run"
    else
      err_msg="remote_run_failed_rc_${rc_run}"
    fi
    set -e
  done

  end_h="$(date '+%F %T %z')"
  end_e="$(date +%s)"
  elapsed="$((end_e - start_e))"

  if [[ "$status" == "PASS" ]]; then
    pass_count=$((pass_count + 1))
    echo "[END] node=$node at $end_h rc=0 elapsed_s=$elapsed status=PASS"
  else
    fail_count=$((fail_count + 1))
    echo "[END] node=$node at $end_h rc=1 elapsed_s=$elapsed status=FAIL error=$err_msg"
  fi

  # CSV-safe error field
  esc_err="${err_msg//\"/\"\"}"
  printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,"%s"\n' \
    "$node" "$safe_node" "$status" "$attempts_done" "$start_h" "$end_h" "$elapsed" "$pulled_results" "$pulled_summary" "$esc_err" \
    >> "$summary_csv"
done

{
  echo "# Single-Node Unit Check Summary"
  echo
  echo "- timestamp: $ts"
  echo "- total_nodes: ${#NODE_ARR[@]}"
  echo "- pass: $pass_count"
  echo "- fail: $fail_count"
  echo
  echo "| node | status | attempts | elapsed_s | error |"
  echo "|---|---|---:|---:|---|"
  tail -n +2 "$summary_csv" | while IFS=',' read -r node safe_node status attempts start_t end_t elapsed res sum err; do
    err_clean="${err#\"}"; err_clean="${err_clean%\"}"
    echo "| $node | $status | $attempts | $elapsed | $err_clean |"
  done
} > "$summary_md"

echo
if [[ "$fail_count" -eq 0 ]]; then
  echo "All unit checks passed."
  echo "Summary CSV: $summary_csv"
  echo "Summary MD : $summary_md"
  exit 0
fi

echo "Some unit checks failed."
echo "Summary CSV: $summary_csv"
echo "Summary MD : $summary_md"
exit 1
