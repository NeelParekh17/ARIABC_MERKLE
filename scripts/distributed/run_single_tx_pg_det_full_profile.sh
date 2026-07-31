#!/usr/bin/env bash
set -euo pipefail

#
# Run one SQL statement through:
#   1. single-node PG
#   2. single-node DET
#   3. full 4-node Kafka-majority Raft+BCDB
#
# The output is an artifact folder with raw logs plus:
#   summary.csv    machine-readable timings
#   breakdown.md   human-readable overhead explanation
#

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

TARGET_NODE="${TARGET_NODE:-neel@10.129.148.247}"
TARGET_MACHINE_LABEL="${TARGET_MACHINE_LABEL:-}"
REMOTE_REPO="${REMOTE_REPO:-/home/neel/Desktop/ariabc_cluster}"
REMOTE_INSTALL="${REMOTE_INSTALL:-/home/neel/Desktop/ariabc_install}"
LOCAL_INSTALL_DIR="${LOCAL_INSTALL_DIR:-/work/ARIABC/install}"
TEMPLATE_CONF_LOCAL="${TEMPLATE_CONF_LOCAL:-/work/ARIABC/pgdata/postgresql.conf}"
SSH_KEY="${SSH_KEY:-$HOME/.ssh/id_rsa}"
SSH_PORT="${SSH_PORT:-22}"

DB_PORT="${DB_PORT:-5438}"
DB_USER="${DB_USER:-postgres}"
DB_NAME="${DB_NAME:-postgres}"

SKIP_SINGLE_SYNC="${SKIP_SINGLE_SYNC:-1}"
FULL_SKIP_SYNC="${FULL_SKIP_SYNC:-1}"
FULL_SKIP_BUILD="${FULL_SKIP_BUILD:-1}"
FULL_SKIP_RDKAFKA_SETUP="${FULL_SKIP_RDKAFKA_SETUP:-1}"

FULL_POOL_SIZE="${FULL_POOL_SIZE:-256}"
FULL_BCDB_WORKER_COUNT="${FULL_BCDB_WORKER_COUNT:-512}"
FULL_DET_WINDOW="${FULL_DET_WINDOW:-1}"
FULL_DET_BATCH_SIZE="${FULL_DET_BATCH_SIZE:-16}"
FULL_DET_BLOCK_PIPELINE="${FULL_DET_BLOCK_PIPELINE:-1}"
FULL_DET_BLOCK_MAX="${FULL_DET_BLOCK_MAX:-2048}"
FULL_BCDB_PHASE_TRACE="${FULL_BCDB_PHASE_TRACE:-1}"
FULL_CASE_TIMEOUT_S="${FULL_CASE_TIMEOUT_S:-900}"
POLL_COUNT="${POLL_COUNT:-120000}"
RESULT_RING_CAPACITY="${RESULT_RING_CAPACITY:-32768}"

SINGLE_TIMEOUT_S="${SINGLE_TIMEOUT_S:-300}"
SINGLE_BCDB_PHASE_TRACE="${SINGLE_BCDB_PHASE_TRACE:-1}"

DEFAULT_SQL="UPDATE usertable_small SET FIELD10='single_tx_profile', FIELD1='single_tx_profile' WHERE YCSB_KEY=686;"
SQL="${SQL:-$DEFAULT_SQL}"

usage() {
  cat <<EOF
Usage:
  $0 [--sql "UPDATE ...;"] [--out-root DIR]

Environment:
  SQL='...'                         SQL statement to run once.
  TARGET_NODE=neel@10.129.148.247   Single-node PG/DET target.
  SKIP_SINGLE_SYNC=1                Use existing remote repo/install by default.
  FULL_SKIP_SYNC=1 FULL_SKIP_BUILD=1
  FULL_BCDB_PHASE_TRACE=1           Enable full-system phase trace for one tx.

Outputs:
  scripts/bench_full_results/single_tx_profile_<timestamp>/
EOF
}

OUT_ROOT=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --sql) SQL="${2:-}"; shift 2 ;;
    --out-root) OUT_ROOT="${2:-}"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown arg: $1" >&2; usage; exit 2 ;;
  esac
done

if [[ -z "$TARGET_MACHINE_LABEL" ]]; then
  TARGET_MACHINE_LABEL="${TARGET_NODE##*@}"
  TARGET_MACHINE_LABEL="${TARGET_MACHINE_LABEL%%:*}"
fi

ts="$(date +%Y%m%d_%H%M%S)"
OUT_ROOT="${OUT_ROOT:-$REPO_ROOT/scripts/bench_full_results/single_tx_profile_${ts}}"
RUN_LOG_DIR="$OUT_ROOT/_run_logs"
LOCAL_WORKLOAD="$OUT_ROOT/single_tx.sql"
SINGLE_LOCAL_DIR="$OUT_ROOT/single_${TARGET_MACHINE_LABEL//./_}"
mkdir -p "$RUN_LOG_DIR" "$SINGLE_LOCAL_DIR"

log() { echo "[$(date +'%F %T')] $*"; }
die() { echo "ERROR: $*" >&2; exit 1; }

ssh_run() {
  ssh -i "$SSH_KEY" -o BatchMode=yes -o StrictHostKeyChecking=no -o ConnectTimeout=15 -p "$SSH_PORT" "$TARGET_NODE" "$@"
}

rsync_to_target() {
  rsync -az -e "ssh -i $SSH_KEY -o BatchMode=yes -o StrictHostKeyChecking=no -o ConnectTimeout=15 -p $SSH_PORT" "$@"
}

rsync_from_target() {
  rsync -az -e "ssh -i $SSH_KEY -o BatchMode=yes -o StrictHostKeyChecking=no -o ConnectTimeout=15 -p $SSH_PORT" "$@"
}

printf '%s\n' "$SQL" > "$LOCAL_WORKLOAD"

cat > "$OUT_ROOT/run_meta.json" <<EOF
{
  "created": "$(date -Is)",
  "target_node": "$TARGET_NODE",
  "sql": $(SQL_VALUE="$SQL" python3 -c 'import json,os; print(json.dumps(os.environ.get("SQL_VALUE","")))'),
  "single_sync_skipped": "$SKIP_SINGLE_SYNC",
  "full_skip_sync": "$FULL_SKIP_SYNC",
  "full_skip_build": "$FULL_SKIP_BUILD",
  "full_bcdb_phase_trace": "$FULL_BCDB_PHASE_TRACE"
}
EOF

sync_single_target() {
  if [[ "$SKIP_SINGLE_SYNC" == "1" ]]; then
    log "Single-node repo/install sync skipped"
    ssh_run "mkdir -p '$REMOTE_REPO/scripts/bench_results/single_tx_profile_${ts}' '$REMOTE_REPO/.bench_tmp'"
    return
  fi

  log "Syncing source/install to $TARGET_NODE"
  "$SCRIPT_DIR/sync_repo_install_to_neel_desktop.sh" \
    --nodes "$TARGET_NODE" \
    --ssh-key "$SSH_KEY" \
    --ssh-port "$SSH_PORT" \
    --remote-repo-root "$REMOTE_REPO" \
    --remote-install-dir "$REMOTE_INSTALL" > "$RUN_LOG_DIR/single_sync.log" 2>&1
  rsync_to_target "$TEMPLATE_CONF_LOCAL" "$TARGET_NODE:$REMOTE_REPO/.bench_tmp/shared_postgresql.conf"
}

run_single_pg_det() {
  local remote_out="$REMOTE_REPO/scripts/bench_results/single_tx_profile_${ts}"
  local remote_workload="$remote_out/single_tx.sql"
  local remote_workload_rel="bench_results/single_tx_profile_${ts}/single_tx.sql"
  local log_file="$RUN_LOG_DIR/single_pg_det.log"

  log "Running single-node PG and DET one-transaction profile on $TARGET_NODE"
  ssh_run "mkdir -p '$remote_out'"
  rsync_to_target "$LOCAL_WORKLOAD" "$TARGET_NODE:$remote_workload"

  ssh_run "bash -lc $(printf '%q' "
set -euo pipefail
cd '$REMOTE_REPO/scripts'
if [[ ! -f '$REMOTE_REPO/.bench_tmp/shared_postgresql.conf' && -f '$REMOTE_REPO/postgresql.conf' ]]; then
  cp '$REMOTE_REPO/postgresql.conf' '$REMOTE_REPO/.bench_tmp/shared_postgresql.conf' || true
fi
bash '$REMOTE_REPO/scripts/distributed/ensure_custom_install_from_repo.sh' \
  --repo-root '$REMOTE_REPO' --install-dir '$REMOTE_INSTALL' --clean-when-rebuild
export ARIABC_REQUIRE_CUSTOM_PG=1
export ARIABC_PSQL='$REMOTE_INSTALL/bin/psql'
export ARIABC_INSTALL_DIR='$REMOTE_INSTALL'
export ARIABC_DIR='$REMOTE_REPO'
export ARIABC_PGPORT='$DB_PORT'
export LD_LIBRARY_PATH='$REMOTE_INSTALL/lib:\${LD_LIBRARY_PATH:-}'
PYTHON_BIN=''
if [[ -x '$REMOTE_REPO/.venv/bin/python3' ]] && '$REMOTE_REPO/.venv/bin/python3' -c 'import psycopg' >/dev/null 2>&1; then
  PYTHON_BIN='$REMOTE_REPO/.venv/bin/python3'
elif python3 -c 'import psycopg' >/dev/null 2>&1; then
  PYTHON_BIN=python3
else
  PYTHON_BIN='$REMOTE_REPO/.venv/bin/python3'
  python3 -m venv --clear '$REMOTE_REPO/.venv'
  \"\$PYTHON_BIN\" -m pip install -q --disable-pip-version-check 'psycopg[binary]' psycopg >/dev/null
fi
export ARIABC_PYTHON=\"\$PYTHON_BIN\"
pgdata_line=\$(bash '$REMOTE_REPO/scripts/distributed/ensure_single_node_postgres.sh' \
  --repo-root '$REMOTE_REPO' --install-dir '$REMOTE_INSTALL' \
  --db-port '$DB_PORT' --db-user '$DB_USER' --db-name '$DB_NAME' \
  --template-config '$REMOTE_REPO/.bench_tmp/shared_postgresql.conf' \
  --require-custom | tail -n 1)
[[ \$pgdata_line == PGDATA=* ]] && export ARIABC_PGDATA=\${pgdata_line#PGDATA=}
if ! \$PYTHON_BIN -c 'import psycopg' >/dev/null 2>&1; then
  \$PYTHON_BIN -m pip install -q --disable-pip-version-check 'psycopg[binary]' psycopg >/dev/null
fi
export BCDB_BLOCK_PROFILE=1
: > '$REMOTE_REPO/server.log' || true
if [[ '$SINGLE_BCDB_PHASE_TRACE' != '0' ]]; then
  rm -f '$remote_out/bcdb_phase_trace_single.'*
  export BCDB_PHASE_TRACE='$remote_out/bcdb_phase_trace_single'
fi
\$PYTHON_BIN -u bench_threads_matrix.py \
  --modes pg,det \
  --signing-modes 0 \
  --enforce-signatures 0 \
  --threads 1 \
  --runs 1 \
  --workloads '$remote_workload_rel' \
  --db '$DB_NAME' --user '$DB_USER' --port '$DB_PORT' \
  --out-dir '$remote_out' \
  --no-resume \
  --timeout-workload-s '$SINGLE_TIMEOUT_S' \
  --timeout-workload-det-s '$SINGLE_TIMEOUT_S'
cp '$REMOTE_REPO/server.log' '$remote_out/server.log' 2>/dev/null || true
")" > "$log_file" 2>&1

  log "Collecting single-node artifacts"
  rsync_from_target "$TARGET_NODE:$remote_out/" "$SINGLE_LOCAL_DIR/"
}

run_full_system() {
  local before_file="$RUN_LOG_DIR/full_before.txt"
  local after_file="$RUN_LOG_DIR/full_after.txt"
  local full_log="$RUN_LOG_DIR/full_system.log"
  local extra_skip=()

  log "Running full-system one-transaction Kafka-majority profile"
  ls -td "$REPO_ROOT"/scripts/bench_full_results/cluster4_* 2>/dev/null > "$before_file" || true
  [[ "$FULL_SKIP_SYNC" == "1" ]] && extra_skip+=(--skip-sync)
  [[ "$FULL_SKIP_BUILD" == "1" ]] && extra_skip+=(--skip-build)
  [[ "$FULL_SKIP_RDKAFKA_SETUP" == "1" ]] && extra_skip+=(--skip-rdkafka-setup)

  set +e
  timeout "$FULL_CASE_TIMEOUT_S" env POLL_COUNT="$POLL_COUNT" RESULT_RING_CAPACITY="$RESULT_RING_CAPACITY" \
    "$REPO_ROOT/scripts/distributed/run_4node_raft_cluster.sh" \
      "${extra_skip[@]}" \
      --workload "$LOCAL_WORKLOAD" \
      --test-queries 1 \
      --req-id-offset 9100001 \
      --pool-size "$FULL_POOL_SIZE" \
      --bcdb-worker-count "$FULL_BCDB_WORKER_COUNT" \
      --bcdb-decouple-workers 1 \
      --det-batch-size "$FULL_DET_BATCH_SIZE" \
      --det-window "$FULL_DET_WINDOW" \
      --det-block-pipeline "$FULL_DET_BLOCK_PIPELINE" \
      --det-block-max "$FULL_DET_BLOCK_MAX" \
      --num-terminals 1 \
      --bcdb-block-profile 1 \
      --bcdb-phase-trace "$FULL_BCDB_PHASE_TRACE" \
      --bcdb-serial-gate-mode 1 \
      --bcdb-dt-skip-readonly-gate 1 \
      > "$full_log" 2>&1
  local rc=$?
  set -e
  echo "$rc" > "$RUN_LOG_DIR/full_exit_code.txt"

  ls -td "$REPO_ROOT"/scripts/bench_full_results/cluster4_* 2>/dev/null > "$after_file" || true
  local artifact
  artifact="$(grep -vxF -f "$before_file" "$after_file" | head -n 1 || true)"
  if [[ -z "$artifact" ]]; then
    artifact="$(head -n 1 "$after_file" || true)"
  fi
  [[ -n "$artifact" ]] || die "could not identify full-system artifact"
  printf '%s\n' "$artifact" > "$OUT_ROOT/full_artifact.txt"
  if [[ "$rc" != "0" ]]; then
    log "WARNING: full-system run exited rc=$rc; report will mark it invalid"
  fi
}

generate_report() {
  log "Generating summary and breakdown"
  python3 - "$OUT_ROOT" "$SINGLE_LOCAL_DIR" "$(cat "$OUT_ROOT/full_artifact.txt" 2>/dev/null || true)" <<'PY'
import csv
import glob
import json
import os
import re
import sys
from pathlib import Path

out = Path(sys.argv[1])
single = Path(sys.argv[2])
full = Path(sys.argv[3]) if len(sys.argv) > 3 and sys.argv[3] else None

kv_re = re.compile(r"(\w+)=([^\s]+)")
overall_re = re.compile(r"overall time taken \(millisec\)\s*=\s*([0-9.]+)")
wait_re = re.compile(r"total wait time \(ms\)\s*([0-9.]+)")
majority_re = re.compile(r"majority wait time \(ms\)\s*([0-9.]+)")
submit_re = re.compile(r"submit time \(ms\)\s*([0-9.]+)")

def fnum(v):
    try:
        return float(v)
    except Exception:
        return None

def parse_single_results():
    rows = {}
    path = single / "results.csv"
    if not path.exists():
        return rows
    with path.open(newline="") as fh:
        for r in csv.DictReader(fh):
            mode = (r.get("mode") or "").strip()
            ms = fnum(r.get("workload_overall_ms"))
            rows[mode] = {
                "mode": mode,
                "overall_ms": ms,
                "single_tx_tps": (1000.0 / ms) if ms and ms > 0 else None,
                "wait_ms": fnum(r.get("workload_wait_ms")),
                "duplicate_key_errors": r.get("duplicate_key_errors", ""),
                "retries_total": r.get("retries_total", ""),
                "permanent_failures": r.get("permanent_failures", ""),
                "merkle_verify": r.get("db_merkle_verify", ""),
                "workload_log": r.get("workload_log", ""),
            }
    return rows

def parse_latest_bcdb_blocks(paths):
    by_node = {}
    for path in paths:
        node = Path(path).name
        groups = {}
        for line in Path(path).read_text(errors="replace").splitlines():
            if "PROFILE_BCDB_BLOCK" not in line:
                continue
            d = dict(kv_re.findall(line))
            pid = d.get("pid", "unknown")
            txs = int(float(d.get("block_txs", "0")))
            groups.setdefault(pid, []).append((txs, d))
        # Pick the latest pid group. For bulk runs, workload rows have
        # block_txs>1. For this script's one-statement full-system run, the
        # same backend usually logs three block_txs=1 rows: preflight, workload,
        # and post-verify marker. In that case, the workload is the middle row.
        candidates = []
        for pid, rows in groups.items():
            if rows:
                candidates.append((rows[-1][1], pid, rows))
        if not candidates:
            continue
        _last, pid, rows = candidates[-1]
        workload_rows = [(t, d) for t, d in rows if t > 1]
        if not workload_rows:
            if len(rows) >= 3:
                workload_rows = [rows[-2]]
            else:
                workload_rows = [rows[-1]]
        tx_sum = sum(t for t, _ in workload_rows)
        sums = {k: 0.0 for k in ["total_ms", "parse_ms", "enqueue_ms", "wait_block_ms", "wait_slot_ms", "format_ms"]}
        blocks = 0
        for txs, d in workload_rows:
            blocks += 1
            for k in sums:
                sums[k] += float(d.get(k, 0) or 0)
        by_node[node] = {"pid": pid, "blocks": blocks, "txs": tx_sum, **sums}
    return by_node

def parse_gateway(log_text):
    g = {}
    m = overall_re.search(log_text)
    if m: g["overall_ms"] = float(m.group(1))
    m = submit_re.search(log_text)
    if m: g["submit_ms"] = float(m.group(1))
    m = majority_re.search(log_text)
    if m: g["majority_wait_ms"] = float(m.group(1))
    m = wait_re.search(log_text)
    if m: g["wait_ms"] = float(m.group(1))
    prof = ""
    for line in log_text.splitlines():
        if line.startswith("PROFILE_GATEWAY"):
            prof = line
            break
    if prof:
        g.update({k: fnum(v) if re.match(r"^-?[0-9.]+$", v) else v for k, v in kv_re.findall(prof)})
    return g

def parse_server_profiles(paths):
    out_rows = {}
    for path in paths:
        text = Path(path).read_text(errors="replace")
        line = next((ln for ln in text.splitlines() if "PROFILE_SERVER" in ln), "")
        if not line:
            continue
        d = {k: fnum(v) if re.match(r"^-?[0-9.eE+]+$", v) else v for k, v in kv_re.findall(line)}
        out_rows[Path(path).name] = d
    return out_rows

def parse_phase_traces(paths):
    rows = {}
    fields = [
        "parse_plan_us", "portal_run_us", "gate_us", "serial_slot_wait_us",
        "conflict_us", "apply_us", "finish_us", "publish_ws_us",
        "publish_hash_clear_us", "apply_merkle_prep_us",
        "apply_merkle_update_us", "finish_result_us", "finish_publish_us",
    ]
    for path in paths:
        p = Path(path)
        try:
            with p.open(newline="", errors="replace") as fh:
                data = list(csv.DictReader(fh))
        except Exception:
            continue
        if not data:
            continue
        sums = {k: 0.0 for k in fields}
        counts = {
            "rows": len(data),
            "restarts": 0,
            "insert_count": 0,
            "update_count": 0,
            "delete_count": 0,
            "merkle_update_count": 0,
        }
        for r in data:
            counts["restarts"] += int(r.get("restarts") or 0)
            counts["insert_count"] += int(r.get("apply_insert_count") or 0)
            counts["update_count"] += int(r.get("apply_update_count") or 0)
            counts["delete_count"] += int(r.get("apply_delete_count") or 0)
            counts["merkle_update_count"] += int(r.get("merkle_update_count") or 0)
            for k in fields:
                v = int(r.get(k) or 0)
                if v > 1_000_000_000:
                    continue
                sums[k] += v
        rows[p.name] = {**counts, **sums}
    return rows

single_rows = parse_single_results()
single_bcdb = parse_latest_bcdb_blocks([str(single / "server.log")]) if (single / "server.log").exists() else {}
single_phase = parse_phase_traces(glob.glob(str(single / "bcdb_phase_trace_single.*")))

full_log_text = ""
gateway = {}
server_profiles = {}
full_bcdb = {}
full_phase = {}
full_valid = False
if full and full.exists():
    gwlog = full / "gateway_test.log"
    full_log_text = gwlog.read_text(errors="replace") if gwlog.exists() else ""
    gateway = parse_gateway(full_log_text)
    server_profiles = parse_server_profiles(glob.glob(str(full / "server_node*.log")))
    full_bcdb = parse_latest_bcdb_blocks(glob.glob(str(full / "postgres_node*.log")))
    full_phase = parse_phase_traces(glob.glob(str(full / "bcdb_phase_trace_node*.*")))
    full_valid = (
        "completion_path=kafka_majority" in full_log_text
        and "divergence_count=0" in full_log_text
        and "permanent_failures=0" in full_log_text
    )

summary_rows = []
for mode in ["pg", "det"]:
    r = single_rows.get(mode, {})
    summary_rows.append({
        "mode": mode,
        "system": "single_node",
        "valid": "1" if r else "0",
        "overall_ms": r.get("overall_ms", ""),
        "single_tx_tps": r.get("single_tx_tps", ""),
        "wait_ms": r.get("wait_ms", ""),
        "submit_ms": "",
        "majority_wait_ms": "",
        "kafka_recs": "",
        "kc_poll_ms": "",
        "kafka_add_reply_ms": "",
        "consume_to_ready_ms_p95": "",
        "ready_queue_depth_max": "",
        "artifact": str(single),
    })
summary_rows.append({
    "mode": "full",
    "system": "kafka_raft_bcdb_4node",
    "valid": "1" if full_valid else "0",
    "overall_ms": gateway.get("overall_ms", ""),
    "single_tx_tps": (1000.0 / gateway["overall_ms"]) if gateway.get("overall_ms") else "",
    "wait_ms": gateway.get("wait_ms", ""),
    "submit_ms": gateway.get("submit_ms", ""),
    "majority_wait_ms": gateway.get("majority_wait_ms", ""),
    "kafka_recs": gateway.get("kafka_recs", ""),
    "kc_poll_ms": gateway.get("kc_poll_ms", ""),
    "kafka_add_reply_ms": gateway.get("kafka_add_reply_ms", ""),
    "consume_to_ready_ms_p95": gateway.get("consume_to_ready_ms_p95", ""),
    "ready_queue_depth_max": gateway.get("ready_queue_depth_max", ""),
    "artifact": str(full) if full else "",
})

with (out / "summary.csv").open("w", newline="") as fh:
    writer = csv.DictWriter(fh, fieldnames=list(summary_rows[0].keys()))
    writer.writeheader()
    writer.writerows(summary_rows)

def fmt(v, unit=""):
    if v == "" or v is None:
        return "n/a"
    try:
        return f"{float(v):.3f}{unit}"
    except Exception:
        return f"{v}{unit}"

def md_table(headers, rows):
    lines = ["| " + " | ".join(headers) + " |", "| " + " | ".join(["---"] * len(headers)) + " |"]
    for row in rows:
        lines.append("| " + " | ".join(str(x) for x in row) + " |")
    return "\n".join(lines)

md = []
md.append("# Single Transaction PG vs DET vs Full-System Profile")
md.append("")
md.append("This is a latency microscope, not a steady-state TPS benchmark. `single_tx_tps` is `1000 / overall_ms` for one statement.")
md.append("")
md.append("## Headline")
md.append(md_table(
    ["mode", "valid", "overall_ms", "single_tx_tps", "artifact"],
    [[r["mode"], r["valid"], fmt(r["overall_ms"], " ms"), fmt(r["single_tx_tps"], " tx/s"), r["artifact"]] for r in summary_rows],
))
md.append("")
pg_ms = single_rows.get("pg", {}).get("overall_ms")
det_ms = single_rows.get("det", {}).get("overall_ms")
full_ms = gateway.get("overall_ms")
if pg_ms and det_ms:
    md.append(f"- PG -> DET extra latency: `{det_ms - pg_ms:.3f} ms` (`{((det_ms / pg_ms) - 1.0) * 100.0:.2f}%`).")
if det_ms and full_ms:
    md.append(f"- DET -> full-system extra latency: `{full_ms - det_ms:.3f} ms` (`{((full_ms / det_ms) - 1.0) * 100.0:.2f}%`).")
md.append("")
md.append("## Gateway Breakdown")
md.append(md_table(
    ["field", "value"],
    [
        ["overall_ms", fmt(gateway.get("overall_ms"), " ms")],
        ["submit_ms", fmt(gateway.get("submit_ms"), " ms")],
        ["majority_wait_ms", fmt(gateway.get("majority_wait_ms"), " ms")],
        ["kafka_recs", fmt(gateway.get("kafka_recs"))],
        ["kc_poll_ms", fmt(gateway.get("kc_poll_ms"), " ms")],
        ["kafka_add_reply_ms", fmt(gateway.get("kafka_add_reply_ms"), " ms")],
        ["consume_to_ready_ms_p95", fmt(gateway.get("consume_to_ready_ms_p95"), " ms")],
        ["ready_queue_depth_max", fmt(gateway.get("ready_queue_depth_max"))],
    ],
))
md.append("")
md.append("## Full-System Server Breakdown")
srv_rows = []
for name, d in sorted(server_profiles.items()):
    exec_calls = d.get("exec_calls") or 0
    per = (d.get("exec_ms") * 1000.0 / exec_calls) if exec_calls else None
    srv_rows.append([
        name,
        fmt(d.get("exec_ms"), " ms"),
        fmt(d.get("pg_query_ms"), " ms"),
        fmt(per, " us/stmt"),
        fmt(d.get("queue_depth_avg")),
        fmt(d.get("queue_depth_max")),
        fmt(d.get("kafka_build_ms"), " ms"),
        fmt(d.get("kafka_send_ms"), " ms"),
    ])
md.append(md_table(["node", "exec_ms", "pg_query_ms", "exec_us_per_stmt", "q_avg", "q_max", "kafka_build", "kafka_send"], srv_rows or [["n/a"] * 8]))
md.append("")
md.append("## BCDB Block Breakdown")
def bcdb_rows(blocks):
    rows = []
    for node, d in sorted(blocks.items()):
        txs = d.get("txs") or 0
        rows.append([
            node,
            d.get("blocks"),
            txs,
            fmt(d.get("total_ms"), " ms"),
            fmt(d.get("parse_ms"), " ms"),
            fmt(d.get("enqueue_ms"), " ms"),
            fmt(d.get("wait_block_ms"), " ms"),
            fmt(d.get("format_ms"), " ms"),
            fmt((d.get("total_ms", 0) * 1000.0 / txs) if txs else None, " us/stmt"),
        ])
    return rows
md.append("### Single DET")
md.append(md_table(["source", "blocks", "txs", "total", "parse", "enqueue", "wait_block", "format", "total_us_per_stmt"], bcdb_rows(single_bcdb) or [["n/a"] * 9]))
md.append("")
md.append("### Full System")
md.append(md_table(["node", "blocks", "txs", "total", "parse", "enqueue", "wait_block", "format", "total_us_per_stmt"], bcdb_rows(full_bcdb) or [["n/a"] * 9]))
md.append("")
md.append("## BCDB Phase Trace")
def phase_rows(phase):
    rows = []
    for name, d in sorted(phase.items()):
        total = sum(float(d.get(k, 0) or 0) for k in [
            "parse_plan_us", "portal_run_us", "gate_us", "conflict_us",
            "apply_us", "finish_us",
        ])
        rows.append([
            name,
            d.get("rows"),
            d.get("restarts"),
            fmt(total / 1000.0, " ms"),
            fmt(d.get("parse_plan_us", 0) / 1000.0, " ms"),
            fmt(d.get("gate_us", 0) / 1000.0, " ms"),
            fmt(d.get("serial_slot_wait_us", 0) / 1000.0, " ms"),
            fmt(d.get("apply_us", 0) / 1000.0, " ms"),
            fmt(d.get("finish_us", 0) / 1000.0, " ms"),
            d.get("insert_count"),
            d.get("update_count"),
            d.get("delete_count"),
            d.get("merkle_update_count"),
        ])
    return rows
md.append("### Single DET Phase Trace")
md.append(md_table(["trace", "rows", "restarts", "sum_core", "parse", "gate", "serial_wait", "apply", "finish", "ins", "upd", "del", "merkle"], phase_rows(single_phase) or [["n/a"] * 13]))
md.append("")
md.append("### Full-System Phase Trace")
md.append(md_table(["trace", "rows", "restarts", "sum_core", "parse", "gate", "serial_wait", "apply", "finish", "ins", "upd", "del", "merkle"], phase_rows(full_phase) or [["n/a"] * 13]))
md.append("")
md.append("## Example Flow")
md.append("PG: client sends the SQL directly to PostgreSQL; the measured time is mostly one client/server round trip plus SQL execution.")
md.append("")
md.append("DET: the same SQL is assigned deterministic order, enters the BCDB deterministic queue, waits for its result slot/block readiness, applies in PostgreSQL, then publishes deterministic result/state.")
md.append("")
md.append("Full system: gateway assigns the request, sends it through the 4-node ordered path, replicas execute deterministic apply, replicas publish Kafka result/hash records, and the gateway counts success only after majority readiness/validation.")
md.append("")
if full and full.exists():
    md.append(f"Raw full-system artifact: `{full}`")
md.append(f"Raw single-node artifact: `{single}`")

(out / "breakdown.md").write_text("\n".join(md) + "\n")
(out / "breakdown.json").write_text(json.dumps({
    "single": single_rows,
    "gateway": gateway,
    "server_profiles": server_profiles,
    "single_bcdb": single_bcdb,
    "full_bcdb": full_bcdb,
    "single_phase": single_phase,
    "full_phase": full_phase,
    "full_valid": full_valid,
}, indent=2) + "\n")
PY
}

log "=== Single transaction PG/DET/full profile ==="
log "Out root: $OUT_ROOT"
log "SQL     : $SQL"
sync_single_target
run_single_pg_det
run_full_system
generate_report
log "Done"
log "Summary  : $OUT_ROOT/summary.csv"
log "Breakdown: $OUT_ROOT/breakdown.md"
