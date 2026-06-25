#!/usr/bin/env bash
set -euo pipefail

# End-to-end 3-node single-machine benchmark pipeline:
# 1) Confidence stage (small run) with progress validation.
# 2) Full stage (requested run matrix) across all 3 nodes in parallel.
# 3) Local export bundling, graph verification/regeneration, and throughput analysis.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
source "$SCRIPT_DIR/benchmark_defaults.sh"
RUNNER="$SCRIPT_DIR/run_single_machine_matrix_all_nodes.sh"

if [[ ! -x "$RUNNER" ]]; then
  echo "ERROR: runner not executable: $RUNNER" >&2
  exit 1
fi

# Defaults aligned with your recent successful runs.
NODES_CSV="neel@10.129.148.248,neel@10.129.148.248,neel@10.129.148.246"
SSH_KEY="/home/neel/.ssh/id_rsa"
SSH_PORT=22
REMOTE_REPO_ROOT="/home/neel/Desktop/ariabc_cluster"
REMOTE_INSTALL_DIR="/home/neel/Desktop/ariabc_install"
DB_PORT=5438
MODES="det"
DB_NAME="postgres"
DB_USER="postgres"
LOCAL_POSTGRES_CONF="/work/ARIABC/pgdata/postgresql.conf"

# Confidence stage: enough to prove run loop is active and stable.
CONF_THREADS="1,2"
CONF_RUNS=2

# Full stage: requested matrix.
FULL_THREADS="$ARIABC_DEFAULT_FULL_THREADS"
FULL_RUNS=3
WORKLOADS="ycsbtx-skew-01-24k-pt-intkey-sid-clean-20k.txt,ycsb-skew0-99-tx-20k-point-safedb-intkey-insert12k-uniq.txt"

usage() {
  cat <<'EOF'
Usage:
  scripts/distributed/run_3node_benchmark_pipeline.sh [options]

Options:
  --nodes <csv>                 user@host list
  --ssh-key <path>              SSH private key path
  --ssh-port <port>             SSH port (default: 22)
  --remote-repo-root <path>     Remote repo root (default: /home/neel/Desktop/ariabc_cluster)
  --remote-install-dir <path>   Remote install dir (default: /home/neel/Desktop/ariabc_install)
  --db-port <port>              Benchmark postgres port (default: 5438)
  --modes <csv>                 Modes to run (default: det)
  --workloads <csv>             Full-stage workloads CSV
  --conf-threads <csv>          Confidence-stage thread CSV (default: 1,2)
  --conf-runs <n>               Confidence-stage runs (default: 2)
  --full-threads <csv>          Full-stage thread CSV
  --full-runs <n>               Full-stage runs (default: 3)
  --db-name <name>              DB name (default: postgres)
  --db-user <name>              DB user (default: postgres)
  -h, --help                    Show this help

Outputs:
  scripts/bench_full_results/pipeline_<timestamp>/
    logs/                per-node stage logs
    confidence_export/   bundled confidence-stage outputs
    full_export/         bundled full-stage outputs
    analysis/            CSV + markdown summaries
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
    --nodes) NODES_CSV="${2:-}"; shift 2 ;;
    --ssh-key) SSH_KEY="${2:-}"; shift 2 ;;
    --ssh-port) SSH_PORT="${2:-22}"; shift 2 ;;
    --remote-repo-root) REMOTE_REPO_ROOT="${2:-}"; shift 2 ;;
    --remote-install-dir) REMOTE_INSTALL_DIR="${2:-}"; shift 2 ;;
    --db-port) DB_PORT="${2:-5438}"; shift 2 ;;
    --modes) MODES="${2:-det}"; shift 2 ;;
    --workloads) WORKLOADS="${2:-}"; shift 2 ;;
    --conf-threads) CONF_THREADS="${2:-1,2}"; shift 2 ;;
    --conf-runs) CONF_RUNS="${2:-2}"; shift 2 ;;
    --full-threads) FULL_THREADS="${2:-}"; shift 2 ;;
    --full-runs) FULL_RUNS="${2:-3}"; shift 2 ;;
    --db-name) DB_NAME="${2:-postgres}"; shift 2 ;;
    --db-user) DB_USER="${2:-postgres}"; shift 2 ;;
    --local-postgres-conf)
      echo "ERROR: --local-postgres-conf is no longer supported. Canonical config is always enforced from /work/ARIABC/pgdata/postgresql.conf" >&2
      exit 2
      ;;
    -h|--help) usage; exit 0 ;;
    *)
      echo "Unknown arg: $1" >&2
      usage
      exit 2
      ;;
  esac
done

declare -a NODES=()
split_csv "$NODES_CSV" NODES
if [[ "${#NODES[@]}" -ne 3 ]]; then
  echo "ERROR: expected exactly 3 nodes, got ${#NODES[@]}: $NODES_CSV" >&2
  exit 2
fi

if [[ -z "$WORKLOADS" ]]; then
  echo "ERROR: workloads cannot be empty" >&2
  exit 2
fi

if [[ ! -f "$LOCAL_POSTGRES_CONF" ]]; then
  echo "ERROR: canonical local postgres config not found: $LOCAL_POSTGRES_CONF" >&2
  exit 2
fi

if [[ ! -f "$SSH_KEY" ]]; then
  echo "ERROR: SSH key not found: $SSH_KEY" >&2
  exit 2
fi

if ! command -v python3 >/dev/null 2>&1; then
  echo "ERROR: python3 is required" >&2
  exit 1
fi

ts="$(date +%Y%m%d_%H%M%S)"
pipeline_root="$REPO_ROOT/scripts/bench_full_results/pipeline_${ts}"
log_root="$pipeline_root/logs"
conf_export="$pipeline_root/confidence_export"
full_export="$pipeline_root/full_export"
analysis_dir="$pipeline_root/analysis"
mkdir -p "$log_root" "$conf_export" "$full_export" "$analysis_dir"

ssh_base=(ssh -o BatchMode=yes -o StrictHostKeyChecking=no -p "$SSH_PORT" -i "$SSH_KEY")

echo "== 3-node benchmark pipeline =="
echo "Nodes            : ${NODES[*]}"
echo "Modes            : $MODES"
echo "DB port          : $DB_PORT"
echo "Config template  : $LOCAL_POSTGRES_CONF"
echo "Confidence stage : threads=$CONF_THREADS runs=$CONF_RUNS"
echo "Full stage       : threads=$FULL_THREADS runs=$FULL_RUNS"
echo "Workloads        : $WORKLOADS"
echo "Pipeline root    : $pipeline_root"
echo

cleanup_port_on_nodes() {
  local stage="$1"
  echo "[$stage] Cleaning remote port $DB_PORT on all nodes"
  for node in "${NODES[@]}"; do
    (
      "${ssh_base[@]}" "$node" "fuser -k ${DB_PORT}/tcp >/dev/null 2>&1 || true; sleep 1; if fuser ${DB_PORT}/tcp >/dev/null 2>&1; then echo port_busy; exit 1; else echo port_free; fi"
    ) >"$log_root/${stage}_$(echo "$node" | tr '@.' '__').port.log" 2>&1 || {
      echo "ERROR: port cleanup failed on $node (see $log_root/${stage}_$(echo "$node" | tr '@.' '__').port.log)" >&2
      return 1
    }
  done
}

progress_seen() {
  local file="$1"
  # Return success if we saw at least the second progress line ([2/..]).
  grep -Eq '^\[[2-9][0-9]*/|^\[[2-9]/' "$file"
}

run_stage_parallel() {
  local stage="$1"
  local threads="$2"
  local runs="$3"
  local out_bundle="$4"

  local -a pids=()
  local -a stage_logs=()
  local -a stage_exits=()
  local -a stage_roots=()

  echo "[$stage] Launching all 3 nodes in parallel"

  for node in "${NODES[@]}"; do
    local tag
    tag="$(echo "$node" | tr '@.' '__')"
    local log="$log_root/${stage}_${tag}.log"
    local exitf="$log_root/${stage}_${tag}.exit"
    stage_logs+=("$log")
    stage_exits+=("$exitf")

    (
      set +e
      /usr/bin/time -p "$RUNNER" \
        --nodes "$node" \
        --ssh-key "$SSH_KEY" \
        --ssh-port "$SSH_PORT" \
        --remote-repo-root "$REMOTE_REPO_ROOT" \
        --remote-install-dir "$REMOTE_INSTALL_DIR" \
        --modes "$MODES" \
        --threads "$threads" \
        --runs "$runs" \
        --workloads "$WORKLOADS" \
        --db-name "$DB_NAME" \
        --db-user "$DB_USER" \
        --db-port "$DB_PORT" \
        >"$log" 2>&1
      ec=$?
      echo "$ec" >"$exitf"
      exit "$ec"
    ) &

    pids+=("$!")
    echo "[$stage] started $node pid=${pids[-1]} log=$log"
  done

  echo "[$stage] Waiting for benchmark progress proof ([2/...])"
  local deadline=$((SECONDS + 900))
  while (( SECONDS < deadline )); do
    local ready=0
    for log in "${stage_logs[@]}"; do
      if [[ -f "$log" ]] && progress_seen "$log"; then
        ready=$((ready + 1))
      fi
    done
    if [[ "$ready" -eq 3 ]]; then
      echo "[$stage] Progress confirmed on all nodes (>=2 runs/steps started)"
      break
    fi
    sleep 10
  done

  local ready=0
  for log in "${stage_logs[@]}"; do
    if [[ -f "$log" ]] && progress_seen "$log"; then
      ready=$((ready + 1))
    fi
  done
  if [[ "$ready" -lt 3 ]]; then
    echo "ERROR: did not observe progress proof on all nodes for stage=$stage" >&2
    for log in "${stage_logs[@]}"; do
      echo "--- tail $log ---" >&2
      tail -n 30 "$log" >&2 || true
    done
    return 1
  fi

  echo "[$stage] Waiting for all node jobs to finish"
  local failed=0
  for pid in "${pids[@]}"; do
    if ! wait "$pid"; then
      failed=1
    fi
  done

  for idx in 0 1 2; do
    local ec="missing"
    [[ -f "${stage_exits[$idx]}" ]] && ec="$(cat "${stage_exits[$idx]}")"
    echo "[$stage] node=${NODES[$idx]} exit=$ec"
    if [[ "$ec" != "0" ]]; then
      failed=1
      echo "--- tail ${stage_logs[$idx]} ---" >&2
      tail -n 60 "${stage_logs[$idx]}" >&2 || true
    fi
  done

  if [[ "$failed" -ne 0 ]]; then
    echo "ERROR: one or more node jobs failed in stage=$stage" >&2
    return 1
  fi

  mkdir -p "$out_bundle"
  for idx in 0 1 2; do
    local node="${NODES[$idx]}"
    local tag
    tag="$(echo "$node" | tr '@.' '__')"
    local log="${stage_logs[$idx]}"
    local root
    root="$(grep -E '^Collected outputs: ' "$log" | tail -n 1 | sed 's/^Collected outputs: //')"
    if [[ -z "$root" || ! -d "$root" ]]; then
      echo "ERROR: cannot parse local output root from $log" >&2
      return 1
    fi

    local node_safe
    node_safe="${node//@/_at_}"
    node_safe="${node_safe//./_}"

    local node_dir
    node_dir="$(find "$root" -maxdepth 1 -mindepth 1 -type d -name "$node_safe" | head -n 1)"
    if [[ -z "$node_dir" || ! -d "$node_dir" ]]; then
      echo "ERROR: node dir not found under root=$root for node=$node" >&2
      return 1
    fi

    cp -a "$node_dir" "$out_bundle/"
    stage_roots+=("$node_dir")
  done

  echo "[$stage] Bundled exports in $out_bundle"
}

verify_graphs() {
  local dir="$1"
  local missing=0
  while IFS= read -r summary; do
    local node_dir
    node_dir="$(dirname "$summary")"
    local count
    count="$(find "$node_dir" -maxdepth 1 -type f -name 'tps_vs_threads_*.png' | wc -l | awk '{print $1}')"
    if [[ "$count" == "0" ]]; then
      missing=1
      echo "[graph] missing in $node_dir -> regenerating"
      python3 - <<PY
import importlib.util
from pathlib import Path
module_path = Path(r"$REPO_ROOT/scripts/bench_threads_matrix.py")
summary_csv = Path(r"$summary")
out_dir = Path(r"$node_dir")
spec = importlib.util.spec_from_file_location("bench_threads_matrix_mod", str(module_path))
mod = importlib.util.module_from_spec(spec)
assert spec.loader is not None
spec.loader.exec_module(mod)
mod._generate_tps_graphs(summary_csv, out_dir)
print("graphs_regenerated", out_dir)
PY
    fi
  done < <(find "$dir" -type f -name summary.csv | sort)

  if [[ "$missing" -eq 0 ]]; then
    echo "[graph] all summaries already have TPS graphs"
  fi
}

analyze_results() {
  local full_dir="$1"
  local out_csv="$analysis_dir/throughput_summary.csv"
  local out_md="$analysis_dir/throughput_summary.md"

  FULL_DIR="$full_dir" OUT_CSV="$out_csv" OUT_MD="$out_md" python3 - <<'PY'
import csv
import os
import statistics
from pathlib import Path

full_dir = Path(os.environ["FULL_DIR"])
out_csv = Path(os.environ["OUT_CSV"])
out_md = Path(os.environ["OUT_MD"])

rows = []
for summary in sorted(full_dir.rglob("summary.csv")):
    node_dir = summary.parent.name
    node = node_dir.replace("_at_", "@").replace("_", ".")
    with summary.open() as fh:
        for r in csv.DictReader(fh):
            rows.append({
                "node": node,
                "workload": r["workload"],
                "threads": int(r["threads"]),
                "mean_tps": float(r["mean_throughput_tps"]),
                "median_tps": float(r["median_throughput_tps"]),
                "pass_rate": float(r["pass_rate_merkle_verify"]),
            })

if not rows:
    raise SystemExit("No summary rows found")

workloads = sorted({r["workload"] for r in rows})
nodes = sorted({r["node"] for r in rows})

out_rows = []
for node in nodes:
    for w in workloads:
        rs = [r for r in rows if r["node"] == node and r["workload"] == w]
        peak = max(rs, key=lambda x: x["mean_tps"])
        median_curve = statistics.median(r["mean_tps"] for r in rs)
        t1 = next((r["mean_tps"] for r in rs if r["threads"] == 1), float("nan"))
        t20 = next((r["mean_tps"] for r in rs if r["threads"] == 20), float("nan"))
        pass_min = min(r["pass_rate"] for r in rs)
        pass_max = max(r["pass_rate"] for r in rs)
        out_rows.append({
            "node": node,
            "workload": w,
            "peak_tps": peak["mean_tps"],
            "peak_threads": peak["threads"],
            "median_curve_tps": median_curve,
            "threads_1_tps": t1,
            "threads_20_tps": t20,
            "pass_rate_min": pass_min,
            "pass_rate_max": pass_max,
        })

with out_csv.open("w", newline="") as fh:
    wr = csv.DictWriter(fh, fieldnames=list(out_rows[0].keys()))
    wr.writeheader()
    wr.writerows(out_rows)

lines = []
lines.append("# Throughput Summary")
lines.append("")
lines.append("| Node | Workload | Peak TPS | Peak Threads | Median Curve TPS | TPS@1 | TPS@20 | Pass Rate Min..Max |")
lines.append("|---|---|---:|---:|---:|---:|---:|---:|")
for r in out_rows:
    lines.append(
        f"| `{r['node']}` | `{r['workload']}` | {r['peak_tps']:.3f} | {r['peak_threads']} | {r['median_curve_tps']:.3f} | {r['threads_1_tps']:.3f} | {r['threads_20_tps']:.3f} | {r['pass_rate_min']:.3f}..{r['pass_rate_max']:.3f} |"
    )

lines.append("")
lines.append("## Relative Peak vs Best (per workload)")
for w in workloads:
    ws = [r for r in out_rows if r["workload"] == w]
    best = max(r["peak_tps"] for r in ws)
    lines.append("")
    lines.append(f"- workload `{w}` (best peak {best:.3f})")
    for r in sorted(ws, key=lambda x: x["peak_tps"], reverse=True):
        pct = 100.0 * r["peak_tps"] / best if best else 0.0
        lines.append(f"  - `{r['node']}`: {pct:.2f}%")

out_md.write_text("\n".join(lines) + "\n")
print(f"wrote {out_csv}")
print(f"wrote {out_md}")
PY
}

cleanup_port_on_nodes "confidence"
run_stage_parallel "confidence" "$CONF_THREADS" "$CONF_RUNS" "$conf_export"
verify_graphs "$conf_export"

cleanup_port_on_nodes "full"
run_stage_parallel "full" "$FULL_THREADS" "$FULL_RUNS" "$full_export"
verify_graphs "$full_export"
analyze_results "$full_export"

echo
echo "Pipeline completed successfully."
echo "Logs     : $log_root"
echo "Conf out : $conf_export"
echo "Full out : $full_export"
echo "Analysis : $analysis_dir"
echo
cat "$analysis_dir/throughput_summary.md"
