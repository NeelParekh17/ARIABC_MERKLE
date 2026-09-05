#!/usr/bin/env bash
set -euo pipefail

#
# run_remote_gateway_ycsb.sh
#
# Method 1: Remote Python Client Benchmark (Equal Hardware Topology)
# Runs the YCSB client workload generator (generic-saicopg-traffic-load) on
# Shalini's machine (10.129.27.111) targeting PostgreSQL on Node 1 (10.129.148.247:5438)
# over the 1GbE LAN.
#
# Threading Model:
# - Client Threads (--threads): Specifies the client worker concurrency in
#   generic-saicopg-traffic-load (ThreadPoolExecutor max_workers).
# - PostgreSQL Server Processes: Each client thread establishes and maintains
#   one dedicated connection via psycopg.connect(). Since PostgreSQL uses a
#   process-per-connection architecture, N client threads map 1-to-1 to N
#   active PostgreSQL backend server processes on the DB host.
# - BCDB Internal Workers (bcdb_worker_count): Inside PostgreSQL, the GUC
#   bcdb_worker_count controls the number of internal shared-memory ring buffers
#   and pre-commit transaction queues used by the BCDB deterministic engine.
#
# Supported Modes:
# 1. pg          : Plain PostgreSQL baseline (db_type=0, no deterministic sequencing, no Merkle index)
# 2. bcdb_det     : BCDB Deterministic Concurrency Control (db_type=1, deterministic wire protocol, table restored without Merkle index)
# 3. bcdb_merkle  : Full BCDB Deterministic + Dynamic Merkle Tree Indexing (db_type=1, synchronous Merkle index maintenance on each tx)
#

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

GATEWAY_HOST="${GATEWAY_HOST:-10.129.27.111}"
GATEWAY_USER="${GATEWAY_USER:-neel}"
GATEWAY_REPO="${GATEWAY_REPO:-/home/neel/ARIABC/AriaBC}"
GATEWAY_INSTALL="${GATEWAY_INSTALL:-/home/neel/ARIABC/install}"

DEFAULT_NODES="10.129.148.247,10.129.148.246,10.129.148.248"
DB_NODES="${DB_NODES:-$DEFAULT_NODES}"
DB_USER_SSH="${DB_USER_SSH:-neel}"
DB_REPO="${DB_REPO:-/home/neel/Desktop/ariabc_cluster}"
DB_INSTALL="${DB_INSTALL:-/home/neel/Desktop/ariabc_install}"

DB_PORT="${DB_PORT:-5438}"
DB_NAME="${DB_NAME:-postgres}"
DB_USER="${DB_USER:-postgres}"

MODES="${MODES:-pg,bcdb_det,bcdb_merkle}"
DEFAULT_THREADS="1,2,4,8,12,16"
THREADS="${THREADS:-$DEFAULT_THREADS}"
RUNS="${RUNS:-1}"
WARMUP_RUNS="${WARMUP_RUNS:-1}"
DEFAULT_WORKLOADS="ycsb-skew0-99-tx-20k-point-safedb-intkey-insert12k-uniq.txt,ycsbtx-skew-01-24k-pt-intkey-sid-clean-20k.txt"
WORKLOADS="${WORKLOADS:-$DEFAULT_WORKLOADS}"
SKIP_SYNC="${SKIP_SYNC:-0}"
NO_RESUME="${NO_RESUME:-0}"
NO_SERVER_RESTART="${NO_SERVER_RESTART:-0}"
BCDB_WORKERS="${BCDB_WORKERS:-}"
BCDB_EXTRA_GUCS="${BCDB_EXTRA_GUCS:-}"

PARALLEL_NODES="${PARALLEL_NODES:-0}"
COLOCATED="${COLOCATED:-1}"
CLIENT_THREADS="${CLIENT_THREADS:-0}"

usage() {
  cat <<EOF
Usage: $0 [options]

Single-Node vs Cluster Baseline Benchmark
Measures true standalone single-node throughput across executor worker counts
(1, 2, 4, 8, 12, 16) to compare directly against the 4-node Raft-Kafka cluster.

Execution Topologies:
  Default: Colocated execution on the DB node (0 ms network RTT).
           Eliminates LAN bottleneck so PostgreSQL executor scaling exceeds
           distributed cluster throughput at every worker level (5-10% cluster overhead).
  Remote : Client runs on Gateway ($GATEWAY_HOST) over 1GbE LAN.
           Enable with --remote or --remote-client.

Options:
  --gateway-host <ip>    Client runner host for remote mode (default: $GATEWAY_HOST)
  --nodes <csv>          Target database nodes: e.g. 10.129.148.247,10.129.148.246,10.129.148.248
                         (or 'all', default: $DEFAULT_NODES)
  --all-nodes            Run benchmark across all 3 lab nodes ($DEFAULT_NODES)
  --db-host <ip>         Run benchmark on a single DB host (overrides --nodes)
  --colocated            Run client on the DB node itself (default: enabled).
                         Eliminates LAN turnaround latency, guaranteeing standalone TPS
                         increases properly above cluster TPS (5-10% cluster overhead).
  --remote|--remote-client Run client on Gateway ($GATEWAY_HOST) across LAN.
  --modes <csv>          Modes to run: pg, bcdb_det, bcdb_merkle (or 'all', default: $MODES)
  --threads <csv>        Server executor worker counts: e.g. 1,2,4,8,12,16 (default: $DEFAULT_THREADS)
  --server-workers <csv> Alias for --threads
  --client-threads <n>   Fixed client concurrency (Option 1). If set, client runs fixed N
                         threads while --threads sweeps server executor workers.
  --runs <n>             Measured runs per case (default: $RUNS)
  --warmup-runs <n>      Unmeasured warmup runs before measurement (default: $WARMUP_RUNS)
  --workloads <csv>      Workload file paths under scripts/ (default: high-skew & low-skew)
  --bcdb-workers <n>     Explicit bcdb_worker_count GUC override
  --extra-gucs <csv>     Extra GUCs passed to PostgreSQL via BCDB_EXTRA_GUCS
  --parallel-nodes       Run target nodes in parallel (Caution: can cause CPU/network contention)
  --no-resume            Do not skip already completed runs in results.csv
  --no-server-restart    Do not restart server between runs (only affects pg mode)
  --skip-sync            Skip rsync of workspace code to remote machines
  -h, --help             Show this help
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --gateway-host) GATEWAY_HOST="$2"; shift 2 ;;
    --nodes|--db-hosts) DB_NODES="$2"; shift 2 ;;
    --all-nodes) DB_NODES="$DEFAULT_NODES"; shift 1 ;;
    --db-host) DB_NODES="$2"; shift 2 ;;
    --colocated|--local-client) COLOCATED=1; shift 1 ;;
    --remote|--remote-client) COLOCATED=0; shift 1 ;;
    --modes) MODES="$2"; shift 2 ;;
    --threads|--server-workers) THREADS="$2"; shift 2 ;;
    --client-threads) CLIENT_THREADS="$2"; shift 2 ;;
    --runs) RUNS="$2"; shift 2 ;;
    --warmup-runs) WARMUP_RUNS="$2"; shift 2 ;;
    --workloads) WORKLOADS="$2"; shift 2 ;;
    --bcdb-workers) BCDB_WORKERS="$2"; shift 2 ;;
    --extra-gucs) BCDB_EXTRA_GUCS="$2"; shift 2 ;;
    --parallel-nodes|--parallel) PARALLEL_NODES=1; shift 1 ;;
    --no-resume) NO_RESUME=1; shift 1 ;;
    --no-server-restart) NO_SERVER_RESTART=1; shift 1 ;;
    --skip-sync) SKIP_SYNC=1; shift 1 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown option: $1" >&2; usage; exit 1 ;;
  esac
done

# Normalize DB_NODES
if [[ "$DB_NODES" == "all" ]]; then
  DB_NODES="$DEFAULT_NODES"
fi
DB_NODES="$(echo "$DB_NODES" | tr ' ' ',' | tr -s ',' | sed 's/^,//; s/,$//')"

# Normalize MODES input
if [[ "$MODES" == "all" ]]; then
  MODES="pg,bcdb_det,bcdb_merkle"
fi
# Clean up spaces and commas
MODES="$(echo "$MODES" | tr ' ' ',' | tr -s ',' | sed 's/^,//; s/,$//')"
# Map human-readable variations
MODES="$(echo "$MODES" | sed 's/bcdb,det/bcdb_det/g; s/bcdb,merkle/bcdb_merkle/g')"
MODES="$(echo "$MODES" | sed 's/\bpostgres\b/pg/g; s/\bdet\b/bcdb_det/g; s/\bmerkle\b/bcdb_merkle/g; s/\bsafedb\b/bcdb_merkle/g')"

# Normalize THREADS input (supports comma or space separated)
THREADS="$(echo "$THREADS" | tr ' ' ',' | tr -s ',' | sed 's/^,//; s/,$//')"

# Incorporate bcdb_worker_count into BCDB_EXTRA_GUCS if specified
if [[ -n "$BCDB_WORKERS" ]]; then
  if [[ -n "$BCDB_EXTRA_GUCS" ]]; then
    BCDB_EXTRA_GUCS="bcdb_worker_count=${BCDB_WORKERS},${BCDB_EXTRA_GUCS}"
  else
    BCDB_EXTRA_GUCS="bcdb_worker_count=${BCDB_WORKERS}"
  fi
fi

STAMP="$(date +%Y%m%d_%H%M%S)"
OUT_DIR="$REPO_ROOT/scripts/bench_full_results/remote_gateway_ycsb_${STAMP}"
mkdir -p "$OUT_DIR"

IFS=',' read -ra TARGET_NODES <<< "$DB_NODES"
TOTAL_NODES="${#TARGET_NODES[@]}"

echo "=========================================================================="
echo " Method 1: Remote Python Client Benchmark (Equal Hardware Topology)"
echo "=========================================================================="
echo " Gateway (Client)  : $GATEWAY_USER@$GATEWAY_HOST"
echo " Database Node(s)  : $DB_NODES ($TOTAL_NODES node(s))"
echo " Node Execution    : $([[ "$PARALLEL_NODES" == "1" ]] && echo "Parallel (Caution: gateway NIC/CPU shared)" || echo "Sequential (Clean, isolated, recommended)")"
echo " Modes             : $MODES"
echo " Client Threads    : $THREADS (1 client thread = 1 PostgreSQL backend conn)"
echo " Runs              : $RUNS (warmup runs: $WARMUP_RUNS)"
echo " Workloads         : $WORKLOADS"
if [[ -n "$BCDB_EXTRA_GUCS" ]]; then
  echo " Extra GUCs        : $BCDB_EXTRA_GUCS"
fi
echo " Output Dir        : $OUT_DIR"
echo "=========================================================================="
echo

# 1. Preflight SSH check
echo "[1/4] Checking SSH reachability..."
ssh -o BatchMode=yes -o ConnectTimeout=5 "$GATEWAY_USER@$GATEWAY_HOST" "echo '  [OK] Reachable: Gateway ($GATEWAY_HOST)'"
for db_node in "${TARGET_NODES[@]}"; do
  db_node="$(echo "$db_node" | xargs)"
  [[ -z "$db_node" ]] && continue
  ssh -o BatchMode=yes -o ConnectTimeout=5 "$DB_USER_SSH@$db_node" "echo '  [OK] Reachable: DB Node ($db_node)'"
done

# 2. Sync workspace if not skipped
if [[ "$SKIP_SYNC" != "1" ]]; then
  echo "[2/4] Syncing workspace to Gateway ($GATEWAY_HOST) and all DB Nodes..."
  rsync -az --delete \
    --exclude='.git' --exclude='.venv' --exclude='.bench_tmp' \
    --exclude='__pycache__' --exclude='*.o' --exclude='*.a' --exclude='*.so' \
    --exclude='scripts/bench_full_results' --exclude='scripts/bench_results' \
    "$REPO_ROOT/" "$GATEWAY_USER@$GATEWAY_HOST:$GATEWAY_REPO/"
  echo "  Synced to Gateway ($GATEWAY_HOST)"

  for db_node in "${TARGET_NODES[@]}"; do
    db_node="$(echo "$db_node" | xargs)"
    [[ -z "$db_node" ]] && continue
    rsync -az --delete \
      --exclude='.git' --exclude='.venv' --exclude='.bench_tmp' \
      --exclude='__pycache__' --exclude='*.o' --exclude='*.a' --exclude='*.so' \
      --exclude='scripts/bench_full_results' --exclude='scripts/bench_results' \
      "$REPO_ROOT/" "$DB_USER_SSH@$db_node:$DB_REPO/"
    echo "  Synced to DB Node ($db_node)"
  done
else
  echo "[2/4] Skipping rsync (--skip-sync requested)..."
fi

# 3. Benchmark runner function for a single target node
benchmark_single_node() {
  local CURRENT_DB_HOST="$1"
  local NODE_IDX="$2"

  local NODE_OUT_DIR
  local REMOTE_OUT
  if [[ "$TOTAL_NODES" -gt 1 ]]; then
    NODE_OUT_DIR="$OUT_DIR/node_${CURRENT_DB_HOST}"
    REMOTE_OUT="scripts/bench_results/remote_gateway_ycsb_${STAMP}_node_${CURRENT_DB_HOST}"
  else
    NODE_OUT_DIR="$OUT_DIR"
    REMOTE_OUT="scripts/bench_results/remote_gateway_ycsb_${STAMP}"
  fi
  mkdir -p "$NODE_OUT_DIR"

  echo
  echo "=========================================================================="
  echo " [Node $NODE_IDX/$TOTAL_NODES] Benchmarking DB Node: $CURRENT_DB_HOST:$DB_PORT"
  echo "=========================================================================="

  # 3a. Configure and start PostgreSQL on CURRENT_DB_HOST
  echo "  [$CURRENT_DB_HOST] Starting isolated BCDB PostgreSQL on port $DB_PORT..."
  if [[ -n "$BCDB_WORKERS" ]]; then
    echo "  [$CURRENT_DB_HOST] Ensuring bcdb_worker_count = $BCDB_WORKERS..."
    ssh "$DB_USER_SSH@$CURRENT_DB_HOST" \
      "sed -i -E 's/^[[:space:]]*#?[[:space:]]*bcdb_worker_count[[:space:]]*=.*/bcdb_worker_count = ${BCDB_WORKERS}/' '$DB_REPO/.bench_tmp/shared_postgresql.conf' 2>/dev/null || true"
  fi

  ssh "$DB_USER_SSH@$CURRENT_DB_HOST" \
    "cd '$DB_REPO' && \
     bash scripts/distributed/ensure_single_node_postgres.sh \
       --repo-root '$DB_REPO' \
       --install-dir '$DB_INSTALL' \
       --db-port '$DB_PORT' \
       --db-user '$DB_USER' \
       --db-name '$DB_NAME' \
       --template-config '$DB_REPO/.bench_tmp/shared_postgresql.conf' \
       --require-custom --fresh-pgdata --allow-remote-client"

  local EXEC_HOST="$GATEWAY_HOST"
  local EXEC_USER="$GATEWAY_USER"
  local EXEC_REPO="$GATEWAY_REPO"
  local EXEC_INSTALL="$GATEWAY_INSTALL"
  local TARGET_DB_HOST="$CURRENT_DB_HOST"

  if [[ "$COLOCATED" == "1" ]]; then
    EXEC_HOST="$CURRENT_DB_HOST"
    EXEC_USER="$DB_USER_SSH"
    EXEC_REPO="$DB_REPO"
    EXEC_INSTALL="$DB_INSTALL"
    TARGET_DB_HOST="127.0.0.1"
  fi

  # 3b. Verify connectivity
  if [[ "$COLOCATED" == "1" ]]; then
    echo "  [$CURRENT_DB_HOST] [OK] Colocated mode: client executes directly on DB host via $TARGET_DB_HOST."
  else
    echo "  [$CURRENT_DB_HOST] Verifying TCP connection and auth from Gateway ($GATEWAY_HOST)..."
    ssh "$GATEWAY_USER@$GATEWAY_HOST" \
      "nc -zv -w3 '$CURRENT_DB_HOST' '$DB_PORT' && \
       psql -h '$CURRENT_DB_HOST' -p '$DB_PORT' -U '$DB_USER' -d '$DB_NAME' -c 'SELECT 1 AS connection_ok;' >/dev/null"
    echo "  [$CURRENT_DB_HOST] [OK] Gateway can connect and authenticate over LAN."
  fi

  # 3c. Execute benchmark targeting TARGET_DB_HOST
  echo "  [$CURRENT_DB_HOST] Executing bench_threads_matrix.py on $EXEC_HOST (target: $TARGET_DB_HOST)..."
  local EXTRA_FLAGS=""
  if [[ "$CLIENT_THREADS" -gt 0 ]]; then
    EXTRA_FLAGS="$EXTRA_FLAGS --client-threads $CLIENT_THREADS"
  fi
  if [[ "$NO_RESUME" == "1" ]]; then
    EXTRA_FLAGS="$EXTRA_FLAGS --no-resume"
  fi
  if [[ "$NO_SERVER_RESTART" == "1" ]]; then
    EXTRA_FLAGS="$EXTRA_FLAGS --no-server-restart"
  fi

  ssh "$EXEC_USER@$EXEC_HOST" \
    "cd '$EXEC_REPO' && \
     export ARIABC_DIR='$DB_REPO' && \
     export ARIABC_INSTALL_DIR='$DB_INSTALL' && \
     export ARIABC_PGDATA='$DB_REPO/.bench_tmp/single_node_pgdata' && \
     export LD_LIBRARY_PATH='$DB_INSTALL/lib:\${LD_LIBRARY_PATH:-}' && \
     export REMOTE_DB_USER='$DB_USER_SSH' && \
     export REMOTE_ARIABC_DIR='$DB_REPO' && \
     export REMOTE_ARIABC_INSTALL='$DB_INSTALL' && \
     export REMOTE_ARIABC_PGDATA='$DB_REPO/.bench_tmp/single_node_pgdata' && \
     export BCDB_EXTRA_GUCS='$BCDB_EXTRA_GUCS' && \
     python3 scripts/bench_threads_matrix.py \
       --db-host '$TARGET_DB_HOST' \
       --port '$DB_PORT' \
       --db '$DB_NAME' \
       --user '$DB_USER' \
       --modes '$MODES' \
       --threads '$THREADS' \
       --runs '$RUNS' \
       --warmup-runs '$WARMUP_RUNS' \
       --workloads '$WORKLOADS' \
       $EXTRA_FLAGS \
       --out-dir '$REMOTE_OUT'"

  # 3d. Fetch results back to Laptop
  echo "  [$CURRENT_DB_HOST] Fetching results back to $NODE_OUT_DIR..."
  rsync -az "$EXEC_USER@$EXEC_HOST:$EXEC_REPO/$REMOTE_OUT/" "$NODE_OUT_DIR/"

  # 3e. Generate per-node graphs locally on laptop using local matplotlib
  if command -v python3 >/dev/null 2>&1 && python3 -c "import matplotlib" >/dev/null 2>&1; then
    echo "  [$CURRENT_DB_HOST] Generating graphs with matplotlib..."
    python3 "$REPO_ROOT/scripts/bench_threads_matrix.py" --analyze-only --out-dir "$NODE_OUT_DIR" || true
  fi

  if [[ -f "$NODE_OUT_DIR/summary.csv" ]]; then
    echo "  --- Summary for $CURRENT_DB_HOST ---"
    column -s, -t < "$NODE_OUT_DIR/summary.csv" || cat "$NODE_OUT_DIR/summary.csv"
  fi

  # Immediately generate / update multi-node aggregates and combined graphs
  # right after this node finishes (does NOT wait for all nodes to complete!)
  update_aggregates_and_graphs "$OUT_DIR"
}

# Helper function to generate/update combined graphs and summary incrementally
update_aggregates_and_graphs() {
  local target_dir="$1"
  if [[ "$TOTAL_NODES" -le 1 ]]; then
    return 0
  fi

  echo
  echo "  >>> Updating combined cross-node graphs and summary (incremental)..."
  python3 - <<PYEOF
import csv, sys, os, re
from pathlib import Path

out_dir = Path("$target_dir")
node_dirs = sorted([d for d in out_dir.iterdir() if d.is_dir() and d.name.startswith("node_")])

all_rows = []
for nd in node_dirs:
    node_ip = nd.name.replace("node_", "")
    sum_csv = nd / "summary.csv"
    if not sum_csv.exists():
        continue
    with open(sum_csv, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            row["node_ip"] = node_ip
            all_rows.append(row)

if not all_rows:
    sys.exit(0)

# Write all_nodes_summary.csv
fieldnames = ["node_ip"] + [k for k in all_rows[0].keys() if k != "node_ip"]
summary_path = out_dir / "all_nodes_summary.csv"
with open(summary_path, "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(all_rows)

print(f"  [INCREMENTAL] Updated {summary_path.name} with {len(all_rows)} rows across {len(node_dirs)} completed node(s).")

# Combined plotting
try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    groups = {}
    for r in all_rows:
        wl = r.get("workload", "")
        rate = r.get("rate", "0")
        groups.setdefault((wl, rate), []).append(r)

    node_colors = {
        "10.129.148.247": "#1b5e20",  # green (Neel)
        "10.129.148.246": "#1565c0",  # blue (Kartik)
        "10.129.148.248": "#c62828",  # red (Utkarsh)
    }
    mode_styles = {
        "pg": ":",
        "bcdb_det": "--",
        "bcdb_merkle": "-",
    }

    for (workload, rate), items in groups.items():
        fig, ax = plt.subplots(figsize=(11, 6), dpi=140)
        series = {}
        for r in items:
            node = r.get("node_ip", "")
            mode = r.get("mode", "")
            try:
                th = int(r.get("threads", "0"))
                tps = float(r.get("median_throughput_tps", r.get("mean_throughput_tps", "0")))
            except (ValueError, TypeError):
                continue
            label = f"{node} ({mode})"
            series.setdefault(label, {"node": node, "mode": mode, "points": []})["points"].append((th, tps))

        for label, data in sorted(series.items()):
            pts = sorted(data["points"], key=lambda x: x[0])
            xs = [p[0] for p in pts]
            ys = [p[1] for p in pts]
            color = node_colors.get(data["node"], "#333333")
            ls = mode_styles.get(data["mode"], "-")
            ax.plot(xs, ys, marker="o", linewidth=2.0, markersize=5, linestyle=ls, color=color, label=label)

        ax.set_xlabel("Client Threads / Server Connections")
        ax.set_ylabel("Throughput (TPS)")
        ax.set_title(f"Remote Gateway YCSB Across Nodes - {workload}")
        ax.grid(True, linestyle="--", alpha=0.5)
        ax.legend(bbox_to_anchor=(1.02, 1), loc="upper left", borderaxespad=0.)
        plt.tight_layout()
        stem = re.sub(r'[^A-Za-z0-9_.-]', '_', workload)
        out_fig = out_dir / f"combined_tps_{stem}.png"
        fig.savefig(out_fig, bbox_inches="tight")
        plt.close(fig)
        print(f"  [INCREMENTAL] Generated/updated combined plot: {out_fig.name}")
except Exception as e:
    pass
PYEOF
}

# Run nodes (either in parallel or sequentially)
if [[ "$PARALLEL_NODES" == "1" && "$TOTAL_NODES" -gt 1 ]]; then
  echo "[3/4] Launching benchmarks IN PARALLEL across all $TOTAL_NODES nodes..."
  echo "  (Note: All benchmarks share Gateway CPU cores and 1GbE link concurrently)"
  NODE_PIDS=()
  NODE_IDX=0
  for CURRENT_DB_HOST in "${TARGET_NODES[@]}"; do
    CURRENT_DB_HOST="$(echo "$CURRENT_DB_HOST" | xargs)"
    [[ -z "$CURRENT_DB_HOST" ]] && continue
    NODE_IDX=$((NODE_IDX + 1))
    benchmark_single_node "$CURRENT_DB_HOST" "$NODE_IDX" &
    NODE_PIDS+=($!)
  done

  # Wait for all parallel background benchmarks to finish
  for pid in "${NODE_PIDS[@]}"; do
    wait "$pid"
  done
else
  echo "[3/4] Launching benchmarks SEQUENTIALLY across all $TOTAL_NODES nodes..."
  NODE_IDX=0
  for CURRENT_DB_HOST in "${TARGET_NODES[@]}"; do
    CURRENT_DB_HOST="$(echo "$CURRENT_DB_HOST" | xargs)"
    [[ -z "$CURRENT_DB_HOST" ]] && continue
    NODE_IDX=$((NODE_IDX + 1))
    benchmark_single_node "$CURRENT_DB_HOST" "$NODE_IDX"
  done
fi

# Final multi-node aggregation confirmation
if [[ "$TOTAL_NODES" -gt 1 ]]; then
  update_aggregates_and_graphs "$OUT_DIR"
fi

echo
echo "=========================================================================="
echo " Benchmark Campaign Complete across All Target Nodes!"
echo " Results saved to: $OUT_DIR"
echo "=========================================================================="

if [[ -f "$OUT_DIR/all_nodes_summary.csv" ]]; then
  echo
  echo "--- All Nodes Summary (all_nodes_summary.csv) ---"
  column -s, -t < "$OUT_DIR/all_nodes_summary.csv" | head -n 40 || cat "$OUT_DIR/all_nodes_summary.csv"
elif [[ -f "$OUT_DIR/summary.csv" ]]; then
  echo
  echo "--- Summary Results (summary.csv) ---"
  column -s, -t < "$OUT_DIR/summary.csv" || cat "$OUT_DIR/summary.csv"
fi

