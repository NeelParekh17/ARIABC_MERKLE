#!/usr/bin/env bash
set -euo pipefail

# Fairness-gated 3-node smoke benchmark:
# 1) Kill non-benchmark postgres processes on each node.
# 2) Force CPU governor=performance for benchmark window.
# 3) Enforce identical runtime knobs via shared config template.
# 4) Run threads=5,runs=3 and pass only if nodes are within a narrow band.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
RUNNER="$SCRIPT_DIR/run_single_machine_matrix_all_nodes.sh"

NODES_CSV="neel@10.129.148.248,neel@10.129.148.248,neel@10.129.27.54"
SSH_KEY="/home/neel/.ssh/id_rsa"
SSH_PORT=22
REMOTE_REPO_ROOT="/home/neel/Desktop/ariabc_cluster"
REMOTE_INSTALL_DIR="/home/neel/Desktop/ariabc_install"
DB_PORT=5438
DB_NAME="postgres"
DB_USER="postgres"

# Required test shape from user.
THREADS="5"
RUNS="3"
WORKLOADS="ycsbtx-skew-01-24k-pt-intkey-sid-clean-20k.txt,ycsb-skew0-99-tx-20k-point-safedb-intkey-insert12k-uniq.txt"

# Band gate: each node must be >= 80% of best node for each workload.
MIN_REL_PCT="80"

usage() {
  cat <<'EOF'
Usage:
  scripts/distributed/run_3node_fairness_smoke_gate.sh [options]

Options:
  --nodes <csv>
  --ssh-key <path>
  --ssh-port <port>
  --remote-repo-root <path>
  --remote-install-dir <path>
  --db-port <port>
  --min-rel-pct <n>       Minimum percent-of-best TPS to pass fairness gate
  -h|--help

Notes:
- Requires passwordless sudo on all nodes to force governor=performance.
- Canonical config is always sourced from /work/ARIABC/pgdata/postgresql.conf by the runner.
- Exits non-zero if any precondition or fairness gate fails.
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
    --max-connections|--shared-buffers|--bcdb-worker-count)
      echo "ERROR: $1 is no longer supported. Config override knobs are removed; canonical config is enforced." >&2
      exit 2
      ;;
    --min-rel-pct) MIN_REL_PCT="${2:-80}"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown arg: $1" >&2; usage; exit 2 ;;
  esac
done

if [[ ! -x "$RUNNER" ]]; then
  echo "ERROR: runner not executable: $RUNNER" >&2
  exit 1
fi
if [[ ! -f "$SSH_KEY" ]]; then
  echo "ERROR: SSH key not found: $SSH_KEY" >&2
  exit 2
fi

declare -a NODES=()
split_csv "$NODES_CSV" NODES
if [[ "${#NODES[@]}" -ne 3 ]]; then
  echo "ERROR: expected 3 nodes, got ${#NODES[@]}" >&2
  exit 2
fi

ts="$(date +%Y%m%d_%H%M%S)"
out_root="$REPO_ROOT/scripts/bench_full_results/fairness_smoke_${ts}"
mkdir -p "$out_root"

echo "== Fairness Smoke Gate =="
echo "nodes             : ${NODES[*]}"
echo "threads/runs      : $THREADS / $RUNS"
echo "db port           : $DB_PORT"
echo "config source     : /work/ARIABC/pgdata/postgresql.conf (hard-locked)"
echo "fairness threshold: ${MIN_REL_PCT}% of best"
echo "out               : $out_root"
echo

ref_conf_src="/work/ARIABC/pgdata/postgresql.conf"
if [[ ! -f "$ref_conf_src" ]]; then
  echo "ERROR: missing reference config: $ref_conf_src" >&2
  exit 2
fi

ssh_base=(ssh -o BatchMode=yes -o StrictHostKeyChecking=no -p "$SSH_PORT" -i "$SSH_KEY")

# Preflight 1+2: kill non-benchmark postgres and force governor=performance.
preflight_ok=1
for node in "${NODES[@]}"; do
  echo "[preflight] $node"
  cmd=$(cat <<'EOS'
set -e
bench_pg="$REMOTE_REPO_ROOT/.bench_tmp/single_node_pgdata"

# Require sudo for both process cleanup and governor control.
if ! sudo -n true >/dev/null 2>&1; then
  echo SUDO_NONINTERACTIVE_UNAVAILABLE
  exit 11
fi

# Kill postgres instances not using benchmark PGDATA.
for pid in $(sudo pgrep -f 'postgres -D' || true); do
  c=$(sudo tr '\0' ' ' </proc/$pid/cmdline || true)
  if [[ "$c" == *'postgres -D'* && "$c" != *"$bench_pg"* ]]; then
    sudo kill -9 $pid || true
  fi
done
sleep 1
left=0
for pid in $(sudo pgrep -f 'postgres -D' || true); do
  c=$(sudo tr '\0' ' ' </proc/$pid/cmdline || true)
  if [[ "$c" == *'postgres -D'* && "$c" != *"$bench_pg"* ]]; then
    echo LEFTOVER_NON_BENCH=$pid "$c"
    left=1
  fi
done
if [[ $left -ne 0 ]]; then
  exit 10
fi

# Force governor to performance for benchmark window.
for g in /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor; do
  [[ -f $g ]] && echo performance | sudo tee $g >/dev/null
done
cur=$(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor 2>/dev/null || echo unknown)
echo GOVERNOR=$cur
if [[ "$cur" != 'performance' ]]; then
  exit 12
fi
EOS
)

  if ! "${ssh_base[@]}" "$node" "REMOTE_REPO_ROOT='$REMOTE_REPO_ROOT' bash -lc $(printf '%q' "$cmd")" >"$out_root/preflight_$(echo "$node" | tr '@.' '__').log" 2>&1; then
    echo "  FAIL (see $out_root/preflight_$(echo "$node" | tr '@.' '__').log)"
    preflight_ok=0
  else
    echo "  OK"
  fi
done

if [[ "$preflight_ok" -ne 1 ]]; then
  echo "ERROR: preflight failed; refusing to run benchmark." >&2
  exit 1
fi

# Run smoke per-node (same shape).
for node in "${NODES[@]}"; do
  echo "[run] $node"
  "$RUNNER" \
    --nodes "$node" \
    --ssh-key "$SSH_KEY" \
    --ssh-port "$SSH_PORT" \
    --remote-repo-root "$REMOTE_REPO_ROOT" \
    --remote-install-dir "$REMOTE_INSTALL_DIR" \
    --modes det \
    --threads "$THREADS" \
    --runs "$RUNS" \
    --workloads "$WORKLOADS" \
    --db-name "$DB_NAME" \
    --db-user "$DB_USER" \
    --db-port "$DB_PORT" \
    >"$out_root/run_$(echo "$node" | tr '@.' '__').log" 2>&1
  echo "  done"
done

# Collect summary paths.
mapfile -t summaries < <(find "$REPO_ROOT/scripts/bench_full_results" -maxdepth 2 -type f -name summary.csv -path '*single_machine_nodes_*' | sort | tail -n 20)

python3 - <<PY
import csv
from pathlib import Path

out = Path(r"$out_root")
min_rel = float("$MIN_REL_PCT")

# Identify run dirs from logs to avoid accidental older summaries.
run_logs = sorted(out.glob('run_*.log'))
node_to_summary = {}
for lg in run_logs:
    node = lg.stem.replace('run_','').replace('_at_','@').replace('_','.')
    root = None
    for line in lg.read_text().splitlines():
        if line.startswith('Collected outputs: '):
            root = line.split(': ',1)[1].strip()
    if not root:
        raise SystemExit(f'No collected root in {lg}')
    p = Path(root)
    s = list(p.rglob('summary.csv'))
    if len(s) != 1:
      raise SystemExit(f'Expected 1 summary in {p}, found {len(s)}')
    node_to_summary[node] = s[0]

rows = []
for node, s in node_to_summary.items():
    with s.open() as f:
        for r in csv.DictReader(f):
            rows.append({
                'node': node,
                'workload': r['workload'],
                'mean_tps': float(r['mean_throughput_tps']),
                'median_tps': float(r['median_throughput_tps']),
                'pass_rate': float(r['pass_rate_merkle_verify']),
            })

workloads = sorted({r['workload'] for r in rows})
lines = []
lines.append('node,workload,mean_tps,median_tps,pass_rate,rel_to_best_pct')
fail = False
for w in workloads:
    ws = [r for r in rows if r['workload']==w]
    best = max(r['mean_tps'] for r in ws)
    for r in sorted(ws, key=lambda x:x['node']):
        rel = (100.0*r['mean_tps']/best) if best else 0.0
        lines.append(f"{r['node']},{w},{r['mean_tps']:.3f},{r['median_tps']:.3f},{r['pass_rate']:.3f},{rel:.2f}")
        if rel < min_rel:
            fail = True

(out/'fairness_summary.csv').write_text('\n'.join(lines)+'\n')

md = []
md.append('# Fairness Smoke Summary')
md.append('')
md.append(f'- threshold: {min_rel:.1f}% of best per workload')
md.append('')
for w in workloads:
    ws=[r for r in rows if r['workload']==w]
    best=max(r['mean_tps'] for r in ws)
    md.append(f'## {w}')
    for r in sorted(ws,key=lambda x:x['mean_tps'], reverse=True):
        rel=(100.0*r['mean_tps']/best) if best else 0.0
        md.append(f"- {r['node']}: mean_tps={r['mean_tps']:.3f}, rel={rel:.2f}%, pass_rate={r['pass_rate']:.3f}")
    md.append('')

(out/'fairness_summary.md').write_text('\n'.join(md))
print((out/'fairness_summary.md').as_posix())
print('FAIRNESS_PASS' if not fail else 'FAIRNESS_FAIL')
if fail:
    raise SystemExit(2)
PY

echo "All checks passed. See $out_root"
