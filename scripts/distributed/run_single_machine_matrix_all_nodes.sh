#!/usr/bin/env bash
set -euo pipefail

# Run single-machine bench_threads_matrix.py on multiple machines and
# pull all outputs back to local scripts/bench_full_results.
#
# Typical use:
# scripts/distributed/run_single_machine_matrix_all_nodes.sh \
#   --nodes local,neel@10.129.148.248,neel@10.129.27.54,neel@10.129.148.236,neel@10.129.148.179 \
#   --ssh-key /home/neel/.ssh/id_rsa \
#   --remote-repo-root /home/neel/Desktop/ariabc_cluster \
#   --remote-install-dir /home/neel/Desktop/ariabc_install \
#   --modes det \
#   --threads 5 \
#   --runs 3

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
source "$SCRIPT_DIR/benchmark_defaults.sh"

NODES=""
SSH_KEY=""
SSH_PORT=22
REMOTE_REPO_ROOT="/home/neel/Desktop/ariabc_cluster"
REMOTE_INSTALL_DIR="/home/neel/Desktop/ariabc_install"
LOCAL_INSTALL_DIR="/work/ARIABC/install"
THREADS="$ARIABC_DEFAULT_FULL_THREADS"
MODES="det"
RUNS=3
WORKLOADS=""
RATES=""
DB_NAME="postgres"
DB_USER="postgres"
DB_PORT=5438
TEMPLATE_CONF_LOCAL="/work/ARIABC/pgdata/postgresql.conf"
ENSURE_SCRIPT_LOCAL="$REPO_ROOT/scripts/distributed/ensure_single_node_postgres.sh"
ENSURE_CUSTOM_INSTALL_LOCAL="$REPO_ROOT/scripts/distributed/ensure_custom_install_from_repo.sh"
BENCH_MATRIX_LOCAL="$REPO_ROOT/scripts/bench_threads_matrix.py"
START_SERVER_LOCAL="$REPO_ROOT/scripts/start_server.sh"
RESTORE_SQL_LOCAL="$REPO_ROOT/scripts/restore_usertable_small.sql"
LOCAL_HOST_FQDN="$(hostname -f 2>/dev/null || hostname)"
LOCAL_HOST_SHORT="$(hostname -s 2>/dev/null || hostname)"
LOCAL_USER="$(id -un 2>/dev/null || printf '%s' "${USER:-neel}")"

usage() {
  cat <<'EOF'
Usage:
  run_single_machine_matrix_all_nodes.sh \
    --nodes <local,user1@host1,user2@host2,...> \
    [--ssh-key <path>] [--ssh-port <22>] \
    [--remote-repo-root </home/neel/Desktop/ariabc_cluster>] \
    [--remote-install-dir </home/neel/Desktop/ariabc_install>] \
    [--modes <det>] [--threads <csv>] [--runs <3>] \
    [--workloads <csv>] [--rates <csv>] \
    [--db-name <postgres>] [--db-user <postgres>] [--db-port <5438>]

Behavior:
1) Remote nodes run from Desktop paths under the target user's account.
2) A `local` node runs from the current worktree and local install tree.
3) Custom BCDB postgres is required; vanilla postgres fallback is not allowed.
4) Results are collected under:
   scripts/bench_full_results/single_machine_nodes_<timestamp>/<node>/
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

safe_label() {
  local s="$1"
  s="${s//@/_at_}"
  s="${s//./_}"
  s="${s//-/_}"
  printf '%s' "$s"
}

is_local_node() {
  local node="$1"
  case "$node" in
    local|localhost|127.0.0.1|"$LOCAL_HOST_FQDN"|"$LOCAL_HOST_SHORT"|"$LOCAL_USER@$LOCAL_HOST_FQDN"|"$LOCAL_USER@$LOCAL_HOST_SHORT"|"$LOCAL_USER@localhost"|"$LOCAL_USER@127.0.0.1")
      return 0
      ;;
  esac
  return 1
}

ensure_graphs() {
  local node="$1"
  local local_node_dir="$2"
  local graph_count
  graph_count="$(find "$local_node_dir" -maxdepth 1 -type f -name 'tps_vs_threads_*.png' | wc -l | awk '{print $1}')"
  if [[ "$graph_count" == "0" ]]; then
    echo "[GRAPH] Regenerating locally for $node"
    python3 - <<PY
import importlib.util
from pathlib import Path

module_path = Path(r"$REPO_ROOT/scripts/bench_threads_matrix.py")
summary_csv = Path(r"$local_node_dir/summary.csv")
out_dir = Path(r"$local_node_dir")

spec = importlib.util.spec_from_file_location("bench_threads_matrix_mod", str(module_path))
mod = importlib.util.module_from_spec(spec)
assert spec.loader is not None
spec.loader.exec_module(mod)
mod._generate_tps_graphs(summary_csv, out_dir)
print("graphs_regenerated", out_dir)
PY
  fi
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --nodes) NODES="${2:-}"; shift 2 ;;
    --ssh-key) SSH_KEY="${2:-}"; shift 2 ;;
    --ssh-port) SSH_PORT="${2:-22}"; shift 2 ;;
    --remote-repo-root) REMOTE_REPO_ROOT="${2:-}"; shift 2 ;;
    --remote-install-dir) REMOTE_INSTALL_DIR="${2:-}"; shift 2 ;;
    --threads) THREADS="${2:-}"; shift 2 ;;
    --modes) MODES="${2:-det}"; shift 2 ;;
    --runs) RUNS="${2:-3}"; shift 2 ;;
    --workloads) WORKLOADS="${2:-}"; shift 2 ;;
    --rates) RATES="${2:-}"; shift 2 ;;
    --db-name) DB_NAME="${2:-postgres}"; shift 2 ;;
    --db-user) DB_USER="${2:-postgres}"; shift 2 ;;
    --db-port) DB_PORT="${2:-5438}"; shift 2 ;;
    --template-conf-local|--disable-bench-overrides)
      echo "ERROR: $1 is no longer supported. Canonical config is always enforced from /work/ARIABC/pgdata/postgresql.conf" >&2
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

if [[ -z "$NODES" ]]; then
  echo "ERROR: --nodes is required." >&2
  usage
  exit 2
fi

declare -a NODE_ARR=()
split_csv "$NODES" NODE_ARR
if [[ "${#NODE_ARR[@]}" -eq 0 ]]; then
  echo "ERROR: no nodes parsed from --nodes." >&2
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

if [[ ! -f "$TEMPLATE_CONF_LOCAL" ]]; then
  echo "ERROR: canonical config file not found: $TEMPLATE_CONF_LOCAL" >&2
  exit 2
fi

for required in "$ENSURE_SCRIPT_LOCAL" "$ENSURE_CUSTOM_INSTALL_LOCAL" "$START_SERVER_LOCAL"; do
  if [[ ! -x "$required" ]]; then
    echo "ERROR: required script not executable: $required" >&2
    exit 2
  fi
done

if [[ ! -f "$BENCH_MATRIX_LOCAL" || ! -f "$RESTORE_SQL_LOCAL" ]]; then
  echo "ERROR: required benchmark files missing under $REPO_ROOT/scripts" >&2
  exit 2
fi

ts="$(date +%Y%m%d_%H%M%S)"
local_root="$REPO_ROOT/scripts/bench_full_results/single_machine_nodes_${ts}"
mkdir -p "$local_root"

echo "== Single-machine multi-node benchmark =="
echo "Nodes           : ${NODE_ARR[*]}"
echo "Remote repo root: $REMOTE_REPO_ROOT"
echo "Remote install  : $REMOTE_INSTALL_DIR"
echo "Local repo root : $REPO_ROOT"
echo "Local install   : $LOCAL_INSTALL_DIR"
echo "Modes           : $MODES"
echo "Threads         : $THREADS"
echo "Runs            : $RUNS"
echo "Local root      : $local_root"
echo

for node in "${NODE_ARR[@]}"; do
  node_start_epoch="$(date +%s)"
  node_start_human="$(date '+%F %T %z')"
  echo "[START] node=$node at $node_start_human"

  is_local=0
  effective_repo_root="$REMOTE_REPO_ROOT"
  effective_install_dir="$REMOTE_INSTALL_DIR"
  safe_node="$(safe_label "$node")"
  if is_local_node "$node"; then
    is_local=1
    effective_repo_root="$REPO_ROOT"
    effective_install_dir="$LOCAL_INSTALL_DIR"
    safe_node="$(safe_label "local_${LOCAL_HOST_SHORT}")"
  fi

  local_node_dir="$local_root/$safe_node"
  mkdir -p "$local_node_dir"

  if [[ "$is_local" == "1" ]]; then
    template_conf_path="$effective_repo_root/.bench_tmp/shared_postgresql.conf"
    mkdir -p "$effective_repo_root/.bench_tmp"
    cp "$TEMPLATE_CONF_LOCAL" "$template_conf_path"

    echo "[RUN] $node (local worktree)"
    local_cmd="set -euo pipefail
cd '$effective_repo_root/scripts'
bash '$ENSURE_CUSTOM_INSTALL_LOCAL' \
  --repo-root '$effective_repo_root' \
  --install-dir '$effective_install_dir'
export ARIABC_REQUIRE_CUSTOM_PG=1
export ARIABC_PSQL='$effective_install_dir/bin/psql'
export ARIABC_INSTALL_DIR='$effective_install_dir'
export ARIABC_DIR='$effective_repo_root'
export ARIABC_PGPORT='$DB_PORT'
export LD_LIBRARY_PATH='$effective_install_dir/lib:'\"\${LD_LIBRARY_PATH:-}\"
pgdata_line=\$(bash '$ENSURE_SCRIPT_LOCAL' \
  --repo-root '$effective_repo_root' \
  --install-dir '$effective_install_dir' \
  --db-port '$DB_PORT' \
  --db-user '$DB_USER' \
  --db-name '$DB_NAME' \
  --template-config '$template_conf_path' \
  --require-custom | tail -n 1)
if [[ \$pgdata_line == PGDATA=* ]]; then
  export ARIABC_PGDATA=\${pgdata_line#PGDATA=}
fi
PYTHON_BIN=''
if [[ -x '$effective_repo_root/.venv/bin/python' ]] && '$effective_repo_root/.venv/bin/python' -c 'import psycopg' >/dev/null 2>&1; then
  PYTHON_BIN='$effective_repo_root/.venv/bin/python'
elif python3 -c 'import psycopg' >/dev/null 2>&1; then
  PYTHON_BIN=python3
else
  echo 'ERROR: psycopg is not available locally. Provision it in the local .venv or system python.' >&2
  exit 2
fi
\$PYTHON_BIN -u bench_threads_matrix.py \
  --modes '$MODES' \
  --threads '$THREADS' \
  --runs '$RUNS' \
  --db '$DB_NAME' \
  --user '$DB_USER' \
  --port '$DB_PORT' \
  --out-dir '$local_node_dir'"

    if [[ -n "$WORKLOADS" ]]; then
      local_cmd+=" \\
  --workloads '$WORKLOADS'"
    fi
    if [[ -n "$RATES" ]]; then
      local_cmd+=" \\
  --rates '$RATES'"
    fi

    bash -lc "$local_cmd"
  else
    remote_template_conf="$effective_repo_root/.bench_tmp/shared_postgresql.conf"
    remote_out_dir="$effective_repo_root/scripts/bench_results/nodecheck_${safe_node}_${ts}"

    "${ssh_base[@]}" "$node" "mkdir -p '$effective_repo_root/.bench_tmp' '$effective_repo_root/scripts/distributed'"
    if [[ -f /usr/include/openssl/sha.h ]]; then
      "${ssh_base[@]}" "$node" "mkdir -p '$effective_repo_root/.bench_tmp/deps/include/openssl'"
      rsync -a -e "$rsync_ssh" /usr/include/openssl/ "$node:$effective_repo_root/.bench_tmp/deps/include/openssl/"
      if [[ -d /usr/include/x86_64-linux-gnu/openssl ]]; then
        rsync -a -e "$rsync_ssh" /usr/include/x86_64-linux-gnu/openssl/ "$node:$effective_repo_root/.bench_tmp/deps/include/openssl/"
      fi
    fi
    "${ssh_base[@]}" "$node" "mkdir -p '$effective_repo_root/.bench_tmp/deps/lib'; for cand in /usr/lib/x86_64-linux-gnu/libcrypto.so /lib/x86_64-linux-gnu/libcrypto.so /usr/lib/x86_64-linux-gnu/libcrypto.so.3 /lib/x86_64-linux-gnu/libcrypto.so.3; do if [[ -e \$cand ]]; then ln -sf \"\$cand\" '$effective_repo_root/.bench_tmp/deps/lib/libcrypto.so'; break; fi; done"
    "${scp_base[@]}" "$TEMPLATE_CONF_LOCAL" "$node:$remote_template_conf"
    "${scp_base[@]}" "$ENSURE_SCRIPT_LOCAL" "$node:$effective_repo_root/scripts/distributed/ensure_single_node_postgres.sh"
    "${scp_base[@]}" "$ENSURE_CUSTOM_INSTALL_LOCAL" "$node:$effective_repo_root/scripts/distributed/ensure_custom_install_from_repo.sh"
    "${scp_base[@]}" "$BENCH_MATRIX_LOCAL" "$node:$effective_repo_root/scripts/bench_threads_matrix.py"
    "${scp_base[@]}" "$START_SERVER_LOCAL" "$node:$effective_repo_root/scripts/start_server.sh"
    "${scp_base[@]}" "$RESTORE_SQL_LOCAL" "$node:$effective_repo_root/scripts/restore_usertable_small.sql"
    "${ssh_base[@]}" "$node" "chmod +x '$effective_repo_root/scripts/distributed/ensure_single_node_postgres.sh' '$effective_repo_root/scripts/distributed/ensure_custom_install_from_repo.sh' '$effective_repo_root/scripts/start_server.sh'"

    echo "[RUN] $node (live progress will stream below)"
    remote_cmd="set -euo pipefail
cd '$effective_repo_root/scripts'
if [[ ! -f bench_threads_matrix.py ]]; then
  echo 'ERROR: bench_threads_matrix.py missing at remote scripts dir' >&2
  exit 2
fi
bash '$effective_repo_root/scripts/distributed/ensure_custom_install_from_repo.sh' \
  --repo-root '$effective_repo_root' \
  --install-dir '$effective_install_dir' \
  --clean-when-rebuild
export ARIABC_REQUIRE_CUSTOM_PG=1
export ARIABC_PSQL='$effective_install_dir/bin/psql'
export ARIABC_INSTALL_DIR='$effective_install_dir'
export ARIABC_DIR='$effective_repo_root'
export ARIABC_PGPORT='$DB_PORT'
export LD_LIBRARY_PATH='$effective_install_dir/lib:'\"\${LD_LIBRARY_PATH:-}\"
pgdata_line=\$(bash '$effective_repo_root/scripts/distributed/ensure_single_node_postgres.sh' \
  --repo-root '$effective_repo_root' \
  --install-dir '$effective_install_dir' \
  --db-port '$DB_PORT' \
  --db-user '$DB_USER' \
  --db-name '$DB_NAME' \
  --template-config '$remote_template_conf' \
  --require-custom | tail -n 1)
if [[ \$pgdata_line == PGDATA=* ]]; then
  export ARIABC_PGDATA=\${pgdata_line#PGDATA=}
fi
PYTHON_BIN=''
if [[ -x '$effective_repo_root/.venv/bin/python' ]] && '$effective_repo_root/.venv/bin/python' -c 'import psycopg' >/dev/null 2>&1; then
  PYTHON_BIN='$effective_repo_root/.venv/bin/python'
elif python3 -c 'import psycopg' >/dev/null 2>&1; then
  PYTHON_BIN=python3
else
  PYTHON_BIN='$effective_repo_root/.venv/bin/python'
  if [[ ! -x \"\$PYTHON_BIN\" ]]; then
    python3 -m venv '$effective_repo_root/.venv'
  fi
  if ! \"\$PYTHON_BIN\" -c 'import psycopg' >/dev/null 2>&1; then
    dest_site=\"\$("\$PYTHON_BIN" -c 'import sysconfig; print(sysconfig.get_path(\"purelib\"))')\"
    for old_root in '$effective_repo_root/.venv' '/home/neel/Desktop/ariabc_cluster/.venv'; do
      cand_site=\"\$(find \"\$old_root/lib\" -maxdepth 2 -type d -name site-packages 2>/dev/null | head -n 1 || true)\"
      if [[ -z \"\$cand_site\" ]]; then
        continue
      fi
      if [[ -d \"\$cand_site/psycopg\" ]]; then
        cp -a \"\$cand_site/psycopg\" \"\$dest_site/\" 2>/dev/null || true
      fi
      for extra in \"\$cand_site\"/psycopg_binary* \"\$cand_site\"/typing_extensions.py \"\$cand_site\"/typing_extensions-*.dist-info \"\$cand_site\"/psycopg-*.dist-info \"\$cand_site\"/psycopg_binary-*.dist-info; do
        if [[ -e \"\$extra\" ]]; then
          cp -a \"\$extra\" \"\$dest_site/\" 2>/dev/null || true
        fi
      done
      if \"\$PYTHON_BIN\" -c 'import psycopg' >/dev/null 2>&1; then
        break
      fi
    done
  fi
  if ! \"\$PYTHON_BIN\" -c 'import psycopg' >/dev/null 2>&1; then
    '$effective_repo_root/.venv/bin/pip' install -q --disable-pip-version-check 'psycopg[binary]' || \
      '$effective_repo_root/.venv/bin/pip' install -q --disable-pip-version-check psycopg
  fi
  if ! \"\$PYTHON_BIN\" -c 'import psycopg' >/dev/null 2>&1; then
    echo 'ERROR: psycopg is not available on remote node; cannot run single-machine benchmark workload.' >&2
    exit 2
  fi
fi
\$PYTHON_BIN -u bench_threads_matrix.py \
  --modes '$MODES' \
  --threads '$THREADS' \
  --runs '$RUNS' \
  --db '$DB_NAME' \
  --user '$DB_USER' \
  --port '$DB_PORT' \
  --out-dir '$remote_out_dir'"

    if [[ -n "$WORKLOADS" ]]; then
      remote_cmd+=" \\
  --workloads '$WORKLOADS'"
    fi
    if [[ -n "$RATES" ]]; then
      remote_cmd+=" \\
  --rates '$RATES'"
    fi

    "${ssh_base[@]}" "$node" "bash -lc $(printf '%q' "$remote_cmd")"

    echo "[PULL] $node"
    rsync -a -e "$rsync_ssh" "$node:$remote_out_dir/" "$local_node_dir/"
  fi

  if [[ ! -f "$local_node_dir/results.csv" ]]; then
    echo "ERROR: missing results.csv for node $node at $local_node_dir" >&2
    exit 1
  fi
  if [[ ! -f "$local_node_dir/summary.csv" ]]; then
    echo "ERROR: missing summary.csv for node $node at $local_node_dir" >&2
    exit 1
  fi

  ensure_graphs "$node" "$local_node_dir"

  final_graph_count="$(find "$local_node_dir" -maxdepth 1 -type f -name 'tps_vs_threads_*.png' | wc -l | awk '{print $1}')"
  echo "[OK] $node results.csv=1 summary.csv=1 graphs=${final_graph_count} local=$local_node_dir"
  node_end_epoch="$(date +%s)"
  node_end_human="$(date '+%F %T %z')"
  node_elapsed="$((node_end_epoch - node_start_epoch))"
  echo "[END] node=$node at $node_end_human rc=0 elapsed_s=$node_elapsed"
  echo
done

echo "All node runs completed."
echo "Collected outputs: $local_root"
