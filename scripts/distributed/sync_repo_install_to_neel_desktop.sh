#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

NODES=""
SSH_KEY=""
SSH_PORT=22
REMOTE_REPO_ROOT="/home/neel/Desktop/ariabc_cluster"
REMOTE_INSTALL_DIR="/home/neel/Desktop/ariabc_install"

usage() {
  cat <<'EOF'
Usage:
  sync_repo_install_to_neel_desktop.sh \
    --nodes <user1@host1,user2@host2,...> \
    [--ssh-key <path>] [--ssh-port <22>] \
    [--remote-repo-root </home/neel/Desktop/ariabc_cluster>] \
    [--remote-install-dir </home/neel/Desktop/ariabc_install>]

Syncs the working AriaBC repo plus the custom PostgreSQL install tree to each
remote Desktop path for user-level benchmarking without /tmp staging.
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
if [[ -n "$SSH_KEY" ]]; then
  ssh_base+=(-i "$SSH_KEY")
  rsync_ssh+=" -i $SSH_KEY"
fi

for node in "${NODE_ARR[@]}"; do
  echo "[SYNC] $node"
  "${ssh_base[@]}" "$node" "mkdir -p '$REMOTE_REPO_ROOT' '$REMOTE_INSTALL_DIR'"

  rsync -az --delete \
    --exclude='.git' \
    --exclude='.venv' \
    --exclude='.bench_tmp' \
    --exclude='__pycache__' \
    --exclude='*.pyc' \
    --exclude='scripts/bench_full_results' \
    --exclude='scripts/bench_results' \
    -e "$rsync_ssh" \
    "$REPO_ROOT/" "$node:$REMOTE_REPO_ROOT/"

  rsync -az --delete \
    -e "$rsync_ssh" \
    /work/ARIABC/install/ "$node:$REMOTE_INSTALL_DIR/"

  echo "[OK] $node repo=$REMOTE_REPO_ROOT install=$REMOTE_INSTALL_DIR"
done

echo "Desktop sync complete."
