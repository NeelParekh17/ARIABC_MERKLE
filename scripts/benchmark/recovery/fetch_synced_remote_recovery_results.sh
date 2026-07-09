#!/usr/bin/env bash
set -Eeuo pipefail

usage() {
  cat <<'USAGE'
Usage: fetch_synced_remote_recovery_results.sh --host HOST --run-id RUN_ID [options]

Options:
  --ssh-user USER
  --ssh-port PORT
  --ssh-key PATH
  --remote-root PATH        default: /work/ARIABC/merkle_recovery_runs
  --keep-remote-archive
USAGE
}

HOST=""
SSH_USER=""
SSH_PORT="22"
SSH_KEY=""
SSH_PASSWORD="${SSH_PASSWORD:-}"
REMOTE_ROOT="/work/ARIABC/merkle_recovery_runs"
RUN_ID=""
KEEP_REMOTE_ARCHIVE=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --host) HOST="${2:?}"; shift 2 ;;
    --ssh-user) SSH_USER="${2:?}"; shift 2 ;;
    --ssh-port) SSH_PORT="${2:?}"; shift 2 ;;
    --ssh-key) SSH_KEY="${2:?}"; shift 2 ;;
    --remote-root) REMOTE_ROOT="${2:?}"; shift 2 ;;
    --run-id) RUN_ID="${2:?}"; shift 2 ;;
    --keep-remote-archive) KEEP_REMOTE_ARCHIVE=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done

[[ -n "$HOST" && -n "$RUN_ID" ]] || { usage >&2; exit 2; }

resolve_host_ip() {
  case "$1" in
    admin123) printf '%s\n' "10.129.148.236" ;;
    user4) printf '%s\n' "10.129.148.246" ;;
    utkarsh) printf '%s\n' "10.129.148.248" ;;
    *) printf '%s\n' "$1" ;;
  esac
}

if [[ -z "$SSH_USER" ]]; then
  SSH_TARGET="$(resolve_host_ip "$HOST")"
else
  SSH_TARGET="$SSH_USER@$(resolve_host_ip "$HOST")"
fi

if [[ -n "$SSH_PASSWORD" ]]; then
  export SSHPASS="$SSH_PASSWORD"
  SSH_CMD=(sshpass -e ssh)
  SCP_CMD=(sshpass -e scp)
else
  SSH_CMD=(ssh)
  SCP_CMD=(scp)
fi
if [[ -n "$SSH_PASSWORD" ]]; then
  SSH_OPTS=(-n -p "$SSH_PORT" -o StrictHostKeyChecking=accept-new)
  SCP_OPTS=(-P "$SSH_PORT" -o StrictHostKeyChecking=accept-new)
else
  SSH_OPTS=(-n -p "$SSH_PORT" -o BatchMode=yes -o StrictHostKeyChecking=accept-new)
  SCP_OPTS=(-P "$SSH_PORT" -o BatchMode=yes -o StrictHostKeyChecking=accept-new)
fi
if [[ -n "$SSH_KEY" ]]; then
  SSH_OPTS+=(-i "$SSH_KEY")
  SCP_OPTS+=(-i "$SSH_KEY")
fi

remote_ssh_cmd() {
  "${SSH_CMD[@]}" "${SSH_OPTS[@]}" "$SSH_TARGET" "$@" </dev/null
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FETCH_ROOT="$SCRIPT_DIR/fetched"
mkdir -p "$FETCH_ROOT"

REMOTE_ARCHIVE="$REMOTE_ROOT/artifacts/$RUN_ID.tar.gz"
LOCAL_ARCHIVE="$FETCH_ROOT/$RUN_ID.tar.gz"
DEST="$FETCH_ROOT/$RUN_ID"

"${SCP_CMD[@]}" "${SCP_OPTS[@]}" "$SSH_TARGET:$REMOTE_ARCHIVE" "$LOCAL_ARCHIVE"

python3 - "$LOCAL_ARCHIVE" <<'PY'
import re
import sys
import tarfile

path = sys.argv[1]
forbidden = re.compile(r"(^|/)(pgdata|scratch|src|install|build)(/|$)|\.copybin$|\.tar$|\.zip$")
with tarfile.open(path, "r:gz") as tar:
    names = tar.getnames()
    bad = [name for name in names if forbidden.search(name)]
    if bad:
        raise SystemExit(f"archive contains forbidden path(s): {bad[:5]}")
PY

rm -rf "$DEST"
mkdir -p "$DEST"
tar -xzf "$LOCAL_ARCHIVE" -C "$DEST"

python3 - "$DEST" <<'PY'
import hashlib
import json
import sys
from pathlib import Path

root = Path(sys.argv[1])
manifest = json.loads((root / "artifact_manifest.json").read_text())
for entry in manifest:
    path = root / entry["path"]
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    if digest != entry["sha256"]:
        raise SystemExit(f"sha256 mismatch for {entry['path']}")
PY

if [[ "$KEEP_REMOTE_ARCHIVE" -eq 0 ]]; then
  remote_ssh_cmd "rm -f '$REMOTE_ARCHIVE'"
fi
rm -f "$LOCAL_ARCHIVE"
printf '%s\n' "$DEST"
