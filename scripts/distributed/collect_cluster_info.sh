#!/usr/bin/env bash
set -euo pipefail

# collect_cluster_info.sh
# Usage examples:
# 1) Quick info gather:
#    scripts/distributed/collect_cluster_info.sh --hosts h1,h2,h3,h4 \
#      --ssh-user ubuntu --ssh-key ~/.ssh/id_rsa
# 2) Provide pg/raft/gateway to auto-generate preflight command:
#    scripts/distributed/collect_cluster_info.sh --pg-hosts p1,p2,p3 \
#      --raft-hosts r1,r2,r3 --gateway-host g1 --ssh-user ubuntu --ssh-key ~/.ssh/id_rsa

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SSH_USER="${USER:-}"
SSH_KEY=""
SSH_PORT=22
HOSTS_CSV=""
PG_HOSTS_CSV=""
RAFT_HOSTS_CSV=""
RAFT_HOST=""
GATEWAY_HOST=""

usage(){
  cat <<'EOF_HELP'
collect_cluster_info.sh --hosts a,b,c,d |
  --pg-hosts p1,p2,p3 [--raft-hosts r1,r2,r3 | --raft-host r1] [--gateway-host g1]
  --ssh-user user --ssh-key /path/to/key --ssh-port 22

Prints host info, SSH reachability, and (if given pg/raft) the ready-to-run preflight command.
Defaults:
- if raft hosts are omitted, raft hosts := pg hosts (co-located).
- if gateway host is omitted, gateway host := first raft host.
EOF_HELP
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

join_csv() {
  local -n arr_ref="$1"
  local IFS=,
  printf '%s' "${arr_ref[*]}"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --hosts) HOSTS_CSV="${2:-}"; shift 2 ;;
    --pg-hosts) PG_HOSTS_CSV="${2:-}"; shift 2 ;;
    --raft-hosts) RAFT_HOSTS_CSV="${2:-}"; shift 2 ;;
    --raft-host) RAFT_HOST="${2:-}"; shift 2 ;;
    --gateway-host) GATEWAY_HOST="${2:-}"; shift 2 ;;
    --ssh-user) SSH_USER="${2:-}"; shift 2 ;;
    --ssh-key) SSH_KEY="${2:-}"; shift 2 ;;
    --ssh-port) SSH_PORT="${2:-22}"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown arg: $1" >&2; usage; exit 2 ;;
  esac
done

if [[ -z "$HOSTS_CSV" && -z "$PG_HOSTS_CSV" ]]; then
  echo "ERROR: either --hosts or --pg-hosts must be provided." >&2
  usage
  exit 2
fi

if [[ -z "$RAFT_HOSTS_CSV" && -n "$RAFT_HOST" ]]; then
  RAFT_HOSTS_CSV="$RAFT_HOST,$RAFT_HOST,$RAFT_HOST"
fi

if [[ -z "$RAFT_HOSTS_CSV" && -n "$PG_HOSTS_CSV" ]]; then
  RAFT_HOSTS_CSV="$PG_HOSTS_CSV"
fi

# If --hosts provided, use that list; else combine pg+raft+gateway.
if [[ -n "$HOSTS_CSV" ]]; then
  split_csv "$HOSTS_CSV" HOSTS
else
  split_csv "$PG_HOSTS_CSV" HOSTS
  if [[ -n "$RAFT_HOSTS_CSV" ]]; then
    split_csv "$RAFT_HOSTS_CSV" _raft_hosts
    HOSTS+=("${_raft_hosts[@]}")
  fi
  if [[ -z "$GATEWAY_HOST" && -n "$RAFT_HOSTS_CSV" ]]; then
    split_csv "$RAFT_HOSTS_CSV" _raft_hosts_first
    if [[ "${#_raft_hosts_first[@]}" -gt 0 ]]; then
      GATEWAY_HOST="${_raft_hosts_first[0]}"
    fi
  fi
  if [[ -n "$GATEWAY_HOST" ]]; then
    HOSTS+=("$GATEWAY_HOST")
  fi
fi

# Unique hosts while preserving order.
declare -A seen=()
declare -a UNIQUE_HOSTS=()
for h in "${HOSTS[@]}"; do
  if [[ -z "${seen[$h]:-}" ]]; then
    seen[$h]=1
    UNIQUE_HOSTS+=("$h")
  fi
done

SSH_OPTS=( -o BatchMode=yes -o ConnectTimeout=8 -o StrictHostKeyChecking=no -p "$SSH_PORT" )
if [[ -n "$SSH_KEY" ]]; then
  SSH_OPTS+=( -i "$SSH_KEY" )
fi

check_host() {
  local host="$1"
  local label="$2"
  printf "\n-- %s (%s) --\n" "$label" "$host"
  if ssh "${SSH_OPTS[@]}" "$SSH_USER@$host" 'echo REACHABLE' >/dev/null 2>&1; then
    ssh "${SSH_OPTS[@]}" "$SSH_USER@$host" \
      'echo "user=$(whoami)"; echo "fqdn=$(hostname -f 2>/dev/null || hostname)"; echo "ips=$(hostname -I 2>/dev/null)"; ip -4 addr show | awk "/inet /{print \$2,\$NF}" | sed "s/^/iface: /"; echo "os=$(grep PRETTY_NAME /etc/os-release 2>/dev/null | cut -d= -f2- | tr -d \"\")"; echo "cpu=$(nproc)"; echo "ram_mb=$(awk \"/MemTotal/ {print int(\$2/1024)}\" /proc/meminfo)"; df -h / | awk "NR==2{print \$4 \" free on \" \$1}"; ls -ld /work/ARIABC /work/ARIABC/AriaBC /work/ARIABC/install 2>/dev/null || true' || true
  else
    echo "SSH UNREACHABLE: could not connect to $host as $SSH_USER (check name/IP and network)."
  fi
}

idx=0
for h in "${UNIQUE_HOSTS[@]}"; do
  idx=$((idx+1))
  check_host "$h" "host-$idx"
done

if [[ -n "$PG_HOSTS_CSV" && -n "$RAFT_HOSTS_CSV" ]]; then
  if [[ -z "$GATEWAY_HOST" ]]; then
    split_csv "$RAFT_HOSTS_CSV" _raft_hosts_first
    GATEWAY_HOST="${_raft_hosts_first[0]}"
  fi

  echo
  echo "== Generated preflight command (copy/paste) =="
  ssh_key_arg=""
  if [[ -n "$SSH_KEY" ]]; then
    ssh_key_arg="--ssh-key $SSH_KEY"
  fi
  cat <<EOF_CMD
scripts/distributed/preflight_then_run_full.sh \\
  --pg-hosts $PG_HOSTS_CSV \\
  --raft-hosts $RAFT_HOSTS_CSV \\
  --gateway-host $GATEWAY_HOST \\
  --ssh-user $SSH_USER \\
  ${ssh_key_arg:+$ssh_key_arg\
  }--ssh-port $SSH_PORT \\
  --remote-repo-root /work/ARIABC/AriaBC \\
  --remote-install-dir /work/ARIABC/install
EOF_CMD
fi

if [[ -n "$PG_HOSTS_CSV" && -z "$RAFT_HOSTS_CSV" ]]; then
  echo
  echo "Note: no raft hosts provided; they default to --pg-hosts for co-located topology."
fi

exit 0
