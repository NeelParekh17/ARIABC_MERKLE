#!/usr/bin/env bash
# Generate per-principal Ed25519 keys for request and result signatures.
#
#   gen_ed25519_keys.sh <out_dir> --nodes 1,2,4 [--clients 1]
#
# Writes <out_dir>/{node,client}_<id>.key.pem (0600) and .pub.pem.
# Distribute (default key dir on every host: ~/.ariabc/keys, or $ARIABC_KEY_DIR):
#   replica <id>: node_<id>.key.pem + every client_*.pub.pem
#   gateway <c>:  client_<c>.key.pem + every node_*.pub.pem
# Never copy a private key to any other host: a replica holding another
# principal's private key can sign as that principal.
set -euo pipefail

usage() { sed -n '2,12p' "$0"; exit 2; }

[[ $# -ge 1 ]] || usage
out="$1"; shift
nodes=""
clients="1"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --nodes) nodes="${2:-}"; shift 2 ;;
    --clients) clients="${2:-}"; shift 2 ;;
    *) usage ;;
  esac
done
[[ -n "$nodes" ]] || usage
command -v openssl >/dev/null || { echo "openssl not found" >&2; exit 1; }

umask 077
mkdir -p "$out"
gen() {
  local role="$1" id="$2"
  [[ "$id" =~ ^[0-9]+$ ]] || { echo "bad $role id '$id'" >&2; exit 1; }
  local priv="$out/${role}_${id}.key.pem" pub="$out/${role}_${id}.pub.pem"
  if [[ -e "$priv" ]]; then
    echo "keep  $priv (exists)"
  else
    openssl genpkey -algorithm ed25519 -out "$priv"
    echo "new   $priv"
  fi
  chmod 600 "$priv"
  openssl pkey -in "$priv" -pubout -out "$pub"
  chmod 644 "$pub"
}
IFS=, read -r -a node_ids <<<"$nodes"
IFS=, read -r -a client_ids <<<"$clients"
for id in "${node_ids[@]}"; do gen node "$id"; done
for id in "${client_ids[@]}"; do gen client "$id"; done
echo "done: $out"
