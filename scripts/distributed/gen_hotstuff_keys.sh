#!/usr/bin/env bash
# Generate secp256k1 vote keys for --consensus hotstuff.
#
#   gen_hotstuff_keys.sh <out_dir> --nodes 1,2,3,4
#
# Writes <out_dir>/hs_node_<id>.key (0600, hex) and hs_node_<id>.pub (hex).
# Distribute (default key dir on every host: ~/.ariabc/keys, or $ARIABC_KEY_DIR):
#   replica <id>: hs_node_<id>.key + every hs_node_*.pub
# Never copy a private key to any other host: a replica holding another
# replica's key can vote as that replica.
set -euo pipefail

usage() { sed -n '2,10p' "$0"; exit 2; }

[[ $# -eq 3 && "$2" == "--nodes" ]] || usage
out="$1"
command -v openssl >/dev/null || { echo "openssl not found" >&2; exit 1; }

umask 077
mkdir -p "$out"
IFS=, read -r -a ids <<<"$3"
for id in "${ids[@]}"; do
  [[ "$id" =~ ^[0-9]+$ ]] || { echo "bad node id '$id'" >&2; exit 1; }
  key="$out/hs_node_$id.key"
  pub="$out/hs_node_$id.pub"
  if [[ -e "$key" ]]; then
    echo "keep  $key (exists)"
    continue
  fi
  pem="$(openssl ecparam -name secp256k1 -genkey -noout)"
  # EC private key DER: 7-byte header, then the 32-byte scalar.
  openssl ec -outform DER -no_public 2>/dev/null <<<"$pem" | tail -c +8 | head -c 32 | od -An -tx1 | tr -d ' \n' >"$key"
  # Public key DER ends with the 33-byte compressed point.
  openssl ec -pubout -conv_form compressed -outform DER 2>/dev/null <<<"$pem" | tail -c 33 | od -An -tx1 | tr -d ' \n' >"$pub"
  chmod 600 "$key"
  chmod 644 "$pub"
  [[ "$(wc -c <"$key")" -eq 64 && "$(wc -c <"$pub")" -eq 66 ]] || { echo "key generation failed for node $id" >&2; rm -f "$key" "$pub"; exit 1; }
  echo "new   $key"
done
echo "done: $out"
