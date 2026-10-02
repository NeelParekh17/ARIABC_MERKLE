#!/bin/bash
# Launch ONLY on .111 in tmux/nohup. Reinvoke with the same output to resume.
set -Eeuo pipefail
export LC_ALL=C
source "$(dirname "$0")/arguments.sh" "${1:-/home/neel/claude_ctl/results/oom_v2_20261001}"
cd "$V2_REPO"
mkdir -p "$V2_OUT/stages"
exec 9>"$V2_OUT/controller.lock"
flock -n 9 || { echo 'Another v2 controller owns this campaign'; exit 1; }
log() { printf '%s %s\n' "$(date -u +%FT%TZ)" "$*" | tee -a "$V2_OUT/status.txt"; }
STAGE=start
trap 'rc=$?; log "CAMPAIGN_FAILED stage=$STAGE rc=$rc line=$LINENO"; exit "$rc"' ERR
remote() {
  printf '%s ssh .247 %s\n' "$(date -u +%FT%TZ)" "$1" >> "$V2_OUT/commands.log"
  ssh -o BatchMode=yes -o ConnectTimeout=15 -o ServerAliveInterval=15 -o ServerAliveCountMax=8 neel@10.129.148.247 bash -s <<< "set -euo pipefail
$1"
}
run_stage() {
  STAGE=$1; shift
  if [[ -f "$V2_OUT/stages/$STAGE.OK" ]]; then log "resume skip $STAGE"; return; fi
  log "START $STAGE"
  printf '%s ' "$(date -u +%FT%TZ)" >> "$V2_OUT/commands.log"
  printf '%q ' "$@" >> "$V2_OUT/commands.log"
  printf '\n' >> "$V2_OUT/commands.log"
  "$@" > "$V2_OUT/$STAGE.log" 2>&1
  touch "$V2_OUT/stages/$STAGE.OK"
  log "OK $STAGE"
}
build_pg() {
  remote 'mkdir -p /home/neel/claude_opt/oom_v2_evidence'
  printf '%s\n' 'rsync immutable snapshot to .247:/home/neel/claude_opt/oom_v2_evidence/' >> "$V2_OUT/commands.log"
  rsync -a --checksum -e 'ssh -o BatchMode=yes' "$V2_OUT/provenance/source.tar.gz" "$V2_OUT/provenance/source.tar.gz.sha256" "$V2_REPO/scripts/distributed/oom_v2/build_pg.sh" neel@10.129.148.247:/home/neel/claude_opt/oom_v2_evidence/
  remote 'bash -n /home/neel/claude_opt/oom_v2_evidence/build_pg.sh'
  local status
  status=$(remote 'cat /home/neel/claude_opt/oom_v2_evidence/BUILD_STATUS 2>/dev/null || true')
  if [[ $status == OK ]]; then return; fi
  if [[ $status == RUNNING* ]]; then
    local pid=${status#*pid=}
    [[ $pid =~ ^[0-9]+$ ]]
    if ! remote "kill -0 $pid"; then
      echo 'Stale RUNNING build status; inspect build evidence before resuming'; return 1
    fi
  else
    remote 'nohup bash /home/neel/claude_opt/oom_v2_evidence/build_pg.sh > /home/neel/claude_opt/oom_v2_evidence/nohup.log 2>&1 < /dev/null & echo $! > /home/neel/claude_opt/oom_v2_evidence/build.pid'
    log 'PG_BUILD_DETACHED_STARTED on .247'
  fi
  for ((i=0; i<720; i++)); do
    status=$(remote 'cat /home/neel/claude_opt/oom_v2_evidence/BUILD_STATUS 2>/dev/null || true')
    if [[ $status == OK ]]; then
      rsync -a -e 'ssh -o BatchMode=yes' neel@10.129.148.247:/home/neel/claude_opt/oom_v2_evidence/ "$V2_OUT/build_evidence/"
      return
    fi
    if [[ $status == FAILED* ]]; then echo "$status"; return 1; fi
    sleep 30
  done
  echo 'PG build timed out; detached build may still be running'; return 1
}
build_cpp() {
  mkdir -p "$V2_INSTALL"
  rsync -a -e 'ssh -o BatchMode=yes' "neel@10.129.148.247:$V2_INSTALL/" "$V2_INSTALL/"
  export LD_LIBRARY_PATH="$V2_INSTALL/lib:/home/neel/Desktop/rdkafka_local/lib:${LD_LIBRARY_PATH:-}"
  cmake -S ariabc_pg -B ariabc_pg/build -DCMAKE_BUILD_TYPE=Release \
    -DLIBPQ_INCLUDE_DIR="$V2_INSTALL/include" -DPOSTGRES_INCLUDE_DIR="$V2_INSTALL/include" \
    -DLIBPQ_LIBRARY="$V2_INSTALL/lib/libpq.so" \
    -DRDKAFKA_INCLUDE_DIR=/home/neel/Desktop/rdkafka_local/include \
    -DRDKAFKA_LIBRARY=/home/neel/Desktop/rdkafka_local/lib/librdkafka.so
  cmake --build ariabc_pg/build --target ariabc_pg_gateway ariabc_pg_server -j2
  remote "mkdir -p $V2_CLUSTER/ariabc_pg/build/bin"
  rsync -a -e 'ssh -o BatchMode=yes' ariabc_pg/build/bin/ariabc_pg_server "neel@10.129.148.247:$V2_CLUSTER/ariabc_pg/build/bin/"
  local binary info
  for binary in ariabc_pg/build/bin/ariabc_pg_gateway ariabc_pg/build/bin/ariabc_pg_server; do
    info=$(ldd "$binary"); printf '%s\n' "$info"
    if [[ $info == *'not found'* ]]; then return 1; fi
  done
  sha256sum ariabc_pg/build/bin/ariabc_pg_gateway ariabc_pg/build/bin/ariabc_pg_server
  remote "export LD_LIBRARY_PATH=$V2_INSTALL/lib:/home/neel/Desktop/rdkafka_local/lib:\${LD_LIBRARY_PATH:-}; for b in $V2_INSTALL/bin/postgres $V2_CLUSTER/ariabc_pg/build/bin/ariabc_pg_server; do info=\$(ldd \"\$b\"); printf '%s\\n' \"\$info\"; case \"\$info\" in *'not found'*) exit 1;; esac; sha256sum \"\$b\"; done"
}
generation() {
  remote "mkdir -p $V2_REMOTE; df -h $V2_REMOTE; free -m; ps -eo pid,comm,rss --sort=-rss | sed -n '1,15p'; test ! -d /tmp/ariabc_oom_100m/benchmark.lock; test \$(df -B1 --output=avail $V2_REMOTE | tail -1) -ge 240000000000"
  python3 -u scripts/distributed/run_oom_100m_benchmark.py "${V2_COMMON[@]}" --gen-only --out-dir "$V2_OUT/generation"
  remote "test -f $V2_REMOTE/golden/done.flag"
}
campaign() {
  python3 -u scripts/distributed/run_oom_100m_benchmark.py "${V2_COMMON[@]}" --skip-gen --auto-resume --out-dir "$V2_OUT/campaign"
}
log 'CAMPAIGN_START frozen v2 source, SERIALIZABLE 84 cases'
run_stage build_pg build_pg
run_stage build_cpp build_cpp
run_stage generation generation
run_stage verification python3 -u scripts/distributed/oom_v2/verify_baseline.py "$V2_OUT/baseline_evidence" "${V2_COMMON[@]}"
run_stage preflight python3 -u scripts/distributed/run_oom_100m_benchmark.py "${V2_COMMON[@]}" --skip-gen --preflight-only --out-dir "$V2_OUT/preflight"
run_stage workload_hashes python3 scripts/distributed/oom_v2/check_workloads.py "$V2_OUT/provenance/published_workloads.json" "$V2_OUT/generation"
run_stage campaign campaign
run_stage summary python3 scripts/distributed/oom_v2/summarize.py "$V2_OUT"
log 'CAMPAIGN_DONE 84/84; summary.csv and summary.md written'
