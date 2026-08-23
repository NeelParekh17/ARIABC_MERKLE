#!/usr/bin/env bash

ARIABC_DEFAULT_FULL_THREADS="${ARIABC_DEFAULT_FULL_THREADS:-1,2,4,8}"

ariabc_apply_comparison_profile_defaults() {
  local profile="${1:-manual}"
  case "$profile" in
    base-no-raft-no-kafka|vanilla-pg)
      NO_KAFKA=1
      WAIT_MAJORITY=0
      SERVER_BYPASS_RAFT=0
      GW_BROADCAST_ALL=0
      ;;
    raft-no-kafka)
      NO_KAFKA=1
      WAIT_MAJORITY=0
      SERVER_BYPASS_RAFT=0
      GW_BROADCAST_ALL=0
      ;;
    raft-kafka)
      NO_KAFKA=0
      WAIT_MAJORITY=1
      SERVER_BYPASS_RAFT=0
      GW_BROADCAST_ALL=0
      ;;
    kafka-only-no-raft)
      NO_KAFKA=0
      WAIT_MAJORITY=1
      SERVER_BYPASS_RAFT=1
      GW_BROADCAST_ALL=1
      ;;
  esac
}

ariabc_normalize_benchmark_flags() {
  if [[ "${NO_KAFKA:-0}" == "1" ]]; then
    WAIT_MAJORITY=0
  fi
  if [[ "${COMPARISON_PROFILE:-manual}" == "vanilla-pg" ]]; then
    MODES="nondet"
  fi
  if [[ "${SRV_PG_EXEC_MODE:-threaded}" == "event" && "${DET_PARALLEL_WORKERS:-0}" != "0" ]]; then
    echo "INFO: forcing DET_PARALLEL_WORKERS=0 for event-mode server execution; deterministic parallel-worker coordination is threaded-only." >&2
    DET_PARALLEL_WORKERS=0
  fi
}
