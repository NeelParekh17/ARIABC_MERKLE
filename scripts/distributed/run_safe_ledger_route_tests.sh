#!/usr/bin/env bash
# Run the remaining safe-ledger route validation checks from PLAN.md:
#   - ordinary user DML stays deferred
#   - direct internal INSERT / UPDATE / DELETE stay direct
#
# Each check is isolated in its own fresh cluster run so a control-table DML
# statement cannot poison the next case.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

RUNNER="$SCRIPT_DIR/run_4node_raft_cluster.sh"
USER_WORKLOAD="$SCRIPT_DIR/test_safe_ledger_user_dml.sql"
INTERNAL_WORKLOAD="$SCRIPT_DIR/test_safe_ledger_internal_dml.sql"

[[ -x "$RUNNER" ]] || { echo "ERROR: missing runner: $RUNNER" >&2; exit 1; }
[[ -f "$USER_WORKLOAD" ]] || { echo "ERROR: missing workload: $USER_WORKLOAD" >&2; exit 1; }
[[ -f "$INTERNAL_WORKLOAD" ]] || { echo "ERROR: missing workload: $INTERNAL_WORKLOAD" >&2; exit 1; }

run_case() {
  local case_name="$1"
  local workload_file="$2"
  local epoch_hex="$3"
  local artifact_dir
  local latest_before latest_after

  latest_before="$(ls -1dt "$REPO_ROOT"/scripts/bench_full_results/cluster4_* 2>/dev/null | head -1 || true)"

  env \
    ARIABC_SAFE_TRACE=1 \
    ARIABC_SAFE_POSTCOMMIT_WITNESS=1 \
    ARIABC_SAFE_EXTERNAL_PROBE=1 \
    FORCE_BUILD=1 \
    SKIP_RDKAFKA_SETUP=1 \
    "$RUNNER" \
      --threads 1 \
      --det-window 1 \
      --det-batch-size 1 \
      --det-pipeline-depth 1 \
      --pool-size 1 \
      --bcdb-worker-count 1 \
      --bcdb-decouple-workers 0 \
      --raft-storage-mode durable \
      --raft-storage-action fresh \
      --raft-apply-ledger-mode safe \
      --raft-epoch-hex "$epoch_hex" \
      --skip-post-verify \
      --workload "$workload_file"

  latest_after="$(ls -1dt "$REPO_ROOT"/scripts/bench_full_results/cluster4_* 2>/dev/null | head -1 || true)"
  artifact_dir="$latest_after"
  if [[ -n "$latest_before" && "$latest_after" == "$latest_before" ]]; then
    echo "ERROR: could not identify a new artifact directory for $case_name" >&2
    exit 1
  fi
  [[ -n "$artifact_dir" ]] || { echo "ERROR: no artifact directory found for $case_name" >&2; exit 1; }

  echo "case=$case_name artifact=$artifact_dir"
  case "$case_name" in
    user-dml)
      grep -R -h "BCDB_DML_ROUTE op=UPDATE relation=public.usertable_small mode=deferred is_bcdb_worker=1" "$artifact_dir" >/dev/null \
        || { echo "ERROR: missing deferred user DML route trace" >&2; exit 1; }
      grep -R -h "SAFE_POSTCOMMIT_WITNESS_VISIBLE" "$artifact_dir" >/dev/null \
        || { echo "ERROR: missing post-commit witness trace" >&2; exit 1; }
      ;;
    internal-dml)
      grep -R -h "BCDB_DML_ROUTE op=INSERT relation=ariabc_internal.raft_apply_epoch mode=direct" "$artifact_dir" >/dev/null \
        || { echo "ERROR: missing direct INSERT route trace" >&2; exit 1; }
      grep -R -h "BCDB_DML_ROUTE op=UPDATE relation=ariabc_internal.raft_apply_epoch mode=direct" "$artifact_dir" >/dev/null \
        || { echo "ERROR: missing direct UPDATE route trace" >&2; exit 1; }
      grep -R -h "BCDB_DML_ROUTE op=DELETE relation=ariabc_internal.raft_apply_epoch mode=direct" "$artifact_dir" >/dev/null \
        || { echo "ERROR: missing direct DELETE route trace" >&2; exit 1; }
      ;;
    *)
      echo "ERROR: unknown case: $case_name" >&2
      exit 1
      ;;
  esac

  if [[ -f "$artifact_dir/run_summary.env" ]]; then
    grep -E '^(divergence_count|permanent_failures)=' "$artifact_dir/run_summary.env" || true
  fi
}

USER_EPOCH="$(openssl rand -hex 32)"
INTERNAL_EPOCH="$(openssl rand -hex 32)"

run_case "user-dml" "$USER_WORKLOAD" "$USER_EPOCH"
run_case "internal-dml" "$INTERNAL_WORKLOAD" "$INTERNAL_EPOCH"

echo "PASS: safe-ledger route checks completed"
