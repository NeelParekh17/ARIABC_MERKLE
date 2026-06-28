#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
BUILD_DIR="$REPO_ROOT/ariabc_pg/build"
OUT_DIR="$REPO_ROOT/scripts/bench_full_results/durable_tests_$(date +%Y%m%d_%H%M%S)"

mkdir -p "$OUT_DIR"

cmake -S "$REPO_ROOT/ariabc_pg" -B "$BUILD_DIR" \
  -DCMAKE_BUILD_TYPE=Release \
  2>&1 | tee "$OUT_DIR/cmake_configure.log"

cmake --build "$BUILD_DIR" -j"$(nproc)" \
  2>&1 | tee "$OUT_DIR/cmake_build.log"

ctest --test-dir "$BUILD_DIR" --output-on-failure \
  2>&1 | tee "$OUT_DIR/ctest.log"

python3 "$REPO_ROOT/scripts/distributed/parse_tps_metrics.py" --self-test \
  2>&1 | tee "$OUT_DIR/parser_self_test.log"

git -C "$REPO_ROOT" diff --check \
  2>&1 | tee "$OUT_DIR/git_diff_check.log"

git -C "$REPO_ROOT" status --short \
  2>&1 | tee "$OUT_DIR/git_status.log"

echo "PASS: durable test evidence saved in $OUT_DIR"
