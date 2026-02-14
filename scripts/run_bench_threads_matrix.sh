#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

# Example:
#   ./run_bench_threads_matrix.sh
#   ./run_bench_threads_matrix.sh --resume --runs 3
#   ./run_bench_threads_matrix.sh --threads 1,4,8,12,16 --modes det,nondet

python3 ./bench_threads_matrix.py "$@"

