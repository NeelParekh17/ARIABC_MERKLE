#!/usr/bin/env bash
set -euo pipefail

#
# run_three_mode_bench.sh
#
# Runs bench_threads_matrix.py three times on the LOCAL node, once for each
# deterministic-mode gate configuration:
#
#   Mode A – paper      : bcdb_serial_gate_source=1  bcdb_advance_commit_watermark=off
#             (pre-conflict gate blocks on last_committed — closest to paper Algorithm 1)
#
#   Mode B – leverd     : bcdb_serial_gate_source=0  bcdb_advance_commit_watermark=off
#             (Lever D v2: published_max gate + legacy blocking finish)
#
#   Mode C – leverd_t3  : bcdb_serial_gate_source=0  bcdb_advance_commit_watermark=on
#             (Lever D v2 + T3 CAS finish — current default)
#
# Each pass writes to its own sub-directory under OUT_DIR.
# After all three passes the script writes combined_results.csv and calls
# plot_three_modes_comparison.py to generate the four comparison graphs.
#
# Usage:
#   bash run_three_mode_bench.sh [OPTIONS]
#
#   --threads <csv>        Thread counts, default: 1,4,8,12,16
#   --runs    <n>          Runs per cell, default: 1
#   --workloads <csv>      Workload filenames, default: bench_threads_matrix.py defaults
#   --out-dir <path>       Output root, default: scripts/bench_results/three_mode_<ts>
#   --port    <5438>       PostgreSQL port
#   --db      <postgres>   DB name
#   --user    <postgres>   DB user
#   --skip-paper           Skip mode A (paper-style)
#   --skip-leverd          Skip mode B (Lever D only)
#   --skip-leverd-t3       Skip mode C (Lever D + T3)
#   --no-pg-baseline       Do not run plain pg baseline (default: runs it once)
#

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
SCRIPTS_DIR="$REPO_ROOT/scripts"

# ---- Defaults ----
THREADS="1,4,8,12,16"
RUNS=1
WORKLOADS=""
OUT_DIR=""
DB_PORT=5438
DB_NAME="postgres"
DB_USER="postgres"
SKIP_PAPER=0
SKIP_LEVERD=0
SKIP_LEVERD_T3=0
NO_PG_BASELINE=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --threads)        THREADS="${2:-}"; shift 2 ;;
    --runs)           RUNS="${2:-1}"; shift 2 ;;
    --workloads)      WORKLOADS="${2:-}"; shift 2 ;;
    --out-dir)        OUT_DIR="${2:-}"; shift 2 ;;
    --port)           DB_PORT="${2:-5438}"; shift 2 ;;
    --db)             DB_NAME="${2:-postgres}"; shift 2 ;;
    --user)           DB_USER="${2:-postgres}"; shift 2 ;;
    --skip-paper)     SKIP_PAPER=1; shift 1 ;;
    --skip-leverd)    SKIP_LEVERD=1; shift 1 ;;
    --skip-leverd-t3) SKIP_LEVERD_T3=1; shift 1 ;;
    --no-pg-baseline) NO_PG_BASELINE=1; shift 1 ;;
    -h|--help)
      sed -n '/^# Usage/,/^[^#]/p' "$0" | head -20
      exit 0 ;;
    *) echo "Unknown arg: $1" >&2; exit 2 ;;
  esac
done

ts="$(date +%Y%m%d_%H%M%S)"
if [[ -z "$OUT_DIR" ]]; then
  OUT_DIR="$SCRIPTS_DIR/bench_results/three_mode_${ts}"
fi
mkdir -p "$OUT_DIR"

lmsg() { echo "[$(date '+%F %T')] $*"; }

# Resolve psql/python just like bench_threads_matrix does
INSTALL_DIR="${ARIABC_INSTALL_DIR:-/work/ARIABC/install}"
if [[ -x "$INSTALL_DIR/bin/psql" ]]; then
  PSQL="$INSTALL_DIR/bin/psql"
else
  PSQL="$(command -v psql 2>/dev/null || echo psql)"
fi

PYTHON_BIN=""
if [[ -x "$REPO_ROOT/.venv/bin/python" ]] && "$REPO_ROOT/.venv/bin/python" -c 'import psycopg' >/dev/null 2>&1; then
  PYTHON_BIN="$REPO_ROOT/.venv/bin/python"
elif python3 -c 'import psycopg' >/dev/null 2>&1; then
  PYTHON_BIN=python3
else
  PYTHON_BIN=python3
fi

BENCH_PY="$SCRIPTS_DIR/bench_threads_matrix.py"
if [[ ! -f "$BENCH_PY" ]]; then
  echo "ERROR: missing $BENCH_PY" >&2; exit 1
fi

WORKLOAD_ARGS=()
if [[ -n "$WORKLOADS" ]]; then
  WORKLOAD_ARGS=(--workloads "$WORKLOADS")
fi

# ---- Helper: apply GUCs and restart ----
apply_gucs() {
  local gucs="$1"   # e.g. "bcdb_serial_gate_source=1,bcdb_advance_commit_watermark=off"
  lmsg "Applying GUCs: $gucs"
  IFS=',' read -ra _gucs <<< "$gucs"
  for _guc in "${_gucs[@]}"; do
    _key="${_guc%%=*}"
    _val="${_guc#*=}"
    LD_LIBRARY_PATH="${INSTALL_DIR}/lib:${LD_LIBRARY_PATH:-}" \
      "$PSQL" -X -q -h localhost -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" \
      -c "ALTER SYSTEM SET ${_key} = '${_val}';" || {
        lmsg "WARN: ALTER SYSTEM SET ${_key}='${_val}' failed — server may not be running yet (will retry via start_server)"
      }
  done
}

reset_gucs() {
  lmsg "Resetting GUCs to defaults"
  LD_LIBRARY_PATH="${INSTALL_DIR}/lib:${LD_LIBRARY_PATH:-}" \
    "$PSQL" -X -q -h localhost -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" \
    -c "ALTER SYSTEM RESET bcdb_serial_gate_source; ALTER SYSTEM RESET bcdb_advance_commit_watermark;" \
    2>/dev/null || true
}

# ---- Helper: run one bench pass ----
run_bench_pass() {
  local label="$1"     # paper | leverd | leverd_t3 | pg
  local mode="$2"      # det | pg
  local gucs="$3"      # GUC string to apply, empty for pg
  local pass_dir="$OUT_DIR/$label"
  mkdir -p "$pass_dir"

  lmsg "=== Starting pass: $label (mode=$mode gucs='$gucs') ==="

  export BCDB_EXTRA_GUCS="$gucs"
  export ARIABC_INSTALL_DIR="$INSTALL_DIR"
  export ARIABC_PYTHON="$PYTHON_BIN"
  export ARIABC_DIR="$REPO_ROOT"

  "$PYTHON_BIN" -u "$BENCH_PY" \
    --modes "$mode" \
    --threads "$THREADS" \
    --runs "$RUNS" \
    --port "$DB_PORT" \
    --db "$DB_NAME" \
    --user "$DB_USER" \
    --out-dir "$pass_dir" \
    --signing-modes 0 \
    "${WORKLOAD_ARGS[@]}" \
    2>&1 | tee "$pass_dir/bench.log"

  # Tag each row in results.csv with the pass label so we can merge later
  if [[ -f "$pass_dir/results.csv" ]]; then
    python3 - "$pass_dir/results.csv" "$label" <<'PYEOF'
import csv, sys
from pathlib import Path
p = Path(sys.argv[1])
label = sys.argv[2]
rows = []
with p.open(newline="") as f:
    reader = csv.DictReader(f)
    fieldnames = reader.fieldnames or []
    if "gate_mode" not in fieldnames:
        fieldnames = list(fieldnames) + ["gate_mode"]
    for row in reader:
        row["gate_mode"] = label
        rows.append(row)
with p.open("w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)
print(f"Tagged {len(rows)} rows with gate_mode={label}")
PYEOF
  fi

  export BCDB_EXTRA_GUCS=""
  lmsg "Pass $label complete — results: $pass_dir"
}

# ---- PG baseline (once, no BCDB GUCs) ----
if [[ "$NO_PG_BASELINE" == "0" ]]; then
  run_bench_pass "pg_baseline" "pg" ""
fi

# ---- Mode A: paper-style ----
if [[ "$SKIP_PAPER" == "0" ]]; then
  run_bench_pass "paper" "det" "bcdb_serial_gate_source=1,bcdb_advance_commit_watermark=off"
fi

# ---- Mode B: Lever D only ----
if [[ "$SKIP_LEVERD" == "0" ]]; then
  run_bench_pass "leverd" "det" "bcdb_serial_gate_source=0,bcdb_advance_commit_watermark=off"
fi

# ---- Mode C: Lever D + T3 ----
if [[ "$SKIP_LEVERD_T3" == "0" ]]; then
  run_bench_pass "leverd_t3" "det" "bcdb_serial_gate_source=0,bcdb_advance_commit_watermark=on"
fi

# ---- Reset to defaults ----
reset_gucs

# ---- Merge results into combined_results.csv ----
lmsg "=== Merging results ==="
COMBINED_CSV="$OUT_DIR/combined_results.csv"
python3 - "$OUT_DIR" "$COMBINED_CSV" <<'PYEOF'
import csv, sys
from pathlib import Path

root = Path(sys.argv[1])
out_csv = Path(sys.argv[2])
all_rows = []
fieldnames = None

for pass_dir in sorted(root.iterdir()):
    csv_path = pass_dir / "results.csv"
    if not csv_path.exists():
        continue
    with csv_path.open(newline="") as f:
        reader = csv.DictReader(f)
        if fieldnames is None:
            fieldnames = list(reader.fieldnames or [])
            if "gate_mode" not in fieldnames:
                fieldnames.append("gate_mode")
        for row in reader:
            if "gate_mode" not in row:
                row["gate_mode"] = pass_dir.name
            all_rows.append(row)

if not all_rows:
    print("WARNING: no results to merge", file=sys.stderr)
    sys.exit(0)

with out_csv.open("w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(all_rows)

print(f"Merged {len(all_rows)} rows from {sum(1 for d in root.iterdir() if (d/'results.csv').exists())} passes → {out_csv}")
PYEOF

lmsg "Combined results: $COMBINED_CSV"

# ---- Generate graphs ----
PLOT_PY="$SCRIPT_DIR/plot_three_modes_comparison.py"
if [[ -f "$PLOT_PY" ]]; then
  lmsg "=== Generating comparison graphs ==="
  python3 "$PLOT_PY" "$COMBINED_CSV" "$OUT_DIR" && lmsg "Graphs written to $OUT_DIR" || lmsg "WARN: graph generation failed"
else
  lmsg "WARN: $PLOT_PY not found — skipping graphs"
fi

lmsg "=== All done. Results: $OUT_DIR ==="
