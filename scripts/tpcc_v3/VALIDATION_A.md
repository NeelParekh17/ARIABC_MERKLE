# Agent A remote validation plan (integrator executes)

No SSH, build, database, server, gateway, or database benchmark was run by Agent A.
Local evidence is in `STATIC_CHECKS_A.json`. The commands below run the complete
pipeline on **protectdr@10.129.7.57**, using only new `~/claude_checks/v3/A_*`
directories. They must succeed before creating `A_READY`. Existing published
sources/installs and the canonical single-node PGDATA are read-only inputs.

1. Integrator stages the current, merged A+B source (including uncommitted files)
   into a **new** `~/claude_checks/v3/A_src` on ranking. Do not reuse an older A_src.
   Include root build sources, `src/`, `config/`, `contrib/`, `NuRaft/`,
   `ariabc_pg/`, and `scripts/`. Exclude existing binaries, build trees, generated
   Makefiles/config.status/pg_config headers, `.git`, `graphify-out`,
   `.bench_tmp`, `Final_Results`, and `scripts/bench_full_results`.
   Preserve a git HEAD, patch, and hash of the staged source with the evidence.
   Integrator staging commands (from the workstation; Agent A did not run these):

```bash
ssh protectdr@10.129.7.57 'set -e; mkdir -p ~/claude_checks/v3; test ! -e ~/claude_checks/v3/A_src; mkdir ~/claude_checks/v3/A_src'
rsync -a --exclude='.git/' --exclude='graphify-out/' --exclude='.bench_tmp/' \
  --exclude='Final_Results/' --exclude='scripts/bench_full_results/' \
  --exclude='ariabc_pg/build/' --exclude='NuRaft/build/' --exclude='**/__pycache__/' \
  --exclude='*.o' --exclude='*.a' --exclude='*.so*' --exclude='*.bc' \
  --exclude='/GNUmakefile' --exclude='/config.status' --exclude='/config.log' --exclude='/config.cache' \
  --exclude='/src/Makefile.global' --exclude='/src/include/pg_config.h' \
  --exclude='/src/include/pg_config_ext.h' --exclude='/src/include/pg_config_os.h' \
  /work/ARIABC/AriaBC/ protectdr@10.129.7.57:claude_checks/v3/A_src/
git -C /work/ARIABC/AriaBC rev-parse HEAD > /tmp/tpcc-v3-A-head.txt
git -C /work/ARIABC/AriaBC diff --binary > /tmp/tpcc-v3-A-working.diff
sha256sum /tmp/tpcc-v3-A-working.diff > /tmp/tpcc-v3-A-working.diff.sha256
scp /tmp/tpcc-v3-A-head.txt /tmp/tpcc-v3-A-working.diff /tmp/tpcc-v3-A-working.diff.sha256 \
  protectdr@10.129.7.57:claude_checks/v3/
```

   The following remaining commands are executed **on ranking**.

2. Build PostgreSQL (C changes require a new install), pg_prewarm, server,
   gateway, and focused executor tests. Create new build/install directories;
   never `make install` into install_v2 or any existing published directory.

```bash
set -euo pipefail
[[ $(id -un) == protectdr && " $(hostname -I) " == *' 10.129.7.57 '* ]]
ROOT=$HOME/claude_checks/v3
export SRC=$ROOT/A_src INST=$ROOT/A_install BINDIR=$ROOT/A_build/bin
PGBUILD=$ROOT/A_pgbuild
for d in "$INST" "$PGBUILD" "$ROOT/A_build"; do
  [[ ! -e $d ]] || { echo "Refusing existing build output: $d" >&2; exit 2; }
done
mkdir "$PGBUILD"
export LD_LIBRARY_PATH=$INST/lib:$HOME/Desktop/compat_lib:$HOME/Desktop/rdkafka_local/lib
export LC_ALL=C TZ=UTC
# Reuse reference configuration choices, replacing prefix and include directories.
python3 - "$HOME/claude_checks/install_v2/bin/pg_config" "$SRC" "$INST" "$PGBUILD" <<'PY'
import shlex,subprocess,sys
pgconfig,src,inst,build=sys.argv[1:]
args=shlex.split(subprocess.check_output([pgconfig,'--configure'],text=True))
# pg_config may include environment assignments; retain flags, not old build paths.
args=[a for a in args if a.startswith('--') and not a.startswith(('--prefix=','--with-includes='))]
subprocess.run([src+'/configure',*args,'--prefix='+inst,'--with-includes='+src+'/src/include/bcdb'],cwd=build,check=True)
PY
make -C "$PGBUILD" -j16 > "$ROOT/A_pg_build.log" 2>&1
make -C "$PGBUILD" install > "$ROOT/A_pg_install.log" 2>&1
make -C "$PGBUILD/contrib/pg_prewarm" -j16 > "$ROOT/A_prewarm_build.log" 2>&1
make -C "$PGBUILD/contrib/pg_prewarm" install > "$ROOT/A_prewarm_install.log" 2>&1
cmake -S "$SRC/ariabc_pg" -B "$ROOT/A_build" -DCMAKE_BUILD_TYPE=Release \
  -DLIBPQ_INCLUDE_DIR="$INST/include" -DPOSTGRES_INCLUDE_DIR="$INST/include" \
  -DLIBPQ_LIBRARY="$INST/lib/libpq.so" \
  -DRDKAFKA_INCLUDE_DIR="$HOME/Desktop/rdkafka_local/include" \
  -DRDKAFKA_LIBRARY="$HOME/Desktop/rdkafka_local/lib/librdkafka.so" \
  > "$ROOT/A_cpp_configure.log" 2>&1
cmake --build "$ROOT/A_build" --target ariabc_pg_server ariabc_pg_gateway \
  pg_error_result_test pg_retry_policy_test -j16 > "$ROOT/A_cpp_build.log" 2>&1
ctest --test-dir "$ROOT/A_build" --output-on-failure \
  -R '^(pg_error_result_test|pg_retry_policy_test)$' | tee "$ROOT/A_cpp_tests.log"
sha256sum "$INST/bin/postgres" "$INST/bin/psql" "$INST/lib/libpq.so" \
  "$INST/lib/pg_prewarm.so" "$BINDIR/ariabc_pg_server" "$BINDIR/ariabc_pg_gateway" \
  > "$ROOT/BINARIES.txt"
```

If reference configure paths or dependencies are unavailable, stop and send the
configure/build log. Do not silently substitute an old PostgreSQL binary: it
lacks the ordered rollback completion and block receipt changes.

3. Select and record an allowed fixed CPU set, check memory, and run the exact
   smoke validation script below. It checks that CPUSET is allowed and that at
   least 16 GB is available for an 8 GB smoke instance. It pins PostgreSQL,
   server, and gateway on ranking. `TAG` must name a fresh evidence root.

```bash
export CPUSET=$(python3 - <<'PY'
import os
print(','.join(map(str,sorted(os.sched_getaffinity(0))[:16])))
PY
)
free -g
lscpu > "$ROOT/A_lscpu.txt"
printf '%s\n' "$CPUSET" > "$ROOT/A_cpuset.txt"
export TAG=$(date -u +%Y%m%dT%H%M%SZ)
bash "$SRC/scripts/tpcc_v3/validate_remote_a.sh" \
  > "$ROOT/A_validation_${TAG}.log" 2>&1
```

`validate_remote_a.sh` is the complete executable command sequence, adapted from
`scripts/distributed/tpcc_v2/sweep_run.sh`. Ports are **55449 / 18110 / 19110**.
Each test gets its own new PGDATA under
`~/claude_checks/v3/A_validation_$TAG/A_{load1,load2,abort_pg,abort_det,abort_merkle,pg,det,merkle}`.
It stops only its own recorded server PID and its own PGDATA, and refuses
occupied ports. The DB, server, gateway and workload driver all run on ranking.

The sequence is: local-equivalent static checks; forbidden-call grep; shared
20k W=2 seed=42 workload and metadata; W=2 load twice; isolated forced-abort plus
successor in each mode; full 20k workload in pg, det and Merkle. Every database
is loaded via COPY into UNLOGGED tables; caller sets fillfactor 90 and LOGGED;
indexes/procedures use the exact legacy psql variable interface. A single
checksum-verified W=2 gzip cache is reused across modes. Smoke buffers are 8 GB;
final campaign buffers remain 32 GB. All transactions are SERIALIZABLE,
autovacuum/fsync/synchronous_commit/full_page_writes are on, enable_seqscan is
off, the v2 BCDB GUCs are preserved, and relations are prewarmed.

Acceptance (enforced by the executable script):

- Both initial loads: all 12 emitted checks true, `consistency_ok=t`, identical
  all-nine-table state hash. Hash serialization includes every timestamp column.
- Each forced abort: gateway rc 0, completed=2, user_aborts=1; unchanged complete
  state hash after earlier attempted district/order/stock/line writes; following
  Stock-Level finishes. Merkle verification returns 9 true rows.
- Each full run: gateway rc 0; final completed=20000 (**includes user aborts**);
  `user_aborts` equals the workload metadata `expected_rollbacks`, including pg;
  permanent_failures=0, divergence_count=0, counter_supported=1, poll_failures=0.
  District next-ID delta equals generated NewOrders minus expected rollbacks.
- Every mode: `consistency_ok=t`; Merkle mode: all nine merkle_verify rows true.
  Det and Merkle final `state.hash` files byte-identical, including timestamps.
- `forbidden_calls.txt` empty: no CURRENT_TIMESTAMP/now()/clock_timestamp()/random()
  in tpcc_procs.sql. The scripted settings CSV must match the settings above.
- PostgreSQL logs contain `[BCDB_USER_ABORT] ... rollback_complete=1` for each
  expected det/Merkle rollback, no expected abort `BCDB_FATAL` lines. The complete
  log, result.txt, hashes, consistency, settings, server stderr, binary hashes,
  pg_prewarm/build/test logs are retained. Inspect ptrace restart counts too.

The 20k tests are correctness smoke tests, not throughput evidence and do not
meet the 300-second measurement window. Agent B's campaign supplies that.
No equality is required between pg's final state and det: concurrent pg may
serialize in a different order. Equality **is** required between det and Merkle.

4. After success, the script writes `~/claude_checks/v3/A_READY` containing INST,
   BINDIR, SRC, PROCS, LOADER and EVIDENCE paths. Send the root log and evidence
   path back. On failure, send the failed build/SQL log, gateway final lines,
   server stderr, postgres_workload.log and both state hashes; Agent A will fix
   the implementation. Do not create readiness before these checks pass.

5. Loader scale feasibility (after correctness; new directories, no DB here):

```bash
/usr/bin/time -v python3 "$SRC/scripts/tpcc_v3/tpcc_load.py" \
  --warehouses 5 --seed 42 --generate-only "$ROOT/A_rows_w5" \
  > "$ROOT/A_rows_w5.json" 2> "$ROOT/A_rows_w5.time"
# To time actual W=5 COPY, create a fresh isolated database/PGDATA with the same
# startup settings and PORT=55449, apply tpcc_schema.sql, and then run:
/usr/bin/time -v python3 "$SRC/scripts/tpcc_v3/tpcc_load.py" \
  --warehouses 5 --seed 42 --host 127.0.0.1 --port 55449 --user postgres --db postgres \
  --cache-dir "$ROOT/A_scale_cache" > "$ROOT/A_load_w5.log" 2> "$ROOT/A_load_w5.time"
# W=100 remains streaming and bounded-memory; reserve disk space first.
# Do not launch a 100-warehouse load before checking free memory/disk and host use.
```

Report generation, COPY, LOGGED rewrite and index/Merkle build time separately.
Local W=5 row/COPY-file generation took 34.88 seconds (no database). Actual
COPY, LOGGED conversion and index build timings remain pending on ranking; no
end-to-end W=5/W=100 load performance claim is made.
