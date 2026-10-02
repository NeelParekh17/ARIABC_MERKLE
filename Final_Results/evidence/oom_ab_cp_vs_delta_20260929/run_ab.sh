set -u
cd /work/ARIABC/AriaBC
D=.bench_tmp/oom_ab_cp_vs_delta_20260929
for m in cp delta; do
  echo "=== reset-mode $m $(date +%T)"
  python3 -u scripts/distributed/run_oom_100m_benchmark.py --skews 0.0 --workers 8 --modes pg bcdb_merkle \
    --trials 2 --skip-gen --reset-mode $m --calibrate-min-wait-s 60 --out-dir $D/$m
  echo "=== exit $? $(date +%T)"
done
