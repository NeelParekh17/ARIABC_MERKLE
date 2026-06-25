# AriaBC Distributed 4-Node Runbook (Det + NuRaft, No-Kafka and Kafka)

## 1. Scope

This runbook is the operator reference for deterministic distributed benchmark runs with:

1. Three PostgreSQL + NuRaft server nodes.
2. One gateway host.
3. One local control machine (this workspace).

It covers both:

1. No-Kafka mode (`--no-kafka`, default benchmark profile).
2. Kafka-enabled mode (`no-kafka=0`).

## 2. Supported Topologies

### 2.1 Strict 4-node topology

1. `PG1 + Raft1`: `neel@10.129.148.248`
2. `PG2 + Raft2`: `neel@10.129.148.246`
3. `PG3 + Raft3`: `neel@10.129.148.246`
4. `Gateway`: `neel@10.129.148.236`
5. `Control`: local machine at `/work/ARIABC/AriaBC`

### 2.2 Temporary fallback topology (if node3 has toolchain mismatch)

If `10.129.148.246` fails with errors like `GLIBC_2.38 not found` / `GLIBCXX_3.4.32`, use:

1. `PG1 + Raft1`: `neel@10.129.148.248`
2. `PG2 + Raft2`: `neel@10.129.148.246`
3. `PG3 + Raft3`: `neel@10.129.148.236`
4. `Gateway`: `neel@10.129.148.236` (co-located)

Use fallback only until node3 binaries are rebuilt compatibly.

## 3. Hard Requirements

1. Always run from `/work/ARIABC/AriaBC`.
2. Keep remote roots consistent:
   1. `--remote-repo-root /home/neel/Desktop/ariabc_cluster`
   2. `--remote-install-dir /home/neel/Desktop/ariabc_install`
3. Before trusting a fix, sync script/binaries to remote hosts. Stale remote files can silently run old behavior.
4. Keep host/user mapping correct (`10.129.148.246` uses `neel`).
5. Ensure Python dependency availability (`psycopg`) on all remote hosts.

## 4. Mandatory Reset Before Session

Run before every distributed session.

```bash
cd /work/ARIABC/AriaBC

for h in 10.129.148.248 10.129.148.246 10.129.148.246 10.129.148.236; do
  u="neel"
  [[ "$h" == "10.129.148.246" ]] && u="neel"
  ssh -i ~/.ssh/id_rsa -o BatchMode=yes -o StrictHostKeyChecking=no "$u@$h" '
    pkill -f "ariabc_pg_gateway|ariabc_pg_server|bench_nuraft_kafka_matrix.py|bench_threads_matrix.py" || true
    pkill -f "postgres .*5438|postgres .*5439|postgres .*5440" || true
    rm -rf /home/neel/Desktop/ariabc_cluster/.bench_tmp/* || true
    mkdir -p /home/neel/Desktop/ariabc_cluster/.bench_tmp
  '
done
```

## 5. One-Command Orchestrator (Preferred)

`preflight_then_run_full.sh` forwards benchmark profile knobs for both smoke and full runs and supports Kafka/no-kafka switching.

### 5.1 Deterministic baseline (No-Kafka)

```bash
cd /work/ARIABC/AriaBC

PROFILE_NO_KAFKA=1 \
PROFILE_GW_SUBMIT_MODE=blocking \
PROFILE_GW_DET_SUBMIT_PIPELINE=0 \
PROFILE_SRV_PG_EXEC_MODE=event \
PROFILE_CASE_TIMEOUT_S=240 \
PROFILE_GATEWAY_TIMEOUT_S=60 \
PROFILE_ABORT_ON_INVALID_CASE=1 \
scripts/distributed/preflight_then_run_full.sh \
  --pg-hosts 10.129.148.248,10.129.148.246,10.129.148.246 \
  --pg-client-hosts 10.129.148.248,10.129.148.246,10.129.148.246 \
  --raft-hosts 10.129.148.248,10.129.148.246,10.129.148.246 \
  --raft-member-hosts 10.129.148.248,10.129.148.246,10.129.148.246 \
  --raft-client-hosts 10.129.148.248,10.129.148.246,10.129.148.246 \
  --pg-users neel,neel,neel \
  --raft-users neel,neel,neel \
  --gateway-host 10.129.148.236 \
  --gateway-user neel \
  --ssh-user neel \
  --ssh-key ~/.ssh/id_rsa \
  --ssh-port 22 \
  --remote-repo-root /home/neel/Desktop/ariabc_cluster \
  --remote-install-dir /home/neel/Desktop/ariabc_install \
  --bcdb-worker-counts 4,8,4 \
  --shared-buffers 512MB,2GB,512MB \
  --max-connections 300 \
  --bcdb-serial-gate-mode 1 \
  --bcdb-result-ring-slots 256 \
  --db-conn-pool-cap 4 \
  --db-conn-pool-size 4 \
  --det-window 16
```

### 5.2 Deterministic + NuRaft + Kafka combined

Use this when validating Kafka-integrated deterministic path.

```bash
cd /work/ARIABC/AriaBC

PROFILE_NO_KAFKA=0 \
PROFILE_GW_SUBMIT_MODE=event \
PROFILE_GW_DET_SUBMIT_PIPELINE=1 \
PROFILE_SRV_PG_EXEC_MODE=threaded \
PROFILE_CASE_TIMEOUT_S=240 \
PROFILE_GATEWAY_TIMEOUT_S=60 \
PROFILE_ABORT_ON_INVALID_CASE=0 \
scripts/distributed/preflight_then_run_full.sh \
  --pg-hosts 10.129.148.248,10.129.148.246,10.129.148.246 \
  --pg-client-hosts 10.129.148.248,10.129.148.246,10.129.148.246 \
  --raft-hosts 10.129.148.248,10.129.148.246,10.129.148.246 \
  --raft-member-hosts 10.129.148.248,10.129.148.246,10.129.148.246 \
  --raft-client-hosts 10.129.148.248,10.129.148.246,10.129.148.246 \
  --pg-users neel,neel,neel \
  --raft-users neel,neel,neel \
  --gateway-host 10.129.148.236 \
  --gateway-user neel \
  --ssh-user neel \
  --ssh-key ~/.ssh/id_rsa \
  --ssh-port 22 \
  --remote-repo-root /home/neel/Desktop/ariabc_cluster \
  --remote-install-dir /home/neel/Desktop/ariabc_install \
  --bcdb-worker-counts 4,8,4 \
  --shared-buffers 512MB,2GB,512MB \
  --max-connections 300 \
  --bcdb-serial-gate-mode 1 \
  --bcdb-result-ring-slots 256 \
  --db-conn-pool-cap 8 \
  --db-conn-pool-size 8 \
  --det-window 16
```

Note: Kafka mode assumes brokers/topics are reachable from runtime paths used by gateway and server processes.

## 6. Phase Split (If Needed)

Use this when you want to stop after smoke:

1. Preflight + DB start + smoke only:

```bash
scripts/distributed/preflight_then_run_full.sh <same args as above> --skip-full
```

2. Full benchmark only:

```bash
scripts/distributed/run_distributed_benchmark.sh <same host/user args> --no-kafka 0|1
```

## 7. Expected Outputs

Artifacts are pulled locally under `scripts/bench_full_results/`:

1. `preflight_smoke_<timestamp>/` from smoke phase.
2. `distributed_<timestamp>/` from full phase.

Key files:

1. `results.csv`
2. `summary.csv`
3. `profiling_summary.csv`
4. Graph files in the same directory

## 8. Acceptance Criteria

Accept a run only when all are true:

1. Preflight checks pass.
2. Smoke benchmark completes and produces `summary.csv`.
3. Full benchmark completes and produces `results.csv` + `summary.csv`.
4. No invalid-case signatures in summary for accepted profile.
5. Result folder is local and complete (CSV + graph artifacts).

## 9. Fast Failure Guide

1. `nuraft_not_ok`:
   1. hard reset all nodes
   2. rerun preflight chain
2. `gateway_exit_-1` or timeout exits:
   1. check stale gateway process
   2. verify gateway reachability to PG and raft client ports
3. `restore_not_ok` / restore hangs:
   1. clear `.bench_tmp`
   2. restart remote Postgres cluster
4. `GLIBC_* not found` on node3:
   1. rebuild on that host
   2. or switch to fallback topology
5. parser-path fallback to compat in full run:
   1. expected when parser functions are unavailable on some node
   2. inspect remote probe errors before forcing throughput mode

## 10. Quick Verification Snippets

Check a full-run folder quickly:

```bash
OUT=scripts/bench_full_results/distributed_<timestamp>
python3 - <<'PY'
import csv
from pathlib import Path
p = Path("$OUT/summary.csv")
rows = list(csv.DictReader(p.open())) if p.exists() else []
print("rows", len(rows))
if rows:
    bad = [r for r in rows if r.get("valid_run_strict", "1") not in ("1", "true", "True")]
    print("strict_fail_rows", len(bad))
PY
```

## 11. Remote Sync Reminder

If local script behavior differs from remote results, sync first, rerun second. Remote execution under `/home/neel/Desktop/ariabc_cluster` can otherwise keep stale wrapper/harness code and produce misleading diagnostics.
