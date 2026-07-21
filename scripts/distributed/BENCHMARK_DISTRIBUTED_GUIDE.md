# AriaBC Distributed Overhead Benchmark — Complete Guide

## Overview

This guide describes how to run the 4-profile overhead benchmark that measures the incremental cost of each system layer in AriaBC:

| Profile | What it measures | Raft | Kafka | Bypass |
|---|---|---|---|---|
| `vanilla-pg` | Baseline: raw PostgreSQL, no AriaBC | No | No | — |
| `base-no-raft-no-kafka` | AriaBC deterministic execution only | No | No | — |
| `kafka-only-no-raft` | Det + Kafka result collection, Raft bypassed | No (bypassed) | Yes | `--bypassRaft 1` |
| `raft-kafka` | Full system: det + Raft consensus + Kafka | Yes | Yes | No |

Expected TPS ordering: `vanilla-pg` > `base-no-raft-no-kafka` > `kafka-only-no-raft` ≈ `raft-kafka`

Each profile runs 102 test cases (17 thread counts × 2 workloads × 3 repetitions).

---

## Cluster Topology

### Current (working) topology — 3 distinct machines

```
Node 1:  10.129.148.247          (user: neel)       — Ubuntu 24.04, BCDB postgres port 5438
Node 2:  10.129.148.248          (user: neel)       — Ubuntu 24.04, BCDB postgres port 5439
Node 3:  10.129.148.246  (user: neel)  — Ubuntu 24.04, BCDB postgres port 5440

Gateway: 10.129.148.247          (user: neel)       — also runs Kafka (port 9092) + ariabc_pg_gateway
```

All 3 Raft nodes are on **physically distinct machines**, and the gateway (240) also hosts Kafka. This gives clean performance isolation — no CPU contention between gateway+Kafka and PG node processes.

### Why not 229?

`10.129.148.248` — campus network firewall drops all inbound packets except SSH (port 22). Any attempt to connect to PG ports (5438–5440) or Kafka (9092) from other nodes returns `EAGAIN` (rc=11). Cannot be used as a PG node or Kafka host without firewall rule changes.

### Why 248 works

`10.129.148.248` — no inbound firewall. Preflight TCP check returns `ECONNREFUSED` (rc=111) when no service is listening, which means the host is network-reachable. PostgreSQL starts and runs normally.on plane TCP frame ariabc_pg_server (leader) Receive · append to Raft log ACCEPTED (n

### Network constraints

- All inter-node Raft and Postgres traffic goes over the campus network.
- Kafka must advertise `10.129.148.247:9092` (not `localhost`) so 27.54 and 248 can consume from it.
- SSH key authentication must work from the control machine to all three nodes:
  - `neel@10.129.148.247`
  - `neel@10.129.148.248`
  - `neel@10.129.148.246`

---

## Persistent Storage

All benchmark data is stored in **`/home/neel/Desktop/`**, which survives reboots on Linux (unlike `/tmp`, which may be cleared by `systemd-tmpfiles` at boot).

| Path | Purpose |
|---|---|
| `/home/neel/Desktop/ariabc_cluster/` | Repo root on each node: scripts, binaries, results |
| `/home/neel/Desktop/ariabc_install/` | BCDB-patched PostgreSQL install: `bin/`, `lib/`, `share/`, `include/` |
| `/home/neel/Desktop/kafka_2.13-3.7.0/` | Kafka on gateway (240) only |

> **Note:** if a node uses a non-`neel` account, use that user's Desktop path (for example `~/Desktop/...`) or pass explicit `--remote-repo-root` / `--remote-install-dir`.

---

## Prerequisites

### 1. SSH key access

From the control machine, passwordless SSH must work to all nodes:

```bash
SSH_KEY=/home/neel/.ssh/id_rsa
ssh -i $SSH_KEY neel@10.129.148.247 'echo ok'
ssh -i $SSH_KEY neel@10.129.148.248 'echo ok'
ssh -i $SSH_KEY neel@10.129.148.246 'echo ok'
```

### 2. Bootstrapping nodes (binaries + scripts)

The `bootstrap_nodes()` function in `run_overhead_distributed_4node.sh` handles all syncing automatically on every run. It:

1. Creates `/home/neel/Desktop/ariabc_cluster/` and `/home/neel/Desktop/ariabc_install/` on each node
2. `rsync`s the full BCDB install dir (`/work/ARIABC/install/`) to each node's `/home/neel/Desktop/ariabc_install/`
3. Copies both `ariabc_pg_server` **and** `ariabc_pg_gateway` binaries to each node

> **Critical:** Both binaries must be present on every Raft node. `preflight_cluster_checks.sh` probes for both `ariabc_pg_server` and `ariabc_pg_gateway` on every non-local host via `_pick_remote_binary_pair()`. If only the server is copied, the preflight fails with `"remote ariabc_pg binaries missing"`.

1. Copies all scripts and workload files
2. Downloads Kafka to the gateway if not already present

To run bootstrap manually (outside a full benchmark):

```bash
cd /work/ARIABC/AriaBC
# Source the script to get the bootstrap_nodes function, or just re-run the script
./scripts/distributed/run_overhead_distributed_4node.sh
# The script always bootstraps before running any profile.
```

Or to manually sync after rebuilding:

```bash
SSH_KEY=/home/neel/.ssh/id_rsa

for NODE in "neel@10.129.148.247" "neel@10.129.148.248" "neel@10.129.148.246"; do
  # Sync install dir (bin + lib + share + include — all required for initdb)
  rsync -az -e "ssh -i $SSH_KEY" /work/ARIABC/install/ "${NODE}:~/Desktop/ariabc_install/"

  # Sync both binaries (both required by preflight probe)
  rsync -az -e "ssh -i $SSH_KEY" \
    /work/ARIABC/AriaBC/ariabc_pg/build/bin/ariabc_pg_server \
    /work/ARIABC/AriaBC/ariabc_pg/build/bin/ariabc_pg_gateway \
    "${NODE}:~/Desktop/ariabc_cluster/ariabc_pg/build/bin/"

  # Sync scripts
  rsync -az --exclude='__pycache__' --exclude='*.pyc' --exclude='bench_full_results' \
    -e "ssh -i $SSH_KEY" \
    /work/ARIABC/AriaBC/scripts/ "${NODE}:~/Desktop/ariabc_cluster/scripts/"
done
```

> **`share/` is mandatory.** `initdb` reads `postgres.bki` from `share/postgresql/`. Syncing only `bin/` and `lib/` causes initdb to fail: `"file postgres.bki does not exist"`. Always sync the full `/work/ARIABC/install/` directory.

### 3. BCDB install directory at `/home/neel/Desktop/ariabc_install`

Verify on each node:

```bash
SSH_KEY=/home/neel/.ssh/id_rsa
for NODE in "neel@10.129.148.247" "neel@10.129.148.248" "neel@10.129.148.246"; do
  ssh -i $SSH_KEY $NODE \
    'LD_LIBRARY_PATH=/home/neel/Desktop/ariabc_install/lib /home/neel/Desktop/ariabc_install/bin/postgres --version'
done
# Expected: postgres (PostgreSQL) 13devel
```

Required structure:

```
/home/neel/Desktop/ariabc_install/bin/postgres
/home/neel/Desktop/ariabc_install/bin/initdb
/home/neel/Desktop/ariabc_install/bin/pg_ctl
/home/neel/Desktop/ariabc_install/bin/pg_isready
/home/neel/Desktop/ariabc_install/lib/librdkafka.so.1    (Ubuntu-version-matched)
/home/neel/Desktop/ariabc_install/share/postgresql/       ← REQUIRED: postgres.bki, etc.
/home/neel/Desktop/ariabc_install/include/                ← headers
```

### 4. Python runtime and psycopg2

Python 3.8+ and `psycopg2` must be available on every node.

```bash
SSH_KEY=/home/neel/.ssh/id_rsa
for NODE in "neel@10.129.148.247" "neel@10.129.148.248" "neel@10.129.148.246"; do
  ssh -i $SSH_KEY $NODE 'python3 -c "import psycopg2; print(psycopg2.__version__)"'
done
```

**If psycopg2 is missing (applies to 236, 248, 27.54):**

```bash
# Ubuntu 24.04 blocks pip install without --break-system-packages.
# Use this on any node where psycopg2 is missing:
ssh -i ~/.ssh/id_rsa neel@10.129.148.248 '
  python3 -m pip install --user --break-system-packages psycopg2-binary
  python3 -c "import psycopg2; print(psycopg2.__version__)"
'

# If pip itself is missing:
ssh -i ~/.ssh/id_rsa neel@10.129.148.246 '
  cd ~/Desktop
  curl -sS https://bootstrap.pypa.io/get-pip.py -o get-pip.py
  python3 get-pip.py --user --quiet --break-system-packages
  ~/.local/bin/pip install --user --quiet --break-system-packages psycopg2-binary
  python3 -c "import psycopg2; print(psycopg2.__version__)"
'
```

### 5. Kafka at `/home/neel/Desktop/kafka_2.13-3.7.0` (gateway only)

Required for profiles 3 and 4. `bootstrap_nodes()` downloads it automatically if absent.

```bash
# Check if present
ssh neel@10.129.148.247 'ls /home/neel/Desktop/kafka_2.13-3.7.0/bin/kafka-server-start.sh && echo ok'

# Manual download if needed
ssh neel@10.129.148.247 '
  cd ~/Desktop
  wget -q https://archive.apache.org/dist/kafka/3.7.0/kafka_2.13-3.7.0.tgz \
    || curl -sSL https://archive.apache.org/dist/kafka/3.7.0/kafka_2.13-3.7.0.tgz -o kafka_2.13-3.7.0.tgz
  tar xzf kafka_2.13-3.7.0.tgz
  echo "Kafka ready at /home/neel/Desktop/kafka_2.13-3.7.0"
'
```

Kafka requires Java. On 236, Java is at the system PATH (`/usr/bin/java`). The `ensure_kafka_ready()` function in the orchestrator script also patches `server.properties` to set `advertised.listeners=PLAINTEXT://10.129.148.247:9092` (required so 248 and 27.54 can connect).

### 6. SSH user map

The env var `ARIABC_SSH_USER_MAP` tells the benchmark python script which SSH user to use for each non-default host:

```bash
export ARIABC_SSH_USER_MAP="10.129.148.246=neel"
```

This is set automatically by `run_overhead_distributed_4node.sh`.

---

## How to Run: Full 4-Profile Benchmark

### One-shot run (all 4 profiles)

```bash
cd /work/ARIABC/AriaBC
./scripts/distributed/run_overhead_distributed_4node.sh
```

This script bootstraps all nodes, runs all 4 profiles in sequence, and produces a combined overhead analysis. Typical wall-clock time: **~3 hours** (~45 min per profile).

Output is written to:
```
scripts/bench_full_results/overhead_4node_<YYYYMMDD_HHMMSS>/
  vanilla-pg.log
  base-no-raft-no-kafka.log
  kafka-only-no-raft.log
  raft-kafka.log
  four_profile_tps_combined.png
  overhead_comparison.csv
```

Each profile's raw results also land in their own timestamped dirs:
```
scripts/bench_full_results/distributed_<YYYYMMDD_HHMMSS>/
  results.csv
  summary.csv
  profiling_summary.csv
  <plots>.png
```

### Profile-by-profile manual run

To run a single profile manually for debugging:

```bash
cd /work/ARIABC/AriaBC

export ARIABC_SSH_USER_MAP="10.129.148.246=neel"
export PROFILE_COMPARISON_PROFILE="vanilla-pg"
export PROFILE_NO_KAFKA=1
export PROFILE_KAFKA_HOME=""
export PROFILE_MODES="nondet"
export PROFILE_WAIT_MAJORITY=0
export PROFILE_SERVER_BYPASS_RAFT=0
export PROFILE_GW_BROADCAST_ALL=0
export PROFILE_CASE_TIMEOUT_S=60
export PROFILE_GATEWAY_TIMEOUT_S=60
export PROFILE_SRV_PG_EXEC_MODE=event
export PROFILE_GW_SUBMIT_MODE=blocking
export PROFILE_GW_DET_SUBMIT_PIPELINE=0
export PROFILE_ABORT_ON_INVALID_CASE=0
export PROFILE_DB_CONN_POOL_SIZE=4
export PROFILE_DB_CONN_POOL_CAP=4
export PROFILE_DET_WINDOW=16
export PROFILE_SKIP_SMOKE=1

./scripts/distributed/preflight_then_run_full.sh \
  --pg-hosts "10.129.148.247,10.129.148.248,10.129.148.246" \
  --pg-users "neel,neel,neel" \
  --raft-hosts "10.129.148.247,10.129.148.248,10.129.148.246" \
  --raft-users "neel,neel,neel" \
  --gateway-host "10.129.148.247" \
  --gateway-user "neel" \
  --ssh-user "neel" \
  --ssh-key "/home/neel/.ssh/id_rsa" \
  --remote-repo-root "/home/neel/Desktop/ariabc_cluster" \
  --remote-install-dir "/home/neel/Desktop/ariabc_install" \
  --bcdb-worker-counts 4,4,4 \
  --shared-buffers 512MB,512MB,512MB \
  --max-connections 300 \
  --bcdb-serial-gate-mode 1 \
  --bcdb-result-ring-slots 256 \
  --db-conn-pool-cap 4 \
  --db-conn-pool-size 4 \
  --det-window 16 \
  --comparison-profile "vanilla-pg"
```

---

## Profile Configuration Details

### Profile 1: vanilla-pg

Measures raw PostgreSQL performance — no AriaBC, no Raft, no Kafka.

```bash
PROFILE_MODES=nondet              # plain SQL, no BCDB sequence numbers
PROFILE_NO_KAFKA=1
PROFILE_WAIT_MAJORITY=0
PROFILE_SERVER_BYPASS_RAFT=0      # irrelevant (nondet never starts Raft server)
PROFILE_GW_BROADCAST_ALL=0
PROFILE_CASE_TIMEOUT_S=60
PROFILE_GATEWAY_TIMEOUT_S=60
```

The benchmark uses `DB_TYPE=2` (nondet mode) — the traffic loader sends plain `cur.execute(sql)` calls without BCDB sequence headers. The BCDB postgres binary is still used (no external vanilla PG needed), but with determinism disabled.

### Profile 2: base-no-raft-no-kafka

Measures AriaBC deterministic single-node execution only.

```bash
PROFILE_MODES=det
PROFILE_NO_KAFKA=1
PROFILE_WAIT_MAJORITY=0           # no Kafka, nothing to wait for
PROFILE_SERVER_BYPASS_RAFT=0
PROFILE_GW_BROADCAST_ALL=0
PROFILE_CASE_TIMEOUT_S=60
PROFILE_GATEWAY_TIMEOUT_S=60
```

Single-node: gateway sends to node 1 only. Transactions are processed in a serial window (det_window=16).

### Profile 3: kafka-only-no-raft

Measures deterministic execution + Kafka result collection, without Raft consensus. The gateway broadcasts to ALL nodes; each node executes in the same deterministic order; results are published to Kafka; the gateway waits for a majority of results to match.

```bash
PROFILE_MODES=det
PROFILE_NO_KAFKA=0
PROFILE_KAFKA_HOME=/home/neel/Desktop/kafka_2.13-3.7.0
PROFILE_WAIT_MAJORITY=1           # gateway waits for Kafka majority — critical for correct TPS
PROFILE_SERVER_BYPASS_RAFT=1      # server skips NuRaft; uses direct_enqueue() with monotonic seq
PROFILE_GW_BROADCAST_ALL=1        # gateway sends to all 3 nodes sequentially (total order)
PROFILE_CASE_TIMEOUT_S=180        # higher timeout because Kafka round-trip adds latency
PROFILE_GATEWAY_TIMEOUT_S=120
```

**Why `PROFILE_WAIT_MAJORITY=1` matters:** Without this, the gateway returns as soon as the first node ACKs `ACCEPTED` — measuring only local enqueue time, not the full Kafka result round-trip. With `wait_majority=1`, TPS reflects actual end-to-end committed throughput.

### Profile 4: raft-kafka (full system)

Measures the full AriaBC stack: deterministic execution + Raft consensus (total order via leader election) + Kafka result collection.

```bash
PROFILE_MODES=det
PROFILE_NO_KAFKA=0
PROFILE_KAFKA_HOME=/home/neel/Desktop/kafka_2.13-3.7.0
PROFILE_WAIT_MAJORITY=1           # gateway waits for Kafka majority
PROFILE_SERVER_BYPASS_RAFT=0      # full NuRaft consensus
PROFILE_GW_BROADCAST_ALL=0        # gateway sends to leader only; Raft replicates to followers
PROFILE_CASE_TIMEOUT_S=180
PROFILE_GATEWAY_TIMEOUT_S=120
```

---

## Script Reference

### `run_overhead_distributed_4node.sh`

**Location:** `scripts/distributed/run_overhead_distributed_4node.sh`

Top-level orchestrator for the 3-distinct-machine topology. Runs all 4 profiles end-to-end.

**Key responsibilities:**
1. Defines topology constants (hosts, users, SSH key, paths)
2. Calls `bootstrap_nodes()` to sync binaries and scripts to all nodes
3. Sets global benchmark knobs shared across all profiles
4. Calls `ensure_kafka_ready()` before profiles 3 and 4
5. Calls `run_one()` for each profile which invokes `preflight_then_run_full.sh`
6. After all 4 profiles, calls `plot_overhead_profiles_combined.py` and `compare_overhead_profiles.py`

**Topology section:**
```bash
PG_HOSTS="10.129.148.247,10.129.148.248,10.129.148.246"
PG_USERS="neel,neel,neel"
GW_HOST="10.129.148.247"
GW_USER="neel"
SSH_KEY="/home/neel/.ssh/id_rsa"
REMOTE_REPO_ROOT="/home/neel/Desktop/ariabc_cluster"
REMOTE_INSTALL_DIR="/home/neel/Desktop/ariabc_install"
KAFKA_HOME="/home/neel/Desktop/kafka_2.13-3.7.0"
KAFKA_BOOTSTRAP="10.129.148.247:9092"
export ARIABC_SSH_USER_MAP="10.129.148.246=neel"
```

**`bootstrap_nodes()` function:**

Loops over all 3 PG nodes and syncs:

- Full install dir (`/work/ARIABC/install/` → `/home/neel/Desktop/ariabc_install/`)
- Both server and gateway binaries to `/home/neel/Desktop/ariabc_cluster/ariabc_pg/build/bin/`
- All scripts and workloads (excluding `bench_full_results/`)
- Downloads Kafka to gateway if not present

**`ensure_kafka_ready()` function:**
SSHes to the gateway and:
1. Patches `server.properties` to set `advertised.listeners=PLAINTEXT://10.129.148.247:9092`
2. Checks if Kafka is already running via `kafka-topics.sh --list`
3. If not: generates cluster UUID, formats KRaft storage, starts in daemon mode
4. Polls up to 60 seconds for the broker to become ready

**`run_one()` function:**
```bash
run_one <profile_name> <no_kafka=0|1> <kafka_home_path> [modes=det|nondet]
```
Syncs the latest `bench_nuraft_kafka_matrix.py` to the gateway, then calls `preflight_then_run_full.sh` with all topology and profile args. Extracts the `Local out dir` from the log, validates outputs via `assert_profile_outputs()`, and returns the path.

**`assert_profile_outputs()` function:**
Validates that a completed run produced:
- A non-empty `summary.csv` (> 1 line)
- A non-empty `results.csv` (> 1 line)
- The log contains the string `"Benchmark completed. Artifacts:"`
- The output directory mtime is newer than when the run started

---

### `preflight_then_run_full.sh`

**Location:** `scripts/distributed/preflight_then_run_full.sh`

4-step pipeline for one profile run. Called by the orchestrator via `run_one()`.

```
Step 1/4: preflight_cluster_checks.sh      — SSH, binary, TCP checks
Step 2/4: start_remote_postgres_cluster.sh  — start BCDB postgres on each node
Step 3/4: preflight_smoke_benchmark.sh     — quick smoke test (skipped when PROFILE_SKIP_SMOKE=1)
Step 4/4: run_distributed_benchmark.sh    — full benchmark across all thread counts
```

**Configuration via environment variables** (all `PROFILE_` prefixed):

| Variable | 4node script value | Description |
|---|---|---|
| `PROFILE_BCDB_WORKER_COUNTS` | `4,4,4` | Worker threads per PG node |
| `PROFILE_SHARED_BUFFERS` | `512MB,512MB,512MB` | Shared buffers per PG node |
| `PROFILE_MAX_CONNECTIONS` | `300` | Max PG connections per node |
| `PROFILE_BCDB_SERIAL_GATE_MODE` | `1` | Serial gate mode for det execution |
| `PROFILE_BCDB_RESULT_RING_SLOTS` | `256` | Result ring buffer size |
| `PROFILE_DB_CONN_POOL_CAP` | `4` | Gateway connection pool cap |
| `PROFILE_DB_CONN_POOL_SIZE` | `4` | Gateway connection pool size |
| `PROFILE_DET_WINDOW` | `16` | Deterministic execution window (concurrent txns) |
| `PROFILE_KAFKA_HOME` | varies by profile | Path to Kafka on gateway |
| `PROFILE_KAFKA_BOOTSTRAP` | `10.129.148.247:9092` | Kafka bootstrap address |
| `PROFILE_NO_KAFKA` | `1` or `0` | Skip Kafka (1=no Kafka, 0=use Kafka) |
| `PROFILE_WAIT_MAJORITY` | `0` or `1` | Gateway waits for Kafka majority |
| `PROFILE_SERVER_BYPASS_RAFT` | `0` or `1` | Server skips NuRaft |
| `PROFILE_GW_BROADCAST_ALL` | `0` or `1` | Gateway sends to all nodes |
| `PROFILE_GATEWAY_TIMEOUT_S` | `60` or `120` | Gateway-level request timeout |
| `PROFILE_CASE_TIMEOUT_S` | `60` or `180` | Per-test-case timeout |
| `PROFILE_GW_SUBMIT_MODE` | `blocking` | Gateway submit mode |
| `PROFILE_GW_DET_SUBMIT_PIPELINE` | `0` | Pipelining for det submits |
| `PROFILE_SRV_PG_EXEC_MODE` | `event` | Server PG execution mode |
| `PROFILE_SKIP_SMOKE` | `1` | Skip smoke test (1=skip) |
| `PROFILE_COMPARISON_PROFILE` | varies | Profile name tag in output CSV |

---

### `preflight_cluster_checks.sh`

**Location:** `scripts/distributed/preflight_cluster_checks.sh`

Runs 5 checks before any benchmark work starts. Fails fast if anything is wrong.

**Check 1 — SSH reachability:**
Tests all unique hosts (pg + raft + gateway, deduplicated). Fails on timeout.

**Check 2 — Repo files:**
Checks that `bench_nuraft_kafka_matrix.py` and both YCSB workload files exist at `$REMOTE_REPO_ROOT/scripts/` on every node.

**Check 3 — Binaries:**

- Raft hosts: checks both `ariabc_pg_server` AND `ariabc_pg_gateway` are executable via `_pick_remote_binary_pair()`. **Both binaries must be present on every Raft host.**
- Gateway host: checks `ariabc_pg_gateway` is executable.
- All hosts: checks `$REMOTE_INSTALL_DIR/bin/initdb`, `pg_ctl` are executable; Python 3 is available.

**Check 4 — Gateway → PG TCP:**

From the gateway, attempts TCP connect to each PG node:port.

- `rc=0`: listener is up (PG already running) — OK
- `rc=111` or `rc=61`: connection refused (host reachable, no listener) — **OK, expected before PG starts**
- `rc=11` (EAGAIN/timeout): host unreachable or firewalled — **FAILS** — this is why 229 cannot be used

**Check 5 — Disk free:**
Prints `df -h` for the repo root on each host. Informational only — does not fail.

---

### `start_remote_postgres_cluster.sh`

**Location:** `scripts/distributed/start_remote_postgres_cluster.sh`

SSHes to each of the 3 PG nodes and starts BCDB postgres.

**Binary selection (per node):**
1. First tries `$REMOTE_INSTALL_DIR/bin/postgres` — the BCDB custom build
2. Falls back to `/usr/lib/postgresql/*/bin/postgres` — system postgres
3. Fails if neither works

**Per-node configuration written to `bench_auto.conf`:**
```
port = 5438/5439/5440         (node index + DB_PORT_BASE)
listen_addresses = '*'
max_connections = 300
shared_buffers = 512MB/512MB/512MB  (uniform across all 3 nodes)
synchronous_commit = off       (maximize throughput — python bench driver overrides to 'on' in local mode only; distributed mode uses these settings)
fsync = off
full_page_writes = off
wal_level = replica
# BCDB-specific (only when using custom binary):
bcdb_worker_count = 4/4/4
bcdb_serial_gate_mode = 1
bcdb_result_ring_slots = 256
```

> **Note on durability settings:** `fsync=off` and `synchronous_commit=off` are intentional for benchmark throughput. In distributed mode, the bench python script does NOT override these — it only verifies postgres is running and starts its own processes in local mode. The benchmark is measuring Raft+Kafka replication overhead, not disk durability.

`bench_auto.conf` is included via `include_if_exists` in `postgresql.conf` so it doesn't permanently modify the cluster.

`pg_hba.conf` gets `host all all 0.0.0.0/0 trust` appended (passwordless connections from any IP — lab use).

**Data directory:** `$REMOTE_REPO_ROOT/.bench_tmp/pgdata/<timestamp>/node{1,2,3}/`

---

### `run_distributed_benchmark.sh`

**Location:** `scripts/distributed/run_distributed_benchmark.sh`

The innermost benchmark runner. SSHes to the gateway and runs `bench_nuraft_kafka_matrix.py` there, then pulls results back to the local machine.

**Step-by-step:**

1. Resolves host/user arrays from comma-separated arguments
2. SSHes to gateway and runs the python script with all parameters:

```bash
ssh neel@10.129.148.247 "
  cd ~/Desktop/ariabc_cluster
  export ARIABC_SSH_USER_MAP='10.129.148.246=neel'
  python3 scripts/bench_nuraft_kafka_matrix.py \
    --distributed \
    --nodes 3 \
    --pg-hosts 10.129.148.247:5438,10.129.148.248:5439,10.129.148.246:5440 \
    --raft-hosts 10.129.148.247:5430,10.129.148.248:5431,10.129.148.246:5432 \
    --gateway-host 10.129.148.247 \
    --remote-repo-root /home/neel/Desktop/ariabc_cluster \
    --installDir /home/neel/Desktop/ariabc_install \
    --kafkaBootstrap 10.129.148.247:9092 \
    --kafkaHome /home/neel/Desktop/kafka_2.13-3.7.0 \
    --no-kafka 0 \
    --waitMajority 1 \
    --bypassRaft 0 \
    --broadcastToAll 0 \
    --comparison-profile raft-kafka \
    --threads 1,4,8,12,16,20,24,28,32,36,40,44,48,52,56,60,64 \
    --runs 3 \
    --mode det ...
"
```

1. Greps SSH output for `"Remote out dir : /path"` to find remote results
2. `rsync`s remote result dir to local `scripts/bench_full_results/distributed_<timestamp>/`
3. Validates local results: checks for non-empty `summary.csv` and `results.csv`

---

### `bench_nuraft_kafka_matrix.py`

**Location:** `scripts/bench_nuraft_kafka_matrix.py` (run on gateway host)

The main benchmark driver (~6500 lines). Manages the full lifecycle of each test case.

**Key responsibilities per test case:**
1. Start `ariabc_pg_server` processes on each Raft node (via SSH)
2. Start `ariabc_pg_gateway` process on the gateway
3. Wait for postgres to be ready (`pg_isready` probe)
4. In `det` mode (unless `--bypassRaft`): wait for NuRaft leader election (`_wait_nuraft_accepting`)
5. Run the traffic loader (Python threads submitting transactions to the gateway)
6. Collect TPS, latency percentiles, validity metrics (row count, Merkle hash verification)
7. Stop gateway and server processes
8. Write per-case row to `results.csv` and aggregate to `summary.csv`

**`--bypassRaft` flag:**
When `1`, the server skips NuRaft initialization entirely and uses `direct_enqueue()` with a monotonically increasing sequence number. No Raft leader election needed — the `_wait_nuraft_accepting` probe is skipped. The gateway broadcasts to all nodes to establish total order.

**`--waitMajority` flag:**
When `1`, the gateway's `wait_majority` lambda returns `true`, causing `submit_to_cluster()` to block until `vote_store` has received matching results from a majority of Kafka consumers. Required for correct end-to-end throughput measurement in profiles 3 and 4.

**Thread counts and workloads:**
```
Threads: 1,4,8,12,16,20,24,28,32,36,40,44,48,52,56,60,64  (17 points)
Workloads:
  ycsbtx-skew-01-24k-pt-intkey-sid-clean-20k.txt          (skew 0.1, transactional)
  ycsb-skew0-99-tx-20k-point-safedb-intkey-insert12k-uniq.txt  (skew 0.99, high contention)
Repetitions per (thread, workload): 3
Total cases: 17 × 2 × 3 = 102
```

---

## Output Structure and Interpretation

### Result files

After a successful 4-profile run via `run_overhead_distributed_4node.sh`:
```
scripts/bench_full_results/overhead_4node_<timestamp>/
  vanilla-pg.log                    — full run log for profile 1
  base-no-raft-no-kafka.log
  kafka-only-no-raft.log
  raft-kafka.log
  four_profile_tps_combined.png     — TPS vs threads: all 4 profiles overlaid
  overhead_comparison.csv           — computed overhead percentages

scripts/bench_full_results/distributed_<timestamp_p1>/  — raw profile 1 results
scripts/bench_full_results/distributed_<timestamp_p2>/  — raw profile 2 results
scripts/bench_full_results/distributed_<timestamp_p3>/  — raw profile 3 results
scripts/bench_full_results/distributed_<timestamp_p4>/  — raw profile 4 results
```

### Key columns in `results.csv`

| Column | Description |
|---|---|
| `thread_count` | Concurrent client threads |
| `workload` | Workload file name |
| `run_id` | Repetition index (1–3) |
| `tps` | Transactions per second |
| `p50_ms`, `p95_ms`, `p99_ms` | Latency percentiles |
| `valid_run` | 1 if run passed validity checks, 0 if not |
| `row_count_all_nodes_equal` | 1 if all 3 PG nodes have the same row count |
| `merkle_verify_all` | true if Merkle root hashes match across nodes |
| `divergence_count` | Number of divergent transactions (must be 0) |
| `permanent_failures` | Transactions never applied (must be 0) |

### Key columns in `summary.csv`

| Column | Description |
|---|---|
| `thread_count` | Concurrent client threads |
| `workload` | Workload file name |
| `mean_tps` | Average TPS across 3 repetitions |
| `max_tps` | Peak TPS across repetitions |
| `p95_ms_mean` | Average P95 latency |

### Overhead analysis (`overhead_comparison.csv`)

Produced by `compare_overhead_profiles.py`:

```
BCDB det overhead      = (vanilla_pg_tps - det_only_tps) / vanilla_pg_tps × 100%
Kafka overhead         = (det_only_tps - kafka_only_tps) / det_only_tps × 100%
Full replication overhead = (det_only_tps - raft_kafka_tps) / det_only_tps × 100%
Raft-only overhead (derived) = full_replication_overhead - kafka_overhead
```

### Reference benchmark numbers (4-node run, 2026-04-07)

```
vanilla-pg (baseline)      : 19,351.7 TPS
base-no-raft (det-only)    : 18,405.8 TPS    BCDB overhead = 4.9%
kafka-only-no-raft         :  1,293.9 TPS    Kafka overhead = 93.0%
raft-kafka (full system)   :  1,423.7 TPS    Raft adds negligible overhead over Kafka path
Total system overhead      : 92.6% vs vanilla-pg
```

Key takeaway: **Kafka result collection (93%) dominates the overhead**; Raft consensus adds negligible cost on top.

---

## Common Failure Modes and Fixes

### `tcp_unreachable_10.129.148.248:XXXX_rc=11` (preflight fails)

**Cause:** `10.129.148.248` is DROP-firewalled on all inbound ports except SSH 22. TCP connections hang (rc=11 = EAGAIN/timeout). Cannot be used as a PG node or Kafka host.

**Resolution:** 229 is not in the current topology. Do not add it.

### `remote ariabc_pg binaries missing on host=...`

**Cause:** `preflight_cluster_checks.sh` check 3 calls `_pick_remote_binary_pair()` which probes BOTH `ariabc_pg_server` and `ariabc_pg_gateway` on every Raft host. If the gateway binary is missing on a non-gateway node, this fails.

**Fix:** Re-run `bootstrap_nodes()` (just run the 4node script — it always bootstraps first), or manually sync:
```bash
SSH_KEY=/home/neel/.ssh/id_rsa
rsync -az -e "ssh -i $SSH_KEY" \
  /work/ARIABC/AriaBC/ariabc_pg/build/bin/ariabc_pg_server \
  /work/ARIABC/AriaBC/ariabc_pg/build/bin/ariabc_pg_gateway \
  "neel@10.129.148.246:~/Desktop/ariabc_cluster/ariabc_pg/build/bin/"
```

### `initdb` fails: `"file postgres.bki does not exist"`

**Cause:** The install dir sync only included `bin/` and `lib/`, missing `share/`.

**Fix:**
```bash
rsync -az -e "ssh -i ~/.ssh/id_rsa" /work/ARIABC/install/ \
  neel@10.129.148.247:~/Desktop/ariabc_install/
```

Always sync the full `/work/ARIABC/install/` directory.

### `ariabc_pg_server --help` fails: `"librdkafka.so.1: cannot open shared object file"`

**Cause:** `librdkafka.so.1` is in `/home/neel/Desktop/ariabc_install/lib/`, not in the system library path.

The orchestration scripts set `LD_LIBRARY_PATH` automatically. To test manually:
```bash
LD_LIBRARY_PATH=/home/neel/Desktop/ariabc_install/lib \
  /home/neel/Desktop/ariabc_cluster/ariabc_pg/build/bin/ariabc_pg_server --help
```

### `psycopg2` missing (Ubuntu 24.04 on any node)

**Symptom:** `ModuleNotFoundError: No module named 'psycopg2'`

**Fix (no sudo required):**
```bash
ssh -i ~/.ssh/id_rsa neel@10.129.148.248 \
  'python3 -m pip install --user --break-system-packages psycopg2-binary'
```

For Ubuntu 22.04 nodes (may need pip installed first):
```bash
ssh -i ~/.ssh/id_rsa neel@10.129.148.246 '
  cd ~/Desktop
  curl -sS https://bootstrap.pypa.io/get-pip.py -o get-pip.py
  python3 get-pip.py --user --quiet --break-system-packages
  ~/.local/bin/pip install --user --quiet --break-system-packages psycopg2-binary
'
```

### Kafka won't start (UUID generation fails)

**Symptom:** `ensure_kafka_ready` fails with `ERROR: failed to generate Kafka cluster ID`

**Cause:** `java` not in PATH for non-login SSH session.

```bash
ssh neel@10.129.148.247 'java -version'
# If not found, check:
ssh neel@10.129.148.247 'which java || ls /usr/bin/java'
```

### `_wait_nuraft_accepting` hangs / NuRaft never elects leader

**Profile 3:** `--bypassRaft 1` must be set — bypass mode never starts NuRaft. Verify `PROFILE_SERVER_BYPASS_RAFT=1` is exported before `run_one`.

**Profile 4:** Raft ports (5430–5432) may not be reachable between nodes. Verify:
```bash
# From 240, can it reach 248's Raft port?
python3 -c "import socket; s=socket.socket(); s.settimeout(3); rc=s.connect_ex(('10.129.148.248',5431)); print(rc)"
# rc=111 (connection refused) = OK (PG not started yet), rc=0 = connected
```

### librdkafka GLIBC version mismatch

**Symptom:** `ariabc_pg_server` fails with `GLIBC_2.38 not found` on a node running Ubuntu 22.04.

**Cause:** `librdkafka.so.1` was compiled for Ubuntu 24.04 (GLIBC 2.38) but the node runs Ubuntu 22.04 (GLIBC 2.35). Current Ubuntu 24.04 nodes (236, 248) are not affected; Ubuntu 22.04 nodes (27.54, 179) may be.

**Fix if encountered on a different node:**
```bash
wget http://archive.ubuntu.com/ubuntu/pool/universe/libr/librdkafka/librdkafka1_1.8.0-1build1_amd64.deb
dpkg-deb -x librdkafka1_1.8.0-1build1_amd64.deb /home/neel/Desktop/rdkafka_extract
cp /home/neel/Desktop/rdkafka_extract/usr/lib/x86_64-linux-gnu/librdkafka.so.1 /home/neel/Desktop/ariabc_install/lib/
```

### Stale postgres processes from a previous run

**Symptom:** `pg_ctl start` fails because a previous instance is still listening on the port.

`start_remote_postgres_cluster.sh` handles this automatically (runs `pg_ctl stop -m fast` first). If that fails:

```bash
SSH_KEY=/home/neel/.ssh/id_rsa
ssh -i $SSH_KEY neel@10.129.148.247 'pkill -f postgres || true; sleep 2'
ssh -i $SSH_KEY neel@10.129.148.248 'pkill -f postgres || true; sleep 2'
ssh -i $SSH_KEY neel@10.129.148.246 'pkill -f postgres || true; sleep 2'
```

### Profile reports 0 valid runs or `divergence_count > 0`

Possible causes:
1. `wait_majority=0` for a Kafka profile → gateway returned before results were published → consistency check fails
2. NuRaft was not elected leader before transactions started
3. Wrong `--bypassRaft` setting for the profile (e.g., set to 1 for profile 4)

Check the individual run log in `scripts/bench_full_results/overhead_4node_<ts>/` for error messages.

---

## Checking and Stopping Benchmark Processes

### Check for running benchmark processes

```bash
SSH_KEY=/home/neel/.ssh/id_rsa

ssh -i $SSH_KEY neel@10.129.148.247 \
  'pgrep -a -f "ariabc_pg_server|ariabc_pg_gateway|bench_nuraft" || echo none'
ssh -i $SSH_KEY neel@10.129.148.248 \
  'pgrep -a -f "ariabc_pg_server|ariabc_pg_gateway|bench_nuraft" || echo none'
ssh -i $SSH_KEY neel@10.129.148.246 \
  'pgrep -a -f "ariabc_pg_server|bench_nuraft" || echo none'

# On control machine
pgrep -a -f "preflight_then_run|run_distributed|bench_nuraft|run_overhead_distributed" || echo none
```

### Stop all benchmark processes

```bash
SSH_KEY=/home/neel/.ssh/id_rsa

ssh -i $SSH_KEY neel@10.129.148.247 \
  'pkill -f "ariabc_pg_server|ariabc_pg_gateway|bench_nuraft" || true; pkill -f postgres || true'
ssh -i $SSH_KEY neel@10.129.148.248 \
  'pkill -f "ariabc_pg_server|ariabc_pg_gateway|bench_nuraft" || true; pkill -f postgres || true'
ssh -i $SSH_KEY neel@10.129.148.246 \
  'pkill -f "ariabc_pg_server|bench_nuraft" || true; pkill -f postgres || true'

# On control machine
pkill -f "preflight_then_run|run_distributed_benchmark|run_overhead_distributed" || true
```

---

## Quick Reference

### Run full 4-profile benchmark
```bash
cd /work/ARIABC/AriaBC && ./scripts/distributed/run_overhead_distributed_4node.sh
```

### Verify nodes are ready (preflight only)

```bash
cd /work/ARIABC/AriaBC
./scripts/distributed/preflight_cluster_checks.sh \
  --pg-hosts "10.129.148.247,10.129.148.248,10.129.148.246" \
  --pg-users "neel,neel,neel" \
  --raft-hosts "10.129.148.247,10.129.148.248,10.129.148.246" \
  --raft-users "neel,neel,neel" \
  --gateway-host "10.129.148.247" --gateway-user "neel" \
  --ssh-key "/home/neel/.ssh/id_rsa" \
  --remote-repo-root "/home/neel/Desktop/ariabc_cluster" \
  --remote-install-dir "/home/neel/Desktop/ariabc_install"
# Expected last line: "Preflight checks PASSED."
```

### Manual spot checks

```bash
SSH_KEY=/home/neel/.ssh/id_rsa

# SSH reachability
ssh -i $SSH_KEY neel@10.129.148.247 'echo ok'
ssh -i $SSH_KEY neel@10.129.148.248 'echo ok'
ssh -i $SSH_KEY neel@10.129.148.246 'echo ok'

# Postgres version (needs LD_LIBRARY_PATH)
for NODE in "neel@10.129.148.247" "neel@10.129.148.248" "neel@10.129.148.246"; do
  ssh -i $SSH_KEY $NODE \
    'LD_LIBRARY_PATH=/home/neel/Desktop/ariabc_install/lib /home/neel/Desktop/ariabc_install/bin/postgres --version'
done

# Both binaries present on each node
for NODE in "neel@10.129.148.247" "neel@10.129.148.248" "neel@10.129.148.246"; do
  ssh -i $SSH_KEY $NODE \
    'ls /home/neel/Desktop/ariabc_cluster/ariabc_pg/build/bin/{ariabc_pg_server,ariabc_pg_gateway} && echo binaries: OK'
done

# --bypassRaft supported
for NODE in "neel@10.129.148.247" "neel@10.129.148.248" "neel@10.129.148.246"; do
  ssh -i $SSH_KEY $NODE \
    'LD_LIBRARY_PATH=/home/neel/Desktop/ariabc_install/lib /home/neel/Desktop/ariabc_cluster/ariabc_pg/build/bin/ariabc_pg_server --help 2>&1 | grep bypassRaft'
done

# psycopg2 present
for NODE in "neel@10.129.148.247" "neel@10.129.148.248" "neel@10.129.148.246"; do
  ssh -i $SSH_KEY $NODE 'python3 -c "import psycopg2; print(psycopg2.__version__)"'
done

# Kafka on gateway
ssh -i $SSH_KEY neel@10.129.148.247 \
  '/home/neel/Desktop/kafka_2.13-3.7.0/bin/kafka-topics.sh --bootstrap-server 10.129.148.247:9092 --list'
```
