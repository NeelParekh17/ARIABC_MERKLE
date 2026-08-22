# Distributed Benchmark Sweep Harness (`run_sweep.sh`)

This document details `scripts/distributed/run_sweep.sh`, the automated campaign wrapper that executes distributed PostgreSQL/executor worker sweeps across the configured 4-node AriaBC Raft-Kafka cluster.

---

## 🌟 What the Script Does

`run_sweep.sh` automates multi-node distributed performance sweeps by wrapping `scripts/distributed/run_4node_raft_cluster.sh`.

It performs the following steps:

1. Changes working directory to repository root (`/work/ARIABC/AriaBC`).
2. Builds updated C++ server (`ariabc_pg_server`) and gateway (`ariabc_pg_gateway`) binaries.
3. Performs syntax validation on `run_4node_raft_cluster.sh` (`bash -n`).
4. Runs `git diff --check` to ensure no workspace formatting errors exist.
5. Initializes a campaign output directory under `scripts/bench_full_results/`.
6. Iterates over specified server executor worker counts (`--executor-workers`).
7. Repeats each worker count configuration for designated repetitions (`--reps`).
8. Collects benchmark artifacts (`cluster4_*`), CSV summaries, and profile logs.

All stdout/stderr output is logged to `out.txt` in the repository root.

---

## 🚀 Usage & Quick Start

### Basic Campaign Execution

From the repository root:

```bash
# Run default distributed sweep
./scripts/distributed/run_sweep.sh
```

### View Help & Options

```bash
./scripts/distributed/run_sweep.sh --help
```

### Monitor Sweep Progress

```bash
tail -f out.txt
```

---

## 📁 Campaign Output Directory Structure

At the end of a run, the campaign location is logged to `out.txt`:

```text
Artifacts: scripts/bench_full_results/pg_executor_sweep_<timestamp>/
```

Inside the campaign directory:

```text
runs.csv      # Maps pg_executor_workers, repetition ID, and artifact paths
run_dirs.txt  # List of produced cluster4_* artifact directories
summary.csv   # Aggregated TPS and latency metrics from summarize_raft_profile.py
campaign.env  # Environment snapshot and flags used for the campaign
```

Individual run artifacts are stored in `scripts/bench_full_results/cluster4_*`.

---

## 🌐 Cluster Topology Configuration

`run_sweep.sh` sources topology parameters from `scripts/distributed/cluster_topology.sh`:

```bash
declare -a NODE_IDS=(1 2 4)
declare -a NODE_IPS=(10.129.148.247 10.129.148.246 10.129.148.248)
declare -a NODE_NAMES=(admin123 user4 utkarsh)
declare -a NODE_USERS=(neel neel neel)
declare -a NODE_IS_U22=(0 1 0)
declare -a NODE_CLIENT_PORTS=(8000 8000 8001)

export RAFT_PORT=9000
export DB_PORT=5438
export DB_USER=postgres
export DB_NAME=postgres
```

> [!NOTE]
> `NODE_IS_U22` specifies OS binary paths: Ubuntu 22.04 nodes use `/home/neel/Desktop/ariabc_pg_build_u22` binaries, while Ubuntu 24.04 nodes use synced `/home/neel/Desktop/ariabc_cluster/ariabc_pg/build` binaries.

---

## 🛠️ CLI Topology & Custom Sweep Flags

Topology parameters can be overridden directly from the command line:

```bash
./scripts/distributed/run_sweep.sh \
  --node-ids 1,2,3 \
  --node-ips 10.10.0.11,10.10.0.12,10.10.0.13 \
  --node-names node-a,node-b,node-c \
  --node-users neel,neel,neel \
  --node-is-u22 0,0,1 \
  --node-client-ports 8000,8000,8001 \
  --kafka-host 10.10.0.11
```

### Customizing Executor Workers & Repetitions

```bash
# Sweep specific worker counts with 3 repetitions
./scripts/distributed/run_sweep.sh --executor-workers 8,16,24,32 --reps 3

# Fast 1-pass smoke sweep
./scripts/distributed/run_sweep.sh --executor-workers 8 --reps 1
```

### Customizing Client Threads & Ordering Policy

```bash
# Set client thread count and worker lanes
./scripts/distributed/run_sweep.sh --threads 96 --det-client-workers 96

# Use preassigned ordering for 100% deterministic, repeatable Merkle root hashes across all runs
./scripts/distributed/run_sweep.sh --raft-ordering-policy preassigned
```

---

## ✅ Post-Sweep Verification

After completing a distributed benchmark campaign, verify replica state consistency and Merkle tree root alignment across all cluster nodes:

```bash
./scripts/distributed/test_merkle_consistency.sh
```

Ensure output reports `divergence_count=0` and `permanent_failures=0`.
