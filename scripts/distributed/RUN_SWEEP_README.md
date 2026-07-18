# `run_sweep.sh` distributed benchmark README

This file documents `scripts/distributed/run_sweep.sh`, the campaign wrapper that
runs the current distributed PG/executor worker sweep across the configured
AriaBC cluster.

## What the script does

`run_sweep.sh` is a wrapper around `scripts/distributed/run_4node_raft_cluster.sh`.
It:

1. changes to `/work/ARIABC/AriaBC`;
2. builds `ariabc_pg_gateway` and `ariabc_pg_server`;
3. runs `bash -n scripts/distributed/run_4node_raft_cluster.sh`;
4. runs `git diff --check`;
5. creates a campaign directory under `scripts/bench_full_results/`;
6. runs the distributed benchmark for each server executor worker count;
7. runs each worker count once by default;
8. runs an untimed warm-up before each measured workload;
9. runs optional untimed distributed merge/re-split/key-move and pending-crash
   gates when enabled;
10. performs post-run marker/Merkle equality after TPS measurement;
11. records each produced `cluster4_*` artifact and summary.

All stdout/stderr streams to the terminal and is also captured in `out.txt` and
the campaign's `console.log`.

## Basic run

From the repository root:

```bash
./run_sweep.sh
```

`./run_sweep.sh` is a repository-root convenience wrapper for the documented
`scripts/distributed/run_sweep.sh` implementation. The latter can also be
invoked directly.

The lab password defaults to the configured cluster credential and can be
overridden with `ARIABC_CLUSTER_PASSWORD`. SSH uses strict host-key checking
and the managed file selected by `ARIABC_KNOWN_HOSTS_FILE` (default:
`~/.ssh/known_hosts`). The wrapper defaults
`ARIABC_ALLOW_DESTRUCTIVE_BENCHMARK_RESET=1` because it restores and resets a
dedicated disposable benchmark database; set it to `0` to refuse the run. Never
point this runner at a database containing production data.

The wrapper defaults to the distributed dynamic-Merkle path:

```text
--restore-sql scripts/distributed/sql/restore_usertable_small_dynamic.sql
--verify-table usertable_small
--enable-merkle-index 1
--merkle-verify-mode dynamic
--dynamic-index public.usertable_small_dynamic_merkle_idx
--dynamic-structure-gate 0
--dynamic-structure-crash-gate 0
--warmup-queries 1000
--kafka-completion-mode majority_async_all3
```

Structure and crash gates are available but disabled by default to keep the
normal throughput sweep bounded. Enable them explicitly when those transition
checks are part of the acceptance run.

Each run has these boundaries:

1. `gateway_warmup.log`: 1,000 state-preserving no-op updates by default;
2. `gateway_test.log`: the timed workload used by `parse_tps_metrics.py`;
3. `dynamic_structure_gateway.log`: untimed merge/re-split/key-route coverage,
   if `--dynamic-structure-gate 1` is enabled;
4. a pending-transition replica crash/restart gate, if
   `--dynamic-structure-crash-gate 1` is enabled;
5. `post_verify_marker_gateway.log` plus dynamic Merkle equality artifacts.

Warm-up and post-run verification are excluded from TPS. Set
`--warmup-queries 0` to disable warm-up, or use `--warmup-workload FILE` for a
custom state-safe warm-up workload.

Passing any of these options explicitly overrides its default because explicit
CLI arguments are forwarded after the default argument set.

Show supported sweep and topology flags:

```bash
./scripts/distributed/run_sweep.sh --help
```

Watch progress in another terminal:

```bash
tail -f out.txt
```

## What “Raft-majority result-completion TPS” means

The sweep hard-codes `--kafka-completion-mode majority_async_all3`. There are
two different quorum boundaries:

1. **Raft replication/commit quorum:** the ordered Raft entry must be replicated
   to the Raft majority (2 of 3 in the normal cluster) before Raft considers it
   committed and applies it to the state machine. This is still required.
2. **Client result quorum:** the gateway's `wait_any_majority` waits for any
   majority of replica result records. For the normal three-node cluster this
   is 2 of 3; custom topologies use `floor(node_count/2)+1`. No particular
   follower is preferred.

Therefore the normal path is: submit to the Raft leader → Raft majority commit
→ every healthy replica applies the committed entry → the gateway waits for any
majority of replica result records → return the client response. The remaining
replica result and Merkle/hash validation continues asynchronously; a lagging
healthy replica still executes the entry and is checked by post-run validation.
A transaction is counted as complete when the majority result boundary returns
the client response. The measured TPS is therefore:

```text
Raft-majority result-completion TPS
  = timed workload transaction count / (overall_wall_ms / 1000)
```

`overall_wall_ms` is the wall-clock duration of the measured workload in
`gateway_test.log` (also reported as `overall time taken (millisec)`). Warm-up
is excluded, and marker verification, replica drain, Kafka hash validation, and
Merkle equality are not added to this timed interval. The campaign records the
contract as
`tps_semantics=raft_majority_result_completion_async_all3_validation` in
`campaign.env`.

This answers “how quickly did the client receive responses after Raft commit,
once any replica majority had returned results?” It is not all-three-replica
drained/audit TPS and is not measured before commit. A high TPS is accepted
only with the required
post-run marker/Merkle equality proof, input-freeze proof, warm-up separation,
latency samples, and any explicitly enabled structure/crash gates. For a
all-three synchronous timing experiment, use the lower-level
`run_4node_raft_cluster.sh` with strict majority mode and label that result
separately; `run_sweep.sh` intentionally fixes its mode for comparable sweeps.

## Command-line options

All options below are parsed by `run_sweep.sh` and forwarded to the cluster
runner where noted. Lists accept commas or quoted spaces, for example `1,2,4`
or `"1 2 4"`.

### Sweep and client sizing

| Option | Default | Effect |
| --- | --- | --- |
| `--threads N` | `96` | Deterministic client lanes and, unless overridden, gateway deterministic workers. |
| `--det-client-workers N` | same as `--threads` | Gateway deterministic thread-pool workers; allows lanes and workers to differ. |
| `--executor-workers LIST` | `1 2 4 8 12 16` | Server executor values to sweep. Each value is also passed as `--server-pg-connections`; the runner requires the pair to match. |
| `--reps LIST` | `1` | Repetition labels for every executor value. Repetitions alternate sweep order to reduce order bias. |

### Dynamic Merkle and verification

| Option | Default | Effect |
| --- | --- | --- |
| `--restore-sql FILE` | `scripts/distributed/sql/restore_usertable_small_dynamic.sql` | Restore/reset SQL before each run; the default creates the native dynamic Merkle layout. |
| `--verify-table TABLE` | `usertable_small` | Table whose replica roots are compared after the run. |
| `--enable-merkle-index N` | `1` | Enable (`1`) or disable (`0`) Merkle index maintenance. |
| `--merkle-verify-mode M` | `dynamic` | `dynamic` checks native topology and leaf assignments; `legacy` uses the legacy root path; `auto` lets the cluster runner choose. |
| `--dynamic-index NAME` | `public.usertable_small_dynamic_merkle_idx` | Fully-qualified native dynamic Merkle index used by dynamic verification. |
| `--dynamic-structure-gate N` | `0` | Enable (`1`) untimed merge/re-split/key-route transition coverage; failure invalidates the run. |
| `--dynamic-structure-crash-gate N` | `0` | Enable (`1`) pending-transition replica crash/restart coverage; failure invalidates the run. |

### Workload phases

| Option | Default | Effect |
| --- | --- | --- |
| `--warmup-queries N` | `1000` | Untimed state-preserving warm-up updates. `0` disables warm-up; warm-up is proven excluded from TPS. |
| `--warmup-workload FILE` | runner default | Replace the default warm-up SQL with an explicit state-safe workload file. |

### Cluster topology and database connection

The six CSV node options are parallel arrays: element `i` in each array
describes the same node.

| Option | Default/source | Effect |
| --- | --- | --- |
| `--node-ids CSV` | `cluster_topology.sh` | Raft/node IDs. |
| `--node-ips CSV` | `cluster_topology.sh` | Reachable node addresses. |
| `--node-names CSV` | `cluster_topology.sh` | Human-readable names in logs/artifacts. |
| `--node-users CSV` | `cluster_topology.sh` | SSH users for each node. |
| `--node-is-u22 CSV` | `cluster_topology.sh` | `1` selects the Ubuntu 22.04 remote build; `0` selects the normal build. |
| `--node-client-ports CSV` | `cluster_topology.sh` | Gateway client port on each node. |
| `--raft-port N` | `9000` | Raft transport port. |
| `--db-port N` | `5438` | PostgreSQL port. |
| `--db-user USER` | `postgres` | PostgreSQL user. |
| `--db-name NAME` | `postgres` | PostgreSQL database. |
| `--kafka-host HOST` | topology default | Kafka broker host. |
| `--kafka-port N` | `9092` | Kafka broker port. |
| `--kafka-home-remote DIR` | topology default | Remote Kafka installation directory. |

The wrapper also fixes the ordering profile, any-majority result completion
with asynchronous all-replica validation, one direct client in-flight request,
deterministic queue watermarks, and the executor/connection pairing so every
point in a campaign is comparable. These are not independent sweep flags.

`-h` and `--help` print the option reference and exit without building or
starting a cluster.

Useful invocations:

```bash
# Default dynamic-native campaign (six executor points, one repetition).
./run_sweep.sh

# Small smoke campaign.
./run_sweep.sh --threads 1 --det-client-workers 1 --executor-workers 1 --reps 1

# Include the optional dynamic topology and pending-crash gates.
./run_sweep.sh --dynamic-structure-gate 1 --dynamic-structure-crash-gate 1

# Explicit legacy verification (the restore SQL must match the chosen layout).
./run_sweep.sh \
  --restore-sql scripts/restore_usertable_small.sql \
  --merkle-verify-mode legacy --enable-merkle-index 1
```

The campaign directory is printed near the end of `out.txt`:

```text
Artifacts: scripts/bench_full_results/pg_executor_sweep_<timestamp>
```

Inside that directory:

```text
runs.csv      maps pg_executor_workers, repetition, and cluster4_* artifact
run_dirs.txt  one cluster4_* artifact path per line
summary.csv   appended output from summarize_raft_profile.py
campaign.env  sweep settings and forwarded cluster args for this campaign
schedule.csv  recorded alternating/interleaved worker execution order
campaign_provenance.env  frozen source, binary, restore, workload, and runner hashes
console.log   live campaign output captured alongside out.txt
```

The summary contains one header and includes client p50/p95/p99 latency,
all-three agreement latency, marker visibility, Merkle drain, and total
post-run equality time. Its `tps` value is Raft-majority result-completion TPS
under asynchronous all-replica validation; it is not all-three durable-completion
TPS. Do not substitute the gateway's `submit_time`: in an
async run that field is a cumulative sum across submissions, not wall-clock
duration. Use `overall_wall_ms` (and the recorded `tps_semantics`) for the
campaign's TPS interpretation.

Each individual distributed run still creates its own normal
`scripts/bench_full_results/cluster4_*` artifact.

## Current cluster topology

`run_sweep.sh` does not define the cluster machines directly. It calls
`run_4node_raft_cluster.sh`, which sources:

```text
scripts/distributed/cluster_topology.sh
```

The current topology in that file is:

```bash
declare -a NODE_IDS=(1 2 4)
declare -a NODE_IPS=(10.129.148.236 10.129.148.246 10.129.148.248)
declare -a NODE_NAMES=(admin123 user4 utkarsh)
declare -a NODE_USERS=(neel neel neel)
declare -a NODE_IS_U22=(0 1 0)
declare -a NODE_CLIENT_PORTS=(8000 8000 8001)

export RAFT_PORT=9000
export DB_PORT=5438
export DB_USER=postgres
export DB_NAME=postgres
```

`NODE_IS_U22` matters because Ubuntu 22.04 nodes use the separate
`/home/neel/Desktop/ariabc_pg_build_u22` server/gateway binaries. Ubuntu 24.04
nodes use the synced repository build under
`/home/neel/Desktop/ariabc_cluster/ariabc_pg/build`.

## Use different cluster nodes from CLI

`run_sweep.sh` accepts topology flags and forwards them to
`run_4node_raft_cluster.sh`. The topology flags are aligned CSV arrays:

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

Keep these arrays aligned by index. For example, index 1 in every array refers
to the same machine:

```text
NODE_IDS[1]=2
NODE_IPS[1]=10.10.0.12
NODE_NAMES[1]=node-b
NODE_USERS[1]=neel
NODE_IS_U22[1]=0
NODE_CLIENT_PORTS[1]=8000
```

Optional topology-related flags:

```bash
--raft-port 9000
--db-port 5438
--db-user postgres
--db-name postgres
--kafka-port 9092
--kafka-home-remote /home/neel/Desktop/kafka_2.13-3.7.0
```

Checklist when replacing a node:

1. SSH must work from the gateway to `NODE_USERS[i]@NODE_IPS[i]`.
2. The remote repository path must exist or be creatable:
   `/home/neel/Desktop/ariabc_cluster`.
3. The remote install path must exist or be creatable:
   `/home/neel/Desktop/ariabc_install`.
4. PostgreSQL uses `DB_PORT=5438`.
5. Raft server traffic uses `RAFT_PORT=9000`.
6. Gateway client traffic uses `NODE_CLIENT_PORTS`.
7. If a node is Ubuntu 22.04, set `NODE_IS_U22=1`.
8. Avoid a client port already used on that machine. The current `utkarsh`
   entry uses `8001` because `8000` is occupied there.

The default Kafka broker is:

```bash
KAFKA_HOST="10.129.148.236"
KAFKA_PORT=9092
KAFKA_HOME_REMOTE="/home/neel/Desktop/kafka_2.13-3.7.0"
```

If you move Kafka to a different machine, pass `--kafka-host` and optionally
`--kafka-port` / `--kafka-home-remote`, or keep node 1 as the Kafka host.

## Gateway machine

`run_4node_raft_cluster.sh` delegates execution to the gateway machine unless it
is already running there. These environment variables control that delegation:

```bash
GATEWAY_HOST=10.129.27.111
GATEWAY_USER=neel
GATEWAY_HOSTNAME=myubuntu
GATEWAY_REPO=/home/neel/ARIABC/AriaBC
GATEWAY_INSTALL=/home/neel/ARIABC/install
```

Example using another gateway:

```bash
GATEWAY_HOST=10.10.0.20 \
GATEWAY_USER=neel \
GATEWAY_HOSTNAME=my-gateway \
GATEWAY_REPO=/home/neel/ARIABC/AriaBC \
GATEWAY_INSTALL=/home/neel/ARIABC/install \
./scripts/distributed/run_sweep.sh
```

To force running on the current machine without delegation:

```bash
BYPASS_DELEGATION=1 ./scripts/distributed/run_sweep.sh
```

## Change client threads

The default sweep runs with:

```bash
--threads 96
--det-client-workers 96
```

To run a 48-thread campaign:

```bash
./scripts/distributed/run_sweep.sh --threads 48
```

`run_sweep.sh --threads N` sets both the deterministic client lane count and
the gateway deterministic threadpool worker count. If you need those to differ:

```bash
./scripts/distributed/run_sweep.sh --threads 96 --det-client-workers 48
```

## Change the server executor sweep

For example, to sweep only 8, 16, and 24 server executor workers:

```bash
./scripts/distributed/run_sweep.sh --executor-workers 8,16,24
```

Quoted space-separated lists also work:

```bash
./scripts/distributed/run_sweep.sh --executor-workers "8 16 24"
```

Each `E` is passed to both:

```bash
--server-exec-workers "$E"
--server-pg-connections "$E"
```

Keep these two values equal. `run_4node_raft_cluster.sh` rejects configurations
where one is set without the other or where they differ.

## Change repetitions

For a quick one-pass smoke sweep:

```bash
./scripts/distributed/run_sweep.sh --reps 1
```

For five repetitions:

```bash
./scripts/distributed/run_sweep.sh --reps 1,2,3,4,5
```

If a node replacement fails, first verify SSH, free ports, OS profile
(`NODE_IS_U22`), remote paths, and whether the Kafka host still points at a
reachable broker.
