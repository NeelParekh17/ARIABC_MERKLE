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
7. repeats each worker count three times;
8. records each produced `cluster4_*` artifact and summary.

All stdout/stderr from the campaign is redirected to `out.txt` in the repository
root. The script does not stream to the terminal after the initial build/check
messages.

## Basic run

From the repository root:

```bash
./scripts/distributed/run_sweep.sh
```

The wrapper defaults to the distributed dynamic-Merkle path:

```text
--restore-sql scripts/distributed/sql/restore_usertable_small_dynamic.sql
--verify-table usertable_small
--enable-merkle-index 1
--merkle-verify-mode dynamic
--dynamic-index public.usertable_small_dynamic_merkle_idx
```

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
```

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
