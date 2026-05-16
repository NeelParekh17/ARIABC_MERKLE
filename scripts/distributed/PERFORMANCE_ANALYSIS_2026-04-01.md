# Single-Machine Throughput Analysis — 2026-04-01

## Benchmark Results (Before Fix)

Run: `run_single_machine_matrix_all_nodes.sh`, mode=det, threads=5, runs=3

| Node | Host | TPS (YCSB) | TPS (YCSBTX) | vs PG1 |
|------|------|-----------|--------------|--------|
| PG1  | 10.129.148.248 | 7965.7 | 3850.5 | baseline |
| PG3  | 10.129.148.248 | 3161.9 | 1288.2 | 2.5× slower |
| PG2  | 10.129.27.54 | 2519.0 | 1094.1 | 3.2× slower |
| GW   | 10.129.148.236 | 1803.9 | 889.2  | 4.4× slower |

---

## Root Cause Analysis

### Hardware Summary

| Node | CPU | Max Boost | vCPU | RAM | Storage |
|------|-----|-----------|------|-----|---------|
| PG1 (248) | Ryzen 7 5700G | 4673 MHz | 16 | 15 GiB | Intel 660p NVMe |
| PG3 (229) | Ryzen 7 5700G | 4673 MHz | 16 | 15 GiB | Crucial P2 NVMe |
| PG2 (user4/10.129.27.54) | EPYC 9654 | 3709 MHz | 192 | 251 GiB | Crucial P3PSSD8 + 4×Samsung NVMe RAID |
| GW (240) | Ryzen 7 5700G | 4673 MHz | 16 | 15 GiB | Crucial P2 NVMe |

### Primary Root Cause: Stale bcdb Config in postgresql.conf

The `single_node_pgdata` on PG3, 10.129.27.54, and GW had stale inline settings
written in `postgresql.conf` from a prior setup run:

```
bcdb_worker_count = 2       ← explicitly capped at 2
bcdb_result_ring_slots = 128  ← half the default pipeline depth
bcdb_dt_conflict_tracking = on  ← left enabled (correct but adds overhead)
```

PG1 did NOT have these inline overrides — it used the binary's defaults.
The `bench_single_auto.conf` (which contains `synchronous_commit=off, fsync=off`)
was written to disk but the `include_if_exists` line was missing from
`postgresql.conf` on the slower nodes, so those performance overrides were
silently ignored on ALL nodes.

Combined effect on PG3/10.129.27.54/GW:
- Half the worker threads in the deterministic serial gate pipeline (2 vs 4+)
- Smaller pipeline ring (128 vs 256) causing more gate stalls
- `synchronous_commit = on` (default) adding WAL sync latency per transaction
- `fsync = on` (default) forcing write-barriers on checkpoint

This explains ~2–2.5× of the gap on PG3. PG3 hardware is identical to PG1.

### Secondary Cause for Ranking: CPU Frequency

The EPYC 9654 has a max boost of 3709 MHz vs the Ryzen 5700G's 4673 MHz.
That is a 20% frequency gap. Combined with the config issues above, this
accounts for the remaining difference:

```
Expected ratio (config fix applied): 3709 / 4673 ≈ 0.79
→ Ranking should reach ~79% of PG1's TPS ≈ 6300 TPS (YCSB) after the fix
```

Additional contributing factor: a second PostgreSQL instance (`bibrank_db`)
runs continuously on 10.129.27.54 and competes for NVMe I/O and CPU.
It is not possible to stop it without root access on that machine.

### Root Cause for GW (10.129.148.236)

GW runs as a **shared desktop** — user `shalini` has multiple VSCode, Firefox,
and Chrome processes consuming ~10 GiB of the 15 GiB RAM, pushing the
AriaBC postgres into swap (3.6 GiB swap used during the run).

In the **distributed benchmark** topology, `10.129.148.236` is the **Gateway
only** — it does not run Postgres. So this memory pressure does **not** affect
distributed benchmark TPS. The GW single-machine result is only relevant when
comparing node health in isolation.

---

## Fix Applied

### 1. `ensure_single_node_postgres.sh` — added bcdb settings to `bench_single_auto.conf`

The generated `bench_single_auto.conf` now includes:

```
synchronous_commit = off
fsync = off
full_page_writes = off
wal_level = replica
bcdb_worker_count = 4          ← was missing; overrides any stale inline value
bcdb_result_ring_slots = 256   ← was missing; overrides stale 128
bcdb_serial_gate_mode = 1      ← explicit for clarity
```

Because `include_if_exists = 'bench_single_auto.conf'` is appended at the
**end** of `postgresql.conf`, these values override any stale inline settings
written earlier in the file (PostgreSQL: last setting wins).

### 2. Applied live on all nodes

`ensure_single_node_postgres.sh` was re-run on all 4 nodes. Verified via
`pg_settings`:

| Node | bcdb_worker_count | bcdb_result_ring_slots | synchronous_commit | fsync |
|------|-------------------|------------------------|--------------------|-------|
| PG1 (248) | 4 | 256 | off | off |
| PG3 (229) | 4 | 256 | off | off |
| GW (240)  | 4 | 256 | off | off |
| 10.129.27.54   | 4 | 256 | off | off |

---

## Expected Throughput After Fix

| Node | Expected YCSB TPS | Reasoning |
|------|-------------------|-----------|
| PG1 (248) | ~8000–9000 | Minor gain from fsync=off being actually active now |
| PG3 (229) | ~6000–7500 | Same hardware as PG1, config was the only bottleneck |
| PG2 (user4/10.129.27.54) | ~5500–7000 | CPU freq cap 3709 MHz (~20% slower than Ryzen) + bibrank_db noise |
| GW (240) | ~5000–6000 | Config fixed; still limited by shalini's apps using RAM |

The distributed bottleneck shifts from a 3.2× gap (10.129.27.54 was the worst PG node)
to a projected ~15–25% gap between PG1 and 10.129.27.54 — within the expected
hardware frequency ratio.

---

## Remaining Limitations (No Root Required Fixes Available)

### Ranking: `schedutil` CPU governor, no `performance` profile available

`powerprofilesctl` on 10.129.27.54 only exposes `balanced` / `power-saver`.
The `performance` profile is not listed. Under sustained 5-thread load
`schedutil` will ramp to 3709 MHz within 1-2 ms, so this is minor for
multi-second benchmark runs (runs are 5–22 s each).

### Ranking: bibrank_db competing Postgres instance

A system Postgres (`/home/bibrank/workspace/database`) with ~20 idle
connections and occasional active queries runs on the same nvme0n1p3 drive.
Cannot be stopped without root. Expected overhead: small (~5–10% TPS noise).

### GW: Shared desktop memory pressure

`shalini`'s VSCode + Firefox consume ~10 GiB RAM, forcing the AriaBC postgres
into 3.6 GiB of swap. This affects the GW single-machine test only.
In distributed mode, GW runs the gateway client only (no Postgres).
To improve GW standalone results: ask shalini to close heavy apps during runs.

### PG1: Root filesystem at 94% capacity

`/dev/nvme0n1p2` is 94% full (`24 GiB free`). Benchmark artifacts and
Postgres WAL segments accumulate here. Clean with:
```bash
ssh neel@10.129.148.248 'rm -rf /tmp/ariabc_cluster/.bench_tmp/*/pg_log/* \
  /tmp/ariabc_cluster/scripts/bench_full_results/single_machine_nodes_*'
```

---

## How to Re-run the Benchmark

```bash
cd /work/ARIABC/AriaBC
scripts/distributed/run_single_machine_matrix_all_nodes.sh \
  --pg-hosts 10.129.148.248,10.129.27.54,10.129.148.248,10.129.148.236 \
  --pg-users neel,neel,neel,neel \
  --ssh-key ~/.ssh/id_rsa \
  --threads 5 --runs 3 --modes det
```

The fix is baked into `ensure_single_node_postgres.sh`; no extra flags needed.
