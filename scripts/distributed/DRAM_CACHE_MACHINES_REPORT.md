# DRAM Cache Machines — Detailed Report

Generated on: 2026-04-20
Source CSV: `scripts/distributed/dram_cache_machines_with_ip.csv`
User: `protectdb` | Probe timeout: 8 s per machine | All 35 machines probed in parallel via SSH.

---

## Reachability Summary

**20 / 35 reachable**

| Cluster | Total | Reachable | Unreachable |
|---------|------:|----------:|------------:|
| sl1     | 11    | 11        | 0           |
| sl2     | 9     | 8         | 1 (sl2-118 timeout) |
| sl3     | 1     | 0         | 1 (auth failed) |
| cs101   | 14    | 0         | 14 (auth failed — protectdb user not provisioned) |
| **Total** | **35** | **20** | **15** |

---

## Reachability Table (all machines)

| Machine | IP | Cluster | Perf Rank | SSD Model | SSH Status |
|---|---|---|---:|---|---|
| sl2-112   | 10.130.154.112 | sl2   | 1 | ESSENCORE NVME GEN3 SSD | **OK** |
| cs101-122 | 10.130.152.122 | cs101 | 2 | ADATA SX6000PNP         | FAIL (auth) |
| cs101-123 | 10.130.152.123 | cs101 | 2 | ADATA SX6000PNP         | FAIL (auth) |
| cs101-125 | 10.130.152.125 | cs101 | 2 | ADATA SX6000PNP         | FAIL (auth) |
| cs101-127 | 10.130.152.127 | cs101 | 2 | ADATA SX6000PNP         | FAIL (auth) |
| cs101-129 | 10.130.152.129 | cs101 | 2 | ADATA SX6000PNP         | FAIL (auth) |
| cs101-131 | 10.130.152.131 | cs101 | 2 | ADATA SX6000PNP         | FAIL (auth) |
| cs101-133 | 10.130.152.133 | cs101 | 2 | ADATA SX6000PNP         | FAIL (auth) |
| cs101-136 | 10.130.152.136 | cs101 | 2 | ADATA SX6000PNP         | FAIL (auth) |
| cs101-140 | 10.130.152.140 | cs101 | 2 | ADATA SX6000PNP         | FAIL (auth) |
| cs101-142 | 10.130.152.142 | cs101 | 2 | ADATA SX6000PNP         | FAIL (auth) |
| cs101-144 | 10.130.152.144 | cs101 | 2 | ADATA SX6000PNP         | FAIL (auth) |
| cs101-146 | 10.130.152.146 | cs101 | 2 | ADATA SX6000PNP         | FAIL (auth) |
| cs101-148 | 10.130.152.148 | cs101 | 2 | ADATA SX6000PNP         | FAIL (auth) |
| sl1-16    | 10.130.153.16  | sl1   | 3 | HP SSD S700 120GB       | **OK** |
| sl1-60    | 10.130.153.60  | sl1   | 3 | HP SSD S700 120GB       | **OK** |
| sl2-101   | 10.130.154.101 | sl2   | 3 | HP SSD S700 120GB       | **OK** |
| sl1-4     | 10.130.153.4   | sl1   | 4 | Kingston SV300S3        | **OK** |
| sl1-30    | 10.130.153.30  | sl1   | 4 | Kingston SV300S37A120G  | **OK** |
| sl1-58    | 10.130.153.58  | sl1   | 4 | Kingston SV300S3        | **OK** |
| sl2-32    | 10.130.154.32  | sl2   | 4 | Kingston SV300S3        | **OK** |
| sl2-88    | 10.130.154.88  | sl2   | 4 | Kingston SV300S3        | **OK** |
| sl2-116   | 10.130.154.116 | sl2   | 4 | Kingston SV300S3        | **OK** |
| sl1-2     | 10.130.153.2   | sl1   | 5 | Kingston SV200S364G     | **OK** |
| sl1-23    | 10.130.153.23  | sl1   | 5 | Kingston SV200S3        | **OK** |
| sl1-27    | 10.130.153.27  | sl1   | 5 | Kingston SV200S364G     | **OK** |
| sl1-32    | 10.130.153.32  | sl1   | 5 | Kingston SV200S364G     | **OK** |
| sl1-41    | 10.130.153.41  | sl1   | 5 | Kingston SV200S3        | **OK** |
| sl1-53    | 10.130.153.53  | sl1   | 5 | Kingston SV200S364G     | **OK** |
| sl2-6     | 10.130.154.6   | sl2   | 5 | Kingston SV200S364G     | **OK** |
| sl2-48    | 10.130.154.48  | sl2   | 5 | Kingston SV200S364G     | **OK** |
| sl2-57    | 10.130.154.57  | sl2   | 5 | Kingston SV200S364G     | **OK** |
| sl2-109   | 10.130.154.109 | sl2   | 5 | Kingston SV200S3        | **OK** |
| sl2-118   | 10.130.154.118 | sl2   | 5 | Kingston SV200S3        | FAIL (timeout) |
| sl3-2     | 10.130.155.2   | sl3   | 5 | Kingston SV200S364G     | FAIL (auth) |

---

## Live System Snapshot (reachable machines only)

Probed: 2026-04-20 via `protectdb@<ip>` SSH

| Machine | OS | Kernel | Threads | RAM | Swap | Root used / free | Root use% | Disk model | Uptime |
|---|---|---|---:|---:|---:|---:|---:|---|---|
| sl2-112   | Ubuntu 22.04.5 | 6.8.0-107-generic   | 16 | 15.0 GB | 14.9 GB | 66G / 142G  | 32%  | ESSENCORE NVME GEN3 SSD (nvme0n1) | up 8 h 56 min |
| sl1-16    | Ubuntu 24.04.3 | 6.17.0-20-generic   |  4 |  7.7 GB |  4.0 GB | 57G / 47G   | 55%  | HP SSD S700 120GB (sda)           | up 8 h 8 min  |
| sl1-60    | Ubuntu 22.04.5 | 6.8.0-107-generic   |  4 |  7.7 GB |  2.0 GB | 45G / 0     | 100% | HP SSD S700 120GB (sda)           | up 1 d 12 h 38 min |
| sl2-101   | Ubuntu 24.04.4 | 6.17.0-20-generic   |  4 |  3.5 GB |  3.5 GB | 39G / 65G   | 38%  | HP SSD S700 120GB (sda)           | up 9 h 11 min |
| sl1-4     | Ubuntu 22.04.4 | 5.15.0-176-generic  |  4 |  7.6 GB |  4.0 GB | 65G / 39G   | 63%  | KINGSTON SV300S3 (sda)            | up 1 d 12 h 38 min |
| sl1-30    | Ubuntu 24.04.3 | 6.17.0-20-generic   |  4 |  7.7 GB |  4.0 GB | 57G / 47G   | 55%  | KINGSTON SV300S37A120G (sda)      | up 7 h 33 min |
| sl1-58    | Ubuntu 22.04.4 | 5.15.0-174-generic  |  4 |  7.6 GB |  4.0 GB | 75G / 29G   | 73%  | KINGSTON SV300S3 (sda)            | up 8 h |
| sl2-32    | Ubuntu 22.04.1 | 5.15.0-176-generic  |  4 |  7.6 GB |  4.0 GB | 82G / 22G   | 80%  | KINGSTON SV300S3 (sda)            | up 1 d 12 h 38 min |
| sl2-88    | Ubuntu 22.04.1 | 5.15.0-176-generic  |  4 |  7.4 GB |  4.0 GB | 67G / 37G   | 65%  | KINGSTON SV300S3 (sda)            | up 9 h 30 min |
| sl2-116   | Ubuntu 22.04.1 | 5.15.0-176-generic  |  4 |  7.7 GB |  4.0 GB | 69G / 34G   | 67%  | KINGSTON SV300S3 (sda)            | up 1 d 12 h 38 min |
| sl1-2     | Ubuntu 24.04.3 | 6.17.0-20-generic   |  4 |  7.7 GB |  4.0 GB | 51G / 3.8G  | 94%  | KINGSTON SV200S364G (sda)         | up 7 h 37 min |
| sl1-23    | Ubuntu 22.04.1 | 5.15.0-174-generic  |  4 |  7.7 GB |  4.0 GB | 47G / 3.7G  | 93%  | KINGSTON SV200S3 (sda)            | up 8 h 11 min |
| sl1-27    | Ubuntu 24.04.4 | 6.17.0-20-generic   |  4 |  7.7 GB |  4.0 GB | 41G / 15G   | 74%  | KINGSTON SV200S364G (sda)         | up 7 h 35 min |
| sl1-32    | Ubuntu 24.04.3 | 6.17.0-20-generic   |  4 |  7.7 GB |  4.0 GB | 54G / 1.1G  | 99%  | KINGSTON SV200S364G (sda)         | up 8 h 10 min |
| sl1-41    | Ubuntu 22.04.1 | 5.15.0-176-generic  |  4 |  7.7 GB |  4.0 GB | 51G / 3.5G  | 94%  | KINGSTON SV200S3 (sda)            | up 8 h 9 min  |
| sl1-53    | Ubuntu 24.04.3 | 6.17.0-20-generic   |  4 |  7.7 GB |  4.0 GB | 52G / 2.6G  | 96%  | KINGSTON SV200S364G (sda)         | up 8 h 1 min  |
| sl2-6     | Ubuntu 22.04.5 | 6.8.0-107-generic   |  4 |  7.7 GB |  2.0 GB | 52G / 3.7G  | 94%  | KINGSTON SV200S364G (sda)         | up 10 h 2 min |
| sl2-48    | Ubuntu 22.04.5 | 6.8.0-107-generic   |  4 |  7.6 GB |  2.0 GB | 55G / 68M   | 100% | KINGSTON SV200S364G (sda)         | up 1 d 10 h 58 min |
| sl2-57    | Ubuntu 22.04.5 | 6.8.0-107-generic   |  2 |  7.7 GB |  2.0 GB | 53G / 2.2G  | 97%  | KINGSTON SV200S364G (sda)         | up 1 d 12 h 38 min |
| sl2-109   | Ubuntu 22.04.1 | 5.15.0-176-generic  |  4 |  7.7 GB |  4.0 GB | 52G / 2.6G  | 96%  | KINGSTON SV200S3 (sda)            | up 1 d 12 h 38 min |

---

## Per-Machine Detail (reachable)

### sl2-112 (`10.130.154.112`) — Perf Rank 1
- **Cluster**: sl2
- **OS**: Ubuntu 22.04.5 LTS
- **Kernel**: 6.8.0-107-generic
- **CPU threads**: 16 (only machine in this set with 16 threads)
- **RAM**: ~15.0 GB
- **Swap**: ~14.9 GB
- **Drive**: `nvme0n1` — ESSENCORE NVME GEN3 SSD (NVMe Gen3, Phison E12)
- **Approx sequential**: 3500 MB/s read / 3000 MB/s write
- **Root FS**: `/dev/nvme0n1p3 219G total, 66G used, 142G free (32%)`
- **Uptime**: up 8 hours, 56 minutes
- **DRAM cache**: YES
- **Note**: Highest-ranked and only NVMe machine in this cluster set; 4× the threads of all other reachable machines.

---

### sl1-16 (`10.130.153.16`) — Perf Rank 3
- **Cluster**: sl1
- **OS**: Ubuntu 24.04.3 LTS
- **Kernel**: 6.17.0-20-generic
- **CPU threads**: 4
- **RAM**: ~7.7 GB
- **Swap**: ~4.0 GB
- **Drive**: `sda` — HP SSD S700 120GB (SATA, SMI SM2258)
- **Approx sequential**: 560 MB/s read / 510 MB/s write
- **Root FS**: `/dev/sda2 109G total, 57G used, 47G free (55%)`
- **Uptime**: up 8 hours, 8 minutes
- **DRAM cache**: YES

---

### sl1-60 (`10.130.153.60`) — Perf Rank 3
- **Cluster**: sl1
- **OS**: Ubuntu 22.04.5 LTS
- **Kernel**: 6.8.0-107-generic
- **CPU threads**: 4
- **RAM**: ~7.7 GB
- **Swap**: ~2.0 GB
- **Drive**: `sda` — HP SSD S700 120GB (SATA, SMI SM2258)
- **Approx sequential**: 560 MB/s read / 510 MB/s write
- **Root FS**: `/dev/sda3 46G total, 45G used, 0 free (100%)` — **DISK FULL**
- **Uptime**: up 1 day, 12 hours, 38 minutes
- **DRAM cache**: YES
- **Warning**: Root filesystem is 100% full.

---

### sl2-101 (`10.130.154.101`) — Perf Rank 3
- **Cluster**: sl2
- **OS**: Ubuntu 24.04.4 LTS
- **Kernel**: 6.17.0-20-generic
- **CPU threads**: 4
- **RAM**: ~3.5 GB (lowest among reachable — half of the typical 7.7 GB)
- **Swap**: ~3.5 GB
- **Drive**: `sda` — HP SSD S700 120GB (SATA, SMI SM2258)
- **Approx sequential**: 560 MB/s read / 510 MB/s write
- **Root FS**: `/dev/sda2 109G total, 39G used, 65G free (38%)`
- **Uptime**: up 9 hours, 11 minutes
- **DRAM cache**: YES
- **Note**: Only 3.5 GB RAM; may be insufficient for memory-intensive workloads.

---

### sl1-4 (`10.130.153.4`) — Perf Rank 4
- **Cluster**: sl1
- **OS**: Ubuntu 22.04.4 LTS
- **Kernel**: 5.15.0-176-generic
- **CPU threads**: 4
- **RAM**: ~7.6 GB
- **Swap**: ~4.0 GB
- **Drive**: `sda` — KINGSTON SV300S3 (SATA, SandForce SF-2281)
- **Approx sequential**: 450 MB/s read / 400 MB/s write
- **Root FS**: `/dev/mapper/ubuntu--vg-ubuntu--lv 108G total, 65G used, 39G free (63%)`
- **Uptime**: up 1 day, 12 hours, 38 minutes
- **DRAM cache**: YES

---

### sl1-30 (`10.130.153.30`) — Perf Rank 4
- **Cluster**: sl1
- **OS**: Ubuntu 24.04.3 LTS
- **Kernel**: 6.17.0-20-generic
- **CPU threads**: 4
- **RAM**: ~7.7 GB
- **Swap**: ~4.0 GB
- **Drive**: `sda` — KINGSTON SV300S37A120G (SATA, SandForce SF-2281)
- **Approx sequential**: 450 MB/s read / 400 MB/s write
- **Root FS**: `/dev/sda2 109G total, 57G used, 47G free (55%)`
- **Uptime**: up 7 hours, 33 minutes
- **DRAM cache**: YES

---

### sl1-58 (`10.130.153.58`) — Perf Rank 4
- **Cluster**: sl1
- **OS**: Ubuntu 22.04.4 LTS
- **Kernel**: 5.15.0-174-generic
- **CPU threads**: 4
- **RAM**: ~7.6 GB
- **Swap**: ~4.0 GB
- **Drive**: `sda` — KINGSTON SV300S3 (SATA, SandForce SF-2281)
- **Approx sequential**: 450 MB/s read / 400 MB/s write
- **Root FS**: `/dev/mapper/ubuntu--vg-ubuntu--lv 108G total, 75G used, 29G free (73%)`
- **Uptime**: up 8 hours
- **DRAM cache**: YES

---

### sl2-32 (`10.130.154.32`) — Perf Rank 4
- **Cluster**: sl2
- **OS**: Ubuntu 22.04.1 LTS
- **Kernel**: 5.15.0-176-generic
- **CPU threads**: 4
- **RAM**: ~7.6 GB
- **Swap**: ~4.0 GB
- **Drive**: `sda` — KINGSTON SV300S3 (SATA, SandForce SF-2281)
- **Approx sequential**: 450 MB/s read / 400 MB/s write
- **Root FS**: `/dev/mapper/ubuntu--vg-ubuntu--lv 108G total, 82G used, 22G free (80%)`
- **Uptime**: up 1 day, 12 hours, 38 minutes
- **DRAM cache**: YES

---

### sl2-88 (`10.130.154.88`) — Perf Rank 4
- **Cluster**: sl2
- **OS**: Ubuntu 22.04.1 LTS
- **Kernel**: 5.15.0-176-generic
- **CPU threads**: 4
- **RAM**: ~7.4 GB
- **Swap**: ~4.0 GB
- **Drive**: `sda` — KINGSTON SV300S3 (SATA, SandForce SF-2281)
- **Approx sequential**: 450 MB/s read / 400 MB/s write
- **Root FS**: `/dev/mapper/ubuntu--vg-ubuntu--lv 108G total, 67G used, 37G free (65%)`
- **Uptime**: up 9 hours, 30 minutes
- **DRAM cache**: YES

---

### sl2-116 (`10.130.154.116`) — Perf Rank 4
- **Cluster**: sl2
- **OS**: Ubuntu 22.04.1 LTS
- **Kernel**: 5.15.0-176-generic
- **CPU threads**: 4
- **RAM**: ~7.7 GB
- **Swap**: ~4.0 GB
- **Drive**: `sda` — KINGSTON SV300S3 (SATA, SandForce SF-2281)
- **Approx sequential**: 450 MB/s read / 400 MB/s write
- **Root FS**: `/dev/mapper/ubuntu--vg-ubuntu--lv 108G total, 69G used, 34G free (67%)`
- **Uptime**: up 1 day, 12 hours, 38 minutes
- **DRAM cache**: YES

---

### sl1-2 (`10.130.153.2`) — Perf Rank 5
- **Cluster**: sl1
- **OS**: Ubuntu 24.04.3 LTS
- **Kernel**: 6.17.0-20-generic
- **CPU threads**: 4
- **RAM**: ~7.7 GB
- **Swap**: ~4.0 GB
- **Drive**: `sda` — KINGSTON SV200S364G (SATA, SandForce SF-2281)
- **Approx sequential**: 300 MB/s read / 250 MB/s write
- **Root FS**: `/dev/sda2 58G total, 51G used, 3.8G free (94%)` — **nearly full**
- **Uptime**: up 7 hours, 37 minutes
- **DRAM cache**: YES
- **Warning**: Root at 94% — very limited headroom.

---

### sl1-23 (`10.130.153.23`) — Perf Rank 5
- **Cluster**: sl1
- **OS**: Ubuntu 22.04.1 LTS
- **Kernel**: 5.15.0-174-generic
- **CPU threads**: 4
- **RAM**: ~7.7 GB
- **Swap**: ~4.0 GB
- **Drive**: `sda` — KINGSTON SV200S3 (SATA, SandForce SF-2281)
- **Approx sequential**: 300 MB/s read / 250 MB/s write
- **Root FS**: `/dev/mapper/ubuntu--vg-ubuntu--lv 53G total, 47G used, 3.7G free (93%)` — **nearly full**
- **Uptime**: up 8 hours, 11 minutes
- **DRAM cache**: YES
- **Warning**: Root at 93%.

---

### sl1-27 (`10.130.153.27`) — Perf Rank 5
- **Cluster**: sl1
- **OS**: Ubuntu 24.04.4 LTS
- **Kernel**: 6.17.0-20-generic
- **CPU threads**: 4
- **RAM**: ~7.7 GB
- **Swap**: ~4.0 GB
- **Drive**: `sda` — KINGSTON SV200S364G (SATA, SandForce SF-2281)
- **Approx sequential**: 300 MB/s read / 250 MB/s write
- **Root FS**: `/dev/sda2 58G total, 41G used, 15G free (74%)`
- **Uptime**: up 7 hours, 35 minutes
- **DRAM cache**: YES

---

### sl1-32 (`10.130.153.32`) — Perf Rank 5
- **Cluster**: sl1
- **OS**: Ubuntu 24.04.3 LTS
- **Kernel**: 6.17.0-20-generic
- **CPU threads**: 4
- **RAM**: ~7.7 GB
- **Swap**: ~4.0 GB
- **Drive**: `sda` — KINGSTON SV200S364G (SATA, SandForce SF-2281)
- **Approx sequential**: 300 MB/s read / 250 MB/s write
- **Root FS**: `/dev/sda2 58G total, 54G used, 1.1G free (99%)` — **CRITICAL: almost full**
- **Uptime**: up 8 hours, 10 minutes
- **DRAM cache**: YES
- **Warning**: Root at 99% — essentially no free space.

---

### sl1-41 (`10.130.153.41`) — Perf Rank 5
- **Cluster**: sl1
- **OS**: Ubuntu 22.04.1 LTS
- **Kernel**: 5.15.0-176-generic
- **CPU threads**: 4
- **RAM**: ~7.7 GB
- **Swap**: ~4.0 GB
- **Drive**: `sda` — KINGSTON SV200S3 (SATA, SandForce SF-2281)
- **Approx sequential**: 300 MB/s read / 250 MB/s write
- **Root FS**: `/dev/mapper/ubuntu--vg-ubuntu--lv 57G total, 51G used, 3.5G free (94%)`
- **Uptime**: up 8 hours, 9 minutes
- **DRAM cache**: YES
- **Warning**: Root at 94%.

---

### sl1-53 (`10.130.153.53`) — Perf Rank 5
- **Cluster**: sl1
- **OS**: Ubuntu 24.04.3 LTS
- **Kernel**: 6.17.0-20-generic
- **CPU threads**: 4
- **RAM**: ~7.7 GB
- **Swap**: ~4.0 GB
- **Drive**: `sda` — KINGSTON SV200S364G (SATA, SandForce SF-2281)
- **Approx sequential**: 300 MB/s read / 250 MB/s write
- **Root FS**: `/dev/sda2 58G total, 52G used, 2.6G free (96%)` — **nearly full**
- **Uptime**: up 8 hours, 1 minute
- **DRAM cache**: YES
- **Warning**: Root at 96%.

---

### sl2-6 (`10.130.154.6`) — Perf Rank 5
- **Cluster**: sl2
- **OS**: Ubuntu 22.04.5 LTS
- **Kernel**: 6.8.0-107-generic
- **CPU threads**: 4
- **RAM**: ~7.7 GB
- **Swap**: ~2.0 GB
- **Drive**: `sda` — KINGSTON SV200S364G (SATA, SandForce SF-2281)
- **Approx sequential**: 300 MB/s read / 250 MB/s write
- **Root FS**: `/dev/sda2 58G total, 52G used, 3.7G free (94%)`
- **Uptime**: up 10 hours, 2 minutes
- **DRAM cache**: YES
- **Warning**: Root at 94%.

---

### sl2-48 (`10.130.154.48`) — Perf Rank 5
- **Cluster**: sl2
- **OS**: Ubuntu 22.04.5 LTS
- **Kernel**: 6.8.0-107-generic
- **CPU threads**: 4
- **RAM**: ~7.6 GB
- **Swap**: ~2.0 GB
- **Drive**: `sda` — KINGSTON SV200S364G (SATA, SandForce SF-2281)
- **Approx sequential**: 300 MB/s read / 250 MB/s write
- **Root FS**: `/dev/sda2 58G total, 55G used, 68M free (100%)` — **DISK FULL**
- **Uptime**: up 1 day, 10 hours, 58 minutes
- **DRAM cache**: YES
- **Warning**: Root filesystem is 100% full.

---

### sl2-57 (`10.130.154.57`) — Perf Rank 5
- **Cluster**: sl2
- **OS**: Ubuntu 22.04.5 LTS
- **Kernel**: 6.8.0-107-generic
- **CPU threads**: 2 (lowest thread count among all reachable machines)
- **RAM**: ~7.7 GB
- **Swap**: ~2.0 GB
- **Drive**: `sda` — KINGSTON SV200S364G (SATA, SandForce SF-2281)
- **Approx sequential**: 300 MB/s read / 250 MB/s write
- **Root FS**: `/dev/sda2 58G total, 53G used, 2.2G free (97%)` — **nearly full**
- **Uptime**: up 1 day, 12 hours, 38 minutes
- **DRAM cache**: YES
- **Warning**: Root at 97%; only 2 CPU threads.

---

### sl2-109 (`10.130.154.109`) — Perf Rank 5
- **Cluster**: sl2
- **OS**: Ubuntu 22.04.1 LTS
- **Kernel**: 5.15.0-176-generic
- **CPU threads**: 4
- **RAM**: ~7.7 GB
- **Swap**: ~4.0 GB
- **Drive**: `sda` — KINGSTON SV200S3 (SATA, SandForce SF-2281)
- **Approx sequential**: 300 MB/s read / 250 MB/s write
- **Root FS**: `/dev/mapper/ubuntu--vg-ubuntu--lv 57G total, 52G used, 2.6G free (96%)`
- **Uptime**: up 1 day, 12 hours, 38 minutes
- **DRAM cache**: YES
- **Warning**: Root at 96%.

---

## Unreachable Machines

### cs101 cluster (cs101-122 through cs101-148, 14 machines) — Perf Rank 2
- **IPs**: 10.130.152.122, .123, .125, .127, .129, .131, .133, .136, .140, .142, .144, .146, .148
- **Drive**: `nvme0n1` — ADATA SX6000PNP (NVMe, SMI SM2263)
- **Approx sequential**: 2100 MB/s read / 1500 MB/s write
- **Failure reason**: SSH Authentication failed — `protectdb` user not provisioned on this cluster's machines.
- **Action needed**: Create `protectdb` user with password `uplink` on all cs101 machines, or use the correct credentials.

### sl2-118 (`10.130.154.118`) — Perf Rank 5
- **Drive**: `sda` — KINGSTON SV200S3 (SATA, SandForce SF-2281)
- **Failure reason**: TCP connection timed out (8 s). Machine may be powered off or behind firewall.

### sl3-2 (`10.130.155.2`) — Perf Rank 5
- **Drive**: `sda` — KINGSTON SV200S364G (SATA, SandForce SF-2281)
- **Failure reason**: SSH Authentication failed — `protectdb` user not provisioned on sl3 cluster.

---

## Disk Health Alerts (reachable machines)

| Machine | IP | Root use% | Action |
|---|---|---:|---|
| sl1-60  | 10.130.153.60  | 100% | **FULL** — clean up immediately |
| sl2-48  | 10.130.154.48  | 100% | **FULL** — clean up immediately |
| sl1-32  | 10.130.153.32  | 99%  | Critical — essentially no free space |
| sl1-53  | 10.130.153.53  | 96%  | Nearly full |
| sl2-57  | 10.130.154.57  | 97%  | Nearly full |
| sl2-109 | 10.130.154.109 | 96%  | Nearly full |
| sl1-2   | 10.130.153.2   | 94%  | Nearly full |
| sl1-23  | 10.130.153.23  | 93%  | Nearly full |
| sl1-41  | 10.130.153.41  | 94%  | Nearly full |
| sl2-6   | 10.130.154.6   | 94%  | Nearly full |

---

## Key Observations

- **sl2-112** is the standout machine: only NVMe + 16 threads + ~15 GB RAM. Best candidate for benchmarking.
- **All sl1/sl2 machines (except sl2-112)** are 4-thread (sl2-57 has 2) with ~7.6–7.7 GB RAM and SATA SSDs.
- **sl2-101** has only 3.5 GB RAM — half the norm; unsuitable for memory-heavy workloads.
- **10 out of 20 reachable machines have root FS ≥ 93% full** — data collection / bench staging will fail on most of them without cleanup.
- **cs101 cluster (14 machines with NVMe, rank 2)** is completely inaccessible with current credentials — highest priority for access fix given their NVMe bandwidth.
- **sl2-118 and sl3-2** are also inaccessible and need investigation.
- All reachable machines run Ubuntu 22.04 or 24.04; kernels range from 5.15 to 6.17.

---

## Benchmark Results — sl2-112 (`10.130.154.112`)

**Run date**: 2026-04-21
**Modes**: `det` vs `pg` | **Threads**: 1, 2, 4, 8, 16 | **Runs per cell**: 3 | **All Merkle verify**: PASS

### Workload A: `ycsb-skew0-99-tx-20k-point-safedb-intkey-insert12k-uniq.txt`

| Threads | det median TPS | pg median TPS | det/pg |
|--------:|---------------:|--------------:|-------:|
| 1       | 4,727          | 3,947         | 1.20×  |
| 2       | 7,826          | 7,021         | 1.11×  |
| 4       | 11,819         | 11,051        | 1.07×  |
| 8       | 12,228         | 12,590        | 0.97×  |
| 16      | 11,188         | 11,936        | 0.94×  |

Notes: pg has ~270 reconnects/run at all thread counts (unique-insert conflict handling); det has zero. Both peak near 8t.

### Workload B: `ycsbtx-skew-01-24k-pt-intkey-sid-clean-20k.txt`

| Threads | det median TPS | pg median TPS | det/pg |
|--------:|---------------:|--------------:|-------:|
| 1       | 3,899          | 4,040         | 0.97×  |
| 2       | 6,312          | 7,267         | 0.87×  |
| 4       | 9,125          | 11,328        | 0.81×  |
| 8       | 11,513         | 13,511        | 0.85×  |
| 16      | 7,734          | 13,572        | 0.57×  |

Notes: det collapses at 16t under skewed contention (serial gate bottleneck). pg scales smoothly to 16t via SSI.

### Summary

- **Workload A (uniform)**: det leads pg by up to +20% at low thread counts. Gap closes at 8t; slight det underperformance at 16t.
- **Workload B (skewed)**: det is 13–19% behind pg at 2–8t. At 16t det drops to 57% of pg throughput — serial gate saturates under hot-key contention.
- **Raw results**: `scripts/bench_full_results/protectdb_ycsb_20260421_162046/protectdb_at_10_130_154_112/`

---

## Probe Method

```
python3 scripts/distributed/probe_dram_cache_machines.py
```

- Paramiko SSH with 35 parallel workers
- Connect/auth timeout: 8 s per machine
- Commands run on each host: `hostname`, `uname -r`, `/etc/os-release`, `nproc`, `/proc/meminfo`, `free`, `df -h /`, `lsblk -d`, `uptime`
