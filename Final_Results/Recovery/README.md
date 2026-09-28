# AriaBC Recovery Results Directory

This directory contains the complete and authoritative evaluation of AriaBC's Merkle recovery systems, structured into two complementary benchmarks:

---

## 1. Single-Node Merkle Tree Size-Scaling Microbenchmark (1M – 50M Tuples)

- **Focus**: Evaluates static vs. dynamic Merkle tree recovery latency across dataset scales from 1M to 50M rows on a dedicated EPYC node (`ranking`).
- **Configuration**: Fanout $F=32$, Split threshold 32, Merge threshold 8, $K=75$ corrupted leaves, $C=300$ corrupted rows, 10 repetitions (110 runs total).
- **Core Report**: [`Report.md`](./Report.md)
- **Replication Script**: [`replicate_recovery_scaling.sh`](./replicate_recovery_scaling.sh)
- **Plots Directory**: [`plots/`](./plots/) (depth verification, phase composition, leaf occupancy, CV%, dataset construction times)
- **Primary Artifact**: [`ariabc-recovery-size-scaling-k75-c300-20260924T053957Z-0031ee/`](./ariabc-recovery-size-scaling-k75-c300-20260924T053957Z-0031ee/)

---

## 2. Distributed Online Replica Recovery (ProtectDB Algorithm 2)

- **Focus**: Evaluates live, non-blocking online recovery within a real 3-node Raft-Kafka distributed cluster under 160,000 YCSB transactions across 96 concurrent client lanes.
- **Key Capabilities Verified**:
  - **Zero Quorum Disruption**: Quorum continues serving client transactions during in-flight corruption (100 corrupted tuples @ 5s) at **8,693 TPS** (only 1.78% overhead vs. 8,850 TPS baseline).
  - **Deterministic Online Repair**: Merkle localization in **5.9 ms**, targeted row streaming in **54 ms** (0 full table copies).
  - **Dynamic Prioritized Reference Selection**: Automatically routes snapshot cut requests to fast replicas, lifting leader corruption throughput from 4,440 TPS to **8,674 TPS** (+95.3%) and eliminating stalls.
  - **Cryptographic Verification**: 100% Phase 8 Merkle root match across all 3 nodes (`root=80566f71...`), 0 permanent failures.
- **Directory**: [`Final_Results/ONLINE_RECOVERY/`](../ONLINE_RECOVERY/)
- **Core Report**: [`Final_Results/ONLINE_RECOVERY/Report.md`](../ONLINE_RECOVERY/Report.md)
- **Summary CSV**: [`Final_Results/ONLINE_RECOVERY/summary.csv`](../ONLINE_RECOVERY/summary.csv)
- **Replication Script**: [`Final_Results/ONLINE_RECOVERY/replicate_distributed_recovery.sh`](../ONLINE_RECOVERY/replicate_distributed_recovery.sh)
- **Graphs**: [`Final_Results/ONLINE_RECOVERY/graphs/`](../ONLINE_RECOVERY/graphs/)
- **Raw Run Artifacts**: [`Final_Results/ONLINE_RECOVERY/runs/`](../ONLINE_RECOVERY/runs/) (12 full run directories with runner logs, timelines, latency CSVs)
