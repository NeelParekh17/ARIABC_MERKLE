# Comprehensive Analysis: YCSB Workload Suite (`scripts/ycsb_suite/`)

This directory contains the complete standardized and extended YCSB workload matrix for evaluating throughput, latency, conflict rates, replication overhead, and deterministic execution behavior in **AriaBC** and PostgreSQL.

---

## 1. Executive Summary & Suite Provenance

* **Directory Path**: `scripts/ycsb_suite/`
* **Generator**: [`scripts/generate_ycsb_workloads.py`](../generate_ycsb_workloads.py)
* **Total Workload Files**: **72 files** (9 workload families x 8 Zipfian skew levels)
* **Scale**: Exactly **20,000 transactions per file** (**1,440,000 transactions total**)
* **Total Storage Footprint**: **~238 MB**
* **Target Table**: `usertable_small` (base 12,000 rows, keys `1` to `12000`)
* **Row Schema**: Integer primary key `YCSB_KEY` + 10 text fields (`FIELD1` .. `FIELD10`), each 20 random alphanumeric characters (`[a-zA-Z0-9]`)
* **Transaction Model**: Single-statement autocommit queries terminated with `;`

---

## 2. Workload Families & Operation Mixes

| Workload Family | Code | SELECT (R) | UPDATE (U) | INSERT (I) | DELETE (D) | Total DML (Writes) | Description & Target Stress |
|---|---|---|---|---|---|---|---|
| **Workload A** | `a` | 50% | 50% | 0% | 0% | **50%** | Standard Update-Heavy (read/update balance) |
| **Workload B** | `b` | 95% | 5% | 0% | 0% | **5%** | Standard Read-Mostly (low write contention) |
| **Workload C** | `c` | 100% | 0% | 0% | 0% | **0%** | Standard Read-Only (embarrassingly parallel) |
| **Workload D** | `d` | 95% | 0% | 5% | 0% | **5%** | Read-Latest (temporal sliding window of recent inserts) |
| **Workload F** | `f` | 50% | 50% | 0% | 0% | **50%** | Read-Modify-Write (identically generated to Workload A) |
| **DML Heavy** | `dml_heavy` | 10% | 50% | 20% | 20% | **90%** | **High DML / Low Read**: high write stress with steady-state key recycling |
| **Delete Heavy** | `delete_heavy` | 10% | 20% | 20% | 50% | **90%** | **Tombstone / Churn Heavy**: 50% deletes, massive MVCC / index cleanup stress |
| **Pure DML** | `pure_dml` | 0% | 50% | 25% | 25% | **100%** | **Write-Only**: zero reads, pure mutation throughput & Raft replication load |
| **Balanced DML** | `balanced_dml` | 20% | 40% | 20% | 20% | **80%** | **Multi-DML Balanced**: realistic mixed enterprise transactional workload |

---

## 3. Key Lifecycle & Deletion/Insertion Mechanics

In standard YCSB, workloads A-F do not issue deletes. To support rigorous evaluation of deletions and DML operations without degrading into no-ops or violating primary key constraints, the generator implements **dynamic key lifecycle tracking**:

1. **Recyclable Deleted Key Buffer**: When a `DELETE` operation removes key $k$, key $k$ is tracked in an in-memory buffer of available slots.
2. **Intelligent Insert Placement**:
   * 75% of `INSERT` statements recycle a previously deleted key $k$ (`INSERT INTO usertable_small (YCSB_KEY, ...) VALUES (k, ...)`), re-populating the empty slot without primary key violation.
   * 25% of `INSERT` statements append a brand new key (`12001`, `12002`, ...) to expand the dataset dynamically.
3. **Dual-Target Deletion**: `DELETE` targets either the active Zipfian distribution (deleting hot rows under contention) or recently inserted keys (cleaning up newly created rows).
4. **Steady-State Stability**: Because insert and delete probabilities are matched (20%/20% or 25%/25%), the active working set remains dense (~12,000 rows), ensuring subsequent `UPDATE` and `SELECT` queries hit live data.

---

## 4. Complete Statistical Inventory (All 72 Workload Files)

| Workload File | Size | SELECT | UPDATE | INSERT | DELETE | Total DML | Uniq Keys | Hottest Key (Count, %) | Top 1% Share | WW Conf % | Any Conf % |
|---|---|---|---|---|---|---|---|---|---|---|---|
| `ycsb_workload_a_skew_0_00_20k.txt` | 3.92 MB | 9947 (49.7%) | 10053 (50.3%) | 0 (0.0%) | 0 (0.0%) | **50.3%** | 9719 | Key 1003 (12, 0.1%) | 3.0% | 0.0021% | 0.0063% |
| `ycsb_workload_a_skew_0_20_20k.txt` | 3.89 MB | 10033 (50.2%) | 9967 (49.8%) | 0 (0.0%) | 0 (0.0%) | **49.8%** | 9616 | Key 18 (11, 0.1%) | 3.2% | 0.0022% | 0.0066% |
| `ycsb_workload_a_skew_0_50_20k.txt` | 3.88 MB | 10056 (50.3%) | 9944 (49.7%) | 0 (0.0%) | 0 (0.0%) | **49.7%** | 8739 | Key 1 (92, 0.5%) | 8.4% | 0.0054% | 0.0158% |
| `ycsb_workload_a_skew_0_70_20k.txt` | 3.91 MB | 9909 (49.5%) | 10091 (50.5%) | 0 (0.0%) | 0 (0.0%) | **50.5%** | 7559 | Key 1 (361, 1.8%) | 18.2% | 0.0293% | 0.0829% |
| `ycsb_workload_a_skew_0_80_20k.txt` | 3.89 MB | 9965 (49.8%) | 10035 (50.2%) | 0 (0.0%) | 0 (0.0%) | **50.2%** | 6672 | Key 1 (683, 3.4%) | 25.7% | 0.0739% | 0.2133% |
| `ycsb_workload_a_skew_0_90_20k.txt` | 3.91 MB | 9899 (49.5%) | 10101 (50.5%) | 0 (0.0%) | 0 (0.0%) | **50.5%** | 5698 | Key 1 (1270, 6.3%) | 34.8% | 0.1885% | 0.5551% |
| `ycsb_workload_a_skew_0_99_20k.txt` | 3.87 MB | 9992 (50.0%) | 10008 (50.0%) | 0 (0.0%) | 0 (0.0%) | **50.0%** | 4642 | Key 1 (1955, 9.8%) | 43.3% | 0.3702% | 1.1543% |
| `ycsb_workload_a_skew_1_20_20k.txt` | 3.87 MB | 9953 (49.8%) | 10047 (50.2%) | 0 (0.0%) | 0 (0.0%) | **50.2%** | 2522 | Key 1 (4098, 20.5%) | 61.5% | 1.4964% | 4.4506% |
| `ycsb_workload_b_skew_0_00_20k.txt` | 1.27 MB | 18994 (95.0%) | 1006 (5.0%) | 0 (0.0%) | 0 (0.0%) | **5.0%** | 9758 | Key 7324 (9, 0.0%) | 2.9% | 0.0000% | 0.0008% |
| `ycsb_workload_b_skew_0_20_20k.txt` | 1.25 MB | 19055 (95.3%) | 945 (4.7%) | 0 (0.0%) | 0 (0.0%) | **4.7%** | 9576 | Key 21 (14, 0.1%) | 3.4% | 0.0000% | 0.0008% |
| `ycsb_workload_b_skew_0_50_20k.txt` | 1.26 MB | 19015 (95.1%) | 985 (4.9%) | 0 (0.0%) | 0 (0.0%) | **4.9%** | 8741 | Key 1 (95, 0.5%) | 8.3% | 0.0001% | 0.0023% |
| `ycsb_workload_b_skew_0_70_20k.txt` | 1.26 MB | 18975 (94.9%) | 1025 (5.1%) | 0 (0.0%) | 0 (0.0%) | **5.1%** | 7511 | Key 1 (412, 2.1%) | 18.2% | 0.0003% | 0.0113% |
| `ycsb_workload_b_skew_0_80_20k.txt` | 1.26 MB | 18959 (94.8%) | 1041 (5.2%) | 0 (0.0%) | 0 (0.0%) | **5.2%** | 6706 | Key 1 (684, 3.4%) | 25.4% | 0.0007% | 0.0284% |
| `ycsb_workload_b_skew_0_90_20k.txt` | 1.24 MB | 18997 (95.0%) | 1003 (5.0%) | 0 (0.0%) | 0 (0.0%) | **5.0%** | 5716 | Key 1 (1227, 6.1%) | 34.3% | 0.0014% | 0.0633% |
| `ycsb_workload_b_skew_0_99_20k.txt` | 1.23 MB | 19031 (95.2%) | 969 (4.8%) | 0 (0.0%) | 0 (0.0%) | **4.8%** | 4707 | Key 1 (1925, 9.6%) | 43.5% | 0.0042% | 0.1591% |
| `ycsb_workload_b_skew_1_20_20k.txt` | 1.23 MB | 18991 (95.0%) | 1009 (5.0%) | 0 (0.0%) | 0 (0.0%) | **5.0%** | 2570 | Key 1 (4269, 21.3%) | 62.0% | 0.0164% | 0.6187% |
| `ycsb_workload_balanced_dml_skew_0_00_20k.txt` | 4.52 MB | 3964 (19.8%) | 7905 (39.5%) | 4013 (20.1%) | 4118 (20.6%) | **80.2%** | 9445 | Key 3305 (15, 0.1%) | 4.5% | 0.0086% | 0.0109% |
| `ycsb_workload_balanced_dml_skew_0_20_20k.txt` | 4.55 MB | 3975 (19.9%) | 7969 (39.8%) | 4032 (20.2%) | 4024 (20.1%) | **80.1%** | 9297 | Key 1805 (24, 0.1%) | 4.7% | 0.0090% | 0.0115% |
| `ycsb_workload_balanced_dml_skew_0_50_20k.txt` | 4.51 MB | 4037 (20.2%) | 7964 (39.8%) | 3932 (19.7%) | 4067 (20.3%) | **79.8%** | 8441 | Key 1 (86, 0.4%) | 8.0% | 0.0149% | 0.0206% |
| `ycsb_workload_balanced_dml_skew_0_70_20k.txt` | 4.51 MB | 4016 (20.1%) | 7954 (39.8%) | 3979 (19.9%) | 4051 (20.3%) | **79.9%** | 7269 | Key 1 (382, 1.9%) | 16.5% | 0.0655% | 0.1002% |
| `ycsb_workload_balanced_dml_skew_0_80_20k.txt` | 4.52 MB | 4044 (20.2%) | 8027 (40.1%) | 3932 (19.7%) | 3997 (20.0%) | **79.8%** | 6618 | Key 1 (620, 3.1%) | 23.2% | 0.1379% | 0.2206% |
| `ycsb_workload_balanced_dml_skew_0_90_20k.txt` | 4.53 MB | 3989 (19.9%) | 8056 (40.3%) | 3976 (19.9%) | 3979 (19.9%) | **80.1%** | 5656 | Key 1 (1145, 5.7%) | 31.2% | 0.3667% | 0.5709% |
| `ycsb_workload_balanced_dml_skew_0_99_20k.txt` | 4.54 MB | 4049 (20.2%) | 7976 (39.9%) | 4080 (20.4%) | 3895 (19.5%) | **79.8%** | 4904 | Key 1 (1681, 8.4%) | 39.7% | 0.7102% | 1.1533% |
| `ycsb_workload_balanced_dml_skew_1_20_20k.txt` | 4.52 MB | 4014 (20.1%) | 7918 (39.6%) | 4118 (20.6%) | 3950 (19.8%) | **79.9%** | 3164 | Key 1 (3642, 18.2%) | 57.8% | 2.8088% | 4.3870% |
| `ycsb_workload_c_skew_0_00_20k.txt` | 0.97 MB | 20000 (100.0%) | 0 (0.0%) | 0 (0.0%) | 0 (0.0%) | **0.0%** | 9707 | Key 6132 (9, 0.0%) | 3.0% | 0.0000% | 0.0000% |
| `ycsb_workload_c_skew_0_20_20k.txt` | 0.97 MB | 20000 (100.0%) | 0 (0.0%) | 0 (0.0%) | 0 (0.0%) | **0.0%** | 9590 | Key 18 (13, 0.1%) | 3.3% | 0.0000% | 0.0000% |
| `ycsb_workload_c_skew_0_50_20k.txt` | 0.97 MB | 20000 (100.0%) | 0 (0.0%) | 0 (0.0%) | 0 (0.0%) | **0.0%** | 8724 | Key 1 (97, 0.5%) | 8.4% | 0.0000% | 0.0000% |
| `ycsb_workload_c_skew_0_70_20k.txt` | 0.96 MB | 20000 (100.0%) | 0 (0.0%) | 0 (0.0%) | 0 (0.0%) | **0.0%** | 7523 | Key 1 (346, 1.7%) | 18.1% | 0.0000% | 0.0000% |
| `ycsb_workload_c_skew_0_80_20k.txt` | 0.96 MB | 20000 (100.0%) | 0 (0.0%) | 0 (0.0%) | 0 (0.0%) | **0.0%** | 6674 | Key 1 (658, 3.3%) | 25.3% | 0.0000% | 0.0000% |
| `ycsb_workload_c_skew_0_90_20k.txt` | 0.95 MB | 20000 (100.0%) | 0 (0.0%) | 0 (0.0%) | 0 (0.0%) | **0.0%** | 5775 | Key 1 (1220, 6.1%) | 34.0% | 0.0000% | 0.0000% |
| `ycsb_workload_c_skew_0_99_20k.txt` | 0.94 MB | 20000 (100.0%) | 0 (0.0%) | 0 (0.0%) | 0 (0.0%) | **0.0%** | 4751 | Key 1 (1902, 9.5%) | 43.1% | 0.0000% | 0.0000% |
| `ycsb_workload_c_skew_1_20_20k.txt` | 0.93 MB | 20000 (100.0%) | 0 (0.0%) | 0 (0.0%) | 0 (0.0%) | **0.0%** | 2571 | Key 1 (4093, 20.5%) | 61.8% | 0.0000% | 0.0000% |
| `ycsb_workload_d_skew_0_00_20k.txt` | 1.30 MB | 18985 (94.9%) | 0 (0.0%) | 1015 (5.1%) | 0 (0.0%) | **5.1%** | 4182 | Key 12002 (75, 0.4%) | 7.2% | 0.0000% | 0.0076% |
| `ycsb_workload_d_skew_0_20_20k.txt` | 1.31 MB | 18943 (94.7%) | 0 (0.0%) | 1057 (5.3%) | 0 (0.0%) | **5.3%** | 4290 | Key 12001 (67, 0.3%) | 7.5% | 0.0000% | 0.0076% |
| `ycsb_workload_d_skew_0_50_20k.txt` | 1.28 MB | 19044 (95.2%) | 0 (0.0%) | 956 (4.8%) | 0 (0.0%) | **4.8%** | 3931 | Key 12001 (92, 0.5%) | 8.0% | 0.0000% | 0.0076% |
| `ycsb_workload_d_skew_0_70_20k.txt` | 1.29 MB | 19000 (95.0%) | 0 (0.0%) | 1000 (5.0%) | 0 (0.0%) | **5.0%** | 3527 | Key 12001 (79, 0.4%) | 6.7% | 0.0000% | 0.0076% |
| `ycsb_workload_d_skew_0_80_20k.txt` | 1.28 MB | 19037 (95.2%) | 0 (0.0%) | 963 (4.8%) | 0 (0.0%) | **4.8%** | 3163 | Key 1 (129, 0.6%) | 7.5% | 0.0000% | 0.0076% |
| `ycsb_workload_d_skew_0_90_20k.txt` | 1.30 MB | 18987 (94.9%) | 0 (0.0%) | 1013 (5.1%) | 0 (0.0%) | **5.1%** | 2935 | Key 1 (225, 1.1%) | 7.9% | 0.0000% | 0.0075% |
| `ycsb_workload_d_skew_0_99_20k.txt` | 1.27 MB | 19072 (95.4%) | 0 (0.0%) | 928 (4.6%) | 0 (0.0%) | **4.6%** | 2391 | Key 1 (379, 1.9%) | 9.3% | 0.0000% | 0.0077% |
| `ycsb_workload_d_skew_1_20_20k.txt` | 1.30 MB | 18952 (94.8%) | 0 (0.0%) | 1048 (5.2%) | 0 (0.0%) | **5.2%** | 1858 | Key 1 (757, 3.8%) | 12.0% | 0.0000% | 0.0076% |
| `ycsb_workload_delete_heavy_skew_0_00_20k.txt` | 3.39 MB | 2004 (10.0%) | 4022 (20.1%) | 4060 (20.3%) | 9914 (49.6%) | **90.0%** | 8854 | Key 698 (19, 0.1%) | 4.7% | 0.0116% | 0.0128% |
| `ycsb_workload_delete_heavy_skew_0_20_20k.txt` | 3.36 MB | 2006 (10.0%) | 4039 (20.2%) | 3963 (19.8%) | 9992 (50.0%) | **90.0%** | 8648 | Key 11315 (16, 0.1%) | 4.8% | 0.0122% | 0.0136% |
| `ycsb_workload_delete_heavy_skew_0_50_20k.txt` | 3.36 MB | 1996 (10.0%) | 3914 (19.6%) | 4088 (20.4%) | 10002 (50.0%) | **90.0%** | 7861 | Key 2 (48, 0.2%) | 7.4% | 0.0183% | 0.0214% |
| `ycsb_workload_delete_heavy_skew_0_70_20k.txt` | 3.36 MB | 2019 (10.1%) | 4043 (20.2%) | 3969 (19.8%) | 9969 (49.8%) | **89.9%** | 6782 | Key 1 (336, 1.7%) | 15.9% | 0.0722% | 0.0890% |
| `ycsb_workload_delete_heavy_skew_0_80_20k.txt` | 3.37 MB | 1994 (10.0%) | 4019 (20.1%) | 4039 (20.2%) | 9948 (49.7%) | **90.0%** | 6087 | Key 1 (653, 3.3%) | 21.5% | 0.1696% | 0.2181% |
| `ycsb_workload_delete_heavy_skew_0_90_20k.txt` | 3.37 MB | 2005 (10.0%) | 3989 (19.9%) | 4085 (20.4%) | 9921 (49.6%) | **90.0%** | 5267 | Key 1 (1152, 5.8%) | 30.2% | 0.4743% | 0.5949% |
| `ycsb_workload_delete_heavy_skew_0_99_20k.txt` | 3.37 MB | 2003 (10.0%) | 4145 (20.7%) | 3977 (19.9%) | 9875 (49.4%) | **90.0%** | 4456 | Key 1 (1620, 8.1%) | 38.0% | 0.8839% | 1.1091% |
| `ycsb_workload_delete_heavy_skew_1_20_20k.txt` | 3.32 MB | 2038 (10.2%) | 3966 (19.8%) | 3996 (20.0%) | 10000 (50.0%) | **89.8%** | 2921 | Key 1 (3613, 18.1%) | 56.0% | 3.5229% | 4.4621% |
| `ycsb_workload_dml_heavy_skew_0_00_20k.txt` | 5.11 MB | 2036 (10.2%) | 9882 (49.4%) | 4056 (20.3%) | 4026 (20.1%) | **89.8%** | 9492 | Key 3639 (16, 0.1%) | 4.5% | 0.0097% | 0.0111% |
| `ycsb_workload_dml_heavy_skew_0_20_20k.txt` | 5.12 MB | 1962 (9.8%) | 10027 (50.1%) | 3956 (19.8%) | 4055 (20.3%) | **90.2%** | 9324 | Key 7953 (16, 0.1%) | 4.5% | 0.0101% | 0.0115% |
| `ycsb_workload_dml_heavy_skew_0_50_20k.txt` | 5.08 MB | 2140 (10.7%) | 9791 (49.0%) | 4048 (20.2%) | 4021 (20.1%) | **89.3%** | 8451 | Key 1 (80, 0.4%) | 8.1% | 0.0180% | 0.0218% |
| `ycsb_workload_dml_heavy_skew_0_70_20k.txt` | 5.14 MB | 1966 (9.8%) | 10026 (50.1%) | 4052 (20.3%) | 3956 (19.8%) | **90.2%** | 7379 | Key 1 (358, 1.8%) | 16.7% | 0.0771% | 0.0963% |
| `ycsb_workload_dml_heavy_skew_0_80_20k.txt` | 5.14 MB | 2000 (10.0%) | 10099 (50.5%) | 3975 (19.9%) | 3926 (19.6%) | **90.0%** | 6594 | Key 1 (583, 2.9%) | 23.0% | 0.1929% | 0.2333% |
| `ycsb_workload_dml_heavy_skew_0_90_20k.txt` | 5.12 MB | 2061 (10.3%) | 10027 (50.1%) | 4016 (20.1%) | 3896 (19.5%) | **89.7%** | 5697 | Key 1 (1172, 5.9%) | 32.6% | 0.5090% | 0.6397% |
| `ycsb_workload_dml_heavy_skew_0_99_20k.txt` | 5.17 MB | 1885 (9.4%) | 10242 (51.2%) | 3981 (19.9%) | 3892 (19.5%) | **90.6%** | 4942 | Key 1 (1734, 8.7%) | 39.1% | 0.9818% | 1.1990% |
| `ycsb_workload_dml_heavy_skew_1_20_20k.txt` | 5.06 MB | 2081 (10.4%) | 9980 (49.9%) | 3926 (19.6%) | 4013 (20.1%) | **89.6%** | 3088 | Key 1 (3697, 18.5%) | 58.5% | 3.7551% | 4.7645% |
| `ycsb_workload_f_skew_0_00_20k.txt` | 2.94 MB | 13293 (66.5%) | 6707 (33.5%) | 0 (0.0%) | 0 (0.0%) | **33.5%** | 8040 | Key 1003 (14, 0.1%) | 3.2% | 0.0009% | 0.0080% |
| `ycsb_workload_f_skew_0_20_20k.txt` | 2.91 MB | 13367 (66.8%) | 6633 (33.2%) | 0 (0.0%) | 0 (0.0%) | **33.2%** | 7938 | Key 18 (12, 0.1%) | 3.4% | 0.0010% | 0.0082% |
| `ycsb_workload_f_skew_0_50_20k.txt` | 2.91 MB | 13374 (66.9%) | 6626 (33.1%) | 0 (0.0%) | 0 (0.0%) | **33.1%** | 7113 | Key 1 (92, 0.5%) | 7.6% | 0.0022% | 0.0146% |
| `ycsb_workload_f_skew_0_70_20k.txt` | 2.92 MB | 13295 (66.5%) | 6705 (33.5%) | 0 (0.0%) | 0 (0.0%) | **33.5%** | 6015 | Key 1 (377, 1.9%) | 16.7% | 0.0131% | 0.0663% |
| `ycsb_workload_f_skew_0_80_20k.txt` | 2.90 MB | 13354 (66.8%) | 6646 (33.2%) | 0 (0.0%) | 0 (0.0%) | **33.2%** | 5286 | Key 1 (709, 3.5%) | 24.1% | 0.0329% | 0.1656% |
| `ycsb_workload_f_skew_0_90_20k.txt` | 2.92 MB | 13282 (66.4%) | 6718 (33.6%) | 0 (0.0%) | 0 (0.0%) | **33.6%** | 4442 | Key 1 (1324, 6.6%) | 33.1% | 0.0875% | 0.4355% |
| `ycsb_workload_f_skew_0_99_20k.txt` | 2.88 MB | 13374 (66.9%) | 6626 (33.1%) | 0 (0.0%) | 0 (0.0%) | **33.1%** | 3617 | Key 1 (1951, 9.8%) | 40.9% | 0.1682% | 0.8583% |
| `ycsb_workload_f_skew_1_20_20k.txt` | 2.89 MB | 13309 (66.5%) | 6691 (33.5%) | 0 (0.0%) | 0 (0.0%) | **33.5%** | 1934 | Key 1 (4123, 20.6%) | 58.7% | 0.6740% | 3.3346% |
| `ycsb_workload_pure_dml_skew_0_00_20k.txt` | 5.41 MB | 0 (0.0%) | 9926 (49.6%) | 4975 (24.9%) | 5099 (25.5%) | **100.0%** | 9221 | Key 6698 (15, 0.1%) | 4.6% | 0.0121% | 0.0121% |
| `ycsb_workload_pure_dml_skew_0_20_20k.txt` | 5.46 MB | 0 (0.0%) | 9969 (49.8%) | 5088 (25.4%) | 4943 (24.7%) | **100.0%** | 9122 | Key 122 (19, 0.1%) | 4.6% | 0.0123% | 0.0123% |
| `ycsb_workload_pure_dml_skew_0_50_20k.txt` | 5.45 MB | 0 (0.0%) | 9876 (49.4%) | 5160 (25.8%) | 4964 (24.8%) | **100.0%** | 8262 | Key 1 (79, 0.4%) | 7.8% | 0.0223% | 0.0223% |
| `ycsb_workload_pure_dml_skew_0_70_20k.txt` | 5.45 MB | 0 (0.0%) | 10033 (50.2%) | 5041 (25.2%) | 4926 (24.6%) | **100.0%** | 7203 | Key 1 (320, 1.6%) | 15.8% | 0.0858% | 0.0858% |
| `ycsb_workload_pure_dml_skew_0_80_20k.txt` | 5.44 MB | 0 (0.0%) | 10133 (50.7%) | 4931 (24.7%) | 4936 (24.7%) | **100.0%** | 6496 | Key 1 (624, 3.1%) | 23.4% | 0.2419% | 0.2419% |
| `ycsb_workload_pure_dml_skew_0_90_20k.txt` | 5.40 MB | 0 (0.0%) | 9983 (49.9%) | 4946 (24.7%) | 5071 (25.4%) | **100.0%** | 5600 | Key 1 (1077, 5.4%) | 31.0% | 0.5525% | 0.5525% |
| `ycsb_workload_pure_dml_skew_0_99_20k.txt` | 5.39 MB | 0 (0.0%) | 9999 (50.0%) | 4943 (24.7%) | 5058 (25.3%) | **100.0%** | 4788 | Key 1 (1719, 8.6%) | 40.0% | 1.2720% | 1.2720% |
| `ycsb_workload_pure_dml_skew_1_20_20k.txt` | 5.40 MB | 0 (0.0%) | 9969 (49.8%) | 5028 (25.1%) | 5003 (25.0%) | **100.0%** | 3264 | Key 1 (3631, 18.2%) | 57.1% | 4.6642% | 4.6642% |

---

## 5. AriaBC Concurrency, Replication & Engine Implications

1. **Write-Write Contention Acceleration**: In pure DML and high DML workloads at high skew ($\\theta \\ge 0.99$), Write-Write (WW) collision rates surge to **1.0%-4.7%**, causing high transaction conflict density in Aria deterministic batch reservation.
2. **Raft & Merkle Tree Pressure**: Workloads like `PURE_DML` and `DML_HEAVY` generate 100% and 90% state modifications respectively, maximizing the cryptographic hashing and delta-tree updating rate in the BCDB Merkle replication layer.
3. **PostgreSQL Vacuum & Page Reorganization**: Workload `DELETE_HEAVY` (50% deletions) aggressively creates dead heap tuples and index line pointers, stressing autovacuum, HOT (Heap-Only-Tuples) chains, and page-pruning paths.

---

## 6. How to Run

```bash
# Run DML Heavy workload with high skew (0.99)
./scripts/distributed/run_remote_gateway_ycsb.sh \
  --workloads "scripts/ycsb_suite/ycsb_workload_dml_heavy_skew_0_99_20k.txt" \
  --threads 16

# Run Delete Heavy workload with extreme skew (1.20)
./scripts/distributed/run_remote_gateway_ycsb.sh \
  --workloads "scripts/ycsb_suite/ycsb_workload_delete_heavy_skew_1_20_20k.txt" \
  --threads 16

# Run Pure DML (100% Writes) uniform benchmark
./scripts/distributed/run_remote_gateway_ycsb.sh \
  --workloads "scripts/ycsb_suite/ycsb_workload_pure_dml_skew_0_00_20k.txt" \
  --threads 16
```