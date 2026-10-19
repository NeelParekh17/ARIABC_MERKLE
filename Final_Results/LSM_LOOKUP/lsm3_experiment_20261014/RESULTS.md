# LSM lookup results

Accepted cases: 75/75 (smoke 3 + ff90 72). Complete: True. Preserved current-stage attempts: 75. Missing configurations are incomplete, not zeros. Smoke is separate from the grid.

| Stage | FF | Theta | W | Mode | n | Median TPS | Min | Max |
|---|---:|---:|---:|---|---:|---:|---:|---:|
| ff90 | 90 | 0.0 | 1 | det | 3 | 1401.64 | 1384.85 | 1473.84 |
| ff90 | 90 | 0.0 | 1 | merkle_btree | 3 | 1321.35 | 1310.27 | 1375.71 |
| ff90 | 90 | 0.0 | 1 | merkle_lsm | 3 | 1308.99 | 1302.00 | 1382.93 |
| ff90 | 90 | 0.0 | 4 | det | 3 | 2777.39 | 2740.48 | 2824.46 |
| ff90 | 90 | 0.0 | 4 | merkle_btree | 3 | 2585.98 | 2567.72 | 2679.89 |
| ff90 | 90 | 0.0 | 4 | merkle_lsm | 3 | 2643.75 | 2641.31 | 2718.87 |
| ff90 | 90 | 0.0 | 8 | det | 3 | 3723.01 | 3716.78 | 3865.48 |
| ff90 | 90 | 0.0 | 8 | merkle_btree | 3 | 3562.52 | 3522.99 | 3731.34 |
| ff90 | 90 | 0.0 | 8 | merkle_lsm | 3 | 3579.74 | 3575.90 | 3750.23 |
| ff90 | 90 | 0.0 | 16 | det | 3 | 4907.98 | 4891.17 | 5085.18 |
| ff90 | 90 | 0.0 | 16 | merkle_btree | 3 | 4917.63 | 4618.94 | 4922.47 |
| ff90 | 90 | 0.0 | 16 | merkle_lsm | 3 | 4867.36 | 4466.28 | 4943.15 |
| ff90 | 90 | 0.99 | 1 | det | 3 | 2024.70 | 1933.86 | 2026.34 |
| ff90 | 90 | 0.99 | 1 | merkle_btree | 3 | 1845.02 | 1772.42 | 1854.60 |
| ff90 | 90 | 0.99 | 1 | merkle_lsm | 3 | 1796.95 | 1747.34 | 1859.43 |
| ff90 | 90 | 0.99 | 4 | det | 3 | 3557.45 | 3526.71 | 3700.28 |
| ff90 | 90 | 0.99 | 4 | merkle_btree | 3 | 3314.00 | 3309.07 | 3330.00 |
| ff90 | 90 | 0.99 | 4 | merkle_lsm | 3 | 3317.30 | 3294.35 | 3325.57 |
| ff90 | 90 | 0.99 | 8 | det | 3 | 5023.86 | 5012.53 | 5205.62 |
| ff90 | 90 | 0.99 | 8 | merkle_btree | 3 | 4593.48 | 4587.16 | 4757.37 |
| ff90 | 90 | 0.99 | 8 | merkle_lsm | 3 | 4614.67 | 4611.48 | 4795.01 |
| ff90 | 90 | 0.99 | 16 | det | 3 | 7158.20 | 6846.97 | 7183.91 |
| ff90 | 90 | 0.99 | 16 | merkle_btree | 3 | 6291.29 | 6195.79 | 6518.90 |
| ff90 | 90 | 0.99 | 16 | merkle_lsm | 3 | 6215.04 | 6053.27 | 6553.08 |
| smoke | 90 | 0.0 | 8 | det | 1 | 3703.02 | 3703.02 | 3703.02 |
| smoke | 90 | 0.0 | 8 | merkle_btree | 1 | 3747.42 | 3747.42 | 3747.42 |
| smoke | 90 | 0.0 | 8 | merkle_lsm | 1 | 3527.96 | 3527.96 | 3527.96 |

| Stage | FF | Theta | W | Btree/det | LSM/det | Gap closed |
|---|---:|---:|---:|---:|---:|---:|
| ff90 | 90 | 0.0 | 1 | 0.9427 | 0.9339 | -15.4% |
| ff90 | 90 | 0.0 | 4 | 0.9311 | 0.9519 | 30.2% |
| ff90 | 90 | 0.0 | 8 | 0.9569 | 0.9615 | 10.7% |
| ff90 | 90 | 0.0 | 16 | 1.0020 | 0.9917 | 520.7% |
| ff90 | 90 | 0.99 | 1 | 0.9113 | 0.8875 | -26.8% |
| ff90 | 90 | 0.99 | 4 | 0.9316 | 0.9325 | 1.4% |
| ff90 | 90 | 0.99 | 8 | 0.9143 | 0.9186 | 4.9% |
| ff90 | 90 | 0.99 | 16 | 0.8789 | 0.8682 | -8.8% |
| smoke | 90 | 0.0 | 8 | 1.0120 | 0.9527 | 494.2% |

Per-point counters below are medians of the accepted ff90 trials; block counters are 8 KiB PostgreSQL buffer accesses. Device KB uses 1024-byte units. HOT is the fraction of updates; lookup inserts are inferred, not AM callback instrumentation.

| Theta | W | Mode | HOT % | Non-HOT | Lookup reads/hits | Node heap/index reads | Node heap/index hits | WAL KiB/stmt | FPI/stmt | Device R/W KiB/stmt | Merges completed (sum) |
|---|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 0.0 | 1 | det | 100.00 | 0 | 0/0 | 0/0 | 0/0 | 3.758 | 0.4980 | 16.691/10.356 | 0 |
| 0.0 | 1 | merkle_btree | 100.00 | 0 | 1/0 | 8492/3916 | 81340/85932 | 4.918 | 0.6464 | 18.229/11.830 | 0 |
| 0.0 | 1 | merkle_lsm | 100.00 | 0 | 0/0 | 8502/3919 | 81330/85929 | 4.918 | 0.6465 | 18.241/11.872 | 0 |
| 0.0 | 4 | det | 100.00 | 0 | 0/0 | 0/0 | 0/0 | 3.758 | 0.4980 | 16.685/8.564 | 0 |
| 0.0 | 4 | merkle_btree | 100.00 | 0 | 1/3 | 8462/3868 | 81463/86069 | 4.918 | 0.6464 | 18.236/9.868 | 0 |
| 0.0 | 4 | merkle_lsm | 100.00 | 0 | 0/0 | 8468/3873 | 81456/86065 | 4.918 | 0.6465 | 18.250/9.829 | 0 |
| 0.0 | 8 | det | 100.00 | 0 | 0/0 | 0/0 | 0/0 | 3.758 | 0.4980 | 16.487/7.167 | 0 |
| 0.0 | 8 | merkle_btree | 100.00 | 0 | 1/7 | 8443/3845 | 81607/86227 | 4.917 | 0.6464 | 18.235/8.444 | 0 |
| 0.0 | 8 | merkle_lsm | 100.00 | 0 | 0/0 | 8448/3853 | 81611/86220 | 4.917 | 0.6465 | 18.232/8.434 | 0 |
| 0.0 | 16 | det | 100.00 | 0 | 0/0 | 0/0 | 0/0 | 3.758 | 0.4980 | 16.678/6.028 | 0 |
| 0.0 | 16 | merkle_btree | 100.00 | 0 | 1/15 | 8433/3837 | 81893/86536 | 4.916 | 0.6464 | 18.228/7.307 | 0 |
| 0.0 | 16 | merkle_lsm | 100.00 | 0 | 0/0 | 8433/3840 | 81907/86547 | 4.916 | 0.6465 | 18.228/7.305 | 0 |
| 0.99 | 1 | det | 100.00 | 0 | 0/0 | 0/0 | 0/0 | 2.034 | 0.2571 | 7.517/7.633 | 0 |
| 0.99 | 1 | merkle_btree | 100.00 | 0 | 1/0 | 5743/2136 | 75095/87937 | 3.155 | 0.3992 | 9.056/9.148 | 0 |
| 0.99 | 1 | merkle_lsm | 100.00 | 0 | 0/0 | 5753/2141 | 75085/87932 | 3.155 | 0.3993 | 9.050/9.221 | 0 |
| 0.99 | 4 | det | 99.96 | 4 | 0/0 | 0/0 | 0/0 | 2.032 | 0.2571 | 7.509/6.157 | 0 |
| 0.99 | 4 | merkle_btree | 99.97 | 3 | 12/4 | 5666/2126 | 77325/88089 | 3.154 | 0.3995 | 9.063/7.555 | 0 |
| 0.99 | 4 | merkle_lsm | 99.96 | 4 | 4/3 | 5666/2128 | 77322/88084 | 3.153 | 0.3994 | 9.050/7.574 | 0 |
| 0.99 | 8 | det | 99.94 | 6 | 0/0 | 0/0 | 0/0 | 2.031 | 0.2571 | 7.516/4.882 | 0 |
| 0.99 | 8 | merkle_btree | 99.95 | 5 | 15/13 | 5632/2116 | 78509/88211 | 3.152 | 0.3996 | 9.058/6.265 | 0 |
| 0.99 | 8 | merkle_lsm | 99.95 | 5 | 2/8 | 5635/2118 | 78518/88214 | 3.150 | 0.3994 | 9.051/6.228 | 0 |
| 0.99 | 16 | det | 99.87 | 13 | 0/0 | 0/0 | 0/0 | 2.029 | 0.2571 | 7.496/3.942 | 0 |
| 0.99 | 16 | merkle_btree | 99.86 | 14 | 40/31 | 5608/2103 | 79797/88460 | 3.151 | 0.4000 | 9.071/5.296 | 0 |
| 0.99 | 16 | merkle_lsm | 99.86 | 14 | 5/18 | 5609/2102 | 79796/88458 | 3.147 | 0.3994 | 9.053/5.254 | 0 |

Exact activity counts summed across the three trials at each point (the per-trial CSV retains every value). Lookup inserts = usertable inserts + non-HOT updates; there are no usertable INSERT statements in workload A.

| Theta | W | det HOT/updates | Btree HOT/updates | Btree lookup inserts | LSM HOT/updates | LSM lookup inserts | LSM requested/completed merges | LSM top0 end bytes min-max | LSM top1 end bytes min-max |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 0.0 | 1 | 29949/29949 | 29949/29949 | 0 | 29949/29949 | 0 | 0/0 | 8192-8192 | 8192-8192 |
| 0.0 | 4 | 29949/29949 | 29949/29949 | 0 | 29949/29949 | 0 | 0/0 | 8192-8192 | 8192-8192 |
| 0.0 | 8 | 29949/29949 | 29949/29949 | 0 | 29949/29949 | 0 | 0/0 | 8192-8192 | 8192-8192 |
| 0.0 | 16 | 29949/29949 | 29949/29949 | 0 | 29949/29949 | 0 | 0/0 | 8192-8192 | 8192-8192 |
| 0.99 | 1 | 30024/30024 | 30024/30024 | 0 | 30024/30024 | 0 | 0/0 | 8192-8192 | 8192-8192 |
| 0.99 | 4 | 30012/30024 | 30013/30024 | 11 | 30013/30024 | 11 | 0/0 | 16384-16384 | 8192-8192 |
| 0.99 | 8 | 30007/30024 | 30008/30024 | 16 | 30009/30024 | 15 | 0/0 | 16384-16384 | 8192-8192 |
| 0.99 | 16 | 29986/30024 | 29981/30024 | 43 | 29979/30024 | 45 | 0/0 | 16384-16384 | 8192-8192 |

Observed ff90 LSM merges: 0 requested, 0 completed.

Gap closed = (LSM - Btree)/(det - Btree), using matched medians. It is unstable when the original gap is small; negative values and values over 100% are retained. When Btree is already as fast as or faster than det, the formula is reported but there is no positive det advantage to close. No causal claim is based on smoke.

Counter attribution: lookup reads/hits sum base and both tops. Lookup inserts are inferred from usertable inserts + non-HOT updates, not measured AM calls. WAL includes compaction completed before the explicit drain; drain time and drain-inclusive TPS are separate from gateway TPS. FPI counts come from decoding the measured WAL interval. Device I/O includes unrelated traffic on that device and sampler/launch overhead around the gateway window; both durations are saved. Checkpoint and restore times are saved separately. PSI averages and maximum sampler gaps are recorded per case; accepted cases pass the same predeclared severe-stall rule in every mode.

Compaction behaviour was not studied by user choice: ff100 baselines/grid and sustained runs were cancelled. The complete authorized dataset is smoke:3 plus ff90:72. Merge requests/completions and end top sizes observed in these short ff90 runs are reported; they do not establish sustained compaction performance.

## Interpretation

Replacing the lookup B-tree with lsm3 did not consistently bring Merkle TPS closer to det at ff90. Four points have higher LSM medians and four have lower medians; LSM/B-tree changes range from -2.61% to +2.23%. Their observed trial min–max ranges overlap at every point. This is a three-trial comparison, not a formal equivalence test or a causal estimate of an index-write benefit.

All θ0 updates were HOT: every mode/point had 29,949 HOT updates out of 29,949 across its three trials, and neither Merkle lookup received inferred inserts. At θ0.99, lookup inserts summed across three trials were B-tree/LSM: w1=0/0, w4=11/11, w8=16/15, w16=43/45. Across all 24 grid cases per Merkle mode, that is only 70 B-tree and 71 LSM lookup insertions out of 239,892 updates. The proposed random-leaf write bottleneck is almost absent in this ff90 window. Lookup inserts are inferred from usertable inserts plus non-HOT updates; they are not direct AM callback counts.

At θ0/w8, median lookup buffer reads/hits are B-tree 1/7 and LSM 0/0. Merkle node heap/index buffer reads remain 8,443/3,845 versus 8,448/3,853, with about 81.6K/86.2K node heap/index hits in either mode. WAL is 5,035.018 versus 5,035.138 bytes/statement, and FPIs are 0.64645 versus 0.64650 per statement. det uses 3,848.530 bytes and 0.49805 FPIs per statement. Device read/write KiB per statement are det 16.487/7.167, B-tree 18.235/8.444, LSM 18.232/8.434. The remaining Merkle traffic is essentially unchanged by replacing the nearly idle lookup index.

At θ0.99/w16, median lookup reads/hits fall from 40/31 to 5/18, but node heap/index reads are 5,608/2,103 versus 5,609/2,102. WAL falls only from 3,226.639 to 3,222.380 bytes/statement (0.13%), FPIs from 0.39995 to 0.39940, and device reads from 9.0714 to 9.0526 KiB/statement (0.21%). LSM median TPS is nevertheless 1.21% lower. At θ0/w4, the arithmetic gap closed is 30.2%, despite zero inferred lookup inserts and no reduction in WAL or device reads. These counters do not support attributing the small, mixed TPS differences to avoided random lookup-index writes.

No LSM grid run requested or completed a merge: 0 in every one of the 24 runs, also 0 in smoke. For θ0 and θ0.99/w1, both tops end at 8,192 bytes in every trial. For θ0.99/w4,w8,w16, top0 ends at 16,384 bytes and top1 at 8,192 bytes in every trial. The nominal 1 MiB threshold and the 65,536-insert check period were never exercised by this short ff90 workload. Compaction behaviour was not studied by user choice; ff100 and sustained runs were cancelled. The small correctness tests exercised automatic merges, which is a separate claim from sustained 100M performance.

At θ0/w16, B-tree is already 9.65 TPS faster than det. The raw formula yields 520.7% because its denominator is negative and tiny; there is no positive det–B-tree gap to close. This value must not be interpreted as an improvement. The full-precision raw formula is retained in ratios.csv.

## Acceptance, variability and bounds

The final dataset is exactly smoke 3 plus ff90 72: 75 accepted attempts, 1,500,000 validated request/row results, gateway exits 0, divergence 0, permanent failures 0, and 50/50 full Merkle verifications. No final-build grid failure, noise exclusion or noise rerun occurred. The three earlier failed smoke campaigns remain under results/failed_attempts, including one completed B-tree case on the previous module that is excluded from the final same-build dataset.

Across the 24 grid configurations, TPS CV ranges from 0.33% to 5.38%. No severe-stall rule fired: maximum sampler gap is below 1.001s, database full-memory PSI is 0%, and database full-I/O PSI averages span 15.59–39.17% across accepted cases. Normal synchronous-I/O waiting remains visible. The rule excludes sampler gaps>=10s, full-I/O PSI>=95% or full-memory PSI>=50% over>=10s, and telemetry errors uniformly across modes; shorter fluctuations remain in all trials.

The heap is 28.25 GB, larger than the approximately15 GiB RAM host; shared_buffers is 32 MiB and each case drops OS caches after restore/validation/device settling. This is database-larger-than-memory cold-start evidence. It does not show that the short workload's working set exceeds RAM. PostgreSQL buffer reads are shared-buffer misses and may hit the OS cache; they are not device reads. Device counters include other traffic and sampler/launch time around the gateway interval; checkpoint/drain/restore costs are retained separately.

All modes use the same PostgreSQL/server/gateway/module hashes and harness source contract, SERIALIZABLE, 96 terminals/96 client workers, 200 partitions/fanout 32/split 1024/merge256, and identical published workloads. All modes preload lsm3. The equally vacuumed/analysed baselines have matching all-visible heap state; that normalization differs from the historical published controls and is disclosed. Smoke met the±10% drift gate. Small tests do not establish arbitrary concurrent SQL, extension crash safety or end-to-end replica repair.
