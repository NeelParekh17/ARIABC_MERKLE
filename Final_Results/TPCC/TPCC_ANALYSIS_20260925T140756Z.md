# TPC-C scaling measurements

Reported PG is the PostgreSQL execution path in the custom AriaBC build; consult per-attempt version and hashes.
Measured tables must be logged and synchronous_commit/fsync/full_page_writes enabled.
Historical campaigns without this evidence remain unqualified and are not upgraded by this report.
Local Merkle verification does not establish distributed agreement or serializability.
Throughput ordering is not a correctness invariant. Retrying transactions can still finish without permanent failures.

## Worker sweep

Campaign: /home/neel/ARIABC/AriaBC/Final_Results/reruns/20260923T153000Z_campaign/TPCC/workers_w100

# Measurement qualification

Successful SQL/verification does not establish stable throughput or serializability.
A throughput ordering is not a correctness invariant. No observations are discarded.
Five repeats is the minimum qualification check; CV <= 10% is a diagnostic, not a confidence interval.

| Configuration | Trials | Median TPS | Min | Max | Sample CV % | Status |
|---|---:|---:|---:|---:|---:|---|
| bcdb_det / tpcc-workload-20000-w100-seed42.txt / 100 / 16 | 3 | 1738.07 | 1732.80 | 1747.34 | 0.42 | insufficient_repeats |
| bcdb_det / tpcc-workload-20000-w100-seed42.txt / 100 / 24 | 3 | 1810.12 | 1805.22 | 1832.84 | 0.81 | insufficient_repeats |
| bcdb_det / tpcc-workload-20000-w100-seed42.txt / 100 / 32 | 3 | 1750.70 | 1739.89 | 1757.47 | 0.51 | insufficient_repeats |
| bcdb_det / tpcc-workload-20000-w100-seed42.txt / 100 / 8 | 3 | 1320.74 | 1306.68 | 1331.03 | 0.93 | insufficient_repeats |
| bcdb_merkle / tpcc-workload-20000-w100-seed42.txt / 100 / 16 | 3 | 567.50 | 566.54 | 568.08 | 0.14 | insufficient_repeats |
| bcdb_merkle / tpcc-workload-20000-w100-seed42.txt / 100 / 24 | 3 | 544.53 | 269.84 | 565.23 | 35.86 | insufficient_repeats |
| bcdb_merkle / tpcc-workload-20000-w100-seed42.txt / 100 / 32 | 3 | 553.10 | 323.08 | 555.05 | 27.96 | insufficient_repeats |
| bcdb_merkle / tpcc-workload-20000-w100-seed42.txt / 100 / 8 | 3 | 324.15 | 220.56 | 529.09 | 43.87 | insufficient_repeats |
| pg / tpcc-workload-20000-w100-seed42.txt / 100 / 16 | 3 | 2743.48 | 2738.60 | 2744.61 | 0.12 | insufficient_repeats |
| pg / tpcc-workload-20000-w100-seed42.txt / 100 / 24 | 3 | 3469.81 | 1899.70 | 3564.43 | 31.40 | insufficient_repeats |
| pg / tpcc-workload-20000-w100-seed42.txt / 100 / 32 | 3 | 4326.20 | 4230.12 | 4517.73 | 3.36 | insufficient_repeats |
| pg / tpcc-workload-20000-w100-seed42.txt / 100 / 8 | 3 | 1642.98 | 1640.55 | 1654.26 | 0.44 | insufficient_repeats |


## Warehouse sweep

Campaign: /home/neel/ARIABC/AriaBC/Final_Results/reruns/20260923T153000Z_campaign/TPCC/warehouses_w32

# Measurement qualification

Successful SQL/verification does not establish stable throughput or serializability.
A throughput ordering is not a correctness invariant. No observations are discarded.
Five repeats is the minimum qualification check; CV <= 10% is a diagnostic, not a confidence interval.

| Configuration | Trials | Median TPS | Min | Max | Sample CV % | Status |
|---|---:|---:|---:|---:|---:|---|
| bcdb_det / tpcc-workload-20000-w10-seed42.txt / 10 / 32 | 3 | 685.31 | 623.81 | 696.55 | 5.86 | insufficient_repeats |
| bcdb_det / tpcc-workload-20000-w100-seed42.txt / 100 / 32 | 3 | 1749.93 | 1113.52 | 1758.24 | 24.01 | insufficient_repeats |
| bcdb_det / tpcc-workload-20000-w20-seed42.txt / 20 / 32 | 3 | 906.74 | 805.87 | 907.07 | 6.68 | insufficient_repeats |
| bcdb_det / tpcc-workload-20000-w30-seed42.txt / 30 / 32 | 3 | 1086.43 | 1083.01 | 1088.79 | 0.27 | insufficient_repeats |
| bcdb_det / tpcc-workload-20000-w5-seed42.txt / 5 / 32 | 3 | 551.44 | 549.54 | 554.74 | 0.48 | insufficient_repeats |
| bcdb_det / tpcc-workload-20000-w50-seed42.txt / 50 / 32 | 3 | 1324.94 | 1314.15 | 1325.38 | 0.48 | insufficient_repeats |
| bcdb_det / tpcc-workload-20000-w75-seed42.txt / 75 / 32 | 3 | 1589.95 | 1577.78 | 1590.96 | 0.46 | insufficient_repeats |
| bcdb_merkle / tpcc-workload-20000-w10-seed42.txt / 10 / 32 | 3 | 427.51 | 427.42 | 427.71 | 0.03 | insufficient_repeats |
| bcdb_merkle / tpcc-workload-20000-w100-seed42.txt / 100 / 32 | 3 | 553.94 | 306.27 | 555.54 | 30.40 | insufficient_repeats |
| bcdb_merkle / tpcc-workload-20000-w20-seed42.txt / 20 / 32 | 3 | 404.83 | 404.08 | 405.12 | 0.13 | insufficient_repeats |
| bcdb_merkle / tpcc-workload-20000-w30-seed42.txt / 30 / 32 | 3 | 483.54 | 482.30 | 483.66 | 0.16 | insufficient_repeats |
| bcdb_merkle / tpcc-workload-20000-w5-seed42.txt / 5 / 32 | 3 | 281.31 | 280.27 | 352.89 | 13.66 | insufficient_repeats |
| bcdb_merkle / tpcc-workload-20000-w50-seed42.txt / 50 / 32 | 3 | 373.69 | 367.14 | 382.31 | 2.03 | insufficient_repeats |
| bcdb_merkle / tpcc-workload-20000-w75-seed42.txt / 75 / 32 | 3 | 541.59 | 381.61 | 542.08 | 18.94 | insufficient_repeats |
| pg / tpcc-workload-20000-w10-seed42.txt / 10 / 32 | 3 | 2849.41 | 2698.69 | 2959.02 | 4.61 | insufficient_repeats |
| pg / tpcc-workload-20000-w100-seed42.txt / 100 / 32 | 3 | 4335.57 | 4203.45 | 4442.47 | 2.77 | insufficient_repeats |
| pg / tpcc-workload-20000-w20-seed42.txt / 20 / 32 | 3 | 3511.85 | 3376.67 | 3647.64 | 3.86 | insufficient_repeats |
| pg / tpcc-workload-20000-w30-seed42.txt / 30 / 32 | 3 | 4040.40 | 3920.03 | 4137.36 | 2.70 | insufficient_repeats |
| pg / tpcc-workload-20000-w5-seed42.txt / 5 / 32 | 3 | 1253.29 | 1084.19 | 1796.78 | 27.02 | insufficient_repeats |
| pg / tpcc-workload-20000-w50-seed42.txt / 50 / 32 | 3 | 4111.00 | 3901.68 | 4200.80 | 3.77 | insufficient_repeats |
| pg / tpcc-workload-20000-w75-seed42.txt / 75 / 32 | 3 | 4304.78 | 4142.50 | 4403.35 | 3.07 | insufficient_repeats |
