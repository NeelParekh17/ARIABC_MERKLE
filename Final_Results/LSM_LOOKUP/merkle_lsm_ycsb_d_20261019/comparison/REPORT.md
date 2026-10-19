# Large-database cold-start benchmark

Only completed, successful cases appear below. Merkle cases require full verification.
TPS uses the same gateway `overall time taken` denominator as the historical single-node suite.
32MB shared buffers does not bound OS page cache. Device I/O includes other filesystem traffic.
Checkpoint writeback is measured separately after the timed workload. Units are MiB.

| Workload | Skew | Mode | Workers | Trials | Median TPS | Min TPS | Max TPS | Median read MiB |
|---|---:|---|---:|---:|---:|---:|---:|---:|
| d | 0.99 | bcdb_det | 16 | 3 | 18436.29 | 18422.43 | 18462.96 | 5684.17 |
| d | 0.99 | bcdb_merkle | 16 | 3 | 13090.72 | 12355.72 | 13160.71 | 6729.88 |
| d | 0.99 | pg | 16 | 3 | 30675.63 | 30670.53 | 30720.39 | 5668.63 |
