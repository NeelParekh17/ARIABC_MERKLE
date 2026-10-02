# Large-database cold-start benchmark

Only completed, successful cases appear below. Merkle cases require full verification.
TPS uses the same gateway `overall time taken` denominator as the historical single-node suite.
32MB shared buffers does not bound OS page cache. Device I/O includes other filesystem traffic.
Checkpoint writeback is measured separately after the timed workload. Units are MiB.

| Workload | Skew | Mode | Workers | Trials | Median TPS | Min TPS | Max TPS | Median read MiB |
|---|---:|---|---:|---:|---:|---:|---:|---:|
| a | 0.99 | bcdb_det | 8 | 1 | 5780.35 | 5780.35 | 5780.35 | 13.86 |
| a | 0.99 | bcdb_merkle | 8 | 1 | 5847.95 | 5847.95 | 5847.95 | 15.33 |
| a | 0.99 | pg | 8 | 1 | 3448.28 | 3448.28 | 3448.28 | 13.82 |
