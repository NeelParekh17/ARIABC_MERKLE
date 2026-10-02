# Large-database cold-start benchmark

Only completed, successful cases appear below. Merkle cases require full verification.
TPS uses the same gateway `overall time taken` denominator as the historical single-node suite.
32MB shared buffers does not bound OS page cache. Device I/O includes other filesystem traffic.
Checkpoint writeback is measured separately after the timed workload. Units are MiB.

| Workload | Skew | Mode | Workers | Trials | Median TPS | Min TPS | Max TPS | Median read MiB |
|---|---:|---|---:|---:|---:|---:|---:|---:|
| a | 0.0 | bcdb_merkle | 1 | 1 | 1147.45 | 1147.45 | 1147.45 | 444.35 |
| a | 0.0 | bcdb_merkle | 16 | 1 | 3639.01 | 3639.01 | 3639.01 | 444.26 |
| f | 0.99 | bcdb_merkle | 1 | 1 | 1588.94 | 1588.94 | 1588.94 | 225.91 |
| f | 0.99 | bcdb_merkle | 16 | 1 | 5082.59 | 5082.59 | 5082.59 | 225.98 |
