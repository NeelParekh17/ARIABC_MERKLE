# Large-database cold-start benchmark

Only completed, successful cases appear below. Merkle cases require full verification.
TPS uses the same gateway `overall time taken` denominator as the historical single-node suite.
32MB shared buffers does not bound OS page cache. Device I/O includes other filesystem traffic.
Checkpoint writeback is measured separately after the timed workload. Units are MiB.

| Workload | Skew | Mode | Workers | Trials | Median TPS | Min TPS | Max TPS | Median read MiB |
|---|---:|---|---:|---:|---:|---:|---:|---:|
| a | 0.0 | bcdb_det | 1 | 1 | 1587.93 | 1587.93 | 1587.93 | 322.19 |
| a | 0.0 | bcdb_det | 16 | 1 | 5053.06 | 5053.06 | 5053.06 | 321.93 |
| f | 0.99 | bcdb_det | 1 | 1 | 2025.11 | 2025.11 | 2025.11 | 142.08 |
| f | 0.99 | bcdb_det | 16 | 1 | 7535.80 | 7535.80 | 7535.80 | 141.88 |
