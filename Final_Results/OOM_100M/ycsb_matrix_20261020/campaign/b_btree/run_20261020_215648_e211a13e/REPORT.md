# Large-database cold-start benchmark

Only completed, successful cases appear below. Merkle cases require full verification.
TPS uses the same gateway `overall time taken` denominator as the historical single-node suite.
32MB shared buffers does not bound OS page cache. Device I/O includes other filesystem traffic.
Checkpoint writeback is measured separately after the timed workload. Units are MiB.

| Workload | Skew | Mode | Workers | Trials | Median TPS | Min TPS | Max TPS | Median read MiB |
|---|---:|---|---:|---:|---:|---:|---:|---:|
| b | 0.0 | bcdb_merkle | 16 | 1 | 8519.04 | 8519.04 | 8519.04 | 22042.95 |
| b | 0.5 | bcdb_merkle | 16 | 1 | 7782.05 | 7782.05 | 7782.05 | 20754.55 |
| b | 0.99 | bcdb_merkle | 16 | 1 | 14789.71 | 14789.71 | 14789.71 | 5847.88 |
| b | 1.2 | bcdb_merkle | 16 | 1 | 20774.36 | 20774.36 | 20774.36 | 1332.50 |
