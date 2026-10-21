# Large-database cold-start benchmark

Only completed, successful cases appear below. Merkle cases require full verification.
TPS uses the same gateway `overall time taken` denominator as the historical single-node suite.
32MB shared buffers does not bound OS page cache. Device I/O includes other filesystem traffic.
Checkpoint writeback is measured separately after the timed workload. Units are MiB.

| Workload | Skew | Mode | Workers | Trials | Median TPS | Min TPS | Max TPS | Median read MiB |
|---|---:|---|---:|---:|---:|---:|---:|---:|
| f | 0.0 | bcdb_merkle | 16 | 1 | 2758.74 | 2758.74 | 2758.74 | 21989.99 |
| f | 0.5 | bcdb_merkle | 16 | 1 | 2517.11 | 2517.11 | 2517.11 | 20666.27 |
| f | 0.99 | bcdb_merkle | 16 | 1 | 4680.73 | 4680.73 | 4680.73 | 6141.57 |
| f | 1.2 | bcdb_merkle | 16 | 1 | 5224.17 | 5224.17 | 5224.17 | 1569.89 |
