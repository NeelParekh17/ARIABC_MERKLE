# Large-database cold-start benchmark

Only completed, successful cases appear below. Merkle cases require full verification.
TPS uses the same gateway `overall time taken` denominator as the historical single-node suite.
32MB shared buffers does not bound OS page cache. Device I/O includes other filesystem traffic.
Checkpoint writeback is measured separately after the timed workload. Units are MiB.

| Workload | Skew | Mode | Workers | Trials | Median TPS | Min TPS | Max TPS | Median read MiB |
|---|---:|---|---:|---:|---:|---:|---:|---:|
| a | 0.0 | bcdb_merkle | 1 | 1 | 965.58 | 965.58 | 965.58 | 444.75 |
| a | 0.0 | bcdb_merkle | 16 | 1 | 4026.58 | 4026.58 | 4026.58 | 444.26 |
| f | 0.99 | bcdb_merkle | 1 | 1 | 1503.76 | 1503.76 | 1503.76 | 225.52 |
| f | 0.99 | bcdb_merkle | 16 | 1 | 5165.29 | 5165.29 | 5165.29 | 225.93 |
