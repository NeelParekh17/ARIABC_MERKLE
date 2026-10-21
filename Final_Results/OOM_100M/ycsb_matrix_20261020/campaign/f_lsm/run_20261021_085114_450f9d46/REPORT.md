# Large-database cold-start benchmark

Only completed, successful cases appear below. Merkle cases require full verification.
TPS uses the same gateway `overall time taken` denominator as the historical single-node suite.
32MB shared buffers does not bound OS page cache. Device I/O includes other filesystem traffic.
Checkpoint writeback is measured separately after the timed workload. Units are MiB.

| Workload | Skew | Mode | Workers | Trials | Median TPS | Min TPS | Max TPS | Median read MiB |
|---|---:|---|---:|---:|---:|---:|---:|---:|
| f | 0.0 | bcdb_merkle | 16 | 1 | 2815.11 | 2815.11 | 2815.11 | 22156.18 |
| f | 0.5 | bcdb_merkle | 16 | 1 | 2487.90 | 2487.90 | 2487.90 | 20414.19 |
| f | 0.99 | bcdb_merkle | 16 | 1 | 4681.58 | 4681.58 | 4681.58 | 5871.83 |
| f | 1.2 | bcdb_merkle | 16 | 1 | 5428.18 | 5428.18 | 5428.18 | 1324.59 |
