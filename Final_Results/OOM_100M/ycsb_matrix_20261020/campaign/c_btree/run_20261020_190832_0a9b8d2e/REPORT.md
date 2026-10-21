# Large-database cold-start benchmark

Only completed, successful cases appear below. Merkle cases require full verification.
TPS uses the same gateway `overall time taken` denominator as the historical single-node suite.
32MB shared buffers does not bound OS page cache. Device I/O includes other filesystem traffic.
Checkpoint writeback is measured separately after the timed workload. Units are MiB.

| Workload | Skew | Mode | Workers | Trials | Median TPS | Min TPS | Max TPS | Median read MiB |
|---|---:|---|---:|---:|---:|---:|---:|---:|
| c | 0.0 | bcdb_merkle | 16 | 1 | 16428.90 | 16428.90 | 16428.90 | 21640.71 |
| c | 0.5 | bcdb_merkle | 16 | 1 | 17127.44 | 17127.44 | 17127.44 | 20330.46 |
| c | 0.99 | bcdb_merkle | 16 | 1 | 27210.88 | 27210.88 | 27210.88 | 5809.97 |
| c | 1.2 | bcdb_merkle | 16 | 1 | 35558.19 | 35558.19 | 35558.19 | 1293.15 |
