# Large-database cold-start benchmark

Only completed, successful cases appear below. Merkle cases require full verification.
TPS uses the same gateway `overall time taken` denominator as the historical single-node suite.
32MB shared buffers does not bound OS page cache. Device I/O includes other filesystem traffic.
Checkpoint writeback is measured separately after the timed workload. Units are MiB.

| Workload | Skew | Mode | Workers | Trials | Median TPS | Min TPS | Max TPS | Median read MiB |
|---|---:|---|---:|---:|---:|---:|---:|---:|
| a | 0.0 | bcdb_merkle | 16 | 1 | 2801.59 | 2801.59 | 2801.59 | 22531.28 |
| a | 0.5 | bcdb_merkle | 16 | 1 | 2612.81 | 2612.81 | 2612.81 | 20475.86 |
| a | 0.99 | bcdb_merkle | 16 | 1 | 4765.14 | 4765.14 | 4765.14 | 5864.36 |
| a | 1.2 | bcdb_merkle | 16 | 1 | 5520.68 | 5520.68 | 5520.68 | 1335.10 |
