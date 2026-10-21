# Large-database cold-start benchmark

Only completed, successful cases appear below. Merkle cases require full verification.
TPS uses the same gateway `overall time taken` denominator as the historical single-node suite.
32MB shared buffers does not bound OS page cache. Device I/O includes other filesystem traffic.
Checkpoint writeback is measured separately after the timed workload. Units are MiB.

| Workload | Skew | Mode | Workers | Trials | Median TPS | Min TPS | Max TPS | Median read MiB |
|---|---:|---|---:|---:|---:|---:|---:|---:|
| a | 0.0 | bcdb_merkle | 16 | 1 | 2579.50 | 2579.50 | 2579.50 | 22363.89 |
| a | 0.5 | bcdb_merkle | 16 | 1 | 2638.95 | 2638.95 | 2638.95 | 20870.12 |
| a | 0.99 | bcdb_merkle | 16 | 1 | 4434.35 | 4434.35 | 4434.35 | 5867.09 |
| a | 1.2 | bcdb_merkle | 16 | 1 | 5546.54 | 5546.54 | 5546.54 | 1333.66 |
