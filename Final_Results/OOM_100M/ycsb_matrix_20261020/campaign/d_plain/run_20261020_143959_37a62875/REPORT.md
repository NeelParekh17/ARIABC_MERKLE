# Large-database cold-start benchmark

Only completed, successful cases appear below. Merkle cases require full verification.
TPS uses the same gateway `overall time taken` denominator as the historical single-node suite.
32MB shared buffers does not bound OS page cache. Device I/O includes other filesystem traffic.
Checkpoint writeback is measured separately after the timed workload. Units are MiB.

| Workload | Skew | Mode | Workers | Trials | Median TPS | Min TPS | Max TPS | Median read MiB |
|---|---:|---|---:|---:|---:|---:|---:|---:|
| d | 0.0 | bcdb_det | 16 | 1 | 11720.35 | 11720.35 | 11720.35 | 20919.62 |
| d | 0.0 | pg | 16 | 1 | 13747.20 | 13747.20 | 13747.20 | 20558.36 |
| d | 0.5 | bcdb_det | 16 | 1 | 12316.03 | 12316.03 | 12316.03 | 19020.91 |
| d | 0.5 | pg | 16 | 1 | 14589.31 | 14589.31 | 14589.31 | 18991.09 |
| d | 0.99 | bcdb_det | 16 | 1 | 18530.10 | 18530.10 | 18530.10 | 5602.40 |
| d | 0.99 | pg | 16 | 1 | 30914.69 | 30914.69 | 30914.69 | 5601.95 |
| d | 1.2 | bcdb_det | 16 | 1 | 23640.89 | 23640.89 | 23640.89 | 1236.84 |
| d | 1.2 | pg | 16 | 1 | 46659.93 | 46659.93 | 46659.93 | 1231.12 |
