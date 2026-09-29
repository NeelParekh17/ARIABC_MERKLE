# YCSB campaign results

Shared buffers: **32MB**. Recorded cases: **480**.

| Mode | Accepted cases | Minimum TPS | Maximum TPS |
|---|---:|---:|---:|
| bcdb_det | 80 | 2604.85 | 49140.05 |
| bcdb_merkle | 80 | 2455.19 | 46838.41 |
| cluster | 80 | 2503.13 | 46403.71 |
| pg | 240 | 2672.72 | 71428.57 |

TPS spans different workloads and worker counts; it is not a matched speedup comparison.
See [summary.csv](summary.csv), [campaign.json](campaign.json), and the per-attempt logs for settings and provenance.
