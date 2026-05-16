# Single-node PG+Raft Throughput Comparison (threads=5, runs=3, mode=det)

## Sources
- neel@10.129.148.248: `single_machine_nodes_20260403_051754/neel_at_10_129_148_248/summary.csv` (rerun after fixing missing psycopg)
- neel@10.129.27.54: `single_machine_nodes_20260403_051108/neel_at_10_129_27_54/summary.csv`
- neel@127.0.0.1: `single_machine_nodes_20260403_051108/neel_at_127_0_0_1/summary.csv`

## Median TPS

| Node | ycsb-skew0-99...insert12k | ycsbtx-skew-01-24k...clean-20k |
|---|---:|---:|
| neel@10.129.27.54 | 9466.143 | 4925.442 |
| neel@10.129.148.248 | 7401.387 | 3741.661 |
| neel@127.0.0.1 | 5579.901 | 4446.652 |

## Mean TPS

| Node | ycsb-skew0-99...insert12k | ycsbtx-skew-01-24k...clean-20k |
|---|---:|---:|
| neel@10.129.27.54 | 9482.321 | 4930.909 |
| neel@10.129.148.248 | 7387.329 | 3741.573 |
| neel@127.0.0.1 | 5573.000 | 4447.138 |

## Notes
- All listed rows have `pass_rate_merkle_verify=1.000`.
- Initial run for `neel@10.129.148.248` had `workload_exit=1` due `ModuleNotFoundError: psycopg`; this was fixed by creating `/tmp/ariabc_cluster/.venv` and installing `psycopg[binary]`, then rerunning that node.
