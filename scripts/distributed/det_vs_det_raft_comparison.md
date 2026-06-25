# Det-Aria vs Det-Aria+Raft Comparison

**Requested setup:** threads=5, runs=3, same workloads, Samsung-backed storage for the Raft voter role.

## Status

The `Det-Aria` single-node baseline is available from:
- [scripts/distributed/throughput_comparison_all_nodes.md](scripts/distributed/throughput_comparison_all_nodes.md)

The strict `Det-Aria+Raft` run completed its launcher flow, but all 6 benchmark cases were marked invalid because NuRaft never became ready (`nuraft_ok=0`). The produced summary is:
- [scripts/bench_full_results/distributed_20260403_060701/summary.csv](scripts/bench_full_results/distributed_20260403_060701/summary.csv)

## A/B Table

| Workload | Det-Aria median TPS | Det-Aria+Raft median TPS | Status |
|---|---:|---:|---|
| ycsb-skew0-99-tx-20k-point-safedb-intkey-insert12k-uniq.txt | 7401.387 / 9466.143 / 3826.688 / 5579.901* | N/A | Raft side invalid (`nuraft_ok=0`) |
| ycsbtx-skew-01-24k-pt-intkey-sid-clean-20k.txt | 3741.661 / 4925.442 / 1916.586 / 4446.652* | N/A | Raft side invalid (`nuraft_ok=0`) |

\* Det-Aria baseline rows are listed per node in [throughput_comparison_all_nodes.md](throughput_comparison_all_nodes.md). The comparison above preserves the single-node node-ordering used in that report.

## Failure summary for the Raft run

- Gateway `psql` was fixed by using loopback SSH tunnels for the PG client ports.
- The full benchmark reached the distributed driver successfully.
- Every case ended with `invalid_reason=nuraft_not_ok;gateway_exit_-1;root_hash_mismatch;post_root_hash_drift`.
- `summary.csv` reports `completed_runs=0` and `valid_runs=0` for both workloads.

## Notes

- The gateway-side tunnel setup was forced because `10.129.148.246` and `10.129.148.248` require local-forwarded client ports in this lab topology.
- The runbook remains updated to use `10.129.148.246` as Raft voter and `10.129.148.236` as the gateway.
