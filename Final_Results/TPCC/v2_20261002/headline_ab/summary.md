# TPC-C v2

Only accepted runs are included. SERIALIZABLE; synchronous Merkle; durability on.
Headline: W=100, 32 workers, 20,000 transactions; trial-outer C1..C5.
Shared-host noise: inspect each host_before.txt; compare det controls from the same trial.
TPS uses gateway workload wall time. Phase sums overlap across workers and are not elapsed time.
state.hash uses the published eight-table logical projection, excluding wall-clock timestamp fields.
Accepted PGDATA is removed to bound disk use; logs, hashes, traces, settings and WAL stats remain.

| Group | Workers | Config | Trials | Best TPS | Median TPS | Min TPS | Median WAL bytes/tx |
|---|---:|---|---:|---:|---:|---:|---:|
| headline | 32 | C1 | 3 | 2610.28 | 2573.34 | 887.78 | 71086.6 |
| headline | 32 | C2 | 2 | 1530.10 | 1515.63 | 1501.16 | 182766.6 |
| headline | 32 | C3 | 2 | 1773.84 | 1762.50 | 1751.16 | 139152.0 |
| headline | 32 | C4 | 2 | 2763.19 | 2752.21 | 2741.23 | 48455.5 |
| headline | 32 | C5 | 2 | 1994.61 | 1991.54 | 1988.47 | 93929.7 |
| smoke | 32 | C5 | 1 | 551.42 | 551.42 | 551.42 | 135899.1 |

C1=det default; C2=Merkle 32/8 default; C3=Merkle 1024/256 default; C4=det FF90; C5=Merkle 1024/256 FF90.

| Group | Workers | Ratio | Median paired ratio | Best TPS ratio |
|---|---:|---|---:|---:|
| headline | 32 | C2/C1 | 1.1428 | 0.5862 |
| headline | 32 | C3/C1 | 1.3309 | 0.6796 |
| headline | 32 | C5/C4 | 0.7236 | 0.7219 |
| headline | 32 | C5/C1 | 1.5097 | 0.7641 |

| Group | Workers | Config | Table | Non-HOT / updates (pooled) |
|---|---:|---|---|---:|
| headline | 32 | C1 | customer | 97.6346% (48127/49293) |
| headline | 32 | C1 | district | 1.0871% (574/52800) |
| headline | 32 | C1 | oorder | 100.0000% (23670/23670) |
| headline | 32 | C1 | order_line | 42.1748% (90193/213855) |
| headline | 32 | C1 | stock | 90.7568% (246794/271929) |
| headline | 32 | C1 | warehouse | 0.4644% (119/25623) |
| headline | 32 | C2 | customer | 97.6234% (32081/32862) |
| headline | 32 | C2 | district | 1.1392% (401/35200) |
| headline | 32 | C2 | oorder | 100.0000% (15780/15780) |
| headline | 32 | C2 | order_line | 42.3189% (60334/142570) |
| headline | 32 | C2 | stock | 90.7875% (164585/181286) |
| headline | 32 | C2 | warehouse | 0.4859% (83/17082) |
| headline | 32 | C3 | customer | 97.6234% (32081/32862) |
| headline | 32 | C3 | district | 1.1477% (404/35200) |
| headline | 32 | C3 | oorder | 100.0000% (15780/15780) |
| headline | 32 | C3 | order_line | 42.2571% (60246/142570) |
| headline | 32 | C3 | stock | 90.7814% (164574/181286) |
| headline | 32 | C3 | warehouse | 0.5796% (99/17082) |
| headline | 32 | C4 | customer | 0.0061% (2/32862) |
| headline | 32 | C4 | district | 0.4801% (169/35200) |
| headline | 32 | C4 | oorder | 100.0000% (15780/15780) |
| headline | 32 | C4 | order_line | 19.3961% (27653/142570) |
| headline | 32 | C4 | stock | 0.0000% (0/181286) |
| headline | 32 | C4 | warehouse | 0.3747% (64/17082) |
| headline | 32 | C5 | customer | 0.0061% (2/32860) |
| headline | 32 | C5 | district | 0.5541% (195/35195) |
| headline | 32 | C5 | oorder | 100.0000% (15780/15780) |
| headline | 32 | C5 | order_line | 19.5336% (27849/142570) |
| headline | 32 | C5 | stock | 0.0000% (0/181246) |
| headline | 32 | C5 | warehouse | 0.4098% (70/17080) |
| smoke | 32 | C5 | customer | 0.0000% (0/1614) |
| smoke | 32 | C5 | district | 3.2258% (57/1767) |
| smoke | 32 | C5 | oorder | 100.0000% (760/760) |
| smoke | 32 | C5 | order_line | 25.7533% (1829/7102) |
| smoke | 32 | C5 | stock | 0.0000% (0/9054) |
| smoke | 32 | C5 | warehouse | 2.9274% (25/854) |

| Group | Workers | Config | Trial | Trace rows | Restarts | Estimated corrected residual us/tx | Residual / total | Wrapped intervals |
|---|---:|---|---:|---:|---:|---:|---:|---:|
| headline | 32 | C1 | 1 | 20000 | 1497 | 161.25 | 1.3430% | 486 |
| headline | 32 | C1 | 2 | 20000 | 1400 | 656.98 | 1.8596% | 1251 |
| headline | 32 | C1 | 3 | 20000 | 1502 | 160.18 | 1.3510% | 476 |
| headline | 32 | C4 | 1 | 20000 | 1490 | 140.99 | 1.2605% | 444 |
| headline | 32 | C4 | 2 | 20000 | 1500 | 144.66 | 1.2844% | 435 |
| headline | 32 | C2 | 1 | 20000 | 1965 | 1717.41 | 8.3450% | 750 |
| headline | 32 | C2 | 2 | 20000 | 1960 | 1753.24 | 8.3540% | 790 |
| headline | 32 | C3 | 1 | 20000 | 1880 | 968.08 | 5.4731% | 675 |
| headline | 32 | C3 | 2 | 20000 | 1885 | 984.37 | 5.4966% | 748 |
| headline | 32 | C5 | 1 | 20000 | 1877 | 887.18 | 5.6393% | 618 |
| headline | 32 | C5 | 2 | 20000 | 1832 | 886.30 | 5.6538% | 605 |
| smoke | 32 | C5 | 0 | 2000 | 186 | 3236.91 | 5.7266% | 249 |

Residual = sum(total_us) minus sum(parse_plan_us + portal_run_us + gate_us + conflict_us + apply_us + finish_us).
Existing worker.c bcdb_ptrace_delta_us casts negative nanoseconds to uint64 before dividing by 1000. Each wrapped interval adds floor(2^64/1000) microseconds. Top-level phase values are normalized modulo that quantum per row, assuming an interval is much shorter than the quantum (about 585 years); integer truncation introduces at most 1 us uncertainty per corrected interval. Raw traces/totals and correction counts remain in accepted.json. These corrections affect phase analysis only, not gateway TPS or database execution.
Nested wait/apply counters remain in accepted.json and ptrace_keep; do not add them to the top-level phases.
Trace tx_id labels can be reused because worker.c emits them after delete_tx(tx); gateway verified terminal results prove completion. Reused-label counts are recorded in accepted.json.

Accepted runs: 12. Unique headline state hashes: 1.
The master stops on any failed completion, settings, Merkle, state, trace or WAL evidence check.
