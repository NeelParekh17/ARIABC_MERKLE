# Recovery Architecture Analysis

This report contains one authoritative results comparison only: the latest optimized native-dynamic EPYC full sweep versus the best static F32/L1024 EPYC full sweep. Historical dynamic layouts, initial optimization runs, admin123 campaigns, and older static geometry tables are intentionally excluded so stale measurements cannot be mistaken for current results.

## Current EPYC Comparison: Static Best vs Latest Optimized Dynamic

This is the only current dynamic-results comparison in this report. Older
dynamic side-table, layout-v5, and initial layout-v6 measurements have been
removed because they no longer represent the active implementation.

### Authoritative artifacts and comparison contract

```text
best static EPYC artifact (F32/L1024, three repetitions):
scripts/benchmark/recovery/fetched/
  ariabc-recovery-best-scaling-f32-l1024-k75-c300-20260714T040459Z-0068d0

latest optimized native-dynamic EPYC artifact (one repetition):
scripts/benchmark/recovery/fetched/
  ariabc-recovery-dynamic-size-scaling-k75-c300-20260720T105842Z-00c5e9
```

Both artifacts were produced on `user-MZ73-LM0-000`, use release builds,
cover the same 1M-50M row sizes, select K=75 bad ranges and C=300 update
corruptions, and use `audit_mode=skip`. Static reports the median of three
repetitions at each size; dynamic has one repetition. Static is 33/33 valid and
dynamic is 11/11 valid. Both artifact manifests validate without checksum
failures.

The compared sparse-recovery metric is `restore_repair_ms`. The O(N) full-table
audit is intentionally excluded. Static uses its best measured F=32,
L=1024 geometry with 204,800 fixed leaves. Dynamic uses P=200, logical fanout
32, physical node fanout 2, leaf capacity 32, native layout version 6, and
synchronous path-local COW.

### Recovery latency

| Rows | Static best median | Latest dynamic | Dynamic reduction |
|---:|---:|---:|---:|
| 1M | 952.861 ms | 260.881 ms | 72.6% |
| 3M | 940.471 ms | 250.420 ms | 73.4% |
| 5M | 988.369 ms | 241.860 ms | 75.5% |
| 7M | 1,062.612 ms | 333.006 ms | 68.7% |
| 10M | 1,097.359 ms | 1,437.316 ms | -31.0% |
| 15M | 1,155.665 ms | 338.602 ms | 70.7% |
| 20M | 4,423.527 ms | 350.772 ms | 92.1% |
| 25M | 1,234.877 ms | 355.833 ms | 71.2% |
| 30M | 1,281.075 ms | 406.334 ms | 68.3% |
| 40M | 1,369.660 ms | 356.235 ms | 74.0% |
| 50M | 1,367.062 ms | 365.336 ms | 73.3% |

![Static best versus latest dynamic recovery](epyc_static_vs_dynamic_recovery.png)

Latest dynamic wins at 10 of 11 sizes. Median latency across the size points is
350.772 ms, compared with 1,155.665 ms for static best, a 69.6% reduction. At
50M, dynamic remains at 365.336 ms while static reaches 1,367.062 ms, a 73.3%
reduction. Dynamic's normal 15M-50M values stay within 338.602-406.334 ms even
though table size grows by 3.33x.

The two isolated spikes must remain visible. Dynamic's 10M repair write takes
1,019.966 ms but returns to 31.371 ms at 15M; its compatibility queue drain
also spikes independently after the measured recovery boundary. Static's 20M
repair write takes 4,048.904 ms but returns to 844.539 ms at 25M. Neither spike
persists at larger sizes, so neither is evidence of an algorithmic size knee.
The dynamic 10M point should be repeated because the dynamic campaign contains
only one sample per size.

### Phase comparison

The candidate and comparison phases operate on different representations:
static fetches candidate heap rows and compares rows, while dynamic fetches
bounded native summaries and compares commitments before fetching exactly 300
heap rows. Static targeted confirmation and dynamic native commit visibility
serve the same correctness boundary but use different mechanisms.

| Rows | Static localisation | Dynamic localisation | Static candidate fetch | Dynamic summary fetch | Static comparison | Dynamic comparison | Static repair | Dynamic repair |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1M | 50.462 | 181.327 | 11.320 | 21.520 | 3.184 | 0.509 | 845.822 | 32.336 |
| 3M | 50.847 | 173.588 | 23.778 | 21.021 | 4.355 | 0.744 | 817.153 | 31.454 |
| 5M | 39.938 | 149.875 | 27.642 | 23.484 | 5.836 | 0.742 | 839.819 | 42.152 |
| 7M | 51.348 | 252.244 | 54.326 | 19.043 | 7.454 | 0.397 | 859.507 | 34.322 |
| 10M | 51.804 | 363.869 | 72.073 | 17.418 | 9.218 | 0.263 | 849.963 | 1,019.966 |
| 15M | 51.848 | 260.575 | 98.105 | 16.366 | 12.737 | 0.339 | 852.283 | 31.371 |
| 20M | 51.797 | 268.389 | 116.775 | 17.254 | 18.432 | 0.382 | 4,048.904 | 34.185 |
| 25M | 51.997 | 274.529 | 135.539 | 17.338 | 20.436 | 0.416 | 844.539 | 33.063 |
| 30M | 52.111 | 307.711 | 156.994 | 17.615 | 23.896 | 0.459 | 849.373 | 48.645 |
| 40M | 52.140 | 269.193 | 190.522 | 21.257 | 31.123 | 0.589 | 864.074 | 33.980 |
| 50M | 52.335 | 280.946 | 217.963 | 19.998 | 35.993 | 0.579 | 802.547 | 34.362 |

![Static best versus latest dynamic phases](epyc_static_vs_dynamic_phases.png)

Static localisation is cheaper and nearly constant at roughly 40-52 ms.
Dynamic localisation costs 150-364 ms because it compares a richer native
frontier, but it remains bounded by only 2-3 logical levels. Dynamic wins
overall elsewhere:

- Static candidate fetch rises from 11.320 ms at 1M to 217.963 ms at 50M;
  dynamic summary fetch stays between 16.366 and 23.484 ms.
- Static row comparison rises from 3.184 to 35.993 ms; dynamic commitment
  comparison remains below 0.75 ms.
- Static repair normally costs approximately 803-864 ms for the fixed 300
  rows; dynamic path-local COW normally costs 31-49 ms.
- Dynamic therefore spends more to localise precisely, then saves much more by
  bounding candidate work and applying a compact native COW repair.

### Geometry and candidate-work scaling

| Rows | Static rows/leaf | Static leaves | Dynamic rows/leaf | Dynamic leaves |
|---:|---:|---:|---:|---:|
| 1M | 4.92 | 204,800 | 20.97 | 47,693 |
| 3M | 14.65 | 204,800 | 23.07 | 130,027 |
| 5M | 24.41 | 204,800 | 23.19 | 215,626 |
| 7M | 34.18 | 204,800 | 21.31 | 328,442 |
| 10M | 48.83 | 204,800 | 23.19 | 431,290 |
| 15M | 73.24 | 204,800 | 20.93 | 716,556 |
| 20M | 97.66 | 204,800 | 23.19 | 862,496 |
| 25M | 122.07 | 204,800 | 22.60 | 1,106,021 |
| 30M | 146.48 | 204,800 | 20.93 | 1,433,277 |
| 40M | 195.31 | 204,800 | 23.19 | 1,724,884 |
| 50M | 244.14 | 204,800 | 22.60 | 2,212,490 |

![Static best versus latest dynamic leaf geometry](epyc_static_vs_dynamic_leaf_geometry.png)

Static leaf count is fixed, so rows per leaf grow almost linearly with N.
Dynamic splits leaves as the table grows and holds mean occupancy near 21-23,
below its capacity of 32. Consequently, static candidate rows rise from 894 at
1M to 36,480 at 50M, while dynamic candidate summaries remain between 1,194
and 3,376 and never exceed the configured 4,800 bound.

![Static best versus latest dynamic localisation payload](epyc_static_vs_dynamic_localisation_payload.png)

This is the central architectural difference. Static F32/L1024 improves the
constant factor but retains a candidate-work term proportional to rows per
fixed leaf. Dynamic adds leaves and keeps candidate work bounded for fixed K,
so sparse recovery remains largely independent of total table size.

### Correctness and interpretation boundary

For every latest-dynamic size, all 300 corruptions are repaired, remaining bad
ranges are zero, roots and root counts match, schema/planner checks pass,
native API authority failures are zero, and the post-repair queue barrier does
not alter the already-correct native roots. Static best is likewise 33/33
valid. These are sparse-recovery correctness proofs, not full O(N) audits,
because both campaigns use `audit_mode=skip`.

The measured conclusion is:

```text
comparison retained:     static best EPYC vs latest optimized dynamic EPYC
dynamic total wins:      10 of 11 sizes
median total reduction:  69.6%
50M total reduction:     73.3%
static advantage:        cheaper root/tree localisation
dynamic advantages:      bounded leaf occupancy, candidate work, comparison,
                         and path-local repair cost
remaining measurement:   repeat latest dynamic 10M to quantify its one-run stall
```

### Reproducing the graphs

```bash
MPLCONFIGDIR=/tmp/ariabc-mpl python3 \
  scripts/benchmark/recovery/plot_epyc_static_vs_dynamic.py \
  --static-artifact scripts/benchmark/recovery/fetched/ariabc-recovery-best-scaling-f32-l1024-k75-c300-20260714T040459Z-0068d0 \
  --dynamic-artifact scripts/benchmark/recovery/fetched/ariabc-recovery-dynamic-size-scaling-k75-c300-20260720T105842Z-00c5e9 \
  --output-dir Dynamic_merkle_docs
```
