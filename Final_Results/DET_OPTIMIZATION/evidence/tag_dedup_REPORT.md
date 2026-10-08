# tag_dedup deterministic tag optimisation (2026-10-07)

Worktree: `/work/ARIABC/AriaBC.worktrees/detopt_tag_dedup`; branch `detopt/tag_dedup`.
Commit: `3ff5f36facf40e9a57730ef8b0c969c3f0df58b1` (required Codex co-author trailer; not pushed).

Base: main `8d0968d4`. All builds and database runs use ranking host `protectdr@10.129.7.57`, variant `~/claude_checks/detopt_20261007/tag_dedup`. No local builds/database runs, harness changes, or pushes.

## Implementation

- `src/backend/bcdb/shm_transaction.c:1200-1457`: process-local open-addressed full-tag set, initially 256 buckets, grows at 50% occupancy. Stores the PostgreSQL tag hash and independent read / checked-write / publish-only membership bits. Reservation drops exact duplicates within each category. Read records remember whether an identical checked-write reservation exists, regardless of reservation order.
- `src/include/bcdb/shm_transaction.h:218-250`: cached hash and read/write overlap flag in each list record; exported switch/reset functions.
- `src/backend/bcdb/shm_transaction.c:1593`, `:3413-3474`, `:3675`: conflict probes reuse cached hashes, skip overlapping read probes after the write phase succeeds; publication reuses cached hashes. Publish-only tags stay separate and are never checked or used to suppress checked tags.
- `src/backend/bcdb/worker.c:2548`, `:3539`, `:3619`: cache the transaction ID before execution for trace emission, since `delete_tx()` returns its shared slot to the pool before the original emission reads it. The first measurement found duplicate/mislabelled IDs; aggregate reservation counts are usable, but type mapping is invalid for that initial run. Subsequent runs use the stable cached ID. Reset (`worker.c:2588`, `:2698`, `:3807`) the local set at all three list initialisations (DT entry, speculative retry, non-DT entry); context-reset callback also invalidates pointers before transaction context memory is freed. Trace CSV (`worker.c:674`, `:779`; `src/include/bcdb/worker.h:69-75`) appends seven reservation/distinct/overlap counters, without reordering existing columns.
- Predicate locking and linked-list allocation are otherwise retained. PostgreSQL's predicate-lock table may promote/coarsen its locks and BCDB often disables those locks; using it as an authoritative early skip would need another correctness argument. The local exact set covers these paths.

## Correctness argument

For each attempt, let R, W, and P denote the full-tag sets from read, checked-write, and publish-only reservations. The original check probes W followed by R, with duplicates; the enabled check probes W followed by R minus W, without duplicates. These have the same union. Shared entries and the snapshot watermark are unchanged during this serial turn: predecessor writes have already published, successors cannot publish, and map rotation still occurs only in the subsequent publication path. Therefore repeated probing of an identical tag cannot reveal a different predecessor. If an overlapping write tag conflicts, the write phase already returns conflict; otherwise its omitted read probe would also pass. Neither removal can miss a real conflict. Conflict witness order can change, so retry counts/timing need not be identical.

Original publication inserts W and P with multiplicity; enabled publication inserts each distinct tag in W and each distinct tag in P. It uses the same full tag, cached value of the same PostgreSQL hash function, same partition locks, same maps, and same maximum writer-id update. Repeated publication by this transaction is idempotent. No cross-category merge involving P is performed.

Hash bucket collisions use hash equality AND a full 16-byte `memcmp` (the tag consists of four uint32 fields). Collisions only affect local lookup cost. Growth preserves membership and stable separately allocated read-record pointers. Set memory belongs to `bcdb_tx_context`, and list/retry resets plus the context callback prevent deduplication across attempts or transactions. Gates, snapshot visibility, commit ordering, and retry waiting are unchanged.

## Runtime switch and measurement

`BCDB_DT_TAG_DEDUP` (`shm_transaction.c:1213`) is cached once per backend and defaults ON. `0` disables deduplication, cached-hash use, and overlapping-read skipping; it restores both original lists and the original publish-only 32-record recent-duplicate scan. `false`, `FALSE`, `no`, and `NO` also disable it.

With tracing off and the switch off, no local set is allocated. With tracing on and the switch off, the set is maintained solely to measure reservations; it does not alter baseline lists/probes/publication. Measurement adds CPU work outside the serial turn and must not be treated as an uninstrumented TPS baseline.

Counters: `rs_reservations`, `ws_reservations`, `publish_only_reservations` count raw reservation calls before dedup; corresponding `*_distinct` count membership additions; `rs_ws_overlap` counts exact tags present in both R and W. Each CSV row sums these across all speculative attempts of that transaction, with sets reset between attempts. Means therefore include retries; no-restart groups are also computed. Publish-only raw duplicate percentage includes duplicates already removed by the baseline's 32-record scan, so it overstates new insert savings.

## Validation and performance

Initial W5 switch-off trace-on run passed the reference hash. Per-type analysis detected duplicate transaction IDs caused by the existing emit-after-delete trace bug. The final build caches the ID; W5 is being repeated. Initial measurement had exactly 20,000 rows; aggregate reads were 3,776,992 raw / 2,206,832 distinct (41.572% duplicates), writes 998,697 / 998,697 (0%), publish-only 1,235,291 / 261,675 (78.817%), and overlap 827,146 tags (37.481% of distinct reads). These are attempt-summed quantities.

Important: harness `total_restarts` is computed from trace files, so zero in trace-off runs means unmeasured, not zero actual retries. Commands use `PORT=55444 CLIENT_PORT=18104 RAFT_PORT=19104` and the unmodified ranking harness, W=5/30/100, 32 workers, default window=65536.

## Risks / open questions

- Cached hash/overlap fields enlarge `WSTableEntryRecord` (on a 64-bit ABI, 32 to 40 bytes before allocator rounding), including the switch-off path. OFF preserves baseline conflict/reservation/publication semantics, but this common record-layout allocation overhead means its timing may differ slightly from the separate `base` binary.
- Only the requested deterministic path/workloads are validated; the legacy non-DT worker reset site is covered, but no non-DT benchmark is added.
- Extra local hashing, set growth/zeroing, and memory consumption occur during speculative execution; their net benefit needs measured TPS, especially on workloads with few duplicates.
- Duplicate elimination can change the first conflict witness and thereby retry scheduling, though not conflict coverage.
- Trace-on runs carry timing/counter overhead; comparisons with trace-off base TPS are descriptive rather than controlled A/B estimates.
- Final state hashes and zero divergence/permanent-failure counters are required for every run; trace coverage and type mapping must also be verified.

## Step 0: W5 baseline reservation measurements (switch off, trace on, trial 2)

Exactly 20,000 distinct transaction IDs, 0..19,999, were verified. Types were joined to the immutable seed42 workload by zero-based transaction ID (gateway `--detStartSeq 0`). Counts below are means per completed transaction, accumulated over all attempts; percentages are ratios of total calls/counts.

| Type | n | R raw / distinct | R dup % | W raw / distinct | W dup % | P raw / distinct | P dup % | R∩W / distinct R % |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Delivery | 801 | 1245.57 / 590.63 | 52.58 | 547.76 / 547.76 | 0.00 | 757.33 / 115.75 | 84.72 | 92.74 |
| NewOrder | 8985 | 132.91 / 60.03 | 54.83 | 48.60 / 48.60 | 0.00 | 64.32 / 12.87 | 79.98 | 52.38 |
| OrderStatus | 793 | 23.54 / 16.82 | 28.54 | 0.00 / 0.00 | 0.00 | 0.00 / 0.00 | 0.00 | 0.00 |
| Payment-id | 5161 | 18.42 / 12.30 | 33.23 | 14.35 / 14.35 | 0.00 | 6.15 / 6.15 | 0.00 | 100.00 |
| Payment-name | 3469 | 88.12 / 53.15 | 39.68 | 14.59 / 14.59 | 0.00 | 6.25 / 6.25 | 0.00 | 23.53 |
| StockLevel | 791 | 1482.65 / 1185.14 | 20.07 | 0.00 / 0.00 | 0.00 | 0.00 / 0.00 | 0.00 | 0.00 |

No-restart subset (one speculative attempt; subset composition is affected by contention):

| Type | n | R raw / distinct | W raw | P raw / distinct | R∩W |
|---|---:|---:|---:|---:|---:|
| Delivery | 40 | 580.60 / 275.30 | 255.30 | 352.95 / 54.00 | 255.30 |
| NewOrder | 5276 | 92.62 / 41.84 | 33.88 | 44.84 / 9.01 | 21.92 |
| OrderStatus | 444 | 14.64 / 10.59 | 0.00 | 0.00 / 0.00 | 0.00 |
| Payment-id | 222 | 8.98 / 6.00 | 7.00 | 3.00 / 3.00 | 6.00 |
| Payment-name | 127 | 39.02 / 23.30 | 7.00 | 3.00 / 3.00 | 6.00 |
| StockLevel | 355 | 959.89 / 767.25 | 0.00 | 0.00 / 0.00 | 0.00 |

Analysis script: `~/claude_checks/detopt_20261007/tag_dedup/analyze.py`; per-run JSON: `tag_dedup_analysis.json` within each variant run directory. Initial trial 1 has only aggregate analysis because its original trace IDs were unreliable.

## W5 serial-turn measurements: OFF trial 2 vs ON trial 3 (both trace on)

Both traces have exactly one row for each of 20,000 transaction IDs. Means include retry attempts and early exits on conflicts. Aggregate restarts: 14,746 OFF vs 14,782 ON; nearly unchanged. TPS 885.61 → 914.61 (+3.27%), one OFF/ON pair interleaved with other variants in the shared campaign, rather than a stable speedup estimate. The harness records 13,536 OFF vs 13,533 ON transactions with at least one restart (67.68% vs 67.67%). The optimisation leaves the held-turn retry mechanism intact, so this persistent retry rate plausibly limits the TPS benefit at W5.

| Type | R probes OFF → ON | R time µs OFF → ON | W probes OFF → ON | W time µs OFF → ON | Published entries OFF → ON | Publish time µs OFF → ON |
|---|---:|---:|---:|---:|---:|---:|
| ALL | 141.10 → 60.06 | 106.69 → 62.67 | 43.37 → 43.41 | 53.20 → 51.99 | 36.17 → 36.01 | 24.79 → 25.46 |
| Delivery | 784.02 → 27.08 | 449.88 → 36.27 | 481.52 → 482.86 | 474.63 → 467.32 | 313.35 → 309.35 | 101.58 → 99.65 |
| NewOrder | 92.90 → 19.98 | 65.48 → 26.80 | 41.54 → 41.51 | 57.72 → 56.36 | 42.98 → 42.98 | 31.64 → 31.70 |
| OrderStatus | 17.78 → 13.69 | 22.46 → 20.21 | 0.00 → 0.00 | 0.10 → 0.10 | 0.00 → 0.00 | 0.16 → 0.17 |
| Payment-id | 8.99 → 0.00 | 5.06 → 0.23 | 12.47 → 12.49 | 18.98 → 18.22 | 10.00 → 10.00 | 13.01 → 16.06 |
| Payment-name | 42.96 → 20.54 | 39.44 → 27.14 | 12.69 → 12.71 | 19.36 → 18.70 | 10.00 → 10.00 | 12.46 → 12.19 |
| StockLevel | 1453.48 → 1160.33 | 1269.90 → 1102.58 | 0.00 → 0.00 | 0.11 → 0.11 | 0.00 → 0.00 | 24.82 → 24.28 |

Read probes fall 57.44% overall and read-check time falls 41.26%. Write probes remain effectively unchanged because no duplicate checked-write reservations were observed. Actual published entries fall only 0.44%, much less than raw publish-only duplicate calls suggest: the existing 32-record scan already removes most repeats. Publish timing is noisy and is not a demonstrated improvement in this pair.
