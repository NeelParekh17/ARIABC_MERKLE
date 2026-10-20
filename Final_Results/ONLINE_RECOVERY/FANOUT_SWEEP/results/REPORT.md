# Fanout, recovery, and cluster TPS

Status: COMPLETE — 60/60 accepted targets; 70 attempts.

Smoke trials are excluded. All failed/noisy matrix attempts remain in the attempt tables.

Engine source HEAD: `9cf6c70a39e9bf848c93c2c24bdb55181f4b3356`; build and binary hashes are in `../provenance/`.
Gateway/controller: .247; replicas: .247/.246/.248. Historical .111 gateway results are not matched baselines.
S1024/M256/P200 and synchronous durability remain fixed. Faults are injected replica corruption, not crashes.

| Rows | Fanout | Scenario | n | Median TPS | TPS min–max | CV % | Median repair ms | Median cut ms | Median catch-up ms | Median total ms |
|---:|---:|---|---:|---:|---|---:|---:|---:|---:|---:|
| 4000000 | 2 | A | 3 | 7572.5 | 7553.9–7643.8 | 0.62 | 0.0 | 0.0 | 0.0 | 0.0 |
| 4000000 | 2 | M_mixed | 3 | 6265.4 | 5522.6–6752.5 | 10.02 | 2372.0 | 3753.0 | 9920.0 | 24914.0 |
| 4000000 | 4 | A | 3 | 7048.1 | 6217.9–7678.6 | 10.49 | 0.0 | 0.0 | 0.0 | 0.0 |
| 4000000 | 4 | M_mixed | 3 | 6483.5 | 5683.2–6958.0 | 10.11 | 1644.0 | 3459.0 | 8819.0 | 14256.0 |
| 4000000 | 8 | A | 3 | 7670.2 | 7293.6–7688.2 | 2.95 | 0.0 | 0.0 | 0.0 | 0.0 |
| 4000000 | 8 | M_mixed | 3 | 7357.3 | 6724.4–7567.9 | 6.08 | 1711.0 | 3555.0 | 8971.0 | 14313.0 |
| 4000000 | 16 | A | 3 | 7663.2 | 7044.7–7680.9 | 4.85 | 0.0 | 0.0 | 0.0 | 0.0 |
| 4000000 | 16 | M_mixed | 3 | 7400.2 | 5838.8–7450.5 | 13.29 | 489.0 | 3373.0 | 8859.0 | 12794.0 |
| 4000000 | 32 | A | 3 | 7543.3 | 7082.8–7728.7 | 4.46 | 0.0 | 0.0 | 0.0 | 0.0 |
| 4000000 | 32 | M_mixed | 3 | 6263.2 | 6015.0–7608.2 | 12.93 | 5099.0 | 3028.0 | 5556.0 | 20094.0 |
| 4000000 | 64 | A | 3 | 7719.8 | 7570.4–7789.7 | 1.46 | 0.0 | 0.0 | 0.0 | 0.0 |
| 4000000 | 64 | M_mixed | 3 | 7636.9 | 5863.6–7660.6 | 14.61 | 1437.0 | 3160.0 | 2641.0 | 7341.0 |
| 4000000 | 128 | A | 3 | 7739.9 | 7694.5–7757.6 | 0.42 | 0.0 | 0.0 | 0.0 | 0.0 |
| 4000000 | 128 | M_mixed | 3 | 7414.6 | 7361.7–7630.3 | 1.90 | 962.0 | 3302.0 | 8622.0 | 13176.0 |
| 4000000 | 256 | A | 3 | 7694.9 | 7546.8–7783.2 | 1.56 | 0.0 | 0.0 | 0.0 | 0.0 |
| 4000000 | 256 | M_mixed | 3 | 7591.6 | 7387.6–7658.1 | 1.87 | 443.0 | 3183.0 | 8375.0 | 12857.0 |
| 4000000 | 512 | A | 3 | 7598.8 | 7585.4–7657.7 | 0.50 | 0.0 | 0.0 | 0.0 | 0.0 |
| 4000000 | 512 | M_mixed | 3 | 7360.7 | 7291.3–7542.2 | 1.75 | 408.0 | 3519.0 | 7428.0 | 11908.0 |
| 4000000 | 1024 | A | 3 | 7527.6 | 6597.1–7590.1 | 7.68 | 0.0 | 0.0 | 0.0 | 0.0 |
| 4000000 | 1024 | M_mixed | 3 | 7431.5 | 6466.7–7533.3 | 8.24 | 397.0 | 3637.0 | 8419.0 | 12834.0 |

Use actual geometry in `matrix_enriched_attempts.csv` to interpret fanout effects.
Recovery boundary and reference watermark are retained: different replay backlogs can change total recovery time independently of Merkle repair.
Concurrent workload updates can overwrite injected UPDATE faults before detection. Compare actual repaired rows and candidate volume along with repair time; 100 injected rows does not imply 100 surviving damaged rows.
Min/max show observed repetitions, not confidence intervals. Missing counters remain unavailable.
Storage profiles are diagnostic evidence; no post-hoc storage exclusion is applied.

1 host-change note(s) in the inherited provenance log predate this campaign's first run and are not applied here.

Harness identity ignores only the fanout allowlist line of `run_case.sh`, so widening the fanout set does not split the identity.
Figures: `figures/fanout_overview.png` plus one figure each: TPS; total recovery split into every phase; cut (prepare/export/digest); repair (drain/localise/transfer/apply/verify/rebase); replay (catch-up time, blocks replayed, backlog); percent share per phase; each phase as a line; repair work/tree depth.

For every rechecked (fanout, scenario), the statistics and figures use only the recheck runs
(`recheck_attempts.jsonl`, same build and fault seeds); the matrix runs they replace stay in the attempt tables.
