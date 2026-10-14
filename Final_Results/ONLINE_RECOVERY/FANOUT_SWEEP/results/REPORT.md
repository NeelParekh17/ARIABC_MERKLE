# Fanout, recovery, and cluster TPS

Status: INCOMPLETE — 30/48 accepted targets; 54 attempts.

Smoke trials are excluded. All failed/noisy matrix attempts remain in the attempt tables.

Engine source HEAD: `9cf6c70a39e9bf848c93c2c24bdb55181f4b3356`; build and binary hashes are in `../provenance/`.
Gateway/controller: .247; replicas: .247/.246/.248. Historical .111 gateway results are not matched baselines.
S1024/M256/P200 and synchronous durability remain fixed. Faults are injected replica corruption, not crashes.

| Rows | Fanout | Scenario | n | Median TPS | TPS min–max | CV % | Median repair ms | Median cut ms | Median catch-up ms | Median total ms |
|---:|---:|---|---:|---:|---|---:|---:|---:|---:|---:|
| 4000000 | 16 | A | 3 | 7658.8 | 7612.9–7716.8 | 0.68 | 0.0 | 0.0 | 0.0 | 0.0 |
| 4000000 | 16 | M_mixed | 3 | 7104.5 | 7103.9–7302.6 | 1.60 | 11234.0 | 3348.0 | 9144.0 | 24277.0 |
| 4000000 | 32 | A | 3 | 7761.0 | 7753.1–7782.5 | 0.20 | 0.0 | 0.0 | 0.0 | 0.0 |
| 4000000 | 32 | M_mixed | 3 | 7412.9 | 7389.3–7451.9 | 0.43 | 12563.0 | 3186.0 | 18409.0 | 34638.0 |
| 4000000 | 64 | A | 3 | 7605.3 | 7392.4–7821.7 | 2.82 | 0.0 | 0.0 | 0.0 | 0.0 |
| 4000000 | 64 | M_mixed | 3 | 6189.1 | 6046.9–7295.9 | 10.50 | 11543.0 | 3177.0 | 8968.0 | 23898.0 |
| 4000000 | 128 | A | 3 | 7728.0 | 7691.9–7748.9 | 0.37 | 0.0 | 0.0 | 0.0 | 0.0 |
| 4000000 | 128 | M_mixed | 3 | 5559.0 | 3340.6–7008.6 | 34.84 | 11289.0 | 3037.0 | 9040.0 | 23474.0 |
| 4000000 | 256 | A | 3 | 7780.6 | 7773.0–7790.4 | 0.11 | 0.0 | 0.0 | 0.0 | 0.0 |
| 4000000 | 256 | M_mixed | 3 | 7319.6 | 4417.0–7745.9 | 27.89 | 11105.0 | 3188.0 | 9028.0 | 23642.0 |

Use actual geometry in `matrix_enriched_attempts.csv` to interpret fanout effects.
Recovery boundary and reference watermark are retained: different replay backlogs can change total recovery time independently of Merkle repair.
Concurrent workload updates can overwrite injected UPDATE faults before detection. Compare actual repaired rows and candidate volume along with repair time; 100 injected rows does not imply 100 surviving damaged rows.
Min/max show observed repetitions, not confidence intervals. Missing counters remain unavailable.
Storage profiles are diagnostic evidence; no post-hoc storage exclusion is applied.

1 host-change note(s) in the inherited provenance log predate this campaign's first run and are not applied here.

Harness identity ignores only the fanout allowlist line of `run_case.sh`, so widening the fanout set does not split the identity.
Figures: `figures/fanout_overview.png` plus one figure each: TPS, total recovery split into every phase, Merkle repair split into localise/transfer/apply/verify, percent share per phase, each phase as a line, repair work/tree depth.
