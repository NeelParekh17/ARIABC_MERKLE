Signed overhead = 100 * (TPS / median valid baseline TPS in the same group - 1). Missing evidence is UNKNOWN.

| dataset | run_id | scenario | group | tps_majority_visible | overhead_vs_baseline_pct | cut_ms | repair_ms | catchup_ms | total_recovery_ms | mismatched_partitions | differing_leaves | candidate_rows | rows_upserted | rows_deleted | empty_100ms_buckets | max_completion_gap_ms | phase8_merkle_pass | evidence_status |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| published | cluster4_final_A_baseline_180113 | Baseline (Recovery Off) | historical | 8850.05 | 0.0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 20.31 | PASS | PUBLISHED |
| published | cluster4_final_B_nofault_180113 | Recovery Overhead (No Fault) | historical | 8829.05 | -0.24 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 22.28 | PASS | PUBLISHED |
| published | cluster4_final_C_fault_180113 | Follower Fault (Update, Mode: Both) | historical | 8692.82 | -1.78 | 2741 | 59 | 7970 | 10824 | 30 | 31 | 473 | 31 | 0 | 0 | 91.89 | PASS | PUBLISHED |
| published | cluster4_mode_M_passive_181118 | Follower Fault (Passive Detection Only) | historical | 8772.89 | -0.87 | 2713 | 55 | 4512 | 7340 | 7 | 7 | 115 | 7 | 0 | 0 | 103.86 | PASS | PUBLISHED |
| published | cluster4_mode_M_active_r1_183115 | Follower Fault (Active Detection Trial 1) | historical | 8601.23 | -2.81 | 2785 | 103 | 8274 | 11217 | 108 | 133 | 2075 | 154 | 0 | 0 | 180.54 | PASS | PUBLISHED |
| published | cluster4_mode_M_active_r2_183115 | Follower Fault (Active Detection Trial 2) | historical | 8700.38 | -1.69 | 2780 | 54 | 8284 | 11173 | 27 | 29 | 442 | 29 | 0 | 0 | 39.83 | PASS | PUBLISHED |
| published | cluster4_test_mixed_utkarsh_184953 | Follower Fault (Mixed: Upd + Del + Ins) | historical | 8686.21 | -1.85 | 2759 | 88 | 8094 | 10997 | 66 | 77 | 1195 | 43 | 34 | 0 | 89.12 | PASS | PUBLISHED |
| published | cluster4_test_leader_184843 | Leader Fault Update (Unprioritized Ref) | historical | 8052.34 | -9.01 | 3290 | 60 | 8093 | 12990 | 28 | 31 | 537 | 31 | 0 | 3 | 387.38 | PASS | PUBLISHED |
| published | cluster4_test_leader_mixed_185108 | Leader Fault Mixed (Unprioritized Ref) | historical | 4440.0 | -49.83 | 14559 | 324 | 2 | 14953 | 51 | 65 | 990 | 33 | 34 | 49 | 3306.52 | PASS | PUBLISHED |
| published | cluster4_test_leader_192800 | Leader Fault Update (Prioritized Ref) | historical | 8320.77 | -5.98 | 2785 | 102 | 8160 | 12588 | 115 | 151 | 2385 | 164 | 0 | 0 | 133.54 | PASS | PUBLISHED |
| published | cluster4_test_leader_mixed_192316 | Leader Fault Mixed (Prioritized Ref) | historical | 8673.5 | -1.99 | 2777 | 115 | 7413 | 10363 | 132 | 185 | 2881 | 167 | 34 | 0 | 94.31 | PASS | PUBLISHED |
| published | cluster4_recov_C_fault_140220 | Follower Fault Deep Dive (140220) | historical | 8793.14 | -0.64 | 2622 | 54 | 45 | 2816 | 19 | 20 | 324 | 20 | 0 | 0 | 145.94 | PASS | PUBLISHED |
| new | cluster4_s1024_20261001T060514ZJ_f32s1024_A_r3 | A | f32s1024 | 8609.56 | 0.0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 34.92 | PASS | PASS |
| new | cluster4_s1024_20261001T060514ZJ_f32s1024_B_r1 | B | f32s1024 | 8868.20 | 3.0041 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 18.66 | PASS | PASS |
| new | cluster4_s1024_20261001T060514ZJ_f32s1024_C_r1 | C | f32s1024 | 8740.30 | 1.5185 | 3129 | 60 | 8152 | 11367 | 30 | 30 | 1835 | 35 | 0 | 1 | 212.3 | PASS | PASS |
| new | cluster4_s1024_20261001T060514ZJ_f32s1024_L_mix_prio_r1 | L_mix_prio | f32s1024 | 8853.96 | 2.8387 | 2731 | 95 | 7471 | 10343 | 65 | 65 | 4015 | 45 | 34 | 0 | 64.6 | PASS | PASS |
| new | cluster4_s1024_20261001T060514ZJ_f32s1024_M_mixed_r1 | M_mixed | f32s1024 | 8675.85 | 0.77 | 3153 | 93 | 8187 | 11484 | 62 | 62 | 3723 | 45 | 34 | 0 | 102.56 | PASS | PASS |
| new | cluster4_s1024_20261001T060514ZJ_f4s32_A_r1 | A | f4s32 | 8848.09 | 0.0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 28.71 | PASS | PASS |
| new | cluster4_s1024_20261001T060514ZJ_f4s32_C_r1 | C | f4s32 | 8610.48 | -2.6854 | 3154 | 67 | 7641 | 10940 | 38 | 39 | 654 | 39 | 0 | 0 | 139.62 | PASS | PASS |
| new | cluster4_s1024_20261001T060514ZJ_f4s32_M_mixed_r1 | M_mixed | f4s32 | 8378.72 | -5.3048 | 3661 | 91 | 445 | 4266 | 66 | 73 | 1129 | 46 | 34 | 1 | 139.2 | PASS | PASS |
