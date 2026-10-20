### smoke f1024 M_mixed rep1 attempt1 — **NOISE_EXCLUDED**
- run: `cluster4_fanout_sweep_20261015_smoke_n4000000_f1024_M_mixed_r1_a1` (4.2 min)
- TPS (majority visible): 7351
- noise: ['10.129.148.248 more than 64 MiB swap IO during gateway interval']
- recovery total 13.66s: detect→quarantine 0.00s, cut 3.58s, repair call 10.06s
- cut: prepare 0.00s, export (backlog wait + snapshot) 3.57s, digest 0.01s
- repair: drain 0.47s, localise 0.24s, transfer 0.03s, apply 0.08s, verify 0.01s, rebase 0.03s (differing leaves 100, rows read 2020, upserted 66, deleted 34, full copies 0)
- replay: catch-up 9.14s, blocks replayed 339, live=1
- final check, all 3 nodes: PASS (3 nodes, rows 4000001, root 2a6f6a6505fa…); divergences: 0

### smoke f1024 M_mixed rep1 attempt2 — **ACCEPTED**
- run: `cluster4_fanout_sweep_20261015_smoke_n4000000_f1024_M_mixed_r1_a2` (3.4 min)
- TPS (majority visible): 7409
- recovery total 13.20s: detect→quarantine 0.00s, cut 3.51s, repair call 9.66s
- cut: prepare 0.00s, export (backlog wait + snapshot) 3.50s, digest 0.01s
- repair: drain 0.02s, localise 0.26s, transfer 0.03s, apply 0.08s, verify 0.01s, rebase 0.03s (differing leaves 100, rows read 2020, upserted 66, deleted 34, full copies 0)
- replay: catch-up 9.18s, blocks replayed 342, live=1
- final check, all 3 nodes: PASS (3 nodes, rows 4000001, root 2a6f6a6505fa…); divergences: 0

### smoke f512 M_mixed rep1 attempt1 — **ACCEPTED**
- run: `cluster4_fanout_sweep_20261015_smoke_n4000000_f512_M_mixed_r1_a1` (3.2 min)
- TPS (majority visible): 7499
- recovery total 13.00s: detect→quarantine 0.00s, cut 3.57s, repair call 9.42s
- cut: prepare 0.00s, export (backlog wait + snapshot) 3.55s, digest 0.01s
- repair: drain 0.00s, localise 0.12s, transfer 0.04s, apply 0.14s, verify 0.00s, rebase 0.03s (differing leaves 100, rows read 3956, upserted 66, deleted 34, full copies 0)
- replay: catch-up 9.04s, blocks replayed 342, live=1
- final check, all 3 nodes: PASS (3 nodes, rows 4000001, root 2a6f6a6505fa…); divergences: 0

### matrix f2 A rep1 attempt1 — **ACCEPTED**
- run: `cluster4_fanout_sweep_20261015_matrix_n4000000_f2_A_r1_a1` (3.0 min)
- TPS (majority visible): 7554
- final check, all 3 nodes: PASS (3 nodes, rows 4000001, root 2a6f6a6505fa…); divergences: 0

### matrix f2 M_mixed rep1 attempt1 — **ACCEPTED**
- run: `cluster4_fanout_sweep_20261015_matrix_n4000000_f2_M_mixed_r1_a1` (3.6 min)
- TPS (majority visible): 4549
- recovery total 21.61s: detect→quarantine 0.00s, cut 3.56s, repair call 18.03s
- cut: prepare 0.00s, export (backlog wait + snapshot) 3.55s, digest 0.01s
- repair: drain 0.09s, localise 0.03s, transfer 0.58s, apply 1.74s, verify 0.01s, rebase 0.03s (differing leaves 100, rows read 62592, upserted 66, deleted 34, full copies 0)
- replay: catch-up 15.52s, blocks replayed 339, live=1
- final check, all 3 nodes: PASS (3 nodes, rows 4000001, root 2a6f6a6505fa…); divergences: 0

### matrix f4 A rep1 attempt1 — **ACCEPTED**
- run: `cluster4_fanout_sweep_20261015_matrix_n4000000_f4_A_r1_a1` (2.7 min)
- TPS (majority visible): 6218
- final check, all 3 nodes: PASS (3 nodes, rows 4000001, root 2a6f6a6505fa…); divergences: 0

### matrix f4 M_mixed rep1 attempt1 — **ACCEPTED**
- run: `cluster4_fanout_sweep_20261015_matrix_n4000000_f4_M_mixed_r1_a1` (2.8 min)
- TPS (majority visible): 6958
- recovery total 14.39s: detect→quarantine 0.00s, cut 3.46s, repair call 10.91s
- cut: prepare 0.00s, export (backlog wait + snapshot) 3.45s, digest 0.01s
- repair: drain 0.00s, localise 0.04s, transfer 0.26s, apply 1.33s, verify 0.01s, rebase 0.03s (differing leaves 100, rows read 31259, upserted 66, deleted 34, full copies 0)
- replay: catch-up 9.23s, blocks replayed 343, live=1
- final check, all 3 nodes: PASS (3 nodes, rows 4000001, root 2a6f6a6505fa…); divergences: 0

### matrix f8 A rep1 attempt1 — **ACCEPTED**
- run: `cluster4_fanout_sweep_20261015_matrix_n4000000_f8_A_r1_a1` (2.8 min)
- TPS (majority visible): 7688
- final check, all 3 nodes: PASS (3 nodes, rows 4000001, root 2a6f6a6505fa…); divergences: 0

### matrix f8 M_mixed rep1 attempt1 — **ACCEPTED**
- run: `cluster4_fanout_sweep_20261015_matrix_n4000000_f8_M_mixed_r1_a1` (3.0 min)
- TPS (majority visible): 7357
- recovery total 14.31s: detect→quarantine 0.00s, cut 3.56s, repair call 10.74s
- cut: prepare 0.00s, export (backlog wait + snapshot) 3.54s, digest 0.01s
- repair: drain 0.01s, localise 0.03s, transfer 0.31s, apply 1.36s, verify 0.01s, rebase 0.03s (differing leaves 100, rows read 31259, upserted 66, deleted 34, full copies 0)
- replay: catch-up 8.97s, blocks replayed 335, live=1
- final check, all 3 nodes: PASS (3 nodes, rows 4000001, root 2a6f6a6505fa…); divergences: 0

### matrix f16 A rep1 attempt1 — **ACCEPTED**
- run: `cluster4_fanout_sweep_20261015_matrix_n4000000_f16_A_r1_a1` (3.3 min)
- TPS (majority visible): 7663
- final check, all 3 nodes: PASS (3 nodes, rows 4000001, root 2a6f6a6505fa…); divergences: 0

### matrix f16 M_mixed rep1 attempt1 — **ACCEPTED**
- run: `cluster4_fanout_sweep_20261015_matrix_n4000000_f16_M_mixed_r1_a1` (3.1 min)
- TPS (majority visible): 5839
- recovery total 13.15s: detect→quarantine 0.00s, cut 3.65s, repair call 9.48s
- cut: prepare 0.00s, export (backlog wait + snapshot) 3.64s, digest 0.01s
- repair: drain 0.00s, localise 0.07s, transfer 0.13s, apply 0.27s, verify 0.01s, rebase 0.03s (differing leaves 100, rows read 7915, upserted 66, deleted 34, full copies 0)
- replay: catch-up 8.94s, blocks replayed 340, live=1
- final check, all 3 nodes: PASS (3 nodes, rows 4000001, root 2a6f6a6505fa…); divergences: 0

### matrix f32 A rep1 attempt1 — **ACCEPTED**
- run: `cluster4_fanout_sweep_20261015_matrix_n4000000_f32_A_r1_a1` (3.2 min)
- TPS (majority visible): 7083
- final check, all 3 nodes: PASS (3 nodes, rows 4000001, root 2a6f6a6505fa…); divergences: 0

### matrix f32 M_mixed rep1 attempt1 — **ACCEPTED**
- run: `cluster4_fanout_sweep_20261015_matrix_n4000000_f32_M_mixed_r1_a1` (3.4 min)
- TPS (majority visible): 6263
- recovery total 30.82s: detect→quarantine 0.00s, cut 3.03s, repair call 27.77s
- cut: prepare 0.00s, export (backlog wait + snapshot) 3.02s, digest 0.01s
- repair: drain 12.18s, localise 0.03s, transfer 0.48s, apply 9.44s, verify 0.01s, rebase 0.03s (differing leaves 100, rows read 62592, upserted 66, deleted 34, full copies 0)
- replay: catch-up 5.56s, blocks replayed 205, live=1
- final check, all 3 nodes: PASS (3 nodes, rows 4000001, root 2a6f6a6505fa…); divergences: 0

### matrix f64 A rep1 attempt1 — **ACCEPTED**
- run: `cluster4_fanout_sweep_20261015_matrix_n4000000_f64_A_r1_a1` (2.6 min)
- TPS (majority visible): 7570
- final check, all 3 nodes: PASS (3 nodes, rows 4000001, root 2a6f6a6505fa…); divergences: 0

### matrix f64 M_mixed rep1 attempt1 — **ACCEPTED**
- run: `cluster4_fanout_sweep_20261015_matrix_n4000000_f64_M_mixed_r1_a1` (3.3 min)
- TPS (majority visible): 5864
- recovery total 4.35s: detect→quarantine 0.00s, cut 2.70s, repair call 1.62s
- cut: prepare 0.00s, export (backlog wait + snapshot) 2.69s, digest 0.01s
- repair: drain 0.12s, localise 0.02s, transfer 0.28s, apply 1.11s, verify 0.00s, rebase 0.03s (differing leaves 100, rows read 31259, upserted 66, deleted 34, full copies 0)
- replay: catch-up 0.00s, blocks replayed 0, live=1
- final check, all 3 nodes: PASS (3 nodes, rows 4000001, root 2a6f6a6505fa…); divergences: 0

### matrix f128 A rep1 attempt1 — **ACCEPTED**
- run: `cluster4_fanout_sweep_20261015_matrix_n4000000_f128_A_r1_a1` (2.8 min)
- TPS (majority visible): 7758
- final check, all 3 nodes: PASS (3 nodes, rows 4000001, root 2a6f6a6505fa…); divergences: 0

### matrix f128 M_mixed rep1 attempt1 — **ACCEPTED**
- run: `cluster4_fanout_sweep_20261015_matrix_n4000000_f128_M_mixed_r1_a1` (2.8 min)
- TPS (majority visible): 7415
- recovery total 13.18s: detect→quarantine 0.00s, cut 3.52s, repair call 9.64s
- cut: prepare 0.00s, export (backlog wait + snapshot) 3.50s, digest 0.01s
- repair: drain 0.01s, localise 0.04s, transfer 0.16s, apply 0.74s, verify 0.01s, rebase 0.03s (differing leaves 100, rows read 15737, upserted 66, deleted 34, full copies 0)
- replay: catch-up 8.62s, blocks replayed 336, live=1
- final check, all 3 nodes: PASS (3 nodes, rows 4000001, root 2a6f6a6505fa…); divergences: 0

### matrix f256 A rep1 attempt1 — **ACCEPTED**
- run: `cluster4_fanout_sweep_20261015_matrix_n4000000_f256_A_r1_a1` (2.8 min)
- TPS (majority visible): 7695
- final check, all 3 nodes: PASS (3 nodes, rows 4000001, root 2a6f6a6505fa…); divergences: 0

### matrix f256 M_mixed rep1 attempt1 — **ACCEPTED**
- run: `cluster4_fanout_sweep_20261015_matrix_n4000000_f256_M_mixed_r1_a1` (3.1 min)
- TPS (majority visible): 6982
- recovery total 12.88s: detect→quarantine 0.00s, cut 3.53s, repair call 9.33s
- cut: prepare 0.00s, export (backlog wait + snapshot) 3.52s, digest 0.01s
- repair: drain 0.00s, localise 0.07s, transfer 0.08s, apply 0.37s, verify 0.00s, rebase 0.03s (differing leaves 100, rows read 7915, upserted 66, deleted 34, full copies 0)
- replay: catch-up 8.75s, blocks replayed 329, live=1
- final check, all 3 nodes: PASS (3 nodes, rows 4000001, root 2a6f6a6505fa…); divergences: 0

### matrix f512 A rep1 attempt1 — **ACCEPTED**
- run: `cluster4_fanout_sweep_20261015_matrix_n4000000_f512_A_r1_a1` (3.3 min)
- TPS (majority visible): 7658
- final check, all 3 nodes: PASS (3 nodes, rows 4000001, root 2a6f6a6505fa…); divergences: 0

### matrix f512 M_mixed rep1 attempt1 — **ACCEPTED**
- run: `cluster4_fanout_sweep_20261015_matrix_n4000000_f512_M_mixed_r1_a1` (3.0 min)
- TPS (majority visible): 7361
- recovery total 11.91s: detect→quarantine 0.00s, cut 3.52s, repair call 8.37s
- cut: prepare 0.00s, export (backlog wait + snapshot) 3.51s, digest 0.01s
- repair: drain 0.55s, localise 0.12s, transfer 0.04s, apply 0.15s, verify 0.01s, rebase 0.03s (differing leaves 100, rows read 3956, upserted 66, deleted 34, full copies 0)
- replay: catch-up 7.43s, blocks replayed 298, live=1
- final check, all 3 nodes: PASS (3 nodes, rows 4000001, root 2a6f6a6505fa…); divergences: 0

### matrix f1024 A rep1 attempt1 — **ACCEPTED**
- run: `cluster4_fanout_sweep_20261015_matrix_n4000000_f1024_A_r1_a1` (2.9 min)
- TPS (majority visible): 6597
- final check, all 3 nodes: PASS (3 nodes, rows 4000001, root 2a6f6a6505fa…); divergences: 0

### matrix f1024 M_mixed rep1 attempt1 — **ACCEPTED**
- run: `cluster4_fanout_sweep_20261015_matrix_n4000000_f1024_M_mixed_r1_a1` (3.1 min)
- TPS (majority visible): 5714
- recovery total 13.29s: detect→quarantine 0.00s, cut 3.57s, repair call 9.70s
- cut: prepare 0.00s, export (backlog wait + snapshot) 3.56s, digest 0.01s
- repair: drain 0.04s, localise 0.23s, transfer 0.03s, apply 0.08s, verify 0.00s, rebase 0.03s (differing leaves 100, rows read 2020, upserted 66, deleted 34, full copies 0)
- replay: catch-up 9.22s, blocks replayed 342, live=1
- final check, all 3 nodes: PASS (3 nodes, rows 4000001, root 2a6f6a6505fa…); divergences: 0

### matrix f4 M_mixed rep2 attempt1 — **ACCEPTED**
- run: `cluster4_fanout_sweep_20261015_matrix_n4000000_f4_M_mixed_r2_a1` (3.2 min)
- TPS (majority visible): 5683
- recovery total 8.24s: detect→quarantine 0.00s, cut 3.14s, repair call 5.08s
- cut: prepare 0.00s, export (backlog wait + snapshot) 3.13s, digest 0.01s
- repair: drain 0.00s, localise 0.04s, transfer 0.28s, apply 1.09s, verify 0.01s, rebase 0.03s (differing leaves 100, rows read 31114, upserted 66, deleted 34, full copies 0)
- replay: catch-up 3.60s, blocks replayed 137, live=1
- final check, all 3 nodes: PASS (3 nodes, rows 4000001, root 2a6f6a6505fa…); divergences: 0

### matrix f4 A rep2 attempt1 — **ACCEPTED**
- run: `cluster4_fanout_sweep_20261015_matrix_n4000000_f4_A_r2_a1` (3.0 min)
- TPS (majority visible): 7679
- final check, all 3 nodes: PASS (3 nodes, rows 4000001, root 2a6f6a6505fa…); divergences: 0

### matrix f8 M_mixed rep2 attempt1 — **ACCEPTED**
- run: `cluster4_fanout_sweep_20261015_matrix_n4000000_f8_M_mixed_r2_a1` (2.8 min)
- TPS (majority visible): 7568
- recovery total 1.56s: detect→quarantine 0.00s, cut 0.01s, repair call 1.53s
- cut: prepare 0.00s, export (backlog wait + snapshot) 0.00s, digest 0.01s
- repair: drain 0.00s, localise 0.03s, transfer 0.27s, apply 1.16s, verify 0.00s, rebase 0.03s (differing leaves 100, rows read 31114, upserted 66, deleted 34, full copies 0)
- replay: catch-up 0.00s, blocks replayed 0, live=1
- final check, all 3 nodes: PASS (3 nodes, rows 4000001, root 2a6f6a6505fa…); divergences: 0

### matrix f8 A rep2 attempt1 — **ACCEPTED**
- run: `cluster4_fanout_sweep_20261015_matrix_n4000000_f8_A_r2_a1` (2.7 min)
- TPS (majority visible): 7294
- final check, all 3 nodes: PASS (3 nodes, rows 4000001, root 2a6f6a6505fa…); divergences: 0

### matrix f16 M_mixed rep2 attempt1 — **ACCEPTED**
- run: `cluster4_fanout_sweep_20261015_matrix_n4000000_f16_M_mixed_r2_a1` (3.0 min)
- TPS (majority visible): 7400
- recovery total 6.29s: detect→quarantine 0.00s, cut 3.07s, repair call 3.20s
- cut: prepare 0.00s, export (backlog wait + snapshot) 3.06s, digest 0.01s
- repair: drain 0.00s, localise 0.07s, transfer 0.08s, apply 0.25s, verify 0.00s, rebase 0.03s (differing leaves 100, rows read 7761, upserted 66, deleted 34, full copies 0)
- replay: catch-up 2.72s, blocks replayed 147, live=1
- final check, all 3 nodes: PASS (3 nodes, rows 4000001, root 2a6f6a6505fa…); divergences: 0

### matrix f16 A rep2 attempt1 — **ACCEPTED**
- run: `cluster4_fanout_sweep_20261015_matrix_n4000000_f16_A_r2_a1` (2.9 min)
- TPS (majority visible): 7681
- final check, all 3 nodes: PASS (3 nodes, rows 4000001, root 2a6f6a6505fa…); divergences: 0

### matrix f32 M_mixed rep2 attempt1 — **ACCEPTED**
- run: `cluster4_fanout_sweep_20261015_matrix_n4000000_f32_M_mixed_r2_a1` (3.2 min)
- TPS (majority visible): 6015
- recovery total 12.52s: detect→quarantine 0.00s, cut 3.00s, repair call 9.50s
- cut: prepare 0.00s, export (backlog wait + snapshot) 2.99s, digest 0.01s
- repair: drain 0.01s, localise 0.03s, transfer 1.09s, apply 3.96s, verify 0.01s, rebase 0.03s (differing leaves 215, rows read 133975, upserted 183, deleted 34, full copies 0)
- replay: catch-up 4.35s, blocks replayed 144, live=1
- final check, all 3 nodes: PASS (3 nodes, rows 4000001, root 2a6f6a6505fa…); divergences: 0

### matrix f32 A rep2 attempt1 — **ACCEPTED**
- run: `cluster4_fanout_sweep_20261015_matrix_n4000000_f32_A_r2_a1` (2.9 min)
- TPS (majority visible): 7543
- final check, all 3 nodes: PASS (3 nodes, rows 4000001, root 2a6f6a6505fa…); divergences: 0

### matrix f64 M_mixed rep2 attempt1 — **ACCEPTED**
- run: `cluster4_fanout_sweep_20261015_matrix_n4000000_f64_M_mixed_r2_a1` (3.1 min)
- TPS (majority visible): 7637
- recovery total 7.34s: detect→quarantine 0.00s, cut 3.16s, repair call 4.16s
- cut: prepare 0.00s, export (backlog wait + snapshot) 3.15s, digest 0.01s
- repair: drain 0.13s, localise 0.03s, transfer 0.27s, apply 1.02s, verify 0.01s, rebase 0.03s (differing leaves 100, rows read 31114, upserted 66, deleted 34, full copies 0)
- replay: catch-up 2.64s, blocks replayed 140, live=1
- final check, all 3 nodes: PASS (3 nodes, rows 4000001, root 2a6f6a6505fa…); divergences: 0

### matrix f64 A rep2 attempt1 — **ACCEPTED**
- run: `cluster4_fanout_sweep_20261015_matrix_n4000000_f64_A_r2_a1` (3.3 min)
- TPS (majority visible): 7790
- final check, all 3 nodes: PASS (3 nodes, rows 4000001, root 2a6f6a6505fa…); divergences: 0

### matrix f128 M_mixed rep2 attempt1 — **ACCEPTED**
- run: `cluster4_fanout_sweep_20261015_matrix_n4000000_f128_M_mixed_r2_a1` (2.8 min)
- TPS (majority visible): 7630
- recovery total 7.31s: detect→quarantine 0.00s, cut 3.07s, repair call 4.23s
- cut: prepare 0.00s, export (backlog wait + snapshot) 3.05s, digest 0.01s
- repair: drain 0.00s, localise 0.04s, transfer 0.16s, apply 0.51s, verify 0.00s, rebase 0.03s (differing leaves 100, rows read 15496, upserted 66, deleted 34, full copies 0)
- replay: catch-up 3.45s, blocks replayed 143, live=1
- final check, all 3 nodes: PASS (3 nodes, rows 4000001, root 2a6f6a6505fa…); divergences: 0

### matrix f128 A rep2 attempt1 — **ACCEPTED**
- run: `cluster4_fanout_sweep_20261015_matrix_n4000000_f128_A_r2_a1` (3.0 min)
- TPS (majority visible): 7740
- final check, all 3 nodes: PASS (3 nodes, rows 4000001, root 2a6f6a6505fa…); divergences: 0

### matrix f256 M_mixed rep2 attempt1 — **ACCEPTED**
- run: `cluster4_fanout_sweep_20261015_matrix_n4000000_f256_M_mixed_r2_a1` (3.6 min)
- TPS (majority visible): 7427
- recovery total 6.03s: detect→quarantine 0.00s, cut 3.34s, repair call 2.68s
- cut: prepare 0.00s, export (backlog wait + snapshot) 3.32s, digest 0.01s
- repair: drain 0.00s, localise 0.07s, transfer 0.09s, apply 0.27s, verify 0.01s, rebase 0.03s (differing leaves 100, rows read 7761, upserted 66, deleted 34, full copies 0)
- replay: catch-up 2.18s, blocks replayed 134, live=1
- final check, all 3 nodes: PASS (3 nodes, rows 4000001, root 2a6f6a6505fa…); divergences: 0

### matrix f256 A rep2 attempt1 — **ACCEPTED**
- run: `cluster4_fanout_sweep_20261015_matrix_n4000000_f256_A_r2_a1` (3.3 min)
- TPS (majority visible): 7783
- final check, all 3 nodes: PASS (3 nodes, rows 4000001, root 2a6f6a6505fa…); divergences: 0

### matrix f512 M_mixed rep2 attempt1 — **ACCEPTED**
- run: `cluster4_fanout_sweep_20261015_matrix_n4000000_f512_M_mixed_r2_a1` (3.3 min)
- TPS (majority visible): 7291
- recovery total 6.03s: detect→quarantine 0.00s, cut 3.14s, repair call 2.87s
- cut: prepare 0.00s, export (backlog wait + snapshot) 3.13s, digest 0.01s
- repair: drain 0.00s, localise 0.12s, transfer 0.04s, apply 0.21s, verify 0.01s, rebase 0.03s (differing leaves 100, rows read 3940, upserted 66, deleted 34, full copies 0)
- replay: catch-up 2.41s, blocks replayed 142, live=1
- final check, all 3 nodes: PASS (3 nodes, rows 4000001, root 2a6f6a6505fa…); divergences: 0

### matrix f512 A rep2 attempt1 — **ACCEPTED**
- run: `cluster4_fanout_sweep_20261015_matrix_n4000000_f512_A_r2_a1` (3.2 min)
- TPS (majority visible): 7585
- final check, all 3 nodes: PASS (3 nodes, rows 4000001, root 2a6f6a6505fa…); divergences: 0

### matrix f1024 M_mixed rep2 attempt1 — **ACCEPTED**
- run: `cluster4_fanout_sweep_20261015_matrix_n4000000_f1024_M_mixed_r2_a1` (3.4 min)
- TPS (majority visible): 6184
- recovery total 6.35s: detect→quarantine 0.00s, cut 3.29s, repair call 3.04s
- cut: prepare 0.00s, export (backlog wait + snapshot) 3.28s, digest 0.02s
- repair: drain 0.00s, localise 0.25s, transfer 0.03s, apply 0.08s, verify 0.00s, rebase 0.03s (differing leaves 100, rows read 2043, upserted 66, deleted 34, full copies 0)
- replay: catch-up 2.57s, blocks replayed 143, live=1
- final check, all 3 nodes: PASS (3 nodes, rows 4000001, root 2a6f6a6505fa…); divergences: 0

### matrix f1024 A rep2 attempt1 — **ACCEPTED**
- run: `cluster4_fanout_sweep_20261015_matrix_n4000000_f1024_A_r2_a1` (3.5 min)
- TPS (majority visible): 7590
- final check, all 3 nodes: PASS (3 nodes, rows 4000001, root 2a6f6a6505fa…); divergences: 0

### matrix f2 M_mixed rep2 attempt1 — **ACCEPTED**
- run: `cluster4_fanout_sweep_20261015_matrix_n4000000_f2_M_mixed_r2_a1` (3.5 min)
- TPS (majority visible): 4638
- recovery total 21.66s: detect→quarantine 0.00s, cut 3.36s, repair call 18.28s
- cut: prepare 0.00s, export (backlog wait + snapshot) 3.34s, digest 0.02s
- repair: drain 0.00s, localise 0.04s, transfer 0.57s, apply 5.56s, verify 0.01s, rebase 0.03s (differing leaves 100, rows read 62523, upserted 66, deleted 34, full copies 0)
- replay: catch-up 12.04s, blocks replayed 145, live=1
- final check, all 3 nodes: PASS (3 nodes, rows 4000001, root 2a6f6a6505fa…); divergences: 0

### matrix f2 A rep2 attempt1 — **ACCEPTED**
- run: `cluster4_fanout_sweep_20261015_matrix_n4000000_f2_A_r2_a1` (3.1 min)
- TPS (majority visible): 7573
- final check, all 3 nodes: PASS (3 nodes, rows 4000001, root 2a6f6a6505fa…); divergences: 0

### matrix f8 A rep3 attempt1 — **ACCEPTED**
- run: `cluster4_fanout_sweep_20261015_matrix_n4000000_f8_A_r3_a1` (3.0 min)
- TPS (majority visible): 7670
- final check, all 3 nodes: PASS (3 nodes, rows 4000001, root 2a6f6a6505fa…); divergences: 0

### matrix f8 M_mixed rep3 attempt1 — **ACCEPTED**
- run: `cluster4_fanout_sweep_20261015_matrix_n4000000_f8_M_mixed_r3_a1` (3.2 min)
- TPS (majority visible): 6724
- recovery total 14.65s: detect→quarantine 0.00s, cut 3.64s, repair call 10.99s
- cut: prepare 0.00s, export (backlog wait + snapshot) 3.62s, digest 0.01s
- repair: drain 0.00s, localise 0.04s, transfer 0.27s, apply 1.38s, verify 0.01s, rebase 0.03s (differing leaves 100, rows read 31372, upserted 66, deleted 34, full copies 0)
- replay: catch-up 9.24s, blocks replayed 338, live=1
- final check, all 3 nodes: PASS (3 nodes, rows 4000001, root 2a6f6a6505fa…); divergences: 0

### matrix f16 A rep3 attempt1 — **ACCEPTED**
- run: `cluster4_fanout_sweep_20261015_matrix_n4000000_f16_A_r3_a1` (3.1 min)
- TPS (majority visible): 7045
- final check, all 3 nodes: PASS (3 nodes, rows 4000001, root 2a6f6a6505fa…); divergences: 0

### matrix f16 M_mixed rep3 attempt1 — **ACCEPTED**
- run: `cluster4_fanout_sweep_20261015_matrix_n4000000_f16_M_mixed_r3_a1` (2.9 min)
- TPS (majority visible): 7451
- recovery total 12.79s: detect→quarantine 0.00s, cut 3.37s, repair call 9.40s
- cut: prepare 0.00s, export (backlog wait + snapshot) 3.36s, digest 0.01s
- repair: drain 0.01s, localise 0.07s, transfer 0.09s, apply 0.31s, verify 0.01s, rebase 0.03s (differing leaves 100, rows read 7917, upserted 66, deleted 34, full copies 0)
- replay: catch-up 8.86s, blocks replayed 339, live=1
- final check, all 3 nodes: PASS (3 nodes, rows 4000001, root 2a6f6a6505fa…); divergences: 0

### matrix f32 A rep3 attempt1 — **ACCEPTED**
- run: `cluster4_fanout_sweep_20261015_matrix_n4000000_f32_A_r3_a1` (3.2 min)
- TPS (majority visible): 7729
- final check, all 3 nodes: PASS (3 nodes, rows 4000001, root 2a6f6a6505fa…); divergences: 0

### matrix f32 M_mixed rep3 attempt1 — **ACCEPTED**
- run: `cluster4_fanout_sweep_20261015_matrix_n4000000_f32_M_mixed_r3_a1` (3.4 min)
- TPS (majority visible): 7608
- recovery total 20.09s: detect→quarantine 0.00s, cut 3.24s, repair call 16.83s
- cut: prepare 0.00s, export (backlog wait + snapshot) 3.23s, digest 0.01s
- repair: drain 0.12s, localise 0.02s, transfer 0.50s, apply 1.94s, verify 0.01s, rebase 0.03s (differing leaves 100, rows read 62814, upserted 66, deleted 34, full copies 0)
- replay: catch-up 14.20s, blocks replayed 338, live=1
- final check, all 3 nodes: PASS (3 nodes, rows 4000001, root 2a6f6a6505fa…); divergences: 0

### matrix f64 A rep3 attempt1 — **ACCEPTED**
- run: `cluster4_fanout_sweep_20261015_matrix_n4000000_f64_A_r3_a1` (2.9 min)
- TPS (majority visible): 7720
- final check, all 3 nodes: PASS (3 nodes, rows 4000001, root 2a6f6a6505fa…); divergences: 0

### matrix f64 M_mixed rep3 attempt1 — **REJECTED**
- run: `cluster4_fanout_sweep_20261015_matrix_n4000000_f64_M_mixed_r3_a1` (2.7 min)
- TPS (majority visible): 5495
- failures: ['missing fetched rows']
- recovery total 14.43s: detect→quarantine 0.00s, cut 10.92s, repair call 3.49s
- cut: prepare 0.00s, export (backlog wait + snapshot) 10.86s, digest 0.05s
- repair: drain n/a, localise n/a, transfer n/a, apply n/a, verify n/a, rebase n/a (differing leaves 100, rows read ?, upserted 66, deleted 34, full copies 0)
- replay: catch-up 1.96s, blocks replayed n/a, live=1
- final check, all 3 nodes: PASS (3 nodes, rows 4000001, root 2a6f6a6505fa…); divergences: 0

### matrix f64 M_mixed rep3 attempt2 — **ACCEPTED**
- run: `cluster4_fanout_sweep_20261015_matrix_n4000000_f64_M_mixed_r3_a2` (3.1 min)
- TPS (majority visible): 7661
- recovery total 14.08s: detect→quarantine 0.00s, cut 3.17s, repair call 10.89s
- cut: prepare 0.00s, export (backlog wait + snapshot) 3.15s, digest 0.01s
- repair: drain 0.00s, localise 0.03s, transfer 0.35s, apply 1.47s, verify 0.01s, rebase 0.03s (differing leaves 100, rows read 31372, upserted 66, deleted 34, full copies 0)
- replay: catch-up 8.99s, blocks replayed 338, live=1
- final check, all 3 nodes: PASS (3 nodes, rows 4000001, root 2a6f6a6505fa…); divergences: 0

### matrix f128 A rep3 attempt1 — **ACCEPTED**
- run: `cluster4_fanout_sweep_20261015_matrix_n4000000_f128_A_r3_a1` (2.9 min)
- TPS (majority visible): 7695
- final check, all 3 nodes: PASS (3 nodes, rows 4000001, root 2a6f6a6505fa…); divergences: 0

### matrix f128 M_mixed rep3 attempt1 — **ACCEPTED**
- run: `cluster4_fanout_sweep_20261015_matrix_n4000000_f128_M_mixed_r3_a1` (3.1 min)
- TPS (majority visible): 7362
- recovery total 16.39s: detect→quarantine 0.00s, cut 3.30s, repair call 13.07s
- cut: prepare 0.00s, export (backlog wait + snapshot) 3.29s, digest 0.01s
- repair: drain 0.00s, localise 0.07s, transfer 0.33s, apply 1.50s, verify 0.01s, rebase 0.03s (differing leaves 233, rows read 36648, upserted 199, deleted 34, full copies 0)
- replay: catch-up 11.11s, blocks replayed 335, live=1
- final check, all 3 nodes: PASS (3 nodes, rows 4000001, root 2a6f6a6505fa…); divergences: 0

### matrix f256 A rep3 attempt1 — **ACCEPTED**
- run: `cluster4_fanout_sweep_20261015_matrix_n4000000_f256_A_r3_a1` (3.0 min)
- TPS (majority visible): 7547
- final check, all 3 nodes: PASS (3 nodes, rows 4000001, root 2a6f6a6505fa…); divergences: 0

### matrix f256 M_mixed rep3 attempt1 — **ACCEPTED**
- run: `cluster4_fanout_sweep_20261015_matrix_n4000000_f256_M_mixed_r3_a1` (3.2 min)
- TPS (majority visible): 7506
- recovery total 12.56s: detect→quarantine 0.00s, cut 3.04s, repair call 9.50s
- cut: prepare 0.00s, export (backlog wait + snapshot) 3.03s, digest 0.01s
- repair: drain 0.06s, localise 0.06s, transfer 0.11s, apply 0.37s, verify 0.00s, rebase 0.03s (differing leaves 100, rows read 7917, upserted 66, deleted 34, full copies 0)
- replay: catch-up 8.83s, blocks replayed 330, live=1
- final check, all 3 nodes: PASS (3 nodes, rows 4000001, root 2a6f6a6505fa…); divergences: 0

### matrix f512 A rep3 attempt1 — **ACCEPTED**
- run: `cluster4_fanout_sweep_20261015_matrix_n4000000_f512_A_r3_a1` (3.0 min)
- TPS (majority visible): 7599
- final check, all 3 nodes: PASS (3 nodes, rows 4000001, root 2a6f6a6505fa…); divergences: 0

### matrix f512 M_mixed rep3 attempt1 — **ACCEPTED**
- run: `cluster4_fanout_sweep_20261015_matrix_n4000000_f512_M_mixed_r3_a1` (2.9 min)
- TPS (majority visible): 7542
- recovery total 13.15s: detect→quarantine 0.00s, cut 3.66s, repair call 9.47s
- cut: prepare 0.00s, export (backlog wait + snapshot) 3.64s, digest 0.01s
- repair: drain 0.00s, localise 0.12s, transfer 0.04s, apply 0.31s, verify 0.01s, rebase 0.03s (differing leaves 100, rows read 4038, upserted 66, deleted 34, full copies 0)
- replay: catch-up 8.92s, blocks replayed 336, live=1
- final check, all 3 nodes: PASS (3 nodes, rows 4000001, root 2a6f6a6505fa…); divergences: 0

### matrix f1024 A rep3 attempt1 — **ACCEPTED**
- run: `cluster4_fanout_sweep_20261015_matrix_n4000000_f1024_A_r3_a1` (3.2 min)
- TPS (majority visible): 7528
- final check, all 3 nodes: PASS (3 nodes, rows 4000001, root 2a6f6a6505fa…); divergences: 0

### matrix f1024 M_mixed rep3 attempt1 — **ACCEPTED**
- run: `cluster4_fanout_sweep_20261015_matrix_n4000000_f1024_M_mixed_r3_a1` (2.9 min)
- TPS (majority visible): 5290
- recovery total 12.77s: detect→quarantine 0.00s, cut 3.55s, repair call 9.20s
- cut: prepare 0.00s, export (backlog wait + snapshot) 3.53s, digest 0.01s
- repair: drain 0.01s, localise 0.24s, transfer 0.03s, apply 0.08s, verify 0.00s, rebase 0.03s (differing leaves 100, rows read 2073, upserted 66, deleted 34, full copies 0)
- replay: catch-up 8.76s, blocks replayed 339, live=1
- final check, all 3 nodes: PASS (3 nodes, rows 4000001, root 2a6f6a6505fa…); divergences: 0

### matrix f2 A rep3 attempt1 — **ACCEPTED**
- run: `cluster4_fanout_sweep_20261015_matrix_n4000000_f2_A_r3_a1` (3.1 min)
- TPS (majority visible): 7644
- final check, all 3 nodes: PASS (3 nodes, rows 4000001, root 2a6f6a6505fa…); divergences: 0

### matrix f2 M_mixed rep3 attempt1 — **ACCEPTED**
- run: `cluster4_fanout_sweep_20261015_matrix_n4000000_f2_M_mixed_r3_a1` (3.6 min)
- TPS (majority visible): 4763
- recovery total 16.14s: detect→quarantine 0.00s, cut 3.73s, repair call 12.39s
- cut: prepare 0.00s, export (backlog wait + snapshot) 3.71s, digest 0.02s
- repair: drain 0.00s, localise 0.04s, transfer 0.55s, apply 1.80s, verify 0.01s, rebase 0.03s (differing leaves 100, rows read 62814, upserted 66, deleted 34, full copies 0)
- replay: catch-up 9.94s, blocks replayed 338, live=1
- final check, all 3 nodes: PASS (3 nodes, rows 4000001, root 2a6f6a6505fa…); divergences: 0

### matrix f4 A rep3 attempt1 — **ACCEPTED**
- run: `cluster4_fanout_sweep_20261015_matrix_n4000000_f4_A_r3_a1` (2.9 min)
- TPS (majority visible): 7048
- final check, all 3 nodes: PASS (3 nodes, rows 4000001, root 2a6f6a6505fa…); divergences: 0

### matrix f4 M_mixed rep3 attempt1 — **ACCEPTED**
- run: `cluster4_fanout_sweep_20261015_matrix_n4000000_f4_M_mixed_r3_a1` (3.2 min)
- TPS (majority visible): 6484
- recovery total 14.26s: detect→quarantine 0.00s, cut 3.65s, repair call 10.59s
- cut: prepare 0.00s, export (backlog wait + snapshot) 3.63s, digest 0.01s
- repair: drain 0.00s, localise 0.04s, transfer 0.27s, apply 1.40s, verify 0.01s, rebase 0.03s (differing leaves 100, rows read 31372, upserted 66, deleted 34, full copies 0)
- replay: catch-up 8.82s, blocks replayed 334, live=1
- final check, all 3 nodes: PASS (3 nodes, rows 4000001, root 2a6f6a6505fa…); divergences: 0

