## TPS

| case | run TPS | total TPS (incl. drain+ckpt) | drain s | ckpt s | verify | unmapped I/O |
|---|---|---|---|---|---|---|
| insert_btree_t1 | 3405 | 3404 | 0.0 | 0.2 | t | 0.16 GB |
| insert_btree_t2 | 3027 | 2978 | 0.0 | 12.8 | t | 0.15 GB |
| insert_lsm_t1 | 3963 | 3874 | 11.7 | 2.2 | t | 0.75 GB |
| insert_lsm_t2 | 3524 | 3302 | 45.8 | 0.1 | t | 0.69 GB |
| insert_plain_t1 | 8641 | 8639 | 0.0 | 0.1 | N/A | 17 MB |
| insert_plain_t2 | 8223 | 8172 | 0.0 | 1.8 | N/A | 18 MB |
| update_btree_t1 | 3085 | 3081 | 0.0 | 1.0 | t | 3.11 GB |
| update_btree_t2 | 2736 | 2736 | 0.0 | 0.0 | t | 3.11 GB |
| update_lsm_t1 | 2817 | 2808 | 0.9 | 1.8 | t | 3.06 GB |
| update_lsm_t2 | 2773 | 2746 | 1.1 | 7.4 | t | 3.07 GB |
| update_plain_t1 | 3541 | 3483 | 0.0 | 11.3 | N/A | 3.11 GB |
| update_plain_t2 | 3086 | 2984 | 0.0 | 26.7 | N/A | 3.10 GB |

## insert t1: per relation, all phases summed (blocks read / written from file, WAL, FPI, pages dirtied)

| relation | plain rd / wr / WAL / FPI | btree rd / wr / WAL / FPI | lsm rd / wr / WAL / FPI |
|---|---|---|---|
| heap | 679 / 166K / 0.65 GB / 1 | 56K / 166K / 0.65 GB / 1 | 42K / 166K / 0.65 GB / 1 |
| PK | 5 / 13K / 0.16 GB / 3 | 5 / 13K / 0.16 GB / 3 | 5 / 13K / 0.16 GB / 3 |
| lookup base | 0 / 0 / 0 / 0 | 3.94M / 2.39M / 3.00 GB / 382K | 1.51M / 1.49M / 3.00 GB / 382K |
| LSM L0 | 0 / 0 / 0 / 0 | 0 / 0 / 0 / 0 | 30K / 50K / 0.22 GB / 98 |
| LSM L1 | 0 / 0 / 0 / 0 | 0 / 0 / 0 / 0 | 106K / 118K / 0.21 GB / 7 |
| node heap | 0 / 0 / 0 / 0 | 1.29M / 1.29M / 1.17 GB / 3020 | 1.53M / 1.51M / 0.95 GB / 3020 |
| node pkey | 0 / 0 / 0 / 0 | 221K / 0 / 0 / 0 | 553K / 94K / 18 MB / 814 |
| node prefix_idx | 0 / 0 / 0 / 0 | 0 / 0 / 0 / 0 | 89K / 91K / 19 MB / 854 |
| node root_idx | 0 / 0 / 0 / 0 | 0 / 0 / 0 / 0 | 17K / 17K / 3 MB / 1 |
| LSM meta | 0 / 0 / 0 / 0 | 0 / 0 / 0 / 0 | 52 / 2 / 0 MB / 1 |
| merkle idx | 0 / 0 / 0 / 0 | 1 / 0 / 0 / 0 | 1 / 0 / 0 / 0 |

## insert t2: per relation, all phases summed (blocks read / written from file, WAL, FPI, pages dirtied)

| relation | plain rd / wr / WAL / FPI | btree rd / wr / WAL / FPI | lsm rd / wr / WAL / FPI |
|---|---|---|---|
| heap | 1389 / 166K / 0.65 GB / 1 | 55K / 166K / 0.65 GB / 1 | 42K / 166K / 0.65 GB / 1 |
| PK | 5 / 13K / 0.16 GB / 3 | 5 / 13K / 0.16 GB / 3 | 5 / 13K / 0.16 GB / 3 |
| lookup base | 0 / 0 / 0 / 0 | 3.94M / 2.39M / 3.00 GB / 382K | 1.51M / 1.50M / 3.00 GB / 382K |
| LSM L0 | 0 / 0 / 0 / 0 | 0 / 0 / 0 / 0 | 28K / 48K / 0.22 GB / 98 |
| LSM L1 | 0 / 0 / 0 / 0 | 0 / 0 / 0 / 0 | 106K / 118K / 0.21 GB / 7 |
| node heap | 0 / 0 / 0 / 0 | 1.29M / 1.29M / 1.17 GB / 3020 | 1.48M / 1.46M / 0.95 GB / 3020 |
| node pkey | 0 / 0 / 0 / 0 | 221K / 0 / 0 / 0 | 533K / 85K / 17 MB / 814 |
| node prefix_idx | 0 / 0 / 0 / 0 | 0 / 0 / 0 / 0 | 79K / 80K / 18 MB / 854 |
| node root_idx | 0 / 0 / 0 / 0 | 0 / 0 / 0 / 0 | 13K / 13K / 2 MB / 1 |
| LSM meta | 0 / 0 / 0 / 0 | 0 / 0 / 0 / 0 | 37 / 2 / 0 MB / 1 |
| merkle idx | 0 / 0 / 0 / 0 | 1 / 0 / 0 / 0 | 1 / 0 / 0 / 0 |

## update t1: per relation, all phases summed (blocks read / written from file, WAL, FPI, pages dirtied)

| relation | plain rd / wr / WAL / FPI | btree rd / wr / WAL / FPI | lsm rd / wr / WAL / FPI |
|---|---|---|---|
| heap | 1.19M / 1.19M / 4.45 GB / 504K | 1.32M / 1.32M / 4.45 GB / 504K | 1.32M / 1.32M / 4.45 GB / 504K |
| PK | 1.09M / 28 / 0 MB / 26 | 1.35M / 37 / 0 MB / 26 | 1.34M / 36 / 0 MB / 29 |
| lookup base | 0 / 0 / 0 / 0 | 647 / 172 / 1 MB / 157 | 314 / 153 / 1 MB / 153 |
| LSM L0 | 0 / 0 / 0 / 0 | 0 / 0 / 0 / 0 | 131 / 128 / 0 MB / 1 |
| LSM L1 | 0 / 0 / 0 / 0 | 0 / 0 / 0 / 0 | 2 / 3 / 0 MB / 1 |
| node heap | 0 / 0 / 0 / 0 | 1.02M / 1.02M / 1.04 GB / 3020 | 1.02M / 1.02M / 1.04 GB / 3020 |
| node pkey | 0 / 0 / 0 / 0 | 181K / 1 / 0 MB / 1 | 181K / 0 / 0 / 0 |
| node prefix_idx | 0 / 0 / 0 / 0 | 4 / 1 / 0 MB / 1 | 0 / 0 / 0 / 0 |
| LSM meta | 0 / 0 / 0 / 0 | 0 / 0 / 0 / 0 | 281 / 1 / 0 MB / 1 |
| merkle idx | 0 / 0 / 0 / 0 | 1 / 0 / 0 / 0 | 1 / 0 / 0 / 0 |

## update t2: per relation, all phases summed (blocks read / written from file, WAL, FPI, pages dirtied)

| relation | plain rd / wr / WAL / FPI | btree rd / wr / WAL / FPI | lsm rd / wr / WAL / FPI |
|---|---|---|---|
| heap | 1.19M / 1.19M / 4.45 GB / 504K | 1.31M / 1.32M / 4.45 GB / 504K | 1.31M / 1.32M / 4.45 GB / 504K |
| PK | 1.09M / 17 / 0 MB / 17 | 1.35M / 36 / 0 MB / 26 | 1.35M / 40 / 0 MB / 28 |
| lookup base | 0 / 0 / 0 / 0 | 579 / 158 / 1 MB / 152 | 330 / 160 / 1 MB / 160 |
| LSM L0 | 0 / 0 / 0 / 0 | 0 / 0 / 0 / 0 | 138 / 134 / 0 MB / 1 |
| LSM L1 | 0 / 0 / 0 / 0 | 0 / 0 / 0 / 0 | 2 / 3 / 0 MB / 1 |
| node heap | 0 / 0 / 0 / 0 | 1.02M / 1.02M / 1.04 GB / 3020 | 1.02M / 1.02M / 1.04 GB / 3020 |
| node pkey | 0 / 0 / 0 / 0 | 181K / 1 / 0 MB / 1 | 181K / 1 / 0 MB / 1 |
| node prefix_idx | 0 / 0 / 0 / 0 | 4 / 1 / 0 MB / 1 | 4 / 1 / 0 MB / 1 |
| LSM meta | 0 / 0 / 0 / 0 | 0 / 0 / 0 / 0 | 317 / 1 / 0 MB / 1 |
| merkle idx | 0 / 0 / 0 / 0 | 1 / 0 / 0 / 0 | 1 / 0 / 0 / 0 |

## Access pattern (bpftrace VFS offsets, main fork). far = distance >= 64 blocks from the previous I/O of the same pid on the same file

| case | phase | actor | relation | op | n | +1 % | near(<64) % | far % |
|---|---|---|---|---|---|---|---|---|
| insert_btree_t1 | foreground | bgwriter | heap | write | 6421 | 83 | 94 | 6 |
| insert_btree_t1 | foreground | foreground_or_monitor | heap | read | 55K | 0 | 96 | 4 |
| insert_btree_t1 | foreground | foreground_or_monitor | heap | write | 159K | 54 | 95 | 5 |
| insert_btree_t1 | foreground | foreground_or_monitor | PK | write | 13K | 7 | 100 | 0 |
| insert_btree_t1 | foreground | bgwriter | lookup base | write | 188K | 0 | 0 | 100 |
| insert_btree_t1 | foreground | foreground_or_monitor | lookup base | read | 3.94M | 0 | 13 | 87 |
| insert_btree_t1 | foreground | foreground_or_monitor | lookup base | write | 2.20M | 0 | 0 | 100 |
| insert_btree_t1 | foreground | unknown_pid | lookup base | write | 8094 | 0 | 0 | 100 |
| insert_btree_t1 | foreground | bgwriter | node heap | write | 103K | 0 | 6 | 94 |
| insert_btree_t1 | foreground | foreground_or_monitor | node heap | read | 1.29M | 0 | 13 | 87 |
| insert_btree_t1 | foreground | foreground_or_monitor | node heap | write | 1.18M | 0 | 4 | 96 |
| insert_btree_t1 | foreground | unknown_pid | node heap | write | 4497 | 0 | 6 | 94 |
| insert_btree_t1 | foreground | foreground_or_monitor | node pkey | read | 221K | 1 | 15 | 85 |
| insert_lsm_t1 | foreground | bgwriter | heap | write | 11K | 90 | 96 | 4 |
| insert_lsm_t1 | foreground | foreground_or_monitor | heap | read | 42K | 0 | 86 | 14 |
| insert_lsm_t1 | foreground | foreground_or_monitor | heap | write | 145K | 55 | 89 | 11 |
| insert_lsm_t1 | foreground | merger | heap | write | 8764 | 68 | 97 | 3 |
| insert_lsm_t1 | foreground | foreground_or_monitor | PK | write | 12K | 7 | 99 | 1 |
| insert_lsm_t1 | drain | bgwriter | lookup base | write | 5172 | 12 | 64 | 36 |
| insert_lsm_t1 | drain | merger | lookup base | read | 239K | 54 | 99 | 1 |
| insert_lsm_t1 | drain | merger | lookup base | write | 228K | 19 | 50 | 50 |
| insert_lsm_t1 | foreground | bgwriter | lookup base | write | 34K | 19 | 47 | 53 |
| insert_lsm_t1 | foreground | foreground_or_monitor | lookup base | write | 343K | 1 | 30 | 70 |
| insert_lsm_t1 | foreground | merger | lookup base | read | 1.27M | 65 | 100 | 0 |
| insert_lsm_t1 | foreground | merger | lookup base | write | 881K | 17 | 48 | 52 |
| insert_lsm_t1 | foreground | foreground_or_monitor | LSM L0 | read | 28K | 1 | 56 | 44 |
| insert_lsm_t1 | foreground | foreground_or_monitor | LSM L0 | write | 28K | 4 | 74 | 26 |
| insert_lsm_t1 | foreground | merger | LSM L0 | read | 2174 | 1 | 54 | 46 |
| insert_lsm_t1 | foreground | merger | LSM L0 | write | 18K | 1 | 62 | 38 |
| insert_lsm_t1 | foreground | bgwriter | LSM L1 | write | 12K | 1 | 2 | 98 |
| insert_lsm_t1 | foreground | foreground_or_monitor | LSM L1 | write | 87K | 4 | 12 | 88 |
| insert_lsm_t1 | foreground | merger | LSM L1 | read | 104K | 1 | 1 | 99 |
| insert_lsm_t1 | foreground | merger | LSM L1 | write | 18K | 66 | 67 | 33 |
| insert_lsm_t1 | foreground | bgwriter | node heap | write | 156K | 0 | 3 | 97 |
| insert_lsm_t1 | foreground | foreground_or_monitor | node heap | read | 1.53M | 0 | 4 | 96 |
| insert_lsm_t1 | foreground | foreground_or_monitor | node heap | write | 1.13M | 0 | 3 | 97 |
| insert_lsm_t1 | foreground | merger | node heap | write | 217K | 0 | 4 | 96 |
| insert_lsm_t1 | foreground | bgwriter | node pkey | write | 6544 | 0 | 8 | 92 |
| insert_lsm_t1 | foreground | foreground_or_monitor | node pkey | read | 553K | 1 | 9 | 91 |
| insert_lsm_t1 | foreground | foreground_or_monitor | node pkey | write | 47K | 0 | 8 | 92 |
| insert_lsm_t1 | foreground | merger | node pkey | write | 41K | 0 | 10 | 90 |
| insert_lsm_t1 | foreground | bgwriter | node prefix_idx | write | 3122 | 0 | 10 | 90 |
| insert_lsm_t1 | foreground | foreground_or_monitor | node prefix_idx | read | 89K | 1 | 14 | 86 |
| insert_lsm_t1 | foreground | foreground_or_monitor | node prefix_idx | write | 38K | 0 | 9 | 91 |
| insert_lsm_t1 | foreground | merger | node prefix_idx | write | 50K | 0 | 9 | 91 |
| insert_lsm_t1 | foreground | foreground_or_monitor | node root_idx | read | 17K | 1 | 90 | 10 |
| insert_lsm_t1 | foreground | foreground_or_monitor | node root_idx | write | 6476 | 2 | 89 | 11 |
| insert_lsm_t1 | foreground | merger | node root_idx | write | 10K | 1 | 90 | 10 |
| insert_plain_t1 | checkpoint | checkpointer | heap | write | 3112 | 100 | 100 | 0 |
| insert_plain_t1 | foreground | bgwriter | heap | write | 74K | 100 | 100 | 0 |
| insert_plain_t1 | foreground | foreground_or_monitor | heap | write | 88K | 85 | 86 | 14 |
| insert_plain_t1 | foreground | bgwriter | PK | write | 6022 | 98 | 99 | 1 |
| insert_plain_t1 | foreground | foreground_or_monitor | PK | write | 6866 | 11 | 96 | 4 |
| update_btree_t1 | foreground | bgwriter | heap | write | 169K | 0 | 0 | 100 |
| update_btree_t1 | foreground | foreground_or_monitor | heap | read | 1.31M | 0 | 0 | 100 |
| update_btree_t1 | foreground | foreground_or_monitor | heap | write | 1.14M | 0 | 0 | 100 |
| update_btree_t1 | foreground | unknown_pid | heap | write | 3548 | 0 | 1 | 99 |
| update_btree_t1 | foreground | foreground_or_monitor | PK | read | 1.35M | 0 | 7 | 93 |
| update_btree_t1 | foreground | bgwriter | node heap | write | 133K | 1 | 7 | 93 |
| update_btree_t1 | foreground | foreground_or_monitor | node heap | read | 1.02M | 2 | 14 | 86 |
| update_btree_t1 | foreground | foreground_or_monitor | node heap | write | 881K | 0 | 5 | 95 |
| update_btree_t1 | foreground | unknown_pid | node heap | write | 4599 | 1 | 7 | 93 |
| update_btree_t1 | foreground | foreground_or_monitor | node pkey | read | 181K | 1 | 15 | 85 |
| update_lsm_t1 | foreground | bgwriter | heap | write | 177K | 0 | 0 | 100 |
| update_lsm_t1 | foreground | foreground_or_monitor | heap | read | 1.31M | 0 | 0 | 100 |
| update_lsm_t1 | foreground | foreground_or_monitor | heap | write | 1.13M | 0 | 0 | 100 |
| update_lsm_t1 | foreground | merger | heap | write | 3502 | 0 | 0 | 100 |
| update_lsm_t1 | foreground | foreground_or_monitor | PK | read | 1.34M | 0 | 7 | 93 |
| update_lsm_t1 | foreground | bgwriter | node heap | write | 139K | 1 | 7 | 93 |
| update_lsm_t1 | foreground | foreground_or_monitor | node heap | read | 1.02M | 2 | 14 | 86 |
| update_lsm_t1 | foreground | foreground_or_monitor | node heap | write | 876K | 0 | 5 | 95 |
| update_lsm_t1 | foreground | merger | node heap | write | 2777 | 0 | 7 | 93 |
| update_lsm_t1 | foreground | foreground_or_monitor | node pkey | read | 181K | 1 | 15 | 85 |
| update_plain_t1 | foreground | bgwriter | heap | write | 269K | 0 | 0 | 100 |
| update_plain_t1 | foreground | foreground_or_monitor | heap | read | 1.19M | 0 | 0 | 100 |
| update_plain_t1 | foreground | foreground_or_monitor | heap | write | 911K | 0 | 0 | 100 |
| update_plain_t1 | foreground | unknown_pid | heap | write | 3965 | 0 | 0 | 100 |
| update_plain_t1 | foreground | foreground_or_monitor | PK | read | 1.09M | 0 | 5 | 95 |
