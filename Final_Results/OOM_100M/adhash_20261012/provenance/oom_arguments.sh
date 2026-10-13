#!/bin/bash
F15_REPO=/home/neel/claude_ctl/f15_adhash_20261012/oom_repo
F15_OUT=/home/neel/claude_ctl/f15_adhash_20261012/results
F15_REMOTE=/home/neel/ariabc_data/f15_adhash_20261012/oom
F15_COMMON=(--remote-host 10.129.148.247 --remote-user neel
  --remote-dir "$F15_REMOTE" --install-dir /home/neel/claude_checks/f15_adhash_20261012/install
  --cluster-dir /home/neel/claude_checks/f15_adhash_20261012/cluster
  --gateway-host 10.129.27.111 --gateway-user neel --gateway-repo "$F15_REPO"
  --db-port 5478 --server-port 8078 --raft-port 9078 --base-dir-name pgdata_adhash
  --db-rows 100000000 --shared-buffers 32MB --txs 20000 --seed 42
  --trials 3 --modes bcdb_merkle bcdb_det --workers 1 4 8 16
  --combos a:0.0 a:0.99 a:1.2 b:0.99 c:0.99 d:0.99 f:0.99
  --pg-retry-jitter on --pg-exec-mode event --verify-mode fast
  --reset-mode delta --delta-content-check sampled
  --gateway-timeout 1800 --reset-timeout 7200 --verify-timeout 7200
  --settle-factor 1.25 --settle-min-s 20 --settle-max-s 1800 --calibrate-min-wait-s 300
  --usertable-fillfactor 90 --gen-shared-buffers 512MB
  --gen-maintenance-work-mem 256MB --gen-parallel-maintenance-workers 0
  --gen-min-mem-available-mb 4608 --generation-timeout 21600)
