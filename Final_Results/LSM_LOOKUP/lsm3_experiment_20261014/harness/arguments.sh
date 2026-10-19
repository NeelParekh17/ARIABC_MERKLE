#!/bin/bash
LSM_ROOT=/home/neel/claude_ctl/lsm_lookup_20261014
LSM_REPO=$LSM_ROOT/src
LSM_OUT=$LSM_ROOT/results
LSM_COMMON=(--remote-host 10.129.148.247 --remote-user neel
 --remote-dir /home/neel/ariabc_data/lsm_lookup_20261014
 --install-dir /home/neel/claude_checks/lsm_lookup_20261014/install
 --cluster-dir /home/neel/claude_checks/lsm_lookup_20261014/cluster
 --gateway-host 10.129.27.111 --gateway-user neel --gateway-repo "$LSM_REPO"
 --db-port 5488 --server-port 8088 --raft-port 9088 --base-dir-name base_ff90
 --db-rows 100000000 --shared-buffers 32MB --txs 20000 --seed 42
 --trials 3 --modes det merkle_btree merkle_lsm --workers 1 4 8 16
 --combos a:0.0 a:0.99 --pg-retry-jitter on --pg-exec-mode event --verify-mode fast
 --reset-mode delta --delta-content-check sampled --gateway-timeout 1800 --reset-timeout 7200 --verify-timeout 7200
 --settle-factor 1.25 --settle-min-s 20 --settle-max-s 1800 --calibrate-min-wait-s 300
 --usertable-fillfactor 90 --gen-shared-buffers 512MB --gen-maintenance-work-mem 256MB
 --gen-parallel-maintenance-workers 0 --gen-min-mem-available-mb 4608 --generation-timeout 21600)
