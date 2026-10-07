#!/bin/bash
# Source on .111; fixed paths keep the v2 campaign independent of other installs.
V2_REPO=/home/neel/claude_ctl/AriaBC_v2
V2_OUT=${1:-/home/neel/claude_ctl/results/oom_v2_20261001}
V2_REMOTE=/home/neel/ariabc_data/oom_v2_20261001
V2_INSTALL=/home/neel/claude_opt/install_v2
V2_CLUSTER=/home/neel/claude_opt/cluster_v2
V2_COMMON=(--remote-host 10.129.148.247 --remote-user neel
  --remote-dir "$V2_REMOTE" --install-dir "$V2_INSTALL" --cluster-dir "$V2_CLUSTER"
  --gateway-host 10.129.27.111 --gateway-user neel --gateway-repo "$V2_REPO"
  --db-port 5458 --server-port 8058 --base-dir-name pgdata_base_v2_ff90
  --db-rows 100000000 --shared-buffers 32MB --txs 20000 --seed 42
  --trials 1 --modes pg bcdb_det bcdb_merkle --workers 1 4 8 16
  --combos a:0.0 a:0.99 a:1.2 b:0.99 c:0.99 d:0.99 f:0.99
  --pg-retry-jitter on --pg-exec-mode event --verify-mode fast
  --reset-mode delta --delta-content-check sampled
  --gateway-timeout 1800 --reset-timeout 7200 --verify-timeout 7200
  --settle-factor 1.25 --settle-min-s 20 --settle-max-s 1800 --calibrate-min-wait-s 300
  --usertable-fillfactor 90 --gen-shared-buffers 512MB
  --gen-maintenance-work-mem 1GB --gen-parallel-maintenance-workers 2
  --gen-min-mem-available-mb 4608 --generation-timeout 21600)
