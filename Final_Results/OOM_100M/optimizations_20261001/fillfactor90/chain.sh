#!/bin/bash
cd /work/ARIABC/AriaBC
O=.bench_tmp/oom_ff90_20261001
until grep -q CAMPAIGN_DONE .bench_tmp/oom_walcomp_20261001/status.txt 2>/dev/null; do sleep 60; done
echo "walcomp done $(date)" >> $O/status.txt
ssh neel@10.129.148.247 'cd /tmp/ariabc_oom_100m && for d in pgdata pgdata1 pgdata_base pgdata_base_fanout32; do test -f $d/postmaster.pid && { echo "REFUSE $d running"; exit 1; }; done; rm -rf pgdata pgdata1 pgdata_base pgdata_base_fanout32 && sync && df -h / | tail -1' >> $O/status.txt 2>&1 || { echo DELETE_FAILED >> $O/status.txt; exit 1; }
TAG=$(date -u +%Y%m%dT%H%M%SZ)
ssh neel@10.129.148.247 "bash /tmp/ariabc_oom_100m/prepare_fillfactor90_inroot.sh /tmp/ariabc_oom_100m/ff90_prep_$TAG" > $O/prepare.log 2>&1
echo "prepare_rc=$? tag=$TAG" >> $O/status.txt
grep -q "pgdata_base_f32s1024_ff90" $O/prepare.log || { echo PREP_FAILED >> $O/status.txt; exit 1; }
C=(--install-dir /home/neel/claude_opt/install_opt --workers 1 16 --combos a:0.0 f:0.99 --trials 1 --skip-gen --verify-mode fast)
python3 -u scripts/distributed/oom_opt/run_wal_compression.py --wal-compression off "${C[@]}" --base-dir-name pgdata_base_f32s1024_ff90 --modes bcdb_det bcdb_merkle --out-dir $O/ff90_off > $O/ff90_off.log 2>&1
echo "ff90_rc=$?" >> $O/status.txt
echo CHAIN_DONE >> $O/status.txt
