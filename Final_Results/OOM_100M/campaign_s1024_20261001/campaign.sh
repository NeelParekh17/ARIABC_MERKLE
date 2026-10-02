#!/bin/bash
cd /work/ARIABC/AriaBC
OUT=.bench_tmp/oom_s1024_20261001
R="python3 -u scripts/distributed/run_oom_100m_benchmark.py --workers 1 4 8 16 --trials 1 --skip-gen"
$R --install-dir /home/neel/Desktop/ariabc_install --base-dir-name pgdata_base_fanout32_tblnamed --modes bcdb_det --combos a:0.99 f:0.99 --workers 1 16 --out-dir $OUT/det_control > $OUT/det_control.log 2>&1
echo "det_rc=$?" > $OUT/status.txt
$R --install-dir /home/neel/claude_opt/install_opt --base-dir-name pgdata_base_f32s1024 --modes bcdb_merkle --combos a:0.0 a:0.99 a:1.2 b:0.99 d:0.99 f:0.99 --out-dir $OUT/merkle > $OUT/merkle.log 2>&1
echo "merkle_rc=$?" >> $OUT/status.txt
echo CAMPAIGN_DONE >> $OUT/status.txt
