#!/bin/bash
cd /work/ARIABC/AriaBC
O=.bench_tmp/oom_walcomp_20261001
C=(--install-dir /home/neel/claude_opt/install_opt --workers 1 16 --combos a:0.0 f:0.99 --trials 1 --skip-gen --verify-mode fast)
W=(python3 -u scripts/distributed/oom_opt/run_wal_compression.py)
M=(--base-dir-name pgdata_base_f32s1024 --modes bcdb_merkle)
D=(--base-dir-name pgdata_base_fanout32_tblnamed --modes bcdb_det)
"${W[@]}" --wal-compression off "${C[@]}" "${D[@]}" --out-dir $O/det_off > $O/det_off.log 2>&1; echo "det_off=$?" >> $O/status.txt
"${W[@]}" --wal-compression on  "${C[@]}" "${D[@]}" --out-dir $O/det_on  > $O/det_on.log 2>&1;  echo "det_on=$?" >> $O/status.txt
"${W[@]}" --wal-compression on  "${C[@]}" "${M[@]}" --out-dir $O/merkle_on  > $O/merkle_on.log 2>&1;  echo "merkle_on=$?" >> $O/status.txt
"${W[@]}" --wal-compression off "${C[@]}" "${M[@]}" --out-dir $O/merkle_off > $O/merkle_off.log 2>&1; echo "merkle_off=$?" >> $O/status.txt
echo CAMPAIGN_DONE >> $O/status.txt
