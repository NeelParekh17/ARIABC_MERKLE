#!/bin/bash
# Run ON .111 (controller) inside tmux/nohup. Survives the laptop being closed.
cd /home/neel/claude_ctl/AriaBC
O=/home/neel/claude_ctl/results/oom_ff90c_20261001; mkdir -p $O
S=$O/status.txt; log() { echo "$(date '+%F %T') $*" | tee -a $S; }
EV=/home/neel/ariabc_data/oom_100m/ff90c_prep_20261001
log "campaign start"
scp -q scripts/distributed/oom_opt/prep_ff90c_compact.sh neel@10.129.148.247:/home/neel/ariabc_data/oom_100m/prep_ff90c_compact.sh
ssh neel@10.129.148.247 "test -f $EV/PREP_STATUS && cat $EV/PREP_STATUS" | grep -q OK && log "prep already done" || {
  ssh neel@10.129.148.247 "rm -rf $EV; nohup bash /home/neel/ariabc_data/oom_100m/prep_ff90c_compact.sh $EV >/dev/null 2>&1 </dev/null &"
  log "prep started on .247"
  for i in $(seq 1 360); do st=$(ssh neel@10.129.148.247 "cat $EV/PREP_STATUS 2>/dev/null"); [ -n "$st" ] && break; sleep 30; done
  [ "$st" = OK ] || { log "PREP FAILED ($st)"; scp -qr neel@10.129.148.247:$EV $O/preparation; echo CAMPAIGN_FAILED >> $S; exit 1; }
  log "prep OK"; }
scp -qr neel@10.129.148.247:$EV $O/preparation
C=(--workers 1 16 --combos a:0.0 f:0.99 --trials 1 --skip-gen --verify-mode fast)
W=(python3 -u scripts/distributed/oom_opt/run_wal_compression.py --wal-compression off)
COMPACT=(--install-dir /home/neel/claude_opt/install_opt2 --base-dir-name pgdata_base_f32s1024_ff90c --modes bcdb_merkle)
BLOATED=(--install-dir /home/neel/claude_opt/install_opt --base-dir-name pgdata_base_f32s1024_ff90 --modes bcdb_merkle)
DET=(--install-dir /home/neel/claude_opt/install_opt --base-dir-name pgdata_base_f32s1024_ff90 --modes bcdb_det)
run() { name=$1; shift; log "start $name"; "${W[@]}" "$@" "${C[@]}" --out-dir $O/$name > $O/$name.log 2>&1; rc=$?; log "end $name rc=$rc"; [ $rc -eq 0 ] || { echo CAMPAIGN_FAILED >> $S; exit 1; }; }
run R1_compact "${COMPACT[@]}"
run R2_bloated "${BLOATED[@]}"
run R3_compact "${COMPACT[@]}"
run R4_bloated "${BLOATED[@]}"
run R5_det "${DET[@]}"
python3 scripts/distributed/oom_opt/summarize_ff90c.py $O > $O/summary.md 2>&1; log "summary written"
echo CAMPAIGN_DONE >> $S
