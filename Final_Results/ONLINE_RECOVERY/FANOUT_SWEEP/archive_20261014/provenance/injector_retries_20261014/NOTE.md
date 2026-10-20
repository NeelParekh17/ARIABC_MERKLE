# Fault-injector retry budget 10 -> 30 (owner approved 2026-10-14)

Reason: 4M M_mixed runs on follower node4 lost the SERIALIZABLE corruption transaction to 40001
pivot cancellations on all 10 attempts (f128 r1 a1; f256 r2 a1 and a2), each stopping the sweep.
Change: serializable_fault_injector.py max_attempts 10 -> 30; backoff unchanged (full jitter, cap 2 s);
fault content, keys, seed, isolation unchanged. Accepted runs 1-16 never needed more than 10 attempts.
Actual injection time is recorded per run (fault_timeline.env: fault_inject_start/done_epoch_ms).
