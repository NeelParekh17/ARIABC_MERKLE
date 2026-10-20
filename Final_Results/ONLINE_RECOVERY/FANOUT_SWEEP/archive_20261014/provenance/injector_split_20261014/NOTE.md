# Fault injector split into per-type transactions + late-fault guard (owner approved 2026-10-14)

Trigger: with one 100-row SERIALIZABLE fault transaction, busy-follower pivot cancellations either
exhausted the retries (f128 r1, f256 r2) or, at 30 retries, delayed the commit past the end of the
workload (f16 M_mixed r2 a1: done 17.5 s after start, after the last workload cut), so it could
not be detected live.

Changes:
- serializable_fault_injector.py: update / delete / insert phases each commit in their own
  SERIALIZABLE transaction (<=10 attempts each, same jittered backoff), all within a 10 s deadline
  (FANOUT_FAULT_DEADLINE_S). Keys fixed before any attempt; inserted keys no longer change between
  retries. Same 100 rows (33/33/34), same seed.
- collect_run.py: fault cases are rejected if fault_inject_done_epoch_ms is after
  PHASE_6_WORKLOAD_FINISHED ('fault landed after workload finished').
Accepted runs 1-18 are unaffected (their single transaction committed well inside the workload).
