# Post-publication re-execution reproducer

Shows that a det transaction re-executed after it released its serial turn can
read a successor's committed write (the historical full-restart branches), and
that the settle path (`BCDB_DT_POST_PUBLISH_SETTLE`, default on) keeps det
execution equal to serial execution.

- `schema.sql`: `acct`, `outt`, `uq` (two unique indexes) and three functions.
  `ab_proc(a,b)` reads `acct[a]` and folds it into `outt[b]`; `bump_proc` writes
  `acct`; `uq_proc` moves a row to a new email (23505 when the email is taken).
- `gen_workload.py`: seeded mix (45% ab, 45% bump on 8 hot keys, 10% uq).
- `run_repro.sh <root> <label> <workers> <workload> [VAR=VALUE ...]`: serial
  psql run vs det run on one fresh cluster; prints `state_equal=yes|NO`.

The test failpoint `BCDB_FAILPOINT_POST_PUBLISH_APPLY=N` fails every attempt
of the first apply call of each transaction with `tx_id % N == 0` as a unique
violation. With `BCDB_DT_POST_PUBLISH_SETTLE=0` the `*_proc` statement then
takes the historical immediate full restart; with settle on it waits for its
predecessors and re-applies the same writes.

With settle off, a `*_proc` call that hits a genuine 23505 restarts forever
(the historical fast path re-executes and fails again), so historical-mode runs
use `--uq 0`.

Run only on the lab host (.247), never on the workstation.
