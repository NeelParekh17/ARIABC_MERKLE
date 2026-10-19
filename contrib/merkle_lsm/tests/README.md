# Extended validation

Run only on the lab host, against a disposable installation, database cluster,
and socket directory inside `~/claude_checks/lsm_integration_20261019`. These
scripts create/drop their test tables/databases and `crash.py` deliberately
kills the merger or stops that cluster immediately. Never point them at an
existing deployment. The default PostgreSQL port is 5494. Set
`MERKLE_LSM_TEST_ROOT` only to an equivalent isolated root containing `install/`,
`data/`, `socket/`, `scratch/` and `evidence/`.

Install `merkle_lsm` and `pgcrypto`, preload `merkle_lsm`, enable synchronous
Merkle maintenance, use at most 256 MB shared_buffers/maintenance_work_mem,
set max_worker_processes >= 16 and default_transaction_isolation=serializable.
These scripts need psycopg 3. Run each detached with a separate retained log:

```sh
export TMPDIR="$HOME/claude_checks/lsm_integration_20261019/scratch"
nohup python3 -u src/contrib/merkle_lsm/tests/levels.py > evidence/levels.log 2>&1 < /dev/null &
```

Run `levels.py`, `capabilities.py`, `merkle.py`, `concurrency.py`, `pgbench.py`, `multiple.py`, then `crash.py`.
Run crash validation alone: it restarts PostgreSQL and changes debug GUCs.
Do not run core regression concurrently with crash validation. Every script
writes JSONL plus its normal log into `evidence/`. Failed runs are retained;
a final PASS marker only follows all assertions. `make installcheck` remains
the fast, self-contained SQL regression test.

`pgbench.py` runs six concurrent clients through actual custom pgbench scripts.
For this fork without --max-tries, an external per-client wrapper retries only
serialization failures and retains every attempt log.
