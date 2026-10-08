# Shared brief for all det-optimisation agents (2026-10-07)

You are one of four parallel agents. Each implements one optimisation of AriaBC's deterministic (det) execution path in its OWN git worktree and branch. Another agent (Claude) reviews, merges and runs the final timed A/B benchmarks. Work only inside your worktree, plus your own ranking variant directory, plus your report file.

## System background (verified by reading the code)

- Det path: `bcdb_worker_process_tx_dt()` in `src/backend/bcdb/worker.c` (~L2537).
  1. A transaction (tx) executes speculatively against a snapshot.
     - `activeTx->tx_id_committed` is the baseline, i.e. the contiguous commit watermark at snapshot time.
     - Read and write tags are recorded into process-local lists `rs_table_record`, `ws_table_record` and `ws_table_publish_record` by `rs_table_reserveDT`, `ws_table_reserveDT` and `ws_table_reserve_publish_onlyDT` (`src/backend/bcdb/shm_transaction.c` ~L1195-1290).
  2. The tx then waits for its turn in `bcdb_wait_for_serial_slot()` (worker.c ~L1297): `published_max+1 >= tx_id`.
  3. `conflict_checkDT()` (shm_transaction.c ~L3223) probes every ws and rs tag via `table_checkDT()` (~L1404) against two shared partitioned hash maps (`map`, `mapB`).
     - Each entry stores only the MAX writer tx_id for that tag.
     - A conflict is any entry with `tx_id_committed < entry.tx_id < own tx_id`.
  4. If there is no conflict, `publish_ws_tableDT()` (~L3368) inserts the tags, then `set_published_max_txid()` / `mark_published_ready_txid()` (`src/backend/bcdb/shm_block.c` ~L1079) releases the turn to tx+1.
     - Apply and PostgreSQL commit then happen OUTSIDE the turn.
     - `result_committed_txid[slot]` is set after `finish_xact_command()`.
     - `advance_last_committed_txid()` (shm_block.c ~L694) advances the contiguous watermark without blocking.
  5. On conflict, the tx retries WHILE HOLDING THE TURN.
     - The `for(;;)` loop goes to the `rw_conflicts == 1` branch at ~L2680.
     - That branch aborts, then `bcdb_wait_for_target_committed()` (~L1643) waits for the CONTIGUOUS watermark to reach the conflicting tx.
     - It then re-executes and re-checks. Every later tx is blocked meanwhile.
- Measured on the v2 TPC-C traces (W100, 32 workers, 2815 TPS ⇒ ~355 µs of serial "turn" per tx):
  - ~40% is retries held at the turn: ~1.45 ms of unaccounted retry wait per retried tx, plus re-execution.
  - ~39% is the in-turn conflict check (115 µs, ~145 probes) plus publish (23 µs).
  - ~20% is handoff and other costs.
  - At W5 retries dominate: 65% of txs retry.
- Correctness invariant you MUST keep: never miss a real conflict.
  - A real conflict is a predecessor whose writes to a tag we touched are not visible in our snapshot.
  - False positives (extra retries) are allowed but cost TPS.
  - The final DB state for a given workload is deterministic and must match the reference state hashes exactly (see below).

## Hard rules

- Never build or run PostgreSQL or benchmarks on this workstation.
  - All builds and DB runs happen on the ranking host `protectdr@10.129.7.57` (ssh works without a password), under `~/claude_checks/detopt_20261007/<variant>/` only.
  - Never use /tmp there.
  - Never touch other directories on ranking, the canonical `~/Desktop/ariabc_cluster`, or other users' processes.
- Do not modify `harness/*.sh` on ranking. Do not change benchmark settings to gain TPS. Optimise the code only.
- Keep PostgreSQL C style (tabs, `bcdb_` prefixes). Keep the change focused; no broad refactors.
- Do NOT reintroduce `BCDB_DT_SKIP_READONLY_GATE` usage. It is unsafe, because max-writer map entries let a successor's publish hide an earlier writer.
- Commit your work on your branch in your worktree (`git commit`); do NOT push and do NOT touch `main` or other branches.
  - End commit messages with: `Co-Authored-By: Codex (gpt-6.1-sol) <noreply@openai.com>`
- Make your optimisation switchable at runtime by an environment variable read once and cached, the same way `bcdb_dt_skip_readonly_gate_enabled()` does it in worker.c.
  - Default it to ON in your branch.
  - Setting it to `0` must give exactly baseline behaviour, so the A/B can be done with one binary.
- Graphify: the graph lives in the main tree. Run `cd /work/ARIABC/AriaBC && graphify query "..."` for navigation if useful. Do not run `graphify update` in the worktree.

## Ranking workflow (your variant name is given in your task)

1. Sync the files you changed (the remote src already holds an exact copy of main HEAD 8d0968d4):
   `cd <your worktree> && { git diff --name-only main; git ls-files --others --exclude-standard; } | sort -u | rsync -a --files-from=- ./ protectdr@10.129.7.57:claude_checks/detopt_20261007/<variant>/src/`
2. Build with `ssh protectdr@10.129.7.57 'bash ~/claude_checks/detopt_20261007/harness/build.sh <variant>'`.
   - The first build configures (~1-2 min); later builds are incremental.
   - On failure it prints the tail of the log; full logs are in `<variant>/build_*.log`.
3. Benchmark with `ssh protectdr@10.129.7.57 'PORT=<p> CLIENT_PORT=<c> RAFT_PORT=<r> ~/claude_checks/detopt_20261007/harness/bench.sh <variant> <W> 32 <trial> <off|on>'`.
   - The last argument turns the per-tx phase trace on or off. The trace CSVs end up in `~/claude_checks/tpcc_sweep_v2_detopt_20261007/<variant>/det_p<pt>_k32_t<trial>_w<W>/ptrace_keep/`.
   - A blocking flock serialises all DB runs host-wide, so your run may queue behind others. That is expected; just wait.
   - Approximate run times: W5 ≈ 1.5 min, W30 ≈ 2 min, W100 ≈ 8-10 min.
   - Run budget: W5 and W30 as often as you need for correctness, and at most 2 runs at W100.
   - Use a fresh trial number for each attempt; existing run dirs are refused.
   - To pass env flags to the server (e.g. your on/off switch), prefix them to bench.sh. The harness starts postgres via pg_ctl, which inherits the env.
4. Acceptance. Every run must show `rc=0 divergence_count=0 permanent_failures=0`, and the final-state hash must match the reference exactly:
   - W5: `state=fdb9545f1c588209`
   - W30: `state=dcb895e5961ab8e8`
   - W100: `state=e82921e1ebb1b7ab`
   - The result line is printed by bench.sh and appended to `~/claude_checks/detopt_20261007/status.txt`.
   - A mismatch means a correctness bug. Fix it; never accept it.
5. Baseline numbers (variant `base`, ptrace off, window 65536): see status.txt. So far W5 ≈ 974 TPS and W30 ≈ 2100 TPS; W100 is pending (it was ~2815 with trace on in the published v2 campaign).

## Deliverable

Write a report at `/work/ARIABC/AriaBC/.bench_tmp/detopt_20261007/<variant>_REPORT.md`. It must cover:
- what you changed, with file:line references;
- why it is correct, as an argument about the conflict invariant;
- the env switch;
- every run you did: the exact status.txt lines and the TPS against base;
- risks and open questions.

Your final message should summarise the same in at most 25 lines.
