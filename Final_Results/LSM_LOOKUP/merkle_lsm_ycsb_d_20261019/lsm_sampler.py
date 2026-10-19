# 1 Hz sampler for the merkle_lsm D runs: merge state, component sizes,
# commits and WAL position. Reconnects across server restarts; stops when
# the stop file exists. Uses merkle_lsm_stats (no component scans).
import json, os, sys, time
from pathlib import Path
import psycopg
root = Path.home() / 'claude_checks/lsm_bench_20261019'
out = root / 'evidence/lsm_sampler.jsonl'
stop = root / 'sampler.stop'
port = int(sys.argv[1]) if len(sys.argv) > 1 else 5488
Q = """SELECT merkle_lsm_stats('usertable_merkle_lookup_idx')::text,
  (SELECT xact_commit FROM pg_stat_database WHERE datname='postgres'),
  pg_current_wal_lsn()::text,
  (SELECT string_agg(coalesce(query,''),'|') FROM pg_stat_activity WHERE backend_type='merkle_lsm merger')"""
conn = None
with out.open('a') as f:
    while not stop.exists():
        t = time.time()
        try:
            if conn is None or conn.closed:
                conn = psycopg.connect(f'host=127.0.0.1 port={port} user=postgres dbname=postgres',
                                       autocommit=True, connect_timeout=2)
                conn.execute("SET default_transaction_isolation = 'read committed'")
                f.write(json.dumps(dict(t=t, event='connected')) + '\n')
            stats, commits, lsn, merger = conn.execute(Q).fetchone()
            f.write(json.dumps(dict(t=t, stats=json.loads(stats), commits=commits, lsn=lsn, merger=merger)) + '\n')
        except Exception as e:
            if conn is not None and not conn.closed:
                try: conn.close()
                except Exception: pass
            conn = None
            f.write(json.dumps(dict(t=t, error=str(e)[:300])) + "\n")
        f.flush()
        time.sleep(max(0.0, 1.0 - (time.time() - t)))
