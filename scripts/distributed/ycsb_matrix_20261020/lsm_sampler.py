"""1 Hz WAL/commit sampler; dynamic stats SQL supports all three baselines."""
import fcntl
import json
import os
from pathlib import Path
import time

import psycopg

ROOT = Path('/home/neel/claude_checks/ycsb_matrix_20261020')
lock = (ROOT / 'sampler.lock').open('w')
fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
(ROOT / 'sampler.pid').write_text(str(os.getpid()) + '\n')
AM = """SELECT a.amname FROM pg_class c JOIN pg_am a ON a.oid=c.relam
        WHERE c.oid=to_regclass('usertable_merkle_lookup_idx')"""
Q = """SELECT {stats}, (SELECT xact_commit FROM pg_stat_database
        WHERE datname=current_database()), pg_current_wal_lsn()::text,
        pg_postmaster_start_time()::text,
        (SELECT string_agg(coalesce(query,''),'|') FROM pg_stat_activity
         WHERE backend_type='merkle_lsm merger')"""
conn = None
with (ROOT / 'evidence/wal_sampler.jsonl').open('a') as out:
    while not (ROOT / 'sampler.stop').exists():
        start = time.monotonic()
        row = dict(t=time.time(), monotonic_ns=time.monotonic_ns())
        try:
            context = ROOT / 'sampler_context.json'
            row.update(json.loads(context.read_text()) if context.exists() else {})
            if conn is None or conn.closed:
                conn = psycopg.connect('host=127.0.0.1 port=5488 user=postgres dbname=postgres',
                                       autocommit=True, connect_timeout=2,
                                       application_name='matrix_wal_sampler')
                conn.execute("SET default_transaction_isolation='read committed'")
                am = (conn.execute(AM).fetchone() or [None])[0]
                query = Q.format(stats="merkle_lsm_stats('usertable_merkle_lookup_idx')::text"
                                 if am == 'merkle_lsm' else 'NULL::text')
            stats, commits, lsn, postmaster_start, merger = conn.execute(query).fetchone()
            row.update(lookup_am=am, stats=json.loads(stats) if stats else None,
                       commits=commits, lsn=lsn, postmaster_start=postmaster_start, merger=merger)
        except Exception as error:
            row['error'] = str(error)[:500]
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass
            conn = None
        out.write(json.dumps(row) + '\n')
        out.flush()
        time.sleep(max(0, 1 - (time.monotonic() - start)))
if conn is not None:
    conn.close()
