"""Exercise localization statistics through Psycopg's real parameter binding."""
import sys
import time
from pathlib import Path

import psycopg
from psycopg.rows import dict_row
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from merkle_recovery.db import (
    diff_merkle_node_index_stats,
    execute,
    merkle_node_index_stats,
    scalar,
    wait_for_stats,
)


@pytest.mark.integration
def test_index_statistics_bind_pattern_and_observe_real_scan(pytestconfig):
    dsn = pytestconfig.getoption('--dsn')
    with psycopg.connect(dsn, autocommit=True, row_factory=dict_row) as observer:
        execute(observer, 'CREATE SCHEMA IF NOT EXISTS ariabc_internal')
        execute(observer, 'CREATE TABLE ariabc_internal.merkle_node_stats_probe '
                          '(key integer PRIMARY KEY)')
        try:
            execute(observer, 'INSERT INTO ariabc_internal.merkle_node_stats_probe '
                              'SELECT generate_series(1,1000)')
            execute(observer, 'ANALYZE ariabc_internal.merkle_node_stats_probe')
            before = merkle_node_index_stats(observer)
            index = 'merkle_node_stats_probe_pkey'
            record = next(row for row in before if row['index_name'] == index)
            assert record['track_counts_enabled'] is True
            assert record['index_bytes'] > 0
            assert record['relation_bytes'] > 0

            # Closing the writer publishes its counters on PostgreSQL 13.
            # The independent observer still has to refresh its snapshot.
            with psycopg.connect(dsn, autocommit=True, row_factory=dict_row) as writer:
                execute(writer, 'SET enable_seqscan = off')
                rows = execute(writer, 'SELECT key FROM ariabc_internal.merkle_node_stats_probe '
                                       'WHERE key BETWEEN %s AND %s', (1, 100))
                assert len(rows) == 100
            deadline = time.monotonic() + 5
            while True:
                after = merkle_node_index_stats(observer)
                delta = next(row for row in diff_merkle_node_index_stats(before, after)
                             if row['index_name'] == index)
                if delta['idx_scan_delta'] > 0:
                    break
                assert time.monotonic() < deadline, delta
                time.sleep(0.1)
            assert delta['idx_tup_read_delta'] >= 100
            assert delta['idx_blks_read_delta'] + delta['idx_blks_hit_delta'] > 0
        finally:
            execute(observer, 'DROP TABLE ariabc_internal.merkle_node_stats_probe')


@pytest.mark.integration
def test_statistics_barrier_keeps_previous_scans_out_of_next_phase(pytestconfig):
    with psycopg.connect(pytestconfig.getoption('--dsn'), autocommit=True,
                         row_factory=dict_row) as conn:
        execute(conn, 'CREATE TEMP TABLE stats_boundary_probe AS SELECT generate_series(1, 1000) AS key')
        oid = scalar(conn, "SELECT 'pg_temp.stats_boundary_probe'::regclass::oid")

        def snapshot():
            wait_for_stats(conn)
            return int(scalar(conn, 'SELECT pg_stat_get_numscans(%s::oid)', (oid,)))

        before = snapshot()
        for _ in range(3):
            # Short scans used to be published during the following recovery.
            assert scalar(conn, 'SELECT count(*) FROM stats_boundary_probe') == 1000
            after_scan = snapshot()
            assert after_scan - before == 1
            execute(conn, 'SELECT 1')
            after_idle = snapshot()
            assert after_idle == after_scan
            before = after_idle


@pytest.mark.integration
def test_statistics_barrier_rejects_disabled_counters(pytestconfig):
    with psycopg.connect(pytestconfig.getoption('--dsn'), autocommit=True,
                         row_factory=dict_row) as conn:
        execute(conn, 'SET track_counts = off')
        with pytest.raises(RuntimeError, match='requires track_counts=on'):
            wait_for_stats(conn)
