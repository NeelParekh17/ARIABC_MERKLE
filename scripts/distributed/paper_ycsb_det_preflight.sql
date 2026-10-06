\set ON_ERROR_STOP on
-- Run before DET / DET+Merkle paper YCSB on each replica, after PG restart.
-- These postmaster settings must be configured in postgresql.conf first.
DO $paper_ycsb_preflight$
BEGIN
    IF current_setting('bcdb_dt_conflict_tracking', true) IS DISTINCT FROM 'on' THEN
        RAISE EXCEPTION 'paper YCSB DET requires bcdb_dt_conflict_tracking=on'
            USING HINT = 'Enable it in the isolated instance configuration, restart PostgreSQL, and rerun this preflight.';
    END IF;
    IF current_setting('bcdb_dt_completion_only_skip_reads', true) IS DISTINCT FROM 'off' THEN
        RAISE EXCEPTION 'paper YCSB requires bcdb_dt_completion_only_skip_reads=off'
            USING HINT = 'The procedure is a SELECT that performs updates; its body must execute.';
    END IF;
END;
$paper_ycsb_preflight$;

SELECT name, setting
FROM pg_settings
WHERE name IN ('bcdb_dt_conflict_tracking',
               'bcdb_dt_completion_only_skip_reads',
               'bcdb_serial_gate_source',
               'bcdb_worker_count')
ORDER BY name;
