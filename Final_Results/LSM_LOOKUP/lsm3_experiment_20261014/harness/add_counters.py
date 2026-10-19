from pathlib import Path
p=Path('/work/ARIABC/AriaBC/.bench_tmp/lsm_lookup_20261014/repo/scripts/distributed/run_oom_100m_benchmark.py');s=p.read_text();at=s.index('\ndef get_pg_io_stats(')
s=s[:at]+'''
def detailed_stats(args, mode):
    result = json.loads(sql(args, """SELECT json_build_object(
      'lsn',pg_current_wal_insert_lsn()::text,
      'tables',(SELECT json_agg(t) FROM (SELECT schemaname,relname,heap_blks_read,heap_blks_hit,idx_blks_read,idx_blks_hit FROM pg_statio_all_tables WHERE relname LIKE '%usertable%')t),
      'indexes',(SELECT json_agg(t) FROM (SELECT schemaname,relname,indexrelname,idx_blks_read,idx_blks_hit FROM pg_statio_all_indexes WHERE relname LIKE '%usertable%')t),
      'updates',(SELECT json_agg(t) FROM (SELECT schemaname,relname,n_tup_ins,n_tup_upd,n_tup_del,n_tup_hot_upd FROM pg_stat_all_tables WHERE relname LIKE '%usertable%')t),
      'sizes',(SELECT json_agg(t) FROM (SELECT n.nspname,c.relname,pg_relation_size(c.oid) bytes FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.relname LIKE '%usertable%')t)
    )"""))
    if mode == 'merkle_lsm':
        result['merges_requested'] = int(sql(args, "SELECT lsm3_get_merge_count('usertable_merkle_lookup_idx')"))
    return result

''' +s[at:]
s=s.replace('        pg_before = get_pg_io_stats(args)\n        device_before', '        pg_before = get_pg_io_stats(args)\n        detailed_before = detailed_stats(args, mode)\n        device_before')
s=s.replace('        device_after = get_remote_nvme_stats(args)\n        device_window_ms', '        device_after = get_remote_nvme_stats(args)\n        device_window_ms')
s=s.replace('        stop_server(args)\n        collect_logs(args, case_dir)','''        stop_server(args)
        compaction_start = time.monotonic()
        detailed_at_end = detailed_stats(args, mode)
        if mode == 'merkle_lsm':
            sql(args, "SET statement_timeout='10min'; SELECT lsm3_wait_merge_completion('usertable_merkle_lookup_idx');", timeout=660)
        detailed_drained = detailed_stats(args, mode)
        compaction_drain_ms = (time.monotonic() - compaction_start) * 1000
        wal_bytes = int(sql(args, "SELECT pg_wal_lsn_diff('" + detailed_drained['lsn'] + "','" + detailed_before['lsn'] + "')::bigint"))
        if wal_bytes:
            wal = run_remote(args.remote_host, args.remote_user, db_shell(args) +
                f"{args.install_dir}/bin/pg_waldump --path={pgdata(args)}/pg_wal --start={detailed_before['lsn']} --end={detailed_drained['lsn']} --stats", timeout=600)
            (case_dir / 'wal_stats.txt').write_text(wal.stdout + wal.stderr)
        collect_logs(args, case_dir)''')
s=s.replace('        pg_after = get_pg_io_stats(args)\n        snapshots', '''        pg_after = get_pg_io_stats(args)
        detailed_after = detailed_stats(args, mode)
        (case_dir / 'detailed_counters.json').write_text(json.dumps(dict(before=detailed_before,
            at_gateway_end=detailed_at_end,after_compaction_drain=detailed_drained,
            after_shutdown_restart=detailed_after,wal_bytes=wal_bytes,
            compaction_drain_ms=compaction_drain_ms), indent=2) + "\\n")
        snapshots''')
s=s.replace('isolation=isolation_for(mode),','isolation=isolation_for(mode),fillfactor=args.usertable_fillfactor,wal_bytes=wal_bytes,compaction_drain_ms=compaction_drain_ms,')
s=s.replace('F15 baseline manifest','LSM baseline manifest').replace('F15 must reuse its approved, verified scratch baseline','LSM must reuse its verified baseline')
p.write_text(s)
