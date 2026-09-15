#!/usr/bin/env python3
"""Detailed Phase-by-Phase Restoration Timing Measurement for 31 GB Database.

Measures each individual subphase of reset_remote_pgdata:
- Process stop / cleanup
- Pre-checks (space, permissions, versions)
- Deletion of old pgdata (rm -rf)
- Pristine copy (cp -a --reflink=never)
- Config injection (postgresql.auto.conf)
- Start PostgreSQL #1 (for schema/checks)
- Prepare ledger schema
- Merkle index drop (for standard PG/DET mode)
- Fast O(1) post-copy checks (min/max, reltuples, file count, pg_indexes)
- Stop PostgreSQL #1
- OS cache drop & disk sync (sync, drop_caches, meminfo delta)
- Start PostgreSQL #2 (cold start)
- Settings & sizing validation queries
- Ariabc server startup & port polling
- (Comparison) Old full COUNT(*) scan timing on the 31GB database
"""
import json
import os
import re
import shlex
import subprocess
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "scripts/distributed"))

import run_oom_100m_benchmark as oom

def get_meminfo(args):
    out = oom.run_remote(args.remote_host, args.remote_user, "cat /proc/meminfo").stdout
    mem = {}
    for line in out.splitlines():
        parts = line.split(":")
        if len(parts) == 2:
            key = parts[0].strip()
            val = parts[1].strip().split()[0]
            if val.isdigit():
                mem[key] = int(val)
    return mem

def get_nvme_stat(args):
    out = oom.run_remote(args.remote_host, args.remote_user, f"""
major_minor=$(findmnt -n -r -o MAJ:MIN -T {args.remote_dir} | tr -d '[:space:]')
readlink -f /sys/dev/block/"$major_minor"
cat /sys/dev/block/"$major_minor"/stat
""").stdout.splitlines()
    vals = [int(x) for x in out[1].split()]
    return dict(device=out[0], read_sectors=vals[2], write_sectors=vals[6],
                read_ms=vals[3], write_ms=vals[7], io_ms=vals[9])

def run_restoration_profile():
    args = oom.parse_args([])
    oom.load_sudo_password()

    print("=" * 80)
    print("STARTING DETAILED 31 GB DATABASE RESTORATION PROFILING")
    print(f"Target host: {args.remote_host}")
    print(f"Remote dir:  {args.remote_dir}")
    print(f"Database:    {args.db_rows:,} rows (31 GB)")
    print("=" * 80)

    timings = {}
    meminfo_snapshots = {}
    nvme_snapshots = {}

    total_start = time.perf_counter()

    # --- Phase 1: Process Stop & Cleanup ---
    t0 = time.perf_counter()
    oom.stop_server(args)
    oom.stop_postgres(args)
    timings["01_stop_processes"] = time.perf_counter() - t0
    print(f"[{timings['01_stop_processes']:6.3f} s] Phase 1: Stop server & postgres")

    # --- Phase 2: Pre-checks & Filesystem verification ---
    t0 = time.perf_counter()
    meta = oom.get_remote_baseline_identity(args)
    res = oom.run_remote(args.remote_host, args.remote_user, f"""
test -f {args.remote_dir}/pgdata_base/PG_VERSION
test ! -L {args.remote_dir}/pgdata
test ! -L {args.remote_dir}/pgdata_base
needed=$(du -sb {args.remote_dir}/pgdata_base | cut -f1)
available=$(df -B1 --output=avail {args.remote_dir} | tail -1)
test "$available" -gt "$((needed + 21474836480))"
echo "NEEDED=$needed AVAIL=$available"
""")
    timings["02_prechecks_and_space"] = time.perf_counter() - t0
    print(f"[{timings['02_prechecks_and_space']:6.3f} s] Phase 2: Pre-checks & space verification ({res.stdout.strip()})")

    # --- Phase 3: Remove existing disposable pgdata ---
    meminfo_snapshots["before_rm"] = get_meminfo(args)
    nvme_snapshots["before_rm"] = get_nvme_stat(args)
    t0 = time.perf_counter()
    oom.run_remote(args.remote_host, args.remote_user, f"rm -rf -- {args.remote_dir}/pgdata", timeout=args.reset_timeout)
    timings["03_rm_rf_old_pgdata"] = time.perf_counter() - t0
    nvme_snapshots["after_rm"] = get_nvme_stat(args)
    print(f"[{timings['03_rm_rf_old_pgdata']:6.3f} s] Phase 3: Remove old 31GB pgdata (rm -rf)")

    # --- Phase 4: Pristine Copy (cp -a --reflink=never) & Durable Sync ---
    meminfo_snapshots["before_cp"] = get_meminfo(args)
    nvme_snapshots["before_cp"] = get_nvme_stat(args)

    # 4A: Userspace copy process
    t0 = time.perf_counter()
    oom.run_remote(args.remote_host, args.remote_user,
                   f"cp -a --reflink=never {args.remote_dir}/pgdata_base {args.remote_dir}/pgdata",
                   timeout=args.reset_timeout)
    timings["04a_cp_userspace"] = time.perf_counter() - t0
    meminfo_snapshots["after_cp"] = get_meminfo(args)
    nvme_snapshots["after_cp"] = get_nvme_stat(args)

    # 4B: Immediate durable sync to flush dirty page cache before startup
    t0_sync = time.perf_counter()
    oom.run_remote(args.remote_host, args.remote_user, "sync", timeout=args.reset_timeout)
    timings["04b_copy_sync"] = time.perf_counter() - t0_sync
    meminfo_snapshots["after_copy_sync"] = get_meminfo(args)
    nvme_snapshots["after_copy_sync"] = get_nvme_stat(args)

    timings["04_durable_copy_total"] = timings["04a_cp_userspace"] + timings["04b_copy_sync"]

    pristine_bytes = meta["total_bytes"]
    cp_sectors_written = nvme_snapshots["after_cp"]["write_sectors"] - nvme_snapshots["before_cp"]["write_sectors"]
    cp_sectors_read = nvme_snapshots["after_cp"]["read_sectors"] - nvme_snapshots["before_cp"]["read_sectors"]
    cp_write_mib = (cp_sectors_written * 512) / (1024**2)
    cp_read_mib = (cp_sectors_read * 512) / (1024**2)
    dataset_mib = pristine_bytes / (1024**2)
    dataset_mb = pristine_bytes / 1e6

    userspace_mib_s = dataset_mib / timings["04a_cp_userspace"]
    userspace_mb_s = dataset_mb / timings["04a_cp_userspace"]
    durable_mib_s = dataset_mib / timings["04_durable_copy_total"]
    durable_mb_s = dataset_mb / timings["04_durable_copy_total"]
    part_write_mib_s = cp_write_mib / timings["04a_cp_userspace"]
    part_read_mib_s = cp_read_mib / timings["04a_cp_userspace"]

    print(f"[{timings['04_durable_copy_total']:6.3f} s] Phase 4: Durable pristine copy total (cp + sync)")
    print(f"         - cp userspace duration:   {timings['04a_cp_userspace']:6.3f} s (dataset: {userspace_mib_s:.2f} MiB/s, {userspace_mb_s:.2f} MB/s)")
    print(f"         - copy sync duration:      {timings['04b_copy_sync']:6.3f} s")
    print(f"         - durable copy rate:       {durable_mib_s:.2f} MiB/s ({durable_mb_s:.2f} MB/s)")
    print(f"         - NVMe block write rate:   {part_write_mib_s:.2f} MiB/s ({cp_write_mib:.1f} MiB written)")
    print(f"         - NVMe block read rate:    {part_read_mib_s:.2f} MiB/s ({cp_read_mib:.1f} MiB read)")
    print(f"         - Dirty pages after cp:    {meminfo_snapshots['after_cp'].get('Dirty', 0) / 1024:.1f} MiB")
    print(f"         - Dirty pages after sync:  {meminfo_snapshots['after_copy_sync'].get('Dirty', 0) / 1024:.1f} MiB")

    # --- Phase 4C: Pre-Startup Copy Integrity Verification (Before Postmaster Starts) ---
    t0 = time.perf_counter()
    oom.run_remote(args.remote_host, args.remote_user, f"""
test "$(find {args.remote_dir}/pgdata -type f | wc -l)" -eq "{meta['file_count']}"
test "$(du -sb {args.remote_dir}/pgdata | cut -f1)" -eq "{meta['total_bytes']}"
test "$(sha256sum {args.remote_dir}/pgdata/PG_VERSION | cut -d' ' -f1)" = "{meta['pg_version_sha256']}"
test "$(sha256sum {args.remote_dir}/pgdata/global/pg_control | cut -d' ' -f1)" = "{meta['pg_control_sha256']}"
""")
    timings["04c_prestart_copy_verification"] = time.perf_counter() - t0
    print(f"[{timings['04c_prestart_copy_verification']:6.3f} s] Phase 4C: Pre-startup copy verification (verified {meta['file_count']} files, {meta['total_bytes']/2**30:.2f} GiB)")

    # --- Phase 5: Configuration injection & log reset ---
    t0 = time.perf_counter()
    config = f"""port = {args.db_port}
listen_addresses = '*'
shared_buffers = '{args.shared_buffers}'
enable_merkle_index = off
bcdb_worker_count = 1
synchronous_commit = on
fsync = on
full_page_writes = on
autovacuum = off
work_mem = '4MB'
maintenance_work_mem = '2GB'
effective_cache_size = '4GB'
max_parallel_maintenance_workers = 4
max_parallel_workers_per_gather = 0
checkpoint_timeout = '30min'
max_wal_size = '20GB'
max_connections = 256
track_counts = on
track_io_timing = on
log_checkpoints = on
bcdb_ledger_trace = off
bcdb_serial_gate_mode = 1
bcdb_serial_gate_source = 0
bcdb_dt_conflict_tracking = on
bcdb_result_ring_slots = 2048
bcdb_dt_completion_only_skip_reads = off
bcdb_dt_hashtab_switch_threshold = 1500
bcdb_gate_telemetry = off
bcdb_gate_snapshot_each_block = off
merkle_apply_synchronous_direct = on
"""
    oom.run_remote(args.remote_host, args.remote_user, f"""
printf %s {shlex.quote(config)} > {args.remote_dir}/pgdata/postgresql.auto.conf
: > {args.remote_dir}/postgres.log
""")
    timings["05_config_and_log_init"] = time.perf_counter() - t0
    print(f"[{timings['05_config_and_log_init']:6.3f} s] Phase 5: Config injection & log reset")

    # --- Phase 6: First PostgreSQL Startup (pg_ctl start -w) ---
    t0 = time.perf_counter()
    oom.start_postgres(args)
    timings["06_start_postgres_initial"] = time.perf_counter() - t0
    print(f"[{timings['06_start_postgres_initial']:6.3f} s] Phase 6: First PostgreSQL startup (pg_ctl start -w)")

    # --- Phase 7: Prepare Ledger Schema ---
    t0 = time.perf_counter()
    oom.prepare_ledger_schema(args)
    timings["07_prepare_ledger_schema"] = time.perf_counter() - t0
    print(f"[{timings['07_prepare_ledger_schema']:6.3f} s] Phase 7: Prepare ledger schema (raft_apply_ledger_schema.sql)")

    # --- Phase 8: Merkle Index Adjustment (Drop for non-merkle mode) ---
    t0 = time.perf_counter()
    oom.sql(args, "DROP INDEX IF EXISTS usertable_merkle_idx; DROP INDEX IF EXISTS usertable_merkle_lookup_idx;")
    timings["08_drop_merkle_indexes"] = time.perf_counter() - t0
    print(f"[{timings['08_drop_merkle_indexes']:6.3f} s] Phase 8: Merkle index adjustment (DROP INDEX IF EXISTS)")

    # --- Phase 9A: Fast Post-Copy Verification (Exact Byte/Page and B-Tree Checks) ---
    t0 = time.perf_counter()
    bounds = oom.sql(args, "SELECT min(ycsb_key), max(ycsb_key) FROM usertable;")
    t_bounds = time.perf_counter() - t0

    t0_stats = time.perf_counter()
    table_stats = oom.sql(args, "SELECT pg_relation_size('usertable'), "
                                "pg_relation_size('usertable_pkey1'), "
                                "relpages FROM pg_class WHERE relname='usertable';")
    heap_bytes, index_bytes, relpages = [int(x) for x in table_stats.split('|')]
    t_stats = time.perf_counter() - t0_stats

    if bounds != f"1|{args.db_rows}":
        raise RuntimeError(f"Post-copy bounds mismatch: {bounds}")
    if heap_bytes != 25600008192:
        raise RuntimeError(f"Post-copy heap size mismatch: {heap_bytes}")
    if index_bytes != 2246197248:
        raise RuntimeError(f"Post-copy index size mismatch: {index_bytes}")
    if relpages != 3125001:
        raise RuntimeError(f"Post-copy relpages mismatch: {relpages}")

    t0_idx = time.perf_counter()
    indexes = oom.sql(args, "SELECT json_agg(row_to_json(i)) FROM "
                           "(SELECT indexname, indexdef FROM pg_indexes WHERE tablename='usertable' ORDER BY indexname) i;")
    t_idx = time.perf_counter() - t0_idx

    timings["09a_fast_validation_total"] = t_bounds + t_stats + t_idx
    timings["09a_sub_btree_bounds"] = t_bounds
    timings["09a_sub_exact_relation_sizes"] = t_stats
    timings["09a_sub_index_query"] = t_idx
    print(f"[{timings['09a_fast_validation_total']:6.3f} s] Phase 9A: Fast post-copy verification total")
    print(f"         - B-tree min/max bounds:      {t_bounds:6.3f} s (bounds={bounds})")
    print(f"         - Exact heap/index/relpages:  {t_stats:6.3f} s (heap={heap_bytes/2**30:.2f} GiB, idx={index_bytes/2**20:.1f} MiB, pages={relpages:,})")
    print(f"         - Index catalog query:        {t_idx:6.3f} s")

    # --- Phase 10: Stop PostgreSQL before Cache Drop ---
    t0 = time.perf_counter()
    oom.stop_postgres(args)
    timings["10_stop_postgres_pre_cache_drop"] = time.perf_counter() - t0
    print(f"[{timings['10_stop_postgres_pre_cache_drop']:6.3f} s] Phase 10: Stop PostgreSQL before cache drop")

    # --- Phase 11: OS Page Cache Drop & Disk Sync ---
    meminfo_snapshots["before_sync"] = get_meminfo(args)
    nvme_snapshots["before_sync"] = get_nvme_stat(args)

    # Sub-phase 11A: sync
    t0 = time.perf_counter()
    oom.run_remote(args.remote_host, args.remote_user, "sync", timeout=300)
    timings["11a_os_sync"] = time.perf_counter() - t0

    meminfo_snapshots["after_sync"] = get_meminfo(args)
    nvme_snapshots["after_sync"] = get_nvme_stat(args)
    sync_sectors = nvme_snapshots["after_sync"]["write_sectors"] - nvme_snapshots["before_sync"]["write_sectors"]
    sync_mib = sync_sectors / 2048

    # Sub-phase 11B: drop_caches
    t0 = time.perf_counter()
    cache_cmd = oom.sudo_command('echo 3 > /proc/sys/vm/drop_caches')
    oom.run_remote(args.remote_host, args.remote_user, cache_cmd, timeout=60)
    timings["11b_drop_caches"] = time.perf_counter() - t0

    meminfo_snapshots["after_drop_caches"] = get_meminfo(args)
    nvme_snapshots["after_drop_caches"] = get_nvme_stat(args)

    timings["11_cache_drop_total"] = timings["11a_os_sync"] + timings["11b_drop_caches"]
    print(f"[{timings['11_cache_drop_total']:6.3f} s] Phase 11: Cache drop & sync total")
    print(f"         - sync (flush dirty writeback): {timings['11a_os_sync']:6.3f} s ({sync_mib:.1f} MiB flushed to NVMe)")
    print(f"         - drop_caches (evict pages):    {timings['11b_drop_caches']:6.3f} s")
    cached_freed_mib = (meminfo_snapshots["after_sync"].get("Cached", 0) -
                        meminfo_snapshots["after_drop_caches"].get("Cached", 0)) / 1024
    print(f"         - Page cache evicted:           {cached_freed_mib:.1f} MiB")

    # --- Phase 12: Second PostgreSQL Startup (Cold Start) ---
    t0 = time.perf_counter()
    oom.start_postgres(args)
    timings["12_start_postgres_cold"] = time.perf_counter() - t0
    print(f"[{timings['12_start_postgres_cold']:6.3f} s] Phase 12: Second PostgreSQL startup (cold start)")

    # --- Phase 13: Settings verification & Sizing queries ---
    t0 = time.perf_counter()
    settings_raw = oom.sql(args, "SELECT json_object_agg(name, setting) FROM pg_settings;")
    settings = json.loads(settings_raw)
    t_settings = time.perf_counter() - t0

    t0_sizes = time.perf_counter()
    sizes_raw = oom.sql(args, "SELECT json_build_object('heap_bytes', pg_relation_size('usertable'), "
                             "'table_total_bytes', pg_total_relation_size('usertable'), "
                             "'database_bytes', pg_database_size(current_database()));")
    sizes = json.loads(sizes_raw)
    t_sizes = time.perf_counter() - t0_sizes

    timings["13_settings_and_sizes"] = t_settings + t_sizes
    timings["13_sub_settings_query"] = t_settings
    timings["13_sub_sizes_query"] = t_sizes
    print(f"[{timings['13_settings_and_sizes']:6.3f} s] Phase 13: Settings & relation sizing queries")
    print(f"         - pg_settings check: {t_settings:6.3f} s (shared_buffers={settings['shared_buffers']})")
    print(f"         - table/db sizes:    {t_sizes:6.3f} s (db={sizes['database_bytes'] / 2**30:.2f} GiB)")

    # --- Phase 14: Ariabc Server Startup & Ready Check ---
    t0 = time.perf_counter()
    oom.start_remote_ariabc_server(args, "pg", 1)
    timings["14_start_ariabc_server"] = time.perf_counter() - t0
    print(f"[{timings['14_start_ariabc_server']:6.3f} s] Phase 14: Ariabc server startup & port polling")

    # Stop server after profiling
    oom.stop_server(args)

    total_restore_time = time.perf_counter() - total_start
    timings["TOTAL_RESTORATION_TIME"] = total_restore_time

    print("=" * 80)
    print(f"TOTAL RESTORATION & SETUP TIME: {total_restore_time:6.3f} s ({total_restore_time / 60:.2f} min)")
    print("=" * 80)

    # Save detailed JSON artifact
    out_dir = REPO_ROOT / "scripts/bench_full_results/restoration_profile"
    out_dir.mkdir(parents=True, exist_ok=True)
    report_file = out_dir / f"profile_{int(time.time())}.json"
    result_data = {
        "timings_seconds": timings,
        "baseline_metadata": {
            "pristine_directory_bytes": meta["total_bytes"],
            "pristine_file_count": meta["file_count"],
            "pg_control_sha256": meta["pg_control_sha256"],
            "pg_version_sha256": meta["pg_version_sha256"],
            "checkpoint_lsn": meta["checkpoint_lsn"],
            "db_system_id": meta["db_system_id"],
        },
        "copy_throughputs": {
            "dataset_userspace_throughput_mb_s": userspace_mb_s,
            "dataset_userspace_throughput_mib_s": userspace_mib_s,
            "dataset_durable_throughput_mb_s": durable_mb_s,
            "dataset_durable_throughput_mib_s": durable_mib_s,
            "nvme_partition_write_rate_mib_s": part_write_mib_s,
            "nvme_partition_read_rate_mib_s": part_read_mib_s,
            "nvme_write_mib_during_cp": cp_write_mib,
            "nvme_read_mib_during_cp": cp_read_mib,
        },
        "nvme_snapshots": nvme_snapshots,
        "meminfo_snapshots": meminfo_snapshots,
        "post_setup_database_sizes": sizes,
    }
    report_file.write_text(json.dumps(result_data, indent=2))
    print(f"Detailed profile JSON written to: {report_file}")

    return result_data

if __name__ == "__main__":
    run_restoration_profile()
