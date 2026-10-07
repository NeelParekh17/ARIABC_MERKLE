#!/usr/bin/env python3
"""Controller-side verification of NEW v2 data, before any timed case."""
import json
import sys
from pathlib import Path

repo = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(repo / 'scripts/distributed'))
import run_oom_100m_benchmark as runner


def main():
    evidence = Path(sys.argv[1])
    args = runner.parse_args(sys.argv[2:])
    assert args.remote_dir == '/home/neel/ariabc_data/oom_v2_20261001'
    assert args.install_dir == '/home/neel/claude_opt/install_v2'
    assert args.usertable_fillfactor == 90
    assert args.db_rows == 100000000
    assert set(args.modes) == {'pg', 'bcdb_det', 'bcdb_merkle'}
    assert all(runner.isolation_for(mode) == 'serializable' for mode in args.modes)
    evidence.mkdir(parents=True, exist_ok=True)
    golden = runner.validate_golden_baseline(args)
    (evidence / 'golden_manifest.json').write_text(json.dumps(golden, indent=2) + '\n')
    check_dir = args.remote_dir + '/pgdata_v2_verification'
    base_dir = args.remote_dir + '/' + args.base_dir_name
    conf = """port = 5458
listen_addresses = '127.0.0.1'
shared_buffers = '32MB'
maintenance_work_mem = '1GB'
max_parallel_maintenance_workers = 2
max_parallel_workers_per_gather = 2
enable_merkle_index = on
merkle_apply_synchronous_direct = on
bcdb_worker_count = 1
bcdb_ledger_trace = off
autovacuum = off
fsync = on
full_page_writes = on
synchronous_commit = on
default_transaction_isolation = 'serializable'
"""
    import shlex
    args._pgdata = check_dir
    try:
        runner.run_remote(args.remote_host, args.remote_user, runner.db_shell(args) + f"""
test ! -e {check_dir}
cp -a --reflink=never {base_dir} {check_dir}
printf %s {shlex.quote(conf)} > {check_dir}/postgresql.auto.conf
{args.install_dir}/bin/pg_ctl -D {check_dir} -l {args.remote_dir}/v2_verification.log -w -t 120 start
""", timeout=args.reset_timeout)
        keyspace = runner.sql(args, 'SELECT count(*), min(ycsb_key), max(ycsb_key) FROM usertable;', timeout=args.verify_timeout)
        assert keyspace == '100000000|1|100000000', keyspace
        options = runner.sql(args, "SELECT reloptions::text FROM pg_class WHERE oid='usertable'::regclass;")
        assert options == '{fillfactor=90}', options
        stats = json.loads(runner.sql(args, "SELECT merkle_tree_stats('usertable');", timeout=args.verify_timeout))
        for name, value in dict(partitions=200, fanout=32, split_threshold=1024, merge_threshold=256).items():
            assert stats[name] == value, stats
        verified = runner.sql(args, "SELECT merkle_verify('usertable');", timeout=args.verify_timeout)
        assert verified == 't', verified
        runner.sql(args, 'ANALYZE ariabc_internal.merkle_node_usertable;')
        sizes = json.loads(runner.sql(args, "SELECT json_build_object('heap_bytes', pg_relation_size('ariabc_internal.merkle_node_usertable'), 'total_bytes', pg_total_relation_size('ariabc_internal.merkle_node_usertable'), 'nodes', (SELECT count(*) FROM ariabc_internal.merkle_node_usertable));"))
        assert sizes['nodes'] == stats['total_nodes'] > 0, sizes
        assert sizes['heap_bytes'] / sizes['nodes'] < 512, sizes
        indexes = json.loads(runner.sql(args, "SELECT json_agg(row_to_json(i)) FROM (SELECT indexname,indexdef FROM pg_indexes WHERE tablename='usertable' ORDER BY indexname) i;"))
        assert {i['indexname'] for i in indexes} == {'usertable_pkey1', 'usertable_merkle_idx', 'usertable_merkle_lookup_idx'}, indexes
        record = dict(keyspace=keyspace, reloptions=options, merkle_stats=stats, merkle_verify=verified, node_storage=sizes, indexes=indexes)
        (evidence / 'verification.json').write_text(json.dumps(record, indent=2) + '\n')
        print(json.dumps(record, indent=2), flush=True)
    finally:
        # This path is created solely by this verifier; preserve it after a failure.
        runner.run_remote(args.remote_host, args.remote_user, runner.db_shell(args) + f"if [ -f {check_dir}/postmaster.pid ]; then {args.install_dir}/bin/pg_ctl -D {check_dir} -w -t 120 stop -m fast; fi", timeout=150)
        del args._pgdata
    runner.run_remote(args.remote_host, args.remote_user, f'rm -rf -- {check_dir}', timeout=args.reset_timeout)
    plain = runner.ensure_plain_baseline(args, golden)
    assert plain['heap_bytes'] == golden['heap_bytes']
    assert {i['indexname'] for i in plain['indexes']} == {'usertable_pkey1'}
    (evidence / 'plain_manifest.json').write_text(json.dumps(plain, indent=2) + '\n')
    print('BASELINE_VERIFICATION_PASS', flush=True)


if __name__ == '__main__':
    main()
