#!/usr/bin/env python3
"""Validate logical undo through real PG/DET/Merkle gateways on one disposable clone.

This is a correctness experiment, not a cold-cache throughput sweep. It does
not drop global caches. Full-table verification is timed separately from undo.
"""
import argparse
import hashlib
import json
from pathlib import Path
import shlex
import time

import run_oom_100m_benchmark as oom


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--remote-host', default=oom.DEFAULT_REMOTE_HOST)
    parser.add_argument('--remote-user', default=oom.DEFAULT_REMOTE_USER)
    parser.add_argument('--source-dir', default=oom.DEFAULT_REMOTE_DIR)
    parser.add_argument('--remote-dir', required=True, help='New disposable directory, never the original campaign')
    parser.add_argument('--prepared-copy', action='store_true', help='Use an already copied, unstarted clone with identical control file')
    parser.add_argument('--output-dir', type=Path, required=True)
    parser.add_argument('--workloads', type=Path, nargs='+', required=True)
    parser.add_argument('--db-port', type=int, default=5548)
    parser.add_argument('--server-port', type=int, default=18080)
    parser.add_argument('--workers', type=int, default=8)
    parser.add_argument('--db-rows', type=int, default=100000000)
    parser.add_argument('--install-dir', default=oom.DEFAULT_INSTALL_DIR)
    parser.add_argument('--cluster-dir', default=oom.DEFAULT_CLUSTER_DIR)
    parser.add_argument('--gateway-host', default=oom.DEFAULT_GATEWAY_HOST)
    parser.add_argument('--gateway-user', default=oom.DEFAULT_GATEWAY_USER)
    parser.add_argument('--gateway-repo', default=oom.DEFAULT_GATEWAY_REPO)
    args = parser.parse_args()
    # Reuse the existing runner's validation of remotely interpolated arguments.
    checked = oom.parse_args(['--remote-host', args.remote_host, '--remote-user', args.remote_user,
                              '--remote-dir', args.remote_dir, '--install-dir', args.install_dir,
                              '--cluster-dir', args.cluster_dir, '--gateway-host', args.gateway_host,
                              '--gateway-user', args.gateway_user, '--gateway-repo', args.gateway_repo,
                              '--db-port', str(args.db_port), '--server-port', str(args.server_port),
                              '--workers', str(args.workers), '--db-rows', str(args.db_rows)])
    args.gateway_timeout, args.verify_timeout = checked.gateway_timeout, 3600
    args.remote_dir = checked.remote_dir
    args.output_dir.mkdir(parents=True, exist_ok=False)
    out = args.output_dir

    def remote(command, timeout=600, check=True):
        return oom.run_remote(args.remote_host, args.remote_user, oom.db_shell(args) + command, timeout, check)

    def save(name, value):
        (out / name).write_text(json.dumps(value, indent=2) + '\n')

    def upload(path, destination):
        remote(f'printf %s {shlex.quote(path.read_text())} > {shlex.quote(destination)}')

    def helper(action, extra='', timeout=3600):
        command = (f'python3 {args.remote_dir}/ycsb_undo.py {action} '
                   f'--psql {args.install_dir}/bin/psql --port {args.db_port} ' + extra)
        return json.loads(remote(command, timeout).stdout)

    def config(mode):
        return f"""port = {args.db_port}
listen_addresses = '*'
shared_buffers = '32MB'
enable_merkle_index = {'on' if mode == 'bcdb_merkle' else 'off'}
bcdb_worker_count = {args.workers if mode != 'pg' else 1}
synchronous_commit = on
fsync = on
full_page_writes = on
autovacuum = off
work_mem = '4MB'
maintenance_work_mem = '2GB'
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

    # Never touch a pre-existing working database or original golden database.
    source = shlex.quote(args.source_dir + '/pgdata_base')
    target = shlex.quote(args.remote_dir + '/pgdata')
    remote(f'test "$(realpath -m {source})" != "$(realpath -m {target})"\n'
           f'test ! -L {shlex.quote(args.remote_dir)}\n'
           f'test ! -L {target}\n'
           f'test ! -e {target}/postmaster.pid\n'
           f'! fuser {args.db_port}/tcp >/dev/null 2>&1\n'
           f'! fuser {args.server_port}/tcp >/dev/null 2>&1')
    started = time.monotonic()
    if args.prepared_copy:
        remote(f'cmp {source}/global/pg_control {target}/global/pg_control\n'
               f'test ! -e {args.remote_dir}/undo_validation.json\n')
    else:
        remote(f'mkdir {args.remote_dir}\ncp -a --reflink=never {source} {target}', timeout=3600)
    save('arguments.json', {k: str(v) if isinstance(v, Path) else [str(p) for p in v]
                            if k == 'workloads' else v for k, v in vars(args).items()})
    remote(f'printf %s {shlex.quote(json.dumps(dict(output=str(out))))} > {args.remote_dir}/undo_validation.json')
    upload(Path(__file__).with_name('ycsb_undo.py'), args.remote_dir + '/ycsb_undo.py')
    (out / 'provenance.txt').write_text(remote(
        f'sha256sum {args.install_dir}/bin/postgres {args.cluster_dir}/ariabc_pg/build/bin/ariabc_pg_server\n'
        f'sha256sum {source}/global/pg_control {args.remote_dir}/ycsb_undo.py').stdout +
        oom.run_remote(args.gateway_host, args.gateway_user,
                       f'sha256sum {args.gateway_repo}/ariabc_pg/build/bin/ariabc_pg_gateway').stdout)
    results = []
    try:
        remote(f'cat > {target}/postgresql.auto.conf <<\'CONF\'\n{config("bcdb_merkle")}CONF\n')
        oom.start_postgres(args)
        oom.prepare_ledger_schema(args)
        index_state = oom.sql(args, "SELECT indexname,indexdef FROM pg_indexes WHERE tablename='usertable';")
        (out / 'indexes.txt').write_text(index_state+'\n')
        if 'USING merkle' not in index_state:
            raise RuntimeError('Golden clone has no Merkle index; cannot validate Merkle mode')
        print('Computing baseline SHA256 of all rows and columns...', flush=True)
        baseline = helper('hash')
        if baseline['rows'] != args.db_rows:
            raise RuntimeError(f'Unexpected baseline row count: {baseline}')
        save('baseline_hash.json', baseline)
        root = oom.sql(args, "SELECT merkle_root_hash('usertable');")
        (out / 'baseline_merkle_root.txt').write_text(root+'\n')
        if oom.sql(args, "SELECT merkle_verify('usertable');", timeout=3600) != 't':
            raise RuntimeError('Baseline Merkle verification failed')
        print(f'Baseline verified: {baseline["rows"]} rows, SHA256={baseline["sha256"]}', flush=True)
        for mode in ('bcdb_merkle', 'pg', 'bcdb_det'):
            oom.stop_postgres(args)
            remote(f'cat > {target}/postgresql.auto.conf <<\'CONF\'\n{config(mode)}CONF\n')
            oom.start_postgres(args)
            if mode == 'pg':
                oom.sql(args, 'DROP INDEX usertable_merkle_idx; DROP INDEX usertable_merkle_lookup_idx;')
            for number, workload in enumerate(args.workloads):
                name = f'{mode}_{number}_{workload.stem}'
                case = out / name
                case.mkdir()
                remote_wl = f'{args.remote_dir}/{name}.sql'
                undo_dir = f'{args.remote_dir}/{name}_undo'
                upload(workload, remote_wl)
                print(f'{name}: capturing before-images', flush=True)
                capture = helper('prepare', f'--workload {remote_wl} --output-dir {undo_dir}')
                (case / 'capture.json').write_text(json.dumps(capture, indent=2)+'\n')
                # Persist the actual generated restore file and manifest for review/reuse.
                for file in ('restore.sql', 'manifest.json', 'before.tsv'):
                    (case / file).write_text(remote(f'cat {undo_dir}/{file}').stdout)
                digest = hashlib.sha256(workload.read_bytes()).hexdigest()
                oom.run_remote(args.gateway_host, args.gateway_user,
                               f'printf %s {shlex.quote(workload.read_text())} > /tmp/oom_{digest}.sql')
                oom.start_remote_ariabc_server(args, mode, args.workers)
                print(f'{name}: executing {sum(capture["operations"].values())} statements', flush=True)
                gateway = oom.run_local_gateway_benchmark(args, workload, mode, args.workers, case / 'gateway.log')
                oom.stop_server(args)
                settings = json.loads(oom.sql(args, 'SELECT json_object_agg(name,setting) FROM pg_settings;'))
                (case / 'settings.json').write_text(json.dumps(settings, indent=2)+'\n')
                # A successful direct-completion report must also have changed the data.
                changed = remote(f'python3 {args.remote_dir}/ycsb_undo.py verify '
                                 f'--psql {args.install_dir}/bin/psql --port {args.db_port} '
                                 f'--output-dir {undo_dir}', check=False)
                writes = sum(capture['operations'].get(op, 0) for op in ('update', 'insert', 'delete'))
                if writes:
                    if changed.returncode == 0 or 'Touched-row SHA256 mismatch' not in changed.stderr:
                        raise RuntimeError(f'Workload mutation was not established: {changed.stdout} {changed.stderr}')
                elif changed.returncode:
                    raise RuntimeError(f'Read-only workload changed baseline: {changed.stderr}')
                begin = time.monotonic()
                restore = remote(f'{args.install_dir}/bin/psql -X -v ON_ERROR_STOP=1 '
                                 f'-h 127.0.0.1 -p {args.db_port} -U postgres -d postgres -f {undo_dir}/restore.sql')
                restore_ms = (time.monotonic()-begin)*1000
                (case / 'restore.log').write_text(restore.stdout+restore.stderr)
                fast = helper('verify', f'--output-dir {undo_dir}')
                print(f'{name}: undo {restore_ms:.1f} ms; checking full 100M-row SHA256', flush=True)
                full = helper('hash')
                if (full['sha256'], full['rows']) != (baseline['sha256'], baseline['rows']):
                    raise RuntimeError(f'Full database hash mismatch after {name}: {full}')
                merkle = None
                if mode == 'bcdb_merkle':
                    restored_root = oom.sql(args, "SELECT merkle_root_hash('usertable');")
                    merkle = oom.sql(args, "SELECT merkle_verify('usertable');", timeout=3600)
                    if restored_root != root or merkle != 't':
                        raise RuntimeError(f'Merkle undo mismatch: root={restored_root}, verify={merkle}')
                    (case / 'restored_merkle_root.txt').write_text(restored_root+'\n')
                row = dict(case=name, mode=mode, restore_ms=restore_ms, capture=capture,
                           fast_verification=fast, full_verification=full, merkle_verify=merkle, gateway=gateway)
                (case / 'result.json').write_text(json.dumps(row, indent=2)+'\n')
                results.append(row)
                save('results.json', results)
                oom.collect_logs(args, case)
                # Restart clears volatile DET queues before reusing gateway request IDs.
                oom.stop_postgres(args)
                oom.start_postgres(args)
                print(f'{name}: PASS (full hash and row count match original)', flush=True)
        save('COMPLETE.json', dict(cases=len(results), elapsed_ms=(time.monotonic()-started)*1000))
    except BaseException as exc:
        (out / 'FAILED.txt').write_text(str(exc)+'\n')
        raise
    finally:
        oom.stop_server(args)
        oom.stop_postgres(args)
        oom.collect_logs(args, out)


if __name__ == '__main__':
    main()
