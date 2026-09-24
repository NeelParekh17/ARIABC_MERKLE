"""Shared, fail-closed experimental contracts for benchmark campaigns."""
import collections
import hashlib
import json
from pathlib import Path
import random
import re
import statistics
import sys

VERSION = 5


def balanced_cases(configurations, modes, trials, seed=42):
    """Shuffle configuration blocks, rotate mode order, retain every trial."""
    rng = random.Random(seed)
    modes = list(modes)
    if not configurations or not modes or trials < 1 or len(set(modes)) != len(modes):
        raise ValueError("Nonempty unique modes, configurations and positive trials required")
    rng.shuffle(modes)
    offsets = {tuple(c): i % len(modes) for i, c in enumerate(configurations)}
    for trial in range(1, trials + 1):
        blocks = list(configurations)
        rng.shuffle(blocks)
        for config in blocks:
            shift = (offsets[tuple(config)] + trial - 1) % len(modes)
            for mode in modes[shift:] + modes[:shift]:
                yield (*config, mode, trial)


def source_hashes(repo):
    paths = [repo / 'scripts/generate_ycsb_workloads.py',
             repo / 'scripts/restore_usertable_small.sql',
             repo / 'scripts/distributed/sql/raft_apply_ledger_schema.sql']
    paths += sorted((repo / 'scripts/distributed').glob('*.py'))
    paths += [repo / 'scripts/distributed/run_4node_raft_cluster.sh']
    return {str(p.relative_to(repo)): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in paths if p.is_file()}


def materialize_workloads(repo, out, workloads):
    """Never overwrite historical suite files; use versioned campaign copies."""
    sys.path.insert(0, str(repo / 'scripts'))
    from generate_ycsb_workloads import generate_workload_statements
    result = []
    directory = out.resolve() / 'workloads_v5'
    directory.mkdir(exist_ok=True)
    for wl in workloads:
        source = repo / wl
        match = re.fullmatch(r'ycsb_workload_([abcdf])_skew_(\d+)_(\d+)_(\d+)k\.txt', source.name)
        if match:
            family, whole, fraction, count = match.groups()
            skew = float(whole + '.' + fraction)
            sql = '\n'.join(generate_workload_statements(family, skew, int(count)*1000,
                            seed=42 + int(skew*100))) + '\n'
        else:
            sql = source.read_text()
        target = directory / source.name
        if target.exists() and target.read_text() != sql:
            raise RuntimeError(f'Workload changed on resume: {target}')
        target.write_text(sql)
        result.append(str(target))
    return result


def validate_settings(settings, buffers, workers, mode):
    size = re.fullmatch(r'(\d+)(kB|MB|GB)', buffers)
    expected_bytes = int(size[1]) * {'kB':1024, 'MB':1024**2, 'GB':1024**3}[size[2]]
    if int(settings['shared_buffers']) * int(settings['block_size']) != expected_bytes:
        raise RuntimeError('Effective shared_buffers differs from campaign contract')
    expected = dict(fsync='on', full_page_writes='on', synchronous_commit='on',
                    autovacuum='off', track_io_timing='on',
                    enable_merkle_index='on' if mode == 'bcdb_merkle' else 'off',
                    bcdb_worker_count=str(workers if mode != 'pg' else 1))
    for key, value in expected.items():
        if settings.get(key) != value:
            raise RuntimeError(f'Effective {key}={settings.get(key)!r}, expected {value!r}')


def require_merkle(output, index_count=None):
    expected = 't' if index_count is None else f'{index_count}|t'
    if output.strip() != expected:
        raise RuntimeError(f'Merkle verification must be exactly {expected!r}: {output!r}')


def validate_ycsb_results(output, expected_queries, client='single-gateway-direct', mode=None, allow_empty_reads=None):
    """Require one affected/returned row per canonical operation.

    For baseline PostgreSQL ('pg' mode), concurrent connection interleaving can
    cause a SELECT on a recently inserted key to execute before the insert commits,
    returning SELECT 0. This non-deterministic interleaving is tracked in empty_results
    rather than rejected. Deterministic modes strictly require row count 1.
    """
    if allow_empty_reads is None:
        allow_empty_reads = (mode == 'pg')
    # Concurrent executor output may append the next record before writing a
    # newline (e.g. UPDATE 1single-gateway-direct-2 ...). Delimit complete
    # request headers first; never invent missing IDs or affected-row counts.
    header = re.escape(client) + r'-\d+[ \t]+\d+[ \t]+(?:SELECT|UPDATE|INSERT|DELETE)[ \t]+'
    output = re.sub(r'(?=' + header + ')', '\n', output)
    pattern = re.compile(re.escape(client) + r'-(\d+)[ \t]+\d+[ \t]+(SELECT|UPDATE|INSERT|DELETE)[ \t]+(\d+)(?:[ \t]+(.*))?$', re.M)
    seen = set()
    empty_results = 0
    for m in pattern.finditer(output):
        request, command, count, rest = m.groups()
        request = int(request)
        # INSERT tags contain an OID before the affected-row count. Corrupted
        # stdout is incomplete evidence, even when terminal counters succeed.
        affected = (rest.split()[0] if rest else '') if command == 'INSERT' else count
        if command == 'SELECT' and affected == '0' and allow_empty_reads:
            empty_results += 1
        elif affected != '1' or (command == 'INSERT' and count != '0'):
            raise RuntimeError(f'YCSB request {request} returned {command} with row count {affected!r}')
        if request in seen:
            raise RuntimeError(f'Duplicate YCSB result for request {request}')
        seen.add(request)
    if seen != set(range(1, expected_queries + 1)):
        raise RuntimeError(f'Missing YCSB row-result evidence: {len(seen)}/{expected_queries}')
    return dict(validated_row_results=len(seen), empty_results=empty_results)


def variability(values):
    values = [float(x) for x in values]
    if not values or min(values) <= 0:
        raise ValueError('Throughput observations must be positive')
    mean = statistics.mean(values)
    cv = statistics.stdev(values) / mean * 100 if len(values) > 1 else None
    return dict(trials=len(values), median=statistics.median(values), mean=mean,
                min=min(values), max=max(values), cv_pct=cv,
                status='insufficient_repeats' if len(values) < 5 else
                       'high_variability' if cv > 10 else 'repeatability_check_passed')


def write_variability_report(out, rows):
    groups = collections.defaultdict(list)
    for row in rows:
        key = tuple(str(row.get(k, '')) for k in ('mode','workload','skew','warehouses','workers','server_workers'))
        groups[key].append(float(row['tps']))
    result = [dict(configuration=key, **variability(v)) for key,v in sorted(groups.items())]
    (out / 'variability.json').write_text(json.dumps(result, indent=2) + '\n')
    lines = ['# Measurement qualification', '',
             'Successful SQL/verification does not establish stable throughput or serializability.',
             'A throughput ordering is not a correctness invariant. No observations are discarded.',
             'Five repeats is the minimum qualification check; CV <= 10% is a diagnostic, not a confidence interval.', '',
             '| Configuration | Trials | Median TPS | Min | Max | Sample CV % | Status |',
             '|---|---:|---:|---:|---:|---:|---|']
    for r in result:
        cv = 'unknown' if r['cv_pct'] is None else f"{r['cv_pct']:.2f}"
        lines.append(f"| {' / '.join(k for k in r['configuration'] if k)} | {r['trials']} | {r['median']:.2f} | {r['min']:.2f} | {r['max']:.2f} | {cv} | {r['status']} |")
    (out / 'MEASUREMENT_QUALIFICATION.md').write_text('\n'.join(lines) + '\n')


class RemoteTelemetry:
    """Stream an inline sampler over SSH; closing the stream stops the sampler."""
    def __init__(self, host, user, path, psql=None, port=5432, lib=None):
        self.host, self.user, self.path = host, user, Path(path)
        self.psql, self.port, self.lib = psql, port, lib

    def __enter__(self):
        import shlex
        import subprocess
        import time
        source = Path(__file__).with_name('benchmark_telemetry.py').read_text()
        command = 'python3 -u -c ' + shlex.quote(source)
        if self.psql:
            command += ' --psql ' + shlex.quote(self.psql) + ' --port ' + str(self.port)
        if self.lib:
            command = 'export LD_LIBRARY_PATH=' + shlex.quote(self.lib) + '; ' + command
        self.stream = self.path.open('w')
        self.proc = subprocess.Popen(['ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=10',
                                     f'{self.user}@{self.host}', command], stdout=self.stream, stderr=subprocess.STDOUT)
        for _ in range(100):
            if self.path.stat().st_size and self.path.read_text().endswith('\n'):
                break
            if self.proc.poll() is not None:
                self.stream.close()
                raise RuntimeError(f'Telemetry startup failed: {self.path.read_text()}')
            time.sleep(.1)
        else:
            self.__exit__(None, None, None)
            raise RuntimeError('Telemetry produced no startup sample')
        first = json.loads(self.path.read_text().splitlines()[0])
        if 'error' in first or first.get('pg_waits', {}).get('returncode', 0):
            self.__exit__(None, None, None)
            raise RuntimeError(f'Telemetry sampling failed: {first}')
        return self

    def __exit__(self, *_):
        import subprocess
        self.proc.terminate()
        try:
            self.proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait()
        self.stream.close()


def prepare_remote_cold(args, pgdata, pg_ctl, psql, lib, run):
    """Use the same stopped-PG eviction sequence for both standalone suites."""
    import shlex
    source = Path(__file__).with_name('benchmark_cache.py').read_text()
    command = (f'set -e; export LD_LIBRARY_PATH={shlex.quote(lib)}; '
               f'{shlex.quote(psql)} -X -v ON_ERROR_STOP=1 -p {args.db_port} -U postgres -d postgres -c CHECKPOINT; '
               f'{shlex.quote(pg_ctl)} -D {shlex.quote(pgdata)} -w -t 120 -m fast stop; '
               f'python3 -c {shlex.quote(source)} {shlex.quote(pgdata)}; '
               f'{shlex.quote(pg_ctl)} -D {shlex.quote(pgdata)} -l /tmp/postgres_single.log -w -t 120 start')
    _, output = run(['ssh', f'{args.db_user}@{args.db_host}', command], check=True, timeout=300)
    records = [json.loads(line) for line in output.splitlines() if line.startswith('{')]
    if len(records) != 1:
        raise RuntimeError('Missing cache residency evidence')
    return records[0]


def validate_resume_evidence(out):
    """A CSV row may be skipped only if its saved terminal evidence still passes."""
    import csv
    from benchmark_validation import parse_gateway_result
    path = out / 'summary.csv'
    if not path.exists() or not path.stat().st_size:
        return
    for row in csv.DictReader(path.open()):
        run_id = row.get('run_id')
        if not run_id or Path(run_id).name != run_id:
            raise RuntimeError('Resume requires a per-row attempt id')
        metadata_path = out / 'attempts' / (run_id + '.json')
        metadata = json.loads(metadata_path.read_text())
        if metadata.get('status') != 'passed':
            raise RuntimeError(f'Unverified attempt on resume: {run_id}')
        count = int(row['total_queries'])
        if row['mode'] == 'cluster':
            from cluster_sweep_support import accept_cluster_artifact
            accept_cluster_artifact(Path(metadata['artifact_dir']), count)
        else:
            prefix = out / 'attempts' / run_id
            parse_gateway_result(prefix.with_suffix('.gateway.log').read_text(), count,
                                 metadata['gateway_returncode'], mode=row['mode'])
            if row['mode'] == 'bcdb_merkle':
                require_merkle(prefix.with_suffix('.merkle.txt').read_text(),
                               9 if row.get('benchmark') == 'tpcc' else None)
            if re.fullmatch(r'ycsb_workload_[abcdf]_skew_.*', row['workload']):
                validate_ycsb_results(prefix.with_suffix('.server.log').read_text(), count, mode=row['mode'])


def verify_remote_inputs(args, repo, paths, run):
    """Reject stale restore logic instead of silently benchmarking different SQL."""
    import shlex
    remote_root = f'/home/{args.db_user}/Desktop/ariabc_cluster'
    for path in paths:
        expected = hashlib.sha256((repo / path).read_bytes()).hexdigest()
        _, output = run(['ssh', f'{args.db_user}@{args.db_host}',
                         'sha256sum ' + shlex.quote(remote_root + '/' + path)], check=True)
        if not output.split() or output.split()[0] != expected:
            raise RuntimeError(f'Remote input differs from campaign source: {path}; sync before running')
