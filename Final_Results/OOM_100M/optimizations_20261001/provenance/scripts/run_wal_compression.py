#!/usr/bin/env python3
"""Orchestrator entry point: canonical OOM runner plus explicit WAL-FPI treatment.

No baseline writes or PostgreSQL code changes. Apply the one setting only to
the runner's disposable copy, before either reset-phase startup. Keep the
runner's physical reset, durability, SERIALIZABLE, retries and verification.
"""
import argparse
import hashlib
import shlex
import sys
from pathlib import Path, PurePosixPath

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import run_oom_100m_benchmark as runner


def main(argv=None):
    parser = argparse.ArgumentParser(add_help=False, allow_abbrev=False)
    parser.add_argument("--wal-compression", choices=("on", "off"), required=True)
    experiment, remaining = parser.parse_known_args(argv)
    original_parse = runner.parse_args
    original_start = runner.start_postgres
    original_reset = runner.reset_remote_pgdata
    original_hashes = runner.source_hashes
    resetting = False

    def parse_args(argv=None):
        args = original_parse(argv)
        if "pg_rc" in args.modes:
            raise ValueError("This experiment requires SERIALIZABLE in every mode")
        if not args.skip_gen or args.gen_only:
            raise ValueError("Use existing stopped baselines with --skip-gen")
        args.oom_opt_wal_compression = experiment.wal_compression
        return args

    def source_hashes(repo):
        hashes = original_hashes(repo)
        path = Path(__file__).resolve()
        hashes[str(path.relative_to(repo))] = hashlib.sha256(path.read_bytes()).hexdigest()
        # The stock resume source contract compares this dictionary before
        # restoring saved arguments. Switching treatment cannot resume old cases.
        hashes["oom_opt:wal_compression"] = hashlib.sha256(experiment.wal_compression.encode()).hexdigest()
        return hashes

    def start_postgres(args):
        if resetting:
            work = PurePosixPath(runner.pgdata(args))
            root = PurePosixPath(args.remote_dir)
            if work.parent != root or work.name not in ("pgdata", "pgdata_plain", "pgdata_merkle"):
                raise RuntimeError(f"Refusing config treatment outside disposable working copy: {work}")
            path = shlex.quote(str(work))
            conf = shlex.quote(str(work / "postgresql.auto.conf"))
            # A stopped physical copy is already restored/validated by the runner.
            # Remove an earlier treatment line on the second reset startup.
            runner.run_remote(args.remote_host, args.remote_user, f"""
test -f {path}/PG_VERSION
test ! -L {path}
test ! -f {path}/postmaster.pid
test ! -L {conf}
sed -i '/^[[:space:]]*wal_compression[[:space:]]*=/d' {conf}
printf '%s\\n' 'wal_compression = {experiment.wal_compression}' >> {conf}
""", timeout=30)
        return original_start(args)

    def reset_remote_pgdata(args, mode, workers):
        nonlocal resetting
        resetting = True
        try:
            setup = original_reset(args, mode, workers)
        finally:
            resetting = False
        if setup["settings"]["wal_compression"] != experiment.wal_compression:
            raise RuntimeError("Effective wal_compression does not match treatment")
        for key in ("transaction_isolation", "default_transaction_isolation"):
            if setup["settings"][key] != "serializable":
                raise RuntimeError(f"Unexpected {key}: {setup['settings'][key]}")
        setup["oom_opt"] = dict(wal_compression=experiment.wal_compression,
                                 config_target=runner.pgdata(args),
                                 wrapper_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest())
        return setup

    runner.parse_args = parse_args
    runner.source_hashes = source_hashes
    runner.start_postgres = start_postgres
    runner.reset_remote_pgdata = reset_remote_pgdata
    runner.main(remaining)


if __name__ == "__main__":
    main()
