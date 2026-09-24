#!/usr/bin/env python3
"""Launch a disposable, real AriaBC PostgreSQL cluster and its live inspector."""
from __future__ import annotations

import argparse
import os
from pathlib import Path
import shlex
import signal
import subprocess
import tempfile
import time

import psycopg
from psycopg.conninfo import make_conninfo

if __package__:
    from .app import VisualizerServer
    from .db import Inspector, Settings
else:
    from app import VisualizerServer
    from db import Inspector, Settings

REPO = Path(__file__).resolve().parents[1]


class DemoCluster:
    """Own only a freshly created data directory; never reset an existing cluster."""
    def __init__(self, pg_bin=None, parent=None, rows=512):
        self.pg_bin = Path(pg_bin or REPO.parent / "install/bin")
        parent = Path(parent or REPO / ".bench_tmp")
        parent.mkdir(parents=True, exist_ok=True)
        self.path = Path(tempfile.mkdtemp(prefix="merkle-viz-", dir=parent))
        self.data = self.path / "data"
        self.socket = self.path / "socket"
        self.socket.mkdir(mode=0o700)
        self.rows = rows
        self.running = False
        self.dsn = make_conninfo(host=str(self.socket), port=55439, dbname="postgres", user="merkle_viz")

    def command(self, name, *args):
        with (self.path / "setup.log").open("a") as log:
            subprocess.run([str(self.pg_bin / name), *map(str, args)], check=True,
                           stdout=log, stderr=subprocess.STDOUT)

    def start(self):
        try:
            self.command("initdb", "-D", self.data, "-U", "merkle_viz", "--auth-local=trust",
                         "--auth-host=reject", "--no-locale", "--encoding=UTF8")
            options = shlex.join(["-k", str(self.socket), "-h", "", "-p", "55439",
                                  "-c", "shared_buffers=32MB", "-c", "max_connections=16",
                                  "-c", "max_parallel_workers_per_gather=0"])
            self.command("pg_ctl", "-D", self.data, "-l", self.path / "postgres.log", "-o", options, "-w", "start")
            self.running = True
            self.command("psql", "-X", "-v", "ON_ERROR_STOP=1", "-d", self.dsn,
                         "-f", REPO / "scripts/distributed/sql/raft_apply_ledger_schema.sql")
            with psycopg.connect(self.dsn, autocommit=True) as conn:
                conn.execute("CREATE TABLE public.merkle_demo (id bigint PRIMARY KEY, payload text NOT NULL, revision integer NOT NULL DEFAULT 0, note text)")
                conn.execute("INSERT INTO public.merkle_demo (id,payload) SELECT n, 'row-' || n FROM generate_series(1,%s) g(n)", (self.rows,))
                conn.execute("CREATE INDEX merkle_demo_idx ON public.merkle_demo USING merkle (id) WITH (partitions=4,fanout=4,split_threshold=16,merge_threshold=4)")
                self.index_oid = conn.execute("SELECT 'public.merkle_demo_idx'::regclass::oid").fetchone()[0]
                if not conn.execute("SELECT merkle_verify('public.merkle_demo')").fetchone()[0]:
                    raise RuntimeError("Native verification failed after demo initialization")
            return self
        except BaseException:
            self.stop()
            raise

    def stop(self):
        if self.running:
            self.command("pg_ctl", "-D", self.data, "-m", "fast", "-w", "stop")
            self.running = False

    def __enter__(self):
        return self.start()

    def __exit__(self, *_):
        self.stop()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pg-bin", default=os.getenv("MERKLE_VIZ_PG_BIN"))
    parser.add_argument("--port", type=int, default=8787)
    parser.add_argument("--rows", type=int, default=512)
    parser.add_argument("--db-only", action="store_true", help="Keep the isolated database running without the web UI")
    args = parser.parse_args()
    if not 1 <= args.rows <= 100000:
        parser.error("--rows must be between 1 and 100000")
    signal.signal(signal.SIGTERM, lambda *_: (_ for _ in ()).throw(KeyboardInterrupt()))
    cluster = DemoCluster(args.pg_bin, rows=args.rows)
    try:
        with cluster:
            print(f"Real PostgreSQL demo: {cluster.path}", flush=True)
            print(f"MERKLE_VIZ_CONNINFO={shlex.quote(cluster.dsn)}", flush=True)
            if args.db_only:
                while True:
                    time.sleep(1)
            else:
                settings = Settings(cluster.dsn, table="public.merkle_demo", index="public.merkle_demo_idx", allow_writes=True)
                with VisualizerServer(("127.0.0.1", args.port), Inspector(settings)) as server:
                    print(f"Open http://127.0.0.1:{server.server_address[1]} — writes affect only this demo cluster", flush=True)
                    server.serve_forever()
    except KeyboardInterrupt:
        pass
    except Exception:
        print(f"Startup failed; inspect {cluster.path / 'setup.log'}", flush=True)
        raise
    finally:
        print(f"Owned demo cluster stopped. Data and logs retained at {cluster.path}", flush=True)


if __name__ == "__main__":
    main()
