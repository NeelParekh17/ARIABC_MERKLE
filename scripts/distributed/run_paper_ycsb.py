#!/usr/bin/env python3
"""Run paper YCSB through the lab gateway with isolated replica instances.

Run the controller on .111. Builds are not performed: installed binaries must
have valid manifests and a common, live source fingerprint. Existing databases,
Kafka topics, and processes are never reset. Defaults mirror Final_Results YCSB.
"""

import argparse
import base64
from concurrent.futures import ThreadPoolExecutor
import csv
import hashlib
import json
import os
from pathlib import Path
import random
import re
import shlex
import shutil
import signal
import socket
import subprocess
import sys
import time
import uuid

from benchmark_validation import count_workload_queries, parse_gateway_result


HOSTS = ("10.129.148.247", "10.129.148.246", "10.129.148.248")
IDS = (1, 2, 4)
INSTALL = "/home/neel/Desktop/ariabc_install"
# Persistent per-run root on every replica (never /tmp: it is wiped on reboot).
PAPER_ROOT_PREFIX = "/home/neel/ariabc_data/paper_cluster_"
REPO = "/home/neel/Desktop/ariabc_cluster"
KAFKA = "/home/neel/Desktop/kafka_2.13-3.7.0"
PORTS = dict(pg=55493, client=18693, raft=19693, kafka=19092, controller=19093)
GATEWAY = "/home/neel/ARIABC/AriaBC/ariabc_pg/build/bin/ariabc_pg_gateway"
MODES = ("cluster", "pg", "bcdb_det", "bcdb_merkle")
ENVIRONMENT = {
    "BCDB_DECOUPLE_WORKERS": "1", "BCDB_POLL_MAX_US": "8",
    "BCDB_DT_PARSE_BARRIER": "1", "BCDB_DT_LIGHT_SNAPSHOT": "0",
    "BCDB_DT_SKIP_READONLY_GATE": "0", "BCDB_DT_COMPLETION_ONLY_SKIP_READS": "0",
    "BCDB_DET_QUEUE_HIGH_WM": "65536", "BCDB_DET_QUEUE_LOW_WM": "32768",
    "BCDB_FLOW_DEBUG": "0", "ARIABC_PROFILE": "1", "ARIABC_PG_MAX_RETRIES": "100",
    "ARIABC_DET_BLOCK_PARALLEL": "64", "ARIABC_DET_BLOCK_PIPELINE": "4",
    "ARIABC_DET_BLOCK_MAX": "2048", "ARIABC_DET_EVENT_BLOCK_FASTPATH": "0",
    "ARIABC_DET_PREFIXED_DIRECT_PARALLEL": "1", "ARIABC_DET_ORDER_START_SEQ": "0",
    "ARIABC_DET_ALLOW_RAW_COMPAT": "0", "ARIABC_DET_BLOCK_SKIP_READONLY": "0",
    "ARIABC_DET_COMPLETION_ONLY_SUCCESS": "0", "ARIABC_FULL_RESULT_REPLICA_LIMIT": "-1",
    "ARIABC_RESULT_PUBLISH_REPLICA_LIMIT": "0", "ARIABC_PREFERRED_LEADER_ID": "1",
    "ARIABC_RAFT_ORDERED_FANOUT": "1", "ARIABC_RAFT_ORDERED_BATCH_APPEND": "1",
    "ARIABC_RAFT_ORDERED_COALESCE_LOG": "1", "ARIABC_RAFT_ORDERING_POLICY": "leader-assigned",
    "ARIABC_RAFT_ORDERED_BATCH_TARGET_ENTRIES": "64",
    "ARIABC_RAFT_ORDERED_BATCH_LINGER_US": "1000",
    "ARIABC_RAFT_DURABLE_ASYNC_FLUSH": "1", "ARIABC_RAFT_STREAM_GAP": "512",
    "ARIABC_KAFKA_ASYNC_RESULT_PUBLISHER": "1", "ARIABC_KAFKA_PAYLOAD_FORMAT": "text",
    "ARIABC_KAFKA_RESULT_BATCH_MAX_DELAY_US": "3000",
    "ARIABC_KAFKA_RESULT_TARGET_BATCH_RECORDS": "128",
    "ARIABC_KAFKA_RESULT_BATCH_TARGET_RECORDS": "128",
    "ARIABC_KAFKA_ASYNC_RESULT_BATCH_RECORDS": "256",
    # WAIT_RESULTS accepts at most 600000 ms in the installed server protocol.
    "ARIABC_GATEWAY_DISPATCH_WORKERS": "8", "ARIABC_WAIT_RESULT_TIMEOUT_MS": "600000",
    # The saved runner uses compact unsigned T1 receipts inside the trusted lab.
    # Transaction BLAKE3 authentication remains enabled; full state is checked.
    "ARIABC_TRUSTED_RESULT_SIG_FASTPATH": "1",
}


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def write_json(path, value):
    Path(path).write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


class CaseFailure(RuntimeError):
    """A rejected case whose owned processes and artifacts were collected."""

    def __init__(self, metadata):
        super().__init__(metadata["error"])
        self.metadata = metadata


def checked(argv, *, env=None, cwd=None, stdin=None, timeout=180):
    result = subprocess.run([str(v) for v in argv], input=stdin, text=True,
                            capture_output=True, env=env, cwd=cwd, timeout=timeout)
    if result.returncode:
        raise RuntimeError(f"{shlex.join([str(v) for v in argv])} exited {result.returncode}: "
                           + (result.stdout + result.stderr)[-4000:])
    return result.stdout


def server_path(host):
    if host == HOSTS[1]:
        return "/home/neel/Desktop/ariabc_pg_build_u22/bin/ariabc_pg_server"
    return REPO + "/ariabc_pg/build/bin/ariabc_pg_server"


def verify_manifest(path, expected=None):
    manifest = dict(line.split("=", 1) for line in Path(str(path) + ".manifest").read_text().splitlines()
                    if "=" in line)
    if sha(path) != manifest["binary_sha256"]:
        raise RuntimeError(f"Executable differs from build manifest: {path}")
    if expected and manifest["source_fingerprint"] != expected:
        raise RuntimeError(f"Build source differs between executables: {path}")
    return dict(path=str(path), **manifest)


def owned_stop(pid_file, token, working_directory=None):
    if not Path(pid_file).exists():
        return
    pid = int(Path(pid_file).read_text().strip())
    proc = Path(f"/proc/{pid}")
    if not proc.exists():
        return
    if token.encode() not in (proc / "cmdline").read_bytes() or (
            working_directory is not None and (proc / "cwd").resolve() != Path(working_directory).resolve()):
        raise RuntimeError(f"Refusing to stop unowned PID {pid}")
    os.kill(pid, signal.SIGTERM)
    for _ in range(150):
        if not proc.exists() or (proc / "stat").read_text().split()[2] == "Z":
            return
        time.sleep(.1)
    # This PID was started by this campaign and its command was verified above.
    # A broker shutdown can wait indefinitely if its controller has stopped.
    if proc.exists() and token.encode() in (proc / "cmdline").read_bytes():
        os.kill(pid, signal.SIGKILL)


def node_action(request):
    """RPC executed only on a replica; stdout is one JSON result."""
    root = Path(request["root"])
    if not re.fullmatch(PAPER_ROOT_PREFIX + r"[a-zA-Z0-9_]+", str(root)):
        raise ValueError("Expected a fresh isolated " + PAPER_ROOT_PREFIX + "* root")
    env = dict(os.environ, **ENVIRONMENT)
    if request.get("result_receipts") == "signed":
        env["ARIABC_KAFKA_PAYLOAD_FORMAT"] = "bin"
        env["ARIABC_TRUSTED_RESULT_SIG_FASTPATH"] = "0"
    env["LD_LIBRARY_PATH"] = "/home/neel/Desktop/rdkafka_local/lib:" + INSTALL + "/lib"
    if not shutil_which("java"):
        env["JAVA_HOME"] = "/home/neel/Desktop/usr/lib/jvm/java-21-openjdk-amd64"
        env["PATH"] = env["JAVA_HOME"] + "/bin:" + env["PATH"]
    else:
        env.pop("JAVA_HOME", None)
    action = request["action"]
    if action == "preflight":
        if root.exists():
            raise RuntimeError("Remote campaign directory already exists")
        for port in PORTS.values():
            with socket.socket() as sock:
                if sock.connect_ex(("127.0.0.1", port)) == 0:
                    raise RuntimeError(f"Isolated port {port} already in use")
        binary = verify_manifest(server_path(request["host"]))
        postgres = verify_manifest(INSTALL + "/bin/postgres", binary["source_fingerprint"])
        live = checked(["python3", REPO + "/scripts/distributed/source_fingerprint.py",
                        "--repo", REPO, "--ring-capacity", "2048"]).strip()
        if live != binary["source_fingerprint"]:
            raise RuntimeError("Live replica source differs from its build manifest")
        root.mkdir(parents=True)
        return dict(server=binary, postgres=postgres, live_source_fingerprint=live,
                    hostname=socket.gethostname(), health=checked(["free", "-m"]))
    if action == "kafka_start":
        kafka_root = root / "kafka"
        kafka_root.mkdir()
        nid = HOSTS.index(request["host"]) + 1
        config = kafka_root / "server.properties"
        listener = f"PLAINTEXT://0.0.0.0:{PORTS['kafka']}"
        if nid == 1:
            listener += f",CONTROLLER://0.0.0.0:{PORTS['controller']}"
        config.write_text(f"""process.roles={'broker,controller' if nid == 1 else 'broker'}
node.id={nid}
controller.quorum.voters=1@{HOSTS[0]}:{PORTS['controller']}
listeners={listener}
inter.broker.listener.name=PLAINTEXT
advertised.listeners=PLAINTEXT://{request['host']}:{PORTS['kafka']}
controller.listener.names=CONTROLLER
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
num.network.threads=8
num.io.threads=8
socket.send.buffer.bytes=4194304
socket.receive.buffer.bytes=4194304
socket.request.max.bytes=104857600
log.dirs={kafka_root / 'data'}
num.partitions=1
num.recovery.threads.per.data.dir=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
log.retention.hours=168
log.segment.bytes=1073741824
log.retention.check.interval.ms=300000
""")
        checked([KAFKA + "/bin/kafka-storage.sh", "format", "-t", request["cluster_id"],
                 "-c", config], env=env)
        env["LOG_DIR"] = str(kafka_root / "logs")
        # Match the installed Kafka launcher used by Final_Results.
        env["KAFKA_HEAP_OPTS"] = "-Xmx1G -Xms1G"
        with (kafka_root / "stdout.log").open("w") as stream:
            proc = subprocess.Popen([KAFKA + "/bin/kafka-server-start.sh", str(config)],
                                    stdout=stream, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL,
                                    start_new_session=True, env=env)
        (kafka_root / "pid").write_text(str(proc.pid))
        return dict(pid=proc.pid, config=config.read_text(), heap_opts=env["KAFKA_HEAP_OPTS"])
    if action == "kafka_stop":
        owned_stop(root / "kafka/pid", str(root / "kafka/server.properties"))
        return dict(stopped=True)
    case = root / request["case"]
    pgdata = case / "pgdata"
    pg_ctl = [INSTALL + "/bin/pg_ctl", "-D", pgdata, "-w", "-t", "120"]
    psql = [INSTALL + "/bin/psql", "-X", "-v", "ON_ERROR_STOP=1", "-h", "127.0.0.1",
            "-p", str(PORTS["pg"]), "-U", "postgres", "-d", "postgres"]

    def sql(statement):
        return checked(psql + ["-At", "-c", statement], env=env, cwd=case)

    if action == "setup":
        case.mkdir()
        (root / "socket").mkdir(exist_ok=True)
        checked([INSTALL + "/bin/initdb", "-D", pgdata, "-U", "postgres", "--locale=C", "-A", "trust"],
                env=env, cwd=case)
        merkle = request["mode"] in ("cluster", "bcdb_merkle")
        workers = 1 if request["mode"] == "pg" else request["workers"]
        conf = f"""
port = {PORTS['pg']}
listen_addresses = '127.0.0.1'
unix_socket_directories = '{root / 'socket'}'
shared_buffers = '32MB'
max_connections = 832
max_worker_processes = 8
max_locks_per_transaction = 4012
maintenance_work_mem = '2GB'
bcdb_worker_count = {workers}
bcdb_advance_commit_watermark = on
bcdb_serial_gate_mode = 1
bcdb_serial_gate_source = 0
bcdb_dt_conflict_tracking = on
bcdb_dt_completion_only_skip_reads = off
bcdb_dt_hashtab_switch_threshold = 1500
bcdb_result_ring_slots = 2048
bcdb_gate_telemetry = off
bcdb_gate_snapshot_each_block = off
enable_merkle_index = {'on' if merkle else 'off'}
merkle_apply_synchronous_direct = {'on' if merkle else 'off'}
synchronous_commit = on
fsync = on
full_page_writes = on
wal_level = replica
autovacuum = off
track_io_timing = on
checkpoint_timeout = '30min'
max_wal_size = '20GB'
default_transaction_isolation = 'serializable'
log_min_messages = warning
"""
        with (pgdata / "postgresql.conf").open("a") as stream:
            stream.write(conf)
        checked(pg_ctl + ["-l", case / "postgres.log", "start"], env=env)
        checked(psql + ["-f", request["inputs"] + "/setup.sql"], env=env, cwd=case)
        if merkle:
            sql("CREATE INDEX usertable_merkle ON paper_ycsb.usertable USING merkle(ycsb_key) "
                "WITH(partitions=200,fanout=4,split_threshold=32,merge_threshold=8);"
                "CREATE INDEX usertable_merkle_lookup ON paper_ycsb.usertable "
                "(merkle_partition_for_hash(merkle_key_hash(ycsb_key),200),"
                "merkle_key_hash(ycsb_key),ycsb_key);")
        settings = json.loads(sql("SELECT json_object_agg(name,setting) FROM pg_settings;"))
        expected = dict(bcdb_worker_count=str(workers), bcdb_dt_conflict_tracking="on",
                        bcdb_dt_completion_only_skip_reads="off", bcdb_serial_gate_mode="1",
                        bcdb_serial_gate_source="0", bcdb_result_ring_slots="2048",
                        synchronous_commit="on", fsync="on", full_page_writes="on",
                        max_connections="832", default_transaction_isolation="serializable",
                        enable_merkle_index="on" if merkle else "off",
                        merkle_apply_synchronous_direct="on" if merkle else "off")
        for key, value in expected.items():
            if settings.get(key) != value:
                raise RuntimeError(f"Unexpected {key}={settings.get(key)}; expected {value}")
        if int(settings["shared_buffers"]) * int(settings["block_size"]) != 32 * 1024**2:
            raise RuntimeError("shared_buffers must be 32MB")
        checked(psql + ["-f", str(root / "paper_ycsb_det_preflight.sql")], env=env)
        initial = sql('COPY (SELECT * FROM paper_ycsb.usertable ORDER BY ycsb_key COLLATE "C") TO STDOUT;')
        (case / "initial.tsv").write_text(initial)
        sql("VACUUM ANALYZE paper_ycsb.usertable;")
        sql("CHECKPOINT;")
        checked(pg_ctl + ["stop", "-m", "fast"], env=env)
        cache = json.loads(checked(["python3", str(root / "benchmark_cache.py"), str(pgdata)], env=env))
        write_json(case / "cache.json", cache)
        checked(pg_ctl + ["-l", case / "postgres.log", "start"], env=env)
        write_json(case / "settings.json", settings)
        return dict(settings=settings, cache=cache, initial_state_sha256=sha(case / "initial.tsv"))
    if action == "oracle":
        checked(psql + ["-q", "-f", request["trace"]], env=env, cwd=case, timeout=600)
        final = sql('COPY (SELECT * FROM paper_ycsb.usertable ORDER BY ycsb_key COLLATE "C") TO STDOUT;')
        (case / "oracle.tsv").write_text(final)
        return dict(sha256=sha(case / "oracle.tsv"))
    if action == "start":
        host = request["host"]
        nid = IDS[HOSTS.index(host)]
        cluster = request["mode"] == "cluster"
        det = request["mode"] != "pg"
        members = ",".join(f"{i}={h}:{PORTS['raft']}" for i, h in zip(IDS, HOSTS)) if cluster else f"1=127.0.0.1:{PORTS['raft']}"
        argv = [server_path(host), "--id", str(nid if cluster else 1),
                "--raftEndpoint", f"{host if cluster else '127.0.0.1'}:{PORTS['raft']}",
                "--clientPort", str(PORTS["client"]), "--raftMembers", members,
                "--dbName", "postgres", "--dbHost", "127.0.0.1", "--dbPort", str(PORTS["pg"]),
                "--dbUser", "postgres", "--dbType", str(int(det)), "--safedb", str(int(det)),
                "--dbConnPoolSize", str(request["workers"]), "--bcdbInitBlockSize", str(request["workers"]),
                "--pgExecMode", "event", "--bypassRaft", "0" if cluster else "1"]
        if cluster:
            env["ARIABC_RAFT_CLUSTER_ID"] = request["case"]
            env["ARIABC_RAFT_NODE_ID"] = str(nid)
            argv += ["--raft-storage-mode", "durable", "--raft-storage-dir", str(case / "raft"),
                     "--raft-cluster-id", request["case"], "--raft-apply-ledger", "off",
                     "--kafkaBootstrap", f"localhost:{PORTS['kafka']}", "--resultTopic", request["topic"]]
        recorded_env = {key: value for key, value in env.items()
                        if key in ENVIRONMENT or key in ("LD_LIBRARY_PATH", "ARIABC_RAFT_CLUSTER_ID", "ARIABC_RAFT_NODE_ID")}
        write_json(case / "server-command.json", dict(argv=argv, environment=recorded_env))
        with (case / "server.log").open("w") as stream:
            proc = subprocess.Popen(argv, env=env, cwd=case, stdout=stream, stderr=subprocess.STDOUT,
                                    stdin=subprocess.DEVNULL, start_new_session=True)
        (case / "server.pid").write_text(str(proc.pid))
        return dict(pid=proc.pid, argv=argv)
    if action == "verify":
        final = sql('COPY (SELECT * FROM paper_ycsb.usertable ORDER BY ycsb_key COLLATE "C") TO STDOUT;')
        (case / "final.tsv").write_text(final)
        count = int(sql("SELECT count(*) FROM paper_ycsb.usertable;"))
        marker = sql("SELECT label FROM paper_ycsb.run_marker WHERE id=1;").strip()
        merkle = request["mode"] in ("cluster", "bcdb_merkle")
        verify = sql("SELECT merkle_verify_index('paper_ycsb.usertable_merkle'::regclass);").strip() if merkle else "disabled"
        root_hash = sql("SELECT merkle_root_hash('paper_ycsb.usertable');").strip() if merkle else "disabled"
        if count != 10000 or marker != request["case"] or (merkle and verify != "t"):
            raise RuntimeError(f"Invalid final state: {count}, marker={marker}, merkle={verify}")
        return dict(rows=count, marker=marker, merkle_verify=verify, merkle_root=root_hash,
                    final_state_sha256=sha(case / "final.tsv"))
    if action == "marker_table":
        sql("CREATE TABLE paper_ycsb.run_marker(id integer PRIMARY KEY,label text NOT NULL);")
        return dict(created=True)
    if action == "stop":
        owned_stop(case / "server.pid", "ariabc_pg_server", case)
        if (pgdata / "postmaster.pid").exists():
            checked(pg_ctl + ["stop", "-m", "fast"], env=env)
        return dict(stopped=True)
    raise ValueError(f"Unknown node action {action}")


def shutil_which(program):
    import shutil
    return shutil.which(program)


class Campaign:
    def __init__(self, args):
        self.args = args
        self.out = args.out.resolve()
        self.root = PAPER_ROOT_PREFIX + args.run_id
        self.node_script = self.root + "/run_paper_ycsb.py"
        self.active = []
        self.kafka_active = []
        self.env = dict(os.environ, **ENVIRONMENT)
        self.env["LD_LIBRARY_PATH"] = "/home/neel/ARIABC/install/lib:/home/neel/Desktop/rdkafka_local/lib"
        self.oracles = {}
        self.results = []
        self.failed_cases = []
        self.source = None
        self.completed = set()
        if args.result_receipts == "signed":
            self.env["ARIABC_KAFKA_PAYLOAD_FORMAT"] = "bin"
            self.env["ARIABC_TRUSTED_RESULT_SIG_FASTPATH"] = "0"

    def rpc(self, host, action, **values):
        request = dict(root=self.root, host=host, action=action,
                       result_receipts=self.args.result_receipts, **values)
        command = shlex.join(["python3", self.node_script, "--node-action"])
        # Preflight is executed from the uploaded staging directory because it
        # must reject an existing campaign root rather than reuse it.
        if action == "preflight":
            command = shlex.join(["python3", str(self.args.staging / "run_paper_ycsb.py"), "--node-action"])
        result = checked(["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", "neel@" + host,
                          command], stdin=json.dumps(request), timeout=720)
        return json.loads(result)

    def parallel(self, hosts, action, **values):
        with ThreadPoolExecutor(max_workers=3) as pool:
            futures = {host: pool.submit(self.rpc, host, action, **values) for host in hosts}
            results, failures = {}, []
            for host, future in futures.items():
                try:
                    results[host] = future.result()
                except Exception as error:
                    failures.append(f"{host}: {error}")
            if failures:
                raise RuntimeError("; ".join(failures))
            return results

    def prepare(self):
        self.out.mkdir(parents=True, exist_ok=False)
        write_json(self.out / "command.json", dict(argv=sys.argv, ports=PORTS,
                   environment={key: self.env[key] for key in ENVIRONMENT}))
        write_json(self.out / "input-manifest.json", json.loads((self.args.inputs / "manifest.json").read_text()))
        (self.out / "executed-runner.py").write_bytes(Path(__file__).read_bytes())
        gateway = verify_manifest(GATEWAY)
        self.source = gateway["source_fingerprint"]
        live = checked(["python3", "/home/neel/ARIABC/AriaBC/scripts/distributed/source_fingerprint.py",
                        "--repo", "/home/neel/ARIABC/AriaBC", "--ring-capacity", "2048"]).strip()
        if live != self.source:
            raise RuntimeError("Gateway live source differs from binary manifest")
        provenance = self.parallel(HOSTS, "preflight")
        for host, value in provenance.items():
            if value["live_source_fingerprint"] != self.source:
                raise RuntimeError(f"Source fingerprint mismatch on {host}")
        write_json(self.out / "provenance.json", dict(gateway=gateway, replicas=provenance))
        for host in HOSTS:
            checked(["scp", "-r", "-o", "BatchMode=yes", str(self.args.inputs), "neel@" + host + ":" + self.root + "/inputs"])
            for name in ("run_paper_ycsb.py", "benchmark_validation.py", "benchmark_cache.py", "paper_ycsb_det_preflight.sql"):
                checked(["scp", "-o", "BatchMode=yes", str(self.args.staging / name), "neel@" + host + ":" + self.root + "/" + name])
        if "cluster" in self.args.modes:
            cluster_id = base64.urlsafe_b64encode(uuid.uuid4().bytes).decode().rstrip("=")
            self.kafka_active = list(HOSTS)
            started = self.parallel(HOSTS, "kafka_start", cluster_id=cluster_id)
            write_json(self.out / "kafka.json", started)
            self.wait_ports(HOSTS, PORTS["kafka"], 120)
        print("Preflight PASS: common installed source and binary manifests; isolated ports", flush=True)

    @staticmethod
    def wait_ports(hosts, port, limit=90):
        deadline = time.monotonic() + limit
        while time.monotonic() < deadline:
            ready = True
            for host in hosts:
                with socket.socket() as sock:
                    sock.settimeout(.5)
                    ready &= sock.connect_ex((host, port)) == 0
            if ready:
                return
            time.sleep(.25)
        raise RuntimeError(f"Port {port} did not become ready on {hosts}")

    def topic(self, name):
        # Three partitions, one assigned to each broker, matching the saved
        # colocated Kafka runner's --replica-assignment 1,2,3 exactly.
        command = shlex.join([KAFKA + "/bin/kafka-topics.sh", "--bootstrap-server",
                              f"localhost:{PORTS['kafka']}", "--create", "--topic", name,
                              "--replica-assignment", "1,2,3"])
        fallback = "export JAVA_HOME=/home/neel/Desktop/usr/lib/jvm/java-21-openjdk-amd64; export PATH=$JAVA_HOME/bin:$PATH; "
        result = checked(["ssh", "-o", "BatchMode=yes", "neel@" + HOSTS[0], fallback + command], timeout=120)
        return result

    def gateway(self, case, hosts, mode, workers, trace, count, marker=False):
        cluster = mode == "cluster"
        argv = [GATEWAY, "--nodes", ",".join(f"{h}:{PORTS['client']}" for h in hosts),
                "--queryFrom", str(trace), "--dbType", str(int(mode != "pg")),
                "--detStartSeq", str(count if marker else 0), "--reqIdOffset", str(count + 1 if marker else 1),
                "--detWindow", "1" if marker else "1024", "--detBatchSize", "256",
                "--dbConnPoolSize", str(workers), "--submitMode", "event",
                "--detSubmitPipeline", "0" if marker else "1", "--detPipelineDepth", "1" if marker else "1024",
                "--detClientMode", "event", "--detClientWorkers", "1" if marker else "96",
                "--detClientInflight", "1" if marker else "16", "--numTerminals", "1" if marker else "96",
                "--clientId", "paper-marker" if marker else "paper-ycsb", "--connFanout", "1",
                "--txSign", "blake3", "--totalNodes", str(len(hosts))]
        if cluster:
            argv += ["--raft-node-ids", "1,2,4", "--kafkaBootstrap",
                     ",".join(f"{h}:{PORTS['kafka']}" for h in HOSTS), "--resultTopic", case.name,
                     "--waitMajority", "1", "--completionPath", "kafka_majority",
                     "--validationMode", "majority_async_all3"]
        else:
            argv += ["--waitMajority", "0", "--completionPath", "direct"]
        write_json(case / ("marker-command.json" if marker else "gateway-command.json"), argv)
        path = case / ("marker-gateway.log" if marker else "gateway.log")
        with path.open("w") as stream:
            proc = subprocess.Popen(argv, cwd=case, env=self.env, stdout=stream, stderr=subprocess.STDOUT,
                                    start_new_session=True)
            try:
                rc = proc.wait(timeout=self.args.timeout)
            except BaseException:
                proc.terminate()
                try:
                    proc.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait()
                raise
        metrics = parse_gateway_result(path.read_text(), 1 if marker else count, rc,
                                       mode=None if cluster else mode)
        if cluster:
            profile = re.findall(r"^PROFILE_GATEWAY .*$", path.read_text(), re.M)[-1]
            values = dict(re.findall(r"(\w+)=(\S+)", profile))
            terminal_count = 1 if marker else count
            expected = dict(async_all3_verified_count=str(terminal_count), async_all3_failure_count="0",
                            async_all3_timeout_count="0", async_all3_missing_count="0",
                            async_all3_capacity_exhausted_count="0", audit_pending_current="0",
                            kafka_parse_failures="0", tx_signature_mismatches="0")
            for nid in IDS:
                expected[f"reply_records_from_node{nid}"] = str(terminal_count)
            for key, value in expected.items():
                if values.get(key) != value:
                    raise RuntimeError(f"Invalid cluster audit {key}={values.get(key)}, expected {value}")
            metrics["async_all3_verified_count"] = terminal_count
        return metrics

    def stop_active(self):
        failures = []
        for host, name in self.active:
            try:
                self.rpc(host, "stop", case=name)
            except Exception as error:
                failures.append(f"{host}/{name}: {error}")
        self.active = []
        if failures:
            raise RuntimeError("Cleanup failed: " + "; ".join(failures))

    def collect(self, case, hosts):
        for host in hosts:
            dest = case / host
            dest.mkdir(exist_ok=True)
            command = "cd " + shlex.quote(self.root + "/" + case.name) + "; tar -cf - --exclude=pgdata --exclude=raft ."
            with (dest / "artifacts.tar").open("wb") as stream:
                subprocess.run(["ssh", "-o", "BatchMode=yes", "neel@" + host, command],
                               stdout=stream, check=True, timeout=120)
            checked(["tar", "-xf", dest / "artifacts.tar", "-C", dest])

    def case(self, trace, workers, mode, trial, smoke=False):
        count = count_workload_queries(trace)
        name = f"{'smoke' if smoke else 'run'}_{trace.stem}_w{workers}_{mode}_t{trial}"
        case = self.out / name
        case.mkdir()
        hosts = HOSTS if mode == "cluster" else HOSTS[:1]
        metadata = dict(status="running", mode=mode, workers=workers, trial=trial,
                        smoke=smoke,
                        workload=trace.name, workload_sha256=sha(trace), transactions=count,
                        internal_operations=count * 10, source_fingerprint=self.source)
        write_json(case / "result.json", metadata)
        self.active = [(h, name) for h in hosts]
        failure = None
        try:
            setup = self.parallel(hosts, "setup", case=name, workers=workers, mode=mode,
                                  inputs=self.root + "/inputs")
            write_json(case / "setup.json", setup)
            if len({v["initial_state_sha256"] for v in setup.values()}) != 1:
                raise RuntimeError("Replica initial state hashes differ")
            if mode == "cluster":
                (case / "topic.log").write_text(self.topic(name))
            for host in hosts:
                self.rpc(host, "marker_table", case=name)
            starts = self.parallel(hosts, "start", case=name, workers=workers, mode=mode, topic=name)
            write_json(case / "servers.json", starts)
            self.wait_ports(hosts, PORTS["client"])
            if mode == "cluster":
                time.sleep(3)
            metrics = self.gateway(case, hosts, mode, workers, trace, count)
            marker = case / "marker.sql"
            marker.write_text(f"INSERT INTO paper_ycsb.run_marker VALUES (1,'{name}');\n")
            marker_metrics = self.gateway(case, hosts, mode, workers, marker, count, marker=True)
            verified = self.parallel(hosts, "verify", case=name, mode=mode)
            if len({v["final_state_sha256"] for v in verified.values()}) != 1:
                raise RuntimeError("Replica full table states differ after the all-replica marker")
            if mode == "cluster" and len({v["merkle_root"] for v in verified.values()}) != 1:
                raise RuntimeError("Replica Merkle roots differ after marker")
            metadata.update(metrics=metrics, marker_metrics=marker_metrics, replicas=verified)
            if mode in ("bcdb_det", "bcdb_merkle") and trace.name in self.oracles:
                expected = self.oracles[trace.name]
                if verified[HOSTS[0]]["final_state_sha256"] != expected:
                    raise RuntimeError("Deterministic final table differs from serial PG oracle")
                metadata["serial_oracle_match"] = True
            # A leader-assigned Raft admission order can differ from file order.
            # Record oracle equality without treating a different legal order
            # as divergence; the replica equality check above is mandatory.
            if mode == "cluster" and trace.name in self.oracles:
                metadata["input_order_oracle_match"] = verified[HOSTS[0]]["final_state_sha256"] == self.oracles[trace.name]
            metadata["status"] = "passed"
        except BaseException as error:
            metadata.update(status="failed", error=str(error) or type(error).__name__)
            failure = error
        finally:
            try:
                self.stop_active()
            finally:
                self.collect(case, hosts)
                write_json(case / "result.json", metadata)
        if failure is not None:
            if isinstance(failure, Exception):
                raise CaseFailure(metadata) from failure
            raise failure
        try:
            for host in hosts:
                profiles = re.findall(r"^PROFILE_SERVER .*$", (case / host / "server.log").read_text(), re.M)
                values = dict(re.findall(r"(\w+)=(\S+)", profiles[-1])) if profiles else {}
                expected = dict(exec_calls=str(count + 1), retry_exhausted_total="0", backlog_cur="0",
                                inflight_cur="0", unique_owned_pg_connections=str(workers),
                                bcdb_init_enabled="0" if mode == "pg" else "1")
                if mode != "pg":
                    expected["bcdb_init_arg_size_configured"] = str(workers)
                for key, value in expected.items():
                    if values.get(key) != value:
                        raise RuntimeError(f"Invalid final server profile on {host}: {key}={values.get(key)}, expected {value}")
        except Exception as error:
            metadata.update(status="failed", error=str(error))
            write_json(case / "result.json", metadata)
            raise CaseFailure(metadata) from error
        print(f"PASS {name}: {count} tx, {metrics['tps']:.2f} TPS; replicas, marker and final server profiles verified", flush=True)
        self.results.append(metadata)
        self.write_summary()

    def oracle(self, trace):
        name = "oracle_" + trace.stem
        case = self.out / name
        case.mkdir()
        self.active = [(HOSTS[0], name)]
        try:
            self.rpc(HOSTS[0], "setup", case=name, workers=1, mode="pg", inputs=self.root + "/inputs")
            checked(["scp", "-o", "BatchMode=yes", str(trace), "neel@" + HOSTS[0] + ":" + self.root + "/" + name + "/trace.sql"])
            result = self.rpc(HOSTS[0], "oracle", case=name, trace=self.root + "/" + name + "/trace.sql")
            self.oracles[trace.name] = result["sha256"]
            write_json(case / "result.json", result)
        finally:
            self.stop_active()
            self.collect(case, HOSTS[:1])
        print("Serial oracle ready: " + trace.name, flush=True)

    def write_summary(self):
        fields = ("mode", "workload", "workers", "trial", "smoke", "transactions", "internal_operations",
                  "wall_time_ms", "wall_including_drains_ms", "tps", "divergence_count", "permanent_failures",
                  "replica_state_match", "post_marker_pass", "merkle_pass", "source_fingerprint")
        with (self.out / "summary.csv").open("w", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=fields)
            writer.writeheader()
            for item in self.results:
                row = {key: item[key] for key in fields if key in item}
                row.update({key: item["metrics"][key] for key in fields if key in item["metrics"]})
                row.update(replica_state_match=1, post_marker_pass=1,
                           merkle_pass=1 if item["mode"] in ("cluster", "bcdb_merkle") else "not_applicable")
                writer.writerow(row)

    def run(self):
        try:
            self.prepare()
            traces = sorted(self.args.inputs.glob("ycsb_paper_skew_*.txt"))
            if self.args.skews:
                names = {"ycsb_paper_skew_" + ("%.2f" % float(value)).replace(".", "_") + ".txt"
                         for value in self.args.skews.split(",")}
                traces = [path for path in traces if path.name in names]
                if {path.name for path in traces} != names:
                    raise RuntimeError("Requested skew is missing from the input package")
            if not traces:
                raise RuntimeError("No paper workload files in input package")
            for name, digest in json.loads((self.args.inputs / "manifest.json").read_text())["sha256"].items():
                if sha(self.args.inputs / name) != digest:
                    raise RuntimeError("Input package hash mismatch: " + name)
            if not self.args.no_smoke:
                smoke = self.out / "smoke.sql"
                smoke.write_text("".join("\n".join([line for line in p.read_text().splitlines()
                                                   if line.strip() and not line.startswith("--")][:100])
                                         + "\n" for p in traces))
                self.oracle(smoke)
                self.case(smoke, 4, "cluster" if "cluster" in self.args.modes else self.args.modes[0], 1, smoke=True)
            for trace in traces:
                if self.args.oracle_from:
                    previous = self.args.oracle_from / ("oracle_" + trace.stem)
                    result = json.loads((previous / "result.json").read_text())
                    source = json.loads((self.args.oracle_from / "provenance.json").read_text())["gateway"]["source_fingerprint"]
                    artifact = previous / HOSTS[0]
                    if source != self.source or sha(artifact / "trace.sql") != sha(trace) or sha(artifact / "oracle.tsv") != result["sha256"]:
                        raise RuntimeError("Reference state provenance or workload hash differs")
                    self.oracles[trace.name] = result["sha256"]
                    write_json(self.out / ("reference_" + trace.stem + ".json"),
                               dict(path=str(previous), sha256=result["sha256"], workload_sha256=sha(trace)))
                else:
                    self.oracle(trace)
            if self.args.resume_from:
                previous_root = self.args.resume_from.resolve()
                previous_source = json.loads((previous_root / "provenance.json").read_text())["gateway"]["source_fingerprint"]
                if previous_source != self.source:
                    raise RuntimeError("Cannot resume results from a different installed build")
                for row in csv.DictReader((previous_root / "summary.csv").open()):
                    if row.get("smoke") == "True":
                        continue
                    key = (row["workload"], int(row["workers"]), row["mode"], int(row["trial"]))
                    if key[0] not in {p.name for p in traces} or key[1] not in self.args.workers or key[2] not in self.args.modes or key[3] > self.args.trials:
                        continue
                    name = f"run_{Path(key[0]).stem}_w{key[1]}_{key[2]}_t{key[3]}"
                    previous_case = previous_root / name
                    result = json.loads((previous_case / "result.json").read_text())
                    if result["status"] != "passed" or result["source_fingerprint"] != self.source or result["workload_sha256"] != sha(self.args.inputs / key[0]):
                        raise RuntimeError("Resume result does not match current workload/build: " + name)
                    metrics = parse_gateway_result((previous_case / "gateway.log").read_text(), result["transactions"],
                                                   mode=None if key[2] == "cluster" else key[2])
                    if metrics != {k: v for k, v in result["metrics"].items() if k != "async_all3_verified_count"}:
                        raise RuntimeError("Resume metrics differ from gateway log: " + name)
                    for host, evidence in result["replicas"].items():
                        if sha(previous_case / host / "final.tsv") != evidence["final_state_sha256"]:
                            raise RuntimeError("Resume table dump differs from recorded hash: " + name)
                        profiles = re.findall(r"^PROFILE_SERVER .*$", (previous_case / host / "server.log").read_text(), re.M)
                        if not profiles or dict(re.findall(r"(\w+)=(\S+)", profiles[-1])).get("retry_exhausted_total") != "0":
                            raise RuntimeError("Resume server profile is incomplete or failed: " + name)
                    result["resumed_from"] = str(previous_case)
                    shutil.copytree(previous_case, self.out / name)
                    write_json(self.out / name / "result.json", result)
                    self.results.append(result)
                    self.completed.add(key)
                self.write_summary()
                if self.args.resume_rejected_pg:
                    for record_path in sorted(previous_root.glob("run_*_pg_t*/result.json")):
                        result = json.loads(record_path.read_text())
                        if result["status"] != "failed" or result["mode"] != "pg":
                            continue
                        key = (result["workload"], result["workers"], result["mode"], result["trial"])
                        if key[0] not in {p.name for p in traces} or key[1] not in self.args.workers or key[3] > self.args.trials or "pg" not in self.args.modes:
                            continue
                        if key in self.completed or result["source_fingerprint"] != self.source or result["workload_sha256"] != sha(self.args.inputs / key[0]):
                            raise RuntimeError("Rejected PG case identity differs: " + str(record_path))
                        previous_case = record_path.parent
                        log = (previous_case / "gateway.log").read_text()
                        profiles = re.findall(r"^PROFILE_GATEWAY .*$", log, re.M)
                        values = dict(re.findall(r"(\w+)=(\S+)", profiles[-1])) if profiles else {}
                        # Retain only an actual workload timeout at the supported
                        # limit; setup/protocol errors must be fixed and rerun.
                        if "reason=timeout" not in log or int(values.get("overall_wall_ms", "0")) < 600000 or int(values.get("permanent_failures", "0")) < 1:
                            raise RuntimeError("Rejected PG evidence is not a measured result timeout")
                        server_profiles = re.findall(r"^PROFILE_SERVER .*$", (previous_case / HOSTS[0] / "server.log").read_text(), re.M)
                        if not server_profiles or f"loaded {result['transactions']} queries" not in log:
                            raise RuntimeError("Rejected PG workload/server evidence is incomplete")
                        result.update(resumed_from=str(previous_case), rejection_kind="result_wait_timeout")
                        shutil.copytree(previous_case, self.out / previous_case.name)
                        write_json(self.out / previous_case.name / "result.json", result)
                        self.failed_cases.append(result)
                        self.completed.add(key)
                    write_json(self.out / "rejected-cases.json", self.failed_cases)
                print(f"Resumed {len(self.results)} verified cases and {len(self.failed_cases)} retained rejections from {previous_root}", flush=True)
            cases = [(trace, workers, trial) for trace in traces for workers in self.args.workers
                     for trial in range(1, self.args.trials + 1)]
            random.Random(42).shuffle(cases)
            for index, (trace, workers, trial) in enumerate(cases):
                modes = self.args.modes[index % len(self.args.modes):] + self.args.modes[:index % len(self.args.modes)]
                for mode in modes:
                    if (trace.name, workers, mode, trial) in self.completed:
                        continue
                    try:
                        self.case(trace, workers, mode, trial)
                    except CaseFailure as error:
                        if not self.args.keep_going:
                            raise
                        self.failed_cases.append(error.metadata)
                        write_json(self.out / "rejected-cases.json", self.failed_cases)
                        print(f"REJECTED {trace.stem} w{workers} {mode} t{trial}: {error}", flush=True)
            write_json(self.out / "campaign-result.json", dict(status="failed" if self.failed_cases else "passed", cases=sum(not r["smoke"] for r in self.results),
                       rejected_cases=len(self.failed_cases),
                       attempted_cases=sum(not r["smoke"] for r in self.results) + len(self.failed_cases),
                       smoke_cases=sum(r["smoke"] for r in self.results),
                       expected_cases=len(cases) * len(self.args.modes), oracles=self.oracles))
        finally:
            cleanup_errors = []
            try:
                self.stop_active()
            except Exception as error:
                cleanup_errors.append(str(error))
            for host in reversed(self.kafka_active):
                try:
                    self.rpc(host, "kafka_stop")
                except Exception as error:
                    cleanup_errors.append(str(error))
            if cleanup_errors:
                raise RuntimeError("Cleanup failed: " + "; ".join(cleanup_errors))
        return not self.failed_cases


def main():
    if "--node-action" in sys.argv:
        print(json.dumps(node_action(json.load(sys.stdin))))
        return
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inputs", required=True, type=Path)
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument("--staging", required=True, type=Path,
                        help="Same uploaded script directory on controller and replicas")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--workers", default="1,4,8,16")
    parser.add_argument("--modes", default=",".join(MODES))
    parser.add_argument("--trials", type=int, default=1)
    parser.add_argument("--timeout", type=int, default=3600)
    parser.add_argument("--no-smoke", action="store_true")
    parser.add_argument("--oracle-from", type=Path, help="Reuse verified, matching serial reference artifacts")
    parser.add_argument("--resume-from", type=Path, help="Copy and verify accepted cases from a previous output")
    parser.add_argument("--keep-going", action="store_true",
                        help="Preserve rejected cases and run remaining points; final exit remains nonzero")
    parser.add_argument("--resume-rejected-pg", action="store_true",
                        help="Retain measured ten-minute PG timeout rejections when resuming; never count them as passes")
    parser.add_argument("--skews", help="Select input skews, e.g. 0.6; default all package skews")
    parser.add_argument("--result-receipts", choices=("trusted-text", "signed"), default="trusted-text",
                        help="Match Final_Results trusted T1 receipts, or additionally verify signed binary receipts")
    args = parser.parse_args()
    def interrupted(signum, frame):
        raise KeyboardInterrupt(f"Received signal {signum}")
    signal.signal(signal.SIGTERM, interrupted)
    if socket.gethostname().split(".")[0] != "myubuntu":
        parser.error("Run the controller on the .111 gateway, not on the developer workstation")
    if not re.fullmatch("[a-zA-Z0-9_]+", args.run_id):
        parser.error("run-id must contain letters, digits and underscores")
    args.workers = [int(v) for v in args.workers.split(",")]
    args.modes = args.modes.split(",")
    if args.trials < 1 or any(v < 1 for v in args.workers) or any(v not in MODES for v in args.modes):
        parser.error("Invalid workers, trials or modes")
    if args.resume_rejected_pg and (not args.resume_from or not args.keep_going):
        parser.error("--resume-rejected-pg requires --resume-from and --keep-going")
    if not Campaign(args).run():
        raise SystemExit(1)


if __name__ == "__main__":
    main()
