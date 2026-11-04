#!/usr/bin/env python3
"""Run on the lab host with an isolated build and a fresh output directory.

python3 control_plane_regression.py --build-dir /tmp/control/build \
    --pg-bin ~/Desktop/ariabc_install/bin --output-dir /tmp/control/live
"""

import argparse
import json
import os
from pathlib import Path
import socket
import socketserver
import struct
import subprocess
import threading
import time


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def exact(sock, count):
    chunks = bytearray()
    while len(chunks) < count:
        chunk = sock.recv(count - len(chunks))
        if not chunk:
            raise EOFError("incomplete RPC frame")
        chunks.extend(chunk)
    return bytes(chunks)


def single_frame(sql, req_id, terminal=False):
    rid, text = req_id.encode(), sql.encode()
    header = (struct.pack("!IIIII", 0xFFFFFFFE, 1, 5000, len(rid), len(text))
              if terminal else struct.pack("!II", len(rid), len(text)))
    return header + rid + text


def batch_frame(items, tagged=False):
    data = struct.pack("!II", 0xFFFFFFFD if tagged else 0xFFFFFFFF, len(items))
    for rid, text in items:
        rid, text = rid.encode(), text.encode()
        if tagged:
            data += struct.pack("!I", 2)
        data += struct.pack("!II", len(rid), len(text))
        if tagged:
            data += bytes(32)
        data += rid + text
    return data


def rpc(sock, frame):
    sock.sendall(frame)
    status, size = struct.unpack("!BI", exact(sock, 5))
    return status, exact(sock, size).decode()


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def stop_process(proc):
    proc.terminate()
    try:
        proc.wait(timeout=15)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=5)


def gateway_legacy_test(gateway, out, env):
    """Emulate an older server: only GET_COMMIT_INDEX is a safe stats probe."""
    requests = []

    class Handler(socketserver.BaseRequestHandler):
        def handle(self):
            self.request.settimeout(5)
            try:
                while True:
                    rid_len, sql_len = struct.unpack("!II", exact(self.request, 8))
                    rid = exact(self.request, rid_len).decode()
                    sql = exact(self.request, sql_len).decode()
                    requests.append({"req_id": rid, "sql": sql})
                    # An old server would submit an unknown verb, then report
                    # ACCEPTED. The regression must catch even that response.
                    msg = ("0" if sql == "__ARIABC_CTRL_GET_COMMIT_INDEX" else
                           "ACCEPTED accepted=1 raft_log_idx=2").encode()
                    self.request.sendall(struct.pack("!BI", 0, len(msg)) + msg)
            except (EOFError, OSError):
                pass

    class Server(socketserver.ThreadingTCPServer):
        daemon_threads = True

    empty = out / "empty.sql"
    empty.write_text("")
    with Server(("127.0.0.1", 0), Handler) as server:
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            run = subprocess.run(
                [str(gateway), "--queryFrom", str(empty), "--nodes",
                 f"127.0.0.1:{server.server_address[1]}", "--txSign", "0", "--dbType", "2"],
                env=env, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=30)
            (out / "legacy-gateway.log").write_text(run.stdout)
            require(run.returncode == 0, "gateway failed against legacy control response")
            require("user_abort_counter_supported=0" in run.stdout, "legacy stats fallback missing")
            require(bool(requests), "gateway made no capability request")
            require(all(r["sql"] == "__ARIABC_CTRL_GET_COMMIT_INDEX" for r in requests),
                    f"unsafe legacy probe: {requests}")
            require(requests[0]["req_id"].startswith("ctrl-stats-v1-"), "stats opt-in missing")
        finally:
            server.shutdown()
            thread.join(timeout=5)
    (out / "legacy-requests.json").write_text(json.dumps(requests, indent=2) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build-dir", type=Path, required=True)
    parser.add_argument("--pg-bin", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    out = args.output_dir.resolve()
    out.mkdir(parents=True, exist_ok=False)
    pg_bin = args.pg_bin.resolve()
    build = args.build_dir.resolve()
    env = os.environ.copy()
    env["LD_LIBRARY_PATH"] = str(pg_bin.parent / "lib") + ":" + env.get("LD_LIBRARY_PATH", "")
    env["ARIABC_TX_TAG_VERIFY"] = "off"
    env["ARIABC_LEADER_ASSIGNED_ORDERING"] = "0"
    pgdata = out / "pgdata"
    pgport = free_port()
    summary = {}

    def pg_sql(sql):
        return subprocess.check_output(
            [str(pg_bin / "psql"), "-XAt", "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-h", "127.0.0.1",
             "-p", str(pgport), "-d", "postgres", "-c", sql], env=env, text=True).strip()

    with (out / "initdb.log").open("w") as log:
        subprocess.run([str(pg_bin / "initdb"), "-D", str(pgdata), "-U", "postgres", "-A", "trust", "--no-locale"],
                       env=env, check=True, stdout=log, stderr=subprocess.STDOUT)
    with (pgdata / "postgresql.conf").open("a") as config:
        config.write(f"\nlisten_addresses='127.0.0.1'\nport={pgport}\n"
                     f"unix_socket_directories='{out}'\nlog_statement='all'\n")
    subprocess.run([str(pg_bin / "pg_ctl"), "-D", str(pgdata), "-l", str(out / "postgres.log"),
                    "-w", "start"], env=env, check=True)
    try:
        pg_sql("CREATE TABLE control_guard_probe(id int PRIMARY KEY, note text)")
        gateway_legacy_test(build / "bin/ariabc_pg_gateway", out, env)
        summary["legacy_gateway"] = "PASS"
        for mode in ("direct", "raft"):
            mode_dir = out / mode
            mode_dir.mkdir()
            pg_sql("TRUNCATE control_guard_probe")
            client_port, raft_port = free_port(), free_port()
            command = [str(build / "bin/ariabc_pg_server"), "--id", "1", "--raftEndpoint",
                       f"127.0.0.1:{raft_port}", "--clientPort", str(client_port),
                       "--raftMembers", f"1=127.0.0.1:{raft_port}", "--dbName", "postgres",
                       "--dbHost", "127.0.0.1", "--dbPort", str(pgport), "--dbType", "2",
                       "--dbConnPoolSize", "2", "--safedb", "0", "--txTagVerify", "off",
                       "--raft-storage-mode", "in_memory", "--raft-storage-dir", "",
                       "--bypassRaft", "1" if mode == "direct" else "0"]
            (mode_dir / "command.json").write_text(json.dumps(command, indent=2) + "\n")
            with (mode_dir / "server.log").open("w") as log:
                proc = subprocess.Popen(command, cwd=mode_dir, env=env, stdout=log, stderr=subprocess.STDOUT)
                try:
                    deadline = time.monotonic() + 30
                    while True:
                        require(proc.poll() is None, f"{mode} server exited during startup")
                        try:
                            with socket.create_connection(("127.0.0.1", client_port), timeout=1) as sock:
                                if rpc(sock, single_frame("__ARIABC_CTRL_IS_LEADER", "leader")) == (0, "1"):
                                    break
                        except (OSError, EOFError):
                            pass
                        require(time.monotonic() < deadline, f"{mode} leader startup timed out")
                        time.sleep(0.1)

                    checks = []
                    with socket.create_connection(("127.0.0.1", client_port), timeout=8) as sock:
                        def call(sql, rid="ctrl-test", terminal=False):
                            return rpc(sock, single_frame(sql, rid, terminal))

                        initial = call("__ARIABC_CTRL_GET_COMMIT_INDEX")
                        require(initial[0] == 0 and initial[1].isdigit(), "legacy index response changed")
                        require(call("__ARIABC_CTRL_GET_USER_ABORTS") == (0, "user_aborts=0"), "abort RPC failed")
                        stats = call("__ARIABC_CTRL_GET_COMMIT_INDEX", "ctrl-stats-v1-test")
                        require(stats == (0, initial[1] + " user_aborts=0"), "opt-in stats failed")
                        malformed = ["__ARIABC_CTRL_FUTURE", "__ARIABC_CTRL_GET_USER_ABORTS ",
                                     " __ARIABC_CTRL_GET_USER_ABORTS;", "__ariabc_ctrl_get_user_aborts",
                                     "/* nested /* x */ x */ __ARIABC_CTRL_GET_USER_ABORTS",
                                     "s 00000001 __ARIABC_CTRL_GET_USER_ABORTS",
                                     "s __ARIABC_CTRL_GET_USER_ABORTS", "; __ARIABC_CTRL_GET_USER_ABORTS"]
                        for sql in malformed:
                            for terminal in (False, True):
                                response = call(sql, terminal=terminal)
                                require(response == (2, "UNKNOWN_OR_MALFORMED_CONTROL_COMMAND"),
                                        f"{mode}: malformed command admitted: {sql!r}: {response}")
                                checks.append({"sql": sql, "terminal": terminal, "response": response})
                        for tagged in (False, True):
                            for sql in ("__ARIABC_CTRL_GET_USER_ABORTS", "WAIT_RESULT_ID absent 1"):
                                for first in (False, True):
                                    items = [("valid-peer", "INSERT INTO control_guard_probe VALUES (99, 'bad')"),
                                             ("batch-control", sql)]
                                    if first:
                                        items.reverse()
                                    response = rpc(sock, batch_frame(items, tagged))
                                    require(response == (2, f"CONTROL_COMMAND_IN_WORKLOAD ord={0 if first else 1}"),
                                            f"{mode}: batch admitted: {response}")
                                    checks.append({"batch": items, "tagged": tagged, "response": response})
                        require(call("__ARIABC_CTRL_GET_COMMIT_INDEX") == initial, "rejection advanced index")
                        require(pg_sql("SELECT count(*) FROM control_guard_probe") == "0", "rejected peer executed")
                        response = call("INSERT INTO control_guard_probe VALUES (1, '__ARIABC_CTRL_GET_USER_ABORTS')",
                                        "valid-single")
                        require(response[0] == 0, f"valid SQL rejected: {response}")
                        require(call("WAIT_RESULT_ID valid-single 5000")[0] == 0, "valid single failed")
                        response = rpc(sock, batch_frame([
                            ("valid-a", "INSERT INTO control_guard_probe VALUES (2, 'a')"),
                            ("valid-b", "INSERT INTO control_guard_probe VALUES (3, 'b')")]))
                        require(response[0] == 0, "valid batch rejected")
                        for rid in ("valid-a", "valid-b"):
                            require(call(f"WAIT_RESULT_ID {rid} 5000")[0] == 0, "valid batch failed")
                        require(pg_sql("SELECT count(*) FROM control_guard_probe") == "3", "valid SQL lost")
                    (mode_dir / "rpc-checks.json").write_text(json.dumps(checks, indent=2) + "\n")
                    workload = mode_dir / "gateway.sql"
                    workload.write_text("INSERT INTO control_guard_probe VALUES (4, 'gateway');\n")
                    run = subprocess.run([str(build / "bin/ariabc_pg_gateway"), "--queryFrom", str(workload),
                                          "--nodes", f"127.0.0.1:{client_port}", "--txSign", "0", "--dbType", "2"],
                                         env=env, text=True, stdout=subprocess.PIPE,
                                         stderr=subprocess.STDOUT, timeout=30)
                    (mode_dir / "gateway.log").write_text(run.stdout)
                    require(run.returncode == 0, f"{mode} gateway failed")
                    for expected in ("user_abort_counter_supported=1", "permanent_failures=0", "divergence_count=0"):
                        require(expected in run.stdout, f"{mode} gateway missing {expected}")
                    require(pg_sql("SELECT count(*) FROM control_guard_probe") == "4", "gateway SQL lost")
                    summary[mode] = {"status": "PASS", "rejected_requests": len(checks), "valid_rows": 4}
                finally:
                    stop_process(proc)
            require("SAFE_TASK_FAILED" not in (mode_dir / "server.log").read_text(), f"{mode} failed DB task")
        pg_log = (out / "postgres.log").read_text()
        require("syntax error" not in pg_log, "control command reached PostgreSQL")
        summary["postgres_syntax_errors"] = 0
    finally:
        subprocess.run([str(pg_bin / "pg_ctl"), "-D", str(pgdata), "-m", "fast", "-w", "stop"],
                       env=env, check=True)
    (out / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps(summary, indent=2), flush=True)


if __name__ == "__main__":
    main()
