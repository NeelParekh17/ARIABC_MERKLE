#!/usr/bin/env python3
"""Generate an isolated copy of the cluster runner; never edit the original.

Run on a staged remote checkout. This is preparation only: no SSH/build/start.
Review the emitted diff and run bash -n on the lab before using it.
"""
import argparse
import difflib
from pathlib import Path
import re


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--repo", type=Path, required=True)
    ap.add_argument("--tag", required=True, help="Fresh alphanumeric/underscore experiment tag")
    a = ap.parse_args()
    if not re.fullmatch(r"[A-Za-z0-9_]+", a.tag):
        ap.error("invalid tag")
    root = a.repo.resolve()
    base = "/home/neel/Desktop/recovery_s1024_" + a.tag
    if str(root) != base + "/repo":
        ap.error("stage checkout must be " + base + "/repo")
    src = root / "scripts/distributed/run_4node_raft_cluster.sh"
    old = src.read_text()
    new = old
    # Global literal replacements also redirect diagnostic/cleanup paths.
    replacements = {
        "/home/neel/Desktop/ariabc_cluster": base + "/repo",
        "/home/neel/Desktop/ariabc_install": base + "/install",
        "/home/neel/Desktop/ariabc_pg_build_u22": base + "/u22_build",
        'BUILD_DIR="$REMOTE_DATA_ROOT/build/ariabc_pg_build_u22"': 'BUILD_DIR="' + base + '/u22_build_work"',
        'REMOTE_LOG_DIR="$REMOTE_DATA_ROOT/cluster_logs"': 'REMOTE_LOG_DIR="' + base + '/cluster_logs"',
        "/home/neel/ariabc_pg_srv": base + "/ariabc_pg_srv",
        "single_node_pgdata": "recovery_pgdata",
        "/home/neel/Desktop/rdkafka_local": base + "/rdkafka",
    }
    for before, after in replacements.items():
        if before not in new:
            ap.error("runner drift: missing " + before)
        new = new.replace(before, after)
    # The generated file lives one level deeper, but its helper imports must
    # continue to resolve to scripts/distributed/.
    anchor = 'SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"'
    if new.count(anchor) != 1:
        ap.error("runner drift: SCRIPT_DIR")
    new = new.replace(anchor, 'SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"')
    # The original has hardcoded port 9000 in cleanup and final profiling.
    new = new.replace("9000/tcp", "$RAFT_PORT/tcp")
    # pg_ctl remains scoped to the isolated data directory. Never kill all
    # processes owned by neel (would kill the OOM/canonical postgres).
    lines = new.splitlines(keepends=True)
    removed = [line for line in lines if "pkill " in line and not line.lstrip().startswith("#")]
    if len(removed) != 3:
        ap.error("runner drift: expected three broad pkill lines, found " + str(len(removed)))
    new = "".join(line for line in lines if line not in removed)
    # Execution must use already-built artifacts; the orchestrator builds on
    # .247, including an Ubuntu 22 ABI build there for user4.
    guard = '\nif [[ "${BYPASS_DELEGATION:-0}" != 1 || "${SKIP_BUILD:-0}" != 1 || "${SKIP_SYNC:-0}" != 1 ]]; then\n  echo "isolated runner requires BYPASS_DELEGATION=1 SKIP_BUILD=1 SKIP_SYNC=1" >&2\n  exit 2\nfi\n'
    new = new.replace("set -euo pipefail\n", "set -euo pipefail\n" + guard, 1)
    injector_call = '$SCRIPT_DIR/recovery/fault_injector.py'
    if new.count(injector_call) != 1:
        ap.error("runner drift: fault injector call")
    new = new.replace(injector_call, '$SCRIPT_DIR/recovery_s1024/serializable_fault_injector.py')
    # Copies only: connection default and injection transaction are both
    # SERIALIZABLE. Retain the existing whole-transaction retry loop.
    remote_db = (root / "scripts/distributed/recovery/remote_db.py").read_text()
    isolation = "SET default_transaction_isolation = 'read committed'"
    if remote_db.count(isolation) != 1:
        ap.error("remote_db drift: isolation setting")
    remote_db = remote_db.replace(isolation, "SET default_transaction_isolation = 'serializable'")
    injector = (root / "scripts/distributed/recovery/fault_injector.py").read_text()
    if injector.count("psycopg.IsolationLevel.READ_COMMITTED") != 1:
        ap.error("fault injector drift: isolation setting")
    injector = injector.replace("psycopg.IsolationLevel.READ_COMMITTED", "psycopg.IsolationLevel.SERIALIZABLE")
    injector = injector.replace("from .remote_db import NodeConnection", "from serializable_remote_db import NodeConnection")
    injector = injector.replace("from remote_db import NodeConnection", "from serializable_remote_db import NodeConnection")
    injector = injector.replace("from __future__ import annotations\n",
                                "from __future__ import annotations\nimport sys\n"
                                "sys.path.append(" + repr(str(root / "scripts/distributed/recovery")) + ")\n", 1)
    output = root / ("scripts/distributed/recovery_s1024/cluster_runner_" + a.tag + ".sh")
    diff = output.with_suffix(".diff")
    db_output = output.parent / "serializable_remote_db.py"
    injector_output = output.parent / "serializable_fault_injector.py"
    if any(p.exists() for p in (output, diff, db_output, injector_output)):
        ap.error("refusing to overwrite prepared runner/diff")
    with db_output.open("x") as f:
        f.write(remote_db)
    with injector_output.open("x") as f:
        f.write(injector)
    with output.open("x") as f:
        f.write(new)
    with diff.open("x") as f:
        f.writelines(difflib.unified_diff(old.splitlines(True), new.splitlines(True),
                                        fromfile=str(src), tofile=str(output)))
        for original, generated in ((root / "scripts/distributed/recovery/remote_db.py", db_output),
                                    (root / "scripts/distributed/recovery/fault_injector.py", injector_output)):
            f.writelines(difflib.unified_diff(original.read_text().splitlines(True),
                                            generated.read_text().splitlines(True),
                                            fromfile=str(original), tofile=str(generated)))
    print(output)
    print(diff)


if __name__ == "__main__":
    main()
