"""Owned benchmark processes and strict, artifact-backed cluster acceptance."""
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import time
import uuid
import re

from benchmark_contract import source_hashes, VERSION
from summarize_raft_profile import collect_csv_row, read_env_file


def terminate_process(proc):
    if proc.poll() is not None:
        return
    os.killpg(proc.pid, signal.SIGTERM)
    try:
        proc.wait(timeout=60)
    except subprocess.TimeoutExpired:
        os.killpg(proc.pid, signal.SIGKILL)
        proc.wait()


def captured_command(command, shell=False, timeout=180):
    proc = subprocess.Popen(command, shell=shell, stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, text=True,
                            start_new_session=True)
    try:
        output, _ = proc.communicate(timeout=timeout)
        return proc.returncode, output
    except BaseException:
        # communicate while the shell handles TERM: do not deadlock its cleanup
        # by leaving a full stdout pipe unread.
        if proc.poll() is None:
            os.killpg(proc.pid, signal.SIGTERM)
            try:
                proc.communicate(timeout=60)
            except subprocess.TimeoutExpired:
                os.killpg(proc.pid, signal.SIGKILL)
                proc.communicate()
        raise


def campaign_contract(repo, out, args, workloads, workers, modes):
    source_hash = hashlib.sha256()
    for directory in (repo / "src", repo / "ariabc_pg"):
        for path in sorted(directory.rglob("*")):
            if (path.is_file() and "build" not in path.parts and
                    path.suffix in (".c", ".h", ".cxx", ".cpp", ".hxx", ".dat")):
                source_hash.update(str(path.relative_to(repo)).encode())
                source_hash.update(path.read_bytes())
    contract = {
        "version": VERSION, "shared_buffers": args.db_shared_buffers,
        "harness_sha256": source_hashes(repo),
        "trials": args.trials, "cold_runs": args.cold_runs, "order_seed": args.order_seed,
        "tpcc": {k: v for k, v in vars(args).items() if k.startswith("tpcc_") or k == "warehouses"},
        "source_sha256": source_hash.hexdigest(),
        "workers": workers, "modes": modes,
        "workloads": {wl: hashlib.sha256((repo / wl).read_bytes()).hexdigest()
                      for wl in workloads},
    }
    path = out / "campaign.json"
    if path.exists():
        if json.loads(path.read_text()) != contract:
            raise RuntimeError(f"Campaign settings changed; use a fresh output directory: {out}")
    elif (out / "summary.csv").exists():
        raise RuntimeError(f"Cannot verify legacy results' settings; use a fresh output directory: {out}")
    else:
        path.write_text(json.dumps(contract, indent=2) + "\n")


def write_campaign_report(out):
    import csv
    from collections import Counter
    summary_path = out / "summary.csv"
    with summary_path.open() as f:
        reader = csv.reader(f)
        first_row = next(reader, [])
    if first_row and first_row[0] == "mode":
        rows = list(csv.DictReader(summary_path.open()))
    else:
        fields = [
            "mode", "workload", "server_workers", "bcdb_workers", "pool_size",
            "total_queries", "wall_time_ms", "tps", "merkle_pass",
            "divergence_count", "permanent_failures", "run_id", "shared_buffers", "source_fingerprint",
        ]
        rows = list(csv.DictReader(summary_path.open(), fieldnames=fields))
    contract = json.loads((out / "campaign.json").read_text())
    counts = Counter(row["mode"] for row in rows)
    lines = ["# YCSB campaign results", "",
             f"Shared buffers: **{contract['shared_buffers']}**. "
             f"Recorded cases: **{len(rows)}**.", "",
             "| Mode | Accepted cases | Minimum TPS | Maximum TPS |",
             "|---|---:|---:|---:|"]
    for mode, count in sorted(counts.items()):
        selected = [r for r in rows if r["mode"] == mode]
        if any(r["merkle_pass"] != "1" or r["divergence_count"] != "0" or
               r["permanent_failures"] != "0" or float(r["tps"]) <= 0 for r in selected):
            raise RuntimeError("Refusing to publish a report containing failed cases")
        values = [float(r["tps"]) for r in selected]
        lines.append(f"| {mode} | {count} | {min(values):.2f} | {max(values):.2f} |")
    lines += ["", "TPS spans different workloads and worker counts; it is not a matched speedup comparison.",
              "See [summary.csv](summary.csv), [campaign.json](campaign.json), and the per-attempt logs for settings and provenance.", ""]
    (out / "REPORT.md").write_text("\n".join(lines))


def accept_cluster_artifact(root, expected_queries):
    metrics = collect_csv_row(root)
    summary = read_env_file(root / "run_summary.env")
    if metrics["merkle_pass"] != 1:
        raise RuntimeError("missing successful post-marker Merkle verification")
    for key in ("permanent_failures", "divergence_count"):
        if str(metrics[key]) != "0":
            raise RuntimeError(f"{key}={metrics[key] or 'unknown'}")
    if summary.get("all3_audit_valid") != "yes":
        raise RuntimeError("all-three audit did not finish successfully")
    if int(summary.get("async_all3_verified_count", -1)) != expected_queries:
        raise RuntimeError("all-three audit count does not match workload")
    if float(metrics["tps"] or 0) <= 0:
        raise RuntimeError("missing completed throughput")
    # Matching error receipts must not turn an unsuccessful SQL workload into a
    # passing throughput point, even when error payloads are canonicalized.
    gateway = (root / "gateway_test.log").read_text(errors="replace")
    profiles = [line for line in gateway.splitlines() if line.startswith("PROFILE_GATEWAY ")]
    if not profiles:
        raise RuntimeError("Missing terminal gateway profile")
    if profiles:
        for key in ("deterministic_error_count", "nonterminal_failure_count"):
            value = re.search(r"\b" + key + r"=(\d+)", profiles[-1])
            if not value or int(value[1]) != 0:
                raise RuntimeError(f"Missing or nonzero {key}")
    for log in root.glob("postgres_node*.log"):
        if re.search(r"ERROR:.*(?:merkle_|Merkle)", log.read_text(errors="replace")):
            raise RuntimeError(f"Merkle execution error in {log.name}")
    return metrics


def run_cluster_case(args, repo, out, workload, workers, run_index, restart):
    run_id = time.strftime("cluster4_%Y%m%d_%H%M%S_") + uuid.uuid4().hex[:8]
    attempts = out / "attempts"
    attempts.mkdir(exist_ok=True)
    root = repo / "scripts/bench_full_results" / run_id
    log = attempts / (run_id + ".log")
    metadata = {"run_id": run_id, "workload": workload, "workers": workers,
                "shared_buffers": getattr(args, "db_shared_buffers", "32MB"), "status": "running",
                "artifact_dir": str(root)}
    meta_path = attempts / (run_id + ".json")
    env = os.environ.copy()
    env.update({
        "BENCH_COLD_CACHE": "1" if getattr(args, "cold_runs", False) else "0",
        "GATEWAY_HOST": getattr(args, "gateway_host", "10.129.27.111"),
        "GATEWAY_USER": getattr(args, "gateway_user", "neel"),
        "GATEWAY_REPO": getattr(args, "gateway_repo", "/home/neel/ARIABC/AriaBC"),
        "LOCAL_INSTALL_DIR": os.environ.get("LOCAL_INSTALL_DIR", "/home/neel/ARIABC/install"),
        "PATH": f"{Path.home()}/bin:{Path.home()}/.local/bin:{os.environ.get('PATH', '')}",
        "CLUSTER_RUN_ID": run_id, "FORCE_BUILD": os.environ.get("FORCE_BUILD", "0"), "SKIP_RDKAFKA_SETUP": "1",
        "SKIP_SYNC": os.environ.get("SKIP_SYNC", "0" if run_index == 0 else "1"),
        "SKIP_BUILD": os.environ.get("SKIP_BUILD", "0" if run_index == 0 else "1"),
        "KAFKA_FAST_RESET": "1", "DUMP_VERIFY_CSV": "0",
        "ARIABC_PREFERRED_LEADER_ID": "1", "ARIABC_RAFT_DURABLE_ASYNC_FLUSH": "1",
        "ARIABC_RAFT_STREAM_GAP": "512", "ARIABC_KAFKA_ASYNC_RESULT_PUBLISHER": "1",
        "ARIABC_GATEWAY_DISPATCH_WORKERS": os.environ.get("ARIABC_GATEWAY_DISPATCH_WORKERS", "8"),
        "ARIABC_KAFKA_RESULT_BATCH_MAX_DELAY_US": os.environ.get("ARIABC_KAFKA_RESULT_BATCH_MAX_DELAY_US", "3000"),
        "ARIABC_KAFKA_RESULT_TARGET_BATCH_RECORDS": os.environ.get("ARIABC_KAFKA_RESULT_TARGET_BATCH_RECORDS", "128"),
        "ARIABC_KAFKA_RESULT_BATCH_TARGET_RECORDS": os.environ.get("ARIABC_KAFKA_RESULT_BATCH_TARGET_RECORDS", "128"),
        "ARIABC_KAFKA_ASYNC_RESULT_BATCH_RECORDS": "256",
        "ARIABC_FULL_RESULT_REPLICA_LIMIT": "-1",
        "ARIABC_KAFKA_PAYLOAD_FORMAT": os.environ.get("ARIABC_KAFKA_PAYLOAD_FORMAT", "text"),
        "BCDB_DET_QUEUE_HIGH_WM": "65536", "BCDB_DET_QUEUE_LOW_WM": "32768",
        "GATEWAY_STALL_WATCHDOG": "1", "GATEWAY_STALL_POLL_SECONDS": "5",
        "GATEWAY_STALL_MAX_CYCLES": "12",
    })
    # Versioned workload files may live in .bench_tmp, excluded from workspace
    # synchronization. Upload the exact bytes independently of --skip-sync.
    digest = hashlib.sha256((repo / workload).read_bytes()).hexdigest()
    remote_workload = "/tmp/ariabc_cluster_" + digest + ".sql"
    subprocess.run(["scp", "-o", "BatchMode=yes", str(repo / workload),
                    f"{args.gateway_user}@{args.gateway_host}:{remote_workload}"], check=True, timeout=60)
    for name in ("run_4node_raft_cluster.sh", "benchmark_cache.py", "source_fingerprint.py"):
        subprocess.run(["scp", "-o", "BatchMode=yes", str(repo / "scripts/distributed" / name),
                        f"{args.gateway_user}@{args.gateway_host}:{args.gateway_repo}/scripts/distributed/{name}"], check=True, timeout=60)
    metadata["workload_sha256"] = digest
    gw_workers = str(os.environ.get("DET_CLIENT_WORKERS", "96"))
    tx_sign = getattr(args, "tx_sign", None) or os.environ.get("TX_SIGN") or os.environ.get("ARIABC_TX_SIGN") or "blake3"
    env["TX_SIGN"] = str(tx_sign)
    env["ARIABC_TX_SIGN"] = str(tx_sign)
    metadata["tx_sign"] = str(tx_sign)
    command = [str(repo / "scripts/distributed/run_4node_raft_cluster.sh"),
               "--workload", remote_workload, "--db-shared-buffers", getattr(args, "db_shared_buffers", "32MB"),
               "--ordering-mode", "raft-kafka", "--enable-merkle-index", "1",
               "--tx-sign", str(tx_sign),
               "--raft-apply-ledger-mode", "off", "--threads", gw_workers,
               "--det-client-workers", gw_workers, "--det-client-inflight", "16",
               "--server-exec-workers", str(workers), "--server-pg-connections", str(workers),
               "--pool-size", str(workers), "--bcdb-workers", str(workers),
               "--bcdb-init-block-size", str(workers), "--bcdb-decouple-workers", "1",
               "--conn-fanout", "1", "--raft-ordered-fanout", "1",
               "--raft-ordering-policy", "leader-assigned", "--raft-ordered-batch-append", "1",
               "--raft-ordered-batch-target-entries", "64", "--raft-ordered-batch-linger-us", "1000",
               "--raft-ordered-coalesce-log", "1", "--kafka-completion-mode", "majority_async_all3",
               "--det-window", "65536"]
    if env["SKIP_SYNC"] == "1":
        command.append("--skip-sync")
    if env["SKIP_BUILD"] == "1":
        command.append("--skip-build")
    if not restart:
        command.append("--skip-pg-restart")
    metadata["command"] = command
    meta_path.write_text(json.dumps(metadata, indent=2) + "\n")
    print(f"  Cluster run {run_id}; live log: {log}", flush=True)
    try:
        with log.open("w") as stream:
            proc = subprocess.Popen(command, env=env, stdout=stream, stderr=subprocess.STDOUT,
                                    start_new_session=True)
            try:
                metadata["exit_code"] = proc.wait(timeout=1800 if run_index == 0 else 600)
            except BaseException:
                terminate_process(proc)
                metadata["exit_code"] = proc.returncode
                raise
        if metadata["exit_code"] != 0:
            raise RuntimeError(f"cluster runner exited {metadata['exit_code']}")
        count = sum(bool(line.strip()) and not line.lstrip().startswith("--")
                    for line in (repo / workload).read_text().splitlines())
        metrics = accept_cluster_artifact(root, count)
        provenance = read_env_file(root / "run_meta.env")
        if provenance.get("db_shared_buffers") != args.db_shared_buffers:
            raise RuntimeError("run artifact does not confirm requested shared_buffers")
        majority_tps = float(metrics["tps"])
        gateway = (root / "gateway_test.log").read_text()
        drains = re.findall(r"^\s*overall wall time including drains \(millisec\) = (\d+)\s*$", gateway, re.M)
        if not drains or int(drains[-1]) <= 0:
            raise RuntimeError("Missing all-three drained wall time")
        if args.cold_runs:
            cache_logs = list(root.glob("cache_node*.log"))
            if len(cache_logs) != 3 or any('"policy": "stopped_pg_fadvise_verified_mincore"' not in p.read_text() for p in cache_logs):
                raise RuntimeError("Missing verified cold-cache preparation on all three replicas")
        all3_wall_ms = int(drains[-1])
        all3_tps = count * 1000.0 / all3_wall_ms
        tps = majority_tps
        metadata.update(majority_tps=majority_tps, all3_tps=all3_tps,
                        timing="majority_visible_completion", all3_wall_ms=all3_wall_ms)
        result = dict(mode="cluster", workload=Path(workload).name, server_workers=workers,
                      bcdb_workers=workers, pool_size=workers, total_queries=count,
                      wall_time_ms=round(count / tps * 1000, 1), tps=tps,
                      merkle_pass=1, divergence_count=0, permanent_failures=0,
                      run_id=run_id, shared_buffers=args.db_shared_buffers,
                      source_fingerprint=provenance.get("source_fingerprint", ""))
        metadata.update(status="passed", result=result)
        return result
    except BaseException as exc:
        metadata.update(status="failed", error=str(exc) or type(exc).__name__)
        raise RuntimeError(f"Cluster case failed: {metadata['error']}; evidence: {log}") from exc
    finally:
        meta_path.write_text(json.dumps(metadata, indent=2) + "\n")
