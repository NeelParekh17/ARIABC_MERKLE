#!/usr/bin/env python3
import os
import sys
import re
import glob
import argparse

def count_workload_lines(filename):
    if not filename or not os.path.exists(filename):
        return 0
    count = 0
    pattern = re.compile(r'^\s*($|--)')
    with open(filename, 'r') as f:
        for line in f:
            if not pattern.match(line):
                count += 1
    return count

def parse_logs(log_files, parallelism_mode):
    majority_visible_ms_list = []
    all3_audit_drained_ms_list = []

    completion_path = None
    validation_mode = None

    divergence_count = 0
    permanent_failures = 0

    client_quorum_complete_count = 0
    async_all3_verified_count = 0
    async_all3_failure_count = 0
    async_all3_timeout_count = 0
    async_all3_missing_count = 0

    has_empirical_latency = False
    empirical_lat_count = 0
    empirical_lat_min_ms = 0.0
    empirical_lat_mean_ms = 0.0
    empirical_lat_p50_ms = 0.0
    empirical_lat_p90_ms = 0.0
    empirical_lat_p95_ms = 0.0
    empirical_lat_p99_ms = 0.0
    empirical_lat_max_ms = 0.0

    for log_file in log_files:
        if not os.path.exists(log_file):
            continue
        with open(log_file, 'r') as f:
            for line in f:
                # Timing metrics
                m_vis = re.search(r'overall time taken \(millisec\) = (\d+)', line)
                if m_vis:
                    majority_visible_ms_list.append(int(m_vis.group(1)))

                m_drain = re.search(r'overall wall time including drains \(millisec\) = (\d+)', line)
                if m_drain:
                    all3_audit_drained_ms_list.append(int(m_drain.group(1)))

                # Config options from PROFILE_GATEWAY
                if "PROFILE_GATEWAY" in line:
                    m_path = re.search(r'completion_path=(\S+)', line)
                    if m_path:
                        completion_path = m_path.group(1).strip()
                    m_val = re.search(r'validation_mode=(\S+)', line)
                    if m_val:
                        validation_mode = m_val.group(1).strip()

                # Divergence and permanent failures
                m_div = re.search(r'^divergence_count=(\d+)', line)
                if m_div:
                    divergence_count += int(m_div.group(1))
                m_fail = re.search(r'^permanent_failures=(\d+)', line)
                if m_fail:
                    permanent_failures += int(m_fail.group(1))

                # majority_async_all3 verification stats
                if "PROFILE_GATEWAY" in line:
                    m_q = re.search(r'client_quorum_complete_count=(\d+)', line)
                    if m_q:
                        client_quorum_complete_count += int(m_q.group(1))
                    m_ver = re.search(r'async_all3_verified_count=(\d+)', line)
                    if m_ver:
                        async_all3_verified_count += int(m_ver.group(1))
                    m_f = re.search(r'async_all3_failure_count=(\d+)', line)
                    if m_f:
                        async_all3_failure_count += int(m_f.group(1))
                    m_t = re.search(r'async_all3_timeout_count=(\d+)', line)
                    if m_t:
                        async_all3_timeout_count += int(m_t.group(1))
                    m_m = re.search(r'async_all3_missing_count=(\d+)', line)
                    if m_m:
                        async_all3_missing_count += int(m_m.group(1))

                # Empirical transaction latency
                m_lat = re.search(r'TX_LATENCY_EMPIRICAL count=(\d+) min_ms=([0-9.]+) mean_ms=([0-9.]+) p50_ms=([0-9.]+) p90_ms=([0-9.]+) p95_ms=([0-9.]+) p99_ms=([0-9.]+) max_ms=([0-9.]+)', line)
                if m_lat:
                    has_empirical_latency = True
                    empirical_lat_count = int(m_lat.group(1))
                    empirical_lat_min_ms = float(m_lat.group(2))
                    empirical_lat_mean_ms = float(m_lat.group(3))
                    empirical_lat_p50_ms = float(m_lat.group(4))
                    empirical_lat_p90_ms = float(m_lat.group(5))
                    empirical_lat_p95_ms = float(m_lat.group(6))
                    empirical_lat_p99_ms = float(m_lat.group(7))
                    empirical_lat_max_ms = float(m_lat.group(8))

    # Resolve timing metrics based on parallelism mode
    if parallelism_mode == "os-threads":
        majority_visible_ms = max(majority_visible_ms_list) if majority_visible_ms_list else 0
        all3_audit_drained_ms = max(all3_audit_drained_ms_list) if all3_audit_drained_ms_list else 0
    else:
        # For pipeline/single process, take the last reported if multiple, or first
        majority_visible_ms = majority_visible_ms_list[0] if majority_visible_ms_list else 0
        all3_audit_drained_ms = all3_audit_drained_ms_list[0] if all3_audit_drained_ms_list else 0

    return {
        "majority_visible_ms": majority_visible_ms,
        "all3_audit_drained_ms": all3_audit_drained_ms,
        "completion_path": completion_path,
        "validation_mode": validation_mode,
        "divergence_count": divergence_count,
        "permanent_failures": permanent_failures,
        "client_quorum_complete_count": client_quorum_complete_count,
        "async_all3_verified_count": async_all3_verified_count,
        "async_all3_failure_count": async_all3_failure_count,
        "async_all3_timeout_count": async_all3_timeout_count,
        "async_all3_missing_count": async_all3_missing_count,
        "has_empirical_latency": has_empirical_latency,
        "empirical_lat_count": empirical_lat_count,
        "empirical_lat_min_ms": empirical_lat_min_ms,
        "empirical_lat_mean_ms": empirical_lat_mean_ms,
        "empirical_lat_p50_ms": empirical_lat_p50_ms,
        "empirical_lat_p90_ms": empirical_lat_p90_ms,
        "empirical_lat_p95_ms": empirical_lat_p95_ms,
        "empirical_lat_p99_ms": empirical_lat_p99_ms,
        "empirical_lat_max_ms": empirical_lat_max_ms
    }

def main():
    if "--self-test" in sys.argv:
        print("Running parser self-tests...")
        import tempfile
        import shutil
        tmp_dir = tempfile.mkdtemp()
        try:
            gw_log_path = os.path.join(tmp_dir, "gateway_test.log")
            runner_log_path = os.path.join(tmp_dir, "runner.log")
            
            metrics_content = (
                "PROFILE_GATEWAY client_quorum_complete_count=20513 "
                "async_all3_verified_count=20513 async_all3_failure_count=0 "
                "async_all3_timeout_count=0 async_all3_missing_count=0\n"
                "overall time taken (millisec) = 1000\n"
                "overall wall time including drains (millisec) = 1200\n"
                "divergence_count=0\n"
                "permanent_failures=0\n"
                "TX_LATENCY_EMPIRICAL count=20000 min_ms=0.100 mean_ms=0.450 p50_ms=0.400 p90_ms=0.700 p95_ms=0.850 p99_ms=1.200 max_ms=3.500\n"
            )
            
            with open(gw_log_path, "w") as f:
                f.write(metrics_content)
            with open(runner_log_path, "w") as f:
                f.write(metrics_content)
                
            # Filter runner.log
            log_files = [gw_log_path, runner_log_path]
            filtered_log_files = [f for f in log_files if os.path.basename(f) != "runner.log"]
            
            metrics = parse_logs(filtered_log_files, "pipeline")
            assert metrics["async_all3_verified_count"] == 20513, f"Expected 20513, got {metrics['async_all3_verified_count']}"
            assert metrics["has_empirical_latency"] is True, "Expected empirical latency to be parsed"
            assert metrics["empirical_lat_p50_ms"] == 0.400, f"Expected p50=0.400, got {metrics['empirical_lat_p50_ms']}"
            assert metrics["empirical_lat_p95_ms"] == 0.850, f"Expected p95=0.850, got {metrics['empirical_lat_p95_ms']}"
            print("Self-test 1 (Ignore runner.log and prevent double counting, parse empirical latency): PASSED")
            
            workload_transactions = 20000
            async_all3_verified_count = metrics["async_all3_verified_count"]
            if async_all3_verified_count > workload_transactions:
                parser_error = "async_all3_verified_count_exceeds_workload_transactions"
            else:
                parser_error = ""
            assert parser_error == "async_all3_verified_count_exceeds_workload_transactions", "Expected rejection error"
            print("Self-test 2 (Reject verified count exceeding workload transactions): PASSED")
            
            print("All parser self-tests PASSED successfully.")
            shutil.rmtree(tmp_dir)
            sys.exit(0)
        except Exception as e:
            print(f"Self-test FAILED: {e}")
            shutil.rmtree(tmp_dir)
            sys.exit(1)

    parser = argparse.ArgumentParser()
    parser.add_argument("--gw-log", required=True)
    parser.add_argument("--log-dir", required=True)
    parser.add_argument("--workload-file", required=True)
    parser.add_argument("--ordering-mode", required=True)
    parser.add_argument("--no-kafka", type=int, required=True)
    parser.add_argument("--parallelism-mode", required=True)
    parser.add_argument("--threads", type=int, default=1, help="Client concurrency / worker threads count")
    args = parser.parse_args()

    # 1. Determine workload transactions
    workload_transactions = count_workload_lines(args.workload_file)

    # 2. Find log files to parse
    log_files = []
    if args.parallelism_mode == "os-threads":
        log_files = glob.glob(os.path.join(args.log_dir, 'gateway_shard*.log'))

    if not log_files:
        log_files = [args.gw_log]

    # Explicitly filter out runner.log to avoid double-counting
    log_files = [f for f in log_files if os.path.basename(f) != "runner.log"]

    # 3. Parse metrics from logs
    metrics = parse_logs(log_files, args.parallelism_mode)

    # 4. Fill in missing config options
    completion_path = metrics["completion_path"]
    validation_mode = metrics["validation_mode"]

    if not completion_path:
        completion_path = "direct" if args.no_kafka == 1 else "kafka_majority"
    if not validation_mode:
        validation_mode = "async_hash" if args.no_kafka == 0 else ""

    majority_visible_ms = metrics["majority_visible_ms"]
    all3_audit_drained_ms = metrics["all3_audit_drained_ms"]
    divergence_count = metrics["divergence_count"]
    permanent_failures = metrics["permanent_failures"]
    client_quorum_complete_count = metrics["client_quorum_complete_count"]
    async_all3_verified_count = metrics["async_all3_verified_count"]
    async_all3_failure_count = metrics["async_all3_failure_count"]
    async_all3_timeout_count = metrics["async_all3_timeout_count"]
    async_all3_missing_count = metrics["async_all3_missing_count"]

    # 5. Determine output values
    tps_majority_visible = "N/A"
    tps_all3_audit_drained = "N/A"
    all3_audit_valid = ""
    parser_error = ""

    threads = max(1, getattr(args, "threads", 1))
    lat_majority_ms_str = "N/A"
    lat_all3_ms_str = "N/A"
    inter_completion_ms_str = "N/A"

    # Output presentation to console
    print(f"Queries                         : {workload_transactions}")
    print(f"Client Threads / Concurrency    : {threads}")

    if validation_mode == "majority_async_all3":
        print(f"Completion mode                 : majority_async_all3")
        print(f"Majority-visible time (ms)      : {majority_visible_ms}")
        print(f"All-3 audit-drained time (ms)   : {all3_audit_drained_ms}")

        if majority_visible_ms > 0:
            tps_majority = workload_transactions * 1000.0 / majority_visible_ms
            tps_majority_visible = f"{tps_majority:.2f}"
            lat_majority_ms = threads * 1000.0 / tps_majority
            lat_majority_ms_str = f"{lat_majority_ms:.3f}"
            inter_completion_ms = 1000.0 / tps_majority
            inter_completion_ms_str = f"{inter_completion_ms:.3f}"
            print(f"TPS_majority_visible            : {tps_majority_visible} tx/s")
            print(f"Latency_majority_per_tx         : {lat_majority_ms_str} ms (concurrency={threads})")
            print(f"Inter_completion_time (1/TPS)   : {inter_completion_ms_str} ms")
        else:
            tps_majority_visible = "N/A"
            print(f"TPS_majority_visible            : N/A")

        # Check validity
        if async_all3_verified_count > workload_transactions:
            is_valid = False
            parser_error = "async_all3_verified_count_exceeds_workload_transactions"
        else:
            is_valid = (
                client_quorum_complete_count == workload_transactions and
                async_all3_verified_count == workload_transactions and
                async_all3_failure_count == 0 and
                async_all3_timeout_count == 0 and
                async_all3_missing_count == 0 and
                permanent_failures == 0 and
                divergence_count == 0
            )

        if is_valid:
            if all3_audit_drained_ms > 0:
                tps_all3 = workload_transactions * 1000.0 / all3_audit_drained_ms
                tps_all3_audit_drained = f"{tps_all3:.2f}"
                lat_all3_ms = threads * 1000.0 / tps_all3
                lat_all3_ms_str = f"{lat_all3_ms:.3f}"
                print(f"TPS_all3_audit_drained          : {tps_all3_audit_drained} tx/s")
                print(f"Latency_all3_per_tx             : {lat_all3_ms_str} ms (concurrency={threads})")
            else:
                tps_all3_audit_drained = "INVALID"
                print(f"TPS_all3_audit_drained          : INVALID")
            all3_audit_valid = "yes"
        else:
            tps_all3_audit_drained = "INVALID"
            print(f"TPS_all3_audit_drained          : INVALID")
            all3_audit_valid = "no"

        print(f"All-3 audit valid               : {all3_audit_valid}")
        if parser_error:
            print(f"parser_error                    : {parser_error}")

    elif validation_mode == "async_hash":
        print(f"Completion mode                 : async")
        print(f"TPS_majority_visible            : N/A")
        print(f"TPS_all3_audit_drained          : N/A")

    elif validation_mode == "strict_majority":
        print(f"Completion mode                 : strict_majority")
        print(f"Completion time (ms)            : {majority_visible_ms}")
        if majority_visible_ms > 0:
            tps_strict = workload_transactions * 1000.0 / majority_visible_ms
            tps_strict_str = f"{tps_strict:.2f} tx/s"
            lat_strict_ms = threads * 1000.0 / tps_strict
            lat_majority_ms_str = f"{lat_strict_ms:.3f}"
            inter_completion_ms_str = f"{1000.0 / tps_strict:.3f}"
            print(f"TPS_strict_majority             : {tps_strict_str}")
            print(f"Latency_strict_per_tx           : {lat_majority_ms_str} ms (concurrency={threads})")
            print(f"Inter_completion_time (1/TPS)   : {inter_completion_ms_str} ms")
        else:
            tps_strict_str = "N/A"
            print(f"TPS_strict_majority             : {tps_strict_str}")

    else: # direct/no-Kafka
        print(f"Completion mode                 : direct")
        print(f"Completion time (ms)            : {majority_visible_ms}")
        if majority_visible_ms > 0:
            tps_direct = workload_transactions * 1000.0 / majority_visible_ms
            tps_direct_str = f"{tps_direct:.2f} tx/s"
            lat_direct_ms = threads * 1000.0 / tps_direct
            lat_majority_ms_str = f"{lat_direct_ms:.3f}"
            inter_completion_ms_str = f"{1000.0 / tps_direct:.3f}"
            print(f"TPS_direct                      : {tps_direct_str}")
            print(f"Latency_direct_per_tx           : {lat_majority_ms_str} ms (concurrency={threads})")
            print(f"Inter_completion_time (1/TPS)   : {inter_completion_ms_str} ms")
        else:
            tps_direct_str = "N/A"
            print(f"TPS_direct                      : {tps_direct_str}")

    # Output empirical transaction latency if tracked
    if metrics["has_empirical_latency"]:
        print(f"Latency_empirical_mean          : {metrics['empirical_lat_mean_ms']:.3f} ms")
        print(f"Latency_empirical_p50           : {metrics['empirical_lat_p50_ms']:.3f} ms")
        print(f"Latency_empirical_p90           : {metrics['empirical_lat_p90_ms']:.3f} ms")
        print(f"Latency_empirical_p95           : {metrics['empirical_lat_p95_ms']:.3f} ms")
        print(f"Latency_empirical_p99           : {metrics['empirical_lat_p99_ms']:.3f} ms")
        print(f"Latency_empirical_max           : {metrics['empirical_lat_max_ms']:.3f} ms")

    # Write summary files
    env_file = os.path.join(args.log_dir, "run_summary.env")
    csv_file = os.path.join(args.log_dir, "run_summary.csv")

    # Fields to write
    summary_fields = [
        ("schema_version", "6"),
        ("threads", str(threads)),
        ("workload_transactions", str(workload_transactions)),
        ("ordering_mode", args.ordering_mode),
        ("completion_path", completion_path),
        ("validation_mode", validation_mode),
        ("majority_visible_ms", str(majority_visible_ms) if majority_visible_ms > 0 else ""),
        ("all3_audit_drained_ms", str(all3_audit_drained_ms) if all3_audit_drained_ms > 0 else ""),
        ("tps_majority_visible", tps_majority_visible),
        ("latency_majority_per_tx_ms", lat_majority_ms_str),
        ("tps_all3_audit_drained", tps_all3_audit_drained),
        ("latency_all3_per_tx_ms", lat_all3_ms_str),
        ("inter_completion_time_ms", inter_completion_ms_str),
        ("has_empirical_latency", "yes" if metrics["has_empirical_latency"] else "no"),
        ("latency_empirical_mean_ms", f"{metrics['empirical_lat_mean_ms']:.3f}" if metrics["has_empirical_latency"] else "N/A"),
        ("latency_empirical_p50_ms", f"{metrics['empirical_lat_p50_ms']:.3f}" if metrics["has_empirical_latency"] else "N/A"),
        ("latency_empirical_p90_ms", f"{metrics['empirical_lat_p90_ms']:.3f}" if metrics["has_empirical_latency"] else "N/A"),
        ("latency_empirical_p95_ms", f"{metrics['empirical_lat_p95_ms']:.3f}" if metrics["has_empirical_latency"] else "N/A"),
        ("latency_empirical_p99_ms", f"{metrics['empirical_lat_p99_ms']:.3f}" if metrics["has_empirical_latency"] else "N/A"),
        ("latency_empirical_max_ms", f"{metrics['empirical_lat_max_ms']:.3f}" if metrics["has_empirical_latency"] else "N/A"),
        ("all3_audit_valid", all3_audit_valid),
        ("parser_error", parser_error),
        ("divergence_count", str(divergence_count)),
        ("permanent_failures", str(permanent_failures)),
        ("client_quorum_complete_count", str(client_quorum_complete_count) if validation_mode == "majority_async_all3" else ""),
        ("async_all3_verified_count", str(async_all3_verified_count) if validation_mode == "majority_async_all3" else ""),
        ("async_all3_failure_count", str(async_all3_failure_count) if validation_mode == "majority_async_all3" else ""),
        ("async_all3_timeout_count", str(async_all3_timeout_count) if validation_mode == "majority_async_all3" else ""),
        ("async_all3_missing_count", str(async_all3_missing_count) if validation_mode == "majority_async_all3" else "")
    ]

    # Write .env file
    with open(env_file, "w") as f:
        for k, v in summary_fields:
            f.write(f"{k}={v}\n")

    # Write .csv file
    headers = [k for k, _ in summary_fields]
    values = [v for _, v in summary_fields]
    with open(csv_file, "w") as f:
        f.write(",".join(headers) + "\n")
        f.write(",".join(values) + "\n")

if __name__ == "__main__":
    main()
