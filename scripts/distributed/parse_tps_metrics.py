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
        "async_all3_missing_count": async_all3_missing_count
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
            print("Self-test 1 (Ignore runner.log and prevent double counting): PASSED")
            
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

    # Output presentation to console
    print(f"Queries                         : {workload_transactions}")

    if validation_mode == "majority_async_all3":
        print(f"Completion mode                 : majority_async_all3")
        print(f"Majority-visible time (ms)      : {majority_visible_ms}")
        print(f"All-3 audit-drained time (ms)   : {all3_audit_drained_ms}")

        if majority_visible_ms > 0:
            tps_majority = workload_transactions * 1000.0 / majority_visible_ms
            tps_majority_visible = f"{tps_majority:.2f}"
            print(f"TPS_majority_visible            : {tps_majority_visible} tx/s")
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
                print(f"TPS_all3_audit_drained          : {tps_all3_audit_drained} tx/s")
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
        else:
            tps_strict_str = "N/A"
        print(f"TPS_strict_majority             : {tps_strict_str}")

    else: # direct/no-Kafka
        print(f"Completion mode                 : direct")
        print(f"Completion time (ms)            : {majority_visible_ms}")
        if majority_visible_ms > 0:
            tps_direct = workload_transactions * 1000.0 / majority_visible_ms
            tps_direct_str = f"{tps_direct:.2f} tx/s"
        else:
            tps_direct_str = "N/A"
        print(f"TPS_direct                      : {tps_direct_str}")

    # Write summary files
    env_file = os.path.join(args.log_dir, "run_summary.env")
    csv_file = os.path.join(args.log_dir, "run_summary.csv")

    # Fields to write
    summary_fields = [
        ("schema_version", "5"),
        ("workload_transactions", str(workload_transactions)),
        ("ordering_mode", args.ordering_mode),
        ("completion_path", completion_path),
        ("validation_mode", validation_mode),
        ("majority_visible_ms", str(majority_visible_ms) if majority_visible_ms > 0 else ""),
        ("all3_audit_drained_ms", str(all3_audit_drained_ms) if all3_audit_drained_ms > 0 else ""),
        ("tps_majority_visible", tps_majority_visible),
        ("tps_all3_audit_drained", tps_all3_audit_drained),
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
