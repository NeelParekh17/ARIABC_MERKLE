"""Fail-closed acceptance of single-node gateway completion summaries."""
import re


def count_workload_queries(path):
    # Match ariabc_pg::is_skippable_sql_line exactly (one query per line).
    return sum(bool(line.strip()) and not line.lstrip().startswith(("--", "/*", "\\"))
               for line in path.read_text().splitlines())


def parse_gateway_result(output, expected_queries, returncode=0, mode=None):
    if returncode:
        raise ValueError(f"Gateway exited with status {returncode}")

    def final_int(name):
        values = re.findall(rf"\b{re.escape(name)}=(\d+)\b", output)
        if not values:
            raise ValueError(f"Missing gateway counter: {name}")
        return int(values[-1])

    loaded = re.findall(r"^loaded (\d+) queries\s*$", output, re.M)
    if not loaded or int(loaded[-1]) != expected_queries:
        raise ValueError("Loaded query count does not match workload file")
    counters = {name: final_int(name) for name in (
        "divergence_count", "permanent_failures", "success_count",
        "deterministic_error_count", "nonterminal_failure_count",
        "client_quorum_complete_count", "duplicate_key_errors")}
    for name in ("divergence_count", "permanent_failures", "deterministic_error_count",
                 "nonterminal_failure_count", "duplicate_key_errors"):
        if counters[name] != 0:
            raise ValueError(f"Gateway reported {name}={counters[name]}")
    if mode is None:
        if any(counters[name] != expected_queries for name in
               ("success_count", "client_quorum_complete_count")):
            raise ValueError("Gateway did not successfully complete every query")
        completion_evidence = "quorum_success_counters"
    else:
        # The current gateway counts success_count/client_quorum_complete_count
        # only on Kafka quorum paths. Direct single-node runs legitimately emit
        # zeros there. Validate the actual direct-path completion evidence.
        profiles = re.findall(r"^PROFILE_GATEWAY .*$", output, re.M)
        if not profiles:
            raise ValueError("Missing final gateway profile")
        profile = dict(re.findall(r"(\w+)=([^\s]+)", profiles[-1]))
        if profile.get("completion_path") != "direct" or profile.get("submit_mode") != "event":
            raise ValueError("Expected direct event-mode gateway profile")
        if profile.get("not_accepted") != "0":
            raise ValueError("Gateway had rejected requests/retries")
        if profile.get("direct_completion_protocol") != "2":
            raise ValueError("Gateway lacks the all-request terminal completion protocol")
        if profile.get("direct_terminal_success_count") != str(expected_queries):
            raise ValueError("Gateway did not verify every successful terminal result")
        if mode == "pg":
            completion_evidence = "direct_all_request_terminal_results_v2"
        elif mode in ("bcdb_det", "bcdb_merkle"):
            progress = re.findall(r"^PROGRESS_GATEWAY_DET .*\bfinal=1\s*$", output, re.M)
            if not progress:
                raise ValueError("Missing final deterministic completion progress")
            values = dict(re.findall(r"(\w+)=(\S+)", progress[-1]))
            if any(values.get(k) != str(expected_queries) for k in ("total", "sent", "accepted", "completed")):
                raise ValueError("Deterministic gateway did not complete every query")
            if any(values.get(k) != "0" for k in ("pipeline_outstanding", "majority_inflight", "pending_accept")):
                raise ValueError("Deterministic gateway still has outstanding requests")
            completion_evidence = "direct_all_request_terminal_results_v2"
        else:
            raise ValueError(f"Unsupported single-node mode: {mode}")
    times = re.findall(r"^\s*overall time taken \(millisec\) = (\d+)\s*$", output, re.M)
    drains = re.findall(r"^\s*overall wall time including drains \(millisec\) = (\d+)\s*$",
                        output, re.M)
    if not times or not drains or min(int(times[-1]), int(drains[-1])) <= 0:
        raise ValueError("Missing positive final gateway elapsed time")
    completed = re.findall(r"\bcompleted_tps=([0-9.]+)", output)
    # Direct protocol v2 includes all terminal waits in the reported interval.
    elapsed = int(times[-1])
    return dict(counters, total_queries=expected_queries, validated_completed_queries=expected_queries,
                completion_evidence=completion_evidence, wall_time_ms=elapsed,
                wall_including_drains_ms=int(drains[-1]),
                tps=expected_queries * 1000.0 / elapsed,
                completed_tps=float(completed[-1]) if completed else 0.0)
