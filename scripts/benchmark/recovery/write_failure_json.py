#!/usr/bin/env python3
"""Write a compact failure.json for a remote benchmark run."""
import json
import sys
from pathlib import Path

path, run_id, profile, artifact_mode, exit_code, failure_reason, keep_logs = sys.argv[1:]
Path(path).write_text(
    json.dumps(
        {
            "artifact_mode": artifact_mode,
            "exit_code": int(exit_code),
            "failure_reason": failure_reason,
            "keep_failure_logs": int(keep_logs),
            "profile": profile,
            "run_id": run_id,
        },
        indent=2,
        sort_keys=True,
    )
    + "\n"
)
