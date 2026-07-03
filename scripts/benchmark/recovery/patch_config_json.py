#!/usr/bin/env python3
"""Patch build_profile and configure_arguments into an existing config.json."""
import json
import sys
from pathlib import Path

path, build_profile, configure_args = sys.argv[1], sys.argv[2], sys.argv[3]
data = json.loads(Path(path).read_text())
data["build_profile"] = build_profile
data["configure_arguments"] = configure_args
Path(path).write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")
