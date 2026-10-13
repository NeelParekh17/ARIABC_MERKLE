#!/usr/bin/env bash
# Open both source-backed explorers from any working directory.
set -euo pipefail
repo_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
exec python3 "$repo_dir/scripts/open_architecture_explorer.py" --page both "$@"
