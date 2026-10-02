#!/usr/bin/env python3
"""Render the primary OOM README from the curated v2 evidence and preserved text."""
import argparse
import csv
import json
from pathlib import Path

from publish_v2 import CAMPAIGN, COMBOS, MODES, WORKERS, require


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path("Final_Results/OOM_100M"))
    args = parser.parse_args()
    root = args.root
    archive = root / "runs" / CAMPAIGN
    with (root / "summary.csv").open() as handle:
        rows = [r for r in csv.DictReader(handle) if r["campaign"] == CAMPAIGN]
    require(len(rows) == 84, "Expected exactly 84 v2 rows")
    data = {(r["workload"], str(float(r["skew"])), int(r["workers"]), r["mode"]): r for r in rows}
    verification = json.loads((archive / "baseline_evidence/verification.json").read_text())
    golden = json.loads((archive / "baseline_evidence/golden_manifest.json").read_text())
    plain = json.loads((archive / "baseline_evidence/plain_manifest.json").read_text())
    freeze = json.loads((archive / "provenance/freeze.json").read_text())
    hashes = json.loads((archive / "provenance/published_workloads.json").read_text())
    case = next((archive / "campaign").glob("run_*/a_s0.0_bcdb_merkle_w1_t1/setup.json"))
    executables = json.loads(case.read_text())["provenance"]["executables"]
    ratios = [float(data[(wl, skew, w, "bcdb_merkle")]["tps"]) /
              float(data[(wl, skew, w, "bcdb_det")]["tps"]) for wl, skew in COMBOS for w in WORKERS]
    prefix = f"""# YCSB on a 100M-row database larger than memory (OOM)

## Primary result: fresh v2 campaign (2026-10-01)

**84/84 accepted cases:** pg SERIALIZABLE with retry jitter, det, and synchronous
det + Merkle, each measured at workers 1/4/8/16 on all seven workload/skew
combinations. Every case validated 20,000 terminal results; reported divergence,
permanent failures and exhausted retries are zero. All 28 Merkle cases passed
post-workload `merkle_verify`. This publication replaces the primary figures
with v2 measurements and preserves all previous observations below.

![Fresh v2 TPS for all modes and workload/skew combinations](figures/oom_scaling_all.png)

### Setup and configuration

| Item | Recorded v2 value |
|---|---|
| DB/server host | `neel@10.129.148.247`; gateway/controller `neel@10.129.27.111` |
| Dataset | Fresh COPY of 100,000,000 rows from scratch; `usertable WITH (fillfactor=90)` |
| Common heap | {golden['heap_bytes']:,} bytes ({golden['heap_bytes'] / 2**30:.2f} GiB), {golden['relpages']:,} pages; same heap for pg/det and Merkle |
| Stopped baselines | Merkle {golden['total_bytes']:,} bytes; plain {plain['total_bytes']:,} bytes; plain derived by dropping Merkle indexes |
| Merkle geometry | 200 partitions, fanout 32, split threshold 1024, merge threshold 256 |
| Initial tree | {verification['merkle_stats']['total_nodes']:,} nodes, {verification['merkle_stats']['leaf_nodes']:,} leaves |
| Compact node storage | Heap {verification['node_storage']['heap_bytes']:,} bytes; total {verification['node_storage']['total_bytes']:,} bytes |
| Timed settings | `shared_buffers=32MB`, SERIALIZABLE, `fsync=on`, `full_page_writes=on`, `synchronous_commit=on`, `wal_compression=off` |
| Integrity contract | `merkle_apply_synchronous_direct=on`; every leaf and ancestor through the partition root updated inside the user transaction, exact at commit |
| Load settings | 512MB shared buffers; 1GB maintenance memory; 2 maintenance workers; 4608MB MemAvailable guard; 21600s generation timeout |
| Workload | Seed 42; 20,000 SQL statements; A θ0/0.99/1.2 and B/C/D/F θ0.99; same seven published SQL hashes |
| Execution | Standalone direct completion; 96 terminals / 96 deterministic client workers; pg event execution with serialization failures retried client-side and jitter on |
| Trials | One per configuration; balanced randomized workload/worker blocks with rotating modes |

The PG mode uses this PostgreSQL 13 fork with det/Merkle execution disabled;
it is not an independent upstream PostgreSQL installation. PG and det both use
the fillfactor-90 heap, with only the primary-key index in their plain baseline.
Merkle additionally has its integrity index and lookup B-tree. PostgreSQL
shared buffers do not bound OS page cache or this fork's other shared memory.

The frozen working-tree snapshot includes the fanout-32 split-1024/merge-256
defaults, routing TID hints, one traversal snapshot, lazy executor index state,
the `PG_TRY` correction, cached send-function lookups, compact rebuild storage,
and concurrent-reindex rejection. The PG install and server/gateway were built
for v2 from that snapshot. Canonical row-hash format remains version 1;
the recovery semantics were required to remain unchanged.

### Method and timing boundaries

1. Generate one fresh dataset, then full-scan the 100M keyspace, verify fillfactor,
   geometry, physical node storage and baseline Merkle integrity. Derive the
   plain baseline from the same heap; preserve both manifests.
2. Restore the stopped per-mode baseline by `rsync --inplace --no-whole-file`
   delta copying. Check size/mtime every case; `--delta-content-check sampled`
   compares full file contents on the first restore and every tenth restore.
   `--verify-mode fast` is the per-case reset check, separate from the full
   baseline scan and post-workload Merkle checks.
3. Wait for QD1/QD8 read and O_DSYNC write probes to reach the 1.25× idle
   calibration bound, drop OS caches, start PostgreSQL cold, run SERIALIZABLE,
   and validate all terminal results.
4. Measure a separate checkpoint, then restart and verify Merkle for each
   Merkle case. Restore, settle, checkpoint and verification are outside TPS.

TPS is gateway workload statements/s. Timed device I/O uses its own sampling
window, slightly wider than gateway wall time, excludes the later checkpoint
and verification, and can include unrelated host traffic. PostgreSQL block
reads may hit OS cache. This is a database-larger-than-memory study, not evidence
of an OOM-killer event. No replicated-cluster or recovery-fault test ran here.

### All 84 TPS observations and matched ratios

Each row contains three measured TPS values, in statements/s. Ratios use the
**same v2 campaign** at the same workload/skew/worker count. Display values are
rounded to three decimals; [the figure table](figures/oom_tps_table.csv) and
[consolidated summary](summary.csv) retain unrounded values and retry counters.

| Workload | Workers | pg SERIALIZABLE TPS | det TPS | det + Merkle TPS | Merkle/det | Merkle/pg | det/pg | pg retries |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
"""
    for wl, skew in COMBOS:
        for w in WORKERS:
            r = [data[(wl, skew, w, mode)] for mode in MODES]
            pg, det, merkle = [float(x["tps"]) for x in r]
            prefix += (f"| {wl.upper()} θ{skew} | {w} | {pg:,.3f} | {det:,.3f} | {merkle:,.3f} | "
                       f"{merkle/det:.3f} | {merkle/pg:.3f} | {det/pg:.3f} | {r[0]['retry_attempts']} |\n")
    prefix += f"""
Across the 28 matched points, observed Merkle/det ranges from {min(ratios):.3f}
to {max(ratios):.3f}. This range describes single observations, not stable rankings
or an isolated optimization effect.

![Fresh v2 throughput relative to pg SERIALIZABLE](figures/oom_relative_to_pg.png)

### Timed device I/O per statement: all modes

**KB = 1000 bytes:** `device_*_mib × 2^20 / (1000 × total_queries)`.
Checkpoint writes remain separate in `summary.csv`, `io.json` and
`checkpoint.json`. These are device traffic values, not direct WAL-byte counts.

| Workload | Workers | pg read KB | det read KB | Merkle read KB | pg write KB | det write KB | Merkle write KB |
|---|---:|---:|---:|---:|---:|---:|---:|
"""
    for wl, skew in COMBOS:
        for w in WORKERS:
            r = [data[(wl, skew, w, mode)] for mode in MODES]
            values = [float(x[f"device_{direction}_mib"]) * 2**20 / (1000 * int(x["total_queries"]))
                      for direction in ("read", "write") for x in r]
            prefix += f"| {wl.upper()} θ{skew} | {w} | " + " | ".join(f"{v:.3f}" for v in values) + " |\n"
    prefix += """
![Matched v2 Merkle/det ratio and device I/O](figures/oom_merkle_ratios_io.png)

### Verification and frozen provenance

The publisher checked every raw summary row against `result.json`, successful
gateway exit status, settings, baseline heap size, cold/settle evidence,
executable hashes, and all 20,000 successful unique request IDs in each server
log: **1,680,000 terminal results** total. It verified 84 unique configurations,
28 Merkle PASS files, zero divergence/permanent failures/exhausted retries,
and the seven SQL files against the published hashes. The baseline verifier
recorded `100000000|1|100000000`, `{fillfactor=90}` and `merkle_verify=t`.
This verifies the saved evidence; publication performed no remote checks.

| Identity | Value |
|---|---|
"""
    prefix += f"| Git HEAD (working tree included uncommitted changes) | `{freeze['head']}` |\n"
    prefix += f"| Working-tree diff SHA-256 | `{freeze['git_diff_sha256']}` |\n"
    prefix += f"| **Final successful source archive SHA-256** | `{freeze['snapshot_sha256']}` |\n"
    for e in executables:
        prefix += f"| `{e['host']}:{e['path']}` | `{e['sha256']}` |\n"
    prefix += "\n| Published SQL input | SHA-256 |\n|---|---|\n"
    for name, item in sorted(hashes.items()):
        prefix += f"| `{name}` | `{item['sha256']}` |\n"
    prefix += """
The final successful snapshot identity above supersedes the intermediate
launch-report hash. [Frozen provenance](runs/v2_20261001/provenance/) retains
the source-file manifest, git status/diff and freeze metadata. Successful build
and frozen source manifests match, and the measured runner hash matches the
frozen manifest. Final controller settings changed after source freeze (4608MB
guard and `rsync --checksum`); supplied final script copies and their origin
are recorded in [controller_scripts](runs/v2_20261001/controller_scripts/).
Bulky source
tarballs are retained remotely at
`neel@10.129.27.111:/home/neel/claude_ctl/results/oom_v2_20261001/`;
[their SHA-256 values](runs/v2_20261001/remote_bulk_sha256.txt) are published
instead of copying tarballs to this nearly full workstation.

The preparation history includes failed builds that produced no data, and an
aborted first load because the 12288MB MemAvailable guard could not pass.
The orchestrator lowered the guard to 4608MB and deleted the partial load;
the completed generation was a single clean run. Stale same-size source/build
state on .247 was moved aside and snapshot rsync changed to `--checksum`.
Failed build logs are separated under `failed_preparation_no_data/`; the aborted
and completed generation manifests remain labeled by their original run IDs.
These preparation attempts are excluded from the 84 measured cases.

### Caveats and comparison with previous campaigns

- One trial per point does not establish stable throughput rankings, error bars,
  or causal attribution. Cold storage, host activity, retries and cache behavior
  can affect small differences.
- Fillfactor 90 is part of the v2 configuration. The October 1 paired experiment
  below observed Merkle gains of 1.62–23.70% and det reductions of 2.15–9.82%
  when moving from fillfactor 100 to 90. Those four paired points used a rewritten
  heap and an earlier rebuild path; they are not isolated evidence for this fresh
  campaign. HOT fraction was not measured. V2 det/pg also run on fillfactor 90.
- Fresh data, compact rebuild storage, geometry, optimized code and new binaries
  differ from the September study. Matched SQL hashes do not make physical
  datasets or builds identical. Do not treat cross-campaign differences as the
  effect of a single optimization.
- PG READ COMMITTED was intentionally not rerun. Its historical observations
  remain below and are absent from the primary figures.
- Earlier OOM pgdata was deleted at the user's request. Earlier campaigns cannot
  be rerun from their original physical datasets; historical artifacts remain.
- Merkle verification and zero terminal failures do not establish crash recovery,
  replica repair, or serializability from root equality.

![Merkle ratios across different datasets and configurations](figures/v2_vs_published_merkle_ratio.png)

The comparison uses September split-32 Merkle divided by September det/pg,
October split-1024 Merkle divided by **September det/pg** (no new C point),
and fresh v2 Merkle divided by **v2 det/pg**. The October drift controls are not
substituted into historical denominators. Exact plotted ratios are in
[the comparison CSV](figures/v2_vs_published_merkle_ratio.csv).

### Contents and regeneration

| Path | Content |
|---|---|
| `summary.csv` | 220 rows: 136 historical values preserved, plus 84 v2; explicit `campaign` and `fillfactor` columns |
| `runs/v2_20261001/` | Every case file plus generation/verification/build/provenance/preflight evidence; raw paths retained |
| `runs/v2_20261001/SOURCE_TO_ARCHIVE.csv` | Input-to-archive mapping, SHA-256 and deduplication policy |
| `runs/v2_20261001/ARCHIVE_SHA256SUMS` | Checksums of every retained file; identical duplicate SQL uses relative links |
| `runs/v2_20261001/publication_input/` | Pre-v2 consolidated summary and README |
| `runs/v2_20261001/controller_scripts/` | Supplied final orchestration scripts, including post-freeze guard/rsync corrections |
| `figures/oom_scaling_all.png`, `scaling_<wl>_<skew>.png` | All v2 TPS series, including read-only C |
| `figures/oom_relative_to_pg.png` | V2 ratios against v2 pg at workers 1 and 16 |
| `figures/oom_tps_table.csv` | All 84 unrounded v2 TPS values, matched ratios, retries and per-statement I/O |
| `figures/oom_merkle_ratios_io.png`, `.csv` | Matched v2 Merkle/det and all three modes' timed device I/O |
| `figures/v2_vs_published_merkle_ratio.png`, `.csv` | Explicitly labeled cross-campaign comparison |
| `figures/previous/` | Original primary figures/tables, preserved before v2 publication |

Local artifact analysis only (no build, database or benchmark):

```bash
python3 scripts/distributed/oom_v2/publish_v2.py \\
  --source Final_Results/OOM_100M/runs/v2_20261001 --validate-only
(cd Final_Results/OOM_100M/runs/v2_20261001 && sha256sum -c ARCHIVE_SHA256SUMS)
MPLCONFIGDIR=/tmp/ariabc-oom-v2-matplotlib python3 scripts/distributed/plot_oom_figures.py --campaign v2
python3 scripts/distributed/oom_v2/publish_readme.py
# Historical figure regeneration into a fresh directory:
MPLCONFIGDIR=/tmp/ariabc-oom-previous-matplotlib python3 scripts/distributed/plot_oom_figures.py \\
  --campaign previous --out /tmp/ariabc-oom-previous-figures
```

Exact completed remote campaign commands are in
[COMMANDS.md, section 2](../COMMANDS.md#2-oom-100m--fresh-v2-primary-result).
The one-time publisher refuses an existing archive; use `--validate-only` after
publication. No remote run is needed to regenerate these results.

## Previous results

The text and observations below are preserved from the pre-v2 publication.
Its figure descriptions refer to the files now under `figures/previous/`.
For its historical plots, use `--campaign previous` with a separate `--out`
directory, as shown above; the current default selects v2. Statements such as
"no new C measurement" describe the earlier split-1024 campaign, not v2.

"""
    old = (archive / "publication_input/README_before_v2.md").read_text()
    # Keep every historical paragraph and table; the document title is already above.
    old = old.split("\n", 1)[1].lstrip("\n")
    (root / "README.md").write_text(prefix + old)
    print("Wrote primary v2 README: 84 TPS values, 28 matched ratio rows, 84 I/O pairs; previous text preserved")


if __name__ == "__main__":
    main()
