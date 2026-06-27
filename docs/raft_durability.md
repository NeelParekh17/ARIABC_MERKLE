# Durable NuRaft Storage Layer in AriaBC

This document describes the design, implementation, and operations of the durable consensus storage layer in AriaBC, replacing NuRaft's in-memory storage manager and log store with an append-only, crash-safe, and fsynced local disk storage system.

---

## 1. Architecture & Design Principles

The durable storage layer consists of two main abstractions implementing the NuRaft interfaces:
1. `durable_state_mgr` (inherits from `nuraft::state_mgr`): Manages cluster configurations, server state (term and voting history), and node identity.
2. `durable_log_store` (inherits from `nuraft::log_store`): Manages the replication log, utilizing segmented append-only files and robust validation.

### Durability Model & Group Commit
To achieve high throughput while maintaining safety guarantees:
* **Group Commit Integration**: Appends to the log store do not execute `fdatasync()` immediately. Instead, transactions are buffered and written sequentially. An explicit fsync barrier is called at the end of each append batch in `end_of_append_batch()`.
* **Sync Boundary**: `parallel_log_appending_` is disabled (`false`) in `raft_params` to guarantee in-order appending and sequential fsync boundary commits.

---

## 2. Directory Layout & File Formats

Storage is organized under a single root directory (e.g. `raft_storage/node1/`):

```
raft_storage/node1/
├── LOCK                 # Exclusive flock for process concurrency safety
├── identity.bin         # Node identity details (written once)
├── srv_state.bin        # Serialized NuRaft term and vote (atomic temp-rename write)
├── cluster_config.bin   # Serialized NuRaft membership config (atomic temp-rename write)
└── log/                 # Log directory
    ├── durable_watermark.bin            # Persisted durable watermark index (atomic checksummed write)
    ├── segment_00000000000000000001.log
    ├── segment_00000000000000000124.log
    └── ...
```

### Identity and State Management
* **`LOCK`**: Locked exclusively via `flock(fd, LOCK_EX | LOCK_NB)` on startup. Prevents multiple server instances from attaching to the same directory.
* **`identity.bin`**: Written once. Validates `node_id`, `endpoint`, and `cluster_id` on restart to prevent accidental data contamination from mismatching nodes.
* **`durable_watermark.bin`**: Persists the highest log index successfully written and synced to disk. It is written atomically (`durable_watermark.bin.tmp` then renamed) after syncing all dirty segments in a batch. Wrapped in a checksum envelope consisting of a 4-byte Magic (`0xAB1C0D0B`), a 4-byte format version, a 8-byte watermark index, and a 4-byte CRC-32 checksum. On restart, the log store loads the persisted watermark, ensuring a truthful durable watermark instead of assuming `next_slot - 1`.
* **Atomic File Updates & Checksum Envelopes**: Updates to `srv_state.bin` and `cluster_config.bin` use a temporary file write followed by `rename()`, ensuring atomic transitions. Furthermore, the contents of both files are wrapped in a checksum envelope consisting of a 4-byte Magic (`0xAB1CDEFE` for state, `0xAB1CC0FF` for config), a 4-byte format version, a 4-byte payload length, the serialized payload itself, and a 4-byte CRC-32 checksum calculated over the entire header and payload.

### Log Segment Format
Log records are divided into 64MB segments. A segment file begins with a 16-byte header:
* `RAFT_SEG` (8 bytes magic)
* Format Version (4 bytes, little-endian)
* Reserved (4 bytes)

Each record inside a segment uses a binary framed format:

| Field | Size (Bytes) | Description |
|---|---|---|
| Magic | 4 | `0xAB1CFACE` to detect frame boundaries |
| Format Version | 4 | Currently `1` |
| Record Type | 4 | Record type: `1` (Log entry), `2` (Truncate marker) |
| Reserved | 4 | Zero-padded |
| Log Index | 8 | Raft index of the entry |
| Log Term | 8 | Raft term of the entry |
| Payload Size | 8 | Size of the serialized NuRaft log entry |
| Header CRC | 4 | CRC32 of fields from Magic to Payload Size (first 40 bytes) |
| Payload | N | Serialized log entry (empty for Truncate marker) |
| Payload CRC | 4 | CRC32 of the Payload bytes |

---

## 3. Crash Recovery and Truncation

### Fail-Closed Recovery Scan
On server startup, `durable_log_store` scans all segment log files in order with strict validation:
1. **Header & Payload Integrity**: Validates both the 44-byte header CRC (computed over the first 40 bytes) and the payload CRC. Any corruption in non-tail records triggers a fatal `storage_corruption_error`.
2. **Index Sequence Continuity**: Validates that all log entry indices are strictly continuous. Truncate records can reset the expected index backward, but cannot jump forward.
3. **Term Consistency**: Validates that the deserialized log entry term matches the term recorded in the frame header.
4. **Tail Truncation**: Only incomplete or corrupted trailing frames at the exact EOF of the final active segment are allowed to be truncated. Any other mismatch is fatal.
5. **Config & State Safety**: Strict parsing of `srv_state.bin` and `cluster_config.bin`. Missing or malformed configuration/state files throw `storage_corruption_error` immediately without silent fallback bootstrapping.

### Log Rollback (`write_at`) and `RT_TRUNCATE`
If the Raft leader issues a rollback (truncating entries at index `X` and writing a new entry):
1. **RT_TRUNCATE Marker**: The store writes a persistent `RT_TRUNCATE` record containing index `X` at the end of the log and performs an `fdatasync()`.
2. **Physical Truncation**: It physically truncates all segment files starting from index `X` and deletes any obsolete higher segment files.
3. This write-ahead marker design prevents recovery scanner corruption in case the process crashes *during* the physical segment deletion or truncation phase.

---

## 4. Durability Guarantees & Bounds

> [!IMPORTANT]
> **Consensus Durability Bounds**
> The durable storage layer guarantees that Raft log entries, membership configuration, and voting history survive process crashes and machine reboots.
> This does **not** mean full database crash recovery is completed; PostgreSQL level transaction log replay/snapshots remain separate from the Raft layer.

---

## 5. Telemetry & Profiling Counters

The durable storage layer records low-overhead atomic counters that are printed on server shutdown or status requests under `PROFILE_RAFT_STORAGE`:

* `append_calls`: Total number of entry append invocations.
* `append_batches`: Total number of completed append batches (`end_of_append_batch()`).
* `bytes_appended`: Total raw payload bytes written to segments.
* `fdatasync_calls`: Total number of `fdatasync()` calls executed on segment files.
* `fdatasync_total_ms`: Total cumulative duration spent in `fdatasync()` system calls.
* `fdatasync_max_ms`: Maximum observed latency of a single `fdatasync()` call.
* `append_batch_entries_max`: Maximum number of entries committed in a single batch.
* `append_batch_entries_total`: Total number of log entries written.
* `last_durable_index`: The highest log index successfully written and synced to disk.

---

## 6. Build and Test Commands

### Run Unit and Integration Tests
```bash
# Configure CMake
cmake -S ariabc_pg -B ariabc_pg/build -DCMAKE_BUILD_TYPE=Release

# Build targets
cmake --build ariabc_pg/build --target durable_state_mgr_test durable_log_store_test durable_raft_cluster_smoke_test -j$(nproc)

# Run tests
./ariabc_pg/build/bin/durable_state_mgr_test
./ariabc_pg/build/bin/durable_log_store_test
./ariabc_pg/build/bin/durable_raft_cluster_smoke_test
```

### Run 4-Node Raft Cluster with Durable Storage
```bash
./scripts/distributed/run_4node_raft_cluster.sh --raft-storage-mode durable --raft-storage-dir ./.raft_storage_data
```
