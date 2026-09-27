#pragma once

#include <cstdint>
#include <string>

struct pg_conn;
typedef struct pg_conn PGconn;

namespace ariabc_pg {

/*
 * Sparse replica repair against a healthy replica's exported snapshot
 * (ProtectDB Algorithm 2, lines 10-11).
 *
 * The reference connection imports `snapshot_id`, so every read of the
 * healthy node (user rows and Merkle node tables) observes exactly the cut
 * boundary.  For each Merkle-indexed table the damaged node's partition roots
 * and leaves are compared with the snapshot's, only rows in differing leaf
 * ranges are streamed (COPY) into a temporary table on the damaged node, and
 * one set-oriented DELETE + INSERT .. ON CONFLICT makes those ranges equal.
 * Tables without a usable Merkle index are compared by checksum and copied
 * in full when they differ.  Every repaired table must end with partition
 * roots identical to the snapshot's; otherwise it is fully re-copied.
 * Each table's repair commits before it is verified, because the Merkle
 * nodes of ordinary SQL writes are materialised at pre-commit.  The replica
 * is quarantined, so intermediate states are never observed.
 */
struct replica_repair_stats {
    int tables_checked = 0;
    int tables_repaired = 0;
    int full_table_copies = 0;
    int mismatched_partitions = 0;
    int differing_leaves = 0;
    uint64_t candidate_rows = 0;
    uint64_t rows_deleted = 0;
    uint64_t rows_upserted = 0;
    uint64_t localise_us = 0;
    uint64_t transfer_us = 0;
    uint64_t apply_us = 0;
    uint64_t verify_us = 0;
    uint64_t total_us = 0;
    std::string digest_after;
};

bool repair_replica_from_snapshot(const std::string& local_conninfo,
                                  const std::string& ref_conninfo,
                                  const std::string& snapshot_id,
                                  bool heap_verify,
                                  replica_repair_stats& stats,
                                  std::string& err);

/*
 * Per-table Merkle digest ("table:roothex,...", sorted) read through the
 * connection's current snapshot.  Returns false on query failure.
 */
bool compute_merkle_digest(PGconn* c, std::string& out_digest, std::string& err);

/* Open a connection that reads through an exported snapshot. */
PGconn* open_snapshot_reader(const std::string& conninfo,
                             const std::string& snapshot_id,
                             std::string& err);

} // namespace ariabc_pg
