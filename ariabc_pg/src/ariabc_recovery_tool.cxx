/*
 * ariabc_recovery_tool — operator / test utility for online replica recovery.
 *
 *   ariabc_recovery_tool ctl <host:port> <VERB k=v ...>
 *       Send "__ARIABC_CTRL_RECOVERY <VERB ...>" to an ariabc_pg_server.
 *   ariabc_recovery_tool rejoin <self host:port> <ref host:port> <ref_db_host> <ref_db_port> [wait_live_ms]
 *       Bring a replica started with --raft-rejoin back: cut a live replica, repair
 *       and rebase this one from that cut, then replay the log after it.
 *   ariabc_recovery_tool digest <conninfo> [snapshot_id]
 *       Print the per-table Merkle digest (optionally through a snapshot).
 *   ariabc_recovery_tool repair <local_conninfo> <ref_conninfo> <snapshot_id> [heap_verify=1]
 *       Run the sparse Merkle repair of the local database from a reference snapshot.
 */
#include "replica_repair.hxx"
#include "wire_protocol.hxx"
#include "ariabc_pg_util.hxx"

#include <libpq-fe.h>

#include <cstdlib>
#include <iostream>
#include <string>

#include <netdb.h>
#include <sys/socket.h>
#include <unistd.h>

using namespace ariabc_pg;

static int run_ctl(const std::string& endpoint, const std::string& verb, std::string& out) {
    const host_port hp = parse_host_port(endpoint);
    addrinfo hints{};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    addrinfo* res = nullptr;
    if (::getaddrinfo(hp.host.c_str(), std::to_string(hp.port).c_str(), &hints, &res) != 0 || !res) {
        std::cerr << "resolve failed" << std::endl;
        return 2;
    }
    int fd = -1;
    for (addrinfo* ai = res; ai; ai = ai->ai_next) {
        fd = ::socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
        if (fd < 0) continue;
        if (::connect(fd, ai->ai_addr, ai->ai_addrlen) == 0) break;
        ::close(fd);
        fd = -1;
    }
    ::freeaddrinfo(res);
    if (fd < 0) {
        std::cerr << "connect failed" << std::endl;
        return 2;
    }
    client_api_request req;
    req.req_id = "__recovery_tool";
    req.sql = "__ARIABC_CTRL_RECOVERY " + verb;
    client_api_response resp;
    std::string err;
    if (!write_request_frame(fd, req, err) || !read_response_frame(fd, resp, err)) {
        std::cerr << "io failed: " << err << std::endl;
        ::close(fd);
        return 2;
    }
    ::close(fd);
    out = resp.msg;
    return resp.status == 0 ? 0 : 1;
}

static int cmd_ctl(const std::string& endpoint, const std::string& verb) {
    std::string out;
    const int rc = run_ctl(endpoint, verb, out);
    if (rc != 2) std::cout << out << std::endl;
    return rc;
}

static std::string msg_field(const std::string& msg, const std::string& key) {
    const std::string needle = " " + key + "=";
    const size_t at = msg.find(needle);
    if (at == std::string::npos) return "";
    const size_t start = at + needle.size();
    return msg.substr(start, msg.find(' ', start) - start);
}

static int cmd_rejoin(const std::string& self, const std::string& ref,
                      const std::string& ref_db_host, const std::string& ref_db_port,
                      const std::string& wait_live_ms) {
    std::string cut;
    if (run_ctl(ref, "CUT target=0 hold=1 digest=0", cut) != 0) {
        std::cerr << "CUT on " << ref << " failed: " << cut << std::endl;
        return 1;
    }
    const std::string snapshot = msg_field(cut, "snapshot");
    const std::string L = msg_field(cut, "L");
    const std::string B = msg_field(cut, "B");
    if (snapshot.empty() || L.empty() || B.empty()) {
        std::cerr << "unexpected CUT reply: " << cut << std::endl;
        return 1;
    }
    std::cout << "REJOIN_CUT " << cut << std::endl;
    std::string rec;
    const int rc = run_ctl(self, "RECOVER snapshot=" + snapshot + " ref_host=" + ref_db_host +
                                     " ref_port=" + ref_db_port + " L=" + L + " B=" + B +
                                     " wait_live_ms=" + wait_live_ms, rec);
    std::string released;
    (void)run_ctl(ref, "RELEASE snapshot=" + snapshot, released);
    std::cout << "REJOIN_RECOVER " << rec << std::endl;
    if (rc != 0) return 1;
    return msg_field(rec, "live") == "1" ? 0 : 1;
}

int main(int argc, char** argv) {
    if (argc < 3) {
        std::cerr << "usage: ariabc_recovery_tool ctl <host:port> <VERB ...> | "
                     "rejoin <self host:port> <ref host:port> <ref_db_host> <ref_db_port> [wait_live_ms] | "
                     "digest <conninfo> [snapshot] | "
                     "repair <local_ci> <ref_ci> <snapshot> [heap_verify]" << std::endl;
        return 2;
    }
    const std::string mode = argv[1];
    if (mode == "ctl") {
        std::string verb;
        for (int i = 3; i < argc; ++i) {
            if (i > 3) verb += " ";
            verb += argv[i];
        }
        return cmd_ctl(argv[2], verb);
    }
    if (mode == "rejoin" && argc >= 6) {
        return cmd_rejoin(argv[2], argv[3], argv[4], argv[5], argc > 6 ? argv[6] : "600000");
    }
    if (mode == "digest") {
        std::string err, digest;
        PGconn* c = nullptr;
        if (argc > 3) {
            c = open_snapshot_reader(argv[2], argv[3], err);
        } else {
            c = PQconnectdb(argv[2]);
            if (c && PQstatus(c) != CONNECTION_OK) {
                err = PQerrorMessage(c);
                PQfinish(c);
                c = nullptr;
            }
        }
        if (!c) {
            std::cerr << "connect failed: " << err << std::endl;
            return 2;
        }
        const bool ok = compute_merkle_digest(c, digest, err);
        PQfinish(c);
        if (!ok) {
            std::cerr << "digest failed: " << err << std::endl;
            return 1;
        }
        std::cout << digest << std::endl;
        return 0;
    }
    if (mode == "repair" && argc >= 5) {
        replica_repair_stats st;
        std::string err;
        const bool heap_verify = argc < 6 || std::atoi(argv[5]) != 0;
        const bool ok = repair_replica_from_snapshot(argv[2], argv[3], argv[4], heap_verify, st, err);
        std::cout << (ok ? "OK" : "FAIL") << " tables=" << st.tables_checked
                  << " repaired_tables=" << st.tables_repaired
                  << " full_copies=" << st.full_table_copies
                  << " mismatched_partitions=" << st.mismatched_partitions
                  << " differing_leaves=" << st.differing_leaves
                  << " descent_levels=" << st.descent_levels
                  << " nodes_compared=" << st.nodes_compared
                  << " leaf_mode_tables=" << st.leaf_mode_tables
      << " tuplehash_mode_tables=" << st.tuplehash_mode_tables
      << " entries_compared=" << st.entries_compared
                  << " differing_entries=" << st.differing_entries
                  << " ref_hash_bytes=" << st.ref_hash_bytes
                  << " candidate_rows=" << st.candidate_rows
      << " ref_row_bytes=" << st.ref_row_bytes
                  << " rows_deleted=" << st.rows_deleted
                  << " rows_upserted=" << st.rows_upserted
                  << " localise_us=" << st.localise_us
                  << " transfer_us=" << st.transfer_us
                  << " apply_us=" << st.apply_us
                  << " verify_us=" << st.verify_us
                  << " total_us=" << st.total_us
                  << " digest=" << st.digest_after;
        if (!ok) std::cout << " error=" << err;
        std::cout << std::endl;
        return ok ? 0 : 1;
    }
    std::cerr << "bad arguments" << std::endl;
    return 2;
}
