#include "../src/pg_state_machine.hxx"
#include <iostream>
#include <cstring>
#include <vector>
#include <string>
#include <openssl/sha.h>
#include <libpq-fe.h>

#define REQUIRE(expr) \
    do { \
        if (!(expr)) { \
            std::cerr << "CRITICAL FAILURE at " << __FILE__ << ":" << __LINE__ << " - requirement failed: " #expr << std::endl; \
            std::abort(); \
        } \
    } while (0)

using namespace ariabc_pg;

// ============================================================================
// Libpq mocks to test safe_sync_startup without a database
// ============================================================================
struct pg_conn {
    // dummy struct
};

struct pg_result {
    ExecStatusType status;
    std::vector<std::vector<std::string>> rows;
};

static int g_mock_claimed_rows_count = 0;
enum class terminal_case {
    valid,
    null_digest,
    digest_31_bytes,
    digest_33_bytes,
    bad_format_version,
    digest_payload_mismatch
};
static terminal_case g_mock_terminal_case = terminal_case::valid;

static uint32_t to_be32(uint32_t val) {
    uint32_t ret = 0;
    ret |= ((val >> 24) & 0xFF);
    ret |= ((val >> 16) & 0xFF) << 8;
    ret |= ((val >> 8) & 0xFF) << 16;
    ret |= (val & 0xFF) << 24;
    return ret;
}

extern "C" {

PGconn* PQconnectdb(const char* conninfo) {
    (void)conninfo;
    return (PGconn*)new pg_conn();
}

ConnStatusType PQstatus(const PGconn* conn) {
    (void)conn;
    return CONNECTION_OK;
}

void PQfinish(PGconn* conn) {
    if (conn) {
        delete (pg_conn*)conn;
    }
}

void PQclear(PGresult* res) {
    if (res) {
        delete (pg_result*)res;
    }
}

ExecStatusType PQresultStatus(const PGresult* res) {
    if (!res) return PGRES_FATAL_ERROR;
    return ((pg_result*)res)->status;
}

int PQntuples(const PGresult* res) {
    if (!res) return 0;
    return ((pg_result*)res)->rows.size();
}

int PQnfields(const PGresult* res) {
    if (!res || ((pg_result*)res)->rows.empty()) return 0;
    return ((pg_result*)res)->rows[0].size();
}

char* PQgetvalue(const PGresult* res, int tup_num, int field_num) {
    if (!res) return nullptr;
    auto* r = (pg_result*)res;
    if (tup_num < 0 || tup_num >= (int)r->rows.size()) return nullptr;
    if (field_num < 0 || field_num >= (int)r->rows[tup_num].size()) return nullptr;
    return const_cast<char*>(r->rows[tup_num][field_num].data());
}

int PQgetisnull(const PGresult* res, int tup_num, int field_num) {
    if (!res) return 1;
    auto* r = (pg_result*)res;
    if (tup_num < 0 || tup_num >= (int)r->rows.size()) return 1;
    if (field_num < 0 || field_num >= (int)r->rows[tup_num].size()) return 1;
    if (r->rows[tup_num][field_num] == "__NULL__") return 1;
    return 0;
}

int PQgetlength(const PGresult* res, int tup_num, int field_num) {
    if (!res) return 0;
    auto* r = (pg_result*)res;
    if (tup_num < 0 || tup_num >= (int)r->rows.size()) return 0;
    if (field_num < 0 || field_num >= (int)r->rows[tup_num].size()) return 0;
    if (r->rows[tup_num][field_num] == "__NULL__") return 0;
    return r->rows[tup_num][field_num].size();
}

static std::string compute_test_terminal_digest(bool is_error, int fmtver, const std::string& sqlstate, const std::string& payload) {
    unsigned char computed_hash[SHA256_DIGEST_LENGTH];
    SHA256_CTX ctx;
    SHA256_Init(&ctx);

    const char* prefix = is_error ? "ariabc-terminal-error-v1" : "ariabc-terminal-ok-v1";
    SHA256_Update(&ctx, prefix, strlen(prefix));

    uint32_t fmtver_be = to_be32(static_cast<uint32_t>(fmtver));
    SHA256_Update(&ctx, &fmtver_be, sizeof(fmtver_be));

    if (is_error) {
        uint32_t sqlstate_len = sqlstate.size();
        uint32_t sqlstate_len_be = to_be32(sqlstate_len);
        SHA256_Update(&ctx, &sqlstate_len_be, sizeof(sqlstate_len_be));
        SHA256_Update(&ctx, sqlstate.data(), sqlstate.size());
    }

    uint32_t payload_len = payload.size();
    uint32_t payload_len_be = to_be32(payload_len);
    SHA256_Update(&ctx, &payload_len_be, sizeof(payload_len_be));
    if (!payload.empty()) {
        SHA256_Update(&ctx, payload.data(), payload.size());
    }

    SHA256_Final(computed_hash, &ctx);
    return std::string((char*)computed_hash, SHA256_DIGEST_LENGTH);
}

PGresult* PQexec(PGconn* conn, const char* query) {
    (void)conn;
    std::string q(query);
    pg_result* res = new pg_result();
    res->status = PGRES_TUPLES_OK;

    if (q.find("raft_apply_schema_meta") != std::string::npos) {
        res->rows.push_back({"1", "2", "2"}); // count = 1, min = 2, max = 2
    } else if (q.find("raft_apply_epoch") != std::string::npos) {
        res->rows.push_back({"1"}); // protocol_version = 1
    } else if (q.find("raft_apply_item") != std::string::npos && q.find("state = 1") != std::string::npos) {
        res->rows.push_back({std::to_string(g_mock_claimed_rows_count)});
    } else if (q.find("state IN (2, 3)") != std::string::npos) {
        if (g_mock_terminal_case == terminal_case::null_digest) {
            res->rows.push_back({
                "2",
                "0",
                "2",
                "1",
                "payload_2",
                "__NULL__",
                "__NULL__",
                "__NULL__",
                "__NULL__" // Null digest
            });
        } else if (g_mock_terminal_case == terminal_case::digest_31_bytes) {
            res->rows.push_back({
                "2",
                "0",
                "2",
                "1",
                "payload_2",
                "__NULL__",
                "__NULL__",
                "__NULL__",
                std::string(31, 'x')
            });
        } else if (g_mock_terminal_case == terminal_case::digest_33_bytes) {
            res->rows.push_back({
                "2",
                "0",
                "2",
                "1",
                "payload_2",
                "__NULL__",
                "__NULL__",
                "__NULL__",
                std::string(33, 'x')
            });
        } else if (g_mock_terminal_case == terminal_case::bad_format_version) {
            res->rows.push_back({
                "2",
                "0",
                "2",
                "99", // Unsupported format version
                "payload_2",
                "__NULL__",
                "__NULL__",
                "__NULL__",
                compute_test_terminal_digest(false, 99, "", "payload_2")
            });
        } else if (g_mock_terminal_case == terminal_case::digest_payload_mismatch) {
            res->rows.push_back({
                "2",
                "0",
                "2",
                "1",
                "payload_2",
                "__NULL__",
                "__NULL__",
                "__NULL__",
                compute_test_terminal_digest(false, 1, "", "different_payload")
            });
        } else {
            // Logs 2 and 5 terminal, log 4 is missing/incomplete
            res->rows.push_back({
                "2",
                "0",
                "2",
                "1",
                "payload_2",
                "__NULL__",
                "__NULL__",
                "__NULL__",
                compute_test_terminal_digest(false, 1, "", "payload_2")
            });
            res->rows.push_back({
                "5",
                "0",
                "3",
                "__NULL__",
                "__NULL__",
                "1",
                "42000",
                "error_payload_5",
                compute_test_terminal_digest(true, 1, "42000", "error_payload_5")
            });
        }
    }
    return (PGresult*)res;
}

} // extern "C"

int main() {
    std::cout << "Running pg_state_machine contract tests..." << std::endl;

    db_options db_opt;
    db_opt.dbname = "dummy_test";
    db_opt.port = "5432";
    db_opt.raft_epoch_hex = "0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20";
    kafka_options k_opt;
    pg_state_machine sm(1, db_opt, k_opt);

    // 1. Test zero-byte empty commit (Raft no-op entry, data.size() == 0)
    {
        sm.seed_durable_prefix(99);
        nuraft::ptr<nuraft::buffer> empty_buf = nuraft::buffer::alloc(0);
        nuraft::ptr<nuraft::buffer> ack = sm.commit(100, *empty_buf);
        REQUIRE(ack != nullptr);
        REQUIRE(ack->size() == 0);
        REQUIRE(sm.last_commit_index() >= 100);
        std::cout << "Zero-byte no-op commit test passed." << std::endl;
    }

    // 2. Test commit_config (configuration change entries advance prefix)
    {
        nuraft::ptr<nuraft::cluster_config> dummy_conf;
        sm.commit_config(101, dummy_conf);
        REQUIRE(sm.last_commit_index() >= 101);
        std::cout << "Commit config test passed." << std::endl;
    }

    // 3. Test non-empty buffer that parses as zero-item batch (invalid payload in non-safe mode)
    {
        nuraft::ptr<nuraft::buffer> junk_buf = nuraft::buffer::alloc(4);
        nuraft::buffer_serializer bs(*junk_buf);
        bs.put_u32(0xDEADBEEF);
        nuraft::ptr<nuraft::buffer> ack = sm.commit(102, *junk_buf);
        REQUIRE(ack != nullptr);
        std::cout << "Invalid-payload non-safe commit test passed." << std::endl;
    }

    // 4. Test safe_sync_startup with logs 2 and 5, missing 4
    {
        db_options db_opt_safe;
        db_opt_safe.dbname = "dummy_test";
        db_opt_safe.port = "5432";
        db_opt_safe.raft_epoch_hex = "0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20";
        db_opt_safe.raft_apply_ledger_mode = "safe";
        kafka_options k_opt_safe;
        pg_state_machine sm_safe(1, db_opt_safe, k_opt_safe);

        g_mock_claimed_rows_count = 0;
        g_mock_terminal_case = terminal_case::valid;

        uint64_t prefix = sm_safe.safe_sync_startup(0);
        REQUIRE(prefix == 0);
        prefix = sm_safe.safe_sync_startup(1);
        REQUIRE(prefix == 0);
        bool compacted_threw = false;
        try {
            sm_safe.safe_sync_startup(2);
        } catch (const std::runtime_error& e) {
            std::string msg = e.what();
            REQUIRE(msg.find("SAFE_STARTUP_FAILED: safe v1 requires retained Raft logs") != std::string::npos);
            compacted_threw = true;
        }
        REQUIRE(compacted_threw);
        std::cout << "safe_sync_startup gap test passed: returned 0 for logs 2 and 5, missing 4." << std::endl;
    }

    // 5. Test safe_sync_startup throwing on persistent CLAIMED rows
    {
        db_options db_opt_safe;
        db_opt_safe.dbname = "dummy_test";
        db_opt_safe.port = "5432";
        db_opt_safe.raft_epoch_hex = "0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20";
        db_opt_safe.raft_apply_ledger_mode = "safe";
        kafka_options k_opt_safe;
        pg_state_machine sm_safe(1, db_opt_safe, k_opt_safe);

        g_mock_claimed_rows_count = 3; // 3 claimed rows
        bool threw = false;
        try {
            sm_safe.safe_sync_startup(0);
        } catch (const std::runtime_error& e) {
            std::string msg = e.what();
            REQUIRE(msg.find("SAFE_STARTUP_FAILED: persistent_claimed_row") != std::string::npos);
            threw = true;
        }
        REQUIRE(threw);
        std::cout << "safe_sync_startup persistent CLAIMED row check passed." << std::endl;
    }

    // 6. Test safe_sync_startup throwing on malformed terminal digest
    {
        db_options db_opt_safe;
        db_opt_safe.dbname = "dummy_test";
        db_opt_safe.port = "5432";
        db_opt_safe.raft_epoch_hex = "0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20";
        db_opt_safe.raft_apply_ledger_mode = "safe";
        kafka_options k_opt_safe;
        pg_state_machine sm_safe(1, db_opt_safe, k_opt_safe);

        g_mock_claimed_rows_count = 0;
        g_mock_terminal_case = terminal_case::null_digest;
        bool threw = false;
        try {
            sm_safe.safe_sync_startup(0);
        } catch (const std::runtime_error& e) {
            std::string msg = e.what();
            REQUIRE(msg.find("SAFE_STARTUP_FAILED: terminal digest malformed") != std::string::npos);
            threw = true;
        }
        REQUIRE(threw);
        g_mock_terminal_case = terminal_case::digest_31_bytes;
        threw = false;
        try {
            sm_safe.safe_sync_startup(0);
        } catch (const std::runtime_error& e) {
            std::string msg = e.what();
            REQUIRE(msg.find("SAFE_STARTUP_FAILED: terminal digest malformed") != std::string::npos);
            threw = true;
        }
        REQUIRE(threw);
        g_mock_terminal_case = terminal_case::digest_33_bytes;
        threw = false;
        try {
            sm_safe.safe_sync_startup(0);
        } catch (const std::runtime_error& e) {
            std::string msg = e.what();
            REQUIRE(msg.find("SAFE_STARTUP_FAILED: terminal digest malformed") != std::string::npos);
            threw = true;
        }
        REQUIRE(threw);
        std::cout << "safe_sync_startup malformed terminal digest checks passed." << std::endl;
    }

    // 7. Test safe_sync_startup throwing on bad terminal metadata
    {
        db_options db_opt_safe;
        db_opt_safe.dbname = "dummy_test";
        db_opt_safe.port = "5432";
        db_opt_safe.raft_epoch_hex = "0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20";
        db_opt_safe.raft_apply_ledger_mode = "safe";
        kafka_options k_opt_safe;
        pg_state_machine sm_safe(1, db_opt_safe, k_opt_safe);

        g_mock_claimed_rows_count = 0;
        g_mock_terminal_case = terminal_case::bad_format_version;
        bool threw = false;
        try {
            sm_safe.safe_sync_startup(0);
        } catch (const std::runtime_error& e) {
            std::string msg = e.what();
            REQUIRE(msg.find("SAFE_STARTUP_FAILED: terminal row metadata bad") != std::string::npos);
            threw = true;
        }
        REQUIRE(threw);
        g_mock_terminal_case = terminal_case::digest_payload_mismatch;
        threw = false;
        try {
            sm_safe.safe_sync_startup(0);
        } catch (const std::runtime_error& e) {
            std::string msg = e.what();
            REQUIRE(msg.find("SAFE_STARTUP_FAILED: terminal row metadata bad") != std::string::npos);
            threw = true;
        }
        REQUIRE(threw);
        std::cout << "safe_sync_startup bad terminal metadata checks passed." << std::endl;
    }

    std::cout << "ALL pg_state_machine contract tests PASSED!" << std::endl;
    return 0;
}
