#include "replica_repair.hxx"

#include <libpq-fe.h>

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <map>
#include <set>
#include <sstream>
#include <string>
#include <vector>

namespace ariabc_pg {
namespace {

using rows_t = std::vector<std::vector<std::string>>;

uint64_t now_us() {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count());
}

std::string conn_error(PGconn* c) {
    std::string e = c ? PQerrorMessage(c) : std::string("null connection");
    while (!e.empty() && (e.back() == '\n' || e.back() == ' ')) e.pop_back();
    return e;
}

bool exec_cmd(PGconn* c, const std::string& sql, std::string& err, uint64_t* affected = nullptr) {
    PGresult* r = PQexec(c, sql.c_str());
    const ExecStatusType st = r ? PQresultStatus(r) : PGRES_FATAL_ERROR;
    if (st != PGRES_COMMAND_OK && st != PGRES_TUPLES_OK) {
        err = conn_error(c) + " [sql: " + sql.substr(0, 240) + "]";
        if (r) PQclear(r);
        return false;
    }
    if (affected) {
        const char* n = PQcmdTuples(r);
        *affected = (n && *n) ? std::strtoull(n, nullptr, 10) : 0;
    }
    PQclear(r);
    return true;
}

bool query_rows(PGconn* c, const std::string& sql, rows_t& out, std::string& err) {
    out.clear();
    PGresult* r = PQexec(c, sql.c_str());
    if (!r || PQresultStatus(r) != PGRES_TUPLES_OK) {
        err = conn_error(c) + " [sql: " + sql.substr(0, 240) + "]";
        if (r) PQclear(r);
        return false;
    }
    const int nr = PQntuples(r);
    const int nf = PQnfields(r);
    out.reserve(static_cast<size_t>(nr));
    for (int i = 0; i < nr; ++i) {
        std::vector<std::string> row;
        row.reserve(static_cast<size_t>(nf));
        for (int j = 0; j < nf; ++j) {
            row.emplace_back(PQgetisnull(r, i, j) ? std::string() : std::string(PQgetvalue(r, i, j)));
        }
        out.push_back(std::move(row));
    }
    PQclear(r);
    return true;
}

std::string quote_ident(PGconn* c, const std::string& s) {
    char* q = PQescapeIdentifier(c, s.c_str(), s.size());
    std::string out = q ? std::string(q) : ("\"" + s + "\"");
    if (q) PQfreemem(q);
    return out;
}

std::vector<std::string> split_csv(const std::string& s) {
    std::vector<std::string> out;
    std::string cur;
    for (char ch : s) {
        if (ch == ',') {
            if (!cur.empty()) out.push_back(cur);
            cur.clear();
        } else {
            cur.push_back(ch);
        }
    }
    if (!cur.empty()) out.push_back(cur);
    return out;
}

bool hex_to_bytes(const std::string& hex, std::vector<uint8_t>& out) {
    out.clear();
    if (hex.size() % 2 != 0) return false;
    for (size_t i = 0; i < hex.size(); i += 2) {
        out.push_back(static_cast<uint8_t>(std::strtoul(hex.substr(i, 2).c_str(), nullptr, 16)));
    }
    return true;
}

std::string bytes_to_hex(const std::vector<uint8_t>& b) {
    static const char* k = "0123456789abcdef";
    std::string out;
    out.reserve(b.size() * 2);
    for (uint8_t v : b) {
        out.push_back(k[v >> 4]);
        out.push_back(k[v & 0xF]);
    }
    return out;
}

struct table_info {
    std::string name;             // unquoted relname (public schema)
    std::vector<std::string> pk;  // unquoted
    std::string merkle_key;       // single column, unquoted; empty if none/unsupported
    int partitions = 200;
    std::vector<std::string> cols; // unquoted, attnum order
};

const char* kTableDiscoverySql =
    "SELECT c.relname, "
    "  (SELECT string_agg(a.attname, ',' ORDER BY k.ord) "
    "     FROM pg_index i CROSS JOIN LATERAL unnest(i.indkey::int2[]) WITH ORDINALITY AS k(attnum, ord) "
    "     JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum "
    "    WHERE i.indrelid = c.oid AND i.indisprimary), "
    "  (SELECT string_agg(a.attname, ',' ORDER BY k.ord) "
    "     FROM pg_index i JOIN pg_class ic ON ic.oid = i.indexrelid JOIN pg_am am ON am.oid = ic.relam "
    "     CROSS JOIN LATERAL unnest(i.indkey::int2[]) WITH ORDINALITY AS k(attnum, ord) "
    "     JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum "
    "    WHERE i.indrelid = c.oid AND am.amname = 'merkle'), "
    "  (SELECT array_to_string(ic.reloptions, ',') "
    "     FROM pg_index i JOIN pg_class ic ON ic.oid = i.indexrelid JOIN pg_am am ON am.oid = ic.relam "
    "    WHERE i.indrelid = c.oid AND am.amname = 'merkle' LIMIT 1), "
    "  (SELECT string_agg(a.attname, ',' ORDER BY a.attnum) FROM pg_attribute a "
    "    WHERE a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped), "
    "  to_regclass('ariabc_internal.' || quote_ident('merkle_node_' || c.relname)) IS NOT NULL "
    "FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
    "WHERE n.nspname = 'public' AND c.relkind = 'r' ORDER BY c.relname";

bool discover_tables(PGconn* ref, std::vector<table_info>& out, std::string& err) {
    rows_t rows;
    if (!query_rows(ref, kTableDiscoverySql, rows, err)) return false;
    for (const auto& r : rows) {
        table_info t;
        t.name = r[0];
        t.pk = split_csv(r[1]);
        std::vector<std::string> mk = split_csv(r[2]);
        const bool node_table_exists = (r[5] == "t");
        if (mk.size() == 1 && node_table_exists && !t.pk.empty()) {
            t.merkle_key = mk[0];
        }
        for (const std::string& opt : split_csv(r[3])) {
            if (opt.rfind("partitions=", 0) == 0) {
                t.partitions = std::max(1, std::atoi(opt.c_str() + 11));
            }
        }
        t.cols = split_csv(r[4]);
        out.push_back(std::move(t));
    }
    return true;
}

std::string node_table(PGconn* c, const table_info& t) {
    return "ariabc_internal." + quote_ident(c, "merkle_node_" + t.name);
}

bool read_partition_roots(PGconn* c, const table_info& t,
                          std::map<int, std::string>& out, std::string& err) {
    out.clear();
    rows_t rows;
    if (!query_rows(c,
                    "SELECT partition_id, encode(hash, 'hex') FROM " + node_table(c, t) +
                        " WHERE prefix_len = 0",
                    rows, err)) {
        return false;
    }
    for (const auto& r : rows) out[std::atoi(r[0].c_str())] = r[1];
    return true;
}

struct leaf_key {
    int partition = 0;
    std::string node_hex;
    int prefix_len = 0;
    bool operator<(const leaf_key& o) const {
        if (partition != o.partition) return partition < o.partition;
        if (prefix_len != o.prefix_len) return prefix_len < o.prefix_len;
        return node_hex < o.node_hex;
    }
};

bool read_leaves(PGconn* c, const table_info& t, const std::string& part_array,
                 std::map<leaf_key, std::string>& out, std::string& err) {
    out.clear();
    rows_t rows;
    if (!query_rows(c,
                    "SELECT partition_id, encode(node_id, 'hex'), prefix_len, encode(hash, 'hex') FROM " +
                        node_table(c, t) + " WHERE is_leaf AND partition_id = ANY('" + part_array +
                        "'::int[])",
                    rows, err)) {
        return false;
    }
    for (const auto& r : rows) {
        leaf_key k;
        k.partition = std::atoi(r[0].c_str());
        k.node_hex = r[1];
        k.prefix_len = std::atoi(r[2].c_str());
        out[k] = r[3];
    }
    return true;
}

/* WITH-clause relation b(p, lo, hi) holding the differing leaf key ranges. */
std::string bounds_cte(const std::vector<leaf_key>& ranges) {
    std::ostringstream p, n, l;
    p << "ARRAY[";
    n << "ARRAY[";
    l << "ARRAY[";
    for (size_t i = 0; i < ranges.size(); ++i) {
        if (i) {
            p << ",";
            n << ",";
            l << ",";
        }
        p << ranges[i].partition;
        n << "decode('" << ranges[i].node_hex << "','hex')";
        l << ranges[i].prefix_len;
    }
    p << "]::int4[]";
    n << "]::bytea[]";
    l << "]::int4[]";
    return "WITH b(p, lo, hi) AS (SELECT u.p, u.lo, merkle_node_upper_bound(u.lo, u.l) FROM unnest(" +
           p.str() + ", " + n.str() + ", " + l.str() + ") AS u(p, lo, l)) ";
}

std::string range_pred(const std::string& key_expr, int partitions) {
    const std::string h = "merkle_key_hash(" + key_expr + ")";
    return "merkle_partition_for_hash(" + h + ", " + std::to_string(partitions) +
           ") = b.p AND " + h + " BETWEEN b.lo AND b.hi";
}

/* Stream `copy_out_sql` from ref into `copy_in_sql` on local. */
bool stream_copy(PGconn* ref, const std::string& copy_out_sql,
                 PGconn* local, const std::string& copy_in_sql,
                 uint64_t& rows, std::string& err) {
    rows = 0;
    PGresult* rin = PQexec(local, copy_in_sql.c_str());
    if (!rin || PQresultStatus(rin) != PGRES_COPY_IN) {
        err = "COPY IN failed: " + conn_error(local);
        if (rin) PQclear(rin);
        return false;
    }
    PQclear(rin);
    PGresult* rout = PQexec(ref, copy_out_sql.c_str());
    if (!rout || PQresultStatus(rout) != PGRES_COPY_OUT) {
        err = "COPY OUT failed: " + conn_error(ref);
        if (rout) PQclear(rout);
        PQputCopyEnd(local, "reference copy failed");
        while (PGresult* x = PQgetResult(local)) PQclear(x);
        return false;
    }
    PQclear(rout);
    bool ok = true;
    for (;;) {
        char* buf = nullptr;
        const int n = PQgetCopyData(ref, &buf, 0);
        if (n > 0) {
            ++rows;
            if (PQputCopyData(local, buf, n) != 1) {
                err = "PQputCopyData failed: " + conn_error(local);
                ok = false;
            }
            PQfreemem(buf);
            if (!ok) break;
            continue;
        }
        if (n == -2) {
            err = "PQgetCopyData failed: " + conn_error(ref);
            ok = false;
        }
        break;
    }
    while (PGresult* x = PQgetResult(ref)) {
        if (ok && PQresultStatus(x) != PGRES_COMMAND_OK) {
            err = "reference COPY failed: " + conn_error(ref);
            ok = false;
        }
        PQclear(x);
    }
    if (PQputCopyEnd(local, ok ? nullptr : "reference copy failed") != 1) {
        if (ok) err = "PQputCopyEnd failed: " + conn_error(local);
        ok = false;
    }
    while (PGresult* x = PQgetResult(local)) {
        if (ok && PQresultStatus(x) != PGRES_COMMAND_OK) {
            err = "local COPY failed: " + conn_error(local);
            ok = false;
        }
        PQclear(x);
    }
    return ok;
}

std::string join_quoted(PGconn* c, const std::vector<std::string>& cols,
                        const std::string& prefix) {
    std::string out;
    for (size_t i = 0; i < cols.size(); ++i) {
        if (i) out += ", ";
        out += prefix + quote_ident(c, cols[i]);
    }
    return out;
}

/* Merkle nodes of ordinary SQL are applied at pre-commit, so every repair
 * step commits before its result is verified. */
bool commit_or_rollback(PGconn* local, bool ok, std::string& err) {
    if (ok) return exec_cmd(local, "COMMIT", err);
    std::string ignored;
    exec_cmd(local, "ROLLBACK", ignored);
    return false;
}

bool full_table_copy(PGconn* ref, PGconn* local, const table_info& t,
                     replica_repair_stats& st, std::string& err) {
    const std::string tbl = "public." + quote_ident(local, t.name);
    uint64_t deleted = 0;
    uint64_t rows = 0;
    const std::string cols = join_quoted(local, t.cols, "");
    bool ok = exec_cmd(local, "BEGIN", err) && exec_cmd(local, "DELETE FROM " + tbl, err, &deleted) &&
              stream_copy(ref, "COPY (SELECT " + cols + " FROM " + tbl + ") TO STDOUT",
                          local, "COPY " + tbl + " (" + cols + ") FROM STDIN", rows, err);
    if (!commit_or_rollback(local, ok, err)) return false;
    st.rows_deleted += deleted;
    st.rows_upserted += rows;
    st.candidate_rows += rows;
    st.full_table_copies++;
    return true;
}

bool roots_equal(PGconn* ref, PGconn* local, const table_info& t, bool& equal, std::string& err) {
    std::map<int, std::string> a, b;
    if (!read_partition_roots(ref, t, a, err)) return false;
    if (!read_partition_roots(local, t, b, err)) return false;
    equal = (a == b);
    return true;
}

bool heap_verify_ok(PGconn* local, const table_info& t, bool& ok, std::string& err) {
    rows_t rows;
    if (!query_rows(local,
                    "SELECT merkle_verify(" + std::string("'public.") + t.name + "'::regclass)",
                    rows, err)) {
        return false;
    }
    ok = !rows.empty() && rows[0][0] == "t";
    return true;
}

bool repair_merkle_table(PGconn* ref, PGconn* local, const table_info& t, int table_no,
                         bool heap_verify, replica_repair_stats& st, std::string& err) {
    const uint64_t t0 = now_us();
    std::map<int, std::string> ref_roots, loc_roots;
    if (!read_partition_roots(ref, t, ref_roots, err)) return false;
    if (!read_partition_roots(local, t, loc_roots, err)) return false;

    std::set<int> parts;
    for (const auto& kv : ref_roots) {
        auto it = loc_roots.find(kv.first);
        if (it == loc_roots.end() || it->second != kv.second) parts.insert(kv.first);
    }
    for (const auto& kv : loc_roots) {
        if (!ref_roots.count(kv.first)) parts.insert(kv.first);
    }

    bool heap_ok = true;
    if (parts.empty()) {
        st.localise_us += now_us() - t0;
        if (heap_verify) {
            const uint64_t v0 = now_us();
            if (!heap_verify_ok(local, t, heap_ok, err)) return false;
            st.verify_us += now_us() - v0;
        }
        if (heap_ok) return true;
        /* Heap diverged from the (matching) Merkle nodes: copy the table. */
        st.tables_repaired++;
        return full_table_copy(ref, local, t, st, err);
    }

    st.tables_repaired++;
    st.mismatched_partitions += static_cast<int>(parts.size());

    std::string part_array = "{";
    bool first = true;
    for (int p : parts) {
        if (!first) part_array += ",";
        first = false;
        part_array += std::to_string(p);
    }
    part_array += "}";

    std::map<leaf_key, std::string> ref_leaves, loc_leaves;
    if (!read_leaves(ref, t, part_array, ref_leaves, err)) return false;
    if (!read_leaves(local, t, part_array, loc_leaves, err)) return false;

    std::vector<leaf_key> ranges;
    for (const auto& kv : ref_leaves) {
        auto it = loc_leaves.find(kv.first);
        if (it == loc_leaves.end() || it->second != kv.second) ranges.push_back(kv.first);
    }
    for (const auto& kv : loc_leaves) {
        auto it = ref_leaves.find(kv.first);
        if (it == ref_leaves.end()) ranges.push_back(kv.first);
    }
    /* Partition roots differ but no leaf does (should not happen): whole partitions. */
    if (ranges.empty()) {
        for (int p : parts) {
            leaf_key k;
            k.partition = p;
            k.node_hex = "0000000000000000";
            k.prefix_len = 0;
            ranges.push_back(k);
        }
    }
    st.differing_leaves += static_cast<int>(ranges.size());
    st.localise_us += now_us() - t0;

    /* Stream the reference rows of the differing ranges into a temp table. */
    const uint64_t x0 = now_us();
    const std::string tbl = "public." + quote_ident(local, t.name);
    const std::string tmp = "ariabc_repair_rows_" + std::to_string(table_no);
    const std::string key = quote_ident(local, t.merkle_key);
    const std::string cte = bounds_cte(ranges);
    const std::string pk_list = join_quoted(local, t.pk, "");
    const std::string pk_list_t = join_quoted(local, t.pk, "t.");
    const std::string cols = join_quoted(local, t.cols, "");

    if (!exec_cmd(local, "BEGIN", err)) return false;
    if (!exec_cmd(local, "CREATE TEMP TABLE " + tmp + " (LIKE " + tbl + ") ON COMMIT DROP", err)) {
        return commit_or_rollback(local, false, err);
    }
    uint64_t candidates = 0;
    const std::string copy_out =
        "COPY (" + cte + "SELECT DISTINCT ON (" + pk_list_t + ") " + join_quoted(local, t.cols, "t.") +
        " FROM b JOIN " + tbl + " t ON " + range_pred("t." + key, t.partitions) +
        " ORDER BY " + pk_list_t + ") TO STDOUT";
    if (!stream_copy(ref, copy_out, local, "COPY " + tmp + " (" + cols + ") FROM STDIN",
                     candidates, err)) {
        return commit_or_rollback(local, false, err);
    }
    st.candidate_rows += candidates;
    st.transfer_us += now_us() - x0;

    /* One set-oriented pass: drop extra rows, upsert differing rows. */
    const uint64_t a0 = now_us();
    std::string pk_match;
    for (size_t i = 0; i < t.pk.size(); ++i) {
        if (i) pk_match += " AND ";
        const std::string c = quote_ident(local, t.pk[i]);
        pk_match += "r." + c + " = t." + c;
    }
    uint64_t deleted = 0;
    if (!exec_cmd(local,
                  cte + "DELETE FROM " + tbl + " t USING b WHERE " + range_pred("t." + key, t.partitions) +
                      " AND NOT EXISTS (SELECT 1 FROM " + tmp + " r WHERE " + pk_match + ")",
                  err, &deleted)) {
        return commit_or_rollback(local, false, err);
    }
    std::vector<std::string> data_cols;
    for (const std::string& c : t.cols) {
        if (std::find(t.pk.begin(), t.pk.end(), c) == t.pk.end()) data_cols.push_back(c);
    }
    std::string upsert = "INSERT INTO " + tbl + " AS t (" + cols + ") SELECT " + cols + " FROM " + tmp +
                         " ON CONFLICT (" + pk_list + ") ";
    if (data_cols.empty()) {
        upsert += "DO NOTHING";
    } else {
        std::string set, lhs, rhs;
        for (size_t i = 0; i < data_cols.size(); ++i) {
            const std::string c = quote_ident(local, data_cols[i]);
            if (i) {
                set += ", ";
                lhs += ", ";
                rhs += ", ";
            }
            set += c + " = EXCLUDED." + c;
            lhs += "t." + c;
            rhs += "EXCLUDED." + c;
        }
        upsert += "DO UPDATE SET " + set + " WHERE (" + lhs + ") IS DISTINCT FROM (" + rhs + ")";
    }
    uint64_t upserted = 0;
    if (!exec_cmd(local, upsert, err, &upserted)) return commit_or_rollback(local, false, err);
    if (!commit_or_rollback(local, true, err)) return false;
    st.rows_deleted += deleted;
    st.rows_upserted += upserted;
    st.apply_us += now_us() - a0;

    /* The table must now match the snapshot exactly. */
    const uint64_t v0 = now_us();
    bool equal = false;
    if (!roots_equal(ref, local, t, equal, err)) return false;
    if (equal && heap_verify) {
        if (!heap_verify_ok(local, t, heap_ok, err)) return false;
    }
    st.verify_us += now_us() - v0;
    if (equal && heap_ok) return true;

    if (!full_table_copy(ref, local, t, st, err)) return false;
    if (!roots_equal(ref, local, t, equal, err)) return false;
    if (!equal) {
        err = "table " + t.name + " still differs from the snapshot after a full copy";
        return false;
    }
    return true;
}

bool checksum(PGconn* c, const table_info& t, std::string& out, std::string& err) {
    const std::string tbl = "public." + quote_ident(c, t.name);
    const std::string order = t.pk.empty() ? std::string("x::text") : join_quoted(c, t.pk, "x.");
    rows_t rows;
    if (!query_rows(c,
                    "SELECT count(*)::text || ':' || coalesce(md5(string_agg(md5(x::text), '' ORDER BY " +
                        order + ")), '') FROM " + tbl + " x",
                    rows, err)) {
        return false;
    }
    out = rows.empty() ? std::string() : rows[0][0];
    return true;
}

} // namespace

PGconn* open_snapshot_reader(const std::string& conninfo,
                             const std::string& snapshot_id,
                             std::string& err) {
    for (char ch : snapshot_id) {
        if (!(std::isxdigit(static_cast<unsigned char>(ch)) || ch == '-')) {
            err = "invalid snapshot id";
            return nullptr;
        }
    }
    PGconn* c = PQconnectdb(conninfo.c_str());
    if (!c || PQstatus(c) != CONNECTION_OK) {
        err = "connect failed: " + conn_error(c);
        if (c) PQfinish(c);
        return nullptr;
    }
    if (!exec_cmd(c, "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY", err) ||
        !exec_cmd(c, "SET TRANSACTION SNAPSHOT '" + snapshot_id + "'", err)) {
        PQfinish(c);
        return nullptr;
    }
    return c;
}

bool compute_merkle_digest(PGconn* c, std::string& out_digest, std::string& err) {
    std::vector<table_info> tables;
    if (!discover_tables(c, tables, err)) return false;
    std::ostringstream out;
    bool first = true;
    for (const table_info& t : tables) {
        if (t.merkle_key.empty()) continue;
        std::map<int, std::string> roots;
        if (!read_partition_roots(c, t, roots, err)) return false;
        std::vector<uint8_t> acc;
        for (const auto& kv : roots) {
            std::vector<uint8_t> h;
            if (!hex_to_bytes(kv.second, h)) continue;
            if (acc.size() < h.size()) acc.resize(h.size(), 0);
            for (size_t i = 0; i < h.size(); ++i) acc[i] ^= h[i];
        }
        if (!first) out << ",";
        first = false;
        out << t.name << ":" << bytes_to_hex(acc);
    }
    out_digest = out.str();
    return true;
}

bool repair_replica_from_snapshot(const std::string& local_conninfo,
                                  const std::string& ref_conninfo,
                                  const std::string& snapshot_id,
                                  bool heap_verify,
                                  replica_repair_stats& st,
                                  std::string& err) {
    const uint64_t t0 = now_us();
    PGconn* ref = open_snapshot_reader(ref_conninfo, snapshot_id, err);
    if (!ref) {
        err = "reference: " + err;
        return false;
    }
    PGconn* local = PQconnectdb(local_conninfo.c_str());
    if (!local || PQstatus(local) != CONNECTION_OK) {
        err = "local connect failed: " + conn_error(local);
        if (local) PQfinish(local);
        PQfinish(ref);
        return false;
    }

    bool ok = exec_cmd(local, "SET default_transaction_isolation = 'read committed'", err) &&
              exec_cmd(local, "SET session_replication_role = replica", err) &&
              exec_cmd(local, "SET enable_merkle_index = on", err) &&
              exec_cmd(local,
                       "DO $$ BEGIN "
                       "  IF NOT EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'merkle_node_upper_bound' AND pronamespace = 'pg_catalog'::regnamespace) THEN "
                       "    CREATE FUNCTION pg_catalog.merkle_node_upper_bound(node_id bytea, prefix_len integer) "
                       "    RETURNS bytea LANGUAGE internal IMMUTABLE STRICT AS 'merkle_node_upper_bound_sql'; "
                       "  END IF; "
                       "  IF NOT EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'merkle_partition_for_hash' AND pronamespace = 'pg_catalog'::regnamespace) THEN "
                       "    CREATE FUNCTION pg_catalog.merkle_partition_for_hash(key_hash bytea, partitions integer) "
                       "    RETURNS smallint LANGUAGE internal IMMUTABLE STRICT AS 'merkle_partition_for_hash'; "
                       "  END IF; "
                       "  IF NOT EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'merkle_key_hash' AND pronamespace = 'pg_catalog'::regnamespace) THEN "
                       "    CREATE FUNCTION pg_catalog.merkle_key_hash(anyelement) "
                       "    RETURNS bytea LANGUAGE internal IMMUTABLE STRICT AS 'merkle_key_hash_sql'; "
                       "  END IF; "
                       "END $$; "
                       "CREATE OR REPLACE FUNCTION public.merkle_node_upper_bound(node_id bytea, prefix_len integer) "
                       "RETURNS bytea LANGUAGE internal IMMUTABLE STRICT AS 'merkle_node_upper_bound_sql'; "
                       "CREATE OR REPLACE FUNCTION public.merkle_partition_for_hash(key_hash bytea, partitions integer) "
                       "RETURNS smallint LANGUAGE internal IMMUTABLE STRICT AS 'merkle_partition_for_hash'; "
                       "CREATE OR REPLACE FUNCTION public.merkle_key_hash(anyelement) "
                       "RETURNS bytea LANGUAGE internal IMMUTABLE STRICT AS 'merkle_key_hash_sql';",
                       err);

    std::vector<table_info> tables;
    if (ok) ok = discover_tables(ref, tables, err);

    int table_no = 0;
    for (size_t i = 0; ok && i < tables.size(); ++i) {
        const table_info& t = tables[i];
        /*
         * A plain table that exists only on the reference (e.g. left over from
         * another benchmark) is not replicated state; skip it.  A Merkle table
         * is always replicated, so a missing one is a repair failure.
         */
        if (t.merkle_key.empty()) {
            std::string escaped;
            for (char ch : t.name) {
                escaped += ch;
                if (ch == '\'') escaped += ch;
            }
            rows_t chk;
            ok = query_rows(local, "SELECT to_regclass(format('public.%I', '" + escaped + "')) IS NOT NULL",
                            chk, err);
            if (!ok) {
                err = "table " + t.name + ": " + err;
                break;
            }
            if (chk.empty() || chk[0][0] != "t") {
                std::cerr << "REPLICA_REPAIR skip_table=" << t.name
                          << " reason=absent_on_local non_merkle=1" << std::endl;
                continue;
            }
        }

        st.tables_checked++;
        if (!t.merkle_key.empty()) {
            ok = repair_merkle_table(ref, local, t, table_no++, heap_verify, st, err);
            if (!ok) err = "table " + t.name + ": " + err;
            continue;
        }
        std::string a, b;
        ok = checksum(ref, t, a, err) && checksum(local, t, b, err);
        if (ok && a != b) {
            st.tables_repaired++;
            ok = full_table_copy(ref, local, t, st, err) && checksum(local, t, b, err);
            if (ok && a != b) {
                err = "checksum still differs after full copy";
                ok = false;
            }
        }
        if (!ok) err = "table " + t.name + ": " + err;
    }

    if (ok) ok = compute_merkle_digest(local, st.digest_after, err);
    {
        std::string ignored;
        exec_cmd(ref, "ROLLBACK", ignored);
    }
    PQfinish(local);
    PQfinish(ref);
    st.total_us = now_us() - t0;
    return ok;
}

} // namespace ariabc_pg
