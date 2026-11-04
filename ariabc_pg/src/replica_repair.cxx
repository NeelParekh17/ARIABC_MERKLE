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
#include <utility>
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
    int partition_key_columns = 0; // Merkle leading-key routing; 0 = hash(key) % partitions
    int subpartitions = 1;
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
            } else if (opt.rfind("partition_key_columns=", 0) == 0) {
                t.partition_key_columns = std::max(0, std::atoi(opt.c_str() + 22));
            } else if (opt.rfind("subpartitions=", 0) == 0) {
                t.subpartitions = std::max(1, std::atoi(opt.c_str() + 14));
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
    const std::string tbl = "public." + quote_ident(c, t.name);
    char* literal = PQescapeLiteral(c, tbl.c_str(), tbl.size());
    if (!literal) {
        err = conn_error(c);
        return false;
    }
    // The native helper checks the index format before returning roots.
    const std::string sql =
        "SELECT roots.partition, roots.hash FROM pg_index i "
        "JOIN pg_class ic ON ic.oid = i.indexrelid "
        "JOIN pg_am am ON am.oid = ic.relam "
        "CROSS JOIN LATERAL pg_catalog.merkle_get_partition_root_hashes(i.indexrelid) roots "
        "WHERE i.indrelid = " + std::string(literal) + "::regclass AND am.amname = 'merkle'";
    PQfreemem(literal);
    if (!query_rows(c,
                    sql,
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

/* unnest(...) AS u(p, lo, l) over node coordinates. */
std::string unnest_nodes(const std::vector<leaf_key>& ranges) {
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
    return "unnest(" + p.str() + ", " + n.str() + ", " + l.str() + ") AS u(p, lo, l)";
}

/* WITH-clause relation b(i, p, lo, hi) holding the differing leaf key ranges;
 * i numbers them 1..n in vector order. */
std::string bounds_cte(const std::vector<leaf_key>& ranges) {
    std::string u = unnest_nodes(ranges);
    const std::string alias = " AS u(p, lo, l)";
    u = u.substr(0, u.size() - alias.size()) + " WITH ORDINALITY AS u(p, lo, l, i)";
    return "WITH b(i, p, lo, hi) AS (SELECT u.i::int4, u.p, u.lo, merkle_node_upper_bound(u.lo, u.l) FROM " +
           u + ") ";
}

struct node_row {
    bool is_leaf = false;
    std::string hash;
    leaf_key parent;
};

/* Nodes child_bits below each parent (child_bits = 0 reads the parents themselves).
 * The LATERAL probe keeps each parent on the (partition_id, prefix_len, node_id) index. */
bool read_children(PGconn* c, const table_info& t, const std::vector<leaf_key>& parents,
                   int child_bits, std::map<leaf_key, node_row>& out, std::string& err) {
    out.clear();
    rows_t rows;
    const std::string bits = std::to_string(child_bits);
    if (!query_rows(c,
                    "SELECT n.partition_id, encode(n.node_id, 'hex'), n.prefix_len, n.is_leaf, "
                    "encode(n.hash, 'hex'), u.p, encode(u.lo, 'hex'), u.l FROM " +
                        unnest_nodes(parents) + " CROSS JOIN LATERAL (SELECT * FROM " +
                        node_table(c, t) +
                        " n WHERE n.partition_id = u.p::int2 AND n.prefix_len = (u.l + " + bits +
                        ")::int2 AND n.node_id BETWEEN u.lo AND merkle_node_upper_bound(u.lo, u.l)) n",
                    rows, err)) {
        return false;
    }
    for (const auto& r : rows) {
        leaf_key k;
        k.partition = std::atoi(r[0].c_str());
        k.node_hex = r[1];
        k.prefix_len = std::atoi(r[2].c_str());
        node_row& nr = out[k];
        nr.is_leaf = (r[3] == "t");
        nr.hash = r[4];
        nr.parent.partition = std::atoi(r[5].c_str());
        nr.parent.node_hex = r[6];
        nr.parent.prefix_len = std::atoi(r[7].c_str());
    }
    return true;
}

/* Prefix bits per tree level: the shallowest non-root level of a split partition. */
bool read_split_bits(PGconn* c, const table_info& t, int partition, int& bits, std::string& err) {
    rows_t rows;
    if (!query_rows(c,
                    "SELECT min(prefix_len) FROM " + node_table(c, t) + " WHERE partition_id = " +
                        std::to_string(partition) + " AND prefix_len > 0",
                    rows, err)) {
        return false;
    }
    bits = (rows.empty() || rows[0][0].empty()) ? 0 : std::atoi(rows[0][0].c_str());
    return true;
}

/*
 * Localise the differing ranges by descending from the differing partition
 * roots: at each level read the children of the differing internal nodes on
 * both sides and follow only children whose hashes differ.  A differing node
 * that is a leaf, or missing, on either side becomes a repair range; its key
 * range also covers whatever subtree the other side has there.
 */
bool descend_differing(PGconn* ref, PGconn* local, const table_info& t, const std::set<int>& parts,
                       std::vector<leaf_key>& ranges, replica_repair_stats& st, std::string& err) {
    std::vector<leaf_key> frontier;
    for (int p : parts) {
        leaf_key k;
        k.partition = p;
        k.node_hex = "0000000000000000";
        k.prefix_len = 0;
        frontier.push_back(k);
    }
    int bits = 0; /* first pass reads the roots themselves */
    while (!frontier.empty()) {
        std::map<leaf_key, node_row> ref_nodes, loc_nodes;
        if (!read_children(ref, t, frontier, bits, ref_nodes, err)) return false;
        if (!read_children(local, t, frontier, bits, loc_nodes, err)) return false;
        st.descent_levels++;
        st.nodes_compared += static_cast<int>(ref_nodes.size() + loc_nodes.size());

        /* An internal node with no children on one side cannot be compared below: repair it whole. */
        std::set<leaf_key> ref_parents, loc_parents;
        for (const auto& kv : ref_nodes) ref_parents.insert(kv.second.parent);
        for (const auto& kv : loc_nodes) loc_parents.insert(kv.second.parent);
        std::set<leaf_key> whole;
        for (const leaf_key& f : frontier) {
            if (!ref_parents.count(f) || !loc_parents.count(f)) {
                whole.insert(f);
                ranges.push_back(f);
            }
        }

        std::set<leaf_key> keys;
        for (const auto& kv : ref_nodes) keys.insert(kv.first);
        for (const auto& kv : loc_nodes) keys.insert(kv.first);
        std::vector<leaf_key> next;
        for (const leaf_key& k : keys) {
            auto r = ref_nodes.find(k);
            auto l = loc_nodes.find(k);
            const leaf_key& parent = (r != ref_nodes.end() ? r->second : l->second).parent;
            if (whole.count(parent)) continue;
            if (r != ref_nodes.end() && l != loc_nodes.end() && r->second.hash == l->second.hash) continue;
            if (r == ref_nodes.end() || l == loc_nodes.end() || r->second.is_leaf || l->second.is_leaf) {
                ranges.push_back(k);
            } else {
                next.push_back(k);
            }
        }
        if (next.empty()) break;
        if (bits == 0) {
            int ref_bits = 0, loc_bits = 0;
            if (!read_split_bits(ref, t, next.front().partition, ref_bits, err)) return false;
            if (!read_split_bits(local, t, next.front().partition, loc_bits, err)) return false;
            if (ref_bits <= 0 || ref_bits != loc_bits) {
                err = "merkle level width differs or is unknown for " + t.name + " (reference " +
                      std::to_string(ref_bits) + ", local " + std::to_string(loc_bits) + ")";
                return false;
            }
            bits = ref_bits;
        }
        /* Node ids are 64 bits; a deeper level cannot exist. */
        if (next.front().prefix_len + bits > 64) {
            ranges.insert(ranges.end(), next.begin(), next.end());
            break;
        }
        frontier.swap(next);
    }
    return true;
}

/* Route hash of a single-column Merkle key, as merkle_compute_route() computes it. */
std::string route_hash_expr(const table_info& t, const std::string& key_expr) {
    if (t.partition_key_columns > 0)
        return "merkle_key_hash_routed(ROW(" + key_expr + "), ROW(" + key_expr + "), " +
               std::to_string(t.partitions) + ", " + std::to_string(t.subpartitions) + ")";
    return "merkle_key_hash(" + key_expr + ")";
}

std::string range_pred(const table_info& t, const std::string& key_expr) {
    const int partitions = t.partitions;
    const std::string h = route_hash_expr(t, key_expr);
    return "merkle_partition_for_hash(" + h + ", " + std::to_string(partitions) +
           ") = b.p AND " + h + " BETWEEN b.lo AND b.hi";
}

/* One row's identity for repair: (Merkle partition, route hash, tuple hash), hex. */
struct row_pair {
    int partition;
    std::string key_hash_hex;
    std::string tuple_hash_hex;
    row_pair(int p, const std::string& kh, const std::string& th)
        : partition(p), key_hash_hex(kh), tuple_hash_hex(th) {}
    bool operator<(const row_pair& o) const {
        if (partition != o.partition) return partition < o.partition;
        if (key_hash_hex != o.key_hash_hex) return key_hash_hex < o.key_hash_hex;
        return tuple_hash_hex < o.tuple_hash_hex;
    }
};

/* WITH-clause relation d(p, kh, th) holding the differing row pairs. */
std::string pairs_cte(const std::vector<row_pair>& pairs) {
    std::ostringstream p, k, h;
    p << "ARRAY[";
    k << "ARRAY[";
    h << "ARRAY[";
    for (size_t i = 0; i < pairs.size(); ++i) {
        if (i) {
            p << ",";
            k << ",";
            h << ",";
        }
        p << pairs[i].partition;
        k << "decode('" << pairs[i].key_hash_hex << "','hex')";
        h << "decode('" << pairs[i].tuple_hash_hex << "','hex')";
    }
    p << "]::int2[]";
    k << "]::bytea[]";
    h << "]::bytea[]";
    return "WITH d(p, kh, th) AS (SELECT u.p, u.kh, u.th FROM unnest(" + p.str() + ", " + k.str() +
           ", " + h.str() + ") AS u(p, kh, th)) ";
}

/* WITH-clause relation f(p, lo, hi, th): a differing tuple hash and the leaf
 * range it was listed in.  Sent to the reference, which never sees key hashes. */
std::string fetch_cte(const std::vector<leaf_key>& ranges,
                      const std::vector<std::pair<int, std::string>>& wanted) {
    std::ostringstream p, n, l, h;
    p << "ARRAY[";
    n << "ARRAY[";
    l << "ARRAY[";
    h << "ARRAY[";
    for (size_t i = 0; i < wanted.size(); ++i) {
        const leaf_key& r = ranges[static_cast<size_t>(wanted[i].first - 1)];
        if (i) {
            p << ",";
            n << ",";
            l << ",";
            h << ",";
        }
        p << r.partition;
        n << "decode('" << r.node_hex << "','hex')";
        l << r.prefix_len;
        h << "decode('" << wanted[i].second << "','hex')";
    }
    p << "]::int4[]";
    n << "]::bytea[]";
    l << "]::int4[]";
    h << "]::bytea[]";
    return "WITH f(p, lo, hi, th) AS (SELECT u.p, u.lo, merkle_node_upper_bound(u.lo, u.l), u.th FROM unnest(" +
           p.str() + ", " + n.str() + ", " + l.str() + ", " + h.str() + ") AS u(p, lo, l, th)) ";
}

/* Row t lies in f's range and has f's tuple hash: one lookup-index range probe. */
std::string fetch_pred(const table_info& t, const std::string& key_expr) {
    const std::string h = route_hash_expr(t, key_expr);
    return "merkle_partition_for_hash(" + h + ", " + std::to_string(t.partitions) + ") = f.p AND " + h +
           " BETWEEN f.lo AND f.hi AND merkle_tuple_hash(t.*) = f.th";
}

/* Hex of each 8-byte tuple hash packed in a binary bytea value. */
bool split_tuple_hashes(const char* data, int len, std::vector<std::string>& out) {
    static const char* k = "0123456789abcdef";
    if (len < 0 || len % 8 != 0) return false;
    for (int off = 0; off < len; off += 8) {
        std::string hex(16, '0');
        for (int j = 0; j < 8; ++j) {
            const unsigned char v = static_cast<unsigned char>(data[off + j]);
            hex[2 * j] = k[v >> 4];
            hex[2 * j + 1] = k[v & 0xF];
        }
        out.push_back(hex);
    }
    return true;
}

/* Row t matches pair d: the three lookup-index columns, so it is an index probe. */
std::string pair_pred(const table_info& t, const std::string& key_expr) {
    const std::string h = route_hash_expr(t, key_expr);
    return "merkle_partition_for_hash(" + h + ", " + std::to_string(t.partitions) + ") = d.p AND " + h +
           " = d.kh AND merkle_tuple_hash(t.*) = d.th";
}

/* The planner overestimates rows per leaf range (~2k vs ~80 at 4M rows) and
 * prefers a hash join over a full-table walk to ~100 parameterized scans of
 * the Merkle lookup index (3.5 s vs 40 ms). Steer only the range-join
 * statements; the caller is inside a transaction and restores the defaults after. */
bool force_range_index_join(PGconn* c, std::string& err) {
    return exec_cmd(c, "SET LOCAL enable_hashjoin = off", err) &&
           exec_cmd(c, "SET LOCAL enable_mergejoin = off", err);
}

bool restore_join_planning(PGconn* c, std::string& err) {
    return exec_cmd(c, "SET LOCAL enable_hashjoin TO DEFAULT", err) &&
           exec_cmd(c, "SET LOCAL enable_mergejoin TO DEFAULT", err);
}

/* Collect a connection's results after COPY. If it is still mid-COPY (e.g.
 * PQgetCopyData failed with -2), PQgetResult keeps returning PGRES_COPY_OUT/IN,
 * so report failure instead of looping. */
bool drain_copy_results(PGconn* c, const char* what, bool& ok, std::string& err) {
    while (PGresult* x = PQgetResult(c)) {
        const ExecStatusType st = PQresultStatus(x);
        PQclear(x);
        if (st == PGRES_COPY_OUT || st == PGRES_COPY_IN || st == PGRES_COPY_BOTH) {
            if (ok) err = std::string(what) + "connection still in COPY: " + conn_error(c);
            ok = false;
            return false;
        }
        if (ok && st != PGRES_COMMAND_OK) {
            err = std::string(what) + conn_error(c);
            ok = false;
        }
    }
    return true;
}

/* Stream `copy_out_sql` from ref into `copy_in_sql` on local. */
bool stream_copy(PGconn* ref, const std::string& copy_out_sql,
                 PGconn* local, const std::string& copy_in_sql,
                 uint64_t& rows, std::string& err, uint64_t* bytes = nullptr) {
    rows = 0;
    if (bytes) *bytes = 0;
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
        bool ignored_ok = false;
        std::string ignored;
        if (PQputCopyEnd(local, "reference copy failed") == 1)
            drain_copy_results(local, "", ignored_ok, ignored);
        return false;
    }
    PQclear(rout);
    bool ok = true;
    for (;;) {
        char* buf = nullptr;
        const int n = PQgetCopyData(ref, &buf, 0);
        if (n > 0) {
            ++rows;
            if (bytes) *bytes += static_cast<uint64_t>(n);
            if (ok && PQputCopyData(local, buf, n) != 1) {
                err = "PQputCopyData failed: " + conn_error(local);
                ok = false;
            }
            PQfreemem(buf);
            /* On a local failure keep draining: the reference stays in COPY OUT
             * (and PQgetResult keeps returning PGRES_COPY_OUT) until it is consumed. */
            continue;
        }
        if (n == -2) {
            err = "PQgetCopyData failed: " + conn_error(ref);
            ok = false;
        }
        break;
    }
    if (!drain_copy_results(ref, "reference COPY failed: ", ok, err)) return false;
    if (PQputCopyEnd(local, ok ? nullptr : "reference copy failed") != 1) {
        /* Still in COPY IN: collecting results would loop. Fail; the caller's
         * connections are closed, which rolls the transaction back. */
        if (ok) err = "PQputCopyEnd failed: " + conn_error(local);
        return false;
    }
    if (!drain_copy_results(local, "local COPY failed: ", ok, err)) return false;
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

bool roots_equal(PGconn* ref, PGconn* local, const table_info& t, bool& equal, std::string& err) {
    std::map<int, std::string> a, b;
    if (!read_partition_roots(ref, t, a, err)) return false;
    if (!read_partition_roots(local, t, b, err)) return false;
    equal = (a == b);
    return true;
}

bool heap_verify_ok(PGconn* local, const table_info& t, bool& ok, std::string& err) {
    rows_t rows;
    const std::string tbl = "public." + quote_ident(local, t.name);
    char* literal = PQescapeLiteral(local, tbl.c_str(), tbl.size());
    if (!literal) {
        err = conn_error(local);
        return false;
    }
    const std::string statement = "SELECT merkle_verify(" + std::string(literal) + "::regclass)";
    PQfreemem(literal);
    if (!query_rows(local,
                    statement,
                    rows, err)) {
        return false;
    }
    ok = !rows.empty() && rows[0][0] == "t";
    return true;
}

bool rebuild_merkle_indexes(PGconn* local, const table_info& t, std::string& err) {
    const std::string tbl = "public." + quote_ident(local, t.name);
    char* literal = PQescapeLiteral(local, tbl.c_str(), tbl.size());
    if (!literal) {
        err = conn_error(local);
        return false;
    }
    const std::string statement =
        "SELECT n.nspname, c.relname FROM pg_index i "
        "JOIN pg_class c ON c.oid = i.indexrelid "
        "JOIN pg_namespace n ON n.oid = c.relnamespace "
        "JOIN pg_am am ON am.oid = c.relam "
        "WHERE i.indrelid = " + std::string(literal) + "::regclass "
        "AND am.amname = 'merkle' ORDER BY c.oid";
    PQfreemem(literal);
    rows_t indexes;
    if (!query_rows(local, statement, indexes, err)) return false;
    if (indexes.empty()) {
        if (t.merkle_key.empty()) return true;
        err = "Merkle index missing before full copy";
        return false;
    }

    if (!exec_cmd(local, "BEGIN", err)) return false;
    for (const auto& index : indexes) {
        if (!exec_cmd(local, "REINDEX INDEX " + quote_ident(local, index[0]) + "." +
                          quote_ident(local, index[1]), err)) {
            return commit_or_rollback(local, false, err);
        }
    }
    return commit_or_rollback(local, true, err);
}

bool full_table_copy(PGconn* ref, PGconn* local, const table_info& t,
                     replica_repair_stats& st, std::string& err) {
    /* Re-anchor incremental maintenance to the actual heap before copying.
     * DELETE + COPY coalesces changes at commit and cannot remove existing
     * errors in leaf summaries. Native REINDEX must run in its own transaction
     * before DML; rebuilding first also avoids committing restored rows with
     * stale metadata if the rebuild fails or recovery is interrupted. */
    if (!rebuild_merkle_indexes(local, t, err)) return false;
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

    if (!t.merkle_key.empty()) {
        const uint64_t v0 = now_us();
        bool equal = false;
        bool heap_ok = false;
        if (!roots_equal(ref, local, t, equal, err) ||
            !heap_verify_ok(local, t, heap_ok, err)) return false;
        st.verify_us += now_us() - v0;
        if (!equal || !heap_ok) {
            err = "table " + t.name + " failed verification after full copy and Merkle rebuild";
            return false;
        }
    }
    return true;
}

/* True when `tbl_lit` (an SQL literal) has a tuple-hash lookup index.  A
 * server without the lookup functions (older build) has none. */
bool has_tuplehash_lookup(PGconn* c, const std::string& tbl_lit, bool& out, std::string& err) {
    out = false;
    rows_t rows;
    if (!query_rows(c,
                    "SELECT to_regprocedure('pg_catalog.merkle_lookup_index(regclass)') IS NOT NULL AND "
                    "to_regprocedure('pg_catalog.merkle_range_entries(regclass,integer,bytea,bytea)') IS NOT NULL",
                    rows, err)) return false;
    if (rows.empty() || rows[0][0] != "t") return true;
    rows.clear();
    if (!query_rows(c, "SELECT pg_catalog.merkle_lookup_index(" + tbl_lit + "::regclass) IS NOT NULL", rows, err))
        return false;
    out = !rows.empty() && rows[0][0] == "t";
    return true;
}

/* Key-hash lookup mode: stream every reference row of the differing leaf
 * ranges into a temp table, delete local range rows absent from it, and
 * upsert the rest by primary key.  Range probes use whatever index covers
 * (partition, route hash): the key-hash lookup index or the planner's choice. */
bool repair_ranges_full_leaf(PGconn* ref, PGconn* local, const table_info& t, int table_no,
                             const std::vector<leaf_key>& ranges, replica_repair_stats& st,
                             std::string& err) {
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
    uint64_t bytes = 0;
    const std::string copy_out =
        "COPY (" + cte + "SELECT DISTINCT ON (" + pk_list_t + ") " + join_quoted(local, t.cols, "t.") +
        " FROM b JOIN " + tbl + " t ON " + range_pred(t, "t." + key) +
        " ORDER BY " + pk_list_t + ") TO STDOUT";
    if (!force_range_index_join(ref, err) ||
        !stream_copy(ref, copy_out, local, "COPY " + tmp + " (" + cols + ") FROM STDIN",
                     candidates, err, &bytes) ||
        !restore_join_planning(ref, err)) {
        return commit_or_rollback(local, false, err);
    }
    st.candidate_rows += candidates;
    st.ref_row_bytes += bytes;
    st.transfer_us += now_us() - x0;

    /* One set-oriented pass: drop extra rows, upsert differing rows. With hash
     * and merge joins off, the DELETE's NOT EXISTS probes the candidate rows per
     * deleted-range row, so they need a primary-key index (else it is quadratic). */
    const uint64_t a0 = now_us();
    if (!t.pk.empty() &&
        (!exec_cmd(local, "CREATE INDEX ON " + tmp + " (" + pk_list + ")", err) ||
         !exec_cmd(local, "ANALYZE " + tmp, err))) {
        return commit_or_rollback(local, false, err);
    }
    std::string pk_match;
    for (size_t i = 0; i < t.pk.size(); ++i) {
        if (i) pk_match += " AND ";
        const std::string c = quote_ident(local, t.pk[i]);
        pk_match += "r." + c + " = t." + c;
    }
    uint64_t deleted = 0;
    if (!force_range_index_join(local, err) ||
        !exec_cmd(local,
                  cte + "DELETE FROM " + tbl + " t USING b WHERE " + range_pred(t, "t." + key) +
                      " AND NOT EXISTS (SELECT 1 FROM " + tmp + " r WHERE " + pk_match + ")",
                  err, &deleted) ||
        !restore_join_planning(local, err)) {
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
    return true;
}

/* Tuple-hash lookup mode, per differing leaf range (localised by key hash in
 * the descent).  The reference sends only its tuple hashes, packed as one
 * binary bytea per range (8 bytes per row); the local side lists (key hash,
 * tuple hash) from its own lookup index without any network.  Both read
 * merkle_range_entries, which serves them from the lookup index.  A tuple
 * hash whose count differs between the sides marks bad rows: local copies are
 * deleted by (key hash, tuple hash), and the reference copies are fetched by
 * (range, tuple hash).  A row with the same key but different content shows
 * up as two differing tuple hashes. */
bool repair_ranges_by_tuple_hash(PGconn* ref, PGconn* local, const table_info& t, int table_no,
                                 const std::vector<leaf_key>& ranges, const std::string& tbl_lit,
                                 replica_repair_stats& st, std::string& err) {
    const uint64_t x0 = now_us();
    const std::string tbl = "public." + quote_ident(local, t.name);
    const std::string tmp = "ariabc_repair_rows_" + std::to_string(table_no);
    const std::string key = quote_ident(local, t.merkle_key);
    const std::string cte = bounds_cte(ranges);
    const std::string cols = join_quoted(local, t.cols, "");
    const std::string lateral = " FROM b CROSS JOIN LATERAL pg_catalog.merkle_range_entries(" +
                                tbl_lit + "::regclass, b.p, b.lo, b.hi) e";
    const std::string ref_sql = cte + "SELECT b.i, string_agg(e.tuple_hash, ''::bytea)" + lateral + " GROUP BY b.i";
    const std::string local_sql = cte + "SELECT b.i, encode(e.key_hash, 'hex'), encode(e.tuple_hash, 'hex')" + lateral;

    std::map<std::pair<int, std::string>, long> balance;  // (range, tuple hash): ref count - local count
    std::set<std::pair<int, std::string>> on_ref;         // (range, tuple hash) listed by the reference
    {
        PGresult* r = PQexecParams(ref, ref_sql.c_str(), 0, nullptr, nullptr, nullptr, nullptr, 1);
        if (!r || PQresultStatus(r) != PGRES_TUPLES_OK) {
            err = conn_error(ref) + " [sql: " + ref_sql.substr(0, 240) + "]";
            if (r) PQclear(r);
            return false;
        }
        for (int row = 0; row < PQntuples(r); ++row) {
            if (PQgetlength(r, row, 0) != 4 || PQgetisnull(r, row, 1)) {
                PQclear(r);
                err = "malformed reference tuple-hash list";
                return false;
            }
            const unsigned char* ib = reinterpret_cast<const unsigned char*>(PQgetvalue(r, row, 0));
            const int range_no = static_cast<int>((static_cast<uint32_t>(ib[0]) << 24) |
                                                  (static_cast<uint32_t>(ib[1]) << 16) |
                                                  (static_cast<uint32_t>(ib[2]) << 8) | ib[3]);
            std::vector<std::string> hashes;
            if (range_no < 1 || range_no > static_cast<int>(ranges.size()) ||
                !split_tuple_hashes(PQgetvalue(r, row, 1), PQgetlength(r, row, 1), hashes)) {
                PQclear(r);
                err = "malformed reference tuple-hash list";
                return false;
            }
            st.ref_hash_bytes += static_cast<uint64_t>(PQgetlength(r, row, 1));
            st.entries_compared += hashes.size();
            for (const std::string& h : hashes) {
                balance[std::make_pair(range_no, h)]++;
                on_ref.insert(std::make_pair(range_no, h));
            }
        }
        PQclear(r);
    }
    rows_t local_entries;
    if (!query_rows(local, local_sql, local_entries, err)) return false;
    st.entries_compared += local_entries.size();
    for (const auto& e : local_entries) balance[std::make_pair(std::atoi(e[0].c_str()), e[2])]--;

    /* Every local copy of a differing tuple hash is deleted and every
     * reference copy fetched (duplicate rows keep their multiplicity). */
    std::vector<std::pair<int, std::string>> wanted;  // differing and listed by the reference
    std::set<std::pair<int, std::string>> differing;
    for (const auto& kv : balance) {
        if (kv.second == 0) continue;
        differing.insert(kv.first);
        if (on_ref.count(kv.first)) wanted.push_back(kv.first);
    }
    on_ref.clear();
    std::set<row_pair> bad_local;
    for (const auto& e : local_entries) {
        const int range_no = std::atoi(e[0].c_str());
        if (differing.count(std::make_pair(range_no, e[2])))
            bad_local.insert(row_pair(ranges[static_cast<size_t>(range_no - 1)].partition, e[1], e[2]));
    }
    local_entries.clear();
    st.differing_entries += differing.size();

    if (differing.empty()) {
        /* Nodes differ but every row matches: the node summaries are wrong.
         * The caller's root check fails and the full copy rebuilds them. */
        st.transfer_us += now_us() - x0;
        return true;
    }

    uint64_t candidates = 0;
    uint64_t bytes = 0;
    uint64_t deleted = 0;
    uint64_t inserted = 0;
    const std::vector<row_pair> bad(bad_local.begin(), bad_local.end());
    const std::string match = pair_pred(t, "t." + key);

    if (!exec_cmd(local, "BEGIN", err)) return false;
    if (!exec_cmd(local, "CREATE TEMP TABLE " + tmp + " (LIKE " + tbl + ") ON COMMIT DROP", err)) {
        return commit_or_rollback(local, false, err);
    }
    /* Fetch only the reference rows whose tuple hash differs. */
    const std::string copy_out = "COPY (" + fetch_cte(ranges, wanted) + "SELECT " +
                                 join_quoted(local, t.cols, "t.") + " FROM f JOIN " + tbl + " t ON " +
                                 fetch_pred(t, "t." + key) + ") TO STDOUT";
    if (!wanted.empty() &&
        (!force_range_index_join(ref, err) ||
         !stream_copy(ref, copy_out, local, "COPY " + tmp + " (" + cols + ") FROM STDIN",
                      candidates, err, &bytes) ||
         !restore_join_planning(ref, err))) {
        return commit_or_rollback(local, false, err);
    }
    st.candidate_rows += candidates;
    st.ref_row_bytes += bytes;
    st.transfer_us += now_us() - x0;

    /* Replace every local row whose pair differs by the reference copies. */
    const uint64_t a0 = now_us();
    if (!force_range_index_join(local, err) ||
        (!bad.empty() &&
         !exec_cmd(local, pairs_cte(bad) + "DELETE FROM " + tbl + " t USING d WHERE " + match, err, &deleted)) ||
        !restore_join_planning(local, err) ||
        !exec_cmd(local, "INSERT INTO " + tbl + " (" + cols + ") SELECT " + cols + " FROM " + tmp,
                  err, &inserted)) {
        return commit_or_rollback(local, false, err);
    }
    if (!commit_or_rollback(local, true, err)) return false;
    st.rows_deleted += deleted;
    st.rows_upserted += inserted;
    st.apply_us += now_us() - a0;
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

    std::vector<leaf_key> ranges;
    if (!descend_differing(ref, local, t, parts, ranges, st, err)) return false;
    /* Partition roots differ but no node below does (should not happen): whole partitions. */
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

    /* The lookup index on both sides selects the row-level algorithm:
     *  - tuple-hash lookup (merkle_create_lookup_index(..., mode => 'tuplehash'))
     *    on both: compare tuple hashes and fetch only the mismatching rows;
     *  - otherwise (key-hash lookup, or none, on either side): ship the full
     *    contents of every differing leaf range and reconcile by primary key. */
    const std::string tbl = "public." + quote_ident(local, t.name);
    char* literal = PQescapeLiteral(local, tbl.c_str(), tbl.size());
    if (!literal) {
        err = conn_error(local);
        return false;
    }
    const std::string tbl_lit = literal;
    PQfreemem(literal);
    bool ref_lookup = false, local_lookup = false;
    if (!has_tuplehash_lookup(ref, tbl_lit, ref_lookup, err) ||
        !has_tuplehash_lookup(local, tbl_lit, local_lookup, err)) return false;
    if (ref_lookup && local_lookup) {
        st.tuplehash_mode_tables++;
        if (!repair_ranges_by_tuple_hash(ref, local, t, table_no, ranges, tbl_lit, st, err)) return false;
    } else {
        st.leaf_mode_tables++;
        if (!repair_ranges_full_leaf(ref, local, t, table_no, ranges, st, err)) return false;
    }

    /* The table must now match the snapshot exactly. */
    const uint64_t v0 = now_us();
    bool equal = false;
    if (!roots_equal(ref, local, t, equal, err)) return false;
    if (equal && heap_verify) {
        if (!heap_verify_ok(local, t, heap_ok, err)) return false;
    }
    st.verify_us += now_us() - v0;
    if (equal && heap_ok) return true;

    return full_table_copy(ref, local, t, st, err);
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
        std::vector<uint8_t> acc(8, 0);  // partition roots are 64-bit AdHash sums
        for (const auto& kv : roots) {
            std::vector<uint8_t> h;
            if (!hex_to_bytes(kv.second, h) || h.size() != acc.size()) {
                err = "invalid 64-bit Merkle partition root";
                return false;
            }
            unsigned int carry = 0;
            for (size_t i = 0; i < acc.size(); ++i) {
                const unsigned int sum = static_cast<unsigned int>(acc[i]) + h[i] + carry;
                acc[i] = static_cast<uint8_t>(sum);
                carry = sum >> 8;
            }
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
                       "  IF NOT EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'merkle_lookup_index' AND pronamespace = 'pg_catalog'::regnamespace) THEN "
                       "    CREATE FUNCTION pg_catalog.merkle_lookup_index(tbl regclass) "
                       "    RETURNS regclass LANGUAGE internal STABLE STRICT AS 'merkle_lookup_index_sql'; "
                       "  END IF; "
                       "  IF NOT EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'merkle_range_entries' AND pronamespace = 'pg_catalog'::regnamespace) THEN "
                       "    CREATE FUNCTION pg_catalog.merkle_range_entries(tbl regclass, partition integer, lo bytea, hi bytea) "
                       "    RETURNS TABLE(key_hash bytea, tuple_hash bytea) LANGUAGE internal STABLE STRICT ROWS 1000 "
                       "    AS 'merkle_range_entries_sql'; "
                       "  END IF; "
                       "  IF NOT EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'merkle_key_hash_routed' AND pronamespace = 'pg_catalog'::regnamespace) THEN "
                       "    CREATE FUNCTION pg_catalog.merkle_key_hash_routed(full_key \"any\", leading_key \"any\", partitions integer, subpartitions integer) "
                       "    RETURNS bytea LANGUAGE internal IMMUTABLE STRICT AS 'merkle_key_hash_routed_sql'; "
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
