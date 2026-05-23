#include "pg_executor.hxx"

#include "ariabc_pg_util.hxx"

#include <algorithm>
#include <chrono>
#include <cctype>
#include <cstdlib>
#include <cerrno>
#include <fcntl.h>
#include <functional>
#include <iomanip>
#include <iostream>
#include <map>
#include <poll.h>
#include <sstream>
#include <string.h>
#include <unistd.h>

#include <libpq-fe.h>

#ifndef SSL_LIBRARY_NOT_FOUND
#include <openssl/hmac.h>
#include <openssl/sha.h>
#endif

namespace ariabc_pg {
namespace {

constexpr const char* kBcdbMerkleTag = "BCDB_MERKLE_ROOTS:";
// Majority-confirmed throughput is latency-sensitive: keep batches moderate so
// reply visibility is not delayed too long.
constexpr size_t kKafkaBatchMaxBytes = 128 * 1024;
constexpr size_t kKafkaBatchMaxRecords = 64;
constexpr int kKafkaBatchMaxDelayMs = 1;
// Admission control watermarks are for the *queue depth* (ready+delayed), not
// queue+inflight. In deterministic mode, inflight work is expected; excessive
// queued work is what drives tail latency and straggler behavior.
constexpr size_t kQueueHighWatermarkMin = 32;
constexpr size_t kQueueLowWatermarkMin = 16;
constexpr size_t kQueueHighWatermarkFactor = 4;  // high = max(minHigh, 4*pool)
constexpr size_t kQueueLowWatermarkFactor = 2;   // low  = max(minLow,  2*pool)
// Session-restart heuristic for deterministic-apply ordering: a newly queued
// tx_seq more than this much below the current head means a fresh client
// session (e.g. workload starting at tx_seq=1 after a probe used 99000000)
// rather than late out-of-order arrival inside the same session.
constexpr uint64_t kDetApplyEpochGap = 1000000ULL;
constexpr size_t kDetQueueHighWatermarkMin = 24;
constexpr size_t kDetQueueLowWatermarkMin = 12;
constexpr size_t kDetQueueHighWatermarkFactor = 2;  // high = max(minHigh, 2*pool)
constexpr size_t kDetQueueLowWatermarkFactor = 1;   // low  = max(minLow,  1*pool)
constexpr uint64_t kDefaultDetPartialBlockMaxWaitNs = 2ULL * 1000ULL * 1000ULL;
constexpr const char* kHashAlgo = "sha256";
constexpr uint8_t kHashAlgoId = 1;
constexpr const char* kDefaultResultSigKey = "ariabc-result-v2-dev-key";

bool debug_req_trace_enabled() {
    static const bool enabled = []() -> bool {
        const char* v = std::getenv("ARIABC_DEBUG_REQ_TRACE");
        if (!v) return false;
        const std::string s = trim_copy(v);
        return !(s.empty() || s == "0" || s == "false" || s == "FALSE" || s == "no" || s == "NO");
    }();
    return enabled;
}

bool det_event_block_fastpath_enabled() {
    static const bool enabled = []() -> bool {
        const char* v = std::getenv("ARIABC_DET_EVENT_BLOCK_FASTPATH");
        if (!v) return true;   // on by default; set =0 to disable
        const std::string s = trim_copy(v);
        return !(s.empty() || s == "0" || s == "false" || s == "FALSE" || s == "no" || s == "NO");
    }();
    return enabled;
}

uint64_t det_partial_block_max_wait_ns() {
    static const uint64_t wait_ns = []() -> uint64_t {
        const char* v = std::getenv("ARIABC_DET_PARTIAL_BLOCK_MAX_WAIT_US");
        if (!v || !*v) return kDefaultDetPartialBlockMaxWaitNs;

        errno = 0;
        char* end = nullptr;
        const unsigned long long parsed = std::strtoull(v, &end, 10);
        if (end == v || errno != 0) return kDefaultDetPartialBlockMaxWaitNs;
        if (end && *end != '\0' && !trim_copy(end).empty()) {
            return kDefaultDetPartialBlockMaxWaitNs;
        }

        constexpr unsigned long long kMaxWaitUs = 1000000ULL;
        const unsigned long long capped = std::min(parsed, kMaxWaitUs);
        return static_cast<uint64_t>(capped) * 1000ULL;
    }();
    return wait_ns;
}

bool det_block_skip_readonly_enabled() {
    static const bool enabled = []() -> bool {
        const char* v = std::getenv("ARIABC_DET_BLOCK_SKIP_READONLY");
        if (!v) return false;
        const std::string s = trim_copy(v);
        return !(s.empty() || s == "0" || s == "false" || s == "FALSE" || s == "no" || s == "NO");
    }();
    return enabled;
}

bool sql_is_plain_select(const std::string& sql) {
    const std::string s = trim_copy(sql);
    if (s.size() < 6) return false;
    const char prefix[] = {'s', 'e', 'l', 'e', 'c', 't'};
    for (size_t i = 0; i < 6; ++i) {
        if (static_cast<char>(std::tolower(static_cast<unsigned char>(s[i]))) != prefix[i]) {
            return false;
        }
    }
    return s.size() == 6 ||
           std::isspace(static_cast<unsigned char>(s[6])) ||
           s[6] == '(';
}

uint64_t debug_req_trace_limit() {
    static const uint64_t limit = []() -> uint64_t {
        const char* v = std::getenv("ARIABC_DEBUG_REQ_TRACE_LIMIT");
        if (!v || !*v) return 32;
        try {
            const unsigned long long n = std::stoull(v);
            return (n == 0ULL) ? 32ULL : static_cast<uint64_t>(n);
        } catch (...) {
            return 32ULL;
        }
    }();
    return limit;
}

int full_result_replica_limit() {
    static const int limit = []() -> int {
        const char* v = std::getenv("ARIABC_FULL_RESULT_REPLICA_LIMIT");
        if (!v || !*v) return 0;
        char* end = nullptr;
        const long parsed = std::strtol(v, &end, 10);
        if (end == v || parsed <= 0) return 0;
        return static_cast<int>(std::min<long>(parsed, 1024));
    }();
    return limit;
}

std::atomic<uint64_t> g_debug_exec_trace_count(0);

void debug_trace_exec(const std::string& req_id,
                      uint64_t raft_log_idx,
                      const std::string& out) {
    if (!debug_req_trace_enabled()) return;
    const uint64_t idx = g_debug_exec_trace_count.fetch_add(1, std::memory_order_relaxed);
    if (idx >= debug_req_trace_limit()) return;
    std::string out_head = trim_copy(out);
    if (out_head.size() > 96) out_head.resize(96);
    std::cerr << "REQ_TRACE exec"
              << " idx=" << idx
              << " req_id=" << req_id
              << " raft_log_idx=" << raft_log_idx
              << " out=" << out_head
              << std::endl;
}

bool is_retryable_sqlstate(const char* sqlstate) {
    if (!sqlstate) return false;
    // 40001: serialization_failure, 40P01: deadlock_detected, 57014: query_canceled.
    return (strcmp(sqlstate, "40001") == 0) ||
           (strcmp(sqlstate, "40P01") == 0) ||
           (strcmp(sqlstate, "57014") == 0);
}

std::string format_result(PGresult* res) {
    if (!res) return "ERROR null_result";
    const ExecStatusType st = PQresultStatus(res);
    if (st == PGRES_TUPLES_OK) {
        const char* tag = PQcmdStatus(res);
        std::string out = tag ? std::string(tag) : std::string("SELECT");
        const int rows = PQntuples(res);
        const int cols = PQnfields(res);
        if (rows <= 0 || cols <= 0) return out;

        // Sort row indices by lexicographic value of the row text,
        // avoiding a per-row std::string copy and a post-sort re-append.
        // Row-text format is " <v0> <v1> ..." (space then value, per column).
        std::vector<int> order;
        order.reserve(static_cast<size_t>(rows));
        for (int r = 0; r < rows; ++r) order.push_back(r);

        auto cmp = [&](int a, int b) {
            for (int c = 0; c < cols; ++c) {
                const char* va = PQgetvalue(res, a, c);
                const char* vb = PQgetvalue(res, b, c);
                const int la = PQgetlength(res, a, c);
                const int lb = PQgetlength(res, b, c);
                // Each field is prefixed by ' ' in the canonical row text;
                // the leading-space is identical across rows so skip it.
                const int n = std::min(la, lb);
                const int d = memcmp(va ? va : "", vb ? vb : "", static_cast<size_t>(std::max(0, n)));
                if (d != 0) return d < 0;
                if (la != lb) return la < lb;
            }
            return false;
        };
        std::sort(order.begin(), order.end(), cmp);

        // Single pass: reserve exact byte size, then append once per field.
        size_t total = out.size();
        for (int r = 0; r < rows; ++r) {
            for (int c = 0; c < cols; ++c) {
                total += 1u + static_cast<size_t>(std::max(0, PQgetlength(res, r, c)));
            }
        }
        out.reserve(total);
        for (int r : order) {
            for (int c = 0; c < cols; ++c) {
                out.push_back(' ');
                const char* v = PQgetvalue(res, r, c);
                const int n = PQgetlength(res, r, c);
                if (v && n > 0) out.append(v, static_cast<size_t>(n));
            }
        }
        return out;
    }
    if (st == PGRES_COMMAND_OK) {
        return PQcmdStatus(res) ? PQcmdStatus(res) : "OK";
    }
    const char* msg = PQresultErrorMessage(res);
    std::string out = "ERROR ";
    if (msg && *msg) out.append(trim_copy(msg));
    else out.append("unknown");
    return out;
}

bool parse_req_num(const std::string& req_id, uint64_t& out_req_num) {
    out_req_num = 0;
    const size_t dash = req_id.rfind('-');
    if (dash == std::string::npos || dash + 1 >= req_id.size()) return false;
    try {
        out_req_num = static_cast<uint64_t>(std::stoull(req_id.substr(dash + 1)));
        return true;
    } catch (...) {
        return false;
    }
}

bool parse_det_prefixed_sql_parts(const std::string& sql,
                                  uint64_t* out_seq,
                                  std::string* out_raw_sql) {
    const std::string t = trim_copy(sql);
    if (t.size() < 3) return false;
    if (!(t[0] == 's' || t[0] == 'S') ||
        !std::isspace(static_cast<unsigned char>(t[1]))) {
        return false;
    }

    size_t pos = 2;
    while (pos < t.size() && std::isspace(static_cast<unsigned char>(t[pos]))) ++pos;
    size_t digits_begin = pos;
    while (pos < t.size() && std::isdigit(static_cast<unsigned char>(t[pos]))) ++pos;

    uint64_t seq = 0;
    if (pos > digits_begin && pos < t.size() && std::isspace(static_cast<unsigned char>(t[pos]))) {
        try {
            seq = static_cast<uint64_t>(std::stoull(t.substr(digits_begin, pos - digits_begin)));
        } catch (...) {
            return false;
        }
        while (pos < t.size() && std::isspace(static_cast<unsigned char>(t[pos]))) ++pos;
    } else {
        pos = 2;
        while (pos < t.size() && std::isspace(static_cast<unsigned char>(t[pos]))) ++pos;
    }

    if (pos >= t.size()) return false;
    if (out_seq) *out_seq = seq;
    if (out_raw_sql) *out_raw_sql = t.substr(pos);
    return true;
}

std::string maybe_strip_det_prefix_for_compat(const std::string& sql,
                                              bool det_raw_compat_mode,
                                              int db_type) {
    if (!det_raw_compat_mode || db_type != 1) {
        return sql;
    }
    uint64_t seq = 0;
    std::string raw_sql;
    if (!parse_det_prefixed_sql_parts(sql, &seq, &raw_sql)) {
        return sql;
    }
    if (raw_sql.empty()) {
        return sql;
    }
    return raw_sql;
}

void json_escape_append(std::string& out, const std::string& in) {
    static const char kHex[] = "0123456789abcdef";
    out.reserve(out.size() + in.size() + 8);
    for (unsigned char ch : in) {
        switch (ch) {
            case '\\': out.append("\\\\", 2); break;
            case '"':  out.append("\\\"", 2); break;
            case '\b': out.append("\\b", 2); break;
            case '\f': out.append("\\f", 2); break;
            case '\n': out.append("\\n", 2); break;
            case '\r': out.append("\\r", 2); break;
            case '\t': out.append("\\t", 2); break;
            default:
                if (ch < 0x20) {
                    char buf[6] = {'\\','u','0','0', kHex[(ch >> 4) & 0xF], kHex[ch & 0xF]};
                    out.append(buf, 6);
                } else {
                    out.push_back(static_cast<char>(ch));
                }
                break;
        }
    }
}

std::string json_escape(const std::string& in) {
    std::string out;
    json_escape_append(out, in);
    return out;
}

std::string sql_escape_literal(const std::string& in) {
    std::string out;
    out.reserve(in.size() + 16);
    for (char ch : in) {
        if (ch == '\'') out += "''";
        else out.push_back(ch);
    }
    return out;
}

std::string build_bcdb_block_submit_results_sql(
    uint64_t block_id,
    const std::vector<std::pair<std::string, std::string>>& txs)
{
    // Estimate JSON size: 32 (bid prefix) + per-tx overhead + hash/sql bodies.
    size_t est = 64;
    for (const auto& p : txs) est += 64 + p.first.size() + 2 * p.second.size();

    std::string json;
    json.reserve(est);
    json.append("{\"bid\":");
    json.append(std::to_string(block_id));
    json.append(",\"txs\":[");
    for (size_t i = 0; i < txs.size(); ++i) {
        if (i) json.push_back(',');
        json.append("{\"hash\":\"");
        json_escape_append(json, txs[i].first);
        json.append("\",\"sql\":\"");
        json_escape_append(json, txs[i].second);
        json.append("\"}");
    }
    json.append("]}");

    std::string sql;
    sql.reserve(json.size() * 2 + 48);
    sql.append("SELECT bcdb_block_submit_results('");
    // Inline sql_escape_literal to avoid an extra allocation+copy.
    for (char ch : json) {
        if (ch == '\'') sql.append("''", 2);
        else sql.push_back(ch);
    }
    sql.append("');");
    return sql;
}

bool decode_hex_char(char ch, uint8_t& out) {
    if (ch >= '0' && ch <= '9') {
        out = static_cast<uint8_t>(ch - '0');
        return true;
    }
    if (ch >= 'a' && ch <= 'f') {
        out = static_cast<uint8_t>(10 + (ch - 'a'));
        return true;
    }
    if (ch >= 'A' && ch <= 'F') {
        out = static_cast<uint8_t>(10 + (ch - 'A'));
        return true;
    }
    return false;
}

bool hex_decode_string(const std::string& in, std::string& out) {
    if ((in.size() % 2) != 0) return false;
    out.clear();
    out.reserve(in.size() / 2);
    for (size_t i = 0; i < in.size(); i += 2) {
        uint8_t hi = 0;
        uint8_t lo = 0;
        if (!decode_hex_char(in[i], hi) || !decode_hex_char(in[i + 1], lo)) {
            out.clear();
            return false;
        }
        out.push_back(static_cast<char>((hi << 4) | lo));
    }
    return true;
}

bool parse_bcdb_block_results_text(
    const std::string& payload,
    std::unordered_map<std::string, std::string>& out_by_hash)
{
    out_by_hash.clear();
    size_t pos = 0;
    while (pos < payload.size()) {
        const size_t nl = payload.find('\n', pos);
        const size_t end = (nl == std::string::npos) ? payload.size() : nl;
        if (end > pos) {
            const size_t tab = payload.find('\t', pos);
            if (tab == std::string::npos || tab >= end) return false;
            const std::string hash = payload.substr(pos, tab - pos);
            const std::string hex = payload.substr(tab + 1, end - (tab + 1));
            std::string decoded;
            if (!hex_decode_string(hex, decoded)) return false;
            out_by_hash.emplace(hash, std::move(decoded));
        }
        pos = (nl == std::string::npos) ? payload.size() : (nl + 1);
    }
    return true;
}

void append_u16_le(std::string& out, uint16_t v) {
    out.push_back(static_cast<char>(v & 0xFFu));
    out.push_back(static_cast<char>((v >> 8) & 0xFFu));
}

void append_u8(std::string& out, uint8_t v) {
    out.push_back(static_cast<char>(v));
}

void append_u32_le(std::string& out, uint32_t v) {
    out.push_back(static_cast<char>(v & 0xFFu));
    out.push_back(static_cast<char>((v >> 8) & 0xFFu));
    out.push_back(static_cast<char>((v >> 16) & 0xFFu));
    out.push_back(static_cast<char>((v >> 24) & 0xFFu));
}

void append_u64_le(std::string& out, uint64_t v) {
    for (int i = 0; i < 8; ++i) {
        out.push_back(static_cast<char>((v >> (8 * i)) & 0xFFu));
    }
}

std::string hex_encode(const unsigned char* data, size_t len) {
    static const char* kHex = "0123456789abcdef";
    std::string out;
    out.resize(len * 2);
    for (size_t i = 0; i < len; ++i) {
        const unsigned char b = data[i];
        out[2 * i] = kHex[(b >> 4) & 0xF];
        out[2 * i + 1] = kHex[b & 0xF];
    }
    return out;
}

[[maybe_unused]] std::string fnv1a64_hex(const std::string& in) {
    uint64_t h = 1469598103934665603ULL;
    for (unsigned char c : in) {
        h ^= static_cast<uint64_t>(c);
        h *= 1099511628211ULL;
    }
    std::ostringstream oss;
    oss << std::hex << std::setw(16) << std::setfill('0') << h;
    return oss.str();
}

std::string canonical_result_hash(const std::string& result) {
#ifndef SSL_LIBRARY_NOT_FOUND
    unsigned char md[SHA256_DIGEST_LENGTH];
    SHA256(reinterpret_cast<const unsigned char*>(result.data()),
           static_cast<size_t>(result.size()),
           md);
    return hex_encode(md, SHA256_DIGEST_LENGTH);
#else
    return fnv1a64_hex(result);
#endif
}

std::string sign_payload(const std::string& key, const std::string& payload) {
#ifndef SSL_LIBRARY_NOT_FOUND
    unsigned int out_len = 0;
    unsigned char mac[EVP_MAX_MD_SIZE];
    unsigned char* out = HMAC(EVP_sha256(),
                              reinterpret_cast<const unsigned char*>(key.data()),
                              static_cast<int>(key.size()),
                              reinterpret_cast<const unsigned char*>(payload.data()),
                              static_cast<int>(payload.size()),
                              mac,
                              &out_len);
    if (!out || out_len == 0) return "";
    return hex_encode(mac, static_cast<size_t>(out_len));
#else
    return fnv1a64_hex(key + "|" + payload);
#endif
}

uint64_t now_epoch_ms() {
    const auto now = std::chrono::system_clock::now().time_since_epoch();
    return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(now).count());
}

uint64_t now_steady_ns() {
    const auto now = std::chrono::steady_clock::now().time_since_epoch();
    return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(now).count());
}

std::string make_sig_payload(uint64_t req_num,
                             uint64_t raft_log_idx,
                             const std::string& req_id,
                             int node_id,
                             int leader_node_id,
                             const std::string& result_hash,
                             uint64_t timestamp_ms,
                             bool has_full_result)
{
    std::ostringstream oss;
    oss << req_num << "|"
        << raft_log_idx << "|"
        << req_id << "|"
        << node_id << "|"
        << leader_node_id << "|"
        << result_hash << "|"
        << kHashAlgo << "|"
        << timestamp_ms << "|"
        << (has_full_result ? 1 : 0);
    return oss.str();
}

std::string build_bin_batch_payload_v2(const std::vector<std::string>& req_ids,
                                       const std::vector<std::string>& results,
                                       const std::vector<uint64_t>& raft_log_idxs,
                                       const std::vector<int>& leader_node_hints,
                                       uint16_t node_id,
                                       const std::string& sig_key)
{
    std::string out;
    out.reserve(8 + req_ids.size() * 128);
    out.push_back('B');
    out.push_back('3');
    append_u16_le(out, static_cast<uint16_t>(req_ids.size()));
    append_u32_le(out, 0u); // reserved

    for (size_t i = 0; i < req_ids.size(); ++i) {
        uint64_t req_num = 0;
        if (!parse_req_num(req_ids[i], req_num)) {
            // Fallback hash keeps the pipeline alive for unexpected req ids.
            req_num = static_cast<uint64_t>(std::hash<std::string>{}(req_ids[i]));
        }

        const int leader_node_id = (i < leader_node_hints.size()) ? leader_node_hints[i] : -1;
        const uint64_t raft_log_idx = (i < raft_log_idxs.size()) ? raft_log_idxs[i] : 0;
        const int full_result_limit = full_result_replica_limit();
        const bool include_full_result =
            (full_result_limit <= 0 ||
             static_cast<int>(node_id) <= full_result_limit);
        const uint64_t ts_ms = now_epoch_ms();
        const std::string result_hash = canonical_result_hash(results[i]);
        const std::string sig_payload = make_sig_payload(req_num,
                                                         raft_log_idx,
                                                         req_ids[i],
                                                         static_cast<int>(node_id),
                                                         leader_node_id,
                                                         result_hash,
                                                         ts_ms,
                                                         include_full_result);
        const std::string sig = sign_payload(sig_key, sig_payload);
        const uint8_t flags = include_full_result ? 0x1u : 0x0u;

        append_u64_le(out, req_num);
        append_u64_le(out, raft_log_idx);
        append_u16_le(out, node_id);
        append_u16_le(out, static_cast<uint16_t>(leader_node_id > 0 ? leader_node_id : 0));
        append_u64_le(out, ts_ms);
        append_u8(out, flags);
        append_u8(out, kHashAlgoId);
        append_u16_le(out, static_cast<uint16_t>(req_ids[i].size()));
        append_u16_le(out, static_cast<uint16_t>(result_hash.size()));
        append_u16_le(out, static_cast<uint16_t>(sig.size()));
        append_u32_le(out, static_cast<uint32_t>(include_full_result ? results[i].size() : 0));
        out.append(req_ids[i]);
        out.append(result_hash);
        out.append(sig);
        if (include_full_result) {
            out.append(results[i]);
        }
    }
    return out;
}

} // namespace

struct pg_executor::notice_state {
    std::string last_merkle_roots;
};

void pg_executor::notice_processor(void* arg, const char* message) {
    if (!arg || !message) return;
    notice_state* st = static_cast<notice_state*>(arg);
    const char* p = ::strstr(message, kBcdbMerkleTag);
    if (!p) return;
    std::string payload = trim_copy(p + ::strlen(kBcdbMerkleTag));
    if (!payload.empty()) st->last_merkle_roots = payload;
}

bool pg_executor::is_event_mode(const std::string& mode) {
    const std::string m = trim_copy(mode);
    return (m == "event" || m == "reactor" || m == "async");
}

bool pg_executor::is_det_prefixed_sql(const std::string& sql) const {
    const std::string t = trim_copy(sql);
    if (t.size() < 2) return false;
    // Deterministic/safedb parser-path commands are shaped as:
    //   "s <SQL>" or "s <8digit-seq> <SQL>"
    // If this prefix is absent in safedb deterministic mode, executing many
    // queued requests concurrently can reorder conflicting writes and break
    // Merkle consistency. We detect that case and fall back to serial dispatch.
    return (t[0] == 's' || t[0] == 'S') && std::isspace(static_cast<unsigned char>(t[1]));
}

void pg_executor::ensure_bcdb_initialized_for_sql(const std::string& sql) {
    if (db_opt_.db_type != 1) return;
    if (!is_det_prefixed_sql(sql)) return;
    if (bcdb_init_done_ || bcdb_init_failed_) return;

    std::lock_guard<std::mutex> lk(bcdb_init_mu_);
    if (bcdb_init_done_ || bcdb_init_failed_) return;

    PGconn* c = PQconnectdb(conninfo_.c_str());
    if (!c || PQstatus(c) != CONNECTION_OK) {
        std::cerr << "bcdb_init skipped on node " << node_id_
                  << ": connect failed: "
                  << trim_copy(c ? PQerrorMessage(c) : std::string("null conn"))
                  << std::endl;
        if (c) PQfinish(c);
        bcdb_init_failed_ = true;
        if (event_mode_) {
            det_raw_compat_mode_ = true;
            std::cerr << "bcdb_init fallback: enabling serialized deterministic compatibility mode on node "
                      << node_id_ << std::endl;
        }
        return;
    }

    const int block_size = std::max(2, db_opt_.conn_pool_size);
    std::ostringstream q;
    q << "SELECT bcdb_init(True, " << block_size << ");";
    PGresult* res = PQexec(c, q.str().c_str());
    const ExecStatusType st = res ? PQresultStatus(res) : PGRES_FATAL_ERROR;
    if (!(st == PGRES_TUPLES_OK || st == PGRES_COMMAND_OK)) {
        std::cerr << "bcdb_init skipped on node " << node_id_
                  << ": " << trim_copy(res ? PQresultErrorMessage(res) : "null result")
                  << std::endl;
        if (res) PQclear(res);
        PQfinish(c);
        bcdb_init_failed_ = true;
        if (event_mode_) {
            det_raw_compat_mode_ = true;
            std::cerr << "bcdb_init fallback: enabling serialized deterministic compatibility mode on node "
                      << node_id_ << std::endl;
        }
        return;
    }

    if (res) PQclear(res);
    bcdb_ctrl_conn_ = c;
    bcdb_init_done_ = true;
    bcdb_block_size_ = block_size;
    std::cerr << "bcdb_init enabled on node " << node_id_
              << " with block_size=" << block_size
              << std::endl;
}

pg_executor::pg_executor(int node_id,
                         const db_options& db_opt,
                         const kafka_options& k_opt,
                         completion_callback on_task_applied)
    : node_id_(node_id)
    , db_opt_(db_opt)
    , kafka_opt_(k_opt)
    , result_sig_key_(k_opt.result_sig_key)
    , kafka_enabled_(false)
    , on_task_applied_(std::move(on_task_applied))
{
    event_mode_ = is_event_mode(db_opt_.exec_mode);
    if (db_opt_.db_type == 1) {
        // Parallel deterministic workers are on by default in threaded mode:
        // ordering is enforced by the PG serial gate (worker.c:bcdb_wait_for_serial_slot),
        // so multiple client-side worker threads dispatching in parallel stay
        // deterministic while fully utilising the connection pool. Users can
        // force the legacy single-worker block-batch fast path with
        // ARIABC_DET_PARALLEL_WORKERS=0.
        det_parallel_workers_ = !event_mode_;
        const char* v = ::getenv("ARIABC_DET_PARALLEL_WORKERS");
        if (v && *v) {
            const std::string s = trim_copy(v);
            det_parallel_workers_ =
                !(s.empty() || s == "0" || s == "false" || s == "FALSE" || s == "no" || s == "NO");
        }
    }
    if (event_mode_ && det_parallel_workers_) {
        std::cerr << "event-mode deterministic server execution does not support parallel worker coordination; "
                     "forcing ARIABC_DET_PARALLEL_WORKERS=0 on node "
                  << node_id_ << std::endl;
        det_parallel_workers_ = false;
    }
    if (db_opt_.db_type == 1) {
        const char* vp = ::getenv("ARIABC_DET_BLOCK_PARALLEL");
        if (vp && *vp) {
            char* end = nullptr;
            const long parsed = std::strtol(vp, &end, 10);
            if (end != vp && parsed > 0) {
                /*
                 * Event-mode deterministic blocks can be submitted on several
                 * PG connections at once.  The backend serial gate preserves
                 * tx order; the event loop emits completed block results in
                 * submission order.
                 */
                det_block_parallel_ = static_cast<int>(std::min<long>(parsed, 64));
            }
        }
        const char* v = ::getenv("ARIABC_DET_BLOCK_PIPELINE");
        if (v && *v) {
            char* end = nullptr;
            const long parsed = std::strtol(v, &end, 10);
            if (end != v && parsed > 0) {
                /*
                 * Allow one backend submit to cover multiple ordered BCDB
                 * worker waves. The runtime cap is still bounded below by the
                 * result ring size used by the trusted 4-node runner.
                 */
                det_block_pipeline_ = static_cast<int>(std::min<long>(parsed, 64));
            }
        }
        v = ::getenv("ARIABC_DET_BLOCK_MAX");
        if (v && *v) {
            char* end = nullptr;
            const long parsed = std::strtol(v, &end, 10);
            if (end != v && parsed > 0) {
                det_block_max_ = static_cast<size_t>(std::min<long>(parsed, 8192));
            }
        }
    }

    // Assign a per-process-instance base so BCDB tx_pool keys are unique
    // across ariabc_pg_server restarts sharing the same PostgreSQL instance.
    {
        static std::atomic<uint64_t> s_block_tx_key_session{0};
        det_block_tx_key_base_ =
            s_block_tx_key_session.fetch_add(100000000ULL, std::memory_order_relaxed);
    }

    if (result_sig_key_.empty()) {
        const char* env_key = ::getenv("ARIABC_RESULT_SIG_KEY");
        if (env_key && *env_key) {
            result_sig_key_ = env_key;
        } else {
            result_sig_key_ = kDefaultResultSigKey;
        }
    }

    // Kafka producer (optional).
    if (!kafka_opt_.bootstrap.empty()) {
        std::string err;
        if (!kafka_prod_.start(kafka_opt_.bootstrap,
                               kafka_opt_.result_topic,
                               kafka_producer_profile::result_fast,
                               err)) {
            std::cerr << "Kafka disabled: " << err << std::endl;
            kafka_enabled_ = false;
        } else {
            kafka_enabled_ = true;
        }
    }

    // DB connections.
    if (db_opt_.dbname.empty() || db_opt_.port.empty()) {
        throw std::runtime_error("missing --dbName/--dbPort");
    }
    if (db_opt_.conn_pool_size <= 0) {
        throw std::runtime_error("invalid --dbConnPoolSize");
    }

    // Hysteresis watermarks for admission control. Deterministic mode uses
    // lower queue targets to reduce overload latch oscillation and long-tail
    // append stalls under serialized execution.
    const bool det_mode = (db_opt_.db_type == 1);
    const size_t high_min = det_mode ? kDetQueueHighWatermarkMin : kQueueHighWatermarkMin;
    const size_t low_min = det_mode ? kDetQueueLowWatermarkMin : kQueueLowWatermarkMin;
    const size_t high_factor = det_mode ? kDetQueueHighWatermarkFactor : kQueueHighWatermarkFactor;
    const size_t low_factor = det_mode ? kDetQueueLowWatermarkFactor : kQueueLowWatermarkFactor;
    queue_high_wm_ = std::max<size_t>(
        high_min,
        static_cast<size_t>(db_opt_.conn_pool_size) * high_factor);
    queue_low_wm_ = std::max<size_t>(
        low_min,
        static_cast<size_t>(db_opt_.conn_pool_size) * low_factor);
    if (queue_low_wm_ > queue_high_wm_) {
        queue_low_wm_ = queue_high_wm_;
    }

    if (event_mode_) {
        int pfd[2];
        if (::pipe(pfd) != 0) {
            throw std::runtime_error(std::string("pipe failed: ") + ::strerror(errno));
        }
        wakeup_rfd_ = pfd[0];
        wakeup_wfd_ = pfd[1];
        // Nonblocking wakeups: best-effort; ignore errors.
        (void)::fcntl(wakeup_rfd_, F_SETFL, ::fcntl(wakeup_rfd_, F_GETFL, 0) | O_NONBLOCK);
        (void)::fcntl(wakeup_wfd_, F_SETFL, ::fcntl(wakeup_wfd_, F_GETFL, 0) | O_NONBLOCK);
        (void)::fcntl(wakeup_rfd_, F_SETFD, ::fcntl(wakeup_rfd_, F_GETFD, 0) | FD_CLOEXEC);
        (void)::fcntl(wakeup_wfd_, F_SETFD, ::fcntl(wakeup_wfd_, F_GETFD, 0) | FD_CLOEXEC);
    }

    conninfo_ = "host=" + db_opt_.host +
                " port=" + db_opt_.port +
                " dbname=" + db_opt_.dbname +
                " user=" + db_opt_.user;
    if (!db_opt_.password.empty()) {
        conninfo_ += " password=" + db_opt_.password;
    }

    for (int i = 0; i < db_opt_.conn_pool_size; ++i) {
        PGconn* c = PQconnectdb(conninfo_.c_str());
        if (!c || PQstatus(c) != CONNECTION_OK) {
            std::string msg = c ? PQerrorMessage(c) : "null conn";
            if (c) PQfinish(c);
            throw std::runtime_error("PQconnectdb failed: " + trim_copy(msg));
        }

        if (event_mode_) {
            if (PQsetnonblocking(c, 1) != 0) {
                std::string msg = PQerrorMessage(c);
                PQfinish(c);
                throw std::runtime_error("PQsetnonblocking failed: " + trim_copy(msg));
            }
        }

        // Capture BCDB merkle roots emitted via NOTICE (for deterministic result strings).
        std::unique_ptr<notice_state> ns(new notice_state());
        notice_state* ns_ptr = ns.get();
        PQsetNoticeProcessor(c, &pg_executor::notice_processor, ns_ptr);
        notice_state_by_conn_.emplace(c, ns_ptr);
        notice_states_.push_back(std::move(ns));

        // NOTE:
        // We intentionally do NOT run bcdb_init() here.
        //
        // In our deterministic pipeline, the gateway shapes requests as
        // "s <8digit(seq)> <SQL>" (see Postgres tcop/postgres.c), which routes
        // execution through safedb_txdt() on the server side.
        //
        // Calling bcdb_init(True, N) is extremely heavy: it spawns N additional
        // libpq "worker" backends per calling session. If we do that for each
        // pooled connection (N sessions), the total number of connections grows
        // ~O(N^2) and quickly exhausts Postgres max_connections at N>=12.
        //
        // If a future setup requires bcdb_init, it should be performed once,
        // outside the per-connection pool, with an explicitly chosen block size.

        all_conns_.push_back(c);
        if (!event_mode_) {
            pool_.push(c);
        }
    }

    if (event_mode_) {
        conns_.reserve(all_conns_.size());
        for (PGconn* c : all_conns_) {
            conn_state st;
            st.c = c;
            st.fd = PQsocket(c);
            st.st = conn_state::state::IDLE;
            conns_.push_back(std::move(st));
        }
        st_inflight_cur_.store(0, std::memory_order_relaxed);
        st_delayed_cur_.store(0, std::memory_order_relaxed);
        event_thread_ = std::thread([this] { event_loop(); });
    } else {
        // Worker threads. Deterministic parallel workers are opt-in.
        const int n_threads =
            (db_opt_.db_type == 1 && !det_parallel_workers_)
                ? 1
                : db_opt_.conn_pool_size;
        threads_.reserve(n_threads);
        for (int i = 0; i < n_threads; ++i) {
            threads_.emplace_back([this] { worker_loop(); });
        }
    }
}

pg_executor::~pg_executor() {
    stop();
}

void pg_executor::enqueue(const std::string& req_id,
                          const std::string& sql,
                          int leader_node_hint,
                          uint64_t raft_log_idx) {
    std::vector<std::string> req_ids;
    std::vector<std::string> sqls;
    req_ids.push_back(req_id);
    sqls.push_back(sql);
    enqueue_batch(req_ids, sqls, leader_node_hint, raft_log_idx);
}

void pg_executor::enqueue_batch(const std::vector<std::string>& req_ids,
                                const std::vector<std::string>& sqls,
                                int leader_node_hint,
                                uint64_t raft_log_idx) {
    if (stop_.load()) return;
    if (req_ids.empty() || req_ids.size() != sqls.size()) return;

    for (const std::string& sql : sqls) {
        ensure_bcdb_initialized_for_sql(sql);
    }

    st_enqueued_.fetch_add(static_cast<uint64_t>(req_ids.size()), std::memory_order_relaxed);
    {
        const uint64_t now_ns = now_steady_ns();
        std::lock_guard<std::mutex> lk(q_mu_);
        for (size_t i = 0; i < req_ids.size(); ++i) {
            task t;
            t.req_id = req_ids[i];
            t.sql = sqls[i];
            t.leader_node_hint = leader_node_hint;
            t.raft_log_idx = raft_log_idx;
            t.enqueue_ns = now_ns;
            t.exec_begin_ns = 0;
            t.attempt = 0;
            q_.push(std::move(t));

            const size_t depth = q_.size();
            st_queue_depth_cur_.store(static_cast<uint64_t>(depth), std::memory_order_relaxed);
            st_queue_depth_samples_.fetch_add(1, std::memory_order_relaxed);
            st_queue_depth_sum_.fetch_add(static_cast<uint64_t>(depth), std::memory_order_relaxed);
            if (depth <= 16) {
                st_queue_depth_bin_le_16_.fetch_add(1, std::memory_order_relaxed);
            } else if (depth <= 64) {
                st_queue_depth_bin_17_64_.fetch_add(1, std::memory_order_relaxed);
            } else if (depth <= 256) {
                st_queue_depth_bin_65_256_.fetch_add(1, std::memory_order_relaxed);
            } else {
                st_queue_depth_bin_gt_256_.fetch_add(1, std::memory_order_relaxed);
            }
            uint64_t cur_max = st_queue_depth_max_.load(std::memory_order_relaxed);
            while (depth > cur_max &&
                   !st_queue_depth_max_.compare_exchange_weak(
                       cur_max,
                       static_cast<uint64_t>(depth),
                       std::memory_order_relaxed,
                       std::memory_order_relaxed)) {
            }
        }
        const size_t delayed = delayed_.size();
        const size_t inflight = static_cast<size_t>(st_inflight_cur_.load(std::memory_order_relaxed));
        const size_t queued = q_.size() + delayed;
        const size_t backlog = queued + inflight;
        st_backlog_cur_.store(static_cast<uint64_t>(backlog), std::memory_order_relaxed);
        st_delayed_cur_.store(static_cast<uint64_t>(delayed), std::memory_order_relaxed);
        // NOTE:
        // Do not toggle `queue_overloaded_` here.
        //
        // `enqueue()` runs on the Raft state-machine thread and can observe
        // short bursts where `q_` spikes before the executor thread dispatches
        // work to idle connections. If we flip the overload latch on such a
        // transient spike, the server may reject client submissions with
        // NOT_ACCEPTED_BUSY until the queue drains all the way below the low
        // watermark, causing large throughput variance.
        //
        // Instead, overload state is updated by the consumer loops (worker or
        // event reactor) based on their post-dispatch view.
    }
    q_cv_.notify_one();
    if (event_mode_ && wakeup_wfd_ >= 0) {
        const uint8_t b = 1;
        (void)::write(wakeup_wfd_, &b, 1);
    }
}

pg_executor_stats pg_executor::stats() const {
    pg_executor_stats out;
    out.enqueued = st_enqueued_.load(std::memory_order_relaxed);
    out.dequeued = st_dequeued_.load(std::memory_order_relaxed);
    out.q_wait_ns = st_q_wait_ns_.load(std::memory_order_relaxed);
    out.queue_delay_ns = st_queue_delay_ns_.load(std::memory_order_relaxed);
    out.queue_delay_dequeue_ns = st_queue_delay_dequeue_ns_.load(std::memory_order_relaxed);
    out.queue_delay_exec_start_ns = st_queue_delay_exec_start_ns_.load(std::memory_order_relaxed);
    out.backlog_cur = st_backlog_cur_.load(std::memory_order_relaxed);
    out.inflight_cur = st_inflight_cur_.load(std::memory_order_relaxed);
    out.delayed_cur = st_delayed_cur_.load(std::memory_order_relaxed);
    out.conn_acquire_calls = st_conn_acquire_calls_.load(std::memory_order_relaxed);
    out.conn_acquire_wait_ns = st_conn_acquire_wait_ns_.load(std::memory_order_relaxed);
    out.exec_calls = st_exec_calls_.load(std::memory_order_relaxed);
    out.exec_ns = st_exec_ns_.load(std::memory_order_relaxed);
    out.pg_query_ns = st_pg_query_ns_.load(std::memory_order_relaxed);
    out.result_format_ns = st_result_format_ns_.load(std::memory_order_relaxed);
    out.retryable_sqlstate_40001 = st_retryable_sqlstate_40001_.load(std::memory_order_relaxed);
    out.retryable_sqlstate_40P01 = st_retryable_sqlstate_40P01_.load(std::memory_order_relaxed);
    out.retryable_sqlstate_57014 = st_retryable_sqlstate_57014_.load(std::memory_order_relaxed);
    out.retry_attempts_total = st_retry_attempts_total_.load(std::memory_order_relaxed);
    out.retry_exhausted_total = st_retry_exhausted_total_.load(std::memory_order_relaxed);
    out.kafka_flush_calls = st_kafka_flush_calls_.load(std::memory_order_relaxed);
    out.kafka_payload_bytes = st_kafka_payload_bytes_.load(std::memory_order_relaxed);
    out.kafka_build_payload_ns = st_kafka_build_payload_ns_.load(std::memory_order_relaxed);
    out.kafka_send_ns = st_kafka_send_ns_.load(std::memory_order_relaxed);
    out.queue_depth_samples = st_queue_depth_samples_.load(std::memory_order_relaxed);
    out.queue_depth_sum = st_queue_depth_sum_.load(std::memory_order_relaxed);
    out.queue_depth_max = st_queue_depth_max_.load(std::memory_order_relaxed);
    out.queue_depth_bin_le_16 = st_queue_depth_bin_le_16_.load(std::memory_order_relaxed);
    out.queue_depth_bin_17_64 = st_queue_depth_bin_17_64_.load(std::memory_order_relaxed);
    out.queue_depth_bin_65_256 = st_queue_depth_bin_65_256_.load(std::memory_order_relaxed);
    out.queue_depth_bin_gt_256 = st_queue_depth_bin_gt_256_.load(std::memory_order_relaxed);
    out.queue_overload_enter = st_queue_overload_enter_.load(std::memory_order_relaxed);
    out.queue_overload_exit = st_queue_overload_exit_.load(std::memory_order_relaxed);
    out.queue_high_watermark = static_cast<uint64_t>(queue_high_wm_);
    out.queue_low_watermark = static_cast<uint64_t>(queue_low_wm_);
    out.queue_depth_cur = st_queue_depth_cur_.load(std::memory_order_relaxed);
    out.queue_overloaded = queue_overloaded_.load(std::memory_order_relaxed) ? 1 : 0;
    out.bcdb_init_enabled = bcdb_init_done_ ? 1 : 0;
    out.bcdb_block_size = bcdb_block_size_;
    out.det_block_batches = st_det_block_batches_.load(std::memory_order_relaxed);
    out.det_block_items = st_det_block_items_.load(std::memory_order_relaxed);
    out.det_block_min = st_det_block_min_.load(std::memory_order_relaxed);
    out.det_block_max = st_det_block_max_.load(std::memory_order_relaxed);
    out.det_block_bin_1 = st_det_block_bin_1_.load(std::memory_order_relaxed);
    out.det_block_bin_2_15 = st_det_block_bin_2_15_.load(std::memory_order_relaxed);
    out.det_block_bin_16_63 = st_det_block_bin_16_63_.load(std::memory_order_relaxed);
    out.det_block_bin_64_127 = st_det_block_bin_64_127_.load(std::memory_order_relaxed);
    out.det_block_bin_128_plus = st_det_block_bin_128_plus_.load(std::memory_order_relaxed);
    out.det_block_fallbacks = st_det_block_fallbacks_.load(std::memory_order_relaxed);
    out.det_block_skipped_readonly = st_det_block_skipped_readonly_.load(std::memory_order_relaxed);
    return out;
}

kafka_producer_stats pg_executor::kafka_stats() const {
    return kafka_prod_.stats();
}

bool pg_executor::admission_control_blocked() const {
    return queue_overloaded_.load(std::memory_order_relaxed);
}

bool pg_executor::wait_for_admission_drain(uint64_t max_wait_ns) {
    if (!queue_overloaded_.load(std::memory_order_acquire)) return true;
    if (stop_.load(std::memory_order_relaxed)) return false;
    std::unique_lock<std::mutex> lk(admission_mu_);
    // Re-check under the lock to avoid lost-wakeup.
    if (!queue_overloaded_.load(std::memory_order_acquire)) return true;
    if (stop_.load(std::memory_order_relaxed)) return false;
    const auto deadline = std::chrono::steady_clock::now() +
        std::chrono::nanoseconds(max_wait_ns);
    admission_cv_.wait_until(lk, deadline, [this] {
        return !queue_overloaded_.load(std::memory_order_acquire) ||
               stop_.load(std::memory_order_relaxed);
    });
    return !queue_overloaded_.load(std::memory_order_acquire);
}

bool pg_executor::wait_for_ordered_emit_turn(uint64_t dispatch_seq) {
    if (!(db_opt_.db_type == 1 && !event_mode_ && det_parallel_workers_) || dispatch_seq == 0) {
        return true;
    }
    std::unique_lock<std::mutex> lk(det_emit_mu_);
    det_emit_cv_.wait(lk, [&] {
        return stop_.load(std::memory_order_relaxed) || dispatch_seq == det_next_emit_seq_;
    });
    return !stop_.load(std::memory_order_relaxed);
}

void pg_executor::finish_ordered_emit(uint64_t dispatch_seq) {
    if (!(db_opt_.db_type == 1 && !event_mode_ && det_parallel_workers_) || dispatch_seq == 0) {
        return;
    }
    {
        std::lock_guard<std::mutex> lk(det_emit_mu_);
        if (dispatch_seq >= det_next_emit_seq_) {
            det_next_emit_seq_ = dispatch_seq + 1;
        }
    }
    det_emit_cv_.notify_all();
}

uint64_t pg_executor::get_det_tx_seq(const task& t) const {
    uint64_t tx_seq = 0;
    if (parse_req_num(t.req_id, tx_seq) && tx_seq > 0) {
        return tx_seq;
    }
    return 0;
}

void pg_executor::det_mark_tx_state(uint64_t tx_seq, det_tx_state st) {
    if (tx_seq == 0) return;
    std::lock_guard<std::mutex> lk(det_apply_mu_);
    if (st == det_tx_state::QUEUED) {
        if (!det_apply_initialized_) {
            det_next_apply_seq_ = tx_seq;
            det_apply_initialized_ = true;
        } else if (tx_seq < det_next_apply_seq_) {
            // Session-restart detection. Tasks are enqueued in Raft commit
            // order and popped in FIFO order, so within a single client
            // session tx_seqs arrive here strictly monotonically. A newly
            // queued tx_seq below the current head therefore means a new
            // client/session started a fresh deterministic numbering after a
            // previous session ran (e.g. the bcdb_init probe uses
            // detStartSeq=99000000; the subsequent workload restarts at 1, or
            // back-to-back gateway invocations both start at detStartSeq=1).
            // Rewind so the new session's requests aren't blocked forever
            // waiting for a det_next_apply_seq_ they will never reach.
            det_next_apply_seq_ = tx_seq;
            det_tx_states_.clear();
            det_apply_cv_.notify_all();
        }
    }
    det_tx_states_[tx_seq] = st;
}

bool pg_executor::det_wait_for_apply_turn(uint64_t tx_seq) {
    if (tx_seq == 0) return true;
    std::unique_lock<std::mutex> lk(det_apply_mu_);
    det_apply_cv_.wait(lk, [&] {
        return stop_.load(std::memory_order_relaxed) || tx_seq == det_next_apply_seq_;
    });
    return !stop_.load(std::memory_order_relaxed);
}

void pg_executor::det_finish_apply(uint64_t tx_seq) {
    if (tx_seq == 0) return;
    {
        std::lock_guard<std::mutex> lk(det_apply_mu_);
        det_tx_states_[tx_seq] = det_tx_state::APPLIED;
        if (tx_seq == det_next_apply_seq_) {
            ++det_next_apply_seq_;
        }
        auto it = det_tx_states_.find(tx_seq);
        if (it != det_tx_states_.end()) {
            det_tx_states_.erase(it);
        }
    }
    det_apply_cv_.notify_all();
}

void pg_executor::notify_task_applied(uint64_t raft_log_idx) {
    if (raft_log_idx == 0) return;
    if (on_task_applied_) {
        on_task_applied_(raft_log_idx);
    }
}

void pg_executor::record_det_block_batch(size_t size, bool fallback) {
    if (size == 0) return;

    const uint64_t n = static_cast<uint64_t>(size);
    st_det_block_batches_.fetch_add(1, std::memory_order_relaxed);
    st_det_block_items_.fetch_add(n, std::memory_order_relaxed);
    if (fallback) {
        st_det_block_fallbacks_.fetch_add(1, std::memory_order_relaxed);
    }

    uint64_t cur_min = st_det_block_min_.load(std::memory_order_relaxed);
    while ((cur_min == 0 || n < cur_min) &&
           !st_det_block_min_.compare_exchange_weak(
               cur_min, n, std::memory_order_relaxed, std::memory_order_relaxed)) {
    }

    uint64_t cur_max = st_det_block_max_.load(std::memory_order_relaxed);
    while (n > cur_max &&
           !st_det_block_max_.compare_exchange_weak(
               cur_max, n, std::memory_order_relaxed, std::memory_order_relaxed)) {
    }

    if (n == 1) {
        st_det_block_bin_1_.fetch_add(1, std::memory_order_relaxed);
    } else if (n < 16) {
        st_det_block_bin_2_15_.fetch_add(1, std::memory_order_relaxed);
    } else if (n < 64) {
        st_det_block_bin_16_63_.fetch_add(1, std::memory_order_relaxed);
    } else if (n < 128) {
        st_det_block_bin_64_127_.fetch_add(1, std::memory_order_relaxed);
    } else {
        st_det_block_bin_128_plus_.fetch_add(1, std::memory_order_relaxed);
    }
}

void pg_executor::stop() {
    bool expected = false;
    if (!stop_.compare_exchange_strong(expected, true)) {
        // already stopped
    }
    q_cv_.notify_all();
    pool_cv_.notify_all();
    det_emit_cv_.notify_all();
    det_compat_cv_.notify_all();
    det_apply_cv_.notify_all();
    admission_cv_.notify_all();
    if (event_mode_ && wakeup_wfd_ >= 0) {
        const uint8_t b = 1;
        (void)::write(wakeup_wfd_, &b, 1);
    }
    if (event_thread_.joinable()) {
        event_thread_.join();
    }
    for (auto& t : threads_) {
        if (t.joinable()) t.join();
    }
    threads_.clear();

    kafka_prod_.stop();

    if (bcdb_ctrl_conn_) {
        PQfinish(bcdb_ctrl_conn_);
        bcdb_ctrl_conn_ = nullptr;
    }

    if (wakeup_rfd_ >= 0) {
        ::close(wakeup_rfd_);
        wakeup_rfd_ = -1;
    }
    if (wakeup_wfd_ >= 0) {
        ::close(wakeup_wfd_);
        wakeup_wfd_ = -1;
    }

    for (PGconn* c : all_conns_) {
        if (c) PQfinish(c);
    }
    all_conns_.clear();
    while (!pool_.empty()) pool_.pop();
}

PGconn* pg_executor::acquire_conn() {
    std::unique_lock<std::mutex> lk(pool_mu_);
    st_conn_acquire_calls_.fetch_add(1, std::memory_order_relaxed);
    const auto w0 = std::chrono::steady_clock::now();
    pool_cv_.wait(lk, [this] { return stop_.load() || !pool_.empty(); });
    const auto w1 = std::chrono::steady_clock::now();
    st_conn_acquire_wait_ns_.fetch_add(
        static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(w1 - w0).count()),
        std::memory_order_relaxed);
    if (stop_.load()) return nullptr;
    PGconn* c = pool_.front();
    pool_.pop();
    return c;
}

void pg_executor::release_conn(PGconn* c) {
    if (!c) return;
    {
        std::lock_guard<std::mutex> lk(pool_mu_);
        pool_.push(c);
    }
    pool_cv_.notify_one();
}

std::string pg_executor::exec_sql(PGconn* c, const std::string& sql) {
    if (!c) return "ERROR no_connection";

    st_exec_calls_.fetch_add(1, std::memory_order_relaxed);
    const auto t0 = std::chrono::steady_clock::now();

    notice_state* ns = nullptr;
    auto it = notice_state_by_conn_.find(c);
    if (it != notice_state_by_conn_.end()) ns = it->second;

    // Best-effort reconnect if needed.
    if (PQstatus(c) != CONNECTION_OK) {
        PQreset(c);
        if (ns) {
            PQsetNoticeProcessor(c, &pg_executor::notice_processor, ns);
        }
    }

    for (int attempt = 0; attempt <= db_opt_.max_retries; ++attempt) {
        if (ns) ns->last_merkle_roots.clear();
        const auto q0 = std::chrono::steady_clock::now();
        PGresult* res = PQexec(c, sql.c_str());
        const auto q1 = std::chrono::steady_clock::now();
        st_pg_query_ns_.fetch_add(
            static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(q1 - q0).count()),
            std::memory_order_relaxed);
        const ExecStatusType st = res ? PQresultStatus(res) : PGRES_FATAL_ERROR;

        if (st == PGRES_TUPLES_OK || st == PGRES_COMMAND_OK) {
            const auto f0 = std::chrono::steady_clock::now();
            std::string out = format_result(res);
            const auto f1 = std::chrono::steady_clock::now();
            st_result_format_ns_.fetch_add(
                static_cast<uint64_t>(
                    std::chrono::duration_cast<std::chrono::nanoseconds>(f1 - f0).count()),
                std::memory_order_relaxed);
            if (ns && !ns->last_merkle_roots.empty()) {
                // BCDB emits Merkle roots via NOTICE. Keep those for offline
                // verification/profiling, but do not fold them into the
                // majority-voted SQL result string or identical reads can hash
                // differently across replicas.
                ns->last_merkle_roots.clear();
            }
            PQclear(res);

            const auto t1 = std::chrono::steady_clock::now();
            st_exec_ns_.fetch_add(
                static_cast<uint64_t>(
                    std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count()),
                std::memory_order_relaxed);
            return out;
        }

        const char* sqlstate = res ? PQresultErrorField(res, PG_DIAG_SQLSTATE) : nullptr;
        const bool retryable = is_retryable_sqlstate(sqlstate);
        const std::string err_msg = res ? trim_copy(PQresultErrorMessage(res)) : "null result";
        if (res) PQclear(res);

        if (!retryable) {
            const auto t1 = std::chrono::steady_clock::now();
            st_exec_ns_.fetch_add(
                static_cast<uint64_t>(
                    std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count()),
                std::memory_order_relaxed);
            return "ERROR " + err_msg;
        }
        if (retryable && sqlstate) {
            if (strcmp(sqlstate, "40001") == 0) {
                st_retryable_sqlstate_40001_.fetch_add(1, std::memory_order_relaxed);
            } else if (strcmp(sqlstate, "40P01") == 0) {
                st_retryable_sqlstate_40P01_.fetch_add(1, std::memory_order_relaxed);
            } else if (strcmp(sqlstate, "57014") == 0) {
                st_retryable_sqlstate_57014_.fetch_add(1, std::memory_order_relaxed);
            }
        }
        if (attempt == db_opt_.max_retries) {
            st_retry_exhausted_total_.fetch_add(1, std::memory_order_relaxed);
            const auto t1 = std::chrono::steady_clock::now();
            st_exec_ns_.fetch_add(
                static_cast<uint64_t>(
                    std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count()),
                std::memory_order_relaxed);
            return "ERROR(retry_exhausted) " + err_msg;
        }

        st_retry_attempts_total_.fetch_add(1, std::memory_order_relaxed);
        std::this_thread::sleep_for(std::chrono::milliseconds(db_opt_.retry_backoff_ms));
    }

    {
        const auto t1 = std::chrono::steady_clock::now();
        st_exec_ns_.fetch_add(
            static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count()),
            std::memory_order_relaxed);
    }
    return "ERROR unexpected";
}

bool pg_executor::exec_det_block_batch(PGconn* c,
                                       uint64_t tx_key_start,
                                       const std::vector<task>& tasks,
                                       std::vector<std::string>& out_results) {
    out_results.clear();
    if (!c || tasks.empty()) return false;

    std::vector<std::pair<std::string, std::string>> txs;
    txs.reserve(tasks.size());

    for (size_t i = 0; i < tasks.size(); ++i) {
        uint64_t seq = 0;
        std::string raw_sql;
        if (!parse_det_prefixed_sql_parts(tasks[i].sql, &seq, &raw_sql)) {
            return false;
        }
        // Use a session-unique monotonically allocated key so tx_pool entries
        // from previous server sessions sharing the same PostgreSQL instance
        // don't collide. This used to be block_id * 1024 + slot, which silently
        // capped safe block sizes at 1024 because larger blocks overlapped the
        // next block's key range.
        const std::string key = std::to_string(tx_key_start + static_cast<uint64_t>(i));
        txs.emplace_back(key, std::move(raw_sql));
    }

    const std::string sql = build_bcdb_block_submit_results_sql(det_next_block_id_++, txs);

    notice_state* ns = nullptr;
    auto it = notice_state_by_conn_.find(c);
    if (it != notice_state_by_conn_.end()) ns = it->second;

    if (PQstatus(c) != CONNECTION_OK) {
        PQreset(c);
        if (ns) {
            PQsetNoticeProcessor(c, &pg_executor::notice_processor, ns);
        }
    }
    if (ns) ns->last_merkle_roots.clear();

    const auto q0 = std::chrono::steady_clock::now();
    PGresult* res = PQexec(c, sql.c_str());
    const auto q1 = std::chrono::steady_clock::now();
    st_pg_query_ns_.fetch_add(
        static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(q1 - q0).count()),
        std::memory_order_relaxed);

    const ExecStatusType st = res ? PQresultStatus(res) : PGRES_FATAL_ERROR;
    if (!(st == PGRES_TUPLES_OK || st == PGRES_COMMAND_OK) ||
        !res || PQntuples(res) < 1 || PQnfields(res) < 1) {
        if (res) PQclear(res);
        return false;
    }

    const auto f0 = std::chrono::steady_clock::now();
    const char* val = PQgetvalue(res, 0, 0);
    std::unordered_map<std::string, std::string> result_by_hash;
    const bool parsed = parse_bcdb_block_results_text(val ? std::string(val) : std::string(),
                                                      result_by_hash);
    const auto f1 = std::chrono::steady_clock::now();
    st_result_format_ns_.fetch_add(
        static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(f1 - f0).count()),
        std::memory_order_relaxed);
    if (ns && !ns->last_merkle_roots.empty()) {
        ns->last_merkle_roots.clear();
    }
    PQclear(res);
    if (!parsed) return false;

    out_results.reserve(tasks.size());
    for (size_t i = 0; i < tasks.size(); ++i) {
        const std::string key = std::to_string(tx_key_start + static_cast<uint64_t>(i));
        auto rit = result_by_hash.find(key);
        if (rit == result_by_hash.end()) return false;
        out_results.push_back(rit->second);
    }
    return (out_results.size() == tasks.size());
}

void pg_executor::worker_loop() {
    std::vector<std::string> batch_req_ids;
    std::vector<std::string> batch_results;
    std::vector<uint64_t> batch_raft_log_idxs;
    std::vector<int> batch_leader_hints;
    size_t batch_bytes = 0;
    auto batch_start = std::chrono::steady_clock::now();

    auto flush_batch = [&]() {
        if (!kafka_enabled_ || batch_req_ids.empty()) {
            batch_req_ids.clear();
            batch_results.clear();
            batch_raft_log_idxs.clear();
            batch_leader_hints.clear();
            batch_bytes = 0;
            batch_start = std::chrono::steady_clock::now();
            return;
        }

        st_kafka_flush_calls_.fetch_add(1, std::memory_order_relaxed);

        std::string err;
        // True batching: build ONE multi-record payload for the whole batch,
        // send ONCE. The consumer (gateway) already decodes 'B3' payloads
        // with N>1 records via parse_kafka_payload_records.
        const auto b0 = std::chrono::steady_clock::now();
        const std::string payload = build_bin_batch_payload_v2(
            batch_req_ids,
            batch_results,
            batch_raft_log_idxs,
            batch_leader_hints,
            static_cast<uint16_t>(node_id_),
            result_sig_key_);
        const auto b1 = std::chrono::steady_clock::now();
        st_kafka_build_payload_ns_.fetch_add(
            static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(b1 - b0).count()),
            std::memory_order_relaxed);
        st_kafka_payload_bytes_.fetch_add(static_cast<uint64_t>(payload.size()),
                                          std::memory_order_relaxed);

        const auto s0 = std::chrono::steady_clock::now();
        if (!kafka_prod_.send_payload(payload, batch_req_ids.front(), err)) {
            std::cerr << "Kafka send failed: " << err << std::endl;
        }
        const auto s1 = std::chrono::steady_clock::now();
        st_kafka_send_ns_.fetch_add(
            static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(s1 - s0).count()),
            std::memory_order_relaxed);
        batch_req_ids.clear();
        batch_results.clear();
        batch_raft_log_idxs.clear();
        batch_leader_hints.clear();
        batch_bytes = 0;
        batch_start = std::chrono::steady_clock::now();
    };

    while (!stop_.load()) {
        task t;
        size_t q_depth_after_pop = 0;
        {
            std::unique_lock<std::mutex> lk(q_mu_);
            const auto w0 = std::chrono::steady_clock::now();
            q_cv_.wait(lk, [this] { return stop_.load() || !q_.empty(); });
            const auto w1 = std::chrono::steady_clock::now();
            st_q_wait_ns_.fetch_add(
                static_cast<uint64_t>(
                    std::chrono::duration_cast<std::chrono::nanoseconds>(w1 - w0).count()),
                std::memory_order_relaxed);
            if (stop_.load()) break;
            const size_t q_depth_before_pop = q_.size();
            t = q_.front();
            q_.pop();
            q_depth_after_pop = q_.size();
            st_queue_depth_cur_.store(static_cast<uint64_t>(q_depth_after_pop), std::memory_order_relaxed);
            st_queue_depth_samples_.fetch_add(1, std::memory_order_relaxed);
            st_queue_depth_sum_.fetch_add(static_cast<uint64_t>(q_depth_after_pop), std::memory_order_relaxed);
            if (q_depth_after_pop <= 16) {
                st_queue_depth_bin_le_16_.fetch_add(1, std::memory_order_relaxed);
            } else if (q_depth_after_pop <= 64) {
                st_queue_depth_bin_17_64_.fetch_add(1, std::memory_order_relaxed);
            } else if (q_depth_after_pop <= 256) {
                st_queue_depth_bin_65_256_.fetch_add(1, std::memory_order_relaxed);
            } else {
                st_queue_depth_bin_gt_256_.fetch_add(1, std::memory_order_relaxed);
            }
            uint64_t cur_max = st_queue_depth_max_.load(std::memory_order_relaxed);
            while (q_depth_after_pop > cur_max &&
                   !st_queue_depth_max_.compare_exchange_weak(
                       cur_max,
                       static_cast<uint64_t>(q_depth_after_pop),
                       std::memory_order_relaxed,
                       std::memory_order_relaxed)) {
            }
            if (queue_overloaded_.load(std::memory_order_relaxed) &&
                q_depth_after_pop <= queue_low_wm_) {
                bool expected = true;
                if (queue_overloaded_.compare_exchange_strong(
                        expected, false, std::memory_order_release, std::memory_order_relaxed)) {
                    st_queue_overload_exit_.fetch_add(1, std::memory_order_relaxed);
                    admission_cv_.notify_all();
                }
            } else if (!queue_overloaded_.load(std::memory_order_relaxed) &&
                       q_depth_before_pop >
                           (queue_high_wm_ + ((db_opt_.db_type == 1)
                                                  ? 0
                                                  : std::max<size_t>(1, static_cast<size_t>(db_opt_.conn_pool_size))))) {
                bool expected = false;
                if (queue_overloaded_.compare_exchange_strong(
                        expected, true, std::memory_order_relaxed, std::memory_order_relaxed)) {
                    st_queue_overload_enter_.fetch_add(1, std::memory_order_relaxed);
                }
            }
        }

        st_dequeued_.fetch_add(1, std::memory_order_relaxed);
        t.dispatch_seq = next_dispatch_seq_.fetch_add(1, std::memory_order_relaxed);

        const bool det_raw_compat =
            det_parallel_workers_ &&
            (db_opt_.db_type == 1) && !is_det_prefixed_sql(t.sql);
        if (det_raw_compat) {
            det_raw_compat_mode_ = true;
        }

        bool det_raw_compat_serial_turn = false;
        if (det_raw_compat) {
            // Raw deterministic compatibility mode must execute strictly in
            // dispatch order. Waiting for the ordered turn *before* execution
            // prevents out-of-order apply and avoids slot+emit deadlocks.
            if (!wait_for_ordered_emit_turn(t.dispatch_seq)) {
                break;
            }
            det_raw_compat_serial_turn = true;
        }

        const bool det_prefixed_parallel =
            det_parallel_workers_ &&
            (db_opt_.db_type == 1) && is_det_prefixed_sql(t.sql);
        uint64_t det_tx_seq = 0;
        if (det_prefixed_parallel) {
            det_tx_seq = get_det_tx_seq(t);
            det_mark_tx_state(det_tx_seq, det_tx_state::QUEUED);

            // Phase 2 Stage A (simulation): parse deterministic envelope.
            uint64_t det_sql_seq = 0;
            std::string det_raw_sql;
            (void)parse_det_prefixed_sql_parts(t.sql, &det_sql_seq, &det_raw_sql);
            det_mark_tx_state(det_tx_seq, det_tx_state::SIM_DONE);

            if (!det_wait_for_apply_turn(det_tx_seq)) {
                break;
            }
            det_mark_tx_state(det_tx_seq, det_tx_state::APPLY_READY);
        }

        if (t.enqueue_ns != 0) {
            const auto now = std::chrono::steady_clock::now().time_since_epoch();
            const uint64_t now_ns = static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(now).count());
            if (now_ns >= t.enqueue_ns) {
                const uint64_t delay_ns = now_ns - t.enqueue_ns;
                st_queue_delay_ns_.fetch_add(delay_ns, std::memory_order_relaxed);
                st_queue_delay_dequeue_ns_.fetch_add(delay_ns, std::memory_order_relaxed);
            }
        }

        PGconn* c = acquire_conn();
        const std::string sql_exec =
            maybe_strip_det_prefix_for_compat(t.sql, det_raw_compat_mode_, db_opt_.db_type);
        const std::string result = exec_sql(c, sql_exec);
        release_conn(c);
        if (det_prefixed_parallel) {
            det_finish_apply(det_tx_seq);
        }
        notify_task_applied(t.raft_log_idx);

        if (!det_raw_compat_serial_turn) {
            if (!wait_for_ordered_emit_turn(t.dispatch_seq)) {
                break;
            }
        }

        if (kafka_enabled_) {
            debug_trace_exec(t.req_id, t.raft_log_idx, result);
            if (batch_req_ids.empty()) {
                batch_start = std::chrono::steady_clock::now();
            }
            batch_req_ids.push_back(t.req_id);
            batch_results.push_back(result);
            batch_raft_log_idxs.push_back(t.raft_log_idx);
            batch_leader_hints.push_back(t.leader_node_hint);
            batch_bytes += t.req_id.size() + result.size() + 32;
            const size_t max_records = (q_depth_after_pop >= 64) ? 128 : kKafkaBatchMaxRecords;
            const size_t max_bytes = (q_depth_after_pop >= 64) ? (256 * 1024) : kKafkaBatchMaxBytes;
            const int max_delay_ms = (q_depth_after_pop >= 16) ? 0 : kKafkaBatchMaxDelayMs;
            const auto age_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - batch_start).count();
            if (batch_req_ids.size() >= max_records ||
                batch_bytes >= max_bytes ||
                age_ms >= max_delay_ms) {
                flush_batch();
            }
        } else {
            const std::string msg =
                t.req_id + "  " + std::to_string(node_id_) + "  " + result;
            std::cout << msg << std::endl;
        }
        finish_ordered_emit(t.dispatch_seq);

        {
            std::lock_guard<std::mutex> lk(q_mu_);
            if (q_.empty()) {
                flush_batch();
            }
        }
    }

    flush_batch();
}

void pg_executor::event_loop() {
    std::vector<std::string> batch_req_ids;
    std::vector<std::string> batch_results;
    std::vector<uint64_t> batch_raft_log_idxs;
    std::vector<int> batch_leader_hints;
    size_t batch_bytes = 0;
    auto batch_start = std::chrono::steady_clock::now();

    auto flush_batch = [&]() {
        if (!kafka_enabled_ || batch_req_ids.empty()) {
            batch_req_ids.clear();
            batch_results.clear();
            batch_raft_log_idxs.clear();
            batch_leader_hints.clear();
            batch_bytes = 0;
            batch_start = std::chrono::steady_clock::now();
            return;
        }

        st_kafka_flush_calls_.fetch_add(1, std::memory_order_relaxed);

        std::string err;
        // True batching: build ONE multi-record payload, send ONCE.
        const auto b0 = std::chrono::steady_clock::now();
        const std::string payload = build_bin_batch_payload_v2(
            batch_req_ids,
            batch_results,
            batch_raft_log_idxs,
            batch_leader_hints,
            static_cast<uint16_t>(node_id_),
            result_sig_key_);
        const auto b1 = std::chrono::steady_clock::now();
        st_kafka_build_payload_ns_.fetch_add(
            static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(b1 - b0).count()),
            std::memory_order_relaxed);
        st_kafka_payload_bytes_.fetch_add(static_cast<uint64_t>(payload.size()),
                                          std::memory_order_relaxed);

        const auto s0 = std::chrono::steady_clock::now();
        if (!kafka_prod_.send_payload(payload, batch_req_ids.front(), err)) {
            std::cerr << "Kafka send failed: " << err << std::endl;
        }
        const auto s1 = std::chrono::steady_clock::now();
        st_kafka_send_ns_.fetch_add(
            static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(s1 - s0).count()),
            std::memory_order_relaxed);
        batch_req_ids.clear();
        batch_results.clear();
        batch_raft_log_idxs.clear();
        batch_leader_hints.clear();
        batch_bytes = 0;
        batch_start = std::chrono::steady_clock::now();
    };

    struct ready_det_block {
        std::vector<task> tasks;
        std::vector<std::string> results;
    };
    std::map<uint64_t, ready_det_block> ready_det_blocks;
    uint64_t next_det_block_submit_seq = 0;
    uint64_t next_det_block_emit_seq = 0;

    auto emit_det_result = [&](const task& done_task, const std::string& out) {
        notify_task_applied(done_task.raft_log_idx);
        if (kafka_enabled_) {
            debug_trace_exec(done_task.req_id, done_task.raft_log_idx, out);
            if (batch_req_ids.empty()) batch_start = std::chrono::steady_clock::now();
            batch_req_ids.push_back(done_task.req_id);
            batch_results.push_back(out);
            batch_raft_log_idxs.push_back(done_task.raft_log_idx);
            batch_leader_hints.push_back(done_task.leader_node_hint);
            batch_bytes += done_task.req_id.size() + out.size() + 32;
        } else {
            std::cout << (done_task.req_id + "  " + std::to_string(node_id_) + "  " + out)
                      << std::endl;
        }
    };

    auto drain_ready_det_blocks = [&]() {
        for (;;) {
            auto it = ready_det_blocks.find(next_det_block_emit_seq);
            if (it == ready_det_blocks.end()) break;

            ready_det_block ready = std::move(it->second);
            ready_det_blocks.erase(it);
            const size_t n = std::min(ready.tasks.size(), ready.results.size());
            for (size_t i = 0; i < n; ++i) {
                emit_det_result(ready.tasks[i], ready.results[i]);
            }
            if (kafka_enabled_) {
                const auto age_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::steady_clock::now() - batch_start).count();
                if (batch_req_ids.size() >= kKafkaBatchMaxRecords ||
                    batch_bytes >= kKafkaBatchMaxBytes ||
                    age_ms >= kKafkaBatchMaxDelayMs) {
                    flush_batch();
                }
            }
            ++next_det_block_emit_seq;
        }
    };

    auto update_overload_state = [&](size_t backlog, size_t queued) {
        st_backlog_cur_.store(static_cast<uint64_t>(backlog), std::memory_order_relaxed);
        // Allow dispatch jitter margin before entering overload in non-det
        // mode. Deterministic mode uses zero margin to react directly to
        // queued depth and avoid prolonged append stalls.
        const size_t enter_margin =
            (db_opt_.db_type == 1)
                ? 0
                : std::max<size_t>(1, static_cast<size_t>(db_opt_.conn_pool_size));
        const size_t enter_wm = queue_high_wm_ + enter_margin;
        if (queue_overloaded_.load(std::memory_order_relaxed)) {
            if (queued <= queue_low_wm_) {
                bool expected = true;
                if (queue_overloaded_.compare_exchange_strong(
                        expected, false, std::memory_order_release, std::memory_order_relaxed)) {
                    st_queue_overload_exit_.fetch_add(1, std::memory_order_relaxed);
                    admission_cv_.notify_all();
                }
            }
        } else {
            if (queued > enter_wm) {
                bool expected = false;
                if (queue_overloaded_.compare_exchange_strong(
                        expected, true, std::memory_order_relaxed, std::memory_order_relaxed)) {
                    st_queue_overload_enter_.fetch_add(1, std::memory_order_relaxed);
                }
            }
        }
    };

    while (!stop_.load()) {
        // Move due retries into the ready queue.
        const uint64_t now_ns = now_steady_ns();
        uint64_t next_det_partial_block_ready_ns = 0;
        {
            std::lock_guard<std::mutex> lk(q_mu_);
            while (!delayed_.empty() && delayed_.top().deadline_ns <= now_ns) {
                q_.push(delayed_.top().t);
                delayed_.pop();
            }
            st_delayed_cur_.store(static_cast<uint64_t>(delayed_.size()), std::memory_order_relaxed);
        }

        // Deterministic BCDB block fast path: submit ordered parser-mode
        // blocks on several PG connections, then emit completed block results
        // in submission order.  This preserves the global result stream while
        // allowing later blocks to parse/simulate while earlier blocks finish.
        if (!stop_.load() &&
            db_opt_.db_type == 1 &&
            !det_parallel_workers_ &&
            det_event_block_fastpath_enabled() &&
            bcdb_init_done_ &&
            !conns_.empty()) {
            const size_t base_block_cap = bcdb_init_done_
                ? static_cast<size_t>(bcdb_block_size_)
                : std::max<size_t>(1, static_cast<size_t>(db_opt_.conn_pool_size));
            const size_t block_cap =
                std::min<size_t>(det_block_max_,
                                 base_block_cap *
                                 static_cast<size_t>(std::max(1, det_block_pipeline_)));
            int det_blocks_inflight = 0;

            for (const auto& cs : conns_) {
                if (cs.has_det_block && cs.st != conn_state::state::IDLE) {
                    ++det_blocks_inflight;
                }
            }

            for (auto& cs : conns_) {
                if (det_blocks_inflight >= det_block_parallel_) break;
                if (block_cap == 0 || cs.st != conn_state::state::IDLE) continue;

                std::vector<task> det_batch;
                size_t depth_after_pop = 0;
                {
                    std::lock_guard<std::mutex> lk(q_mu_);
                    if (delayed_.empty() && !q_.empty() && is_det_prefixed_sql(q_.front().sql)) {
                        std::queue<task> restored;
                        std::vector<task> candidate;
                        candidate.reserve(block_cap);
                        while (!q_.empty() && candidate.size() < block_cap) {
                            /*
                             * Keep one BCDB block inside the request batch that
                             * the gateway or Raft state machine accepted.
                             * Kafka-only bypass replicas receive those batches
                             * in the same order, but their executor queues can
                             * drain at different rates; merging adjacent queue
                             * batches here would make replica-local timing pick
                             * BCDB block boundaries.
                             */
                            if (!candidate.empty() &&
                                q_.front().raft_log_idx != candidate.front().raft_log_idx) {
                                break;
                            }
                            candidate.push_back(std::move(q_.front()));
                            q_.pop();
                        }

                        bool eligible = !candidate.empty();
                        uint64_t first_req_num = 0;
                        for (size_t i = 0; eligible && i < candidate.size(); ++i) {
                            uint64_t req_num = 0;
                            if (!parse_req_num(candidate[i].req_id, req_num) ||
                                !is_det_prefixed_sql(candidate[i].sql)) {
                                eligible = false;
                                break;
                            }
                            if (i == 0) {
                                first_req_num = req_num;
                            } else if (req_num != first_req_num + static_cast<uint64_t>(i)) {
                                eligible = false;
                                break;
                            }
                        }

                        const uint64_t partial_wait_ns = det_partial_block_max_wait_ns();
                        if (eligible && partial_wait_ns > 0 && candidate.size() < block_cap) {
                            const uint64_t oldest_enqueue_ns = candidate.front().enqueue_ns;
                            if (oldest_enqueue_ns != 0) {
                                const uint64_t ready_ns = oldest_enqueue_ns + partial_wait_ns;
                                if (now_ns < ready_ns) {
                                    if (next_det_partial_block_ready_ns == 0 ||
                                        ready_ns < next_det_partial_block_ready_ns) {
                                        next_det_partial_block_ready_ns = ready_ns;
                                    }
                                    eligible = false;
                                }
                            }
                        }

                        if (eligible) {
                            det_batch = std::move(candidate);
                            depth_after_pop = q_.size();
                            st_queue_depth_cur_.store(static_cast<uint64_t>(depth_after_pop),
                                                      std::memory_order_relaxed);
                            st_queue_depth_samples_.fetch_add(1, std::memory_order_relaxed);
                            st_queue_depth_sum_.fetch_add(static_cast<uint64_t>(depth_after_pop),
                                                          std::memory_order_relaxed);
                        } else {
                            for (auto& t : candidate) restored.push(std::move(t));
                            while (!q_.empty()) {
                                restored.push(std::move(q_.front()));
                                q_.pop();
                            }
                            q_.swap(restored);
                        }
                    }
                }
                if (det_batch.empty()) break;

                const uint64_t exec_begin_ns = now_steady_ns();
                std::vector<std::pair<std::string, std::string>> txs;
                std::vector<uint8_t> backend_mask;
                const uint64_t tx_key_start = det_block_tx_key_base_ + det_block_next_tx_key_;
                bool build_ok = true;
                uint64_t skipped_readonly = 0;
                txs.reserve(det_batch.size());
                backend_mask.reserve(det_batch.size());
                for (size_t i = 0; i < det_batch.size(); ++i) {
                    uint64_t seq = 0;
                    std::string raw_sql;
                    if (!parse_det_prefixed_sql_parts(det_batch[i].sql, &seq, &raw_sql)) {
                        build_ok = false;
                        break;
                    }
                    if (det_block_skip_readonly_enabled() && sql_is_plain_select(raw_sql)) {
                        backend_mask.push_back(0);
                        ++skipped_readonly;
                        continue;
                    }
                    backend_mask.push_back(1);
                    const std::string key = std::to_string(tx_key_start + static_cast<uint64_t>(i));
                    txs.emplace_back(key, std::move(raw_sql));
                }
                det_block_next_tx_key_ += static_cast<uint64_t>(det_batch.size());
                if (skipped_readonly > 0) {
                    st_det_block_skipped_readonly_.fetch_add(skipped_readonly,
                                                             std::memory_order_relaxed);
                }

                for (auto& t : det_batch) {
                    if (t.exec_begin_ns == 0) t.exec_begin_ns = exec_begin_ns;
                    st_dequeued_.fetch_add(1, std::memory_order_relaxed);
                    st_exec_calls_.fetch_add(1, std::memory_order_relaxed);
                    if (t.enqueue_ns != 0 && exec_begin_ns >= t.enqueue_ns) {
                        const uint64_t delay_ns = exec_begin_ns - t.enqueue_ns;
                        st_queue_delay_ns_.fetch_add(delay_ns, std::memory_order_relaxed);
                        st_queue_delay_exec_start_ns_.fetch_add(delay_ns, std::memory_order_relaxed);
                    }
                }

                const uint64_t block_seq = next_det_block_submit_seq++;
                if (!build_ok) {
                    ready_det_block ready;
                    ready.tasks = std::move(det_batch);
                    ready.results.assign(ready.tasks.size(), "ERROR det_block_build_failed");
                    record_det_block_batch(ready.tasks.size(), true);
                    ready_det_blocks.emplace(block_seq, std::move(ready));
                    continue;
                }
                if (txs.empty()) {
                    ready_det_block ready;
                    ready.tasks = std::move(det_batch);
                    ready.results.assign(ready.tasks.size(), "");
                    record_det_block_batch(ready.tasks.size(), false);
                    ready_det_blocks.emplace(block_seq, std::move(ready));
                    continue;
                }

                const std::string sql = build_bcdb_block_submit_results_sql(det_next_block_id_++, txs);
                if (PQstatus(cs.c) != CONNECTION_OK) {
                    PQreset(cs.c);
                    auto it = notice_state_by_conn_.find(cs.c);
                    if (it != notice_state_by_conn_.end()) {
                        PQsetNoticeProcessor(cs.c, &pg_executor::notice_processor, it->second);
                    }
                }
                auto it = notice_state_by_conn_.find(cs.c);
                notice_state* ns = (it != notice_state_by_conn_.end()) ? it->second : nullptr;
                if (ns) ns->last_merkle_roots.clear();

                if (PQsendQuery(cs.c, sql.c_str()) != 1) {
                    ready_det_block ready;
                    ready.tasks = std::move(det_batch);
                    ready.results.assign(ready.tasks.size(), "ERROR det_block_send_failed");
                    record_det_block_batch(ready.tasks.size(), true);
                    ready_det_blocks.emplace(block_seq, std::move(ready));
                    continue;
                }

                cs.cur = task{};
                cs.has_task = false;
                cs.has_det_block = true;
                cs.det_block_seq = block_seq;
                cs.det_block_tx_key_start = tx_key_start;
                cs.det_block_tasks = std::move(det_batch);
                cs.det_block_backend_mask = std::move(backend_mask);
                cs.exec_start_ns = exec_begin_ns;
                cs.st = conn_state::state::SENDING;
                ++det_blocks_inflight;
            }
        }
        drain_ready_det_blocks();

        // Assign ready work to idle connections.
        size_t inflight = 0;
        for (auto& cs : conns_) {
            if (cs.st != conn_state::state::IDLE) {
                ++inflight;
                continue;
            }

            task t;
            bool have = false;
            size_t depth_after_pop = 0;
            {
                std::lock_guard<std::mutex> lk(q_mu_);
                if (!q_.empty()) {
                    const task& front = q_.front();
                    if (!det_raw_compat_mode_ &&
                        db_opt_.db_type == 1 &&
                        !is_det_prefixed_sql(front.sql)) {
                        det_raw_compat_mode_ = true;
                    }
                    if (!det_parallel_workers_ &&
                        det_event_block_fastpath_enabled() &&
                        db_opt_.db_type == 1 &&
                        bcdb_init_done_ &&
                        is_det_prefixed_sql(front.sql)) {
                        // Deterministic parser-mode throughput path is handled by the
                        // block submit fast path above. Always block det-prefixed BCDB
                        // queries from leaking to the multi-connection async path,
                        // regardless of det_raw_compat_mode_ — concurrent async
                        // connections cause non-deterministic snapshot reads.
                        break;
                    }
                    if (det_raw_compat_mode_ && inflight > 0) {
                        // Raw deterministic SQL compatibility mode.
                        // For BCDB (db_type==1): always enforce single-connection
                        // serialization. Concurrent async connections produce
                        // different per-node MVCC snapshots due to OS-scheduling
                        // variance, causing divergent results after restore even
                        // though the BCDB serial gate orders commits correctly.
                        if (db_opt_.db_type == 1) break;
                        // For non-BCDB installs lockstep is opt-in via
                        // ARIABC_DET_COMPAT_LOCKSTEP=1 (serial gate sufficient).
                        static const bool k_compat_lockstep = [] {
                            const char* v = ::getenv("ARIABC_DET_COMPAT_LOCKSTEP");
                            if (!v || !*v) return false;
                            const std::string s = trim_copy(v);
                            return !(s == "0" || s == "false" || s == "FALSE" ||
                                     s == "no" || s == "NO");
                        }();
                        if (k_compat_lockstep) break;
                    }
                    t = std::move(q_.front());
                    q_.pop();
                    depth_after_pop = q_.size();
                    have = true;
                    st_queue_depth_cur_.store(static_cast<uint64_t>(depth_after_pop), std::memory_order_relaxed);
                    st_queue_depth_samples_.fetch_add(1, std::memory_order_relaxed);
                    st_queue_depth_sum_.fetch_add(static_cast<uint64_t>(depth_after_pop), std::memory_order_relaxed);
                }
            }
            if (!have) break;

            const bool first_attempt = (t.attempt == 0 && t.exec_begin_ns == 0);
            if (t.exec_begin_ns == 0) {
                t.exec_begin_ns = now_ns;
            }
            if (first_attempt) {
                st_dequeued_.fetch_add(1, std::memory_order_relaxed);
                st_exec_calls_.fetch_add(1, std::memory_order_relaxed);
                if (t.enqueue_ns != 0 && now_ns >= t.enqueue_ns) {
                    const uint64_t delay_ns = now_ns - t.enqueue_ns;
                    st_queue_delay_ns_.fetch_add(delay_ns, std::memory_order_relaxed);
                    st_queue_delay_exec_start_ns_.fetch_add(delay_ns, std::memory_order_relaxed);
                }
            }

            // Best-effort reconnect if needed.
            if (PQstatus(cs.c) != CONNECTION_OK) {
                PQreset(cs.c);
                auto it = notice_state_by_conn_.find(cs.c);
                if (it != notice_state_by_conn_.end()) {
                    PQsetNoticeProcessor(cs.c, &pg_executor::notice_processor, it->second);
                }
            }

            // Start async send.
            auto it = notice_state_by_conn_.find(cs.c);
            notice_state* ns = (it != notice_state_by_conn_.end()) ? it->second : nullptr;
            if (ns) ns->last_merkle_roots.clear();

            const std::string sql_exec =
                maybe_strip_det_prefix_for_compat(t.sql, det_raw_compat_mode_, db_opt_.db_type);
            if (PQsendQuery(cs.c, sql_exec.c_str()) != 1) {
                std::string msg = trim_copy(PQerrorMessage(cs.c));
                const std::string out = "ERROR " + (msg.empty() ? std::string("send_failed") : msg);
                if (t.exec_begin_ns != 0 && now_ns >= t.exec_begin_ns) {
                    st_exec_ns_.fetch_add(now_ns - t.exec_begin_ns, std::memory_order_relaxed);
                }
                // Emit result immediately (no retry).
                if (kafka_enabled_) {
                    if (batch_req_ids.empty()) batch_start = std::chrono::steady_clock::now();
                    batch_req_ids.push_back(t.req_id);
                    batch_results.push_back(out);
                    batch_raft_log_idxs.push_back(t.raft_log_idx);
                    batch_leader_hints.push_back(t.leader_node_hint);
                    batch_bytes += t.req_id.size() + out.size() + 32;
                } else {
                    std::cout << (t.req_id + "  " + std::to_string(node_id_) + "  " + out) << std::endl;
                }
                continue;
            }

            cs.cur = std::move(t);
            cs.has_task = true;
            cs.exec_start_ns = now_ns;
            cs.st = conn_state::state::SENDING;
            ++inflight;
        }

        st_inflight_cur_.store(static_cast<uint64_t>(inflight), std::memory_order_relaxed);

        // Backlog is for observability; overload control uses queued depth.
        size_t backlog = 0;
        size_t queued = 0;
        {
            std::lock_guard<std::mutex> lk(q_mu_);
            queued = q_.size() + delayed_.size();
            backlog = queued + inflight;
            st_queue_depth_cur_.store(static_cast<uint64_t>(q_.size()), std::memory_order_relaxed);
        }
        update_overload_state(backlog, queued);

        // Batch flushing policy: adapt to backlog and age.
        if (kafka_enabled_ && !batch_req_ids.empty()) {
            const auto age_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - batch_start).count();
            const size_t max_records = (backlog >= 64) ? 128 : kKafkaBatchMaxRecords;
            const size_t max_bytes = (backlog >= 64) ? (256 * 1024) : kKafkaBatchMaxBytes;
            const int max_delay_ms = (backlog >= 16) ? 0 : kKafkaBatchMaxDelayMs;
            if (batch_req_ids.size() >= max_records ||
                batch_bytes >= max_bytes ||
                age_ms >= max_delay_ms) {
                flush_batch();
            }
        }

        // Prepare poll set (reuse scratch buffer to avoid per-iter alloc).
        std::vector<pollfd>& pfds = pfds_scratch_;
        pfds.clear();
        if (pfds.capacity() < 1 + conns_.size()) {
            pfds.reserve(1 + conns_.size());
        }
        if (wakeup_rfd_ >= 0) {
            pollfd p;
            p.fd = wakeup_rfd_;
            p.events = POLLIN;
            p.revents = 0;
            pfds.push_back(p);
        }
        for (const auto& cs : conns_) {
            if (cs.fd < 0) continue;
            short ev = 0;
            if (cs.st == conn_state::state::SENDING) ev |= POLLOUT;
            if (cs.st == conn_state::state::READING) ev |= POLLIN;
            if (!ev) continue;
            pollfd p;
            p.fd = cs.fd;
            p.events = ev;
            p.revents = 0;
            pfds.push_back(p);
        }

        // Timeout: wake for next retry deadline or batch delay.
        int timeout_ms = 50;
        {
            std::lock_guard<std::mutex> lk(q_mu_);
            if (!delayed_.empty()) {
                const uint64_t dl_ns = delayed_.top().deadline_ns;
                if (dl_ns <= now_ns) {
                    timeout_ms = 0;
                } else {
                    const uint64_t delta_ns = dl_ns - now_ns;
                    const uint64_t delta_ms = delta_ns / 1000000ULL;
                    timeout_ms = static_cast<int>(std::min<uint64_t>(delta_ms, 50ULL));
                }
            }
        }
        if (next_det_partial_block_ready_ns != 0) {
            if (next_det_partial_block_ready_ns <= now_ns) {
                timeout_ms = 0;
            } else {
                const uint64_t delta_ns = next_det_partial_block_ready_ns - now_ns;
                const uint64_t delta_ms = (delta_ns + 999999ULL) / 1000000ULL;
                timeout_ms = std::min(
                    timeout_ms,
                    static_cast<int>(std::max<uint64_t>(1ULL, std::min<uint64_t>(delta_ms, 50ULL))));
            }
        }
        if (kafka_enabled_ && !batch_req_ids.empty()) {
            const auto age_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - batch_start).count();
            const int remaining_ms = std::max(0, kKafkaBatchMaxDelayMs - static_cast<int>(age_ms));
            timeout_ms = std::min(timeout_ms, remaining_ms);
        }

        int rc = 0;
        if (!pfds.empty()) {
            rc = ::poll(pfds.data(), pfds.size(), timeout_ms);
        } else {
            // No sockets in flight; just sleep a bit.
            std::this_thread::sleep_for(std::chrono::milliseconds(timeout_ms));
            rc = 0;
        }
        if (rc < 0) {
            if (errno == EINTR) continue;
        }

        // Drain wakeups.
        if (!pfds.empty() && pfds[0].fd == wakeup_rfd_ && (pfds[0].revents & POLLIN)) {
            uint8_t buf[64];
            while (::read(wakeup_rfd_, buf, sizeof(buf)) > 0) {
            }
        }

        // Drive connections.
        for (auto& cs : conns_) {
            if (cs.fd < 0) continue;
            short re = 0;
            for (const auto& p : pfds) {
                if (p.fd == cs.fd) {
                    re = p.revents;
                    break;
                }
            }
            if (!re) continue;

            if (cs.st == conn_state::state::SENDING && (re & POLLOUT)) {
                const int fr = PQflush(cs.c);
                if (fr == 0) {
                    cs.st = conn_state::state::READING;
                } else if (fr < 0) {
                    cs.st = conn_state::state::IDLE;
                    cs.has_task = false;
                    cs.has_det_block = false;
                    cs.det_block_tasks.clear();
                    cs.det_block_backend_mask.clear();
                }
            }

            if (cs.st == conn_state::state::READING && (re & POLLIN)) {
                if (PQconsumeInput(cs.c) != 1) {
                    cs.st = conn_state::state::IDLE;
                    cs.has_task = false;
                    cs.has_det_block = false;
                    cs.det_block_tasks.clear();
                    cs.det_block_backend_mask.clear();
                    continue;
                }
                if (PQisBusy(cs.c)) continue;

                // Drain all results; keep the last (PQexec-like behavior).
                PGresult* last = nullptr;
                PGresult* r = nullptr;
                while ((r = PQgetResult(cs.c)) != nullptr) {
                    if (last) PQclear(last);
                    last = r;
                }

                const uint64_t query_end_ns = now_steady_ns();
                if (cs.exec_start_ns != 0 && query_end_ns >= cs.exec_start_ns) {
                    st_pg_query_ns_.fetch_add(query_end_ns - cs.exec_start_ns, std::memory_order_relaxed);
                }

                if (cs.has_det_block) {
                    ready_det_block ready;
                    ready.tasks = std::move(cs.det_block_tasks);
                    std::vector<uint8_t> backend_mask = std::move(cs.det_block_backend_mask);
                    if (backend_mask.size() != ready.tasks.size()) {
                        backend_mask.assign(ready.tasks.size(), 1);
                    }
                    bool batch_ok = false;
                    const ExecStatusType st = last ? PQresultStatus(last) : PGRES_FATAL_ERROR;

                    if ((st == PGRES_TUPLES_OK || st == PGRES_COMMAND_OK) &&
                        last && PQntuples(last) >= 1 && PQnfields(last) >= 1) {
                        const auto f0 = std::chrono::steady_clock::now();
                        const char* val = PQgetvalue(last, 0, 0);
                        std::unordered_map<std::string, std::string> result_by_hash;
                        batch_ok = parse_bcdb_block_results_text(
                            val ? std::string(val) : std::string(),
                            result_by_hash);
                        if (batch_ok) {
                            ready.results.assign(ready.tasks.size(), "");
                            for (size_t i = 0; i < ready.tasks.size(); ++i) {
                                if (backend_mask[i] == 0) {
                                    continue;
                                }
                                const std::string key =
                                    std::to_string(cs.det_block_tx_key_start +
                                                   static_cast<uint64_t>(i));
                                auto it = result_by_hash.find(key);
                                if (it == result_by_hash.end()) {
                                    batch_ok = false;
                                    break;
                                }
                                ready.results[i] = it->second;
                            }
                        }
                        const auto f1 = std::chrono::steady_clock::now();
                        st_result_format_ns_.fetch_add(
                            static_cast<uint64_t>(
                                std::chrono::duration_cast<std::chrono::nanoseconds>(f1 - f0).count()),
                            std::memory_order_relaxed);
                    }

                    record_det_block_batch(ready.tasks.size(), !batch_ok);
                    if (batch_ok) {
                        bool expected = false;
                        if (st_det_block_seen_.compare_exchange_strong(expected, true)) {
                            std::cerr << "det_block_batch active on node " << node_id_
                                      << " size=" << ready.tasks.size()
                                      << " parallel=" << det_block_parallel_ << std::endl;
                        }
                    } else {
                        bool expected = false;
                        if (st_det_block_fallback_seen_.compare_exchange_strong(expected, true)) {
                            std::cerr << "det_block_batch fallback on node " << node_id_
                                      << " size=" << ready.tasks.size()
                                      << " parallel=" << det_block_parallel_ << std::endl;
                        }
                        ready.results.clear();
                        ready.results.reserve(ready.tasks.size());
                        for (size_t i = 0; i < ready.tasks.size(); ++i) {
                            if (backend_mask[i] == 0) {
                                ready.results.push_back("");
                            } else {
                                ready.results.push_back(exec_sql(cs.c, ready.tasks[i].sql));
                            }
                        }
                    }

                    const uint64_t finish_ns = now_steady_ns();
                    if (cs.exec_start_ns != 0 && finish_ns >= cs.exec_start_ns) {
                        st_exec_ns_.fetch_add(finish_ns - cs.exec_start_ns, std::memory_order_relaxed);
                    }
                    if (last) PQclear(last);

                    const uint64_t block_seq = cs.det_block_seq;
                    cs.st = conn_state::state::IDLE;
                    cs.has_task = false;
                    cs.has_det_block = false;
                    cs.det_block_seq = 0;
                    cs.det_block_tx_key_start = 0;
                    cs.det_block_backend_mask.clear();
                    cs.exec_start_ns = 0;

                    ready_det_blocks.emplace(block_seq, std::move(ready));
                    drain_ready_det_blocks();
                    continue;
                }

                std::string out;
                bool retry = false;
                std::string err_msg;
                const ExecStatusType st = last ? PQresultStatus(last) : PGRES_FATAL_ERROR;
                if (st == PGRES_TUPLES_OK || st == PGRES_COMMAND_OK) {
                    const auto f0 = std::chrono::steady_clock::now();
                    out = format_result(last);
                    const auto f1 = std::chrono::steady_clock::now();
                    st_result_format_ns_.fetch_add(
                        static_cast<uint64_t>(
                            std::chrono::duration_cast<std::chrono::nanoseconds>(f1 - f0).count()),
                        std::memory_order_relaxed);
                    auto it = notice_state_by_conn_.find(cs.c);
                    notice_state* ns = (it != notice_state_by_conn_.end()) ? it->second : nullptr;
                    if (ns && !ns->last_merkle_roots.empty()) {
                        ns->last_merkle_roots.clear();
                    }
                } else {
                    const char* sqlstate = last ? PQresultErrorField(last, PG_DIAG_SQLSTATE) : nullptr;
                    retry = is_retryable_sqlstate(sqlstate);
                    err_msg = last ? trim_copy(PQresultErrorMessage(last)) : "null result";
                    if (retry && sqlstate) {
                        if (strcmp(sqlstate, "40001") == 0) {
                            st_retryable_sqlstate_40001_.fetch_add(1, std::memory_order_relaxed);
                        } else if (strcmp(sqlstate, "40P01") == 0) {
                            st_retryable_sqlstate_40P01_.fetch_add(1, std::memory_order_relaxed);
                        } else if (strcmp(sqlstate, "57014") == 0) {
                            st_retryable_sqlstate_57014_.fetch_add(1, std::memory_order_relaxed);
                        }
                    }
                    if (!retry) {
                        out = "ERROR " + err_msg;
                    }
                }
                if (last) PQclear(last);

                task done_task = std::move(cs.cur);
                const int attempt = done_task.attempt;

                cs.st = conn_state::state::IDLE;
                cs.has_task = false;
                cs.exec_start_ns = 0;

                if (retry) {
                    if (attempt >= db_opt_.max_retries) {
                        st_retry_exhausted_total_.fetch_add(1, std::memory_order_relaxed);
                        out = "ERROR(retry_exhausted) " + err_msg;
                    } else {
                        st_retry_attempts_total_.fetch_add(1, std::memory_order_relaxed);
                        done_task.attempt = attempt + 1;
                        delayed_task dt;
                        dt.deadline_ns = now_steady_ns() +
                            static_cast<uint64_t>(std::max(0, db_opt_.retry_backoff_ms)) * 1000000ULL;
                        dt.t = std::move(done_task);
                        {
                            std::lock_guard<std::mutex> lk(q_mu_);
                            delayed_.push(std::move(dt));
                            st_delayed_cur_.store(static_cast<uint64_t>(delayed_.size()), std::memory_order_relaxed);
                        }
                        if (wakeup_wfd_ >= 0) {
                            const uint8_t b = 1;
                            (void)::write(wakeup_wfd_, &b, 1);
                        }
                        continue;
                    }
                }

                const uint64_t finish_ns = now_steady_ns();
                if (done_task.exec_begin_ns != 0 && finish_ns >= done_task.exec_begin_ns) {
                    st_exec_ns_.fetch_add(finish_ns - done_task.exec_begin_ns, std::memory_order_relaxed);
                }
                notify_task_applied(done_task.raft_log_idx);

                if (kafka_enabled_) {
                    debug_trace_exec(done_task.req_id, done_task.raft_log_idx, out);
                    if (batch_req_ids.empty()) batch_start = std::chrono::steady_clock::now();
                    batch_req_ids.push_back(done_task.req_id);
                    batch_results.push_back(out);
                    batch_raft_log_idxs.push_back(done_task.raft_log_idx);
                    batch_leader_hints.push_back(done_task.leader_node_hint);
                    batch_bytes += done_task.req_id.size() + out.size() + 32;
                } else {
                    std::cout << (done_task.req_id + "  " + std::to_string(node_id_) + "  " + out) << std::endl;
                }
            }
        }

        // If we are idle, flush.
        bool idle = true;
        for (const auto& cs : conns_) {
            if (cs.st != conn_state::state::IDLE) {
                idle = false;
                break;
            }
        }
        if (idle) {
            std::lock_guard<std::mutex> lk(q_mu_);
            if (q_.empty() && delayed_.empty()) {
                flush_batch();
            }
        }
    }

    flush_batch();
}

} // namespace ariabc_pg
