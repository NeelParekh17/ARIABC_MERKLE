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
constexpr size_t kDetQueueHighWatermarkMin = 24;
constexpr size_t kDetQueueLowWatermarkMin = 12;
constexpr size_t kDetQueueHighWatermarkFactor = 2;  // high = max(minHigh, 2*pool)
constexpr size_t kDetQueueLowWatermarkFactor = 1;   // low  = max(minLow,  1*pool)
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
        std::string out = PQcmdStatus(res) ? PQcmdStatus(res) : "SELECT";
        const int rows = PQntuples(res);
        const int cols = PQnfields(res);
        std::vector<std::string> canonical_rows;
        canonical_rows.reserve(static_cast<size_t>(std::max(0, rows)));
        for (int r = 0; r < rows; ++r) {
            std::string row;
            for (int c = 0; c < cols; ++c) {
                row.push_back(' ');
                const char* v = PQgetvalue(res, r, c);
                if (v) row.append(v);
            }
            canonical_rows.push_back(std::move(row));
        }
        std::sort(canonical_rows.begin(), canonical_rows.end());
        for (const auto& row : canonical_rows) {
            out.append(row);
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

std::string json_escape(const std::string& in) {
    std::string out;
    out.reserve(in.size() + 16);
    for (unsigned char ch : in) {
        switch (ch) {
            case '\\': out += "\\\\"; break;
            case '"': out += "\\\""; break;
            case '\b': out += "\\b"; break;
            case '\f': out += "\\f"; break;
            case '\n': out += "\\n"; break;
            case '\r': out += "\\r"; break;
            case '\t': out += "\\t"; break;
            default:
                if (ch < 0x20) {
                    std::ostringstream oss;
                    oss << "\\u"
                        << std::hex << std::setw(4) << std::setfill('0')
                        << static_cast<int>(ch);
                    out += oss.str();
                } else {
                    out.push_back(static_cast<char>(ch));
                }
                break;
        }
    }
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
    std::ostringstream json;
    json << "{\"bid\":" << block_id << ",\"txs\":[";
    for (size_t i = 0; i < txs.size(); ++i) {
        if (i) json << ",";
        json << "{\"hash\":\"" << json_escape(txs[i].first)
             << "\",\"sql\":\"" << json_escape(txs[i].second)
             << "\"}";
    }
    json << "]}";

    std::ostringstream sql;
    sql << "SELECT bcdb_block_submit_results('"
        << sql_escape_literal(json.str())
        << "');";
    return sql.str();
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
        // Only send full result from the leader node; followers send hash-only.
        // This reduces Kafka bandwidth proportionally to the cluster size.
        // The gateway extracts the full result from the leader's payload and
        // verifies hashes from follower payloads for majority agreement.
        const bool include_full_result =
            (leader_node_id <= 0) ||
            (static_cast<int>(node_id) == leader_node_id);
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
        append_u32_le(out, static_cast<uint32_t>(results[i].size()));
        out.append(req_ids[i]);
        out.append(result_hash);
        out.append(sig);
        out.append(results[i]);
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
        return;
    }

    if (res) PQclear(res);
    bcdb_ctrl_conn_ = c;
    bcdb_init_done_ = true;
    std::cerr << "bcdb_init enabled on node " << node_id_
              << " with block_size=" << block_size
              << std::endl;
}

pg_executor::pg_executor(int node_id, const db_options& db_opt, const kafka_options& k_opt)
    : node_id_(node_id)
    , db_opt_(db_opt)
    , kafka_opt_(k_opt)
    , result_sig_key_(k_opt.result_sig_key)
    , kafka_enabled_(false)
{
    event_mode_ = is_event_mode(db_opt_.exec_mode);
    if (db_opt_.db_type == 1) {
        const char* v = ::getenv("ARIABC_DET_PARALLEL_WORKERS");
        if (v && *v) {
            const std::string s = trim_copy(v);
            det_parallel_workers_ =
                !(s.empty() || s == "0" || s == "false" || s == "FALSE" || s == "no" || s == "NO");
        }
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
    if (stop_.load()) return;
    ensure_bcdb_initialized_for_sql(sql);
    st_enqueued_.fetch_add(1, std::memory_order_relaxed);
    {
        const uint64_t now_ns = now_steady_ns();
        std::lock_guard<std::mutex> lk(q_mu_);
        task t;
        t.req_id = req_id;
        t.sql = sql;
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
        const size_t delayed = delayed_.size();
        const size_t inflight = static_cast<size_t>(st_inflight_cur_.load(std::memory_order_relaxed));
        const size_t queued = depth + delayed;
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
    return out;
}

kafka_producer_stats pg_executor::kafka_stats() const {
    return kafka_prod_.stats();
}

bool pg_executor::admission_control_blocked() const {
    return queue_overloaded_.load(std::memory_order_relaxed);
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
    return t.dispatch_seq;
}

void pg_executor::det_mark_tx_state(uint64_t tx_seq, det_tx_state st) {
    if (tx_seq == 0) return;
    std::lock_guard<std::mutex> lk(det_apply_mu_);
    if (st == det_tx_state::QUEUED && !det_apply_initialized_) {
        det_next_apply_seq_ = tx_seq;
        det_apply_initialized_ = true;
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
                                       const std::vector<task>& tasks,
                                       std::vector<std::string>& out_results) {
    out_results.clear();
    if (!c || tasks.empty()) return false;

    std::vector<std::pair<std::string, std::string>> txs;
    txs.reserve(tasks.size());
    uint64_t first_req_num = 0;

    for (size_t i = 0; i < tasks.size(); ++i) {
        uint64_t req_num = 0;
        if (!parse_req_num(tasks[i].req_id, req_num)) {
            return false;
        }
        uint64_t seq = 0;
        std::string raw_sql;
        if (!parse_det_prefixed_sql_parts(tasks[i].sql, &seq, &raw_sql)) {
            return false;
        }
        if (i == 0) first_req_num = req_num;
        txs.emplace_back(std::to_string(req_num), std::move(raw_sql));
    }

    const uint64_t block_id = first_req_num;
    const std::string sql = build_bcdb_block_submit_results_sql(block_id, txs);

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
    for (const auto& t : tasks) {
        uint64_t req_num = 0;
        if (!parse_req_num(t.req_id, req_num)) return false;
        const std::string key = std::to_string(req_num);
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
        for (size_t i = 0; i < batch_req_ids.size(); ++i) {
            std::vector<std::string> one_req{batch_req_ids[i]};
            std::vector<std::string> one_res{batch_results[i]};
            std::vector<uint64_t> one_log_idx{batch_raft_log_idxs[i]};
            std::vector<int> one_leader{batch_leader_hints[i]};

            const auto b0 = std::chrono::steady_clock::now();
            const std::string payload = build_bin_batch_payload_v2(
                one_req,
                one_res,
                one_log_idx,
                one_leader,
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
            if (!kafka_prod_.send_payload(payload, batch_req_ids[i], err)) {
                std::cerr << "Kafka send failed: " << err << std::endl;
            }
            const auto s1 = std::chrono::steady_clock::now();
            st_kafka_send_ns_.fetch_add(
                static_cast<uint64_t>(
                    std::chrono::duration_cast<std::chrono::nanoseconds>(s1 - s0).count()),
                std::memory_order_relaxed);
        }
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
                        expected, false, std::memory_order_relaxed, std::memory_order_relaxed)) {
                    st_queue_overload_exit_.fetch_add(1, std::memory_order_relaxed);
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
        const std::string result = exec_sql(c, t.sql);
        release_conn(c);
        if (det_prefixed_parallel) {
            det_finish_apply(det_tx_seq);
        }

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
        for (size_t i = 0; i < batch_req_ids.size(); ++i) {
            std::vector<std::string> one_req{batch_req_ids[i]};
            std::vector<std::string> one_res{batch_results[i]};
            std::vector<uint64_t> one_log_idx{batch_raft_log_idxs[i]};
            std::vector<int> one_leader{batch_leader_hints[i]};

            const auto b0 = std::chrono::steady_clock::now();
            const std::string payload = build_bin_batch_payload_v2(
                one_req,
                one_res,
                one_log_idx,
                one_leader,
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
            if (!kafka_prod_.send_payload(payload, batch_req_ids[i], err)) {
                std::cerr << "Kafka send failed: " << err << std::endl;
            }
            const auto s1 = std::chrono::steady_clock::now();
            st_kafka_send_ns_.fetch_add(
                static_cast<uint64_t>(
                    std::chrono::duration_cast<std::chrono::nanoseconds>(s1 - s0).count()),
                std::memory_order_relaxed);
        }
        batch_req_ids.clear();
        batch_results.clear();
        batch_raft_log_idxs.clear();
        batch_leader_hints.clear();
        batch_bytes = 0;
        batch_start = std::chrono::steady_clock::now();
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
                        expected, false, std::memory_order_relaxed, std::memory_order_relaxed)) {
                    st_queue_overload_exit_.fetch_add(1, std::memory_order_relaxed);
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
        {
            std::lock_guard<std::mutex> lk(q_mu_);
            while (!delayed_.empty() && delayed_.top().deadline_ns <= now_ns) {
                q_.push(delayed_.top().t);
                delayed_.pop();
            }
            st_delayed_cur_.store(static_cast<uint64_t>(delayed_.size()), std::memory_order_relaxed);
        }

        // Deterministic BCDB block fast path: batch a full block of parser-mode
        // requests into one SQL call so worker fanout can execute them together.
        if (!stop_.load() &&
            db_opt_.db_type == 1 &&
            !det_parallel_workers_ &&
            !det_raw_compat_mode_ &&
            bcdb_init_done_ &&
            !conns_.empty()) {
            bool any_inflight = false;
            for (const auto& cs : conns_) {
                if (cs.st != conn_state::state::IDLE) {
                    any_inflight = true;
                    break;
                }
            }

            if (!any_inflight) {
                const size_t block_cap =
                    std::max<size_t>(1, static_cast<size_t>(db_opt_.conn_pool_size));
                std::vector<task> det_batch;
                size_t depth_after_pop = 0;

                if (block_cap > 0) {
                    std::lock_guard<std::mutex> lk(q_mu_);
                    if (delayed_.empty() && !q_.empty()) {
                        std::queue<task> restored;
                        std::vector<task> candidate;
                        candidate.reserve(block_cap);
                        while (!q_.empty() && candidate.size() < block_cap) {
                            candidate.push_back(std::move(q_.front()));
                            q_.pop();
                        }

                        bool eligible = !candidate.empty();
                        for (size_t i = 0; eligible && i < candidate.size(); ++i) {
                            uint64_t req_num = 0;
                            if (!parse_req_num(candidate[i].req_id, req_num) ||
                                !is_det_prefixed_sql(candidate[i].sql)) {
                                eligible = false;
                                break;
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

                if (!det_batch.empty()) {
                    const uint64_t exec_begin_ns = now_steady_ns();
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

                    std::vector<std::string> det_results;
                    bool batch_ok = exec_det_block_batch(conns_[0].c, det_batch, det_results);
                    if (batch_ok) {
                        bool expected = false;
                        if (st_det_block_seen_.compare_exchange_strong(expected, true)) {
                            std::cerr << "det_block_batch active on node " << node_id_
                                      << " size=" << det_batch.size() << std::endl;
                        }
                    }
                    if (!batch_ok || det_results.size() != det_batch.size()) {
                        bool expected = false;
                        if (st_det_block_fallback_seen_.compare_exchange_strong(expected, true)) {
                            std::cerr << "det_block_batch fallback on node " << node_id_
                                      << " size=" << det_batch.size() << std::endl;
                        }
                        det_results.clear();
                        det_results.reserve(det_batch.size());
                        for (const auto& t : det_batch) {
                            det_results.push_back(exec_sql(conns_[0].c, t.sql));
                        }
                    }

                    const uint64_t finish_ns = now_steady_ns();
                    if (finish_ns >= exec_begin_ns) {
                        st_exec_ns_.fetch_add(finish_ns - exec_begin_ns, std::memory_order_relaxed);
                    }

                    for (size_t i = 0; i < det_batch.size(); ++i) {
                        const task& done_task = det_batch[i];
                        const std::string& out = det_results[i];
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
                    continue;
                }
            }
        }

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
                        !det_raw_compat_mode_ &&
                        db_opt_.db_type == 1 &&
                        bcdb_init_done_ &&
                        is_det_prefixed_sql(front.sql)) {
                        // Deterministic parser-mode throughput path is handled by the
                        // block submit fast path above. Do not leak these requests
                        // into the individual async-connection dispatch path.
                        break;
                    }
                    if (det_raw_compat_mode_ && inflight > 0) {
                        // Raw deterministic SQL compatibility mode:
                        // dispatch exactly one in-flight request at a time.
                        // This preserves commit order and keeps Merkle valid
                        // on installs that do not support "s <seq> <SQL>".
                        break;
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

            if (PQsendQuery(cs.c, t.sql.c_str()) != 1) {
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

        // Prepare poll set.
        std::vector<pollfd> pfds;
        pfds.reserve(1 + conns_.size());
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
                }
            }

            if (cs.st == conn_state::state::READING && (re & POLLIN)) {
                if (PQconsumeInput(cs.c) != 1) {
                    cs.st = conn_state::state::IDLE;
                    cs.has_task = false;
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
