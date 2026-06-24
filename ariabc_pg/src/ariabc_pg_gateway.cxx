#include "ariabc_pg_util.hxx"
#include "async_cluster_submitter.hxx"
#include "kafka_console.hxx"
#include "wire_protocol.hxx"

#include <arpa/inet.h>
#include <errno.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <signal.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <algorithm>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <cctype>
#include <cstdlib>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <memory>
#include <mutex>
#include <deque>
#include <set>
#include <sstream>
#include <stdexcept>
#include <limits>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <stdio.h>

#ifndef SSL_LIBRARY_NOT_FOUND
#include <openssl/bio.h>
#include <openssl/evp.h>
#include <openssl/hmac.h>
#include <openssl/pem.h>
#include <openssl/sha.h>
#endif

namespace ariabc_pg {

struct gateway_options {
    std::string query_from;
    int query_sign = 0;
    std::string pub_key_file;
    std::string priv_key_file;

    // 0: "s <SQL>" (safe wrapper), 1: "s <8digit(seq)> <SQL>" (det), 2: "<SQL>" (raw).
    int db_type = 2;
    uint64_t det_start_seq = 0;
    // Deterministic (dbType=1) compatibility mode:
    // 0 => send "s <8digit(seq)> <SQL>" (default behavior),
    // 1 => keep ordered deterministic submission but send raw SQL text.
    int det_raw_sql = 0;

    int tx_interval_ms = 0;
    int qrate = 0; // per terminal, 0=unthrottled
    int num_terminals = 1;
    std::string client_id = "cli";
    uint64_t req_id_offset = 0;

    std::string nodes_csv;

    std::string kafka_bootstrap;
    std::string result_topic = "topic2";
    std::string err_topic = "errTopic";
    std::string result_sig_key;

    // Faster majority-result detection under load.
    int poll_interval_us = 500;
    int poll_count = 0; // 0=forever

    // 1: wait for Kafka majority per request, 0: do not block on majority.
    // New default is direct completion path (no synchronous Kafka majority wait).
    int wait_majority = 0;

    // Completion path:
    //   - direct: append accepted by leader, completion returned on leader path.
    //   - kafka_majority: wait for Kafka majority terminal result synchronously.
    std::string completion_path = "direct";

    // Validation mode:
    //   - async_hash: follower divergence is observed asynchronously via Kafka.
    //   - strict_majority: legacy strict majority validation semantics.
    std::string validation_mode = "async_hash";

    // Optional override for deterministic mode in-flight window.
    // 0 => auto (larger pipeline for modern multi-core boxes).
    int det_window = 512;
    int det_batch_size = 16;
    // Optional DB connection-pool hint from benchmark harness. Auto window mode
    // scales from this value; explicit --detWindow is respected as-is.
    int db_conn_pool_size = 0;

    // Optional cap for concurrent in-flight submit RPCs in non-deterministic mode.
    // 0 => auto-tune based on num_terminals.
    int submit_limit = 0;

    // Gateway submit I/O mode:
    //  - "blocking" (default): per-caller blocking read/write on sockets.
    //  - "event": shared reactor thread with nonblocking sockets + multiplexing.
    std::string submit_mode = "blocking";

    // Deterministic leader-ACK pipelining.
    // 0 => disabled
    // 1 => enabled (default)
    int det_submit_pipeline = 1;

    // Deterministic client-lane pipeline depth.
    // 0 => auto from detWindow / numTerminals for backwards compatibility.
    int det_pipeline_depth = 0;

    // Optional per-worker in-flight majority window in non-deterministic mode.
    // 0 => auto (scales to keep total in-flight bounded).
    int nondet_window = 0;

    int total_nodes = 0; // 0 => use nodes.size()
    size_t vote_store_max_entries = 300000;

    // When 1: submit each request to ALL nodes (broadcast), not just the leader.
    // Used for kafka-only-no-raft profile where ordering is provided by the
    // gateway itself (sequential broadcast) rather than Raft consensus.
    int broadcast_to_all = 0;

    // In broadcast + async validation mode, allow the gateway to pipeline
    // after N accept replies, while still draining every late replica accept
    // before the process exits. 0 preserves the legacy majority accept.
    int broadcast_accept_quorum = 0;

    // In broadcast + direct completion mode, optionally wait for N accepted
    // replicas to finish executing the batch before counting it completed.
    // 0 preserves the legacy accept-quorum completion surface.
    int broadcast_result_quorum = 0;

    // 1: include the final blocking late-replica accept drain in the reported
    // workload time. 0: report client-visible accept-quorum completion time,
    // then drain late accepts before exit so the harness can still verify all
    // replicas with the post-workload marker/Merkle gate.
    int broadcast_drain_in_timed_run = 1;

    // In non-broadcast direct-completion mode, wait for this many replicas to
    // apply the submitted Raft log entry before reporting completion. The
    // benchmark workload keeps the default single-node wait; correctness
    // markers can raise this to all nodes as a catch-up barrier.
    int direct_completion_quorum = 1;

    // Number of parallel TCP connections to open per logical node. >1 gives
    // the gateway multiple concurrent write paths into a single server and
    // forces the server to spawn that many per-connection handler threads,
    // which is essential for single-node gateway-direct throughput to scale.
    int conn_fanout = 1;
};

void usage(const char* argv0) {
    std::cout
        << "Usage:\n"
        << "  " << argv0 << " \\\n"
        << "    --queryFrom <file|port> --nodes <host:port,host:port,...> \\\n"
        << "    [--querySign 0|1] [--pubKeyFile <path>] [--privKeyFile <path>] \\\n"
        << "    [--dbType 0|1|2] [--detStartSeq <n>] [--detRawSql 0|1] [--qrate <n>] [--txIntervalMs <ms>] \\\n"
        << "    [--numTerminals <N>] [--clientId <id>] [--reqIdOffset <n>] \\\n"
        << "    [--kafkaBootstrap <host:port>] \\\n"
        << "    [--resultTopic <t>] [--errTopic <t>] [--resultSigKey <k>] \\\n"
        << "    [--pollIntervalUs <us>] [--pollCount <n>] [--waitMajority 0|1] [--completionPath direct|kafka_majority] [--validationMode async_hash|strict_majority] [--detWindow <n>] [--detBatchSize <n>] [--dbConnPoolSize <n>] [--submitLimit <n>] [--submitMode blocking|event] [--detSubmitPipeline 0|1] [--detPipelineDepth <n>] [--nondetWindow <n>] [--totalNodes <n>] [--voteStoreMax <n>] [--broadcastToAll 0|1] [--broadcastAcceptQuorum <n>] [--broadcastResultQuorum <n>] [--broadcastDrainInTimedRun 0|1] [--directCompletionQuorum <n>] [--connFanout <N>]\n";
}

bool parse_args(int argc, char** argv, gateway_options& opt, std::string& err) {
    bool completion_path_explicit = false;
    for (int i = 1; i < argc; ++i) {
        const std::string a = argv[i];
        auto need = [&](const char* flag) -> std::string {
            if (i + 1 >= argc) throw std::runtime_error(std::string("missing value for ") + flag);
            return std::string(argv[++i]);
        };

        try {
            if (a == "--help" || a == "-h") {
                usage(argv[0]);
                exit(0);
            } else if (a == "--queryFrom") {
                opt.query_from = need("--queryFrom");
            } else if (a == "--querySign") {
                opt.query_sign = std::stoi(need("--querySign"));
            } else if (a == "--pubKeyFile") {
                opt.pub_key_file = need("--pubKeyFile");
            } else if (a == "--privKeyFile") {
                opt.priv_key_file = need("--privKeyFile");
            } else if (a == "--dbType") {
                opt.db_type = std::stoi(need("--dbType"));
            } else if (a == "--detStartSeq") {
                opt.det_start_seq = static_cast<uint64_t>(std::stoull(need("--detStartSeq")));
            } else if (a == "--detRawSql") {
                opt.det_raw_sql = std::stoi(need("--detRawSql"));
            } else if (a == "--qrate") {
                opt.qrate = std::stoi(need("--qrate"));
            } else if (a == "--txIntervalMs") {
                opt.tx_interval_ms = std::stoi(need("--txIntervalMs"));
            } else if (a == "--numTerminals") {
                opt.num_terminals = std::stoi(need("--numTerminals"));
            } else if (a == "--clientId") {
                opt.client_id = need("--clientId");
            } else if (a == "--reqIdOffset") {
                opt.req_id_offset = static_cast<uint64_t>(std::stoull(need("--reqIdOffset")));
            } else if (a == "--nodes") {
                opt.nodes_csv = need("--nodes");
            } else if (a == "--kafkaBootstrap") {
                opt.kafka_bootstrap = need("--kafkaBootstrap");
            } else if (a == "--resultTopic") {
                opt.result_topic = need("--resultTopic");
            } else if (a == "--errTopic") {
                opt.err_topic = need("--errTopic");
            } else if (a == "--resultSigKey") {
                opt.result_sig_key = need("--resultSigKey");
            } else if (a == "--pollIntervalUs") {
                opt.poll_interval_us = std::stoi(need("--pollIntervalUs"));
            } else if (a == "--pollCount") {
                opt.poll_count = std::stoi(need("--pollCount"));
            } else if (a == "--waitMajority") {
                opt.wait_majority = std::stoi(need("--waitMajority"));
            } else if (a == "--completionPath") {
                opt.completion_path = ariabc_pg::trim_copy(need("--completionPath"));
                completion_path_explicit = true;
            } else if (a == "--validationMode") {
                opt.validation_mode = ariabc_pg::trim_copy(need("--validationMode"));
            } else if (a == "--detWindow") {
                opt.det_window = std::stoi(need("--detWindow"));
            } else if (a == "--detBatchSize") {
                opt.det_batch_size = std::stoi(need("--detBatchSize"));
            } else if (a == "--dbConnPoolSize") {
                opt.db_conn_pool_size = std::stoi(need("--dbConnPoolSize"));
            } else if (a == "--submitLimit") {
                opt.submit_limit = std::stoi(need("--submitLimit"));
            } else if (a == "--submitMode") {
                opt.submit_mode = need("--submitMode");
            } else if (a == "--detSubmitPipeline") {
                opt.det_submit_pipeline = std::stoi(need("--detSubmitPipeline"));
            } else if (a == "--detPipelineDepth") {
                opt.det_pipeline_depth = std::stoi(need("--detPipelineDepth"));
            } else if (a == "--nondetWindow") {
                opt.nondet_window = std::stoi(need("--nondetWindow"));
            } else if (a == "--totalNodes") {
                opt.total_nodes = std::stoi(need("--totalNodes"));
            } else if (a == "--voteStoreMax") {
                opt.vote_store_max_entries = static_cast<size_t>(std::stoull(need("--voteStoreMax")));
            } else if (a == "--broadcastToAll") {
                opt.broadcast_to_all = std::stoi(need("--broadcastToAll"));
            } else if (a == "--broadcastAcceptQuorum") {
                opt.broadcast_accept_quorum = std::stoi(need("--broadcastAcceptQuorum"));
            } else if (a == "--broadcastResultQuorum") {
                opt.broadcast_result_quorum = std::stoi(need("--broadcastResultQuorum"));
            } else if (a == "--broadcastDrainInTimedRun") {
                opt.broadcast_drain_in_timed_run = std::stoi(need("--broadcastDrainInTimedRun"));
            } else if (a == "--directCompletionQuorum") {
                opt.direct_completion_quorum = std::stoi(need("--directCompletionQuorum"));
            } else if (a == "--connFanout") {
                opt.conn_fanout = std::stoi(need("--connFanout"));
                if (opt.conn_fanout < 1) opt.conn_fanout = 1;
            } else {
                throw std::runtime_error("unknown flag: " + a);
            }
        } catch (const std::exception& e) {
            err = e.what();
            return false;
        }
    }

    if (opt.query_from.empty()) {
        err = "missing --queryFrom";
        return false;
    }
    if (opt.nodes_csv.empty()) {
        err = "missing --nodes";
        return false;
    }
    if (opt.db_type != 0 && opt.db_type != 1 && opt.db_type != 2) {
        err = "invalid --dbType (expected 0, 1, or 2)";
        return false;
    }
    if (opt.db_type == 1 && opt.det_start_seq >= 100000000ULL) {
        err = "--detStartSeq too large for 8-digit seq";
        return false;
    }
    if (opt.det_raw_sql != 0 && opt.det_raw_sql != 1) {
        err = "invalid --detRawSql (expected 0 or 1)";
        return false;
    }
    if (opt.qrate < 0) {
        err = "invalid --qrate (expected >= 0)";
        return false;
    }
    if (opt.num_terminals <= 0) {
        err = "invalid --numTerminals";
        return false;
    }
    if (opt.poll_interval_us <= 0) {
        err = "invalid --pollIntervalUs";
        return false;
    }
    if (opt.wait_majority != 0 && opt.wait_majority != 1) {
        err = "invalid --waitMajority (expected 0 or 1)";
        return false;
    }
    if (opt.completion_path != "direct" && opt.completion_path != "kafka_majority") {
        err = "invalid --completionPath (expected direct|kafka_majority)";
        return false;
    }
    if (opt.validation_mode != "async_hash" && opt.validation_mode != "strict_majority") {
        err = "invalid --validationMode (expected async_hash|strict_majority)";
        return false;
    }
    // completion_path is authoritative when explicitly provided.
    if (completion_path_explicit) {
        if (opt.completion_path == "direct") {
            opt.wait_majority = 0;
        } else if (opt.completion_path == "kafka_majority") {
            opt.wait_majority = 1;
        }
    } else {
        // Keep completion_path/legacy waitMajority coherent for profile/reporting.
        opt.completion_path = (opt.wait_majority == 1) ? "kafka_majority" : "direct";
    }
    if (opt.validation_mode == "strict_majority" && opt.wait_majority == 0) {
        opt.wait_majority = 1;
        opt.completion_path = "kafka_majority";
    }
    if (opt.det_window < 0) {
        err = "invalid --detWindow (expected >= 0)";
        return false;
    }
    if (opt.det_batch_size <= 0) {
        err = "invalid --detBatchSize (expected > 0)";
        return false;
    }
    if (opt.db_conn_pool_size < 0) {
        err = "invalid --dbConnPoolSize (expected >= 0)";
        return false;
    }
    if (opt.submit_limit < 0) {
        err = "invalid --submitLimit (expected >= 0)";
        return false;
    }
    if (opt.det_submit_pipeline != 0 && opt.det_submit_pipeline != 1) {
        err = "invalid --detSubmitPipeline (expected 0 or 1)";
        return false;
    }
    if (opt.det_pipeline_depth < 0) {
        err = "invalid --detPipelineDepth (expected >= 0)";
        return false;
    }
    if (opt.nondet_window < 0) {
        err = "invalid --nondetWindow (expected >= 0)";
        return false;
    }
    {
        const std::string m = ariabc_pg::trim_copy(opt.submit_mode);
        if (!m.empty() && m != "blocking" && m != "event") {
            err = "invalid --submitMode (expected blocking|event)";
            return false;
        }
    }
    if (opt.vote_store_max_entries == 0) {
        err = "invalid --voteStoreMax (expected > 0)";
        return false;
    }
    if (opt.broadcast_accept_quorum < 0) {
        err = "invalid --broadcastAcceptQuorum (expected >= 0)";
        return false;
    }
    if (opt.broadcast_result_quorum < 0) {
        err = "invalid --broadcastResultQuorum (expected >= 0)";
        return false;
    }
    if (opt.broadcast_drain_in_timed_run != 0 && opt.broadcast_drain_in_timed_run != 1) {
        err = "invalid --broadcastDrainInTimedRun (expected 0|1)";
        return false;
    }
    if (opt.direct_completion_quorum <= 0) {
        err = "invalid --directCompletionQuorum (expected > 0)";
        return false;
    }
    if (opt.query_sign != 0 && opt.query_sign != 1) {
        err = "invalid --querySign (expected 0 or 1)";
        return false;
    }
    if (opt.query_sign == 1 && opt.pub_key_file.empty()) {
        err = "--pubKeyFile is required when --querySign=1";
        return false;
    }
    return true;
}

std::string format_det_seq8(uint64_t seq) {
    std::ostringstream oss;
    oss << std::setw(8) << std::setfill('0') << seq;
    return oss.str();
}

bool is_duplicate_key_result(const std::string& s) {
    if (s.find("23505") != std::string::npos) return true;
    std::string lower;
    lower.reserve(s.size());
    for (unsigned char ch : s) lower.push_back(static_cast<char>(std::tolower(ch)));
    return (lower.find("duplicate key") != std::string::npos) ||
           (lower.find("unique constraint") != std::string::npos);
}

int connect_tcp(const std::string& host, int port, std::string& err) {
    struct addrinfo hints;
    ::memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    struct addrinfo* res = nullptr;
    const std::string port_s = std::to_string(port);
    const int rc = ::getaddrinfo(host.c_str(), port_s.c_str(), &hints, &res);
    if (rc != 0) {
        err = std::string("getaddrinfo failed: ") + gai_strerror(rc);
        return -1;
    }

    int fd = -1;
    for (struct addrinfo* p = res; p; p = p->ai_next) {
        fd = ::socket(p->ai_family, p->ai_socktype, p->ai_protocol);
        if (fd < 0) continue;
        if (::connect(fd, p->ai_addr, p->ai_addrlen) == 0) {
            // Low-latency request/response: avoid Nagle delays on small frames.
            int one = 1;
            (void)::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
            ::freeaddrinfo(res);
            return fd;
        }
        ::close(fd);
        fd = -1;
    }
    ::freeaddrinfo(res);
    err = std::string("connect failed: ") + ::strerror(errno);
    return -1;
}

bool parse_signed_sql_line(const std::string& line,
                           std::string& out_sql,
                           std::string& out_sig_b64,
                           std::string& err)
{
    const std::string s = trim_copy(line);
    const size_t pos = s.rfind(';');
    if (pos == std::string::npos || pos + 1 >= s.size()) {
        err = "expected 'SQL; <base64_sig>'";
        return false;
    }
    out_sql = trim_copy(s.substr(0, pos + 1));
    out_sig_b64 = trim_copy(s.substr(pos + 1));
    if (out_sql.empty() || out_sig_b64.empty()) {
        err = "empty sql or signature";
        return false;
    }
    return true;
}

bool parse_kafka_result_line(const std::string& line,
                             std::string& out_req_id,
                             int& out_node_id,
                             std::string& out_result)
{
    // "<req_id>␠␠<node_id>␠␠<result>"
    const std::string sep = "  ";
    const size_t p1 = line.find(sep);
    if (p1 == std::string::npos) return false;
    const size_t p2 = line.find(sep, p1 + sep.size());
    if (p2 == std::string::npos) return false;

    out_req_id = line.substr(0, p1);
    const std::string node_s = line.substr(p1 + sep.size(), p2 - (p1 + sep.size()));
    out_result = line.substr(p2 + sep.size());
    try {
        out_node_id = std::stoi(trim_copy(node_s));
    } catch (...) {
        return false;
    }
    out_req_id = trim_copy(out_req_id);
    return !out_req_id.empty();
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

bool starts_with(const std::string& s, const std::string& prefix) {
    return s.size() >= prefix.size() && s.compare(0, prefix.size(), prefix) == 0;
}

std::string lower_copy(const std::string& s) {
    std::string out;
    out.reserve(s.size());
    for (unsigned char ch : s) out.push_back(static_cast<char>(std::tolower(ch)));
    return out;
}

bool is_reset_barrier_sql(const std::string& sql) {
    const std::string t = lower_copy(trim_copy(sql));
    // Keep this strict to avoid adding barrier latency to hot-path workloads.
    return t.find("bcdb_reset(") != std::string::npos;
}

bool send_control_req_to_node(const host_port& hp,
                              const std::string& control_sql,
                              client_api_response& out_resp,
                              std::string& err)
{
    err.clear();
    int fd = connect_tcp(hp.host, hp.port, err);
    if (fd < 0) return false;

    const uint64_t ts_ms = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count());
    client_api_request req;
    req.req_id = "ctrl-" + std::to_string(ts_ms);
    req.sql = control_sql;

    std::string io_err;
    if (!write_request_frame(fd, req, io_err)) {
        ::close(fd);
        err = io_err;
        return false;
    }

    if (!read_response_frame(fd, out_resp, io_err)) {
        ::close(fd);
        err = io_err;
        return false;
    }

    ::close(fd);
    return true;
}

bool parse_u64_str(const std::string& s, uint64_t& out) {
    out = 0;
    try {
        out = static_cast<uint64_t>(std::stoull(trim_copy(s)));
        return true;
    } catch (...) {
        return false;
    }
}

bool parse_named_u64_field(const std::string& msg,
                           const std::string& key,
                           uint64_t& out) {
    out = 0;
    const size_t pos = msg.find(key);
    if (pos == std::string::npos) return false;
    size_t i = pos + key.size();
    if (i >= msg.size() || !std::isdigit(static_cast<unsigned char>(msg[i]))) {
        return false;
    }
    uint64_t v = 0;
    while (i < msg.size() && std::isdigit(static_cast<unsigned char>(msg[i]))) {
        v = (v * 10) + static_cast<uint64_t>(msg[i] - '0');
        ++i;
    }
    out = v;
    return true;
}

struct kafka_reply_record {
    uint64_t req_num = 0;
    uint64_t raft_log_idx = 0;
    std::string req_id;
    int node_id = -1;
    int leader_node_id = -1;
    std::string result_hash;
    std::string hash_algo;
    std::string server_sig;
    uint64_t timestamp_ms = 0;
    std::string full_result;
    bool has_full_result = false;
};

constexpr const char* kHashAlgo = "sha256";
constexpr uint8_t kHashAlgoIdSha256 = 1;
constexpr const char* kDefaultResultSigKey = "ariabc-result-v2-dev-key";

uint8_t read_u8(const char* p) {
    return static_cast<uint8_t>(p[0]);
}

uint16_t read_u16_le(const char* p) {
    return static_cast<uint16_t>(
        static_cast<uint8_t>(p[0]) |
        (static_cast<uint16_t>(static_cast<uint8_t>(p[1])) << 8));
}

uint32_t read_u32_le(const char* p) {
    return static_cast<uint32_t>(
        static_cast<uint8_t>(p[0]) |
        (static_cast<uint32_t>(static_cast<uint8_t>(p[1])) << 8) |
        (static_cast<uint32_t>(static_cast<uint8_t>(p[2])) << 16) |
        (static_cast<uint32_t>(static_cast<uint8_t>(p[3])) << 24));
}

uint64_t read_u64_le(const char* p) {
    uint64_t v = 0;
    for (int i = 0; i < 8; ++i) {
        v |= (static_cast<uint64_t>(static_cast<uint8_t>(p[i])) << (8 * i));
    }
    return v;
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

uint64_t now_epoch_ms() {
    using namespace std::chrono;
    return static_cast<uint64_t>(
        duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count());
}

uint64_t steady_now_ns() {
    using namespace std::chrono;
    return static_cast<uint64_t>(
        duration_cast<nanoseconds>(steady_clock::now().time_since_epoch()).count());
}

bool trusted_result_sig_fastpath_enabled() {
    static const bool enabled = []() -> bool {
        const char* v = std::getenv("ARIABC_TRUSTED_RESULT_SIG_FASTPATH");
        if (!v || !*v) return false;
        const std::string s = trim_copy(v);
        return !(s.empty() || s == "0" || s == "false" || s == "FALSE" || s == "no" || s == "NO");
    }();
    return enabled;
}

void atomic_max_u64(std::atomic<uint64_t>& dst, uint64_t v) {
    uint64_t cur = dst.load(std::memory_order_relaxed);
    while (cur < v &&
           !dst.compare_exchange_weak(cur, v,
                                     std::memory_order_relaxed,
                                     std::memory_order_relaxed)) {
    }
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

std::string make_sig_payload_legacy(uint64_t req_num,
                                    const std::string& req_id,
                                    int node_id,
                                    int leader_node_id,
                                    const std::string& result_hash,
                                    uint64_t timestamp_ms,
                                    bool has_full_result)
{
    std::ostringstream oss;
    oss << req_num << "|"
        << req_id << "|"
        << node_id << "|"
        << leader_node_id << "|"
        << result_hash << "|"
        << kHashAlgo << "|"
        << timestamp_ms << "|"
        << (has_full_result ? 1 : 0);
    return oss.str();
}

bool verify_result_signature(const kafka_reply_record& rec, const std::string& sig_key) {
    if (rec.req_id.empty()) return false;
    if (rec.hash_algo != kHashAlgo) return false;
    if (trusted_result_sig_fastpath_enabled()) {
        return !rec.result_hash.empty();
    }
    if (rec.server_sig.empty()) return false;
    const bool has_full = rec.has_full_result;
    const std::string payload = make_sig_payload(rec.req_num,
                                                 rec.raft_log_idx,
                                                 rec.req_id,
                                                 rec.node_id,
                                                 rec.leader_node_id,
                                                 rec.result_hash,
                                                 rec.timestamp_ms,
                                                 has_full);
    const std::string expected = sign_payload(sig_key, payload);
    if (!expected.empty() && expected == rec.server_sig) {
        return true;
    }
    // Backward compatibility: accept pre-raft_log_idx signature shape.
    if (rec.raft_log_idx == 0) {
        const std::string legacy = make_sig_payload_legacy(rec.req_num,
                                                           rec.req_id,
                                                           rec.node_id,
                                                           rec.leader_node_id,
                                                           rec.result_hash,
                                                           rec.timestamp_ms,
                                                           has_full);
        const std::string expected_legacy = sign_payload(sig_key, legacy);
        if (!expected_legacy.empty() && expected_legacy == rec.server_sig) {
            return true;
        }
    }
    return false;
}

bool parse_kafka_payload_records(const std::string& payload,
                                 std::vector<kafka_reply_record>& out)
{
    out.clear();
    if (payload.size() >= 8 && payload[0] == 'B' && payload[1] == '3') {
        const char* p = payload.data();
        size_t pos = 0;
        const uint16_t nrec = read_u16_le(p + 2);
        pos = 8; // magic+ver+count+reserved
        out.reserve(nrec);
        for (uint16_t i = 0; i < nrec; ++i) {
            if (pos + 40 > payload.size()) return false;
            kafka_reply_record r;
            r.req_num = read_u64_le(p + pos);
            pos += 8;
            r.raft_log_idx = read_u64_le(p + pos);
            pos += 8;
            r.node_id = static_cast<int>(read_u16_le(p + pos));
            pos += 2;
            r.leader_node_id = static_cast<int>(read_u16_le(p + pos));
            if (r.leader_node_id == 0) r.leader_node_id = -1;
            pos += 2;
            r.timestamp_ms = read_u64_le(p + pos);
            pos += 8;
            const uint8_t flags = read_u8(p + pos);
            pos += 1;
            const uint8_t hash_algo_id = read_u8(p + pos);
            pos += 1;
            const uint16_t req_id_len = read_u16_le(p + pos);
            pos += 2;
            const uint16_t hash_len = read_u16_le(p + pos);
            pos += 2;
            const uint16_t sig_len = read_u16_le(p + pos);
            pos += 2;
            const uint32_t full_len = read_u32_le(p + pos);
            pos += 4;
            if (pos + req_id_len + hash_len + sig_len + full_len > payload.size()) return false;
            r.req_id.assign(p + pos, p + pos + req_id_len);
            pos += req_id_len;
            r.result_hash.assign(p + pos, p + pos + hash_len);
            pos += hash_len;
            r.server_sig.assign(p + pos, p + pos + sig_len);
            pos += sig_len;
            r.has_full_result = ((flags & 0x1u) != 0u);
            if (r.has_full_result) {
                r.full_result.assign(p + pos, p + pos + full_len);
            }
            pos += full_len;
            if (hash_algo_id == kHashAlgoIdSha256) {
                r.hash_algo = kHashAlgo;
            } else {
                r.hash_algo = "unknown";
            }
            out.push_back(std::move(r));
        }
        return true;
    }

    // Legacy B2 payload support (without raft_log_idx).
    if (payload.size() >= 8 && payload[0] == 'B' && payload[1] == '2') {
        const char* p = payload.data();
        size_t pos = 0;
        const uint16_t nrec = read_u16_le(p + 2);
        pos = 8; // magic+ver+count+reserved
        out.reserve(nrec);
        for (uint16_t i = 0; i < nrec; ++i) {
            if (pos + 32 > payload.size()) return false;
            kafka_reply_record r;
            r.req_num = read_u64_le(p + pos);
            pos += 8;
            r.node_id = static_cast<int>(read_u16_le(p + pos));
            pos += 2;
            r.leader_node_id = static_cast<int>(read_u16_le(p + pos));
            if (r.leader_node_id == 0) r.leader_node_id = -1;
            pos += 2;
            r.timestamp_ms = read_u64_le(p + pos);
            pos += 8;
            const uint8_t flags = read_u8(p + pos);
            pos += 1;
            const uint8_t hash_algo_id = read_u8(p + pos);
            pos += 1;
            const uint16_t req_id_len = read_u16_le(p + pos);
            pos += 2;
            const uint16_t hash_len = read_u16_le(p + pos);
            pos += 2;
            const uint16_t sig_len = read_u16_le(p + pos);
            pos += 2;
            const uint32_t full_len = read_u32_le(p + pos);
            pos += 4;
            if (pos + req_id_len + hash_len + sig_len + full_len > payload.size()) return false;
            r.req_id.assign(p + pos, p + pos + req_id_len);
            pos += req_id_len;
            r.result_hash.assign(p + pos, p + pos + hash_len);
            pos += hash_len;
            r.server_sig.assign(p + pos, p + pos + sig_len);
            pos += sig_len;
            r.has_full_result = ((flags & 0x1u) != 0u);
            if (r.has_full_result) {
                r.full_result.assign(p + pos, p + pos + full_len);
            }
            pos += full_len;
            if (hash_algo_id == kHashAlgoIdSha256) {
                r.hash_algo = kHashAlgo;
            } else {
                r.hash_algo = "unknown";
            }
            out.push_back(std::move(r));
        }
        return true;
    }

    // Legacy B1 payload support (unsigned).
    if (payload.size() >= 8 && payload[0] == 'B' && payload[1] == '1') {
        const char* p = payload.data();
        size_t pos = 0;
        const uint16_t nrec = read_u16_le(p + 2);
        pos = 8; // magic+ver+count+reserved
        out.reserve(nrec);
        for (uint16_t i = 0; i < nrec; ++i) {
            if (pos + 14 > payload.size()) return false;
            kafka_reply_record r;
            r.req_num = read_u64_le(p + pos);
            pos += 8;
            r.node_id = static_cast<int>(read_u16_le(p + pos));
            pos += 2;
            const uint32_t rlen = read_u32_le(p + pos);
            pos += 4;
            if (pos + rlen > payload.size()) return false;
            r.full_result.assign(p + pos, p + pos + rlen);
            pos += rlen;
            r.has_full_result = true;
            r.result_hash = canonical_result_hash(r.full_result);
            r.hash_algo = kHashAlgo;
            out.push_back(std::move(r));
        }
        return true;
    }

    std::string req_id;
    int node_id = -1;
    std::string result;
    if (!parse_kafka_result_line(payload, req_id, node_id, result)) {
        return false;
    }
    uint64_t req_num = 0;
    if (!parse_req_num(req_id, req_num)) {
        return false;
    }
    kafka_reply_record r;
    r.req_num = req_num;
    r.req_id = req_id;
    r.node_id = node_id;
    r.result_hash = canonical_result_hash(result);
    r.hash_algo = kHashAlgo;
    r.full_result = std::move(result);
    r.has_full_result = true;
    out.push_back(std::move(r));
    return true;
}

std::string json_escape(const std::string& s) {
    std::string out;
    out.reserve(s.size() + 16);
    for (unsigned char ch : s) {
        switch (ch) {
        case '\\': out += "\\\\"; break;
        case '"': out += "\\\""; break;
        case '\n': out += "\\n"; break;
        case '\r': out += "\\r"; break;
        case '\t': out += "\\t"; break;
        default:
            if (ch < 0x20) {
                char buf[7];
                ::snprintf(buf, sizeof(buf), "\\u%04x", static_cast<unsigned>(ch));
                out += buf;
            } else {
                out.push_back(static_cast<char>(ch));
            }
        }
    }
    return out;
}

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

std::atomic<uint64_t> g_debug_submit_trace_count(0);
std::atomic<uint64_t> g_debug_kafka_trace_count(0);

void debug_trace_submit(uint64_t req_num, const std::string& req_id, const std::string& sql) {
    if (!debug_req_trace_enabled()) return;
    const uint64_t idx = g_debug_submit_trace_count.fetch_add(1, std::memory_order_relaxed);
    if (idx >= debug_req_trace_limit()) return;
    std::string sql_head = trim_copy(sql);
    if (sql_head.size() > 96) sql_head.resize(96);
    std::cerr << "REQ_TRACE submit"
              << " idx=" << idx
              << " req_num=" << req_num
              << " req_id=" << req_id
              << " sql=" << json_escape(sql_head)
              << std::endl;
}

void debug_trace_kafka(const kafka_reply_record& rec, bool sig_valid) {
    if (!debug_req_trace_enabled()) return;
    const uint64_t idx = g_debug_kafka_trace_count.fetch_add(1, std::memory_order_relaxed);
    if (idx >= debug_req_trace_limit()) return;
    std::cerr << "REQ_TRACE kafka"
              << " idx=" << idx
              << " req_num=" << rec.req_num
              << " req_id=" << rec.req_id
              << " node=" << rec.node_id
              << " leader=" << rec.leader_node_id
              << " raft_log_idx=" << rec.raft_log_idx
              << " sig=" << (sig_valid ? 1 : 0)
              << " full=" << (rec.has_full_result ? 1 : 0)
              << std::endl;
}

#ifndef SSL_LIBRARY_NOT_FOUND
struct openssl_keypair {
    EVP_PKEY* pub = nullptr;
    EVP_PKEY* priv = nullptr;

    ~openssl_keypair() {
        if (pub) EVP_PKEY_free(pub);
        if (priv) EVP_PKEY_free(priv);
    }
};

bool load_public_key(const std::string& path, EVP_PKEY*& out, std::string& err) {
    const std::string content = read_file_all(path);
    BIO* bio = BIO_new_mem_buf(content.data(), static_cast<int>(content.size()));
    if (!bio) {
        err = "BIO_new_mem_buf failed";
        return false;
    }

    EVP_PKEY* key = PEM_read_bio_PUBKEY(bio, nullptr, nullptr, nullptr);
    BIO_free(bio);
    if (key) {
        out = key;
        return true;
    }

    // Fallback: Base64 DER (SubjectPublicKeyInfo).
    try {
        const std::string b64 = trim_copy(content);
        const std::string der = base64_decode(b64);
        const unsigned char* p = reinterpret_cast<const unsigned char*>(der.data());
        key = d2i_PUBKEY(nullptr, &p, static_cast<long>(der.size()));
        if (!key) {
            err = "failed to parse public key (PEM or base64 DER)";
            return false;
        }
        out = key;
        return true;
    } catch (const std::exception& e) {
        err = std::string("public key decode failed: ") + e.what();
        return false;
    }
}

bool load_private_key(const std::string& path, EVP_PKEY*& out, std::string& err) {
    const std::string content = read_file_all(path);
    BIO* bio = BIO_new_mem_buf(content.data(), static_cast<int>(content.size()));
    if (!bio) {
        err = "BIO_new_mem_buf failed";
        return false;
    }

    EVP_PKEY* key = PEM_read_bio_PrivateKey(bio, nullptr, nullptr, nullptr);
    BIO_free(bio);
    if (key) {
        out = key;
        return true;
    }

    // Fallback: Base64 DER (PKCS8).
    try {
        const std::string b64 = trim_copy(content);
        const std::string der = base64_decode(b64);
        const unsigned char* p = reinterpret_cast<const unsigned char*>(der.data());
        key = d2i_AutoPrivateKey(nullptr, &p, static_cast<long>(der.size()));
        if (!key) {
            err = "failed to parse private key (PEM or base64 DER)";
            return false;
        }
        out = key;
        return true;
    } catch (const std::exception& e) {
        err = std::string("private key decode failed: ") + e.what();
        return false;
    }
}

bool verify_sig_sha256_rsa(EVP_PKEY* pub,
                           const std::string& msg,
                           const std::string& sig_b64,
                           std::string& err)
{
    if (!pub) {
        err = "public key not loaded";
        return false;
    }
    std::string sig;
    try {
        sig = base64_decode(sig_b64);
    } catch (const std::exception& e) {
        err = std::string("base64 sig decode failed: ") + e.what();
        return false;
    }

    EVP_MD_CTX* ctx = EVP_MD_CTX_new();
    if (!ctx) {
        err = "EVP_MD_CTX_new failed";
        return false;
    }
    bool ok = false;
    do {
        if (EVP_DigestVerifyInit(ctx, nullptr, EVP_sha256(), nullptr, pub) != 1) {
            err = "EVP_DigestVerifyInit failed";
            break;
        }
        if (EVP_DigestVerifyUpdate(ctx, msg.data(), msg.size()) != 1) {
            err = "EVP_DigestVerifyUpdate failed";
            break;
        }
        const int rc = EVP_DigestVerifyFinal(ctx,
                                             reinterpret_cast<const unsigned char*>(sig.data()),
                                             sig.size());
        if (rc == 1) {
            ok = true;
        } else {
            err = "signature verification failed";
        }
    } while (false);

    EVP_MD_CTX_free(ctx);
    return ok;
}

bool sign_sha256_rsa(EVP_PKEY* priv,
                     const std::string& msg,
                     std::string& out_sig_b64,
                     std::string& err)
{
    if (!priv) {
        err = "private key not loaded";
        return false;
    }
    EVP_MD_CTX* ctx = EVP_MD_CTX_new();
    if (!ctx) {
        err = "EVP_MD_CTX_new failed";
        return false;
    }

    bool ok = false;
    do {
        if (EVP_DigestSignInit(ctx, nullptr, EVP_sha256(), nullptr, priv) != 1) {
            err = "EVP_DigestSignInit failed";
            break;
        }
        if (EVP_DigestSignUpdate(ctx, msg.data(), msg.size()) != 1) {
            err = "EVP_DigestSignUpdate failed";
            break;
        }
        size_t sig_len = 0;
        if (EVP_DigestSignFinal(ctx, nullptr, &sig_len) != 1) {
            err = "EVP_DigestSignFinal(size) failed";
            break;
        }
        std::string sig(sig_len, '\0');
        if (EVP_DigestSignFinal(ctx,
                                reinterpret_cast<unsigned char*>(&sig[0]),
                                &sig_len) != 1) {
            err = "EVP_DigestSignFinal(data) failed";
            break;
        }
        sig.resize(sig_len);
        out_sig_b64 = base64_encode(sig);
        ok = true;
    } while (false);

    EVP_MD_CTX_free(ctx);
    return ok;
}
#endif

struct vote_entry {
    struct node_obs {
        kafka_reply_record rec;
        bool sig_valid = false;
    };

    struct reply_identity {
        int node_id = 0;
        uint64_t raft_log_idx = 0;

        reply_identity() = default;
        reply_identity(int node_id_in, uint64_t raft_log_idx_in)
            : node_id(node_id_in)
            , raft_log_idx(raft_log_idx_in)
            {}

        bool operator==(const reply_identity& other) const {
            return node_id == other.node_id && raft_log_idx == other.raft_log_idx;
        }
    };

    struct reply_identity_hash {
        size_t operator()(const reply_identity& key) const {
            const uint64_t a = static_cast<uint64_t>(static_cast<uint32_t>(key.node_id));
            const uint64_t b = key.raft_log_idx;
            return std::hash<uint64_t>{}((a << 32) ^ (b + 0x9e3779b97f4a7c15ULL + (a << 6) + (a >> 2)));
        }
    };

    struct reply_payload_fingerprint {
        std::string result_hash;
        bool has_full_result = false;

        reply_payload_fingerprint() = default;
        reply_payload_fingerprint(std::string result_hash_in, bool has_full_result_in)
            : result_hash(std::move(result_hash_in))
            , has_full_result(has_full_result_in)
            {}

        bool operator==(const reply_payload_fingerprint& other) const {
            return result_hash == other.result_hash &&
                   has_full_result == other.has_full_result;
        }
    };

    std::unordered_map<std::string, uint64_t> hash_to_nodes_valid;
    std::unordered_map<int, node_obs> by_node;
    uint64_t nodes_seen_mask = 0;
    int nodes_seen_count = 0;
    int leader_node_id = -1;
    bool divergence_reported = false;
    bool all_reported = false;
    std::string majority_hash;
    std::unordered_map<reply_identity,
                       reply_payload_fingerprint,
                       reply_identity_hash> seen_reply_keys;
    bool terminal_set = false;
    std::string terminal_result;
    std::string terminal_error;
    uint64_t first_reply_ns = 0;
    bool ready_recorded = false;
};

struct vote_store_profile {
    double consume_to_ready_ms_mean = 0.0;
    double consume_to_ready_ms_p95 = 0.0;
    double wait_cv_sleep_ms_mean = 0.0;
    double wait_cv_sleep_ms_p95 = 0.0;
    double ready_queue_depth_mean = 0.0;
    size_t ready_queue_depth_max = 0;
};

struct vote_store {
    explicit vote_store(int total_nodes,
                        int majority,
                        size_t max_entries,
                        std::string sig_key)
        : total_nodes_(total_nodes)
        , majority_(majority)
        , max_entries_(std::max<size_t>(1024, max_entries))
        , sig_key_(std::move(sig_key))
        {}

    void add_reply(const kafka_reply_record& rec,
                   std::string& out_recovery_note)
    {
        out_recovery_note.clear();
        std::lock_guard<std::mutex> lk(mu_);
        add_reply_inner_locked(rec, out_recovery_note);
        cv_.notify_all();
    }

    // Batch add: process all records under a single mutex lock and
    // notify waiters once at the end. This replaces N individual
    // mutex + cv_.notify_all() cycles with a single pair.
    void add_replies_batch(const std::vector<kafka_reply_record>& recs,
                           std::vector<std::string>& out_recovery_notes)
    {
        out_recovery_notes.resize(recs.size());
        std::lock_guard<std::mutex> lk(mu_);
        for (size_t i = 0; i < recs.size(); ++i) {
            add_reply_inner_locked(recs[i], out_recovery_notes[i]);
        }
        if (!recs.empty()) {
            cv_.notify_all();
        }
    }

private:
    void add_reply_inner_locked(const kafka_reply_record& rec,
                                std::string& out_recovery_note)
    {
        out_recovery_note.clear();
        const uint64_t add_ns = steady_now_ns();
        vote_entry& e = m_[rec.req_num];
        if (seen_reqs_.insert(rec.req_num).second) {
            req_order_.push_back(rec.req_num);
        }
        evict_if_needed_locked();
        if (e.first_reply_ns == 0) {
            e.first_reply_ns = add_ns;
        }

        note_node_seen_locked(e, rec.node_id);
        if (rec.leader_node_id > 0) {
            e.leader_node_id = rec.leader_node_id;
        }

        const vote_entry::reply_identity reply_identity{rec.node_id, rec.raft_log_idx};
        const vote_entry::reply_payload_fingerprint reply_fingerprint{
            rec.result_hash,
            rec.has_full_result
        };
        auto it_seen = e.seen_reply_keys.find(reply_identity);
        if (it_seen != e.seen_reply_keys.end()) {
            if (it_seen->second == reply_fingerprint) {
                return;
            }
            e.terminal_set = true;
            e.terminal_result.clear();
            e.terminal_error = "duplicate_identity_conflict";
            enqueue_ready_locked(rec.req_num);
            std::ostringstream oss;
            const std::string req_id = rec.req_id.empty() ? first_req_id_locked(e) : rec.req_id;
            oss << "{\"type\":\"duplicate_identity_conflict\""
                << ",\"req_num\":" << rec.req_num
                << ",\"request_id\":\"" << json_escape(req_id) << "\""
                << ",\"node_id\":" << rec.node_id
                << ",\"raft_log_idx\":" << rec.raft_log_idx
                << "}";
            out_recovery_note = oss.str();
            return;
        }
        e.seen_reply_keys[reply_identity] = reply_fingerprint;

        vote_entry::node_obs obs;
        obs.rec = rec;
        obs.sig_valid = verify_result_signature(rec, sig_key_);
        debug_trace_kafka(rec, obs.sig_valid);

        auto prev_it = e.by_node.find(rec.node_id);
        if (prev_it != e.by_node.end()) {
            const vote_entry::node_obs& prev_obs = prev_it->second;
            if (prev_obs.sig_valid && !prev_obs.rec.result_hash.empty()) {
                auto it_hash = e.hash_to_nodes_valid.find(prev_obs.rec.result_hash);
                if (it_hash != e.hash_to_nodes_valid.end()) {
                    it_hash->second &= ~node_bit(rec.node_id);
                    if (it_hash->second == 0) {
                        e.hash_to_nodes_valid.erase(it_hash);
                    }
                }
            }
        }
        e.by_node[rec.node_id] = obs;

        if (obs.sig_valid && !rec.result_hash.empty()) {
            e.hash_to_nodes_valid[rec.result_hash] |= node_bit(rec.node_id);
        }
        refresh_majority_locked(rec.req_num, e, add_ns);

        if (!e.all_reported && e.nodes_seen_count >= total_nodes_) {
            e.all_reported = true;
            if (!e.divergence_reported &&
                (e.hash_to_nodes_valid.size() > 1 || has_invalid_sig_locked(e))) {
                e.divergence_reported = true;
                std::ostringstream oss;
                const std::string req_id = first_req_id_locked(e);
                oss << "{\"type\":\"result_divergence\""
                    << ",\"req_num\":" << rec.req_num
                    << ",\"request_id\":\"" << json_escape(req_id) << "\""
                    << ",\"leader_node\":" << e.leader_node_id
                    << ",\"invalid_sig_nodes\":\"" << json_escape(invalid_sig_nodes_csv_locked(e)) << "\""
                    << ",\"hash_votes\":\"";
                bool first = true;
                for (const auto& kv : e.hash_to_nodes_valid) {
                    if (!first) oss << ";";
                    first = false;
                    oss << kv.first << "=" << popcount_mask(kv.second);
                }
                oss << "\""
                    << ",\"node_results\":\"";
                bool first_node = true;
                for (const auto& kv : e.by_node) {
                    if (!first_node) oss << ";";
                    first_node = false;
                    std::string sample = kv.second.rec.full_result;
                    if (sample.size() > 96) sample.resize(96);
                    oss << kv.first
                        << "|sig=" << (kv.second.sig_valid ? 1 : 0)
                        << "|hash=" << kv.second.rec.result_hash
                        << "|has_full=" << (kv.second.rec.has_full_result ? 1 : 0)
                        << "|full=" << json_escape(sample);
                }
                oss << "\"}";
                out_recovery_note = oss.str();
            }
            if (e.majority_hash.empty() || has_invalid_sig_locked(e)) {
                enqueue_ready_locked(rec.req_num);
            }
        }
    }

public:

    bool wait_majority(uint64_t req_num,
                       int poll_interval_us,
                       int poll_count,
                       std::string& out_result,
                       std::string& out_error)
    {
        std::unique_lock<std::mutex> lk(mu_);
        out_result.clear();
        out_error.clear();

        auto terminal = [&]() -> bool {
            return resolve_terminal_locked(req_num, out_result, out_error);
        };

        if (terminal()) return out_error.empty();
        if (poll_count <= 0) {
            while (!terminal()) {
                const auto wait_t0 = std::chrono::steady_clock::now();
                cv_.wait(lk);
                const auto wait_t1 = std::chrono::steady_clock::now();
                record_wait_sleep_locked(static_cast<uint64_t>(
                    std::chrono::duration_cast<std::chrono::nanoseconds>(wait_t1 - wait_t0).count()));
            }
            return out_error.empty();
        }

        const long long total_us =
            static_cast<long long>(poll_interval_us) * static_cast<long long>(poll_count);
        const auto deadline = std::chrono::steady_clock::now() +
            std::chrono::microseconds(std::max<long long>(1, total_us));
        while (!terminal()) {
            const auto wait_t0 = std::chrono::steady_clock::now();
            const std::cv_status st = cv_.wait_until(lk, deadline);
            const auto wait_t1 = std::chrono::steady_clock::now();
            record_wait_sleep_locked(static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(wait_t1 - wait_t0).count()));
            if (st == std::cv_status::timeout && !terminal()) {
                std::cerr << "vote_store timeout req=" << req_num
                          << " detail=" << describe_req_locked(req_num)
                          << std::endl;
                out_error = "majority_timeout";
                return false;
            }
        }
        return out_error.empty();
    }

    // Wait until any req_id in `inflight` reaches majority, remove it from
    // `inflight`, and return its majority result.
    bool wait_any_majority(std::deque<uint64_t>& inflight,
                           int poll_interval_us,
                           int poll_count,
                           uint64_t& out_req_num,
                           std::string& out_result,
                           std::string& out_error)
    {
        out_req_num = 0;
        out_result.clear();
        out_error.clear();
        std::unique_lock<std::mutex> lk(mu_);
        // Fast path only: the producer (apply_record / refresh_majority_locked /
        // duplicate_identity_conflict) pushes req_num onto ready_reqs_ whenever
        // the entry may have transitioned to terminal. Consumer just drains that
        // queue — no O(N) scan of `inflight` per wakeup.
        auto terminal = [&]() -> bool {
            return pop_terminal_inflight_locked(inflight, out_req_num, out_result, out_error);
        };

        if (terminal()) return out_error.empty();
        if (poll_count <= 0) {
            while (!terminal()) {
                const auto wait_t0 = std::chrono::steady_clock::now();
                cv_.wait(lk);
                const auto wait_t1 = std::chrono::steady_clock::now();
                record_wait_sleep_locked(static_cast<uint64_t>(
                    std::chrono::duration_cast<std::chrono::nanoseconds>(wait_t1 - wait_t0).count()));
            }
            return out_error.empty();
        }

        const long long total_us =
            static_cast<long long>(poll_interval_us) * static_cast<long long>(poll_count);
        const auto deadline = std::chrono::steady_clock::now() +
            std::chrono::microseconds(std::max<long long>(1, total_us));
        while (!terminal()) {
            const auto wait_t0 = std::chrono::steady_clock::now();
            const std::cv_status st = cv_.wait_until(lk, deadline);
            const auto wait_t1 = std::chrono::steady_clock::now();
            record_wait_sleep_locked(static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(wait_t1 - wait_t0).count()));
            if (st == std::cv_status::timeout && !terminal()) {
                std::cerr << "vote_store timeout inflight="
                          << describe_inflight_locked(inflight)
                          << std::endl;
                out_error = "majority_timeout";
                return false;
            }
        }
        return out_error.empty();
    }

    // Wait until all nodes have reported this request and all valid signatures
    // agree on a single result hash across the full replica set.
    bool wait_all_nodes_consistent(uint64_t req_num,
                                   int poll_interval_us,
                                   int poll_count,
                                   std::string& out_error)
    {
        out_error.clear();
        std::unique_lock<std::mutex> lk(mu_);

        auto all_nodes_ready = [&]() -> bool {
            return resolve_all_nodes_consistent_locked(req_num, out_error);
        };

        if (all_nodes_ready()) return out_error.empty();
        if (poll_count <= 0) {
            while (!all_nodes_ready()) {
                const auto wait_t0 = std::chrono::steady_clock::now();
                cv_.wait(lk);
                const auto wait_t1 = std::chrono::steady_clock::now();
                record_wait_sleep_locked(static_cast<uint64_t>(
                    std::chrono::duration_cast<std::chrono::nanoseconds>(wait_t1 - wait_t0).count()));
            }
            return out_error.empty();
        }

        const long long total_us =
            static_cast<long long>(poll_interval_us) * static_cast<long long>(poll_count);
        const auto deadline = std::chrono::steady_clock::now() +
            std::chrono::microseconds(std::max<long long>(1, total_us));
        while (!all_nodes_ready()) {
            const auto wait_t0 = std::chrono::steady_clock::now();
            const std::cv_status st = cv_.wait_until(lk, deadline);
            const auto wait_t1 = std::chrono::steady_clock::now();
            record_wait_sleep_locked(static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(wait_t1 - wait_t0).count()));
            if (st == std::cv_status::timeout && !all_nodes_ready()) {
                std::cerr << "vote_store all-nodes timeout req=" << req_num
                          << " detail=" << describe_req_locked(req_num)
                          << std::endl;
                out_error = "all_nodes_timeout";
                return false;
            }
        }
        return out_error.empty();
    }

    vote_store_profile profile() {
        std::lock_guard<std::mutex> lk(mu_);
        vote_store_profile out;
        out.consume_to_ready_ms_mean = mean_ms_locked(consume_to_ready_samples_, consume_to_ready_sum_ns_);
        out.consume_to_ready_ms_p95 = p95_ms_locked(consume_to_ready_samples_);
        out.wait_cv_sleep_ms_mean = mean_ms_locked(wait_cv_sleep_samples_, wait_cv_sleep_sum_ns_);
        out.wait_cv_sleep_ms_p95 = p95_ms_locked(wait_cv_sleep_samples_);
        if (ready_queue_depth_obs_ > 0) {
            out.ready_queue_depth_mean =
                static_cast<double>(ready_queue_depth_sum_) / static_cast<double>(ready_queue_depth_obs_);
        }
        out.ready_queue_depth_max = ready_queue_depth_max_;
        return out;
    }

    bool try_pop_any_terminal(std::deque<uint64_t>& inflight,
                              uint64_t& out_req_num,
                              std::string& out_result,
                              std::string& out_error)
    {
        out_req_num = 0;
        out_result.clear();
        out_error.clear();
        std::lock_guard<std::mutex> lk(mu_);
        return pop_terminal_inflight_locked(inflight, out_req_num, out_result, out_error);
    }

private:
    static uint64_t node_bit(int node_id) {
        if (node_id <= 0 || node_id > 64) return 0;
        return 1ULL << static_cast<unsigned>(node_id - 1);
    }

    static int popcount_mask(uint64_t mask) {
#if defined(__GNUC__) || defined(__clang__)
        return __builtin_popcountll(mask);
#else
        int count = 0;
        while (mask != 0) {
            mask &= (mask - 1);
            ++count;
        }
        return count;
#endif
    }

    void note_node_seen_locked(vote_entry& e, int node_id) const {
        const uint64_t bit = node_bit(node_id);
        if (bit == 0) return;
        if ((e.nodes_seen_mask & bit) == 0) {
            e.nodes_seen_mask |= bit;
            ++e.nodes_seen_count;
        }
    }

    bool has_invalid_sig_locked(const vote_entry& e) const {
        for (const auto& kv : e.by_node) {
            if (!kv.second.sig_valid) return true;
        }
        return false;
    }

    std::string first_req_id_locked(const vote_entry& e) const {
        for (const auto& kv : e.by_node) {
            if (!kv.second.rec.req_id.empty()) return kv.second.rec.req_id;
        }
        return "";
    }

    std::string invalid_sig_nodes_csv_locked(const vote_entry& e) const {
        std::ostringstream oss;
        bool first = true;
        for (const auto& kv : e.by_node) {
            if (kv.second.sig_valid) continue;
            if (!first) oss << ",";
            first = false;
            oss << kv.first;
        }
        return oss.str();
    }

    std::string compute_majority_hash_locked(const vote_entry& e) const {
        std::string best_hash;
        int best_votes = 0;
        for (const auto& kv : e.hash_to_nodes_valid) {
            const int votes = popcount_mask(kv.second);
            if (votes < majority_) continue;
            if (votes > best_votes || (votes == best_votes && (best_hash.empty() || kv.first < best_hash))) {
                best_votes = votes;
                best_hash = kv.first;
            }
        }
        return best_hash;
    }

    void record_consume_to_ready_locked(uint64_t delta_ns) {
        consume_to_ready_sum_ns_ += delta_ns;
        consume_to_ready_samples_.push_back(delta_ns);
    }

    void record_wait_sleep_locked(uint64_t delta_ns) {
        wait_cv_sleep_sum_ns_ += delta_ns;
        wait_cv_sleep_samples_.push_back(delta_ns);
    }

    void enqueue_ready_locked(uint64_t req_num) {
        if (ready_seen_.insert(req_num).second) {
            ready_queue_depth_sum_ += static_cast<uint64_t>(ready_seen_.size());
            ++ready_queue_depth_obs_;
            ready_queue_depth_max_ = std::max<size_t>(ready_queue_depth_max_, ready_seen_.size());
        }
    }

    static double mean_ms_locked(const std::vector<uint64_t>& samples, uint64_t sum_ns) {
        if (samples.empty()) return 0.0;
        return (static_cast<double>(sum_ns) / static_cast<double>(samples.size())) / 1000000.0;
    }

    static double p95_ms_locked(std::vector<uint64_t> samples) {
        if (samples.empty()) return 0.0;
        const size_t idx = static_cast<size_t>(
            std::ceil(0.95 * static_cast<double>(samples.size()))) - 1;
        std::nth_element(samples.begin(),
                         samples.begin() + std::min(idx, samples.size() - 1),
                         samples.end());
        return static_cast<double>(samples[std::min(idx, samples.size() - 1)]) / 1000000.0;
    }

    void refresh_majority_locked(uint64_t req_num, vote_entry& e, uint64_t now_ns) {
        const bool had_majority = !e.majority_hash.empty();
        e.majority_hash = compute_majority_hash_locked(e);
        if (!had_majority && !e.terminal_set && !e.majority_hash.empty() && !e.ready_recorded) {
            e.ready_recorded = true;
            if (e.first_reply_ns > 0 && now_ns >= e.first_reply_ns) {
                record_consume_to_ready_locked(now_ns - e.first_reply_ns);
            }
        }
        if (!e.terminal_set && !e.majority_hash.empty()) {
            enqueue_ready_locked(req_num);
        }
    }

    void evict_if_needed_locked() {
        while (m_.size() > max_entries_ && !req_order_.empty()) {
            const uint64_t old_req = req_order_.front();
            req_order_.pop_front();
            seen_reqs_.erase(old_req);
            m_.erase(old_req);
            ready_seen_.erase(old_req);
        }
    }

    bool resolve_terminal_locked(uint64_t req_num,
                                 std::string& out_result,
                                 std::string& out_error)
    {
        out_result.clear();
        out_error.clear();
        auto it = m_.find(req_num);
        if (it == m_.end()) return false;
        vote_entry& e = it->second;
        if (e.terminal_set) {
            out_result = e.terminal_result;
            out_error = e.terminal_error;
            return true;
        }
        // Majority success rule:
        // 1) majority (>= quorum) of *valid signatures* agrees on result_hash
        // 2) return a canonical full_result whose hash matches that majority
        //
        // NOTE:
        // We intentionally do not require the Raft leader's full_result here.
        // Under strict byzantine semantics, if the leader is the minority, the
        // gateway should still be able to return a correct full result from the
        // majority quorum. (The recovery/errTopic pipeline can still flag the
        // divergent node.)
        e.majority_hash = compute_majority_hash_locked(e);
        if (e.majority_hash.empty()) {
            if (e.all_reported) {
                out_error = "no_majority";
                e.terminal_set = true;
                e.terminal_error = out_error;
                return true;
            }
            return false;
        }

        auto it_maj = e.hash_to_nodes_valid.find(e.majority_hash);
        if (it_maj == e.hash_to_nodes_valid.end() ||
            popcount_mask(it_maj->second) < majority_) {
            return false;
        }

        for (const auto& kv : e.by_node) {
            const int node_id = kv.first;
            if ((it_maj->second & node_bit(node_id)) == 0) continue;
            const vote_entry::node_obs& obs = kv.second;
            if (!obs.sig_valid) continue;
            if (obs.rec.result_hash != e.majority_hash) continue;
            if (!obs.rec.has_full_result) continue;
            const std::string full_hash = canonical_result_hash(obs.rec.full_result);
            if (full_hash != e.majority_hash) continue;
            out_result = obs.rec.full_result;
            e.terminal_set = true;
            e.terminal_result = out_result;
            e.terminal_error.clear();
            return true;
        }

        // Majority exists but we don't yet have a full result for it.
        if (!e.all_reported) return false;
        out_error = "majority_full_result_missing";
        e.terminal_set = true;
        e.terminal_error = out_error;
        return true;
    }

    bool resolve_all_nodes_consistent_locked(uint64_t req_num,
                                             std::string& out_error) const
    {
        out_error.clear();
        auto it = m_.find(req_num);
        if (it == m_.end()) return false;
        const vote_entry& e = it->second;

        if (static_cast<int>(e.by_node.size()) < total_nodes_) {
            return false;
        }
        if (has_invalid_sig_locked(e)) {
            out_error = "all_nodes_signature_invalid";
            return true;
        }

        bool unanimous_hash = false;
        for (const auto& kv : e.hash_to_nodes_valid) {
            if (popcount_mask(kv.second) == total_nodes_) {
                unanimous_hash = true;
                break;
            }
        }
        if (!unanimous_hash) {
            out_error = "all_nodes_hash_mismatch";
        }
        return true;
    }

    bool pop_terminal_inflight_locked(std::deque<uint64_t>& inflight,
                                      uint64_t& out_req_num,
                                      std::string& out_result,
                                      std::string& out_error)
    {
        for (auto it_req = inflight.begin(); it_req != inflight.end(); ++it_req) {
            const uint64_t req_num = *it_req;
            if (ready_seen_.find(req_num) != ready_seen_.end()) {
                std::string err;
                std::string result;
                if (resolve_terminal_locked(req_num, result, err)) {
                    out_req_num = req_num;
                    out_result = std::move(result);
                    out_error = std::move(err);
                    inflight.erase(it_req);
                    ready_seen_.erase(req_num);
                    return true;
                }
            }
        }

        // A Kafka reply can occasionally win the race against the submitter
        // adding the accepted request to `inflight`.  The ready queue entry is
        // then consumed above before it can match the deque.  Scan the bounded
        // deterministic window as a correctness fallback so terminal entries
        // are not stranded until the global poll timeout.
        for (auto it_req = inflight.begin(); it_req != inflight.end(); ++it_req) {
            std::string err;
            std::string result;
            if (!resolve_terminal_locked(*it_req, result, err)) {
                continue;
            }

            out_req_num = *it_req;
            out_result = std::move(result);
            out_error = std::move(err);
            inflight.erase(it_req);
            ready_seen_.erase(out_req_num);
            return true;
        }
        return false;
    }

    std::string describe_req_locked(uint64_t req_num) const
    {
        std::ostringstream oss;
        auto it = m_.find(req_num);
        if (it == m_.end()) {
            oss << "missing(req=" << req_num << ")";
            return oss.str();
        }

        const vote_entry& e = it->second;
        oss << "req=" << req_num
            << ",nodes_seen=" << e.nodes_seen_count
            << ",by_node=" << e.by_node.size()
            << ",leader=" << e.leader_node_id
            << ",majority_hash=" << (e.majority_hash.empty() ? "<none>" : e.majority_hash)
            << ",terminal=" << (e.terminal_set ? 1 : 0)
            << ",all_reported=" << (e.all_reported ? 1 : 0)
            << ",hash_votes=";
        bool first_hash = true;
        for (const auto& kv : e.hash_to_nodes_valid) {
            if (!first_hash) oss << ";";
            first_hash = false;
            oss << kv.first << "=" << popcount_mask(kv.second);
        }
        if (first_hash) oss << "<none>";
        oss << ",node_obs=";
        bool first_obs = true;
        for (const auto& kv : e.by_node) {
            if (!first_obs) oss << ";";
            first_obs = false;
            const vote_entry::node_obs& obs = kv.second;
            oss << kv.first
                << "[sig=" << (obs.sig_valid ? 1 : 0)
                << ",hash=" << (obs.rec.result_hash.empty() ? "<none>" : obs.rec.result_hash)
                << ",full=" << (obs.rec.has_full_result ? 1 : 0)
                << ",raft_log_idx=" << obs.rec.raft_log_idx
                << "]";
        }
        if (first_obs) oss << "<none>";
        return oss.str();
    }

    std::string describe_inflight_locked(const std::deque<uint64_t>& inflight) const
    {
        std::ostringstream oss;
        bool first = true;
        for (uint64_t req_num : inflight) {
            if (!first) oss << " || ";
            first = false;
            oss << describe_req_locked(req_num);
        }
        if (first) oss << "<empty>";
        return oss.str();
    }

    int total_nodes_;
    int majority_;
    size_t max_entries_;
    std::string sig_key_;
    std::mutex mu_;
    std::condition_variable cv_;
    std::unordered_map<uint64_t, vote_entry> m_;
    std::deque<uint64_t> req_order_;
    std::unordered_set<uint64_t> seen_reqs_;
    std::unordered_set<uint64_t> ready_seen_;
    uint64_t consume_to_ready_sum_ns_ = 0;
    std::vector<uint64_t> consume_to_ready_samples_;
    uint64_t wait_cv_sleep_sum_ns_ = 0;
    std::vector<uint64_t> wait_cv_sleep_samples_;
    uint64_t ready_queue_depth_sum_ = 0;
    uint64_t ready_queue_depth_obs_ = 0;
    size_t ready_queue_depth_max_ = 0;
};

struct cluster_client {
    int fd = -1;
    size_t node_idx = 0;
    bool leader_known = false;
    size_t leader_idx = 0;

    void close_fd() {
        if (fd >= 0) {
            ::close(fd);
            fd = -1;
        }
    }

    ~cluster_client() { close_fd(); }
};

struct submit_profile_stats {
    std::atomic<uint64_t> attempts{0};
    std::atomic<uint64_t> connect_calls{0};
    std::atomic<uint64_t> connect_ns{0};
    std::atomic<uint64_t> write_calls{0};
    std::atomic<uint64_t> write_ns{0};
    std::atomic<uint64_t> read_calls{0};
    std::atomic<uint64_t> read_ns{0};
    std::atomic<uint64_t> not_accepted{0};
};

submit_profile_stats g_submit_prof;
std::atomic<int> g_event_submit_leader_idx(-1);

static bool parse_leader_hint_from_msg(const std::string& msg, int& out_leader_id) {
    out_leader_id = -1;
    const std::string key = "leader=";
    const size_t pos = msg.find(key);
    if (pos == std::string::npos) return false;
    size_t i = pos + key.size();
    bool neg = false;
    if (i < msg.size() && msg[i] == '-') {
        neg = true;
        ++i;
    }
    if (i >= msg.size()) return false;
    if (!std::isdigit(static_cast<unsigned char>(msg[i]))) return false;
    long v = 0;
    while (i < msg.size() && std::isdigit(static_cast<unsigned char>(msg[i]))) {
        v = v * 10 + (msg[i] - '0');
        ++i;
        if (v > 1000000) break;
    }
    out_leader_id = neg ? -static_cast<int>(v) : static_cast<int>(v);
    return true;
}

bool submit_to_cluster(const std::vector<host_port>& nodes,
                       std::atomic<size_t>& rr_idx,
                       const client_api_request& req,
                       std::string& err,
                       client_api_response* out_resp = nullptr,
                       size_t* out_node_idx = nullptr)
{
    // Reuse one TCP connection per gateway worker thread to avoid:
    // - 20k connect()/close() calls per run.
    // - one server-side std::thread spawn per request.
    static thread_local cluster_client cli;

    const size_t n = nodes.size();
    if (n == 0) {
        err = "no nodes";
        return false;
    }

    size_t start = 0;
    if (cli.leader_known && cli.leader_idx < n) {
        start = cli.leader_idx;
    } else if (cli.fd >= 0 && cli.node_idx < n) {
        start = cli.node_idx;
    } else {
        cli.close_fd();
        start = rr_idx.fetch_add(1) % n;
    }
    for (size_t attempt = 0; attempt < n; ++attempt) {
        g_submit_prof.attempts.fetch_add(1, std::memory_order_relaxed);
        const size_t idx = (start + attempt) % n;
        const host_port& hp = nodes[idx];

        if (cli.fd < 0 || cli.node_idx != idx) {
            cli.close_fd();
            g_submit_prof.connect_calls.fetch_add(1, std::memory_order_relaxed);
            const auto c0 = std::chrono::steady_clock::now();
            int fd = connect_tcp(hp.host, hp.port, err);
            const auto c1 = std::chrono::steady_clock::now();
            g_submit_prof.connect_ns.fetch_add(
                static_cast<uint64_t>(
                    std::chrono::duration_cast<std::chrono::nanoseconds>(c1 - c0).count()),
                std::memory_order_relaxed);
            if (fd < 0) continue;
            cli.fd = fd;
            cli.node_idx = idx;
        }

        std::string werr;
        g_submit_prof.write_calls.fetch_add(1, std::memory_order_relaxed);
        const auto w0 = std::chrono::steady_clock::now();
        const bool ok_write = write_request_frame(cli.fd, req, werr);
        const auto w1 = std::chrono::steady_clock::now();
        g_submit_prof.write_ns.fetch_add(
            static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(w1 - w0).count()),
            std::memory_order_relaxed);
        if (!ok_write) {
            cli.close_fd();
            err = werr;
            continue;
        }

        client_api_response resp;
        g_submit_prof.read_calls.fetch_add(1, std::memory_order_relaxed);
        const auto r0 = std::chrono::steady_clock::now();
        const bool ok_read = read_response_frame(cli.fd, resp, werr);
        const auto r1 = std::chrono::steady_clock::now();
        g_submit_prof.read_ns.fetch_add(
            static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(r1 - r0).count()),
            std::memory_order_relaxed);
        if (!ok_read) {
            cli.close_fd();
            err = werr;
            continue;
        }

        if (resp.status == 0) {
            // Sticky leader hint: most requests should go to the leader.
            cli.leader_known = true;
            cli.leader_idx = idx;
            if (out_resp) *out_resp = resp;
            if (out_node_idx) *out_node_idx = idx;
            return true;
        }
        if (resp.status == 1) {
            // Not accepted: follower redirect, Raft transient, or admission-control backpressure.
            g_submit_prof.not_accepted.fetch_add(1, std::memory_order_relaxed);
            err = resp.msg;

            int leader_id = -1;
            if (parse_leader_hint_from_msg(resp.msg, leader_id) && leader_id > 0 &&
                static_cast<size_t>(leader_id) <= n) {
                cli.leader_known = true;
                cli.leader_idx = static_cast<size_t>(leader_id - 1);
            }

            const bool is_busy = resp.msg.rfind("NOT_ACCEPTED_BUSY", 0) == 0;
            if (is_busy) {
                // If the current leader is overloaded, probing other nodes only adds load and
                // often creates a submit+connect storm. Treat as backpressure and let the
                // caller retry with bounded backoff, keeping the leader connection open.
                if (cli.leader_known && idx == cli.leader_idx) {
                    return false;
                }
            }

            // Switch away from this node for subsequent attempts.
            cli.close_fd();
            continue;
        }
        err = resp.msg;
        cli.close_fd();
        return false;
    }
    return false;
}

// Broadcast a request to ALL nodes in order, waiting for each to accept.
// Used in kafka-only-no-raft mode: the gateway itself provides total ordering
// by sending each transaction to all replicas sequentially before moving on.
// Correctness: all nodes receive transactions in identical sequence => determinism.
bool submit_to_all_nodes(const std::vector<host_port>& nodes,
                         const client_api_request& req,
                         std::string& err)
{
    // Thread-local persistent connections: one fd per node index.
    static thread_local std::vector<int> node_fds;
    if (node_fds.size() < nodes.size()) {
        node_fds.resize(nodes.size(), -1);
    }

    for (size_t i = 0; i < nodes.size(); ++i) {
        const host_port& hp = nodes[i];

        // Reconnect if needed.
        if (node_fds[i] < 0) {
            std::string cerr;
            node_fds[i] = connect_tcp(hp.host, hp.port, cerr);
            if (node_fds[i] < 0) {
                err = "broadcast connect to node " + std::to_string(i) + " (" +
                      hp.host + ":" + std::to_string(hp.port) + ") failed: " + cerr;
                return false;
            }
        }

        std::string werr;
        if (!write_request_frame(node_fds[i], req, werr)) {
            ::close(node_fds[i]);
            node_fds[i] = -1;
            err = "broadcast write to node " + std::to_string(i) + " failed: " + werr;
            return false;
        }

        client_api_response resp;
        if (!read_response_frame(node_fds[i], resp, werr)) {
            ::close(node_fds[i]);
            node_fds[i] = -1;
            err = "broadcast read from node " + std::to_string(i) + " failed: " + werr;
            return false;
        }

        if (resp.status != 0) {
            err = "broadcast: node " + std::to_string(i) + " rejected: " + resp.msg;
            return false;
        }
    }
    return true;
}

// Parallel broadcast: issue submits to all N nodes concurrently via the async
// submitter, then wait for every ACK. Latency becomes max(RTT_i) instead of
// sum(RTT_i). Preserves "all nodes accepted" semantics — any single-node
// rejection fails the whole broadcast, matching the sequential variant.
bool submit_to_all_nodes_parallel(async_cluster_submitter& submitter,
                                  const std::vector<host_port>& nodes,
                                  const client_api_request& req,
                                  std::string& err)
{
    err.clear();
    const size_t n = nodes.size();
    if (n == 0) { err = "no nodes"; return false; }

    std::vector<std::shared_ptr<async_cluster_submitter::submit_ctx>> ctxs;
    ctxs.reserve(n);
    for (size_t i = 0; i < n; ++i) {
        std::shared_ptr<async_cluster_submitter::submit_ctx> ctx;
        std::string serr;
        if (!submitter.submit_async_to_node(i, req, ctx, serr)) {
            err = "broadcast async-submit to node " + std::to_string(i) + " failed: " + serr;
            // Drain any already-issued contexts so we don't leak reply data.
            for (auto& c : ctxs) {
                client_api_response resp;
                std::string werr;
                (void)submitter.wait_submit(c, resp, werr);
            }
            return false;
        }
        ctxs.push_back(std::move(ctx));
    }

    bool ok = true;
    for (size_t i = 0; i < ctxs.size(); ++i) {
        client_api_response resp;
        std::string werr;
        if (!submitter.wait_submit(ctxs[i], resp, werr)) {
            if (ok) {
                err = "broadcast wait from node " + std::to_string(i) + " failed: " + werr;
                ok = false;
            }
            continue;
        }
        if (resp.status != 0) {
            if (ok) {
                err = "broadcast: node " + std::to_string(i) + " rejected: " + resp.msg;
                ok = false;
            }
            continue;
        }
    }
    return ok;
}

bool submit_to_cluster_event(async_cluster_submitter& submitter,
                             const std::vector<host_port>& nodes,
                             std::atomic<size_t>& rr_idx,
                             const client_api_request& req,
                             std::string& err,
                             client_api_response* out_resp = nullptr,
                             size_t* out_node_idx = nullptr)
{
    const size_t n = nodes.size();
    if (n == 0) {
        err = "no nodes";
        return false;
    }

    size_t start = 0;
    const int li = g_event_submit_leader_idx.load(std::memory_order_relaxed);
    if (li >= 0 && static_cast<size_t>(li) < n) {
        start = static_cast<size_t>(li);
    } else {
        start = rr_idx.fetch_add(1) % n;
    }

    for (size_t attempt = 0; attempt < n; ++attempt) {
        const size_t idx = (start + attempt) % n;

        client_api_response resp;
        std::string io_err;
        if (!submitter.submit_to_node(idx, req, resp, io_err)) {
            err = io_err.empty() ? std::string("submit_io_failed") : io_err;
            continue;
        }

        if (resp.status == 0) {
            g_event_submit_leader_idx.store(static_cast<int>(idx), std::memory_order_relaxed);
            if (out_resp) *out_resp = resp;
            if (out_node_idx) *out_node_idx = idx;
            return true;
        }

        if (resp.status == 1) {
            err = resp.msg;

            int leader_id = -1;
            if (parse_leader_hint_from_msg(resp.msg, leader_id) && leader_id > 0 &&
                static_cast<size_t>(leader_id) <= n) {
                g_event_submit_leader_idx.store(static_cast<int>(leader_id - 1), std::memory_order_relaxed);
            }

            const bool is_busy = resp.msg.rfind("NOT_ACCEPTED_BUSY", 0) == 0;
            if (is_busy) {
                const int cur_li = g_event_submit_leader_idx.load(std::memory_order_relaxed);
                if (cur_li >= 0 && static_cast<size_t>(cur_li) == idx) {
                    // Backpressure from leader: don't probe other nodes.
                    return false;
                }
            }
            continue;
        }

        err = resp.msg;
        return false;
    }
    return false;
}

bool read_fd_line(int fd, std::string& out, std::string& err) {
    out.clear();
    char ch = 0;
    while (true) {
        const ssize_t r = ::read(fd, &ch, 1);
        if (r == 0) {
            err = "EOF";
            return false;
        }
        if (r < 0) {
            if (errno == EINTR) continue;
            err = std::string("read failed: ") + ::strerror(errno);
            return false;
        }
        if (ch == '\n') break;
        out.push_back(ch);
        if (out.size() > (16u * 1024u * 1024u)) {
            err = "line too large";
            return false;
        }
    }
    return true;
}

bool write_fd_all(int fd, const std::string& s, std::string& err) {
    const char* p = s.data();
    size_t left = s.size();
    while (left > 0) {
        const ssize_t w = ::write(fd, p, left);
        if (w < 0) {
            if (errno == EINTR) continue;
            err = std::string("write failed: ") + ::strerror(errno);
            return false;
        }
        p += w;
        left -= static_cast<size_t>(w);
    }
    return true;
}

int listen_tcp(int port) {
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) throw std::runtime_error(std::string("socket failed: ") + ::strerror(errno));
    int on = 1;
    ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));

    sockaddr_in addr;
    ::memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons(static_cast<uint16_t>(port));
    if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
        const std::string msg = std::string("bind failed: ") + ::strerror(errno);
        ::close(fd);
        throw std::runtime_error(msg);
    }
    if (::listen(fd, 128) != 0) {
        const std::string msg = std::string("listen failed: ") + ::strerror(errno);
        ::close(fd);
        throw std::runtime_error(msg);
    }
    return fd;
}

} // namespace ariabc_pg

int main(int argc, char** argv) {
    using ariabc_pg::gateway_options;

    gateway_options opt;
    std::string err;
    if (!ariabc_pg::parse_args(argc, argv, opt, err)) {
        std::cerr << "Argument error: " << err << std::endl;
        ariabc_pg::usage(argv[0]);
        return 1;
    }

    // Ignore SIGPIPE so broken sockets don't kill the process.
    ::signal(SIGPIPE, SIG_IGN);

    // Parse nodes.
    std::vector<ariabc_pg::host_port> nodes;
    try {
        const std::vector<std::string> parts = ariabc_pg::split_csv_trim(opt.nodes_csv);
        for (const auto& p : parts) nodes.push_back(ariabc_pg::parse_host_port(p));
    } catch (const std::exception& e) {
        std::cerr << "nodes parse error: " << e.what() << std::endl;
        return 1;
    }
    if (nodes.empty()) {
        std::cerr << "no nodes given" << std::endl;
        return 1;
    }
    if (opt.direct_completion_quorum > static_cast<int>(nodes.size())) {
        std::cerr << "invalid --directCompletionQuorum ("
                  << opt.direct_completion_quorum
                  << ") exceeds configured nodes (" << nodes.size() << ")"
                  << std::endl;
        return 1;
    }

    const int total_nodes = (opt.total_nodes > 0) ? opt.total_nodes : static_cast<int>(nodes.size());
    const int majority = (total_nodes / 2) + 1;
    std::string result_sig_key = opt.result_sig_key;
    if (result_sig_key.empty()) {
        const char* env_key = ::getenv("ARIABC_RESULT_SIG_KEY");
        if (env_key && *env_key) {
            result_sig_key = env_key;
        } else {
            result_sig_key = ariabc_pg::kDefaultResultSigKey;
        }
    }
    ariabc_pg::vote_store votes(total_nodes, majority, opt.vote_store_max_entries, result_sig_key);

#ifndef SSL_LIBRARY_NOT_FOUND
    ariabc_pg::openssl_keypair keys;
    if (opt.query_sign == 1) {
        if (!ariabc_pg::load_public_key(opt.pub_key_file, keys.pub, err)) {
            std::cerr << "failed to load public key: " << err << std::endl;
            return 1;
        }
    }
    const bool socket_mode = ariabc_pg::is_number(opt.query_from);
    if (socket_mode && !opt.priv_key_file.empty()) {
        if (!ariabc_pg::load_private_key(opt.priv_key_file, keys.priv, err)) {
            std::cerr << "failed to load private key: " << err << std::endl;
            return 1;
        }
    }
#else
    const bool socket_mode = ariabc_pg::is_number(opt.query_from);
    if (opt.query_sign == 1 || (socket_mode && !opt.priv_key_file.empty())) {
        std::cerr << "OpenSSL is not available (built with SSL_LIBRARY_NOT_FOUND=1)" << std::endl;
        return 1;
    }
#endif

    const bool majority_wait_enabled = (opt.wait_majority == 1);
    const bool async_hash_validation = (opt.validation_mode == "async_hash");
    const bool strict_hash_validation = (opt.validation_mode == "strict_majority");

    const std::string submit_mode = ariabc_pg::trim_copy(opt.submit_mode);
    std::unique_ptr<ariabc_pg::async_cluster_submitter> submitter;
    if (submit_mode == "event") {
        submitter.reset(new ariabc_pg::async_cluster_submitter());
        std::string serr;
        const size_t fanout = static_cast<size_t>(opt.conn_fanout > 0 ? opt.conn_fanout : 1);
        if (!submitter->start(nodes, fanout, serr)) {
            std::cerr << "submitMode=event disabled: " << serr << std::endl;
            return 1;
        }
        std::cout << "submitMode=event: shared nonblocking submit reactor enabled (connFanout=" << fanout << ")" << std::endl;
    }

    // Kafka setup (optional).
    ariabc_pg::kafka_console_consumer consumer;
    ariabc_pg::kafka_console_producer err_prod;
    bool kafka_enabled = false;
    if (!opt.kafka_bootstrap.empty()) {
        std::vector<std::string> result_topics{opt.result_topic};
        std::cout << "Kafka result topic: " << opt.result_topic << std::endl;
        std::string kerr;
        if (!consumer.start_latest_multi(opt.kafka_bootstrap, result_topics,
                                         opt.client_id /*group_id*/, kerr)) {
            std::cerr << "Kafka consumer disabled: " << kerr << std::endl;
        } else {
            kafka_enabled = true;
        }
        if (!err_prod.start(opt.kafka_bootstrap,
                            opt.err_topic,
                            ariabc_pg::kafka_producer_profile::control_durable,
                            kerr)) {
            std::cerr << "Kafka errTopic producer disabled: " << kerr << std::endl;
        }
    }
    if (majority_wait_enabled && opt.kafka_bootstrap.empty()) {
        std::cerr << "completion_path=kafka_majority requires --kafkaBootstrap" << std::endl;
        return 1;
    }

    if (socket_mode && !kafka_enabled) {
        std::cerr << "socket mode requires Kafka (set --kafkaBootstrap)" << std::endl;
        return 1;
    }

    std::cout << "completion_path=" << opt.completion_path
              << " validation_mode=" << opt.validation_mode
              << " waitMajority=" << (majority_wait_enabled ? 1 : 0)
              << " broadcastToAll=" << (opt.broadcast_to_all ? 1 : 0)
              << " broadcastAcceptQuorum=" << opt.broadcast_accept_quorum
              << " broadcastResultQuorum=" << opt.broadcast_result_quorum
              << " broadcastDrainInTimedRun=" << opt.broadcast_drain_in_timed_run
              << " directCompletionQuorum=" << opt.direct_completion_quorum
              << std::endl;

    if (!majority_wait_enabled) {
        std::cout << "waitMajority=0: synchronous Kafka majority wait disabled (direct completion path)"
                  << std::endl;
        if (kafka_enabled && async_hash_validation) {
            std::cout << "async_hash validation enabled: consuming follower replies asynchronously"
                      << std::endl;
        }
    }

    std::atomic<bool> stop(false);
    std::thread kafka_thread;
    std::thread dispatch_thread;
    std::mutex dispatch_mu;
    std::condition_variable dispatch_cv;
    std::deque<std::vector<ariabc_pg::kafka_reply_record>> dispatch_queue;
    std::atomic<int> divergence_count(0);
    std::atomic<uint64_t> kafka_messages(0);
    std::atomic<uint64_t> kafka_records(0);
    std::atomic<uint64_t> kafka_parse_failures(0);
    std::atomic<uint64_t> kafka_parse_ns(0);
    std::atomic<uint64_t> kafka_add_reply_ns(0);
    std::atomic<uint64_t> kafka_consume_lag_ns(0);
    std::atomic<uint64_t> kafka_consume_lag_count(0);
    std::atomic<uint64_t> kafka_consume_lag_ns_max(0);
    if (kafka_enabled) {
        dispatch_thread = std::thread([&] {
            while (true) {
                std::vector<ariabc_pg::kafka_reply_record> batch;
                {
                    std::unique_lock<std::mutex> lk(dispatch_mu);
                    dispatch_cv.wait(lk, [&] { return !dispatch_queue.empty() || stop.load(); });
                    if (dispatch_queue.empty() && stop.load()) break;
                    batch = std::move(dispatch_queue.front());
                    dispatch_queue.pop_front();
                }

                std::vector<std::string> recoveries;
                const auto a0 = std::chrono::steady_clock::now();
                votes.add_replies_batch(batch, recoveries);
                const auto a1 = std::chrono::steady_clock::now();
                kafka_add_reply_ns.fetch_add(
                    static_cast<uint64_t>(
                        std::chrono::duration_cast<std::chrono::nanoseconds>(a1 - a0).count()),
                    std::memory_order_relaxed);

                for (size_t i = 0; i < recoveries.size(); ++i) {
                    if (!recoveries[i].empty()) {
                        divergence_count.fetch_add(1);
                        std::cerr << recoveries[i] << std::endl;
                        if (!opt.kafka_bootstrap.empty()) {
                            std::string perr;
                            if (!err_prod.send_line(recoveries[i], perr)) {
                                std::cerr << "errTopic send failed: " << perr << std::endl;
                            }
                        } else {
                            std::cerr << recoveries[i] << std::endl;
                        }
                    }
                }
            }
        });

        kafka_thread = std::thread([&] {
            int poll_timeout_ms = 1;
            while (!stop.load()) {
                std::string kerr;
                std::vector<ariabc_pg::kafka_consumed_message> kafka_batch;
                if (!consumer.poll_batch_messages(kafka_batch, 5000, poll_timeout_ms, kerr)) {
                    if (kerr == "timeout") {
                        poll_timeout_ms = 1;
                        continue;
                    }
                    if (!stop.load()) {
                        std::cerr << "Kafka consumer stopped: " << kerr << std::endl;
                    }
                    break;
                }
                poll_timeout_ms = 1;

                kafka_messages.fetch_add(static_cast<uint64_t>(kafka_batch.size()), std::memory_order_relaxed);

                std::vector<ariabc_pg::kafka_reply_record> all_recs;
                const auto p0 = std::chrono::steady_clock::now();
                for (size_t b = 0; b < kafka_batch.size(); ++b) {
                    std::vector<ariabc_pg::kafka_reply_record> recs;
                    if (ariabc_pg::parse_kafka_payload_records(kafka_batch[b].payload, recs)) {
                        for (auto& r : recs) {
                            all_recs.push_back(std::move(r));
                        }
                    } else {
                        kafka_parse_failures.fetch_add(1, std::memory_order_relaxed);
                    }
                }
                const auto p1 = std::chrono::steady_clock::now();
                kafka_parse_ns.fetch_add(
                    static_cast<uint64_t>(
                        std::chrono::duration_cast<std::chrono::nanoseconds>(p1 - p0).count()),
                    std::memory_order_relaxed);

                if (all_recs.empty()) {
                    continue;
                }

                kafka_records.fetch_add(static_cast<uint64_t>(all_recs.size()), std::memory_order_relaxed);

                // Process lag timing
                for (size_t i = 0; i < all_recs.size(); ++i) {
                    if (all_recs[i].timestamp_ms != 0) {
                        const uint64_t now_ms = ariabc_pg::now_epoch_ms();
                        if (now_ms >= all_recs[i].timestamp_ms) {
                            const uint64_t lag_ns = (now_ms - all_recs[i].timestamp_ms) * 1000000ULL;
                            kafka_consume_lag_ns.fetch_add(lag_ns, std::memory_order_relaxed);
                            kafka_consume_lag_count.fetch_add(1, std::memory_order_relaxed);
                            ariabc_pg::atomic_max_u64(kafka_consume_lag_ns_max, lag_ns);
                        }
                    }
                }

                {
                    std::lock_guard<std::mutex> lk(dispatch_mu);
                    dispatch_queue.push_back(std::move(all_recs));
                }
                dispatch_cv.notify_one();
            }
        });
    }

    std::atomic<size_t> rr_idx(0);
    std::atomic<uint64_t> req_seq(0);
    std::atomic<uint64_t> det_seq(opt.det_start_seq);
    std::atomic<long long> total_wait_ns(0);
    std::atomic<long long> total_submit_ns(0);
    std::atomic<long long> total_majority_wait_ns(0);
    std::atomic<int> duplicate_key_errors(0);
    std::atomic<int> permanent_failures(0);
    std::atomic<uint64_t> term_leader_unknown(0);
    std::atomic<uint64_t> term_leader_reply_missing(0);
    std::atomic<uint64_t> term_leader_full_result_hash_mismatch(0);
    std::atomic<uint64_t> term_leader_signature_invalid(0);
    std::atomic<uint64_t> term_majority_timeout(0);
    std::atomic<uint64_t> term_other_failure(0);
    std::atomic<uint64_t> strict_all_nodes_checks(0);
    std::atomic<uint64_t> strict_all_nodes_failures(0);
    std::atomic<uint64_t> strict_all_nodes_wait_ns(0);

    auto req_id_for_idx = [&](size_t idx) -> std::string {
        const uint64_t id_num = opt.req_id_offset + static_cast<uint64_t>(idx);
        return opt.client_id + "-" + std::to_string(id_num);
    };

    auto req_num_for_idx = [&](size_t idx) -> uint64_t {
        return opt.req_id_offset + static_cast<uint64_t>(idx);
    };

    auto bump_terminal_reason = [&](const std::string& reason) {
        if (reason == "leader_unknown") {
            term_leader_unknown.fetch_add(1, std::memory_order_relaxed);
        } else if (reason == "leader_reply_missing") {
            term_leader_reply_missing.fetch_add(1, std::memory_order_relaxed);
        } else if (reason == "leader_full_result_hash_mismatch") {
            term_leader_full_result_hash_mismatch.fetch_add(1, std::memory_order_relaxed);
        } else if (reason == "leader_signature_invalid") {
            term_leader_signature_invalid.fetch_add(1, std::memory_order_relaxed);
        } else if (reason == "majority_timeout") {
            term_majority_timeout.fetch_add(1, std::memory_order_relaxed);
        } else if (!reason.empty()) {
            term_other_failure.fetch_add(1, std::memory_order_relaxed);
        }
    };

    auto submit_request_quiet = [&](const ariabc_pg::client_api_request& req,
                                    std::string& out_err,
                                    ariabc_pg::client_api_response* out_resp = nullptr,
                                    size_t* out_node_idx = nullptr) -> bool {
        out_err.clear();
        if (out_resp) *out_resp = ariabc_pg::client_api_response();
        if (out_node_idx) *out_node_idx = 0;
        if (opt.broadcast_to_all) {
            // kafka-only-no-raft: broadcast to every node.
            // When the async submitter is available, fan out in parallel
            // (latency = max(RTT_i) instead of sum). Fall back to the
            // thread-local sequential sockets otherwise.
            if (submitter) {
                return ariabc_pg::submit_to_all_nodes_parallel(*submitter, nodes, req, out_err);
            }
            return ariabc_pg::submit_to_all_nodes(nodes, req, out_err);
        }
        if (submitter) {
            return ariabc_pg::submit_to_cluster_event(
                *submitter, nodes, rr_idx, req, out_err, out_resp, out_node_idx);
        }
        return ariabc_pg::submit_to_cluster(
            nodes, rr_idx, req, out_err, out_resp, out_node_idx);
    };

    auto submit_only_quiet = [&](const std::string& req_id,
                                 const std::string& sql_in,
                                 std::string& out_err) -> bool {
        ariabc_pg::client_api_request req;
        req.req_id = req_id;
        req.sql = sql_in;
        return submit_request_quiet(req, out_err);
    };

    auto submit_only = [&](const std::string& req_id, const std::string& sql_in) -> bool {
        std::string sub_err;
        if (!submit_only_quiet(req_id, sql_in, sub_err)) {
            std::cerr << "submit failed: " << sub_err << std::endl;
            return false;
        }
        return true;
    };

    auto submit_request = [&](const ariabc_pg::client_api_request& req,
                              ariabc_pg::client_api_response* out_resp = nullptr,
                              size_t* out_node_idx = nullptr) -> bool {
        std::string sub_err;
        if (!submit_request_quiet(req, sub_err, out_resp, out_node_idx)) {
            std::cerr << "submit failed: " << sub_err << std::endl;
            return false;
        }
        return true;
    };

    auto emit_recovery_event = [&](uint64_t req_num, const std::string& reason) {
        std::ostringstream oss;
        oss << "{\"type\":\"majority_failure\""
            << ",\"req_num\":" << req_num
            << ",\"reason\":\"" << ariabc_pg::json_escape(reason) << "\"}";
        const std::string msg = oss.str();
        if (!opt.kafka_bootstrap.empty()) {
            std::string perr;
            if (!err_prod.send_line(msg, perr)) {
                std::cerr << "errTopic send failed: " << perr << std::endl;
            }
        } else {
            std::cerr << msg << std::endl;
        }
    };

    auto wait_majority = [&](uint64_t req_num, std::string& out_majority) -> bool {
        out_majority.clear();
        if (!majority_wait_enabled) return true;
        if (!kafka_enabled) return true;
        std::string wait_err;
        if (!votes.wait_majority(req_num, opt.poll_interval_us, opt.poll_count, out_majority, wait_err)) {
            if (wait_err.empty()) wait_err = "majority_timeout";
            bump_terminal_reason(wait_err);
            std::cerr << "majority wait failed for req_num=" << req_num << " err=" << wait_err << std::endl;
            emit_recovery_event(req_num, wait_err);
            return false;
        }
        return true;
    };

    auto wait_direct_completion = [&](size_t node_idx,
                                      const ariabc_pg::client_api_response& submit_resp,
                                      const std::string& req_label) -> bool {
        if (majority_wait_enabled) return true;
        if (opt.completion_path != "direct") return true;
        if (node_idx >= nodes.size()) {
            std::cerr << "direct completion wait missing node idx for " << req_label << std::endl;
            permanent_failures.fetch_add(1);
            return false;
        }

        uint64_t raft_log_idx = 0;
        if (!ariabc_pg::parse_named_u64_field(submit_resp.msg, "raft_log_idx=", raft_log_idx) ||
            raft_log_idx == 0) {
            std::cerr << "direct completion wait missing raft_log_idx for " << req_label
                      << " msg=" << submit_resp.msg << std::endl;
            permanent_failures.fetch_add(1);
            return false;
        }

        const std::string wait_cmd =
            "WAIT_RESULT " + std::to_string(raft_log_idx) + " 30000";
        ariabc_pg::client_api_response wait_resp;
        std::string cerr;
        if (!ariabc_pg::send_control_req_to_node(nodes[node_idx], wait_cmd, wait_resp, cerr)) {
            std::cerr << "direct completion wait failed for " << req_label
                      << " log_idx=" << raft_log_idx
                      << " err=" << cerr << std::endl;
            permanent_failures.fetch_add(1);
            return false;
        }
        if (wait_resp.status != 0) {
            std::cerr << "direct completion wait rejected for " << req_label
                      << " log_idx=" << raft_log_idx
                      << " msg=" << wait_resp.msg << std::endl;
            permanent_failures.fetch_add(1);
            return false;
        }
        return true;
    };

    auto wait_direct_completion_quorum =
        [&](size_t submit_node_idx,
            const ariabc_pg::client_api_response& submit_resp,
            const std::string& req_label) -> bool {
            if (majority_wait_enabled) return true;
            if (opt.completion_path != "direct") return true;

            const size_t quorum =
                std::min(nodes.size(),
                         static_cast<size_t>(std::max(1, opt.direct_completion_quorum)));
            if (quorum <= 1) {
                return wait_direct_completion(submit_node_idx, submit_resp, req_label);
            }
            if (submit_node_idx >= nodes.size()) {
                std::cerr << "direct completion quorum wait missing submit node idx for "
                          << req_label << std::endl;
                permanent_failures.fetch_add(1);
                return false;
            }

            std::vector<size_t> wait_nodes;
            wait_nodes.reserve(quorum);
            wait_nodes.push_back(submit_node_idx);
            for (size_t node_idx = 0;
                 node_idx < nodes.size() && wait_nodes.size() < quorum;
                 ++node_idx) {
                if (node_idx == submit_node_idx) continue;
                wait_nodes.push_back(node_idx);
            }

            // Wait on all quorum nodes concurrently so the gateway
            // returns as soon as the fastest `quorum` nodes confirm
            // instead of blocking sequentially on a slow node.
            std::mutex qmu;
            std::condition_variable qcv;
            int ok_count = 0;
            int fail_count = 0;
            std::vector<std::thread> waiters;
            waiters.reserve(wait_nodes.size());

            for (const size_t node_idx : wait_nodes) {
                waiters.emplace_back([&, node_idx]() {
                    bool ok = wait_direct_completion(
                        node_idx,
                        submit_resp,
                        req_label + "/node" + std::to_string(node_idx));
                    std::lock_guard<std::mutex> lk(qmu);
                    if (ok) ++ok_count; else ++fail_count;
                    qcv.notify_one();
                });
            }

            {
                std::unique_lock<std::mutex> lk(qmu);
                qcv.wait(lk, [&] {
                    return ok_count >= static_cast<int>(quorum) ||
                           fail_count > static_cast<int>(wait_nodes.size() - quorum);
                });
            }

            for (auto& t : waiters) { if (t.joinable()) t.join(); }
            return ok_count >= static_cast<int>(quorum);
        };

    std::unordered_set<uint64_t> reset_pending_reqs;
    auto track_reset_req = [&](uint64_t req_num, const std::string& sql_text) {
        if (ariabc_pg::is_reset_barrier_sql(sql_text)) {
            reset_pending_reqs.insert(req_num);
        }
    };
    auto maybe_wait_reset_all_nodes = [&](uint64_t req_num) -> bool {
        auto it = reset_pending_reqs.find(req_num);
        if (it == reset_pending_reqs.end()) return true;
        if (!majority_wait_enabled || !kafka_enabled) {
            reset_pending_reqs.erase(it);
            return true;
        }

        std::string all_err;
        if (!votes.wait_all_nodes_consistent(req_num,
                                             opt.poll_interval_us,
                                             opt.poll_count,
                                             all_err)) {
            if (all_err.empty()) all_err = "all_nodes_timeout";
            bump_terminal_reason(all_err);
            emit_recovery_event(req_num, all_err);
            std::cerr << "reset all-nodes wait failed for req_num=" << req_num
                      << " err=" << all_err << std::endl;
            return false;
        }

        if (!all_err.empty()) {
            bump_terminal_reason(all_err);
            emit_recovery_event(req_num, all_err);
            std::cerr << "reset all-nodes consistency failed for req_num=" << req_num
                      << " err=" << all_err << std::endl;
            return false;
        }

        reset_pending_reqs.erase(it);
        return true;
    };

    auto reset_commit_barrier = [&](const std::string& sql_text) -> bool {
        if (!ariabc_pg::is_reset_barrier_sql(sql_text)) return true;

        uint64_t target_commit_idx = 0;
        bool saw_any = false;
        for (size_t i = 0; i < nodes.size(); ++i) {
            ariabc_pg::client_api_response resp;
            std::string cerr;
            if (!ariabc_pg::send_control_req_to_node(nodes[i], "__ARIABC_CTRL_GET_COMMIT_INDEX", resp, cerr)) {
                std::cerr << "reset barrier get-commit failed node=" << (i + 1)
                          << " err=" << cerr << std::endl;
                continue;
            }
            if (resp.status != 0) {
                std::cerr << "reset barrier get-commit rejected node=" << (i + 1)
                          << " msg=" << resp.msg << std::endl;
                continue;
            }
            uint64_t idx = 0;
            if (!ariabc_pg::parse_u64_str(resp.msg, idx)) {
                std::cerr << "reset barrier get-commit parse failed node=" << (i + 1)
                          << " msg=" << resp.msg << std::endl;
                continue;
            }
            saw_any = true;
            target_commit_idx = std::max<uint64_t>(target_commit_idx, idx);
        }

        if (!saw_any) {
            std::cerr << "reset barrier could not read commit index from any node" << std::endl;
            return false;
        }

        const std::string wait_cmd =
            "__ARIABC_CTRL_WAIT_COMMIT_INDEX " + std::to_string(target_commit_idx) + " 30000";
        for (size_t i = 0; i < nodes.size(); ++i) {
            ariabc_pg::client_api_response resp;
            std::string cerr;
            if (!ariabc_pg::send_control_req_to_node(nodes[i], wait_cmd, resp, cerr)) {
                std::cerr << "reset barrier wait failed node=" << (i + 1)
                          << " target=" << target_commit_idx
                          << " err=" << cerr << std::endl;
                return false;
            }
            if (resp.status != 0) {
                std::cerr << "reset barrier wait rejected node=" << (i + 1)
                          << " target=" << target_commit_idx
                          << " msg=" << resp.msg << std::endl;
                return false;
            }
        }

        std::cout << "reset barrier reached: commit_idx=" << target_commit_idx
                  << " nodes=" << nodes.size() << std::endl;
        return true;
    };

    auto effective_det_window = [&]() -> size_t {
        if (opt.det_window > 0) {
            // Respect an explicit deterministic window. The earlier adaptive
            // clamp (4 * dbConnPoolSize) silently collapsed larger benchmark
            // windows back to tiny values and made the majority-completion path
            // look much worse than the configured run shape.
            return std::max<size_t>(1, static_cast<size_t>(opt.det_window));
        }
        if (opt.db_conn_pool_size <= 0) {
            return 32;
        }
        // Auto mode still scales with the pool size, but with a much wider
        // default pipeline so Kafka-majority wait can amortize poll/wakeup
        // costs instead of stalling behind an 8- or 16-request cap.
        return std::max<size_t>(32, static_cast<size_t>(opt.db_conn_pool_size) * 16);
    };

    auto effective_det_pipeline_depth = [&](size_t terminal_count, size_t window) -> size_t {
        if (opt.det_pipeline_depth > 0) {
            return static_cast<size_t>(opt.det_pipeline_depth);
        }
        // Preserve the old single-lane behavior when the benchmark does not
        // opt into a client-lane pipeline depth: numTerminals=1 gets the full
        // deterministic window, while multi-terminal runs divide it evenly.
        return std::max<size_t>(1, window / std::max<size_t>(1, terminal_count));
    };

    int exit_code = 0;
    if (!socket_mode) {
        // File mode.
        std::ifstream ifs(opt.query_from.c_str());
        if (!ifs) {
            std::cerr << "failed to open query file: " << opt.query_from << std::endl;
            return 1;
        }

        std::vector<std::string> queries;
        std::string line;
        while (std::getline(ifs, line)) {
            if (ariabc_pg::is_skippable_sql_line(line)) continue;
            queries.push_back(line);
        }

        std::cout << "loaded " << queries.size() << " queries" << std::endl;

        const auto t_start = std::chrono::steady_clock::now();
        std::chrono::steady_clock::time_point client_completion_end;
        bool client_completion_end_set = false;
        uint64_t background_accept_drain_ns = 0;
        if (majority_wait_enabled && kafka_enabled) {
            consumer.set_busy_hint(true);
        }

        auto warm_leader_route = [&]() -> bool {
            if (opt.db_type != 1 || opt.det_raw_sql == 1) return true;
            std::chrono::milliseconds backoff(2);
            for (int tries = 0; tries < 20; ++tries) {
                for (size_t i = 0; i < nodes.size(); ++i) {
                    ariabc_pg::client_api_response resp;
                    std::string ctrl_err;
                    if (!ariabc_pg::send_control_req_to_node(
                            nodes[i], "__ARIABC_CTRL_GET_LEADER", resp, ctrl_err)) {
                        continue;
                    }
                    if (resp.status != 0) {
                        continue;
                    }
                    int leader_id = -1;
                    try {
                        leader_id = std::stoi(ariabc_pg::trim_copy(resp.msg));
                    } catch (...) {
                        leader_id = -1;
                    }
                    if (leader_id > 0) {
                        return true;
                    }
                }
                std::this_thread::sleep_for(backoff);
                if (backoff < std::chrono::milliseconds(20)) {
                    backoff *= 2;
                    if (backoff > std::chrono::milliseconds(20)) {
                        backoff = std::chrono::milliseconds(20);
                    }
                }
            }
            std::cerr << "leader warmup failed: no elected leader reported by control plane"
                      << std::endl;
            return false;
        };

        // Deterministic mode (dbType=1) must be appended to the Raft log in the
        // same order as its 8-digit deterministic sequence. Otherwise, the
        // safedb_dt executor can deadlock when a higher seq reaches the head of
        // the FIFO work queue before an earlier seq exists in the queue yet.
        //
        // To guarantee ordering, the gateway still assigns one global sequence
        // in increasing `idx` order.  numTerminals models client lanes on top of
        // that sequencer: request idx is assigned to lane idx % numTerminals,
        // and each lane may have at most detPipelineDepth requests outstanding.
        if (opt.db_type == 1) {
            if (!warm_leader_route()) {
                permanent_failures.fetch_add(1);
                if (majority_wait_enabled && kafka_enabled) {
                    consumer.set_busy_hint(false);
                }
                return 1;
            }
            const size_t window = effective_det_window();
            const size_t det_terminal_count =
                std::max<size_t>(1, static_cast<size_t>(opt.num_terminals));
            const size_t det_lane_pipeline_depth =
                effective_det_pipeline_depth(det_terminal_count, window);
            const size_t det_pipeline_cap =
                std::max<size_t>(
                    1,
                    std::min<size_t>(
                        window,
                        det_terminal_count * det_lane_pipeline_depth));
            std::cout << "det mode: ordered submission (window=" << window
                      << ", configured_det_window=" << std::max(1, opt.det_window > 0 ? opt.det_window : 32)
                      << ", detBatchSize=" << opt.det_batch_size
                      << ", numTerminals=" << det_terminal_count
                      << ", detPipelineDepth=" << det_lane_pipeline_depth
                      << ", detPipelineCap=" << det_pipeline_cap
                      << ", dbConnPoolSize=" << opt.db_conn_pool_size
                      << ", detRawSql=" << opt.det_raw_sql
                      << ")" << std::endl;
            std::deque<uint64_t> inflight;
            std::atomic<bool> det_progress_stop(false);
            std::atomic<size_t> det_sent_count(0);
            std::atomic<size_t> det_accepted_count(0);
            std::atomic<size_t> det_completed_count(0);
            std::atomic<size_t> det_inflight_count(0);
            std::atomic<size_t> det_pending_accept_count(0);
            std::atomic<size_t> det_pipeline_outstanding_count(0);
            std::mutex det_progress_mu;
            std::condition_variable det_progress_cv;
            const auto det_progress_start = std::chrono::steady_clock::now();
            auto emit_det_progress = [&](bool final) {
                const auto now = std::chrono::steady_clock::now();
                const double elapsed_s =
                    std::chrono::duration_cast<std::chrono::duration<double>>(
                        now - det_progress_start).count();
                const size_t completed =
                    det_completed_count.load(std::memory_order_relaxed);
                const ariabc_pg::kafka_consumer_stats kc_prog = consumer.stats();
                const double completed_tps = (elapsed_s > 0.0)
                    ? (static_cast<double>(completed) / elapsed_s)
                    : 0.0;

                std::ostringstream oss;
                oss << "PROGRESS_GATEWAY_DET"
                    << " elapsed_s=" << std::fixed << std::setprecision(1) << elapsed_s
                    << " total=" << queries.size()
                    << " sent=" << det_sent_count.load(std::memory_order_relaxed)
                    << " accepted=" << det_accepted_count.load(std::memory_order_relaxed)
                    << " completed=" << completed
                    << " completed_tps=" << std::fixed << std::setprecision(2) << completed_tps
                    << " terminal_lanes=" << det_terminal_count
                    << " terminal_depth=" << det_lane_pipeline_depth
                    << " pipeline_outstanding=" << det_pipeline_outstanding_count.load(std::memory_order_relaxed)
                    << " majority_inflight=" << det_inflight_count.load(std::memory_order_relaxed)
                    << " pending_accept=" << det_pending_accept_count.load(std::memory_order_relaxed)
                    << " kafka_msgs=" << kafka_messages.load(std::memory_order_relaxed)
                    << " kafka_recs=" << kafka_records.load(std::memory_order_relaxed)
                    << " kafka_parse_failures=" << kafka_parse_failures.load(std::memory_order_relaxed)
                    << " kc_msgs=" << kc_prog.message_count
                    << " kc_timeouts=" << kc_prog.poll_timeouts
                    << " permanent_failures=" << permanent_failures.load(std::memory_order_relaxed)
                    << " divergence_count=" << divergence_count.load(std::memory_order_relaxed);
                if (final) oss << " final=1";
                std::cout << oss.str() << std::endl;
            };
            std::thread det_progress_thread([&] {
                std::unique_lock<std::mutex> lk(det_progress_mu);
                while (!det_progress_stop.load(std::memory_order_relaxed)) {
                    if (det_progress_cv.wait_for(lk, std::chrono::seconds(5), [&] {
                            return det_progress_stop.load(std::memory_order_relaxed);
                        })) {
                        break;
                    }
                    lk.unlock();
                    emit_det_progress(false);
                    lk.lock();
                }
            });

            bool failed = false;
            struct det_shaped_request {
                size_t idx = 0;
                std::string req_id;
                uint64_t req_num = 0;
                std::string sql;
            };

            std::vector<size_t> det_lane_outstanding(det_terminal_count, 0);
            size_t det_total_outstanding = 0;
            std::unordered_map<uint64_t, size_t> det_req_lane;

            auto det_lane_for_idx = [&](size_t idx) -> size_t {
                return idx % det_terminal_count;
            };

            auto release_det_req_lane = [&](uint64_t req_num) {
                auto it = det_req_lane.find(req_num);
                if (it == det_req_lane.end()) {
                    return;
                }
                const size_t lane = it->second;
                if (lane < det_lane_outstanding.size() && det_lane_outstanding[lane] > 0) {
                    --det_lane_outstanding[lane];
                }
                if (det_total_outstanding > 0) {
                    --det_total_outstanding;
                }
                det_req_lane.erase(it);
                det_pipeline_outstanding_count.store(det_total_outstanding, std::memory_order_relaxed);
            };

            auto release_det_item_lanes = [&](const std::vector<det_shaped_request>& items) {
                for (const auto& item : items) {
                    release_det_req_lane(item.req_num);
                }
            };

            auto reserve_det_item_lanes = [&](const std::vector<det_shaped_request>& items) -> bool {
                if (items.empty()) {
                    return true;
                }
                std::vector<size_t> needed(det_terminal_count, 0);
                for (const auto& item : items) {
                    const size_t lane = det_lane_for_idx(item.idx);
                    ++needed[lane];
                    if (det_lane_outstanding[lane] + needed[lane] > det_lane_pipeline_depth) {
                        std::cerr << "det lane capacity exceeded lane=" << lane
                                  << " outstanding=" << det_lane_outstanding[lane]
                                  << " need=" << needed[lane]
                                  << " depth=" << det_lane_pipeline_depth << std::endl;
                        return false;
                    }
                }
                if (det_total_outstanding + items.size() > det_pipeline_cap) {
                    std::cerr << "det pipeline capacity exceeded outstanding=" << det_total_outstanding
                              << " need=" << items.size()
                              << " cap=" << det_pipeline_cap << std::endl;
                    return false;
                }
                for (const auto& item : items) {
                    const size_t lane = det_lane_for_idx(item.idx);
                    ++det_lane_outstanding[lane];
                    ++det_total_outstanding;
                    det_req_lane[item.req_num] = lane;
                }
                det_pipeline_outstanding_count.store(det_total_outstanding, std::memory_order_relaxed);
                return true;
            };

            auto shape_det_request = [&](size_t idx,
                                         std::string& out_req_id,
                                         uint64_t& out_req_num,
                                         std::string& out_sql) -> bool {
                out_sql = queries[idx];
                if (opt.query_sign == 1) {
                    std::string sig_b64;
                    std::string perr;
                    if (!ariabc_pg::parse_signed_sql_line(out_sql, out_sql, sig_b64, perr)) {
                        std::cerr << "bad signed line: " << perr << std::endl;
                        permanent_failures.fetch_add(1);
                        return false;
                    }
#ifndef SSL_LIBRARY_NOT_FOUND
                    std::string verr;
                    const std::string raw_sql = out_sql;
                    const std::string trim_sql = ariabc_pg::trim_copy(out_sql);
                    bool ok = ariabc_pg::verify_sig_sha256_rsa(keys.pub, raw_sql, sig_b64, verr);
                    if (!ok && trim_sql != raw_sql) {
                        ok = ariabc_pg::verify_sig_sha256_rsa(keys.pub, trim_sql, sig_b64, verr);
                    }
                    if (!ok) {
                        std::cerr << "signature verify failed: " << verr << std::endl;
                        permanent_failures.fetch_add(1);
                        return false;
                    }
#endif
                }

                const uint64_t det_seq = opt.det_start_seq + static_cast<uint64_t>(idx);
                if (det_seq >= 100000000ULL) {
                    std::cerr << "det seq overflow for 8-digit: " << det_seq << std::endl;
                    permanent_failures.fetch_add(1);
                    return false;
                }
                if (opt.det_raw_sql == 1) {
                    out_sql = ariabc_pg::trim_copy(out_sql);
                } else {
                    out_sql = "s " + ariabc_pg::format_det_seq8(det_seq) + " " + ariabc_pg::trim_copy(out_sql);
                }

                out_req_id = req_id_for_idx(idx);
                out_req_num = req_num_for_idx(idx);
                return true;
            };

            auto build_det_request_batch = [&](size_t start_idx,
                                               size_t max_items,
                                               ariabc_pg::client_api_request& out_req,
                                               std::vector<det_shaped_request>& out_items,
                                               size_t& out_next_idx) -> bool {
                out_req = ariabc_pg::client_api_request();
                out_items.clear();
                out_next_idx = start_idx;
                if (start_idx >= queries.size()) return true;

                const size_t batch_cap = std::max<size_t>(
                    1,
                    std::min<size_t>(static_cast<size_t>(opt.det_batch_size), max_items));
                for (size_t idx = start_idx;
                     idx < queries.size() && out_items.size() < batch_cap;
                     ++idx) {
                    det_shaped_request shaped;
                    shaped.idx = idx;
                    if (!shape_det_request(idx, shaped.req_id, shaped.req_num, shaped.sql)) {
                        return false;
                    }
                    const bool is_reset = ariabc_pg::is_reset_barrier_sql(shaped.sql);
                    if (!out_items.empty() && is_reset) {
                        out_next_idx = idx;
                        break;
                    }
                    ariabc_pg::debug_trace_submit(shaped.req_num, shaped.req_id, shaped.sql);
                    out_items.push_back(std::move(shaped));
                    out_next_idx = idx + 1;
                    if (is_reset) break;
                }

                if (out_items.empty()) return true;
                if (out_items.size() == 1) {
                    out_req.req_id = out_items.front().req_id;
                    out_req.sql = out_items.front().sql;
                    return true;
                }

                out_req.req_id = out_items.front().req_id;
                out_req.sql = "__ARIABC_BATCH items=" + std::to_string(out_items.size()) +
                              " first=" + out_items.front().req_id +
                              " last=" + out_items.back().req_id;
                out_req.batch_items.reserve(out_items.size());
                for (size_t i = 0; i < out_items.size(); ++i) {
                    ariabc_pg::client_api_request_item item;
                    item.req_id = out_items[i].req_id;
                    item.sql = out_items[i].sql;
                    out_req.batch_items.push_back(std::move(item));
                }
                return true;
            };

            auto on_det_batch_accepted = [&](const std::vector<det_shaped_request>& items) -> bool {
                for (size_t i = 0; i < items.size(); ++i) {
                    if (!reset_commit_barrier(items[i].sql)) {
                        permanent_failures.fetch_add(1);
                        return false;
                    }
                    det_accepted_count.fetch_add(1, std::memory_order_relaxed);
                    if (majority_wait_enabled) {
                        inflight.push_back(items[i].req_num);
                        det_inflight_count.fetch_add(1, std::memory_order_relaxed);
                        track_reset_req(items[i].req_num, items[i].sql);
                    } else {
                        det_completed_count.fetch_add(1, std::memory_order_relaxed);
                        release_det_req_lane(items[i].req_num);
                    }
                }
                return true;
            };

            auto wait_strict_all_nodes_for_req = [&](uint64_t rid) -> bool {
                if (!strict_hash_validation) {
                    return true;
                }
                std::string all_err;
                const auto all0 = std::chrono::steady_clock::now();
                strict_all_nodes_checks.fetch_add(1, std::memory_order_relaxed);
                const bool all_ok = votes.wait_all_nodes_consistent(
                    rid, opt.poll_interval_us, opt.poll_count, all_err);
                const auto all1 = std::chrono::steady_clock::now();
                const uint64_t all_wait_ns = static_cast<uint64_t>(
                    std::chrono::duration_cast<std::chrono::nanoseconds>(all1 - all0).count());
                total_majority_wait_ns.fetch_add(all_wait_ns);
                strict_all_nodes_wait_ns.fetch_add(all_wait_ns, std::memory_order_relaxed);
                if (!all_ok) {
                    strict_all_nodes_failures.fetch_add(1, std::memory_order_relaxed);
                    if (all_err.empty()) all_err = "all_nodes_timeout";
                    bump_terminal_reason(all_err);
                    emit_recovery_event(rid, all_err);
                    permanent_failures.fetch_add(1);
                    return false;
                }
                return true;
            };

            auto wait_det_majority_window = [&](size_t max_outstanding) -> bool {
                while (majority_wait_enabled && inflight.size() > max_outstanding) {
                    std::string maj;
                    std::string wait_err;
                    uint64_t rid = 0;
                    const auto w0 = std::chrono::steady_clock::now();
                    const bool ok_wait = votes.wait_any_majority(
                        inflight, opt.poll_interval_us, opt.poll_count, rid, maj, wait_err);
                    const auto w1 = std::chrono::steady_clock::now();
                    total_majority_wait_ns.fetch_add(
                        std::chrono::duration_cast<std::chrono::nanoseconds>(w1 - w0).count());
                    if (!ok_wait) {
                        if (wait_err.empty()) wait_err = "majority_timeout";
                        bump_terminal_reason(wait_err);
                        emit_recovery_event(rid, wait_err);
                        permanent_failures.fetch_add(1);
                        return false;
                    }
                    if (!wait_strict_all_nodes_for_req(rid)) {
                        return false;
                    }
                    det_completed_count.fetch_add(1, std::memory_order_relaxed);
                    det_inflight_count.fetch_sub(1, std::memory_order_relaxed);
                    release_det_req_lane(rid);
                    if (!maybe_wait_reset_all_nodes(rid)) {
                        permanent_failures.fetch_add(1);
                        return false;
                    }
                }
                return true;
            };

            auto desired_det_batch_slots = [&](size_t start_idx) -> size_t {
                if (start_idx >= queries.size()) return 0;
                const size_t remaining = queries.size() - start_idx;
                const size_t target_slots =
                    std::min<size_t>(
                        static_cast<size_t>(opt.det_batch_size),
                        std::min(det_pipeline_cap, remaining));
                std::vector<size_t> tmp_lane_outstanding = det_lane_outstanding;
                size_t tmp_total = det_total_outstanding;
                size_t slots = 0;
                for (size_t idx = start_idx;
                     idx < queries.size() && slots < target_slots;
                     ++idx) {
                    const size_t lane = det_lane_for_idx(idx);
                    if (tmp_lane_outstanding[lane] >= det_lane_pipeline_depth) {
                        break;
                    }
                    if (tmp_total >= det_pipeline_cap) {
                        break;
                    }
                    ++tmp_lane_outstanding[lane];
                    ++tmp_total;
                    ++slots;
                }
                if (slots < target_slots) {
                    return 0;
                }
                return slots;
            };

            const int event_leader_idx =
                ariabc_pg::g_event_submit_leader_idx.load(std::memory_order_relaxed);
            const bool det_event_pipeline =
                (submitter &&
                 submit_mode == "event" &&
                 opt.det_submit_pipeline == 1 &&
                 (opt.broadcast_to_all ||
                  (event_leader_idx >= 0 &&
                   static_cast<size_t>(event_leader_idx) < nodes.size())));

            if (det_event_pipeline) {
                struct det_submit_ticket {
                    std::vector<det_shaped_request> items;
                    std::vector<std::shared_ptr<ariabc_pg::async_cluster_submitter::submit_ctx>> ctxs;
                    size_t submit_node_idx = 0;
                    std::chrono::steady_clock::time_point submit_started_at;
                };
                struct late_accept_group {
                    std::string label;
                    struct item {
                        size_t node_idx = 0;
                        std::shared_ptr<ariabc_pg::async_cluster_submitter::submit_ctx> ctx;
                    };
                    std::vector<item> ctxs;
                };
                struct background_completion_wait {
                    size_t node_idx = 0;
                    ariabc_pg::client_api_response resp;
                    std::string label;
                };

                const size_t configured_det_submit_limit = (opt.submit_limit > 0)
                    ? static_cast<size_t>(std::max(1, opt.submit_limit))
                    : det_pipeline_cap;
                const size_t det_submit_limit =
                    std::max<size_t>(1, std::min(det_pipeline_cap, configured_det_submit_limit));
                const size_t single_submit_node_idx = opt.broadcast_to_all
                    ? 0
                    : static_cast<size_t>(event_leader_idx);
                std::deque<det_submit_ticket> pending_accepts;
                std::deque<late_accept_group> late_accepts;
                std::deque<background_completion_wait> background_completion_waits;
                size_t pending_request_count = 0;
                size_t next_idx = 0;

                auto enqueue_background_completion_wait =
                    [&](size_t node_idx,
                        const ariabc_pg::client_api_response& resp_in,
                        const std::string& label) {
                        if (!opt.broadcast_to_all ||
                            majority_wait_enabled ||
                            opt.completion_path != "direct") {
                            return;
                        }
                        background_completion_wait wait;
                        wait.node_idx = node_idx;
                        wait.resp = resp_in;
                        wait.label = label;
                        background_completion_waits.push_back(std::move(wait));
                    };

                auto drain_late_accepts = [&](bool block) -> bool {
                    for (auto it = late_accepts.begin(); it != late_accepts.end();) {
                        std::vector<late_accept_group::item> remaining;
                        remaining.reserve(it->ctxs.size());
                        for (const auto& late_item : it->ctxs) {
                            ariabc_pg::client_api_response node_resp;
                            std::string node_err;
                            bool done = false;
                            bool ok_node = false;
                            if (block) {
                                ok_node = submitter->wait_submit(late_item.ctx, node_resp, node_err);
                                done = true;
                            } else {
                                ok_node = submitter->try_collect_submit(
                                    late_item.ctx, node_resp, node_err, done);
                            }
                            if (!done) {
                                remaining.push_back(late_item);
                                continue;
                            }
                            if (!ok_node || node_resp.status != 0) {
                                std::cerr << "late broadcast accept failed for " << it->label
                                          << " err="
                                          << (!ok_node
                                                  ? (node_err.empty() ? std::string("io_failed") : node_err)
                                                  : node_resp.msg)
                                          << std::endl;
                                permanent_failures.fetch_add(1);
                                return false;
                            }
                            enqueue_background_completion_wait(
                                late_item.node_idx,
                                node_resp,
                                it->label + "/node" + std::to_string(late_item.node_idx));
                        }
                        if (remaining.empty()) {
                            it = late_accepts.erase(it);
                        } else {
                            it->ctxs = std::move(remaining);
                            ++it;
                        }
                    }
                    return true;
                };

                auto drain_background_completions = [&]() -> bool {
                    while (!background_completion_waits.empty()) {
                        background_completion_wait wait =
                            std::move(background_completion_waits.front());
                        background_completion_waits.pop_front();
                        if (!wait_direct_completion(wait.node_idx, wait.resp, wait.label)) {
                            return false;
                        }
                    }
                    return true;
                };

                auto drain_one_accept = [&]() -> bool {
                    if (pending_accepts.empty()) return true;
                    det_submit_ticket ticket = pending_accepts.front();
                    pending_accepts.pop_front();
                    if (pending_request_count >= ticket.items.size()) {
                        pending_request_count -= ticket.items.size();
                    } else {
                        pending_request_count = 0;
                    }
                    det_pending_accept_count.fetch_sub(
                        ticket.items.size(),
                        std::memory_order_relaxed);

                    ariabc_pg::client_api_response resp;
                    std::string submit_err;
                    bool ok_submit = true;
                    std::string batch_label = ticket.items.empty()
                        ? std::string("det_pipeline_batch")
                        : ticket.items.front().req_id;
                    std::vector<std::pair<size_t, ariabc_pg::client_api_response>>
                        broadcast_result_waits;
                    if (opt.broadcast_to_all && !majority_wait_enabled) {
                        // Keep the same prefix quorum caught up for every batch; a rotating
                        // quorum can strand later deterministic sequences behind different
                        // missing replicas.
                        const size_t configured_accept_quorum =
                            (opt.broadcast_accept_quorum > 0)
                                ? static_cast<size_t>(opt.broadcast_accept_quorum)
                                : static_cast<size_t>(std::max(1, majority));
                        const size_t accept_quorum =
                            std::min(ticket.ctxs.size(),
                                     std::max<size_t>(1, configured_accept_quorum));
                        const size_t configured_result_quorum =
                            (opt.broadcast_result_quorum > 0)
                                ? static_cast<size_t>(opt.broadcast_result_quorum)
                                : 0;
                        const size_t result_quorum =
                            std::min(ticket.ctxs.size(), configured_result_quorum);
                        const size_t required_quorum =
                            std::min(ticket.ctxs.size(), std::max(accept_quorum, result_quorum));
                        std::vector<uint8_t> collected(ticket.ctxs.size(), 0);
                        std::vector<ariabc_pg::client_api_response> collected_resp(ticket.ctxs.size());
                        size_t accepted = 0;
                        while (accepted < required_quorum &&
                               !stop.load(std::memory_order_relaxed)) {
                            bool progressed = false;
                            for (size_t ctx_idx = 0; ctx_idx < required_quorum; ++ctx_idx) {
                                if (collected[ctx_idx]) continue;
                                ariabc_pg::client_api_response node_resp;
                                std::string node_err;
                                bool done = false;
                                const bool ok_node = submitter->try_collect_submit(
                                    ticket.ctxs[ctx_idx], node_resp, node_err, done);
                                if (!done) continue;
                                progressed = true;
                                collected[ctx_idx] = 1;
                                if (!ok_node) {
                                    submit_err = node_err.empty()
                                        ? std::string("io_failed")
                                        : node_err;
                                    ok_submit = false;
                                    break;
                                }
                                if (node_resp.status != 0) {
                                    submit_err = node_resp.msg;
                                    ok_submit = false;
                                    break;
                                }
                                if (accepted == 0) {
                                    resp = node_resp;
                                }
                                collected_resp[ctx_idx] = node_resp;
                                ++accepted;
                            }
                            if (!ok_submit) break;
                            if (accepted < required_quorum && !progressed) {
                                std::this_thread::sleep_for(std::chrono::microseconds(100));
                            }
                        }
                        if (ok_submit && accepted < required_quorum) {
                            submit_err = "broadcast_required_accepts_not_reached";
                            ok_submit = false;
                        }
                        if (ok_submit) {
                            late_accept_group late;
                            late.label = batch_label;
                            for (size_t ctx_idx = required_quorum; ctx_idx < ticket.ctxs.size(); ++ctx_idx) {
                                late_accept_group::item late_item;
                                late_item.node_idx = ctx_idx;
                                late_item.ctx = ticket.ctxs[ctx_idx];
                                late.ctxs.push_back(std::move(late_item));
                            }
                            if (!late.ctxs.empty()) {
                                late_accepts.push_back(std::move(late));
                            }
                            for (size_t ctx_idx = 0; ctx_idx < result_quorum; ++ctx_idx) {
                                broadcast_result_waits.emplace_back(ctx_idx, collected_resp[ctx_idx]);
                            }
                            for (size_t ctx_idx = result_quorum;
                                 ctx_idx < required_quorum;
                                 ++ctx_idx) {
                                enqueue_background_completion_wait(
                                    ctx_idx,
                                    collected_resp[ctx_idx],
                                    batch_label + "/node" + std::to_string(ctx_idx));
                            }
                        }
                    } else {
                        for (size_t ctx_idx = 0; ctx_idx < ticket.ctxs.size(); ++ctx_idx) {
                            ariabc_pg::client_api_response node_resp;
                            std::string node_err;
                            const bool ok_node =
                                submitter->wait_submit(ticket.ctxs[ctx_idx], node_resp, node_err);
                            if (!ok_node) {
                                if (ok_submit) {
                                    submit_err = node_err.empty()
                                        ? std::string("io_failed")
                                        : node_err;
                                }
                                ok_submit = false;
                                continue;
                            }
                            if (node_resp.status != 0) {
                                if (ok_submit) {
                                    submit_err = node_resp.msg;
                                }
                                ok_submit = false;
                                continue;
                            }
                            if (ctx_idx == 0) {
                                resp = node_resp;
                            }
                        }
                    }
                    const auto submit_done_at = std::chrono::steady_clock::now();
                    total_submit_ns.fetch_add(
                        std::chrono::duration_cast<std::chrono::nanoseconds>(
                            submit_done_at - ticket.submit_started_at).count());
                    if (!ok_submit) {
                        std::cerr << "det pipeline submit failed req="
                                  << (ticket.items.empty() ? 0 : ticket.items.front().req_num)
                                  << " err=" << submit_err << std::endl;
                        release_det_item_lanes(ticket.items);
                        permanent_failures.fetch_add(1);
                        return false;
                    }
                    for (const auto& result_wait : broadcast_result_waits) {
                        if (!wait_direct_completion(
                                result_wait.first,
                                result_wait.second,
                                batch_label + "/node" + std::to_string(result_wait.first))) {
                            release_det_item_lanes(ticket.items);
                            return false;
                        }
                    }
                    if (!opt.broadcast_to_all &&
                        !wait_direct_completion_quorum(
                            ticket.submit_node_idx,
                            resp,
                            batch_label)) {
                        release_det_item_lanes(ticket.items);
                        return false;
                    }

                    if (!on_det_batch_accepted(ticket.items)) {
                        std::cerr << "det pipeline post-accept handling failed req="
                                  << (ticket.items.empty() ? 0 : ticket.items.front().req_num)
                                  << std::endl;
                        return false;
                    }
                    return true;
                };

                while (!failed && (next_idx < queries.size() || !pending_accepts.empty())) {
                    while (!failed &&
                           next_idx < queries.size() &&
                           pending_request_count < det_submit_limit) {
                        size_t batch_cap = desired_det_batch_slots(next_idx);
                        if (batch_cap == 0) break;
                        if (pending_request_count + batch_cap > det_submit_limit) {
                            batch_cap = det_submit_limit - pending_request_count;
                            if (batch_cap == 0) break;
                        }
                        if (majority_wait_enabled) {
                            const size_t inflight_total = inflight.size() + pending_request_count;
                            const size_t max_total_before_submit =
                                (window > batch_cap) ? (window - batch_cap) : 0;
                            if (inflight_total > max_total_before_submit) break;
                        }

                        ariabc_pg::client_api_request req;
                        std::vector<det_shaped_request> batch_items;
                        size_t next_after_batch = next_idx;
                        if (!build_det_request_batch(next_idx,
                                                     batch_cap,
                                                     req,
                                                     batch_items,
                                                     next_after_batch)) {
                            failed = true;
                            break;
                        }
                        if (batch_items.empty()) break;
                        if (!reserve_det_item_lanes(batch_items)) {
                            permanent_failures.fetch_add(1);
                            failed = true;
                            break;
                        }
                        std::vector<std::shared_ptr<ariabc_pg::async_cluster_submitter::submit_ctx>> ctxs;
                        std::string submit_err;
                        const auto submit_started_at = std::chrono::steady_clock::now();
                        bool enqueue_ok = true;
                        if (opt.broadcast_to_all) {
                            ctxs.reserve(nodes.size());
                            for (size_t node_idx = 0; node_idx < nodes.size(); ++node_idx) {
                                std::shared_ptr<ariabc_pg::async_cluster_submitter::submit_ctx> node_ctx;
                                if (!submitter->submit_async_to_node(
                                        node_idx, req, node_ctx, submit_err)) {
                                    enqueue_ok = false;
                                    break;
                                }
                                ctxs.push_back(std::move(node_ctx));
                            }
                        } else {
                            std::shared_ptr<ariabc_pg::async_cluster_submitter::submit_ctx> ctx;
                            enqueue_ok = submitter->submit_async_to_node(
                                single_submit_node_idx, req, ctx, submit_err);
                            if (enqueue_ok) {
                                ctxs.push_back(std::move(ctx));
                            }
                        }
                        if (!enqueue_ok) {
                            for (const auto& queued_ctx : ctxs) {
                                ariabc_pg::client_api_response drain_resp;
                                std::string drain_err;
                                (void)submitter->wait_submit(queued_ctx, drain_resp, drain_err);
                            }
                            std::cerr << "det pipeline enqueue failed idx=" << next_idx
                                      << " err=" << submit_err << std::endl;
                            release_det_item_lanes(batch_items);
                            permanent_failures.fetch_add(1);
                            failed = true;
                            break;
                        }

                        det_submit_ticket ticket;
                        ticket.items = std::move(batch_items);
                        ticket.ctxs = std::move(ctxs);
                        ticket.submit_node_idx = single_submit_node_idx;
                        ticket.submit_started_at = submit_started_at;
                        pending_request_count += ticket.items.size();
                        det_sent_count.fetch_add(ticket.items.size(), std::memory_order_relaxed);
                        det_pending_accept_count.fetch_add(ticket.items.size(), std::memory_order_relaxed);
                        pending_accepts.push_back(std::move(ticket));
                        next_idx = next_after_batch;
                    }

                    if (failed) break;

                    if (!pending_accepts.empty()) {
                        if (!drain_one_accept()) {
                            failed = true;
                            break;
                        }
                    }
                    if (!drain_late_accepts(false)) {
                        failed = true;
                        break;
                    }

                    if (majority_wait_enabled && next_idx < queries.size()) {
                        const size_t next_batch_slots = desired_det_batch_slots(next_idx);
                        size_t max_total_before_submit = 0;
                        if (next_batch_slots == 0) {
                            if (inflight.empty()) {
                                if (pending_accepts.empty()) {
                                    std::cerr << "det pipeline has no available terminal slot, but no request can complete"
                                              << std::endl;
                                    permanent_failures.fetch_add(1);
                                    failed = true;
                                    break;
                                }
                                continue;
                            }
                            max_total_before_submit = inflight.size() - 1;
                        } else {
                            max_total_before_submit =
                                (window > next_batch_slots) ? (window - next_batch_slots) : 0;
                        }
                        if (!wait_det_majority_window(max_total_before_submit)) {
                            failed = true;
                            break;
                        }
                    }
                }
                if (!failed) {
                    const bool report_client_accept_time =
                        opt.broadcast_to_all &&
                        !majority_wait_enabled &&
                        opt.broadcast_drain_in_timed_run == 0;
                    const auto before_final_accept_drain = std::chrono::steady_clock::now();
                    if (report_client_accept_time) {
                        client_completion_end = before_final_accept_drain;
                        client_completion_end_set = true;
                    }
                    if (!drain_late_accepts(true)) {
                        failed = true;
                    }
                    if (!failed && !drain_background_completions()) {
                        failed = true;
                    }
                    if (report_client_accept_time) {
                        const auto after_final_accept_drain = std::chrono::steady_clock::now();
                        background_accept_drain_ns +=
                            static_cast<uint64_t>(
                                std::chrono::duration_cast<std::chrono::nanoseconds>(
                                    after_final_accept_drain - before_final_accept_drain).count());
                    }
                }
            } else {
                for (size_t idx = 0; idx < queries.size();) {
                    size_t batch_cap = desired_det_batch_slots(idx);
                    if (batch_cap == 0) {
                        if (majority_wait_enabled && !inflight.empty()) {
                            if (!wait_det_majority_window(inflight.size() - 1)) {
                                failed = true;
                                break;
                            }
                            continue;
                        }
                        std::cerr << "det submit has no available terminal slot, but no request can complete"
                                  << std::endl;
                        permanent_failures.fetch_add(1);
                        failed = true;
                        break;
                    }
                    if (majority_wait_enabled) {
                        const size_t max_total_before_submit =
                            (window > batch_cap) ? (window - batch_cap) : 0;
                        if (inflight.size() > max_total_before_submit) {
                            if (!wait_det_majority_window(max_total_before_submit)) {
                                failed = true;
                            }
                            if (failed) break;
                        }
                    }

                    ariabc_pg::client_api_request req;
                    std::vector<det_shaped_request> batch_items;
                    size_t next_after_batch = idx;
                    if (!build_det_request_batch(idx,
                                                 batch_cap,
                                                 req,
                                                 batch_items,
                                                 next_after_batch)) {
                        failed = true;
                        break;
                    }
                    if (batch_items.empty()) break;
                    if (!reserve_det_item_lanes(batch_items)) {
                        permanent_failures.fetch_add(1);
                        failed = true;
                        break;
                    }

                    const auto tx_t0 = std::chrono::steady_clock::now();

                    int tries = 0;
                    std::chrono::milliseconds backoff(2);
                    ariabc_pg::client_api_response submit_resp;
                    size_t submit_node_idx = 0;
                    while (true) {
                        const auto submit_t0 = std::chrono::steady_clock::now();
                        const bool ok_submit = submit_request(req, &submit_resp, &submit_node_idx);
                        const auto submit_t1 = std::chrono::steady_clock::now();
                        total_submit_ns.fetch_add(
                            std::chrono::duration_cast<std::chrono::nanoseconds>(submit_t1 - submit_t0).count());
                        if (ok_submit) break;
                        ++tries;
                        if (tries % 50 == 0) {
                            std::cerr << "det submit retry idx=" << idx << " tries=" << tries << std::endl;
                        }
                        std::this_thread::sleep_for(backoff);
                        if (backoff < std::chrono::milliseconds(20)) {
                            backoff *= 2;
                            if (backoff > std::chrono::milliseconds(20)) backoff = std::chrono::milliseconds(20);
                        }
                    }
                    det_sent_count.fetch_add(batch_items.size(), std::memory_order_relaxed);

                    const bool needs_direct_completion_wait =
                        !(opt.broadcast_to_all && !majority_wait_enabled);
                    if (needs_direct_completion_wait &&
                        !wait_direct_completion_quorum(
                            submit_node_idx,
                            submit_resp,
                            batch_items.empty() ? std::string("det_batch")
                                                : batch_items.front().req_id)) {
                        release_det_item_lanes(batch_items);
                        failed = true;
                        break;
                    }

                    if (!on_det_batch_accepted(batch_items)) {
                        failed = true;
                        break;
                    }

                    if (majority_wait_enabled && next_after_batch < queries.size()) {
                        const size_t next_batch_slots = desired_det_batch_slots(next_after_batch);
                        const size_t max_total_before_submit =
                            (window > next_batch_slots) ? (window - next_batch_slots) : 0;
                        if (!wait_det_majority_window(max_total_before_submit)) {
                            failed = true;
                            break;
                        }
                    }

                    const auto tx_t1 = std::chrono::steady_clock::now();
                    if (opt.qrate > 0) {
                        const std::chrono::duration<double> target_interval(
                            static_cast<double>(batch_items.size()) / static_cast<double>(opt.qrate));
                        const std::chrono::duration<double> elapsed = tx_t1 - tx_t0;
                        if (elapsed < target_interval) {
                            const auto wait_d = target_interval - elapsed;
                            total_wait_ns.fetch_add(
                                std::chrono::duration_cast<std::chrono::nanoseconds>(wait_d).count());
                            std::this_thread::sleep_for(wait_d);
                        }
                    } else if (opt.tx_interval_ms > 0) {
                        std::this_thread::sleep_for(
                            std::chrono::milliseconds(
                                static_cast<int64_t>(opt.tx_interval_ms) *
                                static_cast<int64_t>(batch_items.size())));
                    }
                    idx = next_after_batch;
                }
            }

            if (majority_wait_enabled) {
                while (!failed && !inflight.empty()) {
                    std::string maj;
                    std::string wait_err;
                    uint64_t rid = 0;
                    const auto w0 = std::chrono::steady_clock::now();
                    const bool ok_wait = votes.wait_any_majority(
                        inflight, opt.poll_interval_us, opt.poll_count, rid, maj, wait_err);
                    const auto w1 = std::chrono::steady_clock::now();
                    total_majority_wait_ns.fetch_add(
                        std::chrono::duration_cast<std::chrono::nanoseconds>(w1 - w0).count());
                    if (!ok_wait) {
                        if (wait_err.empty()) wait_err = "majority_timeout";
                        bump_terminal_reason(wait_err);
                        emit_recovery_event(rid, wait_err);
                        permanent_failures.fetch_add(1);
                        failed = true;
                        break;
                    }
                    if (!wait_strict_all_nodes_for_req(rid)) {
                        failed = true;
                        break;
                    }
                    det_completed_count.fetch_add(1, std::memory_order_relaxed);
                    det_inflight_count.fetch_sub(1, std::memory_order_relaxed);
                    release_det_req_lane(rid);
                    if (!maybe_wait_reset_all_nodes(rid)) {
                        permanent_failures.fetch_add(1);
                        failed = true;
                        break;
                    }
                }
            }

            det_progress_stop.store(true, std::memory_order_relaxed);
            det_progress_cv.notify_all();
            if (det_progress_thread.joinable()) {
                det_progress_thread.join();
            }
            emit_det_progress(true);

        } else {
            // Non-deterministic modes: parallel terminals.
            //
            // At high terminal counts on a single machine (3 nodes + Postgres + Kafka),
            // letting every terminal issue a Raft client request concurrently can
            // cause request timeouts and leader churn. Limit concurrent submits:
            // the DB/Kafka pipeline is typically the real bottleneck.
            const int submit_limit = (opt.submit_limit > 0)
                ? std::max(1, opt.submit_limit)
                : std::max(1, std::min(std::max(16, opt.num_terminals * 2), 128));
            std::mutex submit_mu;
            std::condition_variable submit_cv;
            int submits_inflight = 0;

            auto acquire_submit_slot = [&] {
                std::unique_lock<std::mutex> lk(submit_mu);
                submit_cv.wait(lk, [&] { return submits_inflight < submit_limit; });
                ++submits_inflight;
            };
            auto release_submit_slot = [&] {
                std::lock_guard<std::mutex> lk(submit_mu);
                --submits_inflight;
                submit_cv.notify_one();
            };

            std::atomic<size_t> next_idx(0);
            std::vector<std::thread> workers;
            workers.reserve(static_cast<size_t>(opt.num_terminals));
            for (int t = 0; t < opt.num_terminals; ++t) {
                workers.emplace_back([&, t] {
                    (void)t;
                    // In nondet mode we pipeline "submit" vs "wait majority".
                    // If we keep a fixed per-worker window, total in-flight
                    // requests grows with thread count and can overwhelm the
                    // DB (lock contention / long tails) and trigger majority
                    // timeouts. Auto-tune to keep total in-flight bounded.
                    const size_t auto_total_inflight = 128;
                    const size_t auto_per_worker = std::max<size_t>(
                        1,
                        auto_total_inflight / std::max(1, opt.num_terminals));
                    const size_t worker_window = std::max<size_t>(
                        1,
                        static_cast<size_t>(opt.nondet_window > 0 ? opt.nondet_window : auto_per_worker));
                    std::deque<uint64_t> inflight;
                    while (true) {
                        const size_t idx = next_idx.fetch_add(1);
                        if (idx >= queries.size()) break;

                        std::string sql = queries[idx];
                        if (opt.query_sign == 1) {
                            std::string sig_b64;
                            std::string perr;
                            if (!ariabc_pg::parse_signed_sql_line(sql, sql, sig_b64, perr)) {
                                std::cerr << "bad signed line: " << perr << std::endl;
                                permanent_failures.fetch_add(1);
                                break;
                            }
#ifndef SSL_LIBRARY_NOT_FOUND
                            // Verify both raw and trimmed forms (best-effort compatibility).
                            std::string verr;
                            const std::string raw_sql = sql;
                            const std::string trim_sql = ariabc_pg::trim_copy(sql);
                            bool ok = ariabc_pg::verify_sig_sha256_rsa(keys.pub, raw_sql, sig_b64, verr);
                            if (!ok && trim_sql != raw_sql) {
                                ok = ariabc_pg::verify_sig_sha256_rsa(keys.pub, trim_sql, sig_b64, verr);
                            }
                            if (!ok) {
                                std::cerr << "signature verify failed: " << verr << std::endl;
                                permanent_failures.fetch_add(1);
                                break;
                            }
#endif
                        }

                        // Apply dbType request shaping.
                        if (opt.db_type == 0) {
                            sql = "s " + ariabc_pg::trim_copy(sql);
                        } else {
                            // 2: raw sql
                            sql = ariabc_pg::trim_copy(sql);
                        }

                        const auto tx_t0 = std::chrono::steady_clock::now();
                        const std::string req_id = req_id_for_idx(idx);
                        const uint64_t req_num = req_num_for_idx(idx);
                        ariabc_pg::debug_trace_submit(req_num, req_id, sql);

                        // Submit + wait for majority (if enabled).
                        bool submitted = false;
                        int tries = 0;
                        std::chrono::milliseconds backoff(2);
                        std::string sub_err;
                        while (true) {
                            const auto submit_t0 = std::chrono::steady_clock::now();
                            acquire_submit_slot();
                            submitted = submit_only_quiet(req_id, sql, sub_err);
                            release_submit_slot();
                            const auto submit_t1 = std::chrono::steady_clock::now();
                            total_submit_ns.fetch_add(
                                std::chrono::duration_cast<std::chrono::nanoseconds>(submit_t1 - submit_t0).count());
                            if (submitted) break;
                            ++tries;
                            if (tries % 50 == 0) {
                                std::cerr << "submit retry idx=" << idx
                                          << " tries=" << tries
                                          << " err=" << sub_err
                                          << std::endl;
                            }
                            std::this_thread::sleep_for(backoff);
                            if (backoff < std::chrono::milliseconds(20)) {
                                backoff *= 2;
                                if (backoff > std::chrono::milliseconds(20)) backoff = std::chrono::milliseconds(20);
                            }
                        }

                        inflight.push_back(req_num);
                        if (inflight.size() >= worker_window) {
                            std::string maj;
                            std::string wait_err;
                            uint64_t rid = 0;
                            const auto w0 = std::chrono::steady_clock::now();
                            bool ok_wait = false;
                            if (votes.try_pop_any_terminal(inflight, rid, maj, wait_err)) {
                                ok_wait = wait_err.empty();
                            } else {
                                rid = inflight.front();
                                inflight.pop_front();
                                ok_wait = wait_majority(rid, maj);
                            }
                            const auto w1 = std::chrono::steady_clock::now();
                            total_majority_wait_ns.fetch_add(
                                std::chrono::duration_cast<std::chrono::nanoseconds>(w1 - w0).count());
                            if (!ok_wait) {
                                if (!wait_err.empty()) {
                                    bump_terminal_reason(wait_err);
                                    emit_recovery_event(rid, wait_err);
                                }
                                permanent_failures.fetch_add(1);
                            } else if (opt.db_type == 2 && !maj.empty() && ariabc_pg::is_duplicate_key_result(maj)) {
                                duplicate_key_errors.fetch_add(1);
                            }
                        }

                        const auto tx_t1 = std::chrono::steady_clock::now();
                        if (opt.qrate > 0) {
                            const std::chrono::duration<double> target_interval(1.0 / static_cast<double>(opt.qrate));
                            const std::chrono::duration<double> elapsed = tx_t1 - tx_t0;
                            if (elapsed < target_interval) {
                                const auto wait_d = target_interval - elapsed;
                                total_wait_ns.fetch_add(
                                    std::chrono::duration_cast<std::chrono::nanoseconds>(wait_d).count());
                                std::this_thread::sleep_for(wait_d);
                            }
                        } else if (opt.tx_interval_ms > 0) {
                            std::this_thread::sleep_for(std::chrono::milliseconds(opt.tx_interval_ms));
                        }
                    }

                    while (!inflight.empty()) {
                        std::string maj;
                        std::string wait_err;
                        uint64_t rid = 0;
                        const auto w0 = std::chrono::steady_clock::now();
                        bool ok_wait = false;
                        if (votes.try_pop_any_terminal(inflight, rid, maj, wait_err)) {
                            ok_wait = wait_err.empty();
                        } else {
                            rid = inflight.front();
                            inflight.pop_front();
                            ok_wait = wait_majority(rid, maj);
                        }
                        const auto w1 = std::chrono::steady_clock::now();
                        total_majority_wait_ns.fetch_add(
                            std::chrono::duration_cast<std::chrono::nanoseconds>(w1 - w0).count());
                        if (!ok_wait) {
                            if (!wait_err.empty()) {
                                bump_terminal_reason(wait_err);
                                emit_recovery_event(rid, wait_err);
                            }
                            permanent_failures.fetch_add(1);
                        } else if (opt.db_type == 2 && !maj.empty() && ariabc_pg::is_duplicate_key_result(maj)) {
                            duplicate_key_errors.fetch_add(1);
                        }
                    }
                });
            }
            for (auto& th : workers) th.join();
        }

        const auto t_end = std::chrono::steady_clock::now();
        if (kafka_enabled) {
            consumer.set_busy_hint(false);
        }
        const auto t_report_end =
            (client_completion_end_set && permanent_failures.load() == 0)
                ? client_completion_end
                : t_end;
        const auto overall_ms =
            std::chrono::duration_cast<std::chrono::milliseconds>(t_report_end - t_start).count();
        const auto overall_wall_ms =
            std::chrono::duration_cast<std::chrono::milliseconds>(t_end - t_start).count();
        const auto wait_ms =
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::nanoseconds(total_wait_ns.load()))
                .count();
        const auto submit_ms =
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::nanoseconds(total_submit_ns.load()))
                .count();
        const auto majority_wait_ms =
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::nanoseconds(total_majority_wait_ns.load()))
                .count();
        const auto background_accept_drain_ms =
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::nanoseconds(background_accept_drain_ns))
                .count();

        // Best-effort: give Kafka thread a moment to observe late replies for divergence detection.
        if (kafka_enabled) {
            std::this_thread::sleep_for(std::chrono::milliseconds(250));
        }

        std::cout << "overall time taken (millisec) = " << overall_ms << std::endl;
        std::cout << " overall wall time including drains (millisec) = " << overall_wall_ms << std::endl;
        std::cout << " total wait time (ms) " << wait_ms << std::endl;
        std::cout << " submit time (ms) " << submit_ms << std::endl;
        std::cout << " majority wait time (ms) " << majority_wait_ms << std::endl;
        std::cout << " background accept drain time (ms) " << background_accept_drain_ms << std::endl;
        std::cout << "duplicate_key_errors=" << duplicate_key_errors.load() << std::endl;
        std::cout << "divergence_count=" << divergence_count.load() << std::endl;
        std::cout << "permanent_failures=" << permanent_failures.load() << std::endl;

        uint64_t sub_attempts = ariabc_pg::g_submit_prof.attempts.load(std::memory_order_relaxed);
        uint64_t sub_conn_calls = ariabc_pg::g_submit_prof.connect_calls.load(std::memory_order_relaxed);
        uint64_t sub_write_calls = ariabc_pg::g_submit_prof.write_calls.load(std::memory_order_relaxed);
        uint64_t sub_read_calls = ariabc_pg::g_submit_prof.read_calls.load(std::memory_order_relaxed);
        uint64_t sub_not_acc = ariabc_pg::g_submit_prof.not_accepted.load(std::memory_order_relaxed);
        double sub_conn_ms = ariabc_pg::g_submit_prof.connect_ns.load(std::memory_order_relaxed) / 1000000.0;
        double sub_write_ms = ariabc_pg::g_submit_prof.write_ns.load(std::memory_order_relaxed) / 1000000.0;
        double sub_read_ms = ariabc_pg::g_submit_prof.read_ns.load(std::memory_order_relaxed) / 1000000.0;
        if (submitter) {
            const ariabc_pg::async_submitter_stats s = submitter->stats();
            sub_attempts = s.attempts;
            sub_conn_calls = s.connect_calls;
            sub_write_calls = s.write_calls;
            sub_read_calls = s.read_calls;
            sub_not_acc = s.not_accepted;
            sub_conn_ms = s.connect_ns / 1000000.0;
            sub_write_ms = s.write_ns / 1000000.0;
            sub_read_ms = s.read_ns / 1000000.0;
        }
        const ariabc_pg::kafka_consumer_stats kc = consumer.stats();
        const ariabc_pg::kafka_producer_stats ep = err_prod.stats();
        const auto vote_prof = votes.profile();
        const uint64_t lag_cnt = kafka_consume_lag_count.load(std::memory_order_relaxed);
        const double lag_ms_sum = kafka_consume_lag_ns.load(std::memory_order_relaxed) / 1000000.0;
        const double lag_ms_mean = (lag_cnt > 0) ? (lag_ms_sum / static_cast<double>(lag_cnt)) : 0.0;
        const double lag_ms_max = kafka_consume_lag_ns_max.load(std::memory_order_relaxed) / 1000000.0;
        const size_t prof_det_window = (opt.db_type == 1) ? effective_det_window() : 0;
        const size_t prof_det_terminals =
            (opt.db_type == 1) ? std::max<size_t>(1, static_cast<size_t>(opt.num_terminals)) : 0;
        const size_t prof_det_pipeline_depth =
            (opt.db_type == 1) ? effective_det_pipeline_depth(prof_det_terminals, prof_det_window) : 0;
        std::cout
            << "PROFILE_GATEWAY "
            << " completion_path=" << opt.completion_path
            << " validation_mode=" << opt.validation_mode
            << " broadcast_to_all=" << (opt.broadcast_to_all ? 1 : 0)
            << " broadcast_accept_quorum=" << opt.broadcast_accept_quorum
            << " broadcast_result_quorum=" << opt.broadcast_result_quorum
            << " broadcast_drain_in_timed_run=" << opt.broadcast_drain_in_timed_run
            << " direct_completion_quorum=" << opt.direct_completion_quorum
            << " submit_mode=" << (submitter ? "event" : "blocking")
            << " num_terminals=" << opt.num_terminals
            << " det_pipeline_depth=" << prof_det_pipeline_depth
            << " det_batch_size=" << opt.det_batch_size
            << " effective_det_window=" << prof_det_window
            << " configured_det_window=" << ((opt.db_type == 1) ? std::max(1, opt.det_window > 0 ? opt.det_window : 32) : 0)
            << " submit_attempts=" << sub_attempts
            << " conn_calls=" << sub_conn_calls
            << " conn_ms=" << sub_conn_ms
            << " write_calls=" << sub_write_calls
            << " write_ms=" << sub_write_ms
            << " read_calls=" << sub_read_calls
            << " read_ms=" << sub_read_ms
            << " not_accepted=" << sub_not_acc
            << " kafka_msgs=" << kafka_messages.load(std::memory_order_relaxed)
            << " kafka_recs=" << kafka_records.load(std::memory_order_relaxed)
            << " kafka_parse_failures=" << kafka_parse_failures.load(std::memory_order_relaxed)
            << " kafka_parse_ms=" << (kafka_parse_ns.load(std::memory_order_relaxed) / 1000000.0)
            << " kafka_add_reply_ms=" << (kafka_add_reply_ns.load(std::memory_order_relaxed) / 1000000.0)
            << " consume_lag_ms_mean=" << lag_ms_mean
            << " consume_lag_ms_max=" << lag_ms_max
            << " consume_to_ready_ms_mean=" << vote_prof.consume_to_ready_ms_mean
            << " consume_to_ready_ms_p95=" << vote_prof.consume_to_ready_ms_p95
            << " wait_cv_sleep_ms_mean=" << vote_prof.wait_cv_sleep_ms_mean
            << " wait_cv_sleep_ms_p95=" << vote_prof.wait_cv_sleep_ms_p95
            << " ready_queue_depth_mean=" << vote_prof.ready_queue_depth_mean
            << " ready_queue_depth_max=" << vote_prof.ready_queue_depth_max
            << " kc_poll_calls=" << kc.poll_calls
            << " kc_poll_ms=" << (kc.poll_ns / 1000000.0)
            << " kc_msgs=" << kc.message_count
            << " kc_timeouts=" << kc.poll_timeouts
            << " kc_errors=" << kc.poll_errors
            << " kc_kb=" << (kc.payload_bytes / 1024.0)
            << " term_leader_unknown=" << term_leader_unknown.load(std::memory_order_relaxed)
            << " term_leader_reply_missing=" << term_leader_reply_missing.load(std::memory_order_relaxed)
            << " term_leader_full_result_hash_mismatch=" << term_leader_full_result_hash_mismatch.load(std::memory_order_relaxed)
            << " term_leader_signature_invalid=" << term_leader_signature_invalid.load(std::memory_order_relaxed)
            << " term_majority_timeout=" << term_majority_timeout.load(std::memory_order_relaxed)
            << " term_other_failure=" << term_other_failure.load(std::memory_order_relaxed)
            << " strict_all_nodes_checks=" << strict_all_nodes_checks.load(std::memory_order_relaxed)
            << " strict_all_nodes_failures=" << strict_all_nodes_failures.load(std::memory_order_relaxed)
            << " strict_all_nodes_wait_ms=" << (strict_all_nodes_wait_ns.load(std::memory_order_relaxed) / 1000000.0)
            << " err_send_calls=" << ep.send_calls
            << " err_send_ok=" << ep.send_ok
            << " err_producev_ms=" << (ep.producev_ns / 1000000.0)
            << " err_poll_ms=" << (ep.poll_ns / 1000000.0)
            << " err_flush_ms=" << (ep.flush_ns / 1000000.0)
            << " overall_wall_ms=" << overall_wall_ms
            << " background_accept_drain_ms=" << background_accept_drain_ms
            << std::endl;

        exit_code = (permanent_failures.load() > 0) ? 1 : 0;

    } else {
        // Socket server mode.
        const int port = std::stoi(opt.query_from);
        if (opt.priv_key_file.empty()) {
            std::cerr << "--privKeyFile is required in socket mode (reply signing)" << std::endl;
            return 1;
        }
#ifndef SSL_LIBRARY_NOT_FOUND
        if (!keys.priv) {
            std::cerr << "private key not loaded" << std::endl;
            return 1;
        }
#endif
        const int listen_fd = ariabc_pg::listen_tcp(port);
        std::cout << "gateway listening on " << port << std::endl;

        while (true) {
            sockaddr_in cli;
            socklen_t len = sizeof(cli);
            const int fd = ::accept(listen_fd, reinterpret_cast<sockaddr*>(&cli), &len);
            if (fd < 0) {
                if (errno == EINTR) continue;
                std::cerr << "accept failed: " << ::strerror(errno) << std::endl;
                continue;
            }

            std::thread([&, fd] {
                struct pending_req {
                    uint64_t req_num;
                };
                std::deque<pending_req> pending;
                const size_t socket_window = effective_det_window();

                auto write_error_json = [&](const std::string& err_text) {
                    std::string werr;
                    ariabc_pg::write_fd_all(fd,
                                            std::string("{\"req\":\"\",\"sig\":\"\",\"err\":\"") +
                                                ariabc_pg::json_escape(err_text) + "\"}\n",
                                            werr);
                };

                auto write_ok_json = [&](const std::string& maj) {
#ifndef SSL_LIBRARY_NOT_FOUND
                    std::string sig_b64;
                    std::string serr;
                    if (!ariabc_pg::sign_sha256_rsa(keys.priv, maj, sig_b64, serr)) {
                        write_error_json(serr);
                        return false;
                    }
                    const std::string json =
                        std::string("{\"req\":\"") + ariabc_pg::json_escape(maj) +
                        "\",\"sig\":\"" + ariabc_pg::json_escape(sig_b64) + "\"}\n";
                    std::string werr;
                    ariabc_pg::write_fd_all(fd, json, werr);
#else
                    std::string werr;
                    ariabc_pg::write_fd_all(fd,
                                            std::string("{\"req\":\"") +
                                                ariabc_pg::json_escape(maj) +
                                                "\",\"sig\":\"\"}\n",
                                            werr);
#endif
                    return true;
                };

                auto maybe_sleep_rate = [&]() {
                    if (opt.qrate > 0) {
                        std::this_thread::sleep_for(
                            std::chrono::duration_cast<std::chrono::milliseconds>(
                                std::chrono::duration<double>(1.0 / static_cast<double>(opt.qrate))));
                    } else if (opt.tx_interval_ms > 0) {
                        std::this_thread::sleep_for(std::chrono::milliseconds(opt.tx_interval_ms));
                    }
                };

                // Keep response order stable while allowing pipelined submission.
                auto drain_pending = [&](bool force_all) -> bool {
                    while (!pending.empty()) {
                        if (!force_all && pending.size() < socket_window) {
                            break;
                        }

                        const uint64_t req_num = pending.front().req_num;
                        std::string maj;
                        const bool ok_wait = wait_majority(req_num, maj);
                        if (!ok_wait) {
                            write_error_json("majority_timeout");
                            return false;
                        }

                        pending.pop_front();
                        if (!write_ok_json(maj)) {
                            return false;
                        }
                        maybe_sleep_rate();
                    }
                    return true;
                };

                while (true) {
                    std::string qline;
                    std::string rerr;
                    if (!ariabc_pg::read_fd_line(fd, qline, rerr)) {
                        break;
                    }
                    if (ariabc_pg::is_skippable_sql_line(qline)) continue;

                    std::string sql = qline;
                    if (opt.query_sign == 1) {
                        std::string sig_b64;
                        std::string perr;
                        if (!ariabc_pg::parse_signed_sql_line(qline, sql, sig_b64, perr)) {
                            std::string werr;
                            ariabc_pg::write_fd_all(fd, std::string("{\"req\":\"\",\"sig\":\"\",\"err\":\"") +
                                                        ariabc_pg::json_escape(perr) + "\"}\n",
                                                    werr);
                            continue;
                        }
#ifndef SSL_LIBRARY_NOT_FOUND
                        std::string verr;
                        const std::string trim_sql = ariabc_pg::trim_copy(sql);
                        bool ok = ariabc_pg::verify_sig_sha256_rsa(keys.pub, sql, sig_b64, verr);
                        if (!ok && trim_sql != sql) {
                            ok = ariabc_pg::verify_sig_sha256_rsa(keys.pub, trim_sql, sig_b64, verr);
                        }
                        if (!ok) {
                            std::string werr;
                            ariabc_pg::write_fd_all(fd, std::string("{\"req\":\"\",\"sig\":\"\",\"err\":\"") +
                                                        ariabc_pg::json_escape(verr) + "\"}\n",
                                                    werr);
                            continue;
                        }
#endif
                    }

                    // Apply dbType request shaping (best-effort parity with file mode).
                    if (opt.db_type == 0) {
                        sql = "s " + ariabc_pg::trim_copy(sql);
                    } else if (opt.db_type == 1) {
                        const uint64_t det = det_seq.fetch_add(1);
                        if (det >= 100000000ULL) {
                            std::string werr;
                            ariabc_pg::write_fd_all(fd, std::string("{\"req\":\"\",\"sig\":\"\",\"err\":\"det_seq_overflow\"}\n"),
                                                    werr);
                            continue;
                        }
                        if (opt.det_raw_sql == 1) {
                            sql = ariabc_pg::trim_copy(sql);
                        } else {
                            sql = "s " + ariabc_pg::format_det_seq8(det) + " " + ariabc_pg::trim_copy(sql);
                        }
                    } else {
                        sql = ariabc_pg::trim_copy(sql);
                    }

                    std::string req_id;
                    uint64_t req_num = 0;
                    bool submitted = false;
                    int det_submit_tries = 0;
                    std::chrono::milliseconds backoff(2);
                    while (true) {
                        const uint64_t id_num = opt.req_id_offset + req_seq.fetch_add(1);
                        req_id = opt.client_id + "-" + std::to_string(id_num);
                        req_num = id_num;
                        submitted = submit_only(req_id, sql);
                        if (submitted) break;

                        // In deterministic mode, retry submit failures (likely follower not accepting)
                        // to preserve forward progress.
                        if (opt.db_type == 1 && !submitted) {
                            ++det_submit_tries;
                            if (det_submit_tries % 50 == 0) {
                                std::cerr << "det submit retry tries=" << det_submit_tries << std::endl;
                            }
                            std::this_thread::sleep_for(backoff);
                            if (backoff < std::chrono::milliseconds(20)) {
                                backoff *= 2;
                                if (backoff > std::chrono::milliseconds(20)) backoff = std::chrono::milliseconds(20);
                            }
                            continue;
                        }

                        // For non-det or majority timeouts, surface an error to the client.
                        write_error_json("submit_failed");
                        break;
                    }
                    if (!submitted) {
                        continue;
                    }
                    pending.push_back(pending_req{req_num});
                    if (!drain_pending(false)) {
                        break;
                    }
                }

                if (!pending.empty()) {
                    (void)drain_pending(true);
                }
                ::close(fd);
            }).detach();
        }
    }

    stop = true;
    dispatch_cv.notify_all();
    if (dispatch_thread.joinable()) dispatch_thread.join();
    if (kafka_thread.joinable()) kafka_thread.join();
    consumer.stop();
    err_prod.stop();
    return exit_code;
}
