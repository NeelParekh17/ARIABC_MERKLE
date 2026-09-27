/*
 * Online replica recovery for pg_state_machine.
 *
 * Control verbs (served on the client port as "__ARIABC_CTRL_RECOVERY <verb> k=v ..."):
 *
 *   CUT target=<log_idx|0> [hold=0|1] [digest=0|1] [timeout_ms=N] [target_timeout_ms=N]
 *       Export a snapshot of exactly the Raft prefix <= L.  target=0 cuts at the
 *       current commit point; target=T cuts before entry T (L = T-1), which lets
 *       every replica cut at the same log position for a state comparison.
 *       target_timeout_ms bounds the wait for entry T to commit (default
 *       timeout_ms); timeout_ms bounds the wait for this replica's execution to
 *       reach the boundary, which under load is its whole execution backlog.
 *       Execution is never paused: the keeper transaction is opened from the
 *       commit thread before entry L+1 is dispatched, and the exact prefix is
 *       materialised by bcdb_cut_snapshot_export() (see src/backend/bcdb/recovery.c).
 *   RELEASE snapshot=<id>
 *   QUARANTINE
 *       Stop applying committed entries (Raft participation and log appends are
 *       unaffected).  Entries already handed to the executor still finish and
 *       publish their results: the gateway counts them only where they match
 *       another replica, so the majority never waits on one slow replica.
 *   RECOVER ref_host=H ref_port=P snapshot=<id> L=<log_idx> B=<det_seq>
 *           [ref_user=U] [ref_db=D] [wait_live_ms=N] [heap_verify=0|1] [allow_rewind=0|1]
 *       Drain, repair the local database from the reference snapshot, rebase
 *       BCDB to B and replay entries L+1.. from the local Raft log store.
 *       L must cover every entry this replica already executed (so no entry is
 *       executed, and its result published, twice) unless allow_rewind=1.
 *   STATUS
 */
#include "pg_state_machine.hxx"
#include "replica_repair.hxx"

#include <libpq-fe.h>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <sstream>

namespace ariabc_pg {
namespace {

uint64_t mono_ns() {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count());
}

std::map<std::string, std::string> parse_kv(std::istringstream& in) {
    std::map<std::string, std::string> kv;
    std::string tok;
    while (in >> tok) {
        const size_t eq = tok.find('=');
        if (eq == std::string::npos) continue;
        kv[tok.substr(0, eq)] = tok.substr(eq + 1);
    }
    return kv;
}

std::string arg_or(const std::map<std::string, std::string>& a,
                   const std::string& k, const std::string& def) {
    auto it = a.find(k);
    return it == a.end() ? def : it->second;
}

uint64_t arg_u64(const std::map<std::string, std::string>& a, const std::string& k, uint64_t def) {
    auto it = a.find(k);
    return it == a.end() ? def : std::strtoull(it->second.c_str(), nullptr, 10);
}

int64_t arg_i64(const std::map<std::string, std::string>& a, const std::string& k, int64_t def) {
    auto it = a.find(k);
    return it == a.end() ? def : std::strtoll(it->second.c_str(), nullptr, 10);
}

std::string pq_err(PGconn* c) {
    std::string e = c ? PQerrorMessage(c) : std::string("null connection");
    for (char& ch : e) {
        if (ch == '\n' || ch == '\r') ch = ' ';
    }
    return e;
}

bool pq_exec(PGconn* c, const std::string& sql, std::string& err, std::string* first_value = nullptr) {
    PGresult* r = PQexec(c, sql.c_str());
    const ExecStatusType st = r ? PQresultStatus(r) : PGRES_FATAL_ERROR;
    if (st != PGRES_COMMAND_OK && st != PGRES_TUPLES_OK) {
        err = pq_err(c);
        if (r) PQclear(r);
        return false;
    }
    if (first_value) {
        *first_value = (st == PGRES_TUPLES_OK && PQntuples(r) > 0 && PQnfields(r) > 0)
                           ? std::string(PQgetvalue(r, 0, 0))
                           : std::string();
    }
    PQclear(r);
    return true;
}

/* Recovery SQL entry points exist as builtins; register them for data
 * directories initialised before they were added to pg_proc.dat. */
std::atomic<bool> g_recovery_functions_ready{false};

bool ensure_recovery_functions(PGconn* c, std::string& err) {
    if (g_recovery_functions_ready.load(std::memory_order_acquire)) return true;
    const char* sql =
        "DO $$ "
        "BEGIN "
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
        "  IF NOT EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'merkle_find_spurious_key' AND pronamespace = 'pg_catalog'::regnamespace) THEN "
        "    CREATE FUNCTION pg_catalog.merkle_find_spurious_key(lower_bound bytea, upper_bound bytea, partition_id integer, partitions integer, base_offset bigint, max_attempts integer) "
        "    RETURNS bigint LANGUAGE internal IMMUTABLE STRICT AS 'merkle_find_spurious_key_sql'; "
        "  END IF; "
        "  IF NOT EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'bcdb_cut_snapshot_export' AND pronamespace = 'pg_catalog'::regnamespace) THEN "
        "    CREATE FUNCTION pg_catalog.bcdb_cut_snapshot_export(integer, integer) "
        "    RETURNS text LANGUAGE internal VOLATILE AS 'bcdb_cut_snapshot_export'; "
        "  END IF; "
        "  IF NOT EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'bcdb_recovery_rebase' AND pronamespace = 'pg_catalog'::regnamespace) THEN "
        "    CREATE FUNCTION pg_catalog.bcdb_recovery_rebase(integer) "
        "    RETURNS boolean LANGUAGE internal VOLATILE AS 'bcdb_recovery_rebase'; "
        "  END IF; "
        "END $$; "
        "CREATE OR REPLACE FUNCTION public.bcdb_cut_snapshot_export(integer, integer) "
        "RETURNS text LANGUAGE internal VOLATILE AS 'bcdb_cut_snapshot_export'; "
        "CREATE OR REPLACE FUNCTION public.bcdb_recovery_rebase(integer) "
        "RETURNS boolean LANGUAGE internal VOLATILE AS 'bcdb_recovery_rebase'; "
        "CREATE OR REPLACE FUNCTION public.merkle_node_upper_bound(node_id bytea, prefix_len integer) "
        "RETURNS bytea LANGUAGE internal IMMUTABLE STRICT AS 'merkle_node_upper_bound_sql'; "
        "CREATE OR REPLACE FUNCTION public.merkle_partition_for_hash(key_hash bytea, partitions integer) "
        "RETURNS smallint LANGUAGE internal IMMUTABLE STRICT AS 'merkle_partition_for_hash'; "
        "CREATE OR REPLACE FUNCTION public.merkle_key_hash(anyelement) "
        "RETURNS bytea LANGUAGE internal IMMUTABLE STRICT AS 'merkle_key_hash_sql'; "
        "CREATE OR REPLACE FUNCTION public.merkle_find_spurious_key(lower_bound bytea, upper_bound bytea, partition_id integer, partitions integer, base_offset bigint, max_attempts integer) "
        "RETURNS bigint LANGUAGE internal IMMUTABLE STRICT AS 'merkle_find_spurious_key_sql';";
    const bool ok = pq_exec(c, sql, err);
    if (ok) g_recovery_functions_ready.store(true, std::memory_order_release);
    return ok;
}

const char* mode_name(int m) {
    switch (m) {
    case 0: return "LIVE";
    case 1: return "QUARANTINED";
    case 2: return "REPLAYING";
    default: return "UNKNOWN";
    }
}

uint64_t replay_window_entries() {
    const char* v = std::getenv("ARIABC_RECOVERY_REPLAY_WINDOW");
    const uint64_t n = (v && *v) ? std::strtoull(v, nullptr, 10) : 0;
    return n > 0 ? n : 64;
}

} // namespace

std::string pg_state_machine::pg_conninfo() const {
    std::string ci = "host=" + db_opt_.host + " port=" + db_opt_.port +
                     " dbname=" + db_opt_.dbname + " user=" + db_opt_.user;
    if (!db_opt_.password.empty()) ci += " password=" + db_opt_.password;
    return ci;
}

PGconn* pg_state_machine::take_keeper_conn() {
    PGconn* c = nullptr;
    {
        std::lock_guard<std::mutex> lk(keeper_mu_);
        c = keeper_spare_;
        keeper_spare_ = nullptr;
    }
    if (c && PQstatus(c) != CONNECTION_OK) {
        PQfinish(c);
        c = nullptr;
    }
    if (!c) {
        c = PQconnectdb(pg_conninfo().c_str());
        if (!c || PQstatus(c) != CONNECTION_OK) {
            if (c) PQfinish(c);
            return nullptr;
        }
    }
    refill_keeper_conn_async();
    return c;
}

void pg_state_machine::refill_keeper_conn_async() {
    {
        std::lock_guard<std::mutex> lk(keeper_mu_);
        if (keeper_spare_ || keeper_refill_running_) return;
        keeper_refill_running_ = true;
    }
    const std::string ci = pg_conninfo();
    std::thread([this, ci] {
        PGconn* c = PQconnectdb(ci.c_str());
        if (c && PQstatus(c) != CONNECTION_OK) {
            PQfinish(c);
            c = nullptr;
        }
        std::lock_guard<std::mutex> lk(keeper_mu_);
        keeper_refill_running_ = false;
        if (keeper_spare_ || !c) {
            if (c) PQfinish(c);
            return;
        }
        keeper_spare_ = c;
    }).detach();
}

/*
 * Open the keeper transaction for a cut whose boundary is the Raft prefix
 * <= boundary_log_idx.  Caller holds commit_mu_, so no entry beyond the
 * boundary has been handed to the executor yet: every transaction beyond the
 * boundary receives its xid after the keeper's snapshot, which is what lets
 * bcdb_cut_snapshot_export() hide them without pausing anything.
 */
bool pg_state_machine::prepare_cut_locked(uint64_t boundary_log_idx, pending_cut& pc) {
    const uint64_t t0 = mono_ns();
    PGconn* k = take_keeper_conn();
    if (!k) {
        pc.failed = true;
        pc.error = "keeper_connect_failed";
        return false;
    }
    std::string err;
    if (!pq_exec(k, "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY; SELECT 1", err)) {
        PQfinish(k);
        pc.failed = true;
        pc.error = "keeper_begin_failed: " + err;
        return false;
    }
    pc.keeper = k;
    pc.boundary_log_idx = boundary_log_idx;
    pc.boundary_det_seq = last_assigned_det_seq_;
    pc.prepared = true;
    pc.prepare_us = (mono_ns() - t0) / 1000;
    return true;
}

void pg_state_machine::maybe_prepare_cut_locked(uint64_t log_idx) {
    pending_cut& pc = pending_cut_;
    if (pc.target_log_idx == 0 || pc.prepared || pc.failed) return;
    if (log_idx < pc.target_log_idx) return;
    if (log_idx == pc.target_log_idx && recov_mode_.load(std::memory_order_acquire) == 0) {
        prepare_cut_locked(log_idx - 1, pc);
    } else {
        pc.failed = true;
        pc.error = "missed_target";
    }
    cut_pending_.store(false, std::memory_order_release);
    cut_cv_.notify_all();
}

nuraft::ptr<nuraft::buffer> pg_state_machine::quarantine_ack(uint64_t log_idx, nuraft::buffer& data) {
    if (data.size() == 0) {
        return nuraft::buffer::alloc(0);
    }
    std::string req_id = "ERR";
    {
        raft_request_batch batch;
        std::string parse_err;
        data.pos(0);
        if (parse_raft_request_log(data, batch, parse_err) && !batch.items.empty()) {
            req_id = batch.items.front().req_id;
        }
        data.pos(0);
    }
    const size_t ack_sz = sizeof(int32_t) + req_id.size() + sizeof(uint64_t);
    nuraft::ptr<nuraft::buffer> ack = nuraft::buffer::alloc(ack_sz);
    nuraft::buffer_serializer bs(ack);
    bs.put_str(req_id);
    bs.put_u64(log_idx);
    return ack;
}

bool pg_state_machine::handle_recovery_control(const std::string& command,
                                               std::string& out_msg) {
    std::istringstream in(command);
    std::string verb;
    in >> verb;
    const std::map<std::string, std::string> args = parse_kv(in);
    reap_expired_cuts();
    bool ok = false;
    if (verb == "CUT") {
        ok = cmd_cut(args, out_msg);
    } else if (verb == "RELEASE") {
        ok = cmd_release(args, out_msg);
    } else if (verb == "QUARANTINE") {
        ok = cmd_quarantine(out_msg);
    } else if (verb == "RECOVER") {
        ok = cmd_recover(args, out_msg);
    } else if (verb == "STATUS") {
        ok = cmd_status(out_msg);
    } else {
        out_msg = "ERR unknown_verb " + verb;
        return false;
    }
    if (!ok && out_msg.rfind("ERR", 0) != 0) out_msg = "ERR " + out_msg;
    std::cerr << "RECOVERY_CTRL node=" << node_id_ << " verb=" << verb
              << " result=" << out_msg << std::endl;
    return ok;
}

bool pg_state_machine::cmd_cut(const std::map<std::string, std::string>& args, std::string& out) {
    if (recov_mode_.load(std::memory_order_acquire) != 0) {
        out = "ERR not_live mode=" + std::string(mode_name(recov_mode_.load()));
        return false;
    }
    const uint64_t target = arg_u64(args, "target", 0);
    const bool want_digest = arg_u64(args, "digest", 1) != 0;
    const bool hold = arg_u64(args, "hold", 1) != 0;
    const int timeout_ms = static_cast<int>(arg_u64(args, "timeout_ms", 30000));
    const int target_timeout_ms = static_cast<int>(arg_u64(args, "target_timeout_ms", timeout_ms));

    /* Make sure a pre-connected keeper is ready before the commit thread needs it. */
    {
        PGconn* warm = take_keeper_conn();
        if (!warm) {
            out = "ERR keeper_connect_failed";
            return false;
        }
        std::string err;
        const bool fn_ok = ensure_recovery_functions(warm, err);
        std::lock_guard<std::mutex> lk(keeper_mu_);
        if (keeper_spare_ == nullptr && fn_ok) {
            keeper_spare_ = warm;
        } else {
            PQfinish(warm);
        }
        if (!fn_ok) {
            out = "ERR ensure_functions_failed " + err;
            return false;
        }
    }

    pending_cut pc;
    {
        std::unique_lock<std::mutex> lk(commit_mu_);
        if (target == 0) {
            prepare_cut_locked(last_commit_seen_, pc);
        } else {
            if (last_commit_seen_ >= target) {
                out = "ERR too_late last_commit=" + std::to_string(last_commit_seen_);
                return false;
            }
            if (cut_pending_.load(std::memory_order_acquire)) {
                out = "ERR cut_busy";
                return false;
            }
            pending_cut_ = pending_cut();
            pending_cut_.target_log_idx = target;
            cut_pending_.store(true, std::memory_order_release);
            const bool done = cut_cv_.wait_for(lk, std::chrono::milliseconds(target_timeout_ms), [&] {
                return pending_cut_.prepared || pending_cut_.failed;
            });
            if (!done) {
                cut_pending_.store(false, std::memory_order_release);
                pending_cut_ = pending_cut();
                out = "ERR cut_target_timeout last_commit=" + std::to_string(last_commit_seen_);
                return false;
            }
            pc = pending_cut_;
            pending_cut_ = pending_cut();
        }
    }
    if (!pc.prepared) {
        out = "ERR " + pc.error;
        return false;
    }

    const uint64_t e0 = mono_ns();
    std::string snapshot_id;
    std::string err;
    std::ostringstream q;
    q << "SELECT bcdb_cut_snapshot_export(" << pc.boundary_det_seq << ", " << timeout_ms << ")";
    if (!pq_exec(pc.keeper, q.str(), err, &snapshot_id) || snapshot_id.empty()) {
        PQfinish(pc.keeper);
        out = "ERR export_failed " + err;
        return false;
    }
    const uint64_t export_us = (mono_ns() - e0) / 1000;

    std::string digest;
    uint64_t digest_us = 0;
    if (want_digest) {
        const uint64_t d0 = mono_ns();
        PGconn* r = open_snapshot_reader(pg_conninfo(), snapshot_id, err);
        if (!r || !compute_merkle_digest(r, digest, err)) {
            if (r) PQfinish(r);
            PQfinish(pc.keeper);
            out = "ERR digest_failed " + err;
            return false;
        }
        PQexec(r, "ROLLBACK");
        PQfinish(r);
        digest_us = (mono_ns() - d0) / 1000;
    }

    std::ostringstream o;
    o << "OK L=" << pc.boundary_log_idx << " B=" << pc.boundary_det_seq
      << " snapshot=" << snapshot_id << " digest=" << (digest.empty() ? "-" : digest)
      << " prepare_us=" << pc.prepare_us << " export_us=" << export_us
      << " digest_us=" << digest_us << " held=" << (hold ? 1 : 0);
    out = o.str();

    if (hold) {
        recovery_cut rc;
        rc.snapshot_id = snapshot_id;
        rc.boundary_log_idx = pc.boundary_log_idx;
        rc.boundary_det_seq = pc.boundary_det_seq;
        rc.keeper = pc.keeper;
        rc.digest = digest;
        rc.created_ns = mono_ns();
        rc.prepare_us = pc.prepare_us;
        rc.export_us = export_us;
        std::lock_guard<std::mutex> lk(cuts_mu_);
        cuts_[snapshot_id] = rc;
    } else {
        PQexec(pc.keeper, "ROLLBACK");
        PQfinish(pc.keeper);
    }
    return true;
}

void pg_state_machine::release_cut_locked(const std::string& snapshot_id) {
    auto it = cuts_.find(snapshot_id);
    if (it == cuts_.end()) return;
    if (it->second.keeper) {
        PGresult* r = PQexec(it->second.keeper, "ROLLBACK");
        if (r) PQclear(r);
        PQfinish(it->second.keeper);
    }
    cuts_.erase(it);
}

void pg_state_machine::reap_expired_cuts() {
    const char* v = std::getenv("ARIABC_RECOVERY_CUT_TTL_MS");
    const uint64_t ttl_ms = (v && *v) ? std::strtoull(v, nullptr, 10) : 300000;
    const uint64_t now = mono_ns();
    std::lock_guard<std::mutex> lk(cuts_mu_);
    std::vector<std::string> expired;
    for (const auto& kv : cuts_) {
        if (now - kv.second.created_ns > ttl_ms * 1000000ULL) expired.push_back(kv.first);
    }
    for (const auto& id : expired) release_cut_locked(id);
}

bool pg_state_machine::cmd_release(const std::map<std::string, std::string>& args, std::string& out) {
    const std::string id = arg_or(args, "snapshot", "");
    std::lock_guard<std::mutex> lk(cuts_mu_);
    const bool found = cuts_.count(id) > 0;
    release_cut_locked(id);
    out = found ? "OK released" : "OK not_found";
    return true;
}

bool pg_state_machine::cmd_quarantine(std::string& out) {
    bool stop_replay = false;
    {
        std::lock_guard<std::mutex> lk(commit_mu_);
        const int prev = recov_mode_.load(std::memory_order_acquire);
        if (prev == 2) stop_replay = true;
        recov_mode_.store(1, std::memory_order_release);
    }
    if (stop_replay) {
        replay_stop_.store(true, std::memory_order_release);
        if (replay_thread_.joinable()) replay_thread_.join();
        replay_stop_.store(false, std::memory_order_release);
    }
    std::lock_guard<std::mutex> lk(commit_mu_);
    std::ostringstream o;
    o << "OK mode=QUARANTINED last_commit=" << last_commit_seen_
      << " last_enqueued=" << last_enqueued_idx_
      << " applied=" << durable_applied_prefix_.load(std::memory_order_acquire)
      << " det_seq=" << last_assigned_det_seq_;
    out = o.str();
    return true;
}

bool pg_state_machine::cmd_recover(const std::map<std::string, std::string>& args, std::string& out) {
    std::unique_lock<std::mutex> rl(recover_mu_, std::try_to_lock);
    if (!rl.owns_lock()) {
        out = "ERR recover_busy";
        return false;
    }
    const std::string snapshot_id = arg_or(args, "snapshot", "");
    const std::string ref_host = arg_or(args, "ref_host", "");
    const std::string ref_port = arg_or(args, "ref_port", db_opt_.port);
    const uint64_t L = arg_u64(args, "L", 0);
    const int64_t B = arg_i64(args, "B", -2);
    const uint64_t wait_live_ms = arg_u64(args, "wait_live_ms", 0);
    const bool heap_verify = arg_u64(args, "heap_verify", 1) != 0;
    const uint64_t drain_timeout_ms = arg_u64(args, "drain_timeout_ms", 60000);
    const bool allow_rewind = arg_u64(args, "allow_rewind", 0) != 0;
    if (snapshot_id.empty() || ref_host.empty() || B < -1) {
        out = "ERR bad_args";
        return false;
    }
    if (!log_store_) {
        out = "ERR no_log_store";
        return false;
    }
    std::string ref_ci = "host=" + ref_host + " port=" + ref_port +
                         " dbname=" + arg_or(args, "ref_db", db_opt_.dbname) +
                         " user=" + arg_or(args, "ref_user", db_opt_.user);
    const std::string ref_pw = arg_or(args, "ref_password", db_opt_.password);
    if (!ref_pw.empty()) ref_ci += " password=" + ref_pw;

    const uint64_t t0 = mono_ns();
    std::string qmsg;
    cmd_quarantine(qmsg);

    /* 1. Drain whatever was already handed to the executor. */
    for (;;) {
        uint64_t target = 0;
        {
            std::lock_guard<std::mutex> lk(commit_mu_);
            target = last_enqueued_idx_;
        }
        if (durable_applied_prefix_.load(std::memory_order_acquire) >= target) break;
        if ((mono_ns() - t0) / 1000000 > drain_timeout_ms) {
            out = "ERR drain_timeout applied=" +
                  std::to_string(durable_applied_prefix_.load()) + " target=" + std::to_string(target);
            return false;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    const uint64_t t_drained = mono_ns();
    {
        std::lock_guard<std::mutex> lk(commit_mu_);
        if (L < last_enqueued_idx_ && !allow_rewind) {
            out = "ERR boundary_behind_local L=" + std::to_string(L) +
                  " last_enqueued=" + std::to_string(last_enqueued_idx_);
            return false;
        }
    }

    /* 2. Repair the local database to the reference cut. */
    replica_repair_stats rs;
    std::string err;
    if (!repair_replica_from_snapshot(pg_conninfo(), ref_ci, snapshot_id, heap_verify, rs, err)) {
        out = "ERR repair_failed " + err;
        return false;
    }
    const uint64_t t_repaired = mono_ns();

    /* 3. Rebase BCDB's deterministic watermarks to B. */
    uint64_t rb_connect_us = 0, rb_sql_us = 0, rb_lock_us = 0;
    {
        const uint64_t r0 = mono_ns();
        PGconn* c = PQconnectdb(pg_conninfo().c_str());
        rb_connect_us = (mono_ns() - r0) / 1000;
        if (!c || PQstatus(c) != CONNECTION_OK) {
            out = "ERR rebase_connect_failed " + pq_err(c);
            if (c) PQfinish(c);
            return false;
        }
        std::ostringstream q;
        q << "SELECT bcdb_recovery_rebase(" << B << ")";
        const uint64_t r1 = mono_ns();
        const bool ok = ensure_recovery_functions(c, err) && pq_exec(c, q.str(), err);
        rb_sql_us = (mono_ns() - r1) / 1000;
        PQfinish(c);
        if (!ok) {
            out = "ERR rebase_failed " + err;
            return false;
        }
    }

    /* 4. Rebase the state machine and start replaying L+1.. from the Raft log. */
    uint64_t replay_target = 0;
    {
        const uint64_t l0 = mono_ns();
        std::lock_guard<std::mutex> lk(commit_mu_);
        rb_lock_us = (mono_ns() - l0) / 1000;
        {
            std::lock_guard<std::mutex> tlk(tracker_mu_);
            entry_tracker_.clear();
            durable_applied_prefix_.store(L, std::memory_order_release);
        }
        {
            std::lock_guard<std::mutex> slk(committed_det_seq_mu_);
            next_committed_det_seq_ = static_cast<uint64_t>(B + 1);
            committed_det_seq_initialized_ = true;
        }
        last_assigned_det_seq_ = B;
        last_enqueued_idx_ = L;
        executor_.reset_det_order(static_cast<uint64_t>(B + 1));
        executor_.set_publish_suppressed(false);
        replay_next_.store(L + 1, std::memory_order_release);
        recov_mode_.store(2, std::memory_order_release);
        replay_target = last_commit_seen_;
    }
    if (replay_thread_.joinable()) replay_thread_.join();
    replay_stop_.store(false, std::memory_order_release);
    replay_thread_ = std::thread([this, L] { replay_loop(L + 1); });
    const uint64_t t_replay_started = mono_ns();

    std::ostringstream o;
    o << "OK drain_ms=" << (t_drained - t0) / 1000000
      << " repair_ms=" << (t_repaired - t_drained) / 1000000
      << " rebase_ms=" << (t_replay_started - t_repaired) / 1000000
      << " rebase_connect_us=" << rb_connect_us << " rebase_sql_us=" << rb_sql_us
      << " rebase_lock_us=" << rb_lock_us
      << " tables=" << rs.tables_checked << " repaired_tables=" << rs.tables_repaired
      << " full_copies=" << rs.full_table_copies
      << " mismatched_partitions=" << rs.mismatched_partitions
      << " differing_leaves=" << rs.differing_leaves
      << " candidate_rows=" << rs.candidate_rows
      << " rows_deleted=" << rs.rows_deleted << " rows_upserted=" << rs.rows_upserted
      << " localise_us=" << rs.localise_us << " transfer_us=" << rs.transfer_us
      << " apply_us=" << rs.apply_us << " verify_us=" << rs.verify_us
      << " digest=" << (rs.digest_after.empty() ? "-" : rs.digest_after)
      << " replay_from=" << (L + 1) << " replay_target=" << replay_target;

    if (wait_live_ms > 0) {
        const uint64_t deadline = mono_ns() + wait_live_ms * 1000000ULL;
        while (recov_mode_.load(std::memory_order_acquire) != 0 && mono_ns() < deadline) {
            std::this_thread::sleep_for(std::chrono::milliseconds(2));
        }
        const bool live = recov_mode_.load(std::memory_order_acquire) == 0;
        o << " live=" << (live ? 1 : 0)
          << " catchup_ms=" << (mono_ns() - t_replay_started) / 1000000;
    }
    o << " total_ms=" << (mono_ns() - t0) / 1000000;
    out = o.str();
    last_recover_summary_ = out;
    return true;
}

void pg_state_machine::replay_loop(uint64_t from_idx) {
    const uint64_t window = replay_window_entries();
    uint64_t next = from_idx;
    uint64_t replayed = 0;
    const uint64_t t0 = mono_ns();
    while (!replay_stop_.load(std::memory_order_acquire)) {
        uint64_t target = 0;
        {
            std::lock_guard<std::mutex> lk(commit_mu_);
            target = last_commit_seen_;
            if (next > target) {
                /* Caught up: from now on commit() applies entries directly. */
                recov_mode_.store(0, std::memory_order_release);
                recoveries_done_.fetch_add(1, std::memory_order_relaxed);
                std::cerr << "RECOVERY_REPLAY_LIVE node=" << node_id_
                          << " from=" << from_idx << " to=" << (next - 1)
                          << " entries=" << replayed
                          << " ms=" << (mono_ns() - t0) / 1000000 << std::endl;
                return;
            }
        }
        while (next <= target && !replay_stop_.load(std::memory_order_acquire)) {
            /* Bound the executor backlog so catching up never starves live work. */
            while (next > durable_applied_prefix_.load(std::memory_order_acquire) + window &&
                   !replay_stop_.load(std::memory_order_acquire)) {
                std::this_thread::sleep_for(std::chrono::microseconds(200));
            }
            nuraft::ptr<nuraft::log_entry> le = log_store_->entry_at(next);
            if (!le) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
                continue;
            }
            std::lock_guard<std::mutex> lk(commit_mu_);
            if (recov_mode_.load(std::memory_order_acquire) != 2) return;
            if (le->get_val_type() == nuraft::log_val_type::app_log && !le->is_buf_null()) {
                nuraft::ptr<nuraft::buffer> buf = le->get_buf_ptr();
                buf->pos(0);
                commit_apply_locked(next, *buf);
            } else {
                commit_noop_locked(next);
            }
            ++next;
            ++replayed;
            replay_next_.store(next, std::memory_order_release);
        }
    }
}

bool pg_state_machine::cmd_status(std::string& out) {
    std::lock_guard<std::mutex> lk(commit_mu_);
    std::ostringstream o;
    const int m = recov_mode_.load(std::memory_order_acquire);
    o << "OK mode=" << mode_name(m)
      << " last_commit=" << last_commit_seen_
      << " last_enqueued=" << last_enqueued_idx_
      << " applied=" << durable_applied_prefix_.load(std::memory_order_acquire)
      << " det_seq=" << last_assigned_det_seq_
      << " replay_next=" << replay_next_.load(std::memory_order_acquire)
      << " recoveries=" << recoveries_done_.load(std::memory_order_relaxed)
      << " suppressed=" << (executor_.publish_suppressed() ? 1 : 0);
    {
        std::lock_guard<std::mutex> clk(cuts_mu_);
        o << " held_cuts=" << cuts_.size();
    }
    out = o.str();
    return true;
}

} // namespace ariabc_pg
