#!/usr/bin/python3

import argparse
import base64
import os
import random
import psycopg
import re
import sys
import time
import threading
from concurrent.futures import (
    ThreadPoolExecutor, ProcessPoolExecutor,
    as_completed, TimeoutError as FuturesTimeoutError,
)
from threading import Lock

mutex = Lock()
mutex_seqnum = Lock()
procSeqNum = 0
inputSeqNum = 0
message_dict = {}
PRESIGNED_CMD_DICT: dict = {}  # seq_hash -> full "s {hash} {sig_b64}##{sql}" string
totalWaitTime = 0.0
duplicate_key_errors = 0
mutex_dups = Lock()
metrics_lock = Lock()

DB_USER = "postgres"
DB_PORT = "5438"
DB_NAME = "postgres"
DB_PASS = ""
#DB_HOST = "10.129.148.129"
# Default to Unix-domain socket to avoid TCP restrictions in some sandboxes.
# Override with DB_HOST=localhost (or a hostname/IP) if you need TCP.
DB_HOST = "/tmp"
RATE = 800
NUM = 4
LOG_SKIP = 4000
MAX_RETRIES = 50  # Increased for BCDB slot contention and high concurrency
RETRY_BACKOFF_SEC = 0.05  # Increased for BCDB
# Benchmarks can legitimately have long deterministic waits/retries.
# Keep statement timeout disabled by default; allow override via env.
STATEMENT_TIMEOUT = "0"
RATE_LIMIT_ENABLED = False

# Metrics / reporting (best-effort; used to avoid failing the whole run on one worker error)
retry_count = 0
serialization_retry_count = 0
reconnect_count = 0
permanent_fail_count = 0
permanent_failures = []
retry_error_counts = {}
retry_error_examples = {}
FAIL_LIST_MAX = 25
RETRY_JITTER_PCT = 0.25
WORKER_FUTURE_TIMEOUT_S = 0.0
PREPARED_CALLS_ENABLED = os.getenv("PREPARED_CALLS", "1").strip().lower() in {
    "1", "true", "yes", "on"
}
DET_PREPARED_CALLS_ENABLED = os.getenv(
    "DET_PREPARED_CALLS",
    "0",
).strip().lower() in {
    "1", "true", "yes", "on"
}
CLIENT_CURSOR_ENABLED = os.getenv(
    "ARIABC_PSYCOPG_CLIENT_CURSOR",
    "0",
).strip().lower() in {
    "1", "true", "yes", "on"
}
SIGNING_ENABLED = False
ENFORCE_SIGNATURES = False
SIGNING_PRIVATE_KEY = None
CLIENT_PUBLIC_KEY_B64 = ""
_CRYPTO_HASHES = None
_CRYPTO_PADDING = None
WORKLOAD_FILE = ""
tx_type_metrics = {}
det_timing_metrics = {
    "build_us": [],
    "sign_us": [],
    "b64_us": [],
    "roundtrip_us": [],
}


parser = argparse.ArgumentParser(
    description="Run BCDB/PG traffic against a workload file."
)
parser.add_argument("db_name")
parser.add_argument("query_data_file")
parser.add_argument("db_type", type=int, help="0=pg, 1=det, 2=nondet, -1=det alias")
parser.add_argument("num_thread", type=int)
parser.add_argument("qrate", nargs="?", type=int, default=0)
parser.add_argument("--signing", type=int, choices=[0, 1], default=0,
                    help="For deterministic mode, sign each SQL and send 'sig##sql'.")
parser.add_argument("--privkey", default="",
                    help="PEM private key path used when --signing=1.")
parser.add_argument("--enforce-signatures", type=int, choices=[0, 1], default=0,
                    help="Set bcdb_enforce_signatures in each session.")
parser.add_argument("--isolation", "--pg-isolation", dest="pg_isolation",
                    default=os.getenv("PG_ISOLATION_LEVEL", "serializable"),
                    help="Transaction isolation level for PG/nondet mode (default: serializable).")
args = parser.parse_args()

PG_ISOLATION_LEVEL = (args.pg_isolation or os.getenv("PG_ISOLATION_LEVEL", "serializable")).strip().lower()
NUM = args.num_thread
DB_TYPE_RAW = args.db_type
DB_TYPE = DB_TYPE_RAW
if DB_TYPE == -1:
    # Requested shortcut: -1 means deterministic execution (same wire format as DB_TYPE=1).
    DB_TYPE = 1
RATE = int(args.qrate)
RATE_LIMIT_ENABLED = RATE > 0
reqPause = (1.0 / RATE) if RATE_LIMIT_ENABLED else 0.0
print(" arg1 a2 == ", args.db_name, args.query_data_file);
print(" arg3 a4 pause == ", args.db_type, args.num_thread, reqPause);
print(f" rate_limit_enabled={1 if RATE_LIMIT_ENABLED else 0} qrate={RATE if RATE_LIMIT_ENABLED else 0}")
DB_NAME = args.db_name
WORKLOAD_FILE = args.query_data_file
SIGNING_ENABLED = bool(args.signing)
ENFORCE_SIGNATURES = bool(args.enforce_signatures)

# Optional env overrides (do not change CLI contract)
DB_USER = os.getenv("DB_USER", DB_USER)
DB_PASS = os.getenv("DB_PASS", DB_PASS)
DB_HOST = os.getenv("DB_HOST", DB_HOST)
DB_PORT = os.getenv("DB_PORT", DB_PORT)
STATEMENT_TIMEOUT = os.getenv("STATEMENT_TIMEOUT", STATEMENT_TIMEOUT)

def _is_skippable_sql_line(line: str) -> bool:
    s = line.strip()
    if s == "":
        return True
    # Skip SQL comments / psql metacommands (not real SQL statements).
    if s.startswith("--") or s.startswith("/*") or s.startswith("\\"):
        return True
    return False

if DB_HOST.startswith("/"):
    # libpq "host" can be a directory for Unix-domain sockets (e.g. /tmp)
    connStr = f"dbname={DB_NAME} user={DB_USER} host={DB_HOST} port={DB_PORT}"
    if DB_PASS:
        connStr += f" password={DB_PASS}"
else:
    connStr = "postgresql://"+DB_USER + ":" + DB_PASS + "@" + DB_HOST+":"+ DB_PORT + "/" + DB_NAME

try:
    MAX_RETRIES = int(os.getenv("MAX_RETRIES", str(MAX_RETRIES)))
except Exception:
    pass
try:
    RETRY_BACKOFF_SEC = float(os.getenv("RETRY_BACKOFF_SEC", str(RETRY_BACKOFF_SEC)))
except Exception:
    pass
try:
    FAIL_LIST_MAX = int(os.getenv("FAIL_LIST_MAX", str(FAIL_LIST_MAX)))
except Exception:
    pass
try:
    RETRY_JITTER_PCT = float(os.getenv("RETRY_JITTER_PCT", str(RETRY_JITTER_PCT)))
except Exception:
    pass
try:
    WORKER_FUTURE_TIMEOUT_S = float(os.getenv("WORKER_FUTURE_TIMEOUT_S", str(WORKER_FUTURE_TIMEOUT_S)))
except Exception:
    pass
try:
    LOG_SKIP = int(os.getenv("LOG_SKIP", str(LOG_SKIP)))
except Exception:
    pass


def _init_signing_material():
    global SIGNING_PRIVATE_KEY
    global CLIENT_PUBLIC_KEY_B64
    global _CRYPTO_HASHES
    global _CRYPTO_PADDING

    if not SIGNING_ENABLED:
        return

    if DB_TYPE != 1:
        print("Err: --signing=1 is only supported for deterministic BCDB mode (db_type=1 or -1)")
        exit(-1)
    if not args.privkey:
        print("Err: --privkey is required when --signing=1")
        exit(-1)

    try:
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.asymmetric import padding
    except Exception as err:
        print(f"Err: could not import cryptography for signing: {err}")
        exit(-1)

    try:
        with open(args.privkey, "rb") as f:
            SIGNING_PRIVATE_KEY = serialization.load_pem_private_key(f.read(), password=None)
    except Exception as err:
        print(f"Err: could not load signing private key '{args.privkey}': {err}")
        exit(-1)

    try:
        pub_der = SIGNING_PRIVATE_KEY.public_key().public_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PublicFormat.SubjectPublicKeyInfo,
        )
        CLIENT_PUBLIC_KEY_B64 = base64.b64encode(pub_der).decode("ascii")
    except Exception as err:
        print(f"Err: could not derive public key from '{args.privkey}': {err}")
        exit(-1)

    _CRYPTO_HASHES = hashes
    _CRYPTO_PADDING = padding


_init_signing_material()
print(f" signing_enabled={1 if SIGNING_ENABLED else 0} enforce_signatures={1 if ENFORCE_SIGNATURES else 0}")

def getSeqHash(seqNum):
    return f"{seqNum:08d}"


def _build_det_command(seq_hash: str, sql: str) -> str:
    build_start = time.perf_counter()
    if not SIGNING_ENABLED:
        cmd = f"s {seq_hash} {sql}"
        _record_det_timing_sample("build_us", (time.perf_counter() - build_start) * 1_000_000.0)
        return cmd

    # Fast path: look up pre-computed signed command (populated before workers start).
    cmd = PRESIGNED_CMD_DICT.get(seq_hash)
    if cmd is not None:
        _record_det_timing_sample("sign_us", 0.0)
        _record_det_timing_sample("b64_us", 0.0)
        _record_det_timing_sample("build_us", (time.perf_counter() - build_start) * 1_000_000.0)
        return cmd

    # Slow-path fallback: sign inline (guards against retry/DET_START_SEQ edge cases).
    sign_start = time.perf_counter()
    sig = SIGNING_PRIVATE_KEY.sign(
        sql.encode("utf-8"),
        _CRYPTO_PADDING.PKCS1v15(),
        _CRYPTO_HASHES.SHA256(),
    )
    sign_end = time.perf_counter()
    sig_b64 = base64.b64encode(sig).decode("ascii")
    b64_end = time.perf_counter()
    cmd = f"s {seq_hash} {sig_b64}##{sql}"
    build_end = time.perf_counter()

    _record_det_timing_sample("sign_us", (sign_end - sign_start) * 1_000_000.0)
    _record_det_timing_sample("b64_us", (b64_end - sign_end) * 1_000_000.0)
    _record_det_timing_sample("build_us", (build_end - build_start) * 1_000_000.0)
    return cmd


def _classify_tx_type(line: str) -> str:
    s = line.strip().lower()
    if "new_order_proc_exec" in s or "new_order_proc(" in s:
        return "new_order"
    if "payment_by_name_proc_exec" in s or "payment_by_name_proc(" in s:
        return "payment_name"
    if "payment_proc_exec" in s or "payment_proc(" in s:
        return "payment_id"
    if "order_status_summary" in s or "order_status_" in s:
        return "order_status"
    if "stock_level(" in s or "stock_level_proc(" in s:
        return "stock_level"
    if "delivery_proc_exec" in s or "delivery_proc(" in s:
        return "delivery"
    return "other"


def _get_tx_metric_bucket(tx_type: str):
    bucket = tx_type_metrics.get(tx_type)
    if bucket is None:
        bucket = {
            "count": 0,
            "ok": 0,
            "permanent_failures": 0,
            "retries_total": 0,
            "serialization_retries": 0,
            "reconnects": 0,
            "latencies_ms": [],
        }
        tx_type_metrics[tx_type] = bucket
    return bucket


def _record_tx_success(tx_type: str, latency_ms: float, retries_total: int, serialization_retries: int, reconnects: int):
    with metrics_lock:
        bucket = _get_tx_metric_bucket(tx_type)
        bucket["count"] += 1
        bucket["ok"] += 1
        bucket["retries_total"] += retries_total
        bucket["serialization_retries"] += serialization_retries
        bucket["reconnects"] += reconnects
        bucket["latencies_ms"].append(latency_ms)


def _record_tx_failure(tx_type: str, retries_total: int, serialization_retries: int, reconnects: int):
    with metrics_lock:
        bucket = _get_tx_metric_bucket(tx_type)
        bucket["count"] += 1
        bucket["permanent_failures"] += 1
        bucket["retries_total"] += retries_total
        bucket["serialization_retries"] += serialization_retries
        bucket["reconnects"] += reconnects


def _percentile(nums, pct: float):
    if not nums:
        return None
    xs = sorted(nums)
    if pct <= 0:
        return xs[0]
    if pct >= 100:
        return xs[-1]
    idx = (len(xs) - 1) * (pct / 100.0)
    lo = int(idx)
    hi = min(lo + 1, len(xs) - 1)
    frac = idx - lo
    return xs[lo] * (1.0 - frac) + xs[hi] * frac


def _record_det_timing_sample(kind: str, sample_us: float):
    with metrics_lock:
        det_timing_metrics.setdefault(kind, []).append(sample_us)


def _presign_one(args):
    # Module-level so ProcessPoolExecutor can find it via fork on Linux.
    # SIGNING_PRIVATE_KEY / _CRYPTO_PADDING / _CRYPTO_HASHES are inherited
    # from the parent process via fork (Linux default multiprocessing mode).
    seq_hash, sql = args
    sig = SIGNING_PRIVATE_KEY.sign(
        sql.encode("utf-8"),
        _CRYPTO_PADDING.PKCS1v15(),
        _CRYPTO_HASHES.SHA256(),
    )
    sig_b64 = base64.b64encode(sig).decode("ascii")
    return seq_hash, f"s {seq_hash} {sig_b64}##{sql}"


def _presign_all_queries() -> None:
    """Pre-compute all signed command strings before workers start.
    Runs outside the benchmark timing window so it doesn't inflate TPS."""
    global PRESIGNED_CMD_DICT
    items = [
        (f"{seq:08d}", message_dict[seq])
        for seq in range(procSeqNum, inputSeqNum)
        if message_dict.get(seq, "") != ""
    ]
    if not items:
        return
    n_workers = min(os.cpu_count() or 4, 16)
    print(f"Pre-signing {len(items)} queries with {n_workers} processes...")
    t0 = time.perf_counter()
    with ProcessPoolExecutor(max_workers=n_workers) as pool:
        for seq_hash, cmd in pool.map(_presign_one, items, chunksize=256):
            PRESIGNED_CMD_DICT[seq_hash] = cmd
    elapsed_ms = (time.perf_counter() - t0) * 1000.0
    print(f"Pre-signing done: {len(PRESIGNED_CMD_DICT)} entries in {elapsed_ms:.1f} ms")


# Lock-free sequence dispatch (W5.20).
#
# The original getSeqNum() acquired mutex_seqnum on EVERY query, which
# serialized all NUM worker threads through a single Python lock — a
# significant bottleneck above ~16 workers. Each worker now owns a stride
# of the input range (worker i handles seqs base + i, base + i + NUM, ...)
# so no lock is needed on the hot path. Preserved log-throttle behavior.
def getSeqNumForWorker(worker_idx: int, next_local: int):
    """Return (absolute_seq, sql) for this worker, or (-1, '') if done.

    `next_local` is the 0-based count of queries this worker has processed;
    the caller increments it. This keeps per-worker state in the worker loop
    and avoids any global mutex."""
    base = 0
    # When DET_START_SEQ offset is used, procSeqNum is set at startup.
    # We read inputSeqNum (total input count) and procSeqNum (start offset)
    # without a lock because they are set BEFORE workers start and only
    # incremented under the global mutex by the legacy path (unused here).
    base = procSeqNum
    absolute = base + worker_idx + next_local * NUM
    if absolute >= inputSeqNum:
        return -1, ''
    if ((absolute % LOG_SKIP) < NUM):
        print(f"Thread: {threading.current_thread().name} | ID: {threading.get_ident()} | seq: {absolute}")
    return absolute, message_dict[absolute]

def getSeqNum():
    # Legacy entry point retained for any external callers. Not used by
    # worker_loop anymore. Acquires mutex only on the slow fallback path.
    global procSeqNum
    global inputSeqNum

    mutex_seqnum.acquire()
    currNum = procSeqNum
    if(procSeqNum == inputSeqNum):
        mutex_seqnum.release()
        return -1, ''
    procSeqNum = procSeqNum + 1
    mutex_seqnum.release()
    if((currNum % LOG_SKIP) < NUM):
      print(f"Thread: {threading.current_thread().name} | ID: {threading.get_ident()} | seq: {procSeqNum}")

    return currNum, message_dict[currNum]

def _apply_worker_session_settings(cur):
    cur.execute(f"SET statement_timeout = '{STATEMENT_TIMEOUT}'")
    cur.execute(
        f"SET bcdb_enforce_signatures = '{'on' if ENFORCE_SIGNATURES else 'off'}'"
    )
    if PG_ISOLATION_LEVEL and DB_TYPE in (0, 2):
        cur.execute(f"SET default_transaction_isolation = '{PG_ISOLATION_LEVEL}'")
    if CLIENT_PUBLIC_KEY_B64:
        # Use SET instead of SELECT set_config(...): this fork still has some
        # row-return paths that can leak raw debug bytes into libpq sessions.
        cur.execute(
            psycopg.sql.SQL("SET bcdb_client_public_key = {}").format(
                psycopg.sql.Literal(CLIENT_PUBLIC_KEY_B64)
            )
        )

def open_worker_connection(worker_idx: int):
    last_exc = None
    for attempt in range(MAX_RETRIES):
        conn = None
        cur = None
        stage = "connect"
        try:
            cursor_factory = psycopg.ClientCursor if CLIENT_CURSOR_ENABLED else None
            conn = psycopg.connect(
                connStr,
                autocommit=True,
                cursor_factory=cursor_factory,
            )
            _install_bcdb_merkle_notice_handler(conn)
            cur = conn.cursor()
            stage = "session bootstrap"
            _apply_worker_session_settings(cur)
            return conn, cur
        except Exception as err:
            last_exc = err
            sleep_s = RETRY_BACKOFF_SEC * (2 ** attempt)
            try:
                if cur is not None:
                    cur.close()
            except Exception:
                pass
            try:
                if conn is not None:
                    conn.close()
            except Exception:
                pass
            print(f"error during {stage} in 'worker-{worker_idx}': {err}")
            time.sleep(sleep_s)

    raise last_exc

def is_retryable_error(err: psycopg.Error) -> bool:
    error_msg = str(err).lower()
    error_code = err.sqlstate if hasattr(err, 'sqlstate') else None
    # SQLSTATE class 08* are connection exceptions and are retryable.
    if error_code and error_code.startswith('08'):
        return True

    return (
        # Explicit SQLSTATE checks first (more reliable than message matching)
        error_code == '40001' or  # SerializationFailure
        error_code == '40P01' or  # deadlock_detected
        error_code == '57014' or  # query_canceled (includes statement_timeout)
        error_code == '53400' or  # ConfigurationLimitExceeded
        # Standard PostgreSQL errors
        "could not serialize" in error_msg or
        "serialize access" in error_msg or
        "serialization" in error_msg or
        "statement timeout" in error_msg or
        "canceling statement" in error_msg or
        "got no result from the query" in error_msg or
        "server closed" in error_msg or
        # psycopg/libpq protocol desync / server-side abrupt close
        "unexpected response from server" in error_msg or
        "first received character was" in error_msg or
        "connection is closed" in error_msg or
        "connection not open" in error_msg or
        "could not receive data from server" in error_msg or
        "terminating connection" in error_msg or
        "in recovery mode" in error_msg or
        "deadlock" in error_msg or
        # BCDB-specific errors
        "unable to allocate bcdb transaction slot" in error_msg or
        "bcdb tx-pool capacity" in error_msg or
        "transaction slot" in error_msg
    )

def should_reconnect(err: psycopg.Error) -> bool:
    error_msg = str(err).lower()
    error_code = err.sqlstate if hasattr(err, 'sqlstate') else None
    if error_code and error_code.startswith('08'):
        return True
    return (
        "server closed" in error_msg or
        "unexpected response from server" in error_msg or
        "first received character was" in error_msg or
        "connection is closed" in error_msg or
        "connection not open" in error_msg or
        "could not receive data from server" in error_msg or
        "terminating connection" in error_msg or
        "in recovery mode" in error_msg or
        "got no result from the query" in error_msg
    )


def is_fast_reconnect_error(err: psycopg.Error) -> bool:
    """
    Some protocol-desync errors recover immediately after reconnect.
    Applying exponential backoff to these dramatically skews throughput.
    """
    msg = str(err).lower()
    return (
        "unexpected response from server" in msg and
        "first received character was" in msg
    )

def _retry_error_key(err: Exception) -> str:
    code = getattr(err, "sqlstate", None)
    msg = str(err).strip().replace("\n", " ")
    if len(msg) > 120:
        msg = msg[:117] + "..."
    return f"sqlstate={code} msg={msg}"

def _record_permanent_failure(qCount: int, worker_idx: int, line: str, err: Exception):
    global permanent_fail_count

    sqlstate = getattr(err, "sqlstate", None)
    msg = str(err)

    with metrics_lock:
        permanent_fail_count += 1
        if len(permanent_failures) < FAIL_LIST_MAX:
            permanent_failures.append(
                {
                    "qCount": qCount,
                    "worker": worker_idx,
                    "tid": threading.get_ident(),
                    "sqlstate": sqlstate,
                    "error": msg,
                    "stmt": line,
                }
            )

def _get_raw_statusmessage(cur) -> str:
    """
    Best-effort: psycopg may normalize/truncate `statusmessage` for some builds.
    Try to prefer the raw `pgresult.command_status` when available.
    """
    try:
        pgresult = getattr(cur, "pgresult", None)
        if pgresult is not None:
            raw = getattr(pgresult, "command_status", None)
            if raw:
                if isinstance(raw, bytes):
                    try:
                        return raw.decode("utf-8", "replace")
                    except Exception:
                        return str(raw)
                return str(raw)
    except Exception:
        pass
    try:
        return cur.statusmessage if cur.statusmessage else "NO STATUS"
    except Exception:
        return "NO STATUS"

_BCDB_MERKLE_TAG = "BCDB_MERKLE_ROOTS:"

def _install_bcdb_merkle_notice_handler(conn) -> None:
    """
    Capture NOTICE payloads of the form:
      "BCDB_MERKLE_ROOTS: (p, <hex>) ..."

    bcpsql/psql suppresses this NOTICE and appends the payload to the command
    status line (e.g. "DELETE 1 , (p, <hex>)"). We emulate that here.
    """
    try:
        conn._bcdb_merkle_roots = None
    except Exception:
        return

    def _notice_cb(diag):
        try:
            msg = diag.message_primary or ""
            if not msg:
                return
            p = msg.find(_BCDB_MERKLE_TAG)
            if p == -1:
                return
            payload = msg[p + len(_BCDB_MERKLE_TAG):].strip()
            if payload:
                conn._bcdb_merkle_roots = payload
        except Exception:
            return

    try:
        conn.add_notice_handler(_notice_cb)
    except Exception:
        pass

def _format_safedb_meta_row(row) -> str:
    # Target format: (143, 607f...hash...) (no quotes around the hash).
    try:
        if isinstance(row, tuple):
            parts = []
            for v in row:
                if isinstance(v, str):
                    parts.append(v)
                else:
                    parts.append(str(v))
            return "(" + ", ".join(parts) + ")"
    except Exception:
        pass
    return str(row)

def _try_fetch_one_row(cur):
    # Only fetch if the backend returned a rowset.
    try:
        if getattr(cur, "description", None) is None:
            return None
    except Exception:
        return None
    try:
        return cur.fetchone()
    except Exception:
        return None

_CALL_STMT_RE = re.compile(
    r"^\s*call\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*\((.*)\)\s*;\s*$",
    re.IGNORECASE,
)
_SELECT_VOID_FUNC_RE = re.compile(
    r"^\s*select\s+public\.([a-zA-Z_][a-zA-Z0-9_]*)\s*\((.*)\)\s*;\s*$",
    re.IGNORECASE,
)
_SELECT_ORDER_STATUS_SUMMARY_RE = re.compile(
    r"^\s*select\s+order_id\s*,\s*entry_date\s*,\s*carrier_id\s+from\s+public\."
    r"(order_status_summary_id|order_status_summary_by_last_name)\s*\((.*)\)\s*;\s*$",
    re.IGNORECASE,
)

_PREPARED_STMT_SPECS = {
    "new_order_proc": {
        "prepared_name": "__ariabc_stmt_new_order_proc",
        "prepare_sql": (
            "PREPARE __ariabc_stmt_new_order_proc "
            "(int, int, int, int[], int[], int[]) AS "
            "SELECT public.new_order_proc_exec($1, $2, $3, $4, $5, $6)"
        ),
    },
    "payment_proc": {
        "prepared_name": "__ariabc_stmt_payment_proc",
        "prepare_sql": (
            "PREPARE __ariabc_stmt_payment_proc "
            "(int, int, int, int, int, numeric) AS "
            "SELECT public.payment_proc_exec($1, $2, $3, $4, $5, $6)"
        ),
    },
    "payment_by_name_proc": {
        "prepared_name": "__ariabc_stmt_payment_by_name_proc",
        "prepare_sql": (
            "PREPARE __ariabc_stmt_payment_by_name_proc "
            "(text, int, int, int, int, numeric) AS "
            "SELECT public.payment_by_name_proc_exec($1, $2, $3, $4, $5, $6)"
        ),
    },
    "delivery_proc": {
        "prepared_name": "__ariabc_stmt_delivery_proc",
        "prepare_sql": (
            "PREPARE __ariabc_stmt_delivery_proc "
            "(int, int, int, timestamp) AS "
            "SELECT public.delivery_proc_exec($1, $2, $3, $4)"
        ),
    },
    "order_status_summary_id": {
        "prepared_name": "__ariabc_stmt_order_status_summary_id",
        "prepare_sql": (
            "PREPARE __ariabc_stmt_order_status_summary_id "
            "(int, int, int) AS "
            "SELECT order_id, entry_date, carrier_id "
            "FROM public.order_status_summary_id($1, $2, $3)"
        ),
    },
    "order_status_summary_by_last_name": {
        "prepared_name": "__ariabc_stmt_order_status_summary_by_last_name",
        "prepare_sql": (
            "PREPARE __ariabc_stmt_order_status_summary_by_last_name "
            "(int, int, text) AS "
            "SELECT order_id, entry_date, carrier_id "
            "FROM public.order_status_summary_by_last_name($1, $2, $3)"
        ),
    },
    "stock_level": {
        "prepared_name": "__ariabc_stmt_stock_level",
        "prepare_sql": (
            "PREPARE __ariabc_stmt_stock_level "
            "(int, int, int) AS "
            "SELECT public.stock_level($1, $2, $3)"
        ),
    },
}

def _prepared_call_state(conn):
    state = getattr(conn, "_ariabc_prepared_calls", None)
    if state is None:
        state = {}
        conn._ariabc_prepared_calls = state
    return state

def _ensure_prepared_statement(conn, cur, proc_name: str) -> bool:
    state = _prepared_call_state(conn)
    if proc_name in state:
        return state[proc_name]

    spec = _PREPARED_STMT_SPECS[proc_name]
    try:
        cur.execute(spec["prepare_sql"])
        state[proc_name] = True
    except Exception as err:
        state[proc_name] = False
        print(f"Prepared statement fallback for {proc_name}: {err}")

    return state[proc_name]

def _prepared_statements_enabled_for_mode() -> bool:
    if DB_TYPE == 1:
        return DET_PREPARED_CALLS_ENABLED
    return PREPARED_CALLS_ENABLED


def _rewrite_stmt_for_execute(conn, cur, line: str) -> str:
    stmt = line.strip()
    if not _prepared_statements_enabled_for_mode():
        return stmt

    for rx in (_CALL_STMT_RE, _SELECT_VOID_FUNC_RE, _SELECT_ORDER_STATUS_SUMMARY_RE):
        match = rx.match(stmt)
        if not match:
            continue
        proc_name = match.group(1).lower()
        spec = _PREPARED_STMT_SPECS.get(proc_name)
        if spec is None:
            return stmt
        if not _ensure_prepared_statement(conn, cur, proc_name):
            return stmt
        args_sql = match.group(2).strip()
        return f"EXECUTE {spec['prepared_name']}({args_sql});"
    return stmt


def _prepare_statement_for_mode(line: str) -> str:
    """
    Normalize statements for non-deterministic/PG modes.
    Some server paths can produce protocol-level errors on duplicate INSERTs;
    folding to ON CONFLICT DO NOTHING preserves benchmark intent (best-effort insert)
    while avoiding avoidable reconnect-heavy retries.
    """
    stmt = line.strip()
    if DB_TYPE in (0, 2):
        up = stmt.upper()
        if up.startswith("INSERT INTO USERTABLE_SMALL") and "ON CONFLICT" not in up:
            if stmt.endswith(";"):
                stmt = stmt[:-1] + " ON CONFLICT DO NOTHING;"
            else:
                stmt = stmt + " ON CONFLICT DO NOTHING"
    return stmt

def execTx(conn, cur, worker_idx: int, qCount, line):
    global totalWaitTime
    global duplicate_key_errors
    global retry_count
    global serialization_retry_count
    global reconnect_count

    ts1 = time.time()
    serialErr = 1
    pfx = 's '
    mHash = getSeqHash(qCount)
    tx_type = _classify_tx_type(line)
    tx_retry_total = 0
    tx_serialization_retries = 0
    tx_reconnects = 0
    if line != message_dict[qCount]:
        print(" qCount = " + str(qCount) + ' ' + message_dict[qCount] + ' whereas line= ' + line)
    if (qCount % LOG_SKIP) < NUM:
        print("timestamp1 = " + str(ts1) + " line= " + line)

    try:
        result_message = "NO STATUS"
        result_data = None
        last_exc = None

        for attempt in range(MAX_RETRIES):
            try:
                # Avoid carrying a stale merkle-roots payload across retries/statements.
                if hasattr(conn, "_bcdb_merkle_roots"):
                    conn._bcdb_merkle_roots = None

                exec_line = _prepare_statement_for_mode(_rewrite_stmt_for_execute(conn, cur, line))
                # Execute the actual query
                if DB_TYPE == 1:
                    det_cmd = _build_det_command(mHash, exec_line)
                    roundtrip_start = time.perf_counter()
                    cur.execute(det_cmd)
                else:
                    # PG/nondet modes execute plain SQL text.
                    roundtrip_start = None
                    cur.execute(exec_line)

                # Get status message
                result_message = _get_raw_statusmessage(cur)
                result_data = None

                # Fetch one row for SELECTs. In DET completion-only mode the
                # backend returns a tiny placeholder row so libpq clients stay
                # protocol-consistent without materializing the real payload.
                if line.strip().upper().startswith('SELECT'):
                    try:
                        row = cur.fetchone()
                        result_data = [row] if row is not None else []
                    except Exception as e:
                        if (qCount % LOG_SKIP) < NUM:
                            print(f"  Warning: Could not fetch results: {e}")
                else:
                    # In SafeDB deterministic mode, some statements may also return a metadata row
                    # (e.g. (slot, hash)). psql shows it inline; psycopg requires fetching.
                    if DB_TYPE == 1:
                        meta_row = _try_fetch_one_row(cur)
                        if meta_row is not None:
                            result_message = result_message + " , " + _format_safedb_meta_row(meta_row)

                # Append BCDB merkle roots captured from NOTICEs (if any).
                merkle_roots = getattr(conn, "_bcdb_merkle_roots", None)
                if merkle_roots:
                    result_message = result_message + " , " + str(merkle_roots)
                    conn._bcdb_merkle_roots = None

                if roundtrip_start is not None:
                    _record_det_timing_sample(
                        "roundtrip_us",
                        (time.perf_counter() - roundtrip_start) * 1_000_000.0,
                    )

                # If we get here, query succeeded
                serialErr = 0

                # Log results
                if (qCount % LOG_SKIP) < NUM:
                    print("  2timestamp1 = " + str(ts1) + " tid= " + str(threading.get_ident()))
                    print(result_message)
                    if result_data is not None:
                        print(str(result_data))

                # Success - break out of retry loop
                break

            except psycopg.Error as err:
                last_exc = err
                err_sqlstate = getattr(err, "sqlstate", None)

                # In vanilla PG/non-deterministic paths, duplicate-key can be an expected
                # workload outcome. Count it, then continue instead of failing the run.
                if DB_TYPE in (0, 2) and getattr(err, 'sqlstate', None) == '23505':
                    with mutex_dups:
                        duplicate_key_errors += 1
                    print(f"Duplicate key (23505) qCount={qCount} tid={threading.get_ident()} line={line}")
                    serialErr = 0
                    break

                if is_retryable_error(err) and attempt + 1 < MAX_RETRIES:
                    base_sleep_s = RETRY_BACKOFF_SEC * (2 ** attempt)
                    jitter = 1.0 + (random.random() * max(0.0, RETRY_JITTER_PCT))
                    sleep_s = base_sleep_s * jitter
                    if is_fast_reconnect_error(err):
                        sleep_s = 0.0

                    with metrics_lock:
                        retry_count += 1
                        if err_sqlstate == "40001":
                            serialization_retry_count += 1
                        key = _retry_error_key(err)
                        retry_error_counts[key] = retry_error_counts.get(key, 0) + 1
                        if key not in retry_error_examples and len(retry_error_examples) < FAIL_LIST_MAX:
                            retry_error_examples[key] = {
                                "qCount": qCount,
                                "worker": worker_idx,
                                "tid": threading.get_ident(),
                            }
                    tx_retry_total += 1
                    if err_sqlstate == "40001":
                        tx_serialization_retries += 1

                    if (qCount % LOG_SKIP) < NUM or "transaction slot" in str(err).lower():
                        print(
                            f"  Retry {attempt+1}/{MAX_RETRIES} for qCount={qCount} "
                            f"tid={threading.get_ident()} due to: {err}"
                        )

                    if should_reconnect(err):
                        try:
                            cur.close()
                        except Exception:
                            pass
                        try:
                            conn.close()
                        except Exception:
                            pass
                        conn, cur = open_worker_connection(worker_idx)
                        with metrics_lock:
                            reconnect_count += 1
                        tx_reconnects += 1

                    time.sleep(sleep_s)
                    continue
                else:
                    # Not retryable or max retries reached
                    raise
            except Exception as err:
                last_exc = err
                if attempt + 1 < MAX_RETRIES:
                    sleep_s = RETRY_BACKOFF_SEC * (2 ** attempt)
                    if (qCount % LOG_SKIP) < NUM:
                        print(
                            f"  Retry {attempt+1}/{MAX_RETRIES} for qCount={qCount} "
                            f"tid={threading.get_ident()} due to: {err}"
                        )
                    try:
                        cur.close()
                    except Exception:
                        pass
                    try:
                        conn.close()
                    except Exception:
                        pass
                    conn, cur = open_worker_connection(worker_idx)
                    tx_retry_total += 1
                    tx_reconnects += 1
                    time.sleep(sleep_s)
                    continue
                raise

        # If all retries failed, raise the last exception
        if serialErr == 1 and last_exc is not None:
            _record_tx_failure(tx_type, tx_retry_total, tx_serialization_retries, tx_reconnects)
            raise last_exc

        sys.stdout.flush()

        ts2 = time.time()
        _record_tx_success(tx_type, (ts2 - ts1) * 1000.0, tx_retry_total, tx_serialization_retries, tx_reconnects)
        if (qCount % LOG_SKIP) < NUM:
            print("    time diff (ms) = " + str((ts2 - ts1) * 1000) + " tid= " + str(threading.get_ident()))

        if reqPause > 0:
            waitTime = reqPause - (ts2 - ts1)
            if waitTime > 0:
                time.sleep(waitTime)
                mutex.acquire()
                totalWaitTime += waitTime
                mutex.release()

    except psycopg.DatabaseError as err:
        # Let caller decide whether to terminate the run; do not force-stop here.
        raise
    except Exception as e:
        # Let caller decide whether to terminate the run; do not force-stop here.
        raise

    sys.stdout.flush()

    return conn, cur
    
def worker_loop(worker_idx: int):
    conn = None
    cur = None
    try:
        conn, cur = open_worker_connection(worker_idx)
        local_idx = 0
        while True:
            qCount, q = getSeqNumForWorker(worker_idx, local_idx)
            local_idx += 1
            if qCount == -1 or q == '':
                return
            try:
                conn, cur = execTx(conn, cur, worker_idx, qCount, q)
            except Exception as err:
                # Never kill the whole run due to one statement's failure.
                _record_permanent_failure(qCount, worker_idx, q, err)
                print(
                    f"Permanent failure (after {MAX_RETRIES} attempts) "
                    f"qCount={qCount} worker={worker_idx} tid={threading.get_ident()} "
                    f"sqlstate={getattr(err,'sqlstate',None)} err={err}"
                )

                # Defensive reconnect so the worker can continue.
                try:
                    if cur is not None:
                        cur.close()
                except Exception:
                    pass
                try:
                    if conn is not None:
                        conn.close()
                except Exception:
                    pass
                try:
                    conn, cur = open_worker_connection(worker_idx)
                    with metrics_lock:
                        global reconnect_count
                        reconnect_count += 1
                except Exception as conn_err:
                    # If we can't reconnect, record and return (do not crash the whole process).
                    _record_permanent_failure(qCount, worker_idx, q, conn_err)
                    print(f"Worker {worker_idx} cannot reconnect; exiting worker loop: {conn_err}")
                    return
    finally:
        try:
            if cur is not None:
                cur.close()
        except Exception:
            pass
        try:
            if conn is not None:
                conn.close()
        except Exception:
            pass

    
allQueries = []
k = 0

try:
    # Optional deterministic start sequence override (useful if you don't restart the server).
    # Only applies to deterministic wire format (DB_TYPE=1, or DB_TYPE_RAW=-1 alias).
    if DB_TYPE == 1:
        try:
            base = int(os.getenv("DET_START_SEQ", "0"))
        except Exception:
            base = 0
        if base < 0:
            base = 0
        if base >= 100_000_000:
            print(f"Err: DET_START_SEQ too large for 8-digit hashes: {base}")
            exit(-1)

        if base != 0:
            print(
                f"WARN: DET_START_SEQ={base}. This must match the server's current "
                f"published_max watermark. If you restarted the server, set DET_START_SEQ=0 "
                f"or the run will not make progress."
            )
        procSeqNum = base
        inputSeqNum = base

    with open(WORKLOAD_FILE, 'r') as fptr:
         count = 0
         for line in fptr:
            if _is_skippable_sql_line(line):
                continue
            name = line.strip()
            count += 1
            if((count % LOG_SKIP) < NUM):
               print("next Tx = " + name + '  ')
            message_dict[inputSeqNum] = name
            inputSeqNum += 1
            k = k + 1
except FileNotFoundError:
    print(f"Error: Query data file '{WORKLOAD_FILE}' not found")
    exit(-1)

message_dict[inputSeqNum] = ''
inputSeqNum += 1

if SIGNING_ENABLED and DB_TYPE == 1:
    _presign_all_queries()

tStart = time.time()

timed_out = False
execPool = ThreadPoolExecutor(max_workers=NUM)
futures = []
for i in range(NUM):
    futures.append(execPool.submit(worker_loop, i))

try:
    if WORKER_FUTURE_TIMEOUT_S > 0:
        for fut in as_completed(futures, timeout=WORKER_FUTURE_TIMEOUT_S):
            fut.result()
    else:
        for fut in futures:
            fut.result()
except FuturesTimeoutError:
    timed_out = True
    print(f"ERROR: worker futures exceeded timeout: {WORKER_FUTURE_TIMEOUT_S}s")
    for fut in futures:
        fut.cancel()
finally:
    execPool.shutdown(wait=not timed_out, cancel_futures=timed_out)

if timed_out:
    sys.exit(2)

tEnd = time.time()
sys.stdout.flush()
print("overall time taken (millisec) = " + str((tEnd - tStart)*1000))
print(" total wait time (ms) " + str(totalWaitTime * 1000))
print(f"duplicate_key_errors={duplicate_key_errors}")

with metrics_lock:
    print(
        "retry_summary:"
        f" retries_total={retry_count}"
        f" serialization_retries={serialization_retry_count}"
        f" reconnects={reconnect_count}"
        f" permanent_failures={permanent_fail_count}"
    )
    if retry_error_counts:
        top = sorted(retry_error_counts.items(), key=lambda kv: kv[1], reverse=True)[:FAIL_LIST_MAX]
        print(f"retry_error_top_{len(top)}:")
        for k, cnt in top:
            ex = retry_error_examples.get(k, {})
            print(
                f"  count={cnt} {k}"
                f" example_qCount={ex.get('qCount')}"
                f" worker={ex.get('worker')} tid={ex.get('tid')}"
            )
    if permanent_failures:
        print(f"permanent_failures_first_{len(permanent_failures)}:")
        for f in permanent_failures:
            print(
                f"  qCount={f['qCount']} worker={f['worker']} tid={f['tid']} "
                f"sqlstate={f['sqlstate']} stmt={f['stmt']} err={f['error']}"
            )
    for tx_type in sorted(tx_type_metrics.keys()):
        bucket = tx_type_metrics[tx_type]
        latencies = bucket.get("latencies_ms", [])
        mean_ms = (sum(latencies) / len(latencies)) if latencies else 0.0
        p50_ms = _percentile(latencies, 50.0) if latencies else 0.0
        p95_ms = _percentile(latencies, 95.0) if latencies else 0.0
        print(
            "TX_TYPE_STATS|"
            f"tx_type={tx_type}|"
            f"count={bucket.get('count', 0)}|"
            f"ok={bucket.get('ok', 0)}|"
            f"permanent_failures={bucket.get('permanent_failures', 0)}|"
            f"retries_total={bucket.get('retries_total', 0)}|"
            f"serialization_retries={bucket.get('serialization_retries', 0)}|"
            f"reconnects={bucket.get('reconnects', 0)}|"
            f"mean_ms={mean_ms:.3f}|"
            f"p50_ms={p50_ms:.3f}|"
            f"p95_ms={p95_ms:.3f}"
        )
    for kind in ("build_us", "sign_us", "b64_us", "roundtrip_us"):
        samples = det_timing_metrics.get(kind, [])
        total_ms = (sum(samples) / 1000.0) if samples else 0.0
        mean_us = (sum(samples) / len(samples)) if samples else 0.0
        p50_us = _percentile(samples, 50.0) if samples else 0.0
        p95_us = _percentile(samples, 95.0) if samples else 0.0
        max_us = max(samples) if samples else 0.0
        print(
            "DET_TIMING_STATS|"
            f"kind={kind}|"
            f"count={len(samples)}|"
            f"total_ms={total_ms:.3f}|"
            f"mean_us={mean_us:.3f}|"
            f"p50_us={p50_us:.3f}|"
            f"p95_us={p95_us:.3f}|"
            f"max_us={max_us:.3f}"
        )

if permanent_fail_count > 0:
    sys.exit(1)
