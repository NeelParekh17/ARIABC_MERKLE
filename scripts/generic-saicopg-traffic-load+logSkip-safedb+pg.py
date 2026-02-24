#!/usr/bin/python3

import os
import random
import psycopg
import sys
import time
import threading
import traceback
from concurrent.futures import ThreadPoolExecutor
from threading import Lock

mutex = Lock()
mutex_seqnum = Lock()
procSeqNum = 0
inputSeqNum = 0
message_dict = {}
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
MAX_RETRIES = 10  # Increased for BCDB slot contention
RETRY_BACKOFF_SEC = 0.1  # Increased for BCDB
STATEMENT_TIMEOUT = "90s"

# Metrics / reporting (best-effort; used to avoid failing the whole run on one worker error)
retry_count = 0
serialization_retry_count = 0
reconnect_count = 0
permanent_fail_count = 0
permanent_failures = []
FAIL_LIST_MAX = 25
RETRY_JITTER_PCT = 0.25
 
#print("arglen== " , len(sys.argv))
if(len(sys.argv) < 4):
  print("Err: missing arguments... ")
  print("Usage: python3 generic-saicopg-traffic-load+logSkip-safedb+pg.py <dbName> <queryDataFile> <pg0Safe1Aria2|-1(det-auto)> <num-thread> [<<Qrate>]");
  exit(-1)

NUM = int(sys.argv[4])
DB_TYPE_RAW = int(sys.argv[3])
DB_TYPE = DB_TYPE_RAW
if DB_TYPE == -1:
    # Requested shortcut: -1 means deterministic execution (same wire format as DB_TYPE=1).
    DB_TYPE = 1
if(len(sys.argv) > 5):
  RATE = int(sys.argv[5])
reqPause = 1.0 / RATE
print(" arg1 a2 == ", sys.argv[1], sys.argv[2]);
print(" arg3 a4 pause == ", sys.argv[3], sys.argv[4], reqPause);
DB_NAME = sys.argv[1]

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

def getSeqHash(seqNum):
    return f"{seqNum:08d}"

def getSeqNum():
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

def open_worker_connection(worker_idx: int):
    last_exc = None
    for attempt in range(MAX_RETRIES):
        try:
            conn = psycopg.connect(connStr, autocommit=True)
            _install_bcdb_merkle_notice_handler(conn)
            cur = conn.cursor()
            cur.execute(f"SET statement_timeout = '{STATEMENT_TIMEOUT}'")
            return conn, cur
        except Exception as err:
            last_exc = err
            sleep_s = RETRY_BACKOFF_SEC * (2 ** attempt)
            print(f"error connecting in 'worker-{worker_idx}': {err}")
            time.sleep(sleep_s)

    raise last_exc

def is_retryable_error(err: psycopg.Error) -> bool:
    error_msg = str(err).lower()
    error_code = err.sqlstate if hasattr(err, 'sqlstate') else None

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
        "connection" in error_msg or
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
    return (
        "server closed" in error_msg or
        "unexpected response from server" in error_msg or
        "first received character was" in error_msg or
        "connection" in error_msg or
        "terminating connection" in error_msg or
        "in recovery mode" in error_msg or
        "got no result from the query" in error_msg
    )

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

                # Execute the actual query
                if DB_TYPE == 1:
                    cur.execute(pfx + mHash + ' ' + line)
                elif DB_TYPE == 2:
                    cur.execute(line)
                else:
                    cur.execute(pfx + line)

                # Get status message
                result_message = _get_raw_statusmessage(cur)
                result_data = None

                # Fetch results for SELECT queries
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

                # Non-determinism path: treat duplicate-key as non-fatal (log + count + continue)
                if DB_TYPE == 2 and getattr(err, 'sqlstate', None) == '23505':
                    with mutex_dups:
                        duplicate_key_errors += 1
                    print(f"Duplicate key (23505) qCount={qCount} tid={threading.get_ident()} line={line}")
                    serialErr = 0
                    break

                if is_retryable_error(err) and attempt + 1 < MAX_RETRIES:
                    base_sleep_s = RETRY_BACKOFF_SEC * (2 ** attempt)
                    jitter = 1.0 + (random.random() * max(0.0, RETRY_JITTER_PCT))
                    sleep_s = base_sleep_s * jitter

                    with metrics_lock:
                        retry_count += 1
                        if err_sqlstate == "40001":
                            serialization_retry_count += 1

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
                    time.sleep(sleep_s)
                    continue
                raise

        # If all retries failed, raise the last exception
        if serialErr == 1 and last_exc is not None:
            raise last_exc

        sys.stdout.flush()

        ts2 = time.time()
        if (qCount % LOG_SKIP) < NUM:
            print("    time diff (ms) = " + str((ts2 - ts1) * 1000) + " tid= " + str(threading.get_ident()))

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
        while True:
            qCount, q = getSeqNum()
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
        procSeqNum = base
        inputSeqNum = base

    with open(sys.argv[2], 'r') as fptr:
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
    print(f"Error: Query data file '{sys.argv[2]}' not found")
    exit(-1)

message_dict[inputSeqNum] = ''
inputSeqNum += 1
tStart = time.time()

with ThreadPoolExecutor(max_workers=NUM) as execPool:
    futures = []
    for i in range(NUM):
        futures.append(execPool.submit(worker_loop, i))
    for fut in futures:
        fut.result()

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
    if permanent_failures:
        print(f"permanent_failures_first_{len(permanent_failures)}:")
        for f in permanent_failures:
            print(
                f"  qCount={f['qCount']} worker={f['worker']} tid={f['tid']} "
                f"sqlstate={f['sqlstate']} stmt={f['stmt']} err={f['error']}"
            )

if permanent_fail_count > 0:
    sys.exit(1)
