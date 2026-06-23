#include "postgres.h"
#include "bcdb/worker.h"
#include "bcdb/shm_transaction.h"
#include "bcdb/shm_block.h"
#include "bcdb/utils/cJSON.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include <unistd.h>
#include <tcop/dest.h>
#include <utils/guc.h>
#include <tcop/tcopprot.h>
#include <pgstat.h>
#include <pg_trace.h>
#include <utils/palloc.h>
#include <utils/portal.h>
#include <utils/memutils.h>
#include <tcop/utility.h>
#include <utils/ps_status.h>
#include <miscadmin.h>
#include <utils/snapmgr.h>
#include <tcop/pquery.h>
#include <access/printtup.h>
#include <storage/ipc.h>
#include <executor/executor.h>
#include <assert.h>
#include <storage/predicate.h>
#include "bcdb/middleware.h"
#include "bcdb/globals.h"
#include "bcdb/bcdb_dsa.h"
#include "bcdb/shm_block.h"
#include "storage/condition_variable.h"
#include "port/atomics.h"
#include "pgstat.h"
#include "parser/analyze.h"
#include <unistd.h>
#include <errno.h>
#include "utils/builtins.h"   /* pg_strncasecmp */

/*
 * Silence ad-hoc stdout debug prints in deterministic execution.  Raw
 * backend printf() output can corrupt client protocol responses.
 */
#undef printf
#define printf(...) ((void)0)
#include <ctype.h>
#include <string.h>
#include "access/heapam.h"
#include "access/hash.h"
#include "catalog/pg_type.h"
#include "catalog/namespace.h"
#include <time.h>
#include <stdlib.h>
#include <stdio.h>
#include "access/xact.h"
#include "utils/resowner.h"

int current_block_id;
int total_num_restarts = 0; // worker specific -- move to shm for global
int t_sinceTx1 = 0;
FILE *fp;
struct timeval tx_start_time;

#define BCDB_APPLY_RETRY_MAX 64
#define BCDB_APPLY_RETRY_BACKOFF_MAX_US 256

/*
 * Hot-path debug I/O gate. /tmp/timestamps.txt open/fprintf/close per-tx is
 * costly under load; default OFF. Enable with BCDB_TRACE_TIMESTAMPS=1 when
 * investigating latency. Evaluated once per worker startup.
 */
static bool
bcdb_trace_timestamps_enabled(void)
{
    static int cached = -1;
    if (cached < 0)
    {
        const char *v = getenv("BCDB_TRACE_TIMESTAMPS");
        cached = (v && *v && *v != '0' && *v != 'n' && *v != 'N' && *v != 'f' && *v != 'F') ? 1 : 0;
    }
    return cached == 1;
}

/*
 * The DET completion-only read fast path deliberately skips materialising the
 * real row payload, but direct libpq/psycopg callers still need a valid
 * SELECT-shaped response. Emit a tiny placeholder row so the cheap path stays
 * protocol-compatible on the single-node benchmark surface.
 */
static void
bcdb_emit_completion_only_select_result(void)
{
    StringInfoData buf;
    static const char *colname = "bcdb_completion";
    static const char *value = "1";

    if (whereToSendOutput != DestRemote &&
        whereToSendOutput != DestRemoteExecute &&
        whereToSendOutput != DestRemoteSimple)
        return;

    pq_beginmessage(&buf, 'T');
    pq_sendint16(&buf, 1);
    pq_sendstring(&buf, colname);
    pq_sendint32(&buf, 0);
    pq_sendint16(&buf, 0);
    pq_sendint32(&buf, TEXTOID);
    pq_sendint16(&buf, -1);
    pq_sendint32(&buf, -1);
    pq_sendint16(&buf, 0);
    pq_endmessage(&buf);

    pq_beginmessage(&buf, 'D');
    pq_sendint16(&buf, 1);
    pq_sendcountedtext(&buf, value, strlen(value), false);
    pq_endmessage(&buf);

    EndCommand("SELECT 1", whereToSendOutput);
}

/*
 * Gate wait diagnostics are intentionally opt-in because they can be chatty
 * under high concurrency.
 */
static bool
bcdb_gate_debug_enabled(void)
{
    static int cached = -1;

    if (cached < 0)
    {
        const char *v = getenv("BCDB_GATE_DEBUG");
        cached = (v && *v && *v != '0' && *v != 'n' && *v != 'N' && *v != 'f' && *v != 'F') ? 1 : 0;
    }
    return cached == 1;
}

/*
 * Broad execution-flow diagnostics for pinpointing where a transaction stalls.
 * Keep this OFF by default; enable temporarily with BCDB_FLOW_DEBUG=1.
 */
static bool
bcdb_flow_debug_enabled(void)
{
    static int cached = -1;

    if (cached < 0)
    {
        const char *v = getenv("BCDB_FLOW_DEBUG");
        cached = (v && *v && *v != '0' && *v != 'n' && *v != 'N' && *v != 'f' && *v != 'F') ? 1 : 0;
    }
    return cached == 1;
}

/*
 * DT parse barrier: all transactions in a deterministic logical block finish
 * parse/plan/write-set capture before the ordered conflict/publish gate opens.
 *
 * Without this, tx N often waits in the serial gate for tx N-1 to finish
 * parsing, so the ordered gate absorbs parse stragglers.  The barrier keeps
 * deterministic order intact: it only moves the start of conflict/publish
 * later, after every tx's local simulation has produced its read/write sets.
 *
 * This barrier is only safe when every tx in the BCDB block can be resident in
 * a worker at the same time. If block_txs exceeds the live worker count, the
 * first wave of workers can all park at the barrier while later txs remain in
 * the queues, creating a hard deadlock. The runtime check below disables the
 * barrier for that shape; use BCDB_DT_PARSE_BARRIER=0 to disable it explicitly
 * for A/B runs.
 */
static bool
bcdb_dt_parse_barrier_enabled(void)
{
    static int cached = -1;

    if (cached < 0)
    {
        const char *v = getenv("BCDB_DT_PARSE_BARRIER");
        cached = !(v && *v && (*v == '0' || *v == 'n' || *v == 'N' || *v == 'f' || *v == 'F'));
    }
    return cached == 1;
}

/*
 * Upper bound for the fallback polling gate.  The distributed runner has
 * exported BCDB_POLL_MAX_US for a while; make it effective so A/B runs can
 * quantify ordered-handoff cost without recompiling.
 */
static int
bcdb_serial_gate_poll_max_us(void)
{
    static int cached = -1;

    if (cached < 0)
    {
        const char *v = getenv("BCDB_POLL_MAX_US");
        int parsed = 8;

        if (v != NULL && *v != '\0')
        {
            parsed = atoi(v);
            if (parsed < 1)
                parsed = 1;
            if (parsed > 64)
                parsed = 64;
        }
        cached = parsed;
    }
    return cached;
}

static bool
bcdb_dt_light_snapshot_enabled(void)
{
    static int cached = -1;

    if (cached < 0)
    {
        const char *v = getenv("BCDB_DT_LIGHT_SNAPSHOT");

        cached = (v != NULL && *v != '\0' &&
                  strcmp(v, "0") != 0 &&
                  strcmp(v, "false") != 0 &&
                  strcmp(v, "FALSE") != 0 &&
                  strcmp(v, "no") != 0 &&
                  strcmp(v, "NO") != 0);
    }
    return cached == 1;
}

static bool
bcdb_dt_skip_readonly_gate_enabled(void)
{
    static int cached = -1;

    if (cached < 0)
    {
        const char *v = getenv("BCDB_DT_SKIP_READONLY_GATE");

        cached = (v != NULL && *v != '\0' &&
                  strcmp(v, "0") != 0 &&
                  strcmp(v, "false") != 0 &&
                  strcmp(v, "FALSE") != 0 &&
                  strcmp(v, "no") != 0 &&
                  strcmp(v, "NO") != 0);
    }
    return cached == 1;
}

static bool
bcdb_dt_completion_only_skip_reads_enabled(void)
{
    return bcdb_dt_completion_only_skip_reads;
}

static bool
bcdb_block_return_actual_results_enabled(void)
{
    static int cached = -1;

    if (cached < 0)
    {
        const char *v = getenv("BCDB_BLOCK_RETURN_ACTUAL_RESULTS");

        cached = (v != NULL && *v != '\0' &&
                  strcmp(v, "0") != 0 &&
                  strcmp(v, "false") != 0 &&
                  strcmp(v, "FALSE") != 0 &&
                  strcmp(v, "no") != 0 &&
                  strcmp(v, "NO") != 0);
    }
    return cached == 1;
}

static bool
bcdb_query_is_select(const char *query)
{
    const char *s = query;

    if (s == NULL)
        return false;
    while (*s != '\0' && isspace((unsigned char)*s))
        s++;
    return strncasecmp(s, "select", 6) == 0;
}

#define BCDB_FLOW_LOG(...)                       \
    do                                           \
    {                                            \
        if (bcdb_flow_debug_enabled())           \
            ereport(LOG, (errmsg(__VA_ARGS__))); \
    } while (0)

/*
 * Per-tx phase tracer. When BCDB_PHASE_TRACE is set to a path prefix, each
 * worker backend appends CSV lines to "<prefix>.<pid>" capturing the time
 * spent in each commit-path phase plus a set of finer-grained deterministic
 * microphases. Zero overhead when disabled beyond a cached bool check.
 */
typedef enum
{
    BCDB_PHASE_PARSE_PLAN = 0, /* pg_parse_query + analyze + plan + CreatePortal */
    BCDB_PHASE_PORTAL_RUN,     /* PortalRun (plpgsql body, nested SELECTs) */
    BCDB_PHASE_GATE,
    BCDB_PHASE_CONFLICT,
    BCDB_PHASE_APPLY,
    BCDB_PHASE_FINISH,
    BCDB_PHASE_TOTAL,
    BCDB_PHASE_COUNT
} bcdb_phase_id;

static FILE *bcdb_ptrace_fp = NULL;
static struct timespec bcdb_ptrace_phase_start[BCDB_PHASE_COUNT];
static uint64 bcdb_ptrace_phase_us[BCDB_PHASE_COUNT];
static uint64 bcdb_ptrace_metric_us[BCDB_PTRACE_METRIC_COUNT];
static uint64 bcdb_ptrace_counter[BCDB_PTRACE_COUNTER_COUNT];
static uint64 bcdb_ptrace_pending_queue_pop_wait_us = 0;

bool bcdb_ptrace_enabled(void)
{
    static int cached = -1;
    if (cached < 0)
    {
        const char *v = getenv("BCDB_PHASE_TRACE");
        cached = (v && *v) ? 1 : 0;
    }
    return cached == 1;
}

uint64
bcdb_ptrace_now_us(void)
{
    struct timespec ts;

    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ((uint64)ts.tv_sec * 1000000ULL) + ((uint64)ts.tv_nsec / 1000ULL);
}

void bcdb_ptrace_add_us(bcdb_ptrace_metric_id metric, uint64 delta_us)
{
    if (!bcdb_ptrace_enabled())
        return;
    Assert(metric >= 0 && metric < BCDB_PTRACE_METRIC_COUNT);
    bcdb_ptrace_metric_us[metric] += delta_us;
}

void bcdb_ptrace_inc_counter(bcdb_ptrace_counter_id counter, uint64 delta)
{
    if (!bcdb_ptrace_enabled())
        return;
    Assert(counter >= 0 && counter < BCDB_PTRACE_COUNTER_COUNT);
    bcdb_ptrace_counter[counter] += delta;
}

void bcdb_ptrace_note_queue_pop_wait(uint64 wait_us)
{
    if (!bcdb_ptrace_enabled())
        return;
    bcdb_ptrace_pending_queue_pop_wait_us += wait_us;
}

static void
bcdb_ptrace_open(void)
{
    const char *prefix;
    char path[512];

    if (bcdb_ptrace_fp != NULL)
        return;

    prefix = getenv("BCDB_PHASE_TRACE");
    if (!prefix || !*prefix)
        return;

    snprintf(path, sizeof(path), "%s.%d", prefix, (int)getpid());
    bcdb_ptrace_fp = fopen(path, "a");
    if (bcdb_ptrace_fp == NULL)
        return;

    /* Line-buffered so flushes happen at each tx row without explicit fflush. */
    setvbuf(bcdb_ptrace_fp, NULL, _IOLBF, 4096);

    /* Header once per file open. */
    fprintf(bcdb_ptrace_fp,
            "tx_id,restarts,parse_plan_us,portal_run_us,gate_us,conflict_us,apply_us,finish_us,total_us,"
            "queue_pop_wait_us,parse_barrier_wait_us,serial_slot_wait_us,"
            "publish_ws_us,publish_hash_clear_us,prev_apply_wait_us,"
            "conflict_ws_us,conflict_rs_us,apply_insert_us,apply_update_us,apply_delete_us,"
            "apply_update_wait_us,apply_delete_wait_us,"
            "apply_delete_lookup_us,apply_merkle_prep_us,apply_merkle_update_us,"
            "finish_result_us,finish_prev_commit_wait_us,finish_publish_us,"
            "ws_publish_entries,ws_conflict_checks,rs_conflict_checks,"
            "apply_insert_count,apply_update_count,apply_delete_count,"
            "apply_update_tm_being_modified_count,apply_delete_tm_being_modified_count,"
            "apply_update_wait_incident_count,apply_delete_wait_incident_count,"
            "merkle_update_count,apply_retry_count,publish_hash_clear_count\n");
}

static inline uint64
bcdb_ptrace_delta_us(struct timespec *start, struct timespec *end)
{
    return ((uint64)(end->tv_sec - start->tv_sec) * 1000000ULL) +
           ((uint64)(end->tv_nsec - start->tv_nsec) / 1000ULL);
}

#define PTRACE_BEGIN(phase)                                                    \
    do                                                                         \
    {                                                                          \
        if (bcdb_ptrace_enabled())                                             \
            clock_gettime(CLOCK_MONOTONIC, &bcdb_ptrace_phase_start[(phase)]); \
    } while (0)

#define PTRACE_END(phase)                                                          \
    do                                                                             \
    {                                                                              \
        if (bcdb_ptrace_enabled())                                                 \
        {                                                                          \
            struct timespec _pt_end;                                               \
            clock_gettime(CLOCK_MONOTONIC, &_pt_end);                              \
            bcdb_ptrace_phase_us[(phase)] +=                                       \
                bcdb_ptrace_delta_us(&bcdb_ptrace_phase_start[(phase)], &_pt_end); \
        }                                                                          \
    } while (0)

static inline void
bcdb_ptrace_reset(void)
{
    if (!bcdb_ptrace_enabled())
        return;
    for (int i = 0; i < BCDB_PHASE_COUNT; i++)
        bcdb_ptrace_phase_us[i] = 0;
    for (int i = 0; i < BCDB_PTRACE_METRIC_COUNT; i++)
        bcdb_ptrace_metric_us[i] = 0;
    for (int i = 0; i < BCDB_PTRACE_COUNTER_COUNT; i++)
        bcdb_ptrace_counter[i] = 0;
    bcdb_ptrace_metric_us[BCDB_PTRACE_METRIC_QUEUE_POP_WAIT_US] =
        bcdb_ptrace_pending_queue_pop_wait_us;
    bcdb_ptrace_pending_queue_pop_wait_us = 0;
}

static void
bcdb_ptrace_emit(int tx_id, int restarts)
{
    if (!bcdb_ptrace_enabled())
        return;
    bcdb_ptrace_open();
    if (bcdb_ptrace_fp == NULL)
        return;
    fprintf(bcdb_ptrace_fp,
            "%d,%d,%llu,%llu,%llu,%llu,%llu,%llu,%llu,"
            "%llu,%llu,%llu,%llu,%llu,%llu,%llu,%llu,%llu,%llu,%llu,"
            "%llu,%llu,%llu,%llu,%llu,%llu,%llu,%llu,"
            "%llu,%llu,%llu,%llu,%llu,%llu,%llu,%llu,%llu,%llu,%llu,%llu,%llu\n",
            tx_id, restarts,
            (unsigned long long)bcdb_ptrace_phase_us[BCDB_PHASE_PARSE_PLAN],
            (unsigned long long)bcdb_ptrace_phase_us[BCDB_PHASE_PORTAL_RUN],
            (unsigned long long)bcdb_ptrace_phase_us[BCDB_PHASE_GATE],
            (unsigned long long)bcdb_ptrace_phase_us[BCDB_PHASE_CONFLICT],
            (unsigned long long)bcdb_ptrace_phase_us[BCDB_PHASE_APPLY],
            (unsigned long long)bcdb_ptrace_phase_us[BCDB_PHASE_FINISH],
            (unsigned long long)bcdb_ptrace_phase_us[BCDB_PHASE_TOTAL],
            (unsigned long long)bcdb_ptrace_metric_us[BCDB_PTRACE_METRIC_QUEUE_POP_WAIT_US],
            (unsigned long long)bcdb_ptrace_metric_us[BCDB_PTRACE_METRIC_PARSE_BARRIER_WAIT_US],
            (unsigned long long)bcdb_ptrace_metric_us[BCDB_PTRACE_METRIC_SERIAL_SLOT_WAIT_US],
            (unsigned long long)bcdb_ptrace_metric_us[BCDB_PTRACE_METRIC_PUBLISH_WS_US],
            (unsigned long long)bcdb_ptrace_metric_us[BCDB_PTRACE_METRIC_PUBLISH_HASH_CLEAR_US],
            (unsigned long long)bcdb_ptrace_metric_us[BCDB_PTRACE_METRIC_PRE_APPLY_WAIT_US],
            (unsigned long long)bcdb_ptrace_metric_us[BCDB_PTRACE_METRIC_CONFLICT_WS_US],
            (unsigned long long)bcdb_ptrace_metric_us[BCDB_PTRACE_METRIC_CONFLICT_RS_US],
            (unsigned long long)bcdb_ptrace_metric_us[BCDB_PTRACE_METRIC_APPLY_INSERT_US],
            (unsigned long long)bcdb_ptrace_metric_us[BCDB_PTRACE_METRIC_APPLY_UPDATE_US],
            (unsigned long long)bcdb_ptrace_metric_us[BCDB_PTRACE_METRIC_APPLY_DELETE_US],
            (unsigned long long)bcdb_ptrace_metric_us[BCDB_PTRACE_METRIC_APPLY_UPDATE_WAIT_US],
            (unsigned long long)bcdb_ptrace_metric_us[BCDB_PTRACE_METRIC_APPLY_DELETE_WAIT_US],
            (unsigned long long)bcdb_ptrace_metric_us[BCDB_PTRACE_METRIC_APPLY_DELETE_LOOKUP_US],
            (unsigned long long)bcdb_ptrace_metric_us[BCDB_PTRACE_METRIC_APPLY_MERKLE_PREP_US],
            (unsigned long long)bcdb_ptrace_metric_us[BCDB_PTRACE_METRIC_APPLY_MERKLE_UPDATE_US],
            (unsigned long long)bcdb_ptrace_metric_us[BCDB_PTRACE_METRIC_FINISH_RESULT_US],
            (unsigned long long)bcdb_ptrace_metric_us[BCDB_PTRACE_METRIC_FINISH_PREV_COMMIT_WAIT_US],
            (unsigned long long)bcdb_ptrace_metric_us[BCDB_PTRACE_METRIC_FINISH_PUBLISH_US],
            (unsigned long long)bcdb_ptrace_counter[BCDB_PTRACE_COUNTER_WS_PUBLISH_ENTRIES],
            (unsigned long long)bcdb_ptrace_counter[BCDB_PTRACE_COUNTER_WS_CONFLICT_CHECKS],
            (unsigned long long)bcdb_ptrace_counter[BCDB_PTRACE_COUNTER_RS_CONFLICT_CHECKS],
            (unsigned long long)bcdb_ptrace_counter[BCDB_PTRACE_COUNTER_APPLY_INSERT_COUNT],
            (unsigned long long)bcdb_ptrace_counter[BCDB_PTRACE_COUNTER_APPLY_UPDATE_COUNT],
            (unsigned long long)bcdb_ptrace_counter[BCDB_PTRACE_COUNTER_APPLY_DELETE_COUNT],
            (unsigned long long)bcdb_ptrace_counter[BCDB_PTRACE_COUNTER_APPLY_UPDATE_TM_BEING_MODIFIED_COUNT],
            (unsigned long long)bcdb_ptrace_counter[BCDB_PTRACE_COUNTER_APPLY_DELETE_TM_BEING_MODIFIED_COUNT],
            (unsigned long long)bcdb_ptrace_counter[BCDB_PTRACE_COUNTER_APPLY_UPDATE_WAIT_INCIDENT_COUNT],
            (unsigned long long)bcdb_ptrace_counter[BCDB_PTRACE_COUNTER_APPLY_DELETE_WAIT_INCIDENT_COUNT],
            (unsigned long long)bcdb_ptrace_counter[BCDB_PTRACE_COUNTER_MERKLE_UPDATE_COUNT],
            (unsigned long long)bcdb_ptrace_counter[BCDB_PTRACE_COUNTER_APPLY_RETRY_COUNT],
            (unsigned long long)bcdb_ptrace_counter[BCDB_PTRACE_COUNTER_PUBLISH_HASH_CLEAR_COUNT]);
}
MemoryContext bcdb_tx_context;
MemoryContext bcdb_worker_context;

CommandDest dest;
char completionTag[COMPLETION_TAG_BUFSIZE];

static void get_write_set(BCDBShmXact *tx, Snapshot snapshot);

static inline void
bcdb_cleanup_optim_write_list(BCDBShmXact *tx, bool drop_slots)
{
    OptimWriteEntry *optim_write_entry;

    if (tx == NULL)
        return;

    while ((optim_write_entry = SIMPLEQ_FIRST(&tx->optim_write_list)))
    {
        if (drop_slots && optim_write_entry->slot)
            ExecDropSingleTupleTableSlot(optim_write_entry->slot);
        SIMPLEQ_REMOVE_HEAD(&tx->optim_write_list, link);
    }
}

/*
 * During retry/error cleanup we may be handling partially-applied entries.
 * Do not drop slot pointers here; just drain the queue and let
 * MemoryContextReset(bcdb_tx_context) reclaim cloned slot memory.
 */
static inline void
bcdb_drain_optim_write_list(BCDBShmXact *tx)
{
    bcdb_cleanup_optim_write_list(tx, false);
}

/*
 * Lever D v2 apply stage.
 *
 * After publish_ws_tableDT + published_max advancement, prefer retrying only
 * the apply stage first. We run each apply attempt in a subtransaction so
 * partial writes rollback cleanly between attempts.
 */
static bool
bcdb_apply_optim_writes_with_retry(BCDBShmXact *tx,
                                   int *num_apply_retries,
                                   bool *nonretryable_error)
{
    int retries = 0;
    int backoff_us = 1;

    if (num_apply_retries)
        *num_apply_retries = 0;
    if (nonretryable_error)
        *nonretryable_error = false;

    for (;;)
    {
        MemoryContext old_context = CurrentMemoryContext;
        ResourceOwner old_owner = CurrentResourceOwner;
        bool apply_ok = false;
        bool apply_failed_unique = false;

        BeginInternalSubTransaction("bcdb_apply_retry");
        PG_TRY();
        {
            bcdb_reset_apply_error_flags();
            BCDB_FLOW_LOG("[BCDB_FLOW] apply_attempt_enter pid=%d txid=%d xid=%u attempt=%d",
                          getpid(),
                          tx ? (int)tx->tx_id : -1,
                          (unsigned int)(tx ? tx->xid : InvalidTransactionId),
                          retries + 1);
            apply_ok = apply_optim_writes();
            apply_failed_unique = (!apply_ok && bcdb_apply_had_unique_violation());
            if (apply_ok)
                ReleaseCurrentSubTransaction();
            else
                RollbackAndReleaseCurrentSubTransaction();

            BCDB_FLOW_LOG("[BCDB_FLOW] apply_attempt_exit pid=%d txid=%d xid=%u attempt=%d ok=%d",
                          getpid(),
                          tx ? (int)tx->tx_id : -1,
                          (unsigned int)(tx ? tx->xid : InvalidTransactionId),
                          retries + 1,
                          apply_ok ? 1 : 0);

            MemoryContextSwitchTo(old_context);
            CurrentResourceOwner = old_owner;
        }
        PG_CATCH();
        {
            MemoryContextSwitchTo(old_context);
            RollbackAndReleaseCurrentSubTransaction();
            MemoryContextSwitchTo(old_context);
            CurrentResourceOwner = old_owner;
            PG_RE_THROW();
        }
        PG_END_TRY();

        if (apply_ok)
        {
            if (num_apply_retries)
                *num_apply_retries = retries;
            return true;
        }

        retries++;
        if (retries > BCDB_APPLY_RETRY_MAX)
        {
            if (num_apply_retries)
                *num_apply_retries = retries;
            BCDB_FLOW_LOG("[BCDB_FLOW] apply_retry_exhausted pid=%d txid=%d xid=%u retries=%d",
                          getpid(),
                          tx ? (int)tx->tx_id : -1,
                          (unsigned int)(tx ? tx->xid : InvalidTransactionId),
                          retries);

            if (apply_failed_unique)
            {
                BCDB_FLOW_LOG("[BCDB_FLOW] apply_retry_exhausted_nonretryable_unique pid=%d txid=%d xid=%u retries=%d",
                              getpid(),
                              tx ? (int)tx->tx_id : -1,
                              (unsigned int)(tx ? tx->xid : InvalidTransactionId),
                              retries);
                if (nonretryable_error)
                    *nonretryable_error = true;
            }
            return false;
        }

        CHECK_FOR_INTERRUPTS();
        pg_usleep((long)backoff_us);
        if (backoff_us < BCDB_APPLY_RETRY_BACKOFF_MAX_US)
            backoff_us <<= 1;
    }
}

static inline const char *
skip_ws(const char *s)
{
    while (*s && isspace((unsigned char)*s))
        s++;
    return s;
}

static inline int
bcdb_runtime_workers(void)
{
    int workers = get_blksz();

    if (workers <= 0)
        workers = bcdb_worker_count;
    if (workers <= 0)
        workers = 1;
    return workers;
}

static inline int
bcdb_runtime_result_slots(void)
{
    return bcdb_get_runtime_result_ring_slots();
}

static inline int
bcdb_result_slot_for_txid(BCTxID tx_id)
{
    int slots = bcdb_runtime_result_slots();
    int idx;

    if (slots <= 0)
        slots = 1;
    idx = tx_id % slots;
    if (idx < 0)
        idx += slots;
    return idx;
}

/*
 * bcdb_wait_for_slot_consumable
 *
 * Overwrite-protection gate: spins until result_consumed_txid[slot] matches
 * the previous occupant's tx_id, ensuring middleware (or the worker's own
 * self-mark in direct mode) has consumed the old result before we overwrite.
 * Logs a warning every 5 s if the wait is unexpectedly long.
 */
static inline void
bcdb_wait_for_slot_consumable(BCBlock *block, BCTxID tx_id, int slot)
{
    BCTxID old_txid = __atomic_load_n(&block->result_committed_txid[slot],
                                      __ATOMIC_ACQUIRE);

    /* Slot unoccupied (-1) or we are the initial writer — no wait needed */
    if (old_txid < 0 || old_txid == tx_id)
        return;

    {
        int spins = 0;
        int poll_us = 0;
        uint64 wait_start = bcdb_get_time();
        uint64 next_warn = wait_start + 5000000; /* 5 s */

        for (;;)
        {
            BCTxID consumed = __atomic_load_n(&block->result_consumed_txid[slot],
                                              __ATOMIC_ACQUIRE);
            if (consumed == old_txid)
                return;

            /* Guard against slot advancing beyond us (shouldn't happen in
             * single-producer-per-slot model, but be defensive). */
            {
                BCTxID cur = __atomic_load_n(&block->result_committed_txid[slot],
                                             __ATOMIC_ACQUIRE);
                if (cur < 0 || cur == tx_id)
                    return;
            }

            {
                uint64 now = bcdb_get_time();
                if (now >= next_warn)
                {
                    ereport(LOG,
                            (errmsg("[BCDB_OVERWRITE] slot=%d old_txid=%d consumed=%d my_txid=%d waited_us=%lu",
                                    slot, (int)old_txid, (int)consumed,
                                    (int)tx_id,
                                    (unsigned long)(now - wait_start))));
                    next_warn = now + 5000000;
                }
            }

            CHECK_FOR_INTERRUPTS();

            if (spins < 64)
            {
                spins++;
                pg_spin_delay();
            }
            else
            {
                if (poll_us == 0)
                    poll_us = 1;
                else if (poll_us < 64)
                    poll_us *= 2;
                pg_usleep((long)poll_us);
            }
        }
    }
}

static inline void
bcdb_publish_error_result(BCBlock *block, BCDBShmXact *tx, const char *sqlstate)
{
    int mem_txid;

    if (block == NULL || tx == NULL)
        return;

    mem_txid = bcdb_result_slot_for_txid(tx->tx_id);
    bcdb_wait_for_slot_consumable(block, tx->tx_id, mem_txid);
    memset(block->result[mem_txid], 0, sizeof(block->result[mem_txid]));
    snprintf(block->result[mem_txid], sizeof(block->result[mem_txid]),
             "\n\t*%s* ERROR %s\n",
             tx->hash,
             (sqlstate != NULL && sqlstate[0] != '\0') ? sqlstate : "XX000");

    pg_write_barrier();
    __atomic_store_n(&block->result_commit_xid[mem_txid],
                     tx->xid, __ATOMIC_RELEASE);
    __atomic_store_n(&block->result_committed_txid[mem_txid],
                     tx->tx_id, __ATOMIC_RELEASE);
}

static inline void
bcdb_wait_for_dt_parse_barrier(BCDBShmXact *tx, bool *barrier_done)
{
    BCBlock *committed_block;
    int32 num_ready;
    uint64 wait_start_us = 0;

    if (barrier_done != NULL && *barrier_done)
        return;
    if (!bcdb_dt_parse_barrier_enabled())
    {
        if (barrier_done != NULL)
            *barrier_done = true;
        return;
    }

    committed_block = get_block_by_id(tx->block_id_committed, false);
    if (committed_block == NULL || committed_block->num_tx <= 1)
    {
        if (barrier_done != NULL)
            *barrier_done = true;
        return;
    }

    if (committed_block->num_tx > bcdb_runtime_workers())
    {
        if (committed_block->txs[0] == tx)
            ereport(LOG,
                    (errmsg("[BCDB_BARRIER] parse barrier skipped for block_id=%d block_txs=%d workers=%d",
                            committed_block->id,
                            committed_block->num_tx,
                            bcdb_runtime_workers())));
        if (barrier_done != NULL)
            *barrier_done = true;
        return;
    }

    wait_start_us = bcdb_ptrace_timer_start();
    num_ready = __sync_add_and_fetch(&committed_block->num_ready, 1);
    if (num_ready >= committed_block->num_tx)
        ConditionVariableBroadcast(&committed_block->cond);

    WaitCondition(&committed_block->cond,
                  committed_block->num_ready >= committed_block->num_tx);
    bcdb_ptrace_timer_stop(BCDB_PTRACE_METRIC_PARSE_BARRIER_WAIT_US,
                           wait_start_us);

    if (barrier_done != NULL)
        *barrier_done = true;
}

static inline void
bcdb_wait_for_serial_slot(BCDBShmXact *tx, BCBlock *block)
{
    /*
     * Lever D publish gate: wait until `published_max_tx_id + 1 >= tx->tx_id`.
     *
     * This replaces the old "wait until last_committed_tx_id + 1 == my_tx_id"
     * gate, which forced every tx to wait for ALL earlier tx's to finish
     * apply + finish_xact_command. Lever D v2 advances published_max right
     * after publish_ws_tableDT, so the gate serializes only the
     * conflict_check + publish_ws critical section.
     *
     * In condvar mode, set_published_max_txid() signals the successor tx's
     * per-slot condition variable.  This removes polling handoff delay without
     * waking the whole worker pool; the correctness predicate remains the
     * published_max_tx_id check below.
     */
    int poll_us = 0;
    int poll_max_us = bcdb_serial_gate_poll_max_us();
    int spins = 0;
    uint64 wait_start_us = bcdb_get_time();
    uint64 trace_start_us = bcdb_ptrace_timer_start();
    uint64 next_log_us = 0;
    uint64 next_warn_us = wait_start_us + 5000000; /* 5 s always-on watchdog */
    bool gate_debug = bcdb_gate_debug_enabled();

    if (gate_debug)
        next_log_us = wait_start_us + 1000000;

    Assert(block != NULL);

    /*
     * Fail-fast on a common deterministic-mode misconfiguration.
     *
     * On a fresh server, the Lever-D serial gate expects tx_ids to start at 0
     * (published_max=-1, last_committed=-1). If the client starts at a higher
     * txid (e.g., DET_START_SEQ set after a restart), the gate would otherwise
     * wait forever because no predecessor can ever advance the watermarks.
     *
     * HOWEVER: multi-threaded clients submit txid=0..N in parallel, and txid>0
     * can legitimately arrive before txid=0. We must not error in that normal
     * start-of-run race.
     *
     * Strategy: when the server looks fresh and we see txid>0, briefly wait
     * for txid=0 to appear in the tx_pool. If txid=0 is in-flight, keep
     * waiting (it may simply be slow). If txid=0 never appears, error with a
     * clear hint instead of hanging forever.
     */
    uint64 fresh_guard_start_us = 0;
    uint64 next_tx0_check_us = 0;
    const uint64 fresh_guard_grace_us = 2000000; /* 2s */
    const uint64 tx0_check_period_us = 1000;     /* 1ms */
    {
        BCTxID published = get_published_max_txid(tx);
        BCTxID committed = get_last_committed_txid(tx);

        if (published < 0 && committed < 0 && tx->tx_id > 0)
            fresh_guard_start_us = wait_start_us;
    }

    for (;;)
    {
        if ((get_published_max_txid(tx) + 1) >= tx->tx_id)
            break;

        if (fresh_guard_start_us != 0)
        {
            uint64 now_us = bcdb_get_time();
            BCTxID published = get_published_max_txid(tx);
            BCTxID committed = get_last_committed_txid(tx);

            if (published >= 0 || committed >= 0)
            {
                /* txid=0 (or a predecessor) advanced watermarks; normal gating applies. */
                fresh_guard_start_us = 0;
            }
            else
            {
                /* Check whether txid=0 exists in-flight (normal parallel startup). */
                if (now_us >= next_tx0_check_us)
                {
                    char tx0_key[TX_HASH_SIZE];
                    BCDBShmXact *tx0;

                    MemSet(tx0_key, 0, sizeof(tx0_key));
                    memcpy(tx0_key, "00000000", 8);
                    tx0 = get_tx_by_hash(tx0_key);
                    if (tx0 != NULL)
                    {
                        /* txid=0 exists; do not treat this as misconfiguration. */
                        fresh_guard_start_us = 0;
                    }

                    next_tx0_check_us = now_us + tx0_check_period_us;
                }

                if (fresh_guard_start_us != 0 && (now_us - fresh_guard_start_us) >= fresh_guard_grace_us)
                {
                    ereport(ERROR,
                            (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                             errmsg("BCDB deterministic serial gate cannot start at txid=%d on a fresh server (published_max=%d last_committed=%d)",
                                    (int)tx->tx_id, (int)published, (int)committed),
                             errhint("This usually means deterministic tx ids did not start at 0 after a restart (e.g., DET_START_SEQ>0). Unset DET_START_SEQ (or set it to 0) for fresh-server runs. If you intend to resume at a higher tx id, keep the server running so published_max advances past the resume point.")));
                }
            }
        }

        {
            uint64 now_us = bcdb_get_time();

            /* Always-on hang watchdog: every 5 s regardless of BCDB_GATE_DEBUG */
            if (now_us >= next_warn_us)
            {
                BCTxID published = get_published_max_txid(tx);
                BCTxID committed = get_last_committed_txid(tx);

                ereport(LOG,
                        (errmsg("[BCDB_HANG] serial_gate_stuck pid=%d txid=%d published_max=%d last_committed=%d waited_us=%lu spins=%d poll_us=%d",
                                (int)getpid(), (int)tx->tx_id, (int)published,
                                (int)committed, (unsigned long)(now_us - wait_start_us),
                                spins, poll_us)));
                next_warn_us = now_us + 5000000;
            }

            /* Opt-in per-second trace (BCDB_GATE_DEBUG=1) */
            if (gate_debug && now_us >= next_log_us)
            {
                BCTxID published = get_published_max_txid(tx);
                BCTxID committed = get_last_committed_txid(tx);

                ereport(LOG,
                        (errmsg("[BCDB_GATE] serial_gate_wait pid=%d txid=%d published_max=%d last_committed=%d waited_us=%lu spins=%d poll_us=%d mode=%d",
                                (int)getpid(), (int)tx->tx_id, (int)published,
                                (int)committed, (unsigned long)(now_us - wait_start_us),
                                spins, poll_us, bcdb_serial_gate_mode)));
                next_log_us = now_us + 1000000;
            }
        }

        CHECK_FOR_INTERRUPTS();

        if (spins < 64)
        {
            /* Hot neighbour finishing any moment — spin without syscall. */
            spins++;
            pg_spin_delay();
        }
        else
        {
            if (bcdb_serial_gate_mode == BCDB_SERIAL_GATE_MODE_CONDVAR)
            {
                ConditionVariable *cv;
                int wake_slot = (int)(tx->tx_id % MAX_TX_PER_BLOCK);

                if (wake_slot < 0)
                    wake_slot += MAX_TX_PER_BLOCK;
                cv = &block->done_conds[wake_slot];

                ConditionVariablePrepareToSleep(cv);
                if ((get_published_max_txid(tx) + 1) < tx->tx_id)
                    ConditionVariableSleep(cv, WAIT_EVENT_BLOCK_COMMIT);
                ConditionVariableCancelSleep();
            }
            else
            {
                if (poll_us == 0)
                    poll_us = 1;
                else if (poll_us < poll_max_us)
                    poll_us *= 2;
                if (poll_us > poll_max_us)
                    poll_us = poll_max_us;
                pg_usleep((long)poll_us);
            }
        }
    }

    if (gate_debug)
    {
        uint64 total_wait_us = bcdb_get_time() - wait_start_us;

        if (total_wait_us >= 100000)
            ereport(LOG,
                    (errmsg("[BCDB_GATE] serial_gate_done pid=%d txid=%d waited_us=%lu",
                            (int)getpid(), (int)tx->tx_id, (unsigned long)total_wait_us)));
    }
    bcdb_ptrace_timer_stop(BCDB_PTRACE_METRIC_SERIAL_SLOT_WAIT_US,
                           trace_start_us);
}

/*
 * Wait until tx_id-1 is durably committed.
 *
 * Lever D v2 allows publish to run ahead, but applying writes out of commit
 * order can create lock/liveness cycles (a higher tx holds tuple locks while
 * waiting to commit, starving the lower tx it depends on).  We therefore
 * serialize write-apply by commit order while keeping early publish overlap.
 *
 * HANG DEBUG: two tiers of logging:
 *  1. Always-on watchdog at 5 s: fires unconditionally — visible without
 *     any env-var setup.  Shows which predecessor is still missing and the
 *     current published_max watermark, which helps distinguish "predecessor
 *     not yet in apply" from "predecessor stuck in serial gate".
 *  2. Opt-in per-second trace at 1 s: BCDB_GATE_DEBUG=1 (existing).
 */
static inline void
bcdb_wait_for_prev_committed(BCDBShmXact *tx)
{
    /* tx_id 0 has no predecessor; tx_id-1 would underflow to UINT32_MAX. */
    if (tx->tx_id == 0)
        return;

    int spins = 0;
    int poll_us = 0;
    uint64 wait_start_us = bcdb_get_time();
    uint64 next_log_us = 0;
    uint64 next_warn_us = wait_start_us + 5000000; /* 5 s always-on */
    bool gate_debug = bcdb_gate_debug_enabled();

    if (gate_debug)
        next_log_us = wait_start_us + 1000000;

    while (get_last_committed_txid(tx) < tx->tx_id - 1)
    {
        uint64 now_us = bcdb_get_time();

        /* Always-on hang watchdog: every 5 s regardless of BCDB_GATE_DEBUG */
        if (now_us >= next_warn_us)
        {
            BCTxID committed = get_last_committed_txid(tx);
            BCTxID published = get_published_max_txid(tx);

            ereport(LOG,
                    (errmsg("[BCDB_HANG] prev_commit_stuck pid=%d txid=%d need_committed=%d last_committed=%d published_max=%d waited_us=%lu spins=%d poll_us=%d",
                            (int)getpid(), (int)tx->tx_id, (int)(tx->tx_id - 1),
                            (int)committed, (int)published,
                            (unsigned long)(now_us - wait_start_us),
                            spins, poll_us)));
            next_warn_us = now_us + 5000000;
        }

        /* Opt-in per-second trace (BCDB_GATE_DEBUG=1) */
        if (gate_debug && now_us >= next_log_us)
        {
            BCTxID committed = get_last_committed_txid(tx);

            ereport(LOG,
                    (errmsg("[BCDB_GATE] prev_commit_wait pid=%d txid=%d prev_needed=%d last_committed=%d waited_us=%lu spins=%d poll_us=%d",
                            (int)getpid(), (int)tx->tx_id, (int)(tx->tx_id - 1),
                            (int)committed, (unsigned long)(now_us - wait_start_us),
                            spins, poll_us)));
            next_log_us = now_us + 1000000;
        }

        CHECK_FOR_INTERRUPTS();
        if (spins < 128)
        {
            spins++;
            pg_spin_delay();
        }
        else
        {
            if (poll_us == 0)
                poll_us = 1;
            else if (poll_us < 64)
                poll_us *= 2;
            pg_usleep((long)poll_us);
        }
    }

    if (gate_debug)
    {
        uint64 total_wait_us = bcdb_get_time() - wait_start_us;

        if (total_wait_us >= 100000)
            ereport(LOG,
                    (errmsg("[BCDB_GATE] prev_commit_done pid=%d txid=%d waited_us=%lu",
                            (int)getpid(), (int)tx->tx_id, (unsigned long)total_wait_us)));
    }
}

static inline void
bcdb_wait_for_target_committed(BCDBShmXact *tx, BCTxID target_txid)
{
    int spins = 0;
    int poll_us = 0;
    uint64 wait_start_us;
    uint64 next_log_us = 0;
    uint64 next_warn_us;
    bool gate_debug;

    if (target_txid < 0)
        return;
    if (get_last_committed_txid(tx) >= target_txid)
        return;

    wait_start_us = bcdb_get_time();
    next_warn_us = wait_start_us + 5000000;
    gate_debug = bcdb_gate_debug_enabled();

    if (gate_debug)
        next_log_us = wait_start_us + 1000000;

    while (get_last_committed_txid(tx) < target_txid)
    {
        uint64 now_us = bcdb_get_time();

        if (now_us >= next_warn_us)
        {
            BCTxID committed = get_last_committed_txid(tx);
            BCTxID published = get_published_max_txid(tx);

            ereport(LOG,
                    (errmsg("[BCDB_HANG] conflict_commit_stuck pid=%d txid=%d wait_for=%d last_committed=%d published_max=%d waited_us=%lu spins=%d poll_us=%d",
                            (int)getpid(), (int)tx->tx_id, (int)target_txid,
                            (int)committed, (int)published,
                            (unsigned long)(now_us - wait_start_us),
                            spins, poll_us)));
            next_warn_us = now_us + 5000000;
        }

        if (gate_debug && now_us >= next_log_us)
        {
            BCTxID committed = get_last_committed_txid(tx);

            ereport(LOG,
                    (errmsg("[BCDB_GATE] conflict_commit_wait pid=%d txid=%d wait_for=%d last_committed=%d waited_us=%lu spins=%d poll_us=%d",
                            (int)getpid(), (int)tx->tx_id, (int)target_txid,
                            (int)committed,
                            (unsigned long)(now_us - wait_start_us),
                            spins, poll_us)));
            next_log_us = now_us + 1000000;
        }

        CHECK_FOR_INTERRUPTS();
        if (spins < 128)
        {
            spins++;
            pg_spin_delay();
        }
        else
        {
            if (poll_us == 0)
                poll_us = 1;
            else if (poll_us < 64)
                poll_us *= 2;
            pg_usleep((long)poll_us);
        }
    }

    if (gate_debug)
    {
        uint64 total_wait_us = bcdb_get_time() - wait_start_us;

        if (total_wait_us >= 100000)
            ereport(LOG,
                    (errmsg("[BCDB_GATE] conflict_commit_done pid=%d txid=%d wait_for=%d waited_us=%lu",
                            (int)getpid(), (int)tx->tx_id, (int)target_txid,
                            (unsigned long)total_wait_us)));
    }
}

static bool
completiontag_is_delete_0(const char *tag)
{
    const char *p = skip_ws(tag);

    if (pg_strncasecmp(p, "DELETE", 6) != 0)
        return false;
    p += 6;
    p = skip_ws(p);
    if (*p == '\0')
        return false;

    errno = 0;
    char *endptr = NULL;
    long n = strtol(p, &endptr, 10);
    if (endptr == p || errno != 0)
        return false;

    return (n == 0);
}

static const char *
find_token_ci(const char *haystack, const char *needle)
{
    size_t nlen = strlen(needle);

    for (const char *p = haystack; *p; p++)
    {
        if (pg_strncasecmp(p, needle, nlen) == 0)
            return p;
    }

    return NULL;
}

static bool
parse_ycsb_key_eq_int32(const char *sql, int32 *key_out)
{
    const char *p;

    p = find_token_ci(sql, "YCSB_KEY");
    if (p == NULL)
        return false;
    p += strlen("YCSB_KEY");

    p = skip_ws(p);
    if (*p != '=')
        return false;
    p++;

    p = skip_ws(p);
    if (*p == '\0')
        return false;

    errno = 0;
    char *endptr = NULL;
    long v = strtol(p, &endptr, 10);
    if (endptr == p || errno != 0)
        return false;
    if (v < PG_INT32_MIN || v > PG_INT32_MAX)
        return false;

    *key_out = (int32)v;
    return true;
}

static inline bool
is_ident_char(unsigned char c)
{
    return isalnum(c) || c == '_';
}

// delete from tablename, here extract from and make sure it is keyword
static const char *
find_keyword_ci(const char *haystack, const char *needle)
{
    size_t nlen = strlen(needle);

    for (const char *p = haystack; *p; p++)
    {
        if (pg_strncasecmp(p, needle, nlen) == 0)
        {
            unsigned char before = (p == haystack) ? 0 : (unsigned char)p[-1];
            unsigned char after = (unsigned char)p[nlen];

            if ((p == haystack || !is_ident_char(before)) &&
                (after == '\0' || !is_ident_char(after)))
                return p;
        }
    }

    return NULL;
}

static bool
parse_quoted_ident(const char **sp, char *out, Size outsz)
{
    const char *s = *sp;
    Size n = 0;

    if (*s != '"')
        return false;
    s++;

    while (*s)
    {
        if (*s == '"')
        {
            if (s[1] == '"')
            {
                if (n + 1 >= outsz)
                    return false;
                out[n++] = '"';
                s += 2;
                continue;
            }

            s++;
            out[n] = '\0';
            *sp = s;
            return (n > 0);
        }

        if (n + 1 >= outsz)
            return false;
        out[n++] = *s++;
    }

    return false;
}

static bool
parse_delete_from_relname(const char *sql, char *relname_out, Size relname_out_sz)
{
    const char *p;
    const char *from;

    p = skip_ws(sql);
    if (pg_strncasecmp(p, "DELETE", 6) != 0)
        return false;

    from = find_keyword_ci(p + 6, "FROM");
    if (from == NULL)
        return false;

    p = skip_ws(from + 4);

    if (pg_strncasecmp(p, "ONLY", 4) == 0 && isspace((unsigned char)p[4]))
        p = skip_ws(p + 4);

    if (*p == '\0')
        return false;

    if (*p == '"')
    {
        char first[NAMEDATALEN];

        if (!parse_quoted_ident(&p, first, sizeof(first)))
            return false;

        p = skip_ws(p);
        if (*p == '.')
        {
            p = skip_ws(p + 1);

            if (*p == '"')
            {
                char second[NAMEDATALEN];

                if (!parse_quoted_ident(&p, second, sizeof(second)))
                    return false;
                strlcpy(relname_out, second, relname_out_sz);
                return true;
            }
            else
            {
                const char *start = p;
                size_t len;

                while (*p &&
                       !isspace((unsigned char)*p) &&
                       *p != ';' && *p != ',' && *p != '(')
                    p++;
                len = (size_t)(p - start);
                if (len == 0 || len >= relname_out_sz)
                    return false;
                memcpy(relname_out, start, len);
                relname_out[len] = '\0';
                return true;
            }
        }

        strlcpy(relname_out, first, relname_out_sz);
        return true;
    }
    else
    {
        const char *start = p;
        size_t len;
        char token[NAMEDATALEN * 2];
        char *last_dot;
        const char *rel;

        while (*p &&
               !isspace((unsigned char)*p) &&
               *p != ';' && *p != ',' && *p != '(')
            p++;
        len = (size_t)(p - start);
        if (len == 0 || len >= sizeof(token))
            return false;

        memcpy(token, start, len);
        token[len] = '\0';

        last_dot = strrchr(token, '.');
        rel = last_dot ? last_dot + 1 : token;
        if (*rel == '\0')
            return false;

        strlcpy(relname_out, rel, relname_out_sz);
        return true;
    }
}

static void
bcdb_maybe_enqueue_deferred_delete0_by_key(BCDBShmXact *tx)
{
    const char *sql;
    int32 keyval;
    Oid relOid;
    char relname[NAMEDATALEN];
    uint32 h;
    PREDICATELOCKTARGETTAG tag;

    if (tx == NULL)
        return;

    sql = skip_ws(tx->sql);
    if (pg_strncasecmp(sql, "DELETE", 6) != 0)
        return;

    if (!completiontag_is_delete_0(completionTag))
        return;

    if (!parse_ycsb_key_eq_int32(sql, &keyval))
        return;

    if (!parse_delete_from_relname(sql, relname, sizeof(relname)))
        return;

    relOid = RelnameGetRelid(relname);
    if (!OidIsValid(relOid))
        return;

    h = hash_any((unsigned char *)&keyval, sizeof(int32));
    SET_PREDICATELOCKTARGETTAG_TUPLE(tag, 0, relOid,
                                     (BlockNumber)(h >> 16),
                                     (OffsetNumber)((h & 0xFFFF) | 1));
    ws_table_reserveDT(&tag);
    store_optim_delete_by_key(relOid, keyval, GetCurrentCommandId(true));
}

BCBlockID
GetCurrentTxBlockId(void)
{
    if (activeTx == NULL)
        return 0;
    return activeTx->block_id_committed;
}

BCBlockID
GetCurrentTxBlockIdSnapshot(void)
{
    if (activeTx == NULL)
        return 0;
    return activeTx->block_id_snapshot;
}

bool bcdb_worker_init(void)
{
    MemoryContext old_context;
    if (bcdb_worker_context != NULL && bcdb_tx_context != NULL)
        return true;
    ereport(DEBUG3,
            (errmsg("[ZL] worker(%d) initializing", getpid())));
    bcdb_worker_context =
        AllocSetContextCreate(TopMemoryContext,
                              "bcdb worker memory context",
                              ALLOCSET_DEFAULT_SIZES);
    bcdb_tx_context =
        AllocSetContextCreate(TopMemoryContext,
                              "bcdb transaction memory context",
                              ALLOCSET_DEFAULT_SIZES);
    old_context = MemoryContextSwitchTo(bcdb_worker_context);
    MemoryContextSwitchTo(old_context);
    pid = getpid();
    srand(pid);
    if (OEP_mode)
        DEBUGNOCHECK("[ZL] worker runing is OEP mode");
    return true;
}

void get_write_set(BCDBShmXact *tx, Snapshot snapshot)
{
    const char *query_string = tx->sql;

    MemoryContext oldcontext;
    List *parsetree_list;
    ListCell *parsetree_item;
    bool save_log_statement_stats = log_statement_stats;
    bool use_implicit_block;
    const char *commandTag;
    MemoryContext per_parsetree_context = NULL;
    List *querytree_list,
        *plantree_list;
    Portal portal;
    DestReceiver *receiver;
    int16 format;
    RawStmt *parsetree;
    bool snapshot_set = false;
    dest = whereToSendOutput;

    /*
     * Report query to various monitoring facilities.
     */
    // printf("safeDbg pid %d %s : %s: %d \n",
    // getpid(), __FILE__, __FUNCTION__, __LINE__ );
    debug_query_string = query_string;
    activeTx->start_parsing_time = bcdb_get_time();
    pgstat_report_activity(STATE_RUNNING, query_string);

    if (bcdb_flow_debug_enabled())
        ereport(LOG,
                (errmsg("[BCDB_FLOW] get_write_set_begin pid=%d txid=%d xid=%u sql=%.120s",
                        getpid(), (int)tx->tx_id, (unsigned int)tx->xid,
                        query_string ? query_string : "<null>")));

    TRACE_POSTGRESQL_QUERY_START(query_string);

    /*
     * We use save_log_statement_stats so ShowUsage doesn't report incorrect
     * results because ResetUsage wasn't called.
     */
    if (save_log_statement_stats)
        ResetUsage();

    /*
     * Start up a transaction command.  All queries generated by the
     * query_string will be in this same command block, *unless* we find a
     * BEGIN/COMMIT/ABORT statement; we have to force a new xact command after
     * one of those, else bad things will happen in xact.c. (Note that this
     * will normally change current memory context.)
     */
    //    start_new_tx_state();
    //    start_xact_command();
    //    if (snapshot == NULL) {
    //        snapshot = GetTransactionSnapshot();
    //    }
    //    PushActiveSnapshot(snapshot);

    /*
     * Zap any pre-existing unnamed statement.  (While not strictly necessary,
     * it seems best to define simple-Query mode as if it used the unnamed
     * statement and portal; this ensures we recover any storage used by prior
     * unnamed operations.)
     */
    drop_unnamed_stmt();

    /*
     * Switch to appropriate context for constructing parsetrees.
     */
    oldcontext = MemoryContextSwitchTo(MessageContext);

    /*
     * Do basic parsing of the query or queries (this should be safe even if
     * we are in aborted transaction state!)
     */
    parsetree_list = pg_parse_query(query_string);

    /* Log immediately if dictated by log_statement */
    if (check_log_statement(parsetree_list))
    {
        ereport(LOG,
                (errmsg("statement: %s", query_string),
                 errhidestmt(true),
                 errdetail_execute(parsetree_list)));
    }

    /*
     * Switch back to transaction context to enter the loop.
     */
    MemoryContextSwitchTo(oldcontext);

    /*
     * For historical reasons, if multiple SQL statements are given in a
     * single "simple Query" message, we execute them as a single transaction,
     * unless explicit transaction control commands are included to make
     * portions of the list be separate transactions.  To represent this
     * behavior properly in the transaction machinery, we use an "implicit"
     * transaction block.
     */
    use_implicit_block = (list_length(parsetree_list) > 1);

    /*
     * Run through the raw parsetree(s) and process each one.
     */
    assert(parsetree_list->length == 1);
    parsetree_item = parsetree_list->elements;
    parsetree = lfirst_node(RawStmt, parsetree_item);

    // chris: snapshot is fixed @ the beginning

    /*
     * Get the command name for use in status display (it also becomes the
     * default completion tag, down inside PortalRun).  Set ps_status and
     * do any special start-of-SQL-command processing needed by the
     * destination.
     */
    commandTag = CreateCommandTag(parsetree->stmt);

    set_ps_display(commandTag, false);

    BeginCommand(commandTag, dest);

    /*
     * If we are in an aborted transaction, reject all commands except
     * COMMIT/ABORT.  It is important that this test occur before we try
     * to do parse analysis, rewrite, or planning, since all those phases
     * try to do database accesses, which may fail in abort state. (It
     * might be safe to allow some additional utility commands in this
     * state, but not many...)
     */
    //        if (IsAbortedTransactionBlockState() &&
    //            !IsTransactionExitStmt(parsetree->stmt))
    //            ereport(ERROR,
    //                    (errcode(ERRCODE_IN_FAILED_SQL_TRANSACTION),
    //                            errmsg("current transaction is aborted, "
    //                                   "commands ignored until end of transaction block"),
    //                            errdetail_abort()));

    /* Make sure we are in a transaction command */
    //        start_xact_command();

    /*
     * If using an implicit transaction block, and we're not already in a
     * transaction block, start an implicit block to force this statement
     * to be grouped together with any following ones.  (We must do this
     * each time through the loop; otherwise, a COMMIT/ROLLBACK in the
     * list would cause later statements to not be grouped.)
     */
    if (use_implicit_block)
        BeginImplicitTransactionBlock();

    /* If we got a cancel signal in parsing or prior command, quit */
    CHECK_FOR_INTERRUPTS();

    //        /*
    //         * Set up a snapshot if parse analysis/planning will need one.
    //         */
    if (analyze_requires_snapshot(parsetree))
    {
        PushActiveSnapshot(snapshot);
        snapshot_set = true;
    }

    /*
     * OK to analyze, rewrite, and plan this query.
     *
     * Switch to appropriate context for constructing query and plan trees
     * (these can't be in the transaction context, as that will get reset
     * when the command is COMMIT/ROLLBACK).  If we have multiple
     * parsetrees, we use a separate context for each one, so that we can
     * free that memory before moving on to the next one.  But for the
     * last (or only) parsetree, just use MessageContext, which will be
     * reset shortly after completion anyway.  In event of an error, the
     * per_parsetree_context will be deleted when MessageContext is reset.
     */
    if (lnext(parsetree_list, parsetree_item) != NULL)
    {
        per_parsetree_context =
            AllocSetContextCreate(MessageContext,
                                  "per-parsetree message context",
                                  ALLOCSET_DEFAULT_SIZES);
        oldcontext = MemoryContextSwitchTo(per_parsetree_context);
    }
    else
        oldcontext = MemoryContextSwitchTo(MessageContext);

    querytree_list = pg_analyze_and_rewrite(parsetree, query_string,
                                            NULL, 0, NULL);

    plantree_list = pg_plan_queries(querytree_list,
                                    CURSOR_OPT_PARALLEL_OK, NULL);

    /* Done with the snapshot used for parsing/planning */
    if (snapshot_set)
        PopActiveSnapshot();

    /* If we got a cancel signal in analysis or planning, quit */
    CHECK_FOR_INTERRUPTS();

    /*
     * Create unnamed portal to run the query or queries in. If there
     * already is one, silently drop it.
     */
    portal = CreatePortal("", true, true);
    /* Don't display the portal in pg_cursors */
    portal->visible = false;

    /*
     * We don't have to copy anything into the portal, because everything
     * we are passing here is in MessageContext or the
     * per_parsetree_context, and so will outlive the portal anyway.
     */
    PortalDefineQuery(portal,
                      NULL,
                      query_string,
                      commandTag,
                      plantree_list,
                      NULL);
    activeTx->end_parsing_time = bcdb_get_time();

    /*
     * Start the portal.  No parameters here.
     * chris: I fix the snapshot here
     */
    PortalStart(portal, NULL, 0, snapshot);

    /*
     * Select the appropriate output format: text unless we are doing a
     * FETCH from a binary cursor.  (Pretty grotty to have to do this here
     * --- but it avoids grottiness in other places.  Ah, the joys of
     * backward compatibility...)
     */
    format = 0; /* TEXT is default */
    if (IsA(parsetree->stmt, FetchStmt))
    {
        FetchStmt *stmt = (FetchStmt *)parsetree->stmt;

        if (!stmt->ismove)
        {
            Portal fportal = GetPortalByName(stmt->portalname);

            if (PortalIsValid(fportal) &&
                (fportal->cursorOptions & CURSOR_OPT_BINARY))
                format = 1; /* BINARY */
        }
    }
    PortalSetResultFormat(portal, 1, &format);

#if SAFEDBG2
    printf("safeDbg pid %d %s : %s: %d  tx= %d dest=%d\n",
           getpid(), __FILE__, __FUNCTION__, __LINE__, activeTx->tx_id, whereToSendOutput);
#endif

    /*
     * Now we can create the destination receiver object.
     */
    receiver = CreateDestReceiver(dest);
    if (dest == DestRemote)
        SetRemoteDestReceiverParams(receiver, portal);

    /*
     * Switch back to transaction context for execution.
     */
    MemoryContextSwitchTo(oldcontext);

    activeTx->portal = portal;
    PTRACE_END(BCDB_PHASE_PARSE_PLAN);
    PTRACE_BEGIN(BCDB_PHASE_PORTAL_RUN);
    if (bcdb_flow_debug_enabled())
        ereport(LOG,
                (errmsg("[BCDB_FLOW] portal_run_enter pid=%d txid=%d xid=%u cmd=%s",
                        getpid(), (int)tx->tx_id, (unsigned int)tx->xid,
                        commandTag ? commandTag : "<null>")));
    (void)PortalRun(portal,
                    FETCH_ALL,
                    true, /* always top level */
                    true,
                    receiver,
                    receiver,
                    completionTag);
    if (bcdb_flow_debug_enabled())
        ereport(LOG,
                (errmsg("[BCDB_FLOW] portal_run_exit pid=%d txid=%d xid=%u completion=%s",
                        getpid(), (int)tx->tx_id, (unsigned int)tx->xid,
                        completionTag)));
    if (receiver)
        receiver->rDestroy(receiver);
    receiver = NULL;
    PTRACE_END(BCDB_PHASE_PORTAL_RUN);
    PTRACE_BEGIN(BCDB_PHASE_PARSE_PLAN); /* continue timing any post-run setup */

#if SAFEDBG1
    printf("safeDbg pid %d %s : %s: %d \n",
           getpid(), __FILE__, __FUNCTION__, __LINE__);
#endif

    MemoryContextSwitchTo(oldcontext);
}

int chk_query_type(const char *query, const char *fmt1_str, const char *fmt2_str)
{

    int cmd_type = 1;
    int cmd_len = 6; // all of INSERT, UPDATE, DELETE, SELECT len = 6 !!
    int ofst = 0;
    for (ofst = 0; ofst < cmd_len; ofst++)
    {
        if ((query[ofst] != fmt1_str[ofst]) && (query[ofst] != fmt2_str[ofst]))
        {
            cmd_type = 0;
            break;
        }
    }
    return cmd_type;
}

/*
 */
void safedb_txdt(BCDBShmXact *tx, bool dualTab)
{
    bool saved_is_bcdb_worker = is_bcdb_worker;
    PG_TRY();
    {
        bcdb_worker_process_tx_dt(tx, dualTab);
    }
    PG_CATCH();
    {
        /*
         * Restore is_bcdb_worker flag even on error to avoid leaking
         * snapshots/relcache references in subsequent queries.
         */
        is_bcdb_worker = saved_is_bcdb_worker;
        PG_RE_THROW();
    }
    PG_END_TRY();
    /*
     * Restore is_bcdb_worker flag after BCDB transaction processing.
     * bcdb_worker_process_tx_dt sets is_bcdb_worker = true, but when called
     * from a regular psql session (not a dedicated worker), this flag must
     * be restored to prevent subsequent queries from using BCDB-specific
     * code paths (PORTAL_MULTI_QUERY strategy, skipped ExecutorFinish/End,
     * modified snapshot/heap visibility, etc.) which cause snapshot reference
     * leaks and other resource leaks.
     */
    is_bcdb_worker = saved_is_bcdb_worker;
}

/* Helper to check if a character is whitespace */
static bool is_sql_whitespace(char c)
{
    return (c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == '\v');
}

/* Helper to skip whitespaces and comments in SQL string */
static const char *skip_sql_whitespace_and_comments(const char *p)
{
    while (*p)
    {
        if (is_sql_whitespace(*p))
        {
            p++;
        }
        else if (p[0] == '-' && p[1] == '-')
        {
            /* Line comment: skip until newline or end of string */
            p += 2;
            while (*p && *p != '\n' && *p != '\r')
            {
                p++;
            }
        }
        else if (p[0] == '/' && p[1] == '*')
        {
            /* Block comment: skip until */
            p += 2;
            while (*p && !(p[0] == '*' && p[1] == '/'))
            {
                p++;
            }
            if (*p)
            {
                p += 2; /* skip * and / */
            }
        }
        else
        {
            break;
        }
    }
    return p;
}

/* Parses recovery commands: select saveState('snapId') or select releaseState('snapId') */
static bool parse_recovery_query(const char *sql, char *snapId, size_t snapId_sz, bool *is_save, bool *is_release)
{
    const char *p = sql;
    char quote_char = '\0';
    size_t len = 0;

    *is_save = false;
    *is_release = false;

    if (!p)
        return false;

    p = skip_sql_whitespace_and_comments(p);

    /* Match "SELECT" (case-insensitive) */
    if (pg_strncasecmp(p, "select", 6) != 0)
        return false;
    p += 6;

    /* There must be whitespace, comment, or opening parenthesis next */
    if (*p != '\0' && !is_sql_whitespace(*p) && *p != '/' && *p != '-' && *p != '(')
        return false;

    p = skip_sql_whitespace_and_comments(p);

    /* Match "saveState" or "releaseState" (case-insensitive) */
    if (pg_strncasecmp(p, "saveState", 9) == 0)
    {
        *is_save = true;
        p += 9;
    }
    else if (pg_strncasecmp(p, "releaseState", 12) == 0)
    {
        *is_release = true;
        p += 12;
    }
    else
    {
        return false;
    }

    /* There must be whitespace, comment, or opening parenthesis next */
    if (*p != '\0' && !is_sql_whitespace(*p) && *p != '/' && *p != '-' && *p != '(')
        return false;

    p = skip_sql_whitespace_and_comments(p);

    /* Match opening parenthesis '(' */
    if (*p != '(')
        return false;
    p++;

    p = skip_sql_whitespace_and_comments(p);

    /* Match opening quote '\'' or '"' */
    if (*p == '\'' || *p == '"')
    {
        quote_char = *p;
        p++;
    }
    else
    {
        return false;
    }

    /* Extract the snapshot ID */
    while (*p && *p != quote_char)
    {
        if (len < snapId_sz - 1)
        {
            snapId[len++] = *p;
        }
        p++;
    }

    if (*p != quote_char)
        return false; /* Unclosed quote */
    p++;

    snapId[len] = '\0';

    p = skip_sql_whitespace_and_comments(p);

    /* Match closing parenthesis ')' */
    if (*p != ')')
        return false;

    return true;
}

void bcdb_worker_process_tx_dt(BCDBShmXact *tx, bool dualTab)
{
    BCBlock *block = NULL;
    Snapshot snapshot;

    int latest_tx_id = 0;
    int rw_conflicts = 1;
    bool init = true;
    bool hold_portal_snapshot = false;
    int num_restarts = -1;
    int t_delta = 0;
    char snapId[32] = "";
    char tx_result[1024];
    struct timeval tv1, tv2;
    int condSig = 0;
    int apply_retries = 0;
    bool apply_nonretryable = false;
    bool apply_terminal_noop = false;
    bool published_max_advanced = false;
    bool parse_barrier_done = false;
    BCTxID retry_wait_committed_txid = -1;

    is_bcdb_worker = true;

    Assert(tx != NULL);
    tx->worker_pid = pid;
    activeTx = tx;
    tx->block_id_snapshot = tx->block_id_committed - 1;

    LIST_INIT(&ws_table_record);
    LIST_INIT(&rs_table_record);

    tv1.tv_sec = 0;
    tv2.tv_sec = 0;
    tv1.tv_usec = 0;
    tv2.tv_usec = 0;
    gettimeofday(&tv1, NULL);
#if SAFEDBG3
    printf(" id= %d  time= %ld.%ld\n", tx->tx_id, tv1.tv_sec, tv1.tv_usec);
#endif

    bcdb_ptrace_reset();
    PTRACE_BEGIN(BCDB_PHASE_TOTAL);

    if (tx->tx_id == 0)
    {
        tx_start_time.tv_sec = 0;
        tx_start_time.tv_usec = 0;
        gettimeofday(&tx_start_time, NULL);
    }

#if SAFEDBG2
    printf("safeDbg start pid %d %s : %s: %d "
           "latest-vs-myId= %d %d dualTab %d \n",
           getpid(), __FILE__, __FUNCTION__, __LINE__,
           get_last_committed_txid(tx), tx->tx_id, dualTab);
#endif
    PG_TRY();
    {
        if (bcdb_dt_completion_only_skip_reads_enabled() &&
            !bcdb_block_return_actual_results_enabled() &&
            bcdb_query_is_select(tx->sql))
        {
            BCTxID fast_tx_id = tx->tx_id;
            int mem_txid;
            BCBlock *committed_block;

            block = get_block_by_id(1, false);
            Assert(block != NULL);
            mem_txid = bcdb_result_slot_for_txid(tx->tx_id);

            tx->status = TX_COMMITTING;
            bcdb_wait_for_slot_consumable(block, tx->tx_id, mem_txid);
			memset(tx->select_result, 0, sizeof(tx->select_result));
            memset(block->result[mem_txid], 0, sizeof(block->result[mem_txid]));
            pg_write_barrier();
            __atomic_store_n(&block->result_commit_xid[mem_txid],
                             tx->xid, __ATOMIC_RELEASE);
            __atomic_store_n(&block->result_committed_txid[mem_txid],
                             tx->tx_id, __ATOMIC_RELEASE);
			if (tx->block_id_committed == BCDBMaxBid)
				__atomic_store_n(&block->result_consumed_txid[mem_txid],
								 (int32)tx->tx_id, __ATOMIC_RELEASE);

            if (bcdb_serial_gate_source != BCDB_GATE_SRC_LAST_COMMITTED)
                set_published_max_txid(tx);
            if (bcdb_advance_commit_watermark)
                advance_last_committed_txid(tx);
            else
            {
                bcdb_wait_for_prev_committed(tx);
                set_last_committed_txid(tx);
            }

            tx->status = TX_COMMITED;
            committed_block = get_block_by_id(tx->block_id_committed, false);
            if (committed_block != NULL)
            {
                int32 num_finished = __sync_add_and_fetch(&committed_block->num_finished, 1);

                if (num_finished == committed_block->num_tx)
                {
                    uint32 global_bmin = __sync_add_and_fetch(&block_meta->global_bmin, 1);

                    ConditionVariableBroadcast(&block_meta->conds[global_bmin % NUM_BMIN_COND]);
                    block_cleaning_dt(committed_block->id);
                }
            }
            condSig = 1;
            bcdb_emit_completion_only_select_result();
            delete_tx(tx);
            MemoryContextReset(bcdb_tx_context);
            PTRACE_END(BCDB_PHASE_TOTAL);
            bcdb_ptrace_emit(fast_tx_id, 0);
            return;
        }

        for (;;)
        {
            latest_tx_id = get_last_committed_txid(tx);
#if SAFEDBG3
            printf("safeDbg init %d own-id %d : latest-id %d \n",
                   init, tx->tx_id, latest_tx_id);
#endif
            if (rw_conflicts == 1)
            {
                // retry:
                num_restarts++;
                LIST_INIT(&ws_table_record);
                LIST_INIT(&rs_table_record);

                if (init)
                {
                    activeTx->tx_id_committed = get_last_committed_txid(tx);
#if SAFEDBG3
                    printf("safeDbg %s : %s: %d init tx-id= %d\n",
                           getpid(), __FILE__, __FUNCTION__, __LINE__,
                           activeTx->tx_id_committed);
#endif
                }
                else
                {
                    /* Fix: Clear stale optim_write_list before retry.
                     * AbortCurrentTransaction will undo heap/index changes,
                     * but the SIMPLEQ entries allocated in bcdb_tx_context
                     * need to be freed. MemoryContextReset frees the memory,
                     * then SIMPLEQ_INIT resets the head pointer. */
                    bcdb_drain_optim_write_list(activeTx);
                    MemoryContextReset(bcdb_tx_context);
                    /* Fix: AbortCurrentTransaction properly releases all resources
                     * (buffer pins, TupleDesc refs, locks) via ResourceOwnerRelease
                     * with isCommit=false (silent, no leak warnings).
                     * Then reset_xact_command sets xact_started=false so
                     * start_xact_command will begin a fresh transaction. */
                    AbortCurrentTransaction();
                    reset_xact_command();
                    /*
                     * AbortCurrentTransaction() already runs AtAbort/AtCleanup logic,
                     * which releases and drops portals/resource owners created in the
                     * failed transaction. Any pointers cached in activeTx may now be
                     * stale; never dereference them here.
                     */
                    activeTx->portal = NULL;
                    tx->queryDesc = NULL;
                    tx->sxact = NULL;
                    hold_portal_snapshot = false;
                    if (retry_wait_committed_txid >= 0)
                    {
                        BCDB_FLOW_LOG("[BCDB_FLOW] conflict_retry_wait pid=%d txid=%d wait_for=%d last_committed=%d published_max=%d",
                                      getpid(),
                                      tx ? (int)tx->tx_id : -1,
                                      (int)retry_wait_committed_txid,
                                      tx ? (int)get_last_committed_txid(tx) : -1,
                                      tx ? (int)get_published_max_txid(tx) : -1);
                        bcdb_wait_for_target_committed(tx, retry_wait_committed_txid);
                        retry_wait_committed_txid = -1;
                    }
#if SAFEDBG2
                    printf("\n\n safeDbg tx %d rerun due to conflict pid %d %s : %s: %d \n",
                           tx->tx_id, getpid(), __FILE__, __FUNCTION__, __LINE__);
#endif
                }
                /*
                 * tx_id_committed defines the "baseline" of what was committed when we
                 * take our snapshot/simulate the transaction. On retries we must
                 * refresh this baseline, otherwise conflict_checkDT will repeatedly
                 * flag already-committed txs as conflicts and can livelock.
                 */
                activeTx->tx_id_committed = get_last_committed_txid(tx);
                init = false;
                if (bcdb_dt_light_snapshot_enabled() && !bcdb_dt_conflict_tracking)
                    XactIsoLevel = XACT_READ_COMMITTED;
                else
                    XactIsoLevel = tx->isolation;
                PTRACE_BEGIN(BCDB_PHASE_PARSE_PLAN);
                start_xact_command();
                tx->status = TX_EXECUTING;

#if SAFEDBG2
                printf("safeDbg pid %d %s : %s: %d \n",
                       getpid(), __FILE__, __FUNCTION__, __LINE__);
#endif
                snapshot = GetTransactionSnapshot();  // get snapshot
                activeTx->snap_xmin = snapshot->xmin; /* T3-v2: save for XID-based conflict skip */
                if (IsolationIsSerializable())
                {
                    tx->sxact = ShareSerializableXact();
                    tx->sxact->bcdb_tx = tx;
                }
                else
                    tx->sxact = NULL;
                get_write_set(tx, snapshot); //  does CreatePortal
                bcdb_maybe_enqueue_deferred_delete0_by_key(tx);
                PTRACE_END(BCDB_PHASE_PARSE_PLAN);
#if SAFEDBG1
                printf("safeDbg pid %d %s : %s: %d \n",
                       getpid(), __FILE__, __FUNCTION__, __LINE__);
#endif
                /*
                 * Drop the unnamed portal immediately after PortalRun completes.
                 * This ensures the executor is shut down and releases buffer pins and
                 * TupleDesc refs before commit-time ResourceOwnerRelease, avoiding leak
                 * warnings in server.log.
                 */
                if (activeTx->portal != NULL)
                {
                    PortalDrop(activeTx->portal, false);
                    activeTx->portal = NULL;
                }
                hold_portal_snapshot = false;
                if (bcdb_dt_skip_readonly_gate_enabled() &&
                    bcdb_serial_gate_source != BCDB_GATE_SRC_LAST_COMMITTED &&
                    LIST_EMPTY(&ws_table_record))
                {
                    mark_published_ready_txid(tx);
                    published_max_advanced = true;
                }
#if SAFEDBG1
                printf("safeDbg pid %d %s : %s: %d \n",
                       getpid(), __FILE__, __FUNCTION__, __LINE__);
#endif
                tx->status = TX_WAIT_FOR_COMMIT;
            }
            init = false;

#if SAFEDBG2
            gettimeofday(&tv2, NULL);
            printf("safeDbg waiting pid %d %s : %s: %d latest-vs-myId= %d %d tx-bid %d\n",
                   getpid(), __FILE__, __FUNCTION__, __LINE__,
                   latest_tx_id, tx->tx_id, tx->block_id_committed);
            printf("safeDbg id= %d  wait-time= %ld.%ld\n", tx->tx_id,
                   tx_start_time.tv_sec, tx_start_time.tv_usec);
#endif
            block = get_block_by_id(1, false);
            Assert(block != NULL);
            latest_tx_id = get_last_committed_txid(tx);
            if (dualTab)
            {
#if SAFEDBG1
                printf("\n *** safeDbg pid %d waiting %s : %s: %d latest-vs-myId= %d %d  blksz= %d *** \n",
                       getpid(), __FILE__, __FUNCTION__, __LINE__,
                       latest_tx_id, tx->tx_id, 2 * block->blksize);
#endif
                /*
                 * Deterministic serial gate via interruptible polling.
                 *
                 * This avoids ConditionVariable wait-list corruption observed
                 * under concurrent direct "s <txid> ..." execution.
                 */
                PTRACE_BEGIN(BCDB_PHASE_GATE);
                bcdb_wait_for_dt_parse_barrier(tx, &parse_barrier_done);
                if (bcdb_serial_gate_source == BCDB_GATE_SRC_LAST_COMMITTED)
                    bcdb_wait_for_prev_committed(tx); /* paper-style: gate on full predecessor commit */
                else
                    bcdb_wait_for_serial_slot(tx, block); /* Lever D: gate on published_max */
                PTRACE_END(BCDB_PHASE_GATE);
				strlcpy(tx_result, tx->select_result, sizeof(tx_result));

#if SAFEDBG2
                printf("safeDbg blk read result = %s\n", tx_result);
                printf("safeDbg worker read result at %d= %s\n", ((tx->tx_id) % (2 * block->blksize)), block->result[(tx->tx_id) % (2 * block->blksize)]);
#endif
                PTRACE_BEGIN(BCDB_PHASE_CONFLICT);
                rw_conflicts = conflict_checkDT();
                PTRACE_END(BCDB_PHASE_CONFLICT);
                // conflict_check();
            }
            else
            {
                printf("\n\n safeDbg **ERROR not dualtab ** pid %d %s : %s: %d \n",
                       getpid(), __FILE__, __FUNCTION__, __LINE__);
            }
            if (rw_conflicts == 1)
            {
                retry_wait_committed_txid = bcdb_get_last_conflict_txid();
                BCDB_FLOW_LOG("[BCDB_FLOW] conflict_retry pid=%d txid=%d xid=%u last_committed=%d published_max=%d",
                              getpid(),
                              tx ? (int)tx->tx_id : -1,
                              (unsigned int)(tx ? tx->xid : InvalidTransactionId),
                              tx ? (int)get_last_committed_txid(tx) : -1,
                              tx ? (int)get_published_max_txid(tx) : -1);
                continue;
            }

            tx->status = TX_COMMITTING;
#if SAFEDBG2
            printf(" safeDbg pid %d %s : %s: %d tx %d \n",
                   getpid(), __FILE__, __FUNCTION__, __LINE__, tx->tx_id);
#endif
            publish_ws_tableDT(tx->tx_id); // HASHTAB_SWITCH_THRESHOLD
            BCDB_FLOW_LOG("[BCDB_FLOW] publish_ws_done pid=%d txid=%d xid=%u last_committed=%d published_max_before=%d",
                          getpid(),
                          tx ? (int)tx->tx_id : -1,
                          (unsigned int)(tx ? tx->xid : InvalidTransactionId),
                          tx ? (int)get_last_committed_txid(tx) : -1,
                          tx ? (int)get_published_max_txid(tx) : -1);

            /*
             * Step 0 (2026-04-19): publish gate release immediately after conflict
             * check so tx N+1 can enter conflict/apply while tx N applies.
             * The pre-apply bcdb_wait_for_prev_committed that was here in v2-fix1
             * has been removed — it re-serialized apply and caused the 8t 335→223
             * TPS regression.  Conflict check + wait=true in apply_optim_update are
             * sufficient to handle any co-write edge cases.
             */
            if (bcdb_serial_gate_source != BCDB_GATE_SRC_LAST_COMMITTED &&
                !published_max_advanced)
            {
                set_published_max_txid(tx);
                published_max_advanced = true;
            }

            /*
             * Lever D v2 apply path:
             * retry apply in a subtransaction without re-running parse/plan.
             */
            PTRACE_BEGIN(BCDB_PHASE_APPLY);
            apply_nonretryable = false;
            apply_terminal_noop = false;
            if (!bcdb_apply_optim_writes_with_retry(tx, &apply_retries,
                                                    &apply_nonretryable))
            {
                PTRACE_END(BCDB_PHASE_APPLY);

                if (apply_nonretryable)
                {
                    /*
                     * Lever D advances published_max before PostgreSQL commit, so a
                     * repeated DELETE/INSERT key chain can make one replica exhaust
                     * unique-key apply retries before the ordered DELETE commits
                     * while another replica observes the DELETE just in time.  Give
                     * that apply one settled retry after all ordered predecessors
                     * have committed before deciding the INSERT is a terminal no-op.
                     */
                    if (bcdb_serial_gate_source != BCDB_GATE_SRC_LAST_COMMITTED &&
                        get_last_committed_txid(tx) < (tx->tx_id - 1))
                    {
                        int settled_apply_retries = 0;
                        bool settled_nonretryable = false;

                        BCDB_FLOW_LOG("[BCDB_FLOW] apply_unique_wait_prev_commit pid=%d txid=%d xid=%u retries=%d last_committed=%d published_max=%d",
                                      getpid(),
                                      tx ? (int)tx->tx_id : -1,
                                      (unsigned int)(tx ? tx->xid : InvalidTransactionId),
                                      apply_retries,
                                      tx ? (int)get_last_committed_txid(tx) : -1,
                                      tx ? (int)get_published_max_txid(tx) : -1);
                        bcdb_wait_for_prev_committed(tx);

                        PTRACE_BEGIN(BCDB_PHASE_APPLY);
                        if (bcdb_apply_optim_writes_with_retry(tx,
                                                               &settled_apply_retries,
                                                               &settled_nonretryable))
                        {
                            PTRACE_END(BCDB_PHASE_APPLY);
                            apply_retries += settled_apply_retries;
                            apply_nonretryable = false;
                            BCDB_FLOW_LOG("[BCDB_FLOW] apply_unique_settled_retry_ok pid=%d txid=%d xid=%u retries=%d",
                                          getpid(),
                                          tx ? (int)tx->tx_id : -1,
                                          (unsigned int)(tx ? tx->xid : InvalidTransactionId),
                                          settled_apply_retries);
                        }
                        else
                        {
                            PTRACE_END(BCDB_PHASE_APPLY);
                            apply_retries += settled_apply_retries;
                            apply_nonretryable = settled_nonretryable;
                            if (!apply_nonretryable)
                            {
                                BCDB_FLOW_LOG("[BCDB_FLOW] apply_unique_settled_retry_restart pid=%d txid=%d xid=%u retries=%d last_committed=%d published_max=%d",
                                              getpid(),
                                              tx ? (int)tx->tx_id : -1,
                                              (unsigned int)(tx ? tx->xid : InvalidTransactionId),
                                              settled_apply_retries,
                                              tx ? (int)get_last_committed_txid(tx) : -1,
                                              tx ? (int)get_published_max_txid(tx) : -1);
                                rw_conflicts = 1;
                                continue;
                            }
                        }
                    }

                    if (apply_nonretryable)
                    {
                        BCDB_FLOW_LOG("[BCDB_FLOW] apply_nonretryable_finalize_noop pid=%d txid=%d xid=%u",
                                      getpid(),
                                      tx ? (int)tx->tx_id : -1,
                                      (unsigned int)(tx ? tx->xid : InvalidTransactionId));
                        apply_terminal_noop = true;
                    }
                }
                else
                {
                    /*
                     * Fallback to full transaction retry if apply retries are exhausted.
                     * With published_max already advanced, gate check uses >= so this
                     * tx_id can re-enter deterministic conflict-check/apply safely.
                     */
                    BCDB_FLOW_LOG("[BCDB_FLOW] apply_retry_restart pid=%d txid=%d xid=%u retries=%d last_committed=%d published_max=%d",
                                  getpid(),
                                  tx ? (int)tx->tx_id : -1,
                                  (unsigned int)(tx ? tx->xid : InvalidTransactionId),
                                  apply_retries,
                                  tx ? (int)get_last_committed_txid(tx) : -1,
                                  tx ? (int)get_published_max_txid(tx) : -1);
                    rw_conflicts = 1;
                    continue;
                }
            }
            else
            {
                PTRACE_END(BCDB_PHASE_APPLY);
            }
            num_restarts += apply_retries;
            bcdb_ptrace_inc_counter(BCDB_PTRACE_COUNTER_APPLY_RETRY_COUNT,
                                    (uint64)apply_retries);

#if SAFEDBG2
            printf(" safeDbg pid %d %s : %s: %d tx %d \n",
                   getpid(), __FILE__, __FUNCTION__, __LINE__, tx->tx_id);
#endif

            if (tx->sxact != NULL)
                tx->sxact->flags |= SXACT_FLAG_PREPARED;

            DEBUGMSG("[ZL] worker(%d) commiting tx(%s)", getpid(), tx->hash);
            tx->status = TX_COMMITED;

#if SAFEDBG2
            printf("safeDbg %s : %s: %d latest-vs-myId= %d %d \n",
                   __FILE__, __FUNCTION__, __LINE__, latest_tx_id, tx->tx_id);
#endif

            SpinLockAcquire(restart_counter_lock);
            total_num_restarts += num_restarts;
            SpinLockRelease(restart_counter_lock);

            gettimeofday(&tv2, NULL);
            t_delta = (tv2.tv_usec - tv1.tv_usec);
            if (t_delta < 0)
                t_delta += 1000000;

#if SAFEDBG1
            printf("\nsafeDbg id= %d start= %ld.%ld finish= %ld.%ld restarts= %d exec= %d tx= %s\n",
                   tx->tx_id, tv1.tv_sec, tv1.tv_usec, tv2.tv_sec, tv2.tv_usec, total_num_restarts, t_delta, tx->sql);
#endif
            t_sinceTx1 += t_delta;

            if (bcdb_trace_timestamps_enabled() &&
                (tx->tx_id) % 1000 < 3 * bcdb_runtime_workers())
            {
                fp = fopen("/tmp/timestamps.txt", "a");
                if (fp)
                {
                    fprintf(fp, "id= %d, pid= %d restarts=%d total_num_restarts= %d "
#if SAFEDBG1
                                "start-time= %ld.%ld finish time= %ld.%ld "
#endif
                                "exectime(us)= %d totalTime(us)= %d\n",
                            tx->tx_id, getpid(), num_restarts, total_num_restarts,
#if SAFEDBG1
                            tv1.tv_sec, tv1.tv_usec, tv2.tv_sec, tv2.tv_usec,
#endif
                            t_delta, t_sinceTx1);
                    fclose(fp);
                }
            }

            /* saveState/releaseState helpers for recovery flow. */
            {
                bool is_save = false;
                bool is_release = false;

                if (parse_recovery_query(tx->sql, snapId, sizeof(snapId), &is_save, &is_release))
                {
                    if (is_save)
                    {
#if SAFEDBG3
                        printf("\n safeDbg ** saveState tx detected !! ** pid %d %s : %s: %d \n",
                               getpid(), __FILE__, __FUNCTION__, __LINE__);
#endif
                        block->snapTid = tx->tx_id;
#if SAFEDBG3
                        printf("safeDbg tx save snapId = %s blk snapTid= %d myPid= %d \n", snapId, block->snapTid, getpid());
#endif
                        WaitConditionTimeoutPid(&block->condRecovery, (block->snapTid != tx->tx_id), 500000, getpid());
#if SAFEDBG3
                        printf("\nsafeDbg  ** saveState tx releasing!! ** pid %d %s : %s: %d \n",
                               getpid(), __FILE__, __FUNCTION__, __LINE__);
#endif
                    }
                    else if (is_release)
                    {
#if SAFEDBG3
                        printf("\n safeDbg ** releaseState tx detected !! ** pid %d %s : %s: %d \n",
                               getpid(), __FILE__, __FUNCTION__, __LINE__);
#endif
#if SAFEDBG3
                        printf("safeDbg tx release snapId = %s blk snapTid= %d myPid= %d \n", snapId, block->snapTid, getpid());
#endif
                        block->snapTid = getpid();
                        ConditionVariableBroadcast(&block->condRecovery);
                    }
                }
            }
            int mem_txid = bcdb_result_slot_for_txid(tx->tx_id);

            /* Portal was dropped right after PortalRun; keep this as a safety net. */
            if (hold_portal_snapshot && activeTx && activeTx->portal)
            {
                PortalDrop(activeTx->portal, false);
                activeTx->portal = NULL;
                hold_portal_snapshot = false;
            }

            PTRACE_BEGIN(BCDB_PHASE_FINISH);

            /*
             * Keep the gateway-visible BCDB result path off PostgreSQL's transaction
             * finish cost.  At this point deterministic conflict-check and apply have
             * completed for this tx, and the result string is fully determined.  The
             * BCDB middleware waits on result_committed_txid / last_committed_tx_id,
             * so publish those before finish_xact_command(); finish_xact_command()
             * can then run behind the result watermark without changing tx-id order.
             */
            bcdb_wait_for_slot_consumable(block, tx->tx_id, mem_txid);
			memset(tx->select_result, 0, sizeof(tx->select_result));
            memset(block->result[mem_txid], 0, sizeof(block->result[mem_txid]));
#if SAFEDBG3
            printf("safeDbg txid= %d mem-txid = %d \n", tx->tx_id, mem_txid);

            printf("\n\n ** safeDbg pid %d %s : %s: %d tx sql %s \n",
                   getpid(), __FILE__, __FUNCTION__, __LINE__, tx->sql);
#endif
#if SAFEDBG1
            if ((tx->tx_id) % 2000 < 2)
                printf("\n *** safeDbg pid %d signaling %s : %s: %d "
                       "latest-vs-myId= %d %d myquery= %s\n",
                       getpid(), __FILE__, __FUNCTION__,
                       __LINE__, get_last_committed_txid(tx), tx->tx_id, tx->sql);
#endif

            {
                uint64 finish_result_start = bcdb_ptrace_timer_start();
                int read_cmd = chk_query_type(tx->sql, "select", "SELECT");

                if (read_cmd == 1)
                    strlcpy(block->result[mem_txid], tx_result, sizeof(block->result[mem_txid]));
                else if (apply_terminal_noop)
                {
                    /*
                     * Transaction was aborted after exhausting retries (e.g. unique
                     * constraint violation, non-retryable error).  Report 0 rows
                     * affected using whatever command type the SQL was.
                     */
                    int del_cmd = chk_query_type(tx->sql, "delete", "DELETE");
                    int upd_cmd = chk_query_type(tx->sql, "update", "UPDATE");
                    int ins_cmd = chk_query_type(tx->sql, "insert", "INSERT");
                    if (ins_cmd == 1)
                        snprintf(block->result[mem_txid], sizeof(block->result[mem_txid]),
                                 "\n\t*%s* INSERT 0 0\n", tx->hash);
                    else if (upd_cmd == 1)
                        snprintf(block->result[mem_txid], sizeof(block->result[mem_txid]),
                                 "\n\t*%s* UPDATE 0\n", tx->hash);
                    else if (del_cmd == 1)
                        snprintf(block->result[mem_txid], sizeof(block->result[mem_txid]),
                                 "\n\t*%s* DELETE 0\n", tx->hash);
                    else
                        snprintf(block->result[mem_txid], sizeof(block->result[mem_txid]),
                                 "\n\t*%s* ERROR\n", tx->hash);
                }
                else
                {
                    /*
                     * Use the real completionTag set by PortalRun — it already
                     * contains the true row count (e.g. "INSERT 0 1", "UPDATE 3",
                     * "DELETE 0").  This is correct even for ON CONFLICT DO NOTHING
                     * (which sets "INSERT 0 0") and multi-row statements.
                     */
                    snprintf(block->result[mem_txid], sizeof(block->result[mem_txid]),
                             "\n\t*%s* %s\n", tx->hash, completionTag);
                }

                /*
                 * Publish result before advertising tx as committed to middleware waiters.
                 * Ensure visibility ordering across CPUs.
                 */
                pg_write_barrier();
                /*
                 * T3-v2: store commit XID before committed marker for release-acquire
                 * ordering.  When a reader observes result_committed_txid[slot]==tx_id,
                 * result_commit_xid[slot] is guaranteed visible.
                 */
                __atomic_store_n(&block->result_commit_xid[mem_txid],
                                 tx->xid, __ATOMIC_RELEASE);
                __atomic_store_n(&block->result_committed_txid[mem_txid],
                                 tx->tx_id, __ATOMIC_RELEASE);

                /*
				 * Self-consume only for direct psycopg transactions.  For
				 * bcdb_block_submit_results(), the middleware owns the slot until
				 * it has formatted the gateway response and publishes
				 * result_consumed_txid itself.  Marking a block-submit result
				 * consumed here lets a later tx reuse the ring slot before the
				 * gateway-side reader has observed it.
                 */
				if (tx->block_id_committed == BCDBMaxBid)
					__atomic_store_n(&block->result_consumed_txid[mem_txid],
									 (int32)tx->tx_id, __ATOMIC_RELEASE);

                bcdb_ptrace_timer_stop(BCDB_PTRACE_METRIC_FINISH_RESULT_US,
                                       finish_result_start);
            }

            {
                uint64 finish_publish_start = bcdb_ptrace_timer_start();
                if (bcdb_serial_gate_source == BCDB_GATE_SRC_LAST_COMMITTED)
                    set_last_committed_txid(tx); /* paper-style: already serialized at gate */
                else if (bcdb_advance_commit_watermark)
                    advance_last_committed_txid(tx); /* Lever D + T3: lock-free CAS */
                else
                {
                    bcdb_wait_for_prev_committed(tx); /* Lever D only: blocking finish */
                    set_last_committed_txid(tx);
                }
                bcdb_ptrace_timer_stop(BCDB_PTRACE_METRIC_FINISH_PUBLISH_US,
                                       finish_publish_start);
            }
            finish_xact_command();
            {
                BCBlock *committed_block = get_block_by_id(tx->block_id_committed, false);

                if (committed_block != NULL)
                {
                    int32 num_finished = __sync_add_and_fetch(&committed_block->num_finished, 1);

                    if (num_finished == committed_block->num_tx)
                    {
                        uint32 global_bmin = __sync_add_and_fetch(&block_meta->global_bmin, 1);

                        ConditionVariableBroadcast(&block_meta->conds[global_bmin % NUM_BMIN_COND]);
                        block_cleaning_dt(committed_block->id);
                    }
                }
            }
            condSig = 1;

#if SAFEDBG3
            // printf("blkmid read result at %d= %s\n", ((tx->tx_id)%(2*block->blksize)), block->result[(tx->tx_id)%(2*block->blksize)]); // does it get overwritten
            printf("safeDbg blk read result = %s\n", tx_result);
            // sprintf(&block->result[tx->tx_id],"\n\t*%s* user | field0 | field1 |\n", tx->hash);
#endif

#if SAFEDBG1
            printf("\n *** safeDbg pid %d endcmd %s : %s: %d "
                   "latest-vs-myId= %d %d \n\n",
                   getpid(), __FILE__, __FUNCTION__,
                   __LINE__, get_last_committed_txid(tx), tx->tx_id);
#endif
            EndCommand(completionTag, dest);
            bcdb_cleanup_optim_write_list(activeTx, true);
            delete_tx(tx);
            MemoryContextReset(bcdb_tx_context);
            PTRACE_END(BCDB_PHASE_FINISH);
            PTRACE_END(BCDB_PHASE_TOTAL);
            bcdb_ptrace_emit(tx->tx_id, num_restarts);
            break;
        }
    }
    PG_CATCH();
    {
        ErrorData *edata = NULL;
        ConditionVariableCancelSleep();

        if (bcdb_flow_debug_enabled())
        {
            const char *msg;
            const char *sqlstate;

            edata = CopyErrorData();
            msg = (edata && edata->message) ? edata->message : "<null>";
            sqlstate = (edata != NULL) ? unpack_sql_state(edata->sqlerrcode) : "00000";
            ereport(LOG,
                    (errmsg("[BCDB_FLOW] tx_error pid=%d txid=%d xid=%u sqlstate=%s status=%d msg=%s",
                            getpid(),
                            tx ? (int)tx->tx_id : -1,
                            (unsigned int)(tx ? tx->xid : InvalidTransactionId),
                            sqlstate,
                            tx ? (int)tx->status : -1,
                            msg)));
            FlushErrorState();
        }

#if SAFEDBG1
        printf("safeDbg pg-catch() pid %d %s : %s: %d  tx %d %s \n",
               getpid(), __FILE__, __FUNCTION__, __LINE__, tx->tx_id, tx->sql);
#endif
        // goto retry;

        // print_trace();
        /* Fix: Do NOT call finish_xact_command() here — it tries to commit
         * a transaction in error state, causing cascading warnings.
         * After PG_RE_THROW(), PostgresMain's sigsetjmp handler will call
         * AbortCurrentTransaction() which properly cleans up all resources. */
        if (condSig == 0 && tx != NULL)
        {
            /*
             * Block-submit callers wait on result_committed_txid[slot],
             * not just the contiguous watermark.  Publish a canonical
             * error result before advancing; otherwise one failed worker
             * can leave bcdb_block_submit_results() asleep forever.
             */
            if (block == NULL)
                block = get_block_by_id(1, false);
            bcdb_publish_error_result(block,
                                      tx,
                                      edata != NULL
                                          ? unpack_sql_state(edata->sqlerrcode)
                                          : "XX000");
            if (bcdb_serial_gate_source == BCDB_GATE_SRC_LAST_COMMITTED)
                set_last_committed_txid(tx);
            else if (bcdb_advance_commit_watermark)
                advance_last_committed_txid(tx);
            else
            {
                bcdb_wait_for_prev_committed(tx);
                set_last_committed_txid(tx);
            }
            {
                BCBlock *committed_block = get_block_by_id(tx->block_id_committed, false);

                if (committed_block != NULL)
                {
                    int32 num_finished = __sync_add_and_fetch(&committed_block->num_finished, 1);

                    if (num_finished == committed_block->num_tx)
                    {
                        uint32 global_bmin = __sync_add_and_fetch(&block_meta->global_bmin, 1);

                        ConditionVariableBroadcast(&block_meta->conds[global_bmin % NUM_BMIN_COND]);
                        block_cleaning_dt(committed_block->id);
                    }
                }
            }
            condSig = 1;
        }
        /* Let AbortCurrentTransaction() perform portal/resource-owner cleanup. */
        hold_portal_snapshot = false;
        if (activeTx)
            activeTx->portal = NULL;

        bcdb_drain_optim_write_list(activeTx);

        MemoryContextReset(bcdb_tx_context);
        delete_tx(tx);
        activeTx = NULL;

        if (edata != NULL)
            ReThrowError(edata);
        PG_RE_THROW();
    }
    PG_END_TRY();
}

void bcdb_worker_process_tx(BCDBShmXact *tx)
{
    BCBlock *block = NULL;
    Snapshot snapshot;
    ResourceOwner old_owner;
    int32 num_ready;
    int32 num_finished;
    bool timing = rand() % 10 == 0;

    Assert(tx != NULL);
    tx->worker_pid = pid;
    activeTx = tx;
    DEBUGMSG("[ZL] tx %s scheduled on worker: %d", tx->hash, tx->worker_pid);

    WaitGlobalBmin(tx->block_id_committed);
    tx->block_id_snapshot = tx->block_id_committed - 1;

    DEBUGMSG("[ZL] tx %s snapshot %d", tx->hash, tx->block_id_snapshot);
    LIST_INIT(&ws_table_record);
    LIST_INIT(&rs_table_record);

    PG_TRY();
    {
        XactIsoLevel = tx->isolation;
        tx->status = TX_EXECUTING;
        if (timing)
            tx->start_simulation_time = bcdb_get_time();
        start_xact_command();
        snapshot = GetTransactionSnapshot();
        tx->sxact = ShareSerializableXact();
        tx->sxact->bcdb_tx = tx;
        get_write_set(tx, snapshot);
        old_owner = CurrentResourceOwner;
        CurrentResourceOwner = activeTx->portal->resowner;
        if (tx->queryDesc != NULL)
        {
            ExecutorFinish(tx->queryDesc);
            ExecutorEnd(tx->queryDesc);
            FreeQueryDesc(tx->queryDesc);
            tx->queryDesc = NULL;
        }
        CurrentResourceOwner = old_owner;
        PortalDrop(activeTx->portal, false);

        if (block == NULL)
        {
            block = get_block_by_id(tx->block_id_committed, false);
            Assert(block != NULL);
        }
        tx->status = TX_WAIT_FOR_COMMIT;

        num_ready = __sync_add_and_fetch(&block->num_ready, 1);
        DEBUGMSG("[ZL] tx %s waiting with num_ready: %d", tx->hash, num_ready);
        if (num_ready == block->num_tx)
            ConditionVariableBroadcast(&block->cond);

        if (timing)
            tx->end_simulation_time = bcdb_get_time();

        WaitCondition(&block->cond, block->num_tx == block->num_ready);

        if (timing)
            tx->start_checking_time = bcdb_get_time();

        tx->status = TX_COMMITTING;

        if (SxactIsDoomed(activeTx->sxact))
            ereport(ERROR,
                    (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                     errmsg("doomed (tx: %s) after conflict check", activeTx->hash),
                     errdetail_internal("probably failed during conflict checking.")));

        conflict_check();
        if (timing)
            tx->end_checking_time = bcdb_get_time();
        /* Note: ignoring apply_optim_writes return value in non-DT path;
         * the non-DT path uses conflict_check() which is less strict. */
        apply_optim_writes();
        if (timing)
            tx->end_local_copy_time = bcdb_get_time();
        tx->sxact->flags |= SXACT_FLAG_PREPARED;
        finish_xact_command();

        DEBUGMSG("[ZL] worker(%d) commiting tx(%s)", getpid(), tx->hash);
        tx->status = TX_COMMITED;
        num_finished = __sync_add_and_fetch(&block->num_finished, 1);
        DEBUGMSG("[ZL] tx %s finishing with num_finish: %d", tx->hash, num_finished);

        if (num_finished == block->num_tx)
        {
            uint32 global_bmin;
            int32 block_committed = 0;
            BCDBShmXact *tx_iter;
            unsigned char tx_state_hash[SHA256_DIGEST_LENGTH];
            SHA256_CTX block_state_hash;
            unsigned char final_block_state_hash[SHA256_DIGEST_LENGTH];
            char *block_hash_b64;

            clean_rs_ws_table();
            global_bmin = __sync_add_and_fetch(&block_meta->global_bmin, 1);

            ConditionVariableBroadcast(&block_meta->conds[global_bmin % NUM_BMIN_COND]);
            DEBUGMSG("[ZL] tx %s incrementing bmin to: %d", activeTx->hash, global_bmin);
            SHA256_Init(&block_state_hash);

            for (int i = 0; i < block->num_tx; i++)
            {
                tx_iter = block->txs[i];
                if (tx_iter->status == TX_COMMITED)
                {
                    block_committed += 1;
                    SHA256_Final(tx_state_hash, &tx_iter->state_hash);
                    SHA256_Update(&block_state_hash, tx_state_hash, SHA256_DIGEST_LENGTH);
                }
            }
            SHA256_Final(final_block_state_hash, &block_state_hash);
            Base64Encode(final_block_state_hash, SHA256_DIGEST_LENGTH, &block_hash_b64);
            ereport(LOG, (errmsg("[ZL] block %d hash: %s", block->id, block_hash_b64)));
            __sync_fetch_and_add(&block_meta->num_committed, block_committed);
            __sync_fetch_and_add(&block_meta->num_aborted, block->num_tx - block_committed);
            block_cleaning(block->id);
        }
        if (tx->create_time != 0)
            tx->commit_time = bcdb_get_time();

        if (timing)
            tx->commit_time = bcdb_get_time();
        bcdb_cleanup_optim_write_list(activeTx, true);
        MemoryContextReset(bcdb_tx_context);
        if (timing)
            ereport(LOG, (errmsg("[ZL] tx (%s) runtime: %lu, parsing: %lu, simulation: %lu, wait: %lu, check: %lu, apply: %lu, commit %lu", tx->hash,
                                 tx->commit_time - tx->start_simulation_time,
                                 tx->end_parsing_time - tx->start_parsing_time,
                                 tx->end_simulation_time - tx->start_simulation_time,
                                 tx->start_checking_time - tx->end_simulation_time,
                                 tx->end_checking_time - tx->start_checking_time,
                                 tx->end_local_copy_time - tx->end_checking_time,
                                 tx->commit_time - tx->end_local_copy_time)));
        if (tx->create_time != 0)
            ereport(LOG, (errmsg("[ZL] tx (%s) latency: %lu", tx->hash, tx->commit_time - tx->create_time)));
    }
    PG_CATCH();
    {
        bcdb_drain_optim_write_list(activeTx);
        PG_RE_THROW();
    }
    PG_END_TRY();
}

void bcdb_on_worker_exit(int code, Datum arg)
{
    /* clean up here */
}
