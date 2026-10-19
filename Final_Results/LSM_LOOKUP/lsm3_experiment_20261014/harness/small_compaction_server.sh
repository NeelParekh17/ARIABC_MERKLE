#!/bin/bash
set -euo pipefail
root=$HOME/claude_checks/lsm_lookup_20261014
export TMPDIR=$root/scratch
export LD_LIBRARY_PATH=$root/install/lib:$HOME/Desktop/rdkafka_local/lib:$HOME/Desktop/librdkafka/src
"$root/install/bin/psql" -X -h 127.0.0.1 -p 5488 -U postgres -d postgres -v ON_ERROR_STOP=1 -f "$root/src/scripts/distributed/sql/raft_apply_ledger_schema.sql" > "$root/small_compaction/ledger.log" 2>&1
cd "$root/small_compaction"
export BCDB_DECOUPLE_WORKERS=1 BCDB_DET_QUEUE_HIGH_WM=65536 BCDB_DET_QUEUE_LOW_WM=32768 ARIABC_DET_BLOCK_PARALLEL=64 ARIABC_DET_BLOCK_PIPELINE=4 ARIABC_DET_BLOCK_MAX=2048 ARIABC_DET_ORDER_START_SEQ=0 ARIABC_DET_EVENT_BLOCK_FASTPATH=0 ARIABC_DET_PREFIXED_DIRECT_PARALLEL=1
nohup "$root/cluster/ariabc_pg/build/bin/ariabc_pg_server" --id 1 --raftEndpoint 127.0.0.1:9088 --clientPort 8088 --raftMembers 1=127.0.0.1:9088 --dbName postgres --dbHost 127.0.0.1 --dbPort 5488 --dbUser postgres --dbType 1 --safedb 1 --dbConnPoolSize 8 --bcdbInitBlockSize 8 --pgExecMode event --bypassRaft 1 > det_server.log 2>&1 < /dev/null &
echo $! > det_server.pid
