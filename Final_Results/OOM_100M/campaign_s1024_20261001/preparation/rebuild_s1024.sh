#!/bin/bash
set -euo pipefail
I=/home/neel/claude_opt/install_opt; D=/tmp/ariabc_oom_100m/pgdata_base_f32s1024
export LD_LIBRARY_PATH=$I/lib
P="$I/bin/psql -X -v ON_ERROR_STOP=1 -h 127.0.0.1 -p 5499 -U postgres -d postgres"
$I/bin/pg_ctl -D $D -o "-p 5499 -c shared_buffers=256MB" -l /tmp/ariabc_oom_100m/rebuild_s1024_pg.log -w -t 600 start
$P -c "select indexrelid::regclass, pg_get_indexdef(indexrelid) from pg_index where indrelid='usertable'::regclass"
date; $P -c "SET maintenance_work_mem='1GB'; DROP INDEX usertable_merkle_idx; CREATE INDEX usertable_merkle_idx ON usertable USING merkle (ycsb_key) WITH (partitions=200, fanout=32);"
date; $P -Atc "select merkle_tree_stats('usertable'::regclass)"
$P -c "select pg_size_pretty(pg_relation_size('ariabc_internal.merkle_node_usertable')), (select count(*) from ariabc_internal.merkle_node_usertable)"
date; $P -Atc "select 'merkle_verify=' || merkle_verify('usertable')"
date; $P -c "CHECKPOINT"
$I/bin/pg_ctl -D $D -m fast -w stop
sync; echo REBUILD_OK
