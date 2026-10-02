#!/bin/bash
set -euo pipefail
I=/home/neel/claude_opt/install_opt; D=/tmp/ariabc_oom_100m/pgdata_base_f32s1024
export LD_LIBRARY_PATH=$I/lib
P="$I/bin/psql -X -v ON_ERROR_STOP=1 -h 127.0.0.1 -p 5499 -U postgres -d postgres"
$I/bin/pg_ctl -D $D -o "-p 5499 -c shared_buffers=256MB" -l /tmp/ariabc_oom_100m/rebuild_s1024_pg.log -w -t 600 start
$P -c "VACUUM FULL ariabc_internal.merkle_node_usertable"
$P -c "select c.relname, pg_size_pretty(pg_relation_size(c.oid)) from pg_class c where relname like 'merkle_node_usertable%'"
$P -c "select prefix_len, is_leaf, count(*), avg(tuple_count)::int, max(tuple_count), count(distinct (ctid::text::point)[0]) blocks from ariabc_internal.merkle_node_usertable group by 1,2 order by 1,2"
$P -Atc "select 'merkle_verify=' || merkle_verify('usertable')"
$P -c "CHECKPOINT"
$I/bin/pg_ctl -D $D -m fast -w stop
sync; echo COMPACT_OK
