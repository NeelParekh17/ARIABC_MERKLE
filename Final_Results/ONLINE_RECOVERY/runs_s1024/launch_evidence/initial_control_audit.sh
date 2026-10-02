set -euo pipefail
B='/home/neel/Desktop/recovery_s1024_20261001T060514ZJ'
export LD_LIBRARY_PATH="$B/rdkafka/lib:$B/install/lib"
export PATH="$B/install/bin:$PATH"
DATA="$B/repo/.bench_tmp/recovery_pgdata"
pg_ctl -D "$DATA" -w -t 120 start -l "$B/initial_control_postgres.log"
trap 'pg_ctl -D "$DATA" -m fast -w stop' EXIT
bash "$B/repo/scripts/distributed/bootstrap_raft_apply_ledger.sh" --db postgres --port 5448 --user postgres --schema-only --reset-for-restore
psql -X -h 127.0.0.1 -p 5448 -U postgres postgres -v ON_ERROR_STOP=1 -v merkle_partitions=200 -v merkle_fanout=4 -v merkle_split_threshold=32 -v merkle_merge_threshold=8 -v bench_enable_merkle=1 -f "$B/repo/scripts/restore_usertable_small.sql" > "$B/initial_control_restore.log" 2>&1
psql -X -h 127.0.0.1 -p 5448 -U postgres postgres -v ON_ERROR_STOP=1 -f "$B/geometry.sql"
psql -X -h 127.0.0.1 -p 5448 -U postgres postgres -v ON_ERROR_STOP=1 <<'SQL'
SHOW fsync;
SHOW synchronous_commit;
SHOW full_page_writes;
SELECT count(*), merkle_root_hash('public.usertable_small'), merkle_verify('public.usertable_small'), md5(string_agg(ycsb_key::text || field1 || field2 || field3 || field4 || field5 || field6 || field7 || field8 || field9 || field10, ',' ORDER BY ycsb_key)) FROM public.usertable_small;
SELECT format('REINDEX INDEX %s;',i.indexrelid::regclass) FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid JOIN pg_am a ON a.oid=c.relam WHERE i.indrelid='public.usertable_small'::regclass AND a.amname='merkle'
\gexec
SELECT count(*), merkle_root_hash('public.usertable_small'), merkle_verify('public.usertable_small'), md5(string_agg(ycsb_key::text || field1 || field2 || field3 || field4 || field5 || field6 || field7 || field8 || field9 || field10, ',' ORDER BY ycsb_key)) FROM public.usertable_small;
DO $$ BEGIN IF NOT merkle_verify('public.usertable_small') OR (SELECT count(*) FROM public.usertable_small) <> 12000 THEN RAISE EXCEPTION 'initial audit failed'; END IF; END $$;
SQL
