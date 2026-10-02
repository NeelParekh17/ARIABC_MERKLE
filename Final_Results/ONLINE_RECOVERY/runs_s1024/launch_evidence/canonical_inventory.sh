set -e
for F in /home/neel/Desktop/ariabc_install/bin/postgres /home/neel/Desktop/ariabc_cluster/ariabc_pg/build/bin/ariabc_pg_server /home/neel/Desktop/ariabc_cluster/ariabc_pg/build/bin/ariabc_pg_gateway /home/neel/Desktop/ariabc_pg_build_u22/bin/ariabc_pg_server /home/neel/ARIABC/install/bin/postgres /home/neel/ARIABC/AriaBC/ariabc_pg/build/bin/ariabc_pg_gateway; do
 if test -f "$F"; then sha256sum "$F"; fi
done
ss -ltnp | awk '$4 ~ /:(5438|8000|8001|9000)$/ {print}'
for DATA in /home/neel/Desktop/ariabc_cluster/.bench_tmp/single_node_pgdata; do
 if test -f "$DATA/postmaster.pid"; then head -n 3 "$DATA/postmaster.pid"; fi
done
