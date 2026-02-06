#!/bin/bash
export LD_LIBRARY_PATH=/work/ARIABC/AriaBC/src/interfaces/libpq:$LD_LIBRARY_PATH
PSQL=/work/ARIABC/AriaBC/src/bin/psql/psql
PGDATA=/work/ARIABC/pgdata
PORT=5438

echo "Starting leak trigger test..."

for i in {1..20}; do
    $PSQL -p $PORT -d postgres -U postgres -c "s 00000000 insert into usertable values(2000$i, 'val$i');" > /dev/null 2>&1
    sleep 0.1
done

echo "Test complete."
