#!/bin/bash
export LD_LIBRARY_PATH=/work/ARIABC/AriaBC/src/interfaces/libpq:$LD_LIBRARY_PATH
# Try to restart if running, otherwise start
/work/ARIABC/AriaBC/src/bin/pg_ctl/pg_ctl restart -D /work/ARIABC/pgdata -m fast -l /work/ARIABC/AriaBC/server.log || \
/work/ARIABC/AriaBC/src/bin/pg_ctl/pg_ctl start -D /work/ARIABC/pgdata -l /work/ARIABC/AriaBC/server.log
