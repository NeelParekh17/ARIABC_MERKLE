#!/bin/bash
ARIABC_DIR="/work/ARIABC/AriaBC"
PGDATA="/work/ARIABC/pgdata"
export LD_LIBRARY_PATH=$ARIABC_DIR/src/interfaces/libpq:$LD_LIBRARY_PATH

echo "Restarting AriaBC on port 5438..."

# 1. Try a graceful restart first
$ARIABC_DIR/src/bin/pg_ctl/pg_ctl restart -D $PGDATA -m fast -l $ARIABC_DIR/server.log || {
    echo "Graceful restart failed. Forcing stop..."
    # 2. Force kill if stuck (crucial for custom engine development)
    pkill -9 -f "postgres -D ${PGDATA}" 2>/dev/null || true
    sleep 2
    # 3. Start fresh
    $ARIABC_DIR/src/bin/pg_ctl/pg_ctl start -D $PGDATA -l $ARIABC_DIR/server.log
}

# 4. Verification
$ARIABC_DIR/src/bin/pg_ctl/pg_ctl status -D $PGDATA
