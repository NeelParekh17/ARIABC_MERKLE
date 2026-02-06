#!/bin/bash
# Full rebuild script for AriaBC PostgreSQL
# This script stops the server, cleans, rebuilds, and restarts

set -e

ARIABC_DIR="/work/ARIABC/AriaBC"
PGDATA="/work/ARIABC/pgdata"
SERVER_LOG="${ARIABC_DIR}/server.log"
INSTALL_DIR="/work/ARIABC/install"

export LD_LIBRARY_PATH=${ARIABC_DIR}/src/interfaces/libpq:$LD_LIBRARY_PATH

echo "========================================="
echo "AriaBC Full Rebuild Script"
echo "========================================="
echo "Started at: $(date)"
echo ""

# Step 1: Stop the server if running
echo "--- Step 1: Stopping PostgreSQL server ---"
if [ -f "${PGDATA}/postmaster.pid" ]; then
    echo "Stopping server with pg_ctl..."
    ${ARIABC_DIR}/src/bin/pg_ctl/pg_ctl stop -D ${PGDATA} -m fast -w -t 30 || {
        echo "pg_ctl stop failed, trying immediate mode..."
        ${ARIABC_DIR}/src/bin/pg_ctl/pg_ctl stop -D ${PGDATA} -m immediate -w -t 10 || true
    }
    sleep 2
fi
# Kill any remaining postgres processes
echo "Ensuring all postgres processes are stopped..."
pkill -9 -f "postgres -D ${PGDATA}" 2>/dev/null || true
sleep 2

# Verify no postgres processes are running
if pgrep -f "postgres -D ${PGDATA}" > /dev/null; then
    echo "ERROR: Failed to stop all postgres processes!"
    echo "Active processes:"
    pgrep -af "postgres -D ${PGDATA}"
    exit 1
fi
echo "Server stopped successfully."
echo ""

# Step 2: Clean build
echo "--- Step 2: Cleaning previous build ---"
cd ${ARIABC_DIR}/src
make clean 2>/dev/null || true
echo "Clean complete."
echo ""

# Step 3: Full rebuild
echo "--- Step 3: Building AriaBC PostgreSQL ---"
cd ${ARIABC_DIR}/src
# Use -j for parallel build (adjust number based on CPU cores)
make -j$(nproc) 2>&1 | tee /tmp/ariabc_build.log
if [ ${PIPESTATUS[0]} -ne 0 ]; then
    echo "ERROR: Build failed! Check /tmp/ariabc_build.log for details."
    exit 1
fi
echo "Build complete."
echo ""

# Step 4: Start the server
echo "--- Step 4: Starting PostgreSQL server ---"
cd ${ARIABC_DIR}/src/backend
./postgres -D ${PGDATA} >> ${SERVER_LOG} 2>&1 &
POSTGRES_PID=$!
sleep 5

# Check if server started
if pgrep -f "postgres -D ${PGDATA}" > /dev/null; then
    echo "Server process started (PID: $POSTGRES_PID)."
    echo "Listening on port 5438"
    
    # Wait for server to be ready to accept connections
    echo "Waiting for server to be ready..."
    MAX_WAIT=30
    WAITED=0
    while [ $WAITED -lt $MAX_WAIT ]; do
        if ${ARIABC_DIR}/src/bin/pg_ctl/pg_ctl status -D ${PGDATA} | grep -q "server is running"; then
            echo "Server is ready!"
            break
        fi
        sleep 1
        WAITED=$((WAITED + 1))
        echo "Waiting... ($WAITED/${MAX_WAIT}s)"
    done
    
    if [ $WAITED -eq $MAX_WAIT ]; then
        echo "WARNING: Server might not be fully ready yet."
    fi
else
    echo "ERROR: Server failed to start! Check ${SERVER_LOG} for details."
    exit 1
fi
echo ""

# Step 5: Quick verification
echo "--- Step 5: Verification ---"
${ARIABC_DIR}/src/bin/psql/psql -p 5438 -d postgres -U postgres -c "SELECT version();" 2>&1 || true
echo ""

echo "========================================="
echo "Rebuild completed at: $(date)"
echo "========================================="
