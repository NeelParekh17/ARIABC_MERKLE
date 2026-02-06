#!/bin/bash

# ==========================================
# CONFIGURATION
# ==========================================
TABLE_NAME="usertable"
ID_COL_NAME="ycsb_key"        # The name of your Primary Key column
DATA_COL_NAME="field1"  # The name of the text column you want to update
START_ID=12001          # The ID to start counting from

# ==========================================
# CLEANUP
# ==========================================
echo "Cleaning up old SQL files..."
rm -f insert.sql update.sql delete.sql

# ==========================================
# 1. GENERATE INSERT (10k rows)
# Sequence: 00000000 -> 00009999
# ==========================================
echo "Generating insert.sql (10,000 rows)..."
for i in {0..9999}; do
  SEQ=$(printf "%08d" $i)
  ID=$((START_ID + i))
  
  # Note: Using explicit column names is more robust than 'VALUES (x,y)'
  echo "s $SEQ insert into $TABLE_NAME values ($ID, 'initial_data');" >> insert.sql
done

# ==========================================
# 2. GENERATE UPDATE (10k rows)
# Sequence: 00010000 -> 00019999
# ==========================================
echo "Generating update.sql (10,000 updates)..."
for i in {0..9999}; do
  # Offset sequence by 10,000 so it continues after inserts
  SEQ_VAL=$((10000 + i)) 
  SEQ=$(printf "%08d" $SEQ_VAL)
  ID=$((START_ID + i))
  
  echo "s $SEQ update $TABLE_NAME set $DATA_COL_NAME = 'updated_data' where $ID_COL_NAME = $ID;" >> update.sql
done

# ==========================================
# 3. GENERATE DELETE (10k rows)
# Sequence: 00020000 -> 00029999
# ==========================================
echo "Generating delete.sql (10,000 deletes)..."
for i in {0..9999}; do
  # Offset sequence by 20,000
  SEQ_VAL=$((20000 + i))
  SEQ=$(printf "%08d" $SEQ_VAL)
  ID=$((START_ID + i))
  
  echo "s $SEQ delete from $TABLE_NAME where $ID_COL_NAME = $ID;" >> delete.sql
done

echo "Success! 3 files generated."
echo "To run them, execute:"
echo "  bcpsql < insert.sql"
echo "  bcpsql < update.sql"
echo "  bcpsql < delete.sql"