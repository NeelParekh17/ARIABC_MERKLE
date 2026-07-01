import psycopg
import json
import sys

def main():
    conninfo = "host=127.0.0.1 port=5438 dbname=postgres user=postgres"

    # 1. Connect and initialize BCDB
    with psycopg.connect(conninfo, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT bcdb_init(True, 4);")
            print("BCDB initialized:", cur.fetchone())

    # 2. Start a transaction block to probe transaction boundary
    with psycopg.connect(conninfo, autocommit=False) as conn:
        with conn.cursor() as cur:
            # Get initial value
            cur.execute("SELECT field1 FROM usertable_small WHERE ycsb_key = 50;")
            initial_val = cur.fetchone()[0]
            print(f"Initial value of field1 for ycsb_key=50: '{initial_val}'")

            # Prepare block submit payload
            payload = json.dumps({
                "bid": 2,
                "txs": [{
                    "hash": "probe_hash_9999",
                    "sql": "UPDATE usertable_small SET field1 = 'probe_val_new' WHERE ycsb_key = 50;"
                }]
            }, separators=(",", ":"))

            print("Submitting block within transaction...")
            cur.execute("SELECT bcdb_block_submit_results(%s);", (payload,))
            result = cur.fetchone()[0]
            print("Block submit result:", result)

            print("Rolling back transaction...")
            conn.rollback()

    # 3. Check if target row was modified despite the rollback
    with psycopg.connect(conninfo, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT field1 FROM usertable_small WHERE ycsb_key = 50;")
            final_val = cur.fetchone()[0]
            print(f"Final value of field1 for ycsb_key=50: '{final_val}'")

            if final_val == 'probe_val_new':
                print("\n[RESULT] The row WAS changed to the new value. ROLLBACK did NOT remove the user-table change.")
                print("Decision: The actual commit is happening inside the BCDB worker execution. Implement the ledger inside the PostgreSQL backend transaction path.")
            else:
                print("\n[RESULT] The row was NOT changed (or stayed as initial value). ROLLBACK DID remove the user-table change.")
                print("Decision: Ledger wrapping can initially live in pg_executor.")

if __name__ == "__main__":
    main()
