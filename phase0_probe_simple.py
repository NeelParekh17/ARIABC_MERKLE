import psycopg
import json

def main():
    conninfo = "host=127.0.0.1 port=5438 dbname=postgres user=postgres"

    # Get initial value using a clean connection
    with psycopg.connect(conninfo, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT field1 FROM usertable_small WHERE ycsb_key = 50;")
            initial_val = cur.fetchone()[0]
            print(f"Initial value of field1: '{initial_val}'")

    # Start a transaction block and do NOT run any other queries in it except block submit
    with psycopg.connect(conninfo, autocommit=False) as conn:
        with conn.cursor() as cur:
            # Prepare block submit payload
            payload = json.dumps({
                "bid": 2,
                "txs": [{
                    "hash": "probe_hash_simple",
                    "sql": f"UPDATE usertable_small SET field1 = 'simple_val_new' WHERE ycsb_key = 50;"
                }]
            }, separators=(",", ":"))

            print("Submitting block within transaction block (autocommit=False)...")
            cur.execute("SELECT bcdb_block_submit_results(%s);", (payload,))
            result = cur.fetchone()[0]
            print("Block submit result:", result)

            print("Rolling back transaction...")
            conn.rollback()

    # Check final value
    with psycopg.connect(conninfo, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT field1 FROM usertable_small WHERE ycsb_key = 50;")
            final_val = cur.fetchone()[0]
            print(f"Final value of field1: '{final_val}'")

            if final_val == 'simple_val_new':
                print("\n[RESULT] The row WAS changed to the new value. ROLLBACK did NOT remove the user-table change.")
            else:
                print("\n[RESULT] The row was NOT changed. ROLLBACK DID remove the user-table change.")

if __name__ == "__main__":
    main()
