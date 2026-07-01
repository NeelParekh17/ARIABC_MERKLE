import psycopg
import json

def main():
    conninfo = "host=127.0.0.1 port=5438 dbname=postgres user=postgres"

    with psycopg.connect(conninfo, autocommit=True) as conn:
        with conn.cursor() as cur:
            # Check initial value
            cur.execute("SELECT field1 FROM usertable_small WHERE ycsb_key = 50;")
            initial_val = cur.fetchone()[0]
            print(f"Initial value: '{initial_val}'")

            # Prepare block submit payload
            payload = json.dumps({
                "bid": 2,
                "txs": [{
                    "hash": "probe_hash_normal",
                    "sql": "UPDATE usertable_small SET field1 = 'normal_val_new' WHERE ycsb_key = 50;"
                }]
            }, separators=(",", ":"))

            print("Submitting block (autocommit=True)...")
            cur.execute("SELECT bcdb_block_submit_results(%s);", (payload,))
            result = cur.fetchone()[0]
            print("Block submit result:", result)

            # Check final value
            cur.execute("SELECT field1 FROM usertable_small WHERE ycsb_key = 50;")
            final_val = cur.fetchone()[0]
            print(f"Final value: '{final_val}'")

if __name__ == "__main__":
    main()
