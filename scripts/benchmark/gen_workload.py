import random
import string
import argparse
import sys

def generate_random_string(length=20):
    chars = string.ascii_letters + string.digits
    return ''.join(random.choice(chars) for _ in range(length))

def generate_insert(start_key, count):
    for i in range(count):
        key = start_key + i
        fields = [generate_random_string() for _ in range(10)]
        # match bash: INSERT INTO usertable (ycsb_key, field1...field10) VALUES ...
        field_cols = ", ".join([f"field{j+1}" for j in range(10)])
        field_vals = ", ".join([f"'{f}'" for f in fields])
        print(f"INSERT INTO usertable (ycsb_key, {field_cols}) VALUES ({key}, {field_vals});")

def generate_update(start_key, count, predictable=False):
    for i in range(count):
        key = start_key + i
        if predictable:
            print(f"UPDATE usertable SET field1='UPDATED-{key}' WHERE ycsb_key={key};")
        else:
            updates = []
            for j in range(10):
                updates.append(f"field{j+1}='{generate_random_string()}'")
            set_clause = ", ".join(updates)
            print(f"UPDATE usertable SET {set_clause} WHERE ycsb_key={key};")

def generate_delete(start_key, count):
    for i in range(count):
        key = start_key + i
        print(f"DELETE FROM usertable WHERE ycsb_key={key};")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("phase", choices=["insert", "update", "delete"])
    parser.add_argument("--start", type=int, default=13000)
    parser.add_argument("--count", type=int, default=10000)
    parser.add_argument("--predictable", action="store_true", help="Use predictable values for updates to enable verification")
    args = parser.parse_args()

    if args.phase == "insert":
        generate_insert(args.start, args.count)
    elif args.phase == "update":
        generate_update(args.start, args.count, args.predictable)
    elif args.phase == "delete":
        generate_delete(args.start, args.count)
