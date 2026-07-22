#!/usr/bin/env python3
"""End-to-end acceptance test through the same HTTP API used by every UI button."""

from __future__ import annotations

import json
import os
import sys
import threading
import urllib.request
import urllib.error
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from app import ApiHandler, AppState, MerkleDatabase, ThreadingHTTPServer, prefix_bits  # noqa: E402

PORT = 8790
BASE = f"http://127.0.0.1:{PORT}"
ROOT = Path(__file__).resolve().parent.parent
WORKLOAD = ROOT / "scripts" / "ycsb-skew0-99-tx-20k-point-safedb-intkey-insert12k-uniq.txt"


def request(path: str, body=None):
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(BASE + path, data=data,
                                 headers={"Content-Type": "application/json"} if data else {})
    try:
        with urllib.request.urlopen(req, timeout=180) as response:
            return json.load(response) if response.headers.get_content_type() == "application/json" else response.read()
    except urllib.error.HTTPError as exc:
        raise RuntimeError(exc.read().decode()) from exc


def main() -> int:
    db = MerkleDatabase(os.environ.get(
        "MERKLE_VIZ_CONNINFO",
        "host=127.0.0.1 port=5438 dbname=postgres user=postgres",
    ))
    ApiHandler.db = db
    ApiHandler.state = AppState()
    server = ThreadingHTTPServer(("127.0.0.1", PORT), ApiHandler)
    created_source_table = False
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        assert b"Dynamic Merkle Lab" in request("/")

        # Dataset upload + Build native index button.
        small = "ycsb_key,field1\n1,one\n2,two\n"
        snap = request("/api/build", {"source": "upload", "format": "csv", "content": small, "config": {}})
        assert snap["verify"] and snap["row_count"] == 2

        # Make the clone-button test self-contained on a fresh PostgreSQL
        # cluster without replacing a user's existing benchmark table.
        with db.connect() as conn:
            source_exists = conn.execute(
                "SELECT to_regclass('public.usertable_small') IS NOT NULL AS ok"
            ).fetchone()["ok"]
        if not source_exists:
            source = request("/api/build", {"source": "canonical_restore", "config": {}})
            assert source["verify"] and source["row_count"] == 11994
            with db.connect() as conn:
                conn.execute("CREATE TABLE public.usertable_small AS TABLE merkle_viz.data")
                conn.execute("INSERT INTO public.usertable_small(ycsb_key,field1) VALUES (12001,'clone-conflict')")
                created_source_table = True

        # Clone-current button and incompatible workload preflight.
        snap = request("/api/build", {"source": "existing_usertable_small", "config": {}})
        loaded = request("/api/workload/load", {"content": WORKLOAD.read_text()})
        assert not loaded["compatible"] and loaded["conflicting_insert_count"] > 0
        assert loaded["next"]["partition"] is not None

        # Step must advance even if this post-workload clone eventually hits a
        # duplicate key. Find and execute through the first conflicting INSERT.
        conflict_key = loaded["conflicting_insert_keys"][0]
        statements = ApiHandler.state.workload
        conflict_at = next(i for i, statement in enumerate(statements)
                           if statement.upper().startswith("INSERT") and
                           db.statement_key(statement) == conflict_key)
        stepped = request("/api/workload/step", {"count": conflict_at + 1})
        assert stepped["cursor"] == conflict_at + 1
        assert any(event.get("error") for event in stepped["events"])

        # Canonical button, Load, Step, Run all: production 20/0 UI path.
        snap = request("/api/build", {"source": "canonical_restore", "config": {}})
        assert snap["row_count"] == 11994 and snap["verify"]
        loaded = request("/api/workload/load", {"content": WORKLOAD.read_text()})
        assert loaded["compatible"] and loaded["next"]["partition"] is not None
        stepped = request("/api/workload/step", {"count": 50})
        assert stepped["cursor"] == 50
        assert stepped["snapshot"]["row_count"] != 11994  # deletes/inserts are reflected immediately
        finished = request("/api/workload/run", {})
        final = finished["snapshot"]
        assert finished["done"] and final["verify"]
        assert final["row_count"] == 12498
        assert int(final["stats"]["split_count"]) == 20
        assert int(final["stats"]["merge_count"]) == 0
        assert final["stats"]["authority"] == "native_index_pages"
        assert int(final["stats"]["layout_version"]) == 5
        assert int(final["stats"]["logical_fanout"]) == 32
        assert int(final["stats"]["physical_node_fanout"]) == 2
        assert len(final["nodes"]) == int(final["stats"]["node_count"])
        node_ids = {node["id"] for node in final["nodes"]}
        assert all(node["parent_id"] in node_ids for node in final["nodes"]
                   if node["parent_id"] is not None)
        assert sum(1 for node in final["nodes"] if not node["is_leaf"]) == (
            int(final["stats"]["node_count"]) - int(final["stats"]["leaf_count"]))
        assert all(node["node_kind"] == "physical" and node["physical"]
                   and int(node["physical_depth"]) == int(node["prefix_len"])
                   for node in final["nodes"])

        # Logical view mirrors recovery localisation: one query-time level is
        # 32 MSB-first children (five bits), while stored topology stays binary.
        busy_partition = max((node for node in final["nodes"] if node["prefix_len"] == 0),
                             key=lambda node: int(node["tuple_count"]))["partition_id"]
        logical = request(f"/api/logical-tree?partition={busy_partition}&include_empty=true")
        logical_nodes = logical["nodes"]
        logical_summary = logical["summary"]
        assert logical_summary["logical_fanout"] == 32
        assert logical_summary["physical_node_fanout"] == 2
        assert any(node["logical_level"] == 1 for node in logical_nodes)
        logical_ids = {node["id"] for node in logical_nodes}
        assert all(node["node_kind"] == "logical_range" and not node["physical"]
                   for node in logical_nodes)
        assert all(node["parent_id"] in logical_ids for node in logical_nodes
                   if node["parent_id"] is not None)
        root = next(node for node in logical_nodes if node["prefix_len"] == 0)
        children = [node for node in logical_nodes if node["parent_id"] == root["id"]]
        assert len(children) == 32
        assert sum(int(node["tuple_count"]) for node in children) == int(root["tuple_count"])

        # Manual mutation buttons and live row-count refresh.
        inserted = request("/api/mutate", {"operation": "insert", "key": 2000000000,
                                           "fields": {"field1": "ui acceptance"}})
        assert inserted["row_count"] == 12499
        assert inserted["transition"]["operation"] == "insert"
        assert {"added_leaves", "removed_leaves", "split_delta", "merge_delta"} <= set(inserted["transition"])
        updated = request("/api/mutate", {"operation": "update", "key": 2000000000,
                                          "fields": {"field1": "updated"}})
        assert updated["row_count"] == 12499
        assert updated["transition"]["operation"] == "update"
        deleted = request("/api/mutate", {"operation": "delete", "key": 2000000000})
        assert deleted["row_count"] == 12498
        assert deleted["transition"]["operation"] == "delete"

        # Refresh, leaf selection/hash detail, and reset-counter buttons.
        refreshed = request("/api/snapshot")
        leaf = next(node for node in refreshed["nodes"] if node["is_leaf"])
        query = (f"/api/leaf-items?partition={leaf['partition_id']}"
                 f"&prefix_len={leaf['prefix_len']}&prefix_hex={leaf['prefix_hex']}")
        items = request(query)
        assert items and {"key_data_hex", "key_text", "route_digest", "tuple_hash"} <= set(items[0])

        # Find unused integer keys by their exact canonical native route
        # digest, then prove the generated row lands in the selected leaf.
        candidates = request("/api/leaf-key-candidates", {
            "partition_id": leaf["partition_id"], "prefix_len": leaf["prefix_len"],
            "prefix_hex": leaf["prefix_hex"], "count": 2, "max_attempts": 100000,
        })
        assert candidates["complete"] and len(candidates["matches"]) == 2
        generated = candidates["matches"][0]
        assert generated["partition_id"] == leaf["partition_id"]
        assert prefix_bits(generated["route_digest"], 256).startswith(leaf["prefix_bits"])
        generated_insert = request("/api/mutate", {
            "operation": "insert", "key": generated["key"], "fields": generated["fields"],
            "expected_leaf": {"partition_id": leaf["partition_id"],
                              "prefix_len": leaf["prefix_len"], "prefix_hex": leaf["prefix_hex"]},
        })
        assert generated_insert["row_count"] == refreshed["row_count"] + 1
        assert generated_insert["transition"]["selected_leaf_match"] is True
        generated_delete = request("/api/mutate", {"operation": "delete", "key": generated["key"]})
        assert generated_delete["row_count"] == refreshed["row_count"]

        # Generation may correctly split a near-capacity target. Reacquire an
        # extant leaf exactly as the UI's selection-following logic does.
        refreshed = generated_delete
        leaf = next(node for node in refreshed["nodes"]
                    if node["is_leaf"] and int(node["tuple_count"]) >= 2)
        query = (f"/api/leaf-items?partition={leaf['partition_id']}"
                 f"&prefix_len={leaf['prefix_len']}&prefix_hex={leaf['prefix_hex']}")
        items = request(query)

        # The Delete button rendered beside a leaf item uses this exact API
        # request and must immediately refresh the reported row count.
        leaf_delete_key = int(items[0]["key_text"])
        leaf_deleted = request("/api/mutate", {"operation": "delete", "key": leaf_delete_key})
        assert leaf_deleted["row_count"] == refreshed["row_count"] - 1
        assert leaf_deleted["verify"]
        assert leaf_deleted["transition"]["key"] == leaf_delete_key

        # The UI preserves this selected leaf and reloads its items, permitting
        # another immediate delete without making the user select it again.
        same_leaf = next((node for node in leaf_deleted["nodes"] if node["id"] == leaf["id"]), None)
        assert same_leaf is not None

        # The Insert row control embedded in this leaf sends the selected
        # native prefix. Reinsert the deleted key and require that exact route.
        leaf_inserted = request("/api/mutate", {
            "operation": "insert", "key": leaf_delete_key,
            "fields": {"field1": "leaf-targeted acceptance"},
            "expected_leaf": {
                "partition_id": leaf["partition_id"],
                "prefix_len": leaf["prefix_len"],
                "prefix_hex": leaf["prefix_hex"],
            },
        })
        assert leaf_inserted["row_count"] == refreshed["row_count"]
        assert leaf_inserted["verify"]
        assert leaf_inserted["transition"]["target_leaf"]["partition_id"] == leaf["partition_id"]

        again_deleted = request("/api/mutate", {"operation": "delete", "key": leaf_delete_key})
        assert again_deleted["row_count"] == leaf_inserted["row_count"] - 1
        remaining_items = request(query)
        assert remaining_items
        second_key = int(remaining_items[0]["key_text"])
        second_deleted = request("/api/mutate", {"operation": "delete", "key": second_key})
        assert second_deleted["row_count"] == again_deleted["row_count"] - 1
        assert second_deleted["verify"]

        # Focused native merge: five rows exceed capacity four and build a
        # split frontier; deleting down to threshold two must merge it again.
        merge_csv = "ycsb_key,field1\n" + "".join(f"{key},merge-{key}\n" for key in range(1, 6))
        merge_base = request("/api/build", {
            "source": "upload", "format": "csv", "content": merge_csv,
            "config": {"partitions": 1, "leaves_per_partition": 32,
                       "fanout": 32, "leaf_capacity": 4, "merge_threshold": 2,
                       "leaf_byte_capacity": 65536, "max_key_bytes": 1024,
                       "update_mode": "synchronous_cow"},
        })
        assert merge_base["verify"] and int(merge_base["stats"]["leaf_count"]) > 1
        merge_result = None
        for key in range(1, 6):
            merge_result = request("/api/mutate", {"operation": "delete", "key": key})
            if int(merge_result["transition"]["merge_delta"]) > 0:
                break
        assert merge_result is not None and int(merge_result["transition"]["merge_delta"]) > 0
        assert int(merge_result["stats"]["merge_count"]) > 0 and merge_result["verify"]

        reset = request("/api/reset-counters", {})
        assert int(reset["stats"]["split_count"]) == 0 and int(reset["stats"]["merge_count"]) == 0

        restored = request("/api/build", {"source": "canonical_restore", "config": {}})
        assert restored["verify"] and restored["row_count"] == 11994

        print(json.dumps({
            "ui_acceptance": "PASS", "workload_statements": finished["total"],
            "final_rows": final["row_count"], "splits": final["stats"]["split_count"],
            "merges": final["stats"]["merge_count"], "next_partition_preview": True,
            "manual_crud": "PASS", "leaf_hash_details": "PASS",
            "leaf_delete_button": "PASS", "repeated_leaf_delete": "PASS",
            "leaf_targeted_insert": "PASS", "native_key_generation": "PASS",
            "split_counter": "PASS", "merge_counter": "PASS",
            "internal_hierarchy": "PASS", "logical_fanout_view": "PASS",
            "native_fanout_contract": "PASS", "transition_metadata": "PASS"
        }, indent=2))
        return 0
    finally:
        server.shutdown()
        server.server_close()
        if created_source_table:
            with db.connect() as conn:
                conn.execute("DROP TABLE IF EXISTS public.usertable_small")


if __name__ == "__main__":
    raise SystemExit(main())
