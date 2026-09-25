"""End-to-end contract tests against the repository's custom PostgreSQL binary."""
from __future__ import annotations

import json
import os
import threading
import unittest
from urllib.request import Request, urlopen
from urllib.error import HTTPError

import psycopg

from dynamic_merkle_visualizer.app import VisualizerServer
from dynamic_merkle_visualizer.db import Inspector, InspectorError, Settings, coordinate
from dynamic_merkle_visualizer.demo import DemoCluster


class PrefixValidation(unittest.TestCase):
    def test_valid_and_invalid_prefixes(self):
        self.assertEqual(coordinate("ab00000000000000", 8), (bytes.fromhex("ab00000000000000"), 8))
        for node, bits in (("zz00000000000000", 8), ("ab00000000000000", 0),
                           ("ab00000000000000", 61), ("0000000000000001", 4)):
            with self.assertRaises(InspectorError): coordinate(node, bits)


@unittest.skipUnless(os.getenv("MERKLE_VIZ_TEST_LIVE") == "1", "set MERKLE_VIZ_TEST_LIVE=1 to start an isolated custom PostgreSQL cluster")
class LiveDatabaseContract(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.cluster = DemoCluster(rows=512).start()
        cls.settings = Settings(cls.cluster.dsn, "public.merkle_demo", "public.merkle_demo_idx", True)
        cls.inspector = Inspector(cls.settings)
        cls.server = VisualizerServer(("127.0.0.1", 0), cls.inspector)
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        cls.base = f"http://127.0.0.1:{cls.server.server_address[1]}"
        cls.catalog = cls.get("/api/catalog")

    @classmethod
    def tearDownClass(cls):
        if hasattr(cls, "server"):
            cls.server.shutdown(); cls.server.server_close(); cls.thread.join(timeout=2)
        if hasattr(cls, "cluster"):
            cls.cluster.stop()

    @classmethod
    def get(cls, path, params=None):
        from urllib.parse import urlencode
        url=cls.base+path+("?"+urlencode(params or {}) if params else "")
        with urlopen(url, timeout=25) as response:return json.load(response)

    @classmethod
    def post(cls, data):
        request=Request(cls.base+"/api/mutate", data=json.dumps(data).encode(), method="POST",
                        headers={"Content-Type":"application/json","Origin":cls.base,
                                 "X-Merkle-Token":cls.catalog["token"]})
        with urlopen(request, timeout=25) as response:return json.load(response)

    def test_native_nodes_and_heap_audit(self):
        catalog=self.get("/api/catalog")
        self.assertEqual(len(catalog["indexes"]),1)
        oid=catalog["indexes"][0]["index_oid"]
        combined=[]
        for partition in range(4):
            snapshot=self.get("/api/snapshot",{"index_oid":oid,"partition":partition,"verify":"1"})
            self.assertTrue(snapshot["verification"])
            self.assertTrue(all(len(n["hash"])==64 and len(n["node_id"])==16 for n in snapshot["nodes"]))
            combined.extend(snapshot["nodes"])
            # Compare every displayed record to the exact dedicated SQL relation.
            tbl_rel = f"merkle_node_{catalog['indexes'][0]['table_name']}"
            with psycopg.connect(self.cluster.dsn) as conn:
                chk = conn.execute("SELECT to_regclass(%s) IS NOT NULL", (f"ariabc_internal.{tbl_rel}",)).fetchone()[0]
                if not chk:
                    tbl_rel = f"merkle_node_{oid}"
                rows=conn.execute(psycopg.sql.SQL("SELECT partition_id,prefix_len,is_leaf,tuple_count,encode(node_id,'hex'),encode(hash,'hex') FROM {} WHERE partition_id=%s ORDER BY prefix_len,node_id").format(psycopg.sql.Identifier("ariabc_internal", tbl_rel)),(partition,)).fetchall()
            actual={(r[4],r[1]):(r[2],r[3],r[5]) for r in rows}
            observed={(n["node_id"],n["prefix_len"]):(n["is_leaf"],n["tuple_count"],n["hash"]) for n in snapshot["nodes"]}
            self.assertEqual(observed,actual)
        stats=self.get("/api/snapshot",{"index_oid":oid})["stats"]
        self.assertEqual(len(combined),stats["total_nodes"])

    def test_dml_drives_native_splits_and_merges(self):
        with psycopg.connect(self.cluster.dsn,autocommit=True) as conn:
            before=conn.execute("SELECT (merkle_tree_stats('public.merkle_demo')::json->>'total_nodes')::int").fetchone()[0]
            conn.execute("INSERT INTO public.merkle_demo(id,payload) SELECT n,'split-'||n FROM generate_series(1000000,1000999)n")
            after,verified=conn.execute("SELECT (merkle_tree_stats('public.merkle_demo')::json->>'total_nodes')::int,merkle_verify('public.merkle_demo')").fetchone()
            self.assertGreater(after,before,"real SQL INSERTs should grow the dynamic tree")
            self.assertTrue(verified)
            conn.execute("DELETE FROM public.merkle_demo WHERE id BETWEEN 1000000 AND 1000999")
            merged,verified=conn.execute("SELECT (merkle_tree_stats('public.merkle_demo')::json->>'total_nodes')::int,merkle_verify('public.merkle_demo')").fetchone()
        self.assertLess(merged,after,"real SQL DELETEs should merge eligible tree nodes")
        self.assertTrue(verified)

    def test_real_dml_updates_hashes_and_rollback_is_invisible(self):
        oid=self.cluster.index_oid
        before=self.get("/api/snapshot",{"index_oid":oid})["root"]
        committed=self.post({"index_oid":oid,"operation":"update","row":{"ctid":"(0,1)","xmin":"1","hash":"0"*64},"values":{"payload":"unused"}}) if False else None
        with psycopg.connect(self.cluster.dsn) as conn:
            row=conn.execute("SELECT ctid::text,xmin::text,encode(merkle_tuple_hash(t),'hex') FROM public.merkle_demo t WHERE id=1").fetchone()
        rolled=self.post({"index_oid":oid,"operation":"update","row":{"ctid":row[0],"xmin":row[1],"hash":row[2]},"values":{"payload":"rollback-check"},"rollback":True})
        self.assertTrue(rolled["rolled_back"])
        self.assertEqual(before,self.get("/api/snapshot",{"index_oid":oid})["root"])
        with psycopg.connect(self.cluster.dsn) as conn:
            row=conn.execute("SELECT ctid::text,xmin::text,encode(merkle_tuple_hash(t),'hex') FROM public.merkle_demo t WHERE id=1").fetchone()
        changed=self.post({"index_oid":oid,"operation":"update","row":{"ctid":row[0],"xmin":row[1],"hash":row[2]},"values":{"payload":"committed-check"}})
        self.assertTrue(changed["committed"])
        after=self.get("/api/snapshot",{"index_oid":oid,"verify":"1"})
        self.assertTrue(after["verification"])
        self.assertNotEqual(before,after["root"])

    def test_stale_row_fails_closed_and_insert_is_native(self):
        oid=self.cluster.index_oid
        rows=self.get("/api/rows",{"index_oid":oid})["rows"]
        stale=rows[0]
        with psycopg.connect(self.cluster.dsn,autocommit=True) as conn:
            conn.execute("UPDATE public.merkle_demo SET payload='external-change' WHERE ctid=%s::tid",(stale["ctid"],))
        request=Request(self.base+"/api/mutate",data=json.dumps({"index_oid":oid,"operation":"update","row":stale,"values":{"payload":"must-not-win"}}).encode(),method="POST",headers={"Content-Type":"application/json","Origin":self.base,"X-Merkle-Token":self.catalog["token"]})
        with self.assertRaises(HTTPError) as caught:urlopen(request,timeout=25)
        self.assertEqual(caught.exception.code,409)
        result=self.post({"index_oid":oid,"operation":"insert","values":{"id":"900001","payload":"native-insert","revision":"0","note":None}})
        self.assertTrue(result["committed"])
        snapshot=self.get("/api/snapshot",{"index_oid":oid,"verify":"1"})
        self.assertTrue(snapshot["verification"])

    def test_served_page_and_script_are_local_application_assets(self):
        with urlopen(self.base+"/",timeout=10) as response:
            page=response.read().decode()
        with urlopen(self.base+"/app.js",timeout=10) as response:
            script=response.read().decode()
        self.assertIn("Live Merkle Inspector",page)
        self.assertIn('api("/api/snapshot"',script)
        self.assertIn('api("/api/mutate"',script)


if __name__ == "__main__": unittest.main()
