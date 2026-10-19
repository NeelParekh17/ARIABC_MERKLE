from pathlib import Path
p=Path('/work/ARIABC/AriaBC/.bench_tmp/lsm_lookup_20261014/repo/scripts/distributed/run_oom_100m_benchmark.py');s=p.read_text()
at=s.index('\ndef detailed_stats(')
s=s[:at]+'''
def lookup_plan_probe(args):
    key = pgdata(args)
    cached = getattr(args, '_lookup_probes', {})
    if key in cached: return cached[key]
    bounds = json.loads(sql(args, "SELECT row_to_json(t) FROM (SELECT partition_id,encode(node_id,'hex') lo,encode(merkle_node_upper_bound(node_id,prefix_len),'hex') hi FROM ariabc_internal.merkle_node_usertable WHERE partition_id=0 AND is_leaf AND prefix_len>0 ORDER BY node_id LIMIT 1)t"))
    lo,hi = bounds['lo'],bounds['hi']
    query = "SELECT merkle_key_hash(ycsb_key),merkle_tuple_hash(u.*) FROM usertable u WHERE merkle_partition_for_hash(merkle_key_hash(ycsb_key),$3)=$4 AND merkle_key_hash(ycsb_key) BETWEEN $1 AND $2"
    plan = sql(args, "PREPARE lookup_probe(bytea,bytea,int,smallint) AS " + query + "; EXPLAIN (COSTS OFF) EXECUTE lookup_probe(decode('" + lo + "','hex'),decode('" + hi + "','hex'),200,0)")
    cte = "WITH b(p,lo,hi) AS (SELECT u.p,u.lo,merkle_node_upper_bound(u.lo,u.l) FROM unnest(ARRAY[0]::int4[],ARRAY[decode('" + lo + "','hex')]::bytea[],ARRAY[10]::int4[])u(p,lo,l)) "
    # The actual bounds length is read rather than inferred from geometry.
    prefix = int(sql(args, "SELECT prefix_len FROM ariabc_internal.merkle_node_usertable WHERE partition_id=0 AND node_id=decode('" + lo + "','hex') AND is_leaf"))
    cte = cte.replace('ARRAY[10]::int4[]', 'ARRAY[' + str(prefix) + ']::int4[]')
    transfer = cte + "SELECT DISTINCT ON(t.ycsb_key) t.* FROM b JOIN usertable t ON merkle_partition_for_hash(merkle_key_hash(t.ycsb_key),200)=b.p AND merkle_key_hash(t.ycsb_key) BETWEEN b.lo AND b.hi ORDER BY t.ycsb_key"
    repair_plan = sql(args, "SET enable_hashjoin=off; SET enable_mergejoin=off; EXPLAIN (COSTS OFF) " + transfer)
    fingerprint = sql(args, "SELECT count(*),min(ycsb_key),max(ycsb_key),md5(string_agg(ycsb_key::text||':'||encode(merkle_tuple_hash(u.*),'hex'),',' ORDER BY ycsb_key)) FROM usertable u WHERE merkle_partition_for_hash(merkle_key_hash(ycsb_key),200)=0 AND merkle_key_hash(ycsb_key) BETWEEN decode('" + lo + "','hex') AND decode('" + hi + "','hex')")
    cached[key] = dict(bounds=bounds,prefix_len=prefix,split_plan=plan,repair_plan=repair_plan,range_fingerprint=fingerprint)
    args._lookup_probes = cached
    return cached[key]

''' + s[at:]
s=s.replace('    merkle_stats = None\n    if is_merkle(mode):', '    merkle_stats = None\n    lookup_probe = None\n    if is_merkle(mode):')
s=s.replace('        if merkle_stats["total_nodes"] <= 0:', '        lookup_probe = lookup_plan_probe(args)\n        if merkle_stats["total_nodes"] <= 0:')
s=s.replace('                merkle_stats=merkle_stats,\n                provenance=', '                merkle_stats=merkle_stats, lookup_probe=lookup_probe,\n                provenance=')
p.write_text(s)
