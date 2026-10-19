"""Local editing helper; no workloads or tests execute here."""
from pathlib import Path
src=Path('/work/ARIABC/AriaBC/.bench_tmp/f15_adhash_20261012/agents/f15/harness')
repo=Path('/work/ARIABC/AriaBC/.bench_tmp/lsm_lookup_20261014/repo')
for name in ['run_oom_100m_benchmark.py','benchmark_contract.py','benchmark_telemetry.py','benchmark_validation.py','device_settle_probe.py','large_zipf.py','oom_v2/paper_noise.py']:
 dst=repo/'scripts/distributed'/name;dst.parent.mkdir(exist_ok=True,parents=True);dst.write_bytes((src/'scripts/distributed'/name).read_bytes())
p=repo/'scripts/distributed/run_oom_100m_benchmark.py';s=p.read_text()
a=s.index('F15_PAPER =');b=s.index('\ndef pgdata(',a)
s=s[:a]+'''LSM_DATA = "/home/neel/ariabc_data/lsm_lookup_20261014"
LSM_WORKING = {v: LSM_DATA + "/pgdata_" + v for v in ("merkle", "lsm", "plain")}


def baseline_path(args, name=None):
    assert args.remote_dir == LSM_DATA
    return args.remote_dir + "/" + (name or args.base_dir_name)


def is_merkle(mode):
    return mode in ("merkle_btree", "merkle_lsm", "bcdb_merkle")

''' + s[b:]
s=s.replace('F15_WORKING','LSM_WORKING').replace('f15_adhash_20261012','lsm_lookup_20261014').replace('5368709120','42949672960')
s=s.replace('return "pg" if mode in PG_ISOLATION else mode','return "bcdb_merkle" if is_merkle(mode) else "bcdb_det" if mode == "det" else "pg" if mode in PG_ISOLATION else mode')
s=s.replace('return "merkle" if mode == "bcdb_merkle" else "plain"','return "lsm" if mode == "merkle_lsm" else "merkle" if is_merkle(mode) else "plain"')
s=s.replace('if args.reset_mode == "delta" and variant(mode) == "plain":\n        return args.base_dir_name + "_plain"','if variant(mode) in ("plain", "lsm"):\n        return args.base_dir_name + "_" + variant(mode)')
a=s.index('def f15_cases(');b=s.index('\ndef manifest_path_for',a)
s=s[:a]+'''def lsm_cases(args):
    points = [(wl, skew, workers) for wl, skew in args.combos for workers in args.workers]
    for wl, skew, workers, mode, trial in balanced_cases(points, args.modes, args.trials, args.seed):
        yield wl, skew, mode, workers, trial

''' + s[b:]
s=s.replace('f15_cases(args)','lsm_cases(args)')
s=s.replace('mode == "bcdb_merkle"','is_merkle(mode)').replace('mode != "bcdb_merkle"','not is_merkle(mode)').replace('row_data["mode"] == "bcdb_merkle"','is_merkle(row_data["mode"])')
s=s.replace('("bcdb_merkle", "bcdb_det")','("merkle_btree", "merkle_lsm", "det")')
s=s.replace('set(args.modes) - {"pg", "pg_rc", "bcdb_det", "bcdb_merkle"}','set(args.modes) - {"det", "merkle_btree", "merkle_lsm"}')
s=s.replace('parser.error("--modes supports pg, pg_rc, bcdb_det, bcdb_merkle")','parser.error("--modes supports det, merkle_btree, merkle_lsm (SERIALIZABLE only)")')
s=s.replace('for mode in ("pg", "pg_rc", "bcdb_det", "bcdb_merkle")','for mode in ("det", "merkle_btree", "merkle_lsm")')
s=s.replace('manifest = (getattr(args, \'_plain_manifest\', None) if base_name != args.base_dir_name\n                else getattr(args, \'_golden_manifest\', None))','manifest = args._manifests[base_name]')
s=s.replace('''        if args.reset_mode == "delta" and set(args.modes) - {"bcdb_merkle"}:
            args._plain_manifest = ensure_plain_baseline(args, golden)''','''        args._manifests = {args.base_dir_name: golden}
        args._plain_manifest = ensure_plain_baseline(args, golden)
        args._manifests[args.base_dir_name + "_plain"] = args._plain_manifest
        lsm_name = args.base_dir_name + "_lsm"
        lsm_manifest = json.loads(run_remote(args.remote_host, args.remote_user, "cat " + manifest_path_for(args, lsm_name)).stdout)
        identity = get_remote_baseline_identity(args, lsm_name)
        for k, value in identity.items():
            if lsm_manifest[k] != value: raise RuntimeError("LSM baseline identity changed: " + k)
        if lsm_manifest["root"] != golden["root"]: raise RuntimeError("Baseline roots differ")
        args._manifests[lsm_name] = lsm_manifest''')
s=s.replace("unix_socket_directories", "unix_socket_directories")
s=s.replace("default_transaction_isolation = '{isolation_for(mode)}'", "default_transaction_isolation = '{isolation_for(mode)}'\nshared_preload_libraries = 'lsm3'\nlsm3.top_index_size = 1024\nmax_worker_processes = 32\nunix_socket_directories = '/home/neel/claude_checks/lsm_lookup_20261014/scratch'")
s=s.replace('# F15 reuses only approved existing scratch copies; never seed a new full copy.\ntest -f {work}/PG_VERSION','# Only campaign-owned working copies may be seeded.')
s=s.replace('needed + 21474836480','needed + 42949672960')
s=s.replace('return f"/home/{args.gateway_user}/claude_ctl/lsm_lookup_20261014/workloads/oom_{digest}.sql"','return f"/home/{args.gateway_user}/claude_ctl/lsm_lookup_20261014/workloads/oom_{digest}.sql"')
# Save case order for an independent rotation audit.
s=s.replace('        for wl, skew, mode, workers, trial in cases:','''        (out_dir / "planned_cases.json").write_text(json.dumps(cases, indent=2) + "\\n")
        for wl, skew, mode, workers, trial in cases:''')
p.write_text(s)
