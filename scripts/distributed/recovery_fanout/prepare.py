#!/usr/bin/env python3
"""Prepare an isolated fanout campaign from the reviewed recovery harness.

Run on .247 after staging the source snapshot. Does not build or start services.
"""
import argparse
import difflib
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
from importlib.metadata import version

OLD = '/home/neel/ariabc_data/paper_20261010_recov'
OLD_HEAD = '16577072ecf888fbbbb5d80d0e90fc340c8ca342'


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--base', type=Path, required=True)
    p.add_argument('--head', required=True)
    a = p.parse_args()
    base = a.base.resolve()
    if not re.fullmatch(r'/home/neel/ariabc_data/fanout_[A-Za-z0-9_]+', str(base)):
        p.error('fresh /home/neel/ariabc_data/fanout_TAG required')
    if '10.129.148.247' not in subprocess.check_output(['hostname', '-I'], text=True).split():
        p.error('prepare only on lab .247')
    repo = base / 'repo'
    archived = repo / 'Final_Results/ONLINE_RECOVERY/paper_20261010/harness/preparation'
    tag = base.name
    config = dict(tag=tag, source_head=a.head, sizes=[12000, 4000000],
                  fanouts=[16, 32, 64, 128, 256], scenarios=['A', 'B', 'M_passive', 'M_mixed', 'L_mix'],
                  repetitions=3, transactions=160000, partitions=200, split=1024, merge=256,
                  gateway_host='10.129.148.247', replica_hosts=['10.129.148.247', '10.129.148.246', '10.129.148.248'],
                  fault_rows=100, fault_delay_seconds=5, fault_seed=20261013,
                  workload_seed=42, noise_rule='retain and stop if any host swaps >64MiB during measured interval')
    with (base / 'campaign.json').open('x') as f:
        f.write(json.dumps(config, indent=2) + '\n')
    for d in ('scratch', 'provenance', 'results', 'tools', 'pids', 'logs', 'image', 'health'):
        (base / d).mkdir(exist_ok=True)
    generated = base / 'provenance/workstation_config_headers'
    generated.mkdir()
    for header in (repo / 'src/include').glob('pg_config*.h'):
        if header.name != 'pg_config_manual.h':
            header.rename(generated / header.name)

    def save(name, text):
        with (base / name).open('x') as f:
            f.write(text)

    def adapt(name):
        return (archived / name).read_text().replace(OLD, str(base)).replace(OLD_HEAD, a.head)

    save('python_dependencies.txt', 'psycopg==' + version('psycopg') + '\npexpect==' + version('pexpect') + '\n')

    # No private-key copying or account changes: use the same password source
    # as the existing cluster harness via OpenSSH's separate askpass channel.
    askpass = r'''#!/usr/bin/env python3
import os,re
from pathlib import Path
p=Path(__file__).resolve().parents[1]/'repo/scripts/distributed/run_4node_raft_cluster.sh'
s=p.read_text()
m=re.search(r'ARIABC_CLUSTER_PASSWORD="\$\{ARIABC_CLUSTER_PASSWORD:-(.*?)\}"',s)
if not m:raise SystemExit('missing cluster authentication setting')
print(os.environ.get('ARIABC_CLUSTER_PASSWORD') or m.group(1))
'''
    save('tools/askpass', askpass)
    save('tools/ssh', '''#!/usr/bin/env bash
set -euo pipefail
export SSH_ASKPASS="''' + str(base / 'tools/askpass') + '''" SSH_ASKPASS_REQUIRE=force DISPLAY=:0
args=()
for arg in "$@"; do
 [[ "$arg" == BatchMode=yes ]] && arg=BatchMode=no
 args+=("$arg")
done
exec setsid -w /usr/bin/ssh -o NumberOfPasswordPrompts=1 -o StrictHostKeyChecking=accept-new "${args[@]}"
''')
    (base / 'tools/ssh').chmod(0o700)
    (base / 'tools/askpass').chmod(0o700)

    # Reuse the existing ABI-compatible dependency and portable compiler tools.
    for name in ('rdkafka',):
        shutil.copytree(Path(OLD) / name, base / name, symlinks=True)
    shutil.copytree(Path(OLD) / 'tools/cmake-portable', base / 'tools/cmake-portable', symlinks=True)
    save('build_inside.sh', adapt('build_inside.sh').replace(
        'git rev-parse HEAD > "$B/provenance/git_head.txt"',
        'printf "%s\\n" "' + a.head + '" > "$B/provenance/git_head.txt"').replace(
        'git status --porcelain > "$B/provenance/git_status_before.txt"',
        'cp "$B/provenance/workspace_status.txt" "$B/provenance/git_status_before.txt"').replace(
        'git status --porcelain > "$B/provenance/git_status_after.txt"',
        'cp "$B/provenance/workspace_status.txt" "$B/provenance/git_status_after.txt"'))

    # Generate the isolated runner using the already audited substitutions.
    prepare = adapt('prepare_campaign.py').replace("tag = 'paper_20261010_recov'", 'tag = ' + repr(tag))
    save('prepare_runner_campaign_driver.py', prepare)
    subprocess.run(['python3', str(base / 'prepare_runner_campaign_driver.py')], check=True)
    runner = repo / ('scripts/distributed/recovery_s1024/cluster_runner_' + tag + '.sh')
    before = runner.read_text()
    # Prevent large full-table CSV diagnostics from filling the constrained disks.
    after = before.replace('DUMP_VERIFY_CSV="${DUMP_VERIFY_CSV:-1}"', 'DUMP_VERIFY_CSV="${DUMP_VERIFY_CSV:-0}"')
    # All SSH/rsync uses the campaign askpass wrapper, including collectors
    # that otherwise silently ignore missing sshpass on the controller.
    after = after.replace('sshpass -p "$CLUSTER_PASSWORD" ', '')
    # Stream the independent full-heap audit. string_agg creates an ~800MB
    # value for 4M rows, with additional copies during aggregation/sorting.
    old_digest = next(line for line in after.splitlines()
                      if 'data_md5=' in line and 'SELECT md5(string_agg' in line)
    new_digest = r'''        data_md5=\$(set -o pipefail; \$INSTALL_DIR/bin/psql -X -q -h 127.0.0.1 -p $DB_PORT -U $DB_USER $DB_NAME -v ON_ERROR_STOP=1 -c \"COPY (SELECT ycsb_key, field1, field2, field3, field4, field5, field6, field7, field8, field9, field10 FROM $VERIFY_TABLE ORDER BY ycsb_key) TO STDOUT WITH (FORMAT binary)\" | sha256sum | cut -d ' ' -f1) || exit 1'''
    after = after.replace(old_digest, new_digest).replace('data_md5=$data_md5', 'data_sha256=$data_md5')
    anchor = 'log "=== Phase 4: Starting ariabc_pg_server on all ${#NODE_IDS[@]} nodes ==="'
    assert after.count(anchor) == 1
    after = after.replace(anchor, '''for idx in "${!NODE_IDS[@]}"; do
  node_ssh "$idx" "LD_LIBRARY_PATH=''' + str(base / 'install/lib') + ''' ''' + str(base / 'install/bin/psql') + ''' -X -h 127.0.0.1 -p '$DB_PORT' -U postgres postgres -f ''' + str(base / 'geometry.sql') + '''" > "$LOG_DIR/geometry_before_${NODE_IPS[$idx]}.txt"
done
''' + anchor)
    runner.write_text(after)
    save('fanout_runner.diff', ''.join(difflib.unified_diff(before.splitlines(True), after.splitlines(True))))
    subprocess.run(['bash', '-n', str(runner)], check=True)

    # Stable indexed fault targets avoid ORDER BY random() heap scans on 4M rows.
    injector = runner.parent / 'serializable_fault_injector.py'
    before = injector.read_text()
    after = before.replace('import sys\n', 'import os\nimport sys\n', 1)
    anchor = '    node = NodeConnection(\n'
    after = after.replace(anchor, '''    fault_rows = int(os.environ['FANOUT_ROWS'])
    fault_rng = random.Random(int(os.environ['FANOUT_FAULT_SEED']))
    chosen = fault_rng.sample(range(1, fault_rows + 1), num_rows)
    print('CONTROLLED_FAULT_KEYS ' + ','.join(map(str, chosen)), flush=True)
''' + anchor, 1)
    after = after.replace('f"SELECT ycsb_key FROM {table} ORDER BY random() LIMIT %s",\n                                (n_update,),',
                          'f"SELECT ycsb_key FROM {table} WHERE ycsb_key = ANY(%s) ORDER BY ycsb_key",\n                                (chosen[:n_update],),')
    after = after.replace('f"SELECT ycsb_key FROM {table} WHERE ycsb_key != ALL(%s) ORDER BY random() LIMIT %s",\n                                    (updated_keys, n_delete),',
                          'f"SELECT ycsb_key FROM {table} WHERE ycsb_key = ANY(%s) ORDER BY ycsb_key",\n                                    (chosen[n_update:n_update+n_delete],),')
    after = after.replace('f"SELECT ycsb_key FROM {table} ORDER BY random() LIMIT %s",\n                                    (n_delete,),',
                          'f"SELECT ycsb_key FROM {table} WHERE ycsb_key = ANY(%s) ORDER BY ycsb_key",\n                                    (chosen[:n_delete],),')
    after = after.replace('random.randint(1, 1000)', 'fault_rng.randint(1, 1000)')
    assert 'ORDER BY random() LIMIT' not in after
    injector.write_text(after)
    save('controlled_injector.diff', ''.join(difflib.unified_diff(before.splitlines(True), after.splitlines(True))))

    for name in ('stop_server.sh', 'host_sample.sh', 'host_probe.py', 'init_node.sh'):
        text = adapt(name)
        if name == 'init_node.sh':
            text = text.replace('git -C "$B/repo" rev-parse HEAD', 'cat "$B/provenance/git_head.txt"').replace(
                'git -C "$B/repo" status --porcelain', 'cat "$B/provenance/workspace_status.txt"')
        save(name, text)
    health = adapt('health_watch.py').replace(
        "hosts=['10.129.148.247','10.129.148.246','10.129.148.248','10.129.27.111']",
        "hosts=['10.129.148.247','10.129.148.246','10.129.148.248']").replace("host.endswith('.111')", "host.endswith('.247')")
    save('health_watch.py', health)
    geom = adapt('geometry.sql')
    start = geom.index('SELECT format($query$')
    end = geom.index("SELECT merkle_root_hash", start)
    geom = geom[:start] + '''SELECT count(*) AS nodes,
 count(*) FILTER (WHERE is_leaf) AS leaves,
 count(*) FILTER (WHERE NOT is_leaf) AS internal_nodes,
 max(prefix_len) AS max_prefix_bits,
 avg(tuple_count) FILTER (WHERE is_leaf AND tuple_count > 0) AS avg_nonempty_occupancy,
 max(tuple_count) FILTER (WHERE is_leaf) AS max_occupancy,
 pg_total_relation_size('ariabc_internal.merkle_node_usertable_small'::regclass) AS node_storage_bytes
FROM ariabc_internal.merkle_node_usertable_small;
SELECT prefix_len, is_leaf, count(*) AS nodes, sum(tuple_count) AS tuple_count_sum
FROM ariabc_internal.merkle_node_usertable_small GROUP BY prefix_len, is_leaf ORDER BY prefix_len, is_leaf;
''' + geom[end:]
    save('geometry.sql', geom)

    run = adapt('run_case.sh').replace('paper_20261010_recov', tag)
    run = run.replace('CASE=${1:?}; REP=${2:?}; ATTEMPT=${3:?}; STAGE=${4:?}',
                      'CASE=${1:?}; REP=${2:?}; ATTEMPT=${3:?}; STAGE=${4:?}; ROWS=${5:?}; FANOUT=${6:?}\n'
                      'export FANOUT_ROWS="$ROWS" FANOUT_FAULT_SEED="$((20261013+REP))"\n'
                      '[[ "$ROWS" == 12000 || "$ROWS" == 4000000 ]]\n'
                      '[[ "$FANOUT" =~ ^(16|32|64|128|256)$ ]]\n'
                      'export PATH="$B/tools:$PATH"\n'
                      'python3 "$B/select_restore.py" "$ROWS"')
    run = run.replace('10.129.27.111', '10.129.148.247')
    run = run.replace('${STAGE}_${CASE}_r${REP}_a${ATTEMPT}', '${STAGE}_n${ROWS}_f${FANOUT}_${CASE}_r${REP}_a${ATTEMPT}')
    run = run.replace('recov_20261010_', tag + '_')
    run = run.replace('NODE_SSH_COMMAND_TIMEOUT=30', 'NODE_SSH_COMMAND_TIMEOUT=300')
    run = run.replace('export POLL_COUNT=', 'export MERKLE_DRAIN_SSH_TIMEOUT=300\nexport POLL_COUNT=')
    run = run.replace('--merkle-fanout 32', '--merkle-fanout "$FANOUT"')
    run = run.replace('ycsb_workload_a_skew_0_00_160k.txt', 'ycsb_a_n${ROWS}_160k.txt')
    run = run.replace("group=f32s1024\\nfanout=32\\nsplit=1024", 'group=n%s_f%s\\nfanout=%s\\nrows=%s\\nsplit=1024')
    run = run.replace('"$CASE" "$REP" "$ATTEMPT" "$STAGE" "$rc" >',
                      '"$CASE" "$ROWS" "$FANOUT" "$FANOUT" "$ROWS" "$REP" "$ATTEMPT" "$STAGE" "$rc" >')
    run = run.replace('"$B/python_dependencies.txt"', '"$B/python_dependencies.txt" "$B/controlled_injector.diff" "$B/campaign.json"')
    save('run_case.sh', run)

    collector = adapt('collect_run.py')
    collector = collector.replace("fails=[]", "fails=[]\nrows=int(case['rows'])\nfanout=int(case['fanout'])")
    collector = collector.replace("parts[0]!='12001'", "parts[0]!=str(rows+1)")
    collector = collector.replace("('fanout','32')", "('fanout',str(fanout))")
    collector = collector.replace("'10.129.148.248','10.129.27.111'", "'10.129.148.248'")
    collector = collector.replace("host.endswith('.111')", "host.endswith('.247')")
    collector = collector.replace("r.update({'stage':", "r.update({'rows':rows,'fanout':fanout,'stage':")
    collector = collector.replace("samples=[]", """for node in ('1_admin123','2_user4','4_utkarsh'):
    profile=cmp.read_text(out/('server_node'+node+'.log'))
    if 'PROFILE_RAFT_STORAGE ' not in profile:fails.append(node+' missing Raft storage profile')
if fault and len(events)!=1:fails.append('expected exactly one controlled recovery event')
if str(r['full_copies'])!='0':fails.append('full-table copy fallback')
if rows > 12000:
    for host in ('10.129.148.247','10.129.148.246','10.129.148.248'):
        g=cmp.read_text(out/('geometry_'+host+'.txt'))
        if not re.search(r'\\n\\s*0\\s*\\|\\s*f\\s*\\|',g):fails.append(host+' no internal tree level')
samples=[]""")
    save('collect_run.py', collector)
    shutil.copyfile(repo / 'scripts/distributed/recovery_fanout/controller.py', base / 'controller.py')

    # Expand the original fixture before index construction; keep original rows.
    original_restore = (repo / 'scripts/restore_usertable_small.sql').read_text()
    marker = 'ALTER TABLE ONLY public.usertable_small\n    ADD CONSTRAINT'
    assert original_restore.count(marker) == 1
    for n in config['sizes']:
        restore = original_restore
        if n > 12000:
            cols = ', '.join("left(md5(k::text || ':%d'),20)" % i for i in range(1, 11))
            restore = restore.replace(marker, 'INSERT INTO public.usertable_small SELECT k, ' + cols +
                                      f' FROM generate_series(12001,{n}) AS k;\n\n' + marker)
        save(f'restore_n{n}.sql', restore)
    save('select_restore.py', '''#!/usr/bin/env python3
import os,shutil,subprocess,sys
from pathlib import Path
B=Path(__file__).resolve().parent
n=int(sys.argv[1]);assert n in (12000,4000000)
p=B/'repo/scripts/restore_usertable_small.sql'
shutil.copyfile(B/f'restore_n{n}.sql',p)
for h in ('10.129.148.246','10.129.148.248'):
 subprocess.run(['rsync','-a',str(p),f'neel@{h}:{p}'],check=True)
''')
    workloads = repo / 'scripts/ycsb_recovery'
    workloads.mkdir(exist_ok=True)
    shutil.copyfile(workloads / 'ycsb_workload_a_skew_0_00_160k.txt', workloads / 'ycsb_a_n12000_160k.txt')
    # Preserve exact statement operations/values, remap only primary keys with
    # a fixed seed. No giant Zipf probability vector or generated 4M-row dump.
    import random
    rng = random.Random(42)
    with (workloads / 'ycsb_a_n4000000_160k.txt').open('x') as f:
        for line in (workloads / 'ycsb_a_n12000_160k.txt').read_text().splitlines():
            rewritten, count = re.subn(r'(?i)(WHERE YCSB_KEY=)\d+', lambda m: m[1] + str(rng.randint(1, 4000000)), line)
            assert count == 1
            f.write(rewritten + '\n')
    with (base / 'provenance/experiment_inputs.sha256').open('x') as f:
        for path in [base / f'restore_n{n}.sql' for n in config['sizes']] + [workloads / f'ycsb_a_n{n}_160k.txt' for n in config['sizes']]:
            f.write(hashlib.sha256(path.read_bytes()).hexdigest() + '  ' + str(path) + '\n')
    subprocess.run(['bash', '-n', str(base / 'run_case.sh')], check=True)
    print(json.dumps(config, indent=2))


if __name__ == '__main__':
    main()
