"""Mixed SERIALIZABLE pgbench scripts, with retries outside pre-14 pgbench."""
import collections
import concurrent.futures
import json
import os
from pathlib import Path
import random
import re
import subprocess
import time

import psycopg

R = Path(os.environ.get('MERKLE_LSM_TEST_ROOT', str(Path.home() / 'claude_checks/lsm_integration_20261019')))
O = R / 'evidence'
env = dict(os.environ, TMPDIR=str(R / 'scratch'), LD_LIBRARY_PATH=str(R / 'install/lib'),
           PGHOST=str(R / 'socket'), PGPORT='5494', PGUSER='postgres', PGDATABASE='postgres', LC_ALL='C')
c = psycopg.connect(f'host={R}/socket port=5494 user=postgres dbname=postgres client_encoding=UTF8', autocommit=True)
c.execute('DROP TABLE IF EXISTS pgbench_lsm')
c.execute('CREATE TABLE pgbench_lsm(k int,v text,hot int) WITH(fillfactor=50)')
c.execute('CREATE INDEX pgbench_lsm_idx ON pgbench_lsm USING merkle_lsm(k) INCLUDE(v) WITH(levels=4,top_index_size=16,size_ratio=2)')
c.execute("INSERT INTO pgbench_lsm SELECT i,'seed-'||i,0 FROM generate_series(1,1000)i")
writer = R / 'scratch/lsm_writer.pgbench'
reader = R / 'scratch/lsm_reader.pgbench'
writer.write_text('''\\set lo :writer_no * 100000 + :turn * 20 + 1
\\set hi :lo + 19
BEGIN ISOLATION LEVEL SERIALIZABLE;
SET LOCAL enable_seqscan=off;
SELECT count(*) FROM pgbench_lsm WHERE k>0;
INSERT INTO pgbench_lsm SELECT i,'row-'||i,0 FROM generate_series(:lo,:hi)i;
UPDATE pgbench_lsm SET hot=hot+1 WHERE k=:turn+1;
COMMIT;
''')
reader.write_text('''BEGIN ISOLATION LEVEL SERIALIZABLE READ ONLY;
SET LOCAL enable_seqscan=off;
SELECT count(*),sum(k) FROM pgbench_lsm WHERE k>0;
COMMIT;
''')

def client(number):
    is_writer = number < 4
    completed = 25 if is_writer else 50
    retries = 0
    path = O / f'pgbench_mixed_client_{number}.log'
    rng = random.Random(8300 + number)
    with path.open('w') as log:
        for turn in range(completed):
            for attempt in range(200):
                args = [R / 'install/bin/pgbench', '-n', '-c', '1', '-j', '1', '-t', '1',
                        '-D', f'writer_no={number+1}', '-D', f'turn={turn}', '-f', writer if is_writer else reader]
                result = subprocess.run(args, env=env, capture_output=True, text=True, timeout=30)
                text = result.stdout + result.stderr
                log.write(f'turn={turn} attempt={attempt} exit={result.returncode}\n{text}\n')
                count = re.search(r'number of transactions actually processed:\s*(\d+)/1', text)
                if result.returncode == 0 and count and count[1] == '1':
                    break
                # This pgbench has no --max-tries. A failed SERIALIZABLE
                # transaction closes its connection before the next attempt.
                if 'could not serialize access' not in text:
                    raise AssertionError(text)
                retries += 1
                time.sleep(rng.uniform(.001, min(.1, .002 * (attempt + 2))))
            else:
                raise AssertionError('serialization retry budget exhausted')
    return dict(client=number, writer=is_writer, transactions=completed, retries=retries)

with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
    clients = list(pool.map(client, range(6)))
c.execute("SELECT merkle_lsm_flush('pgbench_lsm_idx')")
c.execute('SET enable_seqscan=on; SET enable_indexscan=off; SET enable_bitmapscan=off')
truth = collections.Counter(c.execute('SELECT k,ctid::text,v,hot FROM pgbench_lsm WHERE k>0').fetchall())
assert sum(truth.values()) == 3000
for bitmap in [False, True]:
    c.execute('SET enable_seqscan=off; SET enable_indexscan=' + ('off' if bitmap else 'on') +
              '; SET enable_bitmapscan=' + ('on' if bitmap else 'off'))
    assert collections.Counter(c.execute('SELECT k,ctid::text,v,hot FROM pgbench_lsm WHERE k>0').fetchall()) == truth
c.execute('SET enable_indexscan=on; SET enable_bitmapscan=off; SET enable_indexonlyscan=on')
assert collections.Counter(c.execute('SELECT k,v FROM pgbench_lsm WHERE k>0').fetchall()) == collections.Counter((k,v) for k,tid,v,hot in truth.elements())
components = c.execute("SELECT * FROM merkle_lsm_components('pgbench_lsm_idx')").fetchall()
assert all(x[7] > 0 for x in components if x[0] < 3), components
result = dict(pass_check=True, transactions=sum(x['transactions'] for x in clients),
              retries=sum(x['retries'] for x in clients), rows=3000, clients=clients, components=components)
(O / 'pgbench_mixed.json').write_text(json.dumps(result, indent=2, default=str) + '\n')
print('PGBENCH_MIXED_PASS', json.dumps(result, default=str), flush=True)
