#!/usr/bin/env python3
"""Capture before-images and generate an atomic logical YCSB restore file.

Run prepare before an isolated workload, then stop/drain its writers and run
restore.sql with psql -X -v ON_ERROR_STOP=1. This restores usertable contents,
not physical layout, transaction IDs, statistics, or BCDB execution history.
"""
import argparse
import collections
import hashlib
import json
from pathlib import Path
import re
import subprocess
import tempfile
import time


COLUMNS = ['ycsb_key'] + [f'field{i}' for i in range(1, 11)]
VALUE = r"(?:'(?:[^'\\]|'')*'|NULL)"
FIELD = r'field(?:10|[1-9])'
ASSIGN = rf'{FIELD}\s*=\s*{VALUE}'
FLAGS = re.IGNORECASE


def parse_workload(text):
    """Fail closed: only the generator's single-table, literal-key SQL subset."""
    keys, counts = set(), collections.Counter()
    for number, line in enumerate(text.splitlines(), 1):
        line = line.strip()
        if not line:
            continue
        match = re.fullmatch(
            r'SELECT \* FROM usertable WHERE ycsb_key\s*(?:=\s*\d+|>=\s*\d+ '
            r'ORDER BY ycsb_key LIMIT \d+)\s*;', line, FLAGS)
        if match:
            counts['select'] += 1
            continue
        match = re.fullmatch(rf'UPDATE usertable SET ({ASSIGN}(?:\s*,\s*{ASSIGN})*) '
                             r'WHERE ycsb_key\s*=\s*(\d+)\s*;', line, FLAGS)
        operation = 'update'
        if match:
            key = int(match[2])
        else:
            match = re.fullmatch(r'DELETE FROM usertable WHERE ycsb_key\s*=\s*(\d+)\s*;', line, FLAGS)
            operation = 'delete'
            if match:
                key = int(match[1])
            else:
                columns = r'\s*,\s*'.join(COLUMNS)
                match = re.fullmatch(rf'INSERT INTO usertable\s*\(\s*{columns}\s*\) '
                                     rf'VALUES\s*\(\s*(\d+)(?:\s*,\s*{VALUE}){{10}}\s*\)\s*;',
                                     line, FLAGS)
                operation = 'insert'
                if not match:
                    raise ValueError(f'Unsupported SQL at line {number}: {line[:120]}')
                key = int(match[1])
        if not 1 <= key <= 2147483647:
            raise ValueError(f'Out-of-range integer key on line {number}')
        keys.add(key)
        counts[operation] += 1
    if not counts:
        raise ValueError('Empty workload')
    return sorted(keys), dict(counts)


def literal(value):
    return "'" + str(value).replace("'", "''") + "'"


def identifier(value):
    return '"' + value.replace('"', '""') + '"'


class Psql:
    def __init__(self, binary='psql', host='127.0.0.1', port=5438, user='postgres', database='postgres'):
        self.command = [binary, '-X', '-q', '-A', '-t', '-v', 'ON_ERROR_STOP=1',
                        '-h', host, '-p', str(port), '-U', user, '-d', database]

    def run(self, sql):
        result = subprocess.run(self.command, input=sql.encode(), capture_output=True)
        if result.returncode:
            raise RuntimeError(result.stderr.decode(errors='replace'))
        return result.stdout


def inspect_table(db):
    result = db.run("""SELECT json_build_object(
      'system_id', (SELECT system_identifier::text FROM pg_control_system()),
      'database', current_database(), 'oid', c.oid, 'schema', n.nspname,
      'kind', c.relkind, 'rls', c.relrowsecurity,
      'columns', (SELECT json_agg(json_build_array(a.attname, format_type(a.atttypid,a.atttypmod))
                  ORDER BY a.attnum) FROM pg_attribute a
                  WHERE a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped),
      'pk', (SELECT count(*) FROM pg_constraint p WHERE p.conrelid=c.oid
             AND p.contype='p' AND p.conkey=ARRAY[1]::smallint[]),
      'triggers', (SELECT count(*) FROM pg_trigger t WHERE t.tgrelid=c.oid AND NOT t.tgisinternal),
      'foreign_keys', (SELECT count(*) FROM pg_constraint p WHERE p.contype='f'
                       AND (p.conrelid=c.oid OR p.confrelid=c.oid)))
      FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
      WHERE c.oid='usertable'::regclass;""")
    meta = json.loads(result)
    expected = [[name, 'integer' if i == 0 else 'text'] for i, name in enumerate(COLUMNS)]
    if (meta['columns'] != expected or meta['kind'] != 'r' or meta['rls'] or
            meta['pk'] != 1 or meta['triggers'] or meta['foreign_keys']):
        raise ValueError(f'Unsupported table schema or side effects: {meta}')
    meta['table'] = identifier(meta['schema']) + '."usertable"'
    return meta


def key_table(keys):
    return ('CREATE TEMP TABLE undo_keys (ycsb_key integer PRIMARY KEY);\n'
            'COPY undo_keys FROM STDIN;\n' + ''.join(f'{key}\n' for key in keys) + '\\.\n'
            'ANALYZE undo_keys;\n')


def touched_copy(table):
    return (f'COPY (SELECT u.* FROM undo_keys k JOIN {table} u USING (ycsb_key) '
            'ORDER BY u.ycsb_key) TO STDOUT;\n')


def digest_sql(query):
    # JSON distinguishes NULL/empty string and escapes delimiters and newlines.
    return ("SELECT encode(sha256(convert_to(COALESCE(string_agg(row_to_json(r)::text, "
            "E'\\n' ORDER BY r.ycsb_key), ''), 'UTF8')), 'hex') FROM (" + query + ') r')


def merkle_roots_sql(meta):
    return ("SELECT COALESCE(json_object_agg(i.indexrelid::text, "
            "merkle_root_hash_index(i.indexrelid)), '{}'::json) "
            "FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid "
            "JOIN pg_am a ON a.oid=c.relam "
            f"WHERE i.indrelid={meta['oid']} AND a.amname='merkle';\n")


def restore_sql(meta, keys, rows, roots=None):
    table = meta['table']
    assignments = ', '.join(f'{name}=b.{name}' for name in COLUMNS[1:])
    current = f'SELECT u.* FROM undo_keys k JOIN {table} u USING (ycsb_key)'
    before = 'SELECT * FROM undo_before'
    root_checks = ''.join(
        f" SELECT oid::regclass INTO v_reg FROM pg_class WHERE oid = {int(oid)};\n"
        " IF v_reg IS NOT NULL THEN\n"
        f"   IF merkle_root_hash_index(v_reg) IS DISTINCT FROM {literal(root)} THEN\n"
        "     RAISE EXCEPTION 'Post-commit Merkle root mismatch; do not reuse this database';\n"
        "   END IF;\n"
        " END IF;\n"
        for oid, root in (roots or {}).items())
    return f"""-- Generated before-image restore. Stop/drain ALL workload writers first.
-- Logical usertable reset only; retain the baseline and validate full hashes separately.
\\set ON_ERROR_STOP on
\\timing on
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '10min';
SET LOCAL standard_conforming_strings = on;
SET LOCAL enable_seqscan = off;
SET LOCAL enable_hashjoin = off;
SET LOCAL enable_mergejoin = off;
DO $$ BEGIN
 IF (SELECT system_identifier::text FROM pg_control_system()) <> {literal(meta['system_id'])}
    OR current_database() <> {literal(meta['database'])}
    OR {literal(table)}::regclass::oid <> {meta['oid']} THEN
   RAISE EXCEPTION 'Undo file belongs to another database/table';
 END IF;
END $$;
LOCK TABLE {table} IN ACCESS EXCLUSIVE MODE;
{key_table(keys)}
CREATE TEMP TABLE undo_before (LIKE {table});
COPY undo_before FROM STDIN;
{rows.decode()}\\.
ALTER TABLE undo_before ADD PRIMARY KEY (ycsb_key);
ANALYZE undo_before;
-- Remove inserts whose keys were absent before the workload.
DELETE FROM {table} u USING undo_keys k WHERE u.ycsb_key=k.ycsb_key
 AND NOT EXISTS (SELECT 1 FROM undo_before b WHERE b.ycsb_key=k.ycsb_key);
-- Restore overwritten values; avoid making new versions for unchanged rows.
UPDATE {table} u SET {assignments} FROM undo_before b
 WHERE u.ycsb_key=b.ycsb_key AND ROW(u.*) IS DISTINCT FROM ROW(b.*);
-- Reinsert rows deleted by the workload.
INSERT INTO {table} SELECT b.* FROM undo_before b
 WHERE NOT EXISTS (SELECT 1 FROM {table} u WHERE u.ycsb_key=b.ycsb_key);
DO $$ BEGIN
 IF EXISTS (({current} EXCEPT {before}) UNION ALL ({before} EXCEPT {current})) THEN
   RAISE EXCEPTION 'Undo mismatch: rolling back restore';
 END IF;
END $$;
SELECT 'before_sha256=' || ({digest_sql(before)});
SELECT 'restored_sha256=' || ({digest_sql(current)});
COMMIT;
-- Ordinary SQL applies staged Merkle changes at commit. Check roots afterwards if Merkle index exists.
DO $$
DECLARE
  v_reg regclass;
BEGIN
{root_checks or 'NULL;'}
END $$;
"""


def prepare(db, workload, out_dir):
    text = workload.read_text()
    keys, counts = parse_workload(text)
    meta = inspect_table(db)
    out_dir.mkdir(parents=True, exist_ok=False)
    started = time.monotonic()
    captured = db.run('BEGIN; SET LOCAL enable_seqscan=off; SET LOCAL enable_hashjoin=off; '
                  'SET LOCAL enable_mergejoin=off; SET LOCAL lock_timeout=\'5s\';\n'
                  f'LOCK TABLE {meta["table"]} IN ACCESS EXCLUSIVE MODE;\n' + key_table(keys) +
                  merkle_roots_sql(meta) + touched_copy(meta['table']) + 'COMMIT;\n')
    header, rows = captured.split(b'\n', 1)
    roots = json.loads(header)
    manifest = dict(version=2, workload_sha256=hashlib.sha256(workload.read_bytes()).hexdigest(),
                    keys=keys, operations=counts, table=meta,
                    merkle_roots=roots,
                    before_copy_sha256=hashlib.sha256(rows).hexdigest(),
                    before_rows=rows.count(b'\n'), capture_ms=(time.monotonic()-started)*1000)
    (out_dir / 'before.tsv').write_bytes(rows)
    (out_dir / 'restore.sql').write_text(restore_sql(meta, keys, rows, roots))
    (out_dir / 'manifest.json').write_text(json.dumps(manifest, indent=2)+'\n')
    return {k: v for k, v in manifest.items() if k not in ('keys', 'table')}


def verify(db, directory):
    manifest = json.loads((directory / 'manifest.json').read_text())
    meta = inspect_table(db)
    if meta != manifest['table']:
        raise ValueError('Database/table identity changed')
    started = time.monotonic()
    rows = db.run('BEGIN; SET LOCAL enable_seqscan=off; SET LOCAL enable_hashjoin=off; '
                  "SET LOCAL enable_mergejoin=off; SET LOCAL lock_timeout='5s';\n"
                  f'LOCK TABLE {meta["table"]} IN SHARE MODE;\n' + key_table(manifest['keys']) +
                  touched_copy(meta['table']) + 'COMMIT;\n')
    actual = hashlib.sha256(rows).hexdigest()
    if actual != manifest['before_copy_sha256']:
        raise ValueError(f'Touched-row SHA256 mismatch: {actual}')
    roots = json.loads(db.run(merkle_roots_sql(meta)))
    if 'merkle_roots' in manifest and roots and roots != manifest['merkle_roots']:
        raise ValueError('Merkle root mismatch')
    return dict(scope='affected_keys', sha256=actual, rows=rows.count(b'\n'),
                merkle_roots=roots,
                verified=True, elapsed_ms=(time.monotonic()-started)*1000)


def full_hash(db):
    """Ordered SHA256 of every key and field, streaming with bounded memory."""
    meta = inspect_table(db)
    statement = (f'SET enable_seqscan=off; SET max_parallel_workers_per_gather=0; '
                 f'COPY (SELECT * FROM {meta["table"]} ORDER BY ycsb_key) TO STDOUT;')
    digest, rows = hashlib.sha256(), 0
    started = time.monotonic()
    with tempfile.TemporaryFile() as errors:
        process = subprocess.Popen(db.command + ['-c', statement], stdout=subprocess.PIPE, stderr=errors)
        try:
            for chunk in iter(lambda: process.stdout.read(1024*1024), b''):
                digest.update(chunk)
                rows += chunk.count(b'\n')
            if process.wait():
                errors.seek(0)
                raise RuntimeError(errors.read().decode(errors='replace'))
        finally:
            process.stdout.close()
            if process.poll() is None:
                process.terminate()
                process.wait()
    return dict(scope='entire_usertable', algorithm='sha256_ordered_copy_text',
                sha256=digest.hexdigest(), rows=rows, elapsed_ms=(time.monotonic()-started)*1000)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['prepare', 'verify', 'hash'])
    parser.add_argument('--psql', default='psql')
    parser.add_argument('--host', default='127.0.0.1')
    parser.add_argument('--port', type=int, default=5438)
    parser.add_argument('--user', default='postgres')
    parser.add_argument('--database', default='postgres')
    parser.add_argument('--workload', type=Path)
    parser.add_argument('--output-dir', type=Path)
    args = parser.parse_args()
    db = Psql(args.psql, args.host, args.port, args.user, args.database)
    if args.action == 'prepare':
        if not args.workload or not args.output_dir:
            parser.error('prepare requires --workload and --output-dir')
        result = prepare(db, args.workload, args.output_dir)
    elif args.action == 'verify':
        if not args.output_dir:
            parser.error('verify requires --output-dir')
        result = verify(db, args.output_dir)
    else:
        result = full_hash(db)
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()
