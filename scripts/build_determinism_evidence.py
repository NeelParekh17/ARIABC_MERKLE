#!/usr/bin/env python3
"""Build a portable explainer from reviewed assets and anchored source excerpts.
Source changes require model review; finding anchors does not prove semantics.

  python3 scripts/build_determinism_evidence.py                 # Git checkout at REVIEWED_HEAD
  python3 scripts/build_determinism_evidence.py --snapshot S.json  # copied tree without .git
  python3 scripts/build_determinism_evidence.py --check         # compare, write nothing
"""
import argparse
import json
from pathlib import Path
import subprocess
import sys
import tempfile

from explorer_common import (ROOT, REVIEWED_DATE, PREVIOUS_REVIEW, add_snapshot_argument, review_identity,
                             sha256_bytes)

ASSETS = ROOT / 'docs/determinism'
W = 'src/backend/bcdb/worker.c'
T = 'src/backend/bcdb/shm_transaction.c'
M = 'src/backend/executor/nodeModifyTable.c'
E = 'ariabc_pg/src/pg_executor.cxx'
# key: file, literal, lines before/after, label
EXCERPTS = {
 'opfheader': ('src/include/bcdb/shm_transaction.h', '#define ERRCODE_BCDB_OPF', 10, 13, 'Internal BC010 and the eight fallback reasons'),
 'signature': ('src/backend/tcop/postgres.c', 'char *query_string2 = palloc(qlen + 1);', 13, 19, 'Length-aware SQL signature stripping'),
 'indexguard': ('src/backend/access/index/indexam.c', 'bcdb_check_pending_index_scan(scan->indexRelation->rd_index->indrelid,', 6, 15, 'Fallback guard before tuple/bitmap index access'),
 'lockrows': ('src/backend/executor/nodeLockRows.c', 'BCDB_PTRACE_COUNTER_DT_LOCKROWS_SKIPPED, 1);', 14, 8, 'Simulation records row-lock reads without physical locks'),
 'merklebatched': ('src/backend/access/merkle/merkleapply.c', 'route->count_delta = delta_entry->count_delta;', 10, 8, 'Batched apply also carries the net count'),
 'baseline': (W, 'activeTx->tx_id_committed = bcdb_dt_snapshot_baseline(tx);', 11, 27, 'Snapshot baseline before SQL'),
 'gate': (W, 'bcdb_wait_for_serial_slot(BCDBShmXact *tx, BCBlock *block, bool early_validate)', 1, 87, 'Publication-turn wait and early validation'),
 'early': (W, 'if (early_validate && bcdb_dt_validate_published(published, false))', 8, 18, 'Early conflict rejection'),
 'turn': (W, 'bcdb_dt_prepare_validation();', 5, 38, 'Final validation at the turn'),
 'publish': (W, 'publish_ws_tableDT(tx->tx_id); // HASHTAB_SWITCH_THRESHOLD', 2, 42, 'Publish footprint before deferred apply'),
 'settle': (W, 'Post-publication settle.  Successors already passed the', 7, 103, 'Frozen-queue settlement and terminal outcomes'),
 'applyretry': (W, 'BeginInternalSubTransaction("bcdb_apply_retry");', 13, 35, 'Apply retry inside an internal subtransaction'),
 'terminal': (W, 'BCDB_SQLSTATE_POST_PUBLISH_INVARIANT,', 5, 19, 'Unsupported settlement invariant failure'),
 'commit': (W, 'bcdb_finish_terminal_item(tx, block->result[mem_txid],', 34, 34, 'PostgreSQL commit before slot finalization'),
 'prefix': ('src/backend/bcdb/shm_block.c', 'advance_last_committed_txid(BCDBShmXact *tx)', 0, 86, 'Contiguous finalized-prefix advancement'),
 'digest': (T, 'bcdb_dt_validate_published(BCTxID published, bool at_turn)', 1, 126, 'Incremental validation and full-map fallback'),
 'ring': (T, '#define BCDB_DT_DIGEST_SLOTS 8192', 5, 23, 'Digest ring capacity and ownership'),
 'dedup': (T, 'bcdb_dt_prepare_validation(void)', 1, 64, 'Checked dependencies, including relation reads'),
 'relationread': (T, 'if (bound == 0)', 5, 26, 'Empty and unbound scans reserve relation reads'),
 'relationwrite': (T, 'bcdb_reserve_write_relation_tag(Oid relid)', 1, 45, 'All writes publish relation membership'),
 'btfirst': ('src/backend/access/nbtree/nbtsearch.c', 'bcdb_reserve_read_key_tag_scan(scan->heapRelation, rel, startKeys, keysCount);', 10, 30, 'Scan registration before zero-key early return'),
 'heapscan': ('src/backend/access/heap/heapam.c', 'bcdb_reserve_read_relation_tag(RelationGetRelid(relation));', 10, 13, 'Heap scan relation-read coverage'),
 'oldkey': (T, 'bcdb_reserve_old_write_key_tags(Relation rel, ItemPointer tid,', 2, 30, 'Fetch and reserve old chosen-key tags'),
 'modifyold': (M, 'bcdb_reserve_old_write_key_tags(resultRelationDesc, tupleid,', 17, 15, 'UPDATE passes changed columns to old-key coverage'),
 'opf': (T, 'bcdb_request_opf(BCDBOpfReason reason)', 1, 17, 'Request unpublished physical fallback'),
 'opfwait': (W, 'if (tx->needs_opf)', 9, 38, 'Fallback waits before baseline and snapshot'),
 'opfretry': (W, 'edata->sqlerrcode == ERRCODE_BCDB_OPF &&', 12, 25, 'BC010 catch restarts only before publication'),
 'caught': (W, 'if (bcdb_dt_simulating && tx->needs_opf)', 10, 17, 'Defence in depth: reject a trapped fallback after SQL returns'),
 'physical': (M, 'bcdb_should_defer_dml(Relation relation)', 1, 30, 'Physical fallback disables deferral and records tags'),
 'trigger': (M, 'On first call, fire BEFORE STATEMENT triggers before proceeding.', 12, 27, 'Trigger guard before statement triggers fire'),
 'upsert': (M, 'bcdb_request_opf(BCDB_OPF_UPSERT);', 9, 12, 'ON CONFLICT requests physical fallback'),
 'deletereturning': (M, 'bcdb_request_opf(BCDB_OPF_DELETE_RETURNING);', 6, 6, 'Deferred DELETE cannot produce RETURNING rows'),
 'exclusion': (M, 'bcdb_request_opf(BCDB_OPF_EXCLUSION);', 6, 4, 'INSERT into a table with an exclusion constraint'),
 'exclusionupd': (M, 'bcdb_request_opf(BCDB_OPF_EXCLUSION);', 5, 4, 'UPDATE of a table with an exclusion constraint'),
 'exclusionflag': ('src/backend/executor/execIndexing.c', 'resultRelInfo->ri_HasExclusionIndex = true;', 6, 3, 'Exclusion-constraint flag set when indexes are opened'),
 'exceptionblock': ('src/pl/plpgsql/src/pl_exec.c', 'bcdb_request_opf(BCDB_OPF_EXCEPTION_BLOCK);', 8, 6, 'Guard at PL/pgSQL block entry, before any handler can trap'),
 'singlestmt': (W, 'a deterministic request must contain exactly one SQL statement', 13, 8, 'Admission: exactly one top-level SQL statement'),
 'deferredunique': (T, 'heap_apply_index_phase(relation, actual_slot, false, true, HEAP_INDEX_NO_MERKLE);', 10, 6, 'Deferred UPDATE applies indexes with unique checking'),
 'partialpred': ('src/backend/access/heap/heapam.c', "Skip partial indexes whose predicate isn't satisfied.", 4, 16, 'Index phase honours partial-index predicates'),
 'sequence': ('src/backend/commands/sequence.c', 'bcdb_request_opf(BCDB_OPF_SEQUENCE);', 7, 15, 'Sequence guard before speculative mutation'),
 'scanpending': (T, 'bcdb_check_pending_scan_slow(Oid relid)', 1, 15, 'Pending INSERT scan fallback'),
 'indexpending': (T, 'bcdb_check_pending_index_scan_slow(Oid relid, Relation index)', 1, 28, 'Affected and partial-index scan guard'),
 'overlay': (T, 'bcdb_overlay_slot_slow(Relation rel, TupleTableSlot *slot)', 1, 43, 'Earlier-command UPDATE and DELETE overlay'),
 'compose': (T, 'void store_optim_update(Relation rel, TupleTableSlot *slot, ItemPointer old_tid,', 0, 107, 'Compose updates by physical TID'),
 'indexonly': ('src/backend/executor/nodeIndexonlyscan.c', 'bcdb_pending_writes(RelationGetRelid(scandesc->heapRelation))', 9, 23, 'Pending writes force index-only heap fetch'),
 'seed': (W, 'bcdb_seed_random((uint64) tx->tx_id);', 7, 14, 'Seed random before each business attempt'),
 'random': ('src/backend/utils/adt/float.c', 'bcdb_seed_random(uint64 tx_id)', 4, 21, 'Sequence-derived random seed'),
 'longsql': (T, 'tx->sql_long = sql_long;', 30, 18, 'DSA storage beyond the inline SQL buffer'),
 'terminalerror': (W, 'strlcpy(terminal_err_sqlstate, det_err_sqlstate, sizeof(terminal_err_sqlstate));', 12, 16, 'Preserve terminal deterministic SQLSTATE'),
 'finalized': (W, 'errdetail(BCDB_FINALIZED_ERROR_DETAIL)', 14, 5, 'Finalized error marker'),
 'errorcatch': (W, 'bcdb_publish_error_result(block, tx, sqlstate);', 4, 18, 'Catch path finalizes a simulated SQL error without validation'),
 'execconstraints': (M, 'ExecConstraints(resultRelInfo, slot, estate);', 6, 66, 'CHECK constraints run before the UPDATE is deferred'),
 'staleerror': (W, 'bcdb_ptrace_inc_counter(BCDB_PTRACE_COUNTER_DT_OPF_STALE_ERROR, 1);', 12, 8, 'Ordinary route: a stale-snapshot error re-runs physically'),
 'ledgerstale': (W, 'Stale-snapshot error: re-run on the ordered physical route', 6, 34, 'Safe-ledger route: whitelisted errors on a stale snapshot re-run'),
 'turnskip': (W, 'if (tx->needs_opf || apply_outcome == BCDB_OUTCOME_TERMINAL_DETERMINISTIC_ERROR)', 18, 15, 'Terminal outcomes skip the turn check (reached only after a non-stale run)'),
 'errorhelper': ('ariabc_pg/src/pg_error_result.hxx', 'inline bool is_finalized_error_result(', 7, 9, 'Executor recognizes finalized errors'),
 'errorcomplete': (E, 'const std::string fail_reason = (!is_finalized_error_result(result) &&', 6, 15, 'SQL errors complete instead of failing the task'),
 'merklemerge': ('src/backend/access/merkle/merkledelta.c', 'entry->count_delta += source->count_delta;', 11, 32, 'Merge hash and count; rollback discards frames'),
 'merklestage': ('src/backend/access/merkle/merkledelta.c', 'entry->count_delta++;', 25, 10, 'Stage row-count contributions'),
 'merkleapply': ('src/backend/access/merkle/merkleapply.c', 'int64 count_delta = entry->count_delta;', 6, 43, 'Apply accumulated row counts'),
 'lanes': (E, 'void pg_executor::push_task_ordered(task&& t)', 0, 18, 'Sequence modulo connection lanes'),
 'fifo': (E, 't = std::move(conn_qs_[conn_idx].front());', 8, 16, 'FIFO lane-front dispatch'),
 'payload': ('src/backend/bcdb/middleware.c', 'Reads BCDB_BLOCK_RETURN_ACTUAL_RESULTS and caches the result.', 2, 29, 'Actual-result payload switch'),
 'actual': ('src/backend/bcdb/middleware.c', 'BCDB_BLOCK_RETURN_ACTUAL_RESULTS. */', 5, 24, 'Safe-ledger and business-abort payloads'),
 'delivery': (E, 'kafka_delivered_ok = kafka_prod_.wait_for_delivery(5000, err);', 13, 18, 'Safe mode waits for Kafka delivery'),
 'defaults': ('src/backend/bcdb/globals.c', 'bool    bcdb_dt_conflict_tracking = false;', 2, 10, 'Source defaults vs selected walkthrough mode'),
}
# Explicitly reviewed occurrence for repeated call sites.
OCCURRENCES = {'staleerror': 1, 'execconstraints': 1, 'opfwait': 0, 'upsert': 0, 'sequence': 0, 'errorcomplete': 0,
               'delivery': 0, 'indexguard': 0, 'exclusion': 0, 'exclusionupd': 1}
EXPECTED_MATCHES = {'staleerror': 2, 'execconstraints': 2, 'upsert': 2, 'sequence': 2, 'errorcomplete': 2, 'delivery': 2,
                    'indexguard': 2, 'exclusion': 2, 'exclusionupd': 2}
ASSET_NAMES = ['model.js', 'view.js', 'style.css', 'template.html']


def build(head, provenance):
    files, excerpts = {}, {}
    for key, (name, anchor, before, after, label) in EXCERPTS.items():
        raw = (ROOT / name).read_bytes()
        files[name] = {'sha256': sha256_bytes(raw), 'bytes': len(raw)}
        lines = raw.decode().splitlines()
        matches = [i for i, line in enumerate(lines) if anchor in line]
        if key in EXPECTED_MATCHES and len(matches) != EXPECTED_MATCHES[key]:
            raise SystemExit(f'{key}: reviewed call-site count changed; review required')
        occurrence = OCCURRENCES.get(key)
        if not matches or (occurrence is None and len(matches) != 1):
            raise SystemExit(f'{key}: expected unique anchor {anchor!r} in {name}, found {len(matches)}')
        if occurrence is not None and occurrence >= len(matches):
            raise SystemExit(f'{key}: reviewed occurrence missing')
        i = matches[occurrence or 0]
        start, end = max(1, i + 1 - before), min(len(lines), i + 1 + after)
        excerpts[key] = {'path': name, 'anchor': anchor, 'occurrence': occurrence or 0,
                         'start': start, 'end': end, 'label': label,
                         'text': '\n'.join(f'{n}: {lines[n-1]}' for n in range(start, end + 1))}
    paper = ROOT / 'ProtectDB_arxiv-3.pdf'
    files[paper.name] = {'sha256': sha256_bytes(paper.read_bytes()), 'bytes': paper.stat().st_size}
    with tempfile.TemporaryDirectory(prefix='protectdb_text_', dir=ROOT / '.bench_tmp') as temp:
        txt = Path(temp) / 'pages.txt'
        subprocess.run(['pdftotext', '-f', '6', '-l', '7', '-layout', str(paper), str(txt)], check=True)
        text = txt.read_text()
        start = text.index('Algorithm 1 Deterministic Transaction Execution Algorithm')
        end = text.index('37: end function', start) + len('37: end function')
        excerpts['paper'] = {'path': paper.name, 'paper': True, 'label': 'ProtectDB §4 / Algorithm 1 · printed pages 6–7',
                             'text': text[start:end]}
    assets = {name: sha256_bytes((ASSETS / name).read_bytes()) for name in ASSET_NAMES}
    return {'reviewed_date': REVIEWED_DATE,
            'head': head,
            'previous_review': PREVIOUS_REVIEW,
            'provenance': provenance,
            'scope': 'Source-derived illustrative schedules; no new database safety or performance certification',
            'mode': 'DT with conflict tracking; publication gating; default early validation and post-publish settle',
            'files': files, 'excerpts': excerpts, 'assets': assets,
            'chats_used_as_navigation': ['Claude 09a68b55-60dd-43e6-b156-e08c0b922875', 'Codex 01a11ce2-b167-7bb2-ab2c-57a8cd98e4cd',
                                         'Claude 757e4c01-914a-4838-a302-5a06ab089bc9', 'Codex 01a11bb3-7ff3-7232-a2b2-d1e218f43750'],
            'runtime_claims': 'Historical chat reports are not presented as newly verified runtime results.'}


def render(data):
    html = (ASSETS / 'template.html').read_text()
    for marker, name in [('/* STYLE_PLACEHOLDER */', 'style.css'), ('/* MODEL_PLACEHOLDER */', 'model.js'),
                         ('/* VIEW_PLACEHOLDER */', 'view.js')]:
        if html.count(marker) != 1:
            raise SystemExit('Template marker must be unique: ' + marker)
        html = html.replace(marker, (ASSETS / name).read_text())
    if html.count('EVIDENCE_PLACEHOLDER') != 1:
        raise SystemExit('Template evidence marker must be unique')
    return html.replace('EVIDENCE_PLACEHOLDER', json.dumps(data).replace('<', '\\u003c'))


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--check', action='store_true', help='compare manifest and page without writing')
    add_snapshot_argument(parser)
    args = parser.parse_args()
    head, provenance = review_identity([v[0] for v in EXCERPTS.values()] + ['ProtectDB_arxiv-3.pdf'], args.snapshot)
    (ROOT / '.bench_tmp').mkdir(exist_ok=True)
    data = build(head, provenance)
    html = render(data)
    if args.check:
        manifest = ASSETS / 'source_manifest.json'
        page = ROOT / 'DETERMINISM_EXPLORER.html'
        stale = [p.name for p, want in ((manifest, json.dumps(data, indent=2) + '\n'), (page, html))
                 if not p.is_file() or p.read_text() != want]
        if stale:
            print('STALE: ' + ', '.join(stale))
            sys.exit(1)
        print(f'OK: {len(data["excerpts"])} excerpts and the standalone page match {head[:8]}')
        return
    (ASSETS / 'source_manifest.json').write_text(json.dumps(data, indent=2) + '\n')
    (ROOT / 'DETERMINISM_EXPLORER.html').write_text(html)
    print(f'Built standalone page with {len(data["excerpts"])} anchored excerpts at {data["head"][:8]}')


if __name__ == '__main__':
    main()
