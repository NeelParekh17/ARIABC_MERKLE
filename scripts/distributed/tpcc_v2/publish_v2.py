#!/usr/bin/env python3
"""Audit saved TPC-C evidence and regenerate v2 figures, tables and README.

No database, benchmark, build, or remote commands are executed. The fetched
snapshot is immutable; all derived artifacts live outside its source files.
"""
import argparse
import collections
import csv
import hashlib
import json
import pathlib
import re
import shutil
import statistics as st

ROOT = pathlib.Path(__file__).resolve().parents[3]
PUB = ROOT / 'Final_Results/TPCC'
V2 = PUB / 'v2_20261002'
TABLES = {'customer','district','history','item','new_order','oorder','order_line','stock','warehouse'}
POINTS = {'warehouses_w32': [5,10,20,30,50,75,100], 'workers_w100': [8,16,24,32,48,64]}
MODES = ['pg','det','merkle']
CONFIGS = {'C1':'det, default fillfactor', 'C2':'Merkle split 32 / merge 8, default fillfactor',
           'C3':'Merkle split 1024 / merge 256, default fillfactor',
           'C4':'det, fillfactor 90', 'C5':'Merkle split 1024 / merge 256, fillfactor 90'}
TEXT, TEXT2, GRID, SURFACE = '#0b0b0b', '#52514e', '#e6e5e0', '#fcfcfb'
COLORS = {'pg':'#2a78d6','det':'#eb6834','merkle':'#1baf7a','previous_wh':'#687e8d','previous_hash':'#eda100'}
MARKERS = {'pg':'o','det':'s','merkle':'D','previous_wh':'s','previous_hash':'^'}

def need(condition, message):
    if not condition:
        raise ValueError(message)

def rows(path):
    with path.open(newline='') as stream:
        return list(csv.DictReader(stream))

def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

def kv(text):
    return dict(re.findall(r'(\w+)=([^\s]+)', text))

def last_counter(text, counter):
    found = re.findall(r'\b'+counter+r'=(\d+)\b', text)
    need(found, 'missing '+counter)
    return int(found[-1])

def audit_run(path, data, sweep):
    cfg = kv((path/'config.txt').read_text())
    result = kv((path/'result.txt').read_text())
    gateway = (path/'gateway.log').read_text()
    need(data['accepted'] is True, str(path)+': not accepted')
    need(int(cfg['tx']) == 20000, str(path)+': workload size')
    completed = data['completed'] if sweep else data['validated_completed_queries']
    need(completed == 20000 and last_counter(gateway,'direct_terminal_success_count') == 20000,
         str(path)+': completion mismatch')
    for name in ('divergence_count','permanent_failures','deterministic_error_count','nonterminal_failure_count'):
        need(last_counter(gateway,name) == 0, str(path)+': nonzero '+name)
    need(result['gateway_rc'] == '0', str(path)+': gateway exit')
    need(int(result['wal_bytes']) > 0 and 'Total' in (path/'walstats.txt').read_text(), 'WAL evidence')
    need(abs(float(data['wal_per_tx'])-int(result['wal_bytes'])/20000) < 0.0001, 'WAL per tx mismatch')
    # Headline TPS derives from integer-ms wall time; the final progress rate
    # uses a separate high-resolution elapsed sample. Allow 0.05% rounding drift.
    need(abs(float(data['tps'])/float(result['completed_tps'])-1)<0.0005, 'result TPS mismatch')
    progress = [line for line in gateway.splitlines() if line.startswith('PROGRESS_GATEWAY_DET') and 'final=1' in line]
    need(progress and last_counter(progress[-1],'completed')==20000, 'final progress completion')
    settings = {r['name']:r['setting'] for r in rows(path/'settings.csv')}
    expected = {'default_transaction_isolation':'serializable','fsync':'on','synchronous_commit':'on',
                'full_page_writes':'on','merkle_apply_synchronous_direct':'on','shared_buffers':'4194304',
                'bcdb_worker_count': '1' if cfg['mode']=='pg' else cfg['workers']}
    need(all(settings.get(k)==v for k,v in expected.items()), str(path)+': settings')
    restored = rows(path/'restore_relations.csv')
    need({r['relname'] for r in restored} == TABLES and len(restored)==9, 'restored tables')
    need(all(r['relpersistence']=='p' and int(r['heap_bytes'])>0 for r in restored), 'logged relations')
    ff90 = sweep or data['config'] in ('C4','C5')
    # Headline harness changed the eight mutable tables; the later sweep also
    # changes immutable item. Preserve this real configuration distinction.
    need(all(r['reloptions']==('fillfactor=90' if ff90 and (sweep or r['relname']!='item') else '')
             for r in restored), str(path)+': heap fillfactor')
    need(cfg['mode']==data['mode'], 'mode mismatch')
    opts = rows(path/'merkle_options.csv')
    if cfg['mode']=='merkle':
        need(result['merkle_verify']=='9:true' and len(opts)==9, 'Merkle verification')
        expected_opts = {'partitions':'16384','partition_key_columns':'1','subpartitions':'16','fanout':'32',
                         'split_threshold':cfg['split'],'merge_threshold':cfg['merge']}
        if sweep:
            need(cfg['split']=='1024' and cfg['merge']=='256', 'sweep geometry')
        for row in opts:
            opt = dict(p.split('=',1) for p in row['reloptions'].split(';'))
            need(all(opt.get(k)==v for k,v in expected_opts.items()), 'Merkle index options')
    else:
        need(not opts, 'unexpected Merkle indexes')
    state = (path/'state.hash').read_bytes()
    need(len(state.split())==8 and sha(path/'state.hash')==data['state_sha256'], 'state projection checksum')
    with (path/'tabstats.csv').open(newline='') as stream:
        for schema,table,ins,upd,hotupd,deleted in csv.reader(stream):
            if schema!='public':
                continue
            upd,hotupd = int(upd),int(hotupd)
            need(0<=hotupd<=upd, 'HOT counters')
            fraction = hotupd/upd if upd else None
            if sweep:
                saved = data['hot_fractions'][table]
                need(saved==fraction, 'saved HOT fraction mismatch')
            else:
                saved = data['hot'][table]
                need(saved['updates']==upd and saved['hot_updates']==hotupd, 'saved HOT count mismatch')
    for name in ('postgres.log','postgres_workload.log'):
        p = path/name
        if p.exists():
            log = p.read_text(errors='replace')
            # Fast shutdown intentionally terminates administrative connections.
            if name=='postgres.log':
                log = '\n'.join(line for line in log.splitlines() if
                    'FATAL:  terminating connection due to administrator command' not in line)
            need(not re.search(r'\b(?:FATAL|PANIC)\b',log), str(p)+': FATAL/PANIC')
    return cfg

def load_and_audit():
    evidence_files = 0
    evidence_bytes = 0
    for campaign in ('sweeps','headline_ab'):
        base = V2/campaign
        manifest = json.loads((base/'FETCH_MANIFEST.json').read_text())
        for f in manifest['files']:
            p = base/f['path']
            need(p.stat().st_size==f['bytes'] and sha(p)==f['sha256'], 'evidence checksum: '+str(p))
            evidence_files += 1
            evidence_bytes += f['bytes']
    need('EXTRA_COMPLETE' in (V2/'sweeps/status.txt').read_text(), 'unfinished sweep')
    need('COMPLETE headline' in (V2/'headline_ab/status.txt').read_text(), 'unfinished headline')
    sweep = rows(V2/'sweeps/all_runs.csv')
    headline = [r for r in rows(V2/'headline_ab/all_runs.csv') if r['group']=='headline']
    projection = collections.defaultdict(set)
    workload_hashes = collections.defaultdict(set)
    mode_counts = collections.Counter()
    for campaign, data, is_sweep in [('sweeps',sweep,True),('headline_ab',headline,False)]:
        for row in data:
            path = V2/campaign/pathlib.Path(row['run']).name
            accepted = json.loads((path/'accepted.json').read_text())
            cfg = audit_run(path,accepted,is_sweep)
            need(abs(float(row['tps'])-float(accepted['tps']))<0.011, 'CSV TPS mismatch')
            need(row['state_sha256']==accepted['state_sha256'], 'CSV state mismatch')
            w = int(cfg['warehouses'])
            row.update(W=w,workers=int(cfg['workers']),trial=int(row['trial']),tps=float(row['tps']),
                       wal_per_tx=float(row['wal_per_tx']),mode=cfg['mode'],accepted_data=accepted)
            mode_counts[(campaign,cfg['mode'])] += 1
            if is_sweep:
                need(accepted['group']=='matrix' and accepted['W']==w and
                     accepted['workers']==int(cfg['workers']) and accepted['trial']==row['trial'], 'matrix identity')
                workload_hashes[w].add(accepted['workload_sha256'])
                need(int(row['div'])==0 and int(row['perm_fail'])==0, 'CSV correctness')
            if cfg['mode']!='pg':
                projection[w].add(accepted['state_sha256'])
    need(all(len(v)==1 for v in projection.values()), 'det/Merkle state projection mismatch')
    need(all(len(v)==1 for v in workload_hashes.values()), 'within-W workload mismatch')
    need(len({(r['W'],r['workers'],r['mode']) for r in sweep})==36, 'matrix coverage')
    # Detect accepted records omitted from the CSV, without including smoke attempts.
    for campaign,data,group in [('sweeps',sweep,'matrix'),('headline_ab',headline,'headline')]:
        records = [json.loads(p.read_text()) for p in (V2/campaign).glob('*/accepted.json')]
        need({pathlib.Path(r['run']).name for r in records if r['group']==group} ==
             {pathlib.Path(r['run']).name for r in data}, 'CSV/accepted coverage mismatch')
    summary = {}
    for name, points in POINTS.items():
        dimension = 'W' if name=='warehouses_w32' else 'workers'
        selected = [r for r in sweep if (r['workers']==32 if dimension=='W' else r['W']==100)]
        summary[name] = {}
        for mode in MODES:
            for x in points:
                rs = [r for r in selected if r['mode']==mode and r[dimension]==x]
                need(rs, 'missing point')
                values = [r['tps'] for r in rs]
                summary[name][(mode,x)] = dict(mode=mode,point=x,attempts=len(rs),best_tps=max(values),
                    median_tps=st.median(values),min_tps=min(values),max_tps=max(values))
        # Confirm that the remote summaries agree with independent aggregation.
        for r in rows(V2/'sweeps'/name/'summary.csv'):
            mode = r['config'].split('_')[0]
            x = int(r['warehouses' if dimension=='W' else 'workers'])
            own = summary[name][mode,x]
            need(int(r['trials'])==own['attempts'], 'summary attempt count')
            need(all(abs(float(r[k])-own[k])<0.00001 for k in ('best_tps','median_tps','min_tps')), 'summary value')
    ab = {}
    for c in CONFIGS:
        accepted = [r for r in headline if r['config']==c]
        included = [r for r in accepted if not (c=='C1' and r['trial']==2)]
        need(len(included)==2, 'headline included trials')
        ab[c] = dict(config=c,description=CONFIGS[c],attempts=len(accepted),included=len(included),
                     trials=[r['trial'] for r in included],tps=[r['tps'] for r in included],
                     mean_tps=st.mean(r['tps'] for r in included),min_tps=min(r['tps'] for r in included),
                     max_tps=max(r['tps'] for r in included),
                     mean_wal_bytes_per_tx=st.mean(r['wal_per_tx'] for r in included),hot={})
        for table in ('stock','customer'):
            updates = sum(r['accepted_data']['hot'][table]['updates'] for r in included)
            hot = sum(r['accepted_data']['hot'][table]['hot_updates'] for r in included)
            ab[c]['hot'][table] = dict(updates=updates,hot_updates=hot,hot_fraction=hot/updates,
                                       non_hot_fraction=(updates-hot)/updates)
    audit = dict(sweep_attempts=len(sweep),headline_attempts=len(headline),headline_included=10,
                 distinct_points=36,merkle_sweep_attempts=mode_counts['sweeps','merkle'],
                 merkle_headline_attempts=mode_counts['headline_ab','merkle'],
                 completed_per_attempt=20000,divergence=0,permanent_failures=0,merkle_verify='9:true',
                 evidence_files=evidence_files,evidence_bytes=evidence_bytes,
                 det_merkle_projection_sha256={str(w):next(iter(v)) for w,v in sorted(projection.items())},
                 workload_sha256_by_W={str(w):next(iter(v)) for w,v in sorted(workload_hashes.items())},
                 exclusion={'config':'C1','trial':2,'tps':next(r['tps'] for r in headline if r['config']=='C1' and r['trial']==2),
                            'reason':'User-designated ranking stall; excluded from headline means only'},
                 failures_jsonl_present=(V2/'sweeps/failures.jsonl').exists(),
                 state_comparison_limit='Eight mutable tables: row count and 64-bit row-hash sum; timestamps excluded; not full row equality or all-replica recovery validation')
    return summary, ab, audit

def write_csv(path, data):
    with path.open('w',newline='') as stream:
        out = csv.DictWriter(stream,fieldnames=list(data[0]))
        out.writeheader()
        out.writerows(data)

def style(ax):
    import matplotlib
    ax.set_facecolor(SURFACE)
    ax.grid(axis='y',color=GRID,linewidth=0.8)
    ax.set_axisbelow(True)
    for side in ('top','right'):
        ax.spines[side].set_visible(False)
    for side in ('left','bottom'):
        ax.spines[side].set_color(GRID)
    ax.tick_params(colors=TEXT2,labelsize=9)
    ax.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v,_:f'{v:,.0f}'))

def plot_sweeps(summary):
    import matplotlib.pyplot as plt
    for name, points in POINTS.items():
        warehouses = name=='warehouses_w32'
        dimension = 'W' if warehouses else 'workers'
        previous = collections.defaultdict(list)
        for row in rows(PUB/name/'all_runs.csv'):
            if row['mode']=='merkle':
                key = 'previous_wh' if row['layout']=='wh16' else 'previous_hash'
                previous[key,int(row[dimension])].append(float(row['tps']))
        labels = {'pg':'PostgreSQL (pg), new FF90','det':'Deterministic (det), new FF90',
                  'merkle':'New Merkle: split 1024 + FF90, warehouse routing',
                  'previous_wh':'Previous: warehouse routing, split 32 (different config)',
                  'previous_hash':'Previous: hash % 200, split 32 (different config)'}
        fig, axes = plt.subplots(1,2,figsize=(14,5.2),facecolor=SURFACE)
        for ax, keys, title in [(axes[0],MODES,'All modes'),
                (axes[1],['merkle','previous_wh','previous_hash'],'Merkle mode')]:
            top = 0
            for key in keys:
                if key in MODES:
                    ss = [summary[name][key,x] for x in points]
                    best = [r['best_tps'] for r in ss]
                    lo = [r['min_tps'] for r in ss]
                else:
                    best = [max(previous[key,x]) for x in points]
                    lo = [min(previous[key,x]) for x in points]
                top = max(top,max(best))
                ax.fill_between(points,lo,best,color=COLORS[key],alpha=0.08,linewidth=0)
                ax.plot(points,best,color=COLORS[key],linewidth=2,marker=MARKERS[key],markersize=7,
                        markeredgecolor=SURFACE,markeredgewidth=1.5,label=labels[key],zorder=3,
                        linestyle='--' if key.startswith('previous') else '-')
                ax.annotate(f'{best[-1]:,.0f}',(points[-1],best[-1]),xytext=(8,0),
                            textcoords='offset points',va='center',fontsize=9,color=TEXT2)
            style(ax)
            ax.set_title(title,loc='left',fontsize=12,color=TEXT,pad=10)
            ax.set_xlabel('Warehouses' if warehouses else 'Workers',color=TEXT2)
            ax.set_ylabel('Throughput (TPS, best of attempts)',color=TEXT2)
            ax.set_xticks(points)
            ax.set_xlim(0,112 if warehouses else 72)
            ax.set_ylim(0,top*1.35)
            ax.legend(frameon=False,fontsize=8,loc='upper left',labelcolor=TEXT)
        title = 'warehouses' if warehouses else 'workers at 100 warehouses'
        fig.suptitle(f'TPC-C v2 throughput vs {title} on ranking (EPYC 9654)',x=0.01,ha='left',
                     fontsize=14,color=TEXT,y=0.985)
        fixed = '32 workers' if warehouses else '100 warehouses'
        fig.text(0.01,0.905,f'20,000 tx/run · {fixed} · shared_buffers 32GB · prewarmed · SERIALIZABLE · fillfactor 90 for ALL new modes · pg retry jitter\n'
                 'New Merkle: synchronous, warehouse routing 16384/1/16, split 1024 / merge 256 · line = best of accepted attempts; band = min–max (attempt counts in README)',
                 fontsize=8.5,color=TEXT2,ha='left')
        fig.tight_layout(rect=(0,0,1,0.89))
        # Reserve a stable margin for the longer v2 throughput label.
        fig.subplots_adjust(left=0.075,right=0.982,wspace=0.18)
        fig.savefig(PUB/('tpcc_warehouses_scaling.png' if warehouses else 'tpcc_workers_scaling.png'),dpi=200,facecolor=SURFACE)
        plt.close(fig)

def plot_ab(ab):
    import matplotlib.pyplot as plt
    fig, axes = plt.subplots(1,2,figsize=(14,5.2),facecolor=SURFACE)
    keys = list(CONFIGS)
    colors = [COLORS['det'], '#687e8d', '#eda100', COLORS['det'], COLORS['merkle']]
    labels = ['C1\ndet\ndefault FF','C2\nMerkle split 32\ndefault FF',
              'C3\nMerkle split 1024\ndefault FF','C4\ndet\nFF90','C5\nMerkle split 1024\nFF90']
    rates = [ab[c]['mean_tps'] for c in keys]
    for ax, values, title, unit in [(axes[0],rates,'Headline throughput','Throughput (mean TPS)'),
            (axes[1],[ab[c]['mean_wal_bytes_per_tx']/1000 for c in keys],'WAL per completed transaction','WAL (kB/tx; 1 kB = 1,000 bytes)')]:
        style(ax)
        bars = ax.bar(range(5),values,color=colors,width=0.62,zorder=3)
        if ax is axes[0]:
            ax.errorbar(range(5),values,yerr=[[ab[c]['mean_tps']-ab[c]['min_tps'] for c in keys],
                        [ab[c]['max_tps']-ab[c]['mean_tps'] for c in keys]],fmt='none',color=TEXT2,capsize=4,zorder=4)
        for bar,value in zip(bars,values):
            ax.annotate(f'{value:,.0f}' if ax is axes[0] else f'{value:.1f}',
                        (bar.get_x()+bar.get_width()/2,value),xytext=(0,9),
                        textcoords='offset points',ha='center',fontsize=10,color=TEXT)
        ax.set_title(title,loc='left',fontsize=12,color=TEXT,pad=10)
        ax.set_ylabel(unit,color=TEXT2)
        ax.set_xticks(range(5),labels)
        ax.set_ylim(0,max(values)*1.22)
    fig.suptitle('TPC-C v2 headline A/B: synchronous Merkle geometry and heap fillfactor',
                 x=0.01,ha='left',fontsize=14,color=TEXT,y=0.985)
    fig.text(0.01,0.905,'100 warehouses · 32 workers · 20,000 tx/run · SERIALIZABLE · warehouse routing 16384/1/16 · prewarmed\n'
             'Mean of 2 included attempts/config; TPS whiskers = min–max · C1 trial 2 ranking stall (887.78 TPS) excluded from means; evidence retained',
             fontsize=9,color=TEXT2,ha='left')
    fig.tight_layout(rect=(0,0,1,0.89))
    fig.savefig(PUB/'tpcc_headline_ab_v2.png',dpi=200,facecolor=SURFACE)
    plt.close(fig)

def preserve_previous():
    previous = PUB/'previous'
    previous.mkdir(exist_ok=True)
    if not (previous/'README.md').exists():
        shutil.copy2(PUB/'README.md',previous/'README.md')
    for name in ('tpcc_warehouses_scaling.png','tpcc_workers_scaling.png'):
        if not (previous/name).exists():
            shutil.move(PUB/name,previous/name)
    if not (previous/'SHA256SUMS').exists():
        (previous/'SHA256SUMS').write_text(''.join(f'{sha(previous/name)}  {name}\n' for name in
            ('README.md','tpcc_warehouses_scaling.png','tpcc_workers_scaling.png')))

def readme(summary,ab,audit):
    parts = ['''# TPC-C v2 scaling results (ranking, 2026-10-02)

The two primary scaling figures now use the completed v2 campaign: **all new modes use fillfactor 90, SERIALIZABLE, prewarming and 32GB shared buffers**. PostgreSQL uses `dbType 0` with client-executor serialization retries and exponential backoff/full jitter; det uses deterministic execution; Merkle adds synchronous integrity indexes on all nine TPC-C tables. The right panels retain the previously published warehouse-routing split-32 and hash-%-200 observations as **different-configuration references**, not v2 controls.

![TPC-C v2 warehouse scaling](tpcc_warehouses_scaling.png)

![TPC-C v2 worker scaling](tpcc_workers_scaling.png)

## Configuration and provenance

All work ran on ranking (`protectdr@10.129.7.57`), including PostgreSQL, server, gateway and driver, in isolated `~/claude_checks` directories. The measured PostgreSQL install is `install_v2`, built from the working tree including the uncommitted Merkle changes. Frozen source diffs, binary hashes, harness copies and continuation changes are in [headline provenance](v2_20261002/headline_ab/provenance/) and [sweep provenance](v2_20261002/sweeps/provenance/).

| Mode | New sweep configuration |
|---|---|
| pg | AriaBC PostgreSQL path, `dbType 0`; SERIALIZABLE; serialization failures retried by the client executor with jitter; fillfactor 90 |
| det | Deterministic execution; fillfactor 90 |
| Merkle | det plus synchronous Merkle on 9 tables; fanout 32, partitions 16384, partition-key columns 1, subpartitions 16; split 1024 / merge 256; fillfactor 90 |

Every leaf and ancestor through the partition root remains updated inside the user transaction and exact at commit (`merkle_apply_synchronous_direct=on`). Canonical row-hash bytes and recovery semantics are unchanged by this publication. Logged tables, fsync, synchronous commit and full-page writes are enabled. Every measured attempt uses 20,000 transactions. The server SHA-256 is `cc02e6df017f5d1dbbb181ac39ecf2b8a29b971a4c53c72d17cdf52695ca2a17`; PostgreSQL SHA-256 is `8f132d4d1891bc8e43972b98fc3c11b06895408560f202335babf1c81885b3bb` (recorded in `provenance/binaries.sha256`).

## Reporting method and stalls

The sweep ran one initial pass with automatic stall reruns, a full second pass in reverse order with stall reruns, then two extra attempts each for det W20/32 workers, Merkle W100/16 workers, and det W100/64 workers. **Lines plot the best of all accepted attempts per point; bands show min–max across those attempts, including slow attempts.** Tables report best / median / minimum TPS and the exact accepted attempt count. This follows the previous publication's peak-throughput convention, with unequal attempt counts made explicit; it is not a stable median ranking or an equal-budget comparison.

Ranking's intermittent stalls affected many individual attempts even when host load was low. The cause is unknown; NUMA placement is a suspected explanation, not an established cause. The apparent pg W5 regression was a stall: its third attempt matched the previous publication. Failed setup attempts and smoke runs are excluded from scaling tables. Only the headline C1 trial 2 stall is excluded from headline means as specified below. Raw observations remain intact.

The remote `sweeps/summary.md` still says "one initial trial"; it is archived unchanged. The completed `status_history.txt`, accepted JSON records and all-runs CSV establish the actual two-pass method and extras used here. `stall_flags.json` is the saved flag snapshot, not an exhaustive classifier of all slow attempts.
''']
    parts.append(f"\nThere are **{audit['sweep_attempts']} accepted measured sweep attempts over {audit['distinct_points']} distinct mode/W/worker points**. W100/32-worker attempts appear in both sweep tables and are counted once in this total. The headline has {audit['headline_attempts']} accepted measured attempts, of which {audit['headline_included']} enter the means. Three separate 2,000-transaction smoke attempts remain in the archive and are excluded.\n")
    parts.append('''
## Warehouse scaling (32 workers)

Source: [all sweep attempts](v2_20261002/sweeps/all_runs.csv), [warehouse source CSV](v2_20261002/sweeps/warehouses_w32/all_runs.csv), [derived summary](v2_20261002/analysis/warehouses_w32.csv). Each mode cell is **best / median / min TPS (attempts)**. Ratios use new best TPS divided by new best TPS.
''')
    for name,points in POINTS.items():
        if name=='workers_w100':
            parts.append('''
## Worker scaling (100 warehouses)

Source: [worker source CSV](v2_20261002/sweeps/workers_w100/all_runs.csv), [derived summary](v2_20261002/analysis/workers_w100.csv). Cells and ratios follow the same rule as the warehouse table.
''')
        parts.append('\n| '+('Warehouses' if name=='warehouses_w32' else 'Workers')+' | pg: best / median / min (n) | det: best / median / min (n) | Merkle: best / median / min (n) | Merkle/det | Merkle/pg |\n|---:|---:|---:|---:|---:|---:|\n')
        for x in points:
            ss = [summary[name][m,x] for m in MODES]
            cells = [f"{s['best_tps']:,.2f} / {s['median_tps']:,.2f} / {s['min_tps']:,.2f} ({s['attempts']})" for s in ss]
            parts.append('| '+str(x)+' | '+' | '.join(cells)+f" | {ss[2]['best_tps']/ss[1]['best_tps']:.4f} | {ss[2]['best_tps']/ss[0]['best_tps']:.4f} |\n")
    parts.append('''
## Headline A/B: W100, 32 workers

![Headline A/B throughput and WAL](tpcc_headline_ab_v2.png)

Source: [all headline observations](v2_20261002/headline_ab/all_runs.csv), per-run `accepted.json`, `tabstats.csv` and `result.txt`; [derived A/B summary](v2_20261002/analysis/headline_ab.csv). Means use two included attempts per configuration. C1 has three accepted attempts; **trial 2 (887.78 TPS) was a ranking stall and is excluded from both its TPS and WAL means**, while its evidence is retained. C4 trial 2 also has a saved failed stop-timeout setup directory, followed by the successful continuation; the failed setup contributes no measurement. The archived upstream headline summary includes C1's stall in pooled/paired calculations and must not be read as the filtered means below. All C2/C3/C5 Merkle configurations use warehouse routing 16384/1/16 with fanout 32.

The headline C4/C5 fillfactor change applies to the **eight mutable tables**; immutable `item` retains its default fillfactor, as recorded in `restore_relations.csv`. The subsequent scaling sweeps explicitly set fillfactor 90 on all nine tables. The headline and sweep therefore should not be treated as identical physical configurations.

| Config | Configuration | Included trials / accepted attempts | TPS per included trial | Mean TPS | Mean WAL bytes/tx | Mean WAL kB/tx |
|---|---|---|---|---:|---:|---:|
''')
    for c,r in ab.items():
        parts.append(f"| {c} | {r['description']} | {', '.join(map(str,r['trials']))} / {r['attempts']} | {' / '.join(f'{v:,.2f}' for v in r['tps'])} | {r['mean_tps']:,.2f} | {r['mean_wal_bytes_per_tx']:,.1f} | {r['mean_wal_bytes_per_tx']/1000:.2f} |\n")
    parts.append('\nWAL uses decimal kB (1,000 bytes), measured WAL bytes divided by 20,000 completed transactions, then averaged over included attempts. TPS uses gateway workload wall time; overlapping phase-counter sums are not elapsed time.\n\nHOT fractions below pool table-update counters over the same included attempts.\n\n| Config | Stock HOT | Stock non-HOT | Customer HOT | Customer non-HOT |\n|---|---:|---:|---:|---:|\n')
    for c,r in ab.items():
        s,k = r['hot']['stock'],r['hot']['customer']
        parts.append(f"| {c} | {s['hot_fraction']:.4%} | {s['non_hot_fraction']:.4%} | {k['hot_fraction']:.4%} | {k['non_hot_fraction']:.4%} |\n")
    parts.append(f"\nComparing C3 with C5, stock non-HOT updates fall from {ab['C3']['hot']['stock']['non_hot_fraction']:.4%} to {ab['C5']['hot']['stock']['non_hot_fraction']:.4%}; customer non-HOT updates fall from {ab['C3']['hot']['customer']['non_hot_fraction']:.4%} to {ab['C5']['hot']['customer']['non_hot_fraction']:.4%}. The latter is near zero, not exactly zero. C3/C2 mean TPS = {ab['C3']['mean_tps']/ab['C2']['mean_tps']:.4f}; C5/C3 = {ab['C5']['mean_tps']/ab['C3']['mean_tps']:.4f}; matched-FF90 Merkle/det (C5/C4) = {ab['C5']['mean_tps']/ab['C4']['mean_tps']:.4f}. These small repeated samples do not establish a general causal ranking.\n")
    parts.append(f'''
## Correctness and evidence audit

- All {audit['sweep_attempts']} measured sweep attempts and {audit['headline_attempts']} measured headline attempts completed **20,000 terminal successes**, gateway exit 0, divergence 0 and permanent failures 0. The excluded C1 stall passed correctness checks too.
- All {audit['merkle_sweep_attempts']} sweep Merkle attempts and {audit['merkle_headline_attempts']} headline Merkle attempts recorded **`merkle_verify=9:true`**. Saved settings confirm SERIALIZABLE, synchronous Merkle maintenance and durability enabled. Every new sweep's nine restored tables have fillfactor 90.
- det and Merkle have identical `state.hash` SHA-256 per warehouse count, across worker counts and attempts; W100 also matches the headline campaign. This is the published **eight-mutable-table row-count plus 64-bit row-hash-sum projection, with timestamps excluded**. The immutable item table is omitted. It is not a full row-by-row equality proof, a three-replica root comparison, or recovery validation.
- Within each warehouse count, all accepted sweep attempts record the same workload SHA-256. Summary counts and best/median/min values were independently recomputed from per-run CSVs and checked against the accepted JSON records and raw terminal logs.
- {audit['evidence_files']} fetched evidence files ({audit['evidence_bytes']:,} bytes) were checked against hashes calculated on ranking before transfer. [Audit JSON](v2_20261002/analysis/audit.json) records per-W state/workload checksums and the headline exclusion.

Evidence is in [sweeps](v2_20261002/sweeps/) and [headline A/B](v2_20261002/headline_ab/), each with `FETCH_MANIFEST.json` and `SHA256SUMS`. Fetches include compact result/configuration/statistics files, gateway logs, small PostgreSQL logs, scripts and provenance. No pgdata, ptrace trees, server bulk logs or PostgreSQL logs larger than 5 MiB were copied. The sweep source had no `failures.jsonl` at fetch time; the absence is recorded in the audit, not treated as proof that no setup was ever retried. Previous PNGs and the original README are retained under [previous/](previous/).

## Regenerating the publication (existing files only)

From the repository root:

```bash
python3 scripts/distributed/tpcc_v2/publish_v2.py --validate-only
(cd Final_Results/TPCC/v2_20261002/sweeps && sha256sum -c SHA256SUMS)
(cd Final_Results/TPCC/v2_20261002/headline_ab && sha256sum -c SHA256SUMS)
MPLCONFIGDIR=$HOME/.cache/ariabc-matplotlib python3 scripts/distributed/tpcc_v2/publish_v2.py
```

These are saved-evidence analysis/publication commands, with no builds or benchmarks. Original plot scripts are unchanged and can regenerate previous figures into a separate output directory; see [COMMANDS.md](../COMMANDS.md). The fetch script deliberately refuses to overwrite an existing evidence snapshot.

## Previous results

The following is the previous README content. Its measurements, interpretation and reproduction commands refer to the September configuration; its figures below link to the preserved PNGs. Historical numbers are not v2 controls.

''')
    old = (PUB/'previous/README.md').read_text()
    old = old.replace('(tpcc_warehouses_scaling.png)','(previous/tpcc_warehouses_scaling.png)').replace('(tpcc_workers_scaling.png)','(previous/tpcc_workers_scaling.png)')
    parts.append(old)
    (PUB/'README.md').write_text(''.join(parts))

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--validate-only',action='store_true')
    args = parser.parse_args()
    summary,ab,audit = load_and_audit()
    if args.validate_only:
        print(json.dumps(audit,indent=2))
        return
    import matplotlib
    matplotlib.use('Agg')
    preserve_previous()
    analysis = V2/'analysis'
    analysis.mkdir(exist_ok=True)
    for name,points in POINTS.items():
        data = []
        for x in points:
            for mode in MODES:
                r = dict(summary[name][mode,x])
                r['merkle_det_best_ratio'] = summary[name]['merkle',x]['best_tps']/summary[name]['det',x]['best_tps']
                r['merkle_pg_best_ratio'] = summary[name]['merkle',x]['best_tps']/summary[name]['pg',x]['best_tps']
                data.append(r)
        write_csv(analysis/(name+'.csv'),data)
    write_csv(analysis/'headline_ab.csv',[dict(config=c,description=r['description'],accepted_attempts=r['attempts'],
        included_attempts=r['included'],included_trials=';'.join(map(str,r['trials'])),
        mean_tps=r['mean_tps'],min_tps=r['min_tps'],max_tps=r['max_tps'],mean_wal_bytes_per_tx=r['mean_wal_bytes_per_tx'],
        stock_hot_fraction=r['hot']['stock']['hot_fraction'],customer_hot_fraction=r['hot']['customer']['hot_fraction']) for c,r in ab.items()])
    (analysis/'audit.json').write_text(json.dumps(audit,indent=2)+'\n')
    plot_sweeps(summary)
    plot_ab(ab)
    readme(summary,ab,audit)
    outputs = [PUB/'README.md', PUB/'tpcc_warehouses_scaling.png', PUB/'tpcc_workers_scaling.png',
               PUB/'tpcc_headline_ab_v2.png', analysis/'warehouses_w32.csv',
               analysis/'workers_w100.csv', analysis/'headline_ab.csv', analysis/'audit.json']
    (analysis/'PUBLICATION_SHA256SUMS').write_text(''.join(
        f'{sha(path)}  {path.relative_to(PUB)}\n' for path in outputs))
    print(f"Published 3 figures; {audit['sweep_attempts']} sweep and {audit['headline_attempts']} headline attempts audited")

if __name__=='__main__':
    main()
