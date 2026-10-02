"""Read existing experiment artifacts; no database or subprocess execution."""
import importlib.util,json,pathlib,re
B=pathlib.Path(__file__).parent
ROOT=B.parent.parent
spec=importlib.util.spec_from_file_location('comparison',ROOT/'scripts/distributed/recovery_s1024/compare_recovery.py')
cmp=importlib.util.module_from_spec(spec);spec.loader.exec_module(cmp)
allrows=[]
for p in sorted((B/'runs').iterdir()):
    if not p.is_dir():continue
    row=cmp.extract(p);fails=[]
    meta=cmp.env(p/'run_meta.env');summary=cmp.env(p/'run_summary.env')
    expected=cmp.env(p/'recovery_s1024.env')
    if row['evidence_status']!='PASS':fails.append('comparison acceptance')
    if row['phase8_root']!='80566f7114b529dda1e6da732dab5462a8cdbce998afa5fec073e224d0ea6ad5':fails.append('published workload final root')
    if str(row['full_copies'])!='0':fails.append('full copies missing or nonzero')
    if meta.get('BINARY_PROVENANCE_PASS')!='1':fails.append('binary provenance')
    verified=int(summary.get('async_all3_verified_count','-1'))
    attributed=int(summary.get('async_all3_recovery_attributed_count','-1'))
    if verified+attributed!=160000:fails.append('all-three verified plus recovery-attributed count')
    for k in ('async_all3_failure_count','async_all3_timeout_count','async_all3_missing_count'):
        if summary.get(k)!='0':fails.append(k)
    if row['scenario']=='L_mix_prio':
        events=[cmp.kv(x) for x in (p/'gateway_test.log').read_text().splitlines() if x.startswith('RECOVERY_EVENT ')]
        if not events or any(e.get('ref')!='4' for e in events):fails.append('prioritized scenario donor was not 4')
    for host in ('10.129.148.247','10.129.148.246','10.129.148.248'):
        geom=(p/('geometry_'+host+'.txt')).read_text()
        if not re.search(r'5448\s*\|\s*serializable\s*\|\s*on',geom):fails.append(host+' isolation/sync')
        for setting in ('fsync','synchronous_commit','full_page_writes'):
            if not re.search(r'\n\s*'+setting+r'\s*\n[- ]+\n\s*on\s*\n',geom):fails.append(host+' '+setting)
        for k in ('fanout','split','merge'):
            opt={'split':'split_threshold','merge':'merge_threshold'}.get(k,k)
            if opt+"='"+expected[k]+"'" not in geom:fails.append(host+' '+opt)
    row['task_j_status']='PASS' if not fails else 'FAIL'
    row['task_j_failures']=fails
    allrows.append(row)
(B/'acceptance.json').write_text(json.dumps(allrows,indent=2))
for r in allrows:print(r['run_id'],r['task_j_status'],r['task_j_failures'])
raise SystemExit(any(r['task_j_status']!='PASS' for r in allrows))
