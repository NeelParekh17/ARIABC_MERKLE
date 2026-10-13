#!/usr/bin/env python3
""".111 independent raw terminal/tag audit of canonical accepted OOM attempts."""
import argparse, csv, hashlib, json, re
from collections import Counter
from pathlib import Path

def audit(root):
    result=[]
    for summary in sorted(root.glob('run_*/summary.csv')):
        campaign=json.loads((summary.parent/'campaign.json').read_text())
        for row in csv.DictReader(summary.open()):
            case=Path(row['artifact_dir']);saved=json.loads((case/'result.json').read_text())
            assert saved['row']['noise_excluded'] is False
            noise=json.loads((case/'noise_qualification.json').read_text())
            assert noise['excluded'] is False and not noise['findings']
            assert not any((case/n).exists() for n in ['FAILED.txt','cleanup_errors.txt','NOISE_EXCLUDED.txt'])
            workload=summary.parent/'workloads'/f"ycsb_{row['workload']}_skew_{float(row['skew'])}_{row['total_queries']}.sql"
            assert hashlib.sha256(workload.read_bytes()).hexdigest()==row['workload_sha256']
            sql=workload.read_text().splitlines();assert len(sql)==20000
            records=re.findall(r'^single-gateway-direct-(\d+)\s+1\s+(.*)$',(case/'server.log').read_text(),re.M)
            assert len(records)==20000 and {int(i) for i,_ in records}==set(range(1,20001))
            tags=Counter()
            for i,line in records:
                stmt=sql[int(i)-1].lstrip().upper()
                kind='UPDATE' if stmt.startswith('WITH ') else stmt.split()[0]
                expected={'SELECT':'SELECT 1','UPDATE':'UPDATE 1','INSERT':'INSERT 0 1','DELETE':'DELETE 1'}[kind]
                assert line==expected or line.startswith(expected+' '),(i,line,expected)
                tags[kind]+=1
            gateway=(case/'gateway.log').read_text()
            success=re.findall(r'direct_terminal_success_count=(\d+)',gateway)
            assert success and int(success[-1])==20000
            for name in ['divergence_count','permanent_failures','nonterminal_failure_count','deterministic_error_count']:
                values=re.findall(name+r'=(\d+)',gateway)
                assert values and all(int(v)==0 for v in values),(case,name)
            assert saved['gateway']['gateway_returncode']==0
            if row['mode']=='bcdb_merkle':
                assert (case/'merkle_verify.txt').read_text().strip()=='t'
                assert saved['merkle_stats']['version']==11
            setup=json.loads((case/'setup.json').read_text())
            assert setup['settings']['default_transaction_isolation']=='serializable'
            assert setup['settings']['shared_buffers']=='4096'
            result.append(dict(case=str(case),mode=row['mode'],workers=int(row['workers']),trial=int(row['trial']),
                terminals=20000,operation_counts=dict(tags),gateway_exit=0,failures=0,merkle_verify=row['merkle_verify'],
                noise_excluded=False,pass_check=True))
    return result

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--root',type=Path,required=True);p.add_argument('--out',type=Path,required=True)
    a=p.parse_args();r=audit(a.root);a.out.write_text(json.dumps(r,indent=2)+'\n')
    print(json.dumps(dict(accepted_cases=len(r),terminals=20000*len(r),pass_check=True)))
