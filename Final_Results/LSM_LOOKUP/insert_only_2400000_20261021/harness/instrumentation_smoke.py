#!/usr/bin/env python3
"""Controller-host positive control for instrumentation; no database involved.

Compiles an isolated mock process in our .111 scratch directory and observes
only its known calls/offsets. This is not engine correctness/performance evidence.
"""
import ast
import ctypes
import importlib.util
import json
import os
from pathlib import Path
import shlex
import subprocess
import sys
import time

ROOT=Path('/home/neel/claude_ctl/lsm_relation_audit_20261021')
WORK=ROOT/'scratch/instrumentation_smoke'
WORK.mkdir(parents=True,exist_ok=True)
os.environ['TMPDIR']=str(ROOT/'scratch')
sys.path.insert(0,str(ROOT/'scripts'))
from summarize import parse_trace

C=r'''
#include <stdint.h>
#include <stddef.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/prctl.h>
typedef struct { uint32_t spcNode,dbNode,relNode; } RelFileNode;
typedef struct { RelFileNode rnode; int forkNum; unsigned blockNum; } BufferTag;
typedef struct { BufferTag tag; int buf_id; unsigned state; char pad[32]; } BufferDesc;
typedef union { BufferDesc bufferdesc; char pad[64]; } BufferDescPadded;
typedef struct { RelFileNode rd_node; unsigned rd_id; } RelationData;
typedef struct { long shared_blks_hit,shared_blks_read,shared_blks_dirtied; } BufferUsage;
BufferDescPadded desc[4];
BufferDescPadded *BufferDescriptors=desc;
BufferUsage pgBufferUsage;
volatile int dc,hc;
__attribute__((noinline)) void MarkBufferDirty(int b) { if (++dc%3==0) pgBufferUsage.shared_blks_dirtied++; }
__attribute__((noinline)) void MarkBufferDirtyHint(int b,int std) { if (++hc%5==0) pgBufferUsage.shared_blks_dirtied++; }
__attribute__((noinline)) void RelationSetNewRelfilenode(RelationData *r,char p) { r->rd_node.relNode=17000; }
int main(int argc,char**argv) {
 prctl(PR_SET_NAME,"postgres",0,0,0);
 desc[0].bufferdesc.tag.rnode.dbNode=1;desc[0].bufferdesc.tag.rnode.relNode=16400;
 printf("{\"pid\":%d,\"BUFVAR\":%lu,\"USAGEVAR\":%lu}\n",getpid(),(unsigned long)&BufferDescriptors,(unsigned long)&pgBufferUsage);fflush(stdout);
 while(access(argv[1],F_OK)) usleep(20000);
 for(int i=0;i<30;i++)MarkBufferDirty(1);
 for(int i=0;i<30;i++)MarkBufferDirtyHint(1,1);
 RelationData r={{0,1,16400},16399};RelationSetNewRelfilenode(&r,'p');
 char b[8192];memset(b,'x',sizeof(b));int fd=open(argv[2],O_RDWR|O_CREAT|O_TRUNC,0600);
 pwrite(fd,b,sizeof(b),0);pwrite(fd,b,sizeof(b),8192);pwrite(fd,b,sizeof(b),32768);
 pread(fd,b,sizeof(b),0);pread(fd,b,sizeof(b),8192);pread(fd,b,sizeof(b),32768);
 close(fd);return 0;
}
'''
(WORK/'mock.c').write_text(C)
(WORK/'bin').mkdir(exist_ok=True)
binary=WORK/'bin/postgres'
subprocess.run(['gcc','-O0','-g','-fPIE','-pie','-rdynamic','-o',str(binary),str(WORK/'mock.c')],check=True)
expressions=dict(RELID='(size_t)&((RelationData*)0)->rd_id',RELFILENODE='(size_t)&((RelationData*)0)->rd_node.relNode',
    BD_SIZE='sizeof(BufferDescPadded)',BD_REL='(size_t)&((BufferDesc*)0)->tag.rnode.relNode',
    BD_DB='(size_t)&((BufferDesc*)0)->tag.rnode.dbNode',BD_FORK='(size_t)&((BufferDesc*)0)->tag.forkNum',
    BUFVAR='(size_t)&BufferDescriptors',USAGEVAR='(size_t)&pgBufferUsage',USAGEOFFSET='(size_t)&((BufferUsage*)0)->shared_blks_dirtied')
cmd=['gdb','-q','-batch',str(binary)]
for key,expr in expressions.items():cmd+=['-ex',f'printf "{key}=%lu\\n",(unsigned long)({expr})']
r=subprocess.run(cmd,text=True,capture_output=True,check=True)
(WORK/'gdb.txt').write_text(r.stdout+r.stderr)
import re
offsets=dict(re.findall(r'([A-Z_]+)=(\d+)',r.stdout))
assert len(offsets)==len(expressions),r.stdout
ready=WORK/'go';ready.unlink(missing_ok=True)
info=(WORK/'mock.json').open('w')
child=subprocess.Popen([str(binary),str(ready),str(WORK/'data')],stdout=info)
try:
 for _ in range(100):
  try:state=json.loads((WORK/'mock.json').read_text());break
  except (ValueError,FileNotFoundError):time.sleep(.02)
 else:raise RuntimeError('mock did not become ready')
 maps=Path('/proc/'+str(child.pid)+'/maps').read_text().splitlines()
 base=next(int(s.split('-')[0],16) for s in maps if s.endswith(str(binary)) and s.split()[2]=='00000000')
 for key in ['BUFVAR','USAGEVAR']:assert base+int(offsets[key])==state[key],(key,base,offsets,state)
 tree=ast.parse((ROOT/'scripts/audit.py').read_text())
 prep=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='prepare')
 expr=next(n.value for n in prep.body if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='addition' for t in n.targets))
 addition=eval(compile(ast.Expression(expr),'<actual audit template>','eval'),dict(INSTALL=str(WORK),offsets=offsets))
 source=(ROOT/'scripts/io.bt').read_text()+addition
 for key in ['BUFVAR','USAGEVAR']:source=source.replace('AUDIT_'+key,str(state[key]))
 (WORK/'effective.bt').write_text(source)
 spec=importlib.util.spec_from_file_location('audit',ROOT/'scripts/audit.py')
 module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
 h=module.load_harness();h.load_sudo_password()
 command=f'setsid nohup env TMPDIR={ROOT}/scratch BPFTRACE_MAX_MAP_KEYS=65536 bpftrace -q {WORK}/effective.bt > {WORK}/trace.txt 2> {WORK}/trace.stderr < /dev/null & echo $! > {WORK}/trace.pid'
 result=subprocess.run(['bash','-s'],input=h.sudo_command(command),capture_output=True,text=True,check=True)
 for _ in range(100):
  if 'AUDIT_TRACE_READY' in (WORK/'trace.txt').read_text():break
  if 'ERROR' in (WORK/'trace.stderr').read_text():raise RuntimeError((WORK/'trace.stderr').read_text())
  time.sleep(.1)
 else:raise RuntimeError('trace did not start')
 libc=ctypes.CDLL(None);libc.prctl(15,b'audit_phase',0,0,0);libc.getpgid(-1001)
 ready.touch();child.wait(timeout=10);time.sleep(.2)
 pid=int((WORK/'trace.pid').read_text())
 subprocess.run(['bash','-s'],input=h.sudo_command('kill -INT '+str(pid)),capture_output=True,text=True,check=True)
 for _ in range(100):
  if not Path('/proc/'+str(pid)).exists() or Path('/proc/'+str(pid)+'/stat').read_text().split()[2]=='Z':break
  time.sleep(.1)
 parsed=parse_trace(WORK/'trace.txt')
 counts={}
 for metric in ['dirty_calls','dirtied']:
  for hint in [0,1]:counts[metric+'_'+str(hint)]=sum(v for k,v in parsed[metric].items() if k[0]==child.pid and k[1:]==(1,16400,1,0,hint))
 assert counts==dict(dirty_calls_0=30,dirty_calls_1=30,dirtied_0=10,dirtied_1=6),counts
 st=(WORK/'data').stat();dev=os.major(st.st_dev)*1048576+os.minor(st.st_dev)
 generation=next(k[2] for k,v in parsed['names'].items() if k[:2]==(dev,st.st_ino) and v=='data')
 for operation in [0,1]:
  key=(child.pid,dev,st.st_ino,generation,1,operation)
  assert parsed['ops'][key]==3 and parsed['bytes'][key]==24576,(key,parsed['ops'],parsed['bytes'])
  assert parsed['distance'][key+(1,)]==1 and parsed['distance'][key+(3,)]==1
  assert parsed['contiguous'][key]==1
 assert parsed['replacements'][(16399,17000)]==1
 report=dict(result='PASS',counts=counts,file_read_ops=3,file_write_ops=3,bytes_per_direction=24576,
    distance_bins_per_direction={'+1 block':1,'2..7 blocks':1},contiguous_pairs_per_direction=1,
    replacement=[16399,17000],file_generation=generation,pie_base_address_verified=True,evidence=str(WORK))
 (ROOT/'evidence/instrumentation_smoke.json').write_text(json.dumps(report,indent=2)+'\n')
 print(json.dumps(report,indent=2))
finally:
 if child.poll() is None:child.terminate();child.wait(timeout=5)
 info.close()
