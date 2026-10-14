#!/usr/bin/env python3
import os,shutil,subprocess,sys
from pathlib import Path
B=Path(__file__).resolve().parent
n=int(sys.argv[1]);assert n in (12000,4000000)
p=B/'repo/scripts/restore_usertable_small.sql'
shutil.copyfile(B/f'restore_n{n}.sql',p)
for h in ('10.129.148.246','10.129.148.248'):
 subprocess.run(['rsync','-a',str(p),f'neel@{h}:{p}'],check=True)
