import pathlib,time,datetime
b=pathlib.Path(__file__).parent
f=pathlib.Path('/work/ARIABC/AriaBC/.bench_tmp/oom_ff90_20261001/status.txt')
while True:
 text=f.read_text()
 stamp=datetime.datetime.now(datetime.timezone.utc).isoformat()
 with (b/'oom_gate.log').open('a') as out:out.write(stamp+'\n'+text+'\n')
 if 'CHAIN_DONE' in text.splitlines():
  (b/'oom_gate_open.txt').write_text(stamp+'\n'+text)
  print('CHAIN_DONE observed '+stamp,flush=True);break
 time.sleep(60)
 time.sleep(60)
