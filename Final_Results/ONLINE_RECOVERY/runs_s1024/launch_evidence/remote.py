import datetime, fcntl, json, pathlib, shlex, subprocess, sys, uuid
B = pathlib.Path(__file__).parent
C = json.loads((B/'context.json').read_text())
def log(data):
    with (B/'commands.log').open('a') as f:
        fcntl.flock(f,fcntl.LOCK_EX); f.write(json.dumps(data)+'\n'); f.flush()
def run(host, command, label, stdin=None, argv=None):
    ident=uuid.uuid4().hex
    args=argv or ['ssh','-o','BatchMode=yes','-o','ConnectTimeout=12', 'neel@'+host,command]
    log(dict(id=ident,event='start',host=host,command=command,argv=args,time=datetime.datetime.now(datetime.timezone.utc).isoformat()))
    out=B/'ops'/(label+'.log')
    with out.open('xb') as f:
        p=subprocess.run(args,input=stdin,stdout=f,stderr=subprocess.STDOUT)
    log(dict(id=ident,event='end',host=host,command=command,time=datetime.datetime.now(datetime.timezone.utc).isoformat(),exit_code=p.returncode,output=str(out)))
    print(host,label,'rc='+str(p.returncode),str(out),flush=True)
    return p.returncode
if __name__=='__main__':
    host,label,file=sys.argv[1:]
    command=pathlib.Path(file).read_text().replace('@RBASE@',C['base']).replace('@RTAG@',C['tag'])
    sys.exit(run(host,command,label))
