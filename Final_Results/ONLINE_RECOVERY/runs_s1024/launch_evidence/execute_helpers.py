import datetime
import remote
B=remote.B;BASE=remote.C['base'];TAG=remote.C['tag']
def status(message):
    stamp=datetime.datetime.now(datetime.timezone.utc).isoformat()
    (B/'execution_status.txt').write_text(stamp+' '+message+'\n')
    with (B/'execution_timeline.log').open('a') as f:f.write(stamp+' '+message+'\n')
    print(stamp,message,flush=True)
def command(host,cmd,label):
    if remote.run(host,cmd,label):raise RuntimeError(label+' failed')
def readscript(name):return (B/name).read_text().replace('@RBASE@',BASE).replace('@RTAG@',TAG)
