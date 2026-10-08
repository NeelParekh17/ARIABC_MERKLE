import re,datetime,sys
R="/home/protectdr/claude_checks/tpcc_sweep_v2_dx"
def window(gw):
    t=open(gw).read()
    ts=[int(x) for x in re.findall(r"PROGRESS_GATEWAY_DET wall_time_unix_ms=(\d+)",t)]
    el=[float(x) for x in re.findall(r"PROGRESS_GATEWAY_DET .*?elapsed_s=([\d.]+)",t)]
    end=ts[-1]/1000; return end-el[-1], end
runs=[("pin",1),("nopin",2),("pin",3),("nopin",4),("pin",5),("nopin",6)]
for tag,i in runs:
    s,e=window(f"{R}/diag_{tag}/det_d_t{i}_w5/gateway.log")
    hdr=None; cur=None; rows=[]
    for line in open(f"{R}/diag_{tag}/iostat_{i}.txt"):
        m=re.match(r"(\d\d/\d\d/\d{4} \d\d:\d\d:\d\d [AP]M)",line)
        if m: cur=datetime.datetime.strptime(m.group(1),"%m/%d/%Y %I:%M:%S %p").timestamp(); continue
        if line.startswith("Device"): hdr=line.split(); continue
        if line.startswith("nvme0n1 ") and cur and s<=cur<=e: rows.append(dict(zip(hdr,line.split())))
    avg=lambda k: sum(float(r[k]) for r in rows)/max(len(rows),1)
    vm=[l.split() for l in open(f"{R}/diag_{tag}/vmstat_{i}.txt") if re.match(r"\s*\d",l)]
    vmw=[r for r in vm if s<=datetime.datetime.strptime(r[-2]+" "+r[-1],"%Y-%m-%d %H:%M:%S").timestamp()<=e]
    va=lambda idx: sum(float(r[idx]) for r in vmw)/max(len(vmw),1)
    print("i=%d %-5s dur=%5.1fs n=%d w/s=%6.0f wMB/s=%6.1f w_await=%6.2f f/s=%5.0f f_await=%6.2f util=%5.1f | r=%.1f b=%.1f cs=%.0f us=%.0f sy=%.0f wa=%.0f" % (
        i,tag,e-s,len(rows),avg("w/s"),avg("wkB/s")/1024,avg("w_await"),avg("f/s"),avg("f_await"),avg("%util"),va(0),va(1),va(11),va(12),va(13),va(15)))
