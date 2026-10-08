"""usage: run_io.py <gateway.log> <iostat.txt>  -> prints f_await/w_await/util over the measured gateway window"""
import re, datetime, sys
gw, io = sys.argv[1], sys.argv[2]
t = open(gw).read()
ts = [int(x) for x in re.findall(r"PROGRESS_GATEWAY_DET wall_time_unix_ms=(\d+)", t)]
el = [float(x) for x in re.findall(r"PROGRESS_GATEWAY_DET .*?elapsed_s=([\d.]+)", t)]
if not ts:
    print("io=na"); sys.exit(0)
e = ts[-1] / 1000; s = e - el[-1]
hdr = None; cur = None; rows = []
for line in open(io):
    m = re.match(r"(\d\d/\d\d/\d{4} \d\d:\d\d:\d\d [AP]M)", line)
    if m:
        cur = datetime.datetime.strptime(m.group(1), "%m/%d/%Y %I:%M:%S %p").timestamp(); continue
    if line.startswith("Device"):
        hdr = line.split(); continue
    if line.startswith("nvme0n1 ") and cur and s <= cur <= e:
        rows.append(dict(zip(hdr, line.split())))
avg = lambda k: sum(float(r[k]) for r in rows) / max(len(rows), 1)
print("f_await=%.2f w_await=%.2f util=%.0f io_n=%d" % (avg("f_await"), avg("w_await"), avg("%util"), len(rows)))
