#!/usr/bin/env python3
"""Probe SSH reachability + system info for dram_cache_machines_with_ip.csv."""

import csv, sys, time, concurrent.futures
from datetime import datetime

try:
    import paramiko
except ImportError:
    print("paramiko not found; install with: pip install paramiko")
    sys.exit(1)

USERNAME = "protectdb"
PASSWORD = "uplink"
CONNECT_TIMEOUT = 8  # seconds
CMD_TIMEOUT = 10

MACHINES = [
    ("sl2-112",  "10.130.154.112"),
    ("cs101-122","10.130.152.122"),
    ("cs101-123","10.130.152.123"),
    ("cs101-125","10.130.152.125"),
    ("cs101-127","10.130.152.127"),
    ("cs101-129","10.130.152.129"),
    ("cs101-131","10.130.152.131"),
    ("cs101-133","10.130.152.133"),
    ("cs101-136","10.130.152.136"),
    ("cs101-140","10.130.152.140"),
    ("cs101-142","10.130.152.142"),
    ("cs101-144","10.130.152.144"),
    ("cs101-146","10.130.152.146"),
    ("cs101-148","10.130.152.148"),
    ("sl1-16",   "10.130.153.16"),
    ("sl1-60",   "10.130.153.60"),
    ("sl2-101",  "10.130.154.101"),
    ("sl1-4",    "10.130.153.4"),
    ("sl1-30",   "10.130.153.30"),
    ("sl1-58",   "10.130.153.58"),
    ("sl2-32",   "10.130.154.32"),
    ("sl2-88",   "10.130.154.88"),
    ("sl2-116",  "10.130.154.116"),
    ("sl1-2",    "10.130.153.2"),
    ("sl1-23",   "10.130.153.23"),
    ("sl1-27",   "10.130.153.27"),
    ("sl1-32",   "10.130.153.32"),
    ("sl1-41",   "10.130.153.41"),
    ("sl1-53",   "10.130.153.53"),
    ("sl2-6",    "10.130.154.6"),
    ("sl2-48",   "10.130.154.48"),
    ("sl2-57",   "10.130.154.57"),
    ("sl2-109",  "10.130.154.109"),
    ("sl2-118",  "10.130.154.118"),
    ("sl3-2",    "10.130.155.2"),
]

SYSTEM_CMD = (
    "hostname; "
    "uname -r; "
    "grep 'PRETTY_NAME' /etc/os-release | cut -d= -f2 | tr -d '\"'; "
    "nproc; "
    "grep MemTotal /proc/meminfo | awk '{print $2}'; "
    "free -k | awk '/^Swap/{print $2}'; "
    "df -h / | tail -1; "
    "lsblk -d -o NAME,MODEL --noheadings 2>/dev/null | grep -v '^loop' || echo 'N/A'; "
    "uptime -p 2>/dev/null || uptime"
)


def run_cmd(client, cmd, timeout=CMD_TIMEOUT):
    _, stdout, stderr = client.exec_command(cmd, timeout=timeout)
    out = stdout.read().decode(errors="replace").strip()
    err = stderr.read().decode(errors="replace").strip()
    return out, err


def probe(name, ip):
    result = {
        "name": name,
        "ip": ip,
        "reachable": False,
        "error": "",
        "hostname": "",
        "os": "",
        "kernel": "",
        "threads": "",
        "ram_kb": "",
        "swap_kb": "",
        "root_df": "",
        "disks": "",
        "uptime": "",
    }
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        client.connect(
            ip,
            username=USERNAME,
            password=PASSWORD,
            timeout=CONNECT_TIMEOUT,
            auth_timeout=CONNECT_TIMEOUT,
            banner_timeout=CONNECT_TIMEOUT,
            look_for_keys=False,
            allow_agent=False,
        )
        result["reachable"] = True
        out, _ = run_cmd(client, SYSTEM_CMD)
        lines = out.split("\n")
        if len(lines) >= 1: result["hostname"] = lines[0]
        if len(lines) >= 2: result["kernel"]   = lines[1]
        if len(lines) >= 3: result["os"]       = lines[2]
        if len(lines) >= 4: result["threads"]  = lines[3]
        if len(lines) >= 5: result["ram_kb"]   = lines[4]
        if len(lines) >= 6: result["swap_kb"]  = lines[5]
        if len(lines) >= 7: result["root_df"]  = lines[6]
        if len(lines) >= 8:
            # disks may span multiple lines until uptime
            disk_lines = []
            for i in range(7, len(lines)-1):
                disk_lines.append(lines[i])
            result["disks"] = " | ".join(l.strip() for l in disk_lines if l.strip())
        if len(lines) >= 9: result["uptime"] = lines[-1]
    except Exception as e:
        result["error"] = str(e)
    finally:
        try:
            client.close()
        except Exception:
            pass
    return result


def fmt_kb(kb_str):
    try:
        kb = int(kb_str)
        return f"{kb/1024/1024:.1f} GB"
    except Exception:
        return kb_str


def main():
    print(f"Probing {len(MACHINES)} machines with {USERNAME}@... (timeout={CONNECT_TIMEOUT}s each)...")
    t0 = time.time()

    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=35) as ex:
        futures = {ex.submit(probe, name, ip): (name, ip) for name, ip in MACHINES}
        for f in concurrent.futures.as_completed(futures):
            r = f.result()
            results.append(r)
            status = "OK" if r["reachable"] else f"FAIL ({r['error'][:60]})"
            print(f"  {r['name']:12s} {r['ip']:18s} {status}")

    results.sort(key=lambda x: (x["ip"]))
    elapsed = time.time() - t0
    print(f"\nDone in {elapsed:.1f}s")

    reachable = [r for r in results if r["reachable"]]
    unreachable = [r for r in results if not r["reachable"]]
    print(f"Reachable: {len(reachable)}/{len(results)}")

    # Print TSV summary to stdout
    print("\n--- TSV summary ---")
    header = ["name","ip","reachable","hostname","os","kernel","threads","ram","swap","root_df","disks","uptime","error"]
    print("\t".join(header))
    for r in results:
        row = [
            r["name"], r["ip"],
            "YES" if r["reachable"] else "NO",
            r.get("hostname",""), r.get("os",""), r.get("kernel",""),
            r.get("threads",""),
            fmt_kb(r.get("ram_kb","")) if r.get("ram_kb") else "",
            fmt_kb(r.get("swap_kb","")) if r.get("swap_kb") else "",
            r.get("root_df",""),
            r.get("disks",""),
            r.get("uptime",""),
            r.get("error",""),
        ]
        print("\t".join(row))


if __name__ == "__main__":
    main()
