#!/usr/bin/env python3
"""Install local id_ed25519 pub key into protectdb@<ip>:~/.ssh/authorized_keys."""

import concurrent.futures, paramiko, sys, os

USERNAME = "protectdb"
PASSWORD = "uplink"
CONNECT_TIMEOUT = 10

PUBKEY = open(os.path.expanduser("~/.ssh/id_ed25519.pub")).read().strip()

MACHINES = [
    ("sl2-112",  "10.130.154.112"),
    ("sl1-16",   "10.130.153.16"),
    ("sl2-101",  "10.130.154.101"),
    ("sl1-4",    "10.130.153.4"),
    ("sl1-30",   "10.130.153.30"),
    ("sl1-58",   "10.130.153.58"),
    ("sl2-88",   "10.130.154.88"),
]

def install_key(name, ip):
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        client.connect(ip, username=USERNAME, password=PASSWORD,
                       timeout=CONNECT_TIMEOUT, auth_timeout=CONNECT_TIMEOUT,
                       look_for_keys=False, allow_agent=False)
        cmd = (
            f"mkdir -p ~/.ssh && chmod 700 ~/.ssh && "
            f"grep -qxF '{PUBKEY}' ~/.ssh/authorized_keys 2>/dev/null || "
            f"echo '{PUBKEY}' >> ~/.ssh/authorized_keys && "
            f"chmod 600 ~/.ssh/authorized_keys && echo OK"
        )
        _, stdout, stderr = client.exec_command(cmd, timeout=10)
        out = stdout.read().decode().strip()
        err = stderr.read().decode().strip()
        client.close()
        return name, ip, True, out or err
    except Exception as e:
        return name, ip, False, str(e)

with concurrent.futures.ThreadPoolExecutor(max_workers=7) as ex:
    futs = [ex.submit(install_key, n, ip) for n, ip in MACHINES]
    for f in concurrent.futures.as_completed(futs):
        name, ip, ok, msg = f.result()
        status = "OK" if ok else "FAIL"
        print(f"  {name:12s} {ip:18s} {status}  {msg}")
