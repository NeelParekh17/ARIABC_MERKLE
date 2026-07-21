#!/usr/bin/env python3
"""
AriaBC Cluster Node Information Collector and Markdown Reporter.
Queries all 4 cluster nodes in parallel, parses system resources (RAM, Storage, CPU),
service statuses (PostgreSQL, ariabc_pg_server), and updates a Markdown report.
"""

import argparse
import concurrent.futures
import datetime
import json
import os
import subprocess
import sys

# Define cluster details
NODES = [
    {"id": 1, "name": "admin123", "user": "neel", "ip": "10.129.148.247", "client_port": 8000, "is_gateway": False},
    {"id": 2, "name": "user4", "user": "neel", "ip": "10.129.148.246", "client_port": 8000, "is_gateway": False},
    {"id": 4, "name": "utkarsh", "user": "neel", "ip": "10.129.148.248", "client_port": 8001, "is_gateway": False},
    {"id": 5, "name": "asus-laptop", "user": "neel", "ip": "127.0.0.1", "client_port": 8000, "is_gateway": True},
    {"id": 6, "name": "proposed-gw", "user": "neel", "ip": "10.129.27.111", "client_port": 8000, "is_gateway": True},
]

DEFAULT_PASSWORD = "clusterinfolab123"
DEFAULT_MD_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "CLUSTER_NODES_INFO.md")

# Remote payload to gather all details in a single JSON output
REMOTE_PROBE_SCRIPT = r"""import json, subprocess, os, socket

def run_cmd(cmd):
    try:
        return subprocess.check_output(cmd, shell=True, text=True).strip()
    except Exception:
        return ""

mem = {}
try:
    with open("/proc/meminfo") as f:
        for line in f:
            if ":" in line:
                k, v = line.split(":", 1)
                mem[k.strip()] = v.strip()
except:
    pass

df_root = run_cmd("df -hT /")
df_all = run_cmd("df -hT")
lsblk = run_cmd("lsblk -d -P -o NAME,MODEL,SIZE,ROTA,TYPE")

# Root device finding
def get_root_device():
    try:
        with open("/proc/mounts") as f:
            for line in f:
                parts = line.split()
                if len(parts) >= 2 and parts[1] == "/":
                    dev = parts[0]
                    if dev.startswith("/dev/"):
                        import re
                        base = dev[5:]
                        base = re.sub(r'p\d+$', '', base)
                        base = re.sub(r'\d+$', '', base)
                        return base
    except:
        pass
    return "sda"

root_dev = get_root_device()

# iostat -x 1 2
def get_iostat_stats(r_dev):
    try:
        # Run iostat for 2 iterations with 1 sec interval
        output = subprocess.check_output("iostat -x 1 2", shell=True, text=True)
        reports = output.split("avg-cpu:")
        if len(reports) >= 3:
            report = reports[2]
        else:
            report = reports[-1]
            
        lines = report.strip().splitlines()
        headers = []
        device_stats = {}
        for line in lines:
            if "Device" in line:
                headers = line.split()
                continue
            if headers and line.strip():
                parts = line.split()
                if len(parts) >= len(headers):
                    dev_name = parts[0]
                    device_stats[dev_name] = dict(zip(headers, parts))
                    
        stats = None
        for k, v in device_stats.items():
            if k == r_dev or r_dev.startswith(k) or k.startswith(r_dev):
                stats = v
                break
        if not stats and device_stats:
            for k, v in device_stats.items():
                if not k.startswith("loop"):
                    stats = v
                    break
                    
        if stats:
            r_val = float(stats.get("rMB/s", stats.get("rkB/s", 0.0)))
            if "rkB/s" in stats:
                r_val /= 1024.0
            w_val = float(stats.get("wMB/s", stats.get("wkB/s", 0.0)))
            if "wkB/s" in stats:
                w_val /= 1024.0
            util = float(stats.get("%util", 0.0))
            
            await_val = 0.0
            if "await" in stats:
                await_val = float(stats.get("await", 0.0))
            elif "r_await" in stats and "w_await" in stats:
                r_s = float(stats.get("r/s", 0.0))
                w_s = float(stats.get("w/s", 0.0))
                r_await = float(stats.get("r_await", 0.0))
                w_await = float(stats.get("w_await", 0.0))
                if (r_s + w_s) > 0:
                    await_val = (r_s * r_await + w_s * w_await) / (r_s + w_s)
            
            return {
                "read_mb_s": f"{r_val:.2f}",
                "write_mb_s": f"{w_val:.2f}",
                "util_pct": f"{util:.2f}",
                "await_ms": f"{await_val:.2f}",
                "device_name": stats.get("Device", r_dev)
            }
    except Exception as e:
        return {"error": str(e)}
        
    return {
        "read_mb_s": "0.00",
        "write_mb_s": "0.00",
        "util_pct": "0.00",
        "await_ms": "0.00",
        "device_name": r_dev
    }

io_stats = get_iostat_stats(root_dev)

# Network latency ping tests
latencies = {}
# TARGET_NODES is dynamically injected by controller
for target in TARGET_NODES:
    t_name = target["name"]
    t_ip = target["ip"]
    try:
        cmd = f"ping -c 3 -W 1 {t_ip}"
        out = subprocess.check_output(cmd, shell=True, text=True)
        avg_rtt = "N/A"
        for line in out.splitlines():
            if "rtt" in line or "min/avg/max" in line:
                parts = line.split("=")[1].strip().split("/")
                avg_rtt = parts[1]
                break
        latencies[t_name] = f"{avg_rtt} ms"
    except Exception:
        latencies[t_name] = "Timeout/Error"

def check_port(port):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(0.5)
    try:
        s.bind(("127.0.0.1", port))
        s.close()
        return "Stopped"
    except socket.error:
        return "Running"

pg_status = check_port(5438)
server_8000_status = check_port(8000)
server_8001_status = check_port(8001)

info = {
    "hostname": socket.gethostname(),
    "fqdn": socket.getfqdn(),
    "os": run_cmd(". /etc/os-release && echo $PRETTY_NAME") or run_cmd("lsb_release -ds"),
    "kernel": run_cmd("uname -r"),
    "cpu_model": run_cmd("lscpu | grep 'Model name' | cut -d':' -f2 | xargs") or run_cmd("awk -F: '/model name/ {print $2; exit}' /proc/cpuinfo").strip(),
    "cpu_threads": run_cmd("nproc"),
    "ip4": run_cmd("hostname -I"),
    "mem_total": mem.get("MemTotal", ""),
    "mem_free": mem.get("MemFree", ""),
    "mem_avail": mem.get("MemAvailable", ""),
    "swap_total": mem.get("SwapTotal", ""),
    "swap_free": mem.get("SwapFree", ""),
    "df_root": df_root,
    "df_all": df_all,
    "lsblk": lsblk,
    "pg_status": pg_status,
    "server_8000_status": server_8000_status,
    "server_8001_status": server_8001_status,
    "io_stats": io_stats,
    "latencies": latencies
}
print(json.dumps(info))
"""

def parse_mem(mem_total, mem_avail):
    try:
        total_kb = int(''.join(c for c in mem_total if c.isdigit()))
        avail_kb = int(''.join(c for c in mem_avail if c.isdigit()))
        used_kb = total_kb - avail_kb
        used_pct = (used_kb / total_kb) * 100
        return {
            "total_gb": f"{total_kb / (1024 * 1024):.2f} GB",
            "avail_gb": f"{avail_kb / (1024 * 1024):.2f} GB",
            "used_gb": f"{used_kb / (1024 * 1024):.2f} GB",
            "used_pct": f"{used_pct:.1f}%"
        }
    except Exception:
        return {
            "total_gb": mem_total or "N/A",
            "avail_gb": mem_avail or "N/A",
            "used_gb": "N/A",
            "used_pct": "N/A"
        }

def parse_df(df_str):
    try:
        lines = df_str.strip().splitlines()
        if len(lines) >= 2:
            parts = lines[1].split()
            if len(parts) >= 7:
                return {
                    "fs": parts[0],
                    "type": parts[1],
                    "size": parts[2],
                    "used": parts[3],
                    "avail": parts[4],
                    "use_pct": parts[5],
                    "mount": parts[6]
                }
    except Exception:
        pass
    return {
        "fs": "N/A",
        "type": "N/A",
        "size": "N/A",
        "used": "N/A",
        "avail": "N/A",
        "use_pct": "N/A",
        "mount": "N/A"
    }

def probe_node(node, targets, password, ssh_port, ssh_key, timeout_s):
    node_str = f"{node['user']}@{node['ip']}"
    
    # Prepend target list of other nodes to REMOTE_PROBE_SCRIPT
    payload = f"TARGET_NODES = {json.dumps(targets)}\n" + REMOTE_PROBE_SCRIPT

    # Try with passwordless SSH first (BatchMode=yes)
    ssh_cmd = [
        "ssh",
        "-o", "BatchMode=yes",
        "-o", "StrictHostKeyChecking=no",
        "-o", f"ConnectTimeout={timeout_s}",
        "-p", str(ssh_port)
    ]
    if ssh_key:
        ssh_cmd += ["-i", ssh_key]
    ssh_cmd += [node_str, "python3", "-"]

    try:
        res = subprocess.run(
            ssh_cmd,
            input=payload,
            text=True,
            capture_output=True,
            timeout=timeout_s + 5
        )
        if res.returncode == 0:
            return node, True, json.loads(res.stdout), None
    except Exception as e:
        pass

    # If passwordless fails, try fallback using sshpass if password is provided
    if password:
        sshpass_cmd = [
            "sshpass", "-p", password,
            "ssh",
            "-o", "StrictHostKeyChecking=no",
            "-o", f"ConnectTimeout={timeout_s}",
            "-p", str(ssh_port)
        ]
        if ssh_key:
            sshpass_cmd += ["-i", ssh_key]
        sshpass_cmd += [node_str, "python3", "-"]

        try:
            res = subprocess.run(
                sshpass_cmd,
                input=payload,
                text=True,
                capture_output=True,
                timeout=timeout_s + 5
            )
            if res.returncode == 0:
                return node, True, json.loads(res.stdout), None
            else:
                err = res.stderr.strip() or res.stdout.strip() or f"exit code {res.returncode}"
                return node, False, None, err
        except Exception as e:
            return node, False, None, str(e)

    return node, False, None, "SSH connection timed out or auth failed (BatchMode failed and sshpass password not used/available)"

def format_lsblk(lsblk_str):
    if not lsblk_str.strip():
        return "*None*"
    lines = []
    for line in lsblk_str.splitlines():
        # Parse fields from KEY="VALUE"
        items = {}
        import re
        for match in re.finditer(r'(\w+)="([^"]*)"', line):
            items[match.group(1)] = match.group(2)
        if items.get("TYPE") in ["disk", "md"]:
            lines.append(f"- **{items.get('NAME', '?')}**: {items.get('MODEL', 'Unknown Model')} ({items.get('SIZE', '?')})")
    return "\n".join(lines) if lines else "*None*"

def main():
    parser = argparse.ArgumentParser(description="Query cluster nodes and update info markdown.")
    parser.add_argument("--out", default=DEFAULT_MD_PATH, help="Path to write the markdown report")
    parser.add_argument("--ssh-port", type=int, default=22, help="SSH Port")
    parser.add_argument("--ssh-key", default="", help="Path to SSH private key")
    parser.add_argument("--password", default="", help="Cluster password (overrides default/env)")
    parser.add_argument("--timeout", type=int, default=8, help="Connect timeout per node")
    args = parser.parse_args()

    password = args.password or os.environ.get("ARIABC_CLUSTER_PASSWORD") or DEFAULT_PASSWORD
    ssh_key = args.ssh_key.strip() or None

    print(f"Starting parallel probe on 4 cluster nodes...")
    
    results = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        futures = {}
        for node in NODES:
            targets = [{"name": n["name"], "ip": n["ip"]} for n in NODES if n["id"] != node["id"]]
            futures[executor.submit(probe_node, node, targets, password, args.ssh_port, ssh_key, args.timeout)] = node["id"]
        for future in concurrent.futures.as_completed(futures):
            node_id = futures[future]
            try:
                node, reachable, data, error = future.result()
                results[node_id] = {
                    "node": node,
                    "reachable": reachable,
                    "data": data,
                    "error": error
                }
            except Exception as e:
                results[node_id] = {
                    "node": [n for n in NODES if n["id"] == node_id][0],
                    "reachable": False,
                    "data": None,
                    "error": str(e)
                }

    # Generate Markdown Report
    now_str = datetime.datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S %z")
    
    md = []
    md.append("# 🖥️ AriaBC 4-Node Cluster Inventory & Live Status")
    md.append("")
    md.append(f"> **Last Updated:** `{now_str}`  ")
    md.append("> **Automatic Update:** Yes (Updated via `scripts/distributed/update_nodes_info.py`)  ")
    md.append("> **Service Sockets Checked:** BCDB PostgreSQL (Port `5438`), AriaBC Server (Port `8000`/`8001`)  ")
    md.append("")
    md.append("## 📊 Cluster Summary Table")
    md.append("")
    md.append("| Node | Name | IP Address | Status | OS | CPU | RAM (Total/Avail) | Root Storage (Avail/Use%) | Disk Read MB/s | Disk Write MB/s | Disk Util % | Await | PostgreSQL | AriaBC Server |")
    md.append("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|")
 
    for node_id in sorted(results.keys()):
        res = results[node_id]
        n = res["node"]
        client_port = n["client_port"]
        
        if res["reachable"]:
            d = res["data"]
            status = "🟢 Online"
            os_name = d.get("os", "N/A")
            cpu = f"{d.get('cpu_threads', '?')} Cores"
            
            mem_parsed = parse_mem(d.get("mem_total", ""), d.get("mem_avail", ""))
            ram = f"{mem_parsed['total_gb']} / {mem_parsed['avail_gb']} ({mem_parsed['used_pct']} used)"
            
            df_parsed = parse_df(d.get("df_root", ""))
            storage = f"{df_parsed['avail']} / {df_parsed['size']} ({df_parsed['use_pct']})"
            
            io = d.get("io_stats", {})
            disk_read = io.get("read_mb_s", "0.00")
            disk_write = io.get("write_mb_s", "0.00")
            disk_util = io.get("util_pct", "0.00") + "%"
            disk_await = io.get("await_ms", "0.00") + " ms"
            
            if n.get("is_gateway", False):
                pg_status = "—"
                srv_status = "—"
            else:
                pg_status = "🟢 Running" if d.get("pg_status") == "Running" else "🔴 Stopped"
                # check the port configured for this server
                srv_status_val = d.get(f"server_{client_port}_status", "Stopped")
                srv_status = f"🟢 Running (:{client_port})" if srv_status_val == "Running" else "🔴 Stopped"
        else:
            status = "🔴 Offline"
            os_name = "N/A"
            cpu = "N/A"
            ram = "N/A"
            storage = "N/A"
            disk_read = "N/A"
            disk_write = "N/A"
            disk_util = "N/A"
            disk_await = "N/A"
            pg_status = "N/A"
            srv_status = "N/A"
            
        if n.get("is_gateway", False):
            if n["id"] == 5:
                node_label = "**ASUS Laptop (GW)**"
            elif n["id"] == 6:
                node_label = "**Gateway 2 (Proposed)**"
            else:
                node_label = f"**{n['name']} (GW)**"
        else:
            node_label = f"**Node {n['id']}**"
        md.append(f"| {node_label} | {n['name']} | `{n['ip']}` | {status} | {os_name} | {cpu} | {ram} | {storage} | {disk_read} | {disk_write} | {disk_util} | {disk_await} | {pg_status} | {srv_status} |")
 
    md.append("")
    md.append("## 🌐 Network Latency Matrix (RTT)")
    md.append("")
    md.append("| From | To | RTT |")
    md.append("|---|---|---|")
 
    for f_id in sorted(results.keys()):
        res_f = results[f_id]
        n_from = res_f["node"]
        for t_id in sorted(results.keys()):
            if f_id == t_id:
                continue
            res_t = results[t_id]
            n_to = res_t["node"]
            
            if res_f["reachable"]:
                rtt = res_f["data"]["latencies"].get(n_to["name"], "Timeout/Error")
            else:
                rtt = "Offline"
                
            md.append(f"| {n_from['name']} (Node {n_from['id']}) | {n_to['name']} (Node {n_to['id']}) | {rtt} |")

    md.append("")
    md.append("## 🔍 Detailed Node Inventory")
    md.append("")

    for node_id in sorted(results.keys()):
        res = results[node_id]
        n = res["node"]
        client_port = n["client_port"]
        
        md.append(f"### 🖥️ Node {n['id']}: {n['name']} (`{n['ip']}`)")
        md.append("")
        
        if not res["reachable"]:
            md.append("> [!CAUTION]")
            md.append("> **Node is offline or unreachable over SSH.**  ")
            md.append(f"> **Error:** `{res['error']}`")
            md.append("")
            continue
            
        d = res["data"]
        mem_parsed = parse_mem(d.get("mem_total", ""), d.get("mem_avail", ""))
        df_parsed = parse_df(d.get("df_root", ""))
        
        md.append("#### ⚙️ System Specifications")
        md.append(f"- **Host/FQDN:** `{d.get('fqdn')}`")
        md.append(f"- **Operating System:** {d.get('os')}")
        md.append(f"- **Kernel Version:** `{d.get('kernel')}`")
        md.append(f"- **CPU Model:** `{d.get('cpu_model')}`")
        md.append(f"- **CPU Logical Cores (Threads):** `{d.get('cpu_threads')}`")
        md.append(f"- **IPv4 Addresses:** `{d.get('ip4')}`")
        md.append("")
        
        md.append("#### 🧠 Memory (RAM) Allocation")
        md.append(f"- **Total RAM:** {mem_parsed['total_gb']}")
        md.append(f"- **Available RAM:** {mem_parsed['avail_gb']}")
        md.append(f"- **Used RAM:** {mem_parsed['used_gb']} ({mem_parsed['used_pct']})")
        if d.get("swap_total") and int(''.join(c for c in d.get("swap_total") if c.isdigit())) > 0:
            swap_total_kb = int(''.join(c for c in d.get("swap_total") if c.isdigit()))
            swap_free_kb = int(''.join(c for c in d.get("swap_free", "0") if c.isdigit()))
            swap_used_kb = swap_total_kb - swap_free_kb
            md.append(f"- **Swap Space:** {swap_used_kb / (1024 * 1024):.2f} GB used of {swap_total_kb / (1024 * 1024):.2f} GB total")
        else:
            md.append("- **Swap Space:** Disabled / None")
        md.append("")
        
        md.append("#### 💾 Storage & Disks")
        md.append(f"- **Root Mount Filesystem:** `{df_parsed['fs']}` ({df_parsed['type']})")
        md.append(f"- **Root Disk Space:** {df_parsed['used']} used / {df_parsed['avail']} free (Total: {df_parsed['size']}, Use%: {df_parsed['use_pct']})")
        
        io = d.get("io_stats", {})
        if "error" in io:
            md.append(f"- **Disk I/O Stats Error:** `{io['error']}`")
        else:
            md.append(f"- **Disk I/O Read Speed:** `{io.get('read_mb_s', '0.00')} MB/s` (Device: `{io.get('device_name', 'unknown')}`)")
            md.append(f"- **Disk I/O Write Speed:** `{io.get('write_mb_s', '0.00')} MB/s`")
            md.append(f"- **Disk Utilization:** `{io.get('util_pct', '0.00')}%`")
            md.append(f"- **Average Disk Await Time:** `{io.get('await_ms', '0.00')} ms`")
            
        md.append("- **Physical Disks / RAID Groups:**")
        md.append(format_lsblk(d.get("lsblk", "")))
        md.append("")
        md.append("##### Selected `df -hT` output:")
        md.append("```text")
        md.append(d.get("df_all", ""))
        md.append("```")
        md.append("")
        
        if n.get("is_gateway", False):
            md.append("#### 🔌 Port & Service Sockets Status")
            md.append("- **Role:** Gateway Machine (No local PostgreSQL database or AriaBC Server running)")
        else:
            md.append("#### 🔌 Port & Service Sockets Status")
            pg_status_lbl = "🟢 Running (Accepting connections)" if d.get("pg_status") == "Running" else "🔴 Stopped"
            srv_status_lbl = f"🟢 Running (Accepting client traffic)" if d.get(f"server_{client_port}_status") == "Running" else "🔴 Stopped"
            
            md.append(f"- **PostgreSQL DB Server (Port `5438`):** {pg_status_lbl}")
            md.append(f"- **AriaBC Raft Client Server (Port `{client_port}`):** {srv_status_lbl}")
        md.append("")

    # Write to target path
    out_file = args.out
    try:
        with open(out_file, "w", encoding="utf-8") as f:
            f.write("\n".join(md) + "\n")
        print(f"Successfully wrote updated node details to {out_file}")
    except Exception as e:
        print(f"Error writing to file: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
