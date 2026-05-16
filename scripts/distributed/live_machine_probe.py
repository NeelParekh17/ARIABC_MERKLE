#!/usr/bin/env python3
"""Live machine reachability + system inventory probe.

This is intentionally lightweight and dependency-free so it can be used
before rerunning distributed benchmarks.

Definition of "reachable": a non-interactive SSH command succeeds
(`BatchMode=yes`) within the configured connect timeout.

Typical usage:
  python3 scripts/distributed/live_machine_probe.py

  python3 scripts/distributed/live_machine_probe.py \
    --nodes local,neel@10.129.148.248,neel@10.129.27.54,neel@10.129.148.236,neel@10.129.148.179
"""

from __future__ import annotations

import argparse
import datetime as _dt
import getpass
import os
import re
import socket
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path


DEFAULT_NODES = [
    "local",
    "neel@10.129.148.248",
    "neel@10.129.148.215",
    "neel@10.129.27.54",
    "neel@10.129.148.236",
    "neel@10.129.148.179",
]


@dataclass(frozen=True)
class ProbeResult:
    node: str
    reachable: bool
    error: str | None
    facts: dict[str, str]
    df_selected: str
    lsblk: list[dict[str, str]]


def _run(cmd: list[str], *, input_text: str | None = None, timeout_s: int = 15) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        cmd,
        input=input_text,
        text=True,
        capture_output=True,
        timeout=timeout_s,
        check=False,
    )


def _ssh_base_args(ssh_port: int, ssh_key: str | None, connect_timeout_s: int) -> list[str]:
    args = [
        "ssh",
        "-o",
        "BatchMode=yes",
        "-o",
        "StrictHostKeyChecking=no",
        "-o",
        f"ConnectTimeout={connect_timeout_s}",
        "-p",
        str(ssh_port),
    ]
    if ssh_key:
        args += ["-i", ssh_key]
    return args


def _is_local_node(node: str) -> bool:
    node = node.strip()
    if node in {"local", "localhost", "127.0.0.1"}:
        return True

    # Also treat current host's names as local.
    try:
        fqdn = socket.getfqdn()
        short = socket.gethostname().split(".", 1)[0]
    except Exception:
        return False

    try:
        username = getpass.getuser()
    except Exception:
        username = os.environ.get("USER", "")

    return node in {fqdn, short, f"{username}@{fqdn}", f"{username}@{short}"}


def _parse_probe_output(text: str) -> tuple[dict[str, str], str, list[dict[str, str]]]:
    facts: dict[str, str] = {}
    df_lines: list[str] = []
    lsblk_lines: list[str] = []

    mode: str | None = None
    for raw_line in text.splitlines():
        line = raw_line.rstrip("\n")
        if line == "df_selected_begin":
            mode = "df"
            continue
        if line == "df_selected_end":
            mode = None
            continue
        if line == "lsblk_begin":
            mode = "lsblk"
            continue
        if line == "lsblk_end":
            mode = None
            continue

        if mode == "df":
            df_lines.append(line)
            continue
        if mode == "lsblk":
            lsblk_lines.append(line)
            continue

        if "=" in line:
            k, v = line.split("=", 1)
            facts[k.strip()] = v.strip()

    lsblk: list[dict[str, str]] = []
    kv_re = re.compile(r"(\w+)=\"([^\"]*)\"")
    for row in lsblk_lines:
        if not row.strip():
            continue
        d: dict[str, str] = {}
        for k, v in kv_re.findall(row):
            d[k] = v
        if d:
            lsblk.append(d)

    df_selected = "\n".join(df_lines).strip()
    return facts, df_selected, lsblk


def _human_gb_from_kb(kb_str: str) -> str:
    try:
        kb = int(kb_str)
        gb = kb / 1024 / 1024
        return f"{gb:.1f} GB"
    except Exception:
        return "?"


def _summarize_disks(lsblk: list[dict[str, str]]) -> str:
    # Keep only top-level disks / md devices.
    disks = [d for d in lsblk if d.get("TYPE") in {"disk", "md"}]
    if not disks:
        return "(none detected)"

    parts: list[str] = []
    for d in disks:
        name = d.get("NAME", "?")
        model = d.get("MODEL", "").strip() or "(no model)"
        size = d.get("SIZE", "?")
        parts.append(f"{name} {model} {size}")
    return "; ".join(parts)


def _parse_root_df(root_df: str) -> dict[str, str]:
    # Expected: Filesystem Type Size Used Avail Use% Mounted
    fields = root_df.split()
    if len(fields) < 6:
        return {}
    # Some df variants have a trailing "on" token (Mounted on). Prefer last field as mount.
    return {
        "root_filesystem": fields[0],
        "root_type": fields[1] if len(fields) >= 2 else "",
        "root_size": fields[2] if len(fields) >= 3 else "",
        "root_used": fields[3] if len(fields) >= 4 else "",
        "root_avail": fields[4] if len(fields) >= 5 else "",
        "root_usepct": fields[5] if len(fields) >= 6 else "",
        "root_mount": fields[-1],
    }


def _local_probe_script() -> str:
    # POSIX sh (avoid bashisms so we can run it remotely too).
    return """set -eu

host_name=$(hostname -s 2>/dev/null || hostname)
fqdn=$(hostname -f 2>/dev/null || hostname)
os=$( ( . /etc/os-release 2>/dev/null; printf '%s' "${PRETTY_NAME:-}" ) 2>/dev/null || true)
if [ -z "$os" ]; then os=$(lsb_release -ds 2>/dev/null || true); fi
kernel=$(uname -r 2>/dev/null || true)
arch=$(uname -m 2>/dev/null || true)

cpu_model=$(awk -F: '/model name/ {gsub(/^ +/,"",$2); print $2; exit}' /proc/cpuinfo 2>/dev/null || true)
if [ -z "$cpu_model" ]; then cpu_model=$(lscpu 2>/dev/null | awk -F: '/Model name/ {gsub(/^ +/,"",$2); print $2; exit}' || true); fi

cpu_threads=$(nproc 2>/dev/null || true)
mem_total_kb=$(awk '/MemTotal/ {print $2; exit}' /proc/meminfo 2>/dev/null || true)
swap_total_kb=$(awk '/SwapTotal/ {print $2; exit}' /proc/meminfo 2>/dev/null || true)
ip4=$(hostname -I 2>/dev/null | tr -s ' ' | sed 's/ $//' || true)
root_df=$(df -hT / 2>/dev/null | awk 'NR==2{print $0}' || true)

printf 'host_name=%s\n' "$host_name"
printf 'fqdn=%s\n' "$fqdn"
printf 'os=%s\n' "$os"
printf 'kernel=%s\n' "$kernel"
printf 'arch=%s\n' "$arch"
printf 'cpu_model=%s\n' "$cpu_model"
printf 'cpu_threads=%s\n' "$cpu_threads"
printf 'mem_total_kb=%s\n' "$mem_total_kb"
printf 'swap_total_kb=%s\n' "$swap_total_kb"
printf 'ip4=%s\n' "$ip4"
printf 'root_df=%s\n' "$root_df"

echo df_selected_begin
(df -hT / /home /data /work /work/ARIABC 2>/dev/null || true)
echo df_selected_end

echo lsblk_begin
(lsblk -d -P -o NAME,MODEL,SIZE,ROTA,TYPE 2>/dev/null || true)
echo lsblk_end
"""


def probe_node(node: str, ssh_port: int, ssh_key: str | None, connect_timeout_s: int) -> ProbeResult:
    if _is_local_node(node):
        # Run locally via sh -s.
        cp = _run(["sh", "-s"], input_text=_local_probe_script(), timeout_s=connect_timeout_s + 10)
        if cp.returncode != 0:
            err = (cp.stderr.strip() or cp.stdout.strip() or f"local probe failed rc={cp.returncode}").splitlines()[0]
            return ProbeResult(node=node, reachable=True, error=err, facts={}, df_selected="", lsblk=[])

        facts, df_selected, lsblk = _parse_probe_output(cp.stdout)
        return ProbeResult(node=node, reachable=True, error=None, facts=facts, df_selected=df_selected, lsblk=lsblk)

    ssh_base = _ssh_base_args(ssh_port, ssh_key, connect_timeout_s)

    # Reachability probe (fast).
    cp_ok = _run(ssh_base + [node, "echo", "REACHABLE"], timeout_s=connect_timeout_s + 3)
    if cp_ok.returncode != 0 or "REACHABLE" not in cp_ok.stdout:
        err = (cp_ok.stderr.strip() or cp_ok.stdout.strip() or f"ssh failed rc={cp_ok.returncode}").splitlines()[0]
        return ProbeResult(node=node, reachable=False, error=err, facts={}, df_selected="", lsblk=[])

    # Full probe via stdin to avoid quoting issues.
    cp = _run(ssh_base + [node, "sh", "-s"], input_text=_local_probe_script(), timeout_s=connect_timeout_s + 20)
    if cp.returncode != 0:
        err = (cp.stderr.strip() or cp.stdout.strip() or f"remote probe failed rc={cp.returncode}").splitlines()[0]
        return ProbeResult(node=node, reachable=True, error=err, facts={}, df_selected="", lsblk=[])

    facts, df_selected, lsblk = _parse_probe_output(cp.stdout)
    return ProbeResult(node=node, reachable=True, error=None, facts=facts, df_selected=df_selected, lsblk=lsblk)


def _safe_filename(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", s)


def write_markdown_report(path: Path, results: list[ProbeResult], *, connect_timeout_s: int, ssh_port: int, ssh_key: str | None) -> None:
    now = _dt.datetime.now().astimezone()
    controller = socket.gethostname()

    reachable = [r for r in results if r.reachable]
    unreachable = [r for r in results if not r.reachable]

    lines: list[str] = []
    lines.append("# Live Machine Reachability + Inventory Report")
    lines.append("")
    lines.append(f"Generated on: {now.isoformat(timespec='seconds')}")
    lines.append(f"Controller: {controller}")
    lines.append(f"SSH criteria: BatchMode=yes, ConnectTimeout={connect_timeout_s}s, port={ssh_port}" + (f", key={ssh_key}" if ssh_key else ""))
    lines.append("")

    lines.append("## Reachability Summary")
    lines.append("")
    lines.append("| Node | SSH | Error |")
    lines.append("|---|---|---|")
    for r in results:
        ssh_status = "OK" if r.reachable else "FAIL"
        err = (r.error or "").replace("|", "\\|")
        lines.append(f"| {r.node} | {ssh_status} | {err} |")
    lines.append("")

    lines.append("## Inventory Summary (Reachable Only)")
    lines.append("")
    lines.append("| Host | Target | OS | Kernel | Threads | RAM | Swap | Root avail | Root use% | Disks |")
    lines.append("|---|---|---|---|---:|---:|---:|---:|---:|---|")
    for r in reachable:
        facts = r.facts
        host = facts.get("host_name", "?")
        os_name = facts.get("os", "?")
        kernel = facts.get("kernel", "?")
        threads = facts.get("cpu_threads", "?")
        ram = _human_gb_from_kb(facts.get("mem_total_kb", ""))
        swap = _human_gb_from_kb(facts.get("swap_total_kb", ""))
        root = _parse_root_df(facts.get("root_df", ""))
        root_avail = root.get("root_avail", "?")
        root_usepct = root.get("root_usepct", "?")
        disks = _summarize_disks(r.lsblk).replace("|", "\\|")
        lines.append(
            "| "
            + " | ".join(
                [
                    host.replace("|", "\\|"),
                    r.node.replace("|", "\\|"),
                    os_name.replace("|", "\\|"),
                    kernel.replace("|", "\\|"),
                    str(threads).replace("|", "\\|"),
                    ram,
                    swap,
                    root_avail,
                    root_usepct,
                    disks,
                ]
            )
            + " |"
        )
    if not reachable:
        lines.append("| (none) |  |  |  |  |  |  |  |  |  |")
    lines.append("")

    lines.append("## Per-Host Details (Reachable Only)")
    lines.append("")
    for r in reachable:
        facts = r.facts
        host = facts.get("host_name", "(unknown)")
        lines.append(f"### {host} ({r.node})")
        lines.append("")
        if r.error:
            lines.append(f"Warning: probe partial error: {r.error}")
            lines.append("")

        def kv(k: str, label: str | None = None) -> None:
            v = facts.get(k, "")
            if v:
                lines.append(f"- {(label or k)}: {v}")

        kv("fqdn", "fqdn")
        kv("os", "os")
        kv("kernel", "kernel")
        kv("arch", "arch")
        kv("cpu_model", "cpu_model")
        kv("cpu_threads", "cpu_threads")
        kv("mem_total_kb", "mem_total_kb")
        kv("swap_total_kb", "swap_total_kb")
        kv("ip4", "ip4")
        kv("root_df", "root_df")
        lines.append("")

        if r.df_selected:
            lines.append("df -hT (selected paths):")
            lines.append("```")
            lines.append(r.df_selected)
            lines.append("```")
            lines.append("")

        disks = [d for d in r.lsblk if d.get("TYPE") in {"disk", "md"}]
        if disks:
            lines.append("lsblk -d (disks/md):")
            lines.append("```")
            for d in disks:
                lines.append(
                    " ".join(
                        [
                            f"NAME={d.get('NAME','?')}",
                            f"MODEL={d.get('MODEL','')}",
                            f"SIZE={d.get('SIZE','?')}",
                            f"ROTA={d.get('ROTA','?')}",
                            f"TYPE={d.get('TYPE','?')}",
                        ]
                    ).rstrip()
                )
            lines.append("```")
            lines.append("")

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser(description="Probe SSH reachability + basic system inventory.")
    ap.add_argument(
        "--nodes",
        default=",".join(DEFAULT_NODES),
        help="Comma-separated list of nodes (e.g. local,neel@10.0.0.1,neel@10.0.0.2)",
    )
    ap.add_argument("--ssh-key", default="", help="Optional SSH private key path")
    ap.add_argument("--ssh-port", type=int, default=22)
    ap.add_argument("--connect-timeout", type=int, default=5)
    ap.add_argument("--out", default="", help="Output markdown path (default: scripts/distributed/LIVE_MACHINE_STATUS_<ts>.md)")

    args = ap.parse_args(argv)

    nodes = [n.strip() for n in args.nodes.split(",") if n.strip()]
    ssh_key = args.ssh_key.strip() or None

    results: list[ProbeResult] = []
    for node in nodes:
        try:
            results.append(probe_node(node, args.ssh_port, ssh_key, args.connect_timeout))
        except subprocess.TimeoutExpired:
            results.append(
                ProbeResult(
                    node=node,
                    reachable=False,
                    error=f"timeout after {args.connect_timeout}s",
                    facts={},
                    df_selected="",
                    lsblk=[],
                )
            )

    script_dir = Path(__file__).resolve().parent
    ts = _dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = Path(args.out) if args.out else (script_dir / f"LIVE_MACHINE_STATUS_{ts}.md")

    write_markdown_report(
        out_path,
        results,
        connect_timeout_s=args.connect_timeout,
        ssh_port=args.ssh_port,
        ssh_key=ssh_key,
    )

    ok = [r.node for r in results if r.reachable]
    fail = [r.node for r in results if not r.reachable]
    print(f"Wrote report: {out_path}")
    print(f"Reachable ({len(ok)}): {', '.join(ok) if ok else '(none)'}")
    print(f"Unreachable ({len(fail)}): {', '.join(fail) if fail else '(none)'}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
