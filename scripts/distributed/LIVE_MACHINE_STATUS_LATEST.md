# Live Machine Reachability + Inventory Report

Generated on: 2026-04-24T00:00:00+05:30
Controller: neel-ASUS-TUF-Gaming-A15-FA507RE-FA577RE
SSH criteria: BatchMode=yes, ConnectTimeout=5s, port=22

## Reachability Summary

| Node | SSH | Error |
|---|---|---|
| local | OK |  |
| neel@10.129.148.248 | OK |  |
| neel@10.129.27.54 | OK |  |
| neel@10.129.148.236 | OK |  |
| neel@10.129.148.179 | OK |  |
| neel@10.129.148.215 | FAIL | ssh: connect to host 10.129.148.215 port 22: Connection timed out |

## Inventory Summary (Reachable Only)

| Host | Target | OS | Kernel | Threads | RAM | Swap | Root avail | Root use% | Disks |
|---|---|---|---|---:|---:|---:|---:|---:|---|
| neel-ASUS-TUF-Gaming-A15-FA507RE-FA577RE | local | Ubuntu 24.04.4 LTS | 6.8.0-110-generic | 16 | 14.9 GB | 3.8 GB | 54G | 66% | nvme0n1 INTEL SSDPEKNU512GZ 476.9G |
| utkarsh-MS-7C96 | neel@10.129.148.248 | Ubuntu 24.04.3 LTS | 6.17.0-19-generic | 16 | 15.0 GB | 4.0 GB | 116G | 70% | nvme0n1 INTEL SSDPEKNW512G8 476.9G; sda Samsung SSD 840 EVO 500GB 465.8G |
| user4-MS-7C96 | neel@10.129.27.54 | Ubuntu 22.04.2 LTS | 6.8.0-101-generic | 16 | 15.0 GB | 46.6 GB | 109G | 38% | nvme0n1 INTEL SSDPEKNW512G8 476.9G |
| admin123-MS-7C96 | neel@10.129.148.236 | Ubuntu 24.04.3 LTS | 6.17.0-20-generic | 16 | 15.0 GB | 4.0 GB | 409G | 8% | nvme0n1 INTEL SSDPEKNW512G8 476.9G |
| new-node | neel@10.129.148.179 | Ubuntu 22.04.5 LTS | 6.8.0-107-generic | 16 | 15.0 GB | 14.9 GB | 308G | 29% | nvme0n1 INTEL SSDPEKNW512G8 476.9G |

## Per-Host Details (Reachable Only)

### neel-ASUS-TUF-Gaming-A15-FA507RE-FA577RE (local)

- fqdn: neel-ASUS-TUF-Gaming-A15-FA507RE-FA577RE
- os: Ubuntu 24.04.4 LTS
- kernel: 6.8.0-110-generic
- arch: x86_64
- cpu_model: AMD Ryzen 7 6800H with Radeon Graphics
- cpu_threads: 16
- mem_total_kb: 15591800
- swap_total_kb: 3999740
- ip4: 10.51.18.240
- root_df: /dev/nvme0n1p4 ext4  164G  102G   54G  66% /

df -hT (selected paths):
```
Filesystem     Type  Size  Used Avail Use% Mounted on
/dev/nvme0n1p4 ext4  164G  102G   54G  66% /
```

lsblk -d (disks/md):
```
NAME=nvme0n1 MODEL=INTEL SSDPEKNU512GZ SIZE=476.9G ROTA=0 TYPE=disk
```

### utkarsh-MS-7C96 (neel@10.129.148.248)

- fqdn: utkarsh-MS-7C96
- os: Ubuntu 24.04.3 LTS
- kernel: 6.17.0-19-generic
- arch: x86_64
- cpu_model: AMD Ryzen 7 5700G with Radeon Graphics
- cpu_threads: 16
- mem_total_kb: 15748268
- swap_total_kb: 4194300
- ip4: 10.129.148.248
- root_df: /dev/nvme0n1p2 ext4  384G  268G  116G  70% /

df -hT (selected paths):
```
Filesystem     Type  Size  Used Avail Use% Mounted on
/dev/nvme0n1p2 ext4  384G  268G  116G  70% /
/dev/sda1      ext4  459G  ...        /data
```

lsblk -d (disks/md):
```
NAME=nvme0n1 MODEL=INTEL SSDPEKNW512G8 SIZE=476.9G ROTA=0 TYPE=disk
NAME=sda MODEL=Samsung SSD 840 EVO 500GB SIZE=465.8G ROTA=0 TYPE=disk
```

### user4-MS-7C96 (neel@10.129.27.54)

- fqdn: user4-MS-7C96
- os: Ubuntu 22.04.2 LTS
- kernel: 6.8.0-101-generic
- arch: x86_64
- cpu_model: AMD Ryzen 7 5700G with Radeon Graphics
- cpu_threads: 16
- mem_total_kb: 15742568
- swap_total_kb: 48828412
- ip4: 10.129.27.54
- root_df: /dev/nvme0n1p4 ext4  183G   65G  109G  38% /

df -hT (selected paths):
```
Filesystem     Type  Size  Used Avail Use% Mounted on
/dev/nvme0n1p4 ext4  183G   65G  109G  38% /
```

lsblk -d (disks/md):
```
NAME=nvme0n1 MODEL=INTEL SSDPEKNW512G8 SIZE=476.9G ROTA=0 TYPE=disk
```

### admin123-MS-7C96 (neel@10.129.148.236)

- fqdn: admin123-MS-7C96
- os: Ubuntu 24.04.3 LTS
- kernel: 6.17.0-20-generic
- arch: x86_64
- cpu_model: AMD Ryzen 7 5700G with Radeon Graphics
- cpu_threads: 16
- mem_total_kb: 15748268
- swap_total_kb: 4194300
- ip4: 10.129.148.236
- root_df: /dev/nvme0n1p2 ext4  445G   36G  409G   8% /

df -hT (selected paths):
```
Filesystem     Type  Size  Used Avail Use% Mounted on
/dev/nvme0n1p2 ext4  445G   36G  409G   8% /
```

lsblk -d (disks/md):
```
NAME=nvme0n1 MODEL=INTEL SSDPEKNW512G8 SIZE=476.9G ROTA=0 TYPE=disk
```

### new-node (neel@10.129.148.179)

- fqdn: new-node
- os: Ubuntu 22.04.5 LTS
- kernel: 6.8.0-107-generic
- arch: x86_64
- cpu_model: AMD Ryzen 7 5700G with Radeon Graphics
- cpu_threads: 16
- mem_total_kb: 15742704
- swap_total_kb: 15625212
- ip4: 10.129.148.179
- root_df: /dev/nvme0n1 ext4  431G  123G  308G  29% /

df -hT (selected paths):
```
Filesystem  Type  Size  Used Avail Use% Mounted on
/dev/nvme0n1 ext4  431G  123G  308G  29% /
```

lsblk -d (disks/md):
```
NAME=nvme0n1 MODEL=INTEL SSDPEKNW512G8 SIZE=476.9G ROTA=0 TYPE=disk
```

### manish-MS-7C96 (neel@10.129.148.215) — EXCLUDED

- Status: SSH unreachable since 2026-04-09; not in benchmark script.
- Last known: Ubuntu 22.04.5 LTS, Ryzen 7 5700G, 15.0 GB RAM, 159G used / 276G free
