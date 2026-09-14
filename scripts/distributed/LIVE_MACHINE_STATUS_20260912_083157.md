# Live Machine Reachability + Inventory Report

Generated on: 2026-09-12T08:31:57+05:30
Controller: neel-ASUS-TUF-Gaming-A15-FA507RE-FA577RE
SSH criteria: BatchMode=yes, ConnectTimeout=5s, port=22

## Reachability Summary

| Node | SSH | Error |
|---|---|---|
| local | OK |  |
| neel@10.129.148.248 | OK |  |
| neel@10.129.148.215 | FAIL | @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ |
| neel@10.129.148.246 | OK |  |
| neel@10.129.148.247 | OK |  |

## Inventory Summary (Reachable Only)

| Host | Target | OS | Kernel | Threads | RAM | Swap | Root avail | Root use% | Disks |
|---|---|---|---|---:|---:|---:|---:|---:|---|
| neel-ASUS-TUF-Gaming-A15-FA507RE-FA577RE | local | Ubuntu 24.04.4 LTS | 6.8.0-139-generic | 16 | 14.9 GB | 15.8 GB | 15G | 91% | nvme0n1 INTEL SSDPEKNU512GZ 476.9G |
| utkarsh-MS-7C96 | neel@10.129.148.248 | Ubuntu 24.04.3 LTS | 6.17.0-19-generic | 16 | 15.0 GB | 4.0 GB | 98G | 75% | sda Samsung SSD 840 EVO 500GB 465.8G; nvme0n1 INTEL SSDPEKNW512G8 476.9G |
| user4-MS-7C96 | neel@10.129.148.246 | Ubuntu 22.04.2 LTS | 6.8.0-124-generic | 16 | 15.0 GB | 46.6 GB | 48G | 73% | nvme0n1 INTEL SSDPEKNW512G8 476.9G |
| Neel | neel@10.129.148.247 | Ubuntu 24.04.3 LTS | 7.0.0-28-generic | 16 | 15.0 GB | 4.0 GB | 281G | 37% | nvme0n1 INTEL SSDPEKNW512G8 476.9G |

## Per-Host Details (Reachable Only)

### neel-ASUS-TUF-Gaming-A15-FA507RE-FA577RE (local)

- fqdn: neel-ASUS-TUF-Gaming-A15-FA507RE-FA577RE
- os: Ubuntu 24.04.4 LTS
- kernel: 6.8.0-139-generic
- arch: x86_64
- cpu_model: AMD Ryzen 7 6800H with Radeon Graphics
- cpu_threads: 16
- mem_total_kb: 15591728
- swap_total_kb: 16582648
- ip4: 192.168.0.154 100.114.239.70 172.18.0.1 172.19.0.1 172.17.0.1 fd7a:115c:a1e0::d01:efa8
- root_df: /dev/nvme0n1p5 ext4  164G  141G   15G  91% /

df -hT (selected paths):
```
Filesystem     Type  Size  Used Avail Use% Mounted on
/dev/nvme0n1p5 ext4  164G  141G   15G  91% /
/dev/nvme0n1p7 ext4   49G   45G  1.5G  97% /home
/dev/nvme0n1p5 ext4  164G  141G   15G  91% /
/dev/nvme0n1p5 ext4  164G  141G   15G  91% /
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
- mem_total_kb: 15746668
- swap_total_kb: 4194300
- ip4: 10.129.148.248 172.17.0.1
- root_df: /dev/nvme0n1p2 ext4  404G  287G   98G  75% /

df -hT (selected paths):
```
Filesystem     Type  Size  Used Avail Use% Mounted on
/dev/nvme0n1p2 ext4  404G  287G   98G  75% /
/dev/nvme0n1p2 ext4  404G  287G   98G  75% /
/dev/sda1      ext4  458G  332G  103G  77% /data
```

lsblk -d (disks/md):
```
NAME=sda MODEL=Samsung SSD 840 EVO 500GB SIZE=465.8G ROTA=0 TYPE=disk
NAME=nvme0n1 MODEL=INTEL SSDPEKNW512G8 SIZE=476.9G ROTA=0 TYPE=disk
```

### user4-MS-7C96 (neel@10.129.148.246)

- fqdn: user4-MS-7C96
- os: Ubuntu 22.04.2 LTS
- kernel: 6.8.0-124-generic
- arch: x86_64
- cpu_model: AMD Ryzen 7 5700G with Radeon Graphics
- cpu_threads: 16
- mem_total_kb: 15742568
- swap_total_kb: 48828412
- ip4: 10.129.148.246 172.17.0.1 172.18.0.1 172.19.0.1 172.21.0.1 172.20.0.1
- root_df: /dev/nvme0n1p4 ext4  183G  126G   48G  73% /

df -hT (selected paths):
```
Filesystem     Type  Size  Used Avail Use% Mounted on
/dev/nvme0n1p4 ext4  183G  126G   48G  73% /
/dev/nvme0n1p5 ext4  238G  179G   47G  80% /home
```

lsblk -d (disks/md):
```
NAME=nvme0n1 MODEL=INTEL SSDPEKNW512G8 SIZE=476.9G ROTA=0 TYPE=disk
```

### Neel (neel@10.129.148.247)

- fqdn: Neel
- os: Ubuntu 24.04.3 LTS
- kernel: 7.0.0-28-generic
- arch: x86_64
- cpu_model: AMD Ryzen 7 5700G with Radeon Graphics
- cpu_threads: 16
- mem_total_kb: 15743380
- swap_total_kb: 4194300
- ip4: 10.129.148.247 172.17.0.1
- root_df: /dev/nvme0n1p2 ext4  468G  163G  281G  37% /

df -hT (selected paths):
```
Filesystem     Type  Size  Used Avail Use% Mounted on
/dev/nvme0n1p2 ext4  468G  163G  281G  37% /
/dev/nvme0n1p2 ext4  468G  163G  281G  37% /
```

lsblk -d (disks/md):
```
NAME=nvme0n1 MODEL=INTEL SSDPEKNW512G8 SIZE=476.9G ROTA=0 TYPE=disk
```

