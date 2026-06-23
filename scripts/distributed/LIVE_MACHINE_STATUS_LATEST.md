# Live Machine Reachability + Inventory Report

Generated on: 2026-06-19T22:56:18+05:30
Controller: neel-ASUS-TUF-Gaming-A15-FA507RE-FA577RE
SSH criteria: BatchMode=yes, ConnectTimeout=5s, port=22

## Reachability Summary

| Node | SSH | Error |
|---|---|---|
| neel@10.129.148.236 | OK |  |
| neel@10.129.27.54 | OK |  |
| neel@10.129.148.179 | OK |  |
| neel@10.129.148.248 | OK |  |

## Inventory Summary (Reachable Only)

| Host | Target | OS | Kernel | Threads | RAM | Swap | Root avail | Root use% | Disks |
|---|---|---|---|---:|---:|---:|---:|---:|---|
| Neel | neel@10.129.148.236 | Ubuntu 24.04.3 LTS | 6.17.0-22-generic | 16 | 15.0 GB | 4.0 GB | 424G | 5% | nvme0n1 INTEL SSDPEKNW512G8 476.9G |
| user4-MS-7C96 | neel@10.129.27.54 | Ubuntu 22.04.2 LTS | 6.8.0-101-generic | 16 | 15.0 GB | 46.6 GB | 111G | 37% | nvme0n1 INTEL SSDPEKNW512G8 476.9G |
| desk14-179 | neel@10.129.148.179 | Ubuntu 22.04.5 LTS | 6.8.0-111-generic | 16 | 15.0 GB | 14.9 GB | 300G | 31% | nvme0n1 INTEL SSDPEKNW512G8 476.9G |
| utkarsh-MS-7C96 | neel@10.129.148.248 | Ubuntu 24.04.3 LTS | 6.17.0-19-generic | 16 | 15.0 GB | 4.0 GB | 129G | 67% | sda Samsung SSD 840 EVO 500GB 465.8G; nvme0n1 INTEL SSDPEKNW512G8 476.9G |

## Per-Host Details (Reachable Only)

### Neel (neel@10.129.148.236)

- fqdn: Neel
- os: Ubuntu 24.04.3 LTS
- kernel: 6.17.0-22-generic
- arch: x86_64
- cpu_model: AMD Ryzen 7 5700G with Radeon Graphics
- cpu_threads: 16
- mem_total_kb: 15746684
- swap_total_kb: 4194300
- ip4: 10.129.148.236 172.17.0.1
- root_df: /dev/nvme0n1p2 ext4  468G   20G  424G   5% /

df -hT (selected paths):
```
Filesystem     Type  Size  Used Avail Use% Mounted on
/dev/nvme0n1p2 ext4  468G   20G  424G   5% /
/dev/nvme0n1p2 ext4  468G   20G  424G   5% /
```

lsblk -d (disks/md):
```
NAME=nvme0n1 MODEL=INTEL SSDPEKNW512G8 SIZE=476.9G ROTA=0 TYPE=disk
```

### user4-MS-7C96 (neel@10.129.27.54)

- fqdn: user4-MS-7C96
- os: Ubuntu 22.04.2 LTS
- kernel: 6.8.0-101-generic
- arch: x86_64
- cpu_model: AMD Ryzen 7 5700G with Radeon Graphics
- cpu_threads: 16
- mem_total_kb: 15742564
- swap_total_kb: 48828412
- ip4: 10.129.27.54 172.19.0.1 172.21.0.1 172.17.0.1 172.18.0.1
- root_df: /dev/nvme0n1p4 ext4  183G   63G  111G  37% /

df -hT (selected paths):
```
Filesystem     Type  Size  Used Avail Use% Mounted on
/dev/nvme0n1p4 ext4  183G   63G  111G  37% /
/dev/nvme0n1p5 ext4  238G  134G   92G  60% /home
```

lsblk -d (disks/md):
```
NAME=nvme0n1 MODEL=INTEL SSDPEKNW512G8 SIZE=476.9G ROTA=0 TYPE=disk
```

### desk14-179 (neel@10.129.148.179)

- fqdn: desk14-179
- os: Ubuntu 22.04.5 LTS
- kernel: 6.8.0-111-generic
- arch: x86_64
- cpu_model: AMD Ryzen 7 5700G with Radeon Graphics
- cpu_threads: 16
- mem_total_kb: 15742572
- swap_total_kb: 15625212
- ip4: 10.129.148.179
- root_df: /dev/nvme0n1p3 ext4  453G  131G  300G  31% /

df -hT (selected paths):
```
Filesystem     Type  Size  Used Avail Use% Mounted on
/dev/nvme0n1p3 ext4  453G  131G  300G  31% /
/dev/nvme0n1p3 ext4  453G  131G  300G  31% /
```

lsblk -d (disks/md):
```
NAME=nvme0n1 MODEL=INTEL SSDPEKNW512G8 SIZE=476.9G ROTA=0 TYPE=disk
```

### utkarsh-MS-7C96 (neel@10.129.148.248)

- fqdn: utkarsh-MS-7C96
- os: Ubuntu 24.04.3 LTS
- kernel: 6.17.0-19-generic
- arch: x86_64
- cpu_model: AMD Ryzen 7 5700G with Radeon Graphics
- cpu_threads: 16
- mem_total_kb: 15747072
- swap_total_kb: 4194300
- ip4: 10.129.148.248 172.17.0.1
- root_df: /dev/nvme0n1p2 ext4  404G  256G  129G  67% /

df -hT (selected paths):
```
Filesystem     Type  Size  Used Avail Use% Mounted on
/dev/nvme0n1p2 ext4  404G  256G  129G  67% /
/dev/nvme0n1p2 ext4  404G  256G  129G  67% /
/dev/sda1      ext4  458G  226G  209G  53% /data
```

lsblk -d (disks/md):
```
NAME=sda MODEL=Samsung SSD 840 EVO 500GB SIZE=465.8G ROTA=0 TYPE=disk
NAME=nvme0n1 MODEL=INTEL SSDPEKNW512G8 SIZE=476.9G ROTA=0 TYPE=disk
```

