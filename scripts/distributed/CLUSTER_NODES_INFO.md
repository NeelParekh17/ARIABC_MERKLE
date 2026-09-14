# 🖥️ AriaBC 4-Node Cluster Inventory & Live Status

> **Last Updated:** `2026-09-14 17:39:46 +0530`
> **Automatic Update:** Yes (Updated via `scripts/distributed/update_nodes_info.py`)
> **Service Sockets Checked:** BCDB PostgreSQL (Port `5438`), AriaBC Server (Port `8000`/`8001`)

## 📊 Cluster Summary Table

| Node | Name | IP Address | Status | OS | CPU | RAM (Total/Avail) | Root Storage (Avail/Use%) | Disk Read MB/s | Disk Write MB/s | Disk Util % | Await | PostgreSQL | AriaBC Server |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| **Node 1** | admin123 | `10.129.148.247` | 🟢 Online | Ubuntu 24.04.3 LTS | 16 Cores | 15.01 GB / 6.65 GB (55.7% used) | 312G / 468G (30%) | 0.00 | 0.00 | 0.00% | 0.00 ms | 🟢 Running | 🔴 Stopped |
| **Node 2** | user4 | `10.129.148.246` | 🟢 Online | Ubuntu 22.04.2 LTS | 16 Cores | 15.01 GB / 1.43 GB (90.5% used) | 48G / 183G (73%) | 0.00 | 0.00 | 0.00% | 0.00 ms | 🟢 Running | 🔴 Stopped |
| **Node 4** | utkarsh | `10.129.148.248` | 🟢 Online | Ubuntu 24.04.3 LTS | 16 Cores | 15.02 GB / 10.22 GB (32.0% used) | 88G / 404G (78%) | 0.00 | 0.00 | 0.00% | 0.00 ms | 🔴 Stopped | 🔴 Stopped |
| **ASUS Laptop (GW)** | asus-laptop | `127.0.0.1` | 🟢 Online | Ubuntu 24.04.4 LTS | 16 Cores | 14.87 GB / 5.44 GB (63.4% used) | 14G / 164G (92%) | 0.00 | 0.00 | 0.50% | 5.00 ms | — | — |
| **Gateway 2 (Proposed)** | proposed-gw | `10.129.27.111` | 🟢 Online | Ubuntu 24.04.2 LTS | 16 Cores | 15.02 GB / 0.98 GB (93.4% used) | 186G / 457G (58%) | 0.00 | 0.07 | 2.20% | 1.64 ms | — | — |
| **Node 7** | ranking-epyc | `ranking.cse.iitb.ac.in` | 🟢 Online | Ubuntu 24.04.3 LTS | 192 Cores | 251.31 GB / 209.18 GB (16.8% used) | 1.3T / 1.8T (28%) | 0.00 | 0.13 | 0.50% | 1.20 ms | 🔴 Stopped | 🔴 Stopped |

## 🌐 Network Latency Matrix (RTT)

| From | To | RTT |
|---|---|---|
| admin123 (Node 1) | user4 (Node 2) | 0.303 ms |
| admin123 (Node 1) | utkarsh (Node 4) | 0.304 ms |
| admin123 (Node 1) | asus-laptop (Node 5) | 0.042 ms |
| admin123 (Node 1) | proposed-gw (Node 6) | 0.530 ms |
| admin123 (Node 1) | ranking-epyc (Node 7) | 0.250 ms |
| user4 (Node 2) | admin123 (Node 1) | 0.335 ms |
| user4 (Node 2) | utkarsh (Node 4) | 0.259 ms |
| user4 (Node 2) | asus-laptop (Node 5) | 0.041 ms |
| user4 (Node 2) | proposed-gw (Node 6) | 0.475 ms |
| user4 (Node 2) | ranking-epyc (Node 7) | 0.208 ms |
| utkarsh (Node 4) | admin123 (Node 1) | 0.243 ms |
| utkarsh (Node 4) | user4 (Node 2) | 0.180 ms |
| utkarsh (Node 4) | asus-laptop (Node 5) | 0.027 ms |
| utkarsh (Node 4) | proposed-gw (Node 6) | 0.279 ms |
| utkarsh (Node 4) | ranking-epyc (Node 7) | 0.242 ms |
| asus-laptop (Node 5) | admin123 (Node 1) | 7.584 ms |
| asus-laptop (Node 5) | user4 (Node 2) | 2.037 ms |
| asus-laptop (Node 5) | utkarsh (Node 4) | 1.505 ms |
| asus-laptop (Node 5) | proposed-gw (Node 6) | 4.373 ms |
| asus-laptop (Node 5) | ranking-epyc (Node 7) | 1.374 ms |
| proposed-gw (Node 6) | admin123 (Node 1) | 0.553 ms |
| proposed-gw (Node 6) | user4 (Node 2) | 0.601 ms |
| proposed-gw (Node 6) | utkarsh (Node 4) | 0.275 ms |
| proposed-gw (Node 6) | asus-laptop (Node 5) | 0.032 ms |
| proposed-gw (Node 6) | ranking-epyc (Node 7) | 0.302 ms |
| ranking-epyc (Node 7) | admin123 (Node 1) | 0.468 ms |
| ranking-epyc (Node 7) | user4 (Node 2) | 0.378 ms |
| ranking-epyc (Node 7) | utkarsh (Node 4) | 0.199 ms |
| ranking-epyc (Node 7) | asus-laptop (Node 5) | 0.053 ms |
| ranking-epyc (Node 7) | proposed-gw (Node 6) | 0.332 ms |

## 🔍 Detailed Node Inventory

### 🖥️ Node 1: admin123 (`10.129.148.247`)

#### ⚙️ System Specifications
- **Host/FQDN:** `Neel`
- **Operating System:** Ubuntu 24.04.3 LTS
- **Kernel Version:** `7.0.0-28-generic`
- **CPU Model:** `AMD Ryzen 7 5700G with Radeon Graphics`
- **CPU Logical Cores (Threads):** `16`
- **IPv4 Addresses:** `10.129.148.247 172.17.0.1`

#### 🧠 Memory (RAM) Allocation
- **Total RAM:** 15.01 GB
- **Available RAM:** 6.65 GB
- **Used RAM:** 8.36 GB (55.7%)
- **Swap Space:** 0.81 GB used of 4.00 GB total

#### 💾 Storage & Disks
- **Root Mount Filesystem:** `/dev/nvme0n1p2` (ext4)
- **Root Disk Space:** 132G used / 312G free (Total: 468G, Use%: 30%)
- **Disk I/O Read Speed:** `0.00 MB/s` (Device: `nvme0n1`)
- **Disk I/O Write Speed:** `0.00 MB/s`
- **Disk Utilization:** `0.00%`
- **Average Disk Await Time:** `0.00 ms`
- **Physical Disks / RAID Groups:**
- **nvme0n1**: INTEL SSDPEKNW512G8 (476.9G)

##### Selected `df -hT` output:
```text
Filesystem     Type      Size  Used Avail Use% Mounted on
tmpfs          tmpfs     1.6G  3.2M  1.5G   1% /run
/dev/nvme0n1p2 ext4      468G  132G  312G  30% /
tmpfs          tmpfs     7.6G  112K  7.6G   1% /dev/shm
tmpfs          tmpfs     5.0M   12K  5.0M   1% /run/lock
efivarfs       efivarfs  128K   43K   81K  35% /sys/firmware/efi/efivars
/dev/nvme0n1p1 vfat      1.1G  6.2M  1.1G   1% /boot/efi
tmpfs          tmpfs     1.6G  132K  1.6G   1% /run/user/1003
```

#### 🔌 Port & Service Sockets Status
- **PostgreSQL DB Server (Port `5438`):** 🟢 Running (Accepting connections)
- **AriaBC Raft Client Server (Port `8000`):** 🔴 Stopped

### 🖥️ Node 2: user4 (`10.129.148.246`)

#### ⚙️ System Specifications
- **Host/FQDN:** `user4-MS-7C96`
- **Operating System:** Ubuntu 22.04.2 LTS
- **Kernel Version:** `6.8.0-124-generic`
- **CPU Model:** `AMD Ryzen 7 5700G with Radeon Graphics`
- **CPU Logical Cores (Threads):** `16`
- **IPv4 Addresses:** `10.129.148.246 172.17.0.1 172.18.0.1 172.19.0.1 172.21.0.1 172.20.0.1`

#### 🧠 Memory (RAM) Allocation
- **Total RAM:** 15.01 GB
- **Available RAM:** 1.43 GB
- **Used RAM:** 13.59 GB (90.5%)
- **Swap Space:** 0.74 GB used of 46.57 GB total

#### 💾 Storage & Disks
- **Root Mount Filesystem:** `/dev/nvme0n1p4` (ext4)
- **Root Disk Space:** 126G used / 48G free (Total: 183G, Use%: 73%)
- **Disk I/O Read Speed:** `0.00 MB/s` (Device: `nvme0n1`)
- **Disk I/O Write Speed:** `0.00 MB/s`
- **Disk Utilization:** `0.00%`
- **Average Disk Await Time:** `0.00 ms`
- **Physical Disks / RAID Groups:**
- **nvme0n1**: INTEL SSDPEKNW512G8 (476.9G)

##### Selected `df -hT` output:
```text
Filesystem     Type      Size  Used Avail Use% Mounted on
tmpfs          tmpfs     1.6G  2.7M  1.5G   1% /run
/dev/nvme0n1p4 ext4      183G  126G   48G  73% /
tmpfs          tmpfs     7.6G   64K  7.6G   1% /dev/shm
tmpfs          tmpfs     5.0M  4.0K  5.0M   1% /run/lock
efivarfs       efivarfs  128K   33K   91K  27% /sys/firmware/efi/efivars
tmpfs          tmpfs     7.6G     0  7.6G   0% /run/qemu
/dev/nvme0n1p2 ext4      921M  304M  554M  36% /boot
/dev/nvme0n1p1 vfat      952M  6.1M  946M   1% /boot/efi
/dev/nvme0n1p5 ext4      238G  187G   39G  83% /home
tmpfs          tmpfs     1.6G   76K  1.6G   1% /run/user/127
tmpfs          tmpfs     1.6G   60K  1.6G   1% /run/user/1004
```

#### 🔌 Port & Service Sockets Status
- **PostgreSQL DB Server (Port `5438`):** 🟢 Running (Accepting connections)
- **AriaBC Raft Client Server (Port `8000`):** 🔴 Stopped

### 🖥️ Node 4: utkarsh (`10.129.148.248`)

#### ⚙️ System Specifications
- **Host/FQDN:** `utkarsh-MS-7C96`
- **Operating System:** Ubuntu 24.04.3 LTS
- **Kernel Version:** `6.17.0-19-generic`
- **CPU Model:** `AMD Ryzen 7 5700G with Radeon Graphics`
- **CPU Logical Cores (Threads):** `16`
- **IPv4 Addresses:** `10.129.148.248 172.17.0.1`

#### 🧠 Memory (RAM) Allocation
- **Total RAM:** 15.02 GB
- **Available RAM:** 10.22 GB
- **Used RAM:** 4.80 GB (32.0%)
- **Swap Space:** 0.00 GB used of 4.00 GB total

#### 💾 Storage & Disks
- **Root Mount Filesystem:** `/dev/nvme0n1p2` (ext4)
- **Root Disk Space:** 297G used / 88G free (Total: 404G, Use%: 78%)
- **Disk I/O Read Speed:** `0.00 MB/s` (Device: `nvme0n1`)
- **Disk I/O Write Speed:** `0.00 MB/s`
- **Disk Utilization:** `0.00%`
- **Average Disk Await Time:** `0.00 ms`
- **Physical Disks / RAID Groups:**
- **sda**: Samsung SSD 840 EVO 500GB (465.8G)
- **nvme0n1**: INTEL SSDPEKNW512G8 (476.9G)

##### Selected `df -hT` output:
```text
Filesystem     Type      Size  Used Avail Use% Mounted on
tmpfs          tmpfs     1.6G  2.6M  1.5G   1% /run
/dev/nvme0n1p2 ext4      404G  297G   88G  78% /
tmpfs          tmpfs     7.6G   55M  7.5G   1% /dev/shm
tmpfs          tmpfs     5.0M     0  5.0M   0% /run/lock
efivarfs       efivarfs  128K   42K   82K  34% /sys/firmware/efi/efivars
tmpfs          tmpfs     7.6G     0  7.6G   0% /run/qemu
/dev/sda1      ext4      458G  332G  103G  77% /data
/dev/nvme0n1p1 vfat      1.1G  6.2M  1.1G   1% /boot/efi
tmpfs          tmpfs     1.6G  144K  1.6G   1% /run/user/1000
tmpfs          tmpfs     1.6G   92K  1.6G   1% /run/user/1003
```

#### 🔌 Port & Service Sockets Status
- **PostgreSQL DB Server (Port `5438`):** 🔴 Stopped
- **AriaBC Raft Client Server (Port `8001`):** 🔴 Stopped

### 🖥️ Node 5: asus-laptop (`127.0.0.1`)

#### ⚙️ System Specifications
- **Host/FQDN:** `neel-ASUS-TUF-Gaming-A15-FA507RE-FA577RE`
- **Operating System:** Ubuntu 24.04.4 LTS
- **Kernel Version:** `6.8.0-139-generic`
- **CPU Model:** `AMD Ryzen 7 6800H with Radeon Graphics`
- **CPU Logical Cores (Threads):** `16`
- **IPv4 Addresses:** `192.168.0.154 100.114.239.70 172.19.0.1 172.17.0.1 172.18.0.1 fd7a:115c:a1e0::d01:efa8`

#### 🧠 Memory (RAM) Allocation
- **Total RAM:** 14.87 GB
- **Available RAM:** 5.44 GB
- **Used RAM:** 9.43 GB (63.4%)
- **Swap Space:** 2.66 GB used of 15.81 GB total

#### 💾 Storage & Disks
- **Root Mount Filesystem:** `/dev/nvme0n1p5` (ext4)
- **Root Disk Space:** 143G used / 14G free (Total: 164G, Use%: 92%)
- **Disk I/O Read Speed:** `0.00 MB/s` (Device: `nvme0n1`)
- **Disk I/O Write Speed:** `0.00 MB/s`
- **Disk Utilization:** `0.50%`
- **Average Disk Await Time:** `5.00 ms`
- **Physical Disks / RAID Groups:**
- **nvme0n1**: INTEL SSDPEKNU512GZ (476.9G)

##### Selected `df -hT` output:
```text
Filesystem     Type      Size  Used Avail Use% Mounted on
tmpfs          tmpfs     1.5G  3.1M  1.5G   1% /run
/dev/nvme0n1p5 ext4      164G  143G   14G  92% /
tmpfs          tmpfs     7.5G   88M  7.4G   2% /dev/shm
tmpfs          tmpfs     5.0M   12K  5.0M   1% /run/lock
efivarfs       efivarfs  128K   71K   53K  58% /sys/firmware/efi/efivars
tmpfs          tmpfs     7.5G     0  7.5G   0% /run/qemu
/dev/nvme0n1p7 ext4       49G   45G  1.5G  97% /home
/dev/nvme0n1p1 vfat      256M   39M  218M  15% /boot/efi
tmpfs          tmpfs     1.5G  2.6M  1.5G   1% /run/user/1000
```

#### 🔌 Port & Service Sockets Status
- **Role:** Gateway Machine (No local PostgreSQL database or AriaBC Server running)

### 🖥️ Node 6: proposed-gw (`10.129.27.111`)

#### ⚙️ System Specifications
- **Host/FQDN:** `myubuntu`
- **Operating System:** Ubuntu 24.04.2 LTS
- **Kernel Version:** `6.17.0-35-generic`
- **CPU Model:** `AMD Ryzen 7 5700G with Radeon Graphics`
- **CPU Logical Cores (Threads):** `16`
- **IPv4 Addresses:** `10.129.27.111 10.244.1.0 10.244.1.1`

#### 🧠 Memory (RAM) Allocation
- **Total RAM:** 15.02 GB
- **Available RAM:** 0.98 GB
- **Used RAM:** 14.03 GB (93.4%)
- **Swap Space:** Disabled / None

#### 💾 Storage & Disks
- **Root Mount Filesystem:** `/dev/nvme0n1p2` (ext4)
- **Root Disk Space:** 248G used / 186G free (Total: 457G, Use%: 58%)
- **Disk I/O Read Speed:** `0.00 MB/s` (Device: `nvme0n1`)
- **Disk I/O Write Speed:** `0.07 MB/s`
- **Disk Utilization:** `2.20%`
- **Average Disk Await Time:** `1.64 ms`
- **Physical Disks / RAID Groups:**
- **nvme0n1**: CT500P2SSD8 (465.8G)

##### Selected `df -hT` output:
```text
Filesystem     Type      Size  Used Avail Use% Mounted on
tmpfs          tmpfs     1.6G  4.1M  1.5G   1% /run
/dev/nvme0n1p2 ext4      457G  248G  186G  58% /
tmpfs          tmpfs     7.6G  173M  7.4G   3% /dev/shm
tmpfs          tmpfs     5.0M   12K  5.0M   1% /run/lock
efivarfs       efivarfs  128K   42K   82K  34% /sys/firmware/efi/efivars
/dev/nvme0n1p1 vfat      1.1G  6.2M  1.1G   1% /boot/efi
tmpfs          tmpfs     1.6G  144K  1.6G   1% /run/user/1006
tmpfs          tmpfs     1.6G  144K  1.6G   1% /run/user/1005
tmpfs          tmpfs     1.6G  184K  1.6G   1% /run/user/1007
tmpfs          tmpfs     1.6G   96K  1.6G   1% /run/user/1002
```

#### 🔌 Port & Service Sockets Status
- **Role:** Gateway Machine (No local PostgreSQL database or AriaBC Server running)

### 🖥️ Node 7: ranking-epyc (`ranking.cse.iitb.ac.in`)

#### ⚙️ System Specifications
- **Host/FQDN:** `user-MZ73-LM0-000`
- **Operating System:** Ubuntu 24.04.3 LTS
- **Kernel Version:** `7.0.0-29-generic`
- **CPU Model:** `AMD EPYC 9654 96-Core Processor`
- **CPU Logical Cores (Threads):** `192`
- **IPv4 Addresses:** `10.129.7.57 10.0.3.1`

#### 🧠 Memory (RAM) Allocation
- **Total RAM:** 251.31 GB
- **Available RAM:** 209.18 GB
- **Used RAM:** 42.13 GB (16.8%)
- **Swap Space:** Disabled / None

#### 💾 Storage & Disks
- **Root Mount Filesystem:** `/dev/nvme0n1p3` (ext4)
- **Root Disk Space:** 489G used / 1.3T free (Total: 1.8T, Use%: 28%)
- **Disk I/O Read Speed:** `0.00 MB/s` (Device: `nvme0n1`)
- **Disk I/O Write Speed:** `0.13 MB/s`
- **Disk Utilization:** `0.50%`
- **Average Disk Await Time:** `1.20 ms`
- **Physical Disks / RAID Groups:**
- **nvme0n1**: CT2000P3PSSD8 (1.8T)
- **nvme3n1**: Samsung SSD 990 EVO Plus 2TB (1.8T)
- **nvme2n1**: Samsung SSD 990 EVO Plus 2TB (1.8T)
- **nvme1n1**: Samsung SSD 990 EVO Plus 2TB (1.8T)
- **nvme4n1**: Samsung SSD 990 EVO Plus 2TB (1.8T)

##### Selected `df -hT` output:
```text
Filesystem     Type      Size  Used Avail Use% Mounted on
tmpfs          tmpfs      26G  3.6M   26G   1% /run
efivarfs       efivarfs  128K   28K   96K  23% /sys/firmware/efi/efivars
/dev/nvme0n1p3 ext4      1.8T  489G  1.3T  28% /
tmpfs          tmpfs     126G  3.0M  126G   1% /dev/shm
tmpfs          tmpfs     5.0M     0  5.0M   0% /run/lock
tmpfs          tmpfs     126G     0  126G   0% /run/qemu
/dev/nvme0n1p1 ext4      442M  215M  193M  53% /boot
/dev/nvme0n1p2 vfat      1.1G  6.2M  1.1G   1% /boot/efi
/dev/md0       ext4      5.5T  2.9T  2.3T  56% /backup
tmpfs          tmpfs      26G   92K   26G   1% /run/user/120
tmpfs          tmpfs      26G   96K   26G   1% /run/user/1005
tmpfs          tmpfs      26G   88K   26G   1% /run/user/1006
tmpfs          tmpfs      26G   80K   26G   1% /run/user/1001
tmpfs          tmpfs      26G   84K   26G   1% /run/user/1002
tmpfs          tmpfs      26G   84K   26G   1% /run/user/1004
tmpfs          tmpfs      26G   80K   26G   1% /run/user/1007
```

#### 🔌 Port & Service Sockets Status
- **PostgreSQL DB Server (Port `5438`):** 🔴 Stopped
- **AriaBC Raft Client Server (Port `8000`):** 🔴 Stopped

