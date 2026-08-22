# 🖥️ AriaBC 4-Node Cluster Inventory & Live Status

> **Last Updated:** `2026-08-17 20:00:14 +0530`
> **Automatic Update:** Yes (Updated via `scripts/distributed/update_nodes_info.py`)
> **Service Sockets Checked:** BCDB PostgreSQL (Port `5438`), AriaBC Server (Port `8000`/`8001`)

## 📊 Cluster Summary Table

| Node | Name | IP Address | Status | OS | CPU | RAM (Total/Avail) | Root Storage (Avail/Use%) | Disk Read MB/s | Disk Write MB/s | Disk Util % | Await | PostgreSQL | AriaBC Server |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| **Node 1** | admin123 | `10.129.148.247` | 🟢 Online | Ubuntu 24.04.3 LTS | 16 Cores | 15.01 GB / 8.10 GB (46.0% used) | 251G / 468G (44%) | 0.00 | 0.00 | 0.00% | 0.00 ms | 🟢 Running | 🔴 Stopped |
| **Node 2** | user4 | `10.129.148.246` | 🟢 Online | Ubuntu 22.04.2 LTS | 16 Cores | 15.01 GB / 2.26 GB (84.9% used) | 106G / 183G (39%) | 0.00 | 0.00 | 0.00% | 0.00 ms | 🟢 Running | 🔴 Stopped |
| **Node 4** | utkarsh | `10.129.148.248` | 🟢 Online | Ubuntu 24.04.3 LTS | 16 Cores | 15.02 GB / 3.44 GB (77.1% used) | 112G / 404G (72%) | 0.00 | 0.00 | 0.00% | 0.00 ms | 🟢 Running | 🔴 Stopped |
| **ASUS Laptop (GW)** | asus-laptop | `127.0.0.1` | 🟢 Online | Ubuntu 24.04.4 LTS | 16 Cores | 14.87 GB / 7.19 GB (51.6% used) | 71G / 164G (55%) | 0.00 | 0.00 | 0.00% | 0.00 ms | — | — |
| **Gateway 2 (Proposed)** | proposed-gw | `10.129.27.111` | 🟢 Online | Ubuntu 24.04.2 LTS | 16 Cores | 15.02 GB / 11.06 GB (26.3% used) | 205G / 457G (53%) | 0.00 | 0.00 | 0.00% | 0.00 ms | — | — |
| **Node 7** | ranking-epyc | `ranking.cse.iitb.ac.in` | 🟢 Online | Ubuntu 24.04.3 LTS | 192 Cores | 251.32 GB / 125.96 GB (49.9% used) | 1.3T / 1.8T (28%) | 0.00 | 15.59 | 1.00% | 0.18 ms | 🔴 Stopped | 🔴 Stopped |

## 🌐 Network Latency Matrix (RTT)

| From | To | RTT |
|---|---|---|
| admin123 (Node 1) | user4 (Node 2) | 0.501 ms |
| admin123 (Node 1) | utkarsh (Node 4) | 0.565 ms |
| admin123 (Node 1) | asus-laptop (Node 5) | 0.054 ms |
| admin123 (Node 1) | proposed-gw (Node 6) | 0.373 ms |
| admin123 (Node 1) | ranking-epyc (Node 7) | 0.322 ms |
| user4 (Node 2) | admin123 (Node 1) | 0.332 ms |
| user4 (Node 2) | utkarsh (Node 4) | 0.387 ms |
| user4 (Node 2) | asus-laptop (Node 5) | 0.032 ms |
| user4 (Node 2) | proposed-gw (Node 6) | 0.288 ms |
| user4 (Node 2) | ranking-epyc (Node 7) | 0.238 ms |
| utkarsh (Node 4) | admin123 (Node 1) | 0.593 ms |
| utkarsh (Node 4) | user4 (Node 2) | 0.256 ms |
| utkarsh (Node 4) | asus-laptop (Node 5) | 0.046 ms |
| utkarsh (Node 4) | proposed-gw (Node 6) | 0.718 ms |
| utkarsh (Node 4) | ranking-epyc (Node 7) | 0.265 ms |
| asus-laptop (Node 5) | admin123 (Node 1) | 8.458 ms |
| asus-laptop (Node 5) | user4 (Node 2) | 26.170 ms |
| asus-laptop (Node 5) | utkarsh (Node 4) | 9.837 ms |
| asus-laptop (Node 5) | proposed-gw (Node 6) | 67.978 ms |
| asus-laptop (Node 5) | ranking-epyc (Node 7) | 18.601 ms |
| proposed-gw (Node 6) | admin123 (Node 1) | 0.642 ms |
| proposed-gw (Node 6) | user4 (Node 2) | 0.354 ms |
| proposed-gw (Node 6) | utkarsh (Node 4) | 0.522 ms |
| proposed-gw (Node 6) | asus-laptop (Node 5) | 0.022 ms |
| proposed-gw (Node 6) | ranking-epyc (Node 7) | Timeout/Error |
| ranking-epyc (Node 7) | admin123 (Node 1) | 0.478 ms |
| ranking-epyc (Node 7) | user4 (Node 2) | 0.337 ms |
| ranking-epyc (Node 7) | utkarsh (Node 4) | 0.464 ms |
| ranking-epyc (Node 7) | asus-laptop (Node 5) | 0.049 ms |
| ranking-epyc (Node 7) | proposed-gw (Node 6) | 0.404 ms |

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
- **Available RAM:** 8.10 GB
- **Used RAM:** 6.91 GB (46.0%)
- **Swap Space:** 0.00 GB used of 4.00 GB total

#### 💾 Storage & Disks
- **Root Mount Filesystem:** `/dev/nvme0n1p2` (ext4)
- **Root Disk Space:** 194G used / 251G free (Total: 468G, Use%: 44%)
- **Disk I/O Read Speed:** `0.00 MB/s` (Device: `nvme0n1`)
- **Disk I/O Write Speed:** `0.00 MB/s`
- **Disk Utilization:** `0.00%`
- **Average Disk Await Time:** `0.00 ms`
- **Physical Disks / RAID Groups:**
- **nvme0n1**: INTEL SSDPEKNW512G8 (476.9G)

##### Selected `df -hT` output:
```text
Filesystem     Type      Size  Used Avail Use% Mounted on
tmpfs          tmpfs     1.6G  2.2M  1.5G   1% /run
/dev/nvme0n1p2 ext4      468G  194G  251G  44% /
tmpfs          tmpfs     7.6G  8.0K  7.6G   1% /dev/shm
tmpfs          tmpfs     5.0M   12K  5.0M   1% /run/lock
efivarfs       efivarfs  128K   43K   81K  35% /sys/firmware/efi/efivars
/dev/nvme0n1p1 vfat      1.1G  6.2M  1.1G   1% /boot/efi
tmpfs          tmpfs     1.6G   92K  1.6G   1% /run/user/120
tmpfs          tmpfs     1.6G   80K  1.6G   1% /run/user/1003
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
- **IPv4 Addresses:** `10.129.148.246 172.17.0.1 172.18.0.1 172.19.0.1 172.21.0.1`

#### 🧠 Memory (RAM) Allocation
- **Total RAM:** 15.01 GB
- **Available RAM:** 2.26 GB
- **Used RAM:** 12.75 GB (84.9%)
- **Swap Space:** 10.14 GB used of 46.57 GB total

#### 💾 Storage & Disks
- **Root Mount Filesystem:** `/dev/nvme0n1p4` (ext4)
- **Root Disk Space:** 68G used / 106G free (Total: 183G, Use%: 39%)
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
/dev/nvme0n1p4 ext4      183G   68G  106G  39% /
tmpfs          tmpfs     7.6G  143M  7.4G   2% /dev/shm
tmpfs          tmpfs     5.0M  4.0K  5.0M   1% /run/lock
efivarfs       efivarfs  128K   33K   91K  27% /sys/firmware/efi/efivars
tmpfs          tmpfs     7.6G     0  7.6G   0% /run/qemu
/dev/nvme0n1p2 ext4      921M  302M  556M  36% /boot
/dev/nvme0n1p1 vfat      952M  6.1M  946M   1% /boot/efi
/dev/nvme0n1p5 ext4      238G  181G   45G  81% /home
tmpfs          tmpfs     1.6G  128K  1.6G   1% /run/user/1001
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
- **Available RAM:** 3.44 GB
- **Used RAM:** 11.58 GB (77.1%)
- **Swap Space:** 0.05 GB used of 4.00 GB total

#### 💾 Storage & Disks
- **Root Mount Filesystem:** `/dev/nvme0n1p2` (ext4)
- **Root Disk Space:** 273G used / 112G free (Total: 404G, Use%: 72%)
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
tmpfs          tmpfs     1.6G  3.0M  1.5G   1% /run
/dev/nvme0n1p2 ext4      404G  273G  112G  72% /
tmpfs          tmpfs     7.6G  1.4M  7.6G   1% /dev/shm
tmpfs          tmpfs     5.0M     0  5.0M   0% /run/lock
efivarfs       efivarfs  128K   42K   82K  34% /sys/firmware/efi/efivars
tmpfs          tmpfs     7.6G     0  7.6G   0% /run/qemu
/dev/sda1      ext4      458G  226G  209G  53% /data
/dev/nvme0n1p1 vfat      1.1G  6.2M  1.1G   1% /boot/efi
tmpfs          tmpfs     1.6G  2.5M  1.5G   1% /run/user/1003
```

#### 🔌 Port & Service Sockets Status
- **PostgreSQL DB Server (Port `5438`):** 🟢 Running (Accepting connections)
- **AriaBC Raft Client Server (Port `8001`):** 🔴 Stopped

### 🖥️ Node 5: asus-laptop (`127.0.0.1`)

#### ⚙️ System Specifications
- **Host/FQDN:** `neel-ASUS-TUF-Gaming-A15-FA507RE-FA577RE`
- **Operating System:** Ubuntu 24.04.4 LTS
- **Kernel Version:** `6.8.0-137-generic`
- **CPU Model:** `AMD Ryzen 7 6800H with Radeon Graphics`
- **CPU Logical Cores (Threads):** `16`
- **IPv4 Addresses:** `10.51.15.251 100.114.239.70 172.19.0.1 172.17.0.1 172.18.0.1 fd7a:115c:a1e0::d01:efa8`

#### 🧠 Memory (RAM) Allocation
- **Total RAM:** 14.87 GB
- **Available RAM:** 7.19 GB
- **Used RAM:** 7.68 GB (51.6%)
- **Swap Space:** 0.00 GB used of 15.81 GB total

#### 💾 Storage & Disks
- **Root Mount Filesystem:** `/dev/nvme0n1p5` (ext4)
- **Root Disk Space:** 86G used / 71G free (Total: 164G, Use%: 55%)
- **Disk I/O Read Speed:** `0.00 MB/s` (Device: `nvme0n1`)
- **Disk I/O Write Speed:** `0.00 MB/s`
- **Disk Utilization:** `0.00%`
- **Average Disk Await Time:** `0.00 ms`
- **Physical Disks / RAID Groups:**
- **nvme0n1**: INTEL SSDPEKNU512GZ (476.9G)

##### Selected `df -hT` output:
```text
Filesystem     Type      Size  Used Avail Use% Mounted on
tmpfs          tmpfs     1.5G  3.1M  1.5G   1% /run
/dev/nvme0n1p5 ext4      164G   86G   71G  55% /
tmpfs          tmpfs     7.5G   58M  7.4G   1% /dev/shm
tmpfs          tmpfs     5.0M   12K  5.0M   1% /run/lock
efivarfs       efivarfs  128K   60K   64K  49% /sys/firmware/efi/efivars
tmpfs          tmpfs     7.5G     0  7.5G   0% /run/qemu
/dev/nvme0n1p7 ext4       49G   35G   12G  75% /home
/dev/nvme0n1p1 vfat      256M   39M  218M  15% /boot/efi
tmpfs          tmpfs     1.5G  152K  1.5G   1% /run/user/1000
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
- **IPv4 Addresses:** ``

#### 🧠 Memory (RAM) Allocation
- **Total RAM:** 15.02 GB
- **Available RAM:** 11.06 GB
- **Used RAM:** 3.95 GB (26.3%)
- **Swap Space:** 0.00 GB used of 4.00 GB total

#### 💾 Storage & Disks
- **Root Mount Filesystem:** `/dev/nvme0n1p2` (ext4)
- **Root Disk Space:** 229G used / 205G free (Total: 457G, Use%: 53%)
- **Disk I/O Read Speed:** `0.00 MB/s` (Device: `nvme0n1`)
- **Disk I/O Write Speed:** `0.00 MB/s`
- **Disk Utilization:** `0.00%`
- **Average Disk Await Time:** `0.00 ms`
- **Physical Disks / RAID Groups:**
- **nvme0n1**: CT500P2SSD8 (465.8G)

##### Selected `df -hT` output:
```text
Filesystem     Type      Size  Used Avail Use% Mounted on
tmpfs          tmpfs     1.6G  3.2M  1.5G   1% /run
/dev/nvme0n1p2 ext4      457G  229G  205G  53% /
tmpfs          tmpfs     7.6G   40M  7.5G   1% /dev/shm
tmpfs          tmpfs     5.0M   12K  5.0M   1% /run/lock
efivarfs       efivarfs  128K   42K   82K  34% /sys/firmware/efi/efivars
/dev/nvme0n1p1 vfat      1.1G  6.2M  1.1G   1% /boot/efi
tmpfs          tmpfs     1.6G  148K  1.6G   1% /run/user/1006
tmpfs          tmpfs     1.6G  136K  1.6G   1% /run/user/1005
tmpfs          tmpfs     1.6G  164K  1.6G   1% /run/user/1007
```

#### 🔌 Port & Service Sockets Status
- **Role:** Gateway Machine (No local PostgreSQL database or AriaBC Server running)

### 🖥️ Node 7: ranking-epyc (`ranking.cse.iitb.ac.in`)

#### ⚙️ System Specifications
- **Host/FQDN:** `user-MZ73-LM0-000`
- **Operating System:** Ubuntu 24.04.3 LTS
- **Kernel Version:** `6.17.0-35-generic`
- **CPU Model:** `AMD EPYC 9654 96-Core Processor`
- **CPU Logical Cores (Threads):** `192`
- **IPv4 Addresses:** `10.129.7.57 10.0.3.1`

#### 🧠 Memory (RAM) Allocation
- **Total RAM:** 251.32 GB
- **Available RAM:** 125.96 GB
- **Used RAM:** 125.36 GB (49.9%)
- **Swap Space:** Disabled / None

#### 💾 Storage & Disks
- **Root Mount Filesystem:** `/dev/nvme0n1p3` (ext4)
- **Root Disk Space:** 492G used / 1.3T free (Total: 1.8T, Use%: 28%)
- **Disk I/O Read Speed:** `0.00 MB/s` (Device: `nvme0n1`)
- **Disk I/O Write Speed:** `15.59 MB/s`
- **Disk Utilization:** `1.00%`
- **Average Disk Await Time:** `0.18 ms`
- **Physical Disks / RAID Groups:**
- **nvme0n1**: CT2000P3PSSD8 (1.8T)
- **nvme1n1**: Samsung SSD 990 EVO Plus 2TB (1.8T)
- **nvme2n1**: Samsung SSD 990 EVO Plus 2TB (1.8T)
- **nvme3n1**: Samsung SSD 990 EVO Plus 2TB (1.8T)
- **nvme4n1**: Samsung SSD 990 EVO Plus 2TB (1.8T)

##### Selected `df -hT` output:
```text
Filesystem     Type      Size  Used Avail Use% Mounted on
tmpfs          tmpfs      26G  3.7M   26G   1% /run
efivarfs       efivarfs  128K   28K   96K  23% /sys/firmware/efi/efivars
/dev/nvme0n1p3 ext4      1.8T  492G  1.3T  28% /
tmpfs          tmpfs     126G  4.1M  126G   1% /dev/shm
tmpfs          tmpfs     5.0M     0  5.0M   0% /run/lock
tmpfs          tmpfs     126G     0  126G   0% /run/qemu
/dev/nvme0n1p1 ext4      442M  214M  194M  53% /boot
/dev/md0       ext4      5.5T  2.9T  2.4T  56% /backup
/dev/nvme0n1p2 vfat      1.1G  6.2M  1.1G   1% /boot/efi
tmpfs          tmpfs      26G  112K   26G   1% /run/user/120
tmpfs          tmpfs      26G   84K   26G   1% /run/user/1006
tmpfs          tmpfs      26G   96K   26G   1% /run/user/1005
tmpfs          tmpfs      26G   80K   26G   1% /run/user/1007
```

#### 🔌 Port & Service Sockets Status
- **PostgreSQL DB Server (Port `5438`):** 🔴 Stopped
- **AriaBC Raft Client Server (Port `8000`):** 🔴 Stopped

