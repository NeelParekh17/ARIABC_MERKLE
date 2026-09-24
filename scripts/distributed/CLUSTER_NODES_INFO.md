# 🖥️ AriaBC 4-Node Cluster Inventory & Live Status

> **Last Updated:** `2026-09-23 20:40:49 +0530`
> **Automatic Update:** Yes (Updated via `scripts/distributed/update_nodes_info.py`)
> **Service Sockets Checked:** BCDB PostgreSQL (Port `5438`), AriaBC Server (Port `8000`/`8001`)

## 📊 Cluster Summary Table

| Node | Name | IP Address | Status | OS | CPU | RAM (Total/Avail) | Root Storage (Avail/Use%) | Disk Read MB/s | Disk Write MB/s | Disk Util % | Await | PostgreSQL | AriaBC Server |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| **Node 1** | admin123 | `10.129.148.247` | 🟢 Online | Ubuntu 24.04.3 LTS | 16 Cores | 15.01 GB / 12.47 GB (17.0% used) | 237G / 468G (47%) | 0.00 | 0.03 | 0.20% | 0.20 ms | 🔴 Stopped | 🔴 Stopped |
| **Node 2** | user4 | `10.129.148.246` | 🟢 Online | Ubuntu 22.04.2 LTS | 16 Cores | 15.01 GB / 4.31 GB (71.3% used) | 48G / 183G (73%) | 0.00 | 0.00 | 0.00% | 0.00 ms | 🔴 Stopped | 🔴 Stopped |
| **Node 4** | utkarsh | `10.129.148.248` | 🟢 Online | Ubuntu 24.04.3 LTS | 16 Cores | 15.02 GB / 9.15 GB (39.1% used) | 78G / 404G (80%) | 0.00 | 0.00 | 0.00% | 0.00 ms | 🔴 Stopped | 🔴 Stopped |
| **ASUS Laptop (GW)** | asus-laptop | `127.0.0.1` | 🟢 Online | Ubuntu 24.04.4 LTS | 16 Cores | 14.87 GB / 5.94 GB (60.1% used) | 13G / 164G (92%) | 0.00 | 0.00 | 0.00% | 0.00 ms | — | — |
| **Gateway 2 (Proposed)** | proposed-gw | `10.129.27.111` | 🟢 Online | Ubuntu 24.04.2 LTS | 16 Cores | 15.01 GB / 7.74 GB (48.5% used) | 170G / 457G (61%) | 0.00 | 0.08 | 0.10% | 1.67 ms | — | — |
| **Node 7** | ranking-epyc | `ranking.cse.iitb.ac.in` | 🟢 Online | Ubuntu 24.04.3 LTS | 192 Cores | 251.31 GB / 206.94 GB (17.7% used) | 1.2T / 1.8T (31%) | 0.00 | 0.05 | 0.30% | 1.00 ms | 🔴 Stopped | 🔴 Stopped |

## 🌐 Network Latency Matrix (RTT)

| From | To | RTT |
|---|---|---|
| admin123 (Node 1) | user4 (Node 2) | 0.215 ms |
| admin123 (Node 1) | utkarsh (Node 4) | 0.186 ms |
| admin123 (Node 1) | asus-laptop (Node 5) | 0.052 ms |
| admin123 (Node 1) | proposed-gw (Node 6) | 0.560 ms |
| admin123 (Node 1) | ranking-epyc (Node 7) | 0.334 ms |
| user4 (Node 2) | admin123 (Node 1) | 0.229 ms |
| user4 (Node 2) | utkarsh (Node 4) | 0.123 ms |
| user4 (Node 2) | asus-laptop (Node 5) | 0.041 ms |
| user4 (Node 2) | proposed-gw (Node 6) | 0.646 ms |
| user4 (Node 2) | ranking-epyc (Node 7) | 0.279 ms |
| utkarsh (Node 4) | admin123 (Node 1) | 0.118 ms |
| utkarsh (Node 4) | user4 (Node 2) | 0.133 ms |
| utkarsh (Node 4) | asus-laptop (Node 5) | 0.025 ms |
| utkarsh (Node 4) | proposed-gw (Node 6) | 0.434 ms |
| utkarsh (Node 4) | ranking-epyc (Node 7) | 0.301 ms |
| asus-laptop (Node 5) | admin123 (Node 1) | 3.384 ms |
| asus-laptop (Node 5) | user4 (Node 2) | 3.530 ms |
| asus-laptop (Node 5) | utkarsh (Node 4) | 1.291 ms |
| asus-laptop (Node 5) | proposed-gw (Node 6) | 2.050 ms |
| asus-laptop (Node 5) | ranking-epyc (Node 7) | 2.037 ms |
| proposed-gw (Node 6) | admin123 (Node 1) | 0.652 ms |
| proposed-gw (Node 6) | user4 (Node 2) | 0.528 ms |
| proposed-gw (Node 6) | utkarsh (Node 4) | 0.446 ms |
| proposed-gw (Node 6) | asus-laptop (Node 5) | 0.023 ms |
| proposed-gw (Node 6) | ranking-epyc (Node 7) | 0.529 ms |
| ranking-epyc (Node 7) | admin123 (Node 1) | 0.396 ms |
| ranking-epyc (Node 7) | user4 (Node 2) | 0.365 ms |
| ranking-epyc (Node 7) | utkarsh (Node 4) | 0.211 ms |
| ranking-epyc (Node 7) | asus-laptop (Node 5) | 0.050 ms |
| ranking-epyc (Node 7) | proposed-gw (Node 6) | 0.468 ms |

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
- **Available RAM:** 12.47 GB
- **Used RAM:** 2.55 GB (17.0%)
- **Swap Space:** 0.82 GB used of 4.00 GB total

#### 💾 Storage & Disks
- **Root Mount Filesystem:** `/dev/nvme0n1p2` (ext4)
- **Root Disk Space:** 208G used / 237G free (Total: 468G, Use%: 47%)
- **Disk I/O Read Speed:** `0.00 MB/s` (Device: `nvme0n1`)
- **Disk I/O Write Speed:** `0.03 MB/s`
- **Disk Utilization:** `0.20%`
- **Average Disk Await Time:** `0.20 ms`
- **Physical Disks / RAID Groups:**
- **nvme0n1**: INTEL SSDPEKNW512G8 (476.9G)

##### Selected `df -hT` output:
```text
Filesystem     Type      Size  Used Avail Use% Mounted on
tmpfs          tmpfs     1.6G  3.2M  1.5G   1% /run
/dev/nvme0n1p2 ext4      468G  208G  237G  47% /
tmpfs          tmpfs     7.6G   96K  7.6G   1% /dev/shm
tmpfs          tmpfs     5.0M   12K  5.0M   1% /run/lock
efivarfs       efivarfs  128K   43K   81K  35% /sys/firmware/efi/efivars
/dev/nvme0n1p1 vfat      1.1G  6.2M  1.1G   1% /boot/efi
tmpfs          tmpfs     1.6G  140K  1.6G   1% /run/user/1003
```

#### 🔌 Port & Service Sockets Status
- **PostgreSQL DB Server (Port `5438`):** 🔴 Stopped
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
- **Available RAM:** 4.31 GB
- **Used RAM:** 10.70 GB (71.3%)
- **Swap Space:** 10.22 GB used of 46.57 GB total

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
tmpfs          tmpfs     1.6G  3.1M  1.5G   1% /run
/dev/nvme0n1p4 ext4      183G  126G   48G  73% /
tmpfs          tmpfs     7.6G  260M  7.3G   4% /dev/shm
tmpfs          tmpfs     5.0M  4.0K  5.0M   1% /run/lock
efivarfs       efivarfs  128K   33K   91K  27% /sys/firmware/efi/efivars
tmpfs          tmpfs     7.6G     0  7.6G   0% /run/qemu
/dev/nvme0n1p2 ext4      921M  304M  554M  36% /boot
/dev/nvme0n1p1 vfat      952M  6.1M  946M   1% /boot/efi
/dev/nvme0n1p5 ext4      238G  193G   33G  86% /home
tmpfs          tmpfs     1.6G   60K  1.6G   1% /run/user/1004
tmpfs          tmpfs     1.6G  136K  1.6G   1% /run/user/1001
```

#### 🔌 Port & Service Sockets Status
- **PostgreSQL DB Server (Port `5438`):** 🔴 Stopped
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
- **Available RAM:** 9.15 GB
- **Used RAM:** 5.87 GB (39.1%)
- **Swap Space:** 3.13 GB used of 4.00 GB total

#### 💾 Storage & Disks
- **Root Mount Filesystem:** `/dev/nvme0n1p2` (ext4)
- **Root Disk Space:** 307G used / 78G free (Total: 404G, Use%: 80%)
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
tmpfs          tmpfs     1.6G  5.8M  1.5G   1% /run
/dev/nvme0n1p2 ext4      404G  307G   78G  80% /
tmpfs          tmpfs     7.6G   81M  7.5G   2% /dev/shm
tmpfs          tmpfs     5.0M     0  5.0M   0% /run/lock
efivarfs       efivarfs  128K   42K   82K  34% /sys/firmware/efi/efivars
tmpfs          tmpfs     7.6G     0  7.6G   0% /run/qemu
/dev/sda1      ext4      458G  332G  103G  77% /data
/dev/nvme0n1p1 vfat      1.1G  6.2M  1.1G   1% /boot/efi
tmpfs          tmpfs     1.6G  156K  1.6G   1% /run/user/1000
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
- **IPv4 Addresses:** `192.168.0.154 100.114.239.70 172.19.0.1 172.18.0.1 172.17.0.1 fd7a:115c:a1e0::d01:efa8`

#### 🧠 Memory (RAM) Allocation
- **Total RAM:** 14.87 GB
- **Available RAM:** 5.94 GB
- **Used RAM:** 8.93 GB (60.1%)
- **Swap Space:** 1.58 GB used of 15.81 GB total

#### 💾 Storage & Disks
- **Root Mount Filesystem:** `/dev/nvme0n1p5` (ext4)
- **Root Disk Space:** 143G used / 13G free (Total: 164G, Use%: 92%)
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
/dev/nvme0n1p5 ext4      164G  143G   13G  92% /
tmpfs          tmpfs     7.5G   67M  7.4G   1% /dev/shm
tmpfs          tmpfs     5.0M   12K  5.0M   1% /run/lock
efivarfs       efivarfs  128K   71K   53K  58% /sys/firmware/efi/efivars
tmpfs          tmpfs     7.5G     0  7.5G   0% /run/qemu
/dev/nvme0n1p7 ext4       49G   43G  3.7G  93% /home
/dev/nvme0n1p1 vfat      256M   39M  218M  15% /boot/efi
tmpfs          tmpfs     1.5G  2.6M  1.5G   1% /run/user/1000
```

#### 🔌 Port & Service Sockets Status
- **Role:** Gateway Machine (No local PostgreSQL database or AriaBC Server running)

### 🖥️ Node 6: proposed-gw (`10.129.27.111`)

#### ⚙️ System Specifications
- **Host/FQDN:** `myubuntu`
- **Operating System:** Ubuntu 24.04.2 LTS
- **Kernel Version:** `7.0.0-31-generic`
- **CPU Model:** `AMD Ryzen 7 5700G with Radeon Graphics`
- **CPU Logical Cores (Threads):** `16`
- **IPv4 Addresses:** `10.129.27.111`

#### 🧠 Memory (RAM) Allocation
- **Total RAM:** 15.01 GB
- **Available RAM:** 7.74 GB
- **Used RAM:** 7.28 GB (48.5%)
- **Swap Space:** 0.00 GB used of 4.00 GB total

#### 💾 Storage & Disks
- **Root Mount Filesystem:** `/dev/nvme0n1p2` (ext4)
- **Root Disk Space:** 265G used / 170G free (Total: 457G, Use%: 61%)
- **Disk I/O Read Speed:** `0.00 MB/s` (Device: `nvme0n1`)
- **Disk I/O Write Speed:** `0.08 MB/s`
- **Disk Utilization:** `0.10%`
- **Average Disk Await Time:** `1.67 ms`
- **Physical Disks / RAID Groups:**
- **nvme0n1**: CT500P2SSD8 (465.8G)

##### Selected `df -hT` output:
```text
Filesystem     Type      Size  Used Avail Use% Mounted on
tmpfs          tmpfs     1.6G  2.7M  1.5G   1% /run
/dev/nvme0n1p2 ext4      457G  265G  170G  61% /
tmpfs          tmpfs     7.6G  161M  7.4G   3% /dev/shm
tmpfs          tmpfs     5.0M   12K  5.0M   1% /run/lock
efivarfs       efivarfs  128K   42K   82K  34% /sys/firmware/efi/efivars
/dev/nvme0n1p1 vfat      1.1G  6.2M  1.1G   1% /boot/efi
tmpfs          tmpfs     1.6G  156K  1.6G   1% /run/user/1007
tmpfs          tmpfs     1.6G   88K  1.6G   1% /run/user/1006
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
- **Available RAM:** 206.94 GB
- **Used RAM:** 44.37 GB (17.7%)
- **Swap Space:** Disabled / None

#### 💾 Storage & Disks
- **Root Mount Filesystem:** `/dev/nvme0n1p3` (ext4)
- **Root Disk Space:** 540G used / 1.2T free (Total: 1.8T, Use%: 31%)
- **Disk I/O Read Speed:** `0.00 MB/s` (Device: `nvme0n1`)
- **Disk I/O Write Speed:** `0.05 MB/s`
- **Disk Utilization:** `0.30%`
- **Average Disk Await Time:** `1.00 ms`
- **Physical Disks / RAID Groups:**
- **nvme0n1**: CT2000P3PSSD8 (1.8T)
- **nvme3n1**: Samsung SSD 990 EVO Plus 2TB (1.8T)
- **nvme2n1**: Samsung SSD 990 EVO Plus 2TB (1.8T)
- **nvme1n1**: Samsung SSD 990 EVO Plus 2TB (1.8T)
- **nvme4n1**: Samsung SSD 990 EVO Plus 2TB (1.8T)

##### Selected `df -hT` output:
```text
Filesystem     Type      Size  Used Avail Use% Mounted on
tmpfs          tmpfs      26G  4.1M   26G   1% /run
efivarfs       efivarfs  128K   28K   96K  23% /sys/firmware/efi/efivars
/dev/nvme0n1p3 ext4      1.8T  540G  1.2T  31% /
tmpfs          tmpfs     126G  1.1M  126G   1% /dev/shm
tmpfs          tmpfs     5.0M     0  5.0M   0% /run/lock
tmpfs          tmpfs     126G     0  126G   0% /run/qemu
/dev/nvme0n1p1 ext4      442M  215M  193M  53% /boot
/dev/nvme0n1p2 vfat      1.1G  6.2M  1.1G   1% /boot/efi
/dev/md0       ext4      5.5T  2.9T  2.3T  56% /backup
tmpfs          tmpfs      26G   96K   26G   1% /run/user/120
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

