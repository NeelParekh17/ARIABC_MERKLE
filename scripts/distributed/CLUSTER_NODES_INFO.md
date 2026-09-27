# 🖥️ AriaBC 4-Node Cluster Inventory & Live Status

> **Last Updated:** `2026-09-27 17:56:36 +0530`
> **Automatic Update:** Yes (Updated via `scripts/distributed/update_nodes_info.py`)
> **Service Sockets Checked:** BCDB PostgreSQL (Port `5438`), AriaBC Server (Port `8000`/`8001`)

## 📊 Cluster Summary Table

| Node | Name | IP Address | Status | OS | CPU | RAM (Total/Avail) | Root Storage (Avail/Use%) | Disk Read MB/s | Disk Write MB/s | Disk Util % | Await | PostgreSQL | AriaBC Server |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| **Node 1** | admin123 | `10.129.148.247` | 🟢 Online | Ubuntu 24.04.3 LTS | 16 Cores | 15.01 GB / 12.78 GB (14.9% used) | 234G / 468G (48%) | 0.00 | 0.03 | 0.20% | 0.20 ms | 🔴 Stopped | 🔴 Stopped |
| **Node 2** | user4 | `10.129.148.246` | 🟢 Online | Ubuntu 22.04.2 LTS | 16 Cores | 15.01 GB / 1.64 GB (89.1% used) | 48G / 183G (73%) | 0.00 | 0.12 | 0.20% | 0.50 ms | 🔴 Stopped | 🔴 Stopped |
| **Node 4** | utkarsh | `10.129.148.248` | 🟢 Online | Ubuntu 24.04.3 LTS | 16 Cores | 15.02 GB / 9.26 GB (38.3% used) | 76G / 404G (81%) | 0.00 | 0.14 | 0.10% | 0.40 ms | 🔴 Stopped | 🔴 Stopped |
| **ASUS Laptop (GW)** | asus-laptop | `127.0.0.1` | 🟢 Online | Ubuntu 24.04.4 LTS | 16 Cores | 14.87 GB / 7.99 GB (46.3% used) | 12G / 164G (93%) | 0.00 | 0.00 | 0.00% | 0.00 ms | — | — |
| **Gateway 2 (Proposed)** | proposed-gw | `10.129.27.111` | 🟢 Online | Ubuntu 24.04.2 LTS | 16 Cores | 15.01 GB / 9.32 GB (37.9% used) | 163G / 457G (63%) | 0.67 | 0.30 | 2.30% | 0.76 ms | — | — |
| **Node 7** | ranking-epyc | `ranking.cse.iitb.ac.in` | 🟢 Online | Ubuntu 24.04.3 LTS | 192 Cores | 251.31 GB / 205.49 GB (18.2% used) | 1.3T / 1.8T (30%) | 0.00 | 0.16 | 1.00% | 1.25 ms | 🔴 Stopped | 🔴 Stopped |

## 🌐 Network Latency Matrix (RTT)

| From | To | RTT |
|---|---|---|
| admin123 (Node 1) | user4 (Node 2) | 0.139 ms |
| admin123 (Node 1) | utkarsh (Node 4) | 0.230 ms |
| admin123 (Node 1) | asus-laptop (Node 5) | 0.049 ms |
| admin123 (Node 1) | proposed-gw (Node 6) | 0.622 ms |
| admin123 (Node 1) | ranking-epyc (Node 7) | 0.208 ms |
| user4 (Node 2) | admin123 (Node 1) | 0.119 ms |
| user4 (Node 2) | utkarsh (Node 4) | 0.120 ms |
| user4 (Node 2) | asus-laptop (Node 5) | 0.027 ms |
| user4 (Node 2) | proposed-gw (Node 6) | 0.731 ms |
| user4 (Node 2) | ranking-epyc (Node 7) | 0.199 ms |
| utkarsh (Node 4) | admin123 (Node 1) | 0.186 ms |
| utkarsh (Node 4) | user4 (Node 2) | 0.105 ms |
| utkarsh (Node 4) | asus-laptop (Node 5) | 0.024 ms |
| utkarsh (Node 4) | proposed-gw (Node 6) | 0.493 ms |
| utkarsh (Node 4) | ranking-epyc (Node 7) | 0.223 ms |
| asus-laptop (Node 5) | admin123 (Node 1) | 3.278 ms |
| asus-laptop (Node 5) | user4 (Node 2) | 3.459 ms |
| asus-laptop (Node 5) | utkarsh (Node 4) | 2.406 ms |
| asus-laptop (Node 5) | proposed-gw (Node 6) | 4.368 ms |
| asus-laptop (Node 5) | ranking-epyc (Node 7) | 3.541 ms |
| proposed-gw (Node 6) | admin123 (Node 1) | 0.742 ms |
| proposed-gw (Node 6) | user4 (Node 2) | 0.281 ms |
| proposed-gw (Node 6) | utkarsh (Node 4) | 0.454 ms |
| proposed-gw (Node 6) | asus-laptop (Node 5) | 0.023 ms |
| proposed-gw (Node 6) | ranking-epyc (Node 7) | 0.391 ms |
| ranking-epyc (Node 7) | admin123 (Node 1) | 0.428 ms |
| ranking-epyc (Node 7) | user4 (Node 2) | 0.280 ms |
| ranking-epyc (Node 7) | utkarsh (Node 4) | 0.285 ms |
| ranking-epyc (Node 7) | asus-laptop (Node 5) | 0.036 ms |
| ranking-epyc (Node 7) | proposed-gw (Node 6) | 0.398 ms |

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
- **Available RAM:** 12.78 GB
- **Used RAM:** 2.23 GB (14.9%)
- **Swap Space:** 0.67 GB used of 4.00 GB total

#### 💾 Storage & Disks
- **Root Mount Filesystem:** `/dev/nvme0n1p2` (ext4)
- **Root Disk Space:** 211G used / 234G free (Total: 468G, Use%: 48%)
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
/dev/nvme0n1p2 ext4      468G  211G  234G  48% /
tmpfs          tmpfs     7.6G  152K  7.6G   1% /dev/shm
tmpfs          tmpfs     5.0M   12K  5.0M   1% /run/lock
efivarfs       efivarfs  128K   43K   81K  35% /sys/firmware/efi/efivars
/dev/nvme0n1p1 vfat      1.1G  6.2M  1.1G   1% /boot/efi
tmpfs          tmpfs     1.6G  132K  1.6G   1% /run/user/1003
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
- **Available RAM:** 1.64 GB
- **Used RAM:** 13.38 GB (89.1%)
- **Swap Space:** 2.03 GB used of 46.57 GB total

#### 💾 Storage & Disks
- **Root Mount Filesystem:** `/dev/nvme0n1p4` (ext4)
- **Root Disk Space:** 126G used / 48G free (Total: 183G, Use%: 73%)
- **Disk I/O Read Speed:** `0.00 MB/s` (Device: `nvme0n1`)
- **Disk I/O Write Speed:** `0.12 MB/s`
- **Disk Utilization:** `0.20%`
- **Average Disk Await Time:** `0.50 ms`
- **Physical Disks / RAID Groups:**
- **nvme0n1**: INTEL SSDPEKNW512G8 (476.9G)

##### Selected `df -hT` output:
```text
Filesystem     Type      Size  Used Avail Use% Mounted on
tmpfs          tmpfs     1.6G  2.9M  1.5G   1% /run
/dev/nvme0n1p4 ext4      183G  126G   48G  73% /
tmpfs          tmpfs     7.6G  217M  7.3G   3% /dev/shm
tmpfs          tmpfs     5.0M  4.0K  5.0M   1% /run/lock
efivarfs       efivarfs  128K   33K   91K  27% /sys/firmware/efi/efivars
tmpfs          tmpfs     7.6G     0  7.6G   0% /run/qemu
/dev/nvme0n1p2 ext4      921M  304M  554M  36% /boot
/dev/nvme0n1p1 vfat      952M  6.1M  946M   1% /boot/efi
/dev/nvme0n1p5 ext4      238G  192G   34G  86% /home
tmpfs          tmpfs     1.6G   60K  1.6G   1% /run/user/1004
tmpfs          tmpfs     1.6G  132K  1.6G   1% /run/user/1001
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
- **Available RAM:** 9.26 GB
- **Used RAM:** 5.75 GB (38.3%)
- **Swap Space:** 3.35 GB used of 4.00 GB total

#### 💾 Storage & Disks
- **Root Mount Filesystem:** `/dev/nvme0n1p2` (ext4)
- **Root Disk Space:** 309G used / 76G free (Total: 404G, Use%: 81%)
- **Disk I/O Read Speed:** `0.00 MB/s` (Device: `nvme0n1`)
- **Disk I/O Write Speed:** `0.14 MB/s`
- **Disk Utilization:** `0.10%`
- **Average Disk Await Time:** `0.40 ms`
- **Physical Disks / RAID Groups:**
- **sda**: Samsung SSD 840 EVO 500GB (465.8G)
- **nvme0n1**: INTEL SSDPEKNW512G8 (476.9G)

##### Selected `df -hT` output:
```text
Filesystem     Type      Size  Used Avail Use% Mounted on
tmpfs          tmpfs     1.6G  6.1M  1.5G   1% /run
/dev/nvme0n1p2 ext4      404G  309G   76G  81% /
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
- **Kernel Version:** `6.8.0-142-generic`
- **CPU Model:** `AMD Ryzen 7 6800H with Radeon Graphics`
- **CPU Logical Cores (Threads):** `16`
- **IPv4 Addresses:** `192.168.0.154 100.114.239.70 172.19.0.1 172.17.0.1 172.18.0.1 fd7a:115c:a1e0::d01:efa8`

#### 🧠 Memory (RAM) Allocation
- **Total RAM:** 14.87 GB
- **Available RAM:** 7.99 GB
- **Used RAM:** 6.88 GB (46.3%)
- **Swap Space:** 0.43 GB used of 15.81 GB total

#### 💾 Storage & Disks
- **Root Mount Filesystem:** `/dev/nvme0n1p5` (ext4)
- **Root Disk Space:** 144G used / 12G free (Total: 164G, Use%: 93%)
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
/dev/nvme0n1p5 ext4      164G  144G   12G  93% /
tmpfs          tmpfs     7.5G   55M  7.4G   1% /dev/shm
tmpfs          tmpfs     5.0M   12K  5.0M   1% /run/lock
efivarfs       efivarfs  128K   71K   53K  58% /sys/firmware/efi/efivars
tmpfs          tmpfs     7.5G     0  7.5G   0% /run/qemu
/dev/nvme0n1p7 ext4       49G   45G  1.4G  98% /home
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
- **Available RAM:** 9.32 GB
- **Used RAM:** 5.69 GB (37.9%)
- **Swap Space:** 1.95 GB used of 4.00 GB total

#### 💾 Storage & Disks
- **Root Mount Filesystem:** `/dev/nvme0n1p2` (ext4)
- **Root Disk Space:** 271G used / 163G free (Total: 457G, Use%: 63%)
- **Disk I/O Read Speed:** `0.67 MB/s` (Device: `nvme0n1`)
- **Disk I/O Write Speed:** `0.30 MB/s`
- **Disk Utilization:** `2.30%`
- **Average Disk Await Time:** `0.76 ms`
- **Physical Disks / RAID Groups:**
- **nvme0n1**: CT500P2SSD8 (465.8G)

##### Selected `df -hT` output:
```text
Filesystem     Type      Size  Used Avail Use% Mounted on
tmpfs          tmpfs     1.6G  2.6M  1.5G   1% /run
/dev/nvme0n1p2 ext4      457G  271G  163G  63% /
tmpfs          tmpfs     7.6G  162M  7.4G   3% /dev/shm
tmpfs          tmpfs     5.0M   12K  5.0M   1% /run/lock
efivarfs       efivarfs  128K   42K   82K  34% /sys/firmware/efi/efivars
/dev/nvme0n1p1 vfat      1.1G  6.2M  1.1G   1% /boot/efi
tmpfs          tmpfs     1.6G  156K  1.6G   1% /run/user/1007
tmpfs          tmpfs     1.6G   88K  1.6G   1% /run/user/1002
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
- **Available RAM:** 205.49 GB
- **Used RAM:** 45.83 GB (18.2%)
- **Swap Space:** Disabled / None

#### 💾 Storage & Disks
- **Root Mount Filesystem:** `/dev/nvme0n1p3` (ext4)
- **Root Disk Space:** 526G used / 1.3T free (Total: 1.8T, Use%: 30%)
- **Disk I/O Read Speed:** `0.00 MB/s` (Device: `nvme0n1`)
- **Disk I/O Write Speed:** `0.16 MB/s`
- **Disk Utilization:** `1.00%`
- **Average Disk Await Time:** `1.25 ms`
- **Physical Disks / RAID Groups:**
- **nvme0n1**: CT2000P3PSSD8 (1.8T)
- **nvme3n1**: Samsung SSD 990 EVO Plus 2TB (1.8T)
- **nvme2n1**: Samsung SSD 990 EVO Plus 2TB (1.8T)
- **nvme1n1**: Samsung SSD 990 EVO Plus 2TB (1.8T)
- **nvme4n1**: Samsung SSD 990 EVO Plus 2TB (1.8T)

##### Selected `df -hT` output:
```text
Filesystem     Type      Size  Used Avail Use% Mounted on
tmpfs          tmpfs      26G  4.2M   26G   1% /run
efivarfs       efivarfs  128K   28K   96K  23% /sys/firmware/efi/efivars
/dev/nvme0n1p3 ext4      1.8T  526G  1.3T  30% /
tmpfs          tmpfs     126G  2.3M  126G   1% /dev/shm
tmpfs          tmpfs     5.0M     0  5.0M   0% /run/lock
tmpfs          tmpfs     126G     0  126G   0% /run/qemu
/dev/nvme0n1p1 ext4      442M  215M  193M  53% /boot
/dev/nvme0n1p2 vfat      1.1G  6.2M  1.1G   1% /boot/efi
/dev/md0       ext4      5.5T  2.9T  2.3T  56% /backup
tmpfs          tmpfs      26G   96K   26G   1% /run/user/120
tmpfs          tmpfs      26G   92K   26G   1% /run/user/1005
tmpfs          tmpfs      26G   88K   26G   1% /run/user/1006
tmpfs          tmpfs      26G   80K   26G   1% /run/user/1001
tmpfs          tmpfs      26G   84K   26G   1% /run/user/1002
tmpfs          tmpfs      26G   84K   26G   1% /run/user/1004
tmpfs          tmpfs      26G   80K   26G   1% /run/user/1009
tmpfs          tmpfs      26G   80K   26G   1% /run/user/1008
tmpfs          tmpfs      26G   80K   26G   1% /run/user/1007
```

#### 🔌 Port & Service Sockets Status
- **PostgreSQL DB Server (Port `5438`):** 🔴 Stopped
- **AriaBC Raft Client Server (Port `8000`):** 🔴 Stopped

