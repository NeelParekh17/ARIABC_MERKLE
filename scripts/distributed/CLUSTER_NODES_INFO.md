# 🖥️ AriaBC 4-Node Cluster Inventory & Live Status

> **Last Updated:** `2026-09-28 23:05:08 +0530`
> **Automatic Update:** Yes (Updated via `scripts/distributed/update_nodes_info.py`)
> **Service Sockets Checked:** BCDB PostgreSQL (Port `5438`), AriaBC Server (Port `8000`/`8001`)

## 📊 Cluster Summary Table

| Node | Name | IP Address | Status | OS | CPU | RAM (Total/Avail) | Root Storage (Avail/Use%) | Disk Read MB/s | Disk Write MB/s | Disk Util % | Await | PostgreSQL | AriaBC Server |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| **Node 1** | admin123 | `10.129.148.247` | 🟢 Online | Ubuntu 24.04.3 LTS | 16 Cores | 15.01 GB / 13.29 GB (11.5% used) | 172G / 468G (62%) | 0.10 | 0.04 | 0.40% | 0.38 ms | 🔴 Stopped | 🔴 Stopped |
| **Node 2** | user4 | `10.129.148.246` | 🟢 Online | Ubuntu 22.04.2 LTS | 16 Cores | 15.01 GB / 1.14 GB (92.4% used) | 47G / 183G (73%) | 1.62 | 0.31 | 2.30% | 0.38 ms | 🟢 Running | 🔴 Stopped |
| **Node 4** | utkarsh | `10.129.148.248` | 🟢 Online | Ubuntu 24.04.3 LTS | 16 Cores | 15.02 GB / 6.31 GB (58.0% used) | 77G / 404G (81%) | 17.38 | 0.36 | 12.40% | 0.28 ms | 🟢 Running | 🔴 Stopped |
| **ASUS Laptop (GW)** | asus-laptop | `127.0.0.1` | 🟢 Online | Ubuntu 24.04.4 LTS | 16 Cores | 14.87 GB / 6.72 GB (54.8% used) | 13G / 164G (93%) | 0.09 | 0.00 | 0.70% | 0.54 ms | — | — |
| **Gateway 2 (Proposed)** | proposed-gw | `10.129.27.111` | 🔴 Offline | N/A | N/A | N/A | N/A | N/A | N/A | N/A | N/A | N/A | N/A |
| **Node 7** | ranking-epyc | `ranking.cse.iitb.ac.in` | 🟢 Online | Ubuntu 24.04.3 LTS | 192 Cores | 251.31 GB / 203.16 GB (19.2% used) | 1.2T / 1.8T (31%) | 0.24 | 0.20 | 1.10% | 0.76 ms | 🔴 Stopped | 🔴 Stopped |

## 🌐 Network Latency Matrix (RTT)

| From | To | RTT |
|---|---|---|
| admin123 (Node 1) | user4 (Node 2) | 0.312 ms |
| admin123 (Node 1) | utkarsh (Node 4) | 0.425 ms |
| admin123 (Node 1) | asus-laptop (Node 5) | 0.050 ms |
| admin123 (Node 1) | proposed-gw (Node 6) | 0.689 ms |
| admin123 (Node 1) | ranking-epyc (Node 7) | 0.243 ms |
| user4 (Node 2) | admin123 (Node 1) | 0.441 ms |
| user4 (Node 2) | utkarsh (Node 4) | 0.411 ms |
| user4 (Node 2) | asus-laptop (Node 5) | 0.041 ms |
| user4 (Node 2) | proposed-gw (Node 6) | 0.494 ms |
| user4 (Node 2) | ranking-epyc (Node 7) | 0.351 ms |
| utkarsh (Node 4) | admin123 (Node 1) | 0.469 ms |
| utkarsh (Node 4) | user4 (Node 2) | 0.295 ms |
| utkarsh (Node 4) | asus-laptop (Node 5) | 0.042 ms |
| utkarsh (Node 4) | proposed-gw (Node 6) | 0.522 ms |
| utkarsh (Node 4) | ranking-epyc (Node 7) | 0.418 ms |
| asus-laptop (Node 5) | admin123 (Node 1) | 2.977 ms |
| asus-laptop (Node 5) | user4 (Node 2) | 1.637 ms |
| asus-laptop (Node 5) | utkarsh (Node 4) | 1.677 ms |
| asus-laptop (Node 5) | proposed-gw (Node 6) | 2.495 ms |
| asus-laptop (Node 5) | ranking-epyc (Node 7) | 2.390 ms |
| proposed-gw (Node 6) | admin123 (Node 1) | Offline |
| proposed-gw (Node 6) | user4 (Node 2) | Offline |
| proposed-gw (Node 6) | utkarsh (Node 4) | Offline |
| proposed-gw (Node 6) | asus-laptop (Node 5) | Offline |
| proposed-gw (Node 6) | ranking-epyc (Node 7) | Offline |
| ranking-epyc (Node 7) | admin123 (Node 1) | 0.443 ms |
| ranking-epyc (Node 7) | user4 (Node 2) | 0.426 ms |
| ranking-epyc (Node 7) | utkarsh (Node 4) | 0.448 ms |
| ranking-epyc (Node 7) | asus-laptop (Node 5) | 0.051 ms |
| ranking-epyc (Node 7) | proposed-gw (Node 6) | 0.494 ms |

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
- **Available RAM:** 13.29 GB
- **Used RAM:** 1.73 GB (11.5%)
- **Swap Space:** 1.06 GB used of 4.00 GB total

#### 💾 Storage & Disks
- **Root Mount Filesystem:** `/dev/nvme0n1p2` (ext4)
- **Root Disk Space:** 273G used / 172G free (Total: 468G, Use%: 62%)
- **Disk I/O Read Speed:** `0.10 MB/s` (Device: `nvme0n1`)
- **Disk I/O Write Speed:** `0.04 MB/s`
- **Disk Utilization:** `0.40%`
- **Average Disk Await Time:** `0.38 ms`
- **Physical Disks / RAID Groups:**
- **nvme0n1**: INTEL SSDPEKNW512G8 (476.9G)

##### Selected `df -hT` output:
```text

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
- **Available RAM:** 1.14 GB
- **Used RAM:** 13.87 GB (92.4%)
- **Swap Space:** 3.97 GB used of 46.57 GB total

#### 💾 Storage & Disks
- **Root Mount Filesystem:** `/dev/nvme0n1p4` (ext4)
- **Root Disk Space:** 127G used / 47G free (Total: 183G, Use%: 73%)
- **Disk I/O Read Speed:** `1.62 MB/s` (Device: `nvme0n1`)
- **Disk I/O Write Speed:** `0.31 MB/s`
- **Disk Utilization:** `2.30%`
- **Average Disk Await Time:** `0.38 ms`
- **Physical Disks / RAID Groups:**
- **nvme0n1**: INTEL SSDPEKNW512G8 (476.9G)

##### Selected `df -hT` output:
```text
Filesystem     Type      Size  Used Avail Use% Mounted on
tmpfs          tmpfs     1.6G  3.2M  1.5G   1% /run
/dev/nvme0n1p4 ext4      183G  127G   47G  73% /
tmpfs          tmpfs     7.6G   92K  7.6G   1% /dev/shm
tmpfs          tmpfs     5.0M  4.0K  5.0M   1% /run/lock
efivarfs       efivarfs  128K   33K   91K  27% /sys/firmware/efi/efivars
tmpfs          tmpfs     7.6G     0  7.6G   0% /run/qemu
/dev/nvme0n1p2 ext4      921M  304M  554M  36% /boot
/dev/nvme0n1p1 vfat      952M  6.1M  946M   1% /boot/efi
/dev/nvme0n1p5 ext4      238G  193G   33G  86% /home
tmpfs          tmpfs     1.6G   60K  1.6G   1% /run/user/1004
tmpfs          tmpfs     1.6G  128K  1.6G   1% /run/user/1001
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
- **Available RAM:** 6.31 GB
- **Used RAM:** 8.71 GB (58.0%)
- **Swap Space:** 1.74 GB used of 4.00 GB total

#### 💾 Storage & Disks
- **Root Mount Filesystem:** `/dev/nvme0n1p2` (ext4)
- **Root Disk Space:** 308G used / 77G free (Total: 404G, Use%: 81%)
- **Disk I/O Read Speed:** `17.38 MB/s` (Device: `nvme0n1`)
- **Disk I/O Write Speed:** `0.36 MB/s`
- **Disk Utilization:** `12.40%`
- **Average Disk Await Time:** `0.28 ms`
- **Physical Disks / RAID Groups:**
- **sda**: Samsung SSD 840 EVO 500GB (465.8G)
- **nvme0n1**: INTEL SSDPEKNW512G8 (476.9G)

##### Selected `df -hT` output:
```text
Filesystem     Type      Size  Used Avail Use% Mounted on
tmpfs          tmpfs     1.6G  6.0M  1.5G   1% /run
/dev/nvme0n1p2 ext4      404G  308G   77G  81% /
tmpfs          tmpfs     7.6G  1.4M  7.6G   1% /dev/shm
tmpfs          tmpfs     5.0M     0  5.0M   0% /run/lock
efivarfs       efivarfs  128K   42K   82K  34% /sys/firmware/efi/efivars
tmpfs          tmpfs     7.6G     0  7.6G   0% /run/qemu
/dev/sda1      ext4      458G  332G  103G  77% /data
/dev/nvme0n1p1 vfat      1.1G  6.2M  1.1G   1% /boot/efi
tmpfs          tmpfs     1.6G  140K  1.6G   1% /run/user/1000
tmpfs          tmpfs     1.6G   92K  1.6G   1% /run/user/1003
```

#### 🔌 Port & Service Sockets Status
- **PostgreSQL DB Server (Port `5438`):** 🟢 Running (Accepting connections)
- **AriaBC Raft Client Server (Port `8001`):** 🔴 Stopped

### 🖥️ Node 5: asus-laptop (`127.0.0.1`)

#### ⚙️ System Specifications
- **Host/FQDN:** `neel-ASUS-TUF-Gaming-A15-FA507RE-FA577RE`
- **Operating System:** Ubuntu 24.04.4 LTS
- **Kernel Version:** `6.8.0-142-generic`
- **CPU Model:** `AMD Ryzen 7 6800H with Radeon Graphics`
- **CPU Logical Cores (Threads):** `16`
- **IPv4 Addresses:** `192.168.0.154 100.114.239.70 172.17.0.1 172.19.0.1 172.18.0.1 2100:df0:413:12b5:deeb:3f72:9a7d:b071 2100:df0:413:12b5:9c2a:28a1:d9ae:3d6d fd7a:115c:a1e0::d01:efa8`

#### 🧠 Memory (RAM) Allocation
- **Total RAM:** 14.87 GB
- **Available RAM:** 6.72 GB
- **Used RAM:** 8.15 GB (54.8%)
- **Swap Space:** 0.00 GB used of 15.81 GB total

#### 💾 Storage & Disks
- **Root Mount Filesystem:** `/dev/nvme0n1p5` (ext4)
- **Root Disk Space:** 143G used / 13G free (Total: 164G, Use%: 93%)
- **Disk I/O Read Speed:** `0.09 MB/s` (Device: `nvme0n1`)
- **Disk I/O Write Speed:** `0.00 MB/s`
- **Disk Utilization:** `0.70%`
- **Average Disk Await Time:** `0.54 ms`
- **Physical Disks / RAID Groups:**
- **nvme0n1**: INTEL SSDPEKNU512GZ (476.9G)

##### Selected `df -hT` output:
```text
Filesystem     Type      Size  Used Avail Use% Mounted on
tmpfs          tmpfs     1.5G  3.1M  1.5G   1% /run
/dev/nvme0n1p5 ext4      164G  143G   13G  93% /
tmpfs          tmpfs     7.5G   44M  7.4G   1% /dev/shm
tmpfs          tmpfs     5.0M   12K  5.0M   1% /run/lock
efivarfs       efivarfs  128K   71K   53K  58% /sys/firmware/efi/efivars
tmpfs          tmpfs     7.5G     0  7.5G   0% /run/qemu
/dev/nvme0n1p7 ext4       49G   42G  4.2G  91% /home
/dev/nvme0n1p1 vfat      256M   39M  218M  15% /boot/efi
tmpfs          tmpfs     1.5G  160K  1.5G   1% /run/user/1000
```

#### 🔌 Port & Service Sockets Status
- **Role:** Gateway Machine (No local PostgreSQL database or AriaBC Server running)

### 🖥️ Node 6: proposed-gw (`10.129.27.111`)

> [!CAUTION]
> **Node is offline or unreachable over SSH.**
> **Error:** `Command '['sshpass', '-p', 'clusterinfolab123', 'ssh', '-o', 'StrictHostKeyChecking=no', '-o', 'ConnectTimeout=8', '-p', '22', 'neel@10.129.27.111', 'python3', '-']' timed out after 13 seconds`

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
- **Available RAM:** 203.16 GB
- **Used RAM:** 48.15 GB (19.2%)
- **Swap Space:** Disabled / None

#### 💾 Storage & Disks
- **Root Mount Filesystem:** `/dev/nvme0n1p3` (ext4)
- **Root Disk Space:** 538G used / 1.2T free (Total: 1.8T, Use%: 31%)
- **Disk I/O Read Speed:** `0.24 MB/s` (Device: `nvme0n1`)
- **Disk I/O Write Speed:** `0.20 MB/s`
- **Disk Utilization:** `1.10%`
- **Average Disk Await Time:** `0.76 ms`
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
/dev/nvme0n1p3 ext4      1.8T  538G  1.2T  31% /
tmpfs          tmpfs     126G  1.9M  126G   1% /dev/shm
tmpfs          tmpfs     5.0M     0  5.0M   0% /run/lock
tmpfs          tmpfs     126G     0  126G   0% /run/qemu
/dev/nvme0n1p1 ext4      442M  215M  193M  53% /boot
/dev/nvme0n1p2 vfat      1.1G  6.2M  1.1G   1% /boot/efi
/dev/md0       ext4      5.5T  2.9T  2.3T  56% /backup
tmpfs          tmpfs      26G   96K   26G   1% /run/user/120
tmpfs          tmpfs      26G  108K   26G   1% /run/user/1005
tmpfs          tmpfs      26G   88K   26G   1% /run/user/1006
tmpfs          tmpfs      26G   80K   26G   1% /run/user/1001
tmpfs          tmpfs      26G   84K   26G   1% /run/user/1002
tmpfs          tmpfs      26G   84K   26G   1% /run/user/1004
tmpfs          tmpfs      26G   80K   26G   1% /run/user/1008
tmpfs          tmpfs      26G   80K   26G   1% /run/user/1007
```

#### 🔌 Port & Service Sockets Status
- **PostgreSQL DB Server (Port `5438`):** 🔴 Stopped
- **AriaBC Raft Client Server (Port `8000`):** 🔴 Stopped

