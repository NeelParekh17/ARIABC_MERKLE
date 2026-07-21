# 🖥️ AriaBC 4-Node Cluster Inventory & Live Status

> **Last Updated:** `2026-07-05 06:33:35 +0530`
> **Automatic Update:** Yes (Updated via `scripts/distributed/update_nodes_info.py`)
> **Service Sockets Checked:** BCDB PostgreSQL (Port `5438`), AriaBC Server (Port `8000`/`8001`)

## 📊 Cluster Summary Table

| Node | Name | IP Address | Status | OS | CPU | RAM (Total/Avail) | Root Storage (Avail/Use%) | Disk Read MB/s | Disk Write MB/s | Disk Util % | Await | PostgreSQL | AriaBC Server |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| **Node 1** | admin123 | `10.129.148.247` | 🟢 Online | Ubuntu 24.04.3 LTS | 16 Cores | 15.02 GB / 13.99 GB (6.8% used) | 421G / 468G (6%) | 0.00 | 0.12 | 0.20% | 0.40 ms | 🔴 Stopped | 🔴 Stopped |
| **Node 2** | user4 | `10.129.148.246` | 🟢 Online | Ubuntu 22.04.2 LTS | 16 Cores | 15.01 GB / 12.09 GB (19.5% used) | 109G / 183G (38%) | 0.00 | 0.00 | 0.00% | 0.00 ms | 🔴 Stopped | 🔴 Stopped |
| **Node 4** | utkarsh | `10.129.148.248` | 🟢 Online | Ubuntu 24.04.3 LTS | 16 Cores | 15.02 GB / 13.87 GB (7.7% used) | 119G / 404G (70%) | 0.00 | 0.15 | 0.40% | 0.43 ms | 🔴 Stopped | 🔴 Stopped |
| **ASUS Laptop (GW)** | asus-laptop | `127.0.0.1` | 🟢 Online | Ubuntu 24.04.4 LTS | 16 Cores | 14.87 GB / 4.76 GB (68.0% used) | 25G / 164G (85%) | 0.00 | 0.80 | 0.00% | 0.40 ms | — | — |
| **Gateway 2 (Proposed)** | proposed-gw | `10.129.27.111` | 🟢 Online | Ubuntu 24.04.2 LTS | 16 Cores | 15.02 GB / 13.52 GB (10.0% used) | 211G / 457G (52%) | 0.00 | 0.13 | 0.80% | 1.40 ms | — | — |

## 🌐 Network Latency Matrix (RTT)

| From | To | RTT |
|---|---|---|
| admin123 (Node 1) | user4 (Node 2) | 0.564 ms |
| admin123 (Node 1) | utkarsh (Node 4) | 0.600 ms |
| admin123 (Node 1) | asus-laptop (Node 5) | 0.049 ms |
| admin123 (Node 1) | proposed-gw (Node 6) | 0.850 ms |
| user4 (Node 2) | admin123 (Node 1) | 0.562 ms |
| user4 (Node 2) | utkarsh (Node 4) | 0.475 ms |
| user4 (Node 2) | asus-laptop (Node 5) | 0.040 ms |
| user4 (Node 2) | proposed-gw (Node 6) | 0.518 ms |
| utkarsh (Node 4) | admin123 (Node 1) | 0.494 ms |
| utkarsh (Node 4) | user4 (Node 2) | 0.326 ms |
| utkarsh (Node 4) | asus-laptop (Node 5) | 0.051 ms |
| utkarsh (Node 4) | proposed-gw (Node 6) | 0.536 ms |
| asus-laptop (Node 5) | admin123 (Node 1) | 15.038 ms |
| asus-laptop (Node 5) | user4 (Node 2) | 2.299 ms |
| asus-laptop (Node 5) | utkarsh (Node 4) | 6.064 ms |
| asus-laptop (Node 5) | proposed-gw (Node 6) | 1.880 ms |
| proposed-gw (Node 6) | admin123 (Node 1) | 0.780 ms |
| proposed-gw (Node 6) | user4 (Node 2) | 0.695 ms |
| proposed-gw (Node 6) | utkarsh (Node 4) | 0.740 ms |
| proposed-gw (Node 6) | asus-laptop (Node 5) | 0.032 ms |

## 🔍 Detailed Node Inventory

### 🖥️ Node 1: admin123 (`10.129.148.247`)

#### ⚙️ System Specifications
- **Host/FQDN:** `Neel`
- **Operating System:** Ubuntu 24.04.3 LTS
- **Kernel Version:** `6.17.0-22-generic`
- **CPU Model:** `AMD Ryzen 7 5700G with Radeon Graphics`
- **CPU Logical Cores (Threads):** `16`
- **IPv4 Addresses:** `10.129.148.247 172.17.0.1`

#### 🧠 Memory (RAM) Allocation
- **Total RAM:** 15.02 GB
- **Available RAM:** 13.99 GB
- **Used RAM:** 1.03 GB (6.8%)
- **Swap Space:** 0.00 GB used of 4.00 GB total

#### 💾 Storage & Disks
- **Root Mount Filesystem:** `/dev/nvme0n1p2` (ext4)
- **Root Disk Space:** 24G used / 421G free (Total: 468G, Use%: 6%)
- **Disk I/O Read Speed:** `0.00 MB/s` (Device: `nvme0n1`)
- **Disk I/O Write Speed:** `0.12 MB/s`
- **Disk Utilization:** `0.20%`
- **Average Disk Await Time:** `0.40 ms`
- **Physical Disks / RAID Groups:**
- **nvme0n1**: INTEL SSDPEKNW512G8 (476.9G)

##### Selected `df -hT` output:
```text
Filesystem     Type      Size  Used Avail Use% Mounted on
tmpfs          tmpfs     1.6G  1.9M  1.5G   1% /run
/dev/nvme0n1p2 ext4      468G   24G  421G   6% /
tmpfs          tmpfs     7.6G     0  7.6G   0% /dev/shm
tmpfs          tmpfs     5.0M   12K  5.0M   1% /run/lock
efivarfs       efivarfs  128K   43K   81K  35% /sys/firmware/efi/efivars
/dev/nvme0n1p1 vfat      1.1G  6.2M  1.1G   1% /boot/efi
tmpfs          tmpfs     1.6G   92K  1.6G   1% /run/user/120
tmpfs          tmpfs     1.6G   80K  1.6G   1% /run/user/1003
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
- **IPv4 Addresses:** `10.129.148.246 172.17.0.1 172.18.0.1 172.19.0.1 172.21.0.1`

#### 🧠 Memory (RAM) Allocation
- **Total RAM:** 15.01 GB
- **Available RAM:** 12.09 GB
- **Used RAM:** 2.92 GB (19.5%)
- **Swap Space:** 0.00 GB used of 46.57 GB total

#### 💾 Storage & Disks
- **Root Mount Filesystem:** `/dev/nvme0n1p4` (ext4)
- **Root Disk Space:** 65G used / 109G free (Total: 183G, Use%: 38%)
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
/dev/nvme0n1p4 ext4      183G   65G  109G  38% /
tmpfs          tmpfs     7.6G   48K  7.6G   1% /dev/shm
tmpfs          tmpfs     5.0M  4.0K  5.0M   1% /run/lock
efivarfs       efivarfs  128K   33K   91K  27% /sys/firmware/efi/efivars
/dev/nvme0n1p2 ext4      921M  302M  556M  36% /boot
/dev/nvme0n1p1 vfat      952M  6.1M  946M   1% /boot/efi
/dev/nvme0n1p5 ext4      238G  155G   71G  69% /home
tmpfs          tmpfs     1.6G   92K  1.6G   1% /run/user/1004
tmpfs          tmpfs     1.6G   76K  1.6G   1% /run/user/127
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
- **Available RAM:** 13.87 GB
- **Used RAM:** 1.15 GB (7.7%)
- **Swap Space:** 0.00 GB used of 4.00 GB total

#### 💾 Storage & Disks
- **Root Mount Filesystem:** `/dev/nvme0n1p2` (ext4)
- **Root Disk Space:** 266G used / 119G free (Total: 404G, Use%: 70%)
- **Disk I/O Read Speed:** `0.00 MB/s` (Device: `nvme0n1`)
- **Disk I/O Write Speed:** `0.15 MB/s`
- **Disk Utilization:** `0.40%`
- **Average Disk Await Time:** `0.43 ms`
- **Physical Disks / RAID Groups:**
- **sda**: Samsung SSD 840 EVO 500GB (465.8G)
- **nvme0n1**: INTEL SSDPEKNW512G8 (476.9G)

##### Selected `df -hT` output:
```text
Filesystem     Type      Size  Used Avail Use% Mounted on
tmpfs          tmpfs     1.6G  2.5M  1.5G   1% /run
/dev/nvme0n1p2 ext4      404G  266G  119G  70% /
tmpfs          tmpfs     7.6G  1.3M  7.6G   1% /dev/shm
tmpfs          tmpfs     5.0M     0  5.0M   0% /run/lock
efivarfs       efivarfs  128K   42K   82K  34% /sys/firmware/efi/efivars
tmpfs          tmpfs     7.6G     0  7.6G   0% /run/qemu
/dev/nvme0n1p1 vfat      1.1G  6.2M  1.1G   1% /boot/efi
/dev/sda1      ext4      458G  226G  209G  53% /data
tmpfs          tmpfs     1.6G  104K  1.6G   1% /run/user/120
tmpfs          tmpfs     1.6G   92K  1.6G   1% /run/user/1003
```

#### 🔌 Port & Service Sockets Status
- **PostgreSQL DB Server (Port `5438`):** 🔴 Stopped
- **AriaBC Raft Client Server (Port `8001`):** 🔴 Stopped

### 🖥️ Node 5: asus-laptop (`127.0.0.1`)

#### ⚙️ System Specifications
- **Host/FQDN:** `neel-ASUS-TUF-Gaming-A15-FA507RE-FA577RE`
- **Operating System:** Ubuntu 24.04.4 LTS
- **Kernel Version:** `6.8.0-134-generic`
- **CPU Model:** `AMD Ryzen 7 6800H with Radeon Graphics`
- **CPU Logical Cores (Threads):** `16`
- **IPv4 Addresses:** `192.168.0.154 172.19.0.1 172.17.0.1 172.18.0.1`

#### 🧠 Memory (RAM) Allocation
- **Total RAM:** 14.87 GB
- **Available RAM:** 4.76 GB
- **Used RAM:** 10.11 GB (68.0%)
- **Swap Space:** 2.63 GB used of 3.81 GB total

#### 💾 Storage & Disks
- **Root Mount Filesystem:** `/dev/nvme0n1p4` (ext4)
- **Root Disk Space:** 132G used / 25G free (Total: 164G, Use%: 85%)
- **Disk I/O Read Speed:** `0.00 MB/s` (Device: `nvme0n1`)
- **Disk I/O Write Speed:** `0.80 MB/s`
- **Disk Utilization:** `0.00%`
- **Average Disk Await Time:** `0.40 ms`
- **Physical Disks / RAID Groups:**
- **nvme0n1**: INTEL SSDPEKNU512GZ (476.9G)

##### Selected `df -hT` output:
```text
Filesystem     Type      Size  Used Avail Use% Mounted on
tmpfs          tmpfs     1.5G  3.1M  1.5G   1% /run
/dev/nvme0n1p4 ext4      164G  132G   25G  85% /
tmpfs          tmpfs     7.5G  126M  7.4G   2% /dev/shm
tmpfs          tmpfs     5.0M   12K  5.0M   1% /run/lock
efivarfs       efivarfs  128K   60K   64K  49% /sys/firmware/efi/efivars
tmpfs          tmpfs     7.5G     0  7.5G   0% /run/qemu
/dev/nvme0n1p6 ext4       49G   40G  6.9G  86% /home
/dev/nvme0n1p1 vfat      256M   38M  219M  15% /boot/efi
tmpfs          tmpfs     1.5G  3.8M  1.5G   1% /run/user/1000
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
- **IPv4 Addresses:** `10.129.27.111`

#### 🧠 Memory (RAM) Allocation
- **Total RAM:** 15.02 GB
- **Available RAM:** 13.52 GB
- **Used RAM:** 1.50 GB (10.0%)
- **Swap Space:** 0.00 GB used of 4.00 GB total

#### 💾 Storage & Disks
- **Root Mount Filesystem:** `/dev/nvme0n1p2` (ext4)
- **Root Disk Space:** 223G used / 211G free (Total: 457G, Use%: 52%)
- **Disk I/O Read Speed:** `0.00 MB/s` (Device: `nvme0n1`)
- **Disk I/O Write Speed:** `0.13 MB/s`
- **Disk Utilization:** `0.80%`
- **Average Disk Await Time:** `1.40 ms`
- **Physical Disks / RAID Groups:**
- **nvme0n1**: CT500P2SSD8 (465.8G)

##### Selected `df -hT` output:
```text
Filesystem     Type      Size  Used Avail Use% Mounted on
tmpfs          tmpfs     1.6G  2.4M  1.5G   1% /run
/dev/nvme0n1p2 ext4      457G  223G  211G  52% /
tmpfs          tmpfs     7.6G  2.2M  7.6G   1% /dev/shm
tmpfs          tmpfs     5.0M   12K  5.0M   1% /run/lock
efivarfs       efivarfs  128K   42K   82K  34% /sys/firmware/efi/efivars
/dev/nvme0n1p1 vfat      1.1G  6.2M  1.1G   1% /boot/efi
tmpfs          tmpfs     1.6G  100K  1.6G   1% /run/user/121
tmpfs          tmpfs     1.6G   88K  1.6G   1% /run/user/1006
```

#### 🔌 Port & Service Sockets Status
- **Role:** Gateway Machine (No local PostgreSQL database or AriaBC Server running)

