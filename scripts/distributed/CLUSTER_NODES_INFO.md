# 🖥️ AriaBC 4-Node Cluster Inventory & Live Status

> **Last Updated:** `2026-06-23 18:23:08 +0530`  
> **Automatic Update:** Yes (Updated via `scripts/distributed/update_nodes_info.py`)  
> **Service Sockets Checked:** BCDB PostgreSQL (Port `5438`), AriaBC Server (Port `8000`/`8001`)  

## 📊 Cluster Summary Table

| Node | Name | IP Address | Status | OS | CPU | RAM (Total/Avail) | Root Storage (Avail/Use%) | Disk Read MB/s | Disk Write MB/s | Disk Util % | Await | PostgreSQL | AriaBC Server |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| **Node 1** | admin123 | `10.129.148.236` | 🟢 Online | Ubuntu 24.04.3 LTS | 16 Cores | 15.02 GB / 7.13 GB (52.5% used) | 424G / 468G (5%) | 0.00 | 0.25 | 0.90% | 0.33 ms | 🟢 Running | 🔴 Stopped |
| **Node 2** | user4 | `10.129.27.54` | 🟢 Online | Ubuntu 22.04.2 LTS | 16 Cores | 15.01 GB / 1.11 GB (92.6% used) | 111G / 183G (37%) | 0.02 | 0.11 | 0.40% | 0.83 ms | 🟢 Running | 🔴 Stopped |
| **Node 4** | utkarsh | `10.129.148.248` | 🟢 Online | Ubuntu 24.04.3 LTS | 16 Cores | 15.02 GB / 2.47 GB (83.6% used) | 129G / 404G (67%) | 0.00 | 0.01 | 0.00% | 0.00 ms | 🟢 Running | 🔴 Stopped |

## 🌐 Network Latency Matrix (RTT)

| From | To | RTT |
|---|---|---|
| admin123 (Node 1) | user4 (Node 2) | 0.372 ms |
| admin123 (Node 1) | utkarsh (Node 4) | 0.522 ms |
| user4 (Node 2) | admin123 (Node 1) | 0.564 ms |
| user4 (Node 2) | utkarsh (Node 4) | 0.386 ms |
| utkarsh (Node 4) | admin123 (Node 1) | 0.546 ms |
| utkarsh (Node 4) | user4 (Node 2) | 0.332 ms |

## 🔍 Detailed Node Inventory

### 🖥️ Node 1: admin123 (`10.129.148.236`)

#### ⚙️ System Specifications
- **Host/FQDN:** `Neel`
- **Operating System:** Ubuntu 24.04.3 LTS
- **Kernel Version:** `6.17.0-22-generic`
- **CPU Model:** `AMD Ryzen 7 5700G with Radeon Graphics`
- **CPU Logical Cores (Threads):** `16`
- **IPv4 Addresses:** `10.129.148.236 172.17.0.1`

#### 🧠 Memory (RAM) Allocation
- **Total RAM:** 15.02 GB
- **Available RAM:** 7.13 GB
- **Used RAM:** 7.88 GB (52.5%)
- **Swap Space:** 0.00 GB used of 4.00 GB total

#### 💾 Storage & Disks
- **Root Mount Filesystem:** `/dev/nvme0n1p2` (ext4)
- **Root Disk Space:** 20G used / 424G free (Total: 468G, Use%: 5%)
- **Disk I/O Read Speed:** `0.00 MB/s` (Device: `nvme0n1`)
- **Disk I/O Write Speed:** `0.25 MB/s`
- **Disk Utilization:** `0.90%`
- **Average Disk Await Time:** `0.33 ms`
- **Physical Disks / RAID Groups:**
- **nvme0n1**: INTEL SSDPEKNW512G8 (476.9G)

##### Selected `df -hT` output:
```text
Filesystem     Type      Size  Used Avail Use% Mounted on
tmpfs          tmpfs     1.6G  1.9M  1.5G   1% /run
/dev/nvme0n1p2 ext4      468G   20G  424G   5% /
tmpfs          tmpfs     7.6G   44K  7.6G   1% /dev/shm
tmpfs          tmpfs     5.0M   12K  5.0M   1% /run/lock
efivarfs       efivarfs  128K   43K   81K  35% /sys/firmware/efi/efivars
/dev/nvme0n1p1 vfat      1.1G  6.2M  1.1G   1% /boot/efi
tmpfs          tmpfs     1.6G   96K  1.6G   1% /run/user/120
tmpfs          tmpfs     1.6G   80K  1.6G   1% /run/user/1003
```

#### 🔌 Port & Service Sockets Status
- **PostgreSQL DB Server (Port `5438`):** 🟢 Running (Accepting connections)
- **AriaBC Raft Client Server (Port `8000`):** 🔴 Stopped

### 🖥️ Node 2: user4 (`10.129.27.54`)

#### ⚙️ System Specifications
- **Host/FQDN:** `user4-MS-7C96`
- **Operating System:** Ubuntu 22.04.2 LTS
- **Kernel Version:** `6.8.0-101-generic`
- **CPU Model:** `AMD Ryzen 7 5700G with Radeon Graphics`
- **CPU Logical Cores (Threads):** `16`
- **IPv4 Addresses:** `10.129.27.54 172.19.0.1 172.21.0.1 172.17.0.1 172.18.0.1`

#### 🧠 Memory (RAM) Allocation
- **Total RAM:** 15.01 GB
- **Available RAM:** 1.11 GB
- **Used RAM:** 13.91 GB (92.6%)
- **Swap Space:** 5.24 GB used of 46.57 GB total

#### 💾 Storage & Disks
- **Root Mount Filesystem:** `/dev/nvme0n1p4` (ext4)
- **Root Disk Space:** 63G used / 111G free (Total: 183G, Use%: 37%)
- **Disk I/O Read Speed:** `0.02 MB/s` (Device: `nvme0n1`)
- **Disk I/O Write Speed:** `0.11 MB/s`
- **Disk Utilization:** `0.40%`
- **Average Disk Await Time:** `0.83 ms`
- **Physical Disks / RAID Groups:**
- **nvme0n1**: INTEL SSDPEKNW512G8 (476.9G)

##### Selected `df -hT` output:
```text
Filesystem     Type      Size  Used Avail Use% Mounted on
tmpfs          tmpfs     1.6G  3.5M  1.5G   1% /run
/dev/nvme0n1p4 ext4      183G   63G  111G  37% /
tmpfs          tmpfs     7.6G   30M  7.5G   1% /dev/shm
tmpfs          tmpfs     5.0M  4.0K  5.0M   1% /run/lock
efivarfs       efivarfs  128K   33K   91K  27% /sys/firmware/efi/efivars
/dev/nvme0n1p2 ext4      921M  302M  556M  36% /boot
/dev/nvme0n1p1 vfat      952M  6.1M  946M   1% /boot/efi
/dev/nvme0n1p5 ext4      238G  135G   91G  60% /home
tmpfs          tmpfs     1.6G  108K  1.6G   1% /run/user/1004
tmpfs          tmpfs     1.6G  116K  1.6G   1% /run/user/1001
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
- **Available RAM:** 2.47 GB
- **Used RAM:** 12.55 GB (83.6%)
- **Swap Space:** 1.42 GB used of 4.00 GB total

#### 💾 Storage & Disks
- **Root Mount Filesystem:** `/dev/nvme0n1p2` (ext4)
- **Root Disk Space:** 256G used / 129G free (Total: 404G, Use%: 67%)
- **Disk I/O Read Speed:** `0.00 MB/s` (Device: `nvme0n1`)
- **Disk I/O Write Speed:** `0.01 MB/s`
- **Disk Utilization:** `0.00%`
- **Average Disk Await Time:** `0.00 ms`
- **Physical Disks / RAID Groups:**
- **sda**: Samsung SSD 840 EVO 500GB (465.8G)
- **nvme0n1**: INTEL SSDPEKNW512G8 (476.9G)

##### Selected `df -hT` output:
```text
Filesystem     Type      Size  Used Avail Use% Mounted on
tmpfs          tmpfs     1.6G   27M  1.5G   2% /run
/dev/nvme0n1p2 ext4      404G  256G  129G  67% /
tmpfs          tmpfs     7.6G   11M  7.5G   1% /dev/shm
tmpfs          tmpfs     5.0M     0  5.0M   0% /run/lock
efivarfs       efivarfs  128K   42K   82K  34% /sys/firmware/efi/efivars
tmpfs          tmpfs     7.6G     0  7.6G   0% /run/qemu
/dev/sda1      ext4      458G  226G  209G  53% /data
/dev/nvme0n1p1 vfat      1.1G  6.2M  1.1G   1% /boot/efi
tmpfs          tmpfs     1.6G  2.6M  1.5G   1% /run/user/1001
tmpfs          tmpfs     1.6G  152K  1.6G   1% /run/user/1003
tmpfs          tmpfs     1.6G  164K  1.6G   1% /run/user/1000
tmpfs          tmpfs     1.6G   92K  1.6G   1% /run/user/1002
```

#### 🔌 Port & Service Sockets Status
- **PostgreSQL DB Server (Port `5438`):** 🟢 Running (Accepting connections)
- **AriaBC Raft Client Server (Port `8001`):** 🔴 Stopped

