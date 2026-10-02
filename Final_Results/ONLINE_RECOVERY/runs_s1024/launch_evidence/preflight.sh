set -e
hostname
cat /etc/os-release
free -m
df -h /home/neel/Desktop
for t in gcc g++ cmake bison flex rsync python3 sshpass; do command -v "$t" || true; done
ss -ltn | awk '$4 ~ /:(5448|9018|8018|8019|9092)$/ {print}'
ls -ld /home/neel/Desktop/rdkafka_local /tmp/cmake-3.28.3-linux-x86_64 /home/neel/Desktop/kafka_2.13-3.7.0 /tmp/librdkafka-v2.3.0.tar.gz 2>/dev/null || true
ssh -o BatchMode=yes -o ConnectTimeout=8 neel@10.129.148.248 true
