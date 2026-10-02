set -e
hostname
free -m
df -h /home/neel/Desktop
ss -ltn | awk '$4 ~ /:(5448|9018|8018|8019|9092)$/ {print}'
