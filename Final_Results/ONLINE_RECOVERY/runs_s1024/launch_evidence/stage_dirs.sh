set -e
B='@RBASE@'
test ! -e "$B"
mkdir -p "$B/repo" "$B/tools" "$B/rdkafka" "$B/command_audit"
cp -a /home/neel/Desktop/rdkafka_local/. "$B/rdkafka/"
cmake --version | head -1
pkg-config --modversion openssl zlib || true
ls /usr/include/openssl/ssl.h /usr/lib/x86_64-linux-gnu/libssl.so /usr/lib/x86_64-linux-gnu/libz.so
python3 -m venv "$B/venv"
