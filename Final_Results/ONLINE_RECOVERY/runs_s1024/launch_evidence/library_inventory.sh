set -e
B='@RBASE@'
grep -n '#define RD_KAFKA_VERSION ' "$B/rdkafka/include/librdkafka/rdkafka.h"
sha256sum "$B/rdkafka/lib/librdkafka.so.1"
cat "$B/artifacts/"*/build.env
