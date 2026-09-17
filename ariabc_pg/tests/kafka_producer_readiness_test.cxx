#include "kafka_console.hxx"
#include <librdkafka/rdkafka.h>
#include <librdkafka/rdkafka_mock.h>

#include <cassert>
#include <chrono>
#include <iostream>
#include <string>

int main() {
    char error[512];
    rd_kafka_t* owner = rd_kafka_new(RD_KAFKA_PRODUCER, rd_kafka_conf_new(),
                                    error, sizeof(error));
    assert(owner);
    rd_kafka_mock_cluster_t* cluster = rd_kafka_mock_cluster_new(owner, 3);
    assert(cluster);
    const std::string bootstrap = rd_kafka_mock_cluster_bootstraps(cluster);
    assert(rd_kafka_brokers_add(owner, bootstrap.c_str()) == 3);
    const char* topic = "producer-readiness";
    assert(rd_kafka_mock_topic_create(cluster, topic, 3, 1) == RD_KAFKA_RESP_ERR_NO_ERROR);

    // A broker connection is insufficient: missing topic leaders must prevent
    // readiness, with a bounded failure and a reusable producer afterwards.
    assert(rd_kafka_mock_partition_set_leader(cluster, topic, 1, -1) ==
           RD_KAFKA_RESP_ERR_NO_ERROR);
    ariabc_pg::kafka_console_producer producer;
    std::string err;
    const auto begin = std::chrono::steady_clock::now();
    assert(!producer.start(bootstrap, topic,
                           ariabc_pg::kafka_producer_profile::result_fast, err));
    assert(!err.empty());
    assert(std::chrono::steady_clock::now() - begin < std::chrono::seconds(15));
    assert(!producer.send_payload("must-not-send", "key", err, 1));
    assert(producer.stats().send_calls == 0);

    assert(rd_kafka_mock_partition_set_leader(cluster, topic, 1, 2) ==
           RD_KAFKA_RESP_ERR_NO_ERROR);
    for (auto profile : {ariabc_pg::kafka_producer_profile::result_fast,
                         ariabc_pg::kafka_producer_profile::control_durable}) {
        assert(producer.start(bootstrap, topic, profile, err));
        assert(err.empty());
        const auto before = producer.stats();
        for (int partition = 0; partition < 3; ++partition) {
            // Startup must not publish probes or alter topic offsets.
            int64_t low = -1, high = -1;
            assert(rd_kafka_query_watermark_offsets(owner, topic, partition,
                                                     &low, &high, 5000) ==
                   RD_KAFKA_RESP_ERR_NO_ERROR);
            const int64_t expected =
                profile == ariabc_pg::kafka_producer_profile::result_fast ? 0 : 1;
            assert(high == expected);
            assert(producer.send_payload("real-result", "key", err, partition));
        }
        assert(producer.wait_for_delivery(5000, err));
        const auto after = producer.stats();
        assert(after.send_ok - before.send_ok == 3);
        assert(after.delivery_calls - before.delivery_calls == 3);
        assert(after.delivery_errors == 0);
        assert(after.delivery_pending_current == 0);
        producer.stop();
    }

    rd_kafka_mock_cluster_destroy(cluster);
    rd_kafka_destroy(owner);
    std::cout << "Kafka producer readiness PASS\n";
    return 0;
}
