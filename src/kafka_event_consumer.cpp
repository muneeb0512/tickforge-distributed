#include "tickforge/kafka_event_consumer.hpp"
#include "tickforge/market_event_json.hpp"

#include <librdkafka/rdkafka.h>
#include <nlohmann/json.hpp>

#include <chrono>
#include <iostream>
#include <stdexcept>
#include <utility>

namespace tickforge {

namespace {

void setConfOrThrow(rd_kafka_conf_t* conf, const char* key, const std::string& value) {
    char errstr[512];
    if (rd_kafka_conf_set(conf, key, value.c_str(), errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        rd_kafka_conf_destroy(conf);
        throw std::runtime_error(std::string("KafkaEventConsumer: ") + errstr);
    }
}

} // namespace

KafkaEventConsumer::KafkaEventConsumer(KafkaConsumerConfig config) : config_(std::move(config)) {
    rd_kafka_conf_t* conf = rd_kafka_conf_new();
    setConfOrThrow(conf, "bootstrap.servers", config_.brokers);
    setConfOrThrow(conf, "group.id", config_.group_id);
    setConfOrThrow(conf, "auto.offset.reset", config_.auto_offset_reset);

    char errstr[512];
    consumer_ = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
    if (consumer_ == nullptr) {
        rd_kafka_conf_destroy(conf);
        throw std::runtime_error(std::string("KafkaEventConsumer: failed to create consumer: ") + errstr);
    }
    // Required for the high-level, consumer-group-balanced API
    // (rd_kafka_subscribe / rd_kafka_consumer_poll) to work at all.
    rd_kafka_poll_set_consumer(consumer_);
}

KafkaEventConsumer::~KafkaEventConsumer() {
    stop();
    if (consumer_ != nullptr) {
        rd_kafka_consumer_close(consumer_);
        rd_kafka_destroy(consumer_);
    }
}

void KafkaEventConsumer::start() {
    rd_kafka_topic_partition_list_t* topics = rd_kafka_topic_partition_list_new(1);
    rd_kafka_topic_partition_list_add(topics, config_.topic.c_str(), RD_KAFKA_PARTITION_UA);
    const rd_kafka_resp_err_t err = rd_kafka_subscribe(consumer_, topics);
    rd_kafka_topic_partition_list_destroy(topics);
    if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
        throw std::runtime_error(std::string("KafkaEventConsumer: subscribe failed: ") + rd_kafka_err2str(err));
    }
}

void KafkaEventConsumer::stop() {
    stop_requested_ = true;
}

bool KafkaEventConsumer::next(ConsumedRecord& out) {
    while (!stop_requested_) {
        rd_kafka_message_t* msg = rd_kafka_consumer_poll(consumer_, 200 /* ms */);
        if (msg == nullptr) {
            continue; // plain poll timeout - not an error, just no message yet
        }

        if (msg->err) {
            if (msg->err != RD_KAFKA_RESP_ERR__PARTITION_EOF) {
                std::cerr << "[KafkaEventConsumer] consume error: " << rd_kafka_err2str(msg->err) << "\n";
            }
            rd_kafka_message_destroy(msg);
            continue;
        }

        const std::string payload(static_cast<const char*>(msg->payload), msg->len);
        const int partition = msg->partition;
        const std::int64_t offset = msg->offset;
        rd_kafka_message_destroy(msg);

        nlohmann::json parsed;
        try {
            parsed = nlohmann::json::parse(payload);
        } catch (const nlohmann::json::parse_error&) {
            ++messages_malformed_;
            continue; // corrupt/foreign message on the topic - skip, don't crash the consumer
        }
        auto event = marketEventFromJson(parsed);
        if (!event.has_value()) {
            ++messages_malformed_;
            continue;
        }
        event->ingest_time = std::chrono::steady_clock::now();

        ++messages_consumed_;
        out.event = std::move(*event);
        out.partition = partition;
        out.offset = offset;
        return true;
    }
    return false;
}

} // namespace tickforge
