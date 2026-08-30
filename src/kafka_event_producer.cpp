#include "tickforge/kafka_event_producer.hpp"
#include "tickforge/market_event_json.hpp"

#include <librdkafka/rdkafka.h>
#include <nlohmann/json.hpp>

#include <iostream>
#include <stdexcept>

namespace tickforge {

void KafkaEventProducer::deliveryReportTrampoline(rd_kafka_t* rk, const rd_kafka_message_t* msg, void*) {
    // librdkafka only gives us a plain C function pointer for this
    // callback - `this` has to travel through the client handle's own
    // opaque slot (set once, below, via rd_kafka_conf_set_opaque) rather
    // than through a capturing lambda, which no C function-pointer type
    // could hold.
    auto* self = static_cast<KafkaEventProducer*>(rd_kafka_opaque(rk));
    if (msg->err) {
        ++self->delivery_failures_;
    } else {
        ++self->delivery_successes_;
    }
}

KafkaEventProducer::KafkaEventProducer(KafkaProducerConfig config) : config_(std::move(config)) {
    char errstr[512];
    rd_kafka_conf_t* conf = rd_kafka_conf_new();

    if (rd_kafka_conf_set(conf, "bootstrap.servers", config_.brokers.c_str(), errstr, sizeof(errstr)) !=
        RD_KAFKA_CONF_OK) {
        rd_kafka_conf_destroy(conf);
        throw std::runtime_error(std::string("KafkaEventProducer: ") + errstr);
    }

    rd_kafka_conf_set_opaque(conf, this);
    rd_kafka_conf_set_dr_msg_cb(conf, &KafkaEventProducer::deliveryReportTrampoline);

    // rd_kafka_new() takes ownership of conf on success - never destroy it
    // ourselves after this call succeeds. It also does NOT require the
    // broker to be reachable yet: unlike LiveSource's connectOnce()
    // (Milestone 1), librdkafka establishes and maintains broker
    // connections internally and asynchronously from here on.
    producer_ = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
    if (producer_ == nullptr) {
        rd_kafka_conf_destroy(conf);
        throw std::runtime_error(std::string("KafkaEventProducer: failed to create producer: ") + errstr);
    }
}

KafkaEventProducer::~KafkaEventProducer() {
    if (producer_ != nullptr) {
        flush(std::chrono::seconds(10));
        rd_kafka_destroy(producer_);
    }
}

bool KafkaEventProducer::publish(const MarketEvent& event) {
    const std::string payload = marketEventToJson(event).dump();

    const rd_kafka_resp_err_t err = rd_kafka_producev(
        producer_,
        RD_KAFKA_V_TOPIC(config_.topic.c_str()),
        RD_KAFKA_V_KEY(event.symbol.data(), event.symbol.size()),
        RD_KAFKA_V_VALUE(const_cast<char*>(payload.data()), payload.size()),
        // RD_KAFKA_MSG_F_COPY: librdkafka copies the payload into its own
        // buffer immediately, so `payload` (a local about to go out of
        // scope) doesn't need to outlive this call.
        RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
        RD_KAFKA_V_OPAQUE(nullptr),
        RD_KAFKA_V_END);

    // Services the delivery-report callback queue - without periodically
    // calling poll(), delivery reports for earlier messages would never
    // actually reach deliveryReportTrampoline().
    rd_kafka_poll(producer_, 0);

    if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
        std::cerr << "[KafkaEventProducer] produce failed: " << rd_kafka_err2str(err) << "\n";
        return false;
    }
    return true;
}

void KafkaEventProducer::flush(std::chrono::milliseconds timeout) {
    if (producer_ != nullptr) {
        rd_kafka_flush(producer_, static_cast<int>(timeout.count()));
    }
}

} // namespace tickforge
