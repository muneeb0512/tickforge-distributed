#include "tickforge/live_source.hpp"
#include "tickforge/binance_trade_parser.hpp"

#include <boost/asio/connect.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/beast/core.hpp>

#include <algorithm>
#include <cctype>
#include <iostream>
#include <thread>

namespace tickforge {

namespace beast = boost::beast;
namespace http = boost::beast::http;
namespace websocket = boost::beast::websocket;
namespace net = boost::asio;
namespace ssl = boost::asio::ssl;
using tcp = boost::asio::ip::tcp;

LiveSource::LiveSource(LiveSourceConfig config) : config_(std::move(config)) {
    ssl_ctx_.set_default_verify_paths();
    ssl_ctx_.set_verify_mode(ssl::verify_peer);
}

LiveSource::~LiveSource() {
    stop();
}

void LiveSource::start() {
    // Connecting is lazy - it happens on the first next() call, on
    // whichever thread actually owns the read loop. start() only clears a
    // stale stop request left over from a previous stop()/start() cycle.
    stop_requested_ = false;
}

std::string LiveSource::buildTarget() const {
    std::string target = "/stream?streams=";
    for (std::size_t i = 0; i < config_.symbols.size(); ++i) {
        std::string symbol = config_.symbols[i];
        std::transform(symbol.begin(), symbol.end(), symbol.begin(),
                        [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
        if (i > 0) {
            target += '/';
        }
        target += symbol + "@trade";
    }
    return target;
}

bool LiveSource::connectOnce(std::string* error_out) {
    try {
        tcp::resolver resolver{ioc_};
        auto const results = resolver.resolve(config_.host, config_.port);

        WsStream local{ioc_, ssl_ctx_};

        // TLS SNI: without this, some TLS servers (including Binance)
        // won't know which certificate to present, and the handshake can
        // fail or silently present the wrong cert.
        if (!SSL_set_tlsext_host_name(local.next_layer().native_handle(), config_.host.c_str())) {
            if (error_out != nullptr) {
                *error_out = "failed to set TLS SNI hostname";
            }
            return false;
        }

        net::connect(local.next_layer().next_layer(), results);
        local.next_layer().handshake(ssl::stream_base::client);

        local.set_option(websocket::stream_base::decorator([](websocket::request_type& req) {
            req.set(http::field::user_agent, "tickforge-distributed/0.1 (educational project)");
        }));

        local.handshake(config_.host, buildTarget());

        std::lock_guard<std::mutex> lock(ws_mutex_);
        if (stop_requested_) {
            // stop() ran while we were mid-handshake; discard this
            // connection instead of publishing it for next() to use.
            return false;
        }
        ws_.emplace(std::move(local));
        connected_ = true;
        return true;
    } catch (const std::exception& e) {
        if (error_out != nullptr) {
            *error_out = e.what();
        }
        connected_ = false;
        return false;
    }
}

bool LiveSource::ensureConnected() {
    if (connected_) {
        return true;
    }

    auto backoff = config_.initial_backoff;
    while (!stop_requested_) {
        std::string error;
        if (connectOnce(&error)) {
            if (had_connected_once_) {
                ++reconnects_;
            }
            had_connected_once_ = true;
            return true;
        }
        if (stop_requested_) {
            return false;
        }
        std::cerr << "[LiveSource] connect failed: " << error << " - retrying in "
                  << backoff.count() << "ms\n";
        std::this_thread::sleep_for(backoff);
        backoff = std::min(backoff * 2, config_.max_backoff);
    }
    return false;
}

bool LiveSource::next(MarketEvent& out) {
    while (!stop_requested_) {
        if (!ensureConnected()) {
            return false; // stop() was requested while (re)connecting
        }

        WsStream* stream = nullptr;
        {
            std::lock_guard<std::mutex> lock(ws_mutex_);
            if (!ws_.has_value()) {
                connected_ = false;
                continue;
            }
            stream = &*ws_;
        }

        beast::flat_buffer buffer;
        boost::system::error_code ec;
        stream->read(buffer, ec); // blocks; interruptible by stop()'s force-close
        auto ingest_time = std::chrono::steady_clock::now();

        if (ec) {
            // Every disconnect cause looks the same here: Binance's 24h
            // forced close, a missed ping/pong, an ordinary network
            // interruption, or stop() force-closing the socket. next()
            // itself only distinguishes "stop() was requested" from
            // "something else went wrong, try reconnecting."
            connected_ = false;
            if (stop_requested_) {
                return false;
            }
            std::cerr << "[LiveSource] read failed: " << ec.message() << " - reconnecting\n";
            continue;
        }

        ++messages_received_;
        const std::string raw = beast::buffers_to_string(buffer.data());

        std::string parse_error;
        auto event = parseBinanceTradeMessage(raw, &parse_error);
        if (!event.has_value()) {
            ++messages_malformed_;
            std::cerr << "[LiveSource] malformed message: " << parse_error << "\n";
            continue; // one bad message costs one message, not the connection
        }

        event->ingest_time = ingest_time;
        event->source = "live:binance";
        out = *event;
        return true;
    }
    return false;
}

void LiveSource::stop() {
    stop_requested_ = true;
    std::lock_guard<std::mutex> lock(ws_mutex_);
    if (ws_.has_value()) {
        boost::system::error_code ec;
        // Force-close the raw TCP socket underneath the TLS/WS layers.
        // Makes any blocked read() in next(), on whatever thread owns the
        // ingestion loop, return with an error immediately instead of
        // waiting for a timeout or a server-initiated close.
        ws_->next_layer().next_layer().close(ec);
    }
}

} // namespace tickforge
