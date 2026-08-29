#pragma once

#include "tickforge/market_data_source.hpp"

#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ssl/context.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/ssl.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/beast/websocket/ssl.hpp>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

namespace tickforge {

struct LiveSourceConfig {
    std::string host = "stream.binance.com";
    std::string port = "9443";
    // Lowercase, per Binance's stream-name convention (the symbol field
    // inside received messages comes back uppercase regardless).
    std::vector<std::string> symbols{"btcusdt", "ethusdt"};
    std::chrono::milliseconds initial_backoff{1000};
    std::chrono::milliseconds max_backoff{30000};
};

// MarketDataSource backed by a real Binance combined-stream WebSocket
// connection. Owns the entire connection lifecycle - connect, TLS
// handshake, WS handshake, read loop, and reconnect-with-backoff - on
// whichever thread calls next(). Reconnection is transparent to the
// caller: next() only ever returns false because stop() was called, never
// because of a disconnect (see docs/milestone-1-ingestion.md).
//
// stop() is safe to call from a different thread than the one blocked in
// next(): it force-closes the underlying TCP socket, which makes a
// blocked read() fail immediately. This is the same technique Project 1's
// TcpServer::stop() used to unblock threads parked in recv()/send()
// (tickforge-cpp handoff §12/§14), applied through Beast's API instead of
// raw POSIX close().
class LiveSource : public MarketDataSource {
public:
    explicit LiveSource(LiveSourceConfig config);
    ~LiveSource() override;

    LiveSource(const LiveSource&) = delete;
    LiveSource& operator=(const LiveSource&) = delete;

    void start() override;
    void stop() override;
    bool next(MarketEvent& out) override;

    // First-pass observability (stdout only, this milestone). Structured
    // metrics export is Milestone 5's job, once these numbers matter to
    // something other than a human watching the terminal.
    std::uint64_t messagesReceived() const { return messages_received_; }
    std::uint64_t messagesMalformed() const { return messages_malformed_; }
    std::uint64_t reconnectCount() const { return reconnects_; }

private:
    using WsStream = boost::beast::websocket::stream<boost::beast::ssl_stream<boost::asio::ip::tcp::socket>>;

    std::string buildTarget() const;
    bool ensureConnected();
    bool connectOnce(std::string* error_out);

    LiveSourceConfig config_;
    std::atomic<bool> stop_requested_{false};
    std::atomic<bool> connected_{false};
    std::atomic<std::uint64_t> messages_received_{0};
    std::atomic<std::uint64_t> messages_malformed_{0};
    std::atomic<std::uint64_t> reconnects_{0};

    // Only ever read/written from the thread that calls next() - safe as
    // a plain bool because, unlike stop_requested_, nothing else touches
    // it (same reasoning as MarketDataPipeline::stopped_ in Project 1,
    // handoff §10).
    bool had_connected_once_ = false;

    boost::asio::io_context ioc_;
    boost::asio::ssl::context ssl_ctx_{boost::asio::ssl::context::tlsv12_client};

    // Guards ws_ itself (construction/replacement during connect or
    // reconnect) against stop() running concurrently on another thread.
    // Deliberately does NOT guard the blocking read() call in next() -
    // holding the lock across a blocking call would prevent stop() from
    // ever acquiring it while a read is in flight, defeating the entire
    // point of being interruptible.
    std::mutex ws_mutex_;
    std::optional<WsStream> ws_;
};

} // namespace tickforge
