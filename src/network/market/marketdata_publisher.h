#pragma once

#include "src/instrumentation/perf_utils.h"
#include "src/types.h"
#include "src/network/protocol/itch_encoder.h"
#include "QuantLink/Lib/concurrency/lf_queue.h"
#include "QuantLink/Lib/concurrency/thread_utils.h"
#include "QuantLink/Lib/logging/logger.h"
#include "QuantLink/Lib/network/udp_fanout_socket.h"
#include "QuantLink/Lib/protocol/itch_messages.h"
#include "src/network/market/snapshot_stream.h"

#include <atomic>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <thread>

using namespace quantlink;
using namespace nanoexchange;

class MarketDataPublisher {

  private:
    SPSCQueue<MEMarketUpdate>* marketUpdates_ = nullptr; // FROM matching engine
    SPSCQueue<SnapshotUpdate>  streamer_updates_;        // TO snapshot streamer

    quantlink::Logger*                logger_;
    std::unique_ptr<SnapshotStreamer> snapshotStreamer_;

    std::atomic<bool> run_{false};
    std::thread       thread_;
    int               streamer_core_ = -1;

    uint64_t next_seq_num_ = 1; // Incremental ITCH sequence number

    // Published-frame count, exported for monitoring. Kept separate from
    // next_seq_num_ so the hot path keeps its plain non-atomic increment.
    std::atomic<uint64_t> published_count_{0};

    // Feed gateway: clients register with a NEXSUB datagram on the incremental
    // port; every frame is unicast to each registered subscriber (emulates the
    // multicast group without kernel multicast routing).
    quantlink::UdpFanoutSocket incremental_socket_;

    inline void sendToIncrementalNetwork(const void* data, size_t len) noexcept {
        // seq_num prefix + ITCH payload as separate iovecs -> kernel assembles
        // the datagram; no intermediate combined buffer, no memcpy.
        const uint64_t seq_be = itch::swap64(next_seq_num_);
        incremental_socket_.fanout(&seq_be, sizeof(seq_be), data, len);
    }

    auto run() noexcept -> void {
        logger_->log("MarketDataPublisher: thread started.\n");

        while (run_.load(std::memory_order_acquire)) {
            incremental_socket_.pollRegistrations();
            incremental_socket_.sweepStale();

            const auto* update = marketUpdates_->getNextRead();
            if (!update)
                continue;

            NANOEXCHANGE_TIMESTAMP(T5_MarketDataPublisher_LFQueue_read, *logger_);

            // Encode ONCE per update; the same bytes go to UDP and to queue 2
            SnapshotUpdate snap(*update, next_seq_num_);
            snap.len_ = encode(*update, snap.bytes_);
            if (snap.len_ > 0) {
                NANOEXCHANGE_START_MEASURE(Exchange_UdpFanoutSocket_fanout);
                sendToIncrementalNetwork(snap.bytes_, snap.len_);
                NANOEXCHANGE_END_MEASURE(Exchange_UdpFanoutSocket_fanout, *logger_);
                NANOEXCHANGE_TIMESTAMP(T6_MarketDataPublisher_UDP_write, *logger_);
                streamer_updates_.pushBlocking(snap);
            }

            marketUpdates_->updateNextRead();
            ++next_seq_num_;
            published_count_.store(next_seq_num_ - 1, std::memory_order_relaxed);
        }

        logger_->log("MarketDataPublisher: thread exited safely.\n");
    }

  public:
    MarketDataPublisher(SPSCQueue<MEMarketUpdate>* marketUpdates, quantlink::Logger* logger, size_t maxTickers,
                        const std::string& iface, int incremental_port, int snapshot_tcp_port,
                        size_t snapshotQueueSize = 1024, int streamer_core = -1)
        : marketUpdates_(marketUpdates),
          streamer_updates_(snapshotQueueSize),
          logger_(logger),
          streamer_core_(streamer_core),
          incremental_socket_(*logger) {
        ASSERT(incremental_socket_.init(incremental_port, iface) >= 0,
               "MarketDataPublisher: failed to init incremental feed gateway socket");
        logger_->log("MarketDataPublisher: incremental feed gateway on :%d "
                     "(register via NEXSUB, per-subscriber unicast fan-out)\n",
                     incremental_port);

        snapshotStreamer_ =
            std::make_unique<SnapshotStreamer>(&streamer_updates_, logger_, maxTickers, snapshot_tcp_port);
    }

    auto start(int core_id = -1) -> void {
        run_.store(true, std::memory_order_release);
        snapshotStreamer_->start(streamer_core_);
        thread_ = quantlink::utils::create_and_pin_thread(core_id, "MarketDataPublisher", [this]() { run(); });
    }

    auto stop() -> void {
        run_.store(false, std::memory_order_release);
        if (thread_.joinable())
            thread_.join();
        snapshotStreamer_->stop();
    }

    // Number of incremental frames published so far.
    [[nodiscard]] auto publishedCount() const noexcept -> uint64_t {
        return published_count_.load(std::memory_order_relaxed);
    }

    ~MarketDataPublisher() {
        stop();
        // snapshotStreamer_ auto-deleted via unique_ptr
    }
};
