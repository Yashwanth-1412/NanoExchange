#pragma once

#include "../../types.h"
#include "snapshot_stream.h"
#include "../protocol/itch_encoder.h"
#include "QuantLink/Lib/concurrency/lf_queue.h"
#include "QuantLink/Lib/concurrency/thread_utils.h"
#include "QuantLink/Lib/logging/logger.h"
#include "QuantLink/Lib/network/mcast_socket.h"
#include <atomic>
#include <cstdint>
#include <string>
#include <thread>
#include <memory>

using namespace quantlink;

class MarketDataPublisher {

private:
    SPSCQueue<MEMarketUpdate>* marketUpdates_ = nullptr;   // FROM matching engine
    SPSCQueue<SnapshotUpdate> snapshotUpdates_;             // TO snapshot streamer

    quantlink::Logger* logger_;
    std::unique_ptr<SnapshotStreamer> snapshotStreamer_;

    std::atomic<bool> run_{false};
    std::thread thread_;

    uint64_t next_seq_num_ = 1;   // Incremental ITCH sequence number
    uint64_t next_match_num_ = 1;   // Match number for OrderExecuted messages

    quantlink::McastSocket incremental_socket_;   // Incremental UDP multicast socket

    inline void sendToIncrementalNetwork(const void* data, size_t len) noexcept {
        incremental_socket_.send(&next_seq_num_, sizeof(next_seq_num_));   // seq_num prefix
        incremental_socket_.send(data, len);                               // ITCH payload
    }

    auto run() noexcept -> void {
        logger_->log("MarketDataPublisher: thread started.\n");

        alignas(8) char buf[ITCH_MAX_MSG_SIZE];

        while (run_.load(std::memory_order_acquire)) {

            const auto* update = marketUpdates_->getNextRead();
            if (!update) {
                // No updates — still flush any pending outbound data
                incremental_socket_.sendAndRecv();
                continue;
            }

            logger_->log("MarketDataPublisher: seq=% Ticker=% Side=% Px=% Qty=% Type=%\n",
                next_seq_num_,
                update->ticker_id_,
                static_cast<int>(update->side_),
                update->price_,
                update->qty_,
                static_cast<int>(update->type_)
            );

            size_t len = encode(*update, next_match_num_, buf);
            if (len > 0) sendToIncrementalNetwork(buf, len);

            if (update->type_ == UpdateType::TRADE) next_match_num_++;

            snapshotUpdates_.push(SnapshotUpdate{*update, next_seq_num_});

            marketUpdates_->updateNextRead();
            ++next_seq_num_;

            // Flush encoded ITCH message to the wire
            incremental_socket_.sendAndRecv();
        }

        logger_->log("MarketDataPublisher: thread exited safely.\n");
    }


public:

    MarketDataPublisher(SPSCQueue<MEMarketUpdate>* marketUpdates,
                         quantlink::Logger* logger,
                         size_t maxTickers,
                         const std::string& incremental_ip,
                         const std::string& iface,
                         int incremental_port,
                         int snapshot_tcp_port,
                         size_t snapshotQueueSize = 1024) :
        marketUpdates_(marketUpdates),
        snapshotUpdates_(snapshotQueueSize),
        logger_(logger),
        incremental_socket_(*logger)
    {
        ASSERT(incremental_socket_.init(incremental_ip, iface, incremental_port, false) >= 0,
               "MarketDataPublisher: failed to init incremental multicast socket");

        snapshotStreamer_ = std::make_unique<SnapshotStreamer>(&snapshotUpdates_, logger_, maxTickers,
                                                               snapshot_tcp_port);
    }

    auto start(int core_id = -1) -> void {
        run_.store(true, std::memory_order_release);
        snapshotStreamer_->start();
        thread_ = quantlink::utils::create_and_pin_thread(core_id, "MarketDataPublisher", [this]() { run(); });
    }

    auto stop() -> void {
        run_.store(false, std::memory_order_release);
        if (thread_.joinable()) thread_.join();
        snapshotStreamer_->stop();
    }

    ~MarketDataPublisher() {
        stop();
        // snapshotStreamer_ auto-deleted via unique_ptr
    }

};
