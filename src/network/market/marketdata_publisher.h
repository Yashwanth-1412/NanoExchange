#pragma once

#include "../../types.h"
#include "snapshot_stream.h"
#include "../protocol/itch_encoder.h"
#include "QuantLink/Lib/concurrency/lf_queue.h"
#include "QuantLink/Lib/concurrency/thread_utils.h"
#include "QuantLink/Lib/logging/logger.h"
#include <atomic>
#include <cstdint>
#include <thread>

using namespace quantlink;

class MarketDataPublisher {

private:
    SPSCQueue<MEMarketUpdate>* marketUpdates_ = nullptr;   // FROM matching engine
    SPSCQueue<SnapshotUpdate> snapshotUpdates_;             // TO snapshot streamer

    quantlink::Logger* logger_;
    SnapshotStreamer* snapshotStreamer_ = nullptr;

    std::atomic<bool> run_{false};
    std::thread thread_;

    uint64_t next_seq_num_ = 1;   // Incremental ITCH sequence number
    uint64_t next_match_num_ = 1;   // Match number for OrderExecuted messages

    // TODO: incremental UDP multicast socket

    inline void sendToIncrementalNetwork(const void* data, size_t size) {
        // TODO: UDP Multicast sendto(...) on incremental stream
        (void)data; (void)size;
    }

    auto run() noexcept -> void {
        logger_->log("MarketDataPublisher: thread started.\n");

        alignas(8) char buf[ITCH_MAX_MSG_SIZE];

        while (run_.load(std::memory_order_acquire)) {

            const auto* update = marketUpdates_->getNextRead();
            if (!update) continue;

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
        }

        logger_->log("MarketDataPublisher: thread exited safely.\n");
    }


public:

    MarketDataPublisher(SPSCQueue<MEMarketUpdate>* marketUpdates,
                        quantlink::Logger* logger,
                        size_t maxTickers,
                        size_t snapshotQueueSize = 1024) :
        marketUpdates_(marketUpdates),
        snapshotUpdates_(snapshotQueueSize),
        logger_(logger)
    {
        snapshotStreamer_ = new SnapshotStreamer(&snapshotUpdates_, logger_, maxTickers);
    }

    auto start() -> void {
        run_.store(true, std::memory_order_release);
        snapshotStreamer_->start();
        thread_ = quantlink::utils::create_and_pin_thread(-1, "MarketDataPublisher", [this]() { run(); });
    }

    auto stop() -> void {
        run_.store(false, std::memory_order_release);
        if (thread_.joinable()) thread_.join();
        snapshotStreamer_->stop();
    }

    ~MarketDataPublisher() {
        stop();
        delete snapshotStreamer_;
        snapshotStreamer_ = nullptr;
    }

};