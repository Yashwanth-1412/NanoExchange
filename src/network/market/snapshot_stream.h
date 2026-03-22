#pragma once

#include "../../types.h"
#include "QuantLink/Lib/concurrency/lf_queue.h"
#include "QuantLink/Lib/concurrency/thread_utils.h"
#include "QuantLink/Lib/logging/logger.h"
#include "QuantLink/Lib/logging/time_utils.h"
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <thread>
#include <unordered_map>
#include <vector>

using namespace quantlink;

constexpr uint64_t SNAPSHOT_INTERVAL_NS = 60'000'000'000ULL;  

class SnapshotStreamer {

private:
    SPSCQueue<SnapshotUpdate>* snapshotUpdates_ = nullptr;
    quantlink::Logger* logger_;

    std::atomic<bool> run_{false};
    std::thread thread_;

    std::vector<std::unordered_map<OrderId, MEMarketUpdate>> ticker_orders_;
    size_t   maxTickers_;

    uint64_t last_seq_num_     = 0;    
    uint64_t last_snapshot_ns_ = 0;    

    // TODO: snapshot UDP multicast socket

    inline void sendToSnapshotNetwork(const void* data, size_t size) {
        // TODO: UDP Multicast sendto(...) on snapshot stream
        (void)data; (void)size;
    }

    auto run() noexcept -> void {
        logger_->log("SnapshotStreamer: thread started.\n");

        while (run_.load(std::memory_order_acquire)) {

            while (true) {
                const auto* snap = snapshotUpdates_->getNextRead();
                if (!snap) break;

                addToSnapshot(&snap->update_);
                last_seq_num_ = snap->seq_num_;

                snapshotUpdates_->updateNextRead();
            }

            uint64_t now = quantlink::utils::get_current_epoch_nanos();
            if (UNLIKELY(now - last_snapshot_ns_ >= SNAPSHOT_INTERVAL_NS)) {
                publishSnapshot();
                last_snapshot_ns_ = now;
            }
        }

        logger_->log("SnapshotStreamer: thread exited safely.\n");
    }


public:

    SnapshotStreamer(SPSCQueue<SnapshotUpdate>* snapshotUpdates, quantlink::Logger* logger, size_t maxTickers) :
        snapshotUpdates_(snapshotUpdates),
        logger_(logger),
        maxTickers_(maxTickers)
    {
        ticker_orders_.resize(maxTickers_);
    }

    auto start(int core_id = -1) -> void {
        run_.store(true, std::memory_order_release);
        thread_ = quantlink::utils::create_and_pin_thread(core_id, "SnapshotStreamer", [this]() { run(); });
    }

    auto stop() -> void {
        run_.store(false, std::memory_order_release);
        if (thread_.joinable()) thread_.join();
    }

    ~SnapshotStreamer() {
        stop();
    }

    auto addToSnapshot(const MEMarketUpdate* update) -> void {
        TickerId ticker = update->ticker_id_;

        switch (update->type_) {

            case UpdateType::ADD : {
                ticker_orders_[ticker][update->market_order_id_] = *update;
                break;
            }
            break;
            
            case UpdateType::CANCEL : {
                ticker_orders_[ticker].erase(update->market_order_id_);
                break;
            }
            break;

            case UpdateType::MODIFY : {
                ticker_orders_[ticker][update->market_order_id_] = *update;
                break;
            }
            break;

            case UpdateType::TRADE : {
                auto& order_map = ticker_orders_[ticker];
                auto  it        = order_map.find(update->market_order_id_);

                if (it != order_map.end()) {
                    if (it->second.qty_ <= update->qty_) {
                        order_map.erase(it);                // Fully filled — remove
                    }
                    else it->second.qty_ -= update->qty_;   // Partially filled — reduce qty
                }
                else {
                    logger_->log("CRITICAL ERROR: Received TRADE for unknown OrderID: %\n",
                                 update->market_order_id_);
                }
                break;
            }

            case UpdateType::SNAPSHOT_START:
            case UpdateType::SNAPSHOT_END:
            case UpdateType::CLEAR:
            default: break;
        }
    }

    auto publishSnapshot() -> void {
        logger_->log("SnapshotStreamer: Publishing snapshot up to SeqNum: %\n", last_seq_num_);

        MEMarketUpdate start_msg{};
        start_msg.market_order_id_ = last_seq_num_;
        start_msg.type_            = UpdateType::SNAPSHOT_START;
        sendToSnapshotNetwork(&start_msg, sizeof(MEMarketUpdate));

        for (size_t ticker_id = 0; ticker_id < maxTickers_; ticker_id++) {
            const auto& order_map = ticker_orders_[ticker_id];

            MEMarketUpdate clear_msg{};
            clear_msg.type_      = UpdateType::CLEAR;
            clear_msg.ticker_id_ = ticker_id;
            sendToSnapshotNetwork(&clear_msg, sizeof(MEMarketUpdate));

            for (const auto& [orderId, restingOrder] : order_map) {
                MEMarketUpdate snapshot_msg = restingOrder;
                snapshot_msg.type_          = UpdateType::ADD;
                sendToSnapshotNetwork(&snapshot_msg, sizeof(MEMarketUpdate));
            }
        }

        MEMarketUpdate end_msg{};
        end_msg.market_order_id_ = last_seq_num_;
        end_msg.type_            = UpdateType::SNAPSHOT_END;
        sendToSnapshotNetwork(&end_msg, sizeof(MEMarketUpdate));

        logger_->log("SnapshotStreamer: Snapshot published.\n");
    }

};