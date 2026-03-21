#pragma once

#include "../types.h"
#include "QuantLink/Lib/concurrency/lf_queue.h"
#include "QuantLink/Lib/logging/logger.h"
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <unordered_map>
#include <vector>

using namespace quantlink;

class SnapshotStreamer {

private:
    quantlink::SPSCQueue<MEMarketUpdate>* snapshotUpdates_ = nullptr;   
    quantlink::Logger* logger_;
    std::atomic<bool> run_{false};
    std::vector<std::unordered_map<OrderId, MEMarketUpdate>> ticker_orders_;
    size_t MAX_TICKERS_;

    // TODO : SOCKETS

    uint64_t last_sequence_number_ = 0;


    inline void sendToSnapshotNetwork(const void* data, size_t size) {
        // TODO: UDP Multicast sendto(...)
    }


public:
    SnapshotStreamer (SPSCQueue<MEMarketUpdate>* snapshotUpdates, Logger* logger, size_t tickers_size ) : 
                    snapshotUpdates_(snapshotUpdates),
                    logger_(logger),
                    MAX_TICKERS_(tickers_size)
    {
        ticker_orders_.resize(MAX_TICKERS_);
    }

    auto start () {
        run_.store(true, std::memory_order_release);
        //create the thread and keep pop
    }

    auto stop () {
        run_.store(false, std::memory_order_release);
    }

    auto addToSnapshot (const MEMarketUpdate* update) {
        TickerId ticker = update->ticker_id_;

        switch (update->type_) {
            case UpdateType::ADD : {
                ticker_orders_[ticker][update->market_order_id_] = *update;
                break;
            }

            case UpdateType::CANCEL : {
                ticker_orders_[ticker].erase(update->market_order_id_);
                break;
            }

            case UpdateType::MODIFY : {
                ticker_orders_[ticker][update->market_order_id_] = *update;
                break;
            }

            case UpdateType::TRADE : {
                auto& order_map = ticker_orders_[ticker];
                auto it = order_map.find(update->market_order_id_);
                
                if (it != order_map.end()) {
                    if (it->second.qty_ <= update->qty_) {
                        order_map.erase(it); 
                    } 
                    else it->second.qty_ -= update->qty_; 
                } 
                else {
                    logger_->log("CRITICAL ERROR: Received TRADE for unknown OrderID: %\n", 
                                 update->market_order_id_);
                }
                break;
            }

            default: {}
        }
    }


    auto publishSnapshot () {
        logger_->log("Publishing Snapshot up to SeqNum: %\n", last_sequence_number_);
        MEMarketUpdate start_msg{};
        start_msg.market_order_id_ = last_sequence_number_;
        start_msg.type_ = UpdateType::SNAPSHOT_START;

        sendToSnapshotNetwork(&start_msg, sizeof(MEMarketUpdate));

        for (size_t ticker_id = 0; ticker_id < MAX_TICKERS_; ticker_id++) {
            const auto& map = ticker_orders_[ticker_id];

            MEMarketUpdate clear_msg{};
            clear_msg.type_ = UpdateType::CLEAR;
            clear_msg.ticker_id_ = ticker_id;

            sendToSnapshotNetwork(&clear_msg, sizeof(MEMarketUpdate));

            for (const auto& [orderId, restingOrder] : map){
                MEMarketUpdate snapshot_msg = restingOrder;
                snapshot_msg.type_ = UpdateType::ADD;

                sendToSnapshotNetwork(&snapshot_msg, sizeof(MEMarketUpdate));
            }
        }

        MEMarketUpdate end_msg{};
        end_msg.type_ = UpdateType::SNAPSHOT_END;
        sendToSnapshotNetwork(&end_msg, sizeof(MEMarketUpdate));
    }
};