#pragma once

#include "../../types.h"
#include "../protocol/itch_encoder.h"
#include "QuantLink/Lib/concurrency/lf_queue.h"
#include "QuantLink/Lib/concurrency/thread_utils.h"
#include "QuantLink/Lib/logging/logger.h"
#include "QuantLink/Lib/logging/time_utils.h"
#include "QuantLink/Lib/network/mcast_socket.h"
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

using namespace quantlink;
using namespace quantlink::itch;

constexpr uint64_t SNAPSHOT_INTERVAL_NS = 10'000'000'000ULL;


constexpr uint16_t SNAP_CTRL_START = 0xBBBB;
constexpr uint16_t SNAP_CTRL_CLEAR = 0xCCCC;
constexpr uint16_t SNAP_CTRL_END   = 0xEEEE;

class SnapshotStreamer {

private:
    SPSCQueue<SnapshotUpdate>* snapshotUpdates_ = nullptr;
    quantlink::Logger* logger_;

    std::atomic<bool> run_{false};
    std::thread thread_;

    std::vector<std::unordered_map<OrderId, MEMarketUpdate>> ticker_orders_;
    size_t   maxTickers_;

    uint64_t last_seq_num_ = 0;
    uint64_t last_snapshot_ns_ = 0;

    quantlink::McastSocket snapshot_socket_;


    inline void sendToSnapshotNetwork(const void* data, size_t size) {
        snapshot_socket_.send(data, size);
    }

    // Sends a control OrderDelete with magic stock_locate and uint64 payload
    inline void sendCtrl(uint16_t ctrl_code, uint64_t payload) noexcept {
        OrderDelete msg{};
        msg.type          = enums::MsgType::ORDER_DELETE;
        msg.stock_locate  = swap16(ctrl_code);
        msg.tracking_number = 0;
        writeTimestamp(msg.timestamp);
        msg.order_ref_num = swap64(payload);
        sendToSnapshotNetwork(&msg, sizeof(msg));
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

    SnapshotStreamer(SPSCQueue<SnapshotUpdate>* snapshotUpdates, quantlink::Logger* logger, size_t maxTickers,
                    const std::string& snapshot_ip, const std::string& iface, int snapshot_port) :
        snapshotUpdates_(snapshotUpdates),
        logger_(logger),
        maxTickers_(maxTickers),
        snapshot_socket_(*logger)
    {
        ticker_orders_.resize(maxTickers_);
        ASSERT(snapshot_socket_.init(snapshot_ip, iface, snapshot_port, false) >= 0,
               "SnapshotStreamer: failed to init snapshot multicast socket");
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

        alignas(8) char buf[ITCH_MAX_MSG_SIZE];

        // SNAPSHOT_START — OrderDelete with stock_locate=0xBBBB, order_ref=last_seq_num
        sendCtrl(SNAP_CTRL_START, last_seq_num_);

        for (size_t ticker_id = 0; ticker_id < maxTickers_; ticker_id++) {
            const auto& order_map = ticker_orders_[ticker_id];

            // CLEAR — OrderDelete with stock_locate=0xCCCC, order_ref=ticker_id
            sendCtrl(SNAP_CTRL_CLEAR, static_cast<uint64_t>(ticker_id));

            // ADD — encode each resting order as ITCH AddOrder
            for (const auto& [orderId, restingOrder] : order_map) {
                MEMarketUpdate add_msg = restingOrder;
                add_msg.type_         = UpdateType::ADD;
                size_t len = encode(add_msg, 0, buf);
                if (len > 0) sendToSnapshotNetwork(buf, len);
            }
        }

        // SNAPSHOT_END — OrderDelete with stock_locate=0xEEEE, order_ref=last_seq_num
        sendCtrl(SNAP_CTRL_END, last_seq_num_);

        // Flush all buffered snapshot messages to the wire in one shot
        snapshot_socket_.sendAndRecv();

        logger_->log("SnapshotStreamer: Snapshot published.\n");
    }

};