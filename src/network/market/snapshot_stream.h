#pragma once

#include "../../types.h"
#include "../protocol/itch_encoder.h"
#include "QuantLink/Lib/concurrency/lf_queue.h"
#include "QuantLink/Lib/concurrency/thread_utils.h"
#include "QuantLink/Lib/logging/logger.h"
#include "QuantLink/Lib/logging/time_utils.h"
#include "QuantLink/Lib/network/socket_utils.h"
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

using namespace quantlink;
using namespace quantlink::itch;

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
    int snapshot_tcp_fd_ = -1;

    auto sendSnapshot(int fd) noexcept -> void {
        alignas(8) char buf[ITCH_MAX_MSG_SIZE];
        const auto send_all = [fd](const void* data, size_t size) {
            const char* bytes = static_cast<const char*>(data);
            while (size > 0) {
                const ssize_t sent = ::send(fd, bytes, size, MSG_NOSIGNAL);
                if (sent <= 0) return false;
                bytes += sent;
                size -= static_cast<size_t>(sent);
            }
            return true;
        };
        const auto send_ctrl = [&](uint16_t code, uint64_t value) {
            OrderDelete msg{};
            msg.type = enums::MsgType::ORDER_DELETE;
            msg.stock_locate = swap16(code);
            msg.order_ref_num = swap64(value);
            writeTimestamp(msg.timestamp);
            return send_all(&msg, sizeof(msg));
        };

        if (!send_ctrl(SNAP_CTRL_START, last_seq_num_)) return;
        for (size_t ticker_id = 0; ticker_id < maxTickers_; ++ticker_id) {
            if (!send_ctrl(SNAP_CTRL_CLEAR, ticker_id)) return;
            for (const auto& [order_id, order] : ticker_orders_[ticker_id]) {
                (void)order_id;
                MEMarketUpdate add = order;
                add.type_ = UpdateType::ADD;
                const size_t size = encode(add, 0, buf);
                if (size > 0 && !send_all(buf, size)) return;
            }
        }
        send_ctrl(SNAP_CTRL_END, last_seq_num_);
    }

    auto serveSnapshotRequests() noexcept -> void {
        sockaddr_in client{};
        socklen_t client_len = sizeof(client);
        const int client_fd = accept(snapshot_tcp_fd_, reinterpret_cast<sockaddr*>(&client), &client_len);
        if (client_fd < 0) return;

        char request = 0;
        if (recv(client_fd, &request, sizeof(request), 0) == 1 && request == 'S') sendSnapshot(client_fd);
        close(client_fd);
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

            serveSnapshotRequests();
        }

        logger_->log("SnapshotStreamer: thread exited safely.\n");
    }


public:

    SnapshotStreamer(SPSCQueue<SnapshotUpdate>* snapshotUpdates, quantlink::Logger* logger, size_t maxTickers,
                    int snapshot_tcp_port) :
        snapshotUpdates_(snapshotUpdates),
        logger_(logger),
        maxTickers_(maxTickers)
    {
        ticker_orders_.resize(maxTickers_);

        snapshot_tcp_fd_ = socket(AF_INET, SOCK_STREAM, 0);
        ASSERT(snapshot_tcp_fd_ >= 0, "SnapshotStreamer: failed to create TCP snapshot socket");
        int reuse = 1;
        ASSERT(setsockopt(snapshot_tcp_fd_, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) == 0,
               "SnapshotStreamer: failed to set TCP snapshot reuse");
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_ANY);
        addr.sin_port = htons(snapshot_tcp_port);
        ASSERT(bind(snapshot_tcp_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0,
               "SnapshotStreamer: failed to bind TCP snapshot socket");
        ASSERT(listen(snapshot_tcp_fd_, 16) == 0, "SnapshotStreamer: failed to listen for TCP snapshots");
        ASSERT(quantlink::setNonBlocking(snapshot_tcp_fd_), "SnapshotStreamer: failed to make TCP snapshot socket nonblocking");
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
        if (snapshot_tcp_fd_ != -1) close(snapshot_tcp_fd_);
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

};
