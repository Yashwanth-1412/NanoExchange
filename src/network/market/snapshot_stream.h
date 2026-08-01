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
#include <cstring>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>
#include <arpa/inet.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

using namespace quantlink;
using namespace quantlink::itch;

constexpr uint16_t SNAP_CTRL_START = 0xBBBB;
constexpr uint16_t SNAP_CTRL_CLEAR = 0xCCCC;
constexpr uint16_t SNAP_CTRL_END   = 0xEEEE;

// Replay window: the max seq gap a client can recover via 'R' before falling back to 'S'.
constexpr size_t REPLAY_CAPACITY      = 8192;    // power of two, single-owner (no 2x safety needed)
constexpr int    TCP_SND_TIMEOUT_MS = 2000;    // per-client send deadline (slow client -> dropped)
constexpr int    TCP_RCV_TIMEOUT_MS = 10;      // request read deadline (silent client -> dropped)

class SnapshotStreamer {

private:
    struct alignas(64) ReplayEntry {
        char     seq_be_[8];                    // BE seq prefix, pre-swapped at write time
        char     bytes_[ITCH_MAX_MSG_SIZE];     // encoded ITCH payload
        uint16_t len_ = 0;
    };
    static_assert((REPLAY_CAPACITY & (REPLAY_CAPACITY - 1)) == 0, "REPLAY_CAPACITY must be a power of two");

    SPSCQueue<SnapshotUpdate>* streamer_updates_ = nullptr;
    quantlink::Logger* logger_;

    std::atomic<bool> run_{false};
    std::thread thread_;

    std::vector<std::unordered_map<OrderId, MEMarketUpdate>> shadow_book_;
    size_t   maxTickers_;

    uint64_t last_seq_num_ = 0;
    bool     dirty_ = false;              // book changed since last bake

    ReplayEntry replay_buffer_[REPLAY_CAPACITY];       // 8192 x 64B = 512KB

    std::vector<char> snapshot_buf_;      // pre-baked flat snapshot (rebuilt only when dirty)
    std::vector<char> replay_scratch_;    // flatten target for 'R' (benchmarked 7x faster than iovec gather)

    int snapshot_tcp_fd_ = -1;

    // ── replay buffer ─────────────────────────────────────────────────────────

    auto writeReplayEntry(const SnapshotUpdate* snap) noexcept -> void {
        ReplayEntry& e = replay_buffer_[(snap->seq_num_ - 1) & (REPLAY_CAPACITY - 1)];
        const uint64_t seq_be = swap64(snap->seq_num_);
        std::memcpy(e.seq_be_, &seq_be, sizeof(seq_be));
        std::memcpy(e.bytes_, snap->bytes_, snap->len_);
        e.len_ = static_cast<uint16_t>(snap->len_);
    }

    // ── snapshot bake ─────────────────────────────────────────────────────────

    auto bakeSnapshot() noexcept -> void {
        snapshot_buf_.clear();

        const auto append_ctrl = [this](uint16_t code, uint64_t value) {
            OrderDelete msg{};
            msg.type = enums::MsgType::ORDER_DELETE;
            msg.stock_locate = swap16(code);
            msg.order_ref_num = swap64(value);
            writeTimestamp(msg.timestamp);
            const auto* p = reinterpret_cast<const char*>(&msg);
            snapshot_buf_.insert(snapshot_buf_.end(), p, p + sizeof(msg));
        };

        alignas(8) char buf[ITCH_MAX_MSG_SIZE];

        append_ctrl(SNAP_CTRL_START, last_seq_num_);
        for (size_t ticker_id = 0; ticker_id < maxTickers_; ++ticker_id) {
            append_ctrl(SNAP_CTRL_CLEAR, ticker_id);
            for (const auto& [order_id, order] : shadow_book_[ticker_id]) {
                (void)order_id;
                MEMarketUpdate add = order;
                add.type_ = UpdateType::ADD;
                const size_t size = encode(add, buf);
                if (size > 0) snapshot_buf_.insert(snapshot_buf_.end(), buf, buf + size);
            }
        }
        append_ctrl(SNAP_CTRL_END, last_seq_num_);

        dirty_ = false;
    }

    // ── TCP request handling ──────────────────────────────────────────────────

    auto sendAll(int fd, const void* data, size_t size) noexcept -> bool {
        const char* bytes = static_cast<const char*>(data);
        while (size > 0) {
            const ssize_t sent = ::send(fd, bytes, size, MSG_NOSIGNAL);
            if (sent <= 0) return false;
            bytes += sent;
            size -= static_cast<size_t>(sent);
        }
        return true;
    }

    auto recvExact(int fd, void* buf, size_t size) noexcept -> bool {
        char* bytes = static_cast<char*>(buf);
        while (size > 0) {
            const ssize_t n = ::recv(fd, bytes, size, 0);
            if (n <= 0) return false;
            bytes += n;
            size -= static_cast<size_t>(n);
        }
        return true;
    }

    // 'S': full book — bake if dirty or never baked, then one send() of the flat buffer.
    auto serveSnapshot(int fd) noexcept -> void {
        if (dirty_ || snapshot_buf_.empty()) bakeSnapshot();
        sendAll(fd, snapshot_buf_.data(), snapshot_buf_.size());
    }

    // 'R' + client_seq: replay (client_seq, last_seq_], each as seq_be(8) + payload,
    // same framing as the UDP feed. Response starts with a 1-byte status:
    //   'R' + replay frames -> replay follows
    //   'N'                 -> gap too large for the replay buffer (client falls back to 'S')
    //   'C'                 -> client already current (nothing to replay)
    auto serveReplay(int fd, uint64_t client_seq) noexcept -> void {
        if (client_seq >= last_seq_num_) {   // client already current (or ahead)
            const char status = 'C';
            sendAll(fd, &status, 1);
            return;
        }

        const uint64_t oldest_replayable = last_seq_num_ > REPLAY_CAPACITY
                                             ? last_seq_num_ - REPLAY_CAPACITY : 0;
        if (client_seq < oldest_replayable) {   // gap too large for the replay buffer
            const char status = 'N';
            sendAll(fd, &status, 1);
            return;
        }

        const char status = 'R';
        sendAll(fd, &status, 1);

        // Flatten (client_seq, last_seq_] into replay_scratch_, then ONE send().
        // Measured: flat copy + single send = 53us vs 16 x sendmsg gather = 376us.
        replay_scratch_.clear();
        replay_scratch_.reserve(REPLAY_CAPACITY * (sizeof(uint64_t) + ITCH_MAX_MSG_SIZE));
        for (uint64_t seq = client_seq + 1; seq <= last_seq_num_; ++seq) {
            const ReplayEntry& e = replay_buffer_[(seq - 1) & (REPLAY_CAPACITY - 1)];
            replay_scratch_.insert(replay_scratch_.end(), e.seq_be_, e.seq_be_ + 8);
            replay_scratch_.insert(replay_scratch_.end(), e.bytes_, e.bytes_ + e.len_);
        }
        sendAll(fd, replay_scratch_.data(), replay_scratch_.size());
    }

    auto handleClient(int fd) noexcept -> void {
        char request[1 + sizeof(uint64_t)];

        if (!recvExact(fd, request, 1)) return;

        if (request[0] == 'S') { serveSnapshot(fd); return; }

        if (request[0] == 'R') {
            if (!recvExact(fd, request + 1, sizeof(uint64_t))) return;
            const uint64_t client_seq = swap64(*reinterpret_cast<const uint64_t*>(request + 1));
            serveReplay(fd, client_seq);
            return;
        }
    }

    auto serveRequests() noexcept -> void {
        while (true) {
            sockaddr_in client{};
            socklen_t client_len = sizeof(client);
            const int client_fd = accept(snapshot_tcp_fd_, reinterpret_cast<sockaddr*>(&client), &client_len);
            if (client_fd < 0) return;   // EAGAIN or error — no more pending connections

            // Blocking fd + deadlines: a silent/stuck client is dropped in bounded time
            timeval snd_to{TCP_SND_TIMEOUT_MS / 1000, (TCP_SND_TIMEOUT_MS % 1000) * 1000};
            timeval rcv_to{TCP_RCV_TIMEOUT_MS / 1000, (TCP_RCV_TIMEOUT_MS % 1000) * 1000};
            setsockopt(client_fd, SOL_SOCKET, SO_SNDTIMEO, &snd_to, sizeof(snd_to));
            setsockopt(client_fd, SOL_SOCKET, SO_RCVTIMEO, &rcv_to, sizeof(rcv_to));

            handleClient(client_fd);
            close(client_fd);
        }
    }

    auto run() noexcept -> void {
        logger_->log("SnapshotStreamer: thread started.\n");

        while (run_.load(std::memory_order_acquire)) {

            // Drain queue 2 — every update lands in the book AND the replay buffer
            while (true) {
                const auto* snap = streamer_updates_->getNextRead();
                if (!snap) break;

                applyToBook(&snap->update_);
                writeReplayEntry(snap);
                last_seq_num_ = snap->seq_num_;

                streamer_updates_->updateNextRead();
            }

            pollfd p{snapshot_tcp_fd_, POLLIN, 0};
            const int rc = poll(&p, 1, 1);   // 1ms: bounds queue-drain latency
            if (rc > 0 && (p.revents & POLLIN)) serveRequests();
        }

        logger_->log("SnapshotStreamer: thread exited safely.\n");
    }

public:

    SnapshotStreamer(SPSCQueue<SnapshotUpdate>* snapshotUpdates, quantlink::Logger* logger, size_t maxTickers,
                    int snapshot_tcp_port) :
        streamer_updates_(snapshotUpdates),
        logger_(logger),
        maxTickers_(maxTickers)
    {
        shadow_book_.resize(maxTickers_);

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

    auto applyToBook(const MEMarketUpdate* update) -> void {
        TickerId ticker = update->ticker_id_;

        switch (update->type_) {

            case UpdateType::ADD : {
                shadow_book_[ticker][update->market_order_id_] = *update;
                break;
            }

            case UpdateType::CANCEL : {
                shadow_book_[ticker].erase(update->market_order_id_);
                break;
            }

            case UpdateType::MODIFY : {
                auto& orders = shadow_book_[ticker];
                orders.erase(update->market_order_id_);
                MEMarketUpdate replacement = *update;
                replacement.market_order_id_ = update->new_order_id_;
                replacement.new_order_id_ = OrderId_INVALID;
                orders[replacement.market_order_id_] = replacement;
                break;
            }

            case UpdateType::TRADE : {
                auto& order_map = shadow_book_[ticker];
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

        if (update->type_ == UpdateType::ADD ||
            update->type_ == UpdateType::CANCEL ||
            update->type_ == UpdateType::MODIFY ||
            update->type_ == UpdateType::TRADE) {
            dirty_ = true;
        }
    }

};
