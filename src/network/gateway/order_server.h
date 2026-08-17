#pragma once

#include "src/instrumentation/perf_utils.h"
#include "src/types.h"
#include "src/network/protocol/ouch_processor.h"
#include "QuantLink/Lib/common/macros.h"
#include "QuantLink/Lib/concurrency/lf_queue.h"
#include "QuantLink/Lib/concurrency/thread_utils.h"
#include "QuantLink/Lib/logging/logger.h"
#include "QuantLink/Lib/logging/time_utils.h"
#include "QuantLink/Lib/network/tcp_server.h"
#include "src/network/gateway/fifo_process.h"
#include "src/network/gateway/token_manager.h"

#include <atomic>
#include <thread>
#include <unordered_map>

using namespace quantlink;
using namespace nanoexchange;

// Single-thread, epoll-driven OUCH/TCP gateway between clients and the
// MatchingEngine. Inbound bytes are decoded per socket and staged in the FIFO
// sequencer; the batch is timestamp-sorted and published only once the whole
// epoll round has drained. See README "Order gateway" for the full topology.

class OrderServer {

  private:
    // ── Queues (shared with MatchingEngine) ───────────────────────────────────
    SPSCQueue<MEClientRequest>*  requestQ_  = nullptr;
    SPSCQueue<MEClientResponse>* responseQ_ = nullptr;

    // ── Infrastructure ────────────────────────────────────────────────────────
    TCPServer          server_;
    FifoNdProcess      fifo_;
    OrderTokenManager  tokenManager_;
    quantlink::Logger* logger_;

    // ── Client tracking ───────────────────────────────────────────────────────
    // fd → clientId    : used in onRecv to identify the sender
    std::unordered_map<int, ClientId> fdToClientId_;
    // clientId → socket: used in drainResponses to route responses back
    std::unordered_map<ClientId, TCPSocket*> clientToSocket_;

    ClientId nextClientId_ = 1;

    // ── Thread ────────────────────────────────────────────────────────────────
    std::atomic<bool> run_{false};
    std::thread       thread_;

    // Fired per socket when bytes arrive. kernel_time is the SO_TIMESTAMP from
    // the NIC, used to sort orders fairly across clients. Decodes every
    // complete OUCH message into pending; publishing waits for onRecvFinished.
    auto onRecv(TCPSocket* socket, Nanos kernel_time) -> void {
        NANOEXCHANGE_TIMESTAMP(T1_OrderServer_TCP_read, *logger_);
        // Register client on first-seen fd
        if (UNLIKELY(fdToClientId_.find(socket->socket_fd_) == fdToClientId_.end()))
            registerClient(socket);

        ClientId cid    = fdToClientId_[socket->socket_fd_];
        char*    data   = socket->inbound_data_.data();
        size_t&  bufLen = socket->next_rcv_valid_index_;
        size_t   offset = 0;

        while (offset < bufLen) {
            char   msgType = data[offset];
            size_t msgLen  = ouchMsgSize(msgType);

            if (UNLIKELY(msgLen == 0)) {
                logger_->log("OrderServer: unknown OUCH msgType=% clientId=%\n", static_cast<int>(msgType), cid);
                ++offset;
                continue;
            }

            if (bufLen - offset < msgLen)
                break; // incomplete — wait for more bytes

            MEClientRequest req{};
            bool            ok = decodeOuch(data + offset, cid, tokenManager_, req);

            if (LIKELY(ok)) {
                if (UNLIKELY(!fifo_.addToPending(kernel_time, req))) {
                    fifo_.publishPendingOrders();
                    if (UNLIKELY(!fifo_.addToPending(kernel_time, req))) {
                        fifo_.growPending();
                        const bool added = fifo_.addToPending(kernel_time, req);
                        ASSERT(added, "OrderServer: failed to retain decoded request");
                    }
                }
            } else {
                logger_->log("OrderServer: decodeOuch failed msgType=% clientId=%\n", static_cast<int>(msgType), cid);
            }

            offset += msgLen;
        }

        // Compact buffer — move unprocessed tail to front
        if (offset > 0) {
            if (offset < bufLen)
                std::memmove(data, data + offset, bufLen - offset);
            bufLen -= offset;
        }
    }

    // Fired once after every socket in this epoll round has been read, i.e. the
    // whole network batch is in pending_. Sorting here is what makes arrival
    // order fair rather than dependent on epoll's socket ordering.
    auto onRecvFinished() -> void { fifo_.publishPendingOrders(); }

    // Pop engine responses, encode to OUCH, stage into the client's send
    // buffer. TCPServer::sendAndRecv() flushes on the next iteration.
    auto drainResponses() -> void {
        while (true) {
            const auto* resp = responseQ_->getNextRead();
            if (!resp)
                break;

            NANOEXCHANGE_TIMESTAMP(T5t_OrderServer_LFQueue_read, *logger_);

            auto it = clientToSocket_.find(resp->client_id_);
            if (UNLIKELY(it == clientToSocket_.end())) {
                logger_->log("OrderServer: no socket for clientId=% cOID=% status=% — response dropped\n",
                             resp->client_id_, resp->client_order_id_, static_cast<int>(resp->status_));
                responseQ_->updateNextRead();
                continue;
            }

            char   buf[256];
            Nanos  now = quantlink::utils::get_current_epoch_nanos();
            size_t len = encodeOuch(buf, *resp, now);

            if (LIKELY(len > 0)) {
                NANOEXCHANGE_START_MEASURE(Exchange_TCPSocket_send);
                it->second->send(buf, len); // staged; flushed by sendAndRecv()
                NANOEXCHANGE_END_MEASURE(Exchange_TCPSocket_send, *logger_);
                NANOEXCHANGE_TIMESTAMP(T6t_OrderServer_TCP_write, *logger_);
            } else {
                logger_->log("OrderServer: encodeOuch produced 0 bytes clientId=% cOID=% status=% — response dropped\n",
                             resp->client_id_, resp->client_order_id_, static_cast<int>(resp->status_));
            }

            responseQ_->updateNextRead();
        }
    }

    // Inbound OUCH type byte → fixed wire size. Fixed sizes let us frame the
    // TCP stream with no length prefix.
    static auto ouchMsgSize(char t) -> size_t {
        using T = ouch::enums::MsgType;
        switch (static_cast<T>(t)) {
        case T::ENTER_ORDER:
            return sizeof(ouch::EnterOrder);
        case T::CANCEL_ORDER:
            return sizeof(ouch::CancelOrder);
        case T::REPLACE_ORDER:
            return sizeof(ouch::ReplaceOrder);
        default:
            return 0;
        }
    }

    // Called lazily the first time a new fd delivers data.
    auto registerClient(TCPSocket* socket) -> void {
        ClientId cid                      = nextClientId_++;
        fdToClientId_[socket->socket_fd_] = cid;
        clientToSocket_[cid]              = socket;
        logger_->log("OrderServer: new client fd=% assigned clientId=%\n", socket->socket_fd_, cid);
    }

    auto onDisconnect(TCPSocket* socket) -> void {
        const auto fd_it = fdToClientId_.find(socket->socket_fd_);
        if (fd_it == fdToClientId_.end())
            return;

        const ClientId client_id = fd_it->second;
        clientToSocket_.erase(client_id);
        fdToClientId_.erase(fd_it);
        logger_->log("OrderServer: disconnected clientId=% fd=%\n", client_id, socket->socket_fd_);
    }

    auto run() noexcept -> void {
        logger_->log("OrderServer: run loop starting\n");

        while (run_.load(std::memory_order_acquire)) {
            drainResponses();

            // Retry requests retained when the engine queue was full. Doing this
            // after draining responses breaks the gateway/engine deadlock cycle.
            fifo_.publishPendingOrders();

            server_.poll();
            server_.sendAndRecv();
        }

        logger_->log("OrderServer: run loop exited\n");
    }

  public:
    OrderServer(SPSCQueue<MEClientRequest>* requestQ, SPSCQueue<MEClientResponse>* responseQ, quantlink::Logger* logger,
                const std::string& iface, int port, size_t fifoPending = 10000, size_t tokenPoolSz = 1000000)
        : requestQ_(requestQ),
          responseQ_(responseQ),
          server_(*logger),
          fifo_(requestQ, responseQ, logger, fifoPending),
          tokenManager_(tokenPoolSz),
          logger_(logger) {
        // Callbacks must be wired before listen().
        server_.recv_callback_          = [this](TCPSocket* socket, Nanos kernel_time) { onRecv(socket, kernel_time); };
        server_.recv_finished_callback_ = [this]() { onRecvFinished(); };
        server_.disconnect_callback_    = [this](TCPSocket* socket) { onDisconnect(socket); };

        server_.listen(iface, port);
    }

    auto start(int coreId = -1) -> void {
        run_.store(true, std::memory_order_release);
        thread_ = quantlink::utils::create_and_pin_thread(coreId, "OrderServer", [this]() { run(); });
    }

    auto stop() -> void {
        run_.store(false, std::memory_order_release);
        if (thread_.joinable())
            thread_.join();
    }

    ~OrderServer() { stop(); }

    OrderServer()                              = delete;
    OrderServer(const OrderServer&)            = delete;
    OrderServer& operator=(const OrderServer&) = delete;
};
