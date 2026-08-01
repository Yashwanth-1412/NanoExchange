#pragma once

#include <atomic>
#include <thread>
#include <unordered_map>

#include "../../types.h"
#include "fifo_process.h"
#include "token_manager.h"
#include "../protocol/ouch_processor.h"
#include "QuantLink/Lib/network/tcp_server.h"
#include "QuantLink/Lib/concurrency/lf_queue.h"
#include "QuantLink/Lib/concurrency/thread_utils.h"
#include "QuantLink/Lib/logging/logger.h"
#include "QuantLink/Lib/logging/time_utils.h"
#include "QuantLink/Lib/common/macros.h"

using namespace quantlink;

// ─────────────────────────────────────────────────────────────────────────────
//  OrderServer
//
//  Single-thread, epoll-driven TCP gateway that sits between clients and the
//  MatchingEngine.
//
//  Inbound path:
//    TCP bytes
//      → onRecv()         — decodeOuch() per complete OUCH message
//                         → FifoNdProcess::addToPending(kernel_time, req)
//      → onRecvFinished() — fired once the ENTIRE network buffer is empty
//                         → FifoNdProcess::publishPendingOrders()
//                         → SPSCQueue<MEClientRequest>  (timestamp-sorted)
//
//  Outbound path:
//    SPSCQueue<MEClientResponse>
//      → drainResponses() → encodeOuch() → TCPSocket::send() (staged)
//      → TCPServer::sendAndRecv() flushes staged bytes to the wire
//
//  Run loop (one thread, pinnable to a core):
//    loop:
//      drainResponses()       stage encoded responses into socket send bufs
//      server_.poll()         accept new connections, queue readable sockets
//      server_.sendAndRecv()  I/O: fires onRecv per socket,
//                             then fires onRecvFinished once (FIFO publish)
// ─────────────────────────────────────────────────────────────────────────────

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
    std::unordered_map<int, ClientId>        fdToClientId_;
    // clientId → socket: used in drainResponses to route responses back
    std::unordered_map<ClientId, TCPSocket*> clientToSocket_;

    ClientId nextClientId_ = 1;

    // ── Thread ────────────────────────────────────────────────────────────────
    std::atomic<bool> run_{false};
    std::thread       thread_;

    // ─────────────────────────────────────────────────────────────────────────
    //  onRecv
    //
    //  Fired by TCPSocket::sendAndRecv() each time a socket has data.
    //  kernel_time is the SO_TIMESTAMP from the NIC — the true arrival time
    //  used by FifoNdProcess to timestamp-sort orders across all clients.
    //
    //  Parses as many complete OUCH messages as are available in the buffer
    //  and calls fifo_.addToPending() for each one.
    //  Does NOT publish — we wait for onRecvFinished() when the whole
    //  network batch across all clients is done.
    // ─────────────────────────────────────────────────────────────────────────
    auto onRecv(TCPSocket* socket, Nanos kernel_time) -> void {
        // Register client on first-seen fd
        if (UNLIKELY(fdToClientId_.find(socket->socket_fd_) == fdToClientId_.end()))
            registerClient(socket);

        ClientId  cid    = fdToClientId_[socket->socket_fd_];
        char*     data   = socket->inbound_data_.data();
        size_t&   bufLen = socket->next_rcv_valid_index_;
        size_t    offset = 0;

        while (offset < bufLen) {
            char   msgType = data[offset];
            size_t msgLen  = ouchMsgSize(msgType);

            if (UNLIKELY(msgLen == 0)) {
                logger_->log("OrderServer: unknown OUCH msgType=% clientId=%\n",
                             static_cast<int>(msgType), cid);
                ++offset;
                continue;
            }

            if (bufLen - offset < msgLen) break;   // incomplete — wait for more bytes

            MEClientRequest req{};
            bool ok = decodeOuch(data + offset, cid, tokenManager_, req);

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
                logger_->log("OrderServer: decodeOuch failed msgType=% clientId=%\n",
                             static_cast<int>(msgType), cid);
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

    // ─────────────────────────────────────────────────────────────────────────
    //  onRecvFinished
    //
    //  Fired by TCPServer::sendAndRecv() ONCE after ALL sockets in this epoll
    //  round have been fully read.
    //
    //  At this point every order from every client in this batch is sitting
    //  in fifo_.pending_ stamped with its NIC arrival time.
    //  Sort by timestamp and push the whole batch to the matching engine.
    //  This is the correct moment — entire network buffer is empty.
    // ─────────────────────────────────────────────────────────────────────────
    auto onRecvFinished() -> void {
        fifo_.publishPendingOrders();
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  drainResponses
    //
    //  Pop MEClientResponse messages from the engine's output queue,
    //  encode each one to OUCH bytes, stage in the client's TCPSocket
    //  send buffer. TCPServer::sendAndRecv() flushes them on the next
    //  iteration.
    // ─────────────────────────────────────────────────────────────────────────
    auto drainResponses() -> void {
        while (true) {
            const auto* resp = responseQ_->getNextRead();
            if (!resp) break;

            auto it = clientToSocket_.find(resp->client_id_);
            if (UNLIKELY(it == clientToSocket_.end())) {
                logger_->log("OrderServer: no socket for clientId=% cOID=% status=% — response dropped\n",
                             resp->client_id_, resp->client_order_id_,
                             static_cast<int>(resp->status_));
                responseQ_->updateNextRead();
                continue;
            }

            char   buf[256];
            Nanos  now = quantlink::utils::get_current_epoch_nanos();
            size_t len = encodeOuch(buf, *resp, now);

            if (LIKELY(len > 0))
                it->second->send(buf, len);   // staged; flushed by sendAndRecv()
            else
                logger_->log("OrderServer: encodeOuch produced 0 bytes clientId=% cOID=% status=% — response dropped\n",
                             resp->client_id_, resp->client_order_id_,
                             static_cast<int>(resp->status_));

            responseQ_->updateNextRead();
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  ouchMsgSize
    //  Map inbound OUCH message type byte → fixed wire size.
    //  Lets us slice the raw TCP stream into complete messages with no
    //  length prefix needed.
    // ─────────────────────────────────────────────────────────────────────────
    static auto ouchMsgSize(char t) -> size_t {
        using T = ouch::enums::MsgType;
        switch (static_cast<T>(t)) {
            case T::ENTER_ORDER:   return sizeof(ouch::EnterOrder);
            case T::CANCEL_ORDER:  return sizeof(ouch::CancelOrder);
            case T::REPLACE_ORDER: return sizeof(ouch::ReplaceOrder);
            default:               return 0;
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  registerClient
    //  Called lazily the first time a new fd fires recv_callback_.
    //  Assigns a ClientId and populates both lookup maps.
    // ─────────────────────────────────────────────────────────────────────────
    auto registerClient(TCPSocket* socket) -> void {
        ClientId cid = nextClientId_++;
        fdToClientId_[socket->socket_fd_] = cid;
        clientToSocket_[cid]              = socket;
        logger_->log("OrderServer: new client fd=% assigned clientId=%\n",
                     socket->socket_fd_, cid);
    }

    auto onDisconnect(TCPSocket* socket) -> void {
        const auto fd_it = fdToClientId_.find(socket->socket_fd_);
        if (fd_it == fdToClientId_.end()) return;

        const ClientId client_id = fd_it->second;
        clientToSocket_.erase(client_id);
        fdToClientId_.erase(fd_it);
        logger_->log("OrderServer: disconnected clientId=% fd=%\n", client_id, socket->socket_fd_);
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  Main run loop
    // ─────────────────────────────────────────────────────────────────────────
    auto run() noexcept -> void {
        logger_->log("OrderServer: run loop starting\n");

        while (run_.load(std::memory_order_acquire)) {

            // Stage engine responses into socket send buffers
            drainResponses();

            // Retain requests when the engine queue is full and retry after
            // draining responses, avoiding a gateway/engine backpressure cycle.
            fifo_.publishPendingOrders();

            // Accept new connections, queue readable/writable sockets
            server_.poll();

            // Per-socket I/O:
            //   readable sockets → onRecv() → addToPending()
            //   after ALL drained → onRecvFinished() → publishPendingOrders()
            //   writable sockets  → flush staged send buffers to wire
            server_.sendAndRecv();
        }

        logger_->log("OrderServer: run loop exited\n");
    }

public:

    OrderServer(SPSCQueue<MEClientRequest>*  requestQ,
                SPSCQueue<MEClientResponse>* responseQ,
                quantlink::Logger*           logger,
                const std::string&           iface,
                int                          port,
                size_t                       fifoPending = 10000,
                size_t                       tokenPoolSz = 1000000)
        : requestQ_ (requestQ),
          responseQ_(responseQ),
          server_   (*logger),
          fifo_     (requestQ, responseQ, logger, fifoPending),
          tokenManager_(tokenPoolSz),
          logger_   (logger)
    {
        // Wire callbacks into TCPServer before listen()

        // recv_callback_: fired per socket when bytes arrive (with NIC timestamp)
        server_.recv_callback_ = [this](TCPSocket* socket, Nanos kernel_time) {
            onRecv(socket, kernel_time);
        };

        // recv_finished_callback_: fired once after the whole network batch is done
        // This is the FIFO sequencer's publish trigger
        server_.recv_finished_callback_ = [this]() {
            onRecvFinished();
        };

        // Runs on the gateway thread before TCPServer destroys the socket.
        server_.disconnect_callback_ = [this](TCPSocket* socket) {
            onDisconnect(socket);
        };

        server_.listen(iface, port);
    }

    auto start(int coreId = -1) -> void {
        run_.store(true, std::memory_order_release);
        thread_ = quantlink::utils::create_and_pin_thread(
            coreId, "OrderServer", [this]() { run(); });
    }

    auto stop() -> void {
        run_.store(false, std::memory_order_release);
        if (thread_.joinable()) thread_.join();
    }

    ~OrderServer() { stop(); }

    OrderServer()                              = delete;
    OrderServer(const OrderServer&)            = delete;
    OrderServer& operator=(const OrderServer&) = delete;
};
