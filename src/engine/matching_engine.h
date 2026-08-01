#pragma once

#include "QuantLink/Lib/logging/logger.h"
#include "QuantLink/Lib/concurrency/lf_queue.h"
#include "QuantLink/Lib/concurrency/thread_utils.h"
#include "../types.h"
#include <vector>
#include <atomic>
#include <thread>
#include <memory>

constexpr size_t MAX_ORDER_BOOKS  = 256;
constexpr size_t MAX_LOGGER_SIZE  = 8 * 1024 * 1024;
constexpr size_t ORDER_BOOK_SIZE  = 8 * 1024 * 1024;

class MEOrderBook;

using namespace quantlink;

class MatchingEngine {

private:
    SPSCQueue<MEClientRequest>*  requests_      = nullptr;
    SPSCQueue<MEClientResponse>* responses_     = nullptr;
    SPSCQueue<MEMarketUpdate>*   marketUpdates_ = nullptr;

    std::vector<std::unique_ptr<MEOrderBook>> ticker_order_book_;
    size_t maxOrderBooks_;
    OrderId next_match_id_ = 1;

    std::atomic<bool> run_{false};
    std::thread       thread_;

    quantlink::Logger* logger_;

public:
    MatchingEngine(SPSCQueue<MEClientRequest>*  requests,
                   SPSCQueue<MEClientResponse>* responses,
                   SPSCQueue<MEMarketUpdate>*   marketupdates,
                   quantlink::Logger*           logger,
                   size_t maxOrderBooks    = MAX_ORDER_BOOKS,
                   size_t orderBookPoolSize = ORDER_BOOK_SIZE);

    // Launch run() on its own pinned thread — consistent with
    // MarketDataPublisher, SnapshotStreamer, OrderServer
    auto start(int coreId = -1) -> void {
        run_.store(true, std::memory_order_release);
        thread_ = quantlink::utils::create_and_pin_thread(
            coreId, "MatchingEngine", [this]() { run(); });
    }

    auto stop() -> void {
        run_.store(false, std::memory_order_release);
        if (thread_.joinable()) thread_.join();
    }

    auto processClientRequest(const MEClientRequest* request) noexcept -> void;
    OrderId nextMatchId() noexcept { return next_match_id_++; }

    inline auto sendClientResponse(const MEClientResponse* response) noexcept {
        logger_->log("MEClientResponse [Client=% Ticker=% cOID=% mOID=% Side=% Px=% Qty=% Type=% Status=% ExecQty=% ExecPx=% LeavesQty=%]\n",
            response->client_id_,
            response->ticker_id_,
            response->client_order_id_,
            response->market_order_id_,
            static_cast<int>(response->side_),
            response->price_,
            response->qty_,
            static_cast<int>(response->type_),
            static_cast<int>(response->status_),
            response->executed_qty_,
            response->execution_price_,
            response->leaves_qty_
        );
        responses_->pushBlocking(*response);
    }

    inline auto sendMarketUpdate(const MEMarketUpdate* update) noexcept {
        logger_->log("MEMarketUpdate   [Ticker=% Match=% Side=% Px=% Qty=% UpdateType=%]\n",
            update->ticker_id_,
            update->match_id_,
            static_cast<int>(update->side_),
            update->price_,
            update->qty_,
            static_cast<int>(update->type_)
        );
        marketUpdates_->pushBlocking(*update);
    }

    auto run() noexcept -> void;

    ~MatchingEngine();
};
