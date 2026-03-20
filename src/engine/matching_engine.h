#pragma once

#include "Lib/logging/logger.h"
#include "../types.h"
#include "QuantLink/Lib/concurrency/lf_queue.h"
#include <array>
#include <cstddef>

constexpr size_t MAX_ORDER_BOOKS = 256;
constexpr size_t MAX_LOGGER_SIZE = 8*1024*1024;
constexpr size_t ORDER_BOOK_SIZE = 8*1024*1024;

class MEOrderBook;

using namespace quantlink;
class MatchingEngine {

private:
    SPSCQueue<MEClientRequest>* requests_ = nullptr;
    SPSCQueue<MEClientResponse>* responses_ = nullptr;
    SPSCQueue<MEMarketUpdate>* marketUpdates_ = nullptr;

    std::array<MEOrderBook*, MAX_ORDER_BOOKS> ticker_order_book_;
    volatile bool run_ = false;

    quantlink::Logger* logger_;

public:

    MatchingEngine (SPSCQueue<MEClientRequest>* requests, SPSCQueue<MEClientResponse>* responses, SPSCQueue<MEMarketUpdate>* marketupdates, quantlink::Logger* logger);

    auto stop () {
        run_ = false;
    }

    auto start () {
        run_ = true;
    }

    inline auto processClientRequest (const MEClientRequest* request) noexcept -> void;

    inline auto sendClientResponse(const MEClientResponse* response) noexcept {
        logger_->log("MEClientResponse [Client=% Ticker=% cOID=% mOID=% Status=% ExecQty=% ExecPx=% LeavesQty=%]\n", 
            response->client_id_,
            response->ticker_id_,
            response->client_order_id_,
            response->market_order_id_,
            static_cast<int>(response->status_),
            response->executed_qty_,
            response->execution_price_,
            response->leaves_qty_
        );

        responses_->push(*response);

    }

    inline auto sendMarketUpdate (const MEMarketUpdate* update) noexcept {
        logger_->log("MEMarketUpdate [Ticker=% Side=% Px=% Qty=% UpdateType=%]\n", 
            update->ticker_id_,
            static_cast<int>(update->side_),
            update->price_,
            update->qty_,
            static_cast<int>(update->type_)
        );

        marketUpdates_->push(*update);
    }

    auto run() noexcept -> void;

    ~MatchingEngine();
    


};