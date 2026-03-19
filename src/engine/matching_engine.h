#pragma once

#include "Lib/logging/logger.h"
#include "order_book_map.h"
#include "../types.h"
#include "QuantLink/Lib/concurrency/lf_queue.h"
#include <array>
#include <cstddef>

constexpr size_t MAX_ORDER_BOOKS = 256000;
constexpr size_t MAX_LOGGER_SIZE = 8*1024*1024;
constexpr size_t ORDER_BOOK_SIZE = 8*1024*1024;

using namespace quantlink;
class MatchingEngine {

private:
    SPSCQueue<MEClientRequest>* requests_ = nullptr;
    SPSCQueue<MEClientResponse>* responses_ = nullptr;
    SPSCQueue<MEMarketUpdate>* marketUpdates_ = nullptr;

    std::array<MEOrderBook*, MAX_ORDER_BOOKS> ticker_order_book_;
    bool run = false;

    quantlink::Logger logger_;

public:

    MatchingEngine (SPSCQueue<MEClientRequest>* requests, SPSCQueue<MEClientResponse>* responses, SPSCQueue<MEMarketUpdate>* marketupdates) :
                requests_(requests),
                responses_(responses),
                marketUpdates_(marketupdates),
                logger_(MAX_LOGGER_SIZE, "matching_engine", 15) {

        for (size_t i = 0; i < MAX_ORDER_BOOKS; i++) {
            ticker_order_book_[i] = new MEOrderBook(ORDER_BOOK_SIZE, i, this, &logger_);
        }

    }

    
    


};