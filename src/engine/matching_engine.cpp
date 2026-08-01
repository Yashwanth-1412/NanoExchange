#include "matching_engine.h"
#include "order_book_map.h"     // Swap to order_book_vector.h to use the vector implementation


MatchingEngine::MatchingEngine(SPSCQueue<MEClientRequest>* requests, SPSCQueue<MEClientResponse>* responses,
                                SPSCQueue<MEMarketUpdate>* marketupdates, quantlink::Logger* logger,
                                size_t maxOrderBooks, size_t orderBookPoolSize) :
            requests_(requests),
            responses_(responses),
            marketUpdates_(marketupdates),
            maxOrderBooks_(maxOrderBooks),
            logger_(logger) {

    ticker_order_book_.reserve(maxOrderBooks_);
    for (size_t i = 0; i < maxOrderBooks_; i++) {
        ticker_order_book_.emplace_back(
            std::make_unique<MEOrderBook>(orderBookPoolSize, i, this));
    }
}


auto MatchingEngine::processClientRequest(const MEClientRequest* request) noexcept -> void {
    auto* orderbook = ticker_order_book_[request->ticker_id_].get();

    switch (request->action_) {
        case ClientRequestType::NEW: {
            orderbook->addOrder(request->ticker_id_, request->client_id_,
                request->client_order_id_, request->type_, request->price_, request->qty_, request->side_,
                request->order_token_);
            break;
        }
        case ClientRequestType::CANCEL: {
            orderbook->cancelOrder(request->ticker_id_, request->client_id_, request->client_order_id_, request->qty_,
                                   request->order_token_);
            break;
        }
        case ClientRequestType::MODIFY: {
            orderbook->replaceOrder(request->ticker_id_, request->client_id_, request->client_order_id_,
                                    request->new_client_order_id_, request->type_, request->price_, request->qty_,
                                    request->order_token_);
            break;
        }
        default: {
            logger_->log("FATAL ERROR: Received invalid client-request-type: %\n",
                        static_cast<int>(request->action_));
            break;
        }
    }
}


auto MatchingEngine::run() noexcept -> void {
    logger_->log("MatchingEngine: RUN loop starting. Polling incoming lock-free queues...\n");

    while (run_.load(std::memory_order_acquire)) {
        const auto request = requests_->getNextRead();

        if (LIKELY(request)) {
            logger_->log("Processing MEClientRequest [Action=% Client=% Ticker=% cOID=% Type=% Side=% Px=% Qty=%]\n",
                static_cast<int>(request->action_),
                request->client_id_,
                request->ticker_id_,
                request->client_order_id_,
                static_cast<int>(request->type_),
                static_cast<int>(request->side_),
                request->price_,
                request->qty_
            );

            processClientRequest(request);
            requests_->updateNextRead();
        }
    }

    logger_->log("MatchingEngine: RUN loop exited safely. Engine halted.\n");
}


MatchingEngine::~MatchingEngine() {
    stop();   // joins thread, sets run_ = false
    // ticker_order_book_ auto-deleted via unique_ptr
}
