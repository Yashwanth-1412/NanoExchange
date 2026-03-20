#include "order_book_vector.h"
#include "matching_engine.h"


MEOrderBook::MEOrderBook(size_t size, TickerId tickerId, MatchingEngine* matchingEngine, quantlink::Logger* logger) : 
        tickerId_(tickerId), 
        matchingEngine_(matchingEngine), 
        logger_(logger),
        pool_(size),
        bids_(MAX_PRICE_LEVELS),
        asks_(MAX_PRICE_LEVELS)    // Ensure pool is big enough
{
    for (Price p = 0; p < (Price) MAX_PRICE_LEVELS; p++) {
        bids_[p].price_ = p;
        asks_[p].price_ = p;
    }
}


auto MEOrderBook::insertPricelevel(PriceLevel* priceLevel, Side side) -> void {
    if (side == Side::BUY) {
        // Bids: Sorted Highest to Lowest
        if (!best_bid_) best_bid_ = priceLevel;
        
        else if (priceLevel->price_ > best_bid_->price_) {
            priceLevel->next_ = best_bid_;
            best_bid_->prev_ = priceLevel;
            best_bid_ = priceLevel;
        } 
        else {
            PriceLevel* curr = best_bid_;
            while (curr->next_ && curr->next_->price_ > priceLevel->price_) curr = curr->next_;

            priceLevel->next_ = curr->next_;
            if (curr->next_) curr->next_->prev_ = priceLevel;
            curr->next_ = priceLevel;
            priceLevel->prev_ = curr;
        }
    } 
    else {
        // Asks: Sorted Lowest to Highest
        if (!best_ask_) best_ask_ = priceLevel;

        else if (priceLevel->price_ < best_ask_->price_) {
            priceLevel->next_ = best_ask_;
            best_ask_->prev_ = priceLevel;
            best_ask_ = priceLevel;
        } 
        else {
            PriceLevel* curr = best_ask_;
            while (curr->next_ && curr->next_->price_ < priceLevel->price_) curr = curr->next_;

            priceLevel->next_ = curr->next_;
            if (curr->next_) curr->next_->prev_ = priceLevel;
            curr->next_ = priceLevel;
            priceLevel->prev_ = curr;
        }
    }
}

    
auto MEOrderBook::removePriceLevel(PriceLevel* priceLevel, Side side) -> void {
    if (priceLevel->next_) priceLevel->next_->prev_ = priceLevel->prev_;
    if (priceLevel->prev_) priceLevel->prev_->next_ = priceLevel->next_;

    if (side == Side::BUY && best_bid_ == priceLevel) best_bid_ = priceLevel->next_;
    if (side == Side::SELL && best_ask_ == priceLevel) best_ask_ = priceLevel->next_;

    // Clear the array slot and return memory to the pool
    // if (side == Side::BUY) bids_[priceLevel->price_] = nullptr;   // WE DONT HAVE TO DO THIS
    // else asks_[priceLevel->price_] = nullptr;

    // priceLevelPool_.deallocate(priceLevel);

    priceLevel->next_ = nullptr;
    priceLevel->prev_ = nullptr;
    priceLevel->head  = nullptr;
    priceLevel->tail  = nullptr;
}
    

auto MEOrderBook::insert(TickerId tickerId, ClientId clientId, OrderId clientOrderId,
            OrderId marketOrderId, OrderType orderType, Price price,
            Quantity initialQuantity, Side side) -> void {

    MEOrder* order = pool_.allocate(tickerId, clientId, clientOrderId,
                                    marketOrderId, orderType, price, initialQuantity, side);

    PriceLevel* priceLevel = (side == Side::BUY) ? &bids_[price] : &asks_[price];

    // Allocate a new PriceLevel ONLY if it doesn't exist yet
    if (UNLIKELY(priceLevel == nullptr)) {
        //priceLevel = priceLevelPool_.allocate(price);
        insertPricelevel(priceLevel, side); // Link it into the active market
    }

    if (priceLevel->head == nullptr) {                              // Adding the Order First
        priceLevel->head = order;                                   // 1st Order 
        priceLevel->tail = order;
    } 
    else {
        order->prev_ = priceLevel->tail;
        priceLevel->tail->next_ = order;
        priceLevel->tail = order;      
    }
    

    orders_[clientId][clientOrderId] = order;
}


auto MEOrderBook::remove(MEOrder* order) -> void {

    Price price = order->price_;
    PriceLevel* priceLevel = (order->side_ == Side::BUY) ? &bids_[price] : &asks_[price];

    if (order->prev_ != nullptr) order->prev_->next_ = order->next_;    //Order is Head
    else priceLevel->head = order->next_;

    if (order->next_ != nullptr) order->next_->prev_ = order->prev_;
    else priceLevel->tail = order->prev_;                                //Order is Tail

    if (priceLevel->head == nullptr) {
        removePriceLevel(priceLevel, order->side_);          //Only Order
    }

    
    auto& clientMap = orders_[order->clientId_];
    clientMap.erase(order->clientOrderId_);

    if (clientMap.empty()) {
        orders_.erase(order->clientId_);                            // Clean up the client to save RAM!
    }
    pool_.deallocate(order);
    
}


auto MEOrderBook::match(TickerId tickerId, ClientId clientId, OrderId clientOrderId, OrderId marketOrderId,
        OrderType orderType, Price price, Quantity initialQuantity, Side side) -> Quantity {

    Quantity remainingQuantity = initialQuantity;

    if (side == Side::BUY) {
        PriceLevel* level = best_ask_;

        while (level && remainingQuantity > 0) {
            if (price < level->price_) break;

            while (level->head && remainingQuantity > 0) {
                MEOrder* restingOrder = level->head;
                Quantity fill = std::min(remainingQuantity, restingOrder->remainingQuantity_);

                remainingQuantity                -= fill;
                restingOrder->remainingQuantity_ -= fill;

                MEClientResponse restingResponse{
                    restingOrder->clientId_, tickerId, restingOrder->clientOrderId_, restingOrder->marketOrderId_,
                    ResponseType::EXECUTED,
                    fill, level->price_, restingOrder->remainingQuantity_
                };
                matchingEngine_->sendClientResponse(&restingResponse);     // SEND RESPONSE FOR RESTING ORDER

                MEClientResponse aggressiveResponse{
                    clientId, tickerId, clientOrderId, marketOrderId, ResponseType::EXECUTED,
                    fill, level->price_, remainingQuantity
                };
                matchingEngine_->sendClientResponse(&aggressiveResponse);  // SEND RESPONSE FOR AGRESSIVE MATCHER

                MEMarketUpdate marketUpdate{
                    tickerId, restingOrder->marketOrderId_, UpdateType::TRADE,
                    restingOrder->side_, restingOrder->price_, fill
                };
                matchingEngine_->sendMarketUpdate(&marketUpdate);          // SEND MARKET UPDATE

                if (restingOrder->remainingQuantity_ == 0) remove(restingOrder);
            }

            level = best_ask_;   // re-fetch: level may have been removed if it emptied
        }
    } 
    else {
        PriceLevel* level = best_bid_;

        while (level && remainingQuantity > 0) {
            if (price > level->price_) break;

            while (level->head && remainingQuantity > 0) {
                MEOrder* restingOrder = level->head;
                Quantity fill = std::min(remainingQuantity, restingOrder->remainingQuantity_);

                remainingQuantity                -= fill;
                restingOrder->remainingQuantity_ -= fill;

                MEClientResponse restingResponse{
                    restingOrder->clientId_, tickerId, restingOrder->clientOrderId_, restingOrder->marketOrderId_,
                    ResponseType::EXECUTED,
                    fill, level->price_, restingOrder->remainingQuantity_
                };
                matchingEngine_->sendClientResponse(&restingResponse);     // SEND RESPONSE FOR RESTING ORDER

                MEClientResponse aggressiveResponse{
                    clientId, tickerId, clientOrderId, marketOrderId, ResponseType::EXECUTED,
                    fill, level->price_, remainingQuantity
                };
                matchingEngine_->sendClientResponse(&aggressiveResponse);  // SEND RESPONSE FOR AGRESSIVE MATCHER

                MEMarketUpdate marketUpdate{
                    tickerId, restingOrder->marketOrderId_, UpdateType::TRADE,
                    restingOrder->side_, restingOrder->price_, fill
                };
                matchingEngine_->sendMarketUpdate(&marketUpdate);          // SEND MARKET UPDATE

                if (restingOrder->remainingQuantity_ == 0) remove(restingOrder);
            }

            level = best_bid_;   // re-fetch: level may have been removed if it emptied
        }
    }

    return remainingQuantity;
}


auto MEOrderBook::addOrder(TickerId tickerId, ClientId clientId, OrderId clientOrderId,
            OrderType orderType, Price price, Quantity initialQuantity, Side side) noexcept -> void {
    
    OrderId marketOrderId = nextMarketOrderId++;

    Quantity remainingQuantity = match(tickerId, clientId, clientOrderId, marketOrderId, 
                                    orderType, price, initialQuantity, side);

    if (remainingQuantity > 0) {
        if (orderType == OrderType::GoodTillCancel) {
            insert(tickerId, clientId, clientOrderId, marketOrderId, orderType, price, remainingQuantity, side);

            MEClientResponse response{
                clientId, tickerId, clientOrderId, marketOrderId, ResponseType::ACCEPTED,
                0, price, remainingQuantity
            };
            matchingEngine_->sendClientResponse(&response);

            MEMarketUpdate marketUpdate{
                tickerId, marketOrderId, UpdateType::ADD,
                side, price, remainingQuantity
            };
            matchingEngine_->sendMarketUpdate(&marketUpdate);
        }
        else if (orderType == OrderType::FillAndKill) {
            MEClientResponse response{
                clientId, tickerId, clientOrderId, marketOrderId, ResponseType::CANCELED,
                0, price, remainingQuantity
            };
            matchingEngine_->sendClientResponse(&response);
        }
    }
}

auto MEOrderBook::cancelOrder(TickerId tickerId, ClientId clientId, OrderId clientOrderId) -> void {

    auto clientIt = orders_.find(clientId);
    if (UNLIKELY(clientIt == orders_.end())) {

        MEClientResponse response{
            clientId, tickerId, clientOrderId, OrderId_INVALID, ResponseType::CANCEL_REJECTED,
            0, Price_INVALID, 0
        };
        matchingEngine_->sendClientResponse(&response);
        return;
    }

    auto clientOrderIt = clientIt->second.find(clientOrderId);
    if (UNLIKELY(clientOrderIt == clientIt->second.end())) {

        MEClientResponse response{
            clientId, tickerId, clientOrderId, OrderId_INVALID, ResponseType::CANCEL_REJECTED,
            0, Price_INVALID, 0
        };
        matchingEngine_->sendClientResponse(&response);
        return;
    }

    MEOrder* order = clientOrderIt->second;

    MEMarketUpdate marketUpdate{
        tickerId, order->marketOrderId_, UpdateType::CANCEL,
        order->side_, order->price_, order->remainingQuantity_
    };
    matchingEngine_->sendMarketUpdate(&marketUpdate);

    MEClientResponse response{
        clientId, tickerId, clientOrderId, order->marketOrderId_, ResponseType::CANCELED,
        0, order->price_, 0
    };
    matchingEngine_->sendClientResponse(&response);

    remove(order);
}