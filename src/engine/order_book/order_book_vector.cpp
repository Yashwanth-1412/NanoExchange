#include "src/engine/order_book/order_book_vector.h"
#include "src/engine/matching_engine.h"


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

inline auto sendClientResponse(MatchingEngine* me, const MEClientResponse* response) noexcept {
    if (me) me->sendClientResponse(response);
}

inline auto sendMarketUpdate(MatchingEngine* me, const MEMarketUpdate* update) noexcept {
    if (me) me->sendMarketUpdate(update);
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
            Quantity initialQuantity, Side side, const OrderToken& orderToken) -> void {

    MEOrder* order = pool_.allocate(tickerId, clientId, clientOrderId,
                                    marketOrderId, orderType, price, initialQuantity, side, orderToken);

    PriceLevel* priceLevel = (side == Side::BUY) ? &bids_[price] : &asks_[price];

    // Link price level into active list if this is the first order at this price
    if (priceLevel->head == nullptr) {
        insertPricelevel(priceLevel, side);
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
        OrderType orderType, Price price, Quantity initialQuantity, Side side,
        const OrderToken& orderToken) -> Quantity {

    Quantity remainingQuantity = initialQuantity;

    if (side == Side::BUY) {
        PriceLevel* level = best_ask_;

        while (level && remainingQuantity > 0) {
            if (price < level->price_) break;

            while (level->head && remainingQuantity > 0) {
                MEOrder* restingOrder = level->head;
                Quantity fill = std::min(remainingQuantity, restingOrder->remainingQuantity_);
                const OrderId matchId = matchingEngine_ ? matchingEngine_->nextMatchId() : OrderId_INVALID;

                remainingQuantity                -= fill;
                restingOrder->remainingQuantity_ -= fill;

                MEClientResponse restingResponse{
                    restingOrder->clientId_, tickerId, restingOrder->clientOrderId_, restingOrder->marketOrderId_,
                    restingOrder->side_, restingOrder->price_, restingOrder->initialQuantity_, restingOrder->orderType_,
                    ResponseType::EXECUTED,
                    fill, level->price_, restingOrder->remainingQuantity_, matchId
                };
                restingResponse.order_token_ = restingOrder->order_token_;
                sendClientResponse(matchingEngine_, &restingResponse);

                MEClientResponse aggressiveResponse{
                    clientId, tickerId, clientOrderId, marketOrderId,
                    side, price, initialQuantity, orderType,
                    ResponseType::EXECUTED,
                    fill, level->price_, remainingQuantity, matchId
                };
                aggressiveResponse.order_token_ = orderToken;
                sendClientResponse(matchingEngine_, &aggressiveResponse);

                MEMarketUpdate marketUpdate{
                    tickerId, restingOrder->marketOrderId_, UpdateType::TRADE,
                    restingOrder->side_, restingOrder->price_, fill
                };
                sendMarketUpdate(matchingEngine_, &marketUpdate);

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
                const OrderId matchId = matchingEngine_ ? matchingEngine_->nextMatchId() : OrderId_INVALID;

                remainingQuantity                -= fill;
                restingOrder->remainingQuantity_ -= fill;

                MEClientResponse restingResponse{
                    restingOrder->clientId_, tickerId, restingOrder->clientOrderId_, restingOrder->marketOrderId_,
                    restingOrder->side_, restingOrder->price_, restingOrder->initialQuantity_, restingOrder->orderType_,
                    ResponseType::EXECUTED,
                    fill, level->price_, restingOrder->remainingQuantity_, matchId
                };
                restingResponse.order_token_ = restingOrder->order_token_;
                sendClientResponse(matchingEngine_, &restingResponse);

                MEClientResponse aggressiveResponse{
                    clientId, tickerId, clientOrderId, marketOrderId,
                    side, price, initialQuantity, orderType,
                    ResponseType::EXECUTED,
                    fill, level->price_, remainingQuantity, matchId
                };
                aggressiveResponse.order_token_ = orderToken;
                sendClientResponse(matchingEngine_, &aggressiveResponse);

                MEMarketUpdate marketUpdate{
                    tickerId, restingOrder->marketOrderId_, UpdateType::TRADE,
                    restingOrder->side_, restingOrder->price_, fill
                };
                sendMarketUpdate(matchingEngine_, &marketUpdate);

                if (restingOrder->remainingQuantity_ == 0) remove(restingOrder);
            }

            level = best_bid_;   // re-fetch: level may have been removed if it emptied
        }
    }

    return remainingQuantity;
}


auto MEOrderBook::addOrder(TickerId tickerId, ClientId clientId, OrderId clientOrderId,
            OrderType orderType, Price price, Quantity initialQuantity, Side side,
            const OrderToken& orderToken) noexcept -> void {
    
    OrderId marketOrderId = nextMarketOrderId++;

    MEClientResponse accepted{
        clientId, tickerId, clientOrderId, marketOrderId,
        side, price, initialQuantity, orderType,
        ResponseType::ACCEPTED, 0, price, initialQuantity
    };
    accepted.order_token_ = orderToken;
    sendClientResponse(matchingEngine_, &accepted);

    Quantity remainingQuantity = match(tickerId, clientId, clientOrderId, marketOrderId, 
                                     orderType, price, initialQuantity, side, orderToken);

    if (remainingQuantity > 0) {
        if (orderType == OrderType::GoodTillCancel) {
            insert(tickerId, clientId, clientOrderId, marketOrderId, orderType, price, remainingQuantity, side,
                   orderToken);

            MEMarketUpdate marketUpdate{
                tickerId, marketOrderId, UpdateType::ADD,
                side, price, remainingQuantity
            };
            sendMarketUpdate(matchingEngine_, &marketUpdate);
        }
        else if (orderType == OrderType::FillAndKill) {
            MEClientResponse response{
                clientId, tickerId, clientOrderId, marketOrderId,
                side, price, initialQuantity, orderType,
                ResponseType::CANCELED,
                0, price, remainingQuantity
            };
            response.order_token_ = orderToken;
            sendClientResponse(matchingEngine_, &response);
        }
    }
}

auto MEOrderBook::addOrder(TickerId tickerId, ClientId clientId, OrderId clientOrderId,
                           OrderType orderType, Price price, Quantity initialQuantity, Side side) noexcept -> void {
    addOrder(tickerId, clientId, clientOrderId, orderType, price, initialQuantity, side, {});
}

auto MEOrderBook::cancelOrder(TickerId tickerId, ClientId clientId, OrderId clientOrderId, Quantity qty,
                              const OrderToken& orderToken) -> void {

    auto clientIt = orders_.find(clientId);
    if (UNLIKELY(clientIt == orders_.end())) {

        MEClientResponse response{
            clientId, tickerId, clientOrderId, OrderId_INVALID,
            Side::BUY, Price_INVALID, 0, OrderType::GoodTillCancel,
            ResponseType::CANCEL_REJECTED,
            0, Price_INVALID, 0
        };
        response.order_token_ = orderToken;
        sendClientResponse(matchingEngine_, &response);
        return;
    }

    auto clientOrderIt = clientIt->second.find(clientOrderId);
    if (UNLIKELY(clientOrderIt == clientIt->second.end())) {

        MEClientResponse response{
            clientId, tickerId, clientOrderId, OrderId_INVALID,
            Side::BUY, Price_INVALID, 0, OrderType::GoodTillCancel,
            ResponseType::CANCEL_REJECTED,
            0, Price_INVALID, 0
        };
        response.order_token_ = orderToken;
        sendClientResponse(matchingEngine_, &response);
        return;
    }

    MEOrder* order = clientOrderIt->second;

    const Quantity canceledQty = qty == 0 ? order->remainingQuantity_ : qty;
    if (canceledQty > order->remainingQuantity_) {
        MEClientResponse response{
            clientId, tickerId, clientOrderId, order->marketOrderId_,
            order->side_, order->price_, order->initialQuantity_, order->orderType_,
            ResponseType::CANCEL_REJECTED, 0, Price_INVALID, order->remainingQuantity_
        };
        response.order_token_ = order->order_token_;
        sendClientResponse(matchingEngine_, &response);
        return;
    }

    order->remainingQuantity_ -= canceledQty;

    MEMarketUpdate marketUpdate{
        tickerId, order->marketOrderId_, UpdateType::CANCEL,
        order->side_, order->price_, canceledQty
    };
    sendMarketUpdate(matchingEngine_, &marketUpdate);

    MEClientResponse response{
        clientId, tickerId, clientOrderId, order->marketOrderId_,
        order->side_, order->price_, order->initialQuantity_, order->orderType_,
        ResponseType::CANCELED,
        0, order->price_, order->remainingQuantity_
    };
    response.order_token_ = order->order_token_;
    sendClientResponse(matchingEngine_, &response);

    if (order->remainingQuantity_ == 0)
        remove(order);
}

auto MEOrderBook::cancelOrder(TickerId tickerId, ClientId clientId, OrderId clientOrderId) -> void {
    cancelOrder(tickerId, clientId, clientOrderId, 0, {});
}

auto MEOrderBook::replaceOrder(TickerId tickerId, ClientId clientId, OrderId clientOrderId,
                               OrderId newClientOrderId, OrderType orderType, Price price, Quantity qty,
                               const OrderToken& orderToken) -> void {
    const auto clientIt = orders_.find(clientId);
    if (clientIt == orders_.end()) {
        MEClientResponse response{clientId, tickerId, clientOrderId, OrderId_INVALID, Side::BUY, Price_INVALID, 0,
                                  OrderType::GoodTillCancel, ResponseType::CANCEL_REJECTED, 0, Price_INVALID, 0};
        response.order_token_ = orderToken;
        sendClientResponse(matchingEngine_, &response);
        return;
    }

    const auto orderIt = clientIt->second.find(clientOrderId);
    if (orderIt == clientIt->second.end() || newClientOrderId == 0 || qty == 0) {
        MEClientResponse response{clientId, tickerId, clientOrderId, OrderId_INVALID, Side::BUY, Price_INVALID, 0,
                                  OrderType::GoodTillCancel, ResponseType::CANCEL_REJECTED, 0, Price_INVALID, 0};
        response.order_token_ = orderToken;
        sendClientResponse(matchingEngine_, &response);
        return;
    }

    MEOrder* oldOrder = orderIt->second;
    const Side side = oldOrder->side_;
    const OrderId oldMarketOrderId = oldOrder->marketOrderId_;
    const OrderId newMarketOrderId = nextMarketOrderId++;
    remove(oldOrder);

    MEClientResponse replaced{clientId, tickerId, newClientOrderId, newMarketOrderId, side,
                              price, qty, orderType, ResponseType::MODIFIED, 0, price, qty};
    replaced.order_token_ = orderToken;
    sendClientResponse(matchingEngine_, &replaced);

    MEMarketUpdate modified{tickerId, oldMarketOrderId, UpdateType::MODIFY, side, price, qty,
                            newMarketOrderId};
    sendMarketUpdate(matchingEngine_, &modified);

    const Quantity remaining = match(tickerId, clientId, newClientOrderId, newMarketOrderId,
                                     orderType, price, qty, side, orderToken);
    if (remaining > 0 && orderType == OrderType::GoodTillCancel)
        insert(tickerId, clientId, newClientOrderId, newMarketOrderId, orderType, price, remaining, side,
               orderToken);
}
