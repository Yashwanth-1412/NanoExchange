#include "order_book_map.h"
#include "matching_engine.h"
#include "src/types.h"


MEOrderBook::MEOrderBook(size_t size, TickerId tickerId, MatchingEngine* matchingEngine/*, quantlink::Logger* logger*/) : 
        tickerId_(tickerId), 
        matchingEngine_(matchingEngine), 
        //logger_(logger),
        size_(size),
        pool_(size)
{}


auto MEOrderBook::insert(TickerId tickerId, ClientId clientId, OrderId clientOrderId,
            OrderId marketOrderId, OrderType orderType, Price price,
            Quantity initialQuantity, Side side) -> void {

    MEOrder* order = pool_.allocate(tickerId, clientId, clientOrderId,
                                    marketOrderId, orderType, price, initialQuantity, side);

    PriceLevel& priceLevel = (side == Side::BUY) ? bids_[price] : asks_[price];

    if (priceLevel.head == nullptr) {
        priceLevel.head = order;                                   // 1st Order
        priceLevel.tail = order;
    } 
    else {
        order->prev_ = priceLevel.tail;
        priceLevel.tail->next_ = order;
        priceLevel.tail = order;      
    }

    orders_[clientId][clientOrderId] = order;
}


auto MEOrderBook::remove(MEOrder* order) -> void {

    Price price = order->price_;
    PriceLevel& priceLevel = (order->side_ == Side::BUY) ? bids_[price] : asks_[price];

    if (order->prev_ != nullptr) order->prev_->next_ = order->next_;    //Order is Head
    else priceLevel.head = order->next_;

    if (order->next_ != nullptr) order->next_->prev_ = order->prev_;
    else priceLevel.tail = order->prev_;                                //Order is Tail

    if (priceLevel.head == nullptr) {
        if (order->side_ == Side::BUY) bids_.erase(price);          //Only Order
        else asks_.erase(price);
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
    

    auto matchAgainst = [&](auto& oppositeSide) {
        for (auto it = oppositeSide.begin(); it != oppositeSide.end() && remainingQuantity > 0;) {
            Price levelPrice = it->first;

            if ((side == Side::BUY && price < levelPrice) ||
                (side == Side::SELL && price > levelPrice)) break;

            PriceLevel& priceLevel = it->second;

            while (priceLevel.head != nullptr && remainingQuantity > 0) {
                MEOrder* restingOrder = priceLevel.head;
                Quantity fill = std::min(remainingQuantity, restingOrder->remainingQuantity_);

                remainingQuantity -= fill;
                restingOrder->remainingQuantity_ -= fill;

                MEClientResponse restingResponse{
                    restingOrder->clientId_, tickerId, restingOrder->clientOrderId_, restingOrder->marketOrderId_,
                    restingOrder->side_, restingOrder->price_, restingOrder->initialQuantity_, restingOrder->orderType_,
                    ResponseType::EXECUTED, 
                    fill, levelPrice, restingOrder->remainingQuantity_
                };

                matchingEngine_->sendClientResponse(&restingResponse);     // SEND RESPONSE FOR RESTING ORDER

                MEClientResponse agressiveResponse{
                    clientId, tickerId, clientOrderId, marketOrderId, 
                    side, price, initialQuantity, orderType,
                    ResponseType::EXECUTED, fill,
                    levelPrice, remainingQuantity
                };

                matchingEngine_->sendClientResponse(&agressiveResponse);  // SEND RESPONSE FOR AGRESSIVE MATCHER

                MEMarketUpdate restingMarketUpdate {
                    tickerId, restingOrder->marketOrderId_, UpdateType::TRADE,
                    restingOrder->side_, restingOrder->price_, fill
                };

                matchingEngine_->sendMarketUpdate(&restingMarketUpdate);    // SEND MARKET UPDATE
                

                if (restingOrder->remainingQuantity_ == 0) remove(restingOrder);
            }

            it = oppositeSide.begin(); //reevaluate after removals
        }
    };

    if (side == Side::BUY) matchAgainst(asks_);
    else matchAgainst(bids_);


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
                clientId, tickerId, clientOrderId, marketOrderId, 
                side, price, initialQuantity, orderType,
                ResponseType::ACCEPTED,
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
                clientId, tickerId, clientOrderId, marketOrderId, 
                side, price, initialQuantity, orderType,
                ResponseType::CANCELED,
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
            clientId, tickerId, clientOrderId, OrderId_INVALID, 
            Side::BUY, Price_INVALID, 0, OrderType::GoodTillCancel,
            ResponseType::CANCEL_REJECTED,
            0, Price_INVALID, 0
        };
        matchingEngine_->sendClientResponse(&response);
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
        clientId, tickerId, clientOrderId, order->marketOrderId_, 
        order->side_, order->price_, order->initialQuantity_, order->orderType_,
        ResponseType::CANCELED,
        0, order->price_, 0
    };
    matchingEngine_->sendClientResponse(&response);

    remove(order);
}