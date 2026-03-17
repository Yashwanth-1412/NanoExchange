#pragma once
#include <algorithm>
#include <cstdint>
#include <functional>
#include <unordered_map>
#include <map>
#include "../types.h"
#include "QuantLink/Lib/memory/object_pool.h"
#include "QuantLink/Lib/logging/logger.h"
#include "QuantLink/Lib/common/macros.h"


struct MEOrder {

    TickerId tickerId_;
    ClientId clientId_;
    OrderId clientOrderId_;      
    OrderId orderId_;
    OrderType orderType_;

    Price price_;
    Quantity initialQuantity_;
    Quantity remainingQuantity_;
    Side side_;

    MEOrder* next_ = nullptr;
    MEOrder* prev_ = nullptr;

    MEOrder(TickerId tickerId, ClientId clientId, OrderId clientOrderId,
            OrderId orderId, OrderType orderType, Price price,
            Quantity initialQuantity, Side side,
            MEOrder* next = nullptr, MEOrder* prev = nullptr) noexcept
        : tickerId_(tickerId),
          clientId_(clientId),
          clientOrderId_(clientOrderId),
          orderId_(orderId),
          orderType_(orderType),
          price_(price),
          initialQuantity_(initialQuantity),
          remainingQuantity_(initialQuantity),
          side_(side),
          next_(next),
          prev_(prev) 
    {}
};

struct PriceLevel {
    MEOrder* head = nullptr;
    MEOrder* tail = nullptr;
};

class MatchingEngine;
class MEOrderBook {

private:
    TickerId tickerId_;
    MatchingEngine* matchingEngine_;
    quantlink::Logger* logger_;
    
    size_t size_;
    quantlink::ObjectPool<MEOrder> pool_;
    std::map<Price, PriceLevel, std::less<Price>> asks_;
    std::map<Price, PriceLevel, std::greater<Price>> bids_;
    std::unordered_map<OrderId, MEOrder*> orders_;

    auto insert(TickerId tickerId, ClientId clientId, OrderId clientOrderId,
                OrderId orderId,OrderType orderType, Price price,
                Quantity initialQuantity, Side side) {

        MEOrder* order = pool_.allocate(tickerId, clientId, clientOrderId,
                                        orderId, orderType, price, initialQuantity, side);

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

        orders_[orderId] = order;
    }


    auto remove(OrderId orderId) {
        auto it = orders_.find(orderId);
        if (it == orders_.end()) return;
        
        MEOrder* order = it->second; 
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

        pool_.deallocate(order);
        orders_.erase(orderId);
    }


    auto match(TickerId tickerId, ClientId clientId, OrderId clientOrderId, OrderId orderId,
                OrderType orderType, Price price, Quantity initialQuantity, Side side) {
        
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

                    //TODO:notify matchingEngine_ of fill here ----------------------------------------------------------------------
                    //TODO:notify matchingEngine_ of fill here ----------------------------------------------------------------------

                    if (restingOrder->remainingQuantity_ == 0) remove(restingOrder->orderId_);
                }

                it = oppositeSide.begin(); //reevaluate after removals
            }
        };

        if (side == Side::BUY) matchAgainst(asks_);
        else matchAgainst(bids_);

        return remainingQuantity;
    }


public:

    explicit MEOrderBook(size_t size, TickerId tickerId, MatchingEngine* matchingEngine, quantlink::Logger* logger) : 
            tickerId_(tickerId), 
            matchingEngine_(matchingEngine), 
            logger_(logger),
            size_(size),
            pool_(size)
    {}

    auto addOrder(TickerId tickerId, ClientId clientId, OrderId clientOrderId, OrderId orderId,
                OrderType orderType, Price price, Quantity initialQuantity, Side side) noexcept {


        Quantity remainingQuantity = match(tickerId, clientId, clientOrderId, orderId, 
                                        orderType, price, initialQuantity, side);

        if (remainingQuantity > 0 && orderType == OrderType::GoodTillCancel ) {
            insert(tickerId, clientId, clientOrderId, orderId,orderType, price, remainingQuantity, side);

            //TODO:notify matchingEngine_ of insertion
        }
    }

    auto cancelOrder (){}


    



    
};