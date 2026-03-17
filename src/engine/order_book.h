#pragma once
#include <cstdint>
#include <functional>
#include <unordered_map>
#include <map>
#include "../types.h"
#include "QuantLink/Lib/memory/object_pool.h"
#include "QuantLink/Lib/protocol/itch_messages.h"
#include "QuantLink/Lib/common/macros.h"


struct MEOrder{  
    OrderType orderType_; 
    OrderId orderId_;
    Price  price_;
    Quantity initialQuantity_;
    Quantity remainingQuantity_;
    Side side_;

    MEOrder* next_ = nullptr;
    MEOrder* prev_ = nullptr;

    MEOrder(OrderType orderType, OrderId orderId, Price price, Quantity initialQuantity, Side side,
           MEOrder* next = nullptr, MEOrder* prev = nullptr) noexcept : 
           orderType_(orderType),
           orderId_(orderId),
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

class MEOrderBook {

private:
    size_t size_;
    quantlink::ObjectPool<MEOrder> pool_;
    std::map<Price, PriceLevel, std::less<Price>> asks_;
    std::map<Price, PriceLevel, std::greater<Price>> bids_;
    std::unordered_map<OrderId, MEOrder*> orders_;


public:

    explicit MEOrderBook(size_t size) : size_(size), pool_(size)
    {}
    
    auto addOrder(OrderType orderType, OrderId orderId, Price price, Quantity initialQuantity, Side side){
        
        MEOrder* order = pool_.allocate(orderType,orderId,price,initialQuantity,side);
        PriceLevel& priceLevel = (side == Side::BUY) ? bids_[price] : asks_[price];
        
        if(priceLevel.head == nullptr) {
            priceLevel.head = order;
            priceLevel.tail = order;
        }
        else {
            order->prev_ = priceLevel.tail;
            priceLevel.tail->next_ = order;
            priceLevel.tail = order;         //update the tail of the PriceLevel
        }

        orders_[orderId] = order;            //Add to lookup map
    }


    auto cancelOrder(OrderId orderId) {
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


    
};