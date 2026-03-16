#pragma once
#include <cstdint>
#include <functional>
#include <unordered_map>
#include <map>
#include "QuantLink/Lib/memory/object_pool.h"
#include "QuantLink/Lib/protocol/itch_messages.h"
#include "QuantLink/Lib/common/macros.h"

enum class Side {
    BUY,
    SELL
};

enum class OrderType {
    GoodTillCancel,
    FillAndKill
};

using OrderId = std::uint64_t;
using Price = std::uint64_t;
using Quantity = std::uint32_t; 

struct Order {  
    OrderType orderType_; 
    OrderId orderId_;
    Price  price_;
    Quantity initialQuantity_;
    Quantity remainingQuantity_;
    Side side_;

    Order* next_ = nullptr;
    Order* prev_ = nullptr;

    Order(OrderType orderType, OrderId orderId, Price price, Quantity initialQuantity, Side side,
           Order* next = nullptr, Order* prev = nullptr) noexcept : 
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
    Order* head = nullptr;
    Order* tail = nullptr;
};

class OrderBook {

private:
    size_t size_;
    quantlink::ObjectPool<Order> pool_;
    std::map<Price, PriceLevel, std::less<Price>> ask_;
    std::map<Price, PriceLevel, std::greater<Price>> bids_;
    std::unordered_map<OrderId, Order*> orders_;

    


public:

    explicit OrderBook(size_t size) : size_(size), pool_(size)
    {}
    


    
};