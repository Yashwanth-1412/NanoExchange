#pragma once
#include <algorithm>
#include <array>
#include <functional>
#include <unordered_map>
#include <map>
#include "src/types.h"
#include "QuantLink/Lib/memory/object_pool.h"
#include "QuantLink/Lib/logging/logger.h"
#include "QuantLink/Lib/common/macros.h"

using namespace nanoexchange;

constexpr size_t MAX_PRICE_LEVELS = 256000;



class MatchingEngine;
class MEOrderBook {

private:
    TickerId tickerId_;
    MatchingEngine* matchingEngine_;
    quantlink::Logger* logger_;
    
    quantlink::ObjectPool<MEOrder> pool_;
    std::vector<PriceLevel> bids_;
    std::vector<PriceLevel> asks_;

    PriceLevel* best_bid_ = nullptr;
    PriceLevel* best_ask_ = nullptr;
    
    std::unordered_map<ClientId, std::unordered_map<OrderId, MEOrder*>> orders_;    //ClientId, ClientOrderId
    

    OrderId nextMarketOrderId = 1; 

    auto insertPricelevel(PriceLevel* priceLevel, Side side) -> void;

    auto removePriceLevel(PriceLevel* priceLevel, Side side) -> void;

    auto insert(TickerId tickerId, ClientId clientId, OrderId clientOrderId,
                OrderId marketOrderId, OrderType orderType, Price price,
                Quantity initialQuantity, Side side, const OrderToken& orderToken) -> void;

    auto remove(MEOrder* order) -> void;

    auto match(TickerId tickerId, ClientId clientId, OrderId clientOrderId, OrderId marketOrderId,
            OrderType orderType, Price price, Quantity initialQuantity, Side side,
            const OrderToken& orderToken) -> Quantity;


public:

    explicit MEOrderBook(size_t size, TickerId tickerId, MatchingEngine* matchingEngine, quantlink::Logger* logger);

    auto addOrder(TickerId tickerId, ClientId clientId, OrderId clientOrderId, OrderType orderType, Price price,
                  Quantity initialQuantity, Side side, const OrderToken& orderToken) noexcept -> void;

    auto cancelOrder(TickerId tickerId, ClientId clientId, OrderId clientOrderId, Quantity qty,
                     const OrderToken& orderToken) -> void;

    auto addOrder(TickerId tickerId, ClientId clientId, OrderId clientOrderId,
                  OrderType orderType, Price price, Quantity initialQuantity, Side side) noexcept -> void;

    auto cancelOrder(TickerId tickerId, ClientId clientId, OrderId clientOrderId) -> void;

    auto replaceOrder(TickerId tickerId, ClientId clientId, OrderId clientOrderId, OrderId newClientOrderId,
                      OrderType orderType, Price price, Quantity qty, const OrderToken& orderToken) -> void;


};
