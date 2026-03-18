#pragma once
#include <algorithm>
#include <array>
#include <functional>
#include <unordered_map>
#include <map>
#include "../types.h"
#include "QuantLink/Lib/memory/object_pool.h"
#include "QuantLink/Lib/logging/logger.h"
#include "QuantLink/Lib/common/macros.h"

constexpr size_t MAX_PRICE_LEVELS = 256000;

struct MEOrder {

    TickerId tickerId_;
    ClientId clientId_;
    OrderId clientOrderId_;      
    OrderId marketOrderId_;
    OrderType orderType_;

    Price price_;
    Quantity initialQuantity_;
    Quantity remainingQuantity_;
    Side side_;

    MEOrder* next_ = nullptr;
    MEOrder* prev_ = nullptr;

    MEOrder(TickerId tickerId, ClientId clientId, OrderId clientOrderId,
            OrderId marketOrderId, OrderType orderType, Price price,
            Quantity initialQuantity, Side side,
            MEOrder* next = nullptr, MEOrder* prev = nullptr) noexcept
        : tickerId_(tickerId),
          clientId_(clientId),
          clientOrderId_(clientOrderId),
          marketOrderId_(marketOrderId),
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
    Price price_;

    MEOrder* head = nullptr;
    MEOrder* tail = nullptr;

    PriceLevel* next_ = nullptr;
    PriceLevel* prev_ = nullptr;

    PriceLevel() = default;  
    PriceLevel (Price price) : price_(price)
    {}
    
};

class MatchingEngine;
class MEOrderBook {

private:
    TickerId tickerId_;
    MatchingEngine* matchingEngine_;
    quantlink::Logger* logger_;
    
    quantlink::ObjectPool<MEOrder> pool_;
    std::array<PriceLevel, MAX_PRICE_LEVELS> bids_{};
    std::array<PriceLevel, MAX_PRICE_LEVELS> asks_{};

    PriceLevel* best_bid_ = nullptr;
    PriceLevel* best_ask_ = nullptr;
    
    std::unordered_map<ClientId, std::unordered_map<OrderId, MEOrder*>> orders_;    //ClientId, ClientOrderId
    

    OrderId nextMarketOrderId = 1; 

    auto insertPricelevel (PriceLevel* priceLevel, Side side) {
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

    
    auto removePriceLevel (PriceLevel* priceLevel, Side side) {
        if (priceLevel->next_) priceLevel->next_->prev_ = priceLevel->prev_;
        if (priceLevel->prev_) priceLevel->prev_->next_ = priceLevel->next_;

        if (side == Side::BUY && best_bid_ == priceLevel) best_bid_ = priceLevel->next_;
        if (side == Side::SELL && best_ask_ == priceLevel) best_ask_ = priceLevel->next_;

        priceLevel->next_ = nullptr;
        priceLevel->prev_ = nullptr;
    }
    

    auto insert(TickerId tickerId, ClientId clientId, OrderId clientOrderId,
                OrderId marketOrderId,OrderType orderType, Price price,
                Quantity initialQuantity, Side side) {

        MEOrder* order = pool_.allocate(tickerId, clientId, clientOrderId,
                                        marketOrderId, orderType, price, initialQuantity, side);

        PriceLevel& priceLevel = (side == Side::BUY) ? bids_[price] : asks_[price];

        if (priceLevel.head == nullptr) {                              // Adding the Order First
            priceLevel.head = order;                                   // 1st Order 
            priceLevel.tail = order;
            insertPricelevel(&priceLevel, side);
        } 

        else {
            order->prev_ = priceLevel.tail;
            priceLevel.tail->next_ = order;
            priceLevel.tail = order;      
        }
        

        orders_[clientId][clientOrderId] = order;
    }


    auto remove(MEOrder* order) {

        Price price = order->price_;
        PriceLevel& priceLevel = (order->side_ == Side::BUY) ? bids_[price] : asks_[price];

        if (order->prev_ != nullptr) order->prev_->next_ = order->next_;    //Order is Head
        else priceLevel.head = order->next_;

        if (order->next_ != nullptr) order->next_->prev_ = order->prev_;
        else priceLevel.tail = order->prev_;                                //Order is Tail

        if (priceLevel.head == nullptr) {
            removePriceLevel(&priceLevel, order->side_);          //Only Order
        }

        
        auto& clientMap = orders_[order->clientId_];
        clientMap.erase(order->clientOrderId_);

        if (clientMap.empty()) {
            orders_.erase(order->clientId_);                            // Clean up the client to save RAM!
        }
        pool_.deallocate(order);
        
    }


    auto match(TickerId tickerId, ClientId clientId, OrderId clientOrderId, OrderId marketOrderId,
            OrderType orderType, Price price, Quantity initialQuantity, Side side) {

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

                    // TODO: notify matchingEngine_ of fill
                    // TODO: notify matchingEngine_ of fill

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

                    // TODO: notify matchingEngine_ of fill
                    // TODO: notify matchingEngine_ of fill

                    if (restingOrder->remainingQuantity_ == 0) remove(restingOrder);
                }

                level = best_bid_;   // re-fetch: level may have been removed if it emptied
            }
        }

        return remainingQuantity;
    }


public:

    explicit MEOrderBook(size_t size, TickerId tickerId, MatchingEngine* matchingEngine, quantlink::Logger* logger) : 
            tickerId_(tickerId), 
            matchingEngine_(matchingEngine), 
            logger_(logger),
            pool_(size)
    {}

    auto addOrder(TickerId tickerId, ClientId clientId, OrderId clientOrderId,
                OrderType orderType, Price price, Quantity initialQuantity, Side side) noexcept {
        
        OrderId marketOrderId = nextMarketOrderId++;

        Quantity remainingQuantity = match(tickerId, clientId, clientOrderId, marketOrderId, 
                                        orderType, price, initialQuantity, side);

        if (remainingQuantity > 0 && orderType == OrderType::GoodTillCancel ) {
            insert(tickerId, clientId, clientOrderId, marketOrderId, orderType, price, remainingQuantity, side);

            //TODO:notify matchingEngine_ of insertion
        }
    }

    auto cancelOrder (TickerId tickerId, ClientId clientId, OrderId clientOrderId){

        auto clientIt = orders_.find(clientId);
        if (UNLIKELY(clientIt == orders_.end())) {

            // TODO: notify matchingEngine_ CANCEL REJECTED
        }

        auto clientOrderIt = clientIt->second.find(clientOrderId);
        if (UNLIKELY(clientOrderIt == clientIt->second.end())) {

            // TODO: notify matching_ CANCEL REJECTED
        }

        MEOrder* order = clientOrderIt->second;
        // TODO: notify matching_ CANCEL MARKET
        // TODO: notify matching_ CANCEL RESPONSE

        remove(order);
    }





    
    
};