#pragma once
#include <algorithm>
#include <array>
#include <vector>
#include <unordered_map>
#include <map>
#include "../src/types.h"
#include "QuantLink/Lib/memory/object_pool_list.h"
#include "QuantLink/Lib/logging/logger.h"
#include "QuantLink/Lib/common/macros.h"

constexpr size_t MAX_PRICE_LEVELS = 256000;

class MatchingEngine;

class MEOrderBook_PointerPool {
private:
    TickerId           tickerId_;
    MatchingEngine* matchingEngine_;
    quantlink::Logger* logger_;

    quantlink::ObjectPool<MEOrder>    pool_;
    quantlink::ObjectPool<PriceLevel> priceLevelPool_;

    std::array<PriceLevel*, MAX_PRICE_LEVELS> bids_{};
    std::array<PriceLevel*, MAX_PRICE_LEVELS> asks_{};

    PriceLevel* best_bid_ = nullptr;
    PriceLevel* best_ask_ = nullptr;

    std::unordered_map<ClientId, std::unordered_map<OrderId, MEOrder*>> orders_;
    OrderId nextMarketOrderId_ = 1;

    void insertPricelevel(PriceLevel* pl, Side side) {
        if (side == Side::BUY) {
            if (!best_bid_) { best_bid_ = pl; return; }
            if (pl->price_ > best_bid_->price_) {
                pl->next_ = best_bid_; best_bid_->prev_ = pl; best_bid_ = pl; return;
            }
            PriceLevel* c = best_bid_;
            while (c->next_ && c->next_->price_ > pl->price_) c = c->next_;
            pl->next_ = c->next_;
            if (c->next_) c->next_->prev_ = pl;
            c->next_ = pl; pl->prev_ = c;
        } else {
            if (!best_ask_) { best_ask_ = pl; return; }
            if (pl->price_ < best_ask_->price_) {
                pl->next_ = best_ask_; best_ask_->prev_ = pl; best_ask_ = pl; return;
            }
            PriceLevel* c = best_ask_;
            while (c->next_ && c->next_->price_ < pl->price_) c = c->next_;
            pl->next_ = c->next_;
            if (c->next_) c->next_->prev_ = pl;
            c->next_ = pl; pl->prev_ = c;
        }
    }

    void removePriceLevel(PriceLevel* pl, Side side) {
        if (pl->next_) pl->next_->prev_ = pl->prev_;
        if (pl->prev_) pl->prev_->next_ = pl->next_;
        if (side == Side::BUY  && best_bid_ == pl) best_bid_ = pl->next_;
        if (side == Side::SELL && best_ask_ == pl) best_ask_ = pl->next_;
        if (side == Side::BUY) bids_[pl->price_] = nullptr;
        else                   asks_[pl->price_] = nullptr;
        priceLevelPool_.deallocate(pl);
    }

    void insert(TickerId tid, ClientId cid, OrderId coid, OrderId moid,
                OrderType ot, Price price, Quantity qty, Side side) {
        MEOrder* order = pool_.allocate(tid, cid, coid, moid, ot, price, qty, side);
        PriceLevel*& pl = (side == Side::BUY) ? bids_[price] : asks_[price];
        if (UNLIKELY(pl == nullptr)) {
            pl = priceLevelPool_.allocate(price);
            insertPricelevel(pl, side);
        }
        if (!pl->head) { pl->head = order; pl->tail = order; }
        else { order->prev_ = pl->tail; pl->tail->next_ = order; pl->tail = order; }
        orders_[cid][coid] = order;
    }

    void remove(MEOrder* order) {
        PriceLevel* pl = (order->side_ == Side::BUY)
                         ? bids_[order->price_] : asks_[order->price_];
        if (order->prev_) order->prev_->next_ = order->next_;
        else              pl->head = order->next_;
        if (order->next_) order->next_->prev_ = order->prev_;
        else              pl->tail = order->prev_;
        if (!pl->head) removePriceLevel(pl, order->side_);
        auto& cm = orders_[order->clientId_];
        cm.erase(order->clientOrderId_);
        if (cm.empty()) orders_.erase(order->clientId_);
        pool_.deallocate(order);
    }

    Quantity match(Price price, Quantity qty, Side side) {
        Quantity rem = qty;
        if (side == Side::BUY) {
            for (PriceLevel* lv = best_ask_; lv && rem > 0; lv = best_ask_) {
                if (price < lv->price_) break;
                while (lv->head && rem > 0) {
                    MEOrder* r = lv->head;
                    Quantity fill = std::min(rem, r->remainingQuantity_);
                    rem -= fill; r->remainingQuantity_ -= fill;
                    if (r->remainingQuantity_ == 0) remove(r);
                }
            }
        } else {
            for (PriceLevel* lv = best_bid_; lv && rem > 0; lv = best_bid_) {
                if (price > lv->price_) break;
                while (lv->head && rem > 0) {
                    MEOrder* r = lv->head;
                    Quantity fill = std::min(rem, r->remainingQuantity_);
                    rem -= fill; r->remainingQuantity_ -= fill;
                    if (r->remainingQuantity_ == 0) remove(r);
                }
            }
        }
        return rem;
    }

public:
    static constexpr const char* NAME = "A: PointerPool (original)";

    explicit MEOrderBook_PointerPool(size_t poolSz, TickerId tid,
                                     MatchingEngine* me, quantlink::Logger* log)
        : tickerId_(tid), matchingEngine_(me), logger_(log),
          pool_(poolSz), priceLevelPool_(MAX_PRICE_LEVELS)
    { bids_.fill(nullptr); asks_.fill(nullptr); }

    void addOrder(TickerId tid, ClientId cid, OrderId coid,
                  OrderType ot, Price price, Quantity qty, Side side) noexcept {
        OrderId moid  = nextMarketOrderId_++;
        Quantity rem  = match(price, qty, side);
        if (rem > 0 && ot == OrderType::GoodTillCancel)
            insert(tid, cid, coid, moid, ot, price, rem, side);
    }

    void cancelOrder(TickerId, ClientId cid, OrderId coid) {
        auto cit = orders_.find(cid);
        if (UNLIKELY(cit == orders_.end())) return;
        auto oit = cit->second.find(coid);
        if (UNLIKELY(oit == cit->second.end())) return;
        remove(oit->second);
    }
};

class MEOrderBook_DirectVector {
private:
    TickerId           tickerId_;
    MatchingEngine* matchingEngine_;
    quantlink::Logger* logger_;

    quantlink::ObjectPool<MEOrder> pool_;

    std::vector<PriceLevel> bidLevels_;
    std::vector<PriceLevel> askLevels_;

    PriceLevel* best_bid_ = nullptr;
    PriceLevel* best_ask_ = nullptr;

    std::unordered_map<ClientId, std::unordered_map<OrderId, MEOrder*>> orders_;
    OrderId nextMarketOrderId_ = 1;

    void insertPricelevel(PriceLevel* pl, Side side) {
        if (side == Side::BUY) {
            if (!best_bid_) { best_bid_ = pl; return; }
            if (pl->price_ > best_bid_->price_) {
                pl->next_ = best_bid_; best_bid_->prev_ = pl; best_bid_ = pl; return;
            }
            PriceLevel* c = best_bid_;
            while (c->next_ && c->next_->price_ > pl->price_) c = c->next_;
            pl->next_ = c->next_;
            if (c->next_) c->next_->prev_ = pl;
            c->next_ = pl; pl->prev_ = c;
        } else {
            if (!best_ask_) { best_ask_ = pl; return; }
            if (pl->price_ < best_ask_->price_) {
                pl->next_ = best_ask_; best_ask_->prev_ = pl; best_ask_ = pl; return;
            }
            PriceLevel* c = best_ask_;
            while (c->next_ && c->next_->price_ < pl->price_) c = c->next_;
            pl->next_ = c->next_;
            if (c->next_) c->next_->prev_ = pl;
            c->next_ = pl; pl->prev_ = c;
        }
    }

    void removePriceLevel(PriceLevel* pl, Side side) {
        if (pl->next_) pl->next_->prev_ = pl->prev_;
        if (pl->prev_) pl->prev_->next_ = pl->next_;
        if (side == Side::BUY  && best_bid_ == pl) best_bid_ = pl->next_;
        if (side == Side::SELL && best_ask_ == pl) best_ask_ = pl->next_;
        pl->next_ = nullptr;
        pl->prev_ = nullptr;
        pl->head  = nullptr;
        pl->tail  = nullptr;
    }

    void insert(TickerId tid, ClientId cid, OrderId coid, OrderId moid,
                OrderType ot, Price price, Quantity qty, Side side) {
        MEOrder* order = pool_.allocate(tid, cid, coid, moid, ot, price, qty, side);
        PriceLevel& pl = (side == Side::BUY) ? bidLevels_[price] : askLevels_[price];
        if (pl.head == nullptr)
            insertPricelevel(&pl, side);
        if (!pl.head) { pl.head = order; pl.tail = order; }
        else { order->prev_ = pl.tail; pl.tail->next_ = order; pl.tail = order; }
        orders_[cid][coid] = order;
    }

    void remove(MEOrder* order) {
        PriceLevel& pl = (order->side_ == Side::BUY)
                         ? bidLevels_[order->price_] : askLevels_[order->price_];
        if (order->prev_) order->prev_->next_ = order->next_;
        else              pl.head = order->next_;
        if (order->next_) order->next_->prev_ = order->prev_;
        else              pl.tail = order->prev_;
        if (!pl.head) removePriceLevel(&pl, order->side_);
        auto& cm = orders_[order->clientId_];
        cm.erase(order->clientOrderId_);
        if (cm.empty()) orders_.erase(order->clientId_);
        pool_.deallocate(order);
    }

    Quantity match(Price price, Quantity qty, Side side) {
        Quantity rem = qty;
        if (side == Side::BUY) {
            for (PriceLevel* lv = best_ask_; lv && rem > 0; lv = best_ask_) {
                if (price < lv->price_) break;
                while (lv->head && rem > 0) {
                    MEOrder* r = lv->head;
                    Quantity fill = std::min(rem, r->remainingQuantity_);
                    rem -= fill; r->remainingQuantity_ -= fill;
                    if (r->remainingQuantity_ == 0) remove(r);
                }
            }
        } else {
            for (PriceLevel* lv = best_bid_; lv && rem > 0; lv = best_bid_) {
                if (price > lv->price_) break;
                while (lv->head && rem > 0) {
                    MEOrder* r = lv->head;
                    Quantity fill = std::min(rem, r->remainingQuantity_);
                    rem -= fill; r->remainingQuantity_ -= fill;
                    if (r->remainingQuantity_ == 0) remove(r);
                }
            }
        }
        return rem;
    }

public:
    static constexpr const char* NAME = "B: DirectVector (no PL pool)";

    explicit MEOrderBook_DirectVector(size_t poolSz, TickerId tid,
                                      MatchingEngine* me, quantlink::Logger* log)
        : tickerId_(tid), matchingEngine_(me), logger_(log),
          pool_(poolSz),
          bidLevels_(MAX_PRICE_LEVELS),
          askLevels_(MAX_PRICE_LEVELS)
    {
        for (Price p = 0; p < (Price)MAX_PRICE_LEVELS; ++p) {
            bidLevels_[p].price_ = p;
            askLevels_[p].price_ = p;
        }
    }

    void addOrder(TickerId tid, ClientId cid, OrderId coid,
                  OrderType ot, Price price, Quantity qty, Side side) noexcept {
        OrderId moid  = nextMarketOrderId_++;
        Quantity rem  = match(price, qty, side);
        if (rem > 0 && ot == OrderType::GoodTillCancel)
            insert(tid, cid, coid, moid, ot, price, rem, side);
    }

    void cancelOrder(TickerId, ClientId cid, OrderId coid) {
        auto cit = orders_.find(cid);
        if (UNLIKELY(cit == orders_.end())) return;
        auto oit = cit->second.find(coid);
        if (UNLIKELY(oit == cit->second.end())) return;
        remove(oit->second);
    }
};

class MEOrderBook_PointerPreAlloc {
private:
    TickerId           tickerId_;
    MatchingEngine* matchingEngine_;
    quantlink::Logger* logger_;

    quantlink::ObjectPool<MEOrder> pool_;

    std::vector<PriceLevel>                   levelStorage_;
    std::array<PriceLevel*, MAX_PRICE_LEVELS> bids_{};
    std::array<PriceLevel*, MAX_PRICE_LEVELS> asks_{};

    PriceLevel* best_bid_ = nullptr;
    PriceLevel* best_ask_ = nullptr;

    std::unordered_map<ClientId, std::unordered_map<OrderId, MEOrder*>> orders_;
    OrderId nextMarketOrderId_ = 1;

    void insertPricelevel(PriceLevel* pl, Side side) {
        if (side == Side::BUY) {
            if (!best_bid_) { best_bid_ = pl; return; }
            if (pl->price_ > best_bid_->price_) {
                pl->next_ = best_bid_; best_bid_->prev_ = pl; best_bid_ = pl; return;
            }
            PriceLevel* c = best_bid_;
            while (c->next_ && c->next_->price_ > pl->price_) c = c->next_;
            pl->next_ = c->next_;
            if (c->next_) c->next_->prev_ = pl;
            c->next_ = pl; pl->prev_ = c;
        } else {
            if (!best_ask_) { best_ask_ = pl; return; }
            if (pl->price_ < best_ask_->price_) {
                pl->next_ = best_ask_; best_ask_->prev_ = pl; best_ask_ = pl; return;
            }
            PriceLevel* c = best_ask_;
            while (c->next_ && c->next_->price_ < pl->price_) c = c->next_;
            pl->next_ = c->next_;
            if (c->next_) c->next_->prev_ = pl;
            c->next_ = pl; pl->prev_ = c;
        }
    }

    void removePriceLevel(PriceLevel* pl, Side side) {
        if (pl->next_) pl->next_->prev_ = pl->prev_;
        if (pl->prev_) pl->prev_->next_ = pl->next_;
        if (side == Side::BUY  && best_bid_ == pl) best_bid_ = pl->next_;
        if (side == Side::SELL && best_ask_ == pl) best_ask_ = pl->next_;
        pl->next_ = nullptr;
        pl->prev_ = nullptr;
        pl->head  = nullptr;
        pl->tail  = nullptr;
    }

    void insert(TickerId tid, ClientId cid, OrderId coid, OrderId moid,
                OrderType ot, Price price, Quantity qty, Side side) {
        MEOrder* order = pool_.allocate(tid, cid, coid, moid, ot, price, qty, side);
        PriceLevel* pl = (side == Side::BUY) ? bids_[price] : asks_[price];
        if (pl->head == nullptr)
            insertPricelevel(pl, side);
        if (!pl->head) { pl->head = order; pl->tail = order; }
        else { order->prev_ = pl->tail; pl->tail->next_ = order; pl->tail = order; }
        orders_[cid][coid] = order;
    }

    void remove(MEOrder* order) {
        PriceLevel* pl = (order->side_ == Side::BUY)
                         ? bids_[order->price_] : asks_[order->price_];
        if (order->prev_) order->prev_->next_ = order->next_;
        else              pl->head = order->next_;
        if (order->next_) order->next_->prev_ = order->prev_;
        else              pl->tail = order->prev_;
        if (!pl->head) removePriceLevel(pl, order->side_);
        auto& cm = orders_[order->clientId_];
        cm.erase(order->clientOrderId_);
        if (cm.empty()) orders_.erase(order->clientId_);
        pool_.deallocate(order);
    }

    Quantity match(Price price, Quantity qty, Side side) {
        Quantity rem = qty;
        if (side == Side::BUY) {
            for (PriceLevel* lv = best_ask_; lv && rem > 0; lv = best_ask_) {
                if (price < lv->price_) break;
                while (lv->head && rem > 0) {
                    MEOrder* r = lv->head;
                    Quantity fill = std::min(rem, r->remainingQuantity_);
                    rem -= fill; r->remainingQuantity_ -= fill;
                    if (r->remainingQuantity_ == 0) remove(r);
                }
            }
        } else {
            for (PriceLevel* lv = best_bid_; lv && rem > 0; lv = best_bid_) {
                if (price > lv->price_) break;
                while (lv->head && rem > 0) {
                    MEOrder* r = lv->head;
                    Quantity fill = std::min(rem, r->remainingQuantity_);
                    rem -= fill; r->remainingQuantity_ -= fill;
                    if (r->remainingQuantity_ == 0) remove(r);
                }
            }
        }
        return rem;
    }

public:
    static constexpr const char* NAME = "C: PointerPreAlloc (ptr no pool)";

    explicit MEOrderBook_PointerPreAlloc(size_t poolSz, TickerId tid,
                                         MatchingEngine* me, quantlink::Logger* log)
        : tickerId_(tid), matchingEngine_(me), logger_(log),
          pool_(poolSz)
    {
        levelStorage_.resize(MAX_PRICE_LEVELS * 2);
        for (Price p = 0; p < (Price)MAX_PRICE_LEVELS; ++p) {
            levelStorage_[p].price_                    = p;
            levelStorage_[p + MAX_PRICE_LEVELS].price_ = p;
            bids_[p] = &levelStorage_[p];
            asks_[p] = &levelStorage_[p + MAX_PRICE_LEVELS];
        }
    }

    void addOrder(TickerId tid, ClientId cid, OrderId coid,
                  OrderType ot, Price price, Quantity qty, Side side) noexcept {
        OrderId moid  = nextMarketOrderId_++;
        Quantity rem  = match(price, qty, side);
        if (rem > 0 && ot == OrderType::GoodTillCancel)
            insert(tid, cid, coid, moid, ot, price, rem, side);
    }

    void cancelOrder(TickerId, ClientId cid, OrderId coid) {
        auto cit = orders_.find(cid);
        if (UNLIKELY(cit == orders_.end())) return;
        auto oit = cit->second.find(coid);
        if (UNLIKELY(oit == cit->second.end())) return;
        remove(oit->second);
    }
};

class MEOrderBook_StdMap {
private:
    TickerId tickerId_;
    MatchingEngine* matchingEngine_;
    quantlink::Logger* logger_;
    
    size_t size_;
    quantlink::ObjectPool<MEOrder> pool_;
    std::map<Price, PriceLevel, std::less<Price>> asks_;
    std::map<Price, PriceLevel, std::greater<Price>> bids_;
    std::unordered_map<ClientId, std::unordered_map<OrderId, MEOrder*>> orders_;    
    
    OrderId nextMarketOrderId = 1; 

    auto insert(TickerId tickerId, ClientId clientId, OrderId clientOrderId,
                OrderId marketOrderId,OrderType orderType, Price price,
                Quantity initialQuantity, Side side) {

        MEOrder* order = pool_.allocate(tickerId, clientId, clientOrderId,
                                        marketOrderId, orderType, price, initialQuantity, side);

        PriceLevel& priceLevel = (side == Side::BUY) ? bids_[price] : asks_[price];

        if (priceLevel.head == nullptr) {
            priceLevel.head = order;                                   
            priceLevel.tail = order;
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

        if (order->prev_ != nullptr) order->prev_->next_ = order->next_;    
        else priceLevel.head = order->next_;

        if (order->next_ != nullptr) order->next_->prev_ = order->prev_;
        else priceLevel.tail = order->prev_;                                

        if (priceLevel.head == nullptr) {
            if (order->side_ == Side::BUY) bids_.erase(price);          
            else asks_.erase(price);
        }
        
        auto& clientMap = orders_[order->clientId_];
        clientMap.erase(order->clientOrderId_);

        if (clientMap.empty()) {
            orders_.erase(order->clientId_);                            
        }
        pool_.deallocate(order);
    }

    auto match(TickerId tickerId, ClientId clientId, OrderId clientOrderId, OrderId marketOrderId,
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

                    if (restingOrder->remainingQuantity_ == 0) remove(restingOrder);
                }

                it = oppositeSide.begin(); 
            }
        };

        if (side == Side::BUY) matchAgainst(asks_);
        else matchAgainst(bids_);

        return remainingQuantity;
    }

public:
    static constexpr const char* NAME = "D: StdMap (baseline)";

    explicit MEOrderBook_StdMap(size_t size, TickerId tickerId, MatchingEngine* matchingEngine, quantlink::Logger* logger) : 
            tickerId_(tickerId), 
            matchingEngine_(matchingEngine), 
            logger_(logger),
            size_(size),
            pool_(size)
    {}

    auto addOrder(TickerId tickerId, ClientId clientId, OrderId clientOrderId,
                OrderType orderType, Price price, Quantity initialQuantity, Side side) noexcept {
        
        OrderId marketOrderId = nextMarketOrderId++;

        Quantity remainingQuantity = match(tickerId, clientId, clientOrderId, marketOrderId, 
                                        orderType, price, initialQuantity, side);

        if (remainingQuantity > 0 && orderType == OrderType::GoodTillCancel ) {
            insert(tickerId, clientId, clientOrderId, marketOrderId, orderType, price, remainingQuantity, side);
        }
    }

    auto cancelOrder (TickerId tickerId, ClientId clientId, OrderId clientOrderId){
        auto clientIt = orders_.find(clientId);
        if (UNLIKELY(clientIt == orders_.end())) {
            return;
        }

        auto clientOrderIt = clientIt->second.find(clientOrderId);
        if (UNLIKELY(clientOrderIt == clientIt->second.end())) {
            return;
        }

        MEOrder* order = clientOrderIt->second;
        remove(order);
    }
};