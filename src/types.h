#pragma once
#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>

constexpr size_t ITCH_MAX_MSG_SIZE = 40;
constexpr size_t OUCH_TOKEN_LEN   = 14;

using OrderId  = std::uint64_t;
using Price    = std::uint64_t;
using Quantity = std::uint32_t;
using ClientId = std::uint64_t;
using TickerId = std::uint32_t;

// The raw OUCH order token exactly as the client sent it. It travels with the
// request, the resting order and every response, so the outbound encoder never
// has to resolve an internal id back into a token.
using OrderToken = std::array<char, OUCH_TOKEN_LEN>;

constexpr Price Price_INVALID = std::numeric_limits<Price>::max();
constexpr OrderId OrderId_INVALID = std::numeric_limits<OrderId>::max();

// 1-byte underlying types. Each of these holds a handful of values, and the
// padding this reclaims is precisely what lets the structs below carry the
// 14-byte token without growing - see the static_asserts at the end of the file.
enum class Side : std::uint8_t { BUY, SELL };
enum class OrderType : std::uint8_t { GoodTillCancel, FillAndKill };
enum class ClientRequestType : std::uint8_t { NEW, CANCEL, MODIFY };
enum class ResponseType : std::uint8_t { ACCEPTED, EXECUTED, CANCELED, CANCEL_REJECTED, MODIFIED };
enum class UpdateType : std::uint8_t { ADD, CANCEL, MODIFY, TRADE, SNAPSHOT_START, CLEAR, SNAPSHOT_END };

// Fields in every struct below are ordered widest-first so the trailing token
// lands in what used to be tail padding. Keep that ordering when adding fields.
struct MEClientRequest {
    ClientId client_id_;
    OrderId client_order_id_;     // The internal id assigned by the gateway
    OrderId new_client_order_id_; // Used FOR OUCH REPLACE ORDERS
    Price price_;
    TickerId ticker_id_;          //Stock name
    Quantity qty_;
    ClientRequestType action_;
    OrderType type_;
    Side side_;
    OrderToken order_token_{};

    // Default constructor
    MEClientRequest() = default;

    // Parameterized constructor
    MEClientRequest(ClientRequestType action, ClientId client_id, TickerId ticker_id, 
                    OrderId client_order_id, OrderType type, Side side, 
                    Price price, Quantity qty)
        : client_id_(client_id), client_order_id_(client_order_id),
          price_(price), ticker_id_(ticker_id), qty_(qty),
          action_(action), type_(type), side_(side) {}
};

// QUEUE 2: Outbound Private (Engine -> Gateway -> AlphaTrader)
struct MEClientResponse {
    ClientId client_id_;
    OrderId client_order_id_; // So AlphaTrader knows which order we mean
    OrderId market_order_id_; // The Exchange's official ID for this order
    Price price_;             // Required to echo the limit price
    Price execution_price_;
    OrderId match_id_ = OrderId_INVALID;

    TickerId ticker_id_;
    Quantity qty_;            // Required to echo the TOTAL original shares
    Quantity executed_qty_;
    Quantity leaves_qty_;

    Side side_;               // Required to echo 'B' or 'S'
    OrderType type_;          // Required to derive Time-In-Force (0 or 99998)
    ResponseType status_;

    OrderToken order_token_{};

    // Default constructor
    MEClientResponse() = default;

    // Parameterized constructor
    MEClientResponse(ClientId client_id, TickerId ticker_id, OrderId client_order_id, 
                      OrderId market_order_id, Side side, Price price, Quantity qty, OrderType type,
                      ResponseType status, Quantity executed_qty,
                      Price execution_price, Quantity leaves_qty,
                      OrderId match_id = OrderId_INVALID)
        : client_id_(client_id), client_order_id_(client_order_id),
          market_order_id_(market_order_id), price_(price),
          execution_price_(execution_price), match_id_(match_id),
          ticker_id_(ticker_id), qty_(qty), executed_qty_(executed_qty),
          leaves_qty_(leaves_qty), side_(side), type_(type), status_(status) {}
};

// QUEUE 3: Outbound Public (Engine -> Publisher -> ITCH Feed)
struct MEMarketUpdate {
    TickerId   ticker_id_;
    OrderId    market_order_id_;
    UpdateType type_;
    Side       side_;
    Price      price_;
    Quantity   qty_;
    OrderId    new_order_id_ = OrderId_INVALID;    // only used for MODIFY (OrderReplace)
    OrderId    match_id_ = OrderId_INVALID;

    MEMarketUpdate() = default;

    MEMarketUpdate(TickerId ticker_id, OrderId market_order_id, UpdateType type,
                    Side side, Price price, Quantity qty,
                    OrderId new_order_id = OrderId_INVALID,
                    OrderId match_id = OrderId_INVALID)
        : ticker_id_(ticker_id), market_order_id_(market_order_id), type_(type),
          side_(side), price_(price), qty_(qty), new_order_id_(new_order_id), match_id_(match_id)
    {}
};


struct MEOrder {

    ClientId clientId_;
    OrderId clientOrderId_;
    OrderId marketOrderId_;
    Price price_;

    MEOrder* next_ = nullptr;
    MEOrder* prev_ = nullptr;

    TickerId tickerId_;
    Quantity initialQuantity_;
    Quantity remainingQuantity_;

    OrderType orderType_;
    Side side_;

    OrderToken order_token_{};

    MEOrder () = default;

    MEOrder(TickerId tickerId, ClientId clientId, OrderId clientOrderId,
            OrderId marketOrderId, OrderType orderType, Price price,
            Quantity initialQuantity, Side side,
            const OrderToken& orderToken = {},
            MEOrder* next = nullptr, MEOrder* prev = nullptr) noexcept
        : clientId_(clientId),
          clientOrderId_(clientOrderId),
          marketOrderId_(marketOrderId),
          price_(price),
          next_(next),
          prev_(prev),
          tickerId_(tickerId),
          initialQuantity_(initialQuantity),
          remainingQuantity_(initialQuantity),
          orderType_(orderType),
          side_(side),
          order_token_(orderToken)
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


struct SnapshotUpdate {
    MEMarketUpdate update_;
    uint64_t       seq_num_ = 0;
    char           bytes_[ITCH_MAX_MSG_SIZE] = {};
    size_t         len_ = 0;

    SnapshotUpdate() = default;

    SnapshotUpdate(const MEMarketUpdate& update, uint64_t seq_num)
        : update_(update), seq_num_(seq_num)
    {}
};

// Guard rails for the packing described above: carrying the OUCH token must
// stay free. If a new field pushes any of these over, reorder before widening.
static_assert(sizeof(MEClientRequest)  == 64, "MEClientRequest must stay within one cache line");
static_assert(sizeof(MEClientResponse) == 88, "MEClientResponse grew - reorder fields widest-first");
static_assert(sizeof(MEOrder)          == 80, "MEOrder grew - reorder fields widest-first");
