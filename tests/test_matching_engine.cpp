#include <iostream>
#include <vector>
#include <cassert>
#include "../src/engine/matching_engine.h"
#include "../src/engine/order_book_map.h"
#include "../src/types.h"
#include "QuantLink/Lib/concurrency/lf_queue.h"
#include "QuantLink/Lib/logging/logger.h"

using namespace quantlink;

// ─── ANSI ────────────────────────────────────────────────────────────────────

namespace col {
    constexpr const char* RST  = "\033[0m";
    constexpr const char* BOLD = "\033[1m";
    constexpr const char* DIM  = "\033[2m";
    constexpr const char* GRN  = "\033[92m";
    constexpr const char* RED  = "\033[91m";
    constexpr const char* CYN  = "\033[96m";
    constexpr const char* YLW  = "\033[93m";
}

static void line(int w = 70) {
    std::cout << col::DIM;
    for (int i = 0; i < w; ++i) std::cout << '-';
    std::cout << col::RST << '\n';
}

// ─── Global Queues + Engine ───────────────────────────────────────────────────

SPSCQueue<MEClientRequest>  requestQ(1024);
SPSCQueue<MEClientResponse> responseQ(1024);
SPSCQueue<MEMarketUpdate>   marketUpdateQ(1024);
quantlink::Logger           logger( MAX_LOGGER_SIZE,"test_me.log",-1);

MatchingEngine* engine = nullptr;

// ─── Stats ───────────────────────────────────────────────────────────────────

static int passed = 0;
static int failed = 0;

// ─── CHECK macro ─────────────────────────────────────────────────────────────

void CHECK(bool cond, const char* msg) {
    if (cond) {
        std::cout << col::GRN << "    [PASS] " << col::RST << msg << '\n';
        ++passed;
    } else {
        std::cout << col::RED << "    [FAIL] " << col::RST << msg << '\n';
        ++failed;
    }
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

// Push a request directly into the engine (bypasses run() loop)
void send(ClientRequestType action, ClientId clientId, OrderId clientOrderId,
          TickerId tickerId, OrderType type, Side side, Price price, Quantity qty) {
    MEClientRequest req{ action, clientId, tickerId, clientOrderId, type, side, price, qty };
    requestQ.push(req);
    const auto* r = requestQ.getNextRead();
    engine->processClientRequest(r);
    requestQ.updateNextRead();
}

void sendNew(ClientId clientId, OrderId clientOrderId, TickerId tickerId,
             OrderType type, Side side, Price price, Quantity qty) {
    send(ClientRequestType::NEW, clientId, clientOrderId, tickerId, type, side, price, qty);
}

void sendCancel(ClientId clientId, OrderId clientOrderId, TickerId tickerId, Quantity qty = 0) {
    send(ClientRequestType::CANCEL, clientId, clientOrderId, tickerId,
         OrderType::GoodTillCancel, Side::BUY, 0, qty);
}

void sendReplace(ClientId clientId, OrderId clientOrderId, OrderId newClientOrderId,
                 TickerId tickerId, OrderType type, Side side, Price price, Quantity qty) {
    MEClientRequest request{};
    request.action_ = ClientRequestType::MODIFY;
    request.client_id_ = clientId;
    request.ticker_id_ = tickerId;
    request.client_order_id_ = clientOrderId;
    request.new_client_order_id_ = newClientOrderId;
    request.type_ = type;
    request.side_ = side;
    request.price_ = price;
    request.qty_ = qty;
    requestQ.push(request);
    const auto* queued = requestQ.getNextRead();
    engine->processClientRequest(queued);
    requestQ.updateNextRead();
}

// Drain all pending responses
std::vector<MEClientResponse> drainResponses() {
    std::vector<MEClientResponse> out;
    while (true) {
        const auto* r = responseQ.getNextRead();
        if (!r) break;
        out.push_back(*r);
        responseQ.updateNextRead();
    }
    return out;
}

// Drain all pending market updates
std::vector<MEMarketUpdate> drainMarketUpdates() {
    std::vector<MEMarketUpdate> out;
    while (true) {
        const auto* u = marketUpdateQ.getNextRead();
        if (!u) break;
        out.push_back(*u);
        marketUpdateQ.updateNextRead();
    }
    return out;
}

// ─── Test Cases ──────────────────────────────────────────────────────────────

// ── 1. GTC order rests on book ───────────────────────────────────────────────
void test_GTC_rests_on_book() {
    std::cout << col::CYN << col::BOLD << "\n[1] GTC order rests on book\n" << col::RST;
    line();

    sendNew(1, 1, 0, OrderType::GoodTillCancel, Side::BUY, 100, 10);

    auto resps   = drainResponses();
    auto updates = drainMarketUpdates();

    // Queues
    CHECK(resps.size()   == 1,                          "responseQ has 1 message");
    CHECK(updates.size() == 1,                          "marketUpdateQ has 1 message");

    // Response content
    CHECK(resps[0].status_           == ResponseType::ACCEPTED, "status = ACCEPTED");
    CHECK(resps[0].client_id_        == 1,              "client_id correct");
    CHECK(resps[0].client_order_id_  == 1,              "client_order_id correct");
    CHECK(resps[0].leaves_qty_       == 10,             "leaves_qty = 10");
    CHECK(resps[0].executed_qty_     == 0,              "executed_qty = 0");
    CHECK(resps[0].execution_price_  == 100,            "execution_price = 100");

    // Market update content
    CHECK(updates[0].type_   == UpdateType::ADD,        "update type = ADD");
    CHECK(updates[0].side_   == Side::BUY,              "update side = BUY");
    CHECK(updates[0].price_  == 100,                    "update price = 100");
    CHECK(updates[0].qty_    == 10,                     "update qty = 10");
}


// ── 2. Full fill ─────────────────────────────────────────────────────────────
void test_full_fill() {
    std::cout << col::CYN << col::BOLD << "\n[2] Full fill — SELL matches resting BUY exactly\n" << col::RST;
    line();

    // Resting BUY 10 @ 100 (from test 1 still on book)
    sendNew(2, 1, 0, OrderType::GoodTillCancel, Side::SELL, 100, 10);

    auto resps   = drainResponses();
    auto updates = drainMarketUpdates();

    // Queues: aggressive ACCEPTED + resting EXECUTED + aggressive EXECUTED
    CHECK(resps.size()   == 3,                          "responseQ has 3 messages");
    CHECK(updates.size() == 1,                          "marketUpdateQ has 1 message (TRADE)");

    CHECK(resps[0].client_id_       == 2,               "aggressive client_id = 2");
    CHECK(resps[0].status_          == ResponseType::ACCEPTED, "aggressive accepted first");
    CHECK(resps[0].leaves_qty_      == 10,              "accepted original quantity");

    // Resting order response (client 1)
    CHECK(resps[1].client_id_       == 1,               "resting client_id = 1");
    CHECK(resps[1].status_          == ResponseType::EXECUTED, "resting status = EXECUTED");
    CHECK(resps[1].executed_qty_    == 10,              "resting executed_qty = 10");
    CHECK(resps[1].leaves_qty_      == 0,               "resting leaves_qty = 0");
    CHECK(resps[1].execution_price_ == 100,             "resting execution_price = 100");

    // Aggressive order response (client 2)
    CHECK(resps[2].client_id_       == 2,               "aggressive client_id = 2");
    CHECK(resps[2].status_          == ResponseType::EXECUTED, "aggressive status = EXECUTED");
    CHECK(resps[2].executed_qty_    == 10,              "aggressive executed_qty = 10");
    CHECK(resps[2].leaves_qty_      == 0,               "aggressive leaves_qty = 0");
    CHECK(resps[1].match_id_ == resps[2].match_id_ &&
          resps[1].match_id_ == updates[0].match_id_,   "private and public executions share match ID");

    // Market update
    CHECK(updates[0].type_  == UpdateType::TRADE,       "update type = TRADE");
    CHECK(updates[0].qty_   == 10,                      "trade qty = 10");
    CHECK(updates[0].price_ == 100,                     "trade price = 100");
}


// ── 3. Partial fill — resting larger than aggressor ──────────────────────────
void test_partial_fill_resting_larger() {
    std::cout << col::CYN << col::BOLD << "\n[3] Partial fill — resting BUY larger than aggressive SELL\n" << col::RST;
    line();

    // Place resting BUY 20 @ 100
    sendNew(1, 2, 0, OrderType::GoodTillCancel, Side::BUY, 100, 20);
    drainResponses();
    drainMarketUpdates();

    // Aggressive SELL 5 @ 100 — partial fill, resting leaves 15
    sendNew(2, 2, 0, OrderType::GoodTillCancel, Side::SELL, 100, 5);

    auto resps   = drainResponses();
    auto updates = drainMarketUpdates();

    // aggressive ACCEPTED + resting EXECUTED + aggressive EXECUTED
    CHECK(resps.size()   == 3,                          "responseQ has 3 messages");
    CHECK(updates.size() == 1,                          "marketUpdateQ has 1 message (TRADE)");

    CHECK(resps[0].status_       == ResponseType::ACCEPTED, "aggressive accepted first");
    CHECK(resps[1].status_       == ResponseType::EXECUTED, "resting status = EXECUTED");
    CHECK(resps[1].executed_qty_ == 5,                  "resting executed_qty = 5");
    CHECK(resps[1].leaves_qty_   == 15,                 "resting leaves_qty = 15");

    CHECK(resps[2].status_       == ResponseType::EXECUTED, "aggressive status = EXECUTED");
    CHECK(resps[2].executed_qty_ == 5,                  "aggressive executed_qty = 5");
    CHECK(resps[2].leaves_qty_   == 0,                  "aggressive leaves_qty = 0");

    CHECK(updates[0].type_ == UpdateType::TRADE,        "update type = TRADE");
    CHECK(updates[0].qty_  == 5,                        "trade qty = 5");

    // Clean up remaining resting order
    sendCancel(1, 2, 0);
    drainResponses();
    drainMarketUpdates();
}


// ── 4. Partial fill — aggressor larger than resting ──────────────────────────
void test_partial_fill_aggressor_larger() {
    std::cout << col::CYN << col::BOLD << "\n[4] Partial fill — aggressive SELL larger than resting BUY, GTC remainder rests\n" << col::RST;
    line();

    // Resting BUY 5 @ 100
    sendNew(1, 3, 0, OrderType::GoodTillCancel, Side::BUY, 100, 5);
    drainResponses();
    drainMarketUpdates();

    // Aggressive SELL 15 @ 100 — 5 fills, 10 rests
    sendNew(2, 3, 0, OrderType::GoodTillCancel, Side::SELL, 100, 15);

    auto resps   = drainResponses();
    auto updates = drainMarketUpdates();

    // aggressive ACCEPTED + resting EXECUTED + aggressive EXECUTED
    CHECK(resps.size()   == 3,                          "responseQ has 3 messages");
    CHECK(updates.size() == 2,                          "marketUpdateQ has 2 messages (TRADE + ADD)");

    CHECK(resps[0].status_       == ResponseType::ACCEPTED, "aggressive accepted first");
    CHECK(resps[0].leaves_qty_   == 15,                 "accepted original quantity");
    CHECK(resps[1].status_       == ResponseType::EXECUTED, "resting status = EXECUTED");
    CHECK(resps[1].executed_qty_ == 5,                  "resting executed_qty = 5");
    CHECK(resps[1].leaves_qty_   == 0,                  "resting leaves_qty = 0");

    CHECK(resps[2].status_       == ResponseType::EXECUTED, "aggressive EXECUTED partial");
    CHECK(resps[2].executed_qty_ == 5,                  "aggressive executed_qty = 5");

    CHECK(updates[0].type_ == UpdateType::TRADE,        "first update = TRADE");
    CHECK(updates[1].type_ == UpdateType::ADD,          "second update = ADD");
    CHECK(updates[1].qty_  == 10,                       "ADD qty = 10");

    // Clean up
    sendCancel(2, 3, 0);
    drainResponses();
    drainMarketUpdates();
}


// ── 5. Multi-level sweep ──────────────────────────────────────────────────────
void test_multi_level_sweep() {
    std::cout << col::CYN << col::BOLD << "\n[5] Multi-level sweep — single BUY sweeps 3 ask levels\n" << col::RST;
    line();

    // Build 3 ask levels
    sendNew(1, 10, 0, OrderType::GoodTillCancel, Side::SELL, 101, 10);
    sendNew(1, 11, 0, OrderType::GoodTillCancel, Side::SELL, 102, 10);
    sendNew(1, 12, 0, OrderType::GoodTillCancel, Side::SELL, 103, 10);
    drainResponses();
    drainMarketUpdates();

    // Aggressive BUY 30 @ 103 — sweeps all 3 levels
    sendNew(2, 10, 0, OrderType::GoodTillCancel, Side::BUY, 103, 30);

    auto resps   = drainResponses();
    auto updates = drainMarketUpdates();

    // aggressive ACCEPTED + 3 resting/aggressive execution pairs
    CHECK(resps.size()   == 7,                          "responseQ has accepted plus 3 execution pairs");
    CHECK(updates.size() == 3,                          "marketUpdateQ has 3 TRADE updates");

    for (int i = 0; i < 3; ++i) {
        CHECK(updates[i].type_ == UpdateType::TRADE,    "update type = TRADE");
        CHECK(updates[i].qty_  == 10,                   "trade qty = 10 per level");
        CHECK(resps[1 + i * 2].match_id_ == resps[2 + i * 2].match_id_ &&
              resps[1 + i * 2].match_id_ == updates[i].match_id_,
              "each fill has one shared match ID");
        if (i > 0) CHECK(updates[i - 1].match_id_ != updates[i].match_id_,
                         "separate fills have different match IDs");
    }
}


// ── 6. FAK — remainder killed ─────────────────────────────────────────────────
void test_FAK_remainder_killed() {
    std::cout << col::CYN << col::BOLD << "\n[6] FAK — partial fill, remainder killed\n" << col::RST;
    line();

    // Resting BUY 3 @ 100
    sendNew(1, 20, 0, OrderType::GoodTillCancel, Side::BUY, 100, 3);
    drainResponses();
    drainMarketUpdates();

    // FAK SELL 10 @ 100 — fills 3, kills 7
    sendNew(2, 20, 0, OrderType::FillAndKill, Side::SELL, 100, 10);

    auto resps   = drainResponses();
    auto updates = drainMarketUpdates();

    // FAK ACCEPTED + resting EXECUTED + aggressive EXECUTED + FAK CANCELED
    CHECK(resps.size()   == 4,                          "responseQ has 4 messages");
    CHECK(updates.size() == 1,                          "marketUpdateQ has 1 message (TRADE only, no ADD)");

    CHECK(resps[0].status_       == ResponseType::ACCEPTED, "FAK accepted first");
    CHECK(resps[1].status_       == ResponseType::EXECUTED, "resting EXECUTED");
    CHECK(resps[1].executed_qty_ == 3,                  "resting executed_qty = 3");

    CHECK(resps[2].status_       == ResponseType::EXECUTED, "aggressive EXECUTED");
    CHECK(resps[2].executed_qty_ == 3,                  "aggressive executed_qty = 3");

    CHECK(resps[3].status_       == ResponseType::CANCELED,  "FAK remainder CANCELED");
    CHECK(resps[3].leaves_qty_   == 7,                  "FAK killed qty = 7");

    CHECK(updates[0].type_ == UpdateType::TRADE,        "update type = TRADE");
}


// ── 7. FAK — no fill at all ───────────────────────────────────────────────────
void test_FAK_no_fill() {
    std::cout << col::CYN << col::BOLD << "\n[7] FAK — no matching price, entire order killed\n" << col::RST;
    line();

    // FAK BUY 10 @ 50 — no asks anywhere near
    sendNew(2, 21, 0, OrderType::FillAndKill, Side::BUY, 50, 10);

    auto resps   = drainResponses();
    auto updates = drainMarketUpdates();

    CHECK(resps.size()   == 2,                          "responseQ has 2 messages");
    CHECK(updates.size() == 0,                          "marketUpdateQ empty (no TRADE, no ADD)");

    CHECK(resps[0].status_     == ResponseType::ACCEPTED,   "FAK accepted first");
    CHECK(resps[1].status_     == ResponseType::CANCELED,   "FAK fully CANCELED");
    CHECK(resps[1].leaves_qty_ == 10,                   "entire qty killed = 10");
}


// ── 8. FAK — fully filled ─────────────────────────────────────────────────────
void test_FAK_fully_filled() {
    std::cout << col::CYN << col::BOLD << "\n[8] FAK — fully filled, no CANCELED sent\n" << col::RST;
    line();

    // Resting BUY 10 @ 100
    sendNew(1, 30, 0, OrderType::GoodTillCancel, Side::BUY, 100, 10);
    drainResponses();
    drainMarketUpdates();

    // FAK SELL exact match
    sendNew(2, 30, 0, OrderType::FillAndKill, Side::SELL, 100, 10);

    auto resps   = drainResponses();
    auto updates = drainMarketUpdates();

    CHECK(resps.size()   == 3,                          "responseQ has accepted plus executions");
    CHECK(updates.size() == 1,                          "marketUpdateQ has 1 TRADE");

    CHECK(resps[0].status_ == ResponseType::ACCEPTED,   "FAK accepted first");
    CHECK(resps[1].status_ == ResponseType::EXECUTED,   "resting EXECUTED");
    CHECK(resps[2].status_ == ResponseType::EXECUTED,   "FAK EXECUTED");
    CHECK(updates[0].type_ == UpdateType::TRADE,        "update type = TRADE");
}


// ── 9. Cancel order ───────────────────────────────────────────────────────────
void test_cancel_order() {
    std::cout << col::CYN << col::BOLD << "\n[9] Cancel order — valid cancel\n" << col::RST;
    line();

    sendNew(1, 40, 0, OrderType::GoodTillCancel, Side::BUY, 100, 10);
    drainResponses();
    drainMarketUpdates();

    sendCancel(1, 40, 0);

    auto resps   = drainResponses();
    auto updates = drainMarketUpdates();

    CHECK(resps.size()   == 1,                          "responseQ has 1 message");
    CHECK(updates.size() == 1,                          "marketUpdateQ has 1 message");

    CHECK(resps[0].status_          == ResponseType::CANCELED, "status = CANCELED");
    CHECK(resps[0].client_id_       == 1,               "client_id correct");
    CHECK(resps[0].client_order_id_ == 40,              "client_order_id correct");
    CHECK(resps[0].execution_price_ == 100,             "execution_price = 100");
    CHECK(resps[0].leaves_qty_      == 10,              "canceled quantity = 10");

    CHECK(updates[0].type_  == UpdateType::CANCEL,      "update type = CANCEL");
    CHECK(updates[0].price_ == 100,                     "update price = 100");
    CHECK(updates[0].qty_   == 10,                      "update qty = 10");
    CHECK(updates[0].side_  == Side::BUY,               "update side = BUY");
}

void test_partial_cancel_order() {
    std::cout << col::CYN << col::BOLD << "\n[10] Cancel order - partial cancel\n" << col::RST;
    line();

    sendNew(1, 41, 0, OrderType::GoodTillCancel, Side::BUY, 100, 10);
    drainResponses();
    drainMarketUpdates();

    sendCancel(1, 41, 0, 4);
    auto resps = drainResponses();
    auto updates = drainMarketUpdates();

    CHECK(resps.size() == 1, "responseQ has 1 partial-cancel response");
    CHECK(updates.size() == 1, "marketUpdateQ has 1 cancel update");
    CHECK(resps[0].status_ == ResponseType::CANCELED, "status = CANCELED");
    CHECK(resps[0].leaves_qty_ == 4, "response reports canceled quantity = 4");
    CHECK(updates[0].qty_ == 4, "ITCH cancel quantity = 4");

    sendCancel(1, 41, 0, 6);
    resps = drainResponses();
    updates = drainMarketUpdates();
    CHECK(resps.size() == 1 && resps[0].status_ == ResponseType::CANCELED,
          "remaining quantity cancels successfully");
    CHECK(resps[0].leaves_qty_ == 6, "final cancel quantity = 6");
    CHECK(updates.size() == 1 && updates[0].qty_ == 6, "final ITCH cancel quantity = 6");

    sendCancel(1, 41, 0, 1);
    resps = drainResponses();
    CHECK(resps.size() == 1 && resps[0].status_ == ResponseType::CANCEL_REJECTED,
           "cancel after order removal is rejected");
}

void test_replace_order() {
    std::cout << col::CYN << col::BOLD << "\n[11] Replace order\n" << col::RST;
    line();

    sendNew(1, 42, 0, OrderType::GoodTillCancel, Side::BUY, 100, 10);
    drainResponses();
    drainMarketUpdates();

    sendReplace(1, 42, 43, 0, OrderType::GoodTillCancel, Side::BUY, 101, 12);
    auto resps = drainResponses();
    auto updates = drainMarketUpdates();

    CHECK(resps.size() == 1 && resps[0].status_ == ResponseType::MODIFIED,
          "replacement response is MODIFIED");
    CHECK(resps[0].client_order_id_ == 43 && resps[0].price_ == 101 && resps[0].qty_ == 12,
          "replacement uses new client ID, price, and quantity");
    CHECK(updates.size() == 1 && updates[0].type_ == UpdateType::MODIFY,
          "public book receives ITCH replace update");
    CHECK(updates[0].market_order_id_ != updates[0].new_order_id_ && updates[0].price_ == 101 &&
          updates[0].qty_ == 12, "public replace retires old order ID");

    sendCancel(1, 42, 0);
    resps = drainResponses();
    CHECK(resps.size() == 1 && resps[0].status_ == ResponseType::CANCEL_REJECTED,
          "old client order ID is retired");

    sendCancel(1, 43, 0);
    resps = drainResponses();
    updates = drainMarketUpdates();
    CHECK(resps.size() == 1 && resps[0].status_ == ResponseType::CANCELED &&
          resps[0].leaves_qty_ == 12, "replacement can be canceled by new client order ID");
    CHECK(updates.size() == 1 && updates[0].type_ == UpdateType::CANCEL && updates[0].qty_ == 12,
          "replacement cancel updates public book");
}


// ── 10. Cancel rejected — unknown client ─────────────────────────────────────
void test_cancel_rejected_unknown_client() {
    std::cout << col::CYN << col::BOLD << "\n[10] Cancel rejected — unknown client\n" << col::RST;
    line();

    sendCancel(999, 999, 0);

    auto resps   = drainResponses();
    auto updates = drainMarketUpdates();

    CHECK(resps.size()   == 1,                               "responseQ has 1 message");
    CHECK(updates.size() == 0,                               "marketUpdateQ empty");
    CHECK(resps[0].status_ == ResponseType::CANCEL_REJECTED, "status = CANCEL_REJECTED");
    CHECK(resps[0].client_id_ == 999,                        "client_id = 999");
}


// ── 11. Cancel rejected — unknown order id ───────────────────────────────────
void test_cancel_rejected_unknown_order() {
    std::cout << col::CYN << col::BOLD << "\n[11] Cancel rejected — client exists but order id unknown\n" << col::RST;
    line();

    // First place a real order so client 1 exists in the map
    sendNew(1, 50, 0, OrderType::GoodTillCancel, Side::BUY, 100, 5);
    drainResponses();
    drainMarketUpdates();

    // Cancel a completely different order id
    sendCancel(1, 9999, 0);

    auto resps   = drainResponses();
    auto updates = drainMarketUpdates();

    CHECK(resps.size()   == 1,                               "responseQ has 1 message");
    CHECK(updates.size() == 0,                               "marketUpdateQ empty");
    CHECK(resps[0].status_ == ResponseType::CANCEL_REJECTED, "status = CANCEL_REJECTED");

    // Clean up
    sendCancel(1, 50, 0);
    drainResponses();
    drainMarketUpdates();
}


// ── 12. Multi-client same price level (FIFO) ─────────────────────────────────
void test_FIFO_same_price_level() {
    std::cout << col::CYN << col::BOLD << "\n[12] FIFO — 3 clients at same price, filled in order\n" << col::RST;
    line();

    // 3 clients queue BUY 10 @ 100
    sendNew(1, 60, 0, OrderType::GoodTillCancel, Side::BUY, 100, 10);
    sendNew(2, 60, 0, OrderType::GoodTillCancel, Side::BUY, 100, 10);
    sendNew(3, 60, 0, OrderType::GoodTillCancel, Side::BUY, 100, 10);
    drainResponses();
    drainMarketUpdates();

    // Aggressive SELL 30 @ 100 — should fill all 3 in FIFO order
    sendNew(4, 60, 0, OrderType::GoodTillCancel, Side::SELL, 100, 30);

    auto resps   = drainResponses();
    auto updates = drainMarketUpdates();

    // Aggressive ACCEPTED + 3 resting/aggressive execution pairs
    CHECK(resps.size()   == 7,                          "responseQ has accepted plus 6 fills");
    CHECK(updates.size() == 3,                          "marketUpdateQ has 3 TRADE updates");

    // FIFO: client 1 filled first, then 2, then 3
    CHECK(resps[0].status_ == ResponseType::ACCEPTED,   "aggressive accepted first");
    CHECK(resps[1].client_id_ == 1,                     "first fill: client 1 (FIFO)");
    CHECK(resps[3].client_id_ == 2,                     "second fill: client 2 (FIFO)");
    CHECK(resps[5].client_id_ == 3,                     "third fill: client 3 (FIFO)");
}


// ── 13. Queue integrity — logging ────────────────────────────────────────────
void test_logging_and_queue_flow() {
    std::cout << col::CYN << col::BOLD << "\n[13] Queue flow — verify responseQ and marketUpdateQ are independent\n" << col::RST;
    line();

    // Place order — generates 1 response and 1 market update
    sendNew(1, 70, 0, OrderType::GoodTillCancel, Side::SELL, 200, 5);

    // Drain only responses first, leave market updates
    auto resps = drainResponses();
    CHECK(resps.size() == 1,                            "responseQ drained: 1 message");

    // Market update queue should still have its message untouched
    auto updates = drainMarketUpdates();
    CHECK(updates.size() == 1,                          "marketUpdateQ independent: still has 1 message");

    // Verify both queues are now empty
    auto resps2   = drainResponses();
    auto updates2 = drainMarketUpdates();
    CHECK(resps2.size()   == 0,                         "responseQ now empty");
    CHECK(updates2.size() == 0,                         "marketUpdateQ now empty");

    // Clean up
    sendCancel(1, 70, 0);
    drainResponses();
    drainMarketUpdates();
}


// ── 14. Multiple tickers isolated ────────────────────────────────────────────
void test_multiple_tickers_isolated() {
    std::cout << col::CYN << col::BOLD << "\n[14] Multiple tickers — orders on ticker 0 don't affect ticker 1\n" << col::RST;
    line();

    // BUY on ticker 0
    sendNew(1, 80, 0, OrderType::GoodTillCancel, Side::BUY, 100, 10);
    drainResponses();
    drainMarketUpdates();

    // SELL on ticker 1 at same price — should NOT match ticker 0's BUY
    sendNew(2, 80, 1, OrderType::GoodTillCancel, Side::SELL, 100, 10);

    auto resps   = drainResponses();
    auto updates = drainMarketUpdates();

    // Should get ACCEPTED (rests on ticker 1), not EXECUTED
    CHECK(resps.size()   == 1,                          "responseQ has 1 message");
    CHECK(resps[0].status_ == ResponseType::ACCEPTED,   "ticker 1 order ACCEPTED (no cross-ticker match)");
    CHECK(updates[0].type_ == UpdateType::ADD,          "market update = ADD (not TRADE)");

    // Clean up both
    sendCancel(1, 80, 0);
    sendCancel(2, 80, 1);
    drainResponses();
    drainMarketUpdates();
}


// ─── Summary ─────────────────────────────────────────────────────────────────

void printSummary() {
    std::cout << '\n';
    line(70);
    std::cout << col::BOLD << "  Results: ";
    std::cout << col::GRN  << passed << " passed  ";
    if (failed > 0)
        std::cout << col::RED << failed << " FAILED";
    else
        std::cout << col::GRN << "0 failed";
    std::cout << col::RST << '\n';
    line(70);
}


// ─── Main ────────────────────────────────────────────────────────────────────

int main() {
    std::cout << col::BOLD << col::CYN;
    std::cout << "\n  NanoExchange — MatchingEngine Test Suite\n";
    std::cout << col::RST;
    line(70);
    std::cout << col::DIM << "  Queues  : requestQ / responseQ / marketUpdateQ (SPSCQueue)\n";
    std::cout <<             "  Logging : test_me.log (via quantlink::Logger)\n";
    std::cout <<             "  Book    : order_book_map (ticker 0 and 1)\n" << col::RST;
    line(70);

    engine = new MatchingEngine(&requestQ, &responseQ, &marketUpdateQ, &logger, 2, 1024);
    test_GTC_rests_on_book();
    test_full_fill();
    test_partial_fill_resting_larger();
    test_partial_fill_aggressor_larger();
    test_multi_level_sweep();
    test_FAK_remainder_killed();
    test_FAK_no_fill();
    test_FAK_fully_filled();
    test_cancel_order();
    test_partial_cancel_order();
    test_replace_order();
    test_cancel_rejected_unknown_client();
    test_cancel_rejected_unknown_order();
    test_FIFO_same_price_level();
    test_logging_and_queue_flow();
    test_multiple_tickers_isolated();

    printSummary();

    delete engine;
    std::cout << col::DIM << "\n  Log written to: test_me.log\n" << col::RST;
    return failed == 0 ? 0 : 1;
}
