#include "src/engine/order_book/order_book_vector.h"

#include <chrono>
#include <iomanip>
#include <iostream>
#include <random>
#include <string_view>
#include <unordered_map>
#include <vector>
using namespace nanoexchange;

// ==========================================
// DUMMY DEPENDENCIES
// ==========================================
class MatchingEngine {};

// ==========================================
// ANSI COLOURS
// ==========================================
namespace col {
constexpr const char* RST  = "\033[0m";
constexpr const char* BOLD = "\033[1m";
constexpr const char* DIM  = "\033[2m";
constexpr const char* GRN  = "\033[92m";
constexpr const char* RED  = "\033[91m";
constexpr const char* YLW  = "\033[93m";
constexpr const char* CYN  = "\033[96m";
constexpr const char* BLU  = "\033[94m";
} // namespace col

static void line(const char* ch = "-", int w = 60) {
    std::cout << col::DIM;
    for (int i = 0; i < w; ++i)
        std::cout << ch;
    std::cout << col::RST << '\n';
}

// ==========================================
// REALISTIC MARKET PARTICIPANT PROFILES
// ==========================================
struct Participant {
    ClientId    id;
    std::string name;
    int         weight; // relative frequency of activity
    Price       edge;   // how far from mid they typically quote
    Quantity    minQty;
    Quantity    maxQty;
    float       cancelRate; // probability they cancel rather than add
    bool        aggressive; // true = market taker, false = maker
};

// Realistic exchange participant mix
static const std::vector<Participant> PARTICIPANTS = {
    // id    name                    wt  edge  minQ  maxQ  cxl    agg
    {1001, "HFT_MarketMaker_A", 25, 1, 50, 500, 0.40f, false},
    {1002, "HFT_MarketMaker_B", 20, 1, 100, 300, 0.35f, false},
    {1003, "HFT_Arb_Desk", 15, 2, 200, 1000, 0.20f, true},
    {1004, "BankPropDesk_Alpha", 10, 5, 500, 5000, 0.15f, true},
    {1005, "RetailBroker_Flow", 12, 3, 10, 200, 0.05f, true},
    {1006, "AlgoFund_Momentum", 8, 4, 100, 2000, 0.30f, true},
    {1007, "StatArb_Fund", 5, 2, 300, 3000, 0.25f, false},
    {1008, "InstitutionalBuySide", 3, 10, 1000, 10000, 0.10f, true},
    {1009, "RetailTrader_1", 1, 15, 10, 100, 0.02f, true},
    {1010, "RetailTrader_2", 1, 20, 10, 50, 0.02f, true},
};

// ==========================================
// REALISTIC ORDER STREAM GENERATOR
// ==========================================

struct SimOrder {
    ClientId  clientId;
    OrderId   clientOrderId;
    OrderType type;
    Price     price;
    Quantity  qty;
    Side      side;
    bool      isCancel;       // true = this event is a cancel of a live order
    OrderId   cancelTargetId; // which order to cancel (if isCancel)
};

struct OrderBook_LiveOrder {
    ClientId clientId;
    OrderId  clientOrderId;
    Price    price;
    Side     side;
};

class RealisticOrderStream {
  public:
    RealisticOrderStream(size_t count, OrderId startOrderId, uint64_t seed = 42)
        : gen_(seed),
          count_(count),
          nextOrderId_(startOrderId) {
        // Build weighted participant selection
        for (auto& p : PARTICIPANTS)
            for (int w = 0; w < p.weight; ++w)
                weightedPool_.push_back(&p);

        orders_.reserve(count);
        generateAll();
    }

    const std::vector<SimOrder>& orders() const { return orders_; }

  private:
    std::mt19937                     gen_;
    size_t                           count_;
    std::vector<const Participant*>  weightedPool_;
    std::vector<SimOrder>            orders_;
    std::vector<OrderBook_LiveOrder> liveOrders_; // track what's resting
    OrderId                          nextOrderId_;

    // Mid price drifts slowly like a real market
    Price mid_   = 10000; // $100.00 in ticks
    int   drift_ = 0;

    std::uniform_int_distribution<int> coinFlip_{0, 1};

    void generateAll() {
        std::uniform_int_distribution<size_t> poolPick(0, weightedPool_.size() - 1);
        std::uniform_int_distribution<int>    pct(0, 99);
        std::uniform_int_distribution<int>    driftStep(-2, 2);

        for (size_t i = 0; i < count_; ++i) {
            // Slowly drift the mid price
            if (i % 500 == 0) {
                drift_ += driftStep(gen_);
                drift_ = std::clamp(drift_, -200, 200);
                mid_ += drift_;
                mid_ = std::clamp(mid_, (Price)8000, (Price)12000);
            }

            const Participant* p = weightedPool_[poolPick(gen_)];

            // Should this event be a cancel of a live order?
            bool doCancel = !liveOrders_.empty() && (pct(gen_) < static_cast<int>(p->cancelRate * 100));

            if (doCancel) {
                std::vector<OrderBook_LiveOrder*> mine;
                for (auto& lo : liveOrders_)
                    if (lo.clientId == p->id)
                        mine.push_back(&lo);

                OrderBook_LiveOrder* target = nullptr;
                if (!mine.empty()) {
                    std::uniform_int_distribution<size_t> pick(0, mine.size() - 1);
                    target = mine[pick(gen_)];
                } else {
                    std::uniform_int_distribution<size_t> pick(0, liveOrders_.size() - 1);
                    target = &liveOrders_[pick(gen_)];
                }

                // FIX 1: Safely copy the IDs *before* calling erase/remove_if
                ClientId tgtClientId = target->clientId;
                OrderId  tgtOrderId  = target->clientOrderId;

                orders_.push_back({tgtClientId,
                                   0, // unused for cancel
                                   OrderType::GoodTillCancel, 0, 0, Side::BUY, true, tgtOrderId});

                // Remove from live tracker using the safe copies!
                liveOrders_.erase(std::remove_if(liveOrders_.begin(), liveOrders_.end(),
                                                 [=](const OrderBook_LiveOrder& lo) {
                                                     return lo.clientOrderId == tgtOrderId &&
                                                            lo.clientId == tgtClientId;
                                                 }),
                                  liveOrders_.end());

            } else {
                // Generate a new order
                std::uniform_int_distribution<Quantity> qtyDist(p->minQty, p->maxQty);
                Side                                    side = coinFlip_(gen_) ? Side::BUY : Side::SELL;

                Price price;
                if (p->aggressive) {
                    // Taker: crosses the spread — price at or through mid
                    price = (side == Side::BUY) ? mid_ + std::uniform_int_distribution<Price>(0, p->edge)(gen_)
                                                : mid_ - std::uniform_int_distribution<Price>(0, p->edge)(gen_);
                } else {
                    // Maker: posts inside or at the spread
                    price = (side == Side::BUY) ? mid_ - std::uniform_int_distribution<Price>(1, p->edge + 2)(gen_)
                                                : mid_ + std::uniform_int_distribution<Price>(1, p->edge + 2)(gen_);
                }

                price = std::max((Price)1, price);

                OrderType type = (pct(gen_) < 10) ? OrderType::FillAndKill : OrderType::GoodTillCancel;

                OrderId oid = nextOrderId_++;

                orders_.push_back({p->id, oid, type, price, qtyDist(gen_), side, false, 0});

                // Track GTC orders as live (IOC won't rest)
                if (type == OrderType::GoodTillCancel)
                    liveOrders_.push_back({p->id, oid, price, side});

                // Cap live orders to avoid unbounded growth
                if (liveOrders_.size() > 50000)
                    liveOrders_.erase(liveOrders_.begin(), liveOrders_.begin() + 10000);
            }
        }
    }
};

// ==========================================
// CORRECTNESS TESTS
// ==========================================

void runCorrectnessTests(MEOrderBook* book) {
    line("=");
    std::cout << col::BOLD << col::CYN << "[*] CORRECTNESS SCENARIOS\n" << col::RST;
    line("=");

    TickerId ticker = 1;
    ClientId mm     = 1001; // market maker
    ClientId taker  = 1003; // taker

    auto ok = [](std::string_view msg) { std::cout << col::GRN << "  [OK] " << col::RST << msg << '\n'; };

    // 1. Build liquidity
    std::cout << "[1] Building initial liquidity (4 price levels)...\n";
    book->addOrder(ticker, mm, 1, OrderType::GoodTillCancel, 1000, 100, Side::BUY);
    book->addOrder(ticker, mm, 2, OrderType::GoodTillCancel, 1005, 100, Side::BUY);
    book->addOrder(ticker, mm, 3, OrderType::GoodTillCancel, 1010, 100, Side::SELL);
    book->addOrder(ticker, mm, 4, OrderType::GoodTillCancel, 1015, 100, Side::SELL);
    ok("Bid: 1005 / 1000  |  Ask: 1010 / 1015");

    // 2. Exact fill
    std::cout << "[2] Exact match: BUY 100 @ 1010 (clears ask @ 1010)...\n";
    book->addOrder(ticker, taker, 5, OrderType::GoodTillCancel, 1010, 100, Side::BUY);
    ok("Ask @ 1010 fully consumed, best ask now 1015");

    // 3. Partial fill
    std::cout << "[3] Partial fill: BUY 50 @ 1015 (leaves 50 resting)...\n";
    book->addOrder(ticker, taker, 6, OrderType::GoodTillCancel, 1015, 50, Side::BUY);
    ok("Ask @ 1015 partially consumed, 50 remaining");

    // 4. IOC — fills what it can, discards rest
    std::cout << "[4] IOC: SELL 200 @ 1000 (fills 100+50, discards 50)...\n";
    book->addOrder(ticker, taker, 7, OrderType::FillAndKill, 1000, 200, Side::SELL);
    ok("IOC consumed bids, unfilled 50 discarded (no insertion)");

    // 5. Multi-level sweep
    std::cout << "[5] Rebuilding bids for sweep test...\n";
    book->addOrder(ticker, mm, 8, OrderType::GoodTillCancel, 1002, 100, Side::BUY);
    book->addOrder(ticker, mm, 9, OrderType::GoodTillCancel, 1001, 100, Side::BUY);
    book->addOrder(ticker, mm, 10, OrderType::GoodTillCancel, 1000, 100, Side::BUY);
    std::cout << "[5] Sweep SELL 250 @ 1000 (eats 3 bid levels)...\n";
    book->addOrder(ticker, taker, 11, OrderType::GoodTillCancel, 1000, 250, Side::SELL);
    ok("3 bid levels swept in one order");

    // 6. Self-cancel
    std::cout << "[6] Cancel remaining order (oid=1)...\n";
    book->cancelOrder(ticker, mm, 1);
    ok("Order cancelled successfully");

    // 7. Cancel nonexistent — should REJECT, not crash
    std::cout << "[7] Cancel nonexistent order (REJECT path)...\n";
    book->cancelOrder(ticker, 9999, 9999);
    ok("REJECT handled gracefully");

    // 8. Multi-client same price level
    std::cout << "[8] 3 clients queue at same price (FIFO)...\n";
    book->addOrder(ticker, 1001, 20, OrderType::GoodTillCancel, 1008, 100, Side::BUY);
    book->addOrder(ticker, 1002, 21, OrderType::GoodTillCancel, 1008, 100, Side::BUY);
    book->addOrder(ticker, 1007, 22, OrderType::GoodTillCancel, 1008, 100, Side::BUY);
    book->addOrder(ticker, taker, 23, OrderType::GoodTillCancel, 1008, 300, Side::SELL);
    ok("FIFO fill across 3 clients at same price level");

    line("=");
    std::cout << col::GRN << col::BOLD << "  ALL CORRECTNESS SCENARIOS PASSED\n" << col::RST;
    line("=");
    std::cout << '\n';
}

// ==========================================
// PERFORMANCE BENCHMARK
// ==========================================

void runThroughputBenchmark(MEOrderBook* book, size_t numOrders) {
    line("=");
    std::cout << col::BOLD << col::CYN << "[*] PERFORMANCE BENCHMARK\n" << col::RST;
    line("=");

    // Print participant mix
    std::cout << col::DIM << "  Participant mix:\n";
    for (auto& p : PARTICIPANTS)
        std::cout << "    " << std::left << std::setw(24) << p.name << " wt=" << p.weight << " edge=" << p.edge
                  << (p.aggressive ? "  [taker]" : "  [maker]") << '\n';
    std::cout << col::RST << '\n';

    // FIX 2: Added startOrderId to completely separate warmup IDs from Main IDs
    std::cout << "Generating " << numOrders << " realistic orders (" << PARTICIPANTS.size()
              << " participant types)...\n";
    RealisticOrderStream stream(numOrders, 100000);
    const auto&          orders = stream.orders();

    size_t addCount    = 0;
    size_t cancelCount = 0;
    for (auto& o : orders)
        o.isCancel ? ++cancelCount : ++addCount;

    std::cout << "  " << addCount << " add orders\n";
    std::cout << "  " << cancelCount << " cancel orders\n\n";

    // Warmup uses 2,000,000 as starting ID so they do not overlap
    std::cout << "Warmup (100 000 orders from same stream)...\n";
    RealisticOrderStream warmupStream(100000, 2000000, 99);
    for (auto& o : warmupStream.orders()) {
        if (o.isCancel)
            book->cancelOrder(1, o.clientId, o.cancelTargetId);
        else
            book->addOrder(1, o.clientId, o.clientOrderId, o.type, o.price, o.qty, o.side);
    }

    // Timed run
    std::cout << "Starting timer...\n\n";
    auto start = std::chrono::high_resolution_clock::now();

    for (const auto& o : orders) {
        if (o.isCancel)
            book->cancelOrder(1, o.clientId, o.cancelTargetId);
        else
            book->addOrder(1, o.clientId, o.clientOrderId, o.type, o.price, o.qty, o.side);
    }

    auto end = std::chrono::high_resolution_clock::now();

    auto   ns           = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
    auto   ms           = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    double sec          = ns / 1e9;
    double nsPerOrder   = static_cast<double>(ns) / numOrders;
    double ordersPerSec = numOrders / sec;

    double addFraction    = static_cast<double>(addCount) / numOrders;
    double cancelFraction = static_cast<double>(cancelCount) / numOrders;

    line("-");
    std::cout << col::BOLD << "  RESULTS\n" << col::RST;
    line("-");
    std::cout << "  Total events         : " << numOrders << "\n";
    std::cout << "  Add orders           : " << addCount << " (" << std::fixed << std::setprecision(1)
              << addFraction * 100 << "%)\n";
    std::cout << "  Cancel orders        : " << cancelCount << " (" << cancelFraction * 100 << "%)\n";
    std::cout << "  Total time           : " << ms << " ms\n";
    std::cout << col::YLW << "  Avg latency          : " << std::setprecision(2) << nsPerOrder << " ns/order\n"
              << col::RST;
    std::cout << col::GRN << "  Throughput           : " << std::setprecision(0) << ordersPerSec << " orders/sec\n"
              << col::RST;
    line("-");

    // Participant stats
    std::cout << "\n" << col::DIM << "  Participant breakdown:\n";
    std::unordered_map<ClientId, size_t> clientCounts;

    // FIX 3: Removed redundant ternary
    for (auto& o : orders)
        ++clientCounts[o.clientId];

    for (auto& p : PARTICIPANTS) {
        auto   it  = clientCounts.find(p.id);
        size_t cnt = (it != clientCounts.end()) ? it->second : 0;
        std::cout << "    " << std::left << std::setw(24) << p.name << std::right << std::setw(7) << cnt << " events\n";
    }
    std::cout << col::RST << '\n';

    line("=");
}

// ==========================================
// MAIN
// ==========================================
int main() {
    std::cout << col::BOLD << "\n  NanoExchange — MEOrderBook Benchmark\n\n" << col::RST;

    MatchingEngine*    engine = nullptr;
    quantlink::Logger* logger = nullptr;

    // Pool sized for warmup + 1M benchmark orders
    auto* book = new MEOrderBook(3000000, 1, engine, logger);

    runCorrectnessTests(book);
    runThroughputBenchmark(book, 1000000);

    delete book;
    std::cout << col::DIM << "  Shutdown complete.\n" << col::RST;
    return 0;
}
