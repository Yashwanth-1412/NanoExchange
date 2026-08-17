// ============================================================
//  ob_compare.cpp — Order Book Extreme Stress Benchmark
//
//  Phases:
//    0. Realistic    — mixed participant flow (baseline)
//    1. LargeWorking — wide price range, cache pressure
//    2. ColdCancel   — cold random-access cancels
//    3. DeepBook     — 5000 levels per side, O(depth) traversal
//    4. Fragmented   — randomised unique prices, max pool churn
//    5. Sweeps       — bursty IOC volatility spikes
//    6. MemeStock    — deep queue, random middle cancels
//    7. Pennying     — thrashing best_bid / branch predictor
//    8. FlshCrsh     — single order wipes 10,000 price levels
//
//  Per-phase metrics: avg / p50 / p99 / p999 / max / throughput
//
//  Build:
//    add_executable(ob_compare tests/ob_compare.cpp)
//    target_include_directories(ob_compare PRIVATE
//        ${CMAKE_CURRENT_SOURCE_DIR}
//        ${CMAKE_CURRENT_SOURCE_DIR}/QuantLink)
//    cd build && cmake .. && make -j4 ob_compare && ./ob_compare
// ============================================================

#include "tests/ob_variants.h"

#include <algorithm>
#include <chrono>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <limits>
#include <numeric>
#include <random>
#include <string_view>
#include <unordered_map>
#include <vector>
using namespace nanoexchange;

class MatchingEngine {};

// ============================================================
//  ANSI
// ============================================================
namespace col {
constexpr const char* RST  = "\033[0m";
constexpr const char* BOLD = "\033[1m";
constexpr const char* DIM  = "\033[2m";
constexpr const char* GRN  = "\033[92m";
constexpr const char* RED  = "\033[91m";
constexpr const char* YLW  = "\033[93m";
constexpr const char* CYN  = "\033[96m";
constexpr const char* BLU  = "\033[94m";
constexpr const char* MAG  = "\033[95m";
constexpr const char* WHT  = "\033[97m";
} // namespace col

static void line(const char* ch = "-", int w = 95) {
    std::cout << col::DIM;
    for (int i = 0; i < w; ++i)
        std::cout << ch;
    std::cout << col::RST << '\n';
}

// ============================================================
//  POOL SIZE — consistent across all variants / phases
// ============================================================
constexpr size_t POOL_SIZE = 10'000'000;

// ============================================================
//  SHARED ORDER STRUCT
// ============================================================
struct SimOrder {
    ClientId  clientId;
    OrderId   clientOrderId;
    OrderType type;
    Price     price;
    Quantity  qty;
    Side      side;
    bool      isCancel;
    OrderId   cancelTargetId;
};

// Live order tracker — swap-with-last for O(1) erase
struct LiveOrder {
    ClientId clientId;
    OrderId  clientOrderId;
};

struct LiveSet {
    std::vector<LiveOrder> v;

    void add(ClientId cid, OrderId oid) { v.push_back({cid, oid}); }

    void erase(size_t idx) {
        v[idx] = v.back();
        v.pop_back();
    }

    void remove(ClientId cid, OrderId oid) {
        for (size_t i = 0; i < v.size(); ++i) {
            if (v[i].clientId == cid && v[i].clientOrderId == oid) {
                erase(i);
                return;
            }
        }
    }

    bool       empty() const { return v.empty(); }
    size_t     size() const { return v.size(); }
    LiveOrder& operator[](size_t i) { return v[i]; }
};

// ============================================================
//  PARTICIPANTS
// ============================================================
struct Participant {
    ClientId    id;
    std::string name;
    int         weight;
    Price       edge;
    Quantity    minQty;
    Quantity    maxQty;
    float       cancelRate;
    bool        aggressive;
};

static const std::vector<Participant> PARTICIPANTS = {
    {1001, "HFT_MarketMaker_A", 25, 1, 50, 500, 0.55f, false},
    {1002, "HFT_MarketMaker_B", 20, 1, 100, 300, 0.50f, false},
    {1003, "HFT_Arb_Desk", 15, 2, 200, 1000, 0.25f, true},
    {1004, "BankPropDesk_Alpha", 10, 5, 500, 5000, 0.15f, true},
    {1005, "RetailBroker_Flow", 12, 3, 10, 200, 0.05f, true},
    {1006, "AlgoFund_Momentum", 8, 4, 100, 2000, 0.35f, true},
    {1007, "StatArb_Fund", 5, 2, 300, 3000, 0.30f, false},
    {1008, "InstitutionalBuySide", 3, 10, 1000, 10000, 0.10f, true},
    {1009, "RetailTrader_1", 1, 15, 10, 100, 0.02f, true},
    {1010, "RetailTrader_2", 1, 20, 10, 50, 0.02f, true},
};

// ============================================================
//  TAIL LATENCY STATS
// ============================================================
struct PhaseStats {
    double    avg;
    double    p50;
    double    p99;
    double    p999;
    double    max;
    double    throughputM; // M orders/sec
    long long ms;
    size_t    n;
};

PhaseStats computeStats(std::vector<double>& samples, long long totalNs) {
    std::sort(samples.begin(), samples.end());
    size_t n   = samples.size();
    double sum = 0;
    for (double s : samples)
        sum += s;

    PhaseStats st;
    st.n           = n;
    st.avg         = sum / n;
    st.p50         = samples[n * 50 / 100];
    st.p99         = samples[n * 99 / 100];
    st.p999        = samples[std::min(n - 1, n * 999 / 1000)];
    st.max         = samples.back();
    st.ms          = totalNs / 1'000'000;
    st.throughputM = (n / (totalNs / 1e9)) / 1e6;
    return st;
}

// ============================================================
//  PHASE 0 — Realistic
// ============================================================
std::vector<SimOrder> genRealistic(size_t n, uint64_t seed = 42) {
    std::mt19937                    gen(seed);
    std::vector<const Participant*> pool;
    for (auto& p : PARTICIPANTS)
        for (int w = 0; w < p.weight; ++w)
            pool.push_back(&p);
    std::vector<SimOrder> orders;
    LiveSet               live;
    orders.reserve(n);
    OrderId                               nextId = 200000;
    Price                                 mid    = 10000;
    int                                   drift  = 0;
    std::uniform_int_distribution<size_t> pickP(0, pool.size() - 1);
    std::uniform_int_distribution<int>    pct(0, 99);
    std::uniform_int_distribution<int>    driftD(-2, 2);
    std::uniform_int_distribution<int>    coin(0, 1);

    for (size_t i = 0; i < n; ++i) {
        if (i % 500 == 0) {
            drift += driftD(gen);
            drift = std::clamp(drift, -200, 200);
            mid += drift;
            mid = std::clamp(mid, (Price)8000, (Price)12000);
        }
        const Participant* p        = pool[pickP(gen)];
        bool               doCancel = !live.empty() && pct(gen) < (int)(p->cancelRate * 100);
        if (doCancel) {
            std::uniform_int_distribution<size_t> pk(0, live.size() - 1);
            size_t                                idx = pk(gen);
            LiveOrder                             t   = live[idx];
            orders.push_back({t.clientId, 0, OrderType::GoodTillCancel, 0, 0, Side::BUY, true, t.clientOrderId});
            live.erase(idx);
        } else {
            std::uniform_int_distribution<Quantity> qtyD(p->minQty, p->maxQty);
            std::uniform_int_distribution<Price>    edgeD(1, p->edge + 2);
            Side                                    side = coin(gen) ? Side::BUY : Side::SELL;
            Price price    = p->aggressive ? (side == Side::BUY ? mid + edgeD(gen) : mid - edgeD(gen))
                                           : (side == Side::BUY ? mid - edgeD(gen) : mid + edgeD(gen));
            price          = std::max((Price)1, price);
            OrderType type = pct(gen) < 10 ? OrderType::FillAndKill : OrderType::GoodTillCancel;
            OrderId   oid  = nextId++;
            ClientId  cid  = p->id;
            orders.push_back({cid, oid, type, price, qtyD(gen), side, false, 0});
            if (type == OrderType::GoodTillCancel) {
                live.add(cid, oid);
                if (live.size() > 1'000'000)
                    for (int k = 0; k < 100000; ++k)
                        live.erase(0);
            }
        }
    }
    return orders;
}

// ============================================================
//  PHASE 1 — Large Working Set
// ============================================================
std::vector<SimOrder> genLargeWorkingSet(size_t n, uint64_t seed = 1) {
    std::mt19937                            gen(seed);
    std::uniform_int_distribution<Price>    priceD(1, 240000);
    std::uniform_int_distribution<Quantity> qtyD(10, 500);
    std::uniform_int_distribution<int>      pct(0, 99);
    std::uniform_int_distribution<int>      coin(0, 1);
    std::vector<SimOrder>                   orders;
    LiveSet                                 live;
    orders.reserve(n);
    OrderId nextId = 300000;

    for (size_t i = 0; i < n; ++i) {
        bool doCancel = !live.empty() && pct(gen) < 25;
        if (doCancel) {
            std::uniform_int_distribution<size_t> pk(0, live.size() - 1);
            size_t                                idx = pk(gen);
            LiveOrder                             t   = live[idx];
            orders.push_back({t.clientId, 0, OrderType::GoodTillCancel, 0, 0, Side::BUY, true, t.clientOrderId});
            live.erase(idx);
        } else {
            Side     side  = coin(gen) ? Side::BUY : Side::SELL;
            Price    price = priceD(gen);
            OrderId  oid   = nextId++;
            ClientId cid   = 1001 + (oid % 10);
            orders.push_back({cid, oid, OrderType::GoodTillCancel, price, qtyD(gen), side, false, 0});
            live.add(cid, oid);
            if (live.size() > 800'000)
                for (int k = 0; k < 50000; ++k)
                    live.erase(0);
        }
    }
    return orders;
}

// ============================================================
//  PHASE 2 — Cold Cancels
// ============================================================
std::vector<SimOrder> genColdCancels(size_t n, uint64_t seed = 2) {
    std::mt19937                            gen(seed);
    std::uniform_int_distribution<Price>    priceD(9500, 10500);
    std::uniform_int_distribution<Quantity> qtyD(10, 100);
    std::uniform_int_distribution<int>      coin(0, 1);
    std::vector<SimOrder>                   orders;
    std::vector<LiveOrder>                  live;
    orders.reserve(n * 2);
    OrderId nextId = 400000;

    for (size_t i = 0; i < n; ++i) {
        Side     side  = coin(gen) ? Side::BUY : Side::SELL;
        Price    price = priceD(gen);
        OrderId  oid   = nextId++;
        ClientId cid   = 1001 + (oid % 10);
        orders.push_back({cid, oid, OrderType::GoodTillCancel, price, qtyD(gen), side, false, 0});
        live.push_back({cid, oid});
    }
    std::shuffle(live.begin(), live.end(), gen);
    for (auto& lo : live)
        orders.push_back({lo.clientId, 0, OrderType::GoodTillCancel, 0, 0, Side::BUY, true, lo.clientOrderId});
    return orders;
}

// ============================================================
//  PHASE 3 — Deep Book
// ============================================================
std::vector<SimOrder> genDeepBook(size_t n, uint64_t seed = 3) {
    std::mt19937                       gen(seed);
    std::uniform_int_distribution<int> pct(0, 99);
    std::uniform_int_distribution<int> coin(0, 1);
    std::vector<SimOrder>              orders;
    orders.reserve(n + 10000);
    OrderId nextId = 500000;

    for (int p = 10001; p <= 15000; ++p)
        orders.push_back({1001, nextId++, OrderType::GoodTillCancel, (Price)p, 50, Side::SELL, false, 0});
    for (int p = 9999; p >= 5000; --p)
        orders.push_back({1002, nextId++, OrderType::GoodTillCancel, (Price)p, 50, Side::BUY, false, 0});

    size_t                               remaining = n > 10000 ? n - 10000 : 0;
    std::uniform_int_distribution<Price> aggrBuy(10001, 15000);
    std::uniform_int_distribution<Price> aggrSell(5000, 9999);
    std::uniform_int_distribution<Price> passAsk(10001, 15000);
    std::uniform_int_distribution<Price> passBid(5000, 9999);

    for (size_t i = 0; i < remaining; ++i) {
        if (pct(gen) < 70) {
            Side  side  = coin(gen) ? Side::BUY : Side::SELL;
            Price price = (side == Side::BUY) ? aggrBuy(gen) : aggrSell(gen);
            orders.push_back({1003, nextId++, OrderType::GoodTillCancel, price, 50, side, false, 0});
            if (side == Side::BUY)
                orders.push_back({1001, nextId++, OrderType::GoodTillCancel, price, 50, Side::SELL, false, 0});
            else
                orders.push_back({1002, nextId++, OrderType::GoodTillCancel, price, 50, Side::BUY, false, 0});
        } else {
            Side  side  = coin(gen) ? Side::BUY : Side::SELL;
            Price price = (side == Side::BUY) ? passBid(gen) : passAsk(gen);
            orders.push_back({1004, nextId++, OrderType::GoodTillCancel, price, 50, side, false, 0});
        }
    }
    return orders;
}

// ============================================================
//  PHASE 4 — Fragmented
// ============================================================
std::vector<SimOrder> genFragmented(size_t n, uint64_t seed = 4) {
    std::mt19937                         gen(seed);
    std::uniform_int_distribution<Price> priceD(1, 240000);
    std::uniform_int_distribution<int>   coin(0, 1);
    std::vector<SimOrder>                orders;
    orders.reserve(n * 2);
    OrderId nextId = 600000;

    for (size_t i = 0; i < n; ++i) {
        Price    price = priceD(gen);
        Side     side  = coin(gen) ? Side::BUY : Side::SELL;
        OrderId  oid   = nextId++;
        ClientId cid   = 1001 + (oid % 10);
        orders.push_back({cid, oid, OrderType::GoodTillCancel, price, 100, side, false, 0});
        orders.push_back({cid, 0, OrderType::GoodTillCancel, 0, 0, Side::BUY, true, oid});
    }
    return orders;
}

// ============================================================
//  PHASE 5 — Sweeps
// ============================================================
std::vector<SimOrder> genSweeps(size_t n, uint64_t seed = 5) {
    std::mt19937                            gen(seed);
    std::uniform_int_distribution<Price>    makerPrice(9990, 10010);
    std::uniform_int_distribution<Quantity> makerQty(50, 300);
    std::uniform_int_distribution<int>      coin(0, 1), pct(0, 99), burstSize(3, 8), calmLen(500, 2000);
    std::vector<SimOrder>                   orders;
    orders.reserve(n + 100);
    OrderId nextId    = 700000;
    int     nextBurst = calmLen(gen);
    int     burstLeft = 0;

    for (size_t i = 0; i < n; ++i) {
        if (burstLeft == 0 && (int)i >= nextBurst) {
            burstLeft = burstSize(gen);
            nextBurst = (int)i + calmLen(gen);
        }
        if (burstLeft > 0) {
            orders.push_back({1003, nextId++, OrderType::FillAndKill, 15000, 999999, Side::BUY, false, 0});
            orders.push_back({1003, nextId++, OrderType::FillAndKill, 5000, 999999, Side::SELL, false, 0});
            --burstLeft;
        }
        Side  side   = coin(gen) ? Side::BUY : Side::SELL;
        Price price  = makerPrice(gen);
        price        = (side == Side::BUY) ? price - 5 : price + 5;
        OrderId  oid = nextId++;
        ClientId cid = 1001 + (oid % 4);
        orders.push_back({cid, oid, OrderType::GoodTillCancel, price, makerQty(gen), side, false, 0});
    }
    return orders;
}

// ============================================================
//  PHASE 6 — MemeStock (Deep Queue)
//  Proves map/linked-list efficiency when cancelling from the
//  middle of a massive 150,000 order queue at a single tick.
// ============================================================
std::vector<SimOrder> genDeepQueue(size_t n, uint64_t seed = 6) {
    std::mt19937          gen(seed);
    std::vector<SimOrder> orders;
    orders.reserve(n);
    std::vector<LiveOrder> live;
    OrderId                nextId      = 800000;
    Price                  staticPrice = 10000; // Never changes

    // Phase A: Build massive queue
    size_t buildCount = n / 2;
    for (size_t i = 0; i < buildCount; ++i) {
        OrderId oid = nextId++;
        orders.push_back({1005, oid, OrderType::GoodTillCancel, staticPrice, 100, Side::BUY, false, 0});
        live.push_back({1005, oid});
    }

    // Phase B: Randomly cancel from the middle
    std::shuffle(live.begin(), live.end(), gen);
    for (size_t i = 0; i < buildCount; ++i) {
        orders.push_back(
            {live[i].clientId, 0, OrderType::GoodTillCancel, 0, 0, Side::BUY, true, live[i].clientOrderId});
    }

    // Safety pad to exactly n
    while (orders.size() < n) {
        orders.push_back({1005, nextId++, OrderType::GoodTillCancel, staticPrice, 100, Side::BUY, false, 0});
    }
    return orders;
}

// ============================================================
//  PHASE 7 — Pennying (Flickering BBO)
//  Thrashing best_bid_ pointer constantly. Destroys branch prediction.
// ============================================================
std::vector<SimOrder> genFlickeringBBO(size_t n) {
    std::vector<SimOrder> orders;
    orders.reserve(n);
    OrderId nextId    = 900000;
    Price   basePrice = 10000;

    size_t loops = n / 4;
    for (size_t i = 0; i < loops; ++i) {
        OrderId idA = nextId++;
        orders.push_back({1001, idA, OrderType::GoodTillCancel, basePrice, 100, Side::BUY, false, 0});

        OrderId idB = nextId++;
        orders.push_back({1002, idB, OrderType::GoodTillCancel, basePrice + 1, 100, Side::BUY, false, 0});

        orders.push_back({1001, 0, OrderType::GoodTillCancel, 0, 0, Side::BUY, true, idA});

        OrderId idA2 = nextId++;
        orders.push_back({1001, idA2, OrderType::GoodTillCancel, basePrice + 2, 100, Side::BUY, false, 0});

        if (i % 10 == 0)
            basePrice = 10000;
        else
            basePrice++;
    }

    while (orders.size() < n) {
        orders.push_back({1001, nextId++, OrderType::GoodTillCancel, 10000, 100, Side::BUY, false, 0});
    }
    return orders;
}

// ============================================================
//  PHASE 8 — FlashCrash
//  Builds 10,000 consecutive levels, then drops a massive market
//  order to wipe them all out in a single match() call.
// ============================================================
std::vector<SimOrder> genFlashCrash(size_t n) {
    std::vector<SimOrder> orders;
    orders.reserve(n);
    OrderId nextId = 1000000;

    size_t buildCount = std::min(n - 1, (size_t)10000);
    for (size_t i = 0; i < buildCount; ++i) {
        Price p = (Price)std::max((int)1, 10000 - (int)i);
        orders.push_back({1001, nextId++, OrderType::GoodTillCancel, p, 100, Side::BUY, false, 0});
    }

    // THE NUKE
    orders.push_back({1004, nextId++, OrderType::FillAndKill, 1, 10000000, Side::SELL, false, 0});

    while (orders.size() < n) {
        orders.push_back({1001, nextId++, OrderType::GoodTillCancel, 10000, 100, Side::BUY, false, 0});
    }
    return orders;
}

// ============================================================
//  CORRECTNESS
// ============================================================
template<typename Book>
bool runCorrectness(Book* book, std::string_view tag) {
    std::cout << "  " << col::BLU << std::left << std::setw(38) << tag << col::RST << " ... ";
    std::cout.flush();
    try {
        book->addOrder(1, 1001, 1, OrderType::GoodTillCancel, 1000, 100, Side::BUY);
        book->addOrder(1, 1001, 2, OrderType::GoodTillCancel, 1005, 100, Side::BUY);
        book->addOrder(1, 1001, 3, OrderType::GoodTillCancel, 1010, 100, Side::SELL);
        book->addOrder(1, 1001, 4, OrderType::GoodTillCancel, 1015, 100, Side::SELL);
        book->addOrder(1, 1003, 5, OrderType::GoodTillCancel, 1010, 100, Side::BUY);
        book->addOrder(1, 1003, 6, OrderType::GoodTillCancel, 1015, 50, Side::BUY);
        book->addOrder(1, 1003, 7, OrderType::FillAndKill, 1000, 200, Side::SELL);
        book->addOrder(1, 1001, 8, OrderType::GoodTillCancel, 1002, 100, Side::BUY);
        book->addOrder(1, 1001, 9, OrderType::GoodTillCancel, 1001, 100, Side::BUY);
        book->addOrder(1, 1001, 10, OrderType::GoodTillCancel, 1000, 100, Side::BUY);
        book->addOrder(1, 1003, 11, OrderType::GoodTillCancel, 1000, 250, Side::SELL);
        book->cancelOrder(1, 1001, 1);
        book->cancelOrder(1, 9999, 9999);
        book->addOrder(1, 1001, 20, OrderType::GoodTillCancel, 1008, 100, Side::BUY);
        book->addOrder(1, 1002, 21, OrderType::GoodTillCancel, 1008, 100, Side::BUY);
        book->addOrder(1, 1007, 22, OrderType::GoodTillCancel, 1008, 100, Side::BUY);
        book->addOrder(1, 1003, 23, OrderType::GoodTillCancel, 1008, 300, Side::SELL);
    } catch (std::exception& e) {
        std::cout << col::RED << "FAIL — " << e.what() << col::RST << '\n';
        return false;
    } catch (...) {
        std::cout << col::RED << "FAIL" << col::RST << '\n';
        return false;
    }
    std::cout << col::GRN << "PASS" << col::RST << '\n';
    return true;
}

// ============================================================
//  BENCHMARK ENGINE
// ============================================================
struct BenchResult {
    std::string             name;
    std::vector<PhaseStats> phases;
    bool                    correct;
};

template<typename Book>
BenchResult runAllPhases(const std::vector<std::vector<SimOrder>>& phases, const std::vector<SimOrder>& warmup) {
    using Clock = std::chrono::high_resolution_clock;
    BenchResult result;
    result.name    = Book::NAME;
    result.correct = true;

    for (auto& phaseOrders : phases) {
        auto* book = new Book(POOL_SIZE, 1, nullptr, nullptr);
        for (auto& o : warmup) {
            if (o.isCancel)
                book->cancelOrder(1, o.clientId, o.cancelTargetId);
            else
                book->addOrder(1, o.clientId, o.clientOrderId, o.type, o.price, o.qty, o.side);
        }

        std::vector<double> samples;
        samples.reserve(phaseOrders.size());
        long long totalNs = 0;

        for (const auto& o : phaseOrders) {
            auto t0 = Clock::now();
            if (o.isCancel)
                book->cancelOrder(1, o.clientId, o.cancelTargetId);
            else
                book->addOrder(1, o.clientId, o.clientOrderId, o.type, o.price, o.qty, o.side);
            auto   t1 = Clock::now();
            double ns = static_cast<double>(std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count());
            samples.push_back(ns);
            totalNs += static_cast<long long>(ns);
        }
        result.phases.push_back(computeStats(samples, totalNs));
        delete book;
    }
    return result;
}

// ============================================================
//  PRINT PER-PHASE
// ============================================================
void printPhaseTable(const BenchResult& r, const std::vector<std::string>& phaseNames) {
    std::cout << '\n' << col::BOLD << col::CYN << "  " << r.name << col::RST << '\n';
    line();
    std::cout << col::DIM << "  " << std::left << std::setw(12) << "Phase" << std::right << std::setw(9) << "avg"
              << std::right << std::setw(9) << "p50" << std::right << std::setw(9) << "p99" << std::right
              << std::setw(10) << "p999" << std::right << std::setw(10) << "max" << std::right << std::setw(11)
              << "M ops/s" << std::right << std::setw(7) << "ms" << col::RST << '\n';
    line();

    for (size_t i = 0; i < r.phases.size(); ++i) {
        const auto& s = r.phases[i];
        std::cout << "  " << std::left << std::setw(12) << phaseNames[i];
        auto ns = [](double v) -> std::string {
            std::ostringstream ss;
            ss << std::fixed << std::setprecision(1) << v << "ns";
            return ss.str();
        };
        bool spikey = s.p99 > s.avg * 3.0;

        std::cout << col::YLW << std::right << std::setw(9) << ns(s.avg) << col::RST;
        std::cout << std::right << std::setw(9) << ns(s.p50);
        if (spikey)
            std::cout << col::RED;
        std::cout << std::right << std::setw(9) << ns(s.p99);
        if (spikey)
            std::cout << col::RST;
        std::cout << col::MAG << std::right << std::setw(10) << ns(s.p999) << col::RST;
        std::cout << col::RED << std::right << std::setw(10) << ns(s.max) << col::RST;
        std::cout << col::GRN << std::right << std::setw(9) << std::fixed << std::setprecision(1) << s.throughputM
                  << "M" << col::RST;
        std::cout << col::DIM << std::right << std::setw(7) << s.ms << "ms" << col::RST << '\n';
    }
    line();
}

// ============================================================
//  SUMMARY TABLE
// ============================================================
void printSummaryTable(const std::vector<BenchResult>& results, const std::vector<std::string>& phaseNames) {
    size_t              nPhases = phaseNames.size();
    std::vector<double> bestAvg(nPhases, 1e18), bestP99(nPhases, 1e18);
    for (auto& r : results)
        for (size_t p = 0; p < nPhases; ++p) {
            bestAvg[p] = std::min(bestAvg[p], r.phases[p].avg);
            bestP99[p] = std::min(bestP99[p], r.phases[p].p99);
        }

    line("=");
    std::cout << col::BOLD << col::WHT << "  SUMMARY — avg ns / p99 ns  (* = fastest)\n" << col::RST;
    line("=");
    std::cout << col::DIM << "  " << std::left << std::setw(32) << "Variant";
    for (auto& ph : phaseNames)
        std::cout << std::right << std::setw(15) << ph;
    std::cout << col::RST << '\n';
    line();

    for (auto& r : results) {
        std::cout << "  " << col::WHT << std::left << std::setw(32) << r.name << col::RST;
        for (size_t p = 0; p < nPhases; ++p) {
            bool        best = (r.phases[p].avg == bestAvg[p]);
            std::string s =
                std::to_string((int)r.phases[p].avg) + "/" + std::to_string((int)r.phases[p].p99) + (best ? "*" : " ");
            if (best)
                std::cout << col::GRN << col::BOLD;
            else
                std::cout << col::YLW;
            std::cout << std::right << std::setw(15) << s << col::RST;
        }
        std::cout << '\n';
    }
    line("=");
}

// ============================================================
//  MAIN
// ============================================================
int main() {
    std::cout << col::BOLD << col::CYN << "\n  NanoExchange — Extreme Stress Benchmark\n" << col::RST;
    line("=");

    constexpr size_t N  = 300'000;
    constexpr size_t NW = 50'000;

    std::vector<std::string> phaseNames = {"Realistic", "LargeWS",   "ColdCxl",  "DeepBook", "Fragmntd",
                                           "Sweeps",    "MemeStock", "Pennying", "FlshCrsh"};

    std::cout << "\nGenerating " << phaseNames.size() << " stress phases (" << N << " orders each)...\n";

    std::vector<std::vector<SimOrder>> phases = {
        genRealistic(N, 42), genLargeWorkingSet(N, 1), genColdCancels(N, 2),   genDeepBook(N, 3),   genFragmented(N, 4),
        genSweeps(N, 5),     genDeepQueue(N, 6),       genFlickeringBBO(N), genFlashCrash(N),
    };

    std::cout << "  Event counts: ";
    for (size_t i = 0; i < phases.size(); ++i)
        std::cout << phaseNames[i] << "=" << phases[i].size() << " ";
    std::cout << "\n";

    auto warmup = genRealistic(NW, 99);

    std::cout << '\n';
    line();
    std::cout << col::BOLD << "  CORRECTNESS\n" << col::RST;
    line();
    bool allOk = true;
    {
        auto* b = new MEOrderBook_PointerPool(500000, 1, nullptr, nullptr);
        allOk &= runCorrectness(b, MEOrderBook_PointerPool::NAME);
        delete b;
    }
    {
        auto* b = new MEOrderBook_DirectVector(500000, 1, nullptr, nullptr);
        allOk &= runCorrectness(b, MEOrderBook_DirectVector::NAME);
        delete b;
    }
    {
        auto* b = new MEOrderBook_PointerPreAlloc(500000, 1, nullptr, nullptr);
        allOk &= runCorrectness(b, MEOrderBook_PointerPreAlloc::NAME);
        delete b;
    }
    {
        auto* b = new MEOrderBook_BitmapVector(500000, 1, nullptr, nullptr);
        allOk &= runCorrectness(b, MEOrderBook_BitmapVector::NAME);
        delete b;
    }
    {
        auto* b = new MEOrderBook_StdMap(500000, 1, nullptr, nullptr);
        allOk &= runCorrectness(b, MEOrderBook_StdMap::NAME);
        delete b;
    }

    if (!allOk) {
        std::cout << col::RED << "\n  Correctness failure — aborting.\n" << col::RST;
        return 1;
    }
    std::cout << col::GRN << "  All correct.\n" << col::RST;

    std::cout << '\n';
    line();
    std::cout << col::BOLD << "  BENCHMARKS\n" << col::RST;
    line();

    std::vector<BenchResult> results;
    auto                     runVariant = [&](auto* proto) {
        using Book = std::remove_pointer_t<decltype(proto)>;
        delete proto;
        std::cout << col::BLU << "  Running " << Book::NAME << " ...\n" << col::RST;
        std::cout.flush();
        auto r = runAllPhases<Book>(phases, warmup);
        printPhaseTable(r, phaseNames);
        results.push_back(std::move(r));
    };

    runVariant(new MEOrderBook_PointerPool(1, 1, nullptr, nullptr));
    runVariant(new MEOrderBook_DirectVector(1, 1, nullptr, nullptr));
    runVariant(new MEOrderBook_PointerPreAlloc(1, 1, nullptr, nullptr));
    runVariant(new MEOrderBook_BitmapVector(1, 1, nullptr, nullptr));
    runVariant(new MEOrderBook_StdMap(1, 1, nullptr, nullptr));

    printSummaryTable(results, phaseNames);

    std::cout << '\n';
    line();
    std::cout << col::BOLD << "  PHASE GUIDE\n" << col::RST;
    line();
    std::cout << col::DIM << "  Realistic   — mixed flow, baseline\n"
              << "  LargeWS     — prices 1-240k, forces all PriceLevels out of L3\n"
              << "  ColdCxl     — inserts + shuffled cancel, cold orders_ map\n"
              << "  DeepBook    — 5000 levels/side, O(depth) insertPricelevel cost\n"
              << "  Fragmntd    — random unique prices, immediate cancel, max pool churn\n"
              << "  Sweeps      — bursty IOC clusters, worst-case match() inner loop\n"
              << "  MemeStock   — deep queue, random middle cancels (stresses unordered_map & doubly linked list)\n"
              << "  Pennying    — thrashing best_bid / branch predictor\n"
              << "  FlshCrsh    — single market order wipes 10,000 price levels instantly\n"
              << col::RST;
    line();
    std::cout << col::DIM << "\n  Shutdown complete.\n" << col::RST;
    return 0;
}
