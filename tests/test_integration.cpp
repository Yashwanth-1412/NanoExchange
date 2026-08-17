#include "QuantLink/Lib/concurrency/lf_queue.h"
#include "QuantLink/Lib/logging/logger.h"
#include "src/engine/matching_engine.h"
#include "src/engine/order_book/order_book_map.h"
#include "src/network/market/marketdata_publisher.h"
#include "src/types.h"

#include <atomic>
#include <chrono>
#include <csignal>
#include <iostream>
#include <random>
#include <thread>
#include <vector>

using namespace quantlink;
using namespace nanoexchange;

// ── Config ────────────────────────────────────────────────────────────────────

constexpr size_t NUM_TICKERS       = 4;
constexpr size_t ENGINE_POOL_SIZE  = 4096;
constexpr size_t QUEUE_SIZE        = 1024;
constexpr size_t LOGGER_QUEUE_SIZE = 8 * 1024 * 1024;

constexpr uint64_t ORDER_DELAY_US = 1e6; // microseconds between orders — tune to watch visualizer

// Multicast config — must match visualizer.py
const std::string INCREMENTAL_IP   = "127.0.0.1";
const std::string SNAPSHOT_IP      = "127.0.0.1";
const std::string IFACE            = "lo"; // loopback interface
constexpr int     INCREMENTAL_PORT = 20001;
constexpr int     SNAPSHOT_PORT    = 20002;

// ── Signal handling ───────────────────────────────────────────────────────────

static std::atomic<bool> g_running{true};

void sigHandler(int) {
    g_running.store(false, std::memory_order_release);
}

// ── Random order generator ────────────────────────────────────────────────────

struct SimOrder {
    ClientId  client_id_;
    OrderId   client_order_id_;
    TickerId  ticker_id_;
    OrderType type_;
    Side      side_;
    Price     price_;
    Quantity  qty_;
    bool      is_cancel_;
    OrderId   cancel_target_;
};

class RandomOrderGenerator {
  private:
    std::mt19937 rng_;

    std::uniform_int_distribution<int>      client_dist_{1, 10};
    std::uniform_int_distribution<int>      ticker_dist_{0, (int)NUM_TICKERS - 1};
    std::uniform_int_distribution<int>      side_dist_{0, 1};
    std::uniform_int_distribution<int>      type_dist_{0, 9}; // 10% FAK
    std::uniform_int_distribution<uint32_t> qty_dist_{10, 500};
    std::uniform_int_distribution<int>      cancel_dist_{0, 99};

    // Per-ticker mid price
    std::array<Price, NUM_TICKERS>     mid_prices_;
    std::uniform_int_distribution<int> drift_dist_{-2, 2};
    std::uniform_int_distribution<int> edge_dist_{1, 5};

    // Live order tracking for cancels
    struct LiveOrder {
        ClientId client_id_;
        OrderId  order_id_;
        TickerId ticker_id_;
    };
    std::vector<LiveOrder> live_orders_;

    OrderId next_order_id_ = 1;
    size_t  order_count_   = 0;

  public:
    explicit RandomOrderGenerator(uint64_t seed = 42)
        : rng_(seed) {
        for (size_t i = 0; i < NUM_TICKERS; i++)
            mid_prices_[i] = 10000 + i * 500; // each ticker starts at different price
    }

    SimOrder next() {
        ++order_count_;

        // Drift mid prices every 100 orders
        if (order_count_ % 100 == 0) {
            for (auto& mid : mid_prices_) {
                int drift = drift_dist_(rng_);
                mid       = static_cast<Price>(std::max(1000LL, (long long)mid + drift));
            }
        }

        TickerId ticker = ticker_dist_(rng_);
        Price    mid    = mid_prices_[ticker];

        // Random cancel ~20% of the time if we have live orders
        if (!live_orders_.empty() && cancel_dist_(rng_) < 20) {
            std::uniform_int_distribution<size_t> pick(0, live_orders_.size() - 1);
            size_t                                idx    = pick(rng_);
            LiveOrder                             target = live_orders_[idx];
            live_orders_.erase(live_orders_.begin() + idx);

            return SimOrder{target.client_id_, 0, target.ticker_id_, OrderType::GoodTillCancel, Side::BUY, 0, 0, true,
                            target.order_id_};
        }

        // New order
        Side      side = side_dist_(rng_) ? Side::BUY : Side::SELL;
        int       edge = edge_dist_(rng_);
        OrderType type = (type_dist_(rng_) == 0) ? OrderType::FillAndKill : OrderType::GoodTillCancel;

        // Aggressive ~30% of the time (crosses spread), passive otherwise
        bool  aggressive = (cancel_dist_(rng_) < 30);
        Price price;
        if (aggressive) {
            price = (side == Side::BUY) ? mid + edge : mid - edge;
        } else {
            price = (side == Side::BUY) ? mid - edge : mid + edge;
        }
        price = std::max((Price)1, price);

        OrderId  oid = next_order_id_++;
        ClientId cid = client_dist_(rng_);
        Quantity qty = qty_dist_(rng_);

        if (type == OrderType::GoodTillCancel) {
            live_orders_.push_back({cid, oid, ticker});
            // Cap live orders to avoid unbounded growth
            if (live_orders_.size() > 500) {
                live_orders_.erase(live_orders_.begin(), live_orders_.begin() + 100);
            }
        }

        return SimOrder{cid, oid, ticker, type, side, price, qty, false, 0};
    }
};

// ── Stats ─────────────────────────────────────────────────────────────────────

struct Stats {
    std::atomic<uint64_t> orders_sent{0};
    std::atomic<uint64_t> cancels_sent{0};
    std::atomic<uint64_t> responses_received{0};
};

// ── Main ──────────────────────────────────────────────────────────────────────

int main() {
    std::signal(SIGINT, sigHandler);

    std::cout << "\n";
    std::cout << "═══════════════════════════════════════════════════════\n";
    std::cout << "  NanoExchange — Integration Test\n";
    std::cout << "═══════════════════════════════════════════════════════\n";
    std::cout << "  Tickers     : " << NUM_TICKERS << "\n";
    std::cout << "  Order delay : " << ORDER_DELAY_US << " us\n";
    std::cout << "  Incremental : " << INCREMENTAL_IP << ":" << INCREMENTAL_PORT << "\n";
    std::cout << "  Snapshot    : " << SNAPSHOT_IP << ":" << SNAPSHOT_PORT << "\n";
    std::cout << "  Start visualizer.py then open index.html\n";
    std::cout << "  Press Ctrl+C to stop\n";
    std::cout << "═══════════════════════════════════════════════════════\n\n";

    // ── Queues ────────────────────────────────────────────────────────────────

    SPSCQueue<MEClientRequest>  requestQ(QUEUE_SIZE);
    SPSCQueue<MEClientResponse> responseQ(QUEUE_SIZE);
    SPSCQueue<MEMarketUpdate>   marketUpdateQ(QUEUE_SIZE);

    // ── Logger ────────────────────────────────────────────────────────────────

    quantlink::Logger logger(LOGGER_QUEUE_SIZE, "integration_test.log", -1);

    // ── Matching Engine ───────────────────────────────────────────────────────

    MatchingEngine engine(&requestQ, &responseQ, &marketUpdateQ, &logger, NUM_TICKERS, ENGINE_POOL_SIZE);

    // ── Market Data Publisher ─────────────────────────────────────────────────

    MarketDataPublisher publisher(&marketUpdateQ, &logger, NUM_TICKERS, IFACE, INCREMENTAL_PORT, SNAPSHOT_PORT);

    // ── Start everything ──────────────────────────────────────────────────────

    publisher.start();
    engine.start();

    std::cout << "[INFO] Engine and publisher started.\n\n";

    // ── Stats thread ──────────────────────────────────────────────────────────

    Stats stats;

    std::thread stats_thread([&]() {
        while (g_running.load(std::memory_order_acquire)) {
            std::this_thread::sleep_for(std::chrono::seconds(1));

            std::cout << "\r  Orders: " << stats.orders_sent.load() << "  Cancels: " << stats.cancels_sent.load()
                      << "  Responses: " << stats.responses_received.load()
                      << "  MktUpdates: " << publisher.publishedCount() << "    " << std::flush;
        }
    });

    // ── Response drain thread ─────────────────────────────────────────────────
    // Drain responseQ so it doesn't fill up and block the engine

    std::thread response_thread([&]() {
        while (g_running.load(std::memory_order_acquire)) {
            const auto* resp = responseQ.getNextRead();
            if (!resp) {
                std::this_thread::yield();
                continue;
            }
            stats.responses_received.fetch_add(1, std::memory_order_relaxed);
            responseQ.updateNextRead();
        }
    });

    // ── Market update counter thread ──────────────────────────────────────────
    // MarketDataPublisher drains marketUpdateQ internally, but we track count here
    // by hooking into the stats — publisher already drains it so we just count responses

    // ── Order generation loop ─────────────────────────────────────────────────

    RandomOrderGenerator gen(42);

    while (g_running.load(std::memory_order_acquire)) {

        SimOrder order = gen.next();

        if (order.is_cancel_) {
            MEClientRequest req{ClientRequestType::CANCEL,
                                order.client_id_,
                                order.ticker_id_,
                                order.cancel_target_,
                                OrderType::GoodTillCancel,
                                Side::BUY,
                                0,
                                0};
            requestQ.push(req);
            stats.cancels_sent.fetch_add(1, std::memory_order_relaxed);

        } else {
            MEClientRequest req{ClientRequestType::NEW, order.client_id_, order.ticker_id_,
                                order.client_order_id_, order.type_,      order.side_,
                                order.price_,           order.qty_};
            requestQ.push(req);
            stats.orders_sent.fetch_add(1, std::memory_order_relaxed);
        }

        // Small delay so visualizer can keep up
        std::this_thread::sleep_for(std::chrono::microseconds(ORDER_DELAY_US));
    }

    // ── Shutdown ──────────────────────────────────────────────────────────────

    std::cout << "\n\n[INFO] Stopping...\n";

    engine.stop();
    publisher.stop();

    stats_thread.join();
    response_thread.join();

    std::cout << "\n[INFO] Final stats:\n";
    std::cout << "  Orders sent     : " << stats.orders_sent.load() << "\n";
    std::cout << "  Cancels sent    : " << stats.cancels_sent.load() << "\n";
    std::cout << "  Responses recv  : " << stats.responses_received.load() << "\n";
    std::cout << "[INFO] Log written to: integration_test.log\n";
    std::cout << "[INFO] Done.\n\n";

    return 0;
}
