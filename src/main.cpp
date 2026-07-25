#include <iostream>
#include <csignal>
#include <atomic>
#include <string>
#include <cstdlib>
#include <thread>
#include <chrono>

#include "src/engine/matching_engine.h"
#include "src/network/gateway/order_server.h"
#include "src/network/market/marketdata_publisher.h"
#include "src/types.h"
#include "QuantLink/Lib/concurrency/lf_queue.h"
#include "QuantLink/Lib/logging/logger.h"

using namespace quantlink;

// ── Shutdown signal ───────────────────────────────────────────────────────────

static std::atomic<bool> g_running{true};

void sigHandler(int) {
    g_running.store(false, std::memory_order_release);
}

// ── Usage ─────────────────────────────────────────────────────────────────────

static void printUsage(const char* prog) {
    std::cerr
        << "\nUsage: " << prog
        << " <gateway_iface> <gateway_port>"
        << " <incremental_ip> <incremental_port>"
        << " <snapshot_tcp_port>"
        << " <mcast_iface>"
        << " [core_me] [core_gateway] [core_publisher]\n"
        << "\nExample:\n"
        << "  " << prog
        << " lo 10000"
        << " 224.0.0.1 20001"
        << " 20002"
        << " lo 1 2 3\n"
        << "\nNotes:\n"
        << "  core_* = CPU core to pin each thread to (-1 = OS decides)\n"
        << "  Defaults: core_me=1  core_gateway=2  core_publisher=3\n\n";
}

// ── Constants ─────────────────────────────────────────────────────────────────

constexpr size_t NUM_TICKERS       = 4;
constexpr size_t QUEUE_SIZE        = 1024;
constexpr size_t LOGGER_QUEUE_SIZE = 8 * 1024 * 1024;
constexpr size_t ENGINE_POOL_SIZE  = 8 * 1024 * 1024;
constexpr size_t FIFO_PENDING      = 10000;
constexpr size_t TOKEN_POOL_SIZE   = 1000000;
constexpr size_t SNAPSHOT_Q_SIZE   = 1024;

// ── Main ──────────────────────────────────────────────────────────────────────

int main(int argc, char* argv[]) {

    if (argc < 7) {
        printUsage(argv[0]);
        return 1;
    }

    const std::string gatewayIface    = argv[1];
    const int         gatewayPort     = std::atoi(argv[2]);
    const std::string incrementalIp   = argv[3];
    const int         incrementalPort = std::atoi(argv[4]);
    const int         snapshotTcpPort = std::atoi(argv[5]);
    const std::string mcastIface      = argv[6];

    const int coreMe        = (argc > 7) ? std::atoi(argv[7]) : 1;
    const int coreGateway   = (argc > 8) ? std::atoi(argv[8]) : 2;
    const int corePublisher = (argc > 9) ? std::atoi(argv[9]) : 3;

    std::signal(SIGINT,  sigHandler);
    std::signal(SIGTERM, sigHandler);

    std::cout
        << "\n"
        << "╔══════════════════════════════════════════════════════╗\n"
        << "║              NanoExchange  v1.0                      ║\n"
        << "╚══════════════════════════════════════════════════════╝\n"
        << "  Tickers          : " << NUM_TICKERS        << "\n"
        << "  Gateway          : " << gatewayIface << ":" << gatewayPort      << "\n"
        << "  Incremental feed : " << incrementalIp << ":" << incrementalPort << "\n"
        << "  Snapshot TCP     : " << snapshotTcpPort                       << "\n"
        << "  Mcast iface      : " << mcastIface                              << "\n"
        << "  Core ME          : " << coreMe                                  << "\n"
        << "  Core Gateway     : " << coreGateway                             << "\n"
        << "  Core Publisher   : " << corePublisher                           << "\n"
        << "  Press Ctrl+C to stop\n\n";

    // ── Queues ────────────────────────────────────────────────────────────────
    //
    //  requestQ     : OrderServer       → MatchingEngine        (inbound orders)
    //  responseQ    : MatchingEngine    → OrderServer           (private fills)
    //  marketUpdateQ: MatchingEngine    → MarketDataPublisher   (public feed)

    SPSCQueue<MEClientRequest>  requestQ(QUEUE_SIZE);
    SPSCQueue<MEClientResponse> responseQ(QUEUE_SIZE);
    SPSCQueue<MEMarketUpdate>   marketUpdateQ(QUEUE_SIZE);

    // ── Logger ────────────────────────────────────────────────────────────────

    quantlink::Logger logger(LOGGER_QUEUE_SIZE, "nanoexchange.log", -1);

    // ── Components ────────────────────────────────────────────────────────────

    MatchingEngine engine(
        &requestQ, &responseQ, &marketUpdateQ, &logger,
        NUM_TICKERS, ENGINE_POOL_SIZE
    );

    MarketDataPublisher publisher(
        &marketUpdateQ, &logger,
        NUM_TICKERS,
        incrementalIp, mcastIface,
        incrementalPort, snapshotTcpPort,
        SNAPSHOT_Q_SIZE
    );

    OrderServer gateway(
        &requestQ, &responseQ, &logger,
        gatewayIface, gatewayPort,
        FIFO_PENDING, TOKEN_POOL_SIZE
    );

    // ── Start ─────────────────────────────────────────────────────────────────
    //
    //  Publisher first — must be ready before engine emits market updates.
    //  Engine second  — must be ready before gateway accepts client orders.
    //  Gateway last   — starts accepting connections only when everything is up.

    publisher.start(corePublisher);
    engine.start(coreMe);
    gateway.start(coreGateway);

    std::cout << "[INFO] NanoExchange is running.\n\n";

    // ── Wait ──────────────────────────────────────────────────────────────────

    while (g_running.load(std::memory_order_acquire))
        std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // ── Shutdown ──────────────────────────────────────────────────────────────
    //
    //  Reverse order: gateway first (stop new orders), engine drains the queue,
    //  publisher last (engine may still be flushing market updates).

    std::cout << "\n[INFO] Shutting down...\n";

    gateway.stop();
    std::cout << "[INFO] Gateway stopped.\n";

    engine.stop();
    std::cout << "[INFO] Engine stopped.\n";

    publisher.stop();
    std::cout << "[INFO] Publisher stopped.\n";

    std::cout << "[INFO] Log written to: nanoexchange.log\n";
    std::cout << "[INFO] Done.\n\n";

    return 0;
}
