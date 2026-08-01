#include <iostream>
#include <csignal>
#include <atomic>
#include <string>
#include <cstdlib>
#include <thread>
#include <chrono>
#include <fstream>
#include <filesystem>
#include <sstream>
#include <vector>

#include <unistd.h>
#include <fcntl.h>
#include <execinfo.h>
#include <sys/syscall.h>
#include <sys/prctl.h>

// ── Crash diagnostics ─────────────────────────────────────────────────────────
//
// Dumps a backtrace of the crashing thread to /tmp/crash_bt.txt — faster than
// attaching gdb when reproducing races.

static void crashHandler(int sig, siginfo_t* si, void* uc) {
    void* frames[32];
    const int n = backtrace(frames, 32);

    char comm[32] = "?";
    prctl(PR_GET_NAME, comm, 0, 0, 0);

    const int fd = open("/tmp/crash_bt.txt", O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd >= 0) {
        const ucontext_t* ctx = static_cast<const ucontext_t*>(uc);
        const uintptr_t rip = ctx->uc_mcontext.gregs[REG_RIP];
        dprintf(fd, "signal %d, thread '%s', pid %d tid %ld, fault addr %p, RIP %p\n",
                sig, comm, getpid(), syscall(SYS_gettid), si->si_addr, reinterpret_cast<void*>(rip));
        backtrace_symbols_fd(frames, n, fd);
        close(fd);
    }
    _exit(1);
}

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
        << " [core_me] [core_gateway] [core_publisher] [core_streamer] [core_logger]\n"
        << "\nExample:\n"
        << "  " << prog
        << " lo 10000"
        << " 224.0.0.1 20001"
        << " 20002"
        << " lo 1 2 4 6 8\n"
        << "\nNotes:\n"
        << "  core_* = CPU core to pin each thread to (-1 = OS decides).\n"
        << "  Defaults (distinct physical cores on SMT machines):\n"
        << "  core_me=1  core_gateway=2  core_publisher=4  core_streamer=6  core_logger=8\n\n";
}

// ── Thread placement verification ─────────────────────────────────────────────
//
// Prints every thread of this process: current CPU + its physical core id,
// so hot threads (ME, gateway, publisher, streamer, logger) can be confirmed
// to never share a physical core.

static std::string readFile(const std::string& path) {
    std::ifstream f(path);
    std::ostringstream ss;
    ss << f.rdbuf();
    return ss.str();
}

static void printThreadPlacement() {
    std::cout << "\n── Thread placement (PID " << getpid() << ") ───────────────────\n";
    std::cout << std::left
              << std::setw(22) << "THREAD"
              << std::setw(8) << "TID"
              << std::setw(10) << "CPU"
              << "PHYSICAL CORE\n";
    for (const auto& entry : std::filesystem::directory_iterator("/proc/self/task")) {
        const std::string tid = entry.path().filename().string();

        const std::string stat = readFile("/proc/self/task/" + tid + "/stat");
        const size_t rparen = stat.rfind(')');
        std::istringstream iss(stat.substr(rparen + 2));
        std::vector<std::string> f;
        std::string tok;
        while (iss >> tok) f.push_back(tok);
        // After "pid (comm)": fields[0]=state(field 3) ... processor = field 39 → index 36
        if (f.size() <= 36) continue;
        const std::string cpu = f[36];

        const std::string status = readFile("/proc/self/task/" + tid + "/status");
        std::istringstream iss2(status);
        std::string allowed = "?";
        std::string line;
        while (std::getline(iss2, line)) {
            if (line.rfind("Cpus_allowed_list:", 0) == 0) {
                allowed = line.substr(19);
                while (!allowed.empty() && (allowed.back() == '\r' || allowed.back() == '\n'))
                    allowed.pop_back();
                break;
            }
        }

        std::string phys = "?";
        try {
            const std::string coreId = readFile("/sys/devices/system/cpu/cpu" + cpu + "/topology/core_id");
            std::istringstream iss3(coreId);
            iss3 >> phys;
        } catch (...) {}

        std::string comm = "?";
        const std::string c = readFile("/proc/self/task/" + tid + "/comm");
        std::istringstream iss4(c);
        iss4 >> comm;

        std::cout << std::left
                  << std::setw(22) << comm
                  << std::setw(8) << tid
                  << std::setw(10) << cpu
                  << phys << "   (allowed: " << allowed << ")\n";
    }
    std::cout << "───────────────────────────────────────────────────────\n\n";
}

// ── Constants ─────────────────────────────────────────────────────────────────

constexpr size_t NUM_TICKERS       = 4;
constexpr size_t QUEUE_SIZE        = 65536;   // 30ms of engine isolation @ 2M msg/s
constexpr size_t LOGGER_QUEUE_SIZE = 256 * 1024;   // 262144 log msgs ≈ 38MB (MPSC cells); ~30ms burst @ 2M msg/s
constexpr size_t ENGINE_POOL_SIZE  = 65536;        // per ticker book: 56B x 64K x 4 books = 14MB (was 1.9GB!)
constexpr size_t FIFO_PENDING      = 10000;
constexpr size_t TOKEN_POOL_SIZE   = 1000000;      // small gateway tokens
constexpr size_t SNAPSHOT_Q_SIZE   = 65536;   // absorbs bake (3ms) + TCP stall, not 500ms

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
    const int corePublisher = (argc > 9) ? std::atoi(argv[9]) : 4;
    const int coreStreamer  = (argc > 10) ? std::atoi(argv[10]) : 6;
    const int coreLogger    = (argc > 11) ? std::atoi(argv[11]) : 8;

    std::signal(SIGINT,  sigHandler);
    std::signal(SIGTERM, sigHandler);

    struct sigaction sa{};
    sa.sa_sigaction = crashHandler;
    sa.sa_flags = SA_SIGINFO;
    sigemptyset(&sa.sa_mask);
    sigaction(SIGSEGV, &sa, nullptr);
    sigaction(SIGABRT, &sa, nullptr);
    sigaction(SIGBUS,  &sa, nullptr);
    sigaction(SIGFPE,  &sa, nullptr);

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
        << "  Core Streamer    : " << coreStreamer                            << "\n"
        << "  Core Logger      : " << coreLogger                              << "\n"
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

    quantlink::Logger logger(LOGGER_QUEUE_SIZE, "nanoexchange.log", coreLogger);

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
        SNAPSHOT_Q_SIZE, coreStreamer
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

    std::this_thread::sleep_for(std::chrono::milliseconds(200));   // let threads land
    printThreadPlacement();

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
