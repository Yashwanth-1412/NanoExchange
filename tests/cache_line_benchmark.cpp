#include "QuantLink/Lib/concurrency/lf_queue.h"
#include "src/types.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <immintrin.h>
#include <iomanip>
#include <iostream>
#include <pthread.h>
#include <sched.h>
#include <thread>
#include <type_traits>
#include <unistd.h>
#include <vector>
using namespace nanoexchange;

namespace {

constexpr size_t ITERATIONS  = 2'000'000;
constexpr size_t REPETITIONS = 5;
constexpr size_t QUEUE_SIZE  = 1 << 12;

template<size_t Bytes>
struct Payload {
    std::array<std::byte, Bytes> bytes{};

    static_assert(sizeof(bytes) == Bytes);
};

struct QueueResult {
    double   seconds  = 0.0;
    uint64_t checksum = 0;
    bool     pinned   = false;
};

std::vector<int> allowedCores() {
    cpu_set_t allowed;
    CPU_ZERO(&allowed);
    if (sched_getaffinity(0, sizeof(allowed), &allowed) != 0)
        return {};

    std::vector<int> cores;
    for (int core = 0; core < CPU_SETSIZE; ++core) {
        if (CPU_ISSET(core, &allowed))
            cores.push_back(core);
    }
    return cores;
}

bool pinCurrentThread(int core) {
    cpu_set_t requested;
    CPU_ZERO(&requested);
    CPU_SET(core, &requested);
    return pthread_setaffinity_np(pthread_self(), sizeof(requested), &requested) == 0;
}

template<typename T>
T makeSample() {
    T     value{};
    auto* bytes          = reinterpret_cast<unsigned char*>(&value);
    bytes[0]             = 0x5A;
    bytes[sizeof(T) - 1] = 0xA5;
    return value;
}

template<typename T>
QueueResult runQueue(const std::vector<int>& cores) {
    static_assert(std::is_trivially_destructible_v<T>);

    quantlink::SPSCQueue<T> queue(QUEUE_SIZE);
    const T                 sample = makeSample<T>();
    std::atomic<size_t>     ready{0};
    std::atomic<bool>       start{false};
    std::atomic<bool>       pinned{true};
    uint64_t                checksum = 0;

    const auto producer = [&]() {
        const int core = cores.size() >= 2 ? cores[0] : -1;
        if (core >= 0 && !pinCurrentThread(core))
            pinned.store(false, std::memory_order_relaxed);

        ready.fetch_add(1, std::memory_order_release);
        while (!start.load(std::memory_order_acquire))
            _mm_pause();

        for (size_t i = 0; i < ITERATIONS; ++i) {
            while (!queue.push(sample))
                _mm_pause();
        }
    };

    const auto consumer = [&]() {
        const int core = cores.size() >= 2 ? cores[1] : -1;
        if (core >= 0 && !pinCurrentThread(core))
            pinned.store(false, std::memory_order_relaxed);

        ready.fetch_add(1, std::memory_order_release);
        while (!start.load(std::memory_order_acquire))
            _mm_pause();

        T value{};
        for (size_t i = 0; i < ITERATIONS; ++i) {
            while (!queue.pop(value))
                _mm_pause();

            const auto* bytes = reinterpret_cast<const unsigned char*>(&value);
            checksum += bytes[0] + bytes[sizeof(T) - 1];
        }
    };

    std::thread producer_thread(producer);
    std::thread consumer_thread(consumer);

    while (ready.load(std::memory_order_acquire) != 2)
        _mm_pause();

    const auto start_time = std::chrono::steady_clock::now();
    start.store(true, std::memory_order_release);
    producer_thread.join();
    consumer_thread.join();
    const auto end_time = std::chrono::steady_clock::now();

    return {
        std::chrono::duration<double>(end_time - start_time).count(),
        checksum,
        pinned.load(std::memory_order_relaxed) && cores.size() >= 2,
    };
}

template<typename T>
void printLayout(const char* name) {
    const size_t cache_lines = (sizeof(T) + 63) / 64;
    const size_t queue_bytes = QUEUE_SIZE * sizeof(T);

    std::cout << std::left << std::setw(20) << name << " size=" << std::setw(3) << sizeof(T)
              << " align=" << std::setw(2) << alignof(T) << " min_cache_lines=" << std::setw(2) << cache_lines
              << " queue_storage=" << queue_bytes << " bytes\n";
}

template<typename T>
void benchmarkQueue(const char* name, const std::vector<int>& cores) {
    // The first run includes initial page faults; discard it before comparing sizes.
    (void)runQueue<T>(cores);

    std::array<double, REPETITIONS> seconds{};
    uint64_t                        checksum = 0;
    bool                            pinned   = false;
    for (size_t repetition = 0; repetition < REPETITIONS; ++repetition) {
        const QueueResult result = runQueue<T>(cores);
        seconds[repetition]      = result.seconds;
        checksum ^= result.checksum;
        pinned = result.pinned;
    }

    std::sort(seconds.begin(), seconds.end());
    const double median_seconds      = seconds[REPETITIONS / 2];
    const double messages_per_second = static_cast<double>(ITERATIONS) / median_seconds;
    const double bytes_per_second    = messages_per_second * sizeof(T);

    std::cout << std::left << std::setw(20) << name << " size=" << std::setw(3) << sizeof(T)
              << " median_Mmsg/s=" << std::fixed << std::setprecision(2) << std::setw(8)
              << messages_per_second / 1'000'000.0 << " median_GB/s=" << std::setw(8)
              << bytes_per_second / 1'000'000'000.0 << " pinned=" << (pinned ? "yes" : "no") << " checksum=" << checksum
              << '\n';
}

long cacheLineBytes() {
#ifdef _SC_LEVEL1_DCACHE_LINESIZE
    const long detected = sysconf(_SC_LEVEL1_DCACHE_LINESIZE);
    if (detected > 0)
        return detected;
#endif
    return 0;
}

} // namespace

int main() {
    const std::vector<int> cores = allowedCores();

    std::cout << "cache_line_bytes=" << cacheLineBytes() << '\n'
              << "allowed_cores=" << cores.size() << '\n'
              << "iterations_per_thread=" << ITERATIONS << '\n'
              << "repetitions=" << REPETITIONS << '\n'
              << "queue_size_argument=" << QUEUE_SIZE << " (effective capacity is size - 1)\n\n";

    std::cout << "actual_exchange_struct_layouts\n";
    printLayout<MEClientRequest>("MEClientRequest");
    printLayout<MEClientResponse>("MEClientResponse");
    printLayout<MEMarketUpdate>("MEMarketUpdate");
    printLayout<MEOrder>("MEOrder");
    printLayout<SnapshotUpdate>("SnapshotUpdate");

    std::cout << "\nsynthetic_struct_layouts\n";
    printLayout<Payload<32>>("Payload<32>");
    printLayout<Payload<40>>("Payload<40>");
    printLayout<Payload<64>>("Payload<64>");
    printLayout<Payload<80>>("Payload<80>");
    printLayout<Payload<88>>("Payload<88>");
    printLayout<Payload<96>>("Payload<96>");
    printLayout<Payload<128>>("Payload<128>");

    if (cores.size() < 2)
        std::cout << "warning=less_than_two_allowed_cores; queue result may be scheduler-dependent\n";

    std::cout << "\nspsc_queue_throughput\n";
    benchmarkQueue<MEClientRequest>("MEClientRequest", cores);
    benchmarkQueue<MEClientResponse>("MEClientResponse", cores);
    benchmarkQueue<MEMarketUpdate>("MEMarketUpdate", cores);
    benchmarkQueue<Payload<32>>("Payload<32>", cores);
    benchmarkQueue<Payload<40>>("Payload<40>", cores);
    benchmarkQueue<Payload<64>>("Payload<64>", cores);
    benchmarkQueue<Payload<80>>("Payload<80>", cores);
    benchmarkQueue<Payload<88>>("Payload<88>", cores);
    benchmarkQueue<Payload<96>>("Payload<96>", cores);
    benchmarkQueue<Payload<128>>("Payload<128>", cores);

    return 0;
}
