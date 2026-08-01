#include <iostream>

#include "QuantLink/Lib/concurrency/lf_queue.h"
#include "QuantLink/Lib/logging/logger.h"
#include "src/network/gateway/fifo_process.h"

using namespace quantlink;

namespace {

int passed = 0;
int failed = 0;

auto check(bool condition, const char* message) -> void {
    if (condition) {
        std::cout << "PASS: " << message << '\n';
        ++passed;
    } else {
        std::cout << "FAIL: " << message << '\n';
        ++failed;
    }
}

auto request(OrderId id) -> MEClientRequest {
    return {ClientRequestType::NEW, 1, 0, id, OrderType::GoodTillCancel, Side::BUY, 100, 1};
}

}  // namespace

int main() {
    SPSCQueue<MEClientRequest> requests(4);  // Effective capacity is three.
    SPSCQueue<MEClientResponse> responses(4);
    Logger logger(1024, "test_queue_backpressure.log", -1);
    FifoNdProcess fifo(&requests, &responses, &logger, 5);

    check(fifo.addToPending(30, request(3)), "accept first request");
    check(fifo.addToPending(10, request(1)), "accept second request");
    check(fifo.addToPending(20, request(2)), "accept third request");
    check(fifo.addToPending(50, request(5)), "accept fourth request");
    check(fifo.addToPending(40, request(4)), "accept fifth request");

    fifo.publishPendingOrders();
    check(fifo.pendingSize() == 2, "full request queue retains unsent requests");

    for (OrderId expected : {1, 2, 3}) {
        const auto* queued = requests.getNextRead();
        check(queued != nullptr && queued->client_order_id_ == expected, "published requests remain timestamp ordered");
        requests.updateNextRead();
    }

    fifo.publishPendingOrders();
    check(fifo.pendingSize() == 0, "retained requests publish after capacity returns");

    for (OrderId expected : {4, 5}) {
        const auto* queued = requests.getNextRead();
        check(queued != nullptr && queued->client_order_id_ == expected, "retried request remains ordered and undropped");
        requests.updateNextRead();
    }

    std::cout << "Results: " << passed << " passed, " << failed << " failed\n";
    return failed == 0 ? 0 : 1;
}
