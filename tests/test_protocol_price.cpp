#include "src/network/protocol/itch_encoder.h"
#include "src/network/protocol/ouch_processor.h"

#include <cstring>
#include <iostream>
using namespace nanoexchange;

namespace {

int failures = 0;

void check(const char* description, bool condition) {
    std::cout << (condition ? "PASS: " : "FAIL: ") << description << '\n';
    if (!condition)
        ++failures;
}

} // namespace

int main() {
    quantlink::ouch::EnterOrder order{};
    order.type               = quantlink::ouch::enums::MsgType::ENTER_ORDER;
    order.shares             = quantlink::ouch::swap32(10);
    order.price              = quantlink::ouch::swap32(1'001'234);
    order.time_in_force      = quantlink::ouch::swap32(0);
    order.buy_sell_indicator = 'B';
    uint64_t ticker          = 1;
    std::memcpy(order.stock, &ticker, sizeof(ticker));

    OrderTokenManager token_manager(16);
    MEClientRequest   request{};
    check("OUCH decode preserves 1/10000 price",
          decodeOuch(reinterpret_cast<const char*>(&order), 1, token_manager, request) && request.price_ == 1'001'234);

    MEMarketUpdate  update{1, 1, UpdateType::ADD, Side::BUY, 1'001'234, 10};
    alignas(8) char buffer[ITCH_MAX_MSG_SIZE]{};
    const size_t    encoded_size = encode(update, buffer);
    const auto&     encoded      = *reinterpret_cast<const quantlink::itch::AddOrder*>(buffer);
    check("ITCH encode preserves 1/10000 price",
          encoded_size == sizeof(quantlink::itch::AddOrder) && quantlink::itch::swap32(encoded.price) == 1'001'234U);

    std::cout << "Results: " << (2 - failures) << " passed, " << failures << " failed\n";
    return failures == 0 ? 0 : 1;
}
