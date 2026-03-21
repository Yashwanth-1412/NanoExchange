#pragma once

#include "../types.h"
#include "QuantLink/Lib/concurrency/lf_queue.h"
#include "Lib/logging/logger.h"
#include <atomic>
#include <cstdint>


class MarketDataPublisher {

private:
    quantlink::SPSCQueue<MEMarketUpdate>* marketUpdates_ = nullptr;
    quantlink::SPSCQueue<MEMarketUpdate>* snapshotUpdates_ = nullptr;
    quantlink::Logger* logger_;

    std::atomic<bool> run_{false};
    uint64_t next_sequence_number_ = 1;


public:
    

};