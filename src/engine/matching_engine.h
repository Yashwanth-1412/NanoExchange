#pragma once

#include "order_book_map.h"
#include "../types.h"
#include "QuantLink/Lib/concurrency/lf_queue.h"


class MatchingEngine {

private:
    quantlink::SPSCQueue<MEClientRequest>* requests_ = nullptr;
    quantlink::SPSCQueue<MEClientResponse>* responses_ = nullptr;
    quantlink::SPSCQueue<MEMarketUpdate>* marketUpdates_ = nullptr;
    


};