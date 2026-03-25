#pragma once

#include "QuantLink/Lib/logging/logger.h"
#include "QuantLink/Lib/concurrency/lf_queue.h"

#include "../../types.h"
#include <algorithm>
#include <cstddef>
#include <vector>

using namespace quantlink;

class FifoNdProcess {
private:
    SPSCQueue<MEClientRequest>* requests_;
    SPSCQueue<MEClientResponse>* responses_;
    Logger* logger_;

    struct RecvClientRequest {
        size_t time_nanos_;
        MEClientRequest request_;

        RecvClientRequest() = default;

        RecvClientRequest(size_t time_nanos, const MEClientRequest& request) : 
            time_nanos_(time_nanos),
            request_(request)
        {}
    };

    std::vector<RecvClientRequest> pending_orders_;
    size_t pending_size_{0};

public:
    FifoNdProcess(SPSCQueue<MEClientRequest>* requests, SPSCQueue<MEClientResponse>* responses, Logger* logger, size_t max_pending = 10000) : 
        requests_(requests),
        responses_(responses),
        logger_(logger) 
    {
        pending_orders_.resize(max_pending);
    }

    auto addToPending(size_t time_nanos, const MEClientRequest& request) -> bool {
        if (pending_size_ >= pending_orders_.size()) {
            return false;
        }

        pending_orders_[pending_size_++] = RecvClientRequest(time_nanos, request);
        return true;
    }

    auto publishPendingOrders() -> void {
        
        std::sort(pending_orders_.begin(), pending_orders_.begin() + pending_size_,
            [](const RecvClientRequest& a, const RecvClientRequest& b) {
                return a.time_nanos_ < b.time_nanos_;
            }
        );
        
        logger_->log("FifoSequencer: Publishing % sorted requests to Matching Engine\n", pending_size_);

        for (size_t i = 0; i < pending_size_; ++i) {
            requests_->push(pending_orders_[i].request_);
        }

        pending_size_ = 0;
    }
};