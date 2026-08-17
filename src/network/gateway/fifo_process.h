#pragma once

#include "src/instrumentation/perf_utils.h"
#include "src/types.h"
#include "QuantLink/Lib/concurrency/lf_queue.h"
#include "QuantLink/Lib/logging/logger.h"

#include <algorithm>
#include <cstddef>
#include <vector>

using namespace quantlink;
using namespace nanoexchange;

class FifoNdProcess {
  private:
    SPSCQueue<MEClientRequest>*  requests_;
    SPSCQueue<MEClientResponse>* responses_;
    Logger*                      logger_;

    struct RecvClientRequest {
        size_t          time_nanos_;
        MEClientRequest request_;

        RecvClientRequest() = default;

        RecvClientRequest(size_t time_nanos, const MEClientRequest& request)
            : time_nanos_(time_nanos),
              request_(request) {}
    };

    std::vector<RecvClientRequest> pending_orders_;
    size_t                         pending_size_{0};
    bool                           pending_sorted_{true};

  public:
    FifoNdProcess(SPSCQueue<MEClientRequest>* requests, SPSCQueue<MEClientResponse>* responses, Logger* logger,
                  size_t max_pending = 10000)
        : requests_(requests),
          responses_(responses),
          logger_(logger) {
        pending_orders_.resize(std::max<size_t>(max_pending, 1));
    }

    auto addToPending(size_t time_nanos, const MEClientRequest& request) -> bool {
        NANOEXCHANGE_START_MEASURE(Exchange_FIFOSequencer_addToPending);
        if (pending_size_ >= pending_orders_.size()) {
            NANOEXCHANGE_END_MEASURE(Exchange_FIFOSequencer_addToPending, *logger_);
            return false;
        }

        pending_orders_[pending_size_++] = RecvClientRequest(time_nanos, request);
        pending_sorted_                  = false;
        NANOEXCHANGE_END_MEASURE(Exchange_FIFOSequencer_addToPending, *logger_);
        return true;
    }

    auto publishPendingOrders() -> void {
        if (pending_size_ == 0)
            return;

        NANOEXCHANGE_START_MEASURE(Exchange_FIFOSequencer_publishPendingOrders);

        if (!pending_sorted_) {
            std::sort(
                pending_orders_.begin(), pending_orders_.begin() + pending_size_,
                [](const RecvClientRequest& a, const RecvClientRequest& b) { return a.time_nanos_ < b.time_nanos_; });
            pending_sorted_ = true;
        }

        size_t published = 0;
        while (published < pending_size_) {
            if (!requests_->push(pending_orders_[published].request_))
                break;

            NANOEXCHANGE_TIMESTAMP(T2_OrderServer_LFQueue_write, *logger_);
            ++published;
        }

        if (published != 0) {
            logger_->log("FifoSequencer: Published % requests to Matching Engine\n", published);
            std::move(pending_orders_.begin() + published, pending_orders_.begin() + pending_size_,
                      pending_orders_.begin());
            pending_size_ -= published;
        }

        if (pending_size_ != 0) {
            logger_->log("FifoSequencer: request queue full; retaining % requests for retry\n", pending_size_);
        }

        NANOEXCHANGE_END_MEASURE(Exchange_FIFOSequencer_publishPendingOrders, *logger_);
    }

    auto growPending() -> void {
        pending_orders_.resize(pending_orders_.size() * 2);
        logger_->log("FifoSequencer: pending order buffer expanded to % entries\n", pending_orders_.size());
    }

    auto pendingSize() const noexcept -> size_t { return pending_size_; }
};
