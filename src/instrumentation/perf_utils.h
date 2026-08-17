#pragma once

#include "QuantLink/Lib/logging/logger.h"
#include "QuantLink/Lib/logging/time_utils.h"

#include <cstdint>

namespace nanoexchange::instrumentation {

inline void log_cycles(quantlink::Logger& logger, const char* tag, uint64_t cycles) noexcept {
    logger.log("RDTSC % %", tag, cycles);
}

inline void log_timestamp(quantlink::Logger& logger, const char* tag) noexcept {
    logger.log("TTT % %", tag, quantlink::utils::TimeConverter::get_time());
}

inline void log_timestamp(quantlink::Logger& logger, const char* tag, uint64_t timestamp) noexcept {
    logger.log("TTT % %", tag, timestamp);
}

} // namespace nanoexchange::instrumentation

#define NANOEXCHANGE_START_MEASURE(TAG) const auto TAG##_start_cycles = quantlink::utils::rdtsc_start()

#define NANOEXCHANGE_END_MEASURE(TAG, LOGGER)                                                                          \
    do {                                                                                                               \
        const auto TAG##_end_cycles = quantlink::utils::rdtsc_end();                                                   \
        ::nanoexchange::instrumentation::log_cycles((LOGGER), #TAG, TAG##_end_cycles - TAG##_start_cycles);            \
    } while (false)

#define NANOEXCHANGE_TIMESTAMP(TAG, LOGGER)                                                                            \
    do {                                                                                                               \
        ::nanoexchange::instrumentation::log_timestamp((LOGGER), #TAG);                                                \
    } while (false)

#define NANOEXCHANGE_TIMESTAMP_VALUE(TAG, LOGGER, VALUE)                                                               \
    do {                                                                                                               \
        ::nanoexchange::instrumentation::log_timestamp((LOGGER), #TAG, (VALUE));                                       \
    } while (false)
