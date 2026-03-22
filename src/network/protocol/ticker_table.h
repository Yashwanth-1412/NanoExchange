#pragma once
#include <array>
#include <cstdint>
#include <cstring>
#include "../../types.h"


constexpr size_t TICKER_TABLE_SIZE = 8;    // ← change this when you add more tickers

struct TickerInfo {
    const char stock[9];           
    uint16_t stock_locate;
};

constexpr std::array<TickerInfo, TICKER_TABLE_SIZE> TICKER_TABLE = {{
    { "AAPL    ", 1 },
    { "MSFT    ", 2 },
    { "TSLA    ", 3 },
    { "GOOGL   ", 4 },
    { "AMZN    ", 5 },
    { "NVDA    ", 6 },
    { "META    ", 7 },
    { "SPY     ", 8 },
}};

inline const TickerInfo& getTickerInfo(TickerId ticker_id) noexcept {
    static constexpr TickerInfo UNKNOWN = { "UNKNOWN ", 0 };
    if (ticker_id >= TICKER_TABLE_SIZE) return UNKNOWN;
    return TICKER_TABLE[ticker_id];
}

inline void copyStock(char* dst, TickerId ticker_id) noexcept {
    const auto& info = getTickerInfo(ticker_id);
    std::memcpy(dst, info.stock, 8);
}