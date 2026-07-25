#pragma once

#include <cstdint>
#include <cstring>

#include "../../types.h"
#include "QuantLink/Lib/protocol/itch_messages.h"
#include "QuantLink/Lib/logging/time_utils.h"

using namespace quantlink::itch;

constexpr size_t ITCH_MAX_MSG_SIZE = 40;

inline void writeTimestamp(uint8_t* dst) noexcept {
    uint64_t ns_since_midnight = quantlink::utils::get_nanos_since_midnight();

    // ITCH requires a 6-byte big-endian timestamp
    dst[0] = (ns_since_midnight >> 40) & 0xFF;
    dst[1] = (ns_since_midnight >> 32) & 0xFF;
    dst[2] = (ns_since_midnight >> 24) & 0xFF;
    dst[3] = (ns_since_midnight >> 16) & 0xFF;
    dst[4] = (ns_since_midnight >> 8) & 0xFF;
    dst[5] = (ns_since_midnight >> 0) & 0xFF;
}

inline uint16_t getStockLocate(TickerId ticker_id) noexcept {

    // IM USING TICKER ID AS DIRECT OUCH STRING (64 bits cannot cast it to 16 bits)
    return 0; 
}

inline void copyStock(char* dst, TickerId ticker_id) noexcept {
    *reinterpret_cast<uint64_t*>(dst) = static_cast<uint64_t>(ticker_id);
}

inline size_t encode(const MEMarketUpdate& u, uint64_t match_number, void* buf) noexcept {
    switch (u.type_) {

        case UpdateType::ADD: {
            AddOrder msg{};

            msg.msgtype_ = enums::MsgType::ADD_ORDER;
            msg.stock_locate = swap16(getStockLocate(u.ticker_id_));
            msg.tracking_number = 0;
            writeTimestamp(msg.timestamp);
            msg.order_ref_num  = swap64(u.market_order_id_);
            msg.buy_sell = (u.side_ == Side::BUY) ? 'B' : 'S';
            msg.shares = swap32(static_cast<uint32_t>(u.qty_));
            
            // This now executes in 1 instruction
            copyStock(msg.stock, u.ticker_id_); 
            
            // TODO: Remove * 100 when ouch_processor stores raw 1/10000 value.
            // Should be: msg.price = swap32(static_cast<uint32_t>(u.price_));
            msg.price = swap32(static_cast<uint32_t>(u.price_ * 100));

            std::memcpy(buf, &msg, sizeof(msg));
            return sizeof(msg);
        }

        case UpdateType::CANCEL: {
            OrderDelete msg{};

            msg.type = enums::MsgType::ORDER_DELETE;
            msg.stock_locate = swap16(getStockLocate(u.ticker_id_));
            msg.tracking_number = 0;
            writeTimestamp(msg.timestamp);
            msg.order_ref_num = swap64(u.market_order_id_);

            std::memcpy(buf, &msg, sizeof(msg));
            return sizeof(msg);
        }

        case UpdateType::TRADE: {
            OrderExecuted msg{};

            msg.type = enums::MsgType::ORDER_EXECUTED;
            msg.stock_locate = swap16(getStockLocate(u.ticker_id_));
            msg.tracking_number = 0;
            writeTimestamp(msg.timestamp);
            msg.order_ref_num = swap64(u.market_order_id_);
            msg.executed_shares = swap32(static_cast<uint32_t>(u.qty_));
            msg.match_number = swap64(match_number);

            std::memcpy(buf, &msg, sizeof(msg));
            return sizeof(msg);
        }

        case UpdateType::MODIFY: {
            OrderReplace msg{};
 
            msg.type = enums::MsgType::ORDER_REPLACE;
            msg.stock_locate = swap16(getStockLocate(u.ticker_id_));
            msg.tracking_number = 0;
            writeTimestamp(msg.timestamp);
            msg.original_order_ref = swap64(u.market_order_id_);
            msg.new_order_ref = swap64(u.new_order_id_);
            msg.shares = swap32(static_cast<uint32_t>(u.qty_));
            msg.price = swap32(static_cast<uint32_t>(u.price_ * 100));

            std::memcpy(buf, &msg, sizeof(msg));
            return sizeof(msg);
        }

        // Snapshot control messages — not sent on the incremental ITCH feed
        case UpdateType::SNAPSHOT_START:
        case UpdateType::SNAPSHOT_END:
        case UpdateType::CLEAR:
        default:
            return 0;
    }
}