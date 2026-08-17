#pragma once

#include "src/types.h"
#include "src/network/gateway/token_manager.h"
#include "QuantLink/Lib/logging/time_utils.h"
#include "QuantLink/Lib/protocol/ouch_messages.h"

#include <cstdint>
#include <cstring>
#include <fcntl.h>

using namespace quantlink;
using namespace nanoexchange;

inline bool decodeOuch(const char* data, ClientId clientId, OrderTokenManager& token_manager,
                       MEClientRequest& out_request) {
    char mssg_type         = data[0];
    out_request            = MEClientRequest{};
    out_request.client_id_ = clientId;

    switch (mssg_type) {
    case static_cast<char>(ouch::enums::MsgType::ENTER_ORDER): {

        const auto* msg     = reinterpret_cast<const ouch::EnterOrder*>(data);
        out_request.action_ = ClientRequestType::NEW;
        out_request.type_ =
            (ouch::swap32(msg->time_in_force) == 0) ? OrderType::FillAndKill : OrderType::GoodTillCancel;
        out_request.ticker_id_ = ouch::swap64(*reinterpret_cast<const uint64_t*>(msg->stock));
        out_request.client_order_id_ =
            token_manager.registerNewToken(clientId, msg->order_token, out_request.ticker_id_);
        // A failed registration means no token can ever be resolved for this
        // order, so every response we later build for it would be unsendable.
        // Reject at the door instead of admitting an unaddressable order.
        if (!out_request.client_order_id_)
            return false;
        std::memcpy(out_request.order_token_.data(), msg->order_token, OUCH_TOKEN_LEN);
        out_request.side_  = (msg->buy_sell_indicator == 'B') ? Side::BUY : Side::SELL;
        out_request.price_ = ouch::swap32(msg->price);
        out_request.qty_   = msg->get_shares();

        return true;
    }

    case static_cast<char>(ouch::enums::MsgType::CANCEL_ORDER): {
        const auto* msg = reinterpret_cast<const ouch::CancelOrder*>(data);

        out_request.action_          = ClientRequestType::CANCEL;
        out_request.client_order_id_ = token_manager.getID(clientId, msg->order_token);
        if (!out_request.client_order_id_)
            return false;
        std::memcpy(out_request.order_token_.data(), msg->order_token, OUCH_TOKEN_LEN);
        out_request.qty_       = msg->get_shares();
        out_request.ticker_id_ = token_manager.id_to_token_[out_request.client_order_id_].tickerId_;
        return true;
    }

    case static_cast<char>(ouch::enums::MsgType::REPLACE_ORDER): {
        const auto* msg = reinterpret_cast<const ouch::ReplaceOrder*>(data);

        out_request.action_ = ClientRequestType::MODIFY;
        out_request.qty_    = msg->get_shares();
        out_request.price_  = ouch::swap32(msg->price);
        out_request.type_ =
            (ouch::swap32(msg->time_in_force) == 0) ? OrderType::FillAndKill : OrderType::GoodTillCancel;

        out_request.client_order_id_ = token_manager.getID(clientId, msg->existing_order_token);
        if (!out_request.client_order_id_)
            return false;
        out_request.ticker_id_ = token_manager.id_to_token_[out_request.client_order_id_].tickerId_;

        out_request.new_client_order_id_ =
            token_manager.registerNewToken(clientId, msg->replacement_order_token, out_request.ticker_id_);
        if (!out_request.new_client_order_id_)
            return false;
        // Responses for a replace echo the REPLACEMENT token, and the order
        // that survives the replace is the replacement one.
        std::memcpy(out_request.order_token_.data(), msg->replacement_order_token, OUCH_TOKEN_LEN);
        return true;
    }
    };
    return false;
}

// Pure function of the response: the token travels on MEClientResponse, so this
// cannot fail a lookup and silently drop a fill.
inline size_t encodeOuch(char* out_buffer, const MEClientResponse& resp, uint64_t current_ts) {
    const char* token = resp.order_token_.data();

    switch (resp.status_) {
    case ResponseType::ACCEPTED: {
        auto* msg = reinterpret_cast<ouch::OrderAccepted*>(out_buffer);
        std::memset(msg, ' ', sizeof(ouch::OrderAccepted));

        msg->type      = ouch::enums::MsgType::ORDER_ACCEPTED;
        msg->timestamp = ouch::swap64(current_ts);
        std::memcpy(msg->order_token, token, 14);

        msg->buy_sell_indicator                  = (resp.side_ == Side::BUY) ? 'B' : 'S';
        msg->shares                              = ouch::swap32(resp.qty_);
        *reinterpret_cast<uint64_t*>(msg->stock) = ouch::swap64(resp.ticker_id_);
        msg->price                               = ouch::swap32(resp.price_);
        msg->time_in_force                       = ouch::swap32((resp.type_ == OrderType::FillAndKill) ? 0 : 99998);
        msg->order_reference_number              = ouch::swap64(resp.market_order_id_);

        return sizeof(ouch::OrderAccepted);
    }

    case ResponseType::EXECUTED: {
        auto* msg = reinterpret_cast<ouch::OrderExecuted*>(out_buffer);
        std::memset(msg, ' ', sizeof(ouch::OrderExecuted));

        msg->type      = ouch::enums::MsgType::ORDER_EXECUTED;
        msg->timestamp = ouch::swap64(current_ts);
        std::memcpy(msg->order_token, token, 14);

        msg->executed_shares = ouch::swap32(resp.executed_qty_);
        msg->execution_price = ouch::swap32(resp.execution_price_);
        msg->match_number    = ouch::swap64(resp.match_id_);

        return sizeof(ouch::OrderExecuted);
    }

    case ResponseType::CANCELED: {
        auto* msg = reinterpret_cast<ouch::OrderCanceled*>(out_buffer);
        std::memset(msg, ' ', sizeof(ouch::OrderCanceled));

        msg->type      = ouch::enums::MsgType::ORDER_CANCELED;
        msg->timestamp = ouch::swap64(current_ts);
        std::memcpy(msg->order_token, token, 14);

        msg->decrement_shares = ouch::swap32(resp.leaves_qty_);
        msg->reason           = 'U';

        return sizeof(ouch::OrderCanceled);
    }

    case ResponseType::MODIFIED: {
        auto* msg = reinterpret_cast<ouch::OrderReplaced*>(out_buffer);
        std::memset(msg, ' ', sizeof(ouch::OrderReplaced));

        msg->type      = ouch::enums::MsgType::REPLACE_ORDER;
        msg->timestamp = ouch::swap64(current_ts);
        std::memcpy(msg->replacement_order_token, token, 14);

        msg->buy_sell_indicator                  = (resp.side_ == Side::BUY) ? 'B' : 'S';
        msg->shares                              = ouch::swap32(resp.qty_);
        *reinterpret_cast<uint64_t*>(msg->stock) = ouch::swap64(resp.ticker_id_);
        msg->price                               = ouch::swap32(resp.price_);
        msg->time_in_force                       = ouch::swap32((resp.type_ == OrderType::FillAndKill) ? 0 : 99998);
        msg->order_reference_number              = ouch::swap64(resp.market_order_id_);

        return sizeof(ouch::OrderReplaced);
    }

    case ResponseType::CANCEL_REJECTED: {
        auto* msg = reinterpret_cast<ouch::CancelRejected*>(out_buffer);
        std::memset(msg, ' ', sizeof(ouch::CancelRejected));

        msg->type      = ouch::enums::MsgType::CANCEL_REJECTED;
        msg->timestamp = ouch::swap64(current_ts);
        std::memcpy(msg->order_token, token, 14);

        return sizeof(ouch::CancelRejected);
    }

    default:
        return 0;
    }
}
