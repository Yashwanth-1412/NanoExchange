#pragma once

#include <cstdint>
#include <cstring>
#include <fcntl.h>

#include "../../types.h"
#include "QuantLink/Lib/protocol/ouch_messages.h"
#include "QuantLink/Lib/logging/time_utils.h"
#include "../gateway/token_manager.h"

using namespace quantlink;

inline bool decodeOuch (const char* data, ClientId clientId, OrderTokenManager& token_manager, 
                        MEClientRequest& out_request) 
{
    char mssg_type = data[0];
    out_request = MEClientRequest {};
    out_request.client_id_ = clientId;

    switch (mssg_type) {
        case static_cast<char> (ouch::enums::MsgType::ENTER_ORDER) : {

            const auto* msg  = reinterpret_cast<const ouch::EnterOrder*> (data);
            out_request.action_ = ClientRequestType::NEW;
            out_request.type_ = (ouch::swap32(msg->time_in_force) == 0) ? OrderType::FillAndKill : OrderType::GoodTillCancel;
            out_request.ticker_id_ = *reinterpret_cast<const uint64_t*>(msg->stock);
            out_request.client_order_id_ = token_manager.registerNewToken(msg->order_token, out_request.ticker_id_);
            out_request.side_ = (msg->buy_sell_indicator == 'B')? Side::BUY : Side::SELL;
            out_request.price_ = msg->get_price();
            out_request.qty_ = msg->get_shares();
            

            return true;
        } 

        case static_cast<char>(ouch::enums::MsgType::CANCEL_ORDER) : {
            const auto* msg = reinterpret_cast<const ouch::CancelOrder*>(data);

            out_request.action_ = ClientRequestType::CANCEL;
            out_request.client_order_id_ = token_manager.getID(msg->order_token);
            out_request.qty_ = msg->get_shares();
            out_request.ticker_id_ = token_manager.id_to_token_[out_request.client_order_id_].tickerId_;
            return out_request.client_order_id_ != 0;
        }

        case static_cast<char>(ouch::enums::MsgType::REPLACE_ORDER
        ) : {
            const auto* msg = reinterpret_cast<const ouch::ReplaceOrder*>(data);

            out_request.action_= ClientRequestType::MODIFY;
            out_request.qty_ = msg->get_shares();
            out_request.price_ = msg->get_price();
            out_request.type_ = (ouch::swap32(msg->time_in_force) == 0) ? OrderType::FillAndKill : OrderType::GoodTillCancel;

            out_request.client_order_id_ = token_manager.getID(msg->existing_order_token);
            out_request.ticker_id_ = token_manager.id_to_token_[out_request.client_order_id_].tickerId_;

            if (!out_request.client_order_id_) return false;

            out_request.new_client_order_id_ = token_manager.registerNewToken(msg->replacement_order_token, out_request.ticker_id_);

            return true;
        }

    };
    return false;
} 


inline size_t encodeOuch(char* out_buffer, const MEClientResponse& resp, OrderTokenManager& token_manager, uint64_t current_ts) {
    const char* token = token_manager.getToken(resp.client_order_id_);
    if (!token) return 0; 

    switch (resp.status_) {
        case ResponseType::ACCEPTED: {
            auto* msg = reinterpret_cast<ouch::OrderAccepted*>(out_buffer);
            std::memset(msg, ' ', sizeof(ouch::OrderAccepted));

            msg->type = ouch::enums::MsgType::ORDER_ACCEPTED;
            msg->timestamp = ouch::swap64(current_ts);
            std::memcpy(msg->order_token, token, 14);
            
            msg->buy_sell_indicator = (resp.side_ == Side::BUY) ? 'B' : 'S';
            msg->shares = ouch::swap32(resp.qty_);
            *reinterpret_cast<uint64_t*>(msg->stock) = resp.ticker_id_;
            msg->price = ouch::swap32(resp.price_);
            msg->time_in_force = ouch::swap32((resp.type_ == OrderType::FillAndKill) ? 0 : 99998);
            msg->order_reference_number = ouch::swap64(resp.market_order_id_);
            
            return sizeof(ouch::OrderAccepted);
        }

        case ResponseType::EXECUTED: {
            auto* msg = reinterpret_cast<ouch::OrderExecuted*>(out_buffer);
            std::memset(msg, ' ', sizeof(ouch::OrderExecuted));

            msg->type = ouch::enums::MsgType::ORDER_EXECUTED;
            msg->timestamp = ouch::swap64(current_ts);
            std::memcpy(msg->order_token, token, 14);

            msg->executed_shares = ouch::swap32(resp.executed_qty_);
            msg->execution_price = ouch::swap32(resp.execution_price_);
            msg->match_number = ouch::swap64(resp.market_order_id_); 

            return sizeof(ouch::OrderExecuted);
        }

        case ResponseType::CANCELED: {
            auto* msg = reinterpret_cast<ouch::OrderCanceled*>(out_buffer);
            std::memset(msg, ' ', sizeof(ouch::OrderCanceled));

            msg->type = ouch::enums::MsgType::ORDER_CANCELED;
            msg->timestamp = ouch::swap64(current_ts);
            std::memcpy(msg->order_token, token, 14);

            msg->decrement_shares = ouch::swap32(resp.leaves_qty_); 
            msg->reason = 'U'; 

            return sizeof(ouch::OrderCanceled);
        }

        case ResponseType::MODIFIED: {
            auto* msg = reinterpret_cast<ouch::OrderReplaced*>(out_buffer);
            std::memset(msg, ' ', sizeof(ouch::OrderReplaced));

            msg->type = ouch::enums::MsgType::REPLACE_ORDER;
            msg->timestamp = ouch::swap64(current_ts);
            std::memcpy(msg->replacement_order_token, token, 14);

            msg->buy_sell_indicator = (resp.side_ == Side::BUY) ? 'B' : 'S';
            msg->shares = ouch::swap32(resp.qty_);
            *reinterpret_cast<uint64_t*>(msg->stock) = resp.ticker_id_;
            msg->price = ouch::swap32(resp.price_);
            msg->time_in_force = ouch::swap32((resp.type_ == OrderType::FillAndKill) ? 0 : 99998);
            msg->order_reference_number = ouch::swap64(resp.market_order_id_);

            return sizeof(ouch::OrderReplaced);
        }

        case ResponseType::CANCEL_REJECTED: {
            auto* msg = reinterpret_cast<ouch::CancelRejected*>(out_buffer);
            std::memset(msg, ' ', sizeof(ouch::CancelRejected));

            msg->type = ouch::enums::MsgType::CANCEL_REJECTED;
            msg->timestamp = ouch::swap64(current_ts);
            std::memcpy(msg->order_token, token, 14);

            return sizeof(ouch::CancelRejected);
        }

        default:
            return 0;
    }
}

