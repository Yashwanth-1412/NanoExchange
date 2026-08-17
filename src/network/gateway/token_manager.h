#pragma once

#include "src/types.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string_view>
#include <unordered_map>
#include <vector>
using namespace nanoexchange;

class OrderTokenManager {

  public:
    struct info {
        OrderToken token_;
        TickerId   tickerId_;
        ClientId   clientId_;
    };
    std::vector<info> id_to_token_; // OrderId -> Token

    // (ClientId, token) -> OrderId. An OUCH token is only unique within a single
    // client session, so the client id has to be part of the key. Keyed on the
    // token alone, one client could cancel or replace another client's order
    // simply by reusing its token string.
    std::unordered_map<ClientId, std::unordered_map<std::string_view, OrderId>> token_to_id_;

    OrderId next_internal_id_ = 1;

  public:
    explicit OrderTokenManager(size_t size = 1000000) { id_to_token_.resize(size + 1); }

    // Returns 0 when the pool is exhausted. Callers must treat 0 as a hard
    // failure: an order with no token can never have a response encoded for it.
    OrderId registerNewToken(ClientId client_id, const char* ouch_token, TickerId tickerId) {
        if (next_internal_id_ >= id_to_token_.size()) {
            return 0;
        }

        info& slot = id_to_token_[next_internal_id_];
        std::memcpy(slot.token_.data(), ouch_token, OUCH_TOKEN_LEN);
        slot.tickerId_ = tickerId;
        slot.clientId_ = client_id;

        std::string_view token_view(slot.token_.data(), OUCH_TOKEN_LEN);
        // Reused tokens must resolve to the newest order: emplace() would keep
        // the stale mapping and route cancels/replaces at a long-dead order id.
        token_to_id_[client_id].insert_or_assign(token_view, next_internal_id_);

        return next_internal_id_++;
    }

    // USED FOR INSTANT CANCEL
    OrderId getID(ClientId client_id, const char* ouch_token) const {
        const auto client_it = token_to_id_.find(client_id);
        if (client_it == token_to_id_.end())
            return 0;

        const std::string_view token_view(ouch_token, OUCH_TOKEN_LEN);
        const auto             it = client_it->second.find(token_view);

        if (it != client_it->second.end()) {
            return it->second;
        }
        return 0; // Token not found (Invalid Cancel Request)
    }

    const char* getToken(OrderId internal_id) const {
        if (internal_id > 0 && internal_id < next_internal_id_) {
            return id_to_token_[internal_id].token_.data();
        }
        return nullptr;
    }
};