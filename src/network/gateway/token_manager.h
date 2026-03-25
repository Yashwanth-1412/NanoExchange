#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string_view>
#include <unordered_map>
#include <vector>
#include <array>
#include "../../types.h"


class OrderTokenManager {

public:
    struct info {
        std::array<char, 14> token_;
        TickerId tickerId_;
    };
    std::vector<info> id_to_token_;  //OrderId -> Token
    std::unordered_map<std::string_view, OrderId> token_to_id_;   // Token ->OrderId

    OrderId next_internal_id_= 1;

public:
    explicit OrderTokenManager (size_t size = 1000000) {
        id_to_token_.resize(size+1);
        //token_to_id_.reserve(size);
    }

    OrderId registerNewToken (const char* ouch_token, TickerId tickerId) {
        if (next_internal_id_ >= id_to_token_.size()) {
            return  false;
        }

        std::memcpy (id_to_token_[next_internal_id_].token_.data(), ouch_token, 14);
        id_to_token_[next_internal_id_].tickerId_ = tickerId;

        std::string_view token_view(id_to_token_[next_internal_id_].token_.data(), 14);
        token_to_id_.emplace(token_view, next_internal_id_);

        return next_internal_id_++;
    }
    
    // USED FOR INSTANT CANCEL
    OrderId getID (const char* ouch_token) {
        std::string_view token_view(ouch_token, 14);
        auto it = token_to_id_.find(token_view);
        
        if (it != token_to_id_.end()) {
            return it->second;
        }
        return 0; // Token not found (Invalid Cancel Request)
    }

    const char* getToken (OrderId internal_id) {
        if (internal_id > 0 && internal_id < next_internal_id_) {
            return id_to_token_[internal_id].token_.data();
        }
        return nullptr;
    }
    

};