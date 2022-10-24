#pragma once
#include <string>
#include "client/client_impl.h"

namespace TKV {
int ClientImpl::init() {
    return 0;
}

void ClientImpl::put(const std::string& key,
         const std::string& value,
         braft::Closure* done) {
}

void ClientImpl::get(const std::string& key, 
         std::string* value,
         braft::Closure* done) {
}
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
