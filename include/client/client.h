#pragma once

#include <string>
#include <memory>
#include "common/common.h"

namespace TKV {
class Client;
extern Client* NewRawClient(const std::string& meta_server_bns);

class Client {
public:
    virtual int inited();
    
    virtual void put(const std::string& key,
                      const std::string& value);

    virtual void get(const std::string& key, 
                     std::string* value);
};
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
