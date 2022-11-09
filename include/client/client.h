#pragma once

#include <string>
#include <memory>
#include "common/common.h"

namespace TKV {
class Client;
extern std::shared_ptr<Client> NewRawKVClient(const std::string& meta_server_bns, 
                            const std::string& table_name);

class Client {
public:
    virtual int init() {
        return true;
    }
    
    virtual void put(const std::string& key,
                     const std::string& value) {
        /* Not impl */
    }

    virtual void get(const std::string& key, 
                     std::string* value) {
        /* Not impl */
    }
};
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
