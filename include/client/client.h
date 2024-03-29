#pragma once

#include <string>
#include <memory>
#include "common/common.h"

namespace TKV {
class Client;
class Cluster;
extern std::shared_ptr<Client>  NewRawKVClient(std::shared_ptr<Cluster> cluster); 

extern std::shared_ptr<Cluster> NewCluster(const std::string& meta_server_bns,
        const std::string& table_name);

class Client {
public:
    virtual int init() {
        return true;
    }
    
    virtual int put(const std::string& key,
                     const std::string& value) {
        /* Not impl */
        return 0;
    }

    virtual int get(const std::string& key, 
                     std::string* value) {
        /* Not impl */
        return 0;
    }
};
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
