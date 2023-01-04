#pragma once
#include "client/client.h"
#include "common/closure.h"
#include "client/cluster.h"

namespace TKV {
class RawKVClient: public Client {
public:
    RawKVClient(std::shared_ptr<Cluster> cluster)
        : _cluster(cluster)
    {} 
    
    int init() override;
    
    int put(const std::string& key,
            const std::string& value) override;

    int get(const std::string& key, 
            std::string* value) override;


private:
    std::shared_ptr<Cluster> _cluster; 
};
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
