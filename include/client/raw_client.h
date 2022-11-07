#pragma once
#include "client/client.h"
#include "client/client_impl.h"
#include "common/closure.h"

namespace TKV {
class RawKVClient: public Client {
public:
    RawKVClient(const std::string& meta_server_bns)
        : _meta_server_bns(meta_server_bns)
    {} 
    
    int init() override;
    
    void put(const std::string& key,
             const std::string& value) override;

    void get(const std::string& key, 
             std::string* value) override;


private:
    ClientImpl*     _kv; 
    std::string     _meta_server_bns; 
};
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
