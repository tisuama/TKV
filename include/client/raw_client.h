#pragma once
#include "client/client.h"
#include "client/client_impl.h"
#include "common/closure.h"

namespace TKV {
class RawKVClient: public Client {
public:
    RawKVClient(const std::string& meta_server_bns, const std::string& table_name)
        : _kv(std::make_shared<ClientImpl>(meta_server_bns, table_name))
    {} 
    
    int init() override;
    
    void put(const std::string& key,
             const std::string& value) override;

    void get(const std::string& key, 
             std::string* value) override;


private:
    std::shared_ptr<ClientImpl> _kv; 
};
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
