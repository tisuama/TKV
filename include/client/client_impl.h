#pragma once
#include <braft/raft.h>
#include <string>
#include <memory>

#include "client/rpc.h"
#include "client/meta_client.h"
#include "client/region_cache.h"


namespace TKV {
struct RpcSendClosure {
    braft::Closure*     done;
    uint64_t            start_time;
    int                 retry_time;

    virtual void Run();
}; 

class ClientImpl {
public:
    // meta_server_bns: meta_server地址
    // table_name: 请求的表的资源
    ClientImpl(const std::string& meta_server_bns, const std::string& table_name)
        : _meta_client(std::make_shared<MetaClient>(meta_server_bns, table_name))
        , _region_cache(new RegionCache(_meta_client))
        , _rpc_client(new RpcClient)
    {}

    int init();

    void process_request(std::shared_ptr<BatchData> batch_data);
    
    KeyLocation locate_key(const std::string& key);
    
private: 
    // std::shared_ptr也行
    std::shared_ptr<MetaClient>  _meta_client;
    std::unique_ptr<RegionCache> _region_cache;
    std::unique_ptr<RpcClient>   _rpc_client;
    bool                _is_inited { false };
}; 
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */