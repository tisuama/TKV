#pragma once
#include <braft/raft.h>
#include <string>

#include "client/rpc.h"


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
        , _meta_client(std::make_shared<MetaClient>(meta_server_bns, table_name))
        , _cache(std::make_unique<RegionCache>(_meta_client))
        , _rpc_client(std::make_unique<RpcClient>())
    {}

    int init();

    void process_request(AsyncSendMeta* meta, AsyncSendClosure* done);
    void process_response(AsyncSendMeta* meta);
    
    
private: 
    // std::shared_ptr也行
    std::shared_ptr<MetaClient>  _meta_client;
    std::unique_ptr<RegionCache> _cache;
    std::unique_ptr<RpcClient>   _rpc_client;
    bool                _is_inited { false };
}; 
} // namespace TKV
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
